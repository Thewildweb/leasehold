"""The task-worker loop with a fake control API and a fake Kafka producer."""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
from conftest import FakeControl, FakeKafkaProducer, runtime_cfg

from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer
from leasehold.runtime.worker import Permanent, Retry, Worker


def make_worker(handler, keys=("a", "b", "c"), concurrency=2, prefetch=None, fail_topics=(), **cfg):
    control = FakeControl(keys)
    kafka = FakeKafkaProducer(fail_topics)
    c = runtime_cfg("worker", **cfg)
    ctx = Context(c, client=control, producer=EventProducer(c, producer=kafka))
    return Worker(ctx, handler, concurrency, prefetch), control, kafka


def run_until(worker, cond, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        worker.step(wait=0.05)
        if cond():
            return
    raise AssertionError("condition not reached")


def test_handles_emits_and_acks_with_event_id():
    def handle(task, ctx):
        ctx.emit("results", {"id": task.key, "n": 1})

    w, control, kafka = make_worker(handle)
    run_until(w, lambda: len(control.acked) == 3)
    assert {a.ref.key for a in control.acked} == {"a", "b", "c"}
    # One event per task, on the env-prefixed topic, keyed by the topic's key field.
    topics = {t for t, _, _ in kafka.messages}
    assert topics == {"dev.jobs.results.v1"}
    by_key = {k.decode(): json.loads(v) for _, k, v in kafka.messages}
    assert set(by_key) == {"a", "b", "c"}
    env = by_key["a"]
    assert env["task_type"] == "jobs.item" and env["task_key"] == "a"
    assert env["service"] == "jobs" and env["env"] == "dev" and env["schema_version"] == 1
    acked = {a.ref.key: str(a.event_id) for a in control.acked}
    assert acked["a"] == env["event_id"]


def test_retry_permanent_and_errors_fail_the_attempt():
    def handle(task, ctx):
        if task.key == "a":
            raise Retry("rate limited", after=120)
        if task.key == "b":
            raise Permanent("gone")
        raise ValueError("bug")

    w, control, _ = make_worker(handle)
    run_until(w, lambda: len(control.failed) == 3)
    by_key = {f.ref.key: f for f in control.failed}
    assert by_key["a"].retry_after == timedelta(seconds=120) and not by_key["a"].permanent
    assert by_key["b"].permanent and by_key["b"].reason == "gone"
    assert by_key["c"].reason == "ValueError: bug"
    assert control.acked == []


def test_undelivered_event_fails_the_attempt():
    def handle(task, ctx):
        ctx.emit("results", {"id": task.key})

    w, control, _ = make_worker(handle, keys=("a",), fail_topics={"dev.jobs.results.v1"})
    run_until(w, lambda: len(control.failed) == 1)
    assert control.failed[0].reason.startswith("produce failed")
    assert control.acked == []


def test_may_not_emit_to_undeclared_topic():
    def handle(task, ctx):
        ctx.emit("routed", {"x": 1})  # declared by the service, produced by another component

    w, control, _ = make_worker(handle, keys=("a",))
    run_until(w, lambda: len(control.failed) == 1)
    assert "may not produce to 'routed'" in control.failed[0].reason


def test_concurrency_limit_and_prefetch():
    started = []
    release = threading.Event()

    def handle(task, ctx):
        started.append(task.key)
        release.wait(5)

    w, control, _ = make_worker(handle, keys=[str(i) for i in range(10)], concurrency=2, prefetch=3)
    for _ in range(5):
        w.step(wait=0.05)
    assert len(w.running) == 2  # never more than `concurrency` in the handler
    assert len(w.queued) == 3  # and `prefetch` waiting
    assert len(control.pending) == 5
    release.set()
    run_until(w, lambda: len(control.acked) == 10)


def test_lost_lease_drops_queued_and_flags_running():
    gate = threading.Event()
    seen_lost = {}

    def handle(task, ctx):
        gate.wait(5)
        seen_lost[task.key] = task.is_lost

    w, control, _ = make_worker(handle, keys=("a", "b", "c"), concurrency=1, prefetch=2)
    w.step(wait=0.05)
    running = next(iter(w.running.values())).task.key
    queued = [h.task.key for h in w.queued]
    control.lost_on_renew = {running, queued[0]}
    # Make renewal due now.
    for h in list(w.queued) + list(w.running.values()):
        h.expires = datetime.now(UTC) + timedelta(seconds=1)
    w.step(wait=0.05)
    assert [h.task.key for h in w.queued] == [queued[1]]
    gate.set()
    run_until(w, lambda: len(control.acked) == 2)
    assert seen_lost[running] is True


def test_outcomes_are_kept_until_the_api_accepts_them():
    gate = threading.Event()

    def handle(task, ctx):
        gate.wait(5)
        ctx.emit("results", {"id": task.key})

    w, control, _ = make_worker(handle, keys=("a",))
    run_until(w, lambda: w.running)
    control.down = True  # the API goes away while the task runs
    gate.set()
    run_until(w, lambda: w.acks and not w.running)
    assert control.acked == []
    control.down = False
    run_until(w, lambda: control.acked)
    assert [a.ref.key for a in control.acked] == ["a"] and w.acks == []


def test_api_down_fails_closed_at_lease_expiry():
    gate = threading.Event()

    def handle(task, ctx):
        gate.wait(5)

    w, control, _ = make_worker(handle, keys=("a", "b"), concurrency=1, prefetch=1)
    w.step(wait=0.05)
    control.down = True
    for h in list(w.queued) + list(w.running.values()):
        h.expires = datetime.now(UTC) - timedelta(seconds=1)
    w.step(wait=0.05)
    assert not w.queued  # the waiting one is gone
    assert next(iter(w.running.values())).task.is_lost
    gate.set()


def test_drain_releases_waiting_and_finishes_running():
    gate = threading.Event()

    def handle(task, ctx):
        gate.wait(5)
        ctx.emit("results", {"id": task.key})

    w, control, _ = make_worker(
        handle, keys=("a", "b", "c", "d"), concurrency=2, prefetch=2, drain_timeout=5
    )
    w.step(wait=0.05)
    assert len(w.running) == 2 and len(w.queued) == 2
    w.ctx.stopping.set()
    threading.Timer(0.2, gate.set).start()
    w.drain()
    assert len(control.released) == 2  # never started
    assert len(control.acked) == 2  # finished during the drain


def test_drain_timeout_releases_running():
    never = threading.Event()

    def handle(task, ctx):
        never.wait(3)

    w, control, _ = make_worker(handle, keys=("a",), concurrency=1, prefetch=0, drain_timeout=0.3)
    w.step(wait=0.05)
    w.ctx.stopping.set()
    w.drain()
    assert [r.key for r in control.released] == ["a"]
    never.set()


def test_idle_backoff():
    w, control, _ = make_worker(lambda t, c: None, keys=())
    w.step(wait=0)
    first = w.idle_delay
    w.next_lease = 0
    w.step(wait=0)
    assert first == 0.5 and w.idle_delay == 1.0


def test_wrong_component_kind():
    from leasehold.runtime.config import ConfigError
    from leasehold.runtime.context import build

    with pytest.raises(ConfigError, match="is a processor, not a task-worker"):
        build("task-worker", cfg=runtime_cfg("router"), client=FakeControl())
