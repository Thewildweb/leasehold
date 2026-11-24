"""The task-worker with target leases, against a fake control API."""

from __future__ import annotations

import copy
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from conftest import RUNTIME_SPEC, FakeControl, FakeKafkaProducer

from leasehold.client import Applied, LeasedTask, LeaseRef, TargetLease
from leasehold.runtime.config import ConfigError, RuntimeConfig
from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer
from leasehold.runtime.worker import Defer, Retry, Worker
from leasehold.spec import ServiceSpec


def spec() -> ServiceSpec:
    data = copy.deepcopy(RUNTIME_SPEC)
    data["frontier"]["task_types"]["jobs.item"]["target_leases"] = True
    return ServiceSpec.model_validate(data)


class TargetControl(FakeControl):
    """Keys ``t:x`` go to target ``t``; one target lease per target, granted with the
    first lease call that hands out its tasks."""

    def __init__(self, keys, interval_s=0.2, target_s=30.0, lost_targets=()):
        super().__init__(keys)
        self.interval_s = interval_s
        self.target_s = target_s
        self.target_ids: dict[str, uuid.UUID] = {}
        self.lost_targets = set(lost_targets)
        self.renew_calls: list[tuple] = []
        self.egresses: list = []

    def lease_with_targets(self, max, task_types=None, worker_id=None, per_group=None,
                           held=None, egress=None):  # fmt: skip
        self._check()
        self.egresses.append(egress)
        tasks, targets = [], []
        with self.lock:
            for key in list(self.pending)[:max]:
                self.pending.remove(key)
                target = key.split(":")[0]
                ref = LeaseRef(self.task_type, key, uuid.uuid4())
                exp = datetime.now(UTC) + timedelta(seconds=self.lease_s)
                self.leases[ref] = exp
                new = target not in self.target_ids
                tid = self.target_ids.setdefault(target, uuid.uuid4())
                if new:
                    targets.append(
                        TargetLease(
                            target,
                            tid,
                            datetime.now(UTC) + timedelta(seconds=self.target_s),
                            self.interval_s,
                        )  # fmt: skip
                    )
                tasks.append(
                    LeasedTask(
                        ref,
                        exp,
                        50,
                        0,
                        {"id": key},
                        group=target,
                        target=target,
                        target_lease_id=tid,
                    )  # fmt: skip
                )
        return tasks, targets

    def renew_targets(self, egress, leases, release=(), worker_id=None, routes=None):
        self._check()
        self.renew_calls.append((egress, list(leases), list(release)))
        renewed, lost = {}, {}
        for i in leases:
            target = next(t for t, x in self.target_ids.items() if x == i)
            if target in self.lost_targets:
                lost[i] = "revoked: test"
            else:
                renewed[i] = datetime.now(UTC) + timedelta(seconds=self.target_s)
        return renewed, lost

    def fail(self, items, egress=None):
        self.egresses.append(egress)
        return super().fail(items)

    def release(self, refs, egress=None):
        self._check()
        self.released.extend(refs)
        return Applied(len(refs))


_WORKERS: list = []


@pytest.fixture(autouse=True)
def _stop_workers():
    """Stop every worker a test made, so no handler thread outlives its test."""
    yield
    while _WORKERS:
        w = _WORKERS.pop()
        w.ctx.stopping.set()
        for h in list(w.running.values()):
            h.task.lost.set()
        w.pool.shutdown(wait=True, cancel_futures=True)
        w.ctx.close()


def make(handler, control, concurrency=4, egress="192.0.2.7"):
    cfg = RuntimeConfig(
        env="dev",
        service="jobs",
        component="worker",
        spec=spec(),
        instance="alloc-1",
        egress_ip=egress,
        payload_models={"jobs.results.v1": "payload_models:R"},
    )
    ctx = Context(cfg, client=control, producer=EventProducer(cfg, producer=FakeKafkaProducer()))
    w = Worker(ctx, handler, concurrency)
    _WORKERS.append(w)
    return w


def run_until(worker, cond, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        worker.step(wait=0.02)
        if cond():
            return
    raise AssertionError("condition not reached")


def test_needs_an_egress():
    with pytest.raises(ConfigError, match="LEASEHOLD_EGRESS_IP"):
        make(lambda t, c: None, TargetControl([]), egress=None)


def test_one_task_per_target_at_a_time_at_the_interval():
    starts: dict[str, list[float]] = {}
    running: dict[str, int] = {}
    overlap = []
    lock = threading.Lock()

    def handle(task, ctx):
        with lock:
            starts.setdefault(task.target, []).append(time.monotonic())
            running[task.target] = running.get(task.target, 0) + 1
            if running[task.target] > 1:
                overlap.append(task.target)
        time.sleep(0.05)
        with lock:
            running[task.target] -= 1
        ctx.emit("results", {"id": task.key, "n": 1})

    control = TargetControl(["a:1", "a:2", "a:3", "b:1"], interval_s=0.2)
    w = make(handle, control)
    run_until(w, lambda: len(control.acked) == 4)
    assert not overlap
    gaps = [b - a for a, b in zip(starts["a"], starts["a"][1:], strict=False)]
    assert all(g >= 0.19 for g in gaps), gaps
    assert starts["b"][0] < starts["a"][1]  # another target doesn't wait
    assert set(control.egresses) == {"192.0.2.7"}
    # Done: both target leases are given back.
    run_until(w, lambda: any(r for _, _, r in control.renew_calls if len(r) == 2) or
              sum(len(r) for _, _, r in control.renew_calls) >= 2)  # fmt: skip
    assert w.targets == {}


def test_lost_target_stops_its_tasks(monkeypatch):
    import leasehold.runtime.worker as worker_mod

    monkeypatch.setattr(worker_mod, "TARGET_MARGIN_S", 0.1)

    started = []

    def handle(task, ctx):
        started.append(task.key)
        ctx.emit("results", {"id": task.key, "n": 1})

    control = TargetControl(["a:1", "a:2", "a:3"], interval_s=1.0, target_s=0.6)
    control.lost_targets = {"a"}
    w = make(handle, control)
    run_until(w, lambda: len(control.released) == 2)
    assert started == ["a:1"]
    assert {r.ref.key for r in control.released} == {"a:2", "a:3"}


def test_no_start_close_to_expiry():
    """Inside the safety margin before the target lease expires, nothing starts."""
    control = TargetControl(["a:1"], target_s=2.0)  # margin is 5 s
    w = make(lambda t, c: c.emit("results", {"id": t.key, "n": 1}), control)
    run_until(w, lambda: len(control.released) == 1)
    assert not control.acked


def test_pushback_cools_the_target_and_stops_the_rest():
    def handle(task, ctx):
        if task.key == "a:1":
            raise Retry("429", after=600, cooldown=True)
        ctx.emit("results", {"id": task.key, "n": 1})

    control = TargetControl(["a:1", "a:2", "b:1"], interval_s=0.3)
    w = make(handle, control, concurrency=1)
    run_until(w, lambda: control.failed and len(control.acked) == 1 and control.released)
    (f,) = control.failed
    assert f.cooldown and f.json()["cooldown"] is True and f.retry_after == timedelta(seconds=600)
    assert "192.0.2.7" in control.egresses
    assert [r.ref.key for r in control.released] == ["a:2"]
    assert [a.ref.key for a in control.acked] == ["b:1"]


def test_defer_with_cooldown():
    def handle(task, ctx):
        raise Defer("retry-after", after=120, cooldown=True)

    control = TargetControl(["a:1"])
    w = make(handle, control)
    run_until(w, lambda: control.released)
    (r,) = control.released
    assert r.json() == {
        "task_type": "jobs.item",
        "key": "a:1",
        "lease_id": str(r.ref.lease_id),
        "after_s": 120.0,
        "cooldown": True,
        "reason": "retry-after",
    }


# --- with the machine's agent -----------------------------------------------------------------


class AgentControl(TargetControl):
    """The agent's side of the client: tokens paced per target, here in the fake."""

    via_agent = True

    def __init__(self, keys, interval_s=0.2, lost=None, extra_budget=True):
        super().__init__(keys, interval_s=interval_s)
        self.next_at: dict = {}
        self.lost = lost or {}
        self.extra_budget = extra_budget
        self.token_calls: list[tuple] = []
        self.grants: list[tuple[float, bool]] = []

    def agent_token(self, target_lease_id, task_type, cost=1, first=False, credentials_fp=None):
        self._check()
        self.token_calls.append((target_lease_id, cost, first))
        if target_lease_id in self.lost:
            return {"granted": False, "lost": self.lost[target_lease_id]}
        now = time.monotonic()
        if now < self.next_at.get(target_lease_id, 0):
            return {"granted": False, "wait_s": self.next_at[target_lease_id] - now}
        if not first and not self.extra_budget:
            return {"granted": False, "wait_s": 0.05, "reason": "no budget left"}
        self.next_at[target_lease_id] = now + self.interval_s
        self.grants.append((now, first))
        return {"granted": True}

    def renew_targets(self, *a, **kw):
        raise AssertionError("with an agent the worker never renews target leases")


def test_agent_mode_needs_no_egress_and_paces_through_the_agent():
    def handle(task, ctx):
        ctx.token()  # one more request in the same task
        ctx.emit("results", {"id": task.key, "n": 1})

    control = AgentControl(["a:1", "a:2"], interval_s=0.15)
    w = make(handle, control, egress=None)
    run_until(w, lambda: len(control.acked) == 2)
    firsts = [c for c in control.token_calls if c[2]]
    extras = [c for c in control.token_calls if not c[2] and c[1] == 1]
    assert len(firsts) >= 2 and len(extras) >= 2
    # Every grant on the target is at least the interval after the previous one: first,
    # extra, next task's first, extra.
    times = [g for g, _ in control.grants]
    assert len(times) == 4
    assert all(b - a >= 0.149 for a, b in zip(times, times[1:], strict=False)), times


def test_agent_mode_lost_target():
    def handle(task, ctx):
        ctx.emit("results", {"id": task.key, "n": 1})

    control = AgentControl(["a:1"])
    w = make(handle, control, egress=None)
    w._lease()
    (tid,) = w.targets
    control.lost[tid] = "revoked: operator"
    run_until(w, lambda: control.released)
    assert not control.acked
    assert [r.ref.key for r in control.released] == ["a:1"]


def test_extra_token_without_budget_gives_the_task_back(monkeypatch):
    def handle(task, ctx):
        ctx.token(cost=5)

    import leasehold.runtime.worker as worker_mod

    monkeypatch.setattr(worker_mod, "EXTRA_TOKEN_TIMEOUT_S", 0.2)
    control = AgentControl(["a:1"], extra_budget=False)
    w = make(handle, control, egress=None)
    run_until(w, lambda: control.released)
    (r,) = control.released
    assert r.after == timedelta(0) and not r.cooldown


def test_unreachable_agent_waits_and_never_falls_back(tmp_path):
    """LEASEHOLD_AGENT_SOCKET set: the control API URL is never used, and a missing
    socket only makes the worker wait and try again."""
    from leasehold.client import ControlClient

    sock = str(tmp_path / "agent.sock")  # nothing listens here
    client = ControlClient.from_env(
        {
            "LEASEHOLD_AGENT_SOCKET": sock,
            "LEASEHOLD_API_URL": "http://192.0.2.250:9",  # must never be called
            "LEASEHOLD_TOKEN": "t",
        }
    )
    client.retries = 0
    assert client.via_agent and str(client.http.base_url) == "http://agent"
    w = make(lambda t, c: None, client, egress=None)
    for _ in range(3):
        w.next_lease = 0
        w.step(wait=0.01)
    assert w.idle_delay > 0 and not w.queued and not w.running
