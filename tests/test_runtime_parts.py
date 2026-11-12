"""Runtime parts: config, logs, health, events, the processor loop, scheduled runs."""

from __future__ import annotations

import io
import json
import logging
import time
import urllib.request
from pathlib import Path

import pytest
import yaml
from conftest import RUNTIME_SPEC, FakeControl, FakeKafkaProducer, runtime_cfg

from leasehold import naming
from leasehold.client import NewTask
from leasehold.runtime import log as lh_log
from leasehold.runtime.config import ConfigError, RuntimeConfig
from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer, InvalidEvent, parse
from leasehold.runtime.http import Liveness, serve
from leasehold.runtime.processor import Processor
from leasehold.runtime.scheduled import run_scheduled

# --- config -----------------------------------------------------------------------------------


def test_config_from_env(tmp_path):
    path = tmp_path / "service.yml"
    path.write_text(yaml.safe_dump(RUNTIME_SPEC))
    cfg = RuntimeConfig.from_env(
        {
            "LEASEHOLD_ENV": "dev",
            "LEASEHOLD_SERVICE": "jobs",
            "LEASEHOLD_COMPONENT": "worker",
            "LEASEHOLD_SERVICE_FILE": str(path),
            "LEASEHOLD_KAFKA_BOOTSTRAP": "kafka:9092",
            "LEASEHOLD_KAFKA_PASSWORD": "pw",
            "NOMAD_ALLOC_ID": "alloc-9",
            "NOMAD_PORT_http": "25123",
        }
    )
    assert (cfg.instance, cfg.http_port, cfg.component_spec.kind) == (
        "alloc-9",
        25123,
        "task-worker",
    )
    conf = cfg.kafka_conf()
    assert conf["sasl.username"] == "dev.jobs.worker"
    assert conf["security.protocol"] == "SASL_PLAINTEXT"


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"LEASEHOLD_ENV": "dev"}, "missing"),
        (
            {"LEASEHOLD_ENV": "dev", "LEASEHOLD_SERVICE": "other", "LEASEHOLD_COMPONENT": "x"},
            "not 'other'",
        ),
        (
            {"LEASEHOLD_ENV": "dev", "LEASEHOLD_SERVICE": "jobs", "LEASEHOLD_COMPONENT": "nope"},
            "no component",
        ),
    ],
)
def test_config_errors(env, message, spec):
    with pytest.raises(ConfigError, match=message):
        RuntimeConfig.from_env(env, spec=spec)


# --- logs -------------------------------------------------------------------------------------


def test_json_logs_carry_identity_and_scope():
    buf = io.StringIO()
    lh_log.setup("jobs", "worker", "dev", "abc1234", "alloc-1")
    logging.getLogger().handlers[0].stream = buf
    logger = logging.getLogger("leasehold")
    with lh_log.scope(task_type="jobs.item", task_key="a"):
        logger.info("fetched", extra={"fields": {"entries": 3}})
    logger.info("outside")
    first, second = (json.loads(line) for line in buf.getvalue().splitlines())
    assert first["message"] == "fetched" and first["entries"] == 3
    assert (first["service"], first["component"], first["env"], first["version"]) == (
        "jobs",
        "worker",
        "dev",
        "abc1234",
    )
    assert first["task_key"] == "a" and "task_key" not in second
    assert first["timestamp"].endswith("Z")


# --- health -----------------------------------------------------------------------------------


def test_health_and_metrics_endpoints():
    live = Liveness(stall_after=0.2)
    server = serve(0, live, host="127.0.0.1")
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with urllib.request.urlopen(base + "/health") as r:
            assert r.status == 200 and json.loads(r.read())["status"] == "ok"
        with urllib.request.urlopen(base + "/metrics") as r:
            assert b"python_info" in r.read()
        time.sleep(0.3)
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(base + "/health")
        assert e.value.code == 503
    finally:
        server.shutdown()


# --- events -----------------------------------------------------------------------------------


class Msg:
    def __init__(self, topic, value, offset=0, partition=0, key=None, error=None):
        self._t, self._v, self._o, self._p, self._k, self._e = (
            topic,
            value,
            offset,
            partition,
            key,
            error,
        )

    def error(self):
        return self._e

    def topic(self):
        return self._t

    def value(self):
        return self._v

    def offset(self):
        return self._o

    def partition(self):
        return self._p

    def key(self):
        return self._k


def _produced(n=3, topic="results"):
    kafka = FakeKafkaProducer()
    p = EventProducer(runtime_cfg("worker"), producer=kafka)
    for i in range(n):
        p.emit(topic, {"id": str(i)}, task_type="jobs.item", task_key=str(i))
    return [(t, k, v) for t, k, v in kafka.messages]


def test_events_round_trip():
    (topic, key, value), *_ = _produced(1)
    ref = naming.TopicRef.parse("jobs.results.v1")
    e = parse(Msg(topic, value, offset=7, key=key), ref)
    assert (e.ref, e.offset, e.key, e.payload) == (ref, 7, "0", {"id": "0"})
    assert e.task_type == "jobs.item" and e.service == "jobs" and e.schema_version == 1
    with pytest.raises(InvalidEvent):
        parse(Msg(topic, b"not json"), ref)


def test_payload_size_limit():
    from leasehold.runtime.events import PayloadTooLarge

    p = EventProducer(runtime_cfg("worker"), producer=FakeKafkaProducer())
    with pytest.raises(PayloadTooLarge):
        p.emit("results", {"id": "x", "blob": "a" * 2_000_000})


# --- processor --------------------------------------------------------------------------------


class FakeConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.commits = []
        self.subscribed = None

    def subscribe(self, topics):
        self.subscribed = topics

    def consume(self, n, timeout):
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets=None, asynchronous=True):
        self.commits.append([(o.topic, o.partition, o.offset) for o in offsets])

    def assignment(self):
        return []

    def close(self):
        pass


def _processor(handler, batches, fail_topics=()):
    control = FakeControl()
    cfg = runtime_cfg("router")
    kafka = FakeKafkaProducer(fail_topics)
    ctx = Context(cfg, client=control, producer=EventProducer(cfg, producer=kafka))
    consumer = FakeConsumer(batches)
    p = Processor(ctx, handler, consumer=consumer, max_attempts=2)
    return p, consumer, control, kafka


def _batch(offsets, partition=0):
    msgs = []
    for (topic, key, value), off in zip(_produced(len(offsets)), offsets, strict=True):
        msgs.append(Msg(topic, value, offset=off, partition=partition, key=key))
    return msgs


def test_processor_commits_after_handling(monkeypatch):
    seen = []

    def handle(events, ctx):
        seen.extend(e.offset for e in events)
        ctx.upsert("jobs.item", [NewTask(e.payload["id"]) for e in events])
        ctx.emit("routed", {"n": len(events)})

    p, consumer, control, kafka = _processor(handle, [_batch([10, 11, 12])])
    assert consumer.subscribed is None
    p.consumer.subscribe(sorted(p.refs))
    assert consumer.subscribed == ["dev.jobs.results.v1"]
    assert p.step()
    assert seen == [10, 11, 12]
    assert consumer.commits == [[("dev.jobs.results.v1", 0, 13)]]  # one past the last
    assert control.upserts[0][0] == "jobs.item" and len(control.upserts[0][1]) == 3
    assert [t for t, _, _ in kafka.messages if t.endswith("routed.v1")] == ["dev.jobs.routed.v1"]


def test_processor_does_not_commit_a_failing_batch(monkeypatch):
    monkeypatch.setattr("leasehold.runtime.processor.time.sleep", lambda s: None)
    calls = []

    def handle(events, ctx):
        calls.append(1)
        raise RuntimeError("downstream is down")

    p, consumer, _, _ = _processor(handle, [_batch([5])])
    assert p.step() is False
    assert len(calls) == 2 and consumer.commits == []


def test_processor_skips_invalid_events_but_commits_past_them():
    seen = []
    good = _batch([2])
    bad = Msg("dev.jobs.results.v1", b"{broken", offset=1)
    p, consumer, _, _ = _processor(lambda ev, ctx: seen.extend(ev), [[bad, *good]])
    assert p.step()
    assert [e.offset for e in seen] == [2]
    assert consumer.commits == [[("dev.jobs.results.v1", 0, 3)]]


def test_processor_may_only_create_its_own_task_types():
    def handle(events, ctx):
        ctx.upsert("other.thing", [NewTask("x")])

    p, consumer, _, _ = _processor(handle, [_batch([1])])
    assert p.step() is False


# --- scheduled --------------------------------------------------------------------------------


def _sched_ctx(**cfg):
    control = FakeControl()
    return Context(runtime_cfg("planner", **cfg), client=control), control


def test_scheduled_run_ok():
    ctx, control = _sched_ctx()
    assert run_scheduled(lambda run: run.upsert("jobs.item", [NewTask("a")]), ctx=ctx) == 0
    assert control.upserts[0][0] == "jobs.item"


def test_scheduled_run_failure_and_timeout():
    ctx, _ = _sched_ctx()

    def boom(run):
        raise RuntimeError("x")

    assert run_scheduled(boom, ctx=ctx) == 1
    ctx, _ = _sched_ctx(run_timeout=0.2)
    assert run_scheduled(lambda run: time.sleep(2), ctx=ctx) == 1


def test_scheduled_has_no_producer_without_produces():
    ctx, _ = _sched_ctx()
    with pytest.raises(ConfigError, match="produces no topics"):
        ctx.emit("results", {})


def test_base_image_files_exist():
    root = Path(__file__).parent.parent
    assert (root / "images" / "base" / "Dockerfile").exists()
