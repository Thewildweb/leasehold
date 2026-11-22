"""Claim check in the producer and processor, and the processor's batch byte limit."""

from __future__ import annotations

import copy
import json

import pytest
from conftest import RUNTIME_SPEC, FakeControl, FakeKafkaProducer
from test_archive import FakeStore
from test_runtime_parts import Msg

from leasehold.runtime.config import RuntimeConfig
from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer, PayloadTooLarge
from leasehold.runtime.processor import (
    DEFAULT_BATCH_MAX_BYTES,
    Processor,
    batch_max_bytes_from_env,
    prefetch_conf,
)
from leasehold.spec import ServiceSpec

TOPIC = "dev.jobs.results.v1"


def _spec(large=True, reader_grant=True) -> ServiceSpec:
    d = copy.deepcopy(RUNTIME_SPEC)
    d["buckets"] = {"blobs": {}}
    if large:
        d["kafka"]["produces"]["results"]["large_payloads"] = "blobs"
        d["components"]["worker"]["writes_buckets"] = ["blobs"]
    if reader_grant:
        d["components"]["router"]["reads_buckets"] = ["blobs"]
    return ServiceSpec.model_validate(d)


def _cfg(component, spec) -> RuntimeConfig:
    return RuntimeConfig(
        env="dev", service="jobs", component=component, spec=spec, instance="a",
        payload_models={"jobs.results.v1": "payload_models:R"},
    )  # fmt: skip


def _emit(store, payloads, spec=None):
    spec = spec or _spec()
    kafka = FakeKafkaProducer()
    p = EventProducer(_cfg("worker", spec), producer=kafka, objects=store)
    for pl in payloads:
        p.emit("results", pl)
    return [v for _, _, v in kafka.messages]


# --- producer --------------------------------------------------------------------------------


def test_large_payload_goes_to_the_bucket():
    store = FakeStore()
    small, big = _emit(store, [{"id": "s"}, {"id": "b", "blob": "x" * 2_500_000}])
    assert json.loads(small)["payload_ref"] is None
    env = json.loads(big)
    assert env["payload"] is None
    ref = env["payload_ref"]
    assert ref["bucket"] == "jobs.blobs"
    assert ref["key"].startswith("claims/jobs.results.v1/dt=")
    assert ref["key"].endswith(f"/{env['event_id']}.json.gz")
    assert ("dev-jobs-blobs", ref["key"]) in store.objects
    assert len(big) < 2000


def test_large_payload_without_claim_check_is_refused():
    d = copy.deepcopy(RUNTIME_SPEC)
    spec = ServiceSpec.model_validate(d)
    p = EventProducer(_cfg("worker", spec), producer=FakeKafkaProducer(), objects=FakeStore())
    with pytest.raises(PayloadTooLarge, match="set large_payloads"):
        p.emit("results", {"id": "b", "blob": "x" * 2_000_000})


# --- processor -------------------------------------------------------------------------------


class ChunkConsumer:
    """Hands out its messages ``n`` at a time, like librdkafka's consume()."""

    def __init__(self, msgs):
        self.msgs = list(msgs)
        self.commits = []

    def consume(self, n, timeout):
        out, self.msgs = self.msgs[:n], self.msgs[n:]
        return out

    def commit(self, offsets=None, asynchronous=True):
        self.commits.append([(o.partition, o.offset) for o in offsets])

    def assignment(self):
        return []


def _processor(msgs, spec=None, objects=None, **kw):
    spec = spec or _spec()
    cfg = _cfg("router", spec)
    ctx = Context(
        cfg, client=FakeControl(), producer=EventProducer(cfg, producer=FakeKafkaProducer())
    )
    batches = []
    consumer = ChunkConsumer(msgs)
    p = Processor(
        ctx, lambda events, c: batches.append([e.offset for e in events]), consumer=consumer,
        objects=objects, **kw,
    )  # fmt: skip
    return p, consumer, batches


def _msgs(values):
    return [Msg(TOPIC, v, offset=i) for i, v in enumerate(values)]


def test_batch_closes_on_bytes():
    values = _emit(FakeStore(), [{"id": str(i), "pad": "x" * 900} for i in range(10)])
    size = len(values[0])
    p, consumer, batches = _processor(_msgs(values), batch_max_bytes=size * 3 + 10)
    for _ in range(4):
        assert p.step()
    assert batches == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    assert [c[0][1] for c in consumer.commits] == [3, 6, 9, 10]


def test_batch_still_closes_on_count():
    values = _emit(FakeStore(), [{"id": str(i)} for i in range(5)])
    p, _, batches = _processor(_msgs(values), batch_size=2)
    assert p.step() and p.step() and p.step()
    assert batches == [[0, 1], [2, 3], [4]]


def test_oversize_message_is_a_batch_of_its_own():
    values = _emit(FakeStore(), [{"id": "a"}, {"id": "b", "pad": "x" * 5000}, {"id": "c"}])
    p, _, batches = _processor(_msgs(values), batch_max_bytes=1000)
    for _ in range(3):
        assert p.step()
    assert batches == [[0], [1], [2]]


def test_claimed_payloads_are_resolved_and_counted():
    store = FakeStore()
    values = _emit(store, [{"id": "a"}, {"id": "big", "blob": "y" * 2_500_000}, {"id": "c"}])
    seen = []
    spec = _spec()
    cfg = _cfg("router", spec)
    ctx = Context(
        cfg, client=FakeControl(), producer=EventProducer(cfg, producer=FakeKafkaProducer())
    )
    p = Processor(ctx, lambda ev, c: seen.append(ev), consumer=ChunkConsumer(_msgs(values)),
                  objects=store, batch_max_bytes=1_000_000)  # fmt: skip
    assert p.step() and p.step() and p.step()
    # The claim is small on the wire, but its resolved payload counts: it doesn't fit
    # after offset 0, and it fills a batch by itself.
    assert [[e.offset for e in b] for b in seen] == [[0], [1], [2]]
    big = next(e for b in seen for e in b if e.offset == 1)
    assert big.payload["blob"] == "y" * 2_500_000


def test_no_read_grant_stops_with_a_clear_error():
    store = FakeStore()
    store.denied.add("dev-jobs-blobs")
    values = _emit(store, [{"id": "big", "blob": "y" * 2_500_000}])
    p, _, batches = _processor(_msgs(values), objects=store)
    p.consumer.subscribe = lambda topics, on_revoke=None: None
    p.consumer.close = lambda: None
    assert p.run() == 1
    assert batches == []


def test_missing_claim_is_skipped_and_committed():
    store = FakeStore()
    values = _emit(store, [{"id": "big", "blob": "y" * 2_500_000}, {"id": "ok"}])
    store.objects.clear()
    p, consumer, batches = _processor(_msgs(values), objects=store)
    assert p.step()
    assert batches == [[1]] and consumer.commits == [[(0, 2)]]


def test_batch_limit_settings(monkeypatch):
    monkeypatch.delenv("LEASEHOLD_BATCH_MAX_BYTES", raising=False)
    assert batch_max_bytes_from_env(None) == DEFAULT_BATCH_MAX_BYTES == 64 * 1024 * 1024
    assert batch_max_bytes_from_env(1000) == 1000
    monkeypatch.setenv("LEASEHOLD_BATCH_MAX_BYTES", "5000")
    assert batch_max_bytes_from_env(1000) == 5000
    assert prefetch_conf(64 * 1024 * 1024) == {
        "queued.max.messages.kbytes": 32 * 1024,
        "fetch.max.bytes": 32 * 1024 * 1024,
    }
    assert prefetch_conf(1024)["queued.max.messages.kbytes"] == 4096
    assert prefetch_conf(1024 * 1024 * 1024)["fetch.max.bytes"] == 50 * 1024 * 1024
