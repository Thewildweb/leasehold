"""The archive sink: the row layout, cutting files, replacing uncommitted files, claims."""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import threading
from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq
import pytest

from leasehold import naming
from leasehold.archive import layout
from leasehold.archive.sink import Settings, Sink, file_key
from leasehold.runtime import claims
from leasehold.spec.models import ArchiveSink
from leasehold.storage.client import AccessDenied, NotFound, ObjectInfo

SINK = ArchiveSink.model_validate(
    {
        "topic": "responses",
        "bucket": "archive",
        "node_class": "x",
        "columns": {
            "domain": "string",
            "status": "int32",
            "elapsed_ms": "float64",
            "https_fallback": "bool",
            "seen": "timestamp",
            "hops": "json",
            "body_raw": {"type": "base64", "field": "body_b64", "text_fallback": "body"},
        },
        "drop": ["secret"],
        "file_size_mb": 1,
        "row_group_mb": 1,
        "max_age": "5m",
    }
)
T0 = int(datetime(2026, 11, 17, 10, 59, tzinfo=UTC).timestamp() * 1000)


def _env(payload, **kw) -> dict:
    return {
        "event_id": kw.get("event_id", "e1"),
        "schema_version": 1,
        "service": "hh",
        "component": "worker",
        "env": "dev",
        "worker": "w",
        "task_type": "hh.fetch",
        "task_key": "a.nl",
        "target": "1.2.3.4",
        "egress_ip": None,
        "ts": "2026-11-17T10:59:00.123Z",
        "payload": payload,
        "payload_ref": kw.get("payload_ref"),
    }


# --- layout ----------------------------------------------------------------------------------


def test_row_lifts_columns_and_keeps_the_rest():
    payload = {
        "domain": "a.nl",
        "status": 200,
        "elapsed_ms": 12,
        "https_fallback": False,
        "seen": "2026-11-17T10:00:00+01:00",
        "hops": [{"url": "https://a.nl/", "status": 200}],
        "body": "héllo",
        "body_b64": base64.b64encode("héllo".encode()).decode(),
        "secret": "x",
        "new_field": [1, 2],
    }
    row = layout.to_row(SINK, _env(payload), payload, partition=3, offset=7, kafka_ts_ms=T0)
    assert row["event_id"] == "e1" and row["schema_version"] == 1 and row["egress_ip"] is None
    assert row["ts"] == datetime(2026, 11, 17, 10, 59, 0, 123000, tzinfo=UTC)
    assert (row["_partition"], row["_offset"]) == (3, 7)
    assert row["_kafka_ts"] == datetime(2026, 11, 17, 10, 59, tzinfo=UTC)
    assert row["domain"] == "a.nl" and row["status"] == 200 and row["elapsed_ms"] == 12.0
    assert row["https_fallback"] is False
    assert row["seen"] == datetime(2026, 11, 17, 9, 0, tzinfo=UTC)
    assert json.loads(row["hops"]) == payload["hops"]
    assert row["body_raw"] == "héllo".encode() and row["body_raw_from_text"] is False
    assert row["payload_ref"] is None
    # Lifted, dropped and the text fallback are left out; unknown fields stay.
    assert json.loads(row["payload"]) == {"new_field": [1, 2]}


def test_body_from_text_when_there_are_no_bytes():
    payload = {"domain": "a.nl", "body": "oud"}
    row = layout.to_row(SINK, _env(payload), payload, partition=0, offset=0, kafka_ts_ms=None)
    assert row["body_raw"] == b"oud" and row["body_raw_from_text"] is True
    assert row["_kafka_ts"] is None
    payload = {"domain": "a.nl"}
    row = layout.to_row(SINK, _env(payload), payload, partition=0, offset=0, kafka_ts_ms=None)
    assert row["body_raw"] is None and row["body_raw_from_text"] is None
    payload = {"body_b64": "", "body": ""}
    row = layout.to_row(SINK, _env(payload), payload, partition=0, offset=0, kafka_ts_ms=None)
    assert row["body_raw"] == b"" and row["body_raw_from_text"] is False


def test_values_that_dont_fit_stay_in_payload():
    payload = {"domain": 5, "status": "200", "elapsed_ms": True, "seen": "not a date",
               "body_b64": "!!"}  # fmt: skip
    row = layout.to_row(SINK, _env(payload), payload, partition=0, offset=0, kafka_ts_ms=None)
    assert row["domain"] == "5"  # numbers become text in a string column
    assert row["status"] is None and row["elapsed_ms"] is None and row["seen"] is None
    rest = json.loads(row["payload"])
    assert rest == {"status": "200", "elapsed_ms": True, "seen": "not a date", "body_b64": "!!"}
    row = layout.to_row(SINK, _env(None), None, partition=0, offset=0, kafka_ts_ms=None)
    assert row["payload"] is None and row["domain"] is None
    big = {"status": 2**40}
    row = layout.to_row(SINK, _env(big), big, partition=0, offset=0, kafka_ts_ms=None)
    assert row["status"] is None and json.loads(row["payload"]) == big


def test_schema_matches_rows():
    import pyarrow as pa

    schema = layout.arrow_schema(SINK)
    assert schema.names[: len(layout.FIXED_COLUMNS) - 1] == list(layout.FIXED_COLUMNS[:-1])
    assert schema.names[-1] == "payload"
    assert schema.field("body_raw").type == pa.binary()
    assert schema.field("body_raw_from_text").type == pa.bool_()
    payload = {"domain": "a.nl", "status": 1}
    row = layout.to_row(SINK, _env(payload), payload, partition=0, offset=0, kafka_ts_ms=T0)
    assert pa.Table.from_pylist([row], schema=schema).num_rows == 1


# --- the sink --------------------------------------------------------------------------------


class Msg:
    def __init__(self, partition, offset, value, ts_ms):
        self._p, self._o, self._v, self._ts = partition, offset, value, ts_ms

    def error(self):
        return None

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def timestamp(self):
        return (1, self._ts) if self._ts is not None else (0, -1)

    def topic(self):
        return "dev.hh.responses.v1"


class FakeConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.commits: list[tuple[int, int]] = []
        self.on_revoke = None

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        self.on_revoke = on_revoke

    def consume(self, n, wait):
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets, asynchronous):
        self.commits += [(tp.partition, tp.offset) for tp in offsets]

    def assignment(self):
        return []

    def close(self):
        pass


class FakeStore:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.denied: set[str] = set()

    def put_file(self, bucket, key, path, content_type):
        with open(path, "rb") as f:
            self.objects[(bucket, key)] = f.read()

    def put_bytes(self, bucket, key, data, content_type):
        self.objects[(bucket, key)] = data

    def get_bytes(self, bucket, key, max_size=None):
        if bucket in self.denied:
            raise AccessDenied(bucket)
        if (bucket, key) not in self.objects:
            raise NotFound(key)
        return self.objects[(bucket, key)]

    def list(self, bucket, prefix):
        for (b, k), v in sorted(self.objects.items()):
            if b == bucket and k.startswith(prefix):
                yield ObjectInfo(k, len(v))

    def delete(self, bucket, key):
        del self.objects[(bucket, key)]

    def keys(self, bucket="dev-hh-archive"):
        return sorted(k for b, k in self.objects if b == bucket and not k.startswith("claims/"))

    def table(self, key, bucket="dev-hh-archive"):
        return pq.read_table(io.BytesIO(self.objects[(bucket, key)]))


def _settings(tmp_path, sink=SINK) -> Settings:
    return Settings(
        env="dev",
        service="hh",
        component="archive-responses",
        sink_name="responses",
        sink=sink,
        topic="dev.hh.responses.v1",
        topic_ref="hh.responses.v1",
        bucket="dev-hh-archive",
        claims_bucket="dev-hh-archive",
        tmp_dir=str(tmp_path),
    )


def _msg(p, o, ts_ms, payload=None, **kw):
    payload = payload if payload is not None else {"domain": f"d{o}.nl", "status": 200}
    return Msg(p, o, json.dumps(_env(payload, event_id=f"e{p}-{o}", **kw)).encode(), ts_ms)


def _run(tmp_path, batches, store=None, sink=SINK, stop_after=None):
    store = store or FakeStore()
    consumer = FakeConsumer(batches)
    stopping = threading.Event()
    s = Sink(_settings(tmp_path, sink), store, consumer=consumer, stopping=stopping)
    for _ in range(stop_after if stop_after is not None else len(batches)):
        s.step()
    return s, store, consumer


def test_files_per_partition_and_hour(tmp_path):
    hour = 3600_000
    batches = [
        [_msg(0, 0, T0), _msg(1, 0, T0), _msg(0, 1, T0 + 1000)],
        [_msg(0, 2, T0 + hour), _msg(0, 3, T0 - 5000)],  # next hour, then a late one
    ]
    s, store, consumer = _run(tmp_path, batches)
    # Partition 0's first file closed when the next hour began; committed past it.
    assert store.keys() == [file_key("hh.responses.v1", datetime(2026, 11, 17, 10, tzinfo=UTC),
                                     0, 0, 1)]  # fmt: skip
    assert store.keys()[0] == (
        "hh.responses.v1/dt=2026-11-17/hh=10/p000-00000000000000000000-00000000000000000001.parquet"
    )
    assert consumer.commits == [(0, 2)]
    # Stopping uploads what is open: partition 0 (hour 11, with the late message) and 1.
    s.stopping.set()
    s.run()
    keys = store.keys()
    assert (
        "hh.responses.v1/dt=2026-11-17/hh=11/p000-00000000000000000002-00000000000000000003.parquet"
        in keys
    )
    assert (
        "hh.responses.v1/dt=2026-11-17/hh=10/p001-00000000000000000000-00000000000000000000.parquet"
        in keys
    )
    assert sorted(consumer.commits) == [(0, 2), (0, 4), (1, 1)]
    t = store.table(next(k for k in keys if "hh=11/p000" in k))
    assert t.column("_offset").to_pylist() == [2, 3]
    assert t.column("domain").to_pylist() == ["d2.nl", "d3.nl"]
    assert t.schema.field("status").type.bit_width == 32
    assert not list(tmp_path.rglob("*.parquet"))  # temporary files removed


def test_max_age_closes_files(tmp_path, monkeypatch):
    s, store, consumer = _run(tmp_path, [[_msg(0, 0, T0)]])
    assert not store.keys()
    s.open[0].opened -= 301
    s.step()
    assert len(store.keys()) == 1 and consumer.commits == [(0, 1)]


def test_file_size_cut(tmp_path):
    import os

    msgs = [
        _msg(0, i, T0, {"domain": "a", "body_b64": base64.b64encode(os.urandom(400_000)).decode()})
        for i in range(8)
    ]
    s, store, consumer = _run(tmp_path, [msgs])
    assert len(store.keys()) >= 2  # 1 MB files, ~3.2 MB of random bytes
    assert consumer.commits[0][1] < 8


def test_rerun_replaces_uncommitted_file(tmp_path):
    store = FakeStore()
    # First run: file 0..2 uploaded, then a crash before the commit (simulated by
    # writing it and not committing).
    s, store, consumer = _run(tmp_path, [[_msg(0, 0, T0), _msg(0, 1, T0), _msg(0, 2, T0)]],
                              store=store)  # fmt: skip
    f = s.open.pop(0)
    path = f.close()
    store.put_file("dev-hh-archive", file_key("hh.responses.v1", f.hour, 0, 0, 2), str(path), "")
    # Second run replays from offset 0 but cuts after offset 1 (another max_age moment).
    s2, store, consumer2 = _run(tmp_path, [[_msg(0, 0, T0), _msg(0, 1, T0)]], store=store)
    s2.open[0].opened -= 301
    s2.step()
    keys = store.keys()
    assert keys == [file_key("hh.responses.v1", f.hour, 0, 0, 1)]
    assert consumer2.commits == [(0, 2)]


def test_revoke_drops_open_files(tmp_path):
    from confluent_kafka import TopicPartition

    s, store, consumer = _run(tmp_path, [[_msg(0, 0, T0), _msg(1, 0, T0)]])
    s._on_revoke(consumer, [TopicPartition("dev.hh.responses.v1", 0)])
    assert set(s.open) == {1}
    assert not consumer.commits


def test_invalid_messages_are_archived_raw(tmp_path):
    s, store, consumer = _run(tmp_path, [[Msg(0, 0, b"not json", T0), _msg(0, 1, T0)]])
    s.stopping.set()
    s.run()
    t = store.table(store.keys()[0])
    assert t.column("payload").to_pylist()[0] == '"not json"'
    assert t.column("event_id").to_pylist() == [None, "e0-1"]


def _claim(store, payload: dict, event_id="big") -> dict:
    return claims.store(
        store, "dev", naming.BucketRef("hh", "archive"), naming.TopicRef("hh", "responses", 1),
        event_id, json.dumps(payload).encode(),
    )  # fmt: skip


def test_claims_are_resolved_into_columns(tmp_path):
    store = FakeStore()
    ref = _claim(store, {"domain": "big.nl", "status": 200, "body": "x" * 1000})
    assert ref["key"].startswith("claims/hh.responses.v1/dt=")
    data = store.objects[("dev-hh-archive", ref["key"])]
    assert ref["sha256"] == hashlib.sha256(data).hexdigest()
    assert json.loads(gzip.decompress(data))["domain"] == "big.nl"

    missing = dict(ref, key="claims/hh.responses.v1/dt=2026-01-01/gone.json.gz")
    batches = [[
        Msg(0, 0, json.dumps(_env(None, payload_ref=ref)).encode(), T0),
        Msg(0, 1, json.dumps(_env(None, payload_ref=missing)).encode(), T0),
    ]]  # fmt: skip
    s, store, _ = _run(tmp_path, batches, store=store)
    s.stopping.set()
    s.run()
    t = store.table(store.keys()[0])
    assert t.column("domain").to_pylist() == ["big.nl", None]
    assert t.column("body_raw").to_pylist()[0] == b"x" * 1000
    assert json.loads(t.column("payload_ref").to_pylist()[0])["key"] == ref["key"]
    assert t.column("payload_ref").to_pylist()[1] is not None


def test_resolve_errors():
    store = FakeStore()
    ref = _claim(store, {"a": 1})
    assert claims.resolve(store, "dev", ref) == {"a": 1}
    with pytest.raises(claims.ClaimError, match="doesn't match"):
        claims.resolve(store, "dev", dict(ref, size=1))
    store.denied.add("dev-hh-archive")
    with pytest.raises(claims.ClaimNotReadable, match="add 'hh.archive' to its reads_buckets"):
        claims.resolve(store, "dev", ref, "other.reader")
    with pytest.raises(claims.ClaimNotReadable, match="reads_buckets"):
        claims.resolve(None, "dev", ref)
    with pytest.raises(claims.ClaimError, match="invalid payload_ref"):
        claims.resolve(store, "dev", {"key": "x"})


def test_settings_from_env():
    from test_storage import _spec

    from leasehold.nomad import Platform, plan
    from leasehold.spec import ServiceSpec

    p = plan(
        [ServiceSpec.model_validate(_spec())], "dev", "abc1234",
        Platform("r", "http://a", "k:9092", s3_endpoint="http://s3", archive_image="img:1"),
    )  # fmt: skip
    job = next(j for j in p.jobs if j.job_id == "hh-archive-responses")
    env = dict(job.job["TaskGroups"][0]["Tasks"][0]["Env"])
    s = Settings.from_env(env)
    assert s.topic == "dev.hh.responses.v1" and s.bucket == "dev-hh-archive"
    assert s.group == "dev.hh.archive-responses"
    assert s.sink.column_specs()["body_raw"].field == "body_b64"
    assert s.sink.max_age == timedelta(hours=1)
