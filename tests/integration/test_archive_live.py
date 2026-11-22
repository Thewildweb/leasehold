"""The archive sink against the dev stack: Kafka in, Parquet in MinIO out, read back with
DuckDB and ClickHouse's s3() function.

    LEASEHOLD_TEST_KAFKA=localhost:19092 LEASEHOLD_TEST_S3=http://localhost:19000 \\
    LEASEHOLD_TEST_CLICKHOUSE=http://localhost:18123 \\
    uv run pytest tests/integration/test_archive_live.py

ClickHouse reaches MinIO inside the compose network as http://minio:9000
(``LEASEHOLD_TEST_S3_FROM_CLICKHOUSE``). Each run uses a fresh environment name and
removes its topic, consumer group and bucket afterwards.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
import uuid

import httpx
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from leasehold import naming
from leasehold.archive.sink import Settings, Sink
from leasehold.runtime import claims
from leasehold.spec.models import ArchiveSink
from leasehold.storage.client import ObjectStore

BOOTSTRAP = os.environ.get("LEASEHOLD_TEST_KAFKA")
S3 = os.environ.get("LEASEHOLD_TEST_S3")
S3_SECRET = os.environ.get("LEASEHOLD_TEST_S3_SECRET", "dev-admin-secret")
CH = os.environ.get("LEASEHOLD_TEST_CLICKHOUSE")
S3_FROM_CH = os.environ.get("LEASEHOLD_TEST_S3_FROM_CLICKHOUSE", "http://minio:9000")
KAFKA = {
    "bootstrap.servers": BOOTSTRAP or "",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": os.environ.get("LEASEHOLD_TEST_KAFKA_USER", "admin"),
    "sasl.password": os.environ.get("LEASEHOLD_TEST_KAFKA_PASSWORD", "dev-admin"),
}

pytestmark = pytest.mark.skipif(
    not (BOOTSTRAP and S3), reason="LEASEHOLD_TEST_KAFKA / LEASEHOLD_TEST_S3 not set"
)

SINK = ArchiveSink.model_validate(
    {
        "topic": "responses",
        "bucket": "archive",
        "node_class": "x",
        "columns": {
            "domain": "string",
            "status": "int32",
            "body_raw": {"type": "base64", "field": "body_b64", "text_fallback": "body"},
            "hops": "json",
        },
        "max_age": "1m",
    }
)


@pytest.fixture
def world():
    env = f"t{uuid.uuid4().hex[:8]}"
    topic = naming.topic(env, "hh", "responses", 1)
    bucket = naming.bucket(env, "hh", "archive")
    admin = AdminClient(KAFKA)
    for f in admin.create_topics([NewTopic(topic, 2, 1)]).values():
        f.result()
    store = ObjectStore.connect(S3, "admin", S3_SECRET)
    store.s3.make_bucket(bucket)
    yield env, topic, bucket, store
    for f in admin.delete_topics([topic]).values():
        f.result()
    group = naming.consumer_group(env, "hh", "archive-responses")
    for f in admin.delete_consumer_groups([group]).values():
        try:
            f.result()
        except Exception:  # noqa: BLE001 - best effort
            pass
    for o in store.list(bucket, ""):
        store.delete(bucket, o.key)
    store.s3.remove_bucket(bucket)


def _envelope(env, i, payload, ref=None):
    return {
        "event_id": str(uuid.uuid4()),
        "schema_version": 1,
        "service": "hh",
        "component": "worker",
        "env": env,
        "worker": "w",
        "task_type": "hh.fetch",
        "task_key": f"d{i}.nl",
        "target": "192.0.2.1",
        "egress_ip": "198.51.100.1",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "payload": payload,
        "payload_ref": ref,
    }


def _produce(topic, envelopes):
    p = Producer(KAFKA)
    for e in envelopes:
        p.produce(topic, json.dumps(e).encode(), key=e["task_key"].encode())
    assert p.flush(30) == 0


def _sink(env, topic, bucket, store) -> Sink:
    s = Settings(
        env=env, service="hh", component="archive-responses", sink_name="responses", sink=SINK,
        topic=topic, topic_ref="hh.responses.v1", bucket=bucket, claims_bucket=bucket,
        kafka_bootstrap=BOOTSTRAP, kafka_username=KAFKA["sasl.username"],
        kafka_password=KAFKA["sasl.password"],
    )  # fmt: skip
    return Sink(s, store, batch_wait=0.5)


def _run_until(sink: Sink, rows: int, timeout=60.0):
    """Consume until ``rows`` messages are in open files, then stop (which uploads them)."""
    t = threading.Thread(target=sink.run)
    t.start()
    deadline = time.monotonic() + timeout
    while sum(f.count for f in list(sink.open.values())) < rows:
        assert time.monotonic() < deadline, "sink didn't read the messages"
        time.sleep(0.2)
    sink.stopping.set()
    t.join(60)
    assert not t.is_alive()


def _payloads(env, n, start=0):
    out = []
    for i in range(start, start + n):
        body = f"<html>{i}</html>".encode()
        out.append(
            _envelope(
                env,
                i,
                {
                    "domain": f"d{i}.nl",
                    "status": 200,
                    "body": body.decode(),
                    "body_b64": base64.b64encode(body).decode(),
                    "hops": [{"status": 200}],
                    "extra": i,
                },
            )  # fmt: skip
        )
    return out


def _duckdb(bucket: str, sql: str):
    import duckdb

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    except duckdb.Error as e:
        pytest.skip(f"DuckDB httpfs extension not available: {e}")
    host = S3.split("://", 1)[1]
    con.execute(
        f"CREATE SECRET s (TYPE s3, KEY_ID 'admin', SECRET '{S3_SECRET}', ENDPOINT '{host}', "
        "URL_STYLE 'path', USE_SSL false)"
    )
    return con.execute(sql.replace("{files}", f"s3://{bucket}/hh.responses.v1/**/*.parquet"))


def test_topic_to_parquet_and_back(world):
    env, topic, bucket, store = world
    events = _payloads(env, 40)
    # One payload behind a claim check, and one older event without body bytes.
    big = {"domain": "big.nl", "status": 200, "body_b64": base64.b64encode(b"B" * 3000).decode()}
    ref = claims.store(
        store, env, naming.BucketRef("hh", "archive"), naming.TopicRef("hh", "responses", 1),
        "big", json.dumps(big).encode(),
    )  # fmt: skip
    events.append(_envelope(env, 40, None, ref))
    events.append(_envelope(env, 41, {"domain": "old.nl", "status": 301, "body": "oud"}))
    _produce(topic, events)

    _run_until(_sink(env, topic, bucket, store), len(events))
    keys = [o.key for o in store.list(bucket, "hh.responses.v1/")]
    assert keys and all(k.endswith(".parquet") and "/dt=" in k and "/hh=" in k for k in keys)

    rows = _duckdb(
        bucket,
        "SELECT domain, status, body_raw, body_raw_from_text, payload, payload_ref, dt, hh "
        "FROM read_parquet('{files}', hive_partitioning = true) ORDER BY domain",
    ).fetchall()
    assert len(rows) == 42
    by_domain = {r[0]: r for r in rows}
    assert by_domain["d7.nl"][2] == b"<html>7</html>" and by_domain["d7.nl"][3] is False
    assert json.loads(by_domain["d7.nl"][4]) == {"extra": 7}
    assert by_domain["big.nl"][2] == b"B" * 3000 and json.loads(by_domain["big.nl"][5])["key"]
    assert by_domain["old.nl"][2] == b"oud" and by_domain["old.nl"][3] is True

    # A second run continues after the committed offsets: no duplicates.
    _produce(topic, _payloads(env, 10, start=100))
    _run_until(_sink(env, topic, bucket, store), 10)
    n, distinct = _duckdb(
        bucket, "SELECT count(), count(DISTINCT event_id) FROM read_parquet('{files}')"
    ).fetchone()
    assert n == distinct == 52

    if CH:
        url = f"{S3_FROM_CH}/{bucket}/hh.responses.v1/**/*.parquet"
        sql = (
            f"SELECT count(), countIf(status = 200), sum(length(body_raw)) "
            f"FROM s3('{url}', 'admin', '{S3_SECRET}', 'Parquet') FORMAT JSONCompact"
        )
        r = httpx.post(
            CH, content=sql, auth=(os.environ.get("LEASEHOLD_TEST_CLICKHOUSE_USER", "admin"),
                                   os.environ.get("LEASEHOLD_TEST_CLICKHOUSE_PASSWORD",
                                                  "dev-admin")),
            timeout=60,
        )  # fmt: skip
        assert r.status_code == 200, r.text
        count, ok, body_bytes = json.loads(r.text)["data"][0]
        assert int(count) == 52 and int(ok) == 51
        assert (
            int(body_bytes)
            == sum(len(f"<html>{i}</html>") for i in range(40))
            + sum(len(f"<html>{i}</html>") for i in range(100, 110))
            + 3000
            + 3
        )
