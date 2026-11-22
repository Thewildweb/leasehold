"""The storage step against a real MinIO and Nomad (the dev stack).

    LEASEHOLD_TEST_S3=http://localhost:19000 LEASEHOLD_TEST_S3_SECRET=dev-admin-secret \\
    LEASEHOLD_TEST_NOMAD=http://localhost:14646 uv run pytest tests/integration/test_storage_live.py
"""

from __future__ import annotations

import io
import os
import uuid

import pytest
from minio import Minio
from minio.commonconfig import ENABLED, Filter
from minio.error import S3Error
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

from leasehold import storage
from leasehold.nomad.apply import Nomad
from leasehold.spec import ServiceSpec
from leasehold.storage import apply as sa

S3 = os.environ.get("LEASEHOLD_TEST_S3")
SECRET = os.environ.get("LEASEHOLD_TEST_S3_SECRET", "dev-admin-secret")
NOMAD = os.environ.get("LEASEHOLD_TEST_NOMAD")

pytestmark = pytest.mark.skipif(
    not (S3 and NOMAD), reason="LEASEHOLD_TEST_S3 / LEASEHOLD_TEST_NOMAD not set"
)
ENV = "dev"


def _spec(name: str, *, with_reader: bool = True, retention: str | None = "7d") -> ServiceSpec:
    components = {
        "worker": {
            "kind": "task-worker",
            "consumes_tasks": [f"{name}.fetch"],
            "produces": ["responses"],
            "writes_buckets": ["raw"],
            "node_class": "processing",
            "resources": {"cpu": 100, "memory": 64},
            "count": {"min": 1, "max": 1},
        }
    }
    if with_reader:
        components["reader"] = {
            "kind": "processor",
            "consumes": [f"{name}.responses.v1"],
            "reads_buckets": ["raw"],
            "node_class": "processing",
            "resources": {"cpu": 100, "memory": 64},
        }
    raw = {"retention": retention} if retention else {}
    return ServiceSpec.model_validate(
        {
            "spec_version": 1,
            "name": name,
            "description": "x",
            "owner": "t",
            "components": components,
            "kafka": {
                "produces": {
                    "responses": {
                        "payload_model": "m:R",
                        "partitions": 1,
                        "retention": "1d",
                        "large_payloads": "raw",
                    }
                }
            },
            "frontier": {"task_types": {f"{name}.fetch": {"key": "domain"}}},
            "buckets": {"raw": raw},
        }
    )


@pytest.fixture
def env():
    name = f"t{uuid.uuid4().hex[:8]}"
    st = sa.Storage.connect(S3, "admin", SECRET)
    nomad = Nomad(NOMAD)
    yield name, st, nomad
    # Clean up everything the test created (the dev stack is shared).
    for b in st.s3.list_buckets():
        if b.name.startswith(f"{ENV}-{name}-"):
            for o in st.s3.list_objects(b.name, recursive=True):
                st.s3.remove_object(b.name, o.object_name)
            st.s3.remove_bucket(b.name)
    import json

    for u in json.loads(st.admin.user_list() or "{}"):
        if u.startswith(f"{ENV}.{name}."):
            st.admin.user_remove(u)
    for p in json.loads(st.admin.policy_list() or "{}"):
        if p.startswith(f"leasehold.{ENV}.{name}."):
            st.admin.policy_remove(p)
    for path in nomad.var_list(f"nomad/jobs/{name}-", ENV):
        nomad.var_delete(path, ENV)
    nomad.close()


def _run(st, nomad, specs, names, **kw):
    p = storage.plan(specs, ENV)
    d = sa.diff(st, nomad, p, names, rotate=kw.pop("rotate", False))
    sa.apply(st, nomad, d, **kw)
    return d


def _keys(nomad, job):
    items = nomad.var_get(f"nomad/jobs/{job}", ENV)["Items"]
    return items["LEASEHOLD_S3_ACCESS_KEY"], items["LEASEHOLD_S3_SECRET_KEY"]


def test_apply_grants_and_is_idempotent(env):
    name, st, nomad = env
    spec = _spec(name)
    # A Kafka credential already in the worker's variable must be kept.
    nomad.var_put(f"nomad/jobs/{name}-worker", ENV, {"LEASEHOLD_KAFKA_PASSWORD": "k"}, 0)

    d = _run(st, nomad, [spec], [name])
    assert [c.create for c in d.buckets] == [True]
    assert {c.reason for c in d.users} == {"new"}

    bucket = f"{ENV}-{name}-raw"
    rules = {r.rule_id: r for r in st.s3.get_bucket_lifecycle(bucket).rules}
    assert rules["leasehold-retention"].expiration.days == 7
    claims = rules[f"leasehold-claims-{name}.responses.v1"]
    assert claims.rule_filter.prefix == f"claims/{name}.responses.v1/"
    assert claims.expiration.days == 2

    items = nomad.var_get(f"nomad/jobs/{name}-worker", ENV)["Items"]
    assert items["LEASEHOLD_KAFKA_PASSWORD"] == "k"

    w = Minio(S3.split("://")[1], *_keys(nomad, f"{name}-worker"), secure=False)
    w.put_object(bucket, "a/b", io.BytesIO(b"x"), 1)
    r = Minio(S3.split("://")[1], *_keys(nomad, f"{name}-reader"), secure=False)
    assert r.get_object(bucket, "a/b").read() == b"x"
    with pytest.raises(S3Error):
        r.put_object(bucket, "a/c", io.BytesIO(b"x"), 1)

    again = sa.diff(st, nomad, storage.plan([spec], ENV), [name])
    assert again.changes == 0 and not again.stale_buckets and not again.stale_users


def test_lifecycle_change_keeps_foreign_rules(env):
    name, st, nomad = env
    _run(st, nomad, [_spec(name)], [name])
    bucket = f"{ENV}-{name}-raw"
    config = st.s3.get_bucket_lifecycle(bucket)
    config.rules.append(
        Rule(
            ENABLED,
            rule_filter=Filter(prefix="tmp/"),
            rule_id="by-hand",
            expiration=Expiration(days=1),
        )  # fmt: skip
    )
    st.s3.set_bucket_lifecycle(bucket, LifecycleConfig(config.rules))
    # Unchanged: our rules are compared, the hand-made one is left alone.
    assert sa.diff(st, nomad, storage.plan([_spec(name)], ENV), [name]).changes == 0

    d = _run(st, nomad, [_spec(name, retention=None)], [name])
    assert len(d.buckets) == 1
    ids = {r.rule_id for r in st.s3.get_bucket_lifecycle(bucket).rules}
    assert ids == {"by-hand", f"leasehold-claims-{name}.responses.v1"}


def test_stale_and_prune(env):
    name, st, nomad = env
    _run(st, nomad, [_spec(name)], [name])
    st.s3.make_bucket(f"{ENV}-{name}-old")
    st.s3.put_object(f"{ENV}-{name}-old", "x", io.BytesIO(b"12345"), 5)
    nomad.var_put(
        f"nomad/jobs/{name}-reader",
        ENV,
        nomad.var_get(f"nomad/jobs/{name}-reader", ENV)["Items"] | {"OTHER": "kept"},
        nomad.var_get(f"nomad/jobs/{name}-reader", ENV)["ModifyIndex"],
    )

    spec = _spec(name, with_reader=False)
    d = sa.diff(st, nomad, storage.plan([spec], ENV), [name])
    assert [b.name for b in d.stale_buckets] == [f"{ENV}-{name}-old"]
    assert d.stale_buckets[0].objects == 1 and d.stale_buckets[0].size == 5
    assert d.stale_users == [f"{ENV}.{name}.reader"]
    assert d.stale_policies == [f"leasehold.{ENV}.{name}.reader"]
    assert d.stale_variables == [(ENV, f"nomad/jobs/{name}-reader")]

    sa.apply(st, nomad, d, prune=False)
    assert sa.diff(st, nomad, storage.plan([spec], ENV), [name]).stale_users  # still there

    sa.apply(st, nomad, d, prune=True)
    after = sa.diff(st, nomad, storage.plan([spec], ENV), [name])
    assert not after.stale_users and not after.stale_policies and not after.stale_variables
    assert [b.name for b in after.stale_buckets] == [f"{ENV}-{name}-old"]  # never deleted
    assert nomad.var_get(f"nomad/jobs/{name}-reader", ENV)["Items"] == {"OTHER": "kept"}


def test_rotate_and_missing_user(env):
    name, st, nomad = env
    spec = _spec(name)
    _run(st, nomad, [spec], [name])
    before = _keys(nomad, f"{name}-worker")
    d = _run(st, nomad, [spec], [name], rotate=True)
    assert {c.reason for c in d.users} == {"rotate"}
    assert _keys(nomad, f"{name}-worker")[1] != before[1]

    st.admin.user_remove(f"{ENV}.{name}.worker")
    d = _run(st, nomad, [spec], [name])
    assert [(c.user.component, c.reason, c.attach) for c in d.users] == [
        ("worker", "user missing", True)
    ]
    w = Minio(S3.split("://")[1], *_keys(nomad, f"{name}-worker"), secure=False)
    w.put_object(f"{ENV}-{name}-raw", "z", io.BytesIO(b"x"), 1)
