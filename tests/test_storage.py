"""Buckets in service.yml: naming, validation, the storage plan, and what jobs get."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from leasehold import naming, storage
from leasehold.kafka.plan import plan as kafka_plan
from leasehold.nomad import Platform
from leasehold.nomad import plan as nomad_plan
from leasehold.spec import ServiceSpec
from leasehold.spec.registry import check_registry


def _spec(name="hh", **over) -> dict:
    d = {
        "spec_version": 1,
        "name": name,
        "description": "x",
        "owner": "t",
        "components": {
            "worker": {
                "kind": "task-worker",
                "consumes_tasks": [f"{name}.fetch"],
                "produces": ["responses"],
                "writes_buckets": ["archive"],
                "node_class": "processing",
                "resources": {"cpu": 100, "memory": 64},
                "count": {"min": 1, "max": 1},
            },
            "analyzer": {
                "kind": "processor",
                "consumes": [f"{name}.responses.v1"],
                "reads_buckets": ["archive", "platform"],
                "node_class": "processing",
                "resources": {"cpu": 100, "memory": 64},
            },
        },
        "kafka": {
            "produces": {
                "responses": {
                    "payload_model": "m:Response",
                    "partitions": 3,
                    "retention": "36h",
                    "key": "domain",
                    "large_payloads": "archive",
                }
            }
        },
        "frontier": {"task_types": {f"{name}.fetch": {"key": "domain"}}},
        "buckets": {"archive": {}, "platform": {"retention": "30d", "versioning": True}},
        "archives": {
            "responses": {
                "topic": "responses",
                "bucket": "archive",
                "node_class": "processing",
                "columns": {
                    "domain": "string",
                    "status": "int32",
                    "body_raw": {"type": "base64", "field": "body_b64", "text_fallback": "body"},
                },
            }
        },
    }
    for k, v in over.items():
        d[k] = v
    return d


def _errors(d: dict) -> str:
    with pytest.raises(ValidationError) as e:
        ServiceSpec.model_validate(d)
    return str(e.value)


# --- naming ----------------------------------------------------------------------------------


def test_bucket_names():
    assert naming.bucket("prod", "certlog", "raw") == "certlog-raw"
    assert naming.bucket("dev", "certlog", "raw") == "dev-certlog-raw"
    with pytest.raises(naming.NamingError):
        naming.bucket("dev", "certlog", "raw_data")
    with pytest.raises(naming.NamingError):
        naming.bucket("dev", "a" * 40, "b" * 30)


def test_bucket_refs():
    assert naming.BucketRef.parse("raw", "certlog") == naming.BucketRef("certlog", "raw")
    assert naming.BucketRef.parse("dns.raw", "certlog") == naming.BucketRef("dns", "raw")
    assert str(naming.BucketRef("dns", "raw")) == "dns.raw"
    with pytest.raises(naming.NamingError):
        naming.BucketRef.parse("raw")
    assert naming.storage_user("dev", "hh", "worker") == "dev.hh.worker"
    assert naming.storage_policy("prod", "hh", "worker") == "leasehold.hh.worker"
    assert naming.claims_prefix(naming.TopicRef("hh", "responses", 1)) == "claims/hh.responses.v1/"


# --- validation ------------------------------------------------------------------------------


def test_valid_spec():
    s = ServiceSpec.model_validate(_spec())
    assert set(s.all_components()) == {"worker", "analyzer", "archive-responses"}
    a = s.all_components()["archive-responses"]
    assert a.consumes == ["hh.responses.v1"]
    assert a.writes_buckets == ["archive"]
    assert s.large_payload_buckets() == {
        naming.TopicRef("hh", "responses", 1): naming.BucketRef("hh", "archive")
    }
    read, write = s.components["analyzer"].bucket_refs("hh")
    assert read == {naming.BucketRef("hh", "archive"), naming.BucketRef("hh", "platform")}
    assert write == set()


def test_writes_only_own_buckets():
    d = _spec()
    d["components"]["worker"]["writes_buckets"] = ["archive", "nope"]
    assert "writes_buckets: unknown bucket 'nope'" in _errors(d)
    d = _spec()
    d["components"]["worker"]["writes_buckets"] = ["archive", "dns.raw"]
    assert "writes_buckets" in _errors(d)  # pattern: no references


def test_reads_own_bucket_must_exist():
    d = _spec()
    d["components"]["analyzer"]["reads_buckets"] = ["archive", "gone"]
    assert "this service has no bucket 'gone'" in _errors(d)
    d["components"]["analyzer"]["reads_buckets"] = ["archive", "hh.gone"]
    assert "this service has no bucket 'gone'" in _errors(d)


def test_large_payloads_producer_must_write_the_bucket():
    d = _spec()
    d["components"]["worker"]["writes_buckets"] = []
    assert "produces 'responses', whose large payloads go to 'archive'" in _errors(d)
    d = _spec()
    d["kafka"]["produces"]["responses"]["large_payloads"] = "missing"
    assert "large_payloads: unknown bucket 'missing'" in _errors(d)


def test_bucket_retention_whole_days():
    d = _spec()
    d["buckets"]["platform"]["retention"] = "36h"
    assert "whole number of days" in _errors(d)


def test_archive_checks():
    d = _spec()
    d["archives"]["responses"]["bucket"] = "nope"
    assert "archives.responses.bucket: unknown bucket 'nope'" in _errors(d)
    d = _spec()
    d["archives"]["responses"]["topic"] = "nope"
    assert "archives.responses.topic: unknown topic 'nope'" in _errors(d)
    d = _spec()
    d["archives"]["responses"]["topic"] = "hh.responses.v2"
    assert "refers to version 2" in _errors(d)
    d = _spec()
    d["archives"]["responses"]["columns"]["ts"] = "string"
    assert "are the envelope's" in _errors(d)
    d = _spec()
    d["archives"]["responses"]["columns"]["body_raw_from_text"] = "bool"
    assert "used twice" in _errors(d)
    d = _spec()
    d["archives"]["responses"]["columns"]["dom2"] = {"type": "string", "field": "domain"}
    assert "in two columns" in _errors(d)
    d = _spec()
    d["archives"]["responses"]["columns"]["x"] = {"type": "string", "text_fallback": "y"}
    assert "text_fallback is for base64" in _errors(d)


def test_archive_component_names_reserved():
    d = _spec()
    d["components"]["archive-x"] = d["components"].pop("analyzer")
    assert "reserved for archive sinks" in _errors(d)
    d = _spec()
    d["components"]["archive"] = d["components"].pop("analyzer")
    assert "reserved" in _errors(d)


def test_registry_buckets():
    hh = ServiceSpec.model_validate(_spec())
    other = _spec("other")
    other["components"]["analyzer"]["consumes"] = ["hh.responses.v1"]
    other["components"]["analyzer"]["reads_buckets"] = ["hh.platform"]
    del other["components"]["worker"]["produces"]
    del other["kafka"]
    del other["archives"]
    o = ServiceSpec.model_validate(other)
    errors = check_registry([hh, o])
    assert errors == [
        "other: analyzer consumes hh.responses.v1, whose large payloads are in bucket "
        "hh.archive; add 'hh.archive' to its reads_buckets"
    ]
    other["components"]["analyzer"]["reads_buckets"] = ["hh.platform", "hh.archive", "x.y"]
    o = ServiceSpec.model_validate(other)
    assert check_registry([hh, o]) == [
        "other: analyzer reads bucket x.y, which no service declares"
    ]
    assert check_registry([o], partial=True) == []


# --- plan ------------------------------------------------------------------------------------


def test_storage_plan():
    p = storage.plan([ServiceSpec.model_validate(_spec())], "dev")
    by_name = {b.name: b for b in p.buckets}
    archive = by_name["dev-hh-archive"]
    assert [(r.id, r.prefix, r.days) for r in archive.rules] == [
        ("leasehold-claims-hh.responses.v1", "claims/hh.responses.v1/", 3)  # 36h → 2d + 1
    ]
    assert not archive.versioning
    platform = by_name["dev-hh-platform"]
    assert [(r.prefix, r.days) for r in platform.rules] == [("", 30)]
    assert platform.versioning

    users = {u.component: u for u in p.users}
    assert set(users) == {"worker", "analyzer", "archive-responses"}
    w = users["worker"]
    assert (w.user, w.policy, w.variable) == (
        "dev.hh.worker",
        "leasehold.dev.hh.worker",
        "nomad/jobs/hh-worker",
    )
    assert w.write == {"dev-hh-archive"} and w.read == frozenset()
    doc = w.policy_document()
    actions = {a for s in doc["Statement"] for a in s["Action"]}
    assert {"s3:PutObject", "s3:DeleteObject", "s3:GetObject", "s3:ListBucket"} <= actions
    a = users["analyzer"]
    assert a.read == {"dev-hh-archive", "dev-hh-platform"} and not a.write
    actions = {x for s in a.policy_document()["Statement"] for x in s["Action"]}
    assert "s3:PutObject" not in actions
    resources = {r for s in a.policy_document()["Statement"] for r in s["Resource"]}
    assert resources == {
        "arn:aws:s3:::dev-hh-archive",
        "arn:aws:s3:::dev-hh-archive/*",
        "arn:aws:s3:::dev-hh-platform",
        "arn:aws:s3:::dev-hh-platform/*",
    }


def test_archive_sink_reads_claims_of_another_service():
    hh = ServiceSpec.model_validate(_spec())
    other = _spec("other")
    del other["components"]["worker"]["produces"]
    other["components"]["analyzer"]["reads_buckets"] = ["hh.archive"]
    other["components"]["analyzer"]["consumes"] = ["hh.responses.v1"]
    other["kafka"] = {}
    other["archives"]["responses"]["topic"] = "hh.responses.v1"
    o = ServiceSpec.model_validate(other)
    p = storage.plan([hh, o], "prod")
    u = next(u for u in p.users if u.user == "other.archive-responses")
    assert u.write == {"other-archive"} and u.read == {"hh-archive"}


def test_kafka_plan_grants_archive_sink():
    p = kafka_plan([ServiceSpec.model_validate(_spec())], "dev")
    acls = {str(a) for a in p.acls}
    assert "User:dev.hh.archive-responses READ TOPIC:dev.hh.responses.v1" in acls
    assert "User:dev.hh.archive-responses READ GROUP:dev.hh.archive-responses" in acls
    assert not any("archive-responses WRITE" in a for a in acls)


def _platform(**kw) -> Platform:
    return Platform(
        registry="r.example",
        api_url="http://api:8400",
        kafka_bootstrap="kafka:9092",
        s3_endpoint="http://s3:9000",
        archive_image="r.example/leasehold-archive:abc1234",
        **kw,
    )


def test_nomad_jobs_get_storage():
    p = nomad_plan([ServiceSpec.model_validate(_spec())], "dev", "abc1234", _platform())
    jobs = {j.job_id: j.job for j in p.jobs}
    worker = jobs["hh-worker"]["TaskGroups"][0]["Tasks"][0]
    assert worker["Env"]["LEASEHOLD_S3_ENDPOINT"] == "http://s3:9000"
    assert 'nomadVar "nomad/jobs/hh-worker"' in worker["Templates"][0]["EmbeddedTmpl"]

    job = jobs["hh-archive-responses"]
    assert job["Type"] == "service"
    task = job["TaskGroups"][0]["Tasks"][0]
    assert task["Config"]["image"] == "r.example/leasehold-archive:abc1234"
    assert task["Config"]["args"] == ["archive", "run"]
    assert job["Meta"]["leasehold_component"] == "archive-responses"
    # 3 partitions x (128 MB files + 16 MB row group) + margin
    assert job["TaskGroups"][0]["EphemeralDisk"] == {"SizeMB": 3 * 144 + 256}
    assert "EphemeralDisk" not in jobs["hh-worker"]["TaskGroups"][0]
    settings = json.loads(task["Env"]["LEASEHOLD_ARCHIVE"])
    assert settings["topic"] == "dev.hh.responses.v1"
    assert settings["bucket"] == "dev-hh-archive"
    assert settings["claims_bucket"] == "dev-hh-archive"
    assert settings["spec"]["columns"]["status"] == "int32"
    assert "LEASEHOLD_KAFKA_BOOTSTRAP" in task["Env"]
    assert 'nomadVar "nomad/jobs/hh-archive-responses"' in task["Templates"][0]["EmbeddedTmpl"]


def test_nomad_needs_storage_settings():
    spec = ServiceSpec.model_validate(_spec())
    with pytest.raises(ValueError, match="S3 endpoint"):
        nomad_plan([spec], "dev", "abc1234", Platform("r", "http://a", "k:9092"))
    with pytest.raises(ValueError, match="archive image"):
        nomad_plan(
            [spec], "dev", "abc1234", Platform("r", "http://a", "k:9092", s3_endpoint="http://s")
        )
