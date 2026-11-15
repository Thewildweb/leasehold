"""Planner users against a real ClickHouse and Nomad (the dev stack)."""

from __future__ import annotations

import os
import uuid

import httpx
import pytest

from leasehold.clickhouse import users
from leasehold.clickhouse.apply import ClickHouse
from leasehold.nomad.apply import Nomad
from leasehold.spec import ServiceSpec

CH = os.environ.get("LEASEHOLD_TEST_CLICKHOUSE")  # e.g. http://localhost:18123
NOMAD = os.environ.get("LEASEHOLD_TEST_NOMAD")  # e.g. http://localhost:14646

pytestmark = pytest.mark.skipif(
    not (CH and NOMAD), reason="LEASEHOLD_TEST_CLICKHOUSE / LEASEHOLD_TEST_NOMAD not set"
)


def _spec(name: str, reads: list[str]) -> ServiceSpec:
    return ServiceSpec.model_validate(
        {
            "spec_version": 1,
            "name": name,
            "description": "x",
            "owner": "t",
            "components": {
                "plan-new": {
                    "kind": "scheduled",
                    "schedule": "@hourly",
                    "node_class": "services",
                    "resources": {"cpu": 100, "memory": 64},
                    "planner": {
                        "task_type": f"{name}.item",
                        "reads": reads,
                        "backlog_target": 10,
                        "priority": 50,
                    },
                },
                "worker": {
                    "kind": "task-worker",
                    "consumes_tasks": [f"{name}.item"],
                    "node_class": "processing",
                    "resources": {"cpu": 100, "memory": 64},
                    "count": {"min": 1, "max": 1},
                },
            },
            "frontier": {"task_types": {f"{name}.item": {"key": "domain"}}},
            "clickhouse": {"database": name.replace("-", "_")},
        }
    )


def _source(name: str) -> ServiceSpec:
    return ServiceSpec.model_validate(
        {
            "spec_version": 1,
            "name": name,
            "description": "x",
            "owner": "t",
            "components": {
                "worker": {
                    "kind": "task-worker",
                    "consumes_tasks": [f"{name}.item"],
                    "produces": ["out"],
                    "node_class": "processing",
                    "resources": {"cpu": 100, "memory": 64},
                    "count": {"min": 1, "max": 1},
                }
            },
            "kafka": {
                "produces": {"out": {"payload_model": "m:M", "partitions": 1, "retention": "1h"}}
            },
            "frontier": {"task_types": {f"{name}.item": {"key": "domain"}}},
            "clickhouse": {
                "database": name.replace("-", "_"),
                "sinks": {
                    "shared_t": {"topic": "out", "schema_file": "x.sql"},
                    "private_t": {"topic": "out", "schema_file": "x.sql"},
                },
                "shared": {"shared_t": {"description": "x"}},
            },
        }
    )


def test_create_grant_revoke_and_query():
    env = "dev"  # a namespace the dev stack's Nomad has
    tag = uuid.uuid4().hex[:8]
    src, svc = "src" + tag, "svc" + tag
    ch = ClickHouse(CH, "admin", "dev-admin")
    nomad = Nomad(NOMAD)
    srcdb, svcdb = f"{env}_{src}", f"{env}_{svc}"
    try:
        for db in (srcdb, svcdb):
            ch.execute(f"CREATE DATABASE {db}")
        for t in ("shared_t", "private_t"):
            ch.execute(
                f"CREATE TABLE {srcdb}.{t} (domain String) ENGINE = MergeTree ORDER BY domain"
            )
            ch.execute(f"INSERT INTO {srcdb}.{t} VALUES ('a.nl')")

        specs = [_source(src), _spec(svc, [f"{src}.shared_t"])]
        planned = users.plan(specs, env)
        (u,) = planned
        assert u.user == f"{env}.{svc}.plan-new"
        assert u.grants == {(svcdb, "*"), (srcdb, "shared_t")}

        d = users.diff(ch, nomad, planned, env, [src, svc])
        (c,) = d.changes
        assert c.create and c.new_password and len(c.grant) == 2
        users.apply(ch, nomad, d)
        assert users.diff(ch, nomad, planned, env, [src, svc]).empty

        var = nomad.var_get(u.variable, u.namespace)
        pw = var["Items"][users.PASSWORD_KEY]
        q = httpx.Client(base_url=CH, auth=(u.user, pw))
        assert q.post("/", content=f"SELECT count() FROM {srcdb}.shared_t").text.strip() == "1"
        assert q.post("/", content=f"SELECT count() FROM {srcdb}.private_t").status_code != 200
        assert q.post("/", content=f"INSERT INTO {srcdb}.shared_t VALUES ('b')").status_code != 200
        # CONST limits can't be raised.
        r = q.post("/?max_threads=64", content="SELECT 1")
        assert r.status_code != 200

        # reads shrinks: the grant goes.
        planned2 = users.plan([_source(src), _spec(svc, [])], env)
        d2 = users.diff(ch, nomad, planned2, env, [src, svc])
        assert d2.changes[0].revoke == [(srcdb, "shared_t")]
        users.apply(ch, nomad, d2)
        assert q.post("/", content=f"SELECT count() FROM {srcdb}.shared_t").status_code != 200

        # The planner goes away: its user is stale, dropped with prune.
        d3 = users.diff(ch, nomad, [], env, [src, svc])
        assert d3.stale == [u.user]
        users.apply(ch, nomad, d3, prune=True)
        assert not ch.rows(f"SELECT 1 FROM system.users WHERE name = '{u.user}'")
    finally:
        for db in (srcdb, svcdb):
            ch.execute(f"DROP DATABASE IF EXISTS {db}")
        ch.execute(f"DROP USER IF EXISTS `{env}.{svc}.plan-new`")
        try:
            nomad.var_delete(f"nomad/jobs/{svc}-plan-new", env)
        except Exception:  # noqa: BLE001
            pass
