"""The control API end to end: HTTP, auth and permissions, against a real Postgres."""

from __future__ import annotations

import json
import uuid

import pytest
from fastapi.testclient import TestClient

from leasehold.api.app import create_app
from leasehold.api.auth import Registry, hash_token, new_token
from leasehold.api.settings import Settings

from .pg import DSN, SPECS, fresh_env, requires_postgres

pytestmark = requires_postgres

ROLES = {
    "admin": {"admin": "erik"},
    "planner": {"service": "jobs", "component": "planner"},
    "worker": {"service": "jobs", "component": "worker"},
    "feeder": {"service": "feeder", "component": "worker"},
}


@pytest.fixture
def api(tmp_path):
    tokens = {role: new_token() for role in ROLES}
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps({"tokens": [{"sha256": hash_token(tokens[r])} | e for r, e in ROLES.items()]})
    )
    with fresh_env() as env:
        settings = Settings(
            env=env,
            dsn=DSN,
            services=(),
            tokens_file=path,
            sweep_interval=3600,
            stats_interval=3600,
        )
        app = create_app(settings, registry=Registry(SPECS))
        with TestClient(app) as client:
            client.tokens = tokens
            yield client


def call(api, role, method, path, body=None):
    headers = {"Authorization": f"Bearer {api.tokens[role]}"} if role else {}
    return api.request(method, path, json=body, headers=headers)


def upsert(api, role, task_type, keys, **extra):
    return call(
        api,
        role,
        "POST",
        "/v1/tasks/upsert",
        {"task_type": task_type, "tasks": [{"key": k} for k in keys], **extra},
    )


def test_health_and_whoami(api):
    assert api.get("/health").json()["status"] == "ok"
    me = call(api, "worker", "GET", "/v1/whoami").json()
    assert me["name"] == "jobs.worker"
    assert me["may_lease"] == ["jobs.once", "jobs.repeat"]
    assert me["may_create"] == []


def test_requires_token(api):
    assert call(api, None, "GET", "/v1/whoami").status_code == 401
    r = api.get("/v1/whoami", headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401


def test_full_cycle(api):
    r = upsert(api, "planner", "jobs.once", ["a", "b", "c"])
    assert r.status_code == 200 and r.json() == {"inserted": 3, "updated": 0}

    r = call(api, "worker", "POST", "/v1/tasks/lease", {"max": 2, "worker_id": "w1"})
    assert r.status_code == 200
    leased = r.json()["tasks"]
    assert len(leased) == 2
    refs = [{k: t[k] for k in ("task_type", "key", "lease_id")} for t in leased]

    r = call(api, "worker", "POST", "/v1/tasks/renew", {"leases": refs})
    assert len(r.json()["renewed"]) == 2 and r.json()["lost"] == []

    ack = [refs[0] | {"event_id": str(uuid.uuid4())}]
    r = call(api, "worker", "POST", "/v1/tasks/ack", {"tasks": ack})
    assert r.json() == {"applied": 1, "unknown": []}

    fail = [refs[1] | {"reason": "timeout", "retry_after_s": 60}]
    r = call(api, "worker", "POST", "/v1/tasks/fail", {"tasks": fail})
    assert r.json()["applied"] == 1

    # Acking the same lease again: unknown.
    r = call(api, "worker", "POST", "/v1/tasks/ack", {"tasks": ack})
    assert r.json()["applied"] == 0 and len(r.json()["unknown"]) == 1

    types = {t["name"]: t for t in call(api, "admin", "GET", "/v1/task-types").json()["task_types"]}
    once = types["jobs.once"]
    assert (once["eligible"], once["leased"], once["dead"]) == (1, 0, 0)

    metrics = api.get("/metrics").text
    assert (
        'leasehold_tasks_total{env="'
        + api.app.state.settings.env
        + '",op="acked",task_type="jobs.once"} 1.0'
        in metrics
    )


def test_ownership_is_enforced(api):
    # Another service's component can't create or lease jobs.* tasks.
    assert upsert(api, "feeder", "jobs.once", ["x"]).status_code == 403
    r = call(api, "feeder", "POST", "/v1/tasks/lease", {"max": 1, "task_types": ["jobs.once"]})
    assert r.status_code == 403
    # A worker doesn't create tasks; its events go to a processor that does.
    assert upsert(api, "worker", "jobs.once", ["x"]).status_code == 403
    # Admin may create any task type (imports).
    assert upsert(api, "admin", "jobs.once", ["x"]).status_code == 200
    # Nobody may touch another worker's lease through ack.
    r = call(api, "worker", "POST", "/v1/tasks/lease", {"max": 1})
    ref = {k: r.json()["tasks"][0][k] for k in ("task_type", "key", "lease_id")}
    r = call(
        api, "feeder", "POST", "/v1/tasks/ack", {"tasks": [ref | {"event_id": str(uuid.uuid4())}]}
    )
    assert r.status_code == 403


def test_unknown_task_type(api):
    r = upsert(api, "admin", "jobs.nope", ["x"])
    assert r.status_code == 404


def test_validation(api):
    r = call(
        api,
        "planner",
        "POST",
        "/v1/tasks/upsert",
        {"task_type": "jobs.once", "tasks": [{"key": "a", "priority": 101}]},
    )
    assert r.status_code == 422
    r = call(
        api,
        "planner",
        "POST",
        "/v1/tasks/upsert",
        {"task_type": "jobs.once", "tasks": [{"key": "a", "bogus": 1}]},
    )
    assert r.status_code == 422


def test_pause_is_admin_only_and_stops_leasing(api):
    upsert(api, "planner", "jobs.once", ["a", "b"])
    leased = call(
        api, "worker", "POST", "/v1/tasks/lease", {"max": 1, "task_types": ["jobs.once"]}
    ).json()["tasks"]
    path = "/v1/task-types/jobs.once/pause"
    assert call(api, "worker", "POST", path, {"reason": "x"}).status_code == 403
    r = call(api, "admin", "POST", path, {"reason": "target complained"})
    assert r.json() == {"paused": "jobs.once", "released": 1}
    r = call(api, "worker", "POST", "/v1/tasks/lease", {"max": 5, "task_types": ["jobs.once"]})
    assert r.json()["tasks"] == []
    ref = {k: leased[0][k] for k in ("task_type", "key", "lease_id")}
    r = call(api, "worker", "POST", "/v1/tasks/renew", {"leases": [ref]})
    assert r.json()["lost"] == [ref]
    call(api, "admin", "POST", "/v1/task-types/jobs.once/resume")
    r = call(api, "worker", "POST", "/v1/tasks/lease", {"max": 5, "task_types": ["jobs.once"]})
    assert len(r.json()["tasks"]) == 2


def test_sweeper_and_stats_loops(api):
    import psycopg
    from psycopg import sql

    from leasehold.frontier import schema

    upsert(api, "planner", "jobs.once", ["a"])
    call(api, "worker", "POST", "/v1/tasks/lease", {"max": 1})
    env = api.app.state.settings.env
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            sql.SQL("UPDATE {}.tasks SET lease_expires_at = now() - interval '1s'").format(
                sql.Identifier(schema.schema_name(env))
            )
        )
    api.portal.call(api.app.state.sweep_once)
    api.portal.call(api.app.state.stats_once)
    metrics = api.get("/metrics").text
    assert "leasehold_leases_expired_total{env=" in metrics
    assert f'leasehold_frontier_eligible_tasks{{env="{env}",task_type="jobs.once"}}' in metrics
