"""The runtime end to end: control API, Postgres frontier, Kafka with ACLs.

A scheduled planner creates tasks through the control API, a task-worker leases
them and produces one event each to Kafka as its own SCRAM user, and a
processor consumes those events in its consumer group and creates tasks again
(which changes nothing: the upsert is idempotent).

Needs LEASEHOLD_TEST_KAFKA and LEASEHOLD_TEST_POSTGRES (see the other
integration tests). Uses a random environment and removes it afterwards.
"""

from __future__ import annotations

import json
import os
import secrets
import socket
import threading
import time

import psycopg
import pytest
import uvicorn
from confluent_kafka.admin import (
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    ResourcePatternType,
    ResourceType,
    ScramCredentialInfo,
    ScramMechanism,
    UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
)
from conftest import runtime_spec
from psycopg import sql

from leasehold import kafka, naming
from leasehold.api.app import create_app
from leasehold.api.auth import Registry, hash_token, new_token
from leasehold.api.settings import Settings
from leasehold.client import ControlClient, NewTask
from leasehold.frontier import schema
from leasehold.kafka.apply import admin_client, apply, diff
from leasehold.runtime.config import RuntimeConfig
from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer
from leasehold.runtime.processor import Processor
from leasehold.runtime.scheduled import run_scheduled
from leasehold.runtime.worker import Worker

BOOTSTRAP = os.environ.get("LEASEHOLD_TEST_KAFKA")
DSN = os.environ.get("LEASEHOLD_TEST_POSTGRES")
ADMIN_USER = os.environ.get("LEASEHOLD_TEST_KAFKA_USER", "admin")
ADMIN_PASSWORD = os.environ.get("LEASEHOLD_TEST_KAFKA_PASSWORD", "dev-admin")

pytestmark = pytest.mark.skipif(
    not (BOOTSTRAP and DSN), reason="LEASEHOLD_TEST_KAFKA and LEASEHOLD_TEST_POSTGRES not set"
)

N_TASKS = 20


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def stack(tmp_path):
    env = "it" + secrets.token_hex(4)
    spec = runtime_spec()

    # Kafka: topics, ACLs and a SCRAM user per component.
    plan = kafka.plan([spec], env)
    admin = admin_client(BOOTSTRAP, ADMIN_USER, ADMIN_PASSWORD)
    apply(admin, diff(admin, plan, [spec.name]))
    passwords = {u: secrets.token_urlsafe(16) for u in plan.principals}
    info = ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192)
    for fut in admin.alter_user_scram_credentials(
        [UserScramCredentialUpsertion(u, info, p.encode()) for u, p in passwords.items()]
    ).values():
        fut.result()
    deadline = time.monotonic() + 15
    while not diff(admin, plan, [spec.name]).empty and time.monotonic() < deadline:
        time.sleep(0.5)  # ACLs propagate asynchronously

    # Frontier.
    with psycopg.connect(DSN, autocommit=True) as conn:
        schema.migrate(conn, env)
        schema.apply_task_types(conn, env, schema.diff_task_types(conn, env, [spec]))

    # Control API on a real port, with a token per component.
    tokens = {c: new_token() for c in spec.components}
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(
        json.dumps(
            {
                "tokens": [
                    {"sha256": hash_token(t), "service": spec.name, "component": c}
                    for c, t in tokens.items()
                ]
            }
        )
    )
    settings = Settings(
        env=env,
        dsn=DSN,
        services=(),
        tokens_file=tokens_file,
        sweep_interval=3600,
        stats_interval=3600,
    )
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings, registry=Registry([spec])),
            host="127.0.0.1",
            port=port,
            log_level="warning",
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.05)
    url = f"http://127.0.0.1:{port}"

    def component(name: str, with_producer: bool = True) -> Context:
        cfg = RuntimeConfig(
            env=env,
            service=spec.name,
            component=name,
            spec=spec,
            instance=f"{name}-1",
            kafka_bootstrap=BOOTSTRAP,
            kafka_password=passwords.get(naming.kafka_principal(env, spec.name, name)),
            drain_timeout=5,
        )
        producer = EventProducer(cfg) if with_producer and cfg.component_spec.produces else None
        return Context(cfg, client=ControlClient(url, token=tokens[name]), producer=producer)

    yield {"env": env, "spec": spec, "component": component}

    server.should_exit = True
    thread.join(10)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema.schema_name(env)))
        )
    for fut in admin.delete_topics([t.name for t in plan.topics]).values():
        fut.result()
    for fut in admin.delete_acls(
        [
            AclBindingFilter(
                ResourceType.ANY,
                None,
                ResourcePatternType.ANY,
                f"User:{u}",
                None,
                AclOperation.ANY,
                AclPermissionType.ANY,
            )
            for u in plan.principals
        ]
    ).values():
        fut.result()
    for fut in admin.alter_user_scram_credentials(
        [UserScramCredentialDeletion(u, ScramMechanism.SCRAM_SHA_512) for u in passwords]
    ).values():
        fut.result()


def test_planner_worker_processor(stack):
    env = stack["env"]

    # 1. The planner creates tasks.
    planner = stack["component"]("planner")
    rc = run_scheduled(
        lambda run: run.upsert(
            "jobs.item", [NewTask(f"k{i}", payload={"n": i}) for i in range(N_TASKS)]
        ),
        ctx=planner,
    )
    assert rc == 0

    # 2. The worker leases them, emits one event each, acks.
    handled = []

    def handle(task, ctx):
        handled.append(task.key)
        ctx.emit("results", {"id": task.key, "n": task.payload["n"]})

    wctx = stack["component"]("worker")
    worker = Worker(wctx, handle, concurrency=4)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        worker.step(wait=0.1)
        if len(handled) == N_TASKS and not worker.running and not worker.acks:
            break
    wctx.stopping.set()
    worker.drain()
    wctx.close()
    assert sorted(handled) == sorted(f"k{i}" for i in range(N_TASKS))

    with psycopg.connect(DSN, autocommit=True) as conn:
        rows = conn.execute(
            sql.SQL(
                "SELECT status, count(*), count(last_event_id) FROM {}.tasks GROUP BY status"
            ).format(sql.Identifier(schema.schema_name(env)))
        ).fetchall()
    assert rows == [("done", N_TASKS, N_TASKS)]

    # 3. The processor reads the worker's events in its own group and creates tasks.
    seen, results = [], []

    def route(events, ctx):
        seen.extend(events)
        results.append(ctx.upsert("jobs.item", [NewTask(e.payload["id"]) for e in events]))
        ctx.emit("routed", {"n": len(events)})

    pctx = stack["component"]("router")
    proc = Processor(pctx, route, batch_wait=0.5)
    proc.consumer.subscribe(sorted(proc.refs))
    deadline = time.monotonic() + 60
    while len(seen) < N_TASKS and time.monotonic() < deadline:
        assert proc.step()
    proc.consumer.close()
    pctx.close()

    assert sorted(e.payload["id"] for e in seen) == sorted(f"k{i}" for i in range(N_TASKS))
    assert {e.topic for e in seen} == {naming.topic(env, "jobs", "results", 1)}
    assert all(e.task_type == "jobs.item" and e.component == "worker" for e in seen)
    # The tasks already existed: the processor's upserts inserted nothing.
    assert sum(i for i, _ in results) == 0 and sum(u for _, u in results) == N_TASKS
