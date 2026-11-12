"""Against a running broker: ACLs from the plan are enforced, and drift is corrected.

Runs only when LEASEHOLD_TEST_KAFKA is set, e.g. against the dev stack:

    docker compose -f dev/docker-compose.yml up -d
    LEASEHOLD_TEST_KAFKA=localhost:19092 uv run pytest tests/integration

Each run uses a fresh random environment name and removes its topics, ACLs and
users afterwards.
"""

from __future__ import annotations

import os
import secrets
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import (
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    ResourcePatternType,
    ResourceType,
    ScramCredentialInfo,
    ScramMechanism,
    UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
)

from leasehold import kafka, naming
from leasehold.kafka.apply import admin_client, apply, diff
from leasehold.spec import find_specs, load_spec

BOOTSTRAP = os.environ.get("LEASEHOLD_TEST_KAFKA")
ADMIN_USER = os.environ.get("LEASEHOLD_TEST_KAFKA_USER", "admin")
ADMIN_PASSWORD = os.environ.get("LEASEHOLD_TEST_KAFKA_PASSWORD", "dev-admin")
FIXTURES = Path(__file__).parent.parent / "fixtures"

pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="LEASEHOLD_TEST_KAFKA not set")


def _client_conf(user: str, password: str) -> dict:
    return {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": user,
        "sasl.password": password,
    }


@pytest.fixture(scope="module")
def env():
    """A unique environment with the fixture services applied and SCRAM users created."""
    name = "it" + secrets.token_hex(4)
    specs = [load_spec(p) for p in find_specs([FIXTURES])]
    plan = kafka.plan(specs, name)
    admin = admin_client(BOOTSTRAP, ADMIN_USER, ADMIN_PASSWORD)
    apply(admin, diff(admin, plan, [s.name for s in specs]))

    passwords = {user: secrets.token_urlsafe(16) for user in plan.principals}
    info = ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192)
    upserts = [UserScramCredentialUpsertion(u, info, p.encode()) for u, p in passwords.items()]
    for fut in admin.alter_user_scram_credentials(upserts).values():
        fut.result()

    yield {"name": name, "specs": specs, "plan": plan, "admin": admin, "passwords": passwords}

    # Teardown: everything this environment created.
    for fut in admin.delete_topics([t.name for t in plan.topics]).values():
        fut.result()
    acl_filters = [
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
    for fut in admin.delete_acls(acl_filters).values():
        fut.result()
    deletions = [UserScramCredentialDeletion(u, ScramMechanism.SCRAM_SHA_512) for u in passwords]
    for fut in admin.alter_user_scram_credentials(deletions).values():
        fut.result()


def _produce(env, user: str, topic: str) -> KafkaError | None:
    """Produce one message as ``user``; return the delivery error, if any."""
    p = Producer(
        {
            **_client_conf(user, env["passwords"][user]),
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 10000,
        }
    )
    errors: list[KafkaError | None] = []
    p.produce(topic, b"payload", key=b"k", on_delivery=lambda err, msg: errors.append(err))
    p.flush(15)
    return errors[0] if errors else KafkaError(KafkaError._MSG_TIMED_OUT)


def _converged_diff(env, timeout_s: float = 15.0):
    """Diff until it's empty or the timeout passes.

    ACLs propagate through the KRaft metadata log, so describe_acls right after
    create_acls can still return the previous state.
    """
    services = [s.name for s in env["specs"]]
    deadline = time.monotonic() + timeout_s
    while True:
        d = diff(env["admin"], env["plan"], services)
        if d.empty or time.monotonic() > deadline:
            return d
        time.sleep(0.5)


def test_second_diff_is_empty(env):
    d = _converged_diff(env)
    assert d.empty, [str(c) for c in d.changes]
    assert not any("SCRAM" in w for w in d.warnings)


def test_worker_may_write_its_own_topic(env):
    user = naming.kafka_principal(env["name"], "certwatch", "worker")
    topic = naming.topic(env["name"], "certwatch", "entries", 1)
    assert _produce(env, user, topic) is None


def test_worker_may_not_write_another_services_topic(env):
    user = naming.kafka_principal(env["name"], "certwatch", "worker")
    topic = naming.topic(env["name"], "resolver", "results", 1)
    err = _produce(env, user, topic)
    assert err is not None and err.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED


def test_bridge_processor_reads_the_topic_it_declares(env):
    name = env["name"]
    writer = naming.kafka_principal(name, "certwatch", "worker")
    topic = naming.topic(name, "certwatch", "entries", 1)
    marker = uuid.uuid4().bytes
    p = Producer(_client_conf(writer, env["passwords"][writer]))
    p.produce(topic, marker, key=b"m")
    p.flush(15)

    reader = naming.kafka_principal(name, "resolver", "new-domains")
    c = Consumer(
        {
            **_client_conf(reader, env["passwords"][reader]),
            "group.id": naming.consumer_group(name, "resolver", "new-domains"),
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([topic])
    try:
        seen = False
        for _ in range(30):
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            if msg.value() == marker:
                seen = True
                break
        assert seen
    finally:
        c.close()


def test_reader_may_not_use_another_group(env):
    name = env["name"]
    reader = naming.kafka_principal(name, "resolver", "new-domains")
    c = Consumer(
        {
            **_client_conf(reader, env["passwords"][reader]),
            "group.id": f"{name}.someone-else",
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([naming.topic(name, "certwatch", "entries", 1)])
    try:
        error = None
        for _ in range(15):
            msg = c.poll(1.0)
            if msg is not None and msg.error():
                error = msg.error()
                break
        assert error is not None and error.code() == KafkaError.GROUP_AUTHORIZATION_FAILED
    finally:
        c.close()


def test_config_drift_is_detected_and_corrected(env):
    admin = env["admin"]
    services = [s.name for s in env["specs"]]
    topic = naming.topic(env["name"], "certwatch", "entries", 1)
    res = ConfigResource(
        ResourceType.TOPIC,
        topic,
        incremental_configs=[
            ConfigEntry("retention.ms", "1000", incremental_operation=AlterConfigOpType.SET)
        ],
    )
    for fut in admin.incremental_alter_configs([res]).values():
        fut.result()

    d = diff(admin, env["plan"], services)
    assert [str(c) for c in d.changes] == [
        f"set config   {topic}  retention.ms=86400000 (was 1000)"
    ]
    apply(admin, d)
    assert _converged_diff(env).empty
