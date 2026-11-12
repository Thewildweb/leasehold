"""Credentials: which principals get one, where they're stored, and the diff/apply logic."""

from __future__ import annotations

from pathlib import Path

import pytest

from leasehold import credentials
from leasehold.spec import load_spec

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def spec():
    return load_spec(FIXTURES / "certwatch" / "service.yml")


class _Future:
    def __init__(self, value=None):
        self.value = value

    def result(self):
        return self.value


class FakeAdmin:
    def __init__(self, users=()):
        self.users = set(users)
        self.passwords: dict[str, bytes] = {}

    def describe_user_scram_credentials(self, request_timeout=None):
        return _Future({u: object() for u in self.users})

    def alter_user_scram_credentials(self, alterations, request_timeout=None):
        for a in alterations:
            if hasattr(a, "password"):
                self.users.add(a.user)
                self.passwords[a.user] = a.password
            else:
                self.users.discard(a.user)
        return {a: _Future() for a in alterations}


class FakeNomad:
    def __init__(self, variables=None):
        self.vars = {k: {"Items": dict(v), "ModifyIndex": 1} for k, v in (variables or {}).items()}

    def var_get(self, path, namespace):
        v = self.vars.get((namespace, path))
        return None if v is None else {"Items": dict(v["Items"]), "ModifyIndex": v["ModifyIndex"]}

    def var_list(self, prefix, namespace):
        return [p for ns, p in self.vars if ns == namespace and p.startswith(prefix)]

    def var_put(self, path, namespace, items, modify_index):
        current = self.vars.get((namespace, path))
        assert (current["ModifyIndex"] if current else 0) == modify_index
        self.vars[(namespace, path)] = {"Items": dict(items), "ModifyIndex": modify_index + 1}

    def var_delete(self, path, namespace):
        self.vars.pop((namespace, path))


def test_plan(spec):
    creds = credentials.plan([spec], "dev")
    assert [(c.principal, c.namespace, c.path) for c in creds] == [
        ("dev.certwatch.clickhouse", "dev", "leasehold/certwatch/clickhouse-kafka"),
        ("dev.certwatch.worker", "dev", "nomad/jobs/certwatch-worker"),
    ]
    # The planner doesn't use Kafka, so it gets no Kafka credential.
    assert not any("planner" in c.principal for c in creds)


def test_new_credentials_are_set_in_kafka_and_nomad(spec):
    admin, nomad = FakeAdmin(), FakeNomad()
    creds = credentials.plan([spec], "prod")
    d = credentials.diff(admin, nomad, creds, "prod", ["certwatch"])
    assert [(c.credential.principal, c.reason) for c in d.changes] == [
        ("certwatch.clickhouse", "new"),
        ("certwatch.worker", "new"),
    ]
    credentials.apply(admin, nomad, d)
    items = nomad.var_get("nomad/jobs/certwatch-worker", "default")["Items"]
    assert items["LEASEHOLD_KAFKA_USERNAME"] == "certwatch.worker"
    assert admin.passwords["certwatch.worker"] == items["LEASEHOLD_KAFKA_PASSWORD"].encode()
    sink = nomad.var_get("leasehold/certwatch/clickhouse-kafka", "default")["Items"]
    assert admin.passwords["certwatch.clickhouse"] == sink["password"].encode()
    assert credentials.diff(admin, nomad, creds, "prod", ["certwatch"]).empty


def test_missing_kafka_user_gets_a_new_password_and_other_keys_are_kept(spec):
    nomad = FakeNomad(
        {
            ("default", "nomad/jobs/certwatch-worker"): {
                "LEASEHOLD_KAFKA_USERNAME": "certwatch.worker",
                "LEASEHOLD_KAFKA_PASSWORD": "old",
                "OTHER": "keep",
            }
        }
    )
    admin = FakeAdmin(users={"certwatch.clickhouse"})
    creds = [c for c in credentials.plan([spec], "prod") if c.principal == "certwatch.worker"]
    d = credentials.diff(admin, nomad, creds, "prod", ["certwatch"])
    assert [c.reason for c in d.changes] == ["kafka user missing"]
    credentials.apply(admin, nomad, d)
    items = nomad.var_get("nomad/jobs/certwatch-worker", "default")["Items"]
    assert items["OTHER"] == "keep"
    assert items["LEASEHOLD_KAFKA_PASSWORD"] != "old"


def test_variable_for_another_user_counts_as_missing(spec):
    nomad = FakeNomad(
        {
            ("default", "nomad/jobs/certwatch-worker"): {
                "LEASEHOLD_KAFKA_USERNAME": "someone.else",
                "LEASEHOLD_KAFKA_PASSWORD": "x",
            }
        }
    )
    admin = FakeAdmin(users={"certwatch.worker"})
    creds = [c for c in credentials.plan([spec], "prod") if c.principal == "certwatch.worker"]
    d = credentials.diff(admin, nomad, creds, "prod", ["certwatch"])
    assert [c.reason for c in d.changes] == ["variable missing"]


def test_rotate(spec):
    admin, nomad = FakeAdmin(), FakeNomad()
    creds = credentials.plan([spec], "prod")
    credentials.apply(admin, nomad, credentials.diff(admin, nomad, creds, "prod", ["certwatch"]))
    before = dict(admin.passwords)
    d = credentials.diff(admin, nomad, creds, "prod", ["certwatch"], rotate=True)
    assert {c.reason for c in d.changes} == {"rotate"}
    credentials.apply(admin, nomad, d)
    assert all(admin.passwords[u] != before[u] for u in before)


def test_stale_users_and_variables(spec):
    nomad = FakeNomad(
        {
            ("default", "nomad/jobs/certwatch-old"): {
                "LEASEHOLD_KAFKA_USERNAME": "certwatch.old",
                "LEASEHOLD_KAFKA_PASSWORD": "x",
            },
            ("default", "nomad/jobs/certwatch-mixed"): {
                "LEASEHOLD_KAFKA_PASSWORD": "x",
                "OTHER": "keep",
            },
            ("default", "nomad/jobs/certwatch-plain"): {"OTHER": "keep"},
        }
    )
    admin = FakeAdmin(users={"certwatch.old", "other.worker", "dev.certwatch.old"})
    creds = credentials.plan([spec], "prod")
    d = credentials.diff(admin, nomad, creds, "prod", ["certwatch"])
    assert d.stale_users == ["certwatch.old"]  # not other services, not other environments
    assert sorted(p for _, p in d.stale_variables) == [
        "nomad/jobs/certwatch-mixed",
        "nomad/jobs/certwatch-old",
    ]
    credentials.apply(admin, nomad, d)
    assert "certwatch.old" in admin.users  # kept without prune
    d = credentials.diff(admin, nomad, creds, "prod", ["certwatch"])
    assert d.empty and d.stale_users == ["certwatch.old"]
    credentials.apply(admin, nomad, d, prune=True)
    assert "certwatch.old" not in admin.users
    assert nomad.var_get("nomad/jobs/certwatch-old", "default") is None
    assert nomad.var_get("nomad/jobs/certwatch-mixed", "default")["Items"] == {"OTHER": "keep"}
    assert nomad.var_get("nomad/jobs/certwatch-plain", "default")["Items"] == {"OTHER": "keep"}


def test_passwords_are_random_and_long():
    a, b = credentials.new_password(), credentials.new_password()
    assert a != b and len(a) >= 40
