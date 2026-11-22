"""The egress schema against a real Postgres: migrate, apply, the API's provider rules,
and the provider column on frontier tasks."""

from __future__ import annotations

import asyncio
import json
import secrets

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import AsyncConnection, sql
from test_egress import write

from leasehold.api.app import create_app
from leasehold.api.auth import Registry, hash_token, new_token
from leasehold.api.settings import Settings
from leasehold.egress import config, schema
from leasehold.frontier.store import Frontier, UpsertItem

from .pg import DSN, SPECS, fresh_env, requires_postgres

pytestmark = requires_postgres


@pytest.fixture
def egress_schema():
    name = "it" + secrets.token_hex(4) + "_egress"
    with psycopg.connect(DSN, autocommit=True) as conn:
        assert schema.migrate(conn, name) == [v for v, _ in schema.MIGRATIONS]
        assert schema.migrate(conn, name) == []
        try:
            yield name
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))


def test_apply(tmp_path, egress_schema):
    cfg = config.load(write(tmp_path))
    with psycopg.connect(DSN, autocommit=True) as conn:
        d = schema.diff(conn, cfg, egress_schema)
        lines = d.lines()
        assert "add      egress 192.0.2.10" in lines
        assert "add      provider example-shop  platform, 20/min per egress" in lines
        assert "add      provider example-parking  parking, no budget" in lines
        assert "rules    range rules: +4 -0" in lines
        schema.apply(conn, d, egress_schema)
        assert schema.diff(conn, cfg, egress_schema).empty

        rows = conn.execute(
            sql.SQL("SELECT address, subnet, asn, status FROM {}.egresses ORDER BY address").format(
                sql.Identifier(egress_schema)
            )
        ).fetchall()
        assert [(str(a), str(s), n, st) for a, s, n, st in rows] == [
            ("192.0.2.10", "192.0.2.0/24", 64500, "active"),
            ("192.0.2.11", "192.0.2.0/24", 64500, "drain"),
            ("198.51.100.7", "198.51.100.0/25", 64501, "active"),
        ]

        # An egress left out of the registry is disabled, not deleted; a provider left
        # out is removed with its rules.
        registry = "egresses: [{address: 192.0.2.10, asn: 64500, country: NL}]"
        providers = "providers:\n  example-cdn:\n    kind: cdn\n    cname: [cdn.example.net]\n"
        budgets = "providers: { default: 30 }"
        cfg2 = config.load(write(tmp_path, registry, providers, budgets))
        d2 = schema.diff(conn, cfg2, egress_schema)
        assert "disable  egress 192.0.2.11  not in the registry" in d2.lines()
        assert "remove   provider example-shop" in d2.lines()
        schema.apply(conn, d2, egress_schema)
        assert schema.diff(conn, cfg2, egress_schema).empty
        q = sql.SQL("SELECT count(*) FROM {}.{}")
        s = sql.Identifier(egress_schema)
        assert conn.execute(q.format(s, sql.Identifier("egresses"))).fetchone()[0] == 3
        assert conn.execute(q.format(s, sql.Identifier("provider_ranges"))).fetchone()[0] == 0
        assert conn.execute(q.format(s, sql.Identifier("providers"))).fetchone()[0] == 1


def test_not_migrated(tmp_path):
    cfg = config.load(write(tmp_path))
    with psycopg.connect(DSN, autocommit=True) as conn:
        with pytest.raises(RuntimeError, match="not migrated"):
            schema.diff(conn, cfg, "it_missing_egress")


@pytest.fixture
def api(tmp_path, egress_schema):
    token = new_token()
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps(
            {"tokens": [{"sha256": hash_token(token), "service": "jobs", "component": "planner"}]}
        )
    )
    with fresh_env() as env:
        settings = Settings(
            env=env,
            dsn=DSN,
            services=(),
            tokens_file=path,
            sweep_interval=3600,
            stats_interval=3600,
            egress_schema=egress_schema,
        )
        with TestClient(create_app(settings, registry=Registry(SPECS))) as client:
            client.headers["Authorization"] = f"Bearer {token}"
            yield client


def test_api_provider_rules(api, tmp_path, egress_schema):
    with psycopg.connect(DSN, autocommit=True) as conn:
        cfg = config.load(write(tmp_path))
        schema.apply(conn, schema.diff(conn, cfg, egress_schema), egress_schema)
    r = api.get("/v1/egress/providers")
    assert r.status_code == 200
    data = r.json()
    assert data["providers"]["example-shop"] == {"kind": "platform", "per_egress_per_min": 20}
    assert data["providers"]["example-parking"] == {"kind": "parking", "per_egress_per_min": None}
    assert data["cnames"]["cdn.example.net"] == "example-cdn"
    assert data["ranges"]["198.51.100.128/25"] == "example-cdn"
    assert data["shared_ip"] == {"min_domains": 1000, "per_egress": 30, "fleet_per_second": 2.0}

    # Tasks carry the provider; leases return it.
    r = api.post(
        "/v1/tasks/upsert",
        json={
            "task_type": "jobs.once",
            "tasks": [
                {"key": "a.example", "group": "a.example", "provider": "example-cdn"},
                {"key": "b.example", "group": "192.0.2.1"},
            ],
        },
    )
    assert r.status_code == 200, r.text


def test_api_without_egress_schema(tmp_path):
    token = new_token()
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps({"tokens": [{"sha256": hash_token(token), "admin": "x"}]}))
    with fresh_env() as env:
        settings = Settings(
            env=env, dsn=DSN, services=(), tokens_file=path, egress_schema="it_missing_egress"
        )
        with TestClient(create_app(settings, registry=Registry(SPECS))) as client:
            r = client.get("/v1/egress/providers", headers={"Authorization": f"Bearer {token}"})
            assert r.status_code == 503
            assert "not migrated" in r.json()["detail"]


# --- the provider column ----------------------------------------------------------------------


def run(coro_fn):
    async def main():
        async with await AsyncConnection.connect(DSN, autocommit=True) as conn:
            return await coro_fn(conn)

    return asyncio.run(main())


def test_frontier_provider():
    with fresh_env() as env:
        f = Frontier(env)
        run(
            lambda c: f.upsert(
                c,
                "jobs.once",
                [
                    UpsertItem("a", group="a", provider="example-cdn"),
                    UpsertItem("b", group="192.0.2.1"),
                    UpsertItem("c", group="c", provider="example-cdn"),
                ],
                "t",
            )
        )
        # Without a provider the old one stays; '' clears it (the target left the CDN).
        run(
            lambda c: f.upsert(
                c,
                "jobs.once",
                [UpsertItem("a"), UpsertItem("c", group="192.0.2.9", provider="")],
                "t",
            )
        )
        leased = run(lambda c: f.lease(c, ["jobs.once"], 10, "w"))
        got = {t["key"]: (t["group"], t["provider"]) for t in leased}
        assert got == {
            "a": ("a", "example-cdn"),
            "b": ("192.0.2.1", ""),
            "c": ("192.0.2.9", ""),
        }


def test_grant_writers(egress_schema):
    role = "it_writer_" + secrets.token_hex(3)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
        try:
            schema.grant_writers(conn, [role], egress_schema)
            schema.grant_writers(conn, [role], egress_schema)  # idempotent

            def can(table, priv):
                return conn.execute(
                    "SELECT has_table_privilege(%s, %s, %s)",
                    (role, f"{egress_schema}.{table}", priv),
                ).fetchone()[0]

            for t in ("target_leases", "target_cooldowns", "pair_usage", "demand"):
                assert can(t, "INSERT") and can(t, "UPDATE") and not can(t, "DELETE")
            assert not can("allocations", "INSERT") and not can("allocations", "DELETE")
            assert conn.execute(
                "SELECT has_column_privilege(%s, %s, 'used', 'UPDATE'), "
                "has_column_privilege(%s, %s, 'tokens', 'UPDATE')",
                (role, f"{egress_schema}.allocations") * 2,
            ).fetchone() == (True, False)
            for t in ("egresses", "providers", "settings"):
                assert can(t, "SELECT") and not can(t, "INSERT") and not can(t, "UPDATE")
        finally:
            conn.execute(sql.SQL("DROP OWNED BY {r}; DROP ROLE {r}").format(r=sql.Identifier(role)))
