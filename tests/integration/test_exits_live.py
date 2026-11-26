"""Routes through registry exits against a real Postgres: a task type with
``datacenter_via: exit`` leased by a worker without an address, the exit token, one
holder per target across the VPS's own workers and the exit, renewal with a fresh
token, an exit taken out of service, and the egress apply of ``exit`` endpoints."""

from __future__ import annotations

import asyncio
import secrets
import textwrap

import psycopg
import pytest
from psycopg import AsyncConnection, sql

from leasehold.client import Route
from leasehold.egress import config
from leasehold.egress import schema as egress_schema
from leasehold.egress.dispatch import EXIT_ACCOUNT, Dispatcher, EgressRefused
from leasehold.exit import token as exit_token
from leasehold.frontier import schema
from leasehold.frontier.store import Frontier, UpsertItem
from leasehold.spec import ServiceSpec

from .pg import DSN, requires_postgres

pytestmark = requires_postgres

KEY = bytes(range(32))

SPEC = ServiceSpec.model_validate(
    {
        "spec_version": 1,
        "name": "render",
        "description": "test service",
        "owner": "test",
        "components": {
            "worker": {
                "kind": "task-worker",
                "consumes_tasks": ["render.page", "render.fetch"],
                "node_class": "processing",
                "resources": {"cpu": 100, "memory": 64},
                "count": {"min": 1, "max": 1},
            },
        },
        "frontier": {
            "task_types": {
                # Leased by a worker without an address: datacenter through exits.
                "render.page": {
                    "key": "domain",
                    "lease_duration": "30s",
                    "target_leases": True,
                    "egress": {"tiers": ["datacenter"], "datacenter_via": "exit"},
                },
                # The VPS's own workers: datacenter by binding the address.
                "render.fetch": {
                    "key": "domain",
                    "lease_duration": "30s",
                    "target_leases": True,
                },
            }
        },
    }
)

FILES = {
    "registry.yml": """
        egresses:
          - { address: 192.0.2.1, asn: 64500, exit: { endpoint: "http://100.64.0.1:3130" } }
          - { address: 192.0.2.2, asn: 64500, exit: { endpoint: "http://100.64.0.2:3130" } }
          - { address: 192.0.2.3, asn: 64500 }
    """,
    "providers.yml": "providers: {}\n",
    "budgets.yml": "targets: { interval: 10s }\n",
}


def run(fn):
    async def main():
        async with await AsyncConnection.connect(DSN, autocommit=True) as conn:
            return await fn(conn)

    return asyncio.run(main())


@pytest.fixture
def world(tmp_path):
    tag = secrets.token_hex(4)
    env, eg = f"it{tag}", f"it{tag}_egress"
    for name, text in FILES.items():
        (tmp_path / name).write_text(textwrap.dedent(text))
    with psycopg.connect(DSN, autocommit=True) as conn:
        schema.migrate(conn, env)
        schema.apply_task_types(conn, env, schema.diff_task_types(conn, env, [SPEC]))
        egress_schema.migrate(conn, eg)
        egress_schema.apply(conn, egress_schema.diff(conn, config.load(tmp_path), eg), eg)
        try:
            yield env, eg, tmp_path
        finally:
            for s in (schema.schema_name(env), eg):
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(s)))


def upsert(env, task_type, keys, group):
    run(
        lambda c: Frontier(env).upsert(
            c, task_type, [UpsertItem(k, group=group) for k in keys], "t"
        )
    )


def egress_sql(eg, stmt, params=()):
    with psycopg.connect(DSN, autocommit=True) as conn:
        cur = conn.execute(sql.SQL(stmt).format(e=sql.Identifier(eg)), params)
        return cur.fetchall() if cur.description else None


def lease(d, types, egress=None, holder="b1", limit=10):
    return run(lambda c: d.lease(c, list(types), limit, egress, holder))


def test_apply_writes_exit_endpoints(world):
    _, eg, tmp_path = world
    rows = egress_sql(eg, "SELECT host(address), exit_endpoint FROM {e}.egresses ORDER BY 1")
    assert rows == [
        ("192.0.2.1", "http://100.64.0.1:3130"),
        ("192.0.2.2", "http://100.64.0.2:3130"),
        ("192.0.2.3", None),
    ]
    # The file is the desired state: dropping `exit` clears the endpoint.
    (tmp_path / "registry.yml").write_text(
        "egresses:\n  - { address: 192.0.2.1, asn: 64500 }\n"
        "  - { address: 192.0.2.2, asn: 64500 }\n  - { address: 192.0.2.3, asn: 64500 }\n"
    )
    with psycopg.connect(DSN, autocommit=True) as conn:
        d = egress_schema.diff(conn, config.load(tmp_path), eg)
        assert [a for a, _, _ in d.egresses] == ["update", "update"]
        egress_schema.apply(conn, d, eg)
    assert egress_sql(eg, "SELECT count(*) FROM {e}.egresses WHERE exit_endpoint IS NOT NULL") == [
        (0,)
    ]


def test_routes_through_exits_with_a_token_the_exit_accepts(world):
    env, eg, _ = world
    upsert(env, "render.page", ["a.example"], "198.51.100.10")
    upsert(env, "render.page", ["b.example"], "198.51.100.11")
    d = Dispatcher(env, schema.schema_name(env), eg, exit_key=KEY)
    g = lease(d, ["render.page"])
    assert len(g.tasks) == 2 and len(g.targets) == 2
    # Spread over the two exits (least loaded first); never the VPS without one.
    assert {t.route["egress"] for t in g.targets} == {"192.0.2.1", "192.0.2.2"}
    for t in g.targets:
        r = Route.parse(t.route)
        assert r.account == EXIT_ACCOUNT and r.kind == "datacenter"
        assert r.exit_ip == r.egress
        assert (r.proxy.scheme, r.proxy.port) == ("http", 3130)
        assert r.proxy.host == {"192.0.2.1": "100.64.0.1", "192.0.2.2": "100.64.0.2"}[r.egress]
        own = exit_token.exit_key(KEY, r.egress)
        claims = exit_token.verify([own], r.proxy.username, r.proxy.password, r.egress, [env])
        assert (claims.lease_id, claims.target, claims.env) == (str(t.lease_id), t.target, env)
        # Valid until the lease expires, not longer.
        assert claims.expires_at == int(t.expires_at.timestamp())
        # Signed with this exit's derived key: the other exit's key and the master don't verify.
        other = "192.0.2.2" if r.egress == "192.0.2.1" else "192.0.2.1"
        for keys in ([exit_token.exit_key(KEY, other)], [KEY]):
            with pytest.raises(exit_token.TokenError, match="signature"):
                exit_token.verify(keys, r.proxy.username, r.proxy.password, r.egress, [env])
    rows = egress_sql(eg, "SELECT via, account FROM {e}.target_leases")
    assert rows == [("exit", None), ("exit", None)]


def test_no_exit_key_no_routes(world):
    env, eg, _ = world
    upsert(env, "render.page", ["a.example"], "198.51.100.10")
    d = Dispatcher(env, schema.schema_name(env), eg)
    g = lease(d, ["render.page"])
    assert g.tasks == [] and g.waiting == {"no_exit": 1}


def test_never_from_a_workers_own_address(world):
    env, eg, _ = world
    upsert(env, "render.page", ["a.example"], "198.51.100.10")
    d = Dispatcher(env, schema.schema_name(env), eg, exit_key=KEY)
    # A worker that has an address still gets a route through an exit, not its own.
    g = lease(d, ["render.page"], egress="192.0.2.3")
    (t,) = g.targets
    assert t.route["account"] == EXIT_ACCOUNT and t.route["egress"] in ("192.0.2.1", "192.0.2.2")
    # Without exits and with no other tier, nothing is served from it.
    egress_sql(eg, "UPDATE {e}.egresses SET exit_override = 'disabled'")
    upsert(env, "render.page", ["b.example"], "198.51.100.11")
    g = lease(d, ["render.page"], egress="192.0.2.3")
    assert g.tasks == [] and g.waiting == {"no_exit": 1}
    with pytest.raises(EgressRefused):
        lease(d, ["render.page"], egress="192.0.2.99")


def test_one_holder_per_target_across_the_vps_workers_and_the_exit(world):
    env, eg, _ = world
    upsert(env, "render.fetch", ["a.example"], "198.51.100.10")
    upsert(env, "render.page", ["a.example"], "198.51.100.10")
    d = Dispatcher(env, schema.schema_name(env), eg, exit_key=KEY)
    g1 = lease(d, ["render.fetch"], egress="192.0.2.1", holder="vps-1")
    assert [t.target for t in g1.targets] == ["198.51.100.10"]
    g2 = lease(d, ["render.page"])
    assert g2.tasks == [] and g2.waiting == {"held": 1}


def test_pair_budget_is_shared_with_the_vps_workers(world):
    env, eg, _ = world
    egress_sql(
        eg,
        "INSERT INTO {e}.pair_usage (egress, target, hour, requests) "
        "VALUES ('192.0.2.1', '198.51.100.10', date_trunc('hour', now()), 600)",
    )
    upsert(env, "render.page", ["a.example"], "198.51.100.10")
    d = Dispatcher(env, schema.schema_name(env), eg, exit_key=KEY)
    g = lease(d, ["render.page"])
    assert [t.route["egress"] for t in g.targets] == ["192.0.2.2"]


def test_renewal_signs_a_new_token_and_a_disabled_exit_loses_the_lease(world):
    env, eg, _ = world
    upsert(env, "render.page", ["a.example"], "198.51.100.10")
    d = Dispatcher(env, schema.schema_name(env), eg, exit_key=KEY)
    g = lease(d, ["render.page"])
    (t,) = g.targets
    routes: dict = {}
    ok, lost = run(lambda c: d.renew(c, None, "b1", [t.lease_id], routes=routes))
    assert set(ok) == {t.lease_id} and lost == {}
    r = Route.parse(routes[t.lease_id])
    own = exit_token.exit_key(KEY, r.egress)
    claims = exit_token.verify([own], r.proxy.username, r.proxy.password, r.egress, [env])
    assert claims.expires_at == int(ok[t.lease_id].timestamp())
    # Another holder can't renew it.
    ok, lost = run(lambda c: d.renew(c, None, "b2", [t.lease_id]))
    assert ok == {} and lost == {t.lease_id: "not held"}

    egress_sql(
        eg,
        "UPDATE {e}.egresses SET exit_override = 'disabled', exit_override_reason = 'probe' "
        "WHERE host(address) = %s",
        (r.egress,),
    )
    ok, lost = run(lambda c: d.renew(c, None, "b1", [t.lease_id]))
    assert ok == {} and lost == {t.lease_id: "exit out of service"}
    # The VPS itself stays in service for its own workers.
    upsert(env, "render.fetch", ["c.example"], "198.51.100.12")
    g2 = lease(d, ["render.fetch"], egress=r.egress, holder="vps")
    assert [x.target for x in g2.targets] == ["198.51.100.12"]


def test_release_ends_an_exit_lease(world):
    env, eg, _ = world
    upsert(env, "render.page", ["a.example"], "198.51.100.10")
    d = Dispatcher(env, schema.schema_name(env), eg, exit_key=KEY)
    (t,) = lease(d, ["render.page"]).targets
    run(lambda c: d.renew(c, None, "b1", [], release=[t.lease_id]))
    rows = egress_sql(eg, "SELECT expires_at <= now() FROM {e}.target_leases")
    assert rows == [(True,)]


def test_held_by_others_lists_every_other_holders_live_targets(world):
    env, eg, _ = world
    upsert(env, "render.fetch", ["a.example"], "198.51.100.10")
    upsert(env, "render.page", ["b.example"], "198.51.100.11")
    d = Dispatcher(env, schema.schema_name(env), eg, exit_key=KEY)
    lease(d, ["render.fetch"], egress="192.0.2.1", holder="vps-1")
    lease(d, ["render.page"], holder="browser-1")
    # Another environment's holder with the browser's name still counts.
    other = Dispatcher(f"{env}x", schema.schema_name(env), eg, exit_key=KEY)
    egress_sql(
        eg,
        "INSERT INTO {e}.target_leases (target, egress, holder, env, lease_id, interval_ms, "
        "duration_ms, cap, granted, expires_at) VALUES ('198.51.100.12', '192.0.2.3', "
        "'browser-1', %s, gen_random_uuid(), 1000, 30000, 1, 1, now() + interval '1 minute'), "
        "('198.51.100.13', '192.0.2.3', 'old', %s, gen_random_uuid(), 1000, 30000, 1, 1, "
        "now() - interval '1 second')",
        (f"{env}x", env),
    )
    held = run(lambda c: d.held_by_others(c, "browser-1"))
    assert held == ["198.51.100.10", "198.51.100.12"]  # not its own, not the expired one
    assert run(lambda c: other.held_by_others(c, "browser-1")) == [
        "198.51.100.10",
        "198.51.100.11",
    ]
    assert run(lambda c: d.held_by_others(c, "browser-1", limit=1)) == ["198.51.100.10"]
