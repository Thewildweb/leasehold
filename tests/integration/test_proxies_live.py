"""Proxies against a real Postgres: routes from the dispatcher, tiers and escalation,
stops, concurrency, credential changes, proxy errors and account states, clears, import,
audit, rekey, the probe (with a local proxy) and quota states, and the API."""

from __future__ import annotations

import asyncio
import base64
import json
import secrets
import textwrap
from datetime import timedelta

import psycopg
import pytest
from fastapi.testclient import TestClient
from proxyserver import LocalProxy
from psycopg import AsyncConnection, sql

from leasehold.api.app import create_app
from leasehold.api.auth import Registry, hash_token
from leasehold.api.settings import Settings
from leasehold.egress import config
from leasehold.egress import schema as egress_schema
from leasehold.egress import secrets as sec
from leasehold.egress.dispatch import ClearRefused, Cooling, Dispatcher
from leasehold.egress.probe import Network, Prober
from leasehold.egress.proxy_admin import Conflict, ProxyAdmin, ProxyAdminError, parse_lines
from leasehold.egress.quota import QuotaKeeper
from leasehold.frontier import schema
from leasehold.frontier.store import Frontier, UpsertItem
from leasehold.spec import ServiceSpec

from .pg import DSN, requires_postgres

pytestmark = requires_postgres

PASSWORD = "pw-secret-1"

SPEC = ServiceSpec.model_validate(
    {
        "spec_version": 1,
        "name": "sites",
        "description": "test service",
        "owner": "test",
        "components": {
            "worker": {
                "kind": "task-worker",
                "consumes_tasks": ["sites.fetch", "sites.watch", "sites.sticky"],
                "node_class": "processing",
                "resources": {"cpu": 100, "memory": 64},
                "count": {"min": 1, "max": 1},
            },
            "health": {
                "kind": "scheduled",
                "schedule": "*/5 * * * *",
                "node_class": "services",
                "resources": {"cpu": 100, "memory": 64},
                "api_scopes": ["targets:clear", "targets:escalate"],
            },
        },
        "frontier": {
            "task_types": {
                "sites.fetch": {
                    "key": "domain",
                    "lease_duration": "30s",
                    "target_leases": True,
                    "egress": {
                        "tiers": [
                            "datacenter",
                            {"isp": ["isp-a"]},
                            {"residential": ["resi-pool"]},
                        ],
                        "countries": ["NL"],
                        "stop_at_used": {"isp": "70%"},
                    },
                },
                "sites.watch": {
                    "key": "domain",
                    "lease_duration": "30s",
                    "target_leases": True,
                    "egress": {"tiers": [{"isp": ["isp-a"]}]},
                },
                "sites.sticky": {
                    "key": "domain",
                    "lease_duration": "60s",
                    "target_leases": True,
                    "egress": {
                        "tiers": [{"residential": ["sticky-pool"]}],
                        "session": {"mode": "sticky", "ttl": "10m"},
                    },
                },
            }
        },
    }
)

FILES = {
    "registry.yml": "egresses:\n  - { address: 192.0.2.1, asn: 64500 }\n",
    "providers.yml": "providers: {}\n",
    "budgets.yml": "targets: { interval: 10s }\n",
}


def run(fn):
    async def main():
        async with await AsyncConnection.connect(DSN, autocommit=True) as conn:
            return await fn(conn)

    return asyncio.run(main())


class World:
    def __init__(self, env, eg, ring):
        self.env, self.eg, self.ring = env, eg, ring
        self.d = Dispatcher(env, schema.schema_name(env), eg, ring)
        self.admin = ProxyAdmin(eg, env, ring)

    def sql(self, stmt, params=()):
        with psycopg.connect(DSN, autocommit=True) as conn:
            cur = conn.execute(sql.SQL(stmt).format(e=sql.Identifier(self.eg)), params)
            return cur.fetchall() if cur.description else None

    def upsert(self, task_type, keys, group):
        run(lambda c: Frontier(self.env).upsert(
            c, task_type, [UpsertItem(k, group=group) for k in keys], "t"))  # fmt: skip

    def lease(self, task_types, egress=None, holder="w1", limit=10):
        return run(lambda c: self.d.lease(c, list(task_types), limit, egress, holder))


@pytest.fixture
def world(tmp_path):
    tag = secrets.token_hex(4)
    env, eg = f"it{tag}", f"it{tag}_egress"
    for name, text in FILES.items():
        (tmp_path / name).write_text(textwrap.dedent(text))
    ring = sec.Keyring.from_env({"LEASEHOLD_SECRETS_KEYS": f"k1:{sec.new_key()}"})
    with psycopg.connect(DSN, autocommit=True) as conn:
        schema.migrate(conn, env)
        schema.apply_task_types(conn, env, schema.diff_task_types(conn, env, [SPEC]))
        egress_schema.migrate(conn, eg)
        egress_schema.apply(conn, egress_schema.diff(conn, config.load(tmp_path), eg), eg)
        w = World(env, eg, ring)

        async def setup(c):
            a = w.admin
            await a.add_account(c, "admin:t", "isp-a", vendor="vendor-x", envs=[env],
                                cap_gb=100)  # fmt: skip
            await a.set_credentials(c, "admin:t", "isp-a", "isp-user", PASSWORD)
            for i in (1, 2, 3):
                await a.add_exit(c, "admin:t", "isp-a", f"isp.example:1000{i}", kind="isp",
                                 country="NL", address=f"203.0.113.{i}", asn=64501)  # fmt: skip
            await a.account_status(c, "admin:t", "isp-a", "enable")
            await a.add_account(c, "admin:t", "resi-a", vendor="vendor-y", envs=[env],
                                cap_gb=10)  # fmt: skip
            await a.set_credentials(c, "admin:t", "resi-a", "resi-user", PASSWORD)
            await a.add_pool(c, "admin:t", "resi-pool", "resi-a", kind="residential",
                             endpoint="gw.example:9000", countries=["NL"],
                             username_template="{user}-country-{country}")  # fmt: skip
            await a.add_pool(
                c,
                "admin:t",
                "sticky-pool",
                "resi-a",
                kind="residential",
                endpoint="gw.example:9001",
                countries=["NL"],
                sticky=True,
                max_sticky_ttl_s=1800,
                username_template="{user}-session-{session}-ttl-{ttl_min}",
            )
            await a.account_status(c, "admin:t", "resi-a", "enable")

        run(setup)
        try:
            yield w
        finally:
            for s in (schema.schema_name(env), eg):
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(s)))


# --- dispatch -----------------------------------------------------------------------------------


def test_proxy_only_type_gets_routes_and_no_own_egress(world):
    world.upsert("sites.watch", ["a.example", "a2.example"], "198.51.100.10")
    world.upsert("sites.watch", ["b.example"], "198.51.100.11")
    g = world.lease(["sites.watch"])
    assert len(g.tasks) == 3 and len(g.targets) == 2
    keys = {t.route["egress"] for t in g.targets}
    assert keys <= {"203.0.113.1", "203.0.113.2", "203.0.113.3"} and len(keys) == 2  # spread
    r = g.targets[0].route
    assert r["proxy"]["username"] == "isp-user" and r["proxy"]["password"] == PASSWORD
    assert r["proxy"]["host"] == "isp.example" and r["kind"] == "isp" and r["account"] == "isp-a"
    assert r["errors"][0] == {"status": 407, "state": "auth_failed"}
    assert all(t.tier == "isp" for t in g.targets)
    assert PASSWORD not in repr(g.targets[0])
    # Renewed without an egress of its own; a stranger can't renew it.
    ids = [t.lease_id for t in g.targets]
    ok, lost = run(lambda c: world.d.renew(c, None, "w1", ids))
    assert set(ok) == set(ids) and not lost
    ok, lost = run(lambda c: world.d.renew(c, None, "w2", ids))
    assert not ok and set(lost) == set(ids)


def test_fail_closed(world):
    world.upsert("sites.watch", ["a.example"], "198.51.100.10")
    # No key: no routes.
    keyless = Dispatcher(world.env, schema.schema_name(world.env), world.eg)
    assert run(lambda c: keyless.lease(c, ["sites.watch"], 5, None, "w1")).tasks == []
    # The account disabled: nothing, and the target waits.
    run(lambda c: world.admin.account_status(c, "admin:t", "isp-a", "disable"))
    g = world.lease(["sites.watch"])
    assert g.tasks == [] and g.waiting == {"account_unavailable": 1}
    # The account not allowed in this environment.
    run(lambda c: world.admin.account_status(c, "admin:t", "isp-a", "enable"))
    run(lambda c: world.admin.update_account(c, "admin:t", "isp-a", envs=["prod"]))
    assert world.lease(["sites.watch"]).tasks == []


def test_tiers_and_escalation(world):
    world.upsert("sites.fetch", ["a.example"], "198.51.100.20")
    g = world.lease(["sites.fetch"], egress="192.0.2.1")
    (tl,) = g.targets
    assert tl.tier == "datacenter" and tl.route is None
    # Without its own egress, a worker can't serve the datacenter tier.
    world.upsert("sites.fetch", ["b.example"], "198.51.100.21")
    assert world.lease(["sites.fetch"]).waiting == {"no_datacenter_egress": 1}

    # A block on datacenter: cooled on that tier, the target moves to isp.
    esc = []
    run(lambda c: world.d.cool(c, [Cooling("198.51.100.20", timedelta(minutes=10), "403",
                                           "block", "sites.fetch")], "192.0.2.1",
                               timedelta(hours=1), escalated=esc))  # fmt: skip
    assert esc == [("198.51.100.20", "isp", "sites.fetch")]
    world.sql("UPDATE {e}.target_leases SET expires_at = now()")
    world.upsert("sites.fetch", ["a2.example"], "198.51.100.20")
    g = world.lease(["sites.fetch"], egress="192.0.2.1")
    by_target = {t.target: t for t in g.targets}
    assert by_target["198.51.100.20"].tier == "isp" and by_target["198.51.100.20"].route

    # A block on isp: on to residential, through the pool, with the country in the name.
    run(lambda c: world.d.cool(c, [Cooling("198.51.100.20", timedelta(minutes=10), "challenge",
                                           "challenge", "sites.fetch")], None,
                               timedelta(hours=1)))  # fmt: skip
    world.sql("UPDATE {e}.target_leases SET expires_at = now()")
    world.upsert("sites.fetch", ["a3.example"], "198.51.100.20")
    g = world.lease(["sites.fetch"], egress="192.0.2.1")
    (t,) = [t for t in g.targets if t.target == "198.51.100.20"]
    assert t.tier == "residential" and t.route["egress"] == "pool:resi-pool"
    assert t.route["proxy"]["username"] == "resi-user-country-nl" and t.route["exit_ip"] is None

    # Rate pushback cools every tier.
    run(lambda c: world.d.cool(c, [Cooling("198.51.100.20", timedelta(minutes=10), "429",
                                           "rate", "sites.fetch")], None,
                               timedelta(hours=1)))  # fmt: skip
    world.sql("UPDATE {e}.target_leases SET expires_at = now()")
    world.upsert("sites.fetch", ["a4.example"], "198.51.100.20")
    assert "198.51.100.20" not in {
        t.target for t in world.lease(["sites.fetch"], egress="192.0.2.1").targets
    }
    rows = world.sql("SELECT tier, cause FROM {e}.target_cooldowns ORDER BY tier")
    assert rows == [("*", "rate"), ("datacenter", "block"), ("isp", "challenge")]


def test_escalate_verdict_and_last_tier(world):
    world.upsert("sites.fetch", ["a.example"], "198.51.100.30")
    g = world.lease(["sites.fetch"], egress="192.0.2.1")
    assert g.targets[0].tier == "datacenter"
    was, now = run(lambda c: world.d.escalate(c, "198.51.100.30", "sites.fetch",
                                              "unreachable_from_dc", {"verdict": 1}))  # fmt: skip
    assert (was, now) == ("datacenter", "isp")
    # The datacenter lease is revoked: its holder stops.
    ok, lost = run(lambda c: world.d.renew(c, "192.0.2.1", "w1", [g.targets[0].lease_id]))
    assert "escalated to isp" in next(iter(lost.values()))
    run(lambda c: world.d.escalate(c, "198.51.100.30", "sites.fetch", "x", None))
    assert run(lambda c: world.d.escalate(c, "198.51.100.30", "sites.fetch", "x", None)) == (
        "residential", None)  # fmt: skip


def test_stop_at_used_and_concurrency(world):
    # 80% of a 1 GB cap used: sites.fetch stops at 70%, sites.watch has no stop.
    run(lambda c: world.admin.update_account(c, "admin:t", "isp-a", cap_gb=1))
    world.sql(
        "INSERT INTO {e}.proxy_usage (account, ref, env, task_type, day, bytes, requests) "
        "VALUES ('isp-a', '203.0.113.1', 'x', 'sites.watch', current_date, 800000000, 1)"
    )
    world.sql(
        "INSERT INTO {e}.target_tiers (target, tier, reason, task_type, env) "
        "VALUES ('198.51.100.40', 'isp', 't', 'sites.fetch', 'x')"
    )
    world.upsert("sites.fetch", ["a.example"], "198.51.100.40")
    g = world.lease(["sites.fetch"], egress="192.0.2.1")
    assert g.tasks == [] and g.waiting == {"stopped": 1}
    assert {(s.task_type, s.account) for s in world.d.stopped} == {("sites.fetch", "isp-a")}
    world.upsert("sites.watch", ["w.example"], "198.51.100.41")
    assert len(world.lease(["sites.watch"]).tasks) == 1

    # Concurrency 1 on the account: a second target waits.
    run(lambda c: world.admin.update_account(c, "admin:t", "isp-a", concurrency=1))
    world.upsert("sites.watch", ["w2.example"], "198.51.100.42")
    g = world.lease(["sites.watch"])
    assert g.tasks == [] and g.waiting == {"concurrency": 1}


def test_sticky_sessions(world):
    world.upsert("sites.sticky", ["s.example"], "198.51.100.50")
    (t,) = world.lease(["sites.sticky"]).targets
    sess = t.route["session"]
    assert sess["id"].startswith("s") and sess["ttl_s"] == 600
    assert t.route["proxy"]["username"] == f"resi-user-session-{sess['id']}-ttl-10"
    # A pool without sticky support doesn't serve it.
    world.sql("UPDATE {e}.proxy_pools SET sticky = false WHERE name = 'sticky-pool'")
    world.upsert("sites.sticky", ["s2.example"], "198.51.100.51")
    assert world.lease(["sites.sticky"]).waiting == {"no_sticky": 1}


def test_new_credentials_come_with_the_renewal(world):
    world.upsert("sites.watch", ["a.example"], "198.51.100.60")
    (t,) = world.lease(["sites.watch"]).targets
    routes: dict = {}
    run(lambda c: world.d.renew(c, None, "w1", [t.lease_id], routes=routes))
    assert routes == {}
    fp = run(lambda c: world.admin.set_credentials(c, "admin:t", "isp-a", "isp-user", "pw-2"))
    run(lambda c: world.d.renew(c, None, "w1", [t.lease_id], routes=routes))
    assert routes[t.lease_id]["proxy"]["password"] == "pw-2"
    assert routes[t.lease_id]["credentials_fp"] == fp
    routes.clear()
    run(lambda c: world.d.renew(c, None, "w1", [t.lease_id], routes=routes))
    assert routes == {}  # only once


def test_proxy_only_type_never_rides_a_datacenter_lease(world):
    # sites.fetch starts on datacenter, sites.watch only uses proxies; same target.
    world.upsert("sites.fetch", ["a.example"], "198.51.100.10")
    world.upsert("sites.watch", ["b.example"], "198.51.100.10")
    g = world.lease(["sites.fetch", "sites.watch"], egress="192.0.2.1")
    assert [t["task_type"] for t in g.tasks] == ["sites.fetch"]
    assert all(t.route is None for t in g.targets)
    # Only the proxy-only type on our own address: nothing, and not refused.
    world.upsert("sites.watch", ["c.example"], "198.51.100.12")
    g = world.lease(["sites.watch"], egress="192.0.2.1")
    assert [t.route["egress"] is not None for t in g.targets] == [True]


def test_route_refresh_keeps_session_and_country(world):
    world.upsert("sites.sticky", ["s.example"], "198.51.100.55")
    (t,) = world.lease(["sites.sticky"]).targets
    run(lambda c: world.admin.update_pool(c, "admin:t", "sticky-pool", max_sticky_ttl_s=3600))
    run(lambda c: world.admin.set_credentials(c, "admin:t", "resi-a", "resi-user", "pw-new"))
    routes: dict = {}
    run(lambda c: world.d.renew(c, None, "w1", [t.lease_id], routes=routes))
    r = routes[t.lease_id]
    assert r["proxy"]["password"] == "pw-new"
    assert r["proxy"]["username"] == t.route["proxy"]["username"]  # same session, same TTL
    assert r["session"] == t.route["session"]
    # A pool with several countries: the refresh keeps the planned one.
    run(lambda c: world.admin.update_pool(c, "admin:t", "resi-pool", countries=["DE", "NL"]))
    world.sql(
        "INSERT INTO {e}.target_tiers (target, tier, reason, task_type, env) "
        "VALUES ('198.51.100.56', 'residential', 't', 'sites.fetch', 'x')"
    )
    world.upsert("sites.fetch", ["n.example"], "198.51.100.56")
    (t,) = world.lease(["sites.fetch"], egress="192.0.2.1").targets
    assert t.route["proxy"]["username"] == "resi-user-country-nl"
    run(lambda c: world.admin.set_credentials(c, "admin:t", "resi-a", "resi-user", "pw-3"))
    routes = {}
    run(lambda c: world.d.renew(c, "192.0.2.1", "w1", [t.lease_id], routes=routes))
    assert routes[t.lease_id]["proxy"]["username"] == "resi-user-country-nl"


def test_concurrency_holds_across_dispatchers(world):
    run(lambda c: world.admin.update_account(c, "admin:t", "isp-a", concurrency=1))
    world.upsert("sites.watch", ["a.example"], "198.51.100.65")
    world.upsert("sites.watch", ["b.example"], "198.51.100.66")

    async def main():
        async with (
            await AsyncConnection.connect(DSN, autocommit=True) as c1,
            await AsyncConnection.connect(DSN, autocommit=True) as c2,
        ):
            async with c1.transaction():
                first = await world.d.lease(c1, ["sites.watch"], 1, None, "w1")
                # The second dispatcher waits for the first one's claims to commit.
                second = asyncio.create_task(world.d.lease(c2, ["sites.watch"], 1, None, "w2"))
                await asyncio.sleep(0.5)
                assert not second.done()
            return first, await second

    first, second = asyncio.run(main())
    assert len(first.tasks) == 1
    assert second.tasks == [] and set(second.waiting) == {"concurrency"}


# --- proxy errors and account states --------------------------------------------------------------


def report(world, key, status=None, excerpt=""):
    async def go(c):
        return await world.admin.record_proxy_error(
            c, "isp-a", key, status, excerpt, max_failures=3, unreachable_refs=3, window_s=60
        )

    return run(go)


def test_refusal_sets_the_account_state_and_ends_its_leases(world):
    world.upsert("sites.watch", ["a.example"], "198.51.100.70")
    (t,) = world.lease(["sites.watch"]).targets
    assert report(world, t.route["egress"], 407, "Proxy Authentication Required") == "auth_failed"
    ok, lost = run(lambda c: world.d.renew(c, None, "w1", [t.lease_id]))
    assert lost[t.lease_id] == "proxy account isp-a is auth_failed"
    world.upsert("sites.watch", ["b.example"], "198.51.100.71")
    assert world.lease(["sites.watch"]).waiting == {"account_unavailable": 1}
    audit = world.sql("SELECT actor, action, changes FROM {e}.proxy_audit WHERE action = 'state'")
    assert audit[0][0] == "control-api" and audit[0][2]["state"] == ["ok", "auth_failed"]
    # A vendor pattern from adapter_config.
    run(lambda c: world.admin.account_status(c, "admin:t", "isp-a", "enable"))
    run(lambda c: world.admin.update_account(c, "admin:t", "isp-a", adapter_config={"errors": [
        {"status": 403, "body_regex": "traffic limit", "state": "quota_exhausted"}]}))  # fmt: skip
    assert report(world, "203.0.113.2", 403, "Traffic limit reached") == "quota_exhausted"


def test_connect_failures_disable_an_exit_then_the_account(world):
    assert [report(world, "203.0.113.1") for _ in range(3)] == ["connect", "connect", "disabled"]
    assert world.sql("SELECT override_status FROM {e}.egresses WHERE address = '203.0.113.1'") == [
        ("disabled",)]  # fmt: skip
    # A success elsewhere clears the correlation window; failures through three exits
    # within it make the account unreachable.
    run(lambda c: world.admin.clear_failures(c, {"isp-a"}))
    report(world, "203.0.113.2")
    report(world, "203.0.113.3")
    assert report(world, "203.0.113.1") == "unreachable"
    state = world.sql("SELECT state FROM {e}.proxy_accounts WHERE name = 'isp-a'")
    assert state == [("unreachable",)]


def gateway(world, key, target):
    async def go(c):
        return await world.admin.record_proxy_error(
            c, "isp-a", key, 502, "502 Bad Gateway", kind="gateway", target=target,
            max_failures=3, unreachable_refs=3, window_s=60, gateway_targets=3,
            gateway_window_s=600,
        )  # fmt: skip

    return run(go)


def test_gateway_errors_count_only_across_distinct_sites(world):
    # One dead site retried on every exit: not the exits, not the account.
    assert [gateway(world, f"203.0.113.{i}", "dead.example") for i in (1, 2, 3)] == [
        "gateway"] * 3  # fmt: skip
    assert gateway(world, "203.0.113.1", "dead.example") == "gateway"
    assert world.sql("SELECT state FROM {e}.proxy_accounts WHERE name = 'isp-a'") == [("ok",)]
    assert world.sql("SELECT max(consecutive_failures) FROM {e}.egresses") == [(0,)]
    # Distinct sites failing on one exit: from the third on, connect failures.
    assert gateway(world, "203.0.113.1", "b.example") == "gateway"
    assert gateway(world, "203.0.113.1", "c.example") == "connect"
    assert gateway(world, "203.0.113.1", "d.example") == "connect"
    # A success through the exit starts it over.
    run(lambda c: world.admin.record_success(c, "isp-a", "203.0.113.1"))
    assert gateway(world, "203.0.113.1", "e.example") == "gateway"
    assert world.sql("SELECT consecutive_failures FROM {e}.egresses "
                     "WHERE address = '203.0.113.1'") == [(0,)]  # fmt: skip


# --- clears -------------------------------------------------------------------------------------


def test_clear_rules(world):
    cool = [Cooling("t1", timedelta(minutes=10), "challenge page", "challenge"),
            Cooling("t2", timedelta(minutes=10), "Retry-After 600", "retry_after"),
            Cooling("t3", timedelta(minutes=10), "429", "rate")]  # fmt: skip
    run(lambda c: world.d.cool(c, cool, "192.0.2.1", timedelta(hours=1)))
    cleared = run(lambda c: world.d.clear(c, "t1", "health", "site-wide", {"event": 1}, "h"))
    assert [r["cause"] for r in cleared] == ["challenge"]
    with pytest.raises(ClearRefused) as e:
        run(lambda c: world.d.clear(c, "t2", "admin", "x", None, "admin:x"))
    assert e.value.causes == ["retry_after"]
    with pytest.raises(ClearRefused):
        run(lambda c: world.d.clear(c, "t3", "health", "x", None, "h"))
    assert run(lambda c: world.d.clear(c, "t3", "admin", "x", None, "admin:x"))
    assert run(lambda c: world.d.clear(c, "nothing", "admin", "x", None, "admin:x")) == []
    rows = world.sql("SELECT target, cleared, source FROM {e}.target_clears ORDER BY id")
    assert rows == [("t1", True, "health"), ("t2", False, "admin"), ("t3", False, "health"),
                    ("t3", True, "admin")]  # fmt: skip
    assert {r["target"] for r in run(lambda c: world.d.recent_clears(c, ["t1", "t2"]))} == {"t1"}


# --- import, audit, listing, rekey ----------------------------------------------------------------


def test_import(world):
    text = "isp2.example:20001:u2:p2-secret\nisp2.example:20002:u2:p2-secret\n"

    async def imp(c, t=text, **kw):
        return await world.admin.import_list(c, "admin:t", "isp-b", "isp", parse_lines(t),
                                             country="NL", **kw)  # fmt: skip

    r = run(imp)
    assert r.account_created and r.credentials_changed and r.exits_created == 2
    r = run(imp)
    assert not r.credentials_changed and r.exits_existing == 2 and r.exits_created == 0
    r = run(lambda c: imp(c, text.replace("p2-secret", "p3-secret")))
    assert r.credentials_changed
    with pytest.raises(ProxyAdminError, match="split the list"):
        run(lambda c: imp(c, text + "isp2.example:20003:other:pw\n"))
    with pytest.raises(Conflict):  # an endpoint of another account
        run(lambda c: world.admin.import_list(c, "admin:t", "isp-c", "isp",
                                              parse_lines("isp2.example:20001:u:p\n")))  # fmt: skip
    # A pool from one gateway line.
    r = run(lambda c: world.admin.import_list(
        c, "admin:t", "resi-b", "residential", parse_lines("gw2.example:9000:u-nl:pw\n"),
        pool="resi-b-nl", countries=["NL"]))  # fmt: skip
    assert r.pools_created == ["resi-b-nl"]
    status = world.sql("SELECT status FROM {e}.proxy_accounts WHERE name = 'isp-b'")
    assert status == [("disabled",)]  # imported accounts start disabled

    listing = run(lambda c: world.admin.listing(c))
    dump = json.dumps(listing, default=str)
    audit = json.dumps(world.sql("SELECT changes FROM {e}.proxy_audit"), default=str)
    for secret in ("p2-secret", "p3-secret", PASSWORD, "isp-user"):
        assert secret not in dump and secret not in audit
    names = {a["name"] for a in listing["accounts"]}
    assert {"isp-a", "isp-b", "resi-a", "resi-b"} <= names
    exits = [x for x in listing["exits"] if x["account"] == "isp-b"]
    assert len(exits) == 2 and all(x["address"] is None for x in exits)


def test_rekey(world):
    rows = world.sql("SELECT name, key_id FROM {e}.proxy_accounts ORDER BY name")
    assert {k for _, k in rows} == {"k1"}
    k2 = base64.b64decode(sec.new_key())
    new = sec.Keyring([("k1", world.ring._keys["k1"]), ("k2", k2)])
    admin = ProxyAdmin(world.eg, world.env, new)
    assert run(lambda c: admin.rekey(c, "admin:t")) == 2
    assert {k for _, k in world.sql("SELECT name, key_id FROM {e}.proxy_accounts")} == {"k2"}
    # Routes still work with the new key alone.
    only_new = sec.Keyring([("k2", new._keys["k2"])])
    d = Dispatcher(world.env, schema.schema_name(world.env), world.eg, only_new)
    world.upsert("sites.watch", ["a.example"], "198.51.100.80")
    (t,) = run(lambda c: d.lease(c, ["sites.watch"], 5, None, "w1")).targets
    assert t.route["proxy"]["password"] == PASSWORD


# --- probe and quota ---------------------------------------------------------------------------


def test_probe_learns_and_follows_addresses(world):
    proxy = LocalProxy("probe-user", "probe-pw")
    try:

        async def setup(c):
            await world.admin.add_account(c, "admin:t", "isp-p", envs=[world.env], cap_gb=5)
            await world.admin.set_credentials(c, "admin:t", "isp-p", "probe-user", "probe-pw")
            await world.admin.add_exit(c, "admin:t", "isp-p", proxy.endpoint(), country="NL")

        run(setup)

        async def lookup(address):
            return Network(64999, None)

        prober = Prober(world.admin, f"http://{proxy.ECHO_HOST}/", lookup=lookup)
        counts = run(lambda c: prober.recheck_exits(c, account="isp-p"))
        assert counts["learned"] == 1
        row = world.sql("SELECT host(address), subnet::text, asn, source, account, country "
                        "FROM {e}.egresses WHERE account = 'isp-p'")  # fmt: skip
        assert row == [("203.0.113.10", "203.0.113.0/24", 64999, "live", "isp-p", "NL")]
        assert proxy.users == ["probe-user"]
        # Same address: nothing changes. A new one: the old row is disabled, a new one made.
        assert run(lambda c: prober.recheck_exits(c, account="isp-p"))["same"] == 1
        proxy.exit_ip = "203.0.113.11"
        assert run(lambda c: prober.recheck_exits(c, account="isp-p"))["moved"] == 1
        rows = world.sql("SELECT host(address), override_status, override_reason "
                         "FROM {e}.egresses WHERE account = 'isp-p' ORDER BY address")  # fmt: skip
        assert rows == [("203.0.113.10", "disabled", "exit moved to 203.0.113.11"),
                        ("203.0.113.11", None, None)]  # fmt: skip
        # Account recovery: a refusal sets the state, a working probe clears it.
        proxy.password = "changed"
        res = run(lambda c: prober.probe_account(c, "isp-p"))
        assert res.status == 407 and "changed" not in (res.error or "")
        assert world.sql("SELECT state FROM {e}.proxy_accounts WHERE name = 'isp-p'") == [
            ("auth_failed",)]  # fmt: skip
        proxy.password = "probe-pw"
        assert run(lambda c: prober.probe_account(c, "isp-p")).address == "203.0.113.11"
        assert world.sql("SELECT state FROM {e}.proxy_accounts WHERE name = 'isp-p'") == [
            ("ok",)]  # fmt: skip
        # Back to the first address (A -> B -> A): its removed row is reused.
        proxy.exit_ip = "203.0.113.10"
        assert run(lambda c: prober.recheck_exits(c, account="isp-p"))["moved"] == 1
        rows = world.sql("SELECT host(address), override_status, removed_at IS NULL "
                         "FROM {e}.egresses WHERE account = 'isp-p' ORDER BY address")  # fmt: skip
        assert rows == [("203.0.113.10", None, True), ("203.0.113.11", "disabled", False)]
        # A state the vendor's usage API reports isn't cleared by a working probe.
        run(lambda c: world.admin.set_state(c, "isp-p", "suspended",
                                            "vendor: the usage API reports suspended"))  # fmt: skip
        assert run(lambda c: prober.probe_account(c, "isp-p")).address == "203.0.113.10"
        assert world.sql("SELECT state FROM {e}.proxy_accounts WHERE name = 'isp-p'") == [
            ("suspended",)]  # fmt: skip
        run(lambda c: world.admin.account_status(c, "admin:t", "isp-p", "drain"))
        assert run(lambda c: prober.probe_unhealthy(c)) == 0
    finally:
        proxy.close()


class FakeAdapter:
    name = "fake"

    def __init__(self):
        self.calls = []

    def usage(self, account, secret, now):
        from leasehold.egress.usage import Usage

        self.calls.append((account.name, account.username, secret))
        return Usage(used_bytes=950_000_000, limit_bytes=1_000_000_000)


def test_quota_states(world):
    fake = FakeAdapter()
    run(lambda c: world.admin.update_account(c, "admin:t", "isp-a", adapter="fake"))
    run(lambda c: world.admin.set_usage_key(c, "admin:t", "isp-a", b"usage-key-1"))
    keeper = QuotaKeeper(world.admin, {"fake": fake})
    assert run(lambda c: keeper.poll(c))["isp-a"] == "ok"
    assert fake.calls == [("isp-a", "isp-user", b"usage-key-1")]
    ev = run(lambda c: keeper.evaluate(c))
    assert ev.health["isp-a"].warn
    # 95% used by the vendor's count, nothing by ours: a warning, and the drift as info.
    assert {(n.level, n.kind) for n in ev.notices} == {("warning", "warn_used"), ("info", "drift")}
    assert run(lambda c: keeper.evaluate(c)).notices == []  # once per period

    # Our cap used up: quota_exhausted before the vendor refuses; more GB clears it.
    run(lambda c: world.admin.update_account(c, "admin:t", "resi-a", cap_gb=1))
    world.sql(
        "INSERT INTO {e}.proxy_usage (account, ref, env, task_type, day, bytes, requests) "
        "VALUES ('resi-a', 'pool:resi-pool', 'x', 'sites.fetch', current_date, 1100000000, 3)"
    )
    ev = run(lambda c: keeper.evaluate(c))
    assert ("page", "resi-a", "exhausted") in {(n.level, n.account, n.kind) for n in ev.notices}
    assert world.sql("SELECT state FROM {e}.proxy_accounts WHERE name = 'resi-a'") == [
        ("quota_exhausted",)]  # fmt: skip
    run(lambda c: world.admin.update_account(c, "admin:t", "resi-a", cap_gb=2))
    run(lambda c: keeper.evaluate(c))
    assert world.sql("SELECT state FROM {e}.proxy_accounts WHERE name = 'resi-a'") == [("ok",)]


# --- the API ------------------------------------------------------------------------------------


@pytest.fixture
def api(world, tmp_path):
    tokens = {k: secrets.token_hex(8) for k in ("worker", "admin", "health")}
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps({"tokens": [
        {"sha256": hash_token(tokens["worker"]), "service": "sites", "component": "worker"},
        {"sha256": hash_token(tokens["admin"]), "admin": "x"},
        {"sha256": hash_token(tokens["health"]), "service": "sites", "component": "health"},
    ]}))  # fmt: skip
    settings = Settings(
        env=world.env, dsn=DSN, services=(), tokens_file=path, sweep_interval=3600,
        stats_interval=3600, egress_schema=world.eg, egress_owner=True, plan_interval=3600,
        proxy_interval=3600,
    )  # fmt: skip
    app = create_app(settings, registry=Registry([SPEC]), keyring=world.ring, adapters={})
    with TestClient(app) as client:
        client.tokens = tokens
        yield client


def call(api, role, method, path, body=None):
    return api.request(method, path, json=body,
                       headers={"Authorization": f"Bearer {api.tokens[role]}"})  # fmt: skip


def test_api_routes_errors_and_accounts(api, world):
    world.upsert("sites.watch", ["a.example"], "198.51.100.90")
    # A proxy-only task type needs no egress.
    r = call(api, "worker", "POST", "/v1/tasks/lease",
             {"max": 5, "task_types": ["sites.watch"], "worker_id": "w1"})  # fmt: skip
    assert r.status_code == 200, r.text
    (tl,) = r.json()["targets"]
    assert tl["tier"] == "isp" and tl["route"]["proxy"]["password"] == PASSWORD
    # A mixed type still needs it.
    r = call(api, "worker", "POST", "/v1/tasks/lease",
             {"max": 5, "task_types": ["sites.fetch"]})  # fmt: skip
    assert r.status_code == 400
    # Bytes and a proxy refusal with a release: usage, and the account's state.
    world.upsert("sites.watch", ["b.example"], "198.51.100.91")
    r = call(api, "worker", "POST", "/v1/tasks/lease",
             {"max": 5, "task_types": ["sites.watch"], "worker_id": "w1"})  # fmt: skip
    task = r.json()["tasks"][0]
    ref = {k: task[k] for k in ("task_type", "key", "lease_id")}
    r = call(api, "worker", "POST", "/v1/tasks/release",
             {"leases": [ref | {"bytes": 1234, "proxy_error": {
                 "kind": "refused", "status": 407,
                 "excerpt": "via http://isp-user:pw@isp.example:10001"}}]})  # fmt: skip
    assert r.status_code == 200, r.text
    assert world.sql("SELECT state, state_reason FROM {e}.proxy_accounts WHERE name = 'isp-a'")[
        0][0] == "auth_failed"  # fmt: skip
    reason = world.sql("SELECT state_reason FROM {e}.proxy_accounts WHERE name = 'isp-a'")[0][0]
    assert "pw@" not in reason
    assert world.sql("SELECT sum(bytes)::int FROM {e}.proxy_usage") == [(1234,)]
    # The accounts endpoint: any service token, no credentials.
    r = call(api, "worker", "GET", "/v1/egress/accounts")
    assert r.status_code == 200
    body = r.text
    assert "isp-a" in body and PASSWORD not in body and "isp-user" not in body


def test_api_admin_and_scopes(api, world):
    r = call(api, "admin", "POST", "/v1/admin/proxies/import",
             {"account": "isp-z", "kind": "isp", "country": "NL", "probe": False,
              "lines": "z.example:1:zu:z-secret\nz.example:2:zu:z-secret\n"})  # fmt: skip
    assert r.status_code == 200, r.text
    assert "z-secret" not in r.text and r.json()["exits_created"] == 2
    r = call(api, "admin", "POST", "/v1/admin/proxies/import",
             {"account": "isp-z", "kind": "isp", "lines": "z.example:1:zu\n"})  # fmt: skip
    assert r.status_code == 422 and "line 1" in r.text and "zu" not in r.json()["detail"]
    r = call(api, "worker", "POST", "/v1/admin/proxies/rekey")
    assert r.status_code == 403
    r = call(api, "admin", "GET", "/v1/admin/proxies?unused=1")
    assert r.status_code == 200 and "z-secret" not in r.text
    assert "isp-z" in r.json()["unused"] and "isp-a" not in r.json()["unused"]
    r = call(api, "admin", "POST", "/v1/admin/proxies/accounts/isp-z/credentials",
             {"username": "zu", "password": "z-new"})  # fmt: skip
    assert r.status_code == 200 and set(r.json()) == {"credentials_fp"}

    # Clears: the health component's scope, as health; admins as admin; workers not at all.
    run(lambda c: world.d.cool(c, [Cooling("t9", timedelta(minutes=5), "challenge",
                                           "challenge")], None, timedelta(hours=1)))  # fmt: skip
    body = {"source": "health", "reason": "site-wide challenge", "evidence": {"e": 1}}
    assert call(api, "worker", "POST", "/v1/targets/t9/clear", body).status_code == 403
    assert call(api, "health", "POST", "/v1/targets/t9/clear",
                body | {"source": "admin"}).status_code == 403  # fmt: skip
    r = call(api, "health", "POST", "/v1/targets/t9/clear", body)
    assert r.status_code == 200 and r.json()["cleared"] == 1
    run(lambda c: world.d.cool(c, [Cooling("t10", timedelta(minutes=5), "Retry-After",
                                           "retry_after")], None, timedelta(hours=1)))  # fmt: skip
    r = call(api, "admin", "POST", "/v1/targets/t10/clear", {"source": "admin", "reason": "x"})
    assert r.status_code == 409 and r.json()["causes"] == ["retry_after"]
    # Escalation.
    r = call(api, "health", "POST", "/v1/targets/t11/escalate",
             {"task_type": "sites.fetch", "reason": "unreachable_from_dc"})  # fmt: skip
    assert r.json() == {"target": "t11", "from": "datacenter", "to": "isp"}
    assert call(api, "worker", "POST", "/v1/targets/t11/escalate",
                {"task_type": "sites.fetch", "reason": "x"}).status_code == 403  # fmt: skip


def test_registry_apply_leaves_live_rows_and_overrides(world, tmp_path):
    run(lambda c: world.admin.exit_status(c, "admin:t", "192.0.2.1", "drain"))
    with psycopg.connect(DSN, autocommit=True) as conn:
        d = egress_schema.diff(conn, config.load(tmp_path), world.eg)
        assert d.egresses == []  # the live exits are no "disable: not in the registry"
        egress_schema.apply(conn, d, world.eg)
    rows = world.sql("SELECT host(address), status, override_status FROM {e}.egresses "
                     "ORDER BY address")  # fmt: skip
    assert ("192.0.2.1", "active", "drain") in rows
    assert sum(1 for r in rows if r[0].startswith("203.0.113.")) == 3
    # The drained VPS leases nothing.
    world.upsert("sites.fetch", ["a.example"], "198.51.100.99")
    from leasehold.egress.dispatch import EgressRefused

    with pytest.raises(EgressRefused, match="drain"):
        world.lease(["sites.fetch"], egress="192.0.2.1")
    with pytest.raises(Conflict):
        run(lambda c: world.admin.exit_status(c, "admin:t", "192.0.2.1", "remove"))


def test_reader_role_sees_no_ciphertext(world):
    role = f"{world.eg}_reader"
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
        try:
            egress_schema.grant_readers(conn, [role], world.eg)
            with conn.transaction():
                conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
                q = sql.SQL("SELECT name, state FROM {}.proxy_accounts").format(
                    sql.Identifier(world.eg)
                )
                assert len(conn.execute(q).fetchall()) == 2
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    conn.execute(
                        sql.SQL("SELECT credentials FROM {}.proxy_accounts").format(
                            sql.Identifier(world.eg)
                        )
                    )
        finally:
            conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def _lease_one(api, keys):
    r = call(api, "worker", "POST", "/v1/tasks/lease",
             {"max": 1, "task_types": ["sites.watch"], "worker_id": "w1"})  # fmt: skip
    assert r.status_code == 200, r.text
    (task,) = r.json()["tasks"]
    (tl,) = r.json()["targets"]
    ref = {k: task[k] for k in ("task_type", "key", "lease_id")}
    route = {"lease_id": tl["lease_id"], "account": tl["route"]["account"],
             "egress": tl["route"]["egress"],
             "credentials_fp": tl["route"]["credentials_fp"]}  # fmt: skip
    return ref, route


def test_api_charges_the_lease_the_task_ran_on(api, world):
    world.upsert("sites.watch", ["a.example", "a2.example"], "198.51.100.92")
    ref1, route1 = _lease_one(api, ["a.example"])
    # The target lease rolls over to another exit while the first task still runs.
    world.sql("UPDATE {e}.target_leases SET expires_at = now()")
    world.sql("UPDATE {e}.egresses SET override_status = 'drain' WHERE host(address) = %s",
              (route1["egress"],))  # fmt: skip
    ref2, route2 = _lease_one(api, ["a2.example"])
    assert route2["egress"] != route1["egress"] and route2["lease_id"] != route1["lease_id"]
    # The first task's bytes and refusal go to its own route; the new lease lives on.
    r = call(api, "worker", "POST", "/v1/tasks/release",
             {"leases": [ref1 | {"bytes": 500, "route": route1, "proxy_error": {
                 "kind": "connect", "excerpt": "tunnel failed"}}]})  # fmt: skip
    assert r.status_code == 200, r.text
    assert world.sql("SELECT ref, bytes FROM {e}.proxy_usage") == [(route1["egress"], 500)]
    assert world.sql("SELECT host(address) FROM {e}.egresses "
                     "WHERE consecutive_failures = 1") == [(route1["egress"],)]  # fmt: skip
    assert world.sql("SELECT expires_at > now() FROM {e}.target_leases WHERE lease_id = %s",
                     (route2["lease_id"],)) == [(True,)]  # fmt: skip


def test_api_ignores_errors_from_a_rotated_route(api, world):
    world.upsert("sites.watch", ["a.example"], "198.51.100.93")
    ref, route = _lease_one(api, ["a.example"])
    run(lambda c: world.admin.set_credentials(c, "admin:t", "isp-a", "isp-user", "pw-rotated"))
    # The old password's 407: not the account's fault. The task goes back at once and
    # the target lease ends, so the next lease brings a fresh route.
    r = call(api, "worker", "POST", "/v1/tasks/release",
             {"leases": [ref | {"route": route, "proxy_error": {
                 "kind": "refused", "status": 407, "excerpt": "auth required"}}]})  # fmt: skip
    assert r.status_code == 200, r.text
    assert world.sql("SELECT state FROM {e}.proxy_accounts WHERE name = 'isp-a'") == [("ok",)]
    assert world.sql("SELECT max(consecutive_failures) FROM {e}.egresses") == [(0,)]
    assert world.sql("SELECT expires_at > now() FROM {e}.target_leases WHERE lease_id = %s",
                     (route["lease_id"],)) == [(False,)]  # fmt: skip
    ref2, route2 = _lease_one(api, ["a.example"])
    assert ref2["key"] == "a.example" and route2["credentials_fp"] != route["credentials_fp"]
    # A removed exit is stale too.
    world.sql("UPDATE {e}.egresses SET removed_at = now() WHERE host(address) = %s",
              (route2["egress"],))  # fmt: skip
    r = call(api, "worker", "POST", "/v1/tasks/release",
             {"leases": [ref2 | {"route": route2, "proxy_error": {
                 "kind": "connect", "excerpt": "refused"}}]})  # fmt: skip
    assert r.status_code == 200, r.text
    assert world.sql("SELECT max(consecutive_failures) FROM {e}.egresses") == [(0,)]


def test_api_gateway_fail_counts_the_attempt_and_keeps_the_lease(api, world):
    world.upsert("sites.watch", ["a.example"], "198.51.100.94")
    ref, route = _lease_one(api, ["a.example"])
    r = call(api, "worker", "POST", "/v1/tasks/fail",
             {"tasks": [ref | {"reason": "proxy gateway: 502", "route": route,
                               "proxy_error": {"kind": "gateway", "status": 502}}]})  # fmt: skip
    assert r.status_code == 200, r.text
    attempts, due_later = world.sql(
        "SELECT attempts, next_due_at > now() FROM {f}.tasks WHERE key = 'a.example'".replace(
            "{f}", schema.schema_name(world.env)))[0]  # fmt: skip
    assert attempts == 1 and due_later
    assert world.sql("SELECT kind, target FROM {e}.proxy_failures") == [
        ("gateway", "198.51.100.94")]  # fmt: skip
    assert world.sql("SELECT expires_at > now() FROM {e}.target_leases WHERE lease_id = %s",
                     (route["lease_id"],)) == [(True,)]  # fmt: skip


def test_an_account_without_a_limit_cant_be_enabled(world):
    run(lambda c: world.admin.add_account(c, "admin:t", "open-a", envs=[world.env]))
    with pytest.raises(Conflict, match="no limit"):
        run(lambda c: world.admin.account_status(c, "admin:t", "open-a", "enable"))
    # Once the usage API reported a limit, it can.
    world.sql("UPDATE {e}.proxy_accounts SET vendor_limit_bytes = 10 WHERE name = 'open-a'")
    run(lambda c: world.admin.account_status(c, "admin:t", "open-a", "enable"))
    # And an account in use can't drop its only limit.
    with pytest.raises(Conflict, match="no limit"):
        run(lambda c: world.admin.update_account(c, "admin:t", "isp-a", cap_gb=None))
