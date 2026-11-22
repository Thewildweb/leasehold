"""Target leases against a real Postgres: exclusivity across holders and environments,
cooldowns, pair budget, cap, revocation, release, and tasks without a group."""

from __future__ import annotations

import asyncio
import secrets
import textwrap
from datetime import timedelta

import psycopg
import pytest
from psycopg import AsyncConnection, sql

from leasehold.egress import config
from leasehold.egress import schema as egress_schema
from leasehold.egress.dispatch import Cooling, Dispatcher, EgressRefused
from leasehold.egress.planner import Planner, publish_demand
from leasehold.frontier import schema
from leasehold.frontier.store import Frontier, UpsertItem
from leasehold.spec import ServiceSpec

from .pg import DSN, requires_postgres

pytestmark = requires_postgres

SPEC = ServiceSpec.model_validate(
    {
        "spec_version": 1,
        "name": "sites",
        "description": "test service",
        "owner": "test",
        "components": {
            "worker": {
                "kind": "task-worker",
                "consumes_tasks": ["sites.fetch", "sites.other"],
                "node_class": "processing",
                "resources": {"cpu": 100, "memory": 64},
                "count": {"min": 1, "max": 1},
            },
        },
        "frontier": {
            "task_types": {
                "sites.fetch": {"key": "domain", "lease_duration": "30s", "target_leases": True},
                "sites.other": {"key": "domain", "target_leases": True},
            }
        },
    }
)

FILES = {
    "registry.yml": """
        defaults: { asn: 64500 }
        egresses:
          - { address: 192.0.2.1 }
          - { address: 192.0.2.2 }
          - { address: 192.0.2.3, status: disabled }
          - { address: 192.0.2.4, task_types: [sites.other] }
        """,
    "providers.yml": """
        providers:
          example-cdn: { kind: cdn, cname: [cdn.example.net] }
        """,
    "budgets.yml": """
        providers: { default: 30 }
        targets: { interval: 10s, per_kind: { cdn: 15s } }
        pair: { per_hour: 4 }
        """,
}


def run(fn):
    async def main():
        async with await AsyncConnection.connect(DSN, autocommit=True) as conn:
            return await fn(conn)

    return asyncio.run(main())


@pytest.fixture
def setup(tmp_path):
    """Two environments' frontiers and one egress schema."""
    tag = secrets.token_hex(4)
    envs = [f"it{tag}a", f"it{tag}b"]
    eg = f"it{tag}_egress"
    for name, text in FILES.items():
        (tmp_path / name).write_text(textwrap.dedent(text))
    with psycopg.connect(DSN, autocommit=True) as conn:
        for env in envs:
            schema.migrate(conn, env)
            schema.apply_task_types(conn, env, schema.diff_task_types(conn, env, [SPEC]))
        egress_schema.migrate(conn, eg)
        cfg = config.load(tmp_path)
        egress_schema.apply(conn, egress_schema.diff(conn, cfg, eg), eg)
        plan_now(eg)
        try:
            yield envs, eg
        finally:
            for s in [schema.schema_name(e) for e in envs] + [eg]:
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(s)))


def plan_now(eg):
    """Allocations for the current window (the planner plans the next one)."""
    start = run(lambda c: _window_start(c))
    return run(lambda c: Planner(eg).plan(c, start))


async def _window_start(conn):
    cur = await conn.execute("SELECT to_timestamp(floor(extract(epoch FROM now()) / 60) * 60)")
    return (await cur.fetchone())[0]


def upsert(env, items, task_type="sites.fetch"):
    run(lambda c: Frontier(env).upsert(c, task_type, items, "t"))


def dispatcher(env, eg):
    return Dispatcher(env, schema.schema_name(env), eg)


def lease(d, egress, holder, limit=50, types=("sites.fetch",)):
    return run(lambda c: d.lease(c, list(types), limit, egress, holder))


def egress_sql(eg, stmt, params=()):
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL(stmt).format(e=sql.Identifier(eg)), params)


def test_refused_egresses(setup):
    (env, _), eg = setup
    d = dispatcher(env, eg)
    with pytest.raises(EgressRefused, match="not in the registry"):
        lease(d, "198.51.100.1", "w")
    with pytest.raises(EgressRefused, match="disabled"):
        lease(d, "192.0.2.3", "w")
    with pytest.raises(EgressRefused, match="may not lease"):
        lease(d, "192.0.2.4", "w")


def test_grant_cap_and_exclusivity_across_holders_and_environments(setup):
    (env_a, env_b), eg = setup
    items = [UpsertItem(f"a{i}.example", group="192.0.2.100") for i in range(5)]
    items += [UpsertItem("cdn.example", group="cdn.example", provider="example-cdn")]
    upsert(env_a, items)
    upsert(env_b, [UpsertItem("a9.example", group="192.0.2.100")])
    d_a, d_b = dispatcher(env_a, eg), dispatcher(env_b, eg)

    g = lease(d_a, "192.0.2.1", "w1")
    targets = {t.target: t for t in g.targets}
    # cap = lease_duration / interval: 30s / 10s = 3 for the IP target; 30s / 15s = 2 for
    # the CDN domain, of which it has one task.
    assert targets["192.0.2.100"].cap == 3 and targets["192.0.2.100"].interval_s == 10
    assert targets["cdn.example"].interval_s == 15 and targets["cdn.example"].granted == 1
    assert sorted(t["target"] for t in g.tasks) == ["192.0.2.100"] * 3 + ["cdn.example"]
    assert all(t["target_lease_id"] == targets[t["target"]].lease_id for t in g.tasks)

    # Another holder, another egress, another environment: none of them gets the target.
    assert lease(d_a, "192.0.2.2", "w2").tasks == []
    assert lease(d_a, "192.0.2.1", "w2").tasks == []
    assert lease(d_b, "192.0.2.2", "w3").tasks == []
    # The holder itself is at its cap.
    assert lease(d_a, "192.0.2.1", "w1").tasks == []

    # Expired: anyone may take it over (here the other environment).
    egress_sql(eg, "UPDATE {e}.target_leases SET expires_at = now() - interval '1 second'")
    g = lease(d_b, "192.0.2.2", "w3")
    assert [t["key"] for t in g.tasks] == ["a9.example"]
    assert lease(d_a, "192.0.2.1", "w1").tasks == []


def test_task_without_group_is_still_limited(setup):
    """No group and no provider: the key is the target, with its own lease and cap."""
    (env, _), eg = setup
    upsert(env, [UpsertItem("plain.example")])
    d = dispatcher(env, eg)
    g = lease(d, "192.0.2.1", "w1")
    assert [(t["key"], t["target"], t["provider"]) for t in g.tasks] == [
        ("plain.example", "plain.example", "")
    ]
    assert [t.target for t in g.targets] == ["plain.example"]
    run(lambda c: Frontier(env).release(c, [_ref(g.tasks[0])]))
    assert lease(d, "192.0.2.2", "w2").tasks == []  # held by w1


def _ref(t):
    from leasehold.frontier.store import LeaseRef

    return LeaseRef(t["task_type"], t["key"], t["lease_id"])


def test_cooldown_stops_every_egress_and_revokes_the_holder(setup):
    (env, _), eg = setup
    upsert(env, [UpsertItem(f"c{i}.example", group="192.0.2.50") for i in range(5)])
    d = dispatcher(env, eg)
    g = lease(d, "192.0.2.1", "w1", limit=1)
    (tl,) = g.targets
    n = run(lambda c: d.cool(c, [Cooling("192.0.2.50", timedelta(minutes=10), "429")],
                             "192.0.2.1", timedelta(hours=6)))  # fmt: skip
    assert n == 1
    ok, lost = run(lambda c: d.renew(c, "192.0.2.1", "w1", [tl.lease_id]))
    assert ok == {} and lost == {tl.lease_id: "target cooling down: 429"}
    egress_sql(eg, "UPDATE {e}.target_leases SET expires_at = now() - interval '1 second'")
    assert lease(d, "192.0.2.2", "w2").tasks == []  # no rotation to a fresh address
    # A shorter cooldown doesn't shorten a longer one; the cap applies.
    run(lambda c: d.cool(c, [Cooling("192.0.2.50", timedelta(seconds=1), "x")], None,
                         timedelta(hours=6)))  # fmt: skip
    assert lease(d, "192.0.2.2", "w2").tasks == []
    egress_sql(eg, "UPDATE {e}.target_cooldowns SET until = now() - interval '1 second'")
    assert len(lease(d, "192.0.2.2", "w2").tasks) == 3


def test_pair_budget_moves_the_target_to_another_egress(setup):
    (env, _), eg = setup
    upsert(env, [UpsertItem(f"p{i}.example", group="192.0.2.60") for i in range(9)])
    d = dispatcher(env, eg)
    assert len(lease(d, "192.0.2.1", "w1").tasks) == 3
    egress_sql(eg, "UPDATE {e}.target_leases SET expires_at = now() - interval '1 second'")
    assert len(lease(d, "192.0.2.1", "w1").tasks) == 1  # per_hour 4
    egress_sql(eg, "UPDATE {e}.target_leases SET expires_at = now() - interval '1 second'")
    assert lease(d, "192.0.2.1", "w1").tasks == []
    assert len(lease(d, "192.0.2.2", "w2").tasks) == 3


def test_revoke_renew_and_release(setup):
    (env, _), eg = setup
    upsert(env, [UpsertItem("r1.example", group="192.0.2.70"),
                 UpsertItem("r2.example", group="192.0.2.71")])  # fmt: skip
    d = dispatcher(env, eg)
    g = lease(d, "192.0.2.1", "w1")
    ids = {t.target: t.lease_id for t in g.targets}
    ok, lost = run(lambda c: d.renew(c, "192.0.2.1", "w1", list(ids.values())))
    assert set(ok) == set(ids.values()) and lost == {}
    # Only the holder renews.
    ok, lost = run(lambda c: d.renew(c, "192.0.2.2", "w1", [ids["192.0.2.70"]]))
    assert ok == {} and lost == {ids["192.0.2.70"]: "not held"}

    assert run(lambda c: d.revoke(c, "192.0.2.70", "operator"))
    ok, lost = run(lambda c: d.renew(c, "192.0.2.1", "w1", [ids["192.0.2.70"]]))
    assert lost == {ids["192.0.2.70"]: "revoked: operator"}

    # Released: free for another egress at once.
    run(lambda c: d.renew(c, "192.0.2.1", "w1", [], release=[ids["192.0.2.71"]]))
    run(lambda c: Frontier(env).release(c, [_ref(t) for t in g.tasks]))
    g2 = lease(d, "192.0.2.2", "w2")
    assert [t["target"] for t in g2.tasks] == ["192.0.2.71"]  # .70 is revoked until it expires


def test_sweep(setup):
    (env, _), eg = setup
    upsert(env, [UpsertItem("s.example", group="192.0.2.80")])
    d = dispatcher(env, eg)
    lease(d, "192.0.2.1", "w1")
    egress_sql(eg, "UPDATE {e}.target_leases SET expires_at = now() - interval '2 hours'")
    assert run(lambda c: d.sweep(c)) == 1


# --- through the API --------------------------------------------------------------------------


@pytest.fixture
def api(setup, tmp_path):
    import json

    from fastapi.testclient import TestClient

    from leasehold.api.app import create_app
    from leasehold.api.auth import Registry, hash_token, new_token
    from leasehold.api.settings import Settings

    (env, _), eg = setup
    tokens = {"worker": new_token(), "admin": new_token(), "agent": new_token()}
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps(
            {
                "tokens": [
                    {"sha256": hash_token(tokens["worker"]), "service": "sites",
                     "component": "worker"},
                    {"sha256": hash_token(tokens["admin"]), "admin": "x"},
                    {"sha256": hash_token(tokens["agent"]), "agent": "egress-agent"},
                ]
            }
        )
    )  # fmt: skip
    settings = Settings(
        env=env, dsn=DSN, services=(), tokens_file=path, sweep_interval=3600,
        stats_interval=3600, egress_schema=eg, egress_owner=True, plan_interval=3600,
    )  # fmt: skip
    with TestClient(create_app(settings, registry=Registry([SPEC]))) as client:
        client.tokens = tokens
        client.env = env
        yield client


def post(api, role, path, body):
    return api.post(path, json=body, headers={"Authorization": f"Bearer {api.tokens[role]}"})


def test_api_target_path(api):
    upsert(api.env, [UpsertItem(f"x{i}.example", group="192.0.2.90") for i in range(2)])
    # Fail closed: no egress, or an unknown one.
    r = post(api, "worker", "/v1/tasks/lease", {"max": 5})
    assert r.status_code == 400 and "egress" in r.json()["detail"]
    r = post(api, "worker", "/v1/tasks/lease", {"max": 5, "egress": "198.51.100.1"})
    assert r.status_code == 403

    r = post(api, "worker", "/v1/tasks/lease", {"max": 5, "egress": "192.0.2.1",
                                                "worker_id": "w1"})  # fmt: skip
    assert r.status_code == 200, r.text
    body = r.json()
    (tl,) = body["targets"]
    assert tl["target"] == "192.0.2.90" and tl["interval_s"] == 10
    assert [t["target_lease_id"] for t in body["tasks"]] == [tl["lease_id"]] * 2

    r = post(api, "worker", "/v1/targets/renew",
             {"egress": "192.0.2.1", "worker_id": "w1", "leases": [tl["lease_id"]]})  # fmt: skip
    assert r.status_code == 200 and r.json()["lost"] == []

    # Pushback reported with a release: the target cools down for every egress.
    t0 = body["tasks"][0]
    ref = {k: t0[k] for k in ("task_type", "key", "lease_id")}
    r = post(api, "worker", "/v1/tasks/release",
             {"egress": "192.0.2.1",
              "leases": [ref | {"after_s": 300, "cooldown": True, "reason": "429"}]})  # fmt: skip
    assert r.status_code == 200 and r.json()["applied"] == 1
    r = post(api, "worker", "/v1/targets/renew",
             {"egress": "192.0.2.1", "worker_id": "w1", "leases": [tl["lease_id"]]})  # fmt: skip
    assert r.json()["lost"] == [{"lease_id": tl["lease_id"], "reason": "target cooling down: 429"}]

    # Revoke is for admins.
    assert post(api, "worker", "/v1/targets/192.0.2.90/revoke", {"reason": "x"}).status_code == 403
    r = post(api, "admin", "/v1/targets/192.0.2.90/revoke", {"reason": "operator"})
    assert r.status_code == 200 and r.json() == {"revoked": True}


# --- budgets ----------------------------------------------------------------------------------


def tokens(eg, egress="192.0.2.1", provider="example-cdn"):
    with psycopg.connect(DSN, autocommit=True) as conn:
        return conn.execute(
            sql.SQL(
                "SELECT class, tokens, used FROM {}.allocations WHERE egress = %s "
                "AND provider = %s AND window_start <= now() AND window_end > now() "
                "ORDER BY class"
            ).format(sql.Identifier(eg)),
            (egress, provider),
        ).fetchall()


def test_tokens_are_spent_and_run_out(setup):
    (env, _), eg = setup
    # No demand was published, so all of the window's budget is in class '*'.
    assert tokens(eg) == [("*", 30, 0)]
    # 40 CDN domains (one task each) and 2 plain-hosting tasks.
    items = [UpsertItem(f"d{i}.example", group=f"d{i}.example", provider="example-cdn")
             for i in range(40)]  # fmt: skip
    items += [UpsertItem(f"h{i}.example", group="192.0.2.200") for i in range(2)]
    upsert(env, items)
    d = dispatcher(env, eg)
    g = lease(d, "192.0.2.1", "w1", limit=100)
    by_provider = {}
    for t in g.tasks:
        by_provider[t["provider"]] = by_provider.get(t["provider"], 0) + 1
    # The CDN gets its 30 tokens; plain hosting has no provider budget.
    assert by_provider == {"example-cdn": 30, "": 2}
    assert tokens(eg) == [("*", 30, 30)]
    # Out of tokens: no more CDN work on this egress, another egress has its own.
    assert lease(d, "192.0.2.1", "w1", limit=100).tasks == []
    assert len(lease(d, "192.0.2.2", "w2", limit=100).tasks) == 10


def test_no_plan_closes_budgeted_providers(setup):
    (env, _), eg = setup
    egress_sql(eg, "DELETE FROM {e}.allocations")
    upsert(env, [UpsertItem("c.example", group="c.example", provider="example-cdn"),
                 UpsertItem("h.example", group="192.0.2.201")])  # fmt: skip
    g = lease(dispatcher(env, eg), "192.0.2.1", "w1")
    assert [t["provider"] for t in g.tasks] == [""]


def test_demand_shapes_the_plan(setup):
    (env, _), eg = setup
    upsert(env, [UpsertItem(f"n{i}.example", group=f"n{i}.example", provider="example-cdn")
                 for i in range(300)])  # fmt: skip
    n = run(lambda c: publish_demand(c, env, schema.schema_name(env), {"sites.fetch": "normal"},
                                     timedelta(minutes=15), eg))  # fmt: skip
    assert n == 1
    egress_sql(eg, "DELETE FROM {e}.allocations")
    plan_now(eg)
    # 300 due over 15 windows: 20 a window for 'normal'; three active egresses of 30 a
    # window (192.0.2.4 is active, with an allowlist); the rest is '*'.
    rows = tokens(eg) + tokens(eg, "192.0.2.2") + tokens(eg, "192.0.2.4")
    assert sum(r[1] for r in rows if r[0] == "normal") == 20
    assert sum(r[1] for r in rows) == 90
    # A dev task of class 'critical' counts as 'normal' and may use '*' after it.
    d = dispatcher(env, eg)
    g = run(lambda c: d.lease(c, ["sites.fetch"], 100, "192.0.2.1", "w1",
                              {"sites.fetch": "critical"}))  # fmt: skip
    assert len(g.tasks) == 30
    assert all(used == tok for _, tok, used in tokens(eg))


def test_api_plans_the_next_window_and_publishes_demand(api):
    import asyncio as aio

    upsert(api.env, [UpsertItem("q.example", group="q.example", provider="example-cdn")])
    aio.run(api.app.state.plan_once())
    eg = api.app.state.settings.egress_schema
    with psycopg.connect(DSN, autocommit=True) as conn:
        demand = conn.execute(
            sql.SQL("SELECT env, provider, class, due FROM {}.demand").format(sql.Identifier(eg))
        ).fetchall()
        windows = conn.execute(
            sql.SQL("SELECT count(DISTINCT window_start) FROM {}.allocations").format(
                sql.Identifier(eg)
            )
        ).fetchone()[0]
    assert demand == [(api.env, "example-cdn", "bulk", 1)]  # 'normal' in a non-prod env
    # The fixture's window and the one after this call's minute (the API's own loop
    # waits an interval before its first run).
    assert windows == 2


# --- through the machine's agent --------------------------------------------------------------


class _SyncBridge:
    """An async httpx transport that sends through the control API's TestClient."""

    def __init__(self, client):
        import httpx

        self.client = client
        self.httpx = httpx

    async def handle_async_request(self, request):
        import asyncio as aio

        r = await aio.to_thread(
            self.client.request,
            request.method,
            request.url.path,
            content=request.content,
            headers={k: v for k, v in request.headers.items() if k != "host"},
        )
        return self.httpx.Response(r.status_code, content=r.content, headers=r.headers)

    async def aclose(self):
        pass


def test_through_the_agent(api, monkeypatch):
    from fastapi.testclient import TestClient

    from leasehold.agent import server
    from leasehold.agent.server import AgentSettings, create_app

    monkeypatch.setattr(server, "RENEW_EVERY_S", 3600)
    upsert(api.env, [UpsertItem("ag1.example", group="ag1.example", provider="example-cdn"),
                     UpsertItem("ag2.example", group="192.0.2.95")])  # fmt: skip
    settings = AgentSettings("http://api", api.tokens["agent"], "192.0.2.1", "/unused")
    with TestClient(create_app(settings, transport=_SyncBridge(api))) as agent:
        worker = {"Authorization": f"Bearer {api.tokens['worker']}"}
        r = agent.post("/v1/tasks/lease", json={"max": 5, "worker_id": "w1"}, headers=worker)
        assert r.status_code == 200, r.text
        body = r.json()
        assert sorted(t["key"] for t in body["tasks"]) == ["ag1.example", "ag2.example"]
        eg = api.app.state.settings.egress_schema
        with psycopg.connect(DSN, autocommit=True) as conn:
            holders = conn.execute(
                sql.SQL("SELECT DISTINCT holder FROM {}.target_leases").format(sql.Identifier(eg))
            ).fetchall()
            leased_by = conn.execute(
                sql.SQL("SELECT DISTINCT leased_by FROM {}.tasks WHERE status = 'leased'").format(
                    sql.Identifier(schema.schema_name(api.env))
                )
            ).fetchall()
        # The agent holds the targets; the tasks are the worker's.
        assert holders == [("agent:egress-agent@192.0.2.1",)]
        assert leased_by == [("sites.worker:w1",)]

        # The worker itself may not spend; the agent may, for extra requests.
        tl = {t["target"]: t["lease_id"] for t in body["targets"]}["ag1.example"]
        assert agent.post("/v1/agent/token", json={"target_lease_id": tl, "task_type":
                          "sites.fetch", "first": True}).json()["granted"]  # fmt: skip
        r = post(api, "worker", "/v1/targets/spend",
                 {"egress": "192.0.2.1", "provider": "example-cdn", "task_type": "sites.fetch",
                  "n": 1})  # fmt: skip
        assert r.status_code == 403
        r = post(api, "agent", "/v1/targets/spend",
                 {"egress": "192.0.2.1", "provider": "example-cdn", "task_type": "sites.fetch",
                  "n": 2})  # fmt: skip
        assert r.json() == {"granted": 2}

        # Renewal by the agent, as the holder.
        asyncio.run(agent.app.state.agent.maintain())  # nothing due yet
        r = post(api, "agent", "/v1/targets/renew", {"egress": "192.0.2.1", "leases": [tl]})
        assert r.json()["lost"] == []

        # Only agents call for others.
        r = api.post("/v1/tasks/lease", json={"max": 1, "egress": "192.0.2.1"},
                     headers={"Authorization": f"Bearer {api.tokens['worker']}",
                              "X-Leasehold-For": f"Bearer {api.tokens['admin']}"})  # fmt: skip
        assert r.status_code == 403


def test_lending_only_late_in_the_window(setup):
    (env, _), eg = setup
    egress_sql(eg, "DELETE FROM {e}.allocations")
    upsert(env, [UpsertItem(f"l{i}.example", group=f"l{i}.example", provider="example-cdn")
                 for i in range(20)])  # fmt: skip
    d = dispatcher(env, eg)

    def window(started_s, used):
        egress_sql(eg, "DELETE FROM {e}.allocations")
        egress_sql(
            eg,
            "INSERT INTO {e}.allocations (window_start, window_end, egress, provider, class, "
            "tokens, used) VALUES (now() - %s * interval '1 second', "
            "now() + (60 - %s) * interval '1 second', '192.0.2.1', 'example-cdn', "
            "'critical', 10, %s)",
            (started_s, started_s, used),
        )

    def lease_normal():
        return run(lambda c: d.lease(c, ["sites.fetch"], 50, "192.0.2.1", "w1",
                                     {"sites.fetch": "bulk"}))  # fmt: skip

    window(10, 0)  # before lend_after (30 s): nothing is lent
    assert lease_normal().tasks == []
    # 40 s in, 'critical' used 4 so far (0.1/s): it keeps 2 for the last 20 s, lends 4.
    window(40, 4)
    assert len(lease_normal().tasks) == 4
    assert tokens(eg) == [("critical", 10, 8)]
