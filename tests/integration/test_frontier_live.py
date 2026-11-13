"""The frontier against a real Postgres: schema, upsert rules, leasing, ack/fail, sweeper."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg import AsyncConnection, sql

from leasehold.frontier import schema
from leasehold.frontier.store import (
    AckItem,
    FailItem,
    Frontier,
    LeaseRef,
    UnknownTaskType,
    UpsertItem,
)

from .pg import DSN, SPECS, fresh_env, requires_postgres

pytestmark = requires_postgres


@pytest.fixture
def env():
    with fresh_env() as e:
        yield e


def run(coro_fn):
    """Run ``coro_fn(conn)`` with a fresh autocommit async connection."""

    async def main():
        async with await AsyncConnection.connect(DSN, autocommit=True) as conn:
            return await coro_fn(conn)

    return asyncio.run(main())


def _sql(env, stmt, params=()):
    with psycopg.connect(DSN, autocommit=True) as conn:
        q = sql.SQL(stmt).format(s=sql.Identifier(schema.schema_name(env)))
        cur = conn.execute(q, params)
        return cur.fetchall() if cur.description else None


def _row(env, task_type, key):
    rows = _sql(
        env,
        "SELECT status, priority, attempts, next_due_at - now(), lease_id, last_error "
        "FROM {s}.tasks WHERE task_type = %s AND key = %s",
        (task_type, key),
    )
    return rows[0] if rows else None


# --- schema -----------------------------------------------------------------------------------


def test_migrate_is_idempotent(env):
    with psycopg.connect(DSN, autocommit=True) as conn:
        assert schema.migrate(conn, env) == []
        assert schema.current_version(conn, env) == schema.LATEST
        assert schema.diff_task_types(conn, env, SPECS).empty


def test_partitions_created(env):
    rows = _sql(
        env,
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace "
        "WHERE n.nspname = %s AND c.relkind IN ('r', 'p')",
        (schema.schema_name(env),),
    )
    names = sorted(r[0] for r in rows)
    # Three task types; jobs.repeat has 4 hash sub-partitions.
    assert len([n for n in names if "_h" not in n]) == 3
    assert len([n for n in names if "_h" in n]) == 4


def test_task_type_changes_and_undeclare(env):
    changed = [s.model_copy(deep=True) for s in SPECS[:1]]  # feeder removed
    tt = changed[0].frontier.task_types["jobs.once"]
    changed[0].frontier.task_types["jobs.once"] = tt.model_copy(update={"max_attempts": 9})
    with psycopg.connect(DSN, autocommit=True) as conn:
        d = schema.diff_task_types(conn, env, changed)
        actions = sorted((c.action, c.name) for c in d.changes)
        assert actions == [("undeclare", "feeder.item"), ("update", "jobs.once")]
        assert any("feeder.item" in w for w in d.warnings)
        schema.apply_task_types(conn, env, d)
        d2 = schema.diff_task_types(conn, env, SPECS)
        assert sorted((c.action, c.name) for c in d2.changes) == [
            ("redeclare", "feeder.item"),
            ("update", "jobs.once"),
        ]


# --- upsert -----------------------------------------------------------------------------------


def test_upsert_insert_then_update(env):
    f = Frontier(env)

    async def go(conn):
        r1 = await f.upsert(conn, "jobs.once", [UpsertItem("a"), UpsertItem("b")], "t")
        r2 = await f.upsert(conn, "jobs.once", [UpsertItem("a"), UpsertItem("c")], "t")
        return r1, r2

    r1, r2 = run(go)
    assert (r1.inserted, r1.updated) == (2, 0)
    assert (r2.inserted, r2.updated) == (1, 1)


def test_upsert_unknown_task_type(env):
    with pytest.raises(UnknownTaskType):
        run(lambda conn: Frontier(env).upsert(conn, "jobs.nope", [UpsertItem("a")], "t"))


def test_upsert_priority_only_raised_when_given(env):
    f = Frontier(env)

    async def go(conn):
        await f.upsert(conn, "jobs.once", [UpsertItem("a", priority=10)], "t")
        await f.upsert(conn, "jobs.once", [UpsertItem("a")], "t")  # no priority: unchanged
        await f.upsert(conn, "jobs.once", [UpsertItem("a", priority=5)], "t")  # lower: ignored
        await f.upsert(conn, "jobs.once", [UpsertItem("b")], "t")  # default priority

    run(go)
    assert _row(env, "jobs.once", "a")[1] == 10
    assert _row(env, "jobs.once", "b")[1] == 50
    run(lambda conn: f.upsert(conn, "jobs.once", [UpsertItem("a", priority=80)], "t"))
    assert _row(env, "jobs.once", "a")[1] == 80


def test_upsert_due_only_moves_earlier_when_given(env):
    f = Frontier(env)
    now = datetime.now(UTC)
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a", due_at=now + timedelta(hours=5))], "t"))
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a")], "t"))  # rediscovery: unchanged
    assert _row(env, "jobs.once", "a")[3] > timedelta(hours=4)
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a", due_at=now + timedelta(days=1))], "t"))
    assert _row(env, "jobs.once", "a")[3] < timedelta(hours=6)  # later: ignored
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a", due_at=now)], "t"))
    assert _row(env, "jobs.once", "a")[3] <= timedelta(0)  # earlier: applied


def test_upsert_duplicate_keys_in_one_batch(env):
    f = Frontier(env)
    items = [UpsertItem("a", priority=10), UpsertItem("a", priority=70), UpsertItem("a")]
    r = run(lambda c: f.upsert(c, "jobs.once", items, "t"))
    assert (r.inserted, r.updated) == (1, 0)
    assert _row(env, "jobs.once", "a")[1] == 70


def test_done_stays_done_unless_reopened(env):
    f = Frontier(env)

    async def go(conn):
        await f.upsert(conn, "jobs.once", [UpsertItem("a")], "t")
        (t,) = await f.lease(conn, ["jobs.once"], 10, "w")
        await f.ack(conn, [AckItem(LeaseRef("jobs.once", "a", t["lease_id"]), uuid.uuid4())])
        await f.upsert(conn, "jobs.once", [UpsertItem("a", due_at=datetime.now(UTC))], "t")

    run(go)
    assert _row(env, "jobs.once", "a")[0] == "done"
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a")], "t", reopen=True))
    status, _, attempts, due, _, _ = _row(env, "jobs.once", "a")
    assert (status, attempts) == ("pending", 0) and due <= timedelta(0)


# --- leasing ----------------------------------------------------------------------------------


def test_lease_order_and_due(env):
    f = Frontier(env)
    future = datetime.now(UTC) + timedelta(hours=1)
    items = [
        UpsertItem("low", priority=10),
        UpsertItem("high", priority=90),
        UpsertItem("mid", priority=50),
        UpsertItem("later", priority=100, due_at=future),
    ]
    run(lambda c: f.upsert(c, "jobs.once", items, "t"))
    leased = run(lambda c: f.lease(c, ["jobs.once"], 2, "w"))
    assert [t["key"] for t in leased] == ["high", "mid"]
    leased = run(lambda c: f.lease(c, ["jobs.once"], 10, "w"))
    assert [t["key"] for t in leased] == ["low"]  # 'later' isn't due


def test_lease_per_group(env):
    f = Frontier(env)
    # One big group listed first, two small ones, and tasks without a group.
    items = [UpsertItem(f"a{i}", group="a") for i in range(20)]
    items += [UpsertItem(f"b{i}", group="b") for i in range(3)]
    items += [UpsertItem("c0", priority=90, group="c")]
    items += [UpsertItem(f"n{i}") for i in range(2)]
    run(lambda c: f.upsert(c, "jobs.once", items, "t"))

    leased = run(lambda c: f.lease(c, ["jobs.once"], 50, "w", per_group=2))
    groups = sorted(t["group"] for t in leased)
    assert groups == ["", "", "a", "a", "b", "b", "c"]
    assert leased[0]["key"] == "c0"  # the best task overall comes first

    # Held tasks count against the group's room.
    leased = run(lambda c: f.lease(c, ["jobs.once"], 50, "w", per_group=3, held={"a": 2, "b": 3}))
    assert sorted(t["group"] for t in leased) == ["a"]

    # The limit applies after the per-group pick.
    assert len(run(lambda c: f.lease(c, ["jobs.once"], 2, "w", per_group=5))) == 2

    # An upsert without a group keeps the task's group.
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a19", priority=99)], "t"))
    assert _sql(env, "SELECT grp FROM {s}.tasks WHERE key = 'a19'") == [("a",)]


def test_concurrent_leases_never_overlap(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.repeat", [UpsertItem(str(i)) for i in range(400)], "t"))

    async def worker(name):
        got = []
        async with await AsyncConnection.connect(DSN, autocommit=True) as conn:
            while batch := await f.lease(conn, ["jobs.repeat"], 7, name):
                got.extend(t["key"] for t in batch)
        return got

    async def main():
        return await asyncio.gather(*(worker(f"w{i}") for i in range(8)))

    results = asyncio.run(main())
    keys = [k for r in results for k in r]
    assert len(keys) == 400 and len(set(keys)) == 400


def test_paused_task_type_is_not_leased_and_releases(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a"), UpsertItem("b")], "t"))
    (t,) = run(lambda c: f.lease(c, ["jobs.once"], 1, "w"))
    released = run(lambda c: f.set_paused(c, "jobs.once", True, "test"))
    assert released == 1
    assert run(lambda c: f.lease(c, ["jobs.once"], 10, "w")) == []
    ref = LeaseRef("jobs.once", t["key"], t["lease_id"])
    renewed, lost = run(lambda c: f.renew(c, [ref]))
    assert renewed == [] and lost == [ref]
    # Work that finished anyway is still recorded, without an attempt counted.
    assert run(lambda c: f.ack(c, [AckItem(ref, uuid.uuid4())]))[0] == 1
    run(lambda c: f.set_paused(c, "jobs.once", False))
    assert len(run(lambda c: f.lease(c, ["jobs.once"], 10, "w"))) == 1


def test_renew_extends_live_leases_only(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a"), UpsertItem("b")], "t"))
    leased = run(lambda c: f.lease(c, ["jobs.once"], 2, "w"))
    refs = [LeaseRef("jobs.once", t["key"], t["lease_id"]) for t in leased]
    _sql(env, "UPDATE {s}.tasks SET lease_expires_at = now() - interval '1s' WHERE key = 'b'")
    bogus = LeaseRef("jobs.once", "a", uuid.uuid4())
    renewed, lost = run(lambda c: f.renew(c, refs + [bogus]))
    assert [r["key"] for r in renewed] == ["a"]
    assert sorted(r.key for r in lost) == ["a", "b"] and bogus in lost


# --- ack / fail / sweep -----------------------------------------------------------------------


def test_ack_recurring_reschedules(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.repeat", [UpsertItem("a")], "t"))
    (t,) = run(lambda c: f.lease(c, ["jobs.repeat"], 1, "w"))
    ref = LeaseRef("jobs.repeat", "a", t["lease_id"])
    n, unknown = run(lambda c: f.ack(c, [AckItem(ref, uuid.uuid4())]))
    assert (n, unknown) == (1, [])
    status, _, attempts, due, lease_id, _ = _row(env, "jobs.repeat", "a")
    assert status == "pending" and attempts == 0 and lease_id is None
    assert timedelta(hours=23) < due <= timedelta(days=1)
    # The same lease can't be acked twice.
    assert run(lambda c: f.ack(c, [AckItem(ref, uuid.uuid4())])) == (0, [ref])


def test_fail_backoff_then_dead(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a")], "t"))
    expected = [timedelta(minutes=1), timedelta(minutes=2)]
    for i, backoff in enumerate(expected):
        if i:
            _sql(env, "UPDATE {s}.tasks SET next_due_at = now()")
        (t,) = run(lambda c: f.lease(c, ["jobs.once"], 1, "w"))
        ref = LeaseRef("jobs.once", "a", t["lease_id"])
        assert run(lambda c, ref=ref: f.fail(c, [FailItem(ref, "boom")]))[0] == 1
        status, _, attempts, due, _, err = _row(env, "jobs.once", "a")
        assert (status, attempts, err) == ("pending", i + 1, "boom")
        assert backoff - timedelta(seconds=5) < due <= backoff
    _sql(env, "UPDATE {s}.tasks SET next_due_at = now()")
    (t,) = run(lambda c: f.lease(c, ["jobs.once"], 1, "w"))
    run(lambda c: f.fail(c, [FailItem(LeaseRef("jobs.once", "a", t["lease_id"]), "boom")]))
    assert _row(env, "jobs.once", "a")[:3] == ("dead", 50, 3)


def test_fail_permanent_and_retry_after(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a"), UpsertItem("b")], "t"))
    leased = {t["key"]: t for t in run(lambda c: f.lease(c, ["jobs.once"], 2, "w"))}
    items = [
        FailItem(LeaseRef("jobs.once", "a", leased["a"]["lease_id"]), "gone", permanent=True),
        FailItem(
            LeaseRef("jobs.once", "b", leased["b"]["lease_id"]),
            "rate limited",
            retry_after=timedelta(hours=2),
        ),
    ]
    assert run(lambda c: f.fail(c, items))[0] == 2
    assert _row(env, "jobs.once", "a")[0] == "dead"
    assert timedelta(minutes=119) < _row(env, "jobs.once", "b")[3] <= timedelta(hours=2)


def test_sweep_counts_attempt_and_allows_late_ack(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a"), UpsertItem("b")], "t"))
    leased = {t["key"]: t for t in run(lambda c: f.lease(c, ["jobs.once"], 2, "w"))}
    _sql(env, "UPDATE {s}.tasks SET lease_expires_at = now() - interval '1s'")
    assert run(lambda c: f.sweep(c)) == 2
    status, _, attempts, _, _, err = _row(env, "jobs.once", "a")
    assert (status, attempts, err) == ("pending", 1, "lease expired")
    # A late ack for a swept, not re-leased task is accepted.
    ref = LeaseRef("jobs.once", "a", leased["a"]["lease_id"])
    assert run(lambda c: f.ack(c, [AckItem(ref, uuid.uuid4())]))[0] == 1
    assert _row(env, "jobs.once", "a")[0] == "done"
    # A late fail is not: the sweeper already counted the attempt.
    ref_b = LeaseRef("jobs.once", "b", leased["b"]["lease_id"])
    assert run(lambda c: f.fail(c, [FailItem(ref_b, "x")])) == (0, [ref_b])


def test_release_keeps_due_time_and_attempts(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.once", [UpsertItem("a")], "t"))
    (t,) = run(lambda c: f.lease(c, ["jobs.once"], 1, "w"))
    ref = LeaseRef("jobs.once", "a", t["lease_id"])
    assert run(lambda c: f.release(c, [ref]))[0] == 1
    assert _row(env, "jobs.once", "a")[:3] == ("pending", 50, 0)
    assert len(run(lambda c: f.lease(c, ["jobs.once"], 1, "w"))) == 1


def test_stats(env):
    f = Frontier(env)
    run(lambda c: f.upsert(c, "jobs.repeat", [UpsertItem(str(i)) for i in range(30)], "t"))
    run(lambda c: f.lease(c, ["jobs.repeat"], 5, "w"))
    _sql(env, "ANALYZE {s}.tasks")
    stats = {s.task_type: s for s in run(lambda c: f.stats(c, eligible_cap=20))}
    s = stats["jobs.repeat"]
    assert (s.eligible, s.eligible_capped, s.leased, s.dead) == (20, True, 5, 0)
    assert s.rows_estimate == 30
    assert stats["jobs.once"].eligible == 0
