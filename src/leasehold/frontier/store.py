"""Frontier operations: upsert, lease, renew, ack, fail, release, sweep, stats.

All times come from the database clock (``now()``), never from the caller.

Lease rules:

- ``lease`` takes due, pending tasks, highest priority first, with
  ``FOR UPDATE SKIP LOCKED``, and gives each one a new ``lease_id``. Every later
  call about that task must present the same ``lease_id``.
- ``renew`` only works while the lease hasn't expired. A lease that isn't renewed
  is lost, and the worker must stop working on it.
- ``ack`` is also accepted after the lease expired and was swept, or after its task
  type was paused, as long as the task hasn't been leased again: the work was done,
  so it's recorded.
- ``fail`` and ``release`` need a live lease.
- The sweeper puts expired leases back to pending and counts them as a failed
  attempt, so a task that crashes its worker every time ends up ``dead``.
- Pausing a task type releases its leases without counting an attempt.

Upsert rules, for a key that already exists:

- ``priority`` is raised to the given priority, never lowered. Without a priority
  it's left alone.
- ``due_at`` moves a pending task earlier, never later. Without ``due_at`` the due
  time is left alone, so rediscovering a task doesn't reset its backoff or its
  recrawl schedule.
- ``done`` and ``dead`` tasks stay as they are, unless ``reopen`` is set.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from leasehold.frontier.schema import schema_name


class UnknownTaskType(LookupError):
    pass


@dataclass(frozen=True)
class UpsertItem:
    key: str
    priority: int | None = None
    due_at: datetime | None = None
    payload: Any = None
    country: str | None = None


@dataclass(frozen=True)
class LeaseRef:
    task_type: str
    key: str
    lease_id: UUID


@dataclass(frozen=True)
class AckItem:
    ref: LeaseRef
    event_id: UUID


@dataclass(frozen=True)
class FailItem:
    ref: LeaseRef
    reason: str
    event_id: UUID | None = None
    retry_after: timedelta | None = None
    permanent: bool = False


@dataclass(frozen=True)
class UpsertResult:
    inserted: int
    updated: int


@dataclass(frozen=True)
class Stats:
    task_type: str
    paused: bool
    declared: bool
    eligible: int
    eligible_capped: bool
    leased: int
    dead: int
    rows_estimate: int


def _dedupe(items: list[UpsertItem]) -> list[UpsertItem]:
    """Merge duplicate keys within one batch (Postgres can't update a row twice in one
    statement): highest priority, earliest due time, last payload and country."""
    merged: dict[str, UpsertItem] = {}
    for it in items:
        prev = merged.get(it.key)
        if prev is None:
            merged[it.key] = it
            continue
        prios = [p for p in (prev.priority, it.priority) if p is not None]
        dues = [d for d in (prev.due_at, it.due_at) if d is not None]
        merged[it.key] = UpsertItem(
            key=it.key,
            priority=max(prios) if prios else None,
            due_at=min(dues) if dues else None,
            payload=it.payload if it.payload is not None else prev.payload,
            country=it.country if it.country is not None else prev.country,
        )
    return list(merged.values())


def _refs_json(refs: list[LeaseRef]) -> str:
    return json.dumps(
        [{"task_type": r.task_type, "key": r.key, "lease_id": str(r.lease_id)} for r in refs]
    )


def _ref(row: dict) -> LeaseRef:
    return LeaseRef(row["task_type"], row["key"], row["lease_id"])


class Frontier:
    """Frontier operations for one environment. Pass an open async connection to each
    call; every call runs in its own transaction."""

    def __init__(self, env: str):
        self.env = env
        self.schema = schema_name(env)
        self._s = sql.Identifier(self.schema)

    def _q(self, stmt: str) -> sql.Composed:
        return sql.SQL(stmt).format(s=self._s)

    # --- task types ---------------------------------------------------------------------------

    async def task_types(self, conn: AsyncConnection) -> dict[str, dict]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(self._q("SELECT * FROM {s}.task_types"))
            return {r["name"]: r async for r in cur}

    async def set_paused(
        self, conn: AsyncConnection, task_type: str, paused: bool, reason: str | None = None
    ) -> int:
        """Pause or resume a task type. Pausing releases its leases (no attempt counted).
        Returns the number of leases released."""
        async with conn.transaction():
            cur = await conn.execute(
                self._q(
                    "UPDATE {s}.task_types SET paused = %s, paused_reason = %s, "
                    "updated_at = now() WHERE name = %s"
                ),
                (paused, reason if paused else None, task_type),
            )
            if cur.rowcount == 0:
                raise UnknownTaskType(task_type)
            if not paused:
                return 0
            cur = await conn.execute(
                self._q(
                    "UPDATE {s}.tasks SET status = 'pending', leased_by = NULL, "
                    "lease_expires_at = NULL WHERE task_type = %s AND status = 'leased'"
                ),
                (task_type,),
            )
            return cur.rowcount

    # --- upsert -------------------------------------------------------------------------------

    async def upsert(
        self,
        conn: AsyncConnection,
        task_type: str,
        items: list[UpsertItem],
        created_by: str,
        reopen: bool = False,
    ) -> UpsertResult:
        items = _dedupe(items)
        inserted = updated = 0
        async with conn.transaction():
            cur = await conn.execute(
                self._q("SELECT default_priority FROM {s}.task_types WHERE name = %s"),
                (task_type,),
            )
            row = await cur.fetchone()
            if row is None:
                raise UnknownTaskType(task_type)
            default_priority = row[0]
            # Group by which fields were given: each group needs a different ON CONFLICT.
            groups: dict[tuple[bool, bool], list[UpsertItem]] = {}
            for it in items:
                groups.setdefault((it.priority is not None, it.due_at is not None), []).append(it)
            for (has_prio, has_due), group in sorted(groups.items()):
                ins, upd = await self._upsert_group(
                    conn,
                    task_type,
                    group,
                    created_by,
                    reopen,
                    default_priority,
                    has_prio,
                    has_due,
                )
                inserted += ins
                updated += upd
        return UpsertResult(inserted, updated)

    async def _upsert_group(
        self, conn, task_type, items, created_by, reopen, default_priority, has_prio, has_due
    ) -> tuple[int, int]:
        reopening = "(%(reopen)s AND t.status IN ('done', 'dead'))"
        priority = "GREATEST(t.priority, EXCLUDED.priority)" if has_prio else "t.priority"
        if has_due:
            due = (
                f"CASE WHEN {reopening} THEN EXCLUDED.next_due_at "
                "WHEN t.status = 'pending' THEN LEAST(t.next_due_at, EXCLUDED.next_due_at) "
                "ELSE t.next_due_at END"
            )
        else:
            due = f"CASE WHEN {reopening} THEN now() ELSE t.next_due_at END"
        stmt = f"""
            INSERT INTO {{s}}.tasks AS t
              (task_type, key, priority, status, next_due_at, payload, country, created_by)
            SELECT %(task_type)s, i.key, COALESCE(i.priority, %(default_priority)s), 'pending',
                   COALESCE(i.due_at, now()), i.payload, i.country, %(created_by)s
            FROM jsonb_to_recordset(%(items)s::jsonb)
              AS i(key text, priority smallint, due_at timestamptz, payload jsonb, country text)
            ON CONFLICT (task_type, key) DO UPDATE SET
              priority    = {priority},
              payload     = COALESCE(EXCLUDED.payload, t.payload),
              country     = COALESCE(EXCLUDED.country, t.country),
              next_due_at = {due},
              attempts    = CASE WHEN {reopening} THEN 0 ELSE t.attempts END,
              last_error  = CASE WHEN {reopening} THEN NULL ELSE t.last_error END,
              status      = CASE WHEN {reopening} THEN 'pending' ELSE t.status END
            RETURNING (t.created_at = now()) AS inserted
        """
        payload = json.dumps(
            [
                {
                    "key": it.key,
                    "priority": it.priority,
                    "due_at": it.due_at.isoformat() if it.due_at else None,
                    "payload": it.payload,
                    "country": it.country,
                }
                for it in items
            ]
        )
        cur = await conn.execute(
            self._q(stmt),
            {
                "task_type": task_type,
                "default_priority": default_priority,
                "created_by": created_by,
                "items": payload,
                "reopen": reopen,
            },
        )
        rows = await cur.fetchall()
        ins = sum(1 for r in rows if r[0])
        return ins, len(rows) - ins

    # --- leasing ------------------------------------------------------------------------------

    async def lease(
        self, conn: AsyncConnection, task_types: list[str], limit: int, leased_by: str
    ) -> list[dict]:
        """Lease up to ``limit`` due tasks of the given types. Paused and undeclared task
        types are skipped."""
        async with conn.transaction():
            cur = await conn.execute(
                self._q(
                    "SELECT name FROM {s}.task_types "
                    "WHERE name = ANY(%s) AND NOT paused AND declared"
                ),
                (task_types,),
            )
            active = [r[0] for r in await cur.fetchall()]
            if not active:
                return []
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    self._q(
                        """
                        WITH picked AS (
                          SELECT t.task_type, t.key
                          FROM {s}.tasks t
                          WHERE t.task_type = ANY(%(types)s)
                            AND t.status = 'pending'
                            AND t.next_due_at <= now()
                          ORDER BY t.priority DESC, t.next_due_at
                          LIMIT %(limit)s
                          FOR UPDATE OF t SKIP LOCKED
                        )
                        UPDATE {s}.tasks t
                        SET status = 'leased',
                            lease_id = gen_random_uuid(),
                            leased_by = %(by)s,
                            lease_expires_at = now() + tt.lease_duration
                        FROM picked, {s}.task_types tt
                        WHERE t.task_type = picked.task_type AND t.key = picked.key
                          AND tt.name = t.task_type
                        RETURNING t.task_type, t.key, t.lease_id, t.lease_expires_at,
                                  t.priority, t.attempts, t.payload, t.country
                        """
                    ),
                    {"types": active, "limit": limit, "by": leased_by},
                )
                rows = await cur.fetchall()
        rows.sort(key=lambda r: -r["priority"])
        return rows

    async def renew(
        self, conn: AsyncConnection, refs: list[LeaseRef]
    ) -> tuple[list[dict], list[LeaseRef]]:
        """Extend live leases. Returns (renewed rows with the new expiry, lost leases)."""
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    self._q(
                        """
                        UPDATE {s}.tasks t
                        SET lease_expires_at = now() + tt.lease_duration
                        FROM jsonb_to_recordset(%(refs)s::jsonb)
                               AS r(task_type text, key text, lease_id uuid),
                             {s}.task_types tt
                        WHERE t.task_type = r.task_type AND t.key = r.key
                          AND t.lease_id = r.lease_id
                          AND t.status = 'leased' AND t.lease_expires_at > now()
                          AND tt.name = t.task_type AND NOT tt.paused
                        RETURNING t.task_type, t.key, t.lease_id, t.lease_expires_at
                        """
                    ),
                    {"refs": _refs_json(refs)},
                )
                rows = await cur.fetchall()
        ok = {_ref(r) for r in rows}
        return rows, [r for r in refs if r not in ok]

    async def ack(self, conn: AsyncConnection, items: list[AckItem]) -> tuple[int, list[LeaseRef]]:
        """Record success. Recurring task types go back to pending, due after their
        ``recrawl_interval``; one-shot task types become ``done``.
        Returns (number acked, refs that matched no lease)."""
        data = json.dumps(
            [
                {
                    "task_type": i.ref.task_type,
                    "key": i.ref.key,
                    "lease_id": str(i.ref.lease_id),
                    "event_id": str(i.event_id),
                }
                for i in items
            ]
        )
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    self._q(
                        """
                        UPDATE {s}.tasks t
                        SET status = CASE WHEN tt.recrawl_interval IS NULL
                                          THEN 'done' ELSE 'pending' END,
                            next_due_at = CASE WHEN tt.recrawl_interval IS NULL
                                               THEN t.next_due_at
                                               ELSE now() + tt.recrawl_interval END,
                            attempts = 0,
                            lease_id = NULL,
                            leased_by = NULL,
                            lease_expires_at = NULL,
                            last_done_at = now(),
                            last_event_id = r.event_id,
                            last_error = NULL
                        FROM jsonb_to_recordset(%(items)s::jsonb)
                               AS r(task_type text, key text, lease_id uuid, event_id uuid),
                             {s}.task_types tt
                        WHERE t.task_type = r.task_type AND t.key = r.key
                          AND t.lease_id = r.lease_id
                          AND t.status IN ('leased', 'pending')
                          AND tt.name = t.task_type
                        RETURNING t.task_type, t.key, r.lease_id
                        """
                    ),
                    {"items": data},
                )
                rows = await cur.fetchall()
        ok = {_ref(r) for r in rows}
        return len(rows), [i.ref for i in items if i.ref not in ok]

    async def fail(
        self, conn: AsyncConnection, items: list[FailItem]
    ) -> tuple[int, list[LeaseRef]]:
        """Record a failed attempt: back to pending after the backoff, or ``dead`` at
        ``max_attempts`` (or right away when ``permanent``)."""
        data = json.dumps(
            [
                {
                    "task_type": i.ref.task_type,
                    "key": i.ref.key,
                    "lease_id": str(i.ref.lease_id),
                    "event_id": str(i.event_id) if i.event_id else None,
                    "reason": i.reason[:1000],
                    "retry_after_s": i.retry_after.total_seconds() if i.retry_after else None,
                    "permanent": i.permanent,
                }
                for i in items
            ]
        )
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    self._q(
                        """
                        UPDATE {s}.tasks t
                        SET attempts = t.attempts + 1,
                            status = CASE WHEN r.permanent OR t.attempts + 1 >= tt.max_attempts
                                          THEN 'dead' ELSE 'pending' END,
                            next_due_at = now() + COALESCE(
                              make_interval(secs => r.retry_after_s),
                              LEAST(tt.retry_backoff * power(2, t.attempts),
                                    tt.retry_backoff_max)),
                            lease_id = NULL,
                            leased_by = NULL,
                            lease_expires_at = NULL,
                            last_error = r.reason,
                            last_event_id = COALESCE(r.event_id, t.last_event_id)
                        FROM jsonb_to_recordset(%(items)s::jsonb)
                               AS r(task_type text, key text, lease_id uuid, event_id uuid,
                                    reason text, retry_after_s float8, permanent boolean),
                             {s}.task_types tt
                        WHERE t.task_type = r.task_type AND t.key = r.key
                          AND t.lease_id = r.lease_id AND t.status = 'leased'
                          AND tt.name = t.task_type
                        RETURNING t.task_type, t.key, r.lease_id
                        """
                    ),
                    {"items": data},
                )
                rows = await cur.fetchall()
        ok = {_ref(r) for r in rows}
        return len(rows), [i.ref for i in items if i.ref not in ok]

    async def release(
        self, conn: AsyncConnection, refs: list[LeaseRef]
    ) -> tuple[int, list[LeaseRef]]:
        """Give leases back without counting an attempt (graceful shutdown). The due time
        is unchanged, so the tasks can be leased again right away."""
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    self._q(
                        """
                        UPDATE {s}.tasks t
                        SET status = 'pending', lease_id = NULL, leased_by = NULL,
                            lease_expires_at = NULL
                        FROM jsonb_to_recordset(%(refs)s::jsonb)
                               AS r(task_type text, key text, lease_id uuid)
                        WHERE t.task_type = r.task_type AND t.key = r.key
                          AND t.lease_id = r.lease_id AND t.status = 'leased'
                        RETURNING t.task_type, t.key, r.lease_id
                        """
                    ),
                    {"refs": _refs_json(refs)},
                )
                rows = await cur.fetchall()
        ok = {_ref(r) for r in rows}
        return len(rows), [r for r in refs if r not in ok]

    async def sweep(self, conn: AsyncConnection, batch: int = 5000) -> int:
        """Return expired leases to pending (or ``dead``), counting a failed attempt.
        Returns the number of tasks swept."""
        total = 0
        while True:
            async with conn.transaction():
                cur = await conn.execute(
                    self._q(
                        """
                        WITH expired AS (
                          SELECT task_type, key FROM {s}.tasks
                          WHERE status = 'leased' AND lease_expires_at < now()
                          LIMIT %(batch)s
                          FOR UPDATE SKIP LOCKED
                        )
                        UPDATE {s}.tasks t
                        SET attempts = t.attempts + 1,
                            status = CASE WHEN t.attempts + 1 >= tt.max_attempts
                                          THEN 'dead' ELSE 'pending' END,
                            next_due_at = now() + LEAST(tt.retry_backoff * power(2, t.attempts),
                                                        tt.retry_backoff_max),
                            leased_by = NULL,
                            lease_expires_at = NULL,
                            last_error = 'lease expired'
                        FROM expired, {s}.task_types tt
                        WHERE t.task_type = expired.task_type AND t.key = expired.key
                          AND tt.name = t.task_type
                        """
                    ),
                    {"batch": batch},
                )
                n = cur.rowcount
            total += n
            if n < batch:
                return total

    # --- status -------------------------------------------------------------------------------

    async def stats(self, conn: AsyncConnection, eligible_cap: int = 1_000_000) -> list[Stats]:
        """Per task type: eligible (due and pending, counted up to ``eligible_cap``),
        leased, dead, and the planner's row estimate.

        Eligible ignores per-target limits; those come with the target leases.
        """
        out: list[Stats] = []
        types = await self.task_types(conn)
        for name, tt in sorted(types.items()):
            cur = await conn.execute(
                self._q(
                    """
                    SELECT
                      (SELECT count(*) FROM (
                         SELECT 1 FROM {s}.tasks
                         WHERE task_type = %(tt)s AND status = 'pending' AND next_due_at <= now()
                         LIMIT %(cap)s) e),
                      (SELECT count(*) FROM {s}.tasks
                       WHERE task_type = %(tt)s AND status = 'leased'),
                      (SELECT count(*) FROM {s}.tasks
                       WHERE task_type = %(tt)s AND status = 'dead'),
                      (SELECT COALESCE(sum(GREATEST(c.reltuples, 0)), 0)::bigint
                       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE n.nspname = %(schema)s
                         AND (c.relname = %(part)s OR c.relname LIKE %(part)s || '\\_h%%')
                         AND c.relkind = 'r')
                    """
                ),
                {
                    "tt": name,
                    "cap": eligible_cap,
                    "schema": self.schema,
                    "part": tt["partition_table"],
                },
            )
            eligible, leased, dead, estimate = await cur.fetchone()
            out.append(
                Stats(
                    task_type=name,
                    paused=tt["paused"],
                    declared=tt["declared"],
                    eligible=eligible,
                    eligible_capped=eligible >= eligible_cap,
                    leased=leased,
                    dead=dead,
                    rows_estimate=estimate,
                )
            )
        return out
