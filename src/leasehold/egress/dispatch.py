"""The dispatcher: frontier tasks leased together with exclusive target leases.

For task types with ``target_leases``, a worker leases with its egress (the source
address its requests leave from). In one transaction the dispatcher:

1. checks the egress: registered, ``active``, and allowed for the task type;
2. reads due tasks, best first, and groups them by target (the task's group, or its
   key when it has none: a task without a group is still limited);
3. skips targets that another holder leases (any environment), that are cooling down
   after pushback, that this egress used up its pair budget on, or whose lease from
   this holder is revoked or at its cap;
4. locks the chosen tasks, claims their targets (``INSERT … ON CONFLICT … WHERE``:
   only a free, expired or own lease is taken, so two dispatchers never grant one target
   to two holders), and leases the tasks of the targets it claimed;
5. adds the requests to the pair usage of (egress, target, hour), and deducts one token
   per task from the egress's allocation for the task's provider (``leasehold.egress.
   planner``), from the task's class first, then class ``*``. A provider with a budget
   but no tokens left (or no plan) gets no tasks: the worker gets other providers' work.

A target lease carries the interval between requests to the target (``budgets.yml``
``targets``, by the provider's kind) and a cap of tasks per lease. Holders renew their
target leases with their task leases; a renewal lists the leases that are lost
(expired, revoked, or the target is cooling down) so the holder stops at once. Pushback
reported by any holder cools the target for every egress: rotating to another address
after a block is not allowed.

Tables: ``target_leases``, ``target_cooldowns``, ``pair_usage`` and the registry in the
shared egress schema (``leasehold.egress.schema``); tasks in the environment's frontier
schema. Fail closed: an unknown or inactive egress gets nothing.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from leasehold.egress.config import SHARED_IP_PREFIX, Budgets
from leasehold.egress.planner import ANY_CLASS, effective_class
from leasehold.egress.schema import SCHEMA

SCAN_PER_TASK = 20  # due tasks read per task asked for, to look past busy targets
SCAN_MAX = 5000


class EgressRefused(Exception):
    """The egress may not lease these task types (unknown, not active, not allowed)."""


@dataclass(frozen=True)
class TargetLease:
    target: str
    lease_id: UUID
    expires_at: datetime
    interval_s: float
    provider: str
    cap: int
    granted: int


@dataclass
class Grant:
    tasks: list[dict] = field(default_factory=list)
    targets: list[TargetLease] = field(default_factory=list)


@dataclass(frozen=True)
class Cooling:
    target: str
    after: timedelta
    reason: str


def target_of(group: str, key: str) -> str:
    """What a task's target lease is keyed on: its group, or its key without one."""
    return group or key


def _kind(provider: str, kinds: dict[str, str]) -> str:
    if not provider:
        return "none"
    if provider.startswith(SHARED_IP_PREFIX):
        return "shared_ip"
    return kinds.get(provider, "none")


class Dispatcher:
    def __init__(self, env: str, frontier_schema: str, egress_schema: str = SCHEMA):
        self.env = env
        self._f = sql.Identifier(frontier_schema)
        self._e = sql.Identifier(egress_schema)

    def _q(self, stmt: str) -> sql.Composed:
        return sql.SQL(stmt).format(f=self._f, e=self._e)

    async def _rows(self, conn: AsyncConnection, stmt: str, params: Any = None) -> list[dict]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(self._q(stmt), params)
            return await cur.fetchall()

    async def budgets(self, conn: AsyncConnection) -> Budgets:
        rows = await self._rows(conn, "SELECT name, value FROM {e}.settings")
        return Budgets.model_validate({r["name"]: r["value"] for r in rows if r["value"]})

    # --- lease --------------------------------------------------------------------------------

    async def lease(
        self,
        conn: AsyncConnection,
        task_types: list[str],
        limit: int,
        egress: str,
        holder: str,
        classes: dict[str, str] | None = None,
        leased_by: str | None = None,
    ) -> Grant:
        """Lease up to ``limit`` tasks of target-leased task types for ``holder`` on
        ``egress``. ``classes``: task type → its class from ``service.yml`` (default: the
        last class); outside prod it counts ``dev_offset`` classes lower.
        ``holder`` holds the target leases (a worker, or the machine's agent);
        ``leased_by`` (default: ``holder``) is recorded on the tasks.
        Raises ``EgressRefused`` for an egress that may not lease them."""
        async with conn.transaction():
            rows = await self._rows(
                conn,
                "SELECT status, task_types FROM {e}.egresses WHERE address = %s",
                (egress,),
            )
            if not rows:
                raise EgressRefused(f"egress {egress} is not in the registry")
            if rows[0]["status"] != "active":
                raise EgressRefused(f"egress {egress} is {rows[0]['status']}")
            allowed = rows[0]["task_types"]
            if allowed is not None:
                task_types = [t for t in task_types if t in allowed]
                if not task_types:
                    raise EgressRefused(f"egress {egress} may not lease these task types")

            types = {
                r["name"]: r
                for r in await self._rows(
                    conn,
                    "SELECT name, lease_duration, target_cap FROM {f}.task_types "
                    "WHERE name = ANY(%s) AND NOT paused AND declared AND target_leases",
                    (task_types,),
                )
            }
            if not types:
                return Grant()
            budgets = await self.budgets(conn)
            prov = await self._rows(
                conn, "SELECT name, kind, per_egress_per_min FROM {e}.providers"
            )
            kinds = {r["name"]: r["kind"] for r in prov}
            budgeted = {r["name"] for r in prov if r["per_egress_per_min"] is not None}
            last_class = budgets.classes.order[-1]
            classes = classes or {}

            def class_of(task_type: str) -> str:
                return effective_class(classes.get(task_type, last_class), self.env, budgets)

            def has_budget(provider: str) -> bool:
                if provider.startswith(SHARED_IP_PREFIX):
                    return budgets.shared_ip is not None
                return provider in budgeted

            candidates = await self._candidates(
                conn, list(types), min(SCAN_MAX, max(limit * SCAN_PER_TASK, 100))
            )
            if not candidates:
                return Grant()
            targets = sorted({target_of(c["grp"], c["key"]) for c in candidates})
            leases = {
                r["target"]: r
                for r in await self._rows(
                    conn,
                    "SELECT target, host(egress) AS egress, holder, env, cap, granted, "
                    "expires_at > now() AS live, revoked_at IS NOT NULL AS revoked "
                    "FROM {e}.target_leases WHERE target = ANY(%s)",
                    (targets,),
                )
            }
            cooling = {
                r["target"]
                for r in await self._rows(
                    conn,
                    "SELECT target FROM {e}.target_cooldowns "
                    "WHERE target = ANY(%s) AND until > now()",
                    (targets,),
                )
            }
            used = {
                r["target"]: r["requests"]
                for r in await self._rows(
                    conn,
                    "SELECT target, requests FROM {e}.pair_usage WHERE egress = %s "
                    "AND hour = date_trunc('hour', now()) AND target = ANY(%s)",
                    (egress, targets),
                )
            }

            # This window's allocations on this egress, locked so concurrent grants don't
            # spend them twice.
            toks = await _Tokens.load(
                self,
                conn,
                egress,
                sorted({c["provider"] for c in candidates if has_budget(c["provider"])}),
                budgets,
            )
            taken: dict[tuple[str, str], tuple[str, str]] = {}  # task -> allocation row

            # Choose tasks per target, best tasks first.
            room: dict[str, int] = {}
            plan: dict[str, dict] = {}  # target -> lease parameters
            chosen: list[dict] = []
            for c in candidates:
                if len(chosen) >= limit:
                    break
                target = target_of(c["grp"], c["key"])
                if target not in room:
                    room[target] = self._room(
                        target, c, types, budgets, kinds, leases, cooling, used, egress, holder
                    )
                    if room[target] > 0:
                        tt = types[c["task_type"]]
                        kind = _kind(c["provider"], kinds)
                        interval = budgets.targets.interval_for(kind)
                        plan[target] = {
                            "provider": c["provider"],
                            "interval_ms": int(interval.total_seconds() * 1000),
                            "duration_ms": int(tt["lease_duration"].total_seconds() * 1000),
                            "cap": _cap(tt, interval),
                        }
                if room[target] <= 0:
                    continue
                if has_budget(c["provider"]):
                    row = toks.take(c["provider"], class_of(c["task_type"]))
                    if row is None:
                        continue  # no tokens for this provider: other providers' work
                    taken[(c["task_type"], c["key"])] = row
                room[target] -= 1
                chosen.append(c)
            if not chosen:
                return Grant()

            locked = await self._rows(
                conn,
                """
                SELECT t.task_type, t.key, t.grp, t.provider FROM {f}.tasks t
                JOIN jsonb_to_recordset(%s::jsonb) AS c(task_type text, key text)
                  ON t.task_type = c.task_type AND t.key = c.key
                WHERE t.status = 'pending' AND t.next_due_at <= now()
                FOR UPDATE OF t SKIP LOCKED
                """,
                (json.dumps([{"task_type": c["task_type"], "key": c["key"]} for c in chosen]),),
            )
            per_target: dict[str, int] = {}
            for r in locked:
                t = target_of(r["grp"], r["key"])
                per_target[t] = per_target.get(t, 0) + 1
            if not per_target:
                return Grant()

            claims = [
                {"target": t, "granted": n, "lease_id": str(uuid4()), **plan[t]}
                for t, n in sorted(per_target.items())
            ]
            claimed = await self._rows(
                conn,
                """
                INSERT INTO {e}.target_leases AS l
                  (target, egress, holder, env, lease_id, provider, interval_ms, duration_ms,
                   cap, granted, expires_at)
                SELECT c.target, %(egress)s, %(holder)s, %(env)s, c.lease_id, c.provider,
                       c.interval_ms, c.duration_ms, c.cap, c.granted,
                       now() + c.duration_ms * interval '1 millisecond'
                FROM jsonb_to_recordset(%(claims)s::jsonb)
                  AS c(target text, lease_id uuid, provider text, interval_ms int,
                       duration_ms int, cap int, granted int)
                ON CONFLICT (target) DO UPDATE SET
                  lease_id    = CASE WHEN l.expires_at > now() THEN l.lease_id
                                     ELSE EXCLUDED.lease_id END,
                  granted     = CASE WHEN l.expires_at > now() THEN l.granted + EXCLUDED.granted
                                     ELSE EXCLUDED.granted END,
                  cap         = CASE WHEN l.expires_at > now() THEN l.cap ELSE EXCLUDED.cap END,
                  granted_at  = CASE WHEN l.expires_at > now() THEN l.granted_at ELSE now() END,
                  expires_at  = GREATEST(CASE WHEN l.expires_at > now() THEN l.expires_at
                                              ELSE now() END, EXCLUDED.expires_at),
                  egress      = EXCLUDED.egress,
                  holder      = EXCLUDED.holder,
                  env         = EXCLUDED.env,
                  provider    = EXCLUDED.provider,
                  interval_ms = EXCLUDED.interval_ms,
                  duration_ms = EXCLUDED.duration_ms,
                  revoked_at  = NULL,
                  revoke_reason = NULL
                WHERE l.expires_at <= now()
                   OR (l.egress = EXCLUDED.egress AND l.holder = EXCLUDED.holder
                       AND l.env = EXCLUDED.env AND l.revoked_at IS NULL
                       AND l.granted + EXCLUDED.granted <= l.cap)
                RETURNING l.target, l.lease_id, l.expires_at, l.interval_ms, l.provider,
                          l.cap, l.granted
                """,
                {
                    "egress": egress,
                    "holder": holder,
                    "env": self.env,
                    "claims": json.dumps(claims),
                },
            )
            if not claimed:
                return Grant()
            by_target = {r["target"]: r for r in claimed}
            leased_rows = [r for r in locked if target_of(r["grp"], r["key"]) in by_target]
            mine = [{"task_type": r["task_type"], "key": r["key"]} for r in leased_rows]
            spent: dict[tuple[str, str], int] = defaultdict(int)
            for r in leased_rows:
                row = taken.get((r["task_type"], r["key"]))
                if row is not None:
                    spent[row] += 1
            await toks.write(conn, spent)
            tasks = await self._rows(
                conn,
                """
                UPDATE {f}.tasks t
                SET status = 'leased',
                    lease_id = gen_random_uuid(),
                    leased_by = %(by)s,
                    lease_expires_at = now() + tt.lease_duration
                FROM jsonb_to_recordset(%(tasks)s::jsonb) AS c(task_type text, key text),
                     {f}.task_types tt
                WHERE t.task_type = c.task_type AND t.key = c.key AND tt.name = t.task_type
                RETURNING t.task_type, t.key, t.lease_id, t.lease_expires_at, t.priority,
                          t.attempts, t.payload, t.country, t.grp AS "group", t.provider
                """,
                {"by": leased_by or holder, "tasks": json.dumps(mine)},
            )
            await conn.execute(
                self._q(
                    """
                    INSERT INTO {e}.pair_usage AS p (egress, target, hour, requests)
                    SELECT %(egress)s, c.target, date_trunc('hour', now()), c.n
                    FROM jsonb_to_recordset(%(usage)s::jsonb) AS c(target text, n int)
                    ON CONFLICT (egress, target, hour)
                    DO UPDATE SET requests = p.requests + EXCLUDED.requests
                    """
                ),
                {
                    "egress": egress,
                    "usage": json.dumps([{"target": t, "n": per_target[t]} for t in by_target]),
                },
            )
        for t in tasks:
            t["target"] = target_of(t["group"], t["key"])
            t["target_lease_id"] = by_target[t["target"]]["lease_id"]
        tasks.sort(key=lambda r: -r["priority"])
        return Grant(
            tasks=tasks,
            targets=[
                TargetLease(
                    target=r["target"],
                    lease_id=r["lease_id"],
                    expires_at=r["expires_at"],
                    interval_s=r["interval_ms"] / 1000,
                    provider=r["provider"],
                    cap=r["cap"],
                    granted=r["granted"],
                )
                for r in claimed
            ],
        )

    async def spend(
        self,
        conn: AsyncConnection,
        egress: str,
        provider: str,
        klass: str,
        n: int,
    ) -> int:
        """Tokens for requests beyond a task's first (a retry, a fallback, a browser page
        load): up to ``n`` from this window's allocation for ``provider`` on ``egress``,
        with the same order and lending as grants. A provider without a budget costs
        nothing. Returns the number granted."""
        async with conn.transaction():
            budgets = await self.budgets(conn)
            if provider.startswith(SHARED_IP_PREFIX):
                budgeted = budgets.shared_ip is not None
            else:
                rows = await self._rows(
                    conn,
                    "SELECT 1 FROM {e}.providers WHERE name = %s "
                    "AND per_egress_per_min IS NOT NULL",
                    (provider,),
                )
                budgeted = bool(rows)
            if not provider or not budgeted:
                return n
            k = effective_class(klass, self.env, budgets)
            toks = await _Tokens.load(self, conn, egress, [provider], budgets)
            spent: dict[tuple[str, str], int] = defaultdict(int)
            got = 0
            while got < n:
                row = toks.take(provider, k)
                if row is None:
                    break
                spent[row] += 1
                got += 1
            await toks.write(conn, spent)
            return got

    async def _candidates(self, conn, types: list[str], scan: int) -> list[dict]:
        # Distinct priorities by skip-scan, then each one's due tasks in due order (see
        # leasehold.frontier.store._PICK for why not one ORDER BY).
        return await self._rows(
            conn,
            """
            WITH RECURSIVE prios AS (
              SELECT (SELECT t.priority FROM {f}.tasks t
                      WHERE t.task_type = ANY(%(types)s) AND t.status = 'pending'
                      ORDER BY t.priority DESC LIMIT 1) AS p
              UNION ALL
              SELECT (SELECT t.priority FROM {f}.tasks t
                      WHERE t.task_type = ANY(%(types)s) AND t.status = 'pending'
                        AND t.priority < prios.p
                      ORDER BY t.priority DESC LIMIT 1)
              FROM prios WHERE prios.p IS NOT NULL
            )
            SELECT c.* FROM prios
            CROSS JOIN LATERAL (
              SELECT t.task_type, t.key, t.grp, t.provider, t.priority, t.next_due_at
              FROM {f}.tasks t
              WHERE t.task_type = ANY(%(types)s) AND t.status = 'pending'
                AND t.priority = prios.p AND t.next_due_at <= now()
              ORDER BY t.next_due_at
              LIMIT %(scan)s
            ) c
            WHERE prios.p IS NOT NULL
            ORDER BY c.priority DESC, c.next_due_at
            LIMIT %(scan)s
            """,
            {"types": types, "scan": scan},
        )

    def _room(self, target, c, types, budgets, kinds, leases, cooling, used, egress, holder) -> int:
        if target in cooling:
            return 0
        tt = types[c["task_type"]]
        interval = budgets.targets.interval_for(_kind(c["provider"], kinds))
        room = _cap(tt, interval)
        lease = leases.get(target)
        if lease is not None and lease["live"]:
            mine = (
                lease["egress"] == egress and lease["holder"] == holder and lease["env"] == self.env
            )
            if not mine or lease["revoked"]:
                return 0
            room = lease["cap"] - lease["granted"]
        if budgets.pair.per_hour is not None:
            room = min(room, budgets.pair.per_hour - used.get(target, 0))
        return max(room, 0)

    # --- renew, release, cooldowns, revoke ------------------------------------------------------

    async def renew(
        self,
        conn: AsyncConnection,
        egress: str,
        holder: str,
        lease_ids: list[UUID],
        release: list[UUID] = (),  # type: ignore[assignment]
    ) -> tuple[dict[UUID, datetime], dict[UUID, str]]:
        """Extend live target leases; give back ``release`` at once (the holder has no
        more tasks for them). Returns (new expiry per renewed lease, lost lease → why)."""
        async with conn.transaction():
            if release:
                await conn.execute(
                    self._q(
                        "UPDATE {e}.target_leases SET expires_at = now() "
                        "WHERE lease_id = ANY(%s) AND egress = %s AND holder = %s "
                        "AND expires_at > now()"
                    ),
                    (list(release), egress, holder),
                )
            if not lease_ids:
                return {}, {}
            renewed = await self._rows(
                conn,
                """
                UPDATE {e}.target_leases l
                SET expires_at = GREATEST(l.expires_at,
                                          now() + l.duration_ms * interval '1 millisecond')
                WHERE l.lease_id = ANY(%(ids)s) AND l.egress = %(egress)s
                  AND l.holder = %(holder)s AND l.expires_at > now() AND l.revoked_at IS NULL
                  AND NOT EXISTS (SELECT 1 FROM {e}.target_cooldowns c
                                  WHERE c.target = l.target AND c.until > now())
                RETURNING l.lease_id, l.expires_at
                """,
                {"ids": list(lease_ids), "egress": egress, "holder": holder},
            )
            ok = {r["lease_id"]: r["expires_at"] for r in renewed}
            missing = [i for i in lease_ids if i not in ok]
            lost: dict[UUID, str] = {}
            if missing:
                rows = await self._rows(
                    conn,
                    """
                    SELECT l.lease_id, l.expires_at <= now() AS expired, l.revoke_reason,
                           l.revoked_at IS NOT NULL AS revoked, c.reason AS cooling
                    FROM {e}.target_leases l
                    LEFT JOIN {e}.target_cooldowns c
                      ON c.target = l.target AND c.until > now()
                    WHERE l.lease_id = ANY(%s)
                    """,
                    (missing,),
                )
                why = {}
                for r in rows:
                    if r["revoked"]:
                        why[r["lease_id"]] = f"revoked: {r['revoke_reason']}"
                    elif r["cooling"]:
                        why[r["lease_id"]] = f"target cooling down: {r['cooling']}"
                    elif r["expired"]:
                        why[r["lease_id"]] = "expired"
                for i in missing:
                    lost[i] = why.get(i, "not held")
        return ok, lost

    async def cool(
        self, conn: AsyncConnection, items: list[Cooling], egress: str | None, max_after: timedelta
    ) -> int:
        """Record pushback: no egress gets these targets until the cooldown ends. A longer
        cooldown already recorded is kept. Returns the number of targets cooled."""
        if not items:
            return 0
        data = [
            {
                "target": i.target,
                "after_s": min(i.after, max_after).total_seconds(),
                "reason": i.reason[:500],
            }
            for i in items
        ]
        async with conn.transaction():
            cur = await conn.execute(
                self._q(
                    """
                    INSERT INTO {e}.target_cooldowns AS c (target, until, reason, egress, env)
                    SELECT i.target, now() + i.after_s * interval '1 second', i.reason,
                           %(egress)s, %(env)s
                    FROM jsonb_to_recordset(%(items)s::jsonb)
                      AS i(target text, after_s float8, reason text)
                    ON CONFLICT (target) DO UPDATE SET
                      until = GREATEST(c.until, EXCLUDED.until),
                      reason = CASE WHEN EXCLUDED.until > c.until THEN EXCLUDED.reason
                                    ELSE c.reason END,
                      egress = CASE WHEN EXCLUDED.until > c.until THEN EXCLUDED.egress
                                    ELSE c.egress END,
                      env = CASE WHEN EXCLUDED.until > c.until THEN EXCLUDED.env ELSE c.env END,
                      updated_at = now()
                    """
                ),
                {"egress": egress, "env": self.env, "items": json.dumps(data)},
            )
            return cur.rowcount

    async def targets_of(self, conn: AsyncConnection, refs: list[tuple[str, str]]) -> dict:
        """(task_type, key) → target, for tasks in this environment's frontier."""
        rows = await self._rows(
            conn,
            """
            SELECT t.task_type, t.key, t.grp FROM {f}.tasks t
            JOIN jsonb_to_recordset(%s::jsonb) AS r(task_type text, key text)
              ON t.task_type = r.task_type AND t.key = r.key
            """,
            (json.dumps([{"task_type": a, "key": b} for a, b in refs]),),
        )
        return {(r["task_type"], r["key"]): target_of(r["grp"], r["key"]) for r in rows}

    async def revoke(self, conn: AsyncConnection, target: str, reason: str) -> bool:
        """Stop a target lease: no new tasks, and its holder learns at the next renewal."""
        cur = await conn.execute(
            self._q(
                "UPDATE {e}.target_leases SET revoked_at = now(), revoke_reason = %s "
                "WHERE target = %s AND expires_at > now() AND revoked_at IS NULL"
            ),
            (reason[:500], target),
        )
        return cur.rowcount > 0

    async def sweep(self, conn: AsyncConnection, keep: timedelta = timedelta(hours=1)) -> int:
        """Delete expired leases and cooldowns, and pair usage older than two hours. For the
        schema owner's API only (other environments may not delete)."""
        n = 0
        async with conn.transaction():
            for stmt in (
                "DELETE FROM {e}.target_leases WHERE expires_at < now() - %(keep)s",
                "DELETE FROM {e}.target_cooldowns WHERE until < now() - %(keep)s",
                "DELETE FROM {e}.pair_usage WHERE hour < date_trunc('hour', now()) - "
                "interval '2 hours'",
            ):
                cur = await conn.execute(self._q(stmt), {"keep": keep})
                n += cur.rowcount
        return n


class _Tokens:
    """One egress's allocations in the current window, for some providers.

    ``take`` uses the class's own tokens, then class ``*``, then, once ``lend_after``
    of the window has passed, what a higher class will not need in the rest of the
    window: its tokens left minus its rate so far times the time left. Nothing is lent
    ahead or carried into the next window."""

    def __init__(self, d: Dispatcher, egress: str, rows: list[dict], budgets: Budgets):
        self._d = d
        self.egress = egress
        self.order = budgets.classes.order
        self.lend_after = budgets.planning.lend_after
        self.left: dict[tuple[str, str], int] = {}
        self.lendable: dict[tuple[str, str], int] = {}
        for r in rows:
            key = (r["provider"], r["class"])
            left = max(r["tokens"] - r["used"], 0)
            self.left[key] = left
            elapsed = r["elapsed"].total_seconds()
            remaining = r["remaining"].total_seconds()
            if r["elapsed"] >= self.lend_after and elapsed > 0:
                keep = math.ceil(r["used"] / elapsed * remaining)
                self.lendable[key] = max(left - keep, 0)
            else:
                self.lendable[key] = 0

    @classmethod
    async def load(cls, d: Dispatcher, conn, egress: str, providers: list[str], budgets):
        rows = []
        if providers:
            rows = await d._rows(
                conn,
                "SELECT provider, class, tokens, used, now() - window_start AS elapsed, "
                "window_end - now() AS remaining FROM {e}.allocations "
                "WHERE egress = %s AND provider = ANY(%s) "
                "AND window_start <= now() AND window_end > now() "
                "ORDER BY provider, class FOR UPDATE",
                (egress, providers),
            )
        return cls(d, egress, rows, budgets)

    def take(self, provider: str, klass: str) -> tuple[str, str] | None:
        for k in (klass, ANY_CLASS):
            key = (provider, k)
            if self.left.get(key, 0) > 0:
                self.left[key] -= 1
                self.lendable[key] = min(self.lendable.get(key, 0), self.left[key])
                return key
        rank = self.order.index(klass) if klass in self.order else len(self.order)
        for higher in self.order[:rank]:
            key = (provider, higher)
            if self.lendable.get(key, 0) > 0:
                self.lendable[key] -= 1
                self.left[key] -= 1
                return key
        return None

    async def write(self, conn, spent: dict[tuple[str, str], int]) -> None:
        for (provider, k), n in spent.items():
            if n > 0:
                await conn.execute(
                    self._d._q(
                        "UPDATE {e}.allocations SET used = used + %s WHERE egress = %s "
                        "AND provider = %s AND class = %s "
                        "AND window_start <= now() AND window_end > now()"
                    ),
                    (n, self.egress, provider, k),
                )


def _cap(tt: dict, interval: timedelta) -> int:
    if tt.get("target_cap"):
        return int(tt["target_cap"])
    return max(1, int(tt["lease_duration"] / interval))
