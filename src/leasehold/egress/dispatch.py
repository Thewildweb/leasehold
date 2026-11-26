"""The dispatcher: frontier tasks leased together with exclusive target leases.

For task types with ``target_leases``, a worker leases with its egress (the source
address its requests leave from), or, for targets on a proxy tier, gets a route through
a proxy egress the dispatcher picks. In one transaction the dispatcher:

1. checks the worker's egress, when it has one: registered, ``active`` (and not taken
   out by an operator's override), and allowed for the task type; that egress serves
   the ``datacenter`` tier;
2. reads due tasks, best first, and groups them by target (the task's group, or its
   key when it has none: a task without a group is still limited);
3. finds each target's tier for its task type (``leasehold.egress.tiers``: the first
   tier, or the one a block escalated it to) and an egress on that tier: the worker's
   own for ``datacenter``, else the first eligible fixed exit or pool of the accounts
   and pools the tier names, in order (account ``active`` and ``ok`` in this environment,
   the task type's ``stop_at_used`` not reached, concurrency, country and session
   support, pair budget and provider tokens left). A target with no eligible egress on
   its tier waits: there is no fallback to another tier;
4. skips targets that another holder leases (any environment), that are cooling down on
   their tier (or on every tier, after rate pushback), or whose lease from this holder
   is revoked or at its cap;
5. locks the chosen tasks, claims their targets (``INSERT … ON CONFLICT … WHERE``: only a
   free, expired or own lease is taken, so two dispatchers never grant one target to two
   holders), and leases the tasks of the targets it claimed;
6. adds the requests to the pair usage of (egress, target, hour), and deducts one token
   per task from the egress's allocation for the task's provider (``leasehold.egress.
   planner``), from the task's class first, then class ``*``. A provider with a budget
   but no tokens left (or no plan) gets no tasks on that egress.

A target lease carries the interval between requests to the target (``budgets.yml``
``targets``, by the target's override or its provider's kind) and a cap of tasks per
lease; a routed one also the
proxy route (endpoint, rendered username, credentials, session, error rules), which the
API returns with the lease and again on renewal when the account's credentials changed.
Holders renew their target leases with their task leases; a renewal lists the leases
that are lost (expired, revoked, the target cooling down, the proxy account or egress
out of service) so the holder stops at once.

Between two requests to one target there is at least its interval, also across leases,
holders and egresses: a lease that takes over an ended row (released, expired, at its
cap, moved to another egress) carries ``not_before``, the old lease's ``expires_at`` plus
its interval. A holder sends its last request before it releases (which sets
``expires_at`` to now) and none after ``expires_at``, so the new holder's first request
waits until ``not_before``; the worker and the agent start their pacing there. A target
without a row (never leased, or swept about an hour after its last lease) has none.

A task type that reaches the datacenter tier through exits (``datacenter_via: exit``,
``leasehold.egress.tiers``) is leased by workers without an address of their own (the
sandboxed browser). For its targets on that tier the dispatcher picks a registry egress
that runs an exit (``exit_endpoint``, not taken out by ``exit_override``), the least
loaded one first, with the same pair budget, provider tokens and one-holder rule as the
VPS's own workers: the lease is on that egress's address, with ``via = 'exit'``. The route
names the exit's endpoint and carries an exit token (``leasehold.exit.token``) signed
with the exit key, valid until the lease expires plus ``EXIT_TOKEN_GRACE``; every renewal
returns the route again with a fresh token. Without an exit key no such route is given.

Pushback reported by any holder cools the target: a block (``challenge``, ``block``) on
the tier it happened on, escalating the target to the task type's next tier; rate
pushback (``rate``, ``retry_after``, or no cause) on every tier. Moving to another
address after rate pushback is not allowed.

Tables: ``target_leases``, ``target_cooldowns``, ``target_tiers``, ``target_clears``,
``pair_usage`` and the registry and proxies in the shared egress schema
(``leasehold.egress.schema``); tasks in the environment's frontier schema. Fail closed:
an unknown or inactive egress gets nothing, and without a secrets key no route is given.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from leasehold.egress import proxies as px
from leasehold.egress import tiers as tr
from leasehold.egress.config import SHARED_IP_PREFIX, Budgets
from leasehold.egress.planner import ANY_CLASS, effective_class
from leasehold.egress.schema import SCHEMA
from leasehold.egress.secrets import Keyring, SecretsError
from leasehold.exit import token as exit_token

# The account name routes through registry exits carry (RouteRef): no proxy account.
EXIT_ACCOUNT = "exit"
EXIT_TOKEN_GRACE = timedelta(seconds=30)

SCAN_PER_TASK = 20  # due tasks read per task asked for, to look past busy targets
SCAN_MAX = 5000

# Who may lift a cooldown of each cause (POST /v1/targets/{target}/clear). A cooldown
# from before causes were recorded is treated like rate pushback.
CLEARABLE_BY: dict[str | None, tuple[str, ...]] = {
    "challenge": ("health", "classifier", "admin"),
    "block": ("health", "classifier", "admin"),
    "rate": ("admin",),
    "retry_after": (),
    None: ("admin",),
}


class EgressRefused(Exception):
    """The egress may not lease these task types (unknown, not active, not allowed)."""


class ClearRefused(Exception):
    """A cooldown that the caller's source may not lift (HTTP 409)."""

    def __init__(self, message: str, causes: list[str | None]):
        super().__init__(message)
        self.causes = causes


@dataclass(frozen=True)
class TargetLease:
    target: str
    lease_id: UUID
    expires_at: datetime
    interval_s: float
    provider: str
    cap: int
    granted: int
    tier: str = tr.DATACENTER
    route: dict | None = field(default=None, repr=False)  # holds credentials
    # A new lease on a target that was leased before (by any holder, on any egress): its
    # first request waits until then, one interval after the previous lease ended.
    not_before: datetime | None = None


@dataclass
class Grant:
    tasks: list[dict] = field(default_factory=list)
    targets: list[TargetLease] = field(default_factory=list)
    waiting: dict[str, int] = field(default_factory=dict)  # why targets waited, counted


@dataclass(frozen=True)
class Cooling:
    target: str
    after: timedelta
    reason: str
    cause: str | None = None  # challenge, block, rate, retry_after
    task_type: str | None = None


@dataclass(frozen=True)
class Stopped:
    """A task type's ``stop_at_used`` reached for an account (for the info message)."""

    task_type: str
    account: str
    used: float
    stop: float


def target_of(group: str, key: str) -> str:
    """What a task's target lease is keyed on: its group, or its key without one."""
    return group or key


def _kind(provider: str, kinds: dict[str, str]) -> str:
    if not provider:
        return "none"
    if provider.startswith(SHARED_IP_PREFIX):
        return "shared_ip"
    return kinds.get(provider, "none")


def _spread(target: str, key: str) -> str:
    return hashlib.sha256(f"{target}|{key}".encode()).hexdigest()[:8]


_DIRECT = {
    "account": None,
    "route_fp": None,
    "sticky_ttl_s": None,
    "country": None,
    "via": None,
}


class Dispatcher:
    def __init__(
        self,
        env: str,
        frontier_schema: str,
        egress_schema: str = SCHEMA,
        keyring: Keyring | None = None,
        exit_key: bytes | None = None,
    ):
        self.env = env
        self.schema = egress_schema
        self.keyring = keyring or Keyring([])
        self.exit_key = exit_key  # signs exit tokens; None: no routes through exits
        self._f = sql.Identifier(frontier_schema)
        self._e = sql.Identifier(egress_schema)
        self.stopped: set[Stopped] = set()  # from the last lease call, for metrics

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
        egress: str | None,
        holder: str,
        classes: dict[str, str] | None = None,
        leased_by: str | None = None,
    ) -> Grant:
        """Lease up to ``limit`` tasks of target-leased task types for ``holder``.
        ``egress``: the worker's own address, which serves the ``datacenter`` tier (None:
        the worker has none and gets only routed targets). ``classes``: task type → its
        class from ``service.yml`` (default: the last class); outside prod it counts
        ``dev_offset`` classes lower. ``holder`` holds the target leases (a worker, or the
        machine's agent); ``leased_by`` (default: ``holder``) is recorded on the tasks.
        Raises ``EgressRefused`` for an egress that may not lease them."""
        async with conn.transaction():
            types = {
                r["name"]: r
                for r in await self._rows(
                    conn,
                    "SELECT name, lease_duration, target_cap, egress FROM {f}.task_types "
                    "WHERE name = ANY(%s) AND NOT paused AND declared AND target_leases",
                    (task_types,),
                )
            }
            direct: set[str] = set()  # task types the worker's own egress may serve
            if egress is not None:
                direct = await self._check_egress(conn, egress, task_types, types)
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
                    "SELECT target, egress, holder, env, cap, granted, account, tier, session, "
                    "via, expires_at > now() AS live, revoked_at IS NOT NULL AS revoked "
                    "FROM {e}.target_leases WHERE target = ANY(%s)",
                    (targets,),
                )
            }
            cooling = {
                (r["target"], r["tier"])
                for r in await self._rows(
                    conn,
                    "SELECT target, tier FROM {e}.target_cooldowns "
                    "WHERE target = ANY(%s) AND until > now()",
                    (targets,),
                )
            }
            on_tier = {
                r["target"]: r["tier"]
                for r in await self._rows(
                    conn,
                    "SELECT target, tier FROM {e}.target_tiers "
                    "WHERE target = ANY(%s) AND since > now() - %s",
                    (targets, budgets.proxies.tier_ttl),
                )
            }
            used = {
                (r["egress"], r["target"]): r["requests"]
                for r in await self._rows(
                    conn,
                    "SELECT egress, target, requests FROM {e}.pair_usage "
                    "WHERE hour = date_trunc('hour', now()) AND target = ANY(%s)",
                    (targets,),
                )
            }

            routing = await _Routing.load(self, conn, types)
            self.stopped = routing.stopped
            exits = await _Exits.load(self, conn, types)
            keys = sorted(({egress} if egress else set()) | routing.keys | exits.keys)
            toks = await _Tokens.load(
                self,
                conn,
                keys,
                sorted({c["provider"] for c in candidates if has_budget(c["provider"])}),
                budgets,
            )
            ctx = _PlanContext(
                types,
                budgets,
                kinds,
                leases,
                cooling,
                on_tier,
                used,
                egress,
                direct,
                holder,
                routing,
                exits,
            )
            taken: dict[tuple[str, str], tuple[str, str, str]] = {}  # task -> allocation row
            waiting: dict[str, int] = defaultdict(int)

            # Choose tasks per target, best tasks first.
            room: dict[str, int] = {}
            plan: dict[str, dict] = {}  # target -> lease parameters
            chosen: list[dict] = []
            for c in candidates:
                if len(chosen) >= limit:
                    break
                target = target_of(c["grp"], c["key"])
                provider = c["provider"]
                budget = has_budget(provider)
                klass = class_of(c["task_type"])
                if target not in room:
                    room[target] = 0

                    def has_token(key: str, _p=provider, _b=budget, _k=klass) -> bool:
                        return not _b or toks.available(key, _p, _k)

                    p, why = self._plan(ctx, target, c, has_token)
                    if p is None:
                        waiting[why] += 1
                        continue
                    plan[target] = p
                    room[target] = p.pop("room")
                p = plan.get(target)
                if p is None or room[target] <= 0:
                    continue
                if not self._accepts(c["task_type"], p, types, direct, routing, exits):
                    continue
                if budget:
                    row = toks.take(p["egress"], provider, klass)
                    if row is None:
                        continue  # no tokens for this provider here: other providers' work
                    taken[(c["task_type"], c["key"])] = row
                room[target] -= 1
                chosen.append(c)
            if not chosen:
                return Grant(waiting=dict(waiting))

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
                return Grant(waiting=dict(waiting))

            claims = []
            for t, n in sorted(per_target.items()):
                lease_id = uuid4()
                p = plan[t]
                claims.append(
                    {
                        "target": t,
                        "granted": n,
                        "lease_id": str(lease_id),
                        "session": px.session_id(lease_id) if p["sticky_ttl_s"] else None,
                        "session_ttl_s": p["sticky_ttl_s"],
                        **{k: v for k, v in p.items() if k != "sticky_ttl_s"},
                    }
                )
            # The rows these claims may take over, locked so the upsert sees the same
            # ones. A lease that ended (released or expired) sent its last request
            # before its expires_at, so the next lease starts one interval after it.
            ended = {
                r["target"]: r
                for r in await self._rows(
                    conn,
                    "SELECT target, lease_id, "
                    "expires_at + interval_ms * interval '1 millisecond' AS free_at "
                    "FROM {e}.target_leases WHERE target = ANY(%s) ORDER BY target FOR UPDATE",
                    (sorted(per_target),),
                )
            }
            claimed = await self._rows(
                conn,
                """
                INSERT INTO {e}.target_leases AS l
                  (target, egress, holder, env, lease_id, provider, interval_ms, duration_ms,
                   cap, granted, expires_at, account, tier, session, session_ttl_s, country,
                   route_fp, via)
                SELECT c.target, c.egress, %(holder)s, %(env)s, c.lease_id, c.provider,
                       c.interval_ms, c.duration_ms, c.cap, c.granted,
                       now() + c.duration_ms * interval '1 millisecond',
                       c.account, c.tier, c.session, c.session_ttl_s, c.country, c.route_fp,
                       c.via
                FROM jsonb_to_recordset(%(claims)s::jsonb)
                  AS c(target text, lease_id uuid, provider text, interval_ms int,
                       duration_ms int, cap int, granted int, egress text, account text,
                       tier text, session text, session_ttl_s int, country text,
                       route_fp text, via text)
                ON CONFLICT (target) DO UPDATE SET
                  lease_id    = CASE WHEN l.expires_at > now() THEN l.lease_id
                                     ELSE EXCLUDED.lease_id END,
                  granted     = CASE WHEN l.expires_at > now() THEN l.granted + EXCLUDED.granted
                                     ELSE EXCLUDED.granted END,
                  cap         = CASE WHEN l.expires_at > now() THEN l.cap ELSE EXCLUDED.cap END,
                  granted_at  = CASE WHEN l.expires_at > now() THEN l.granted_at ELSE now() END,
                  session     = CASE WHEN l.expires_at > now() THEN l.session
                                     ELSE EXCLUDED.session END,
                  session_ttl_s = CASE WHEN l.expires_at > now() THEN l.session_ttl_s
                                       ELSE EXCLUDED.session_ttl_s END,
                  country     = CASE WHEN l.expires_at > now() THEN l.country
                                     ELSE EXCLUDED.country END,
                  route_fp    = CASE WHEN l.expires_at > now() THEN l.route_fp
                                     ELSE EXCLUDED.route_fp END,
                  expires_at  = GREATEST(CASE WHEN l.expires_at > now() THEN l.expires_at
                                              ELSE now() END, EXCLUDED.expires_at),
                  egress      = EXCLUDED.egress,
                  holder      = EXCLUDED.holder,
                  env         = EXCLUDED.env,
                  provider    = EXCLUDED.provider,
                  interval_ms = EXCLUDED.interval_ms,
                  duration_ms = EXCLUDED.duration_ms,
                  account     = EXCLUDED.account,
                  tier        = EXCLUDED.tier,
                  via         = EXCLUDED.via,
                  revoked_at  = NULL,
                  revoke_reason = NULL
                WHERE l.expires_at <= now()
                   OR (l.egress = EXCLUDED.egress AND l.holder = EXCLUDED.holder
                       AND l.env = EXCLUDED.env AND l.revoked_at IS NULL
                       AND l.granted + EXCLUDED.granted <= l.cap)
                RETURNING l.target, l.lease_id, l.expires_at, l.interval_ms, l.provider,
                          l.cap, l.granted, l.egress, l.account, l.tier, l.session,
                          l.session_ttl_s, l.country, l.via
                """,
                {"holder": holder, "env": self.env, "claims": json.dumps(claims)},
            )
            if not claimed:
                return Grant(waiting=dict(waiting))
            by_target = {r["target"]: r for r in claimed}
            leased_rows = [r for r in locked if target_of(r["grp"], r["key"]) in by_target]
            mine = [{"task_type": r["task_type"], "key": r["key"]} for r in leased_rows]
            spent: dict[tuple[str, str, str], int] = defaultdict(int)
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
                    SELECT c.egress, c.target, date_trunc('hour', now()), c.n
                    FROM jsonb_to_recordset(%(usage)s::jsonb)
                      AS c(egress text, target text, n int)
                    ON CONFLICT (egress, target, hour)
                    DO UPDATE SET requests = p.requests + EXCLUDED.requests
                    """
                ),
                {
                    "usage": json.dumps(
                        [
                            {"egress": by_target[t]["egress"], "target": t, "n": per_target[t]}
                            for t in by_target
                        ]
                    )
                },
            )
        for t in tasks:
            t["target"] = target_of(t["group"], t["key"])
            t["target_lease_id"] = by_target[t["target"]]["lease_id"]
        tasks.sort(key=lambda r: -r["priority"])
        out = []
        for r in claimed:
            route = None
            if r["account"] is not None:
                route = routing.route(r)
            elif r["via"] == tr.VIA_EXIT:
                route = exits.route(r)
            prev = ended.get(r["target"])
            # Another lease_id: the row had ended and this claim took it over.
            not_before = prev["free_at"] if prev and prev["lease_id"] != r["lease_id"] else None
            out.append(
                TargetLease(
                    target=r["target"],
                    lease_id=r["lease_id"],
                    expires_at=r["expires_at"],
                    interval_s=r["interval_ms"] / 1000,
                    provider=r["provider"],
                    cap=r["cap"],
                    granted=r["granted"],
                    tier=r["tier"],
                    route=route,
                    not_before=not_before,
                )
            )
        return Grant(tasks=tasks, targets=out, waiting=dict(waiting))

    async def _check_egress(
        self, conn: AsyncConnection, egress: str, task_types: list[str], types: dict
    ) -> set[str]:
        rows = await self._rows(
            conn,
            "SELECT status, task_types, override_status, account, removed_at "
            "FROM {e}.egresses WHERE address = %s",
            (egress,),
        )
        if not rows or rows[0]["removed_at"] is not None:
            raise EgressRefused(f"egress {egress} is not in the registry")
        r = rows[0]
        if r["account"] is not None:
            raise EgressRefused(f"egress {egress} is a proxy exit, not a worker's address")
        status = r["override_status"] or r["status"]
        if status != "active":
            raise EgressRefused(f"egress {egress} is {status}")
        allowed = r["task_types"]
        # Only task types that list the datacenter tier may run from the worker's own
        # address; a proxy-only type is never served on it, even on a shared target.
        ok = [
            t
            for t in task_types
            if t in types
            and (allowed is None or t in allowed)
            and tr.uses_datacenter(types[t]["egress"])
        ]
        if not ok and not any(
            tr.uses_proxies(types[t]["egress"]) or tr.via_exit(types[t]["egress"]) for t in types
        ):
            raise EgressRefused(f"egress {egress} may not lease these task types")
        return set(ok)

    def _plan(self, x: _PlanContext, target: str, c: dict, has_token) -> tuple[dict | None, str]:
        """Lease parameters for a target (with ``room``: tasks it may get now), or None
        and why it waits."""
        tt = x.types[c["task_type"]]
        interval = x.budgets.targets.interval_for(_kind(c["provider"], x.kinds), target)
        cap = _cap(tt, interval)
        base = {
            "provider": c["provider"],
            "interval_ms": int(interval.total_seconds() * 1000),
            "duration_ms": int(tt["lease_duration"].total_seconds() * 1000),
            "cap": cap,
        }
        per_hour = x.budgets.pair_per_hour(target)

        def pair_left(key: str) -> int:
            if per_hour is None:
                return cap
            return per_hour - x.used.get((key, target), 0)

        lease = x.leases.get(target)
        if lease is not None and lease["live"]:
            mine = lease["holder"] == x.holder and lease["env"] == self.env
            if not mine or lease["revoked"]:
                return None, "held"
            tier = lease["tier"]
            if (target, tr.ALL_TIERS) in x.cooling or (target, tier) in x.cooling:
                return None, "cooling"
            key = lease["egress"]
            if lease["via"] == tr.VIA_EXIT:
                if not tr.via_exit(tt["egress"]) or tier != tr.DATACENTER:
                    return None, "held"
                ex = x.exits.by_key.get(key)
                if ex is None or not ex.serves(c["task_type"]):
                    return None, "exit_unavailable"
                fields = {**_DIRECT, "tier": tier, "via": tr.VIA_EXIT}
            elif lease["account"] is None:
                if key != x.egress or c["task_type"] not in x.direct:
                    return None, "held"
                fields = {**_DIRECT, "tier": tier}
            else:
                eg = x.routing.by_key.get(key)
                acc = x.routing.accounts.get(lease["account"])
                if eg is None or acc is None or not acc.usable:
                    return None, "proxy_unavailable"
                fields = x.routing.plan_fields(eg, tt)
            room = min(lease["cap"] - lease["granted"], pair_left(key))
            if room <= 0:
                return None, "cap"
            return {**base, "egress": key, **fields, "room": room}, ""

        tier = tr.effective(tt["egress"], x.on_tier.get(target))
        if (target, tr.ALL_TIERS) in x.cooling or (target, tier) in x.cooling:
            return None, "cooling"
        if tier == tr.DATACENTER and tr.via_exit(tt["egress"]):
            ex, why = x.exits.pick(c["task_type"], target, pair_left, has_token)
            if ex is None:
                return None, why
            x.exits.claim(ex)
            fields = {**_DIRECT, "tier": tier, "via": tr.VIA_EXIT}
            return {**base, "egress": ex.key, **fields, "room": min(cap, pair_left(ex.key))}, ""
        if tier == tr.DATACENTER:
            if x.egress is None or c["task_type"] not in x.direct:
                return None, "no_datacenter_egress"
            if not has_token(x.egress):
                return None, "no_tokens"
            room = min(cap, pair_left(x.egress))
            if room <= 0:
                return None, "pair_budget"
            return {**base, "egress": x.egress, **_DIRECT, "tier": tier, "room": room}, ""

        eg, why = x.routing.pick(c["task_type"], tt, tier, target, pair_left, has_token)
        if eg is None:
            return None, why
        x.routing.claim(eg)
        fields = x.routing.plan_fields(eg, tt)
        return {**base, "egress": eg.key, **fields, "room": min(cap, pair_left(eg.key))}, ""

    def _accepts(
        self, task_type: str, p: dict, types, direct, routing: _Routing, exits: _Exits
    ) -> bool:
        """A second task type on a planned target must accept the same egress."""
        if p.get("via") == tr.VIA_EXIT:
            ex = exits.by_key.get(p["egress"])
            return (
                ex is not None
                and ex.serves(task_type)
                and tr.via_exit(types[task_type]["egress"])
                and p["tier"] == tr.DATACENTER
            )
        if p["account"] is None:
            return task_type in direct
        eg = routing.by_key.get(p["egress"])
        if eg is None:
            return False
        names = tr.names_for(types[task_type]["egress"], p["tier"])
        return eg.serves(task_type) and (eg.account in names or (eg.pool or "") in names)

    async def spend(
        self,
        conn: AsyncConnection,
        egress: str,
        provider: str,
        klass: str,
        n: int,
    ) -> int:
        """Tokens for requests beyond a task's first (a retry, a fallback, a browser page
        load): up to ``n`` from this window's allocation for ``provider`` on ``egress``
        (an address or ``pool:<name>``), with the same order and lending as grants. A
        provider without a budget costs nothing. Returns the number granted."""
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
            toks = await _Tokens.load(self, conn, [egress], [provider], budgets)
            spent: dict[tuple[str, str, str], int] = defaultdict(int)
            got = 0
            while got < n:
                row = toks.take(egress, provider, k)
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

    # --- renew, release, cooldowns, revoke ------------------------------------------------------

    async def renew(
        self,
        conn: AsyncConnection,
        egress: str | None,
        holder: str,
        lease_ids: list[UUID],
        release: list[UUID] = (),  # type: ignore[assignment]
        routes: dict[UUID, dict] | None = None,
    ) -> tuple[dict[UUID, datetime], dict[UUID, str]]:
        """Extend live target leases; give back ``release`` at once (the holder has no
        more tasks for them). Returns (new expiry per renewed lease, lost lease → why).
        A routed lease matches by holder alone (it isn't on the holder's own egress); its
        route is put in ``routes`` again when the account's credentials changed."""
        async with conn.transaction():
            if release:
                await conn.execute(
                    self._q(
                        "UPDATE {e}.target_leases SET expires_at = now() "
                        "WHERE lease_id = ANY(%s) AND holder = %s "
                        "AND (egress = %s OR account IS NOT NULL OR via IS NOT NULL) "
                        "AND expires_at > now()"
                    ),
                    (list(release), holder, egress),
                )
            if not lease_ids:
                return {}, {}
            renewed = await self._rows(
                conn,
                """
                UPDATE {e}.target_leases l
                SET expires_at = GREATEST(l.expires_at,
                                          now() + l.duration_ms * interval '1 millisecond')
                WHERE l.lease_id = ANY(%(ids)s) AND l.holder = %(holder)s
                  AND l.env = %(env)s
                  AND (l.egress = %(egress)s OR l.account IS NOT NULL OR l.via IS NOT NULL)
                  AND l.expires_at > now() AND l.revoked_at IS NULL
                  AND NOT EXISTS (SELECT 1 FROM {e}.target_cooldowns c
                                  WHERE c.target = l.target AND c.until > now()
                                    AND c.tier IN ('*', l.tier))
                  AND (l.account IS NULL OR EXISTS (
                        SELECT 1 FROM {e}.proxy_accounts a
                        WHERE a.name = l.account AND a.state = 'ok'
                          AND a.status <> 'disabled' AND a.removed_at IS NULL
                          AND a.credentials IS NOT NULL AND %(env)s = ANY(a.envs)))
                  AND (l.account IS NULL OR EXISTS (
                        SELECT 1 FROM {e}.egresses x
                        WHERE host(x.address) = l.egress AND x.override_status IS NULL
                          AND x.status <> 'disabled' AND x.removed_at IS NULL
                        UNION ALL
                        SELECT 1 FROM {e}.proxy_pools p
                        WHERE 'pool:' || p.name = l.egress AND p.override_status IS NULL
                          AND p.status <> 'disabled' AND p.removed_at IS NULL))
                  AND (l.via IS NULL OR EXISTS (
                        SELECT 1 FROM {e}.egresses x
                        WHERE host(x.address) = l.egress AND x.exit_endpoint IS NOT NULL
                          AND x.exit_override IS NULL AND x.override_status IS NULL
                          AND x.status <> 'disabled' AND x.removed_at IS NULL))
                RETURNING l.lease_id, l.expires_at, l.target, l.egress, l.account, l.tier,
                          l.session, l.session_ttl_s, l.country, l.route_fp, l.via
                """,
                {"ids": list(lease_ids), "egress": egress, "holder": holder, "env": self.env},
            )
            ok = {r["lease_id"]: r["expires_at"] for r in renewed}
            routed = [r for r in renewed if r["account"] is not None]
            if routes is not None and routed:
                await self._refresh_routes(conn, routed, routes)
            via_exit = [r for r in renewed if r["via"] == tr.VIA_EXIT]
            if routes is not None and via_exit:
                # The exit token expires with the lease: a renewal always signs a new one.
                for r, route in (await _Exits.routes_for(self, conn, via_exit)).items():
                    routes[r] = route
            missing = [i for i in lease_ids if i not in ok]
            lost: dict[UUID, str] = {}
            if missing:
                rows = await self._rows(
                    conn,
                    """
                    SELECT l.lease_id, l.expires_at <= now() AS expired, l.revoke_reason,
                           l.revoked_at IS NOT NULL AS revoked, l.account, l.via, l.holder,
                           (SELECT c.reason FROM {e}.target_cooldowns c
                            WHERE c.target = l.target AND c.until > now()
                              AND c.tier IN ('*', l.tier) LIMIT 1) AS cooling,
                           a.state AS account_state
                    FROM {e}.target_leases l
                    LEFT JOIN {e}.proxy_accounts a ON a.name = l.account
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
                    elif r["account"] is not None and r["account_state"] != "ok":
                        why[r["lease_id"]] = f"proxy account {r['account']} is {r['account_state']}"
                    elif r["account"] is not None:
                        why[r["lease_id"]] = "proxy egress or account disabled"
                    elif r["via"] is not None and r["holder"] == holder:
                        why[r["lease_id"]] = "exit out of service"
                for i in missing:
                    lost[i] = why.get(i, "not held")
        return ok, lost

    async def _refresh_routes(
        self, conn: AsyncConnection, rows: list[dict], routes: dict[UUID, dict]
    ) -> None:
        accounts = await px.load_accounts(conn, self.schema)
        changed = [
            r
            for r in rows
            if r["account"] in accounts and accounts[r["account"]].credentials_fp != r["route_fp"]
        ]
        if not changed or not self.keyring.configured:
            return
        by_key = {e.key: e for e in await px.load_proxy_egresses(conn, self.schema)}
        for r in changed:
            acc = accounts[r["account"]]
            eg = by_key.get(r["egress"])
            if eg is None or not acc.usable:
                continue
            try:
                routes[r["lease_id"]] = px.render_route(
                    eg,
                    acc,
                    self.keyring,
                    country=r["country"],
                    session=r["session"],
                    session_ttl_s=r["session_ttl_s"] if r["session"] else None,
                    expires_at=r["expires_at"],
                )
            except SecretsError:
                continue
            await conn.execute(
                self._q("UPDATE {e}.target_leases SET route_fp = %s WHERE lease_id = %s"),
                (acc.credentials_fp, r["lease_id"]),
            )

    async def cool(
        self,
        conn: AsyncConnection,
        items: list[Cooling],
        egress: str | None,
        max_after: timedelta,
        escalated: list | None = None,
    ) -> int:
        """Record pushback. A block (``challenge``, ``block``) cools the target on the tier
        it happened on and escalates it to the task type's next tier; anything else cools
        it on every tier. A longer cooldown already recorded is kept. Returns the number of
        cooldowns written or extended; ``escalated`` gets (target, tier, task type)."""
        if not items:
            return 0
        on = await self.leases_of(conn, sorted({i.target for i in items}))
        names = sorted({i.task_type for i in items if i.task_type})
        types = {}
        if names:
            types = {
                r["name"]: r["egress"]
                for r in await self._rows(
                    conn, "SELECT name, egress FROM {f}.task_types WHERE name = ANY(%s)", (names,)
                )
            }
        data = []
        escalate = []
        for i in items:
            tier = tr.ALL_TIERS
            if i.cause in tr.ESCALATING:
                tier = on[i.target]["tier"] if i.target in on else tr.DATACENTER
                if i.task_type in types:
                    nxt = tr.next_tier(types[i.task_type], tier)
                    if nxt is not None:
                        escalate.append((i.target, nxt, i.reason, i.task_type))
            data.append(
                {
                    "target": i.target,
                    "tier": tier,
                    "after_s": min(i.after, max_after).total_seconds(),
                    "reason": i.reason[:500],
                    "cause": i.cause if i.cause in tr.CAUSES else None,
                    "egress": on[i.target]["egress"] if i.target in on else egress,
                }
            )
        async with conn.transaction():
            cur = await conn.execute(
                self._q(
                    """
                    INSERT INTO {e}.target_cooldowns AS c
                      (target, tier, until, reason, cause, egress, env)
                    SELECT i.target, i.tier, now() + i.after_s * interval '1 second', i.reason,
                           i.cause, i.egress, %(env)s
                    FROM jsonb_to_recordset(%(items)s::jsonb)
                      AS i(target text, tier text, after_s float8, reason text, cause text,
                           egress text)
                    ON CONFLICT (target, tier) DO UPDATE SET
                      until = GREATEST(c.until, EXCLUDED.until),
                      reason = CASE WHEN EXCLUDED.until > c.until THEN EXCLUDED.reason
                                    ELSE c.reason END,
                      cause = CASE WHEN EXCLUDED.until > c.until THEN EXCLUDED.cause
                                   ELSE c.cause END,
                      egress = CASE WHEN EXCLUDED.until > c.until THEN EXCLUDED.egress
                                    ELSE c.egress END,
                      env = CASE WHEN EXCLUDED.until > c.until THEN EXCLUDED.env ELSE c.env END,
                      updated_at = now()
                    """
                ),
                {"env": self.env, "items": json.dumps(data)},
            )
            n = cur.rowcount
            if escalate:
                ttl = (await self.budgets(conn)).proxies.tier_ttl
                for target, tier, reason, task_type in escalate:
                    await self._set_tier(
                        conn, target, tier, f"block: {reason}"[:500], None, task_type, ttl
                    )
                    if escalated is not None:
                        escalated.append((target, tier, task_type))
            return n

    async def _set_tier(
        self,
        conn,
        target: str,
        tier: str,
        reason: str,
        evidence: Any,
        task_type: str,
        ttl: timedelta,
    ) -> None:
        """Move a target up to ``tier``, never down (a block on a lower tier from another
        task type doesn't undo an escalation), unless the old row is past ``tier_ttl``."""
        await conn.execute(
            self._q(
                """
                INSERT INTO {e}.target_tiers AS t (target, tier, reason, evidence, task_type, env)
                VALUES (%(target)s, %(tier)s, %(reason)s, %(evidence)s, %(tt)s, %(env)s)
                ON CONFLICT (target) DO UPDATE SET
                  tier = EXCLUDED.tier, since = now(), reason = EXCLUDED.reason,
                  evidence = EXCLUDED.evidence, task_type = EXCLUDED.task_type,
                  env = EXCLUDED.env
                WHERE array_position(%(order)s::text[], t.tier)
                      < array_position(%(order)s::text[], EXCLUDED.tier)
                   OR t.since <= now() - %(ttl)s
                """
            ),
            {
                "target": target,
                "tier": tier,
                "reason": reason,
                "evidence": json.dumps(evidence) if evidence is not None else None,
                "tt": task_type,
                "env": self.env,
                "order": list(tr.TIERS),
                "ttl": ttl,
            },
        )

    async def escalate(
        self, conn: AsyncConnection, target: str, task_type: str, reason: str, evidence: Any
    ) -> tuple[str, str | None]:
        """Move a target to the task type's next tier (an outside verdict such as
        ``unreachable_from_dc``). Returns (tier it was on, tier it is on now; None when it
        was on the task type's last tier). A live lease on a lower tier is revoked."""
        rows = await self._rows(
            conn, "SELECT egress FROM {f}.task_types WHERE name = %s", (task_type,)
        )
        if not rows:
            raise KeyError(task_type)
        spec = rows[0]["egress"]
        ttl = (await self.budgets(conn)).proxies.tier_ttl
        cur = await self._rows(
            conn,
            "SELECT tier FROM {e}.target_tiers WHERE target = %s AND since > now() - %s",
            (target, ttl),
        )
        current = tr.effective(spec, cur[0]["tier"] if cur else None)
        nxt = tr.next_tier(spec, current)
        if nxt is None:
            return current, None
        async with conn.transaction():
            await self._set_tier(conn, target, nxt, reason[:500], evidence, task_type, ttl)
            await conn.execute(
                self._q(
                    "UPDATE {e}.target_leases SET revoked_at = now(), revoke_reason = %s "
                    "WHERE target = %s AND expires_at > now() AND revoked_at IS NULL "
                    "AND array_position(%s::text[], tier) < array_position(%s::text[], %s)"
                ),
                (f"escalated to {nxt}", target, list(tr.TIERS), list(tr.TIERS), nxt),
            )
        return current, nxt

    async def clear(
        self,
        conn: AsyncConnection,
        target: str,
        source: str,
        reason: str,
        evidence: Any,
        caller: str,
    ) -> list[dict]:
        """End a target's cooldowns that ``source`` may lift (``CLEARABLE_BY``). Every
        cooldown looked at gets an audit row in ``target_clears``. Returns the cleared
        cooldowns; raises ``ClearRefused`` when there were some and none could be lifted."""
        async with conn.transaction():
            rows = await self._rows(
                conn,
                "SELECT tier, until, reason, cause, egress, env FROM {e}.target_cooldowns "
                "WHERE target = %s AND until > now() FOR UPDATE",
                (target,),
            )
            cleared, refused = [], []
            for r in rows:
                ok = source in CLEARABLE_BY.get(r["cause"], ())
                if ok:
                    await conn.execute(
                        self._q(
                            "UPDATE {e}.target_cooldowns SET until = now(), updated_at = now() "
                            "WHERE target = %s AND tier = %s"
                        ),
                        (target, r["tier"]),
                    )
                    cleared.append(r)
                else:
                    refused.append(r)
                await conn.execute(
                    self._q(
                        """
                        INSERT INTO {e}.target_clears
                          (target, cleared, refused, cooldown_until, cooldown_reason,
                           cooldown_cause, cooldown_egress, cooldown_env, source, reason,
                           evidence, caller, env)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                    ),
                    (
                        target,
                        ok,
                        None if ok else f"{source} may not clear cause {r['cause']}",
                        r["until"],
                        r["reason"],
                        r["cause"],
                        r["egress"],
                        r["env"],
                        source,
                        reason[:500],
                        json.dumps(evidence) if evidence is not None else None,
                        caller,
                        self.env,
                    ),
                )
        if refused and not cleared:
            causes = [r["cause"] for r in refused]
            raise ClearRefused(
                f"{source} may not clear this cooldown (cause: "
                f"{', '.join(str(c) for c in causes)})",
                causes,
            )
        return cleared

    async def recent_clears(self, conn: AsyncConnection, targets: list[str]) -> list[dict]:
        """Clears within the last hour of these targets (to count clears that were wrong)."""
        if not targets:
            return []
        return await self._rows(
            conn,
            "SELECT DISTINCT target, source, cooldown_cause FROM {e}.target_clears "
            "WHERE target = ANY(%s) AND cleared AND at > now() - interval '1 hour'",
            (targets,),
        )

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

    async def leases_of(self, conn: AsyncConnection, targets: list[str]) -> dict[str, dict]:
        """The current target lease row per target (egress, account, tier, holder)."""
        if not targets:
            return {}
        rows = await self._rows(
            conn,
            "SELECT target, lease_id, egress, account, tier, holder, env, via, "
            "expires_at > now() AS live FROM {e}.target_leases WHERE target = ANY(%s)",
            (targets,),
        )
        return {r["target"]: r for r in rows}

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

    async def release_targets(self, conn: AsyncConnection, leases: list[tuple[str, UUID]]) -> int:
        """End these target leases (target, lease id) now, without a cooldown (a proxy
        error: the target did nothing wrong). A lease that rolled over to another lease id
        meanwhile is left alone."""
        if not leases:
            return 0
        cur = await conn.execute(
            self._q(
                """
                UPDATE {e}.target_leases l SET expires_at = now()
                FROM jsonb_to_recordset(%s::jsonb) AS c(target text, lease_id uuid)
                WHERE l.target = c.target AND l.lease_id = c.lease_id
                  AND l.expires_at > now() AND l.env = %s
                """
            ),
            (json.dumps([{"target": t, "lease_id": str(i)} for t, i in leases]), self.env),
        )
        return cur.rowcount

    async def sweep(self, conn: AsyncConnection, keep: timedelta = timedelta(hours=1)) -> int:
        """Delete expired leases, cooldowns and tiers, pair usage older than two hours, old
        proxy failure rows, and removed exits and pools past ``proxies.remove_after``. For
        the schema owner's API only (other environments may not delete)."""
        n = 0
        budgets = await self.budgets(conn)
        params = {
            "keep": keep,
            "tier_ttl": budgets.proxies.tier_ttl,
            "remove": budgets.proxies.remove_after,
        }
        proxies = await px.proxy_tables_exist(conn, self.schema)
        stmts = [
            "DELETE FROM {e}.target_leases WHERE expires_at < now() - %(keep)s",
            "DELETE FROM {e}.target_cooldowns WHERE until < now() - %(keep)s",
            "DELETE FROM {e}.pair_usage WHERE hour < date_trunc('hour', now()) - "
            "interval '2 hours'",
        ]
        if proxies:
            stmts += [
                "DELETE FROM {e}.target_tiers WHERE since < now() - %(tier_ttl)s",
                "DELETE FROM {e}.proxy_failures WHERE at < now() - interval '1 hour'",
                "DELETE FROM {e}.proxy_pools p WHERE removed_at < now() - %(remove)s "
                "AND NOT EXISTS (SELECT 1 FROM {e}.target_leases l "
                "WHERE l.egress = 'pool:' || p.name)",
                "DELETE FROM {e}.egresses x WHERE account IS NOT NULL "
                "AND removed_at < now() - %(remove)s "
                "AND NOT EXISTS (SELECT 1 FROM {e}.target_leases l "
                "WHERE l.egress = host(x.address)) "
                "AND NOT EXISTS (SELECT 1 FROM {e}.allocations a "
                "WHERE a.egress = host(x.address))",
                "DELETE FROM {e}.proxy_exits p WHERE removed_at < now() - %(remove)s "
                "AND NOT EXISTS (SELECT 1 FROM {e}.egresses x WHERE x.exit_id = p.id)",
            ]
        async with conn.transaction():
            for stmt in stmts:
                cur = await conn.execute(self._q(stmt), params)
                n += cur.rowcount
        return n


@dataclass
class _PlanContext:
    types: dict
    budgets: Budgets
    kinds: dict
    leases: dict
    cooling: set
    on_tier: dict
    used: dict
    egress: str | None
    direct: set
    holder: str
    routing: _Routing
    exits: _Exits


class _Routing:
    """The proxy egresses and accounts a lease call may route through."""

    def __init__(self, d: Dispatcher):
        self.d = d
        self.accounts: dict[str, px.Account] = {}
        self.egresses: list[px.ProxyEgress] = []
        self.by_key: dict[str, px.ProxyEgress] = {}
        self.open_account: dict[str, int] = defaultdict(int)
        self.open_key: dict[str, int] = defaultdict(int)
        self.keys: set[str] = set()
        self.stopped: set[Stopped] = set()

    @classmethod
    async def load(cls, d: Dispatcher, conn: AsyncConnection, types: dict) -> _Routing:
        r = cls(d)
        if not any(tr.uses_proxies(t["egress"]) for t in types.values()):
            return r
        if not d.keyring.configured or not await px.proxy_tables_exist(conn, d.schema):
            return r  # fail closed: no routes without the tables or a key
        r.accounts = {
            name: a
            for name, a in (await px.load_accounts(conn, d.schema)).items()
            if d.env in a.envs
        }
        r.egresses = [
            e for e in await px.load_proxy_egresses(conn, d.schema) if e.account in r.accounts
        ]
        r.by_key = {e.key: e for e in r.egresses}
        wanted: set[str] = set()
        for name, t in types.items():
            stops = (t["egress"] or {}).get("stop_at_used", {})
            for tier in tr.tier_names(t["egress"]):
                if tier == tr.DATACENTER:
                    continue
                for n in tr.names_for(t["egress"], tier):
                    wanted.add(n)
                    acc = r.accounts.get(n)
                    stop = stops.get(tier)
                    if acc is not None and stop is not None and _stopped(acc, stop):
                        r.stopped.add(Stopped(name, acc.name, acc.remaining.used_ratio or 0, stop))
        r.keys = {e.key for e in r.egresses if e.account in wanted or (e.pool or "") in wanted}
        limited = any(
            e.concurrency is not None or r.accounts[e.account].concurrency is not None
            for e in r.egresses
            if e.key in r.keys
        )
        if limited:
            # Concurrency is counted from open leases, so two dispatchers counting at once
            # could both take the last slot. Routed lease calls on limited egresses take
            # this lock until their claims commit.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{d.schema}:proxy-concurrency",)
            )
        for row in await d._rows(
            conn,
            "SELECT account, egress, count(*) AS n FROM {e}.target_leases "
            "WHERE account IS NOT NULL AND expires_at > now() AND revoked_at IS NULL "
            "GROUP BY account, egress",
        ):
            r.open_account[row["account"]] += row["n"]
            r.open_key[row["egress"]] += row["n"]
        return r

    def _eligible(self, eg: px.ProxyEgress, task_type: str, tt: dict, tier: str) -> str:
        """'' when ``eg`` may carry the route, else why not."""
        acc = self.accounts.get(eg.account)
        if acc is None or not acc.usable:
            return "account_unavailable"
        if eg.kind != tier or not eg.serves(task_type):
            return "no_egress"
        spec = tt["egress"] or {}
        stop = spec.get("stop_at_used", {}).get(tier)
        if stop is not None and _stopped(acc, stop):
            return "stopped"
        if eg.country_for(spec.get("countries") or []) is None:
            return "country"
        sticky = (spec.get("session") or {}).get("mode") == "sticky"
        if sticky and eg.pool is not None and not eg.sticky:
            return "no_sticky"
        if acc.concurrency is not None and self.open_account[acc.name] >= acc.concurrency:
            return "concurrency"
        if eg.concurrency is not None and self.open_key[eg.key] >= eg.concurrency:
            return "concurrency"
        return ""

    def pick(self, task_type, tt, tier, target, pair_left, has_token):
        """The first eligible egress of the tier's names, in order; within one name the
        least loaded. Returns (egress, '') or (None, why the target waits)."""
        names = tr.names_for(tt["egress"], tier)
        if not names:
            return None, "no_names"
        why = "no_egress"
        for name in names:
            options = []
            for eg in self.egresses:
                if eg.account != name and eg.pool != name:
                    continue
                w = self._eligible(eg, task_type, tt, tier)
                if w:
                    why = w
                    continue
                if pair_left(eg.key) <= 0:
                    why = "pair_budget"
                    continue
                if not has_token(eg.key):
                    why = "no_tokens"
                    continue
                options.append(eg)
            if options:
                options.sort(key=lambda e: (self.open_key[e.key], _spread(target, e.key)))
                return options[0], ""
        return None, why

    def claim(self, eg: px.ProxyEgress) -> None:
        self.open_account[eg.account] += 1
        self.open_key[eg.key] += 1

    def plan_fields(self, eg: px.ProxyEgress, tt: dict) -> dict:
        acc = self.accounts[eg.account]
        spec = tt["egress"] or {}
        sticky_ttl = None
        session = spec.get("session") or {}
        if session.get("mode") == "sticky" and eg.pool is not None and eg.sticky:
            duration = int(tt["lease_duration"].total_seconds())
            sticky_ttl = max(int(session.get("ttl_s") or 0), duration)
            if eg.max_sticky_ttl_s:
                sticky_ttl = min(sticky_ttl, eg.max_sticky_ttl_s)
        return {
            "account": eg.account,
            "tier": eg.kind,
            "route_fp": acc.credentials_fp,
            "sticky_ttl_s": sticky_ttl,
            "country": eg.country_for(spec.get("countries") or []),
        }

    def route(self, row: dict) -> dict | None:
        """The route for a claimed lease row, with its session and country as stored."""
        eg = self.by_key.get(row["egress"])
        acc = self.accounts.get(row["account"])
        if eg is None or acc is None:
            return None
        try:
            return px.render_route(
                eg,
                acc,
                self.d.keyring,
                country=row["country"],
                session=row["session"],
                session_ttl_s=row["session_ttl_s"] if row["session"] else None,
                expires_at=row["expires_at"],
            )
        except SecretsError:
            return None


@dataclass(frozen=True)
class RegistryExit:
    """A registry egress that runs an exit (``leasehold.exit``)."""

    key: str  # the egress address
    endpoint: str
    country: str | None = None
    task_types: tuple[str, ...] | None = None

    def serves(self, task_type: str) -> bool:
        return self.task_types is None or task_type in self.task_types


_EXITS_SQL = """
    SELECT host(address) AS key, exit_endpoint, country, task_types FROM {e}.egresses
    WHERE source = 'registry' AND exit_endpoint IS NOT NULL AND exit_override IS NULL
      AND status = 'active' AND override_status IS NULL AND removed_at IS NULL
"""


def _exit_of(r: dict) -> RegistryExit:
    return RegistryExit(
        key=r["key"],
        endpoint=r["exit_endpoint"],
        country=r["country"],
        task_types=tuple(r["task_types"]) if r["task_types"] is not None else None,
    )


class _Exits:
    """The registry exits a lease call may route through."""

    def __init__(self, d: Dispatcher):
        self.d = d
        self.by_key: dict[str, RegistryExit] = {}
        self.open: dict[str, int] = defaultdict(int)

    @property
    def keys(self) -> set[str]:
        return set(self.by_key)

    @classmethod
    async def load(cls, d: Dispatcher, conn: AsyncConnection, types: dict) -> _Exits:
        x = cls(d)
        if d.exit_key is None or not any(tr.via_exit(t["egress"]) for t in types.values()):
            return x  # fail closed: no exit routes without a key
        x.by_key = {r["key"]: _exit_of(r) for r in await d._rows(conn, _EXITS_SQL)}
        for row in await d._rows(
            conn,
            "SELECT egress, count(*) AS n FROM {e}.target_leases "
            "WHERE via IS NOT NULL AND expires_at > now() AND revoked_at IS NULL "
            "GROUP BY egress",
        ):
            x.open[row["egress"]] = row["n"]
        return x

    def pick(self, task_type, target, pair_left, has_token):
        """The least loaded exit that serves the task type and has budget left."""
        if not self.by_key:
            return None, "no_exit"
        why = "no_exit"
        options = []
        for ex in self.by_key.values():
            if not ex.serves(task_type):
                continue
            if pair_left(ex.key) <= 0:
                why = "pair_budget"
                continue
            if not has_token(ex.key):
                why = "no_tokens"
                continue
            options.append(ex)
        if not options:
            return None, why
        options.sort(key=lambda e: (self.open[e.key], _spread(target, e.key)))
        return options[0], ""

    def claim(self, ex: RegistryExit) -> None:
        self.open[ex.key] += 1

    def route(self, row: dict) -> dict | None:
        ex = self.by_key.get(row["egress"])
        if ex is None or self.d.exit_key is None:
            return None
        return exit_route(self.d.exit_key, ex, row, self.d.env)

    @staticmethod
    async def routes_for(d: Dispatcher, conn: AsyncConnection, rows: list[dict]) -> dict:
        if d.exit_key is None:
            return {}
        by_key = {r["key"]: _exit_of(r) for r in await d._rows(conn, _EXITS_SQL)}
        out = {}
        for r in rows:
            ex = by_key.get(r["egress"])
            if ex is not None:
                out[r["lease_id"]] = exit_route(d.exit_key, ex, r, d.env)
        return out


def exit_route(key: bytes, ex: RegistryExit, row: dict, env: str) -> dict:
    """The route of a target lease through a registry exit. Holds the exit token: never
    log it."""
    scheme, host, port = px.parse_endpoint(ex.endpoint)
    user, password = exit_token.sign_lease(
        key, str(row["lease_id"]), ex.key, row["target"], env, row["expires_at"] + EXIT_TOKEN_GRACE
    )
    return {
        "egress": ex.key,
        "kind": tr.DATACENTER,
        "country": ex.country,
        "account": EXIT_ACCOUNT,
        "pool": None,
        "proxy": {
            "scheme": scheme,
            "host": host,
            "port": port,
            "username": user,
            "password": password,
        },
        "session": None,
        "exit_ip": ex.key,
        "exit_ip_header": None,
        "account_remaining": None,
        "errors": [],
        "credentials_fp": None,
        "expires_at": row["expires_at"].isoformat(),
    }


def _stopped(acc: px.Account, stop: float) -> bool:
    used = acc.remaining.used_ratio
    return used is not None and used >= stop


class _Tokens:
    """Egresses' allocations in the current window, for some providers.

    ``take`` uses the class's own tokens, then class ``*``, then, once ``lend_after``
    of the window has passed, what a higher class will not need in the rest of the
    window: its tokens left minus its rate so far times the time left. Nothing is lent
    ahead or carried into the next window."""

    def __init__(self, d: Dispatcher, rows: list[dict], budgets: Budgets):
        self._d = d
        self.order = budgets.classes.order
        self.lend_after = budgets.planning.lend_after
        self.left: dict[tuple[str, str, str], int] = {}
        self.lendable: dict[tuple[str, str, str], int] = {}
        for r in rows:
            key = (r["egress"], r["provider"], r["class"])
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
    async def load(cls, d: Dispatcher, conn, egresses: list[str], providers: list[str], budgets):
        rows = []
        if providers and egresses:
            rows = await d._rows(
                conn,
                "SELECT egress, provider, class, tokens, used, now() - window_start AS elapsed, "
                "window_end - now() AS remaining FROM {e}.allocations "
                "WHERE egress = ANY(%s) AND provider = ANY(%s) "
                "AND window_start <= now() AND window_end > now() "
                "ORDER BY egress, provider, class FOR UPDATE",
                (egresses, providers),
            )
        return cls(d, rows, budgets)

    def available(self, egress: str, provider: str, klass: str) -> bool:
        if any(self.left.get((egress, provider, k), 0) > 0 for k in (klass, ANY_CLASS)):
            return True
        rank = self.order.index(klass) if klass in self.order else len(self.order)
        return any(self.lendable.get((egress, provider, h), 0) > 0 for h in self.order[:rank])

    def take(self, egress: str, provider: str, klass: str) -> tuple[str, str, str] | None:
        for k in (klass, ANY_CLASS):
            key = (egress, provider, k)
            if self.left.get(key, 0) > 0:
                self.left[key] -= 1
                self.lendable[key] = min(self.lendable.get(key, 0), self.left[key])
                return key
        rank = self.order.index(klass) if klass in self.order else len(self.order)
        for higher in self.order[:rank]:
            key = (egress, provider, higher)
            if self.lendable.get(key, 0) > 0:
                self.lendable[key] -= 1
                self.left[key] -= 1
                return key
        return None

    async def write(self, conn, spent: dict[tuple[str, str, str], int]) -> None:
        for (egress, provider, k), n in spent.items():
            if n > 0:
                await conn.execute(
                    self._d._q(
                        "UPDATE {e}.allocations SET used = used + %s WHERE egress = %s "
                        "AND provider = %s AND class = %s "
                        "AND window_start <= now() AND window_end > now()"
                    ),
                    (n, egress, provider, k),
                )


def _cap(tt: dict, interval: timedelta) -> int:
    if tt.get("target_cap"):
        return int(tt["target_cap"])
    return max(1, int(tt["lease_duration"] / interval))
