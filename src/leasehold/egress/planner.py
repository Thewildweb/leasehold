"""The budget planner: allocations per (egress, provider, class, window).

Every window (``budgets.yml`` ``planning.window``, one minute) the schema owner's
control API plans the next window from:

- the budget per egress for each provider with one (``providers``; shared IPs
  ``ip:<address>`` from ``shared_ip``), cut down by the network level: an egress gets
  at most its even share of its /24's and its ASN's budget for the provider;
- for shared IPs, the fleet cap (``fleet_per_second``) spread over as few egresses as
  needed;
- the demand: due tasks per (provider, class) within the horizon, which each
  environment's API publishes (``egress.demand``), spread evenly over the horizon's
  windows.

Per provider, classes are served in order (``classes.order``): each gets tokens up to
its demand, after the last class's guaranteed minimum (``min_share``) is set aside when
that class has demand. What is left goes to class ``*``, which any class may draw on
once its own allocation is used. Each class's tokens are spread over the egresses in
proportion to their capacity.

The dispatcher deducts tokens at grant time (one per task), from the task's class
first, then ``*``. A provider with a budget and no allocation row gets nothing: without
a plan, budgeted providers are closed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from leasehold import naming
from leasehold.egress.config import SHARED_IP_PREFIX, Budgets
from leasehold.egress.schema import SCHEMA

ANY_CLASS = "*"


@dataclass(frozen=True)
class EgressRow:
    address: str
    subnet: str
    asn: int | None


@dataclass(frozen=True)
class Allocation:
    egress: str
    provider: str
    klass: str
    tokens: int


def effective_class(klass: str, env: str, budgets: Budgets) -> str:
    """The class a task type's class counts as in an environment: outside prod it moves
    ``dev_offset`` places down the order (never past the last). Unknown classes count as
    the last."""
    order = budgets.classes.order
    i = order.index(klass) if klass in order else len(order) - 1
    if env != naming.PROD:
        i = min(i + budgets.classes.dev_offset, len(order) - 1)
    return order[i]


def capacities(egresses: list[EgressRow], per_egress: float, budgets: Budgets) -> dict[str, float]:
    """Tokens per window each egress may send to one provider, with the network level:
    at most an even share of its /24's and its ASN's budget."""
    window_min = budgets.planning.window / timedelta(minutes=1)
    per_subnet = budgets.network.per_subnet
    per_asn = budgets.network.per_asn
    n_subnet: dict[str, int] = defaultdict(int)
    n_asn: dict[int | None, int] = defaultdict(int)
    for e in egresses:
        n_subnet[e.subnet] += 1
        n_asn[e.asn] += 1
    out = {}
    for e in egresses:
        cap = per_egress
        if per_subnet is not None:
            cap = min(cap, per_subnet / n_subnet[e.subnet])
        if per_asn is not None and e.asn is not None:
            cap = min(cap, per_asn / n_asn[e.asn])
        out[e.address] = cap * window_min
    return out


def _spread(total: int, caps: dict[str, float]) -> dict[str, int]:
    """``total`` tokens over egresses in proportion to ``caps``, whole tokens, never
    more than an egress's cap (rounded down)."""
    whole = {e: math.floor(c) for e, c in caps.items()}
    room = sum(whole.values())
    total = min(total, room)
    if total <= 0 or room <= 0:
        return {}
    exact = {e: total * w / room for e, w in whole.items()}
    out = {e: min(whole[e], math.floor(x)) for e, x in exact.items()}
    left = total - sum(out.values())
    # Largest remainders first; ties by address, so the result is stable.
    for e in sorted(exact, key=lambda e: (-(exact[e] - math.floor(exact[e])), e)):
        if left <= 0:
            break
        if out[e] < whole[e]:
            out[e] += 1
            left -= 1
    return {e: n for e, n in out.items() if n > 0}


def _fewest(egresses: list[EgressRow], provider: str, n: int) -> list[EgressRow]:
    """``n`` egresses for a shared IP, picked by a hash of the provider so different
    shared IPs land on different egresses."""

    def key(e: EgressRow) -> str:
        return hashlib.sha256(f"{provider}|{e.address}".encode()).hexdigest()

    return sorted(egresses, key=key)[:n]


def plan_window(
    egresses: list[EgressRow],
    budgets_per_egress: dict[str, int],
    demand: dict[tuple[str, str], int],
    budgets: Budgets,
) -> list[Allocation]:
    """Allocations for one window.

    ``budgets_per_egress``: provider → requests per minute per egress, for providers with
    a budget; ``demand``: (provider, class) → tasks due within the horizon. Shared-IP
    providers (``ip:``) are planned only when they have demand."""
    order = budgets.classes.order
    windows = max(1.0, budgets.planning.horizon / budgets.planning.window)
    window_min = budgets.planning.window / timedelta(minutes=1)
    providers = set(budgets_per_egress) | {p for p, _ in demand if p.startswith(SHARED_IP_PREFIX)}
    out: list[Allocation] = []
    for provider in sorted(providers):
        if provider.startswith(SHARED_IP_PREFIX):
            if budgets.shared_ip is None:
                continue
            fleet = budgets.shared_ip.fleet_per_second * 60 * window_min
            per = budgets.shared_ip.per_egress
            n = max(1, math.ceil(fleet / max(per * window_min, 1)))
            chosen = _fewest(egresses, provider, n)
            caps = capacities(chosen, per, budgets)
            scale = min(1.0, fleet / max(sum(caps.values()), 1e-9))
            caps = {e: c * scale for e, c in caps.items()}
        else:
            caps = capacities(egresses, budgets_per_egress[provider], budgets)
        capacity = math.floor(sum(caps.values()))
        if capacity <= 0:
            continue
        want = {k: math.ceil(demand.get((provider, k), 0) / windows) for k in order}
        last = order[-1]
        floor_last = 0
        share = budgets.classes.min_share.get(last, 0.0)
        if want[last] > 0 and share > 0:
            floor_last = min(want[last], math.floor(capacity * share))
        left = capacity - floor_last
        given: dict[str, int] = {}
        for k in order:
            extra = floor_last if k == last else 0
            take = min(max(want[k] - extra, 0), left)
            given[k] = take + extra
            left -= take
        given[ANY_CLASS] = left
        # Spread class by class, so egress capacity is never handed out twice.
        remaining = dict(caps)
        for k in [*order, ANY_CLASS]:
            if given.get(k, 0) <= 0:
                continue
            parts = _spread(given[k], remaining)
            for e, n in parts.items():
                remaining[e] -= n
                out.append(Allocation(e, provider, k, n))
    return out


# --- database ---------------------------------------------------------------------------------


class Planner:
    """Reads the registry, settings and demand, and writes the next window's allocations.
    Runs in the schema owner's API only (``LEASEHOLD_EGRESS_OWNER``)."""

    def __init__(self, egress_schema: str = SCHEMA):
        self.schema = egress_schema
        self._e = sql.Identifier(egress_schema)

    def _q(self, stmt: str) -> sql.Composed:
        return sql.SQL(stmt).format(e=self._e)

    async def _rows(self, conn, stmt, params=None) -> list[dict]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(self._q(stmt), params)
            return await cur.fetchall()

    async def plan(self, conn: AsyncConnection, window_start: datetime | None = None) -> int:
        """Plan one window (default: the next one) unless it is planned already.
        Returns the number of allocation rows written."""
        async with conn.transaction():
            got = (
                await (
                    await conn.execute(
                        "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                        (f"leasehold-egress-planner:{self.schema}",),
                    )
                ).fetchone()
            )[0]
            if not got:
                return 0
            settings = {
                r["name"]: r["value"]
                for r in await self._rows(conn, "SELECT name, value FROM {e}.settings")
            }
            budgets = Budgets.model_validate({k: v for k, v in settings.items() if v})
            window_s = int(budgets.planning.window.total_seconds())
            if window_start is None:
                window_start = (
                    await (
                        await conn.execute(
                            "SELECT to_timestamp((floor(extract(epoch FROM now()) / %s) + 1) * %s)",
                            (window_s, window_s),
                        )
                    ).fetchone()
                )[0]
            done = await self._rows(
                conn,
                "SELECT 1 FROM {e}.allocations WHERE window_start = %s LIMIT 1",
                (window_start,),
            )
            if done:
                return 0
            egresses = [
                EgressRow(r["address"], r["subnet"], r["asn"])
                for r in await self._rows(
                    conn,
                    "SELECT host(address) AS address, subnet::text AS subnet, asn "
                    "FROM {e}.egresses WHERE status = 'active' AND kind = 'datacenter' "
                    "ORDER BY address",
                )
            ]
            per_egress = {
                r["name"]: r["per_egress_per_min"]
                for r in await self._rows(
                    conn,
                    "SELECT name, per_egress_per_min FROM {e}.providers "
                    "WHERE per_egress_per_min IS NOT NULL",
                )
            }
            demand: dict[tuple[str, str], int] = defaultdict(int)
            for r in await self._rows(
                conn,
                "SELECT provider, class, sum(due)::int AS due FROM {e}.demand "
                "WHERE updated_at > now() - %s GROUP BY provider, class",
                (budgets.planning.window * 5,),
            ):
                demand[(r["provider"], r["class"])] += r["due"]
            allocations = plan_window(egresses, per_egress, demand, budgets)
            if allocations:
                await conn.execute(
                    self._q(
                        """
                        INSERT INTO {e}.allocations
                          (window_start, window_end, egress, provider, class, tokens)
                        SELECT %(start)s, %(start)s + %(len)s, a.egress::inet, a.provider,
                               a.class, a.tokens
                        FROM jsonb_to_recordset(%(rows)s::jsonb)
                          AS a(egress text, provider text, class text, tokens int)
                        """
                    ),
                    {
                        "start": window_start,
                        "len": budgets.planning.window,
                        "rows": json.dumps(
                            [
                                {
                                    "egress": a.egress,
                                    "provider": a.provider,
                                    "class": a.klass,
                                    "tokens": a.tokens,
                                }
                                for a in allocations
                            ]
                        ),
                    },
                )
            return len(allocations)

    async def sweep(self, conn: AsyncConnection, keep: timedelta = timedelta(hours=2)) -> int:
        cur = await conn.execute(
            self._q("DELETE FROM {e}.allocations WHERE window_end < now() - %s"), (keep,)
        )
        return cur.rowcount


async def publish_demand(
    conn: AsyncConnection,
    env: str,
    frontier_schema: str,
    classes: dict[str, str],
    horizon: timedelta,
    egress_schema: str = SCHEMA,
) -> int:
    """Count this environment's tasks due within ``horizon`` per (provider, effective
    class), for target-leased task types (``classes``: task type → effective class), and
    write them to ``egress.demand``. Rows of this environment that have no due tasks any
    more are set to 0. Returns the number of (provider, class) rows with demand."""
    if not classes:
        return 0
    f, e = sql.Identifier(frontier_schema), sql.Identifier(egress_schema)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            sql.SQL(
                "SELECT task_type, provider, count(*)::int AS due FROM {f}.tasks "
                "WHERE task_type = ANY(%s) AND status = 'pending' AND provider <> '' "
                "AND next_due_at <= now() + %s GROUP BY task_type, provider"
            ).format(f=f),
            (sorted(classes), horizon),
        )
        rows = await cur.fetchall()
    due: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        due[(r["provider"], classes[r["task_type"]])] += r["due"]
    async with conn.transaction():
        await conn.execute(
            sql.SQL("UPDATE {e}.demand SET due = 0, updated_at = now() WHERE env = %s").format(e=e),
            (env,),
        )
        if due:
            await conn.execute(
                sql.SQL(
                    """
                    INSERT INTO {e}.demand AS d (env, provider, class, due)
                    SELECT %(env)s, x.provider, x.class, x.due
                    FROM jsonb_to_recordset(%(rows)s::jsonb)
                      AS x(provider text, class text, due int)
                    ON CONFLICT (env, provider, class)
                    DO UPDATE SET due = EXCLUDED.due, updated_at = now()
                    """
                ).format(e=e),
                {
                    "env": env,
                    "rows": json.dumps(
                        [{"provider": p, "class": k, "due": n} for (p, k), n in due.items()]
                    ),
                },
            )
    return len(due)
