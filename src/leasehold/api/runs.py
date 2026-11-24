"""Outcomes of scheduled components, from their run summaries in the log table.

``run_scheduled`` logs ``run finished`` or ``run failed`` once per run; the log pipeline
writes both to ClickHouse (``LEASEHOLD_LOGS_TABLE``). The control API reads them back and
exports, per component, whether the last run failed and the time by which the next success
is due: the last success (or the first failure, when there is none) plus twice the longest
gap in the schedule, plus the timeout, at least half an hour.

A run that is killed (out of memory, node gone) logs nothing: only the due time finds it. A
component that never logged a run has no series: it can't be told from one that isn't
deployed here.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta

import httpx

from leasehold import cron
from leasehold.spec.models import Scheduled, ServiceSpec

MIN_ALLOWED = timedelta(minutes=30)
LOOKBACK_MIN, LOOKBACK_MAX = timedelta(days=2), timedelta(days=85)  # the log's TTL is 90
_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")


class RunsError(RuntimeError):
    pass


@dataclass(frozen=True)
class Job:
    service: str
    component: str
    allowed_s: float


@dataclass
class State:
    last_s: float
    last_failed: bool
    last_success_s: float | None
    first_failure_s: float | None

    def due_s(self, allowed_s: float) -> float:
        anchor = self.last_success_s if self.last_success_s is not None else self.first_failure_s
        return (anchor if anchor is not None else self.last_s) + allowed_s


def jobs(specs: Iterable[ServiceSpec]) -> list[Job]:
    out = []
    for s in specs:
        for cname, c in s.components.items():
            if not isinstance(c, Scheduled):
                continue
            gap = cron.max_gap(c.schedule)
            if gap is None:
                continue
            allowed = max(gap * 2 + c.timeout, MIN_ALLOWED)
            out.append(Job(s.name, cname, allowed.total_seconds()))
    return out


class RunLog:
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        table: str,
        env: str,
        wanted: list[Job],
        transport=None,
    ):
        if not _TABLE.match(table):
            raise ValueError(f"log table {table!r}: expected database.table")
        self.table = table
        self.env = env
        self.jobs = {(j.service, j.component): j for j in wanted}
        longest = max((j.allowed_s for j in wanted), default=0.0)
        self.lookback = min(max(timedelta(seconds=longest, days=1), LOOKBACK_MIN), LOOKBACK_MAX)
        self.states: dict[tuple[str, str], State] = {}
        self.newest_s: float | None = None  # newest row of the whole log table
        self.ready = False
        self._http = httpx.AsyncClient(
            base_url=url.rstrip("/"), auth=(user, password), timeout=60, transport=transport
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def _query(self, sql: str, **params) -> list[dict]:
        try:
            r = await self._http.post(
                "/",
                params={f"param_{k}": str(v) for k, v in params.items()},
                content=sql.encode(),
            )
        except httpx.HTTPError as e:
            raise RunsError(f"ClickHouse: {type(e).__name__}: {e}") from None
        if r.status_code != 200:
            raise RunsError(f"ClickHouse {r.status_code}: {r.text.strip()[:300]}")
        return [json.loads(line) for line in r.text.splitlines() if line]

    async def refresh(self) -> None:
        """First the whole lookback, then the last days, merged into the states."""
        hours = int((LOOKBACK_MIN if self.ready else self.lookback).total_seconds() // 3600) + 1
        newest = await self._query(
            f"SELECT toUnixTimestamp(max(timestamp)) AS t, count() AS n FROM {self.table} "
            "WHERE timestamp > now() - INTERVAL 1 HOUR FORMAT JSONEachRow"
        )
        rows = await self._query(
            "SELECT service, fields['component'] AS component, "
            "argMax(message, timestamp) AS last_message, "
            "toUnixTimestamp(max(timestamp)) AS last, "
            "toUnixTimestamp(maxIf(timestamp, message = 'run finished')) AS success, "
            "countIf(message = 'run finished') AS finished, "
            "toUnixTimestamp(minIf(timestamp, message = 'run failed')) AS failure, "
            "countIf(message = 'run failed') AS failed "
            f"FROM {self.table} "
            "WHERE timestamp > now() - toIntervalHour({hours:UInt32}) "
            "AND fields['env'] = {env:String} AND message IN ('run finished', 'run failed') "
            "GROUP BY service, component FORMAT JSONEachRow",
            hours=hours,
            env=self.env,
        )
        for r in rows:
            key = (r["service"], r["component"])
            if key not in self.jobs:
                continue
            old = self.states.get(key)
            success = float(r["success"]) if int(r["finished"]) else None
            failure = float(r["failure"]) if int(r["failed"]) else None
            self.states[key] = State(
                last_s=float(r["last"]),
                last_failed=r["last_message"] == "run failed",
                last_success_s=success if success is not None else old and old.last_success_s,
                # The first failure ever seen: the anchor when there is no success.
                first_failure_s=old.first_failure_s
                if old and old.first_failure_s is not None
                else failure,
            )
        if newest and int(newest[0]["n"]):
            self.newest_s = float(newest[0]["t"])
        self.ready = True
