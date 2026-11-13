"""The alert inbox: Grafana alert notifications, stored in ClickHouse.

Grafana sends notifications to ``POST /v1/alerts`` through a webhook contact point.
Every alert in a notification becomes one row in ``<database>.alerts``, append-only;
nothing is updated or deleted, so the table is the full history. One incident is one
``(fingerprint, starts_at)``: Grafana repeats the firing notification while the alert
lasts and sends one resolved notification at the end.

``GET /v1/alerts`` lists incidents with their latest state; ``GET /v1/alerts/events``
lists the notifications themselves.

The control API writes to ClickHouse directly (no Kafka in between), and doesn't
depend on it: when ClickHouse is unreachable at start-up the API starts anyway, the
alert endpoints answer 503, and the table is created as soon as ClickHouse answers.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from leasehold import naming

TABLE = "alerts"
MARK = "leasehold: Grafana alert notifications received by the control API"
RETENTION_DAYS = 365

# Grafana sends this as endsAt while an alert is firing.
_NO_END = datetime(1, 1, 1, tzinfo=UTC)


class InboxError(RuntimeError):
    """ClickHouse unreachable or refused the statement (HTTP 503 at the API)."""


def default_database(env: str) -> str:
    return naming.clickhouse_database(env, "leasehold")


def create_statements(database: str) -> list[str]:
    naming._check(database, naming.SNAKE_RE, "ClickHouse database name")
    return [
        f"CREATE DATABASE IF NOT EXISTS {database}",
        f"""CREATE TABLE IF NOT EXISTS {database}.{TABLE}
(
    received_at   DateTime64(3, 'UTC'),
    fingerprint   String,
    status        LowCardinality(String),
    alertname     String,
    env           LowCardinality(String),
    service       LowCardinality(String),
    priority      LowCardinality(String),
    starts_at     DateTime64(3, 'UTC'),
    ends_at       Nullable(DateTime64(3, 'UTC')),
    summary       String,
    description   String,
    value_string  String,
    labels        Map(String, String),
    annotations   Map(String, String),
    generator_url String,
    receiver      String,
    group_key     String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, fingerprint)
TTL toDateTime(received_at) + INTERVAL {RETENTION_DAYS} DAY
COMMENT '{MARK}'""",
    ]


def _time(value: str | None) -> datetime | None:
    if not value:
        return None
    t = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return None if t <= _NO_END else t.astimezone(UTC)


def _ch_time(t: datetime | None) -> str | None:
    return t.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if t else None


def _strings(d: Any) -> dict[str, str]:
    return {str(k): str(v) for k, v in (d or {}).items()} if isinstance(d, dict) else {}


def rows_from_webhook(payload: dict, received_at: datetime | None = None) -> list[dict]:
    """One row per alert in a Grafana webhook notification."""
    now = received_at or datetime.now(UTC)
    receiver = str(payload.get("receiver") or "")
    group_key = str(payload.get("groupKey") or "")
    out = []
    for a in payload.get("alerts") or []:
        labels = _strings(a.get("labels"))
        annotations = _strings(a.get("annotations"))
        starts = _time(a.get("startsAt")) or now
        out.append(
            {
                "received_at": _ch_time(now),
                "fingerprint": str(a.get("fingerprint") or ""),
                "status": str(a.get("status") or payload.get("status") or ""),
                "alertname": labels.get("alertname", ""),
                "env": labels.get("env", ""),
                "service": labels.get("service", ""),
                "priority": labels.get("priority", labels.get("severity", "")),
                "starts_at": _ch_time(starts),
                "ends_at": _ch_time(_time(a.get("endsAt"))),
                "summary": annotations.get("summary", ""),
                "description": annotations.get("description", ""),
                "value_string": str(a.get("valueString") or ""),
                "labels": labels,
                "annotations": annotations,
                "generator_url": str(a.get("generatorURL") or ""),
                "receiver": receiver,
                "group_key": group_key,
            }
        )
    return out


# Filters shared by both queries; each is a ClickHouse query parameter.
_FILTERS = {
    "env": "env = {env:String}",
    "service": "service = {service:String}",
    "priority": "priority = {priority:String}",
    "alertname": "alertname = {alertname:String}",
    "fingerprint": "fingerprint = {fingerprint:String}",
}


class AlertInbox:
    def __init__(self, url: str, user: str, password: str, database: str, timeout: float = 10):
        self.database = database
        self.table = f"{database}.{TABLE}"
        self.ready = False
        self._http = httpx.AsyncClient(
            base_url=url.rstrip("/"), auth=(user, password), timeout=timeout
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def _post(self, sql: str, body: bytes | None = None, params: dict | None = None) -> str:
        query = {f"param_{k}": v for k, v in (params or {}).items()}
        if body is None:
            content = sql.encode()
        else:
            query["query"] = sql
            content = body
        try:
            r = await self._http.post("/", params=query, content=content)
        except httpx.HTTPError as e:
            raise InboxError(f"ClickHouse: {type(e).__name__}: {e}") from None
        if r.status_code != 200:
            raise InboxError(f"ClickHouse {r.status_code}: {r.text.strip()[:300]}")
        return r.text

    async def ensure_table(self) -> None:
        for stmt in create_statements(self.database):
            await self._post(stmt)
        self.ready = True

    async def insert(self, rows: list[dict]) -> None:
        if not rows:
            return
        body = "\n".join(json.dumps(r) for r in rows).encode()
        await self._post(f"INSERT INTO {self.table} FORMAT JSONEachRow", body=body)

    def _where(self, filters: dict[str, str | None]) -> tuple[list[str], dict]:
        where, params = [], {}
        for k, v in filters.items():
            if v:
                where.append(_FILTERS[k])
                params[k] = v
        return where, params

    async def _select(self, sql: str, params: dict) -> list[dict]:
        out = await self._post(sql + " FORMAT JSONEachRow", params=params)
        return [json.loads(line) for line in out.splitlines() if line]

    async def incidents(
        self,
        since: timedelta,
        status: str | None = None,
        limit: int = 100,
        **filters: str | None,
    ) -> list[dict]:
        """Incidents with a notification in ``since``, plus every one still firing."""
        where, params = self._where(filters)
        params |= {"since_s": int(since.total_seconds()), "limit": limit}
        having = ["(l_status = 'firing' OR last_notified >= now64(3) - {since_s:UInt32})"]
        if status:
            having.append("l_status = {status:String}")
            params["status"] = status
        # The aggregates get their column names only in the outer query: in ClickHouse
        # an alias that shadows a column replaces that column in WHERE as well.
        sql = f"""
SELECT
    fingerprint, l_alertname AS alertname, l_status AS status, l_env AS env,
    l_service AS service, l_priority AS priority, starts_at, l_ends_at AS ends_at,
    last_notified, notifications, l_summary AS summary, l_description AS description,
    l_value_string AS value_string, l_labels AS labels, l_generator_url AS generator_url
FROM
(
    SELECT
        fingerprint,
        starts_at,
        argMax(alertname, received_at)     AS l_alertname,
        argMax(status, received_at)        AS l_status,
        argMax(env, received_at)           AS l_env,
        argMax(service, received_at)       AS l_service,
        argMax(priority, received_at)      AS l_priority,
        argMax(ends_at, received_at)       AS l_ends_at,
        max(received_at)                   AS last_notified,
        count()                            AS notifications,
        argMax(summary, received_at)       AS l_summary,
        argMax(description, received_at)   AS l_description,
        argMax(value_string, received_at)  AS l_value_string,
        argMax(labels, received_at)        AS l_labels,
        argMax(generator_url, received_at) AS l_generator_url
    FROM {self.table}
    {"WHERE " + " AND ".join(where) if where else ""}
    GROUP BY fingerprint, starts_at
)
WHERE {" AND ".join(having)}
ORDER BY status = 'firing' DESC, last_notified DESC
LIMIT {{limit:UInt32}}"""
        return await self._select(sql, params)

    async def events(self, since: timedelta, limit: int = 100, **filters: str | None) -> list[dict]:
        where, params = self._where(filters)
        where.append("received_at >= now64(3) - {since_s:UInt32}")
        params |= {"since_s": int(since.total_seconds()), "limit": limit}
        sql = f"""
SELECT * EXCEPT (group_key)
FROM {self.table}
WHERE {" AND ".join(where)}
ORDER BY received_at DESC
LIMIT {{limit:UInt32}}"""
        return await self._select(sql, params)
