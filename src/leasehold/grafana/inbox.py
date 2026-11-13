"""The alert inbox dashboard: the control API's ``<database>.alerts`` table in Grafana.

One per environment, in the platform folder (``lh-<env>--alerts``). Filters: status,
service and priority, plus the time range, which selects incidents with a notification
in it (firing ones are always shown). Alerts without a ``service`` label, such as
Grafana's own or the infrastructure rules, appear as ``(none)``.

The same incidents as ``GET /v1/alerts``: one row per ``(fingerprint, starts_at)``
with its latest state.
"""

from __future__ import annotations

from typing import Any

from leasehold.api.alerts import TABLE, default_database
from leasehold.grafana.render import (
    Datasources,
    _ch,
    _dashboard,
    _Layout,
    _stat,
    _suffix,
    _tags,
    _uid,
)

NONE = "(none)"
PRIORITIES = ["page", "high", "warning", "info", NONE]
STATUSES = ["firing", "resolved"]


def inbox_uid(env: str) -> str:
    return _uid("lh", env, "-alerts")


def _custom_var(name: str, label: str, values: list[str]) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "custom",
        "query": ",".join(values),
        "multi": True,
        "includeAll": True,
        "current": {"text": ["All"], "value": ["$__all"]},
        "options": [],
    }


def _query_var(name: str, label: str, ds: Datasources, sql: str) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": ds.ref("clickhouse"),
        "query": sql,
        "definition": sql,
        "refresh": 2,  # on time range change
        "sort": 1,
        "multi": True,
        "includeAll": True,
        "current": {"text": ["All"], "value": ["$__all"]},
        "options": [],
    }


def _or_none(column: str) -> str:
    return f"if({column} = '', '{NONE}', {column})"


def _filters(status: str, service: str, priority: str) -> str:
    return (
        f"{status} IN (${{status:singlequote}})"
        f" AND {_or_none(service)} IN (${{service:singlequote}})"
        f" AND {_or_none(priority)} IN (${{priority:singlequote}})"
    )


def _incidents_sql(table: str, extra_where: str = "", select: str = "", tail: str = "") -> str:
    return f"""
SELECT {select}
FROM
(
    SELECT
        fingerprint,
        starts_at,
        argMax(status, received_at)        AS l_status,
        argMax(priority, received_at)      AS l_priority,
        argMax(service, received_at)       AS l_service,
        argMax(alertname, received_at)     AS l_alertname,
        argMax(summary, received_at)       AS l_summary,
        argMax(description, received_at)   AS l_description,
        argMax(ends_at, received_at)       AS l_ends_at,
        argMax(generator_url, received_at) AS l_generator_url,
        max(received_at)                   AS last_notified,
        count()                            AS notifications
    FROM {table}
    GROUP BY fingerprint, starts_at
)
WHERE (l_status = 'firing' OR $__timeFilter(last_notified))
  AND {_filters("l_status", "l_service", "l_priority")}
  {extra_where}
{tail}""".strip()


def _status_colors() -> list[dict]:
    return [
        {
            "type": "value",
            "options": {
                "firing": {"color": "red", "index": 0},
                "resolved": {"color": "green", "index": 1},
                "page": {"color": "purple", "index": 2},
                "high": {"color": "red", "index": 3},
                "warning": {"color": "orange", "index": 4},
                "info": {"color": "blue", "index": 5},
            },
        }
    ]


def _colored(column: str) -> dict:
    return {
        "matcher": {"id": "byName", "options": column},
        "properties": [
            {"id": "mappings", "value": _status_colors()},
            {"id": "custom.cellOptions", "value": {"type": "color-text"}},
            {"id": "custom.width", "value": 90},
        ],
    }


def _table(title: str, target: dict, overrides: list[dict], description: str) -> dict:
    return {
        "type": "table",
        "title": title,
        "description": description,
        "targets": [target],
        "fieldConfig": {
            "defaults": {"custom": {"align": "auto", "filterable": True}},
            "overrides": overrides,
        },
        "options": {"showHeader": True, "cellHeight": "sm"},
    }


def inbox_dashboard(env: str, ds: Datasources) -> dict:
    table = f"{default_database(env)}.{TABLE}"
    lay = _Layout()

    def count_stat(title: str, where: str, steps, description: str) -> dict:
        sql = _incidents_sql(table, where, "count()")
        return _stat(title, [_ch(ds, sql)], thresholds=steps, description=description)

    red_if_any = [("green", None), ("red", 1)]
    lay.add(
        count_stat("Firing", "AND l_status = 'firing'", red_if_any, "Incidents firing now"),
        w=6,
        h=4,
    )
    lay.add(
        count_stat(
            "Firing: page or high",
            "AND l_status = 'firing' AND l_priority IN ('page', 'high')",
            red_if_any,
            "Incidents firing now with priority page or high",
        ),
        w=6,
        h=4,
    )
    lay.add(
        count_stat("Incidents", "", None, "Incidents with a notification in the time range"),
        w=6,
        h=4,
    )
    events_sql = (
        f"SELECT count() FROM {table} WHERE $__timeFilter(received_at)"
        f" AND {_filters('status', 'service', 'priority')}"
    )
    lay.add(
        _stat(
            "Notifications",
            [_ch(ds, events_sql)],
            description="Notifications Grafana sent to the inbox in the time range",
        ),
        w=6,
        h=4,
    )

    incidents = _ch(
        ds,
        _incidents_sql(
            table,
            select=(
                "l_status AS status, l_priority AS priority, l_service AS service,"
                " l_alertname AS alert, l_summary AS summary, starts_at AS started,"
                " l_ends_at AS ended, last_notified, notifications,"
                " l_description AS description, l_generator_url AS rule, fingerprint"
            ),
            tail="ORDER BY l_status = 'firing' DESC, last_notified DESC\nLIMIT 500",
        ),
    )
    link = {
        "matcher": {"id": "byName", "options": "rule"},
        "properties": [
            {"id": "links", "value": [{"title": "Open the rule", "url": "${__value.raw}"}]},
            {"id": "custom.width", "value": 80},
            {"id": "mappings", "value": [{"type": "regex", "options": {
                "pattern": ".+", "result": {"text": "rule"}}}]},
        ],
    }  # fmt: skip
    lay.add(
        _table(
            "Incidents",
            incidents,
            [_colored("status"), _colored("priority"), link],
            "One row per alert firing episode, with its latest state. Firing ones first.",
        ),
        w=24,
        h=14,
    )

    per_hour: dict[str, Any] = _ch(
        ds,
        f"SELECT $__timeInterval(received_at) AS time, {_or_none('priority')} AS priority,"
        f" count() AS notifications FROM {table}"
        f" WHERE $__timeFilter(received_at) AND {_filters('status', 'service', 'priority')}"
        " GROUP BY time, priority ORDER BY time",
    )
    per_hour |= {"format": 0, "queryType": "timeseries"}
    lay.add(
        {
            "type": "timeseries",
            "title": "Notifications by priority",
            "targets": [per_hour],
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "drawStyle": "bars",
                        "fillOpacity": 60,
                        "stacking": {"mode": "normal"},
                    }
                },
                "overrides": [],
            },
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
        },
        w=24,
        h=7,
    )

    events = _ch(
        ds,
        "SELECT received_at, status, priority, service, alertname AS alert, summary,"
        " value_string AS value, receiver, fingerprint"
        f" FROM {table} WHERE $__timeFilter(received_at)"
        f" AND {_filters('status', 'service', 'priority')}"
        " ORDER BY received_at DESC LIMIT 1000",
    )
    lay.add(
        _table(
            "Notifications",
            events,
            [_colored("status"), _colored("priority")],
            "Every notification Grafana sent, newest first.",
        ),
        w=24,
        h=12,
    )

    variables = [
        _custom_var("status", "Status", STATUSES),
        _query_var(
            "service",
            "Service",
            ds,
            f"SELECT DISTINCT s FROM (SELECT {_or_none('service')} AS s FROM {table}"
            f" UNION ALL SELECT '{NONE}')",
        ),
        _custom_var("priority", "Priority", PRIORITIES),
    ]
    d = _dashboard(
        inbox_uid(env), "Alert inbox" + _suffix(env), _tags(env, None), lay.panels, variables
    )
    d["time"] = {"from": "now-7d", "to": "now"}
    return d
