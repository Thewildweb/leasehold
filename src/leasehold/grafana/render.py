"""Grafana dashboards derived from ``service.yml``.

Per environment, ``plan`` produces:

- one folder per service, holding:
  - the **service dashboard**, generated from ``service.yml``: the service's cockpit
    tiles, then per component instances, CPU and memory per instance against the
  reservation, restarts and CPU per task or event, per task type the
    frontier counts and rates, per topic the produce rate and size, per Kafka
    consumer the lag, and per ``expects`` check its current value;
  - the service's **own dashboards** (``dashboards.files``);
- the **cockpit** in a platform folder: one row per service with its instance count,
  its backlog and dead tasks, and its cockpit tiles;
- per service its **alert rules** (``alerts.py``), in the service's folder;
- the **alert inbox** in the platform folder (``inbox.py``): the control API's alerts
  table, filterable by status, service and priority;
- in prod only, the **fleet** dashboard in the platform folder (``fleet.py``): capacity,
  reservations and use across the whole Nomad cluster.

Every dashboard gets hidden constant variables ``env``, ``namespace`` and, when the
service has a ClickHouse database, ``database``, so a query in an own dashboard can
use ``${namespace}`` and ``${database}`` and work in every environment. Data sources
are set by UID per type, so files exported from one Grafana work in another.

UIDs (Grafana's limit is 40 characters):

    folder per service     lhf-<env>-<service>
    service dashboard      lh-<env>-<service>
    own dashboard          lh-<env>-<service>--<file name>
    platform folder        lhf-<env>--platform
    cockpit                lh-<env>--cockpit
    alert inbox            lh-<env>--alerts
    fleet                  lh--fleet (prod plan only)

Service names can't contain ``--``, so these can't collide. Every dashboard is
tagged ``leasehold`` and ``env:<env>``, which is how stale ones are found.
"""

from __future__ import annotations

import copy
import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

from leasehold import naming
from leasehold.spec.models import (
    DATABASE_PLACEHOLDER,
    CockpitTile,
    Processor,
    Scheduled,
    ServiceSpec,
)

PROMETHEUS_TYPE = "prometheus"
CLICKHOUSE_TYPE = "grafana-clickhouse-datasource"
SCHEMA_VERSION = 39
MANAGED_TAG = "leasehold"


@dataclass(frozen=True)
class Datasources:
    """UIDs of the data sources in the target Grafana."""

    prometheus: str = "prometheus"
    clickhouse: str = "clickhouse"

    def ref(self, kind: str) -> dict[str, str]:
        if kind == "prometheus":
            return {"type": PROMETHEUS_TYPE, "uid": self.prometheus}
        return {"type": CLICKHOUSE_TYPE, "uid": self.clickhouse}


@dataclass(frozen=True)
class FolderPlan:
    uid: str
    title: str


@dataclass(frozen=True)
class DashboardPlan:
    uid: str
    title: str
    folder_uid: str
    service: str | None  # None for the cockpit
    source: str  # 'generated', 'cockpit', 'fleet' or 'file:<path>'
    dashboard: dict[str, Any] = field(compare=False, hash=False)


@dataclass
class GrafanaPlan:
    env: str
    folders: list[FolderPlan]
    dashboards: list[DashboardPlan]
    rules: list[Any] = field(default_factory=list)  # alerts.RulePlan


def env_tag(env: str) -> str:
    return f"env:{env}"


def _uid(*parts: str) -> str:
    s = "-".join(parts)
    if len(s) <= 40:
        return s
    return f"{s[:31]}-{hashlib.sha1(s.encode()).hexdigest()[:8]}"


def _suffix(env: str) -> str:
    return "" if env == naming.PROD else f" ({env})"


def _file_id(path: str) -> str:
    stem = path.rsplit("/", 1)[-1].removesuffix(".json").lower()
    return re.sub(r"[^a-z0-9]+", "-", stem).strip("-") or "dashboard"


# --- panels -----------------------------------------------------------------------------------


class _Layout:
    """Places panels left to right on Grafana's 24-column grid."""

    def __init__(self) -> None:
        self.panels: list[dict] = []
        self.x = 0
        self.y = 0
        self.row_h = 0
        self.next_id = 1

    def _newline(self) -> None:
        if self.x:
            self.y += self.row_h
            self.x = 0
            self.row_h = 0

    def row(self, title: str) -> None:
        self._newline()
        self.panels.append(
            {
                "type": "row",
                "title": title,
                "id": self._id(),
                "collapsed": False,
                "panels": [],
                "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1},
            }
        )
        self.y += 1

    def add(self, panel: dict, w: int, h: int) -> None:
        if self.x + w > 24:
            self._newline()
        panel["id"] = self._id()
        panel["gridPos"] = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.panels.append(panel)
        self.x += w
        self.row_h = max(self.row_h, h)

    def _id(self) -> int:
        i = self.next_id
        self.next_id += 1
        return i


def _prom(ds: Datasources, expr: str, legend: str = "", ref: str = "A") -> dict:
    t = {"datasource": ds.ref("prometheus"), "expr": expr, "refId": ref}
    if legend:
        t["legendFormat"] = legend
    return t


def _ch(ds: Datasources, query: str, ref: str = "A") -> dict:
    return {
        "datasource": ds.ref("clickhouse"),
        "editorType": "sql",
        "format": "table",
        "queryType": "table",
        "rawSql": query,
        "refId": ref,
    }


def _thresholds(steps: list[tuple[str, float | None]]) -> dict:
    return {
        "mode": "absolute",
        "steps": [{"color": c, "value": v} for c, v in steps],
    }


def _stat(
    title: str,
    targets: list[dict],
    *,
    unit: str = "short",
    decimals: int | None = None,
    thresholds: list[tuple[str, float | None]] | None = None,
    description: str = "",
    links: list[dict] | None = None,
) -> dict:
    defaults: dict[str, Any] = {
        "unit": unit,
        "thresholds": _thresholds(thresholds or [("text", None)]),
        "color": {"mode": "thresholds"},
    }
    if decimals is not None:
        defaults["decimals"] = decimals
    p = {
        "type": "stat",
        "title": title,
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "background" if thresholds else "value",
            "graphMode": "none",
            "textMode": "value",
            "justifyMode": "center",
        },
    }
    if description:
        p["description"] = description
    if links:
        p["links"] = links
    return p


def _timeseries(
    title: str, targets: list[dict], *, unit: str = "short", description: str = ""
) -> dict:
    p = {
        "type": "timeseries",
        "title": title,
        "targets": targets,
        "fieldConfig": {
            "defaults": {"unit": unit, "custom": {"fillOpacity": 10, "lineWidth": 1}},
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi"},
        },
    }
    if description:
        p["description"] = description
    return p


def _tile(ds: Datasources, tile: CockpitTile, env: str, database: str | None) -> dict:
    query = _fill(tile.query, env, database)
    target = _prom(ds, query) if tile.datasource == "prometheus" else _ch(ds, query)
    steps = [(t.color, t.from_) for t in tile.thresholds] or None
    return _stat(tile.title, [target], unit=tile.unit, decimals=tile.decimals, thresholds=steps)


def _database(spec: ServiceSpec, env: str) -> str | None:
    if spec.clickhouse is None:
        return None
    return naming.clickhouse_database(env, spec.clickhouse.database)


def _fill(query: str, env: str, database: str | None) -> str:
    q = query.replace("{namespace}", naming.nomad_namespace(env)).replace("{env}", env)
    if database is not None:
        q = q.replace(DATABASE_PLACEHOLDER, database)
    return q


# --- selectors --------------------------------------------------------------------------------


def _alloc_selector(env: str, service: str, component: str, c) -> str:
    """Prometheus label selector for a component's allocations (Nomad client metrics).

    Periodic jobs run as child jobs ('<job>/periodic-<time>'); their parent is in
    ``parent_id``.
    """
    job = naming.nomad_job(service, component)
    ns = naming.nomad_namespace(env)
    key = "parent_id" if isinstance(c, Scheduled) else "exported_job"
    return f'namespace="{ns}", {key}="{job}"'


def _runtime_selector(env: str, service: str, component: str) -> str:
    """Selector for metrics a component's runtime exports on /metrics.

    The Nomad job's Prometheus scrape sets ``env``, ``service`` and ``component`` as
    target labels; the runtime's metrics carry none of these themselves.
    """
    return f'env="{env}", service="{service}", component="{component}"'


def _variables(env: str, database: str | None) -> list[dict]:
    def const(name: str, value: str) -> dict:
        return {
            "name": name,
            "type": "constant",
            "hide": 2,
            "query": value,
            "current": {"text": value, "value": value},
            "options": [{"text": value, "value": value, "selected": True}],
        }

    out = [const("env", env), const("namespace", naming.nomad_namespace(env))]
    if database is not None:
        out.append(const("database", database))
    return out


def _dashboard(uid: str, title: str, tags: list[str], panels: list[dict], variables) -> dict:
    return {
        "uid": uid,
        "title": title,
        "tags": tags,
        "timezone": "browser",
        "editable": True,
        "schemaVersion": SCHEMA_VERSION,
        "refresh": "1m",
        "time": {"from": "now-24h", "to": "now"},
        "templating": {"list": variables},
        "annotations": {"list": []},
        "panels": panels,
    }


def _tags(env: str, service: str | None) -> list[str]:
    tags = [MANAGED_TAG, env_tag(env)]
    if service:
        tags.append(f"service:{service}")
    return tags


# --- service dashboard ------------------------------------------------------------------------


def service_dashboard(spec: ServiceSpec, env: str, ds: Datasources) -> dict:
    database = (
        naming.clickhouse_database(env, spec.clickhouse.database) if spec.clickhouse else None
    )
    lay = _Layout()

    if spec.dashboards.cockpit:
        lay.row("Overview")
        for tile in spec.dashboards.cockpit:
            lay.add(_tile(ds, tile, env, database), 6, 4)

    lay.row("Components")
    for cname, c in spec.components.items():
        sel = _alloc_selector(env, spec.name, cname, c)
        targets = [_prom(ds, f"count(nomad_client_allocs_cpu_user{{{sel}}}) or vector(0)", cname)]
        count = getattr(c, "count", None)
        if count is not None:
            targets.append(_prom(ds, f"vector({count.min})", "min", "B"))
            targets.append(_prom(ds, f"vector({count.max})", "max", "C"))
        kind = c.kind
        lay.add(
            _timeseries(
                f"{cname} ({kind}): instances",
                targets,
                description="Running allocations, against the count range",
            ),
            6,
            7,
        )
        rt = _runtime_selector(env, spec.name, cname)
        mb = 1024 * 1024
        lay.add(
            _timeseries(
                f"{cname}: CPU per instance",
                [
                    _prom(
                        ds,
                        f"max(sum by (alloc_id) (nomad_client_allocs_cpu_total_ticks{{{sel}}}))",
                        "used, busiest instance",
                    ),
                    _prom(
                        ds,
                        f"avg(sum by (alloc_id) (nomad_client_allocs_cpu_total_ticks{{{sel}}}))",
                        "used, average",
                        "B",
                    ),
                    _prom(ds, f"vector({c.resources.cpu})", "reserved", "C"),
                ],
                unit="none",
                description="MHz. Reserved is resources.cpu from service.yml",
            ),
            6,
            7,
        )
        mem_targets = [
            _prom(
                ds,
                f"max(sum by (alloc_id) (nomad_client_allocs_memory_usage{{{sel}}}))",
                "used incl. cache (Nomad)",
            ),
            _prom(
                ds,
                f"max(leasehold_container_memory_working_set_bytes{{{rt}}})",
                "working set",
                "B",
            ),
            _prom(ds, f"max(leasehold_container_memory_peak_bytes{{{rt}}})", "peak", "C"),
            _prom(ds, f"vector({c.resources.memory * mb})", "reserved", "D"),
        ]
        if c.resources.memory_max:
            mem_targets.append(_prom(ds, f"vector({c.resources.memory_max * mb})", "max", "E"))
        lay.add(
            _timeseries(
                f"{cname}: memory per instance",
                mem_targets,
                unit="bytes",
                description=(
                    "Highest instance. Nomad counts page cache as used; working set and peak "
                    "come from the component's cgroup (peak since the instance started)"
                ),
            ),
            6,
            7,
        )
        lay.add(
            _stat(
                f"{cname}: restarts (1h)",
                [
                    _prom(
                        ds,
                        f"sum(changes(process_start_time_seconds{{{rt}}}[1h])) or vector(0)",
                    )
                ],
                thresholds=[("green", None), ("orange", 1), ("red", 5)],
                description="Process restarts inside running allocations",
            ),
            3,
            7,
        )
        if isinstance(c, Processor):
            per, unit_metric = "event", "leasehold_processor_events_total"
        elif isinstance(c, Scheduled):
            per, unit_metric = None, None
        else:
            per, unit_metric = "task", "leasehold_worker_tasks_total"
        if per:
            lay.add(
                _stat(
                    f"{cname}: CPU per {per}",
                    [
                        _prom(
                            ds,
                            f"sum(rate(leasehold_container_cpu_seconds_total{{{rt}}}[1h]))"
                            f" / sum(rate({unit_metric}{{{rt}}}[1h]))",
                        )
                    ],
                    unit="s",
                    decimals=3,
                    description=f"CPU seconds per {per}, last hour",
                ),
                3,
                7,
            )

    task_types = list(spec.frontier.task_types)
    if task_types:
        lay.row("Tasks")
        for tt in task_types:
            lbl = f'env="{env}", task_type="{tt}"'
            lay.add(
                _timeseries(
                    f"{tt}: backlog",
                    [
                        _prom(ds, f"max(leasehold_frontier_eligible_tasks{{{lbl}}})", "eligible"),
                        _prom(ds, f"max(leasehold_frontier_leased_tasks{{{lbl}}})", "leased", "B"),
                        _prom(ds, f"max(leasehold_frontier_dead_tasks{{{lbl}}})", "dead", "C"),
                    ],
                    description="Eligible = pending and due (counted up to 1M)",
                ),
                12,
                8,
            )
            lay.add(
                _timeseries(
                    f"{tt}: operations per second",
                    [
                        _prom(
                            ds,
                            f"sum by (op) (rate(leasehold_tasks_total{{{lbl}}}[$__rate_interval]))",
                            "{{op}}",
                        )
                    ],
                    unit="ops",
                ),
                12,
                8,
            )

    if spec.kafka.produces:
        lay.row("Kafka topics")
        for tname in spec.kafka.produces:
            topic = naming.topic_for(env, spec.topic_ref(tname))
            lay.add(
                _timeseries(
                    f"{topic}: messages in",
                    [
                        _prom(
                            ds,
                            "sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_count"
                            f'{{topic="{topic}"}}[$__rate_interval]))',
                            "messages/s",
                        )
                    ],
                    unit="short",
                ),
                12,
                7,
            )
            lay.add(
                _timeseries(
                    f"{topic}: size on disk",
                    [_prom(ds, f'sum(kafka_log_log_size{{topic="{topic}"}})', "bytes")],
                    unit="bytes",
                ),
                12,
                7,
            )

    consumers = _consumers(spec, env)
    if consumers:
        lay.row("Kafka consumers")
        for title, group in consumers:
            lay.add(
                _timeseries(
                    f"{title}: lag",
                    [
                        _prom(
                            ds,
                            f'sum by (topic) (leasehold_kafka_consumer_lag{{group="{group}"}})',
                            "{{topic}}",
                        )
                    ],
                    description=f"Messages behind, consumer group {group}",
                ),
                12,
                7,
            )

    if spec.expects:
        lay.row("Checks (expects)")
        for exp in spec.expects:
            steps: list[tuple[str, float | None]] = [("green", None)]
            if exp.min is not None:
                steps = [("red", None), ("green", exp.min)]
            if exp.max is not None:
                steps.append(("red", exp.max))
            bounds = " and ".join(
                b
                for b in (
                    f">= {exp.min:g}" if exp.min is not None else "",
                    f"<= {exp.max:g}" if exp.max is not None else "",
                )
                if b
            )
            lay.add(
                _stat(
                    exp.name,
                    [_ch(ds, exp.render_sql(database or ""))],
                    thresholds=steps,
                    description=f"Expected {bounds}; alert priority {exp.priority}",
                ),
                6,
                4,
            )

    uid = _uid("lh", env, spec.name)
    return _dashboard(
        uid,
        spec.name + _suffix(env),
        _tags(env, spec.name),
        lay.panels,
        _variables(env, database),
    )


def _consumers(spec: ServiceSpec, env: str) -> list[tuple[str, str]]:
    out = []
    for cname, c in spec.components.items():
        if isinstance(c, Processor):
            out.append((cname, naming.consumer_group(env, spec.name, cname)))
    if spec.clickhouse:
        for sink in spec.clickhouse.sinks:
            out.append(
                (f"ClickHouse sink {sink}", naming.clickhouse_sink_group(env, spec.name, sink))
            )
    return out


# --- cockpit ----------------------------------------------------------------------------------


def cockpit_dashboard(specs: list[ServiceSpec], env: str, ds: Datasources) -> dict:
    ns = naming.nomad_namespace(env)
    lay = _Layout()
    for spec in sorted(specs, key=lambda s: s.name):
        database = _database(spec, env)
        lay.row(spec.name)
        jobs = "|".join(naming.nomad_job(spec.name, c) for c in spec.components)
        link = [{"title": f"{spec.name} dashboard", "url": f"/d/{_uid('lh', env, spec.name)}"}]
        lay.add(
            _stat(
                "instances",
                [
                    _prom(
                        ds,
                        f'count(nomad_client_allocs_cpu_user{{namespace="{ns}", '
                        f'exported_job=~"{jobs}"}}) or vector(0)',
                    )
                ],
                description="Running allocations of the long-running components",
                links=link,
            ),
            4,
            4,
        )
        if spec.frontier.task_types:
            types = "|".join(re.escape(t) for t in spec.frontier.task_types)
            lbl = f'env="{env}", task_type=~"{types}"'
            lay.add(
                _stat(
                    "eligible tasks",
                    [
                        _prom(
                            ds,
                            f"sum(max by (task_type) (leasehold_frontier_eligible_tasks{{{lbl}}}))",
                        )
                    ],
                ),
                4,
                4,
            )
            lay.add(
                _stat(
                    "dead tasks",
                    [
                        _prom(
                            ds, f"sum(max by (task_type) (leasehold_frontier_dead_tasks{{{lbl}}}))"
                        )
                    ],
                    thresholds=[("green", None), ("red", 1)],
                ),
                4,
                4,
            )
        for tile in spec.dashboards.cockpit:
            lay.add(_tile(ds, tile, env, database), 3, 4)
    uid = _uid("lh", env, "-cockpit")
    return _dashboard(
        uid, "Cockpit" + _suffix(env), _tags(env, None), lay.panels, _variables(env, None)
    )


# --- own dashboards ---------------------------------------------------------------------------


def own_dashboard(spec: ServiceSpec, path: str, data: dict, env: str, ds: Datasources) -> dict:
    """An exported dashboard, made ours: UID, title, tags, variables and data sources."""
    d = copy.deepcopy(data)
    d.pop("id", None)
    d.pop("version", None)
    d.pop("__inputs", None)
    d.pop("__requires", None)
    database = (
        naming.clickhouse_database(env, spec.clickhouse.database) if spec.clickhouse else None
    )
    d["uid"] = _uid("lh", env, spec.name, "-" + _file_id(path))
    title = d.get("title") or _file_id(path)
    if not title.startswith(spec.name):
        title = f"{spec.name}: {title}"
    d["title"] = title + _suffix(env)
    # Drop tags from the environment it was exported from; set ours.
    own_tags = [t for t in d.get("tags", []) if t != MANAGED_TAG and ":" not in t]
    d["tags"] = sorted(set(own_tags)) + _tags(env, spec.name)
    ours = {v["name"] for v in _variables(env, database)}
    others = [v for v in d.get("templating", {}).get("list", []) if v.get("name") not in ours]
    d["templating"] = {"list": _variables(env, database) + others}
    _set_datasources(d, ds)
    return d


def _set_datasources(node: Any, ds: Datasources) -> None:
    """Point every Prometheus and ClickHouse data source reference at this Grafana's UIDs."""
    if isinstance(node, dict):
        ref = node.get("datasource")
        if isinstance(ref, dict) and ref.get("type") in (PROMETHEUS_TYPE, CLICKHOUSE_TYPE):
            node["datasource"] = ds.ref(
                "prometheus" if ref["type"] == PROMETHEUS_TYPE else "clickhouse"
            )
        for v in node.values():
            _set_datasources(v, ds)
    elif isinstance(node, list):
        for v in node:
            _set_datasources(v, ds)


# --- plan -------------------------------------------------------------------------------------


def plan(
    services: list[tuple[ServiceSpec, dict[str, dict]]],
    env: str,
    ds: Datasources | None = None,
) -> GrafanaPlan:
    """``services``: each spec with its own dashboard files (path -> parsed JSON)."""
    ds = ds or Datasources()
    naming.check_env(env)
    folders: list[FolderPlan] = []
    dashboards: list[DashboardPlan] = []
    platform = FolderPlan(_uid("lhf", env, "-platform"), "Platform" + _suffix(env))
    folders.append(platform)
    specs = [s for s, _ in services]
    cockpit = cockpit_dashboard(specs, env, ds)
    dashboards.append(
        DashboardPlan(cockpit["uid"], cockpit["title"], platform.uid, None, "cockpit", cockpit)
    )
    from leasehold.grafana.inbox import inbox_dashboard

    inbox = inbox_dashboard(env, ds)
    dashboards.append(
        DashboardPlan(inbox["uid"], inbox["title"], platform.uid, None, "inbox", inbox)
    )
    if env == naming.PROD:
        # One Nomad cluster serves every environment; its dashboard is generated once.
        from leasehold.grafana.fleet import fleet_dashboard

        fleet = fleet_dashboard(ds)
        dashboards.append(
            DashboardPlan(fleet["uid"], fleet["title"], platform.uid, None, "fleet", fleet)
        )
    from leasehold.grafana.alerts import service_rules

    rules = []
    for spec, files in sorted(services, key=lambda x: x[0].name):
        folder = FolderPlan(_uid("lhf", env, spec.name), spec.name + _suffix(env))
        folders.append(folder)
        rules += service_rules(spec, env, folder.uid, ds.prometheus, ds.clickhouse)
        d = service_dashboard(spec, env, ds)
        dashboards.append(
            DashboardPlan(d["uid"], d["title"], folder.uid, spec.name, "generated", d)
        )
        for path in spec.dashboards.files:
            d = own_dashboard(spec, path, files[path], env, ds)
            dashboards.append(
                DashboardPlan(d["uid"], d["title"], folder.uid, spec.name, f"file:{path}", d)
            )
    return GrafanaPlan(env, folders, dashboards, rules)
