"""The Fleet dashboard: capacity, reservations and use across the Nomad cluster.

One Nomad cluster serves every environment, so this dashboard is generated once,
with the prod plan, and covers all jobs (v1/v2 and v3). Sources:

- Nomad node metrics (``nomad_client_allocated_*`` / ``nomad_client_unallocated_*``):
  schedulable capacity and what allocations reserve, in MHz and MB. Both exclude the
  node's ``reserved {}`` block, so they describe what Nomad may hand out.
- Nomad allocation metrics (``nomad_client_allocs_*``): what each allocation reserves
  and uses, in MHz and bytes. On cgroup v2 ``memory_usage`` includes page cache
  (``memory_cache`` is 0), so memory use is overstated for jobs that touch many files.
  CPU throttling is always 0 (Nomad uses CPU shares, not quotas), so contention is
  shown as host pressure instead.
- node-exporter: host CPU including work outside Nomad (ClickHouse, Postgres), steal
  time (other VMs on the hypervisor) and pressure (PSI).

Allocation metrics carry ``instance`` but no ``node_class``, and node-exporter's
``instance`` has port 9100 instead of 4646, so queries join on ``instance`` after
rewriting the port.
"""

from __future__ import annotations

from typing import Any

from leasehold.grafana.render import (
    Datasources,
    _dashboard,
    _Layout,
    _prom,
    _stat,
    _tags,
    _timeseries,
    _uid,
)

UID = _uid("lh", "-fleet")
TITLE = "Fleet"
MB = 1024 * 1024  # node memory metrics are in MB; allocation memory metrics in bytes


def _n(metric: str) -> str:
    """A Nomad node metric, limited to the selected node classes."""
    return f'{metric}{{node_class=~"$node_class"}}'


_NODE = _n("nomad_client_allocated_cpu")
# Keeps series whose node is in the selected classes.
_IN_CLASS = f" and on(instance) {_NODE}"
# Adds node_class to a series keyed by a Nomad instance (and drops other classes).
_WITH_CLASS = (
    f" * on(instance) group_left(node_class) (0 * max by (instance, node_class) ({_NODE}) + 1)"
)

# Per node, labels as on the Nomad node metrics.
NODE_CPU_CAP = f"({_n('nomad_client_allocated_cpu')} + {_n('nomad_client_unallocated_cpu')})"
NODE_CPU_RES = _n("nomad_client_allocated_cpu")
NODE_CPU_FREE = _n("nomad_client_unallocated_cpu")
NODE_MEM_CAP = f"({_n('nomad_client_allocated_memory')} + {_n('nomad_client_unallocated_memory')})"
NODE_MEM_RES = _n("nomad_client_allocated_memory")
NODE_MEM_FREE = _n("nomad_client_unallocated_memory")

# Fleet totals.
CPU_CAP = f"sum({NODE_CPU_CAP})"
CPU_RES = f"sum({NODE_CPU_RES})"
CPU_USED = f"sum(nomad_client_allocs_cpu_total_ticks{_IN_CLASS})"
MEM_CAP = f"sum({NODE_MEM_CAP})"
MEM_RES = f"sum({NODE_MEM_RES})"
MEM_USED = f"sum(nomad_client_allocs_memory_usage{_IN_CLASS})"


def _ne(expr: str) -> str:
    """A node-exporter expression re-keyed to the Nomad instance (host:4646)."""
    return f'label_replace({expr}, "instance", "$1:4646", "instance", "([^:]+):9100")'


def _job(metric: str) -> str:
    """An allocation metric with ``jobname``: periodic children folded into the parent."""
    return (
        f'label_replace({metric}{{namespace=~"$namespace"}}{_IN_CLASS}, '
        '"jobname", "$1", "exported_job", "([^/]+).*")'
    )


def _over(fn: str, metric: str) -> str:
    """Per allocation: ``fn`` over the period per task, summed over the alloc's tasks.

    For max and p95 the sum over tasks is an upper bound of the alloc's own value,
    which is the safe side for sizing.
    """
    rng = f'{metric}{{namespace=~"$namespace"}}[$__range]'
    inner = f"quantile_over_time(0.95, {rng})" if fn == "p95" else f"{fn}({rng})"
    return (
        "sum by (jobname, alloc_id) ("
        f'label_replace({inner}{_IN_CLASS}, "jobname", "$1", "exported_job", "([^/]+).*"))'
    )


def _per_node(expr: str) -> str:
    """One value per node, with its inventory name (``node``, set by the Prometheus scrape).

    ``node`` is joined only here, for the node table (an instant query): the label was
    added on 2026-11-12, and joining on it in range queries would hit the few minutes
    where old series without it and new ones with it both exist.
    """
    names = f"(0 * max by (instance, node) ({_NODE}) + 1)"
    return f"max by (instance, node_class, node) (({expr}) * on(instance) group_left(node) {names})"


# --- panels -----------------------------------------------------------------------------------


def _variables(ds: Datasources) -> list[dict]:
    def query_var(name: str, label: str, query: str) -> dict:
        return {
            "name": name,
            "label": label,
            "type": "query",
            "datasource": ds.ref("prometheus"),
            "query": {"query": query, "refId": "v"},
            "definition": query,
            "refresh": 2,
            "multi": True,
            "includeAll": True,
            "allValue": ".*",
            "current": {"text": ["All"], "value": ["$__all"]},
            "sort": 1,
        }

    def text_var(name: str, label: str, value: str) -> dict:
        return {
            "name": name,
            "label": label,
            "type": "textbox",
            "query": value,
            "current": {"text": value, "value": value},
        }

    return [
        query_var(
            "node_class", "Node class", "label_values(nomad_client_allocated_cpu, node_class)"
        ),
        query_var(
            "namespace", "Namespace", "label_values(nomad_client_allocs_cpu_allocated, namespace)"
        ),
        text_var("fit_cpu", "Fit: MHz", "500"),
        text_var("fit_mem", "Fit: MB", "512"),
    ]


def _table(
    title: str,
    columns: list[tuple[str, str, str, int | None]],
    ds: Datasources,
    *,
    key: list[str],
    sort_by: str,
    description: str = "",
    calculated: list[tuple[str, str, str, str, str, int | None]] = (),  # type: ignore[assignment]
    hidden: tuple[str, ...] = (),
) -> dict:
    """An instant table: one query per column, merged on the key labels.

    ``columns``: (header, expr, unit, decimals); column N is field ``Value #<letter N>``.
    ``calculated``: (header, left field, operator, right field, unit, decimals), worked out
    in the browser after the merge, in order; fields are ``Value #X`` or an earlier
    calculated header. ``hidden``: calculated headers used only as intermediate steps.
    """
    targets = []
    overrides = []
    for i, (header, expr, unit, decimals) in enumerate(columns):
        ref = chr(ord("A") + i)
        targets.append(
            {
                "datasource": ds.ref("prometheus"),
                "expr": expr,
                "refId": ref,
                "instant": True,
                "range": False,
                "format": "table",
            }
        )
        props: list[dict[str, Any]] = [
            {"id": "displayName", "value": header},
            {"id": "unit", "value": unit},
        ]
        if decimals is not None:
            props.append({"id": "decimals", "value": decimals})
        overrides.append(
            {"matcher": {"id": "byName", "options": f"Value #{ref}"}, "properties": props}
        )
    calcs = []
    for header, left, op, right, unit, decimals in calculated:
        calcs.append(
            {
                "id": "calculateField",
                "options": {
                    "mode": "binary",
                    "alias": header,
                    "replaceFields": False,
                    "binary": {
                        "left": {"matcher": {"id": "byName", "options": left}},
                        "operator": op,
                        "right": {"matcher": {"id": "byName", "options": right}},
                    },
                },
            }
        )
        props = [{"id": "unit", "value": unit}]
        if decimals is not None:
            props.append({"id": "decimals", "value": decimals})
        overrides.append({"matcher": {"id": "byName", "options": header}, "properties": props})
    p: dict[str, Any] = {
        "type": "table",
        "title": title,
        "targets": targets,
        "transformations": [
            {"id": "merge", "options": {}},
            *calcs,
            {
                "id": "organize",
                "options": {
                    "excludeByName": {"Time": True, **{h: True for h in hidden}},
                    "indexByName": {k: i for i, k in enumerate(key)},
                },
            },
        ],
        "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": overrides},
        "options": {"showHeader": True, "sortBy": [{"displayName": sort_by, "desc": True}]},
    }
    if description:
        p["description"] = description
    return p


def _pct_stat(
    ds: Datasources, title: str, expr: str, description: str = "", warn: bool = False
) -> dict:
    steps = [("green", None), ("orange", 80), ("red", 95)] if warn else None
    return _stat(
        title,
        [_prom(ds, expr)],
        unit="percent",
        decimals=0,
        thresholds=steps,
        description=description,
    )


def _now(lay: _Layout, ds: Datasources) -> None:
    lay.row("Fleet now")
    lay.add(
        _stat("Nodes", [_prom(ds, f"count({_NODE})")], description="Nomad clients reporting"),
        3,
        4,
    )
    lay.add(
        _stat(
            "CPU schedulable",
            [_prom(ds, f"{CPU_CAP} * 1e6")],
            unit="hertz",
            description="Allocated + unallocated MHz, after each node's reserved block",
        ),
        3,
        4,
    )
    lay.add(_pct_stat(ds, "CPU reserved", f"100 * {CPU_RES} / {CPU_CAP}", warn=True), 3, 4)
    lay.add(
        _pct_stat(
            ds,
            "CPU used by allocations",
            f"100 * {CPU_USED} / {CPU_CAP}",
            "Of schedulable capacity",
        ),
        3,
        4,
    )
    lay.add(_stat("Memory schedulable", [_prom(ds, f"{MEM_CAP} * {MB}")], unit="bytes"), 3, 4)
    lay.add(_pct_stat(ds, "Memory reserved", f"100 * {MEM_RES} / {MEM_CAP}", warn=True), 3, 4)
    lay.add(
        _pct_stat(
            ds,
            "Memory used by allocations",
            f"100 * {MEM_USED} / ({MEM_CAP} * {MB})",
            "Includes page cache (cgroup v2), so it's an upper bound",
        ),
        3,
        4,
    )
    fit_cpu = f"floor({NODE_CPU_FREE} / $fit_cpu)"
    fit_mem = f"floor({NODE_MEM_FREE} / $fit_mem)"
    lay.add(
        _stat(
            "Room for more",
            [_prom(ds, f"sum(({fit_cpu} <= on(instance) {fit_mem}) or on(instance) {fit_mem})")],
            decimals=0,
            description="Allocations of $fit_cpu MHz / $fit_mem MB that still fit, node by node",
        ),
        3,
        4,
    )
    lay.add(
        _stat(
            "CPU reserved, idle",
            [_prom(ds, f"({CPU_RES} - {CPU_USED}) * 1e6")],
            unit="hertz",
            description="Reserved by allocations but not used: what rightsizing can win back",
        ),
        6,
        4,
    )
    lay.add(
        _stat(
            "Memory reserved, idle",
            [_prom(ds, f"{MEM_RES} * {MB} - {MEM_USED}")],
            unit="bytes",
            description="Reserved by allocations but not used (use includes page cache)",
        ),
        6,
        4,
    )
    stuck_cpu = f"sum({NODE_CPU_FREE} and on(instance) ({NODE_MEM_FREE} < $fit_mem))"
    lay.add(
        _stat(
            "CPU free on nodes without memory",
            [_prom(ds, f"{stuck_cpu} * 1e6 or vector(0)")],
            unit="hertz",
            description="Unreserved CPU on nodes with less than $fit_mem MB unreserved",
        ),
        6,
        4,
    )
    stuck_mem = f"sum({NODE_MEM_FREE} and on(instance) ({NODE_CPU_FREE} < $fit_cpu))"
    lay.add(
        _stat(
            "Memory free on nodes without CPU",
            [_prom(ds, f"{stuck_mem} * {MB} or vector(0)")],
            unit="bytes",
            description="Unreserved memory on nodes with less than $fit_cpu MHz unreserved",
        ),
        6,
        4,
    )


def _over_time(lay: _Layout, ds: Datasources) -> None:
    lay.row("Over time")
    lay.add(
        _timeseries(
            "CPU: schedulable, reserved, used",
            [
                _prom(ds, f"{CPU_CAP} * 1e6", "schedulable"),
                _prom(ds, f"{CPU_RES} * 1e6", "reserved", "B"),
                _prom(ds, f"{CPU_USED} * 1e6", "used by allocations", "C"),
            ],
            unit="hertz",
        ),
        12,
        8,
    )
    lay.add(
        _timeseries(
            "Memory: schedulable, reserved, used",
            [
                _prom(ds, f"{MEM_CAP} * {MB}", "schedulable"),
                _prom(ds, f"{MEM_RES} * {MB}", "reserved", "B"),
                _prom(ds, MEM_USED, "used by allocations", "C"),
            ],
            unit="bytes",
        ),
        12,
        8,
    )
    cpu_cap = f"sum by (node_class) ({NODE_CPU_CAP})"
    cpu_used = f"sum by (node_class) (nomad_client_allocs_cpu_total_ticks{_WITH_CLASS})"
    lay.add(
        _timeseries(
            "CPU per node class",
            [
                _prom(
                    ds,
                    f"100 * sum by (node_class) ({NODE_CPU_RES}) / {cpu_cap}",
                    "{{node_class}} reserved",
                ),
                _prom(ds, f"100 * {cpu_used} / {cpu_cap}", "{{node_class}} used", "B"),
            ],
            unit="percent",
        ),
        12,
        8,
    )
    mem_cap = f"sum by (node_class) ({NODE_MEM_CAP})"
    mem_used = f"sum by (node_class) (nomad_client_allocs_memory_usage{_WITH_CLASS})"
    lay.add(
        _timeseries(
            "Memory per node class",
            [
                _prom(
                    ds,
                    f"100 * sum by (node_class) ({NODE_MEM_RES}) / {mem_cap}",
                    "{{node_class}} reserved",
                ),
                _prom(ds, f"100 * {mem_used} / ({mem_cap} * {MB})", "{{node_class}} used", "B"),
            ],
            unit="percent",
        ),
        12,
        8,
    )


def _nodes(lay: _Layout, ds: Datasources) -> None:
    lay.row("Nodes")
    busy = _ne('sum by (instance) (rate(node_cpu_seconds_total{mode!~"idle|iowait|steal"}[5m]))')
    steal = _ne('sum by (instance) (rate(node_cpu_seconds_total{mode="steal"}[5m]))')
    cores = _ne('count by (instance) (node_cpu_seconds_total{mode="idle"})')
    # Denominators keyed by instance only, for one-to-one division.
    cpu_cap = "max by (instance) (nomad_client_allocated_cpu + nomad_client_unallocated_cpu)"
    mem_cap = (
        "max by (instance) "
        f"((nomad_client_allocated_memory + nomad_client_unallocated_memory) * {MB})"
    )
    alloc_cpu = f"sum by (instance) (nomad_client_allocs_cpu_total_ticks){_WITH_CLASS}"
    alloc_mem = f"sum by (instance) (nomad_client_allocs_memory_usage){_WITH_CLASS}"
    allocs = (
        "count by (instance) (count by (instance, alloc_id) (nomad_client_allocs_cpu_allocated))"
    )
    lay.add(
        _table(
            "Nodes",
            [
                ("CPU MHz", _per_node(NODE_CPU_CAP), "none", 0),
                ("CPU reserved", _per_node(f"100 * {NODE_CPU_RES} / {NODE_CPU_CAP}"), "percent", 0),
                (
                    "CPU used by allocs",
                    _per_node(f"100 * {alloc_cpu} / on(instance) {cpu_cap}"),
                    "percent",
                    0,
                ),
                ("Host CPU busy", _per_node(f"100 * {busy} / {cores}{_WITH_CLASS}"), "percent", 0),
                ("Steal", _per_node(f"100 * {steal} / {cores}{_WITH_CLASS}"), "percent", 1),
                ("Memory", _per_node(f"{NODE_MEM_CAP} * {MB}"), "bytes", 0),
                (
                    "Memory reserved",
                    _per_node(f"100 * {NODE_MEM_RES} / {NODE_MEM_CAP}"),
                    "percent",
                    0,
                ),
                (
                    "Memory used by allocs",
                    _per_node(f"100 * {alloc_mem} / on(instance) {mem_cap}"),
                    "percent",
                    0,
                ),
                ("CPU free MHz", _per_node(NODE_CPU_FREE), "none", 0),
                ("Memory free", _per_node(f"{NODE_MEM_FREE} * {MB}"), "bytes", 0),
                ("Allocations", _per_node(f"{allocs}{_WITH_CLASS}"), "none", 0),
            ],  # fmt: skip
            ds,
            key=["node", "node_class", "instance"],
            sort_by="CPU reserved",
            description=(
                "Host CPU busy includes work outside Nomad (ClickHouse, Postgres). Steal is "
                "CPU time taken by other VMs on the same hypervisor."
            ),
        ),
        24,
        12,
    )
    for title, metric, desc in (
        (
            "CPU pressure (top 10 nodes)",
            "node_pressure_cpu_waiting_seconds_total",
            "Share of time runnable tasks waited for a CPU (PSI some)",
        ),
        (
            "Memory pressure (top 10 nodes)",
            "node_pressure_memory_waiting_seconds_total",
            "Share of time tasks waited on memory: reclaim, swap-in (PSI some)",
        ),
    ):
        expr = f"topk(10, 100 * {_ne(f'rate({metric}[5m])')}{_IN_CLASS})"
        lay.add(
            _timeseries(title, [_prom(ds, expr, "{{node}}")], unit="percent", description=desc),
            12,
            8,
        )


def _jobs(lay: _Layout, ds: Datasources) -> None:
    lay.row("Jobs over the selected period")
    cpu_res = f"max by (jobname) ({_over('max_over_time', 'nomad_client_allocs_cpu_allocated')})"
    cpu_avg = f"avg by (jobname) ({_over('avg_over_time', 'nomad_client_allocs_cpu_total_ticks')})"
    cpu_p95 = f"max by (jobname) ({_over('p95', 'nomad_client_allocs_cpu_total_ticks')})"
    mem_res = f"max by (jobname) ({_over('max_over_time', 'nomad_client_allocs_memory_allocated')})"
    mem_avg = f"avg by (jobname) ({_over('avg_over_time', 'nomad_client_allocs_memory_usage')})"
    mem_max = f"max by (jobname) ({_over('max_over_time', 'nomad_client_allocs_memory_usage')})"
    allocs = (
        "avg_over_time((count by (jobname) (count by (jobname, alloc_id) "
        f"({_job('nomad_client_allocs_cpu_allocated')})))[$__range:5m])"
    )
    lay.add(
        _table(
            "Jobs: reserved vs used per allocation",
            [
                ("Allocations (avg)", allocs, "none", 1),
                ("CPU reserved MHz", cpu_res, "none", 0),
                ("CPU avg MHz", cpu_avg, "none", 0),
                ("CPU p95 MHz", cpu_p95, "none", 0),
                ("Memory reserved", mem_res, "bytes", 0),
                ("Memory avg", mem_avg, "bytes", 0),
                ("Memory max", mem_max, "bytes", 0),
            ],  # fmt: skip
            ds,
            key=["jobname"],
            sort_by="CPU reserved, idle MHz (avg)",
            # In the browser, so the table needs 7 Prometheus queries instead of 10.
            calculated=[
                ("CPU use", "Value #C", "/", "Value #B", "percentunit", 0),
                ("Memory use", "Value #F", "/", "Value #E", "percentunit", 0),
                ("idle per allocation", "Value #B", "-", "Value #C", "none", 0),
                ("CPU reserved, idle MHz (avg)", "idle per allocation", "*", "Value #A", "none", 0),
            ],  # fmt: skip
            hidden=("idle per allocation",),
            description=(
                "Per allocation over the selected period. Max and p95 are taken per task and "
                "summed per allocation (an upper bound); Prometheus samples every 15 s, so "
                "shorter peaks are missed. Memory includes page cache. Idle is (reserved minus "
                "average use) per allocation times the average number of allocations. "
                "Periodic runs are folded into their parent job."
            ),
        ),
        24,
        14,
    )


def fleet_dashboard(ds: Datasources) -> dict:
    lay = _Layout()
    _now(lay, ds)
    _over_time(lay, ds)
    _nodes(lay, ds)
    _jobs(lay, ds)
    d = _dashboard(UID, TITLE, [*_tags("prod", None), "fleet"], lay.panels, _variables(ds))
    d["time"] = {"from": "now-7d", "to": "now"}
    d["refresh"] = "5m"
    return d
