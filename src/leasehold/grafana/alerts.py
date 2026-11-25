"""Grafana alert rules derived from ``service.yml``.

Per service and environment, in the service's folder (``lhf-<env>-<service>``):

- the **built-in rules** per component (``alerts.defaults`` changes their priority and
  ``for``, or switches one off):

  ``crashloop``    a component that isn't scheduled restarted more than 3 times in 30m
  ``below_min``    fewer instances than ``count.min``
  ``no_progress``  a task type a worker consumes has due tasks, isn't paused, and no
                   task was acked for the rule's ``for`` (default 2h)
  ``lag_growing``  a processor's consumer lag grew without pause for ``for`` (1h)

- the service's own ``alerts.rules``; a rule with ``escalate`` becomes one Grafana rule
  per step, the same condition with a longer ``for`` and a higher priority;
- one rule per ``expects`` check, on the ClickHouse data source, evaluated every
  ``every``.

``when`` is PromQL with these functions of a task type (only the service's own):

    task_failure_ratio(T)  failed / (acked + failed) attempts, over 15m
    dead_tasks(T)          tasks out of attempts
    eligible_tasks(T)      pending tasks that are due
    leased_tasks(T)        tasks leased now

A range after one of them (``dead_tasks(T)[1h]``) becomes a subquery, so
``delta(dead_tasks(certlog.chunk)[1h]) > 0`` works. The rule fires while the
expression returns any series.

**Platform rules** (``PLATFORM_RULES``), in the platform folder (``lhf-<env>--platform``),
are about the platform itself rather than one service: today the egress system's proxy
accounts. Each is PromQL with ``{env}`` for the environment, and every series it returns
is its own alert, with the series' labels (one alert per account and state, not one for
all). Rules on tables every environment shares (the proxy accounts, owned by prod) are
rendered in prod only, so they don't fire twice. Levels (90% used, paid_until within 7
days, ...) are egress settings (``budgets.proxies``): the control API compares against
them and exports 0/1 gauges, so a changed level needs no re-apply. The outage rule
(``kafka_up``, ``control_api_up``) belongs here too.

The **watchdog** rules are platform rules on stuck and failing pipelines: unread data
against its topic's retention (``leasehold.kafka.lag``), replay loops, slow batches and
dead letters (``leasehold.runtime.inflight``) and scheduled runs (``leasehold.api.runs``).
Those that mean data loss or a stuck component are labelled ``watchdog=<part>`` (``lag``,
``replay`` or ``runs``), so that routing can pick them in an environment whose other
alerts don't go to a phone. Kafka's time retention is measured here; for topics with
``retention_bytes`` the size comes from the broker's JMX exporter (``kafka_log_log_size``).

Every rule is labelled ``managed_by=leasehold``, ``env``, ``service``, ``rule``,
``priority``, and ``component`` when it has one. Platform rules have ``service=platform``,
or the series' own ``service`` when they are about one, and the series' other labels.
``priority: page`` adds ``page="true"``, which Grafana's routing sends to a phone (prod
only, outside this module). The annotation ``leasehold_hash`` records the rendered rule, so
``apply`` updates a rule only when the hash changes.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from leasehold import naming
from leasehold.spec.models import Processor, Scheduled, ServiceSpec, TaskWorker
from leasehold.types import format_duration

MANAGED_LABEL = "managed_by"
MANAGED_VALUE = "leasehold"
HASH_ANNOTATION = "leasehold_hash"
RULE_GROUP = "leasehold"
EXPR_DATASOURCE = "__expr__"

CRASHLOOP_RESTARTS = 3
FAILURE_WINDOW = "15m"


class AlertSpecError(ValueError):
    pass


@dataclass(frozen=True)
class RulePlan:
    uid: str
    title: str
    folder_uid: str
    group: str
    interval_s: int
    service: str
    rule: dict[str, Any] = field(compare=False, hash=False)


def rule_uid(env: str, service: str, name: str) -> str:
    s = f"lha-{env}-{service}-{name}"
    if len(s) <= 40:
        return s
    return f"{s[:31]}-{hashlib.sha1(s.encode()).hexdigest()[:8]}"


# --- `when` -----------------------------------------------------------------------------------

_FUNCTIONS = "task_failure_ratio|dead_tasks|eligible_tasks|leased_tasks"
_MACRO = re.compile(rf"\b({_FUNCTIONS})\(\s*([^)]*?)\s*\)")
_RANGE = re.compile(r"^\[(\d+[smhdw])\]")


def _macro(name: str, env: str, task_type: str) -> str:
    sel = f'env="{env}", task_type="{task_type}"'
    if name == "task_failure_ratio":
        return (
            f'(sum(rate(leasehold_tasks_total{{{sel}, op="failed"}}[{FAILURE_WINDOW}]))'
            f' / sum(rate(leasehold_tasks_total{{{sel}, op=~"acked|failed"}}[{FAILURE_WINDOW}])))'
        )
    metric = {
        "dead_tasks": "leasehold_frontier_dead_tasks",
        "eligible_tasks": "leasehold_frontier_eligible_tasks",
        "leased_tasks": "leasehold_frontier_leased_tasks",
    }[name]
    return f"max({metric}{{{sel}}})"


def render_when(when: str, spec: ServiceSpec, env: str) -> str:
    """``when`` with the task-type functions replaced by PromQL."""
    own = set(spec.frontier.task_types)
    out, pos = [], 0
    for m in _MACRO.finditer(when):
        name, tt = m.group(1), m.group(2)
        if tt not in own:
            raise AlertSpecError(
                f"{spec.name}: {name}({tt}): not a task type of this service "
                f"({', '.join(sorted(own)) or 'none'})"
            )
        out.append(when[pos : m.start()])
        out.append(_macro(name, env, tt))
        pos = m.end()
        r = _RANGE.match(when[pos:])
        if r:
            out.append(f"[{r.group(1)}:1m]")
            pos += r.end()
    out.append(when[pos:])
    return "".join(out).strip()


# --- rule bodies ------------------------------------------------------------------------------


def _prom_rule(expr: str, prometheus_uid: str) -> tuple[list[dict], str]:
    """Fires while ``expr`` returns any series."""
    data = [
        {
            "refId": "A",
            "relativeTimeRange": {"from": 600, "to": 0},
            "datasourceUid": prometheus_uid,
            "model": {"refId": "A", "expr": f"count({expr}) or vector(0)", "instant": True},
        },
        {
            "refId": "B",
            "datasourceUid": EXPR_DATASOURCE,
            "model": {"refId": "B", "type": "reduce", "expression": "A", "reducer": "last"},
        },
        {
            "refId": "C",
            "datasourceUid": EXPR_DATASOURCE,
            "model": {
                "refId": "C",
                "type": "threshold",
                "expression": "B",
                "conditions": [{"evaluator": {"type": "gt", "params": [0]}}],
            },
        },
    ]
    return data, "C"


def _series_rule(expr: str, prometheus_uid: str) -> tuple[list[dict], str]:
    """One alert per series ``expr`` returns, with its labels; ``$values.B`` is its value."""
    data = [
        {
            "refId": "A",
            "relativeTimeRange": {"from": 600, "to": 0},
            "datasourceUid": prometheus_uid,
            "model": {"refId": "A", "expr": expr, "instant": True},
        },
        {
            "refId": "B",
            "datasourceUid": EXPR_DATASOURCE,
            "model": {"refId": "B", "type": "reduce", "expression": "A", "reducer": "last"},
        },
        {
            "refId": "C",
            "datasourceUid": EXPR_DATASOURCE,
            "model": {"refId": "C", "type": "math", "expression": "$B == $B"},
        },
    ]
    return data, "C"


def _ch_rule(sql: str, low: float | None, high: float | None, clickhouse_uid: str):
    """Fires while the query's one number is outside [low, high]."""
    outside = " || ".join(
        x
        for x in (
            f"$B < {low:g}" if low is not None else "",
            f"$B > {high:g}" if high is not None else "",
        )
        if x
    )
    data = [
        {
            "refId": "A",
            "relativeTimeRange": {"from": 600, "to": 0},
            "datasourceUid": clickhouse_uid,
            "model": {
                "refId": "A",
                "editorType": "sql",
                "format": 1,
                "queryType": "table",
                "rawSql": sql.strip(),
            },
        },
        {
            "refId": "B",
            "datasourceUid": EXPR_DATASOURCE,
            "model": {"refId": "B", "type": "reduce", "expression": "A", "reducer": "last"},
        },
        {
            "refId": "C",
            "datasourceUid": EXPR_DATASOURCE,
            "model": {"refId": "C", "type": "math", "expression": outside},
        },
    ]
    return data, "C"


PLATFORM = "platform"


@dataclass
class _Builder:
    spec: ServiceSpec | None  # None: the platform rules
    env: str
    folder_uid: str
    prometheus_uid: str
    clickhouse_uid: str
    rules: list[RulePlan] = field(default_factory=list)

    def add(
        self,
        name: str,
        title: str,
        data: list[dict],
        condition: str,
        priority: str,
        for_: timedelta | None,
        description: str,
        component: str | None = None,
        group: str = RULE_GROUP,
        interval_s: int = 60,
        no_data: str = "OK",
        summary: str | None = None,
        extra_labels: dict[str, str] | None = None,
        series_service: bool = False,
    ) -> None:
        service = self.spec.name if self.spec else PLATFORM
        labels = {
            MANAGED_LABEL: MANAGED_VALUE,
            "env": self.env,
            "service": service,
            "rule": name,
            "priority": priority,
            **(extra_labels or {}),
        }
        if series_service:
            del labels["service"]
        if component:
            labels["component"] = component
        if priority == "page":
            labels["page"] = "true"
        suffix = "" if self.env == naming.PROD else f" ({self.env})"
        rule = {
            "uid": rule_uid(self.env, service if self.spec else f"-{PLATFORM}", name),
            "title": f"{service}: {title}{suffix}",
            "folderUID": self.folder_uid,
            "ruleGroup": group,
            "orgID": 1,
            "condition": condition,
            "data": data,
            "noDataState": no_data,
            "execErrState": "Error",
            "for": format_duration(for_ or timedelta(0)),
            "labels": labels,
            "annotations": {
                "summary": f"{service} {self.env}: {summary or title}",
                "description": description,
            },
            "isPaused": False,
        }
        digest = hashlib.sha256(json.dumps(rule, sort_keys=True).encode()).hexdigest()[:16]
        rule["annotations"][HASH_ANNOTATION] = digest
        self.rules.append(
            RulePlan(rule["uid"], rule["title"], self.folder_uid, group, interval_s, service, rule)
        )

    def prom(self, name, title, expr, priority, for_, description, component=None) -> None:
        data, cond = _prom_rule(expr, self.prometheus_uid)
        self.add(name, title, data, cond, priority, for_, description, component)


def _defaults(b: _Builder) -> None:
    spec, env = b.spec, b.env
    defaults = spec.alerts.effective_defaults() if spec.alerts else _no_overrides()
    ns = naming.nomad_namespace(env)
    for cname, c in sorted(spec.components.items()):
        job = naming.nomad_job(spec.name, cname)
        rt = f'env="{env}", service="{spec.name}", component="{cname}"'

        priority, for_, enabled = defaults["crashloop"]
        if enabled and not isinstance(c, Scheduled):
            b.prom(
                f"{cname}-crashloop",
                f"{cname} is crash-looping",
                f"sum(changes(process_start_time_seconds{{{rt}}}[30m])) > {CRASHLOOP_RESTARTS}",
                priority,
                for_,
                f"{cname} restarted more than {CRASHLOOP_RESTARTS} times in 30 minutes.",
                cname,
            )

        priority, for_, enabled = defaults["below_min"]
        count = getattr(c, "count", None)
        if enabled and count is not None and count.min > 0:
            sel = f'namespace="{ns}", exported_job="{job}"'
            b.prom(
                f"{cname}-below-min",
                f"{cname} has fewer than {count.min} instances",
                f"(count(nomad_client_allocs_cpu_user{{{sel}}}) or vector(0)) < {count.min}",
                priority,
                for_,
                f"Fewer running allocations of {job} than count.min ({count.min}).",
                cname,
            )

        priority, for_, enabled = defaults["no_progress"]
        if enabled and isinstance(c, TaskWorker):
            window = format_duration(for_ or timedelta(hours=2))
            for tt in c.consumes_tasks:
                sel = f'env="{env}", task_type="{tt}"'
                b.prom(
                    f"{cname}-no-progress-{tt.replace('.', '-')}",
                    f"no {tt} task acked in {window}",
                    f'(sum(increase(leasehold_tasks_total{{{sel}, op="acked"}}[{window}]))'
                    f" or vector(0)) == 0"
                    f" and on() max(leasehold_frontier_eligible_tasks{{{sel}}}) > 0"
                    f" and on() max(leasehold_frontier_paused{{{sel}}}) == 0",
                    priority,
                    timedelta(minutes=5),
                    f"{tt} has due tasks and isn't paused, but none was acked in {window}.",
                    cname,
                )

        priority, for_, enabled = defaults["lag_growing"]
        if enabled and isinstance(c, Processor):
            group = naming.consumer_group(env, spec.name, cname)
            b.prom(
                f"{cname}-lag-growing",
                f"{cname} is falling behind",
                f'deriv(sum(leasehold_kafka_consumer_lag{{group="{group}"}})[30m:1m]) > 0',
                priority,
                for_,
                f"Consumer group {group}: lag has kept growing.",
                cname,
            )


def _no_overrides() -> dict[str, tuple[str, timedelta | None, bool]]:
    from leasehold.spec.models import DEFAULT_ALERTS

    return {k: (p, f, True) for k, (p, f) in DEFAULT_ALERTS.items()}


def _custom(b: _Builder) -> None:
    if not b.spec.alerts:
        return
    for r in b.spec.alerts.rules:
        expr = render_when(r.when, b.spec, b.env)
        base = r.for_ or timedelta(0)
        description = f"when: {r.when}" + (f", for {format_duration(base)}" if r.for_ else "")
        if r.escalate is None:
            b.prom(r.name, r.name, expr, r.priority, r.for_, description, r.component)
            continue
        for step in r.escalate:
            first = step.after == timedelta(0)
            name = r.name if first else f"{r.name}-after-{format_duration(step.after)}"
            total = base + step.after
            title = r.name if first else f"{r.name} for {format_duration(total)}"
            b.prom(name, title, expr, step.priority, total, description, r.component)


def _expects(b: _Builder) -> None:
    if not b.spec.expects:
        return
    database = (
        naming.clickhouse_database(b.env, b.spec.clickhouse.database) if b.spec.clickhouse else None
    )
    for exp in b.spec.expects:
        if database is None:
            raise AlertSpecError(f"{b.spec.name}: expects {exp.name} needs a clickhouse database")
        bounds = " and ".join(
            x
            for x in (
                f">= {exp.min:g}" if exp.min is not None else "",
                f"<= {exp.max:g}" if exp.max is not None else "",
            )
            if x
        )
        data, cond = _ch_rule(exp.render_sql(database), exp.min, exp.max, b.clickhouse_uid)
        every = format_duration(exp.every)
        b.add(
            f"expect-{exp.name}",
            f"{exp.name} out of range",
            data,
            cond,
            exp.priority,
            None,
            f"Expected {bounds}: {' '.join(exp.sql.split())}",
            group=f"{RULE_GROUP}-every-{every}",
            interval_s=int(exp.every.total_seconds()),
            no_data="Alerting",
        )


def service_rules(
    spec: ServiceSpec, env: str, folder_uid: str, prometheus_uid: str, clickhouse_uid: str
) -> list[RulePlan]:
    b = _Builder(spec, env, folder_uid, prometheus_uid, clickhouse_uid)
    _defaults(b)
    _custom(b)
    _expects(b)
    seen: set[str] = set()
    for r in b.rules:
        if r.uid in seen:
            raise AlertSpecError(f"{spec.name}: two alert rules render to uid {r.uid}")
        seen.add(r.uid)
    return b.rules


# --- platform rules -----------------------------------------------------------------------------


@dataclass(frozen=True)
class PlatformRule:
    name: str
    title: str
    expr: str  # PromQL; {env} is the environment; every series it returns is an alert
    priority: str
    summary: str  # Grafana template: {{ $labels.account }}, {{ $values.B.Value }}
    description: str
    for_: timedelta | None = None
    owner_only: bool = False  # on tables every environment shares: prod only
    watchdog: str | None = None  # the label ``watchdog``
    # The series' own ``service`` label is kept. A rule's label wins over a series' label
    # of the same name, so with ``service=platform`` two services' components of one name
    # would become one alert.
    series_service: bool = False


_ACTIVE = (
    " and on (account) (max by (account) "
    '(leasehold_proxy_account_status{{env="{env}", status!="disabled"}}) == 1)'
)


def _behind(threshold: float) -> str:
    """Per group and topic: the worst partition's share of retention, above a threshold."""
    by_time = 'leasehold_kafka_group_retention_used_ratio{{env="{env}"}}'
    by_size = (
        'leasehold_kafka_group_unread_share{{env="{env}"}}'
        " * on (topic, partition) group_left() max by (topic, partition) (kafka_log_log_size)"
        " / on (topic) group_left() max by (topic) "
        '(leasehold_kafka_retention_bytes{{env="{env}"}})'
    )
    return (
        f"max by (group, topic) ({by_time}) > {threshold}"
        f" or max by (group, topic) ({by_size}) > {threshold}"
    )


_PERCENT = "{{ humanizePercentage $values.B.Value }}"
# Without fresh log rows run outcomes can't be judged: one alert for that, not one per job.
_LOGS_FRESH = (
    ' and on () (time() - max(leasehold_logs_newest_row_timestamp_seconds{{env="{env}"}}) < 900)'
)


def _subject(aggregate: str, expr: str, *labels: str) -> str:
    """An aggregation of control API metrics by the service (and component) they are
    about. The scrape target has labels of the same names (the API itself), so Prometheus
    stores the metric's own as ``exported_<label>``; the result has them as ``<label>``."""
    out = f"{aggregate} by ({', '.join(f'exported_{x}' for x in labels)}) ({expr})"
    for x in labels:
        out = f'label_replace({out}, "{x}", "$1", "exported_{x}", "(.*)")'
    # without the exported_* names, so the rule's series are labelled like the others
    return f"max by ({', '.join(labels)}) ({out})"


WATCHDOG_RULES: tuple[PlatformRule, ...] = (
    PlatformRule(
        "lag-retention-warning",
        "unread data at 25% of retention",
        _behind(0.25),
        "warning",
        "{{ $labels.group }} on {{ $labels.topic }}: unread data at " + _PERCENT + " of retention",
        "The oldest unread message's share of the topic's retention. At 100% Kafka deletes "
        "messages this group hasn't read.",
        for_=timedelta(minutes=10),
        watchdog="lag",
    ),
    PlatformRule(
        "lag-retention",
        "unread data at 50% of retention",
        _behind(0.5),
        "page",
        "{{ $labels.group }} on {{ $labels.topic }}: unread data at " + _PERCENT + " of retention",
        "The oldest unread message's share of the topic's retention. At 100% Kafka deletes "
        "messages this group hasn't read.",
        for_=timedelta(minutes=5),
        watchdog="lag",
    ),
    PlatformRule(
        "lag-data-lost",
        "Kafka deleted unread messages",
        "sum by (group, topic) (max_over_time("
        'leasehold_kafka_group_skipped_messages{{env="{env}"}}[1h])) > 0',
        "page",
        "{{ $labels.group }} on {{ $labels.topic }}: {{ $values.B.Value }} messages were deleted "
        "before they were read",
        "The group's committed offset is before the first message the partition still has.",
        watchdog="lag",
    ),
    PlatformRule(
        "lag-not-measured",
        "consumer lag not measured",
        "count(time() - leasehold_kafka_lag_last_success_timestamp_seconds"
        '{{env="{env}"}} > 600)'
        ' or absent(leasehold_kafka_lag_last_success_timestamp_seconds{{env="{env}"}})',
        "high",
        "consumer lag isn't measured ({{ $values.B.Value }} group and topic pairs)",
        "The control API's lag measurement fails or isn't running; its log says why.",
        for_=timedelta(minutes=10),
        watchdog="lag",
    ),
    PlatformRule(
        "replay-loop",
        "restart loop",
        "sum by (service, component) (changes("
        'process_start_time_seconds{{env="{env}", service!=""}}[6h])) > 20',
        "page",
        "{{ $labels.service }} {{ $labels.component }} restarted {{ $values.B.Value }} times "
        "in 6 hours",
        "Restarts that go on for hours: a processor then replays the same batch. Its log has "
        "'the previous process died while handling a batch' with the event.",
        for_=timedelta(minutes=10),
        watchdog="replay",
        series_service=True,
    ),
    PlatformRule(
        "batch-killed",
        "processor died on a batch",
        "max by (service, component) (max_over_time("
        'leasehold_processor_batch_kills{{env="{env}"}}[15m])) >= 2',
        "high",
        "{{ $labels.service }} {{ $labels.component }} died {{ $values.B.Value }} times on the "
        "same batch",
        "After 5 times the batch is handled one event at a time and the event that kills the "
        "process goes to the dead-letter topic.",
        watchdog="replay",
        series_service=True,
    ),
    PlatformRule(
        "dead-letter",
        "event dead-lettered",
        _subject(
            "sum",
            'leasehold_dead_letters_total_offset{{env="{env}"}} - '
            'min_over_time(leasehold_dead_letters_total_offset{{env="{env}"}}[1h])',
            "service",
        )
        + " > 0",
        "page",
        "{{ $labels.service }}: {{ $values.B.Value }} events moved to the dead-letter topic",
        "Events that killed their processor when handled alone. The message's headers say "
        "where each came from; kept for 30 days.",
        watchdog="replay",
        series_service=True,
    ),
    PlatformRule(
        "batch-slow",
        "processor batches slow",
        "histogram_quantile(0.99, sum by (le, service, component) (rate("
        'leasehold_processor_batch_seconds_bucket{{env="{env}"}}[15m]))) > 30',
        "warning",
        "{{ $labels.service }} {{ $labels.component }}: p99 batch time "
        '{{ printf "%.0f" $values.B.Value }} s',
        "Batches that finish slowly. One that doesn't finish is in the log ('a batch is slow').",
        for_=timedelta(minutes=15),
        watchdog="replay",
        series_service=True,
    ),
    PlatformRule(
        "run-failed",
        "scheduled run failed",
        _subject(
            "max", 'leasehold_scheduled_last_run_failed{{env="{env}"}}', "service", "component"
        )
        + " == 1"
        + _LOGS_FRESH,
        "warning",
        "{{ $labels.service }} {{ $labels.component }}: the last run failed",
        "Resolved by the next run that finishes. The run's log line is 'run failed'.",
        for_=timedelta(minutes=15),
        watchdog="runs",
        series_service=True,
    ),
    PlatformRule(
        "run-overdue",
        "no successful scheduled run",
        "time() - "
        + _subject(
            "max",
            'leasehold_scheduled_success_due_timestamp_seconds{{env="{env}"}}',
            "service",
            "component",
        )
        + " > 0"
        + _LOGS_FRESH,
        "high",
        "{{ $labels.service }} {{ $labels.component }}: no successful run for too long",
        "Nothing finished in twice the schedule's longest gap plus the timeout (at least 30m): "
        "failing, killed, or not started.",
        for_=timedelta(minutes=5),
        watchdog="runs",
        series_service=True,
    ),
    PlatformRule(
        "runs-not-readable",
        "scheduled runs not checked",
        '(time() - max(leasehold_logs_newest_row_timestamp_seconds{{env="{env}"}}) > 900)'
        ' or (time() - max(leasehold_logs_last_success_timestamp_seconds{{env="{env}"}}) > 900)'
        ' or absent(leasehold_logs_last_success_timestamp_seconds{{env="{env}"}})',
        "high",
        "run outcomes of scheduled components can't be read",
        "No new rows in the log table for 15 minutes, or the control API can't query it.",
        for_=timedelta(minutes=10),
        watchdog="runs",
    ),
)

PLATFORM_RULES: tuple[PlatformRule, ...] = (
    PlatformRule(
        "proxy-account-state",
        "proxy account not ok",
        'max by (account, vendor, state) (leasehold_proxy_account_state{{env="{env}", '
        'state!="ok"}}) == 1' + _ACTIVE,
        "page",
        "proxy account {{ $labels.account }} ({{ $labels.vendor }}) is {{ $labels.state }}",
        "The account's exits and pools are out of dispatch. Evidence: "
        "GET /v1/egress/accounts (state_reason). quota_exhausted is the 100% level.",
        owner_only=True,
    ),
    PlatformRule(
        "proxy-quota-warning",
        "proxy account quota",
        'max by (account, vendor) (leasehold_proxy_remaining_bytes{{env="{env}"}}) / 1e9'
        " and on (account) (max by (account) "
        '(leasehold_proxy_quota_warning{{env="{env}"}}) == 1)' + _ACTIVE,
        "warning",
        "proxy account {{ $labels.account }} ({{ $labels.vendor }}): "
        '{{ printf "%.2f" $values.B.Value }} GB left; add GB',
        "At proxies.warn_used of the quota used, or below the account's warn_remaining_gb.",
        owner_only=True,
    ),
    PlatformRule(
        "proxy-usage-poll-failing",
        "proxy usage API failing",
        'max by (account) (leasehold_proxy_usage_poll_failing{{env="{env}"}}) == 1' + _ACTIVE,
        "warning",
        "proxy account {{ $labels.account }}: the vendor's usage API keeps failing",
        "Failing for longer than proxies.usage_poll_warn_after; dispatch goes on with our own "
        "byte count.",
        owner_only=True,
    ),
    PlatformRule(
        "proxy-paid-until",
        "proxy account paid_until",
        'max by (account, vendor) (leasehold_proxy_paid_until_warning{{env="{env}"}}) == 1'
        + _ACTIVE,
        "warning",
        "proxy account {{ $labels.account }} ({{ $labels.vendor }}) is paid only until soon",
        "paid_until is within proxies.paid_until_warn.",
        owner_only=True,
    ),
    PlatformRule(
        "proxy-auto-disabled",
        "proxy exit or pool disabled",
        'sum by (account) (increase(leasehold_proxy_auto_disabled_total{{env="{env}"}}[15m])) > 0',
        "warning",
        "proxy account {{ $labels.account }}: an exit or pool was disabled by connect failures",
        "proxies.max_connect_failures in a row. leasehold proxy list shows which; enable it "
        "again once it works.",
    ),
    PlatformRule(
        "proxy-usage-drift",
        "proxy byte count drift",
        'max by (account, vendor) (leasehold_proxy_usage_drift_warning{{env="{env}"}}) == 1'
        + _ACTIVE,
        "info",
        "proxy account {{ $labels.account }}: our byte count and the vendor's differ",
        "By more than proxies.drift_warn of the plan.",
        for_=timedelta(hours=1),
        owner_only=True,
    ),
    PlatformRule(
        "proxy-exit-moved",
        "proxy exit moved",
        'sum(increase(leasehold_proxy_exit_moves_total{{env="{env}"}}[1h])) > 0',
        "info",
        "a fixed proxy exit has a new address",
        "The probe saw an exit at a new address; its old egress row was disabled.",
        owner_only=True,
    ),
    PlatformRule(
        "proxy-task-type-stopped",
        "task type stopped on a proxy account",
        'max by (task_type, account) (leasehold_proxy_stopped{{env="{env}"}}) == 1',
        "info",
        "{{ $labels.task_type }} stopped on proxy account {{ $labels.account }} "
        "(stop_at_used); resolved when it resumes",
        "The task type's stop_at_used share of the account is used.",
    ),
    *WATCHDOG_RULES,
)


def platform_rules(env: str, folder_uid: str, prometheus_uid: str) -> list[RulePlan]:
    b = _Builder(None, env, folder_uid, prometheus_uid, "")
    for r in PLATFORM_RULES:
        if r.owner_only and env != naming.PROD:
            continue
        data, cond = _series_rule(r.expr.format(env=env), prometheus_uid)
        b.add(r.name, r.title, data, cond, r.priority, r.for_, r.description, summary=r.summary,
              extra_labels={"watchdog": r.watchdog} if r.watchdog else None,
              series_service=r.series_service)  # fmt: skip
    return b.rules
