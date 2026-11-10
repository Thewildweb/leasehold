"""The ``service.yml`` model.

A service declares everything it owns and touches: its components, the Kafka
topics it produces, the task types it owns, its ClickHouse sinks, its rate
budgets towards non-target destinations, and its checks and alerts.

Validation is strict: unknown fields are errors, and the cross-checks at the
bottom enforce the ownership rules:

- A service owns its task types (names start with ``<service>.``). Only its own
  components create or consume them; work for another service is created by that
  service's own processor, reading this service's events.
- A component may only produce topics this service declares.
- Every declared topic is produced by some component, and every task type is
  consumed by some task-worker.

Checks that need more than one service (does a consumed topic exist?) live in
``leasehold.spec.registry``.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from leasehold import naming
from leasehold.types import (
    PRIORITY_ORDER,
    ComponentName,
    Duration,
    EnvVarName,
    Identifier,
    Priority,
    RuleName,
    SecretPath,
    ServiceName,
    SnakeName,
    TaskTypeName,
    TopicName,
    TopicRefStr,
)

SPEC_VERSION = 1


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# --- Components --------------------------------------------------------------------------------


class Resources(_Strict):
    cpu: int = Field(gt=0, description="CPU reservation in MHz (Nomad)")
    memory: int = Field(gt=0, description="Memory reservation in MB")
    memory_max: int | None = Field(
        default=None, gt=0, description="Hard memory ceiling in MB; allows bursting above `memory`"
    )

    @model_validator(mode="after")
    def _max_above_reservation(self) -> Resources:
        if self.memory_max is not None and self.memory_max < self.memory:
            raise ValueError("memory_max must be >= memory")
        return self


class Count(_Strict):
    min: int = Field(ge=0)
    max: int = Field(ge=1)

    @model_validator(mode="after")
    def _ordered(self) -> Count:
        if self.max < self.min:
            raise ValueError("max must be >= min")
        return self


class Scaling(_Strict):
    metric: Literal["eligible_tasks", "consumer_lag"] = Field(
        description="eligible_tasks for task-workers (tasks dispatchable right now, after "
        "leases, backoff and budgets); consumer_lag for processors"
    )
    per_instance: int = Field(gt=0, description="Target amount of the metric per instance")


_SECRETISH = re.compile(r"(PASSWORD|PASSWD|SECRET|TOKEN|API_KEY|PRIVATE_KEY|CREDENTIAL)")


class _ComponentBase(_Strict):
    node_class: str = Field(min_length=1, description="Nomad node class to run on")
    resources: Resources
    priority: int = Field(
        default=50, ge=1, le=100, description="Nomad job priority; higher preempts lower"
    )
    command: list[str] | None = Field(
        default=None, description="Override the image's default command"
    )
    env: dict[EnvVarName, str] = Field(
        default_factory=dict,
        description="Non-secret configuration only; secrets come from `secrets`",
    )
    produces: list[TopicName] = Field(
        default_factory=list, description="This service's topics this component writes to"
    )
    creates_tasks: list[TaskTypeName] = Field(
        default_factory=list, description="This service's task types this component creates"
    )
    kill_timeout: Duration = Field(
        default=timedelta(seconds=30),
        description="Time between SIGTERM and SIGKILL, for a graceful drain",
    )

    @field_validator("env")
    @classmethod
    def _no_secrets_in_env(cls, value: dict[str, str]) -> dict[str, str]:
        bad = sorted(k for k in value if _SECRETISH.search(k))
        if bad:
            raise ValueError(
                f"env looks like it holds secrets ({', '.join(bad)}); "
                "declare them under `secrets` instead"
            )
        return value


class TaskWorker(_ComponentBase):
    """Leases tasks from the frontier, emits one event per attempt, then acks."""

    kind: Literal["task-worker"]
    consumes_tasks: list[TaskTypeName] = Field(min_length=1)
    count: Count
    scaling: Scaling | None = None


class Processor(_ComponentBase):
    """A Kafka consumer group: reads events, writes events, tasks or ClickHouse rows."""

    kind: Literal["processor"]
    consumes: list[TopicRefStr] = Field(
        min_length=1, description="Topic references, e.g. 'certlog.entries.v1'"
    )
    count: Count = Field(default_factory=lambda: Count(min=1, max=1))
    scaling: Scaling | None = None


_CRON_FIELD = re.compile(r"^(\*|\d+(-\d+)?)(/\d+)?(,(\*|\d+(-\d+)?)(/\d+)?)*$")
_CRON_MACROS = {"@hourly", "@daily", "@weekly", "@monthly"}


class Scheduled(_ComponentBase):
    """A periodic job (Nomad periodic batch), e.g. a planner that creates tasks."""

    kind: Literal["scheduled"]
    schedule: str = Field(description="Cron expression (5 fields) or @hourly/@daily/…")
    timeout: Duration = Field(
        default=timedelta(minutes=10), description="A run longer than this is killed"
    )

    @field_validator("schedule")
    @classmethod
    def _valid_cron(cls, value: str) -> str:
        if value in _CRON_MACROS:
            return value
        fields = value.split()
        if len(fields) != 5 or not all(_CRON_FIELD.match(f) for f in fields):
            raise ValueError(
                f"invalid cron schedule {value!r}: expected 5 fields like '*/5 * * * *'"
            )
        return value


class Api(_ComponentBase):
    """A long-running HTTP service."""

    kind: Literal["api"]
    count: Count


Component = Annotated[TaskWorker | Processor | Scheduled | Api, Field(discriminator="kind")]


# --- Kafka ------------------------------------------------------------------------------------


class KafkaTopic(_Strict):
    version: int = Field(
        default=1, ge=1, description="Major version; bump only for breaking changes (new topic)"
    )
    payload_model: str = Field(
        min_length=1, description="Payload model, e.g. 'certlog_shared.models:CertChunk'"
    )
    partitions: int = Field(ge=1, le=1024)
    retention: Duration
    retention_bytes: int | None = Field(
        default=None, gt=0, description="Size cap per partition, as a backstop to time retention"
    )
    key: str | None = Field(default=None, description="Payload field used as partition key")


class KafkaSection(_Strict):
    produces: dict[TopicName, KafkaTopic] = Field(default_factory=dict)


# --- Frontier ---------------------------------------------------------------------------------


class TaskType(_Strict):
    key: str = Field(min_length=1, description="What the task key is, e.g. 'domain', 'log:chunk'")
    recrawl_interval: Duration | None = Field(
        default=None,
        description="Recurring: back to pending this long after each ack. None = one-shot",
    )
    max_attempts: int = Field(default=5, ge=1)
    retry_backoff: Duration = Field(
        default=timedelta(minutes=5), description="Base backoff; doubles per failed attempt"
    )
    default_priority: int = Field(default=50, ge=0, le=100)
    capabilities: list[Identifier] = Field(
        default_factory=list, description="What a worker needs, e.g. http, playwright"
    )
    payload_model: str | None = None


class FrontierSection(_Strict):
    task_types: dict[TaskTypeName, TaskType] = Field(default_factory=dict)


# --- ClickHouse -------------------------------------------------------------------------------


class Sink(_Strict):
    topic: str = Field(
        description="A topic of this service by name ('entries'), or any topic by reference"
    )
    schema_file: str = Field(min_length=1, description="SQL file, relative to service.yml")


class ClickHouseSection(_Strict):
    database: SnakeName
    sinks: dict[SnakeName, Sink] = Field(default_factory=dict)


# --- Destinations (non-target rate budgets) ---------------------------------------------------


class Destination(_Strict):
    per_egress_rps: float = Field(gt=0, description="Max requests/s per egress IP")
    group_by: str | None = Field(
        default=None, description="What one budget covers, e.g. 'provider'"
    )
    description: str | None = None


# --- Checks and alerts ------------------------------------------------------------------------


DATABASE_PLACEHOLDER = "{database}"


class Expect(_Strict):
    """An output check: SQL on ClickHouse that returns one number.

    Refer to the service's database as ``{database}``; it's replaced by the
    environment's database name (``ct_logs`` in prod, ``dev_ct_logs`` in dev).
    """

    name: RuleName
    sql: str = Field(min_length=1, description="SQL returning one number; use {database}")
    min: float | None = None
    max: float | None = None
    every: Duration = Field(default=timedelta(minutes=15))
    priority: Priority

    @model_validator(mode="after")
    def _bounds(self) -> Expect:
        if self.min is None and self.max is None:
            raise ValueError("an expect needs `min`, `max` or both")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("min must be <= max")
        return self

    def render_sql(self, database: str) -> str:
        return self.sql.replace(DATABASE_PLACEHOLDER, database)


DefaultRuleName = Literal["crashloop", "below_min", "no_progress", "lag_growing"]

# Built-in rules every component gets. No default uses `page`; a rule must set it explicitly.
DEFAULT_ALERTS: dict[str, tuple[str, timedelta | None]] = {
    "crashloop": ("high", None),
    "below_min": ("high", timedelta(minutes=15)),
    "no_progress": ("high", timedelta(hours=2)),
    "lag_growing": ("warning", timedelta(hours=1)),
}


class DefaultOverride(_Strict):
    priority: Priority | None = None
    for_: Duration | None = Field(default=None, alias="for")
    enabled: bool = True


class EscalationStep(_Strict):
    after: Duration
    priority: Priority


class AlertRule(_Strict):
    name: RuleName
    when: str = Field(min_length=1, description="Condition expression (rendered to Grafana)")
    component: ComponentName | None = Field(
        default=None, description="Limit to one component; default: the whole service"
    )
    for_: Duration | None = Field(default=None, alias="for")
    priority: Priority | None = None
    escalate: list[EscalationStep] | None = Field(
        default=None, description="Same condition, raised in priority the longer it lasts"
    )

    @model_validator(mode="after")
    def _priority_or_escalation(self) -> AlertRule:
        if (self.priority is None) == (self.escalate is None):
            raise ValueError("an alert rule needs exactly one of `priority` or `escalate`")
        if self.escalate is not None:
            steps = self.escalate
            if not steps:
                raise ValueError("`escalate` needs at least one step")
            if steps[0].after != timedelta(0):
                raise ValueError("the first escalation step must have `after: 0`")
            for a, b in zip(steps, steps[1:], strict=False):
                if b.after <= a.after:
                    raise ValueError("escalation steps must have increasing `after`")
                if PRIORITY_ORDER[b.priority] < PRIORITY_ORDER[a.priority]:
                    raise ValueError("escalation steps may not lower the priority")
        return self


class Alerts(_Strict):
    defaults: dict[DefaultRuleName, DefaultOverride] = Field(default_factory=dict)
    rules: list[AlertRule] = Field(default_factory=list)

    def effective_defaults(self) -> dict[str, tuple[str, timedelta | None, bool]]:
        """Built-in rules merged with this service's overrides: name → (priority, for, enabled)."""
        merged: dict[str, tuple[str, timedelta | None, bool]] = {}
        for name, (priority, for_) in DEFAULT_ALERTS.items():
            o = self.defaults.get(name)  # type: ignore[call-overload]
            if o is None:
                merged[name] = (priority, for_, True)
            else:
                merged[name] = (o.priority or priority, o.for_ or for_, o.enabled)
        return merged


# --- The service ------------------------------------------------------------------------------


class ServiceSpec(_Strict):
    spec_version: Literal[1] = Field(description="Version of the service.yml format")
    name: ServiceName
    description: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    components: dict[ComponentName, Component] = Field(min_length=1)
    kafka: KafkaSection = Field(default_factory=KafkaSection)
    frontier: FrontierSection = Field(default_factory=FrontierSection)
    clickhouse: ClickHouseSection | None = None
    destinations: dict[Identifier, Destination] = Field(default_factory=dict)
    secrets: list[SecretPath] = Field(default_factory=list, description="Nomad variable paths")
    expects: list[Expect] = Field(default_factory=list)
    alerts: Alerts = Field(default_factory=Alerts)

    def topic_ref(self, topic_name: str) -> naming.TopicRef:
        """Reference to one of this service's own topics."""
        return naming.TopicRef(self.name, topic_name, self.kafka.produces[topic_name].version)

    def own_topic_refs(self) -> set[naming.TopicRef]:
        return {self.topic_ref(t) for t in self.kafka.produces}

    def consumed_topic_refs(self) -> set[naming.TopicRef]:
        """Topics read by this service's processors and ClickHouse sinks (any service)."""
        refs: set[naming.TopicRef] = set()
        for c in self.components.values():
            if isinstance(c, Processor):
                refs.update(naming.TopicRef.parse(r) for r in c.consumes)
        if self.clickhouse:
            for sink in self.clickhouse.sinks.values():
                refs.add(self._resolve_sink_topic(sink.topic))
        return refs

    def _resolve_sink_topic(self, topic: str) -> naming.TopicRef:
        if "." in topic:
            return naming.TopicRef.parse(topic)
        return self.topic_ref(topic)

    @model_validator(mode="after")
    def _consistency(self) -> ServiceSpec:
        errors = list(self._consistency_errors())
        if errors:
            raise ValueError("\n".join(errors))
        return self

    def _consistency_errors(self):
        own_topics = self.kafka.produces
        own_tasks = self.frontier.task_types
        prefix = f"{self.name}."

        for tt in own_tasks:
            if not tt.startswith(prefix):
                yield (
                    f"frontier.task_types.{tt}: task types must start with {prefix!r} "
                    "(a service owns its task types)"
                )

        produced: set[str] = set()
        consumed_tasks: set[str] = set()
        for cname, c in self.components.items():
            where = f"components.{cname}"
            for t in c.produces:
                if t not in own_topics:
                    yield f"{where}.produces: unknown topic {t!r}; declare it under kafka.produces"
                produced.add(t)
            for tt in c.creates_tasks:
                if tt not in own_tasks:
                    yield (
                        f"{where}.creates_tasks: {tt!r} is not one of this service's task types; "
                        "work for another service is created by that service's own processor"
                    )
            if isinstance(c, TaskWorker):
                for tt in c.consumes_tasks:
                    if tt not in own_tasks:
                        yield (
                            f"{where}.consumes_tasks: {tt!r} is not one of this service's task "
                            "types; declare it under frontier.task_types"
                        )
                    consumed_tasks.add(tt)
                if c.scaling and c.scaling.metric != "eligible_tasks":
                    yield f"{where}.scaling.metric: task-workers scale on 'eligible_tasks'"
            if isinstance(c, Processor):
                for ref_str in c.consumes:
                    ref = naming.TopicRef.parse(ref_str)
                    if ref.service == self.name:
                        yield from self._check_own_ref(ref, f"{where}.consumes")
                if c.scaling and c.scaling.metric != "consumer_lag":
                    yield f"{where}.scaling.metric: processors scale on 'consumer_lag'"

        for t in own_topics:
            if t not in produced:
                yield f"kafka.produces.{t}: no component produces this topic"
        for tt in own_tasks:
            if tt not in consumed_tasks:
                yield f"frontier.task_types.{tt}: no task-worker consumes this task type"

        if self.clickhouse:
            for sname, sink in self.clickhouse.sinks.items():
                where = f"clickhouse.sinks.{sname}.topic"
                if "." in sink.topic:
                    try:
                        ref = naming.TopicRef.parse(sink.topic)
                    except naming.NamingError as e:
                        yield f"{where}: {e}"
                        continue
                    if ref.service == self.name:
                        yield from self._check_own_ref(ref, where)
                elif sink.topic not in own_topics:
                    yield f"{where}: unknown topic {sink.topic!r}; declare it under kafka.produces"

        for exp in self.expects:
            where = f"expects.{exp.name}.sql"
            if self.clickhouse is None:
                yield f"{where}: expects query ClickHouse, but there's no clickhouse section"
            elif re.search(rf"\b{re.escape(self.clickhouse.database)}\.", exp.sql):
                yield (
                    f"{where}: hardcodes database {self.clickhouse.database!r}; write "
                    f"{DATABASE_PLACEHOLDER} so each environment queries its own database"
                )

        names = [r.name for r in self.alerts.rules] + [e.name for e in self.expects]
        for dup in sorted({n for n in names if names.count(n) > 1}):
            yield f"alerts/expects: name {dup!r} is used more than once"
        for rule in self.alerts.rules:
            if rule.component is not None and rule.component not in self.components:
                yield f"alerts.rules.{rule.name}.component: unknown component {rule.component!r}"

    def _check_own_ref(self, ref: naming.TopicRef, where: str):
        topic = self.kafka.produces.get(ref.name)
        if topic is None:
            yield f"{where}: {ref} refers to this service, but it has no topic {ref.name!r}"
        elif topic.version != ref.version:
            yield (
                f"{where}: {ref} refers to version {ref.version}, "
                f"but {ref.name!r} is v{topic.version}"
            )
