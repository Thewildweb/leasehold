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
- Buckets work like topics: only this service's components write its buckets
  (``writes_buckets``); any component may read a bucket it names in ``reads_buckets``
  (``<service>.<bucket>`` for another service's).

Checks that need more than one service (does a consumed topic exist?) live in
``leasehold.spec.registry``.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from leasehold import naming
from leasehold.types import (
    PRIORITY_ORDER,
    BucketName,
    BucketRefStr,
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
    max_per_node: int | None = Field(
        default=None,
        ge=1,
        description="At most this many instances on one machine. Use 1 for components that "
        "keep their own rate limit per egress IP: instances on one machine share its IP",
    )
    reads_buckets: list[BucketRefStr] = Field(
        default_factory=list,
        description="Buckets this component reads: this service's by name ('raw'), another "
        "service's as '<service>.<bucket>'",
    )
    writes_buckets: list[BucketName] = Field(
        default_factory=list,
        description="This service's buckets this component writes (and reads)",
    )

    def bucket_refs(self, service: str) -> tuple[set[naming.BucketRef], set[naming.BucketRef]]:
        """(read, write) bucket references. Write implies read; it isn't listed twice."""
        write = {naming.BucketRef(service, b) for b in self.writes_buckets}
        read = {naming.BucketRef.parse(r, service) for r in self.reads_buckets} - write
        return read, write

    def topics_produced(self) -> set[str]:
        """``produces``, plus the plan log for a planner."""
        produced = set(self.produces)
        if getattr(self, "planner", None) is not None:
            produced.add(naming.PLAN_LOG_TOPIC)
        return produced

    def task_types_created(self) -> set[str]:
        """``creates_tasks``, plus a planner's task type."""
        created = set(self.creates_tasks)
        planner = getattr(self, "planner", None)
        if planner is not None:
            created.add(planner.task_type)
        return created

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


TableRefStr = Annotated[
    str, StringConstraints(pattern=rf"^{naming.KEBAB}\.{naming.SNAKE}$", max_length=100)
]


class Planner(_Strict):
    """Tops up a task type's backlog in the frontier from a selection in ClickHouse.

    ``run_planner`` reads the backlog (eligible + leased tasks of ``task_type``) and,
    when it is below ``backlog_target``, asks the service's ``select`` for at most
    ``min(batch_max, backlog_target - backlog)`` keys, upserts them with ``priority``
    and records them in the service's plan log.
    """

    task_type: TaskTypeName = Field(description="One of this service's task types")
    reads: list[TableRefStr] = Field(
        default_factory=list,
        description="ClickHouse tables the selection reads, as '<service>.<table>': this "
        "service's own sinks and shared tables, or another service's shared tables",
    )
    backlog_target: int = Field(gt=0, description="Waiting + leased tasks to keep in the frontier")
    batch_max: int = Field(default=50_000, gt=0, description="Most keys queued in one run")
    priority: int = Field(ge=0, le=100, description="Priority of the tasks this planner queues")
    exclude_planned_for: Duration | None = Field(
        default=None,
        description="Skip keys this planner queued less than this long ago (from the plan log)",
    )

    def read_services(self) -> set[str]:
        return {r.split(".", 1)[0] for r in self.reads}


class Scheduled(_ComponentBase):
    """A periodic job (Nomad periodic batch), e.g. a planner that creates tasks."""

    kind: Literal["scheduled"]
    schedule: str = Field(description="Cron expression (5 fields) or @hourly/@daily/…")
    timeout: Duration = Field(
        default=timedelta(minutes=10), description="A run longer than this is killed"
    )
    planner: Planner | None = Field(
        default=None, description="Makes this component a planner (`run_planner`)"
    )
    reads: list[TableRefStr] = Field(
        default_factory=list,
        description="ClickHouse tables a job that is not a planner reads ('<service>.<table>'), "
        "with the same read-only user a planner gets (`run.clickhouse` in `run_scheduled`). "
        "A planner declares them in its planner block",
    )

    @model_validator(mode="after")
    def _reads_in_one_place(self) -> Scheduled:
        if self.reads and self.planner is not None:
            raise ValueError("a planner declares its reads in planner.reads")
        return self

    @property
    def clickhouse_reads(self) -> list[str]:
        """Tables this job reads in ClickHouse."""
        return list(self.planner.reads) if self.planner is not None else list(self.reads)

    @property
    def reads_clickhouse(self) -> bool:
        """Gets a read-only ClickHouse user: every planner (plan log), and jobs with reads."""
        return self.planner is not None or bool(self.reads)

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


class ArchiveComponent(_ComponentBase):
    """An archive sink as the platform runs it: not declared under ``components``, but
    derived from ``archives.<name>`` as the component ``archive-<name>``. It consumes one
    topic as its own consumer group and writes Parquet files to one bucket."""

    kind: Literal["archive"] = "archive"
    sink: str
    consumes: list[TopicRefStr]
    count: Count


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
    large_payloads: BucketName | None = Field(
        default=None,
        description="Claim check: a payload whose event would be over the inline limit is "
        "stored in this bucket of the service (under claims/), and the event carries a "
        "payload_ref instead. Every component producing the topic must write the bucket. "
        "Claims expire one day after the topic's retention",
    )


PLAN_LOG = KafkaTopic(
    payload_model="leasehold.runtime.planner:PlanLogEntry",
    version=naming.PLAN_LOG_VERSION,
    partitions=3,
    retention=timedelta(days=7),
    key="planner",
)


def plan_log_sinks() -> dict[str, Sink]:
    from pathlib import Path

    here = Path(__file__).resolve().parent.parent / "clickhouse" / "plan_log"
    return {
        name: Sink(topic=naming.PLAN_LOG_TOPIC, schema_file=str(here / f"{name}.sql"))
        for name in (naming.PLAN_LOG_KEYS_SINK, naming.PLAN_LOG_RUNS_SINK)
    }


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
    retry_backoff_max: Duration = Field(
        default=timedelta(hours=6), description="Upper limit of the doubled backoff"
    )
    lease_duration: Duration = Field(
        default=timedelta(minutes=5),
        description="How long a lease lasts without renewal; an expired lease counts as a "
        "failed attempt",
    )
    default_priority: int = Field(default=50, ge=0, le=100)
    capabilities: list[Identifier] = Field(
        default_factory=list, description="What a worker needs, e.g. http, playwright"
    )
    hash_partitions: int = Field(
        default=1,
        ge=1,
        le=64,
        description="Hash sub-partitions of this task type's table, by key. Fixed when the "
        "table is created; use more than 1 for task types with tens of millions of rows",
    )
    payload_model: str | None = None
    keep_done: Duration | None = Field(
        default=None,
        description="Delete done tasks this long after they were done. For one-shot task "
        "types fed by a planner, whose memory of what was done is in ClickHouse. "
        "None = keep them (the frontier itself remembers what was done)",
    )

    @model_validator(mode="after")
    def _limits(self) -> TaskType:
        if self.retry_backoff_max < self.retry_backoff:
            raise ValueError("retry_backoff_max must be >= retry_backoff")
        if self.lease_duration < timedelta(seconds=10):
            raise ValueError("lease_duration must be at least 10s")
        if self.keep_done is not None and self.recrawl_interval is not None:
            raise ValueError("keep_done is for one-shot task types; recurring tasks are never done")
        return self


class FrontierSection(_Strict):
    task_types: dict[TaskTypeName, TaskType] = Field(default_factory=dict)


# --- ClickHouse -------------------------------------------------------------------------------


class Sink(_Strict):
    topic: str = Field(
        description="A topic of this service by name ('entries'), or any topic by reference"
    )
    schema_file: str = Field(min_length=1, description="SQL file, relative to service.yml")
    consumers: int = Field(
        default=1,
        ge=1,
        le=16,
        description="Kafka consumers in ClickHouse, each on its own thread; raise it when "
        "one can't keep up (at most the topic's partitions)",
    )


class SharedTable(_Strict):
    """One of this service's sinks that other services' planners may read. Its columns
    are the contract. (Views come later, when a service must hide columns: they need a
    definer user per service, so readers don't need grants on the tables underneath.)"""

    description: str = Field(min_length=1)


class ClickHouseSection(_Strict):
    database: SnakeName
    sinks: dict[SnakeName, Sink] = Field(default_factory=dict)
    shared: dict[SnakeName, SharedTable] = Field(
        default_factory=dict, description="Sinks other services' planners may read"
    )

    def tables(self) -> set[str]:
        """Names of this service's own tables (its sinks)."""
        return set(self.sinks)


# --- Buckets and archive sinks ----------------------------------------------------------------


class Bucket(_Strict):
    retention: Duration | None = Field(
        default=None,
        description="Objects expire this long after they were written (lifecycle rule, whole "
        "days); omit to keep them",
    )
    versioning: bool = False
    description: str | None = None

    @field_validator("retention")
    @classmethod
    def _whole_days(cls, value: timedelta | None) -> timedelta | None:
        if value is not None and (value.total_seconds() % 86400 or value < timedelta(days=1)):
            raise ValueError(
                "retention must be a whole number of days (lifecycle rules count days)"
            )
        return value


ColumnType = Literal["string", "int32", "int64", "float64", "bool", "timestamp", "json", "base64"]
FieldName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$", max_length=60)]


class Column(_Strict):
    """A payload field lifted into a typed Parquet column.

    ``string``/``int32``/``int64``/``float64``/``bool``: the JSON value as that type.
    ``timestamp``: an ISO 8601 string (or epoch seconds) as a UTC timestamp in ms.
    ``json``: the value as JSON text (lists, nested objects).
    ``base64``: a base64 string decoded into a binary column. With ``text_fallback``,
    an event whose field is empty or missing gets the text field encoded as UTF-8
    instead, and the column ``<name>_from_text`` says which rows those are.
    """

    type: ColumnType
    field: FieldName | None = Field(default=None, description="Payload field; default: the name")
    text_fallback: FieldName | None = None

    @model_validator(mode="after")
    def _fallback_for_base64(self) -> Column:
        if self.text_fallback is not None and self.type != "base64":
            raise ValueError("text_fallback is for base64 columns")
        return self


class ArchiveSink(_Strict):
    """A topic written to Parquet files in one of this service's buckets.

    Files: ``<topic ref>/dt=YYYY-MM-DD/hh=HH/p<partition>-<first offset>-<last offset>.parquet``
    (zstd). Columns: the envelope, the Kafka coordinates, the ``columns`` declared here,
    and ``payload``: every other payload field as JSON text.
    """

    topic: str = Field(
        description="A topic of this service by name ('responses'), or any topic by reference"
    )
    bucket: BucketName = Field(description="One of this service's buckets")
    columns: dict[FieldName, ColumnType | Column] = Field(
        default_factory=dict, description="Payload fields as typed columns"
    )
    drop: list[FieldName] = Field(
        default_factory=list, description="Payload fields left out of the archive"
    )
    file_size_mb: int = Field(
        default=128, ge=1, le=1024, description="A file is closed at about this size"
    )
    max_age: Duration = Field(
        default=timedelta(hours=1), description="A file is closed after being open this long"
    )
    row_group_mb: int = Field(
        default=16,
        ge=1,
        le=256,
        description="Rows buffered in memory per open file (one file per Kafka partition)",
    )
    node_class: str = Field(min_length=1, description="Nomad node class to run on")
    resources: Resources = Field(default_factory=lambda: Resources(cpu=300, memory=512))
    count: Count = Field(default_factory=lambda: Count(min=1, max=1))
    kill_timeout: Duration = Field(default=timedelta(seconds=60))

    def column_specs(self) -> dict[str, Column]:
        return {
            name: (c if isinstance(c, Column) else Column(type=c)).model_copy(
                update={"field": (c.field if isinstance(c, Column) else None) or name}
            )
            for name, c in self.columns.items()
        }

    def column_names(self) -> list[str]:
        """The declared columns, with ``<name>_from_text`` after each text fallback."""
        out = []
        for name, c in self.column_specs().items():
            out.append(name)
            if c.text_fallback:
                out.append(f"{name}_from_text")
        return out

    @model_validator(mode="after")
    def _checks(self) -> ArchiveSink:
        from leasehold.archive.layout import FIXED_COLUMNS

        names = self.column_names()
        clash = sorted(set(names) & set(FIXED_COLUMNS))
        if clash:
            raise ValueError(f"columns {', '.join(clash)} are the envelope's")
        if len(set(names)) != len(names):
            raise ValueError("a column name is used twice (including <name>_from_text)")
        fields = [c.field for c in self.column_specs().values()]
        if len(set(fields)) != len(fields):
            raise ValueError("a payload field is in two columns")
        if self.max_age < timedelta(minutes=1):
            raise ValueError("max_age must be at least 1m")
        if self.row_group_mb > self.file_size_mb:
            raise ValueError("row_group_mb must be <= file_size_mb")
        return self


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


# --- Dashboards -------------------------------------------------------------------------------

ThresholdColor = Literal["green", "yellow", "orange", "red", "blue", "text"]


class Threshold(_Strict):
    color: ThresholdColor
    from_: float | None = Field(
        default=None, alias="from", description="Applies from this value up; omit on the first"
    )


class CockpitTile(_Strict):
    """A stat tile in this service's row of the cockpit (and on its own dashboard)."""

    title: str = Field(min_length=1, max_length=60)
    datasource: Literal["prometheus", "clickhouse"]
    query: str = Field(
        min_length=1,
        description="One number. ClickHouse: use {database}. Prometheus: use {namespace} "
        "and {env} for the environment's Nomad namespace and name",
    )
    unit: str = Field(default="short", description="Grafana unit, e.g. short, percent, s, bytes")
    decimals: int | None = Field(default=None, ge=0, le=6)
    thresholds: list[Threshold] = Field(
        default_factory=list, description="Colours; the first has no `from`"
    )

    @model_validator(mode="after")
    def _thresholds_ordered(self) -> CockpitTile:
        t = self.thresholds
        if not t:
            return self
        if t[0].from_ is not None:
            raise ValueError("the first threshold is the base colour and has no `from`")
        values = [x.from_ for x in t[1:]]
        if any(v is None for v in values):
            raise ValueError("every threshold after the first needs `from`")
        if values != sorted(values) or len(set(values)) != len(values):
            raise ValueError("threshold `from` values must increase")
        return self


COCKPIT_MAX_TILES = 4


class DashboardsSection(_Strict):
    cockpit: list[CockpitTile] = Field(
        default_factory=list,
        max_length=COCKPIT_MAX_TILES,
        description=f"Up to {COCKPIT_MAX_TILES} tiles in this service's cockpit row",
    )
    files: list[str] = Field(
        default_factory=list,
        description="Grafana dashboard JSON files, relative to service.yml",
    )

    @field_validator("files")
    @classmethod
    def _relative_json(cls, value: list[str]) -> list[str]:
        for f in value:
            if f.startswith("/") or ".." in f.split("/") or not f.endswith(".json"):
                raise ValueError(f"{f!r}: a .json path inside the service directory")
        if len(set(value)) != len(value):
            raise ValueError("a dashboard file is listed twice")
        stems = [f.rsplit("/", 1)[-1].removesuffix(".json") for f in value]
        if len(set(stems)) != len(stems):
            raise ValueError("dashboard file names must be unique (they become the dashboard ID)")
        return value


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
    buckets: dict[BucketName, Bucket] = Field(
        default_factory=dict, description="Object storage buckets this service owns"
    )
    archives: dict[ComponentName, ArchiveSink] = Field(
        default_factory=dict, description="Topics written to Parquet files in a bucket"
    )
    destinations: dict[Identifier, Destination] = Field(default_factory=dict)
    secrets: list[SecretPath] = Field(default_factory=list, description="Nomad variable paths")
    expects: list[Expect] = Field(default_factory=list)
    alerts: Alerts = Field(default_factory=Alerts)
    dashboards: DashboardsSection = Field(default_factory=DashboardsSection)

    def topics(self) -> dict[str, KafkaTopic]:
        """This service's topics: those in ``kafka.produces``, and the plan log when the
        service has a planner."""
        topics = dict(self.kafka.produces)
        if self.planners():
            topics[naming.PLAN_LOG_TOPIC] = PLAN_LOG
        return topics

    def topic_ref(self, topic_name: str) -> naming.TopicRef:
        """Reference to one of this service's own topics."""
        return naming.TopicRef(self.name, topic_name, self.topics()[topic_name].version)

    def own_topic_refs(self) -> set[naming.TopicRef]:
        return {self.topic_ref(t) for t in self.topics()}

    def consumed_topic_refs(self) -> set[naming.TopicRef]:
        """Topics read by this service's processors and ClickHouse sinks (any service)."""
        refs: set[naming.TopicRef] = set()
        for c in self.components.values():
            if isinstance(c, Processor):
                refs.update(naming.TopicRef.parse(r) for r in c.consumes)
        for sink in self.sinks().values():
            refs.add(self.sink_topic_ref(sink.topic))
        for archive in self.archives.values():
            refs.add(self.sink_topic_ref(archive.topic))
        return refs

    def archive_components(self) -> dict[str, ArchiveComponent]:
        """Each archive sink as the component the platform runs (``archive-<name>``)."""
        out = {}
        for name, a in self.archives.items():
            out[naming.archive_component(name)] = ArchiveComponent(
                sink=name,
                consumes=[str(self.sink_topic_ref(a.topic))],
                node_class=a.node_class,
                resources=a.resources,
                count=a.count,
                kill_timeout=a.kill_timeout,
                writes_buckets=[a.bucket],
            )
        return out

    def all_components(self) -> dict[str, Component | ArchiveComponent]:
        """``components`` plus the archive sinks' components: everything that gets a job."""
        return dict(self.components) | self.archive_components()

    def large_payload_buckets(self) -> dict[naming.TopicRef, naming.BucketRef]:
        """This service's topics with a claim check → the bucket their claims are in."""
        return {
            self.topic_ref(t): naming.BucketRef(self.name, k.large_payloads)
            for t, k in self.kafka.produces.items()
            if k.large_payloads
        }

    def sink_topic_ref(self, topic: str) -> naming.TopicRef:
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
            if cname in naming.RESERVED_COMPONENT_NAMES:
                yield f"{where}: the component name {cname!r} is reserved"
            for t in c.produces:
                if t not in own_topics:
                    yield f"{where}.produces: unknown topic {t!r}; declare it under kafka.produces"
                produced.add(t)
            for tt in c.task_types_created():
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
            if isinstance(c, Scheduled) and c.planner is not None:
                yield from self._check_planner(c.planner, f"{where}.planner")
            elif isinstance(c, Scheduled) and c.reads:
                if self.clickhouse is None:
                    yield f"{where}.reads: needs a clickhouse section"
                else:
                    yield from self._check_reads(c.reads, f"{where}.reads")
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

        for t in sorted(set(own_topics) & naming.RESERVED_TOPIC_NAMES):
            yield f"kafka.produces.{t}: the topic name {t!r} is reserved (the plan log)"
        if self.clickhouse:
            reserved = naming.RESERVED_SINK_NAMES
            for name in sorted(self.clickhouse.tables() & reserved):
                yield f"clickhouse: the table name {name!r} is reserved (the plan log)"
            for name in self.clickhouse.shared:
                if name not in self.clickhouse.sinks:
                    yield (
                        f"clickhouse.shared.{name}: not a sink of this service; "
                        "only sinks can be shared"
                    )

        for exp in self.expects:
            where = f"expects.{exp.name}.sql"
            if self.clickhouse is None:
                yield f"{where}: expects query ClickHouse, but there's no clickhouse section"
            elif re.search(rf"\b{re.escape(self.clickhouse.database)}\.", exp.sql):
                yield (
                    f"{where}: hardcodes database {self.clickhouse.database!r}; write "
                    f"{DATABASE_PLACEHOLDER} so each environment queries its own database"
                )

        for i, tile in enumerate(self.dashboards.cockpit):
            where = f"dashboards.cockpit.{i}.query"
            if tile.datasource != "clickhouse":
                continue
            if self.clickhouse is None:
                yield f"{where}: a ClickHouse tile needs a clickhouse section"
            elif re.search(rf"\b{re.escape(self.clickhouse.database)}\.", tile.query):
                yield (
                    f"{where}: hardcodes database {self.clickhouse.database!r}; write "
                    f"{DATABASE_PLACEHOLDER} so each environment queries its own database"
                )

        yield from self._bucket_errors()

        names = [r.name for r in self.alerts.rules] + [e.name for e in self.expects]
        for dup in sorted({n for n in names if names.count(n) > 1}):
            yield f"alerts/expects: name {dup!r} is used more than once"
        for rule in self.alerts.rules:
            if rule.component is not None and rule.component not in self.components:
                yield f"alerts.rules.{rule.name}.component: unknown component {rule.component!r}"

    def sinks(self) -> dict[str, Sink]:
        """``clickhouse.sinks``, plus the plan log's two sinks when the service has a planner.

        The plan log's schema files ship with leasehold; their paths are absolute.
        """
        if self.clickhouse is None:
            return {}
        sinks = dict(self.clickhouse.sinks)
        if self.planners():
            sinks |= plan_log_sinks()
        return sinks

    def planners(self) -> dict[str, Planner]:
        return {
            n: c.planner
            for n, c in self.components.items()
            if isinstance(c, Scheduled) and c.planner is not None
        }

    def clickhouse_readers(self) -> dict[str, list[str]]:
        """Scheduled components with a read-only ClickHouse user → the tables they read."""
        return {
            n: c.clickhouse_reads
            for n, c in self.components.items()
            if isinstance(c, Scheduled) and c.reads_clickhouse
        }

    def plan_log_ref(self) -> naming.TopicRef | None:
        """The plan log topic, which apply creates when the service has a planner."""
        if not self.planners():
            return None
        return self.topic_ref(naming.PLAN_LOG_TOPIC)

    def _bucket_errors(self):
        for b in self.buckets:
            if len(self.name) + 1 + len(b) > 50:
                yield (
                    f"buckets.{b}: '{self.name}-{b}' is too long; with an environment prefix "
                    "a bucket name has at most 63 characters (keep service + bucket <= 50)"
                )
        for cname, c in self.components.items():
            where = f"components.{cname}"
            if cname.startswith(naming.ARCHIVE_COMPONENT_PREFIX):
                yield f"{where}: names starting with 'archive-' are reserved for archive sinks"
            for b in c.writes_buckets:
                if b not in self.buckets:
                    yield (
                        f"{where}.writes_buckets: unknown bucket {b!r}; declare it under "
                        "buckets (a service only writes its own buckets)"
                    )
            for r in c.reads_buckets:
                ref = naming.BucketRef.parse(r, self.name)
                if ref.service == self.name and ref.name not in self.buckets:
                    yield f"{where}.reads_buckets: this service has no bucket {ref.name!r}"
            if len(set(c.writes_buckets)) != len(c.writes_buckets) or len(
                {naming.BucketRef.parse(r, self.name) for r in c.reads_buckets}
            ) != len(c.reads_buckets):
                yield f"{where}: a bucket is listed twice"

        for t, k in self.kafka.produces.items():
            if not k.large_payloads:
                continue
            where = f"kafka.produces.{t}.large_payloads"
            if k.large_payloads not in self.buckets:
                yield f"{where}: unknown bucket {k.large_payloads!r}; declare it under buckets"
                continue
            for cname, c in self.components.items():
                if t in c.produces and k.large_payloads not in c.writes_buckets:
                    yield (
                        f"components.{cname}.writes_buckets: produces {t!r}, whose large "
                        f"payloads go to {k.large_payloads!r}; add it to writes_buckets"
                    )

        for aname, a in self.archives.items():
            where = f"archives.{aname}"
            if len(naming.archive_component(aname)) > 30:
                yield f"{where}: the name is too long (at most 22 characters)"
            if a.bucket not in self.buckets:
                yield f"{where}.bucket: unknown bucket {a.bucket!r}; declare it under buckets"
            if "." in a.topic:
                try:
                    ref = naming.TopicRef.parse(a.topic)
                except naming.NamingError as e:
                    yield f"{where}.topic: {e}"
                    continue
                if ref.service == self.name:
                    yield from self._check_own_ref(ref, f"{where}.topic")
            elif a.topic not in self.kafka.produces:
                yield f"{where}.topic: unknown topic {a.topic!r}; declare it under kafka.produces"

    def _check_planner(self, planner: Planner, where: str):
        if planner.task_type not in self.frontier.task_types:
            yield (
                f"{where}.task_type: {planner.task_type!r} is not one of this service's task "
                "types; a planner only queues its own service's work"
            )
        if self.clickhouse is None:
            yield f"{where}: a planner needs a clickhouse section (its plan log lives there)"
            return
        yield from self._check_reads(planner.reads, f"{where}.reads")

    def _check_reads(self, reads: list[str], where: str):
        own = self.clickhouse.tables() | naming.RESERVED_SINK_NAMES
        for ref in reads:
            service, table = ref.split(".", 1)
            if service == self.name and table not in own:
                yield f"{where}: {ref!r}: this service has no sink or shared table {table!r}"
        if len(set(reads)) != len(reads):
            yield f"{where}: a table is listed twice"

    def _check_own_ref(self, ref: naming.TopicRef, where: str):
        topic = self.kafka.produces.get(ref.name)
        if topic is None:
            yield f"{where}: {ref} refers to this service, but it has no topic {ref.name!r}"
        elif topic.version != ref.version:
            yield (
                f"{where}: {ref} refers to version {ref.version}, "
                f"but {ref.name!r} is v{topic.version}"
            )
