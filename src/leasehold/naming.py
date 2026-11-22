"""Names of everything a service owns, derived from environment + service.

This module is the only place where resource names are built. Code that needs a
topic, consumer group, database or Nomad job name calls a function here instead
of formatting a string, so environments stay isolated by construction.

Conventions:

- The production environment is called ``prod`` and gets no prefix.
- Every other environment (e.g. ``dev``) prefixes its resources:
  ``dev.`` for Kafka names, ``dev_`` for database and schema names.
- Nomad jobs are not prefixed; environments are separate Nomad namespaces.
- Buckets get ``dev-`` (bucket names allow neither ``.`` nor ``_`` safely).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

PROD = "prod"

# kebab-case: lowercase words joined by single hyphens. No dots or underscores,
# because these names end up inside Kafka topic names, where '.' is the
# separator and mixing '.' and '_' makes Kafka's metric names collide.
KEBAB = r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*"
SNAKE = r"[a-z][a-z0-9_]*"

ENV_RE = re.compile(r"^[a-z][a-z0-9]{0,11}$")
SERVICE_RE = re.compile(rf"^{KEBAB}$")
TOPIC_NAME_RE = re.compile(rf"^{KEBAB}$")
TOPIC_REF_RE = re.compile(rf"^(?P<service>{KEBAB})\.(?P<name>{KEBAB})\.v(?P<version>[1-9][0-9]*)$")
TASK_TYPE_RE = re.compile(rf"^(?P<service>{KEBAB})\.(?P<name>{SNAKE})$")
SNAKE_RE = re.compile(rf"^{SNAKE}$")


class NamingError(ValueError):
    """A name doesn't follow the conventions."""


def check_env(env: str) -> str:
    if not ENV_RE.match(env):
        raise NamingError(
            f"invalid environment {env!r}: lowercase letters and digits, "
            "starting with a letter, at most 12 characters"
        )
    return env


def _check(value: str, pattern: re.Pattern[str], what: str) -> str:
    if not pattern.match(value):
        raise NamingError(f"invalid {what} {value!r}")
    return value


def _dot_prefix(env: str) -> str:
    return "" if check_env(env) == PROD else f"{env}."


def _underscore_prefix(env: str) -> str:
    return "" if check_env(env) == PROD else f"{env}_"


@dataclass(frozen=True, order=True)
class TopicRef:
    """An environment-independent reference to a topic: ``<service>.<name>.v<version>``.

    This is how ``service.yml`` refers to topics, including other services' topics.
    The environment prefix is added only when the actual topic name is built.
    """

    service: str
    name: str
    version: int

    @classmethod
    def parse(cls, ref: str) -> TopicRef:
        m = TOPIC_REF_RE.match(ref)
        if not m:
            raise NamingError(
                f"invalid topic reference {ref!r}: expected '<service>.<topic>.v<N>', "
                "e.g. 'certlog.entries.v1'"
            )
        return cls(m["service"], m["name"], int(m["version"]))

    def __str__(self) -> str:
        return f"{self.service}.{self.name}.v{self.version}"


def topic(env: str, service: str, name: str, version: int) -> str:
    """Kafka topic name, e.g. ``dev.certlog.entries.v1``."""
    _check(service, SERVICE_RE, "service name")
    _check(name, TOPIC_NAME_RE, "topic name")
    if version < 1:
        raise NamingError(f"invalid topic version {version!r}: must be >= 1")
    return f"{_dot_prefix(env)}{service}.{name}.v{version}"


def topic_for(env: str, ref: TopicRef) -> str:
    return topic(env, ref.service, ref.name, ref.version)


def kafka_principal(env: str, service: str, component: str) -> str:
    """SCRAM user of one component, e.g. ``dev.certlog.worker``. Kafka ACLs are per component."""
    _check(service, SERVICE_RE, "service name")
    _check(component, SERVICE_RE, "component name")
    return f"{_dot_prefix(env)}{service}.{component}"


def consumer_group(env: str, service: str, component: str) -> str:
    """Kafka consumer group of a processor. Its committed offsets are its progress."""
    return kafka_principal(env, service, component)


def clickhouse_sink_principal(env: str, service: str) -> str:
    """SCRAM user ClickHouse uses for a service's Kafka-engine sinks, e.g. ``certlog.clickhouse``.

    The spec rejects a component named ``clickhouse``, so this can't collide with
    a component's principal (``kafka_principal``).
    """
    _check(service, SERVICE_RE, "service name")
    return f"{_dot_prefix(env)}{service}.clickhouse"


def clickhouse_sink_group(env: str, service: str, sink: str) -> str:
    """Consumer group of one ClickHouse sink, e.g. ``certlog.clickhouse.ct_domains``.

    Each sink has its own group, so two sinks on the same topic both see every message.
    """
    _check(sink, SNAKE_RE, "sink name")
    return f"{clickhouse_sink_principal(env, service)}.{sink}"


def clickhouse_kafka_variable(service: str) -> str:
    """Nomad Variable holding the sink principal's password (read by operators only)."""
    _check(service, SERVICE_RE, "service name")
    return f"leasehold/{service}/clickhouse-kafka"


RESERVED_COMPONENT_NAMES = frozenset({"clickhouse", "archive"})

# The plan log: a topic per service with a planner, and its two sinks in the service's
# database (one row per queued key, one per run). apply creates them; a service can't
# declare these names itself.
PLAN_LOG_TOPIC = "planned"
PLAN_LOG_VERSION = 1
PLAN_LOG_KEYS_SINK = "planned"
PLAN_LOG_RUNS_SINK = "planner_runs"
RESERVED_TOPIC_NAMES = frozenset({PLAN_LOG_TOPIC})
RESERVED_SINK_NAMES = frozenset({PLAN_LOG_KEYS_SINK, PLAN_LOG_RUNS_SINK})


def planner_clickhouse_user(env: str, service: str, component: str) -> str:
    """Read-only ClickHouse user of one planner component, e.g. ``dev.dns.plan-new-nl``."""
    return kafka_principal(env, service, component)


def planner_clickhouse_variable(service: str, component: str) -> str:
    """Nomad Variable path holding a planner's ClickHouse password (the job's own path)."""
    return f"nomad/jobs/{nomad_job(service, component)}"


def nomad_namespace(env: str) -> str:
    return "default" if check_env(env) == PROD else env


def nomad_job(service: str, component: str) -> str:
    """Nomad job ID, e.g. ``certlog-worker``. Not prefixed: the namespace separates environments."""
    _check(service, SERVICE_RE, "service name")
    _check(component, SERVICE_RE, "component name")
    return f"{service}-{component}"


def image_name(service: str, component: str) -> str:
    """Image repository name of one component (one image per component)."""
    return nomad_job(service, component)


def image_ref(registry: str, service: str, component: str, git_sha: str) -> str:
    """Full, immutable image reference. Tags are always a git SHA, never ``latest``."""
    if not re.match(r"^[0-9a-f]{7,40}$", git_sha):
        raise NamingError(f"invalid git SHA {git_sha!r}: images are tagged with a commit SHA")
    return f"{registry.rstrip('/')}/{image_name(service, component)}:{git_sha}"


def clickhouse_database(env: str, database: str) -> str:
    """ClickHouse database, e.g. ``ct_logs`` in prod and ``dev_ct_logs`` in dev."""
    _check(database, SNAKE_RE, "ClickHouse database name")
    return f"{_underscore_prefix(env)}{database}"


def postgres_schema(env: str, schema: str) -> str:
    """Postgres schema, e.g. ``frontier`` in prod and ``dev_frontier`` in dev."""
    _check(schema, SNAKE_RE, "Postgres schema name")
    return f"{_underscore_prefix(env)}{schema}"


BUCKET_NAME_RE = re.compile(rf"^{KEBAB}$")
BUCKET_REF_RE = re.compile(rf"^(?:(?P<service>{KEBAB})\.)?(?P<name>{KEBAB})$")


def bucket(env: str, service: str, name: str) -> str:
    """Object storage bucket, e.g. ``certlog-raw`` in prod and ``dev-certlog-raw`` in dev.

    Hyphens only: bucket names can't contain ``_``, and dots break virtual-host TLS.
    """
    _check(service, SERVICE_RE, "service name")
    _check(name, BUCKET_NAME_RE, "bucket name")
    prefix = "" if check_env(env) == PROD else f"{env}-"
    full = f"{prefix}{service}-{name}"
    if not 3 <= len(full) <= 63:
        raise NamingError(f"bucket name {full!r} is longer than 63 characters")
    return full


@dataclass(frozen=True, order=True)
class BucketRef:
    """An environment-independent reference to a bucket: ``<service>.<bucket>``."""

    service: str
    name: str

    @classmethod
    def parse(cls, ref: str, default_service: str | None = None) -> BucketRef:
        """``raw`` (with ``default_service``) or ``certlog.raw``."""
        m = BUCKET_REF_RE.match(ref)
        if not m or (m["service"] is None and default_service is None):
            raise NamingError(f"invalid bucket reference {ref!r}: expected '<service>.<bucket>'")
        return cls(m["service"] or default_service, m["name"])  # type: ignore[arg-type]

    def __str__(self) -> str:
        return f"{self.service}.{self.name}"


def bucket_for(env: str, ref: BucketRef) -> str:
    return bucket(env, ref.service, ref.name)


def storage_user(env: str, service: str, component: str) -> str:
    """Object storage user of one component, e.g. ``dev.certlog.worker`` (as its Kafka user)."""
    return kafka_principal(env, service, component)


def storage_policy(env: str, service: str, component: str) -> str:
    """Policy attached to a component's storage user, e.g. ``leasehold.dev.certlog.worker``."""
    return f"{STORAGE_POLICY_PREFIX}{storage_user(env, service, component)}"


STORAGE_POLICY_PREFIX = "leasehold."

# Archive sinks run as platform components named ``archive-<sink>``: their Nomad job,
# Kafka user, consumer group and storage user are those of that component name.
ARCHIVE_COMPONENT_PREFIX = "archive-"


def archive_component(sink: str) -> str:
    """Component name of an archive sink, e.g. ``archive-responses``."""
    _check(sink, SERVICE_RE, "archive sink name")
    return f"{ARCHIVE_COMPONENT_PREFIX}{sink}"


# Claim check: payloads over the inline limit, stored under this prefix in a bucket.
CLAIMS_PREFIX = "claims/"


def claims_prefix(ref: TopicRef) -> str:
    """Key prefix of one topic's claimed payloads, e.g. ``claims/certlog.entries.v1/``."""
    return f"{CLAIMS_PREFIX}{ref}/"
