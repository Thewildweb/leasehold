"""Environment files: where an environment's systems are, for ``leasehold apply``.

An environment file names addresses and the environment variables that hold the
secrets; it never holds a secret itself::

    env: dev
    kafka:
      bootstrap: localhost:19092        # where leasehold reaches Kafka as admin
      client_bootstrap: kafka:9092      # what ClickHouse and jobs use (default: bootstrap)
      username: admin
      password_env: LEASEHOLD_KAFKA_PASSWORD
    postgres:
      dsn_env: LEASEHOLD_DSN
    nomad:
      url: http://localhost:14646
      token_env: NOMAD_TOKEN
    clickhouse:
      url: http://localhost:18123
      user: admin
      password_env: LEASEHOLD_CLICKHOUSE_PASSWORD
    storage:
      endpoint: http://localhost:19000  # where leasehold reaches MinIO as admin
      client_endpoint: http://minio:9000  # what jobs use (default: endpoint)
      clickhouse_endpoint: http://minio:9000  # what ClickHouse uses (default: client_endpoint)
      access_key: admin
      secret_key_env: LEASEHOLD_S3_SECRET_KEY
      region: ""                        # optional
    grafana:
      url: http://localhost:13000
      token_env: LEASEHOLD_GRAFANA_TOKEN   # or user + password_env
    jobs:
      registry: ghcr.io/example
      api_url: http://control-api:8400
      node_pool: dev                    # optional: run only on this Nomad node pool
      host_network: tailscale           # optional: publish ports only on this host network
      node_classes: {services: processing}  # optional: spec node class -> class used here
      archive_image: ghcr.io/example/leasehold-archive:<sha>  # needed for archive sinks
      snapshot_image: ghcr.io/example/leasehold-base:<sha>  # needed for snapshot loaders
    snapshots:                          # optional: when snapshot loaders may load
      window: {start: "05:00", end: "09:00", timezone: Europe/Amsterdam, skip_first_sunday: true}
      busy_query: SELECT count() FROM ops.heavy_jobs WHERE ...  # > 0: don't load now
      busy_tables: [ops.heavy_jobs]     # what the busy query reads (granted to loaders)
    services: [certlog, dns]            # optional: apply only these services here
    reads_from: {dns: prod}             # optional: readers here read dns's tables in prod

A section may be left out when the steps that need it are skipped.

``services`` makes promotion explicit: a new service.yml reaches this environment
only once it is listed. Unset, every service is applied. A service taken off the
list is treated like a deleted one: its resources show up as stale and are removed
only with ``--prune``.

``snapshots`` says when snapshot loaders load (default: any time, no busy check). A
loader runs every 20 minutes inside the window and loads only when the busy query
returns 0, so heavy loads stay out of the way of backups and other heavy jobs.

``reads_from`` points this environment's readers (planners, scheduled components
with ``reads``) at another environment's tables of a service, e.g. a dev planner
selecting from prod's data instead of the small dev copy. Reads only: the read-only
ClickHouse user of the reader gets ``SELECT`` on the other environment's table.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

from leasehold import naming


class EnvironmentFileError(ValueError):
    pass


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Kafka(_Model):
    bootstrap: str
    client_bootstrap: str | None = None
    username: str = "admin"
    password_env: str = "LEASEHOLD_KAFKA_PASSWORD"

    @property
    def clients(self) -> str:
        return self.client_bootstrap or self.bootstrap


class Postgres(_Model):
    dsn_env: str = "LEASEHOLD_DSN"


class Nomad(_Model):
    url: str
    token_env: str = "NOMAD_TOKEN"


class ClickHouse(_Model):
    url: str
    user: str = "default"
    password_env: str = "LEASEHOLD_CLICKHOUSE_PASSWORD"


class Storage(_Model):
    endpoint: str
    client_endpoint: str | None = None
    clickhouse_endpoint: str | None = None
    access_key: str = "admin"
    secret_key_env: str = "LEASEHOLD_S3_SECRET_KEY"
    region: str = ""

    @property
    def clients(self) -> str:
        return self.client_endpoint or self.endpoint

    @property
    def for_clickhouse(self) -> str:
        return self.clickhouse_endpoint or self.clients


class Window(_Model):
    start: str = "00:00"
    end: str = "24:00"
    timezone: str = "UTC"
    skip_first_sunday: bool = False

    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, v: str) -> str:
        if not re.match(r"^([01][0-9]|2[0-3]):[0-5][0-9]$|^24:00$", v):
            raise ValueError(f"{v!r}: expected HH:MM")
        return v

    @model_validator(mode="after")
    def _ordered(self) -> Window:
        if self.end <= self.start:
            raise ValueError("the window must end after it starts (no window across midnight)")
        try:
            ZoneInfo(self.timezone)
        except Exception:  # noqa: BLE001 - any lookup failure is a bad name
            raise ValueError(f"unknown timezone {self.timezone!r}") from None
        return self


class Snapshots(_Model):
    window: Window = Window()
    busy_query: str | None = None
    busy_tables: list[str] = []

    @field_validator("busy_tables")
    @classmethod
    def _tables(cls, v: list[str]) -> list[str]:
        for t in v:
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*|\*)$", t):
                raise ValueError(f"{t!r}: expected <database>.<table>")
        return v


class Grafana(_Model):
    url: str
    token_env: str = "LEASEHOLD_GRAFANA_TOKEN"
    user: str | None = None  # basic auth instead of a token
    password_env: str = "LEASEHOLD_GRAFANA_PASSWORD"
    prometheus_uid: str = "prometheus"
    clickhouse_uid: str = "clickhouse"


class Jobs(_Model):
    registry: str
    api_url: str
    node_pool: str | None = None  # Nomad node pool; unset: the default pool
    host_network: str | None = None  # Nomad host network for the http port, e.g. tailscale
    node_classes: dict[str, str] = {}  # spec node class -> node class in this environment
    clickhouse_url: str | None = None  # ClickHouse HTTP interface for planners
    agent_volume: str | None = None  # Nomad host volume with the egress agents' sockets
    archive_image: str | None = None  # image the archive sinks run (leasehold's archive target)
    snapshot_image: str | None = None  # image the snapshot loaders run (leasehold's base image)


class Environment(_Model):
    env: str
    kafka: Kafka | None = None
    postgres: Postgres | None = None
    nomad: Nomad | None = None
    clickhouse: ClickHouse | None = None
    storage: Storage | None = None
    grafana: Grafana | None = None
    jobs: Jobs | None = None
    snapshots: Snapshots = Snapshots()
    services: list[str] | None = None
    reads_from: dict[str, str] = {}  # service -> environment whose tables readers here use


# Per step: the sections it needs.
NEEDS = {
    "kafka": ("kafka",),
    "frontier": ("postgres",),
    "credentials": ("kafka", "nomad"),
    "storage": ("storage", "nomad"),
    "clickhouse": ("clickhouse", "kafka", "nomad"),
    "grafana": ("grafana",),
    "nomad": ("nomad", "jobs", "kafka"),
}


def load(path: str | Path) -> Environment:
    try:
        data = yaml.safe_load(Path(path).read_text())
        e = Environment.model_validate(data)
        naming.check_env(e.env)
        for other in e.reads_from.values():
            naming.check_env(other)
    except (OSError, yaml.YAMLError, ValidationError, naming.NamingError) as err:
        raise EnvironmentFileError(f"{path}: {err}") from None
    return e


def missing_sections(e: Environment, steps: list[str]) -> list[str]:
    out = []
    for step in steps:
        for section in NEEDS[step]:
            if getattr(e, section) is None:
                out.append(f"step {step} needs the '{section}' section")
    return out


def secrets(e: Environment, steps: list[str]) -> tuple[dict[str, str | None], list[str]]:
    """The secrets the steps need, read from the environment, and what's missing.

    Keys: ``kafka``, ``dsn``, ``nomad``, ``clickhouse``, ``storage``, ``grafana_token``,
    ``grafana_password``. The Nomad token may be absent (Nomad without ACLs);
    everything else is required by the steps that use it.
    """
    needed = {s for step in steps for s in NEEDS[step]}
    values: dict[str, str | None] = {}
    missing: list[str] = []

    def need(key: str, var: str) -> None:
        values[key] = os.environ.get(var)
        if values[key] is None:
            missing.append(f"${var}")

    if "kafka" in needed and e.kafka and {"kafka", "credentials"} & set(steps):
        need("kafka", e.kafka.password_env)
    if "postgres" in needed and e.postgres:
        need("dsn", e.postgres.dsn_env)
    if "nomad" in needed and e.nomad:
        values["nomad"] = os.environ.get(e.nomad.token_env)
    if "clickhouse" in needed and e.clickhouse:
        need("clickhouse", e.clickhouse.password_env)
    if "storage" in needed and e.storage:
        need("storage", e.storage.secret_key_env)
    if "grafana" in needed and e.grafana:
        if e.grafana.user:
            need("grafana_password", e.grafana.password_env)
        else:
            need("grafana_token", e.grafana.token_env)
    return values, missing
