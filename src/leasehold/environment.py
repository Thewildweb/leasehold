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
    grafana:
      url: http://localhost:13000
      token_env: LEASEHOLD_GRAFANA_TOKEN   # or user + password_env
    jobs:
      registry: ghcr.io/example
      api_url: http://control-api:8400
      node_pool: dev                    # optional: run only on this Nomad node pool
      host_network: tailscale           # optional: publish ports only on this host network

A section may be left out when the steps that need it are skipped.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

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


class Environment(_Model):
    env: str
    kafka: Kafka | None = None
    postgres: Postgres | None = None
    nomad: Nomad | None = None
    clickhouse: ClickHouse | None = None
    grafana: Grafana | None = None
    jobs: Jobs | None = None


# Per step: the sections it needs.
NEEDS = {
    "kafka": ("kafka",),
    "frontier": ("postgres",),
    "credentials": ("kafka", "nomad"),
    "clickhouse": ("clickhouse", "kafka", "nomad"),
    "grafana": ("grafana",),
    "nomad": ("nomad", "jobs", "kafka"),
}


def load(path: str | Path) -> Environment:
    try:
        data = yaml.safe_load(Path(path).read_text())
        e = Environment.model_validate(data)
        naming.check_env(e.env)
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

    Keys: ``kafka``, ``dsn``, ``nomad``, ``clickhouse``, ``grafana_token``,
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
    if "grafana" in needed and e.grafana:
        if e.grafana.user:
            need("grafana_password", e.grafana.password_env)
        else:
            need("grafana_token", e.grafana.token_env)
    return values, missing
