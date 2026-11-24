"""Control API configuration, from environment variables.

One control API deployment serves one environment: the prod API manages the
``frontier`` schema and accepts only jobs from the prod Nomad namespace; the dev
API the same for ``dev``.

    LEASEHOLD_ENV             prod | dev | ...                          (required)
    LEASEHOLD_DSN             Postgres connection string                (required)
    LEASEHOLD_SERVICES        directories with service.yml files, separated by ':'
                                                                        (required)
    LEASEHOLD_NOMAD_JWKS_URL  Nomad's JWKS endpoint, e.g.
                              http://nomad.service:4646/.well-known/jwks.json
                              (unset: workload identities are not accepted)
    LEASEHOLD_JWT_AUDIENCE    audience of the workload identity (default: leasehold)
    LEASEHOLD_JWT_ISSUER      expected issuer, if Nomad is configured with one
    LEASEHOLD_TOKENS_FILE     JSON file with static tokens (admins, CI, dev)
    LEASEHOLD_SWEEP_INTERVAL  seconds between sweeps of expired leases (default: 15)
    LEASEHOLD_STATS_INTERVAL  seconds between frontier metric updates (default: 30)
    LEASEHOLD_EGRESS_SCHEMA   the egress schema shared by all environments (default: egress)
    LEASEHOLD_EGRESS_OWNER    1: this API owns the egress schema (prod): it runs the budget
                              planner and sweeps expired rows. Other environments' APIs
                              read it, publish their demand and write target leases.
    LEASEHOLD_EGRESS_PLAN_INTERVAL  seconds between demand updates and planning checks
                              (default: 15; the planner plans each window once)

Proxy credentials (``leasehold.egress.secrets``); without a key no route through a proxy
is given and proxy credentials can't be stored:

    LEASEHOLD_SECRETS_KEY     32 bytes, base64: the key credentials are encrypted with
    LEASEHOLD_SECRETS_KEYS    instead, several as id:base64,id:base64 (the last encrypts)
    LEASEHOLD_PROXY_INTERVAL  seconds between checks for due proxy jobs: usage polls,
                              exit address checks, probes (default: 60; schema owner only)

The alert inbox (``leasehold.api.alerts``), off when LEASEHOLD_CLICKHOUSE_URL is unset:

    LEASEHOLD_CLICKHOUSE_URL       ClickHouse HTTP interface, e.g. http://clickhouse:8123
    LEASEHOLD_CLICKHOUSE_USER      (default: leasehold)
    LEASEHOLD_CLICKHOUSE_PASSWORD
    LEASEHOLD_ALERTS_DATABASE      (default: leasehold in prod, <env>_leasehold elsewhere)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from leasehold import naming


class SettingsError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    env: str
    dsn: str
    services: tuple[Path, ...]
    nomad_jwks_url: str | None = None
    jwt_audience: str = "leasehold"
    jwt_issuer: str | None = None
    tokens_file: Path | None = None
    sweep_interval: float = 15.0
    stats_interval: float = 30.0
    purge_interval: float = 300.0
    clickhouse_url: str | None = None
    clickhouse_user: str = "leasehold"
    clickhouse_password: str = ""
    alerts_database: str | None = None
    egress_schema: str = "egress"
    egress_owner: bool = False
    plan_interval: float = 15.0
    proxy_interval: float = 60.0

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> Settings:
        e = os.environ if environ is None else environ
        missing = [
            k for k in ("LEASEHOLD_ENV", "LEASEHOLD_DSN", "LEASEHOLD_SERVICES") if not e.get(k)
        ]
        if missing:
            raise SettingsError(f"missing environment variables: {', '.join(missing)}")
        env = naming.check_env(e["LEASEHOLD_ENV"])
        return cls(
            env=env,
            dsn=e["LEASEHOLD_DSN"],
            services=tuple(Path(p) for p in e["LEASEHOLD_SERVICES"].split(":") if p),
            nomad_jwks_url=e.get("LEASEHOLD_NOMAD_JWKS_URL") or None,
            jwt_audience=e.get("LEASEHOLD_JWT_AUDIENCE", "leasehold"),
            jwt_issuer=e.get("LEASEHOLD_JWT_ISSUER") or None,
            tokens_file=Path(e["LEASEHOLD_TOKENS_FILE"])
            if e.get("LEASEHOLD_TOKENS_FILE")
            else None,
            sweep_interval=float(e.get("LEASEHOLD_SWEEP_INTERVAL", "15")),
            stats_interval=float(e.get("LEASEHOLD_STATS_INTERVAL", "30")),
            purge_interval=float(e.get("LEASEHOLD_PURGE_INTERVAL", "300")),
            clickhouse_url=e.get("LEASEHOLD_CLICKHOUSE_URL") or None,
            clickhouse_user=e.get("LEASEHOLD_CLICKHOUSE_USER") or "leasehold",
            clickhouse_password=e.get("LEASEHOLD_CLICKHOUSE_PASSWORD", ""),
            alerts_database=e.get("LEASEHOLD_ALERTS_DATABASE") or None,
            egress_schema=e.get("LEASEHOLD_EGRESS_SCHEMA") or "egress",
            egress_owner=e.get("LEASEHOLD_EGRESS_OWNER", "") in ("1", "true", "yes"),
            plan_interval=float(e.get("LEASEHOLD_EGRESS_PLAN_INTERVAL", "15")),
            proxy_interval=float(e.get("LEASEHOLD_PROXY_INTERVAL", "60")),
        )
