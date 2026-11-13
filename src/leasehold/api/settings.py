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
    clickhouse_url: str | None = None
    clickhouse_user: str = "leasehold"
    clickhouse_password: str = ""
    alerts_database: str | None = None

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
            clickhouse_url=e.get("LEASEHOLD_CLICKHOUSE_URL") or None,
            clickhouse_user=e.get("LEASEHOLD_CLICKHOUSE_USER") or "leasehold",
            clickhouse_password=e.get("LEASEHOLD_CLICKHOUSE_PASSWORD", ""),
            alerts_database=e.get("LEASEHOLD_ALERTS_DATABASE") or None,
        )
