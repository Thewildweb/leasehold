"""The apply steps: compare one part of the platform with ``service.yml`` and apply.

Each step prints what it finds through ``out`` and returns a ``Result``. The
``<part> apply`` commands run one step; ``leasehold apply`` runs them all, in
``ORDER``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from leasehold.spec import ServiceSpec

Out = Callable[[str], None]

ORDER = ("kafka", "frontier", "credentials", "clickhouse", "grafana", "nomad")


@dataclass
class Result:
    changes: int = 0
    applied: bool = False
    # Parts left as they are on purpose (see the warnings); the step still succeeded.
    skipped: list[str] = field(default_factory=list)
    # credentials: principals that get a new password when applied. In a dry run, the
    # clickhouse step treats these as present.
    new_passwords: set[str] = field(default_factory=set)


def _stale(out: Out, what: str, prune: bool, kept: str) -> None:
    out(f"stale    {what}" + ("" if prune else f"  (kept; --prune {kept})"))


def kafka(
    out: Out,
    specs: list[ServiceSpec],
    env: str,
    *,
    bootstrap: str,
    username: str,
    password: str,
    yes: bool,
    prune: bool,
) -> Result:
    from leasehold import kafka as lh_kafka
    from leasehold.kafka import apply as ka

    p = lh_kafka.plan(specs, env)
    admin = ka.admin_client(bootstrap, username, password)
    d = ka.diff(admin, p, [s.name for s in specs])
    for w in d.warnings:
        out(f"warning  {w}")
    for c in d.changes:
        out(str(c))
    r = Result(changes=len(d.changes))
    if yes and not d.empty:
        ka.apply(admin, d, prune=prune)
        r.applied = True
    return r


def frontier(out: Out, specs: list[ServiceSpec], env: str, *, dsn: str, yes: bool) -> Result:
    """Schema migrations, then the task types."""
    import psycopg

    from leasehold.frontier import schema

    with psycopg.connect(dsn, autocommit=True) as conn:
        version = schema.current_version(conn, env)
        pending = [v for v, _ in schema.MIGRATIONS if v > version]
        r = Result(changes=len(pending))
        if pending:
            out(f"migrate  {schema.schema_name(env)}: version {version} → {pending[-1]}")
            if not yes:
                out("task types are compared once the schema is migrated")
                return r
            schema.migrate(conn, env)
            r.applied = True
        d = schema.diff_task_types(conn, env, specs)
        for w in d.warnings:
            out(f"warning  {w}")
        for c in d.changes:
            out(str(c))
        r.changes += len(d.changes)
        if yes and not d.empty:
            schema.apply_task_types(conn, env, d)
            r.applied = True
    return r


def credentials(
    out: Out,
    specs: list[ServiceSpec],
    env: str,
    *,
    bootstrap: str,
    kafka_username: str,
    kafka_password: str,
    nomad_url: str,
    nomad_token: str | None,
    rotate: bool = False,
    yes: bool,
    prune: bool,
) -> Result:
    from leasehold import credentials as lh_credentials
    from leasehold.kafka.apply import admin_client
    from leasehold.nomad.apply import Nomad

    creds = lh_credentials.plan(specs, env)
    admin = admin_client(bootstrap, kafka_username, kafka_password)
    nomad = Nomad(nomad_url, token=nomad_token)
    try:
        d = lh_credentials.diff(
            admin, nomad, creds, env, sorted(s.name for s in specs), rotate=rotate
        )
        for u in d.stale_users:
            _stale(out, f"kafka user {u}", prune, "deletes")
        for ns, path in d.stale_variables:
            _stale(out, f"{ns}/{path}", prune, "clears")
        for c in d.changes:
            out(str(c))
        stale = bool(d.stale_users or d.stale_variables)
        r = Result(
            changes=len(d.changes) + (int(stale) if prune else 0),
            new_passwords={c.credential.principal for c in d.changes},
        )
        if d.empty:
            out(f"{len(creds)} credentials in place")
        if yes and (not d.empty or (prune and stale)):
            lh_credentials.apply(admin, nomad, d, prune=prune)
            r.applied = True
    finally:
        nomad.close()
    return r


def clickhouse(
    out: Out,
    services: list[tuple[ServiceSpec, Path]],
    env: str,
    *,
    url: str,
    user: str,
    password: str,
    kafka_bootstrap: str,
    nomad_url: str,
    nomad_token: str | None,
    accept_tables: set[str] = frozenset(),  # type: ignore[assignment]
    assume_passwords: set[str] = frozenset(),  # type: ignore[assignment]
    yes: bool,
    prune: bool,
) -> Result:
    """``services``: each spec with the directory of its ``service.yml``.

    ``assume_passwords``: sink principals the credentials step will give a password;
    only used in a dry run, so the plan shows what the real run will do.
    """
    from leasehold import clickhouse as lh_clickhouse
    from leasehold.clickhouse.apply import ClickHouse, apply, diff
    from leasehold.nomad.apply import Nomad

    p = lh_clickhouse.plan(services, env)
    ch = ClickHouse(url, user, password)
    nomad = Nomad(nomad_url, token=nomad_token)
    try:
        versions, passwords = {}, {}
        for s in p.sinks():
            if s.principal in versions:
                continue
            var = nomad.var_get(s.credential_path, s.namespace)
            pw = ((var or {}).get("Items") or {}).get("password")
            if pw:
                versions[s.principal] = str(var["ModifyIndex"])
                passwords[s.principal] = pw
        if not yes:
            for principal in assume_passwords:
                versions[principal] = "(new password)"
        d = diff(ch, p, kafka_bootstrap, versions, accept_tables)
        for w in d.warnings:
            out(f"warning  {w}")
        for name, engine in d.stale:
            if engine in ("MaterializedView", "Kafka"):
                _stale(out, f"{name} [{engine}]", prune, "drops")
            else:
                out(f"stale    {name} [{engine}]  (data table: kept, drop by hand)")
        for db in d.new_databases:
            out(f"create   database {db.name}")
        for db in d.errors_tables:
            out(f"create   table {db.name}.sink_errors")
        for c in d.changes:
            out(f"sink     {c}")
        prunable = [n for n, e in d.stale if e in ("MaterializedView", "Kafka")]
        r = Result(
            changes=len(d.new_databases)
            + len(d.errors_tables)
            + len(d.changes)
            + (len(prunable) if prune else 0),  # fmt: skip
            skipped=list(d.skipped),
        )
        if yes and r.changes:
            apply(ch, d, kafka_bootstrap, passwords, prune=prune)
            r.applied = True
    finally:
        ch.close()
        nomad.close()
    return r


def grafana(
    out: Out,
    services: list,
    env: str,
    *,
    url: str,
    token: str | None,
    user: str | None,
    password: str | None,
    prometheus_uid: str,
    clickhouse_uid: str,
    only: set[str] | None = None,
    yes: bool,
    prune: bool,
) -> Result:
    """``services``: each spec with its dashboard files. ``only``: dashboard UIDs."""
    from leasehold import grafana as lh_grafana
    from leasehold.grafana import apply as ga

    ds = lh_grafana.Datasources(prometheus=prometheus_uid, clickhouse=clickhouse_uid)
    p = lh_grafana.plan(services, env, ds)
    if only:
        unknown = sorted(only - {d.uid for d in p.dashboards})
        if unknown:
            raise ValueError(f"not in the plan: {', '.join(unknown)}")
        p = ga.only(p, only)
    g = ga.Grafana(url, token=token, user=user, password=password)
    d = ga.diff(g, p)
    for w in d.warnings:
        out(f"warning  {w}")
    for c in [] if only else d.stale:
        _stale(out, str(c), prune, "deletes")
    for c in d.changes:
        out(str(c))
    r = Result(changes=len(d.changes) + (len(d.stale) if prune and not only else 0))
    if yes and r.changes:
        ga.apply(g, d, prune=prune and not only)
        r.applied = True
    return r


def nomad(
    out: Out,
    specs: list[ServiceSpec],
    env: str,
    *,
    url: str,
    token: str | None,
    sha: str,
    registry: str,
    api_url: str,
    kafka_bootstrap: str | None,
    node_pool: str | None = None,
    only: set[str] | None = None,
    yes: bool,
    prune: bool,
) -> Result:
    from leasehold import naming
    from leasehold import nomad as lh_nomad
    from leasehold.nomad import apply as na

    platform = lh_nomad.Platform(
        registry=registry, api_url=api_url, kafka_bootstrap=kafka_bootstrap, node_pool=node_pool
    )
    naming.image_ref(registry, "x", "x", sha)  # checks the SHA and registry
    p = lh_nomad.plan(specs, env, sha, platform)
    if only:
        unknown = sorted(only - {j.job_id for j in p.jobs})
        if unknown:
            raise ValueError(f"not in the plan: {', '.join(unknown)}")
        p = type(p)(env=p.env, namespace=p.namespace, jobs=[j for j in p.jobs if j.job_id in only])
    client = na.Nomad(url, token=token)
    try:
        d = na.diff(client, p, sorted({j.service for j in p.jobs} | {s.name for s in specs}))
        for w in d.warnings:
            out(f"warning  {w}")
        for ns, job_id in [] if only else d.stale:
            _stale(out, f"{ns}/{job_id}", prune, "stops it")
        for c in d.changes:
            out(str(c))
        r = Result(changes=len(d.changes) + (len(d.stale) if prune and not only else 0))
        if yes and r.changes:
            evals = na.apply(client, d, prune=prune and not only)
            out(f"{len(evals)} evaluations")
            r.applied = True
    finally:
        client.close()
    return r
