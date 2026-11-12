"""The ``leasehold`` command line."""

from __future__ import annotations

import argparse
import json
import os
import sys

from leasehold import __version__, naming
from leasehold import kafka as lh_kafka
from leasehold.spec import (
    ServiceSpec,
    SpecError,
    check_registry,
    find_specs,
    load_spec,
    resource_names,
)


def _cmd_validate(args: argparse.Namespace) -> int:
    paths = find_specs(args.paths)
    if not paths:
        print("no service.yml files found", file=sys.stderr)
        return 1
    specs: list[ServiceSpec] = []
    failed = False
    for path in paths:
        try:
            spec = load_spec(path)
        except SpecError as e:
            failed = True
            print(f"FAIL {path}")
            for msg in e.messages:
                print(f"     {msg}")
            continue
        specs.append(spec)
        print(f"ok   {path} ({spec.name})")
    registry_errors = check_registry(specs, partial=args.partial)
    for msg in registry_errors:
        print(f"FAIL registry: {msg}")
    return 1 if failed or registry_errors else 0


def _cmd_names(args: argparse.Namespace) -> int:
    try:
        naming.check_env(args.env)
        spec = load_spec(args.path)
    except (SpecError, naming.NamingError) as e:
        print(e, file=sys.stderr)
        return 1
    rows = resource_names(spec, args.env)
    widths = [max(len(r[i]) for r in rows) for i in range(2)]
    for kind, declared, actual in rows:
        print(f"{kind:<{widths[0]}}  {declared:<{widths[1]}}  {actual}")
    return 0


def _load_all(paths: list[str]) -> list[ServiceSpec] | None:
    """Load and cross-check every service; print problems and return None if any."""
    specs: list[ServiceSpec] = []
    ok = True
    for path in find_specs(paths):
        try:
            specs.append(load_spec(path))
        except SpecError as e:
            ok = False
            print(f"FAIL {path}", file=sys.stderr)
            for msg in e.messages:
                print(f"     {msg}", file=sys.stderr)
    for msg in check_registry(specs):
        ok = False
        print(f"FAIL registry: {msg}", file=sys.stderr)
    if not specs:
        print("no service.yml files found", file=sys.stderr)
        ok = False
    return specs if ok else None


def _cmd_kafka_plan(args: argparse.Namespace) -> int:
    specs = _load_all(args.paths)
    if specs is None:
        return 1
    p = lh_kafka.plan(specs, args.env)
    print(f"# environment: {p.env}")
    print("\n# topics")
    for t in p.topics:
        configs = " ".join(f"{k}={v}" for k, v in t.configs)
        print(f"{t.name}  partitions={t.partitions}  {configs}")
    print("\n# principals (SCRAM users)")
    for user in p.principals:
        print(user)
    print("\n# ACLs")
    for acl in p.acls:
        print(acl)
    return 0


def _cmd_kafka_apply(args: argparse.Namespace) -> int:
    from leasehold.kafka import apply as kafka_apply

    specs = _load_all(args.paths)
    if specs is None:
        return 1
    password = os.environ.get(args.password_env)
    if not password:
        print(f"set the admin password in ${args.password_env}", file=sys.stderr)
        return 1
    p = lh_kafka.plan(specs, args.env)
    admin = kafka_apply.admin_client(args.bootstrap, args.username, password)
    d = kafka_apply.diff(admin, p, [s.name for s in specs])
    for w in d.warnings:
        print(f"warning  {w}")
    if d.empty:
        print("no changes")
        return 0
    for c in d.changes:
        print(c)
    if not args.yes:
        print("\ndry run; pass --yes to apply")
        return 0
    kafka_apply.apply(admin, d, prune=args.prune)
    print("applied")
    return 0


def _dsn(args: argparse.Namespace) -> str | None:
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"set the Postgres connection string in ${args.dsn_env}", file=sys.stderr)
    return dsn


def _cmd_frontier_migrate(args: argparse.Namespace) -> int:
    import psycopg

    from leasehold.frontier import schema

    if not (dsn := _dsn(args)):
        return 1
    with psycopg.connect(dsn, autocommit=True) as conn:
        before = schema.current_version(conn, args.env)
        if not args.yes:
            pending = [v for v, _ in schema.MIGRATIONS if v > before]
            print(f"schema {schema.schema_name(args.env)}: version {before}")
            print(f"pending migrations: {pending or 'none'}")
            if pending:
                print("\ndry run; pass --yes to apply")
            return 0
        applied = schema.migrate(conn, args.env)
    print(f"schema {schema.schema_name(args.env)}: applied {applied or 'nothing'}")
    return 0


def _cmd_frontier_apply(args: argparse.Namespace) -> int:
    import psycopg

    from leasehold.frontier import schema

    specs = _load_all(args.paths)
    if specs is None or not (dsn := _dsn(args)):
        return 1
    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            d = schema.diff_task_types(conn, args.env, specs)
        except RuntimeError as e:
            print(e, file=sys.stderr)
            return 1
        for w in d.warnings:
            print(f"warning  {w}")
        if d.empty:
            print("no changes")
            return 0
        for c in d.changes:
            print(c)
        if not args.yes:
            print("\ndry run; pass --yes to apply")
            return 0
        schema.apply_task_types(conn, args.env, d)
    print("applied")
    return 0


def _cmd_api_serve(args: argparse.Namespace) -> int:
    import logging

    import uvicorn

    from leasehold.api.app import create_app
    from leasehold.api.settings import Settings, SettingsError

    try:
        settings = Settings.from_env()
    except (SettingsError, naming.NamingError) as e:
        print(e, file=sys.stderr)
        return 1
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    uvicorn.run(create_app(settings), host=args.host, port=args.port, proxy_headers=False)
    return 0


def _cmd_api_token(args: argparse.Namespace) -> int:
    from leasehold.api.auth import hash_token, new_token

    if bool(args.admin) == bool(args.component):
        print("pass either --admin NAME or --component SERVICE.COMPONENT", file=sys.stderr)
        return 1
    token = new_token()
    entry: dict[str, str] = {"sha256": hash_token(token)}
    if args.admin:
        entry["admin"] = args.admin
    else:
        service, _, component = args.component.partition(".")
        if not component:
            print("--component must look like SERVICE.COMPONENT", file=sys.stderr)
            return 1
        entry |= {"service": service, "component": component}
    print(f"token (shown once): {token}", file=sys.stderr)
    print(json.dumps(entry))
    return 0


def _load_with_dashboards(paths: list[str]):
    """Like _load_all, plus each service's own dashboard files."""
    from leasehold.spec import read_dashboard_files

    specs = _load_all(paths)
    if specs is None:
        return None
    by_name = {s.name: s for s in specs}
    out = []
    for path in find_specs(paths):
        spec = by_name[load_spec(path).name]
        out.append((spec, read_dashboard_files(path, spec)))
    return out


def _grafana_plan(args: argparse.Namespace):
    from leasehold import grafana

    services = _load_with_dashboards(args.paths)
    if services is None:
        return None
    ds = grafana.Datasources(prometheus=args.prometheus_uid, clickhouse=args.clickhouse_uid)
    return grafana.plan(services, args.env, ds)


def _cmd_grafana_plan(args: argparse.Namespace) -> int:
    from pathlib import Path

    p = _grafana_plan(args)
    if p is None:
        return 1
    print(f"# environment: {p.env}")
    print("\n# folders")
    for f in p.folders:
        print(f"{f.uid}  {f.title}")
    print("\n# dashboards")
    for d in p.dashboards:
        panels = sum(1 for x in d.dashboard["panels"] if x["type"] != "row")
        print(f"{d.uid}  {d.title}  [{d.source}, {panels} panels, folder {d.folder_uid}]")
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        for d in p.dashboards:
            (out / f"{d.uid}.json").write_text(json.dumps(d.dashboard, indent=2) + "\n")
        print(f"\nwrote {len(p.dashboards)} dashboards to {out}")
    return 0


def _cmd_grafana_apply(args: argparse.Namespace) -> int:
    from leasehold.grafana import apply as grafana_apply

    p = _grafana_plan(args)
    if p is None:
        return 1
    token = os.environ.get(args.token_env)
    password = os.environ.get(args.password_env) if args.user else None
    if not token and not password:
        print(
            f"set a service-account token in ${args.token_env}, "
            f"or pass --user and set ${args.password_env}",
            file=sys.stderr,
        )
        return 1
    if args.only:
        if args.prune:
            print("--only and --prune can't be combined", file=sys.stderr)
            return 1
        unknown = sorted(set(args.only) - {d.uid for d in p.dashboards})
        if unknown:
            print(f"not in the plan: {', '.join(unknown)}", file=sys.stderr)
            return 1
        p = grafana_apply.only(p, set(args.only))
    g = grafana_apply.Grafana(args.url, token=token, user=args.user, password=password)
    try:
        d = grafana_apply.diff(g, p)
    except grafana_apply.GrafanaError as e:
        print(e, file=sys.stderr)
        return 1
    for c in [] if args.only else d.stale:
        print(f"stale    {c}" + ("" if args.prune else "  (kept; --prune deletes)"))
    if not d.changes and not (args.prune and d.stale):
        print("no changes")
        return 0
    for c in d.changes:
        print(c)
    if not args.yes:
        print("\ndry run; pass --yes to apply")
        return 0
    grafana_apply.apply(g, d, prune=args.prune)
    print("applied")
    return 0


def _nomad_plan(args: argparse.Namespace):
    from leasehold import nomad

    specs = _load_all(args.paths)
    if specs is None:
        return None
    platform = nomad.Platform(
        registry=args.registry, api_url=args.api_url, kafka_bootstrap=args.kafka_bootstrap
    )
    try:
        naming.image_ref(args.registry, "x", "x", args.sha)
        return nomad.plan(specs, args.env, args.sha, platform)
    except (ValueError, naming.NamingError) as e:
        print(e, file=sys.stderr)
        return None


def _cmd_nomad_plan(args: argparse.Namespace) -> int:
    from pathlib import Path

    p = _nomad_plan(args)
    if p is None:
        return 1
    print(f"# environment: {p.env}, namespace: {p.namespace}")
    for j in p.jobs:
        task = j.job["TaskGroups"][0]["Tasks"][0]
        r = task["Resources"]
        mem = f"{r['MemoryMB']}" + (f"-{r['MemoryMaxMB']}" if "MemoryMaxMB" in r else "")
        count = (
            "periodic " + j.job["Periodic"]["Specs"][0]
            if j.periodic
            else (f"count {j.count_min}-{j.count_max}")
        )
        print(f"{j.job_id}  {j.job['Type']}  {count}  cpu {r['CPU']} MHz  memory {mem} MB  "
              f"{task['Config']['image']}")  # fmt: skip
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        for j in p.jobs:
            (out / f"{j.job_id}.json").write_text(json.dumps({"Job": j.job}, indent=2) + "\n")
        print(f"\nwrote {len(p.jobs)} jobs to {out}")
    return 0


def _cmd_nomad_apply(args: argparse.Namespace) -> int:
    from leasehold.nomad import apply as nomad_apply

    p = _nomad_plan(args)
    if p is None:
        return 1
    if args.only:
        if args.prune:
            print("--only and --prune can't be combined", file=sys.stderr)
            return 1
        unknown = sorted(set(args.only) - {j.job_id for j in p.jobs})
        if unknown:
            print(f"not in the plan: {', '.join(unknown)}", file=sys.stderr)
            return 1
        p = type(p)(env=p.env, namespace=p.namespace,
                    jobs=[j for j in p.jobs if j.job_id in args.only])  # fmt: skip
    client = nomad_apply.Nomad(args.url, token=os.environ.get(args.token_env))
    try:
        d = nomad_apply.diff(client, p, sorted({j.service for j in p.jobs}))
        for w in d.warnings:
            print(f"warning  {w}")
        for ns, job_id in [] if args.only else d.stale:
            print(f"stale    {ns}/{job_id}" + ("" if args.prune else "  (kept; --prune stops it)"))
        if d.empty and not (args.prune and d.stale):
            print("no changes")
            return 0
        for c in d.changes:
            print(c)
        if not args.yes:
            print("\ndry run; pass --yes to apply")
            return 0
        evals = nomad_apply.apply(client, d, prune=args.prune)
    except nomad_apply.NomadError as e:
        print(e, file=sys.stderr)
        return 1
    finally:
        client.close()
    print(f"applied ({len(evals)} evaluations)")
    return 0


def _cmd_credentials_apply(args: argparse.Namespace) -> int:
    from leasehold import credentials
    from leasehold.kafka.apply import KafkaException, admin_client
    from leasehold.nomad.apply import Nomad, NomadError

    specs = _load_all(args.paths)
    if specs is None:
        return 1
    password = os.environ.get(args.kafka_password_env)
    if not password:
        print(f"set the Kafka admin password in ${args.kafka_password_env}", file=sys.stderr)
        return 1
    creds = credentials.plan(specs, args.env)
    admin = admin_client(args.bootstrap, args.kafka_username, password)
    nomad = Nomad(args.url, token=os.environ.get(args.token_env))
    services = sorted(s.name for s in specs)
    try:
        d = credentials.diff(admin, nomad, creds, args.env, services, rotate=args.rotate)
        for u in d.stale_users:
            print(f"stale    kafka user {u}" + ("" if args.prune else "  (kept; --prune deletes)"))
        for ns, path in d.stale_variables:
            print(f"stale    {ns}/{path}" + ("" if args.prune else "  (kept; --prune clears)"))
        stale = d.stale_users or d.stale_variables
        if d.empty and not (args.prune and stale):
            print(f"no changes ({len(creds)} credentials in place)")
            return 0
        for c in d.changes:
            print(c)
        if not args.yes:
            print("\ndry run; pass --yes to apply")
            return 0
        credentials.apply(admin, nomad, d, prune=args.prune)
    except (KafkaException, NomadError) as e:
        print(e, file=sys.stderr)
        return 1
    finally:
        nomad.close()
    print("applied")
    return 0


def _clickhouse_plan(args: argparse.Namespace):
    from pathlib import Path

    from leasehold import clickhouse

    specs = _load_all(args.paths)
    if specs is None:
        return None
    by_name = {s.name: s for s in specs}
    services = [(by_name[load_spec(p).name], Path(p).parent) for p in find_specs(args.paths)]
    return clickhouse.plan(services, args.env)


def _cmd_clickhouse_plan(args: argparse.Namespace) -> int:
    p = _clickhouse_plan(args)
    if p is None:
        return 1
    print(f"# environment: {p.env}")
    for db in p.databases:
        print(f"\n# database {db.name} ({db.service})")
        for s in db.sinks:
            print(f"{s.table}  ← {s.topic}  group {s.group}  as {s.principal}")
            if args.sql:
                print(f"\n{s.create_table_sql()};\n\n{s.create_mv_sql('…')};\n")
    return 0


def _cmd_clickhouse_apply(args: argparse.Namespace) -> int:
    from leasehold.clickhouse.apply import ClickHouse, ClickHouseError, apply, diff
    from leasehold.nomad.apply import Nomad, NomadError

    p = _clickhouse_plan(args)
    if p is None:
        return 1
    ch_password = os.environ.get(args.password_env)
    if ch_password is None:
        print(f"set the ClickHouse password in ${args.password_env}", file=sys.stderr)
        return 1
    ch = ClickHouse(args.url, args.user, ch_password)
    nomad = Nomad(args.nomad_url, token=os.environ.get(args.nomad_token_env))
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
        d = diff(ch, p, args.kafka_bootstrap, versions, set(args.accept_table or []))
        for w in d.warnings:
            print(f"warning  {w}")
        for name, engine in d.stale:
            kept = engine not in ("MaterializedView", "Kafka")
            note = (
                "  (data table: kept, drop by hand)"
                if kept
                else ("" if args.prune else "  (kept; --prune drops)")
            )
            print(f"stale    {name} [{engine}]{note}")
        if d.empty and not (args.prune and d.stale):
            print("no changes" + (f" (skipped: {', '.join(d.skipped)})" if d.skipped else ""))
            return 1 if d.skipped else 0
        for db in d.new_databases:
            print(f"create   database {db.name}")
        for db in d.errors_tables:
            print(f"create   table {db.name}.sink_errors")
        for c in d.changes:
            print(f"sink     {c}")
        if not args.yes:
            print("\ndry run; pass --yes to apply")
            return 0
        apply(ch, d, args.kafka_bootstrap, passwords, prune=args.prune)
    except (ClickHouseError, NomadError) as e:
        print(e, file=sys.stderr)
        return 1
    finally:
        ch.close()
        nomad.close()
    if d.skipped:
        print(f"applied, except: {', '.join(d.skipped)} (see warnings)")
        return 1
    print("applied")
    return 0


def _cmd_schema(args: argparse.Namespace) -> int:
    schema = ServiceSpec.model_json_schema(by_alias=True)
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["title"] = "leasehold service.yml"
    print(json.dumps(schema, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="leasehold", description="leasehold platform tooling")
    p.add_argument("--version", action="version", version=f"leasehold {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    v = sub.add_parser("validate", help="validate service.yml files and how they fit together")
    v.add_argument("paths", nargs="+", help="service.yml files or directories to search")
    v.add_argument(
        "--partial",
        action="store_true",
        help="accept references to services that aren't among the given files",
    )
    v.set_defaults(func=_cmd_validate)

    n = sub.add_parser("names", help="show the resource names a service gets in an environment")
    n.add_argument("path", help="a service.yml file")
    n.add_argument("--env", default=naming.PROD, help="environment (default: prod)")
    n.set_defaults(func=_cmd_names)

    s = sub.add_parser("schema", help="print the JSON Schema of service.yml (for editors)")
    s.set_defaults(func=_cmd_schema)

    k = sub.add_parser("kafka", help="Kafka topics and ACLs derived from service.yml")
    ksub = k.add_subparsers(dest="kafka_command", required=True)

    kp = ksub.add_parser("plan", help="show the topics, principals and ACLs for an environment")
    kp.add_argument("paths", nargs="+", help="service.yml files or directories (all services)")
    kp.add_argument("--env", default=naming.PROD)
    kp.set_defaults(func=_cmd_kafka_plan)

    ka = ksub.add_parser("apply", help="compare with a cluster and apply (dry run by default)")
    ka.add_argument("paths", nargs="+", help="service.yml files or directories (all services)")
    ka.add_argument("--env", default=naming.PROD)
    ka.add_argument("--bootstrap", required=True, help="e.g. localhost:19092")
    ka.add_argument("--username", default="admin")
    ka.add_argument(
        "--password-env",
        default="LEASEHOLD_KAFKA_PASSWORD",
        help="environment variable holding the admin password",
    )
    ka.add_argument("--yes", action="store_true", help="apply the changes")
    ka.add_argument(
        "--prune",
        action="store_true",
        help="also delete ACLs of managed principals not in the plan",
    )
    ka.set_defaults(func=_cmd_kafka_apply)

    f = sub.add_parser("frontier", help="the frontier schema and task types in Postgres")
    fsub = f.add_subparsers(dest="frontier_command", required=True)
    dsn_help = "environment variable holding the Postgres connection string"

    fm = fsub.add_parser("migrate", help="create or upgrade the schema (dry run by default)")
    fm.add_argument("--env", default=naming.PROD)
    fm.add_argument("--dsn-env", default="LEASEHOLD_DSN", help=dsn_help)
    fm.add_argument("--yes", action="store_true", help="apply the migrations")
    fm.set_defaults(func=_cmd_frontier_migrate)

    fa = fsub.add_parser("apply", help="sync task types from service.yml (dry run by default)")
    fa.add_argument("paths", nargs="+", help="service.yml files or directories (all services)")
    fa.add_argument("--env", default=naming.PROD)
    fa.add_argument("--dsn-env", default="LEASEHOLD_DSN", help=dsn_help)
    fa.add_argument("--yes", action="store_true", help="apply the changes")
    fa.set_defaults(func=_cmd_frontier_apply)

    gr = sub.add_parser("grafana", help="dashboards derived from service.yml")
    grsub = gr.add_subparsers(dest="grafana_command", required=True)

    def grafana_common(x: argparse.ArgumentParser) -> None:
        x.add_argument("paths", nargs="+", help="service.yml files or directories (all services)")
        x.add_argument("--env", default=naming.PROD)
        x.add_argument("--prometheus-uid", default="prometheus", help="data source UID")
        x.add_argument("--clickhouse-uid", default="clickhouse", help="data source UID")

    gp = grsub.add_parser("plan", help="show the folders and dashboards for an environment")
    grafana_common(gp)
    gp.add_argument("--out", help="also write each dashboard's JSON to this directory")
    gp.set_defaults(func=_cmd_grafana_plan)

    ga = grsub.add_parser("apply", help="compare with Grafana and apply (dry run by default)")
    grafana_common(ga)
    ga.add_argument("--url", required=True, help="e.g. http://localhost:13000")
    ga.add_argument(
        "--token-env",
        default="LEASEHOLD_GRAFANA_TOKEN",
        help="environment variable holding a service-account token",
    )
    ga.add_argument("--user", help="basic auth instead of a token")
    ga.add_argument("--password-env", default="LEASEHOLD_GRAFANA_PASSWORD")
    ga.add_argument("--yes", action="store_true", help="apply the changes")
    ga.add_argument("--prune", action="store_true", help="also delete stale leasehold dashboards")
    ga.add_argument(
        "--only",
        action="append",
        metavar="UID",
        help="apply only this dashboard (and its folder); repeatable; no stale report",
    )
    ga.set_defaults(func=_cmd_grafana_apply)

    nm = sub.add_parser("nomad", help="Nomad jobs derived from service.yml")
    nmsub = nm.add_subparsers(dest="nomad_command", required=True)

    def nomad_common(x: argparse.ArgumentParser) -> None:
        x.add_argument("paths", nargs="+", help="service.yml files or directories (all services)")
        x.add_argument("--env", default=naming.PROD)
        x.add_argument("--sha", required=True, help="git SHA the component images are tagged with")
        x.add_argument("--registry", default="ghcr.io/open-data-collection")
        x.add_argument("--api-url", required=True, help="control API, e.g. http://...:8400")
        x.add_argument("--kafka-bootstrap", help="needed when a component uses Kafka")

    nmp = nmsub.add_parser("plan", help="show the jobs for an environment")
    nomad_common(nmp)
    nmp.add_argument("--out", help="also write each job's JSON to this directory")
    nmp.set_defaults(func=_cmd_nomad_plan)

    nma = nmsub.add_parser("apply", help="compare with Nomad and apply (dry run by default)")
    nomad_common(nma)
    nma.add_argument("--url", required=True, help="e.g. http://100.83.247.12:4646")
    nma.add_argument(
        "--token-env", default="NOMAD_TOKEN", help="environment variable holding the ACL token"
    )
    nma.add_argument("--yes", action="store_true", help="apply the changes")
    nma.add_argument("--prune", action="store_true", help="also stop stale jobs of these services")
    nma.add_argument(
        "--only", action="append", metavar="JOB", help="apply only this job; repeatable"
    )
    nma.set_defaults(func=_cmd_nomad_apply)

    cr = sub.add_parser("credentials", help="per-component credentials (Kafka SCRAM users)")
    crsub = cr.add_subparsers(dest="credentials_command", required=True)
    cra = crsub.add_parser(
        "apply", help="create missing credentials in Kafka and Nomad Variables (dry run by default)"
    )
    cra.add_argument("paths", nargs="+", help="service.yml files or directories (all services)")
    cra.add_argument("--env", default=naming.PROD)
    cra.add_argument("--bootstrap", required=True, help="Kafka, e.g. localhost:19092")
    cra.add_argument("--kafka-username", default="admin")
    cra.add_argument("--kafka-password-env", default="LEASEHOLD_KAFKA_PASSWORD")
    cra.add_argument("--url", required=True, help="Nomad, e.g. http://100.83.247.12:4646")
    cra.add_argument("--token-env", default="NOMAD_TOKEN")
    cra.add_argument("--rotate", action="store_true", help="new passwords for all credentials")
    cra.add_argument("--yes", action="store_true", help="apply the changes")
    cra.add_argument(
        "--prune", action="store_true", help="also delete stale users and their variable keys"
    )
    cra.set_defaults(func=_cmd_credentials_apply)

    ch = sub.add_parser("clickhouse", help="ClickHouse sinks derived from service.yml")
    chsub = ch.add_subparsers(dest="clickhouse_command", required=True)
    chp = chsub.add_parser("plan", help="show the databases and sinks for an environment")
    chp.add_argument("paths", nargs="+", help="service.yml files or directories (all services)")
    chp.add_argument("--env", default=naming.PROD)
    chp.add_argument("--sql", action="store_true", help="also print each table and view")
    chp.set_defaults(func=_cmd_clickhouse_plan)
    cha = chsub.add_parser("apply", help="compare with ClickHouse and apply (dry run by default)")
    cha.add_argument("paths", nargs="+", help="service.yml files or directories (all services)")
    cha.add_argument("--env", default=naming.PROD)
    cha.add_argument("--url", required=True, help="ClickHouse HTTP, e.g. http://100.81.80.15:8123")
    cha.add_argument("--user", default="default")
    cha.add_argument("--password-env", default="LEASEHOLD_CLICKHOUSE_PASSWORD")
    cha.add_argument(
        "--kafka-bootstrap", required=True, help="Kafka as ClickHouse reaches it, e.g. kafka:9092"
    )
    cha.add_argument("--nomad-url", required=True, help="where the sink passwords are stored")
    cha.add_argument("--nomad-token-env", default="NOMAD_TOKEN")
    cha.add_argument(
        "--accept-table",
        action="append",
        metavar="DB.TABLE",
        help="the table was altered by hand to match its schema file; record that",
    )
    cha.add_argument("--yes", action="store_true", help="apply the changes")
    cha.add_argument(
        "--prune", action="store_true", help="drop Kafka tables and views of removed sinks"
    )
    cha.set_defaults(func=_cmd_clickhouse_apply)

    a = sub.add_parser("api", help="the control API")
    asub = a.add_subparsers(dest="api_command", required=True)
    aserve = asub.add_parser("serve", help="run the control API (settings from LEASEHOLD_*)")
    aserve.add_argument("--host", default="127.0.0.1")
    aserve.add_argument("--port", type=int, default=8400)
    aserve.set_defaults(func=_cmd_api_serve)
    atok = asub.add_parser("token", help="create a static token; prints its tokens-file entry")
    atok.add_argument("--admin", metavar="NAME")
    atok.add_argument("--component", metavar="SERVICE.COMPONENT")
    atok.set_defaults(func=_cmd_api_token)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
