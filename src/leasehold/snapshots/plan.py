"""What ClickHouse should contain for the snapshot datasets of a set of services.

Per service with ``snapshots``, in its database (``<env prefix><database>``):

- ``snapshot_loads``: one row per state change of a load (``started``, ``loaded``,
  ``current``, ``failed``, ``rolled_back``, ``released``, ``dropped``, ``retry``). Never
  dropped. A ``rolled_back`` row holds the dataset until a ``released`` or ``current`` row.
- ``snapshot_current``: a view, the current snapshot per dataset (the latest
  ``current`` or ``rolled_back`` row). Switching a dataset is one INSERT into
  ``snapshot_loads``; every table's view follows at once.
- per snapshot table ``<t>``:
  - ``<t>__snapshots``: the data, one partition per kept snapshot, from the schema file;
  - ``<t>``: the view readers use, ``SQL SECURITY DEFINER`` as the service's definer
    user, filtered to the current snapshot (``replace``) or to every snapshot up to the
    current one that was ever current (``append``).
- the definer user (``naming.clickhouse_definer_user``): no login, ``SELECT`` on the
  service's database. Readers of a shared view need a grant on the view only.
- per dataset:
  - a named collection (``naming.snapshot_collection``) with the bucket's URL as
    ClickHouse reaches it and the read-only keys of ``naming.clickhouse_storage_user``,
    none of them overridable;
  - the loader user (``naming.snapshot_loader_user``), profile ``leasehold_loader``:
    create, fill and drop ``_staging_*`` tables, attach and drop partitions of its
    ``__snapshots`` tables, read and write ``snapshot_loads``, read through its named
    collection, ``SELECT`` on ``system.parts`` and ``system.tables`` (which show it only
    what it may see) and on the environment's busy-check tables.

Objects carry a ``leasehold:`` comment, as sink objects do. The view of a shared
table (``clickhouse.shared``) has the table's description first, the marker on its own
last line; a changed description recreates the view, like any other change to it.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from leasehold import naming
from leasehold.clickhouse.plan import MARK, _quote, with_description
from leasehold.clickhouse.schema import SOURCE, TABLE, SinkSchema
from leasehold.nomad.render import job_variable_path
from leasehold.snapshots.schema import SNAPSHOT, parse_snapshot
from leasehold.spec.models import ServiceSpec

LOADER_PROFILE = "leasehold_loader"
LOADER_LIMITS = {
    "max_execution_time": "3600",
    "max_memory_usage": str(16 * 1024**3),
    "max_threads": "8",
    "max_insert_threads": "4",
}
STATUSES = (
    "started", "loaded", "current", "failed", "rolled_back", "released", "dropped", "retry",
)  # fmt: skip


def ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


@dataclass(frozen=True, order=True)
class Grant:
    """One grant as ``system.grants`` shows it. ``table`` ending in ``*`` is a prefix
    grant (ClickHouse lists it without the ``*``); ``object`` is a source (``S3``) or a
    named collection."""

    access: str
    database: str | None = None
    table: str | None = None
    object: str | None = None

    def on(self) -> str:
        if self.access == "NAMED COLLECTION":
            return ident(self.object or "")
        if self.object:
            return self.object
        if self.database is None:
            return "*.*"
        if self.table is None:
            return f"{ident(self.database)}.*"
        if self.table.endswith("*"):
            return f"{ident(self.database)}.{self.table}"  # a prefix grant can't be quoted
        return f"{ident(self.database)}.{ident(self.table)}"

    def row(self) -> tuple[str, str | None, str | None, str | None]:
        """As ``system.grants`` lists it."""
        table = self.table[:-1] if self.table and self.table.endswith("*") else self.table
        return (self.access, self.database, table, self.object or None)


@dataclass(frozen=True)
class TablePlan:
    dataset: str
    name: str
    mode: str
    database: str
    schema: SinkSchema
    description: str | None = None  # clickhouse.shared, in the view's comment

    @property
    def storage_table(self) -> str:
        return f"{self.database}.{naming.snapshot_table(self.name)}"

    @property
    def view(self) -> str:
        return f"{self.database}.{self.name}"

    def storage_comment(self) -> str:
        return f"{MARK} snapshot={self.dataset} table={self.schema.table_hash()}"

    def create_storage_sql(self) -> str:
        sql = self.schema.create_table.replace(TABLE, self.storage_table, 1)
        return f"{sql}\nCOMMENT {_quote(self.storage_comment())}"

    def view_select(self) -> str:
        current = (
            f"(SELECT snapshot FROM {self.database}.{naming.SNAPSHOT_CURRENT_VIEW} "
            f"WHERE dataset = {_quote(self.dataset)})"
        )
        if self.mode == "replace":
            where = f"snapshot = {current}"
        else:
            ever = (
                f"(SELECT snapshot FROM {self.database}.{naming.SNAPSHOT_LOADS_TABLE} "
                f"WHERE dataset = {_quote(self.dataset)} AND status = 'current')"
            )
            where = f"snapshot IN {ever} AND snapshot <= {current}"
        return f"SELECT * FROM {self.storage_table} WHERE {where}"

    def view_comment(self, definer: str) -> str:
        parts = [self.view_select(), definer, "1"]
        fp = hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
        return with_description(f"{MARK} snapshot={self.dataset} view={fp}", self.description)

    def create_view_sql(self, definer: str) -> str:
        return (
            f"CREATE VIEW {self.view} DEFINER = {ident(definer)} SQL SECURITY DEFINER\n"
            f"COMMENT {_quote(self.view_comment(definer))}\nAS {self.view_select()}"
        )

    def insert_sql(self, staging: str, snapshot: str, source: str) -> str:
        """Fill ``staging`` (``<database>.<table>``) from ``source`` (an ``s3(...)``)."""
        select = self.schema.select.replace(SOURCE, source, 1).replace(SNAPSHOT, _quote(snapshot))
        return f"INSERT INTO {staging}\n{select}"


@dataclass(frozen=True)
class DatasetPlan:
    service: str
    dataset: str
    database: str
    bucket: str  # the bucket's actual name
    collection: str
    tables: tuple[TablePlan, ...]
    checks: tuple[tuple[str, int | None, float | None], ...]  # (table, min_rows, max_change)
    keep: int
    max_attempts: int
    timeout_s: int
    loader_user: str
    namespace: str
    loader_variable: str  # Nomad Variable of the loader's job

    def grants(self, busy_tables: tuple[str, ...] = ()) -> frozenset[Grant]:
        db = self.database
        staging = f"{naming.SNAPSHOT_STAGING_PREFIX}*"
        g = {Grant(a, db, staging) for a in ("SELECT", "INSERT", "CREATE TABLE", "DROP TABLE")}
        for t in self.tables:
            st = naming.snapshot_table(t.name)
            g |= {Grant(a, db, st) for a in ("SELECT", "INSERT", "ALTER DELETE")}
        g |= {Grant(a, db, naming.SNAPSHOT_LOADS_TABLE) for a in ("SELECT", "INSERT")}
        g.add(Grant("SELECT", db, naming.SNAPSHOT_CURRENT_VIEW))
        # ClickHouse shows only the parts and tables of what the user may see.
        g |= {Grant("SELECT", "system", t) for t in ("parts", "tables")}
        g.add(Grant("CREATE TEMPORARY TABLE"))
        g.add(Grant("READ", object="S3"))
        g.add(Grant("NAMED COLLECTION", object=self.collection))
        for ref in busy_tables:
            d, t = ref.split(".", 1)
            g.add(Grant("SELECT", d, None if t == "*" else t))
        return frozenset(g)

    def settings(self, window: dict, busy_query: str | None) -> dict:
        """What the loader job needs (``LEASEHOLD_SNAPSHOT``): the image has no service.yml."""
        return {
            "service": self.service,
            "dataset": self.dataset,
            "database": self.database,
            "bucket": self.bucket,
            "collection": self.collection,
            "keep": self.keep,
            "max_attempts": self.max_attempts,
            "timeout_s": self.timeout_s,
            "checks": [list(c) for c in self.checks],
            "tables": {
                t.name: {
                    "mode": t.mode,
                    "create": t.schema.create_table,
                    "select": t.schema.select,
                }
                for t in self.tables
            },
            "window": window,
            "busy_query": busy_query,
        }


@dataclass(frozen=True)
class ServicePlan:
    service: str
    database: str
    definer: str
    storage_user: str
    storage_variable: str
    datasets: tuple[DatasetPlan, ...]

    def loads_sql(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self.database}.{naming.SNAPSHOT_LOADS_TABLE}\n(\n"
            "    dataset LowCardinality(String),\n"
            "    snapshot String,\n"
            "    status LowCardinality(String),\n"
            "    run_id String,\n"
            "    source_date Nullable(Date),\n"
            "    rows Map(String, UInt64),\n"
            "    manifest String,\n"
            "    error String,\n"
            "    loader String,\n"
            "    at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')\n)\n"
            "ENGINE = MergeTree\nORDER BY (dataset, at)\n"
            f"COMMENT {_quote(f'{MARK} snapshot loads')}"
        )

    def current_comment(self) -> str:
        return f"{MARK} snapshot current v1"

    def current_sql(self) -> str:
        return (
            f"CREATE VIEW {self.database}.{naming.SNAPSHOT_CURRENT_VIEW}\n"
            f"COMMENT {_quote(self.current_comment())}\n"
            "AS SELECT dataset, argMax(snapshot, at) AS snapshot, max(at) AS since\n"
            f"FROM {self.database}.{naming.SNAPSHOT_LOADS_TABLE}\n"
            "WHERE status IN ('current', 'rolled_back') GROUP BY dataset"
        )


def collection_url(clickhouse_endpoint: str, bucket: str) -> str:
    return f"{clickhouse_endpoint.rstrip('/')}/{bucket}/"


def collection_fingerprint(name: str, url: str, access_key: str, secret_key: str) -> str:
    """Stored next to the keys: ClickHouse doesn't show a collection's secrets back."""
    return hashlib.sha256(f"{name}|{url}|{access_key}|{secret_key}".encode()).hexdigest()[:16]


def plan(services: list[tuple[ServiceSpec, Path]], env: str) -> list[ServicePlan]:
    """``services``: each spec with the directory of its ``service.yml``."""
    naming.check_env(env)
    out = []
    for spec, base in sorted(services, key=lambda x: x[0].name):
        if not spec.snapshots or spec.clickhouse is None:
            continue
        db = naming.clickhouse_database(env, spec.clickhouse.database)
        shared = spec.clickhouse.shared
        datasets = []
        for dname, d in sorted(spec.snapshots.items()):
            tables = tuple(
                TablePlan(
                    dataset=dname,
                    name=tname,
                    mode=t.mode,
                    database=db,
                    schema=parse_snapshot((base / t.schema_file).read_text()),
                    description=shared[tname].description if tname in shared else None,
                )
                for tname, t in sorted(d.tables.items())
            )
            loader = naming.snapshot_loader_component(dname)
            datasets.append(
                DatasetPlan(
                    service=spec.name,
                    dataset=dname,
                    database=db,
                    bucket=naming.bucket(env, spec.name, d.bucket),
                    collection=naming.snapshot_collection(env, spec.name, d.bucket),
                    tables=tables,
                    checks=tuple((c.table, c.min_rows, c.max_change) for c in d.checks),
                    keep=d.keep,
                    max_attempts=d.max_attempts,
                    timeout_s=int(d.timeout.total_seconds()),
                    loader_user=naming.snapshot_loader_user(env, spec.name, dname),
                    namespace=naming.nomad_namespace(env),
                    loader_variable=job_variable_path(naming.nomad_job(spec.name, loader)),
                )
            )
        out.append(
            ServicePlan(
                service=spec.name,
                database=db,
                definer=naming.clickhouse_definer_user(env, spec.name),
                storage_user=naming.clickhouse_storage_user(env, spec.name),
                storage_variable=naming.clickhouse_storage_variable(spec.name),
                datasets=tuple(datasets),
            )
        )
    return out


def loader_settings(
    services: list[tuple[ServiceSpec, Path]],
    env: str,
    window: dict | None = None,
    busy_query: str | None = None,
) -> dict[str, dict]:
    """Loader job ID → its ``LEASEHOLD_SNAPSHOT`` settings."""
    return {
        naming.nomad_job(dp.service, naming.snapshot_loader_component(dp.dataset)): dp.settings(
            window or {}, busy_query
        )
        for sp in plan(services, env)
        for dp in sp.datasets
    }
