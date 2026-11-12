"""Frontier database schema: migrations, and task types synced from ``service.yml``.

Each environment has its own Postgres schema (``frontier`` in prod, ``dev_frontier``
in dev), so prod and dev can share one Postgres server without seeing each other's
tasks.

``tasks`` is list-partitioned by task type. Each task type gets its own partition
table (``tasks_<n>``), optionally hash-partitioned by key into
``hash_partitions`` sub-tables. Partition names are numbered, not derived from the
task type name, so they stay within Postgres' 63-character limit.

These functions use a synchronous psycopg connection; they run from the CLI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

import psycopg
from psycopg import sql

from leasehold import naming
from leasehold.spec.models import ServiceSpec, TaskType

STATUSES = ("pending", "leased", "done", "dead")

# Each migration is a list of statements; {s} is the environment's schema.
MIGRATIONS: list[tuple[int, list[str]]] = [
    (
        1,
        [
            """
            CREATE TABLE {s}.task_types (
              name              text        PRIMARY KEY,
              service           text        NOT NULL,
              key_schema        text        NOT NULL,
              recrawl_interval  interval,
              max_attempts      int         NOT NULL,
              retry_backoff     interval    NOT NULL,
              retry_backoff_max interval    NOT NULL,
              lease_duration    interval    NOT NULL,
              default_priority  smallint    NOT NULL,
              capabilities      text[]      NOT NULL DEFAULT '{{}}',
              hash_partitions   int         NOT NULL,
              partition_table   text        NOT NULL UNIQUE,
              declared          boolean     NOT NULL DEFAULT true,
              paused            boolean     NOT NULL DEFAULT false,
              paused_reason     text,
              created_at        timestamptz NOT NULL DEFAULT now(),
              updated_at        timestamptz NOT NULL DEFAULT now()
            )
            """,
            "CREATE SEQUENCE {s}.partition_seq",
            """
            CREATE TABLE {s}.tasks (
              task_type        text        NOT NULL,
              key              text        NOT NULL,
              priority         smallint    NOT NULL CHECK (priority BETWEEN 0 AND 100),
              status           text        NOT NULL
                               CHECK (status IN ('pending', 'leased', 'done', 'dead')),
              next_due_at      timestamptz NOT NULL,
              attempts         int         NOT NULL DEFAULT 0,
              lease_id         uuid,
              leased_by        text,
              lease_expires_at timestamptz,
              payload          jsonb,
              country          text,
              created_by       text        NOT NULL,
              created_at       timestamptz NOT NULL DEFAULT now(),
              last_done_at     timestamptz,
              last_event_id    uuid,
              last_error       text,
              PRIMARY KEY (task_type, key)
            ) PARTITION BY LIST (task_type)
            """,
            # Leasing: highest priority first, then the longest overdue.
            """
            CREATE INDEX tasks_pending ON {s}.tasks (priority DESC, next_due_at)
              WHERE status = 'pending'
            """,
            # The sweeper: expired leases.
            """
            CREATE INDEX tasks_leased ON {s}.tasks (lease_expires_at)
              WHERE status = 'leased'
            """,
            # Counting dead tasks without scanning the partition.
            "CREATE INDEX tasks_dead ON {s}.tasks (task_type) WHERE status = 'dead'",
        ],
    ),
]

LATEST = MIGRATIONS[-1][0]


def schema_name(env: str) -> str:
    return naming.postgres_schema(env, "frontier")


def _q(stmt: str, schema: str) -> sql.Composed:
    return sql.SQL(stmt).format(s=sql.Identifier(schema))


def current_version(conn: psycopg.Connection, env: str) -> int:
    """Applied migration version, 0 if the schema doesn't exist yet."""
    s = schema_name(env)
    exists = conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'schema_migrations'",
        (s,),
    ).fetchone()
    if not exists:
        return 0
    row = conn.execute(_q("SELECT max(version) FROM {s}.schema_migrations", s)).fetchone()
    return row[0] or 0


def migrate(conn: psycopg.Connection, env: str) -> list[int]:
    """Apply pending migrations in one transaction. Returns the versions applied."""
    s = schema_name(env)
    applied: list[int] = []
    with conn.transaction():
        # One migrator at a time per environment.
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"leasehold-migrate:{s}",))
        # A schema made beforehand (owned by the environment's role) needs no CREATE
        # privilege on the database; CREATE SCHEMA IF NOT EXISTS would still check it.
        exists = conn.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (s,)).fetchone()
        if not exists:
            conn.execute(_q("CREATE SCHEMA {s}", s))
        conn.execute(
            _q(
                "CREATE TABLE IF NOT EXISTS {s}.schema_migrations ("
                "version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())",
                s,
            )
        )
        done = {r[0] for r in conn.execute(_q("SELECT version FROM {s}.schema_migrations", s))}
        for version, statements in MIGRATIONS:
            if version in done:
                continue
            for stmt in statements:
                conn.execute(_q(stmt, s))
            conn.execute(
                _q("INSERT INTO {s}.schema_migrations (version) VALUES (%s)", s), (version,)
            )
            applied.append(version)
    return applied


# --- Task types -------------------------------------------------------------------------------

# Settings that can change after a task type exists. hash_partitions can't: it's
# the shape of the partition table.
_SETTINGS = (
    "service",
    "key_schema",
    "recrawl_interval",
    "max_attempts",
    "retry_backoff",
    "retry_backoff_max",
    "lease_duration",
    "default_priority",
    "capabilities",
)


def _desired_row(service: str, tt: TaskType) -> dict:
    return {
        "service": service,
        "key_schema": tt.key,
        "recrawl_interval": tt.recrawl_interval,
        "max_attempts": tt.max_attempts,
        "retry_backoff": tt.retry_backoff,
        "retry_backoff_max": tt.retry_backoff_max,
        "lease_duration": tt.lease_duration,
        "default_priority": tt.default_priority,
        "capabilities": list(tt.capabilities),
        "hash_partitions": tt.hash_partitions,
    }


@dataclass
class TaskTypeChange:
    action: str  # create | update | undeclare | redeclare
    name: str
    detail: str = ""
    row: dict = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.action:<9} {self.name}" + (f"  {self.detail}" if self.detail else "")


@dataclass
class TaskTypeDiff:
    changes: list[TaskTypeChange] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.changes


def _fmt(v) -> str:
    if isinstance(v, timedelta):
        return f"{int(v.total_seconds())}s"
    return repr(v)


def diff_task_types(conn: psycopg.Connection, env: str, specs: list[ServiceSpec]) -> TaskTypeDiff:
    """Compare the task types in ``service.yml`` with the database.

    Task types that no service declares anymore are marked ``declared = false``,
    never dropped: their rows may still be needed (an import, a rename). They're
    listed as warnings until someone removes them by hand.
    """
    s = schema_name(env)
    if current_version(conn, env) < LATEST:
        raise RuntimeError(f"schema {s} is not migrated; run `leasehold frontier migrate`")

    desired = {
        name: _desired_row(spec.name, tt)
        for spec in specs
        for name, tt in spec.frontier.task_types.items()
    }
    cols = ", ".join(("name", "declared", "hash_partitions", *_SETTINGS))
    rows = conn.execute(_q(f"SELECT {cols} FROM {{s}}.task_types", s)).fetchall()
    existing = {
        r[0]: dict(zip(("declared", "hash_partitions", *_SETTINGS), r[1:], strict=True))
        for r in rows
    }

    d = TaskTypeDiff()
    for name, want in sorted(desired.items()):
        have = existing.get(name)
        if have is None:
            d.changes.append(
                TaskTypeChange("create", name, f"hash_partitions={want['hash_partitions']}", want)
            )
            continue
        if have["hash_partitions"] != want["hash_partitions"]:
            d.warnings.append(
                f"{name}: hash_partitions is {have['hash_partitions']} in the database and "
                f"{want['hash_partitions']} in service.yml; it can't change after creation"
            )
        changed = [k for k in _SETTINGS if have[k] != want[k]]
        if changed:
            detail = ", ".join(f"{k}: {_fmt(have[k])} -> {_fmt(want[k])}" for k in changed)
            d.changes.append(TaskTypeChange("update", name, detail, want))
        if not have["declared"]:
            d.changes.append(TaskTypeChange("redeclare", name))
    for name, have in sorted(existing.items()):
        if name in desired:
            continue
        if have["declared"]:
            d.changes.append(TaskTypeChange("undeclare", name, "no service.yml declares it"))
        d.warnings.append(f"{name}: not declared by any service.yml; its tasks are kept")
    return d


def apply_task_types(conn: psycopg.Connection, env: str, d: TaskTypeDiff) -> None:
    """Apply a diff in one transaction, creating partitions for new task types."""
    s = schema_name(env)
    with conn.transaction():
        for c in d.changes:
            if c.action == "create":
                _create_task_type(conn, s, c.name, c.row)
            elif c.action == "update":
                sets = sql.SQL(", ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(k)) for k in _SETTINGS
                )
                conn.execute(
                    sql.SQL(
                        "UPDATE {s}.task_types SET {sets}, updated_at = now() WHERE name = %s"
                    ).format(s=sql.Identifier(s), sets=sets),
                    [c.row[k] for k in _SETTINGS] + [c.name],
                )
            elif c.action in ("undeclare", "redeclare"):
                conn.execute(
                    _q(
                        "UPDATE {s}.task_types SET declared = %s, updated_at = now() "
                        "WHERE name = %s",
                        s,
                    ),
                    (c.action == "redeclare", c.name),
                )


def _create_task_type(conn: psycopg.Connection, s: str, name: str, row: dict) -> None:
    n = conn.execute(_q("SELECT nextval('{s}.partition_seq')", s)).fetchone()[0]
    part = f"tasks_{n}"
    cols = ("name", "partition_table", "hash_partitions", *_SETTINGS)
    conn.execute(
        sql.SQL("INSERT INTO {s}.task_types ({cols}) VALUES ({vals})").format(
            s=sql.Identifier(s),
            cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
            vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
        ),
        [name, part, row["hash_partitions"]] + [row[k] for k in _SETTINGS],
    )
    hashed = row["hash_partitions"] > 1
    conn.execute(
        sql.SQL("CREATE TABLE {s}.{p} PARTITION OF {s}.tasks FOR VALUES IN ({v}){h}").format(
            s=sql.Identifier(s),
            p=sql.Identifier(part),
            v=sql.Literal(name),
            h=sql.SQL(" PARTITION BY HASH (key)" if hashed else ""),
        )
    )
    if hashed:
        m = row["hash_partitions"]
        for i in range(m):
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {s}.{sub} PARTITION OF {s}.{p} "
                    "FOR VALUES WITH (MODULUS {m}, REMAINDER {i})"
                ).format(
                    s=sql.Identifier(s),
                    sub=sql.Identifier(f"{part}_h{i}"),
                    p=sql.Identifier(part),
                    m=sql.Literal(m),
                    i=sql.Literal(i),
                )
            )
