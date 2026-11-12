"""Compare a ``ClickHousePlan`` with a ClickHouse server and apply it, over the HTTP interface.

Per sink:

- **Target table** (holds data): created when missing. When the schema file's
  CREATE TABLE has changed, the sink is skipped with a warning: change the table by
  hand (``ALTER TABLE``), then ``--accept-table <database.table>`` records the new
  version. A table of that name that leasehold didn't create is never touched.
- **Kafka table and views** (hold no data): recreated when the SELECT, topic, group,
  Kafka address or sink password changed, or when one of them is missing. The
  consumer group's offsets stay in Kafka, so the new Kafka table continues where the
  old one stopped; messages produced in between are read afterwards.

Stale objects (marked ``leasehold:`` in a planned database, for a sink the plan no
longer has) are reported. ``prune`` drops their Kafka tables and views; target tables
are never dropped by ``apply``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

from leasehold.clickhouse.plan import MARK, ClickHousePlan, DatabasePlan, SinkPlan


class ClickHouseError(RuntimeError):
    pass


class ClickHouse:
    def __init__(self, url: str, user: str, password: str, timeout: float = 60):
        self._http = httpx.Client(base_url=url.rstrip("/"), auth=(user, password), timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def execute(self, sql: str, secret: str | None = None) -> str:
        """Run one statement. ``secret`` is blanked out of any error message."""
        try:
            r = self._http.post("/", content=sql.encode())
        except httpx.HTTPError as e:
            raise ClickHouseError(f"ClickHouse: {e}") from None
        if r.status_code != 200:
            text = r.text.strip()[:500]
            if secret:
                text = text.replace(secret, "[HIDDEN]")
            raise ClickHouseError(f"ClickHouse {r.status_code}: {text}")
        return r.text

    def rows(self, sql: str) -> list[list[str]]:
        out = self.execute(sql + " FORMAT TSV")
        return [line.split("\t") for line in out.splitlines() if line]

    def tables(self, database: str) -> dict[str, tuple[str, str]]:
        """name → (engine, comment) for every table and view in ``database``."""
        db = database.replace("'", "")
        rows = self.rows(f"SELECT name, engine, comment FROM system.tables WHERE database = '{db}'")
        return {r[0]: (r[1], r[2] if len(r) > 2 else "") for r in rows}

    def database_exists(self, database: str) -> bool:
        db = database.replace("'", "")
        return bool(self.rows(f"SELECT 1 FROM system.databases WHERE name = '{db}'"))


@dataclass
class SinkChange:
    sink: SinkPlan
    create_table: bool = False
    accept_table: bool = False
    recreate_pipeline: bool = False
    reasons: list[str] = field(default_factory=list)
    _pipeline_comment: str = ""

    def __str__(self) -> str:
        what = []
        if self.create_table:
            what.append(f"create table {self.sink.table}")
        if self.accept_table:
            what.append(f"record table version {self.sink.table}")
        if self.recreate_pipeline:
            what.append(
                f"(re)create {self.sink.kafka_table}, {self.sink.mv}, {self.sink.errors_mv}"
            )
        return "; ".join(what) + (f"  ({', '.join(self.reasons)})" if self.reasons else "")


@dataclass
class ClickHouseDiff:
    new_databases: list[DatabasePlan] = field(default_factory=list)
    errors_tables: list[DatabasePlan] = field(default_factory=list)
    changes: list[SinkChange] = field(default_factory=list)
    stale: list[tuple[str, str]] = field(default_factory=list)  # (full name, engine)
    warnings: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)  # sinks left as they are, see warnings

    @property
    def empty(self) -> bool:
        return not (self.new_databases or self.errors_tables or self.changes)


def _sink_of(comment: str) -> str | None:
    if not comment.startswith(MARK):
        return None
    for part in comment.split():
        if part.startswith("sink="):
            return part[5:]
    return None


def diff(
    ch: ClickHouse,
    plan: ClickHousePlan,
    kafka_bootstrap: str,
    credential_versions: dict[str, str],
    accept_tables: set[str] = frozenset(),  # type: ignore[assignment]
) -> ClickHouseDiff:
    """``credential_versions``: per sink principal, a value that changes with its password."""
    d = ClickHouseDiff()
    for db in plan.databases:
        exists = ch.database_exists(db.name)
        tables = ch.tables(db.name) if exists else {}
        if not exists:
            d.new_databases.append(db)
        if "sink_errors" not in tables:
            d.errors_tables.append(db)
        planned_sinks = set()
        for s in db.sinks:
            planned_sinks.add(s.sink)
            ch_change = SinkChange(s)
            name = s.table.split(".", 1)[1]
            current = tables.get(name)
            if current is None:
                ch_change.create_table = True
            else:
                engine, comment = current
                if not comment.startswith(MARK):
                    d.warnings.append(
                        f"{s.table} exists and wasn't created by leasehold; sink {s.sink} skipped"
                    )
                    d.skipped.append(s.table)
                    continue
                if comment != s.table_comment():
                    if s.table in accept_tables:
                        ch_change.accept_table = True
                        ch_change.reasons.append("table changed by hand")
                    else:
                        d.warnings.append(
                            f"{s.table}: the schema file's CREATE TABLE changed. ALTER the "
                            f"table to match, then run with --accept-table {s.table}; "
                            f"sink {s.sink} skipped"
                        )
                        d.skipped.append(s.table)
                        continue
            version = credential_versions.get(s.principal)
            if version is None:
                d.warnings.append(
                    f"no Kafka credential for {s.principal} (run `leasehold credentials apply`);"
                    f" sink {s.sink} skipped"
                )
                d.skipped.append(s.table)
                continue
            want = s.pipeline_comment(kafka_bootstrap, version)
            ch_change._pipeline_comment = want
            parts = {
                s.kafka_table: tables.get(s.kafka_table.split(".", 1)[1]),
                s.mv: tables.get(s.mv.split(".", 1)[1]),
                s.errors_mv: tables.get(s.errors_mv.split(".", 1)[1]),
            }
            missing = [n for n, t in parts.items() if t is None]
            changed = [n for n, t in parts.items() if t is not None and t[1] != want]
            if missing or changed:
                ch_change.recreate_pipeline = True
                if missing and len(missing) < 3:
                    ch_change.reasons.append("missing: " + ", ".join(missing))
                if changed:
                    ch_change.reasons.append("SELECT, topic, address or password changed")
            if ch_change.create_table or ch_change.accept_table or ch_change.recreate_pipeline:
                d.changes.append(ch_change)

        for name, (engine, comment) in sorted(tables.items()):
            sink = _sink_of(comment)
            if sink is not None and sink not in planned_sinks:
                d.stale.append((f"{db.name}.{name}", engine))
    return d


def apply(
    ch: ClickHouse,
    d: ClickHouseDiff,
    kafka_bootstrap: str,
    passwords: dict[str, str],
    prune: bool = False,
) -> None:
    """``passwords``: per sink principal."""
    for db in d.new_databases:
        ch.execute(db.create_sql())
    for db in d.errors_tables:
        ch.execute(db.errors_table_sql())
    for c in d.changes:
        s = c.sink
        if c.create_table:
            ch.execute(s.create_table_sql())
        if c.accept_table:
            ch.execute(f"ALTER TABLE {s.table} MODIFY COMMENT '{s.table_comment()}'")
        if c.recreate_pipeline:
            # Views first: dropping the Kafka table while a view reads it is refused.
            for name in (s.mv, s.errors_mv, s.kafka_table):
                ch.execute(f"DROP TABLE IF EXISTS {name} SYNC")
            password = passwords[s.principal]
            ch.execute(
                s.create_kafka_sql(kafka_bootstrap, password, c._pipeline_comment), secret=password
            )
            ch.execute(s.create_mv_sql(c._pipeline_comment))
            ch.execute(s.create_errors_mv_sql(c._pipeline_comment))
    if prune:
        # Views before Kafka tables; target tables are never dropped here.
        stale = sorted(d.stale, key=lambda x: (x[1] != "MaterializedView", x[0]))
        for name, engine in stale:
            if engine in ("MaterializedView", "Kafka"):
                ch.execute(f"DROP TABLE IF EXISTS {name} SYNC")
