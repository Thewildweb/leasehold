"""ClickHouse sinks: schema files, generated SQL, and the diff against a fake server."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from leasehold import clickhouse
from leasehold.clickhouse.apply import apply, diff
from leasehold.clickhouse.schema import SchemaFileError, parse, split_statements
from leasehold.spec import SpecError, load_spec

FIXTURES = Path(__file__).parent / "fixtures"
GOOD = """
-- a comment; with a semicolon
CREATE TABLE {table}
(
    a String COMMENT 'x;y',
    b UInt8
)
ENGINE = MergeTree
ORDER BY a;

SELECT JSONExtractString(message, 'payload', 'a') AS a, 1 AS b FROM {source};
"""


def test_parse_and_split():
    s = parse(GOOD)
    assert s.create_table.startswith("CREATE TABLE {table}")
    assert "'x;y'" in s.create_table
    assert s.select.endswith("FROM {source}")
    assert split_statements("SELECT ';'; SELECT 2 /* ; */") == ["SELECT ';'", "SELECT 2"]


def test_hash_ignores_comments_and_whitespace():
    a = parse(GOOD)
    b = parse("-- other\n" + GOOD.replace("ORDER BY a;", "ORDER BY   a ;"))
    assert a.table_hash() == b.table_hash()
    assert a.select_hash() == b.select_hash()
    assert parse(GOOD.replace("b UInt8", "b UInt16")).table_hash() != a.table_hash()


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("CREATE TABLE {table} (a String) ENGINE = Log;", "expected 2 statements"),
        ("CREATE TABLE x.y (a String) ENGINE = Log; SELECT 1 FROM {source}", "first statement"),
        ("CREATE TABLE {table} (a String) ENGINE = Log; INSERT INTO t VALUES (1)", "SELECT"),
        ("CREATE TABLE {table} (a String) ENGINE = Log; SELECT 1 FROM t", "{source}"),
        (
            "CREATE TABLE {table} ON CLUSTER c (a String) ENGINE = Log; SELECT 1 FROM {source}",
            "first statement",
        ),
        (
            "CREATE TABLE {table} (a String) ENGINE = Log COMMENT 'x'; SELECT 1 FROM {source}",
            "COMMENT",
        ),
    ],
)
def test_parse_rejects(sql, message):
    with pytest.raises(SchemaFileError, match=message):
        parse(sql)


def test_spec_validation_checks_schema_files(tmp_path):
    src = FIXTURES / "certwatch"
    (tmp_path / "schema").mkdir()
    (tmp_path / "service.yml").write_text((src / "service.yml").read_text())
    (tmp_path / "schema" / "domains.sql").write_text((src / "schema" / "domains.sql").read_text())
    (tmp_path / "schema" / "chunk_attempts.sql").write_text("SELECT 1")
    for d in ("dashboards",):
        if (src / d).exists():
            (tmp_path / d).symlink_to(src / d)
    with pytest.raises(SpecError) as e:
        load_spec(tmp_path / "service.yml")
    assert any("chunk_attempts.sql" in m for m in e.value.messages)


@pytest.fixture
def chplan():
    spec = load_spec(FIXTURES / "certwatch" / "service.yml")
    return clickhouse.plan([(spec, FIXTURES / "certwatch")], "dev")


def test_plan(chplan):
    (db,) = chplan.databases
    assert db.name == "dev_certwatch"
    s = {x.sink: x for x in db.sinks}["domains"]
    assert s.topic == "dev.certwatch.entries.v1"
    assert s.group == "dev.certwatch.clickhouse.domains"
    assert s.principal == "dev.certwatch.clickhouse"
    assert s.credential_path == "leasehold/certwatch/clickhouse-kafka"
    assert s.create_table_sql().startswith("CREATE TABLE dev_certwatch.domains")
    kafka = s.create_kafka_sql("kafka:9092", "pw", "c")
    assert "kafka_format = 'JSONAsString'" in kafka
    assert "kafka_handle_error_mode = 'stream'" in kafka
    assert "kafka_group_name = 'dev.certwatch.clickhouse.domains'" in kafka
    mv = s.create_mv_sql("c")
    assert "TO dev_certwatch.domains\nCOMMENT 'c'\nAS SELECT" in mv
    assert "FROM dev_certwatch.domains_kafka WHERE length(_error) = 0)" in mv
    assert "{source}" not in mv


def test_pipeline_comment_changes_with_inputs(chplan):
    s = chplan.sinks()[0]
    base = s.pipeline_comment("kafka:9092", "5")
    assert base == s.pipeline_comment("kafka:9092", "5")
    assert base != s.pipeline_comment("kafka:9092", "6")  # password rotated
    assert base != s.pipeline_comment("other:9092", "5")


def test_consumers(chplan):
    s = chplan.sinks()[0]
    assert "kafka_num_consumers" not in s.create_kafka_sql("kafka:9092", "pw", "c")
    more = dataclasses.replace(s, consumers=8)
    kafka = more.create_kafka_sql("kafka:9092", "pw", "c")
    assert "kafka_num_consumers = '8'" in kafka
    assert "kafka_thread_per_consumer = '1'" in kafka
    assert more.pipeline_comment("k", "1") != s.pipeline_comment("k", "1")


class FakeCH:
    def __init__(self, tables=None, databases=()):
        self.tables_by_db = tables or {}
        self.dbs = set(databases) | set(self.tables_by_db)
        self.executed: list[str] = []

    def database_exists(self, db):
        return db in self.dbs

    def tables(self, db):
        return dict(self.tables_by_db.get(db, {}))

    def execute(self, sql, secret=None):
        self.executed.append(sql)
        return ""


def _live(chplan, version="5", bootstrap="kafka:9092"):
    """Tables as apply would leave them."""
    out = {"sink_errors": ("MergeTree", "leasehold: sink errors")}
    for s in chplan.sinks():
        pc = s.pipeline_comment(bootstrap, version)
        out[s.sink] = ("ReplacingMergeTree", s.table_comment())
        for name in (s.kafka_table, s.mv, s.errors_mv):
            out[name.split(".", 1)[1]] = ("Kafka" if name.endswith("_kafka") else "MV", pc)
    return {"dev_certwatch": out}


VERSIONS = {"dev.certwatch.clickhouse": "5"}


def test_diff_new(chplan):
    ch = FakeCH()
    d = diff(ch, chplan, "kafka:9092", VERSIONS)
    assert [db.name for db in d.new_databases] == ["dev_certwatch"]
    assert all(c.create_table and c.recreate_pipeline for c in d.changes)
    apply(ch, d, "kafka:9092", {"dev.certwatch.clickhouse": "pw"})
    assert ch.executed[0].startswith("CREATE DATABASE IF NOT EXISTS dev_certwatch")
    # Per sink: table, then drops, then Kafka table, then the views.
    kinds = [x.split()[0] + " " + x.split()[1] for x in ch.executed]
    assert kinds.count("CREATE MATERIALIZED") == 4


def test_diff_unchanged(chplan):
    d = diff(FakeCH(_live(chplan)), chplan, "kafka:9092", VERSIONS)
    assert d.empty and not d.warnings and not d.skipped


def test_diff_rotated_password_recreates_pipelines_only(chplan):
    d = diff(FakeCH(_live(chplan)), chplan, "kafka:9092", {"dev.certwatch.clickhouse": "6"})
    assert all(c.recreate_pipeline and not c.create_table for c in d.changes)
    assert len(d.changes) == 2


def test_diff_changed_table_is_skipped_until_accepted(chplan):
    live = _live(chplan)
    live["dev_certwatch"]["domains"] = ("ReplacingMergeTree", "leasehold: sink=domains table=old")
    d = diff(FakeCH(live), chplan, "kafka:9092", VERSIONS)
    assert d.skipped == ["dev_certwatch.domains"]
    assert any("--accept-table dev_certwatch.domains" in w for w in d.warnings)
    d = diff(FakeCH(live), chplan, "kafka:9092", VERSIONS, {"dev_certwatch.domains"})
    assert [c.accept_table for c in d.changes] == [True]


def test_diff_foreign_table_is_never_touched(chplan):
    live = _live(chplan)
    live["dev_certwatch"]["domains"] = ("MergeTree", "")
    d = diff(FakeCH(live), chplan, "kafka:9092", VERSIONS)
    assert d.skipped == ["dev_certwatch.domains"]


def test_diff_missing_credential_skips(chplan):
    d = diff(FakeCH(), chplan, "kafka:9092", {})
    assert sorted(d.skipped) == ["dev_certwatch.chunk_attempts", "dev_certwatch.domains"]


def test_stale_and_prune(chplan):
    live = _live(chplan)
    live["dev_certwatch"].update(
        {
            "old": ("MergeTree", "leasehold: sink=old table=x"),
            "old_kafka": ("Kafka", "leasehold: sink=old pipeline=x"),
            "old_mv": ("MaterializedView", "leasehold: sink=old pipeline=x"),
            "unrelated": ("MergeTree", ""),
        }
    )
    ch = FakeCH(live)
    d = diff(ch, chplan, "kafka:9092", VERSIONS)
    assert sorted(n for n, _ in d.stale) == [
        "dev_certwatch.old",
        "dev_certwatch.old_kafka",
        "dev_certwatch.old_mv",
    ]
    apply(ch, d, "kafka:9092", {}, prune=True)
    assert ch.executed == [
        "DROP TABLE IF EXISTS dev_certwatch.old_mv SYNC",
        "DROP TABLE IF EXISTS dev_certwatch.old_kafka SYNC",
    ]
