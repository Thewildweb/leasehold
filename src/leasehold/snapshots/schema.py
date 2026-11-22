"""Schema files of snapshot tables.

Two statements, like a sink's::

    CREATE TABLE {table}
    (
        snapshot LowCardinality(String),
        nummeraanduiding_id String,
        postcode String
    )
    ENGINE = MergeTree
    PARTITION BY snapshot
    ORDER BY (postcode, nummeraanduiding_id);

    SELECT {snapshot} AS snapshot, nummeraanduiding_id, postcode FROM {source};

The table keeps one partition per snapshot: the loader attaches a new snapshot as a new
partition and drops old ones, so the engine is a MergeTree and the partition key is
``snapshot``. ``{source}`` becomes the table's files in the snapshot (Parquet, through
the dataset's named collection); ``{snapshot}`` the snapshot id as a string literal.
"""

from __future__ import annotations

import re

from leasehold.clickhouse.schema import SchemaFileError, SinkSchema, parse

SNAPSHOT = "{snapshot}"

_COLUMN = re.compile(r"(?is)\(\s*(?:.*?,\s*)?snapshot\s+LowCardinality\s*\(\s*String\s*\)")
_ENGINE = re.compile(
    r"(?is)\bENGINE\s*=\s*(Replacing|Summing|Aggregating|Collapsing|"
    r"VersionedCollapsing)?MergeTree\b"
)
_PARTITION = re.compile(
    r"(?is)\bPARTITION\s+BY\s+\(?\s*snapshot\s*\)?\s*(ORDER|PRIMARY|SETTINGS|TTL|SAMPLE|$)"
)


def parse_snapshot(sql: str) -> SinkSchema:
    s = parse(sql)
    if not _COLUMN.search(s.create_table):
        raise SchemaFileError("the table needs a column 'snapshot LowCardinality(String)'")
    if not _ENGINE.search(s.create_table):
        raise SchemaFileError("a snapshot table's engine must be a MergeTree")
    if not _PARTITION.search(s.create_table):
        raise SchemaFileError("a snapshot table is partitioned by snapshot: PARTITION BY snapshot")
    if s.select.count(SNAPSHOT) < 1:
        raise SchemaFileError(f"the SELECT must give {SNAPSHOT} AS snapshot")
    if SNAPSHOT in s.create_table:
        raise SchemaFileError(f"{SNAPSHOT} belongs in the SELECT")
    return s
