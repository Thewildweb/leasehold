"""Sink schema files: the target table and the SELECT that fills it.

A schema file holds exactly two statements::

    CREATE TABLE {table}
    (
        event_id UUID,
        log_name LowCardinality(String),
        attempted_at DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree
    ORDER BY (log_name, event_id);

    SELECT
        toUUID(JSONExtractString(message, 'event_id')) AS event_id,
        JSONExtractString(message, 'payload', 'log_name') AS log_name,
        parseDateTime64BestEffort(JSONExtractString(message, 'ts'), 3, 'UTC') AS attempted_at
    FROM {source};

``{table}`` becomes the sink's table (``<database>.<sink>``). ``{source}`` becomes the
sink's Kafka messages: one row per message with ``message`` (the event as a JSON
string) and Kafka's ``_topic``, ``_partition``, ``_offset`` and ``_timestamp``. Messages
that aren't valid JSON never reach the SELECT; they go to ``<database>.sink_errors``.

The SELECT may filter (``WHERE``) and unnest (``ARRAY JOIN``); its columns must match
the table's. Everything else (the Kafka table, the materialized view, credentials)
is generated.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

TABLE = "{table}"
SOURCE = "{source}"


class SchemaFileError(ValueError):
    pass


@dataclass(frozen=True)
class SinkSchema:
    create_table: str  # with {table}
    select: str  # with {source}

    def table_hash(self) -> str:
        return _hash(self.create_table)

    def select_hash(self) -> str:
        return _hash(self.select)


def _hash(text: str) -> str:
    normalized = re.sub(r"\s+", " ", _strip_comments(text)).strip()
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def _strip_comments(sql: str) -> str:
    out, i, quote = [], 0, None
    while i < len(sql):
        ch = sql[i]
        if quote:
            out.append(ch)
            if ch == "\\" and i + 1 < len(sql):
                out.append(sql[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            out.append(ch)
        elif sql.startswith("--", i):
            while i < len(sql) and sql[i] != "\n":
                i += 1
            continue
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            i = len(sql) if end < 0 else end + 2
            continue
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def split_statements(sql: str) -> list[str]:
    """Statements separated by ``;`` outside quotes, without comments."""
    text = _strip_comments(sql)
    parts, buf, quote, i = [], [], None, 0
    while i < len(text):
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == "\\" and i + 1 < len(text):
                buf.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif ch == ";":
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def parse(sql: str) -> SinkSchema:
    stmts = split_statements(sql)
    if len(stmts) != 2:
        raise SchemaFileError(
            f"expected 2 statements (CREATE TABLE {TABLE} …; SELECT … FROM {SOURCE};), "
            f"found {len(stmts)}"
        )
    create, select = stmts
    if not re.match(r"(?is)^CREATE\s+TABLE\s+\{table\}\s*\(", create):
        raise SchemaFileError(f"the first statement must start with 'CREATE TABLE {TABLE} ('")
    if create.count(TABLE) != 1:
        raise SchemaFileError(f"{TABLE} must appear once, in CREATE TABLE")
    if re.search(r"(?is)\bON\s+CLUSTER\b", create):
        raise SchemaFileError("leave out ON CLUSTER")
    if re.search(r"(?is)\bCOMMENT\s+'(?:[^'\\]|\\.)*'\s*$", create):
        raise SchemaFileError("leave out the table COMMENT; apply sets it")
    if not re.match(r"(?is)^(SELECT|WITH)\b", select):
        raise SchemaFileError("the second statement must be a SELECT")
    if select.count(SOURCE) != 1 or TABLE in select:
        raise SchemaFileError(f"the SELECT must read FROM {SOURCE} (once) and not use {TABLE}")
    if SOURCE in create:
        raise SchemaFileError(f"{SOURCE} belongs in the SELECT")
    return SinkSchema(create_table=create, select=select)
