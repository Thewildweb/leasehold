"""The Parquet layout of an archive sink: which columns, and one event as one row.

Columns, in order:

- the envelope: ``event_id``, ``schema_version``, ``service``, ``component``, ``env``,
  ``worker``, ``task_type``, ``task_key``, ``target``, ``egress_ip``, ``ts`` (UTC, ms);
- ``payload_ref``: the claim-check reference as JSON text, null for inline payloads. The
  sink resolves claims before writing, so the payload columns are filled either way;
- the Kafka coordinates: ``_partition``, ``_offset``, ``_kafka_ts``;
- the sink's declared ``columns`` (payload fields lifted into typed columns);
- ``payload``: every other payload field as JSON text (without the lifted and dropped
  ones), so a field added to the model later is kept without a schema change.

Every column is nullable. A field that is missing, or whose value doesn't convert to its
column's type, is null in that column; a value that doesn't convert stays in ``payload``,
so nothing is lost when a model changes.

This module doesn't import pyarrow at import time: ``service.yml`` validation uses
``FIXED_COLUMNS`` without the ``archive`` extra.
"""

from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

    from leasehold.spec.models import ArchiveSink, Column

ENVELOPE_STRINGS = (
    "event_id",
    "service",
    "component",
    "env",
    "worker",
    "task_type",
    "task_key",
    "target",
    "egress_ip",
)
FIXED_COLUMNS = (
    "event_id",
    "schema_version",
    "service",
    "component",
    "env",
    "worker",
    "task_type",
    "task_key",
    "target",
    "egress_ip",
    "ts",
    "payload_ref",
    "_partition",
    "_offset",
    "_kafka_ts",
    "payload",
)

_INT32 = (-(2**31), 2**31 - 1)
_INT64 = (-(2**63), 2**63 - 1)


def arrow_schema(sink: ArchiveSink) -> pa.Schema:
    import pyarrow as pa

    ts = pa.timestamp("ms", tz="UTC")
    types = {
        "string": pa.string(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "float64": pa.float64(),
        "bool": pa.bool_(),
        "timestamp": ts,
        "json": pa.string(),
        "base64": pa.binary(),
    }
    fields = [pa.field(n, pa.string()) for n in ENVELOPE_STRINGS[:1]]
    fields.append(pa.field("schema_version", pa.int32()))
    fields += [pa.field(n, pa.string()) for n in ENVELOPE_STRINGS[1:]]
    fields += [
        pa.field("ts", ts),
        pa.field("payload_ref", pa.string()),
        pa.field("_partition", pa.int32()),
        pa.field("_offset", pa.int64()),
        pa.field("_kafka_ts", ts),
    ]
    for name, c in sink.column_specs().items():
        fields.append(pa.field(name, types[c.type]))
        if c.text_fallback:
            fields.append(pa.field(f"{name}_from_text", pa.bool_()))
    fields.append(pa.field("payload", pa.string()))
    return pa.schema(fields)


class _Bad(Exception):
    """A value that doesn't fit its column."""


def parse_ts(value: Any) -> datetime | None:
    """An ISO 8601 string (``Z`` or an offset; naive means UTC) or epoch seconds."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise _Bad
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value, UTC)
    if isinstance(value, str):
        try:
            d = datetime.fromisoformat(value)
        except ValueError:
            raise _Bad from None
        return d.replace(tzinfo=UTC) if d.tzinfo is None else d.astimezone(UTC)
    raise _Bad


def _convert(c: Column, value: Any) -> Any:
    if value is None:
        return None
    t = c.type
    if t == "string":
        if isinstance(value, str):
            return value
        if isinstance(value, bool | int | float):
            return json.dumps(value)
        raise _Bad
    if t in ("int32", "int64"):
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise _Bad
        if isinstance(value, float) and not value.is_integer():
            raise _Bad
        lo, hi = _INT32 if t == "int32" else _INT64
        if not lo <= int(value) <= hi:
            raise _Bad
        return int(value)
    if t == "float64":
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise _Bad
        return float(value)
    if t == "bool":
        if not isinstance(value, bool):
            raise _Bad
        return value
    if t == "timestamp":
        return parse_ts(value)
    if t == "json":
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    if t == "base64":
        if not isinstance(value, str):
            raise _Bad
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            raise _Bad from None
    raise AssertionError(t)


def to_row(
    sink: ArchiveSink,
    envelope: dict[str, Any],
    payload: Any,
    *,
    partition: int,
    offset: int,
    kafka_ts_ms: int | None,
) -> dict[str, Any]:
    """One event as one row. ``payload`` is the resolved payload (inline or from a claim)."""
    row: dict[str, Any] = {}
    for n in ENVELOPE_STRINGS:
        v = envelope.get(n)
        row[n] = None if v is None else str(v)
    sv = envelope.get("schema_version")
    row["schema_version"] = sv if isinstance(sv, int) and not isinstance(sv, bool) else None
    try:
        row["ts"] = parse_ts(envelope.get("ts"))
    except _Bad:
        row["ts"] = None
    ref = envelope.get("payload_ref")
    row["payload_ref"] = None if ref is None else json.dumps(ref, separators=(",", ":"))
    row["_partition"] = partition
    row["_offset"] = offset
    row["_kafka_ts"] = (
        datetime.fromtimestamp(kafka_ts_ms / 1000, UTC) if kafka_ts_ms is not None else None
    )

    rest = dict(payload) if isinstance(payload, dict) else {}
    for f in sink.drop:
        rest.pop(f, None)
    for name, c in sink.column_specs().items():
        assert c.field is not None
        value = rest.get(c.field)
        from_text = None
        try:
            converted = _convert(c, value)
            rest.pop(c.field, None)
        except _Bad:
            converted = None  # kept in ``payload``
        if c.text_fallback:
            text = rest.pop(c.text_fallback, None)
            if not converted and isinstance(text, str) and text:
                converted, from_text = text.encode("utf-8"), True
            elif converted is not None:
                from_text = False
        row[name] = converted
        if c.text_fallback:
            row[f"{name}_from_text"] = from_text
    if isinstance(payload, dict):
        row["payload"] = json.dumps(rest, separators=(",", ":"), ensure_ascii=False)
    else:
        # Not an object: the whole payload as JSON (null stays null).
        row["payload"] = None if payload is None else json.dumps(payload, separators=(",", ":"))
    return row


def row_size(row: dict[str, Any]) -> int:
    """Rough in-memory size of a row, for cutting row groups and files."""
    n = 64
    for v in row.values():
        if isinstance(v, str | bytes):
            n += len(v)
        else:
            n += 8
    return n
