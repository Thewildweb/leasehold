"""Shared field types for the spec: durations, priorities and validated names."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Annotated, Literal

from pydantic import BeforeValidator, PlainSerializer, StringConstraints, WithJsonSchema

from leasehold import naming

# --- Durations: "30s", "15m", "24h", "7d", "2w" -------------------------------------------

_DURATION_RE = re.compile(r"^(\d+)(s|m|h|d|w)$")
_UNITS = {"w": 604800, "d": 86400, "h": 3600, "m": 60, "s": 1}


def parse_duration(value: object) -> timedelta:
    if isinstance(value, timedelta):
        return value
    # A bare 0 is accepted (used as `after: 0` in escalations). Any other bare
    # number is rejected because its unit would be ambiguous.
    if value == 0 and not isinstance(value, bool):
        return timedelta(0)
    if isinstance(value, str):
        m = _DURATION_RE.match(value.strip())
        if m:
            return timedelta(seconds=int(m[1]) * _UNITS[m[2]])
    raise ValueError("expected a duration like '30s', '15m', '24h', '7d' or '2w'")


def format_duration(value: timedelta) -> str:
    seconds = int(value.total_seconds())
    if seconds == 0:
        return "0s"
    for unit, size in _UNITS.items():
        if seconds % size == 0:
            return f"{seconds // size}{unit}"
    raise AssertionError("unreachable")


Duration = Annotated[
    timedelta,
    BeforeValidator(parse_duration),
    PlainSerializer(format_duration, return_type=str),
    WithJsonSchema(
        {
            "anyOf": [
                {"type": "string", "pattern": r"^\d+(s|m|h|d|w)$"},
                {"const": 0},
            ],
            "description": "Duration: '30s', '15m', '24h', '7d', '2w' (or 0)",
        }
    ),
]

# --- Alert priorities ------------------------------------------------------------------------

Priority = Literal["info", "warning", "high", "page"]
PRIORITY_ORDER: dict[str, int] = {"info": 0, "warning": 1, "high": 2, "page": 3}

# --- Names -----------------------------------------------------------------------------------

ServiceName = Annotated[str, StringConstraints(pattern=naming.SERVICE_RE.pattern, max_length=40)]
ComponentName = Annotated[str, StringConstraints(pattern=naming.SERVICE_RE.pattern, max_length=30)]
RuleName = Annotated[str, StringConstraints(pattern=naming.SERVICE_RE.pattern, max_length=60)]
TopicName = Annotated[str, StringConstraints(pattern=naming.TOPIC_NAME_RE.pattern, max_length=60)]
TopicRefStr = Annotated[str, StringConstraints(pattern=naming.TOPIC_REF_RE.pattern)]
TaskTypeName = Annotated[str, StringConstraints(pattern=naming.TASK_TYPE_RE.pattern, max_length=80)]
SnakeName = Annotated[str, StringConstraints(pattern=naming.SNAKE_RE.pattern, max_length=60)]
Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_-]*$", max_length=60)]
SecretPath = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9/_.-]*$", max_length=120)]
EnvVarName = Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9_]*$", max_length=80)]
