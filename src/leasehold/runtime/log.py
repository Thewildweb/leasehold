"""JSON logs to stdout, one object per line, stamped with who and what.

Every line carries ``service``, ``component``, ``env``, ``version`` and
``instance``; inside a task or message also ``task_type``, ``task_key`` and
``event_id``, so a log line joins to its Kafka event and its task. Vector ships
stdout to ClickHouse ``logs.app_logs``.

Extra fields: ``log.info("fetched", extra={"fields": {"entries": 256}})``.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import time
from contextlib import contextmanager

_scope: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "leasehold_log_scope", default=None
)


@contextmanager
def scope(**fields):
    """Add fields to every log line inside the block (per thread / task)."""
    token = _scope.set((_scope.get() or {}) | {k: v for k, v in fields.items() if v is not None})
    try:
        yield
    finally:
        _scope.reset(token)


class JsonFormatter(logging.Formatter):
    def __init__(self, static: dict):
        super().__init__()
        self.static = static

    def format(self, record: logging.LogRecord) -> str:
        line = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "message": record.getMessage(),
            **self.static,
            **(_scope.get() or {}),
        }
        if record.name != "leasehold":
            line["logger"] = record.name
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            line.update(fields)
        if record.exc_info:
            line["error"] = self.formatException(record.exc_info)
        return json.dumps(line, default=str)


def setup(
    service: str, component: str, env: str, version: str, instance: str, level: int = logging.INFO
) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter(
            {
                "service": service,
                "component": component,
                "env": env,
                "version": version,
                "instance": instance,
            }
        )
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # Libraries that are chatty at INFO.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("leasehold")
