"""The alert inbox without ClickHouse: webhook parsing and the alerts token."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from leasehold.api.alerts import create_statements, default_database, rows_from_webhook
from leasehold.api.auth import load_tokens
from leasehold.naming import NamingError


def test_rows_from_webhook():
    now = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)
    payload = {
        "receiver": "inbox",
        "status": "firing",
        "groupKey": "k",
        "alerts": [
            {
                "status": "firing",
                "labels": {"alertname": "dead_chunks", "env": "prod", "service": "certlog",
                           "priority": "high", "count": 3},
                "annotations": {"summary": "s", "description": "d"},
                "startsAt": "2026-11-13T11:58:30.123456789Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "fingerprint": "abc",
                "valueString": "[ value=3 ]",
            },
            {"status": "resolved", "labels": {"alertname": "x", "severity": "warning"},
             "startsAt": "2026-11-13T10:00:00+01:00", "endsAt": "2026-11-13T11:00:00Z"},
        ],
    }  # fmt: skip
    first, second = rows_from_webhook(payload, now)
    assert first["received_at"] == "2026-11-13 12:00:00.000"
    assert first["starts_at"] == "2026-11-13 11:58:30.123"
    assert first["ends_at"] is None
    assert (first["alertname"], first["env"], first["service"], first["priority"]) == (
        "dead_chunks", "prod", "certlog", "high",
    )  # fmt: skip
    assert first["labels"]["count"] == "3"
    assert (first["summary"], first["description"], first["receiver"]) == ("s", "d", "inbox")
    assert second["priority"] == "warning"
    assert second["starts_at"] == "2026-11-13 09:00:00.000"
    assert second["ends_at"] == "2026-11-13 11:00:00.000"
    json.dumps([first, second])


def test_database_names():
    assert default_database("prod") == "leasehold"
    assert default_database("dev") == "dev_leasehold"
    assert "dev_leasehold.alerts" in create_statements("dev_leasehold")[1]
    with pytest.raises(NamingError):
        create_statements("x; DROP TABLE y")


def test_alerts_token(tmp_path):
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps({"tokens": [{"sha256": "a" * 64, "alerts": "grafana"}]}))
    [entry] = load_tokens(path)
    assert entry.caller.name == "alerts:grafana"
    assert entry.caller.alerts and not entry.caller.admin
