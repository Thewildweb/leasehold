"""The alert inbox end to end: Grafana webhook in, incidents out, against a real
Postgres (LEASEHOLD_TEST_POSTGRES) and ClickHouse (LEASEHOLD_TEST_CLICKHOUSE).

    LEASEHOLD_TEST_CLICKHOUSE=http://admin:dev-admin@localhost:18123
"""

from __future__ import annotations

import json
import os
import secrets
import time
from urllib.parse import urlsplit

import httpx
import pytest
from fastapi.testclient import TestClient

from leasehold.api.app import create_app
from leasehold.api.auth import Registry, hash_token, new_token
from leasehold.api.settings import Settings

from .pg import DSN, SPECS, fresh_env, requires_postgres

CH = os.environ.get("LEASEHOLD_TEST_CLICKHOUSE")

pytestmark = [
    requires_postgres,
    pytest.mark.skipif(not CH, reason="LEASEHOLD_TEST_CLICKHOUSE not set"),
]


def _ch_parts() -> tuple[str, str, str]:
    u = urlsplit(CH)
    return f"{u.scheme}://{u.hostname}:{u.port}", u.username or "default", u.password or ""


def notification(status: str, fingerprint: str, starts: str, ends: str | None = None, **labels):
    return {
        "receiver": "inbox",
        "status": status,
        "groupKey": "{}:{alertname=\"x\"}",
        "alerts": [
            {
                "status": status,
                "labels": {"alertname": "chunk_failure_rate", "env": "dev",
                           "service": "certlog", "priority": "high"} | labels,
                "annotations": {"summary": "certlog chunks failing"},
                "startsAt": starts,
                "endsAt": ends or "0001-01-01T00:00:00Z",
                "generatorURL": "http://grafana/alerting/x",
                "fingerprint": fingerprint,
                "valueString": "[ var='A' value=0.4 ]",
            }
        ],
    }  # fmt: skip


@pytest.fixture
def api(tmp_path):
    tokens = {"admin": new_token(), "grafana": new_token()}
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps(
            {
                "tokens": [
                    {"sha256": hash_token(tokens["admin"]), "admin": "erik"},
                    {"sha256": hash_token(tokens["grafana"]), "alerts": "grafana"},
                ]
            }
        )
    )
    url, user, password = _ch_parts()
    database = "it" + secrets.token_hex(4) + "_leasehold"
    with fresh_env() as env:
        settings = Settings(
            env=env,
            dsn=DSN,
            services=(),
            tokens_file=path,
            sweep_interval=3600,
            stats_interval=3600,
            clickhouse_url=url,
            clickhouse_user=user,
            clickhouse_password=password,
            alerts_database=database,
        )
        app = create_app(settings, registry=Registry(SPECS))
        try:
            with TestClient(app) as client:
                for _ in range(50):
                    if app.state.inbox.ready:
                        break
                    time.sleep(0.1)
                client.tokens = tokens
                yield client
        finally:
            httpx.post(url, auth=(user, password), content=f"DROP DATABASE IF EXISTS {database}")


def call(api, role, method, path, **kw):
    return api.request(method, path, headers={"Authorization": f"Bearer {api.tokens[role]}"}, **kw)


def test_incident_lifecycle(api):
    fp = secrets.token_hex(8)
    starts = "2026-11-13T10:00:00Z"
    for _ in range(2):  # Grafana repeats the firing notification
        r = call(api, "grafana", "POST", "/v1/alerts", json=notification("firing", fp, starts))
        assert r.status_code == 200, r.text
        assert r.json() == {"stored": 1}

    firing = call(api, "admin", "GET", "/v1/alerts", params={"status": "firing"}).json()["alerts"]
    assert [(a["fingerprint"], a["notifications"], a["ends_at"]) for a in firing] == [(fp, 2, None)]
    a = firing[0]
    assert (a["alertname"], a["env"], a["service"], a["priority"]) == (
        "chunk_failure_rate", "dev", "certlog", "high",
    )  # fmt: skip
    assert a["summary"] == "certlog chunks failing"

    resolved = notification("resolved", fp, starts, "2026-11-13T10:20:00Z")
    assert call(api, "grafana", "POST", "/v1/alerts", json=resolved).status_code == 200
    assert call(api, "admin", "GET", "/v1/alerts", params={"status": "firing"}).json() == {
        "alerts": []
    }
    [a] = call(api, "admin", "GET", "/v1/alerts", params={"service": "certlog"}).json()["alerts"]
    assert (a["status"], a["notifications"], a["ends_at"]) == (
        "resolved", 3, "2026-11-13 10:20:00.000",
    )  # fmt: skip

    # A new firing of the same rule is a new incident.
    later = notification("firing", fp, "2026-11-13T11:00:00Z")
    call(api, "grafana", "POST", "/v1/alerts", json=later)
    states = {
        (a["starts_at"], a["status"])
        for a in call(api, "admin", "GET", "/v1/alerts").json()["alerts"]
    }
    assert states == {
        ("2026-11-13 10:00:00.000", "resolved"),
        ("2026-11-13 11:00:00.000", "firing"),
    }

    events = call(api, "admin", "GET", "/v1/alerts/events", params={"fingerprint": fp}).json()
    assert [e["status"] for e in events["events"]] == ["firing", "resolved", "firing", "firing"]


def test_filters_and_permissions(api):
    starts = "2026-11-13T10:00:00Z"
    call(api, "grafana", "POST", "/v1/alerts", json=notification("firing", "a1", starts))
    other = notification("firing", "b2", starts, env="prod", service="dns", priority="page")
    call(api, "grafana", "POST", "/v1/alerts", json=other)

    get = lambda **p: [  # noqa: E731
        a["fingerprint"] for a in call(api, "admin", "GET", "/v1/alerts", params=p).json()["alerts"]
    ]
    assert sorted(get()) == ["a1", "b2"]
    assert get(env="prod") == ["b2"]
    assert get(service="certlog") == ["a1"]
    assert get(priority="page") == ["b2"]
    assert get(env="x' OR 1=1 --") == []

    # The webhook token posts alerts and does nothing else.
    for path in ("/v1/alerts", "/v1/task-types", "/v1/whoami"):
        assert call(api, "grafana", "GET", path).status_code == 403
    assert call(api, "admin", "GET", "/v1/alerts", params={"since": "1y"}).status_code == 422
    r = call(api, "grafana", "POST", "/v1/alerts", json={"alerts": "nope"})
    assert r.status_code == 422
