"""The control API client, against a mocked transport."""

from __future__ import annotations

import json
import uuid

import httpx
import pytest

from leasehold import client as lh_client
from leasehold.client import (
    AckItem,
    ControlClient,
    FailItem,
    Forbidden,
    LeaseRef,
    NewTask,
    NotFound,
    Unauthorized,
    Unavailable,
)

REF = LeaseRef("jobs.once", "a", uuid.uuid4())


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(lh_client.time, "sleep", lambda s: None)


def make(handler, **kw) -> ControlClient:
    return ControlClient(
        "http://api", token=kw.pop("token", "t"), transport=httpx.MockTransport(handler), **kw
    )


def lease_body(n=1):
    return {
        "tasks": [
            {
                "task_type": "jobs.once",
                "key": f"k{i}",
                "lease_id": str(uuid.uuid4()),
                "lease_expires_at": "2026-11-11T12:00:00.5Z",
                "priority": 50,
                "attempts": 0,
                "payload": {"x": i},
                "country": None,
            }
            for i in range(n)
        ]
    }


def test_lease_parses_tasks_and_sends_auth():
    seen = {}

    def handler(req):
        seen["auth"] = req.headers["authorization"]
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json=lease_body(2))

    tasks = make(handler).lease(2, task_types=["jobs.once"], worker_id="w1")
    assert seen["auth"] == "Bearer t"
    assert seen["body"] == {"max": 2, "task_types": ["jobs.once"], "worker_id": "w1"}
    assert [t.key for t in tasks] == ["k0", "k1"]
    assert tasks[0].payload == {"x": 0} and tasks[0].lease_expires_at.tzinfo is not None


def test_identity_file_is_read_per_request(tmp_path):
    f = tmp_path / "nomad_leasehold.jwt"
    f.write_text("jwt-1\n")
    seen = []

    def handler(req):
        seen.append(req.headers["authorization"])
        return httpx.Response(200, json={"name": "x"})

    c = ControlClient("http://api", identity_file=f, transport=httpx.MockTransport(handler))
    c.whoami()
    f.write_text("jwt-2")  # Nomad rotated it
    c.whoami()
    assert seen == ["Bearer jwt-1", "Bearer jwt-2"]


def test_from_env_prefers_token_then_nomad_identity(tmp_path):
    c = ControlClient.from_env({"LEASEHOLD_API_URL": "http://a", "LEASEHOLD_TOKEN": "x"})
    assert c.token == "x"
    c = ControlClient.from_env({"LEASEHOLD_API_URL": "http://a", "NOMAD_SECRETS_DIR": "/s"})
    assert str(c.identity_file) == "/s/nomad_leasehold.jwt"
    with pytest.raises(ValueError):
        ControlClient.from_env({})


@pytest.mark.parametrize(
    ("status", "exc"), [(401, Unauthorized), (403, Forbidden), (404, NotFound)]
)
def test_errors_are_mapped_and_not_retried(status, exc):
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(status, json={"detail": "no"})

    with pytest.raises(exc) as e:
        make(handler).ack([AckItem(REF, uuid.uuid4())])
    assert e.value.detail == "no" and len(calls) == 1


def test_connect_errors_are_retried_for_every_call():
    calls = []

    def handler(req):
        calls.append(1)
        if len(calls) < 3:
            raise httpx.ConnectError("refused")
        return httpx.Response(200, json=lease_body(1))

    assert len(make(handler).lease(1)) == 1
    assert len(calls) == 3


def test_lease_is_not_retried_after_the_request_was_sent():
    calls = []

    def handler(req):
        calls.append(1)
        raise httpx.ReadTimeout("slow")

    with pytest.raises(Unavailable):
        make(handler).lease(1)
    assert len(calls) == 1


def test_ack_is_retried_on_read_timeout_and_503():
    calls = []

    def handler(req):
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ReadTimeout("slow")
        if len(calls) == 2:
            return httpx.Response(503, json={"detail": "busy"})
        return httpx.Response(200, json={"applied": 1, "unknown": []})

    assert make(handler).ack([AckItem(REF, uuid.uuid4())]).applied == 1
    assert len(calls) == 3


def test_gives_up_after_retries():
    def handler(req):
        raise httpx.ConnectError("refused")

    with pytest.raises(Unavailable):
        make(handler, retries=2).renew([REF])


def test_upsert_and_ack_are_chunked():
    sizes = []

    def handler(req):
        body = json.loads(req.content)
        if req.url.path == "/v1/tasks/upsert":
            sizes.append(len(body["tasks"]))
            return httpx.Response(200, json={"inserted": len(body["tasks"]), "updated": 0})
        sizes.append(len(body["tasks"]))
        return httpx.Response(200, json={"applied": len(body["tasks"]), "unknown": []})

    c = make(handler)
    assert c.upsert("jobs.once", [NewTask(str(i)) for i in range(12001)]) == (12001, 0)
    assert sizes == [5000, 5000, 2001]
    sizes.clear()
    assert c.ack([AckItem(REF, uuid.uuid4())] * 2500).applied == 2500
    assert sizes == [1000, 1000, 500]


def test_item_serialisation():
    from datetime import UTC, datetime, timedelta

    assert NewTask("a").json() == {"key": "a"}
    due = datetime(2026, 11, 11, tzinfo=UTC)
    assert NewTask("a", 80, due, {"p": 1}, "NL").json() == {
        "key": "a",
        "priority": 80,
        "due_at": due.isoformat(),
        "payload": {"p": 1},
        "country": "NL",
    }
    f = FailItem(REF, "x" * 2000, retry_after=timedelta(seconds=0.2), permanent=True).json()
    assert len(f["reason"]) == 1000 and f["retry_after_s"] == 1.0 and f["permanent"] is True
