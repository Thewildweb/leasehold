"""The egress agent against a fake control API."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi.testclient import TestClient

from leasehold.agent import server
from leasehold.agent.server import AgentSettings, create_app


class FakeAPI:
    """Records calls; answers lease with one target lease and two tasks."""

    def __init__(self):
        self.calls: list[tuple[str, dict, dict]] = []
        self.lease_id = str(uuid.uuid4())
        self.spend_granted = None  # None: grant what's asked
        self.lost: list[dict] = []
        self.down = False
        self.not_before: str | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            raise httpx.ConnectError("down")
        body = json.loads(request.content) if request.content else None
        self.calls.append((request.url.path, body, dict(request.headers)))
        now = datetime.now(UTC)
        if request.url.path == "/v1/tasks/lease":
            exp = (now + timedelta(seconds=60)).isoformat()
            return httpx.Response(200, json={
                "tasks": [
                    {"task_type": "sites.fetch", "key": k, "lease_id": str(uuid.uuid4()),
                     "lease_expires_at": exp, "priority": 50, "attempts": 0,
                     "target": "192.0.2.9", "target_lease_id": self.lease_id}
                    for k in ("a.example", "b.example")
                ],
                "targets": [{"target": "192.0.2.9", "lease_id": self.lease_id,
                             "expires_at": exp, "interval_s": 0.3,
                             "provider": "example-cdn", "cap": 10, "granted": 2,
                             "not_before": self.not_before}],
            })  # fmt: skip
        if request.url.path == "/v1/targets/spend":
            n = body["n"] if self.spend_granted is None else self.spend_granted
            return httpx.Response(200, json={"granted": n})
        if request.url.path == "/v1/targets/renew":
            exp = (now + timedelta(seconds=60)).isoformat()
            return httpx.Response(200, json={
                "renewed": [{"lease_id": i, "expires_at": exp} for i in body["leases"]
                            if i not in {x["lease_id"] for x in self.lost}],
                "lost": self.lost,
            })  # fmt: skip
        return httpx.Response(200, json={"applied": len((body or {}).get("tasks", [])),
                                         "unknown": []})  # fmt: skip


@pytest.fixture
def agent(monkeypatch):
    monkeypatch.setattr(server, "RENEW_EVERY_S", 3600)  # maintenance by hand
    api = FakeAPI()
    settings = AgentSettings("http://api", "agent-token", "192.0.2.1", "/unused")
    app = create_app(settings, transport=httpx.MockTransport(api.handler))
    with TestClient(app) as client:
        client.api = api
        client.state = app.state.agent
        yield client


def lease(agent):
    r = agent.post("/v1/tasks/lease", json={"max": 5, "egress": "203.0.113.1"},
                   headers={"Authorization": "Bearer worker-jwt"})  # fmt: skip
    assert r.status_code == 200
    return r.json()


def token(agent, **kw):
    body = {"target_lease_id": agent.api.lease_id, "task_type": "sites.fetch", **kw}
    return agent.post("/v1/agent/token", json=body).json()


def test_lease_goes_out_as_the_agent_for_the_worker(agent):
    lease(agent)
    path, body, headers = agent.api.calls[-1]
    assert path == "/v1/tasks/lease"
    assert body["egress"] == "192.0.2.1"  # the machine's, not what the worker sent
    assert headers["authorization"] == "Bearer agent-token"
    assert headers["x-leasehold-for"] == "Bearer worker-jwt"
    (t,) = agent.state.targets.values()
    assert set(t.tasks) == {("sites.fetch", "a.example"), ("sites.fetch", "b.example")}


def test_tokens_are_paced_per_target(agent):
    lease(agent)
    assert token(agent, first=True)["granted"] is True
    r = token(agent, first=True)
    assert r["granted"] is False and 0 < r["wait_s"] <= 0.3
    time.sleep(0.31)
    assert token(agent, first=True)["granted"] is True
    # A task's first request costs no budget; later ones spend.
    assert [c[0] for c in agent.api.calls].count("/v1/targets/spend") == 0
    time.sleep(0.31)
    assert token(agent, cost=3)["granted"] is True
    spend = [c for c in agent.api.calls if c[0] == "/v1/targets/spend"]
    assert spend[-1][1] == {"egress": "192.0.2.1", "provider": "example-cdn",
                            "task_type": "sites.fetch", "n": 3}  # fmt: skip


def test_no_budget_no_token(agent):
    lease(agent)
    agent.api.spend_granted = 0
    r = token(agent, cost=1)
    assert r["granted"] is False and r["reason"] == "no budget left"


def test_unknown_and_lost_targets(agent):
    r = agent.post("/v1/agent/token", json={"target_lease_id": str(uuid.uuid4()),
                                            "task_type": "sites.fetch"})  # fmt: skip
    assert r.json() == {"granted": False, "lost": "unknown target lease"}
    lease(agent)
    # Pushback in an outcome stops the target at once.
    agent.post("/v1/tasks/fail", json={"tasks": [
        {"task_type": "sites.fetch", "key": "a.example", "lease_id": str(uuid.uuid4()),
         "reason": "429", "cooldown": True, "retry_after_s": 60}]})  # fmt: skip
    assert agent.api.calls[-1][1]["egress"] == "192.0.2.1"
    assert token(agent, first=True) == {"granted": False, "lost": "pushback"}


def test_maintain_renews_releases_and_learns_lost(agent):
    lease(agent)
    t = agent.state.targets[uuid.UUID(agent.api.lease_id)]
    t.expires = datetime.now(UTC) + timedelta(seconds=10)  # due for renewal
    asyncio.run(agent.state.maintain())
    _, body, _ = agent.api.calls[-1]
    assert body["leases"] == [agent.api.lease_id] and body["release"] == []
    assert t.expires > datetime.now(UTC) + timedelta(seconds=50)

    agent.api.lost = [{"lease_id": agent.api.lease_id, "reason": "revoked: operator"}]
    t.expires = datetime.now(UTC) + timedelta(seconds=10)
    asyncio.run(agent.state.maintain())
    assert token(agent, first=True) == {"granted": False, "lost": "revoked: operator"}


def test_extra_token_checks_the_target_lease(agent):
    lease(agent)
    t = agent.state.targets[uuid.UUID(agent.api.lease_id)]
    renews = lambda: [c for c in agent.api.calls if c[0] == "/v1/targets/renew"]  # noqa: E731
    assert token(agent, first=True)["granted"] is True
    assert renews() == []  # the lease call covers the first request
    t.next_at = 0
    assert token(agent)["granted"] is True
    assert renews()[-1][1]["leases"] == [agent.api.lease_id]
    # Revoked: the next extra token is refused, and no budget is spent on it.
    agent.api.lost = [{"lease_id": agent.api.lease_id, "reason": "revoked: operator"}]
    t.next_at = 0
    spent = [c[0] for c in agent.api.calls].count("/v1/targets/spend")
    assert token(agent) == {"granted": False, "lost": "revoked: operator"}
    assert [c[0] for c in agent.api.calls].count("/v1/targets/spend") == spent


def test_extra_token_while_the_api_is_down(agent):
    """The target goes on until its lease ends (a token without budget needs no spend)."""
    lease(agent)
    agent.api.down = True
    t = agent.state.targets[uuid.UUID(agent.api.lease_id)]
    assert token(agent, cost=0)["granted"] is True
    t.next_at = t.stop_at = 0
    assert token(agent, cost=0) == {"granted": False, "lost": "target lease ends"}


def test_done_tasks_give_the_target_back(agent):
    lease(agent)
    agent.post("/v1/tasks/ack", json={"tasks": [
        {"task_type": "sites.fetch", "key": k, "lease_id": str(uuid.uuid4()),
         "event_id": str(uuid.uuid4())} for k in ("a.example", "b.example")]})  # fmt: skip
    asyncio.run(agent.state.maintain())
    _, body, _ = agent.api.calls[-1]
    assert body["release"] == [agent.api.lease_id]
    assert agent.state.targets == {}


def test_next_lease_on_a_target_waits_one_interval(agent):
    """Tasks done, the target lease given back, the target leased again: the first token
    waits for the new lease's not_before (one interval after the last lease ended)."""
    lease(agent)
    assert token(agent, first=True)["granted"] is True
    agent.post("/v1/tasks/ack", json={"tasks": [
        {"task_type": "sites.fetch", "key": k, "lease_id": str(uuid.uuid4()),
         "event_id": str(uuid.uuid4())} for k in ("a.example", "b.example")]})  # fmt: skip
    asyncio.run(agent.state.maintain())
    assert agent.state.targets == {}
    released = datetime.now(UTC)
    agent.api.lease_id = str(uuid.uuid4())
    agent.api.not_before = (released + timedelta(seconds=0.3)).isoformat()
    lease(agent)
    r = token(agent, first=True)
    assert r["granted"] is False and 0.2 < r["wait_s"] <= 0.3
    time.sleep(0.31)
    assert token(agent, first=True)["granted"] is True
    # A not_before in the past: no wait.
    agent.api.lease_id = str(uuid.uuid4())
    agent.api.not_before = (released - timedelta(seconds=60)).isoformat()
    lease(agent)
    assert token(agent, first=True)["granted"] is True


def test_api_down(agent):
    agent.api.down = True
    r = agent.post("/v1/tasks/lease", json={"max": 1}, headers={"Authorization": "Bearer w"})
    assert r.status_code == 503


def test_routed_targets(agent):
    """A route passes through to the worker; extra tokens spend on the route's egress; a
    renewal's new route reaches the worker with its next token."""
    route = {"egress": "pool:resi", "account": "resi-a", "credentials_fp": "fp1",
             "proxy": {"scheme": "http", "host": "gw.example", "port": 9000,
                       "username": "u", "password": "p1"}}  # fmt: skip
    orig = agent.api.handler

    def handler(request):
        r = orig(request)
        if request.url.path == "/v1/tasks/lease":
            data = json.loads(r.content)
            data["targets"][0]["route"] = route
            return httpx.Response(200, json=data)
        if request.url.path == "/v1/targets/renew":
            data = json.loads(r.content)
            data["routes"] = [{"lease_id": agent.api.lease_id,
                               "route": route | {"credentials_fp": "fp2"}}]  # fmt: skip
            return httpx.Response(200, json=data)
        return r

    agent.api.handler = handler
    agent.state.api._transport = httpx.MockTransport(handler)
    body = lease(agent)
    assert body["targets"][0]["route"]["proxy"]["password"] == "p1"
    assert token(agent, first=True, credentials_fp="fp1") == {"granted": True, "interval_s": 0.3}
    t = agent.state.targets[uuid.UUID(agent.api.lease_id)]
    t.expires = datetime.now(UTC) + timedelta(seconds=10)
    asyncio.run(agent.state.maintain())
    t.next_at = 0
    r = token(agent, cost=2, credentials_fp="fp1")
    assert r["granted"] and r["route"]["credentials_fp"] == "fp2"
    spend = [c for c in agent.api.calls if c[0] == "/v1/targets/spend"]
    assert spend[-1][1]["egress"] == "pool:resi"
