"""The egress agent.

One per machine and environment (a Nomad system job), listening on a Unix socket in a
host volume that the target-leasing workers on the machine mount. Workers set
``LEASEHOLD_AGENT_SOCKET`` and use it instead of the control API
(``ControlClient.from_env``); they never talk to the control API themselves.

What it does:

- **Forwards the worker API** (``/v1/tasks/*``, ``/v1/task-types``, ``/v1/whoami``,
  ``/v1/egress/providers``) to the control API with its own agent token and the
  worker's credential in ``X-Leasehold-For``: the API checks the worker's permissions
  and records the worker on its tasks, so events and logs still show the service.
- **Holds the target leases** of the machine: leases go out with the machine's egress
  and the agent as holder, so several workers and services on one machine share them.
  It renews them while they have tasks and gives them back when they have none.
- **Hands out tokens** (``POST /v1/agent/token``): leave to send one request to a
  target. Per target, one token per interval of its lease (across all workers on the
  machine), none after a safety margin before the lease expires or once it is lost
  (revoked, cooling down, not renewed). A task's first request was paid for when the
  task was granted; later requests in the same task (a retry, a fallback, a browser
  page load costing N) spend tokens from the egress's allocation
  (``POST /v1/targets/spend``), including what a higher class lends in the window.
- **Fails closed**: without the control API it stops handing out tokens once leases
  reach their stop time.
- **Proxy routes**: a target on a proxy tier comes with a route (leasehold.egress.proxies);
  the lease response passes through to the worker unchanged. Its extra tokens are spent
  on the route's egress (the exit or pool), and when a renewal brings a new route (the
  account's credentials changed), the next token answer carries it to the worker.

Settings (environment):

    LEASEHOLD_API_URL        the environment's control API
    LEASEHOLD_AGENT_TOKEN    this agent's token (``{"agent": ...}`` in the tokens file)
    LEASEHOLD_EGRESS_IP      the machine's egress address
    LEASEHOLD_AGENT_SOCKET   the socket to listen on
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

log = logging.getLogger("leasehold.agent")

MARGIN_S = 5.0  # no token this close to a target lease's expiry
RENEW_EVERY_S = 2.0
FORWARD = (
    ("POST", "/v1/tasks/upsert"),
    ("GET", "/v1/task-types"),
    ("GET", "/v1/whoami"),
    ("GET", "/v1/egress/providers"),
)


@dataclass
class AgentSettings:
    api_url: str
    token: str
    egress: str
    socket: str

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> AgentSettings:
        e = os.environ if environ is None else environ
        missing = [
            k
            for k in (
                "LEASEHOLD_API_URL",
                "LEASEHOLD_AGENT_TOKEN",
                "LEASEHOLD_EGRESS_IP",
                "LEASEHOLD_AGENT_SOCKET",
            )
            if not e.get(k) or e[k].startswith("${")
        ]
        if missing:
            raise ValueError(f"missing environment variables: {', '.join(missing)}")
        return cls(
            api_url=e["LEASEHOLD_API_URL"],
            token=e["LEASEHOLD_AGENT_TOKEN"],
            egress=e["LEASEHOLD_EGRESS_IP"],
            socket=e["LEASEHOLD_AGENT_SOCKET"],
        )


@dataclass
class _Target:
    target: str
    provider: str
    interval_s: float
    expires: datetime
    duration: timedelta
    stop_at: float  # monotonic
    next_at: float = 0.0  # monotonic: the next token
    lost: str | None = None
    tasks: dict[tuple[str, str], datetime] = field(default_factory=dict)  # task → expiry
    route: dict | None = field(default=None, repr=False)  # a proxy route, with credentials


def _stop_at(expires: datetime, now: datetime) -> float:
    return time.monotonic() + (expires - now).total_seconds() - MARGIN_S


def _time(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


class TokenRequest(BaseModel):
    target_lease_id: UUID
    task_type: str
    cost: int = Field(default=1, ge=0, le=1000)
    first: bool = False
    credentials_fp: str | None = Field(
        default=None, description="The route the worker has; a newer one comes back with the token"
    )


class Agent:
    def __init__(self, settings: AgentSettings, transport: httpx.AsyncBaseTransport | None = None):
        self.s = settings
        self.api = httpx.AsyncClient(
            base_url=settings.api_url.rstrip("/"),
            timeout=httpx.Timeout(30.0, connect=5.0),
            transport=transport,
        )
        self.targets: dict[UUID, _Target] = {}
        self.release: set[UUID] = set()
        self.lock = asyncio.Lock()

    def _headers(self, worker_auth: str | None) -> dict[str, str]:
        h = {"Authorization": f"Bearer {self.s.token}"}
        if worker_auth:
            h["X-Leasehold-For"] = worker_auth
        return h

    async def call(
        self, method: str, path: str, body: Any = None, worker_auth: str | None = None
    ) -> httpx.Response:
        return await self.api.request(method, path, json=body, headers=self._headers(worker_auth))

    # --- leases -------------------------------------------------------------------------------

    def record_lease(self, data: dict) -> None:
        now = datetime.now(UTC)
        for t in data.get("targets", []):
            i = UUID(t["lease_id"])
            exp = _time(t["expires_at"])
            have = self.targets.get(i)
            if have is None:
                self.targets[i] = _Target(
                    target=t["target"],
                    provider=t.get("provider") or "",
                    interval_s=float(t["interval_s"]),
                    expires=exp,
                    duration=max(exp - now, timedelta(seconds=10)),
                    stop_at=_stop_at(exp, now),
                    route=t.get("route"),
                )
            elif not have.lost:
                have.expires = max(have.expires, exp)
                have.stop_at = _stop_at(have.expires, now)
            self.release.discard(i)
        for task in data.get("tasks", []):
            i = task.get("target_lease_id")
            if i and UUID(i) in self.targets:
                self.targets[UUID(i)].tasks[(task["task_type"], task["key"])] = _time(
                    task["lease_expires_at"]
                )

    def record_renewed(self, data: dict) -> None:
        renewed = {
            (r["task_type"], r["key"]): _time(r["lease_expires_at"]) for r in data["renewed"]
        }
        lost = {(r["task_type"], r["key"]) for r in data["lost"]}
        for t in self.targets.values():
            for k in list(t.tasks):
                if k in renewed:
                    t.tasks[k] = renewed[k]
                elif k in lost:
                    del t.tasks[k]

    def record_done(self, items: list[dict]) -> None:
        """Tasks acked, failed or released; pushback stops their target at once."""
        for it in items:
            k = (it["task_type"], it["key"])
            for t in self.targets.values():
                if k in t.tasks:
                    del t.tasks[k]
                    if it.get("cooldown") and not t.lost:
                        t.lost = "pushback"
                    elif (
                        it.get("proxy_error")
                        and it["proxy_error"].get("kind") != "gateway"
                        and not t.lost
                    ):
                        t.lost = "proxy error"

    async def maintain(self) -> None:
        """Renew target leases that have tasks, give back those without, forget lost
        ones. Called every few seconds."""
        now = datetime.now(UTC)
        async with self.lock:
            for i, t in list(self.targets.items()):
                for k, exp in list(t.tasks.items()):
                    if exp <= now:  # the worker's task lease ran out (worker gone)
                        del t.tasks[k]
                if not t.tasks:
                    if t.lost:
                        del self.targets[i]
                    else:
                        self.release.add(i)
            active = {i: t for i, t in self.targets.items() if not t.lost and i not in self.release}
            due = [i for i, t in active.items() if t.expires - now <= t.duration / 2]
            release = sorted(self.release)
        if not due and not release:
            return
        body = {
            "egress": self.s.egress,
            "leases": [str(i) for i in due],
            "release": [str(i) for i in release],
        }
        try:
            r = await self.call("POST", "/v1/targets/renew", body)
            r.raise_for_status()
            data = r.json()
        except httpx.HTTPError as e:
            log.warning("target renewal failed", extra={"fields": {"error": str(e)}})
            mono = time.monotonic()
            async with self.lock:
                for i in due:
                    t = self.targets.get(i)
                    if t is not None and mono >= t.stop_at:
                        t.lost = "not renewed in time"
            return
        async with self.lock:
            for i in release:
                t = self.targets.get(i)
                if t is not None and not t.tasks:
                    del self.targets[i]
            self.release.difference_update(release)
            for x in data["renewed"]:
                t = self.targets.get(UUID(x["lease_id"]))
                if t is not None and not t.lost:
                    t.expires = _time(x["expires_at"])
                    t.stop_at = _stop_at(t.expires, now)
            for x in data["lost"]:
                t = self.targets.get(UUID(x["lease_id"]))
                if t is not None:
                    t.lost = x["reason"]
            for x in data.get("routes") or []:
                t = self.targets.get(UUID(x["lease_id"]))
                if t is not None:
                    t.route = x["route"]

    # --- tokens -------------------------------------------------------------------------------

    async def token(self, req: TokenRequest) -> dict:
        async with self.lock:
            t = self.targets.get(req.target_lease_id)
            if t is None:
                return {"granted": False, "lost": "unknown target lease"}
            if t.lost:
                return {"granted": False, "lost": t.lost}
            mono = time.monotonic()
            if mono >= t.stop_at:
                return {"granted": False, "lost": "target lease ends"}
            if mono < t.next_at:
                return {"granted": False, "wait_s": round(t.next_at - mono, 3)}
            # Reserve the slot before spending, so two workers can't both take it.
            t.next_at = mono + t.interval_s
            provider = t.provider
            route = t.route
        extra: dict = {}
        if (
            route is not None
            and req.credentials_fp is not None
            and route.get("credentials_fp") != req.credentials_fp
        ):
            extra["route"] = route
        if not req.first and req.cost > 0 and provider:
            try:
                r = await self.call(
                    "POST",
                    "/v1/targets/spend",
                    {
                        "egress": route["egress"] if route else self.s.egress,
                        "provider": provider,
                        "task_type": req.task_type,
                        "n": req.cost,
                    },
                )
                r.raise_for_status()
                granted = r.json()["granted"]
            except httpx.HTTPError as e:
                return {"granted": False, "wait_s": 5.0, "reason": f"control API: {e}"}
            if granted < req.cost:
                # Partial spends are not returned: the window's budget is small and
                # handing tokens back would let a busy class drain the rest.
                return {"granted": False, "wait_s": 10.0, "reason": "no budget left"}
        return {"granted": True, "interval_s": t.interval_s, **extra}


def create_app(settings: AgentSettings, transport: httpx.AsyncBaseTransport | None = None):
    agent = Agent(settings, transport)

    async def loop() -> None:
        while True:
            try:
                await agent.maintain()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("agent maintenance failed")
            await asyncio.sleep(RENEW_EVERY_S)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(loop())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await agent.api.aclose()

    app = FastAPI(title="leasehold egress agent", lifespan=lifespan)
    app.state.agent = agent

    def passthrough(r: httpx.Response) -> Response:
        return Response(r.content, status_code=r.status_code, media_type="application/json")

    async def body(request: Request) -> Any:
        raw = await request.body()
        return await request.json() if raw else None

    def unavailable(e: Exception) -> JSONResponse:
        return JSONResponse({"detail": f"control API unreachable: {e}"}, status_code=503)

    @app.post("/v1/tasks/lease")
    async def lease(request: Request):
        data = await body(request) or {}
        data["egress"] = settings.egress  # the machine's, whatever the worker says
        try:
            r = await agent.call(
                "POST", "/v1/tasks/lease", data, request.headers.get("authorization")
            )
        except httpx.HTTPError as e:
            return unavailable(e)
        if r.status_code == 200:
            async with agent.lock:
                agent.record_lease(r.json())
        return passthrough(r)

    @app.post("/v1/tasks/renew")
    async def renew(request: Request):
        try:
            r = await agent.call(
                "POST", "/v1/tasks/renew", await body(request), request.headers.get("authorization")
            )
        except httpx.HTTPError as e:
            return unavailable(e)
        if r.status_code == 200:
            async with agent.lock:
                agent.record_renewed(r.json())
        return passthrough(r)

    def outcome(path: str, field_name: str):
        async def handler(request: Request):
            data = await body(request) or {}
            data["egress"] = settings.egress
            try:
                r = await agent.call("POST", path, data, request.headers.get("authorization"))
            except httpx.HTTPError as e:
                return unavailable(e)
            if r.status_code == 200:
                async with agent.lock:
                    agent.record_done(data.get(field_name, []))
            return passthrough(r)

        return handler

    app.post("/v1/tasks/fail")(outcome("/v1/tasks/fail", "tasks"))
    app.post("/v1/tasks/release")(outcome("/v1/tasks/release", "leases"))

    @app.post("/v1/tasks/ack")
    async def ack(request: Request):
        data = await body(request) or {}
        try:
            r = await agent.call(
                "POST", "/v1/tasks/ack", data, request.headers.get("authorization")
            )
        except httpx.HTTPError as e:
            return unavailable(e)
        if r.status_code == 200:
            async with agent.lock:
                agent.record_done(data.get("tasks", []))
        return passthrough(r)

    for method, path in FORWARD:

        async def forward(request: Request, _m=method, _p=path):
            try:
                r = await agent.call(
                    _m, _p, await body(request), request.headers.get("authorization")
                )
            except httpx.HTTPError as e:
                return unavailable(e)
            return passthrough(r)

        app.add_api_route(path, forward, methods=[method])

    @app.post("/v1/agent/token")
    async def token(req: TokenRequest):
        return await agent.token(req)

    @app.get("/health")
    async def health():
        return {"status": "ok", "egress": settings.egress, "targets": len(agent.targets)}

    return app


def serve(settings: AgentSettings) -> None:
    """Listen on the socket until stopped. The socket is world-writable: the workers'
    containers run as other users, and only allocations that mount the host volume can
    reach it; the control API checks each worker's own credential."""
    import socket as socketlib

    import uvicorn

    path = settings.socket
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)  # a stale socket from the last run
    sock = socketlib.socket(socketlib.AF_UNIX, socketlib.SOCK_STREAM)
    sock.bind(path)
    os.chmod(path, 0o666)
    uvicorn.run(create_app(settings), fd=sock.fileno(), log_level="info")
