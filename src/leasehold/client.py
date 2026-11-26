"""Client for the control API.

    client = ControlClient.from_env()
    tasks = client.lease(max=50)
    client.ack([AckItem(t.ref, event_id)])

Authentication, in this order:

- ``LEASEHOLD_TOKEN``: a static token (people, CI, the dev stack);
- ``LEASEHOLD_IDENTITY_FILE``, or ``$NOMAD_SECRETS_DIR/nomad_leasehold.jwt``: the
  Nomad workload identity named ``leasehold`` (``identity { name = "leasehold"
  aud = ["leasehold"] file = true }``). The file is read on every request, since
  Nomad rotates it.

Retries: a request that never reached the server (connection refused, DNS,
connect timeout) is retried with backoff, as are 502/503/504 responses on
calls that are safe to repeat. ``lease`` is never retried after the request was
sent: the server may have leased tasks, and leasing again would hold twice as
many until the first ones expire.
"""

from __future__ import annotations

import os
import random
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=5.0)
MAX_UPSERT = 5000
MAX_REFS = 1000


class ClientError(Exception):
    """The control API rejected a request. ``status`` is the HTTP status."""

    def __init__(self, status: int, detail: Any):
        self.status = status
        self.detail = detail
        super().__init__(f"HTTP {status}: {detail}")


class Unauthorized(ClientError):
    pass


class Forbidden(ClientError):
    pass


class NotFound(ClientError):
    pass


class Unavailable(Exception):
    """The control API couldn't be reached after retrying."""


@dataclass(frozen=True)
class LeaseRef:
    task_type: str
    key: str
    lease_id: UUID

    def json(self) -> dict:
        return {"task_type": self.task_type, "key": self.key, "lease_id": str(self.lease_id)}

    @classmethod
    def parse(cls, d: dict) -> LeaseRef:
        return cls(d["task_type"], d["key"], UUID(str(d["lease_id"])))


@dataclass(frozen=True)
class LeasedTask:
    ref: LeaseRef
    lease_expires_at: datetime
    priority: int
    attempts: int
    payload: Any = None
    country: str | None = None
    group: str = ""
    provider: str = ""
    target: str = ""  # with target leases: what the target lease is keyed on
    target_lease_id: UUID | None = None

    @property
    def task_type(self) -> str:
        return self.ref.task_type

    @property
    def key(self) -> str:
        return self.ref.key


@dataclass(frozen=True)
class NewTask:
    key: str
    priority: int | None = None
    due_at: datetime | None = None
    payload: Any = None
    country: str | None = None
    group: str | None = None  # e.g. the server the task talks to (workers' per-group limit)
    provider: str | None = None  # provider of the target ('' for none); leasehold.egress

    def json(self) -> dict:
        d: dict[str, Any] = {"key": self.key}
        if self.priority is not None:
            d["priority"] = self.priority
        if self.due_at is not None:
            d["due_at"] = self.due_at.isoformat()
        if self.payload is not None:
            d["payload"] = self.payload
        if self.country is not None:
            d["country"] = self.country
        if self.group is not None:
            d["group"] = self.group
        if self.provider is not None:
            d["provider"] = self.provider
        return d


@dataclass(frozen=True)
class ProxySettings:
    scheme: str  # http, https or socks5h (the proxy resolves the target's name)
    host: str
    port: int
    username: str = field(repr=False)
    password: str = field(repr=False)

    def url(self) -> str:
        """With credentials: never log it (``redacted_url`` is for logs)."""
        u, p = quote(self.username, safe=""), quote(self.password, safe="")
        return f"{self.scheme}://{u}:{p}@{self.host}:{self.port}"

    def redacted_url(self) -> str:
        return f"{self.scheme}://***@{self.host}:{self.port}"


@dataclass(frozen=True)
class Route:
    """How to reach a target on a proxy tier (leasehold.egress.proxies): through
    ``proxy`` with its credentials, never directly. Holds credentials; its repr doesn't
    show them, and nothing should log ``proxy.url()``."""

    egress: str  # the exit address, or pool:<name>
    kind: str
    account: str
    proxy: ProxySettings
    country: str | None = None
    pool: str | None = None
    session: str | None = None
    session_ttl_s: int | None = None
    exit_ip: str | None = None  # known for fixed exits
    exit_ip_header: str | None = None  # a response header the vendor puts the exit IP in
    account_remaining: float | None = None
    errors: tuple[dict, ...] = ()
    credentials_fp: str | None = None

    @classmethod
    def parse(cls, d: dict) -> Route:
        p = d["proxy"]
        sess = d.get("session") or {}
        return cls(
            egress=d["egress"],
            kind=d.get("kind") or "",
            account=d["account"],
            proxy=ProxySettings(
                p["scheme"], p["host"], int(p["port"]), p["username"], p["password"]
            ),
            country=d.get("country"),
            pool=d.get("pool"),
            session=sess.get("id"),
            session_ttl_s=sess.get("ttl_s"),
            exit_ip=d.get("exit_ip"),
            exit_ip_header=d.get("exit_ip_header"),
            account_remaining=d.get("account_remaining"),
            errors=tuple(d.get("errors") or ()),
            credentials_fp=d.get("credentials_fp"),
        )


@dataclass(frozen=True)
class TargetLease:
    """An exclusive lease on a target (leasehold.egress.dispatch): send at most one
    request per ``interval_s`` to it, none before ``not_before`` (when the target was
    leased before) and nothing after ``expires_at``. On a proxy tier, ``route`` says how
    to reach it."""

    target: str
    lease_id: UUID
    expires_at: datetime
    interval_s: float
    provider: str = ""
    cap: int = 0
    granted: int = 0
    tier: str = "datacenter"
    route: Route | None = None
    not_before: datetime | None = None


@dataclass(frozen=True)
class ProxyError:
    """The proxy failed before the target answered (``leasehold.runtime.proxy``)."""

    # refused: the proxy answered CONNECT with an error status (an account matter);
    # gateway: it answered 502/503/504 (it couldn't reach the target: the target or the
    # exit); connect: the proxy itself couldn't be reached or the tunnel failed.
    kind: str
    status: int | None = None
    excerpt: str = ""  # redacted

    def json(self) -> dict:
        d: dict[str, Any] = {"kind": self.kind, "excerpt": self.excerpt[:200]}
        if self.status is not None:
            d["status"] = self.status
        return d


@dataclass(frozen=True)
class RouteRef:
    """The route a task ran on, sent with its outcome: bytes and proxy errors are charged
    to this lease, account and egress, not to whatever the target holds by then. With
    the credentials fingerprint the API recognises a route that was rotated or removed
    meanwhile."""

    lease_id: UUID  # the target lease
    account: str
    egress: str
    credentials_fp: str | None = None

    @classmethod
    def of(cls, lease_id: UUID | None, route: Route | None) -> RouteRef | None:
        if lease_id is None or route is None:
            return None
        return cls(lease_id, route.account, route.egress, route.credentials_fp)

    def json(self) -> dict:
        d: dict[str, Any] = {
            "lease_id": str(self.lease_id),
            "account": self.account,
            "egress": self.egress,
        }
        if self.credentials_fp is not None:
            d["credentials_fp"] = self.credentials_fp
        return d


@dataclass(frozen=True)
class AckItem:
    ref: LeaseRef
    event_id: UUID
    bytes: int | None = None  # through a proxy
    route: RouteRef | None = None

    def json(self) -> dict:
        d = self.ref.json() | {"event_id": str(self.event_id)}
        if self.bytes is not None:
            d["bytes"] = self.bytes
        if self.route is not None:
            d["route"] = self.route.json()
        return d


@dataclass(frozen=True)
class ReleaseItem:
    ref: LeaseRef
    after: timedelta | None = None
    cooldown: bool = False  # the target pushed back: cool it fleet-wide for ``after``
    reason: str | None = None
    cause: str | None = None  # challenge, block, rate, retry_after (with cooldown)
    bytes: int | None = None
    proxy_error: ProxyError | None = None
    route: RouteRef | None = None

    def json(self) -> dict:
        d = self.ref.json()
        if self.route is not None:
            d["route"] = self.route.json()
        if self.after is not None:
            d["after_s"] = max(0.0, self.after.total_seconds())
        if self.cooldown:
            d["cooldown"] = True
        if self.reason:
            d["reason"] = self.reason[:500]
        if self.cause:
            d["cause"] = self.cause
        if self.bytes is not None:
            d["bytes"] = self.bytes
        if self.proxy_error is not None:
            d["proxy_error"] = self.proxy_error.json()
        return d


@dataclass(frozen=True)
class FailItem:
    ref: LeaseRef
    reason: str
    event_id: UUID | None = None
    retry_after: timedelta | None = None
    permanent: bool = False
    cooldown: bool = False  # the target pushed back: cool it fleet-wide for ``retry_after``
    cause: str | None = None
    bytes: int | None = None
    proxy_error: ProxyError | None = None  # gateway: the attempt counts (maybe the target)
    route: RouteRef | None = None

    def json(self) -> dict:
        d = self.ref.json() | {"reason": self.reason[:1000] or "failed"}
        if self.route is not None:
            d["route"] = self.route.json()
        if self.proxy_error is not None:
            d["proxy_error"] = self.proxy_error.json()
        if self.cooldown:
            d["cooldown"] = True
        if self.cause:
            d["cause"] = self.cause
        if self.bytes is not None:
            d["bytes"] = self.bytes
        if self.event_id is not None:
            d["event_id"] = str(self.event_id)
        if self.retry_after is not None:
            d["retry_after_s"] = max(1.0, self.retry_after.total_seconds())
        if self.permanent:
            d["permanent"] = True
        return d


@dataclass
class Applied:
    applied: int
    unknown: list[LeaseRef] = field(default_factory=list)


def _parse_time(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _chunks(items: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class ControlClient:
    def __init__(
        self,
        url: str,
        token: str | None = None,
        identity_file: str | Path | None = None,
        *,
        timeout: httpx.Timeout = DEFAULT_TIMEOUT,
        retries: int = 5,
        transport: httpx.BaseTransport | None = None,
    ):
        if not token and not identity_file:
            raise ValueError("a token or an identity file is required")
        self.token = token
        self.identity_file = Path(identity_file) if identity_file else None
        self.retries = retries
        self.via_agent = False
        self.http = httpx.Client(base_url=url.rstrip("/"), timeout=timeout, transport=transport)

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> ControlClient:
        """From ``LEASEHOLD_API_URL``, or, when ``LEASEHOLD_AGENT_SOCKET`` is set, from the
        machine's egress agent on that Unix socket only: a worker configured for the
        agent never talks to the control API itself, also not when the agent is down."""
        e = os.environ if environ is None else environ
        token = e.get("LEASEHOLD_TOKEN") or None
        identity = e.get("LEASEHOLD_IDENTITY_FILE")
        if not identity and e.get("NOMAD_SECRETS_DIR"):
            identity = str(Path(e["NOMAD_SECRETS_DIR"]) / "nomad_leasehold.jwt")
        identity = identity if not token else None
        socket = e.get("LEASEHOLD_AGENT_SOCKET")
        if socket:
            c = cls(
                "http://agent",
                token=token,
                identity_file=identity,
                transport=httpx.HTTPTransport(uds=socket),
            )
            c.via_agent = True
            return c
        url = e.get("LEASEHOLD_API_URL")
        if not url:
            raise ValueError("set LEASEHOLD_API_URL (or LEASEHOLD_AGENT_SOCKET)")
        return cls(url, token=token, identity_file=identity)

    def close(self) -> None:
        self.http.close()

    # --- transport ----------------------------------------------------------------------------

    def _auth(self) -> str:
        if self.token:
            return f"Bearer {self.token}"
        return f"Bearer {self.identity_file.read_text().strip()}"  # type: ignore[union-attr]

    def _request(self, method: str, path: str, body: Any = None, *, repeatable: bool) -> Any:
        delay = 0.5
        for attempt in range(self.retries + 1):
            last = attempt == self.retries
            try:
                resp = self.http.request(
                    method, path, json=body, headers={"Authorization": self._auth()}
                )
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                # Never reached the server: always safe to retry.
                if last:
                    raise Unavailable(f"{method} {path}: {e}") from e
            except httpx.TransportError as e:
                if not repeatable or last:
                    raise Unavailable(f"{method} {path}: {e}") from e
            else:
                if resp.status_code in (502, 503, 504) and repeatable and not last:
                    pass  # retry below
                elif resp.status_code >= 400:
                    raise _error(resp)
                else:
                    return resp.json()
            time.sleep(delay * (0.5 + random.random()))
            delay = min(delay * 2, 10)
        raise Unavailable(f"{method} {path}: gave up")  # pragma: no cover

    # --- tasks --------------------------------------------------------------------------------

    def lease(
        self,
        max: int,
        task_types: list[str] | None = None,
        worker_id: str | None = None,
        per_group: int | None = None,
        held: dict[str, int] | None = None,
    ) -> list[LeasedTask]:
        """Lease up to ``max`` tasks; with ``per_group``, at most that many per group,
        counting the ``held`` tasks (per group) the caller already has."""
        return self.lease_with_targets(max, task_types, worker_id, per_group, held)[0]

    def lease_with_targets(
        self,
        max: int,
        task_types: list[str] | None = None,
        worker_id: str | None = None,
        per_group: int | None = None,
        held: dict[str, int] | None = None,
        egress: str | None = None,
    ) -> tuple[list[LeasedTask], list[TargetLease]]:
        """``lease``, plus the target leases granted with the tasks. Task types with
        target leases need ``egress``, the address the requests leave from."""
        body: dict[str, Any] = {"max": max}
        if egress:
            body["egress"] = egress
        if task_types is not None:
            body["task_types"] = task_types
        if worker_id:
            body["worker_id"] = worker_id
        if per_group is not None:
            body["per_group"] = per_group
            body["held"] = {g: n for g, n in (held or {}).items() if n > 0}
        r = self._request("POST", "/v1/tasks/lease", body, repeatable=False)
        tasks = [
            LeasedTask(
                ref=LeaseRef.parse(t),
                lease_expires_at=_parse_time(t["lease_expires_at"]),
                priority=t["priority"],
                attempts=t["attempts"],
                payload=t.get("payload"),
                country=t.get("country"),
                group=t.get("group") or "",
                provider=t.get("provider") or "",
                target=t.get("target") or "",
                target_lease_id=UUID(t["target_lease_id"]) if t.get("target_lease_id") else None,
            )
            for t in r["tasks"]
        ]
        targets = [
            TargetLease(
                target=x["target"],
                lease_id=UUID(x["lease_id"]),
                expires_at=_parse_time(x["expires_at"]),
                interval_s=float(x["interval_s"]),
                provider=x.get("provider") or "",
                cap=int(x.get("cap") or 0),
                granted=int(x.get("granted") or 0),
                tier=x.get("tier") or "datacenter",
                route=Route.parse(x["route"]) if x.get("route") else None,
                not_before=_parse_time(x["not_before"]) if x.get("not_before") else None,
            )
            for x in r.get("targets", [])
        ]
        return tasks, targets

    def renew_targets(
        self,
        egress: str | None,
        leases: Sequence[UUID],
        release: Sequence[UUID] = (),
        worker_id: str | None = None,
        routes: dict[UUID, Route] | None = None,
    ) -> tuple[dict[UUID, datetime], dict[UUID, str]]:
        """Extend target leases and give back ``release``. Returns (new expiry per renewed
        lease, lost lease → reason); stop sending to a lost target at once. ``routes``
        gets new routes for leases whose proxy credentials changed."""
        body: dict[str, Any] = {
            "leases": [str(i) for i in leases],
            "release": [str(i) for i in release],
        }
        if egress:
            body["egress"] = egress
        if worker_id:
            body["worker_id"] = worker_id
        r = self._request("POST", "/v1/targets/renew", body, repeatable=True)
        renewed = {UUID(x["lease_id"]): _parse_time(x["expires_at"]) for x in r["renewed"]}
        lost = {UUID(x["lease_id"]): x["reason"] for x in r["lost"]}
        if routes is not None:
            for x in r.get("routes") or []:
                routes[UUID(x["lease_id"])] = Route.parse(x["route"])
        return renewed, lost

    def renew(self, refs: Sequence[LeaseRef]) -> tuple[dict[LeaseRef, datetime], list[LeaseRef]]:
        """Returns (new expiry per renewed lease, lost leases)."""
        renewed: dict[LeaseRef, datetime] = {}
        lost: list[LeaseRef] = []
        for chunk in _chunks(list(refs), MAX_REFS):
            r = self._request(
                "POST", "/v1/tasks/renew", {"leases": [x.json() for x in chunk]}, repeatable=True
            )
            for x in r["renewed"]:
                renewed[LeaseRef.parse(x)] = _parse_time(x["lease_expires_at"])
            lost.extend(LeaseRef.parse(x) for x in r["lost"])
        return renewed, lost

    def ack(self, items: Sequence[AckItem]) -> Applied:
        return self._applied("/v1/tasks/ack", "tasks", [i.json() for i in items])

    def fail(self, items: Sequence[FailItem], egress: str | None = None) -> Applied:
        return self._applied("/v1/tasks/fail", "tasks", [i.json() for i in items], egress)

    def release(self, refs: Sequence[LeaseRef | ReleaseItem], egress: str | None = None) -> Applied:
        """Give leases back without an attempt counted; a ``ReleaseItem`` with ``after``
        also moves the task's due time that far ahead."""
        return self._applied("/v1/tasks/release", "leases", [r.json() for r in refs], egress)

    def _applied(
        self, path: str, field_name: str, items: list[dict], egress: str | None = None
    ) -> Applied:
        out = Applied(0)
        extra = {"egress": egress} if egress else {}
        for chunk in _chunks(items, MAX_REFS):
            r = self._request("POST", path, {field_name: list(chunk), **extra}, repeatable=True)
            out.applied += r["applied"]
            out.unknown.extend(LeaseRef.parse(x) for x in r["unknown"])
        return out

    def upsert(
        self, task_type: str, tasks: Sequence[NewTask], reopen: bool = False
    ) -> tuple[int, int]:
        """Create or update tasks, in batches. Returns (inserted, updated).
        Safe to repeat: an upsert of the same tasks changes nothing the second time."""
        inserted = updated = 0
        for chunk in _chunks(list(tasks), MAX_UPSERT):
            body = {"task_type": task_type, "tasks": [t.json() for t in chunk], "reopen": reopen}
            r = self._request("POST", "/v1/tasks/upsert", body, repeatable=True)
            inserted += r["inserted"]
            updated += r["updated"]
        return inserted, updated

    # --- other --------------------------------------------------------------------------------

    def whoami(self) -> dict:
        return self._request("GET", "/v1/whoami", repeatable=True)

    def task_types(self) -> list[dict]:
        return self._request("GET", "/v1/task-types", repeatable=True)["task_types"]

    def agent_token(
        self,
        target_lease_id: UUID,
        task_type: str,
        cost: int = 1,
        first: bool = False,
        credentials_fp: str | None = None,
    ) -> dict:
        """Ask the machine's agent for leave to send a request to a target: ``first`` for a
        task's first request (its budget was spent at grant), else ``cost`` tokens.
        Returns ``{"granted": bool, "wait_s": float, "lost": str | None, "reason": ...}``."""
        body = {
            "target_lease_id": str(target_lease_id),
            "task_type": task_type,
            "cost": cost,
            "first": first,
        }
        if credentials_fp is not None:
            body["credentials_fp"] = credentials_fp
        return self._request("POST", "/v1/agent/token", body, repeatable=False)

    def clear_target(self, target: str, source: str, reason: str, evidence: Any = None) -> dict:
        """Lift a target's cooldown (scope ``targets:clear``)."""
        body = {"source": source, "reason": reason, "evidence": evidence}
        return self._request(
            "POST", f"/v1/targets/{quote(target, safe='')}/clear", body, repeatable=True
        )

    def escalate_target(
        self, target: str, task_type: str, reason: str, evidence: Any = None
    ) -> dict:
        """Move a target to the task type's next egress tier (scope ``targets:escalate``),
        e.g. on an ``unreachable_from_dc`` verdict."""
        body = {"task_type": task_type, "reason": reason, "evidence": evidence}
        return self._request(
            "POST", f"/v1/targets/{quote(target, safe='')}/escalate", body, repeatable=True
        )

    def egress_accounts(self) -> list[dict]:
        """Proxy accounts with their state and remaining quota (no credentials)."""
        return self._request("GET", "/v1/egress/accounts", repeatable=True)["accounts"]

    def egress_providers(self) -> dict:
        """Provider rules and the shared-IP setting (``leasehold.egress.tagging``)."""
        return self._request("GET", "/v1/egress/providers", repeatable=True)


def _error(resp: httpx.Response) -> ClientError:
    try:
        detail = resp.json().get("detail")
    except ValueError:
        detail = resp.text[:500]
    cls = {401: Unauthorized, 403: Forbidden, 404: NotFound}.get(resp.status_code, ClientError)
    return cls(resp.status_code, detail)
