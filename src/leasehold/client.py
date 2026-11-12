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
        return d


@dataclass(frozen=True)
class AckItem:
    ref: LeaseRef
    event_id: UUID

    def json(self) -> dict:
        return self.ref.json() | {"event_id": str(self.event_id)}


@dataclass(frozen=True)
class FailItem:
    ref: LeaseRef
    reason: str
    event_id: UUID | None = None
    retry_after: timedelta | None = None
    permanent: bool = False

    def json(self) -> dict:
        d = self.ref.json() | {"reason": self.reason[:1000] or "failed"}
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
        self.http = httpx.Client(base_url=url.rstrip("/"), timeout=timeout, transport=transport)

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> ControlClient:
        e = os.environ if environ is None else environ
        url = e.get("LEASEHOLD_API_URL")
        if not url:
            raise ValueError("set LEASEHOLD_API_URL")
        token = e.get("LEASEHOLD_TOKEN") or None
        identity = e.get("LEASEHOLD_IDENTITY_FILE")
        if not identity and e.get("NOMAD_SECRETS_DIR"):
            identity = str(Path(e["NOMAD_SECRETS_DIR"]) / "nomad_leasehold.jwt")
        return cls(url, token=token, identity_file=identity if not token else None)

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
        self, max: int, task_types: list[str] | None = None, worker_id: str | None = None
    ) -> list[LeasedTask]:
        body: dict[str, Any] = {"max": max}
        if task_types is not None:
            body["task_types"] = task_types
        if worker_id:
            body["worker_id"] = worker_id
        r = self._request("POST", "/v1/tasks/lease", body, repeatable=False)
        return [
            LeasedTask(
                ref=LeaseRef.parse(t),
                lease_expires_at=_parse_time(t["lease_expires_at"]),
                priority=t["priority"],
                attempts=t["attempts"],
                payload=t.get("payload"),
                country=t.get("country"),
            )
            for t in r["tasks"]
        ]

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

    def fail(self, items: Sequence[FailItem]) -> Applied:
        return self._applied("/v1/tasks/fail", "tasks", [i.json() for i in items])

    def release(self, refs: Sequence[LeaseRef]) -> Applied:
        return self._applied("/v1/tasks/release", "leases", [r.json() for r in refs])

    def _applied(self, path: str, field_name: str, items: list[dict]) -> Applied:
        out = Applied(0)
        for chunk in _chunks(items, MAX_REFS):
            r = self._request("POST", path, {field_name: list(chunk)}, repeatable=True)
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


def _error(resp: httpx.Response) -> ClientError:
    try:
        detail = resp.json().get("detail")
    except ValueError:
        detail = resp.text[:500]
    cls = {401: Unauthorized, 403: Forbidden, 404: NotFound}.get(resp.status_code, ClientError)
    return cls(resp.status_code, detail)
