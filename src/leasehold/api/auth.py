"""Who is calling, and what they may do.

Two kinds of credentials, both sent as ``Authorization: Bearer <token>``:

- **Nomad workload identity (JWT).** Every component gets one from Nomad. The API
  verifies the signature against Nomad's JWKS endpoint and checks the audience and
  that the namespace belongs to this API's environment. The job ID says which
  component is calling; there are no keys to hand out.
- **Static tokens** for people, CI and the dev stack. The tokens file holds only
  SHA-256 hashes::

      {"tokens": [
        {"sha256": "<hex>", "admin": "erik"},
        {"sha256": "<hex>", "service": "certlog", "component": "worker"},
        {"sha256": "<hex>", "alerts": "grafana"}
      ]}

  ``leasehold api token`` creates a token and prints its entry.

Permissions come from ``service.yml``: a task-worker may lease the task types in its
``consumes_tasks``; a component may create the task types in its ``creates_tasks``.
Admins may create any task type and pause or resume task types. An ``alerts`` token
(for Grafana's webhook) may only post alert notifications.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import secrets
from dataclasses import dataclass
from pathlib import Path

import jwt

from leasehold import naming
from leasehold.spec import ServiceSpec
from leasehold.spec.models import Component, TaskWorker


class AuthError(Exception):
    """Credentials missing or invalid (HTTP 401)."""


class Forbidden(Exception):
    """Valid credentials, but not allowed (HTTP 403)."""


@dataclass(frozen=True)
class Caller:
    name: str  # 'certlog.worker' or 'admin:erik'
    service: str | None = None
    component: str | None = None
    admin: bool = False
    alerts: bool = False  # may post alert notifications, nothing else
    instance: str | None = None  # Nomad allocation ID, when known


class Registry:
    """The components of all services, looked up by service and component or by Nomad job."""

    def __init__(self, specs: list[ServiceSpec]):
        self.specs = {s.name: s for s in specs}
        self.components: dict[tuple[str, str], Component] = {
            (s.name, cname): c for s in specs for cname, c in s.components.items()
        }
        self.jobs = {naming.nomad_job(svc, comp): (svc, comp) for svc, comp in self.components}
        self.task_types = {tt: s.name for s in specs for tt in s.frontier.task_types}

    def component(self, caller: Caller) -> Component | None:
        if caller.service is None or caller.component is None:
            return None
        return self.components.get((caller.service, caller.component))

    def leasable(self, caller: Caller) -> set[str]:
        c = self.component(caller)
        return set(c.consumes_tasks) if isinstance(c, TaskWorker) else set()

    def creatable(self, caller: Caller) -> set[str]:
        if caller.admin:
            return set(self.task_types)
        c = self.component(caller)
        return set(c.creates_tasks) if c else set()

    def require_lease(self, caller: Caller, task_types: set[str]) -> None:
        allowed = self.leasable(caller)
        denied = sorted(task_types - allowed)
        if denied:
            raise Forbidden(f"{caller.name} may not lease {', '.join(denied)}")

    def require_create(self, caller: Caller, task_type: str) -> None:
        if task_type not in self.creatable(caller):
            raise Forbidden(f"{caller.name} may not create {task_type} tasks")


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def new_token() -> str:
    return "lh_" + secrets.token_urlsafe(32)


@dataclass(frozen=True)
class _StaticEntry:
    sha256: str
    caller: Caller


def load_tokens(path: Path | None) -> list[_StaticEntry]:
    if path is None:
        return []
    data = json.loads(path.read_text())
    out: list[_StaticEntry] = []
    for i, t in enumerate(data.get("tokens", [])):
        if "admin" in t:
            caller = Caller(name=f"admin:{t['admin']}", admin=True)
        elif "alerts" in t:
            caller = Caller(name=f"alerts:{t['alerts']}", alerts=True)
        elif "service" in t and "component" in t:
            caller = Caller(
                name=f"{t['service']}.{t['component']}",
                service=t["service"],
                component=t["component"],
            )
        else:
            raise ValueError(
                f"{path}: token {i} needs 'admin', 'alerts', or 'service' and 'component'"
            )
        if not re.fullmatch(r"[0-9a-f]{64}", t.get("sha256", "")):
            raise ValueError(f"{path}: token {i} needs a 'sha256' hex digest")
        out.append(_StaticEntry(t["sha256"], caller))
    return out


# Periodic and dispatched jobs run as child jobs: 'certlog-planner/periodic-1731300000'.
_CHILD_JOB = re.compile(r"/(periodic|dispatch)-[^/]+$")


class Authenticator:
    def __init__(
        self,
        env: str,
        registry: Registry,
        tokens: list[_StaticEntry],
        jwks_url: str | None = None,
        audience: str = "leasehold",
        issuer: str | None = None,
        jwks_client: jwt.PyJWKClient | None = None,
    ):
        self.env = env
        self.namespace = naming.nomad_namespace(env)
        self.registry = registry
        self.tokens = tokens
        self.audience = audience
        self.issuer = issuer
        self.jwks = jwks_client or (
            jwt.PyJWKClient(jwks_url, cache_keys=True, lifespan=300) if jwks_url else None
        )

    async def authenticate(self, authorization: str | None) -> Caller:
        if not authorization or not authorization.startswith("Bearer "):
            raise AuthError("missing bearer token")
        token = authorization.removeprefix("Bearer ").strip()
        if token.count(".") == 2:
            return await asyncio.to_thread(self._verify_jwt, token)
        return self._verify_static(token)

    def _verify_static(self, token: str) -> Caller:
        digest = hash_token(token)
        for entry in self.tokens:
            if hmac.compare_digest(entry.sha256, digest):
                c = entry.caller
                if not (c.admin or c.alerts) and self.registry.component(c) is None:
                    raise AuthError(f"token for unknown component {c.name}")
                return c
        raise AuthError("unknown token")

    def _verify_jwt(self, token: str) -> Caller:
        if self.jwks is None:
            raise AuthError("workload identities are not accepted (no JWKS URL configured)")
        try:
            key = self.jwks.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                key.key,
                algorithms=["RS256", "EdDSA"],
                audience=self.audience,
                issuer=self.issuer,
                options={"require": ["exp", "aud", "nomad_namespace", "nomad_job_id"]},
            )
        except jwt.PyJWTError as e:
            raise AuthError(f"invalid workload identity: {e}") from e
        if claims["nomad_namespace"] != self.namespace:
            raise AuthError(
                f"namespace {claims['nomad_namespace']!r} is not this API's "
                f"({self.namespace!r}, environment {self.env})"
            )
        job = _CHILD_JOB.sub("", claims["nomad_job_id"])
        found = self.registry.jobs.get(job)
        if found is None:
            raise AuthError(f"job {job!r} is not a component of any service")
        service, component = found
        return Caller(
            name=f"{service}.{component}",
            service=service,
            component=component,
            instance=claims.get("nomad_allocation_id"),
        )
