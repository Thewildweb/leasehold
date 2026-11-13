"""The control API: the frontier over HTTP, plus the lease sweeper and frontier metrics.

Run it with ``leasehold api serve`` (settings from the environment, see
``leasehold.api.settings``), or build an app with ``create_app`` in tests.

Endpoints (all under ``/v1`` need a bearer token):

    POST /v1/tasks/upsert          create or update tasks of one task type
    POST /v1/tasks/lease           lease due tasks
    POST /v1/tasks/renew           extend leases; lists the ones that are lost
    POST /v1/tasks/ack             record success
    POST /v1/tasks/fail            record a failed attempt
    POST /v1/tasks/release         give leases back without an attempt counted
    GET  /v1/task-types            settings and counts per task type
    POST /v1/task-types/{name}/pause   stop leasing; release current leases (admin)
    POST /v1/task-types/{name}/resume  (admin)
    GET  /v1/whoami                the caller, as the API sees it
    POST /v1/alerts                Grafana webhook: store alert notifications (alerts token)
    GET  /v1/alerts                alert incidents with their latest state (admin)
    GET  /v1/alerts/events         the alert notifications themselves (admin)
    GET  /health                   200 when the database answers
    GET  /metrics                  Prometheus
"""

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)
from psycopg import sql
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, ConfigDict, Field

from leasehold.api.alerts import AlertInbox, InboxError, default_database, rows_from_webhook
from leasehold.api.auth import (
    Authenticator,
    AuthError,
    Caller,
    Forbidden,
    Registry,
    load_tokens,
)
from leasehold.api.settings import Settings
from leasehold.frontier import schema
from leasehold.frontier.store import (
    AckItem,
    FailItem,
    Frontier,
    LeaseRef,
    UnknownTaskType,
    UpsertItem,
)
from leasehold.spec import ServiceSpec, find_specs, load_spec
from leasehold.types import parse_duration

log = logging.getLogger("leasehold.api")

MAX_UPSERT = 5000
MAX_LEASE = 500
MAX_REFS = 1000
KEY_MAX = 2048


# --- request and response models --------------------------------------------------------------


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


Key = Annotated[str, Field(min_length=1, max_length=KEY_MAX)]


class UpsertTask(_Model):
    key: Key
    priority: int | None = Field(default=None, ge=0, le=100)
    due_at: datetime | None = Field(default=None, description="Timezone-aware")
    payload: Any = None
    country: str | None = Field(default=None, min_length=2, max_length=2)
    group: str | None = Field(
        default=None, max_length=100, description="e.g. the server the task talks to"
    )


class UpsertRequest(_Model):
    task_type: str
    tasks: list[UpsertTask] = Field(min_length=1, max_length=MAX_UPSERT)
    reopen: bool = Field(default=False, description="Set done and dead tasks back to pending")


class UpsertResponse(_Model):
    inserted: int
    updated: int


class LeaseRequest(_Model):
    task_types: list[str] | None = Field(
        default=None, description="Default: every task type this worker consumes"
    )
    max: int = Field(ge=1, le=MAX_LEASE)
    worker_id: str | None = Field(
        default=None, max_length=200, description="Instance ID, if the token doesn't say"
    )
    per_group: int | None = Field(
        default=None, ge=1, le=MAX_LEASE, description="At most this many tasks per group"
    )
    held: dict[str, int] = Field(
        default_factory=dict,
        max_length=1000,
        description="Tasks per group the worker already holds; count against per_group",
    )


class Ref(_Model):
    task_type: str
    key: Key
    lease_id: UUID

    def to_store(self) -> LeaseRef:
        return LeaseRef(self.task_type, self.key, self.lease_id)


class LeasedTask(Ref):
    lease_expires_at: datetime
    priority: int
    attempts: int
    payload: Any = None
    country: str | None = None
    group: str = ""


class LeaseResponse(_Model):
    tasks: list[LeasedTask]


class RefsRequest(_Model):
    leases: list[Ref] = Field(min_length=1, max_length=MAX_REFS)


class RenewedLease(Ref):
    lease_expires_at: datetime


class RenewResponse(_Model):
    renewed: list[RenewedLease]
    lost: list[Ref] = Field(description="Stop working on these")


class AckTask(Ref):
    event_id: UUID


class AckRequest(_Model):
    tasks: list[AckTask] = Field(min_length=1, max_length=MAX_REFS)


class FailTask(Ref):
    reason: str = Field(min_length=1, max_length=1000)
    event_id: UUID | None = None
    retry_after_s: float | None = Field(
        default=None, gt=0, le=30 * 86400, description="Override the backoff"
    )
    permanent: bool = Field(default=False, description="Mark dead now; no more attempts")


class FailRequest(_Model):
    tasks: list[FailTask] = Field(min_length=1, max_length=MAX_REFS)


class AppliedResponse(_Model):
    applied: int
    unknown: list[Ref] = Field(description="No matching lease (already acked, re-leased, ...)")


class PauseRequest(_Model):
    reason: str = Field(min_length=1, max_length=500)


def _refs(refs: list[LeaseRef]) -> list[Ref]:
    return [Ref(task_type=r.task_type, key=r.key, lease_id=r.lease_id) for r in refs]


# --- metrics ----------------------------------------------------------------------------------


class Metrics:
    """Every series carries ``env``, so dashboards and alerts can tell the prod and dev
    control APIs apart."""

    def __init__(self, env: str) -> None:
        self.env = env
        self.registry = CollectorRegistry()
        self.ops = Counter(
            "leasehold_tasks",
            "Task operations through the control API",
            ["env", "task_type", "op"],
            registry=self.registry,
        )
        self.swept = Counter(
            "leasehold_leases_expired",
            "Expired leases returned by the sweeper",
            ["env"],
            registry=self.registry,
        )
        g = lambda name, doc: Gauge(name, doc, ["env", "task_type"], registry=self.registry)  # noqa: E731
        self.eligible = g(
            "leasehold_frontier_eligible_tasks",
            "Pending tasks that are due (capped at 1M); the autoscaling input",
        )
        self.leased = g("leasehold_frontier_leased_tasks", "Tasks currently leased")
        self.dead = g("leasehold_frontier_dead_tasks", "Tasks that ran out of attempts")
        self.paused = g("leasehold_frontier_paused", "1 when the task type is paused")
        self.rows = g("leasehold_frontier_rows_estimate", "Rows per task type (planner estimate)")
        self.alerts = Counter(
            "leasehold_alerts_received",
            "Alert notifications stored in the alert inbox",
            ["env", "status"],
            registry=self.registry,
        )
        self.alert_errors = Counter(
            "leasehold_alerts_store_errors",
            "Alert notifications that could not be stored (ClickHouse unavailable)",
            ["env"],
            registry=self.registry,
        )


# --- app --------------------------------------------------------------------------------------


def load_registry(settings: Settings) -> Registry:
    specs: list[ServiceSpec] = [load_spec(p) for p in find_specs(list(settings.services))]
    return Registry(specs)


def create_app(
    settings: Settings, registry: Registry | None = None, authenticator: Authenticator | None = None
) -> FastAPI:
    registry = registry or load_registry(settings)
    auth = authenticator or Authenticator(
        env=settings.env,
        registry=registry,
        tokens=load_tokens(settings.tokens_file),
        jwks_url=settings.nomad_jwks_url,
        audience=settings.jwt_audience,
        issuer=settings.jwt_issuer,
    )
    frontier = Frontier(settings.env)
    metrics = Metrics(settings.env)
    # jit off: the lease query runs as a prepared statement, and its generic plan's
    # cost crosses jit_above_cost, so Postgres would JIT-compile it on every call
    # (~400 ms of CPU for a query that runs in ~3 ms).
    pool = AsyncConnectionPool(
        settings.dsn,
        open=False,
        min_size=2,
        max_size=20,
        kwargs={"autocommit": True, "options": "-c jit=off"},
    )

    async def background(name: str, interval: float, fn) -> None:
        while True:
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s failed", name)
            await asyncio.sleep(interval)

    inbox = (
        AlertInbox(
            settings.clickhouse_url,
            settings.clickhouse_user,
            settings.clickhouse_password,
            settings.alerts_database or default_database(settings.env),
        )
        if settings.clickhouse_url
        else None
    )

    async def inbox_table() -> None:
        # Retried until ClickHouse answers; the frontier doesn't wait for it.
        while inbox is not None and not inbox.ready:
            try:
                await inbox.ensure_table()
                log.info("alert inbox: %s", inbox.table)
            except InboxError as e:
                log.warning("alert inbox not ready: %s", e)
                await asyncio.sleep(30)

    lock_key = f"leasehold-sweeper:{frontier.schema}"

    async def sweep_once() -> None:
        # Only one API instance sweeps at a time.
        async with pool.connection() as conn:
            got = (
                await (
                    await conn.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock_key,))
                ).fetchone()
            )[0]
            if not got:
                return
            try:
                n = await frontier.sweep(conn)
                if n:
                    metrics.swept.labels(settings.env).inc(n)
                    log.info("returned %d expired leases", n)
            finally:
                await conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))

    async def stats_once() -> None:
        async with pool.connection() as conn:
            for s in await frontier.stats(conn):
                metrics.eligible.labels(settings.env, s.task_type).set(s.eligible)
                metrics.leased.labels(settings.env, s.task_type).set(s.leased)
                metrics.dead.labels(settings.env, s.task_type).set(s.dead)
                metrics.paused.labels(settings.env, s.task_type).set(1 if s.paused else 0)
                metrics.rows.labels(settings.env, s.task_type).set(s.rows_estimate)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await pool.open(wait=True)
        async with pool.connection() as conn:
            version = await _schema_version(conn, settings.env)
        if version < schema.LATEST:
            await pool.close()
            raise RuntimeError(
                f"frontier schema for {settings.env} is at version {version}, "
                f"expected {schema.LATEST}; run `leasehold frontier migrate`"
            )
        tasks = [
            asyncio.create_task(background("sweeper", settings.sweep_interval, sweep_once)),
            asyncio.create_task(background("stats", settings.stats_interval, stats_once)),
            asyncio.create_task(inbox_table()),
        ]
        try:
            yield
        finally:
            for t in tasks:
                t.cancel()
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await t
            await pool.close()
            if inbox is not None:
                await inbox.close()

    app = FastAPI(title="leasehold control API", version="1", lifespan=lifespan)
    app.state.settings = settings
    app.state.registry = registry
    app.state.metrics = metrics
    app.state.sweep_once = sweep_once
    app.state.stats_once = stats_once
    app.state.inbox = inbox

    @app.exception_handler(AuthError)
    async def _auth_error(request: Request, exc: AuthError):
        return JSONResponse(
            {"detail": str(exc)}, status_code=401, headers={"WWW-Authenticate": "Bearer"}
        )

    @app.exception_handler(Forbidden)
    async def _forbidden(request: Request, exc: Forbidden):
        return JSONResponse({"detail": str(exc)}, status_code=403)

    @app.exception_handler(UnknownTaskType)
    async def _unknown(request: Request, exc: UnknownTaskType):
        return JSONResponse({"detail": f"unknown task type {exc.args[0]!r}"}, status_code=404)

    async def any_caller(authorization: Annotated[str | None, Header()] = None) -> Caller:
        return await auth.authenticate(authorization)

    async def caller(c: Annotated[Caller, Depends(any_caller)]) -> Caller:
        if c.alerts:
            raise Forbidden(f"{c.name} may only post alert notifications")
        return c

    CallerDep = Annotated[Caller, Depends(caller)]

    def require_admin(c: Caller) -> None:
        if not c.admin:
            raise Forbidden(f"{c.name} is not an admin")

    def count(task_type: str, op: str, n: int = 1) -> None:
        if n:
            metrics.ops.labels(settings.env, task_type, op).inc(n)

    # --- tasks ---

    @app.post("/v1/tasks/upsert", response_model=UpsertResponse)
    async def upsert(req: UpsertRequest, c: CallerDep):
        if req.task_type not in registry.task_types:
            raise UnknownTaskType(req.task_type)
        registry.require_create(c, req.task_type)
        items = [
            UpsertItem(
                t.key,
                t.priority,
                t.due_at,
                t.payload,
                t.country.upper() if t.country else None,
                t.group,
            )
            for t in req.tasks
        ]
        async with pool.connection() as conn:
            r = await frontier.upsert(conn, req.task_type, items, c.name, reopen=req.reopen)
        count(req.task_type, "inserted", r.inserted)
        count(req.task_type, "updated", r.updated)
        return UpsertResponse(inserted=r.inserted, updated=r.updated)

    @app.post("/v1/tasks/lease", response_model=LeaseResponse)
    async def lease(req: LeaseRequest, c: CallerDep):
        types = set(req.task_types) if req.task_types is not None else registry.leasable(c)
        if not types:
            raise Forbidden(f"{c.name} consumes no task types")
        registry.require_lease(c, types)
        instance = c.instance or req.worker_id or "unknown"
        async with pool.connection() as conn:
            rows = await frontier.lease(
                conn, sorted(types), req.max, f"{c.name}:{instance}", req.per_group, req.held
            )
        for r in rows:
            count(r["task_type"], "leased")
        return LeaseResponse(tasks=[LeasedTask(**r) for r in rows])

    def _check_refs(c: Caller, refs) -> None:
        registry.require_lease(c, {r.task_type for r in refs})

    @app.post("/v1/tasks/renew", response_model=RenewResponse)
    async def renew(req: RefsRequest, c: CallerDep):
        _check_refs(c, req.leases)
        async with pool.connection() as conn:
            rows, lost = await frontier.renew(conn, [r.to_store() for r in req.leases])
        for r in lost:
            count(r.task_type, "lost")
        return RenewResponse(renewed=[RenewedLease(**r) for r in rows], lost=_refs(lost))

    @app.post("/v1/tasks/ack", response_model=AppliedResponse)
    async def ack(req: AckRequest, c: CallerDep):
        _check_refs(c, req.tasks)
        items = [AckItem(t.to_store(), t.event_id) for t in req.tasks]
        async with pool.connection() as conn:
            n, unknown = await frontier.ack(conn, items)
        _count_applied(req.tasks, unknown, "acked")
        return AppliedResponse(applied=n, unknown=_refs(unknown))

    @app.post("/v1/tasks/fail", response_model=AppliedResponse)
    async def fail(req: FailRequest, c: CallerDep):
        _check_refs(c, req.tasks)
        items = [
            FailItem(
                t.to_store(),
                t.reason,
                t.event_id,
                timedelta(seconds=t.retry_after_s) if t.retry_after_s else None,
                t.permanent,
            )
            for t in req.tasks
        ]
        async with pool.connection() as conn:
            n, unknown = await frontier.fail(conn, items)
        _count_applied(req.tasks, unknown, "failed")
        return AppliedResponse(applied=n, unknown=_refs(unknown))

    @app.post("/v1/tasks/release", response_model=AppliedResponse)
    async def release(req: RefsRequest, c: CallerDep):
        _check_refs(c, req.leases)
        async with pool.connection() as conn:
            n, unknown = await frontier.release(conn, [r.to_store() for r in req.leases])
        _count_applied(req.leases, unknown, "released")
        return AppliedResponse(applied=n, unknown=_refs(unknown))

    def _count_applied(refs, unknown: list[LeaseRef], op: str) -> None:
        missed = {(u.task_type, u.key, u.lease_id) for u in unknown}
        for r in refs:
            if (r.task_type, r.key, r.lease_id) not in missed:
                count(r.task_type, op)

    # --- task types ---

    @app.get("/v1/task-types")
    async def task_types(c: CallerDep):
        async with pool.connection() as conn:
            types = await frontier.task_types(conn)
            stats = {s.task_type: s for s in await frontier.stats(conn)}
        out = []
        for name, tt in sorted(types.items()):
            s = stats.get(name)
            out.append(
                {
                    "name": name,
                    "service": tt["service"],
                    "declared": tt["declared"],
                    "paused": tt["paused"],
                    "paused_reason": tt["paused_reason"],
                    "recrawl_interval_s": _secs(tt["recrawl_interval"]),
                    "lease_duration_s": _secs(tt["lease_duration"]),
                    "max_attempts": tt["max_attempts"],
                    "eligible": s.eligible if s else None,
                    "eligible_capped": s.eligible_capped if s else None,
                    "leased": s.leased if s else None,
                    "dead": s.dead if s else None,
                    "rows_estimate": s.rows_estimate if s else None,
                }
            )
        return {"task_types": out}

    @app.post("/v1/task-types/{name}/pause")
    async def pause(name: str, req: PauseRequest, c: CallerDep):
        require_admin(c)
        async with pool.connection() as conn:
            released = await frontier.set_paused(conn, name, True, f"{c.name}: {req.reason}")
        metrics.paused.labels(settings.env, name).set(1)
        log.warning("%s paused %s (%s); released %d leases", c.name, name, req.reason, released)
        return {"paused": name, "released": released}

    @app.post("/v1/task-types/{name}/resume")
    async def resume(name: str, c: CallerDep):
        require_admin(c)
        async with pool.connection() as conn:
            await frontier.set_paused(conn, name, False)
        metrics.paused.labels(settings.env, name).set(0)
        log.warning("%s resumed %s", c.name, name)
        return {"resumed": name}

    @app.get("/v1/whoami")
    async def whoami(c: CallerDep):
        return {
            "name": c.name,
            "admin": c.admin,
            "service": c.service,
            "component": c.component,
            "instance": c.instance,
            "env": settings.env,
            "may_lease": sorted(registry.leasable(c)),
            "may_create": sorted(registry.creatable(c)),
        }

    # --- alert inbox ---

    def require_inbox() -> AlertInbox:
        if inbox is None:
            raise HTTPException(404, "the alert inbox is not configured on this API")
        if not inbox.ready:
            raise HTTPException(503, "the alert inbox is not ready (ClickHouse unavailable)")
        return inbox

    def since_param(since: str) -> timedelta:
        try:
            d = parse_duration(since)
        except ValueError as e:
            raise HTTPException(422, f"since: {e}") from None
        if d <= timedelta(0) or d > timedelta(days=365):
            raise HTTPException(422, "since: between 1s and 365d")
        return d

    @app.post("/v1/alerts")
    async def receive_alerts(
        c: Annotated[Caller, Depends(any_caller)], payload: Annotated[dict, Body()]
    ):
        if not (c.alerts or c.admin):
            raise Forbidden(f"{c.name} may not post alerts")
        ib = require_inbox()
        try:
            rows = rows_from_webhook(payload)
        except (ValueError, TypeError, AttributeError) as e:
            raise HTTPException(422, f"not a Grafana webhook notification: {e}") from None
        try:
            await ib.insert(rows)
        except InboxError as e:
            metrics.alert_errors.labels(settings.env).inc(len(rows))
            log.error("alert inbox: %d notifications not stored: %s", len(rows), e)
            raise HTTPException(503, "alert inbox unavailable") from None
        for r in rows:
            metrics.alerts.labels(settings.env, r["status"]).inc()
        return {"stored": len(rows)}

    @app.get("/v1/alerts")
    async def list_alerts(
        c: CallerDep,
        status: Literal["firing", "resolved"] | None = None,
        since: str = "24h",
        env: str | None = None,
        service: str | None = None,
        priority: str | None = None,
        alertname: str | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ):
        require_admin(c)
        ib = require_inbox()
        try:
            rows = await ib.incidents(
                since_param(since),
                status=status,
                limit=limit,
                env=env,
                service=service,
                priority=priority,
                alertname=alertname,
            )
        except InboxError as e:
            raise HTTPException(503, f"alert inbox: {e}") from None
        return {"alerts": rows}

    @app.get("/v1/alerts/events")
    async def alert_events(
        c: CallerDep,
        since: str = "24h",
        fingerprint: str | None = None,
        env: str | None = None,
        service: str | None = None,
        priority: str | None = None,
        alertname: str | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ):
        require_admin(c)
        ib = require_inbox()
        try:
            rows = await ib.events(
                since_param(since),
                limit=limit,
                fingerprint=fingerprint,
                env=env,
                service=service,
                priority=priority,
                alertname=alertname,
            )
        except InboxError as e:
            raise HTTPException(503, f"alert inbox: {e}") from None
        return {"events": rows}

    # --- operations ---

    @app.get("/health")
    async def health():
        try:
            async with pool.connection(timeout=2) as conn:
                await conn.execute("SELECT 1")
        except Exception as e:  # noqa: BLE001
            raise HTTPException(503, f"database: {e}") from e
        return {"status": "ok", "env": settings.env}

    @app.get("/metrics")
    async def prometheus():
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    return app


def _secs(v) -> int | None:
    return int(v.total_seconds()) if v is not None else None


async def _schema_version(conn, env: str) -> int:
    s = schema.schema_name(env)
    cur = await conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'schema_migrations'",
        (s,),
    )
    if await cur.fetchone() is None:
        return 0
    cur = await conn.execute(
        sql.SQL("SELECT max(version) FROM {}.schema_migrations").format(sql.Identifier(s))
    )
    return (await cur.fetchone())[0] or 0
