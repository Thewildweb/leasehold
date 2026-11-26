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
                                   (optionally due again only after ``after_s``;
                                   ``cooldown`` cools the task's target fleet-wide)
    POST /v1/targets/renew         extend target leases, give some back; lists lost ones
    POST /v1/targets/{target}/revoke   stop a target lease (admin)
    POST /v1/targets/{target}/clear    lift a target's cooldown (targets:clear scope)
    POST /v1/targets/{target}/escalate move a target to its next egress tier
                                       (targets:escalate scope)
    POST /v1/targets/spend         tokens for requests beyond a task's first (agents)
    GET  /v1/task-types            settings and counts per task type
    POST /v1/task-types/{name}/pause   stop leasing; release current leases (admin)
    POST /v1/task-types/{name}/resume  (admin)
    GET  /v1/egress/providers      provider rules and the shared-IP setting, for tagging
    GET  /v1/egress/accounts       proxy accounts: state, remaining quota (no credentials)
    /v1/admin/proxies/...          proxy accounts, pools and exits (admin; the schema
                                   owner's API only): see ``leasehold.api.proxy_routes``
    GET  /v1/whoami                the caller, as the API sees it
    POST /v1/alerts                Grafana webhook: store alert notifications (alerts token)
    GET  /v1/alerts                alert incidents with their latest state (admin)
    GET  /v1/alerts/events         the alert notifications themselves (admin)
    GET  /health                   200 when the database answers
    GET  /metrics                  Prometheus
"""

import asyncio
import contextlib
import dataclasses
import logging
import time
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

from leasehold import naming
from leasehold.api.alerts import AlertInbox, InboxError, default_database, rows_from_webhook
from leasehold.api.auth import (
    Authenticator,
    AuthError,
    Caller,
    Forbidden,
    Registry,
    load_tokens,
)
from leasehold.api.proxy_routes import redact_text, register_proxy_routes
from leasehold.api.runs import RunLog
from leasehold.api.runs import jobs as scheduled_jobs
from leasehold.api.settings import Settings
from leasehold.egress import tiers
from leasehold.egress.dispatch import (
    EXIT_ACCOUNT,
    ClearRefused,
    Cooling,
    Dispatcher,
    EgressRefused,
)
from leasehold.egress.planner import Planner, effective_class, publish_demand
from leasehold.egress.probe import Prober
from leasehold.egress.proxies import STATES as PROXY_STATES
from leasehold.egress.proxies import load_accounts, proxy_tables_exist
from leasehold.egress.proxy_admin import ProxyAdmin, UsageRow, account_view
from leasehold.egress.quota import QuotaKeeper
from leasehold.egress.schema import LATEST as EGRESS_LATEST
from leasehold.egress.secrets import Keyring
from leasehold.egress.store import EgressNotReady, provider_rules
from leasehold.egress.store import _version as egress_version
from leasehold.egress.usage import load_adapters
from leasehold.frontier import schema
from leasehold.frontier.store import (
    AckItem,
    FailItem,
    Frontier,
    LeaseRef,
    UnknownTaskType,
    UpsertItem,
)
from leasehold.kafka import lag as kafka_lag
from leasehold.kafka.apply import admin_client
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
    provider: str | None = Field(
        default=None,
        max_length=100,
        description="Provider of the task's target (leasehold.egress.tagging); '' for none",
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
    egress: str | None = Field(
        default=None,
        max_length=64,
        description="The address the worker's requests leave from; required for task types "
        "with target leases",
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
    provider: str = ""
    target: str = ""
    target_lease_id: UUID | None = None


class TargetLeaseOut(_Model):
    target: str
    lease_id: UUID
    expires_at: datetime
    interval_s: float = Field(description="At least this long between requests to the target")
    provider: str
    cap: int
    granted: int
    tier: str = "datacenter"
    route: dict | None = Field(
        default=None,
        description="For a target on a proxy tier: the proxy to reach it through, with "
        "credentials (leasehold.egress.proxies.render_route)",
    )
    not_before: datetime | None = Field(
        default=None,
        description="For a target leased before: no request before this time (one interval "
        "after the previous lease ended, whatever its holder or egress)",
    )

    def __repr__(self) -> str:
        return f"TargetLeaseOut(target={self.target!r}, lease_id={self.lease_id}, tier={self.tier})"


class LeaseResponse(_Model):
    tasks: list[LeasedTask]
    targets: list[TargetLeaseOut] = Field(default_factory=list)


class TargetRenewRequest(_Model):
    egress: str | None = Field(
        default=None,
        max_length=64,
        description="The holder's own egress; none for a holder with routed leases only",
    )
    worker_id: str | None = Field(default=None, max_length=200)
    leases: list[UUID] = Field(default_factory=list, max_length=MAX_REFS)
    release: list[UUID] = Field(
        default_factory=list, max_length=MAX_REFS, description="No more tasks for these"
    )


class RenewedTarget(_Model):
    lease_id: UUID
    expires_at: datetime


class LostTarget(_Model):
    lease_id: UUID
    reason: str


class RouteUpdate(_Model):
    lease_id: UUID
    route: dict


class TargetRenewResponse(_Model):
    renewed: list[RenewedTarget]
    lost: list[LostTarget] = Field(description="Stop sending to these targets now")
    routes: list[RouteUpdate] = Field(
        default_factory=list,
        description="New routes for renewed leases whose proxy credentials changed",
    )


class SpendRequest(_Model):
    egress: str = Field(max_length=64)
    provider: str = Field(max_length=100)
    task_type: str
    n: int = Field(ge=1, le=1000)


class SpendResponse(_Model):
    granted: int


class RevokeRequest(_Model):
    reason: str = Field(min_length=1, max_length=500)


class RefsRequest(_Model):
    leases: list[Ref] = Field(min_length=1, max_length=MAX_REFS)


class RenewedLease(Ref):
    lease_expires_at: datetime


class RenewResponse(_Model):
    renewed: list[RenewedLease]
    lost: list[Ref] = Field(description="Stop working on these")


Cause = Literal["challenge", "block", "rate", "retry_after"]


class ProxyErrorIn(_Model):
    kind: Literal["refused", "gateway", "connect"] = Field(
        description="refused: the proxy answered the CONNECT with a 4xx; gateway: with "
        "502/503/504 (it couldn't reach the target; sent with a fail, the attempt counts); "
        "connect: the proxy couldn't be reached, or the tunnel failed before the target "
        "answered"
    )
    status: int | None = Field(default=None, ge=100, le=599)
    excerpt: str = Field(default="", max_length=200, description="Redacted; no credentials")


class RouteRefIn(_Model):
    """The route a task ran on: bytes and proxy errors are charged to it."""

    lease_id: UUID = Field(description="The target lease")
    account: str = Field(max_length=100)
    egress: str = Field(max_length=100, description="The exit address or pool:<name>")
    credentials_fp: str | None = Field(default=None, max_length=64)


class AckTask(Ref):
    event_id: UUID
    bytes: int | None = Field(
        default=None, ge=0, description="Bytes through the proxy (routed tasks)"
    )
    route: RouteRefIn | None = None


class AckRequest(_Model):
    tasks: list[AckTask] = Field(min_length=1, max_length=MAX_REFS)


class ReleaseRef(Ref):
    after_s: float | None = Field(
        default=None,
        ge=0,
        le=30 * 86400,
        description="Due again this many seconds from now; unset: due time unchanged",
    )
    cooldown: bool = Field(
        default=False,
        description="The target pushed back: no egress gets it for after_s (target leases)",
    )
    reason: str | None = Field(default=None, max_length=500)
    cause: Cause | None = Field(
        default=None,
        description="What the pushback was: challenge and block cool the target's tier and "
        "move it to the next; rate and retry_after cool every tier",
    )
    bytes: int | None = Field(default=None, ge=0)
    proxy_error: ProxyErrorIn | None = Field(
        default=None, description="The proxy failed, not the target: no cooldown, no attempt"
    )
    route: RouteRefIn | None = None


class ReleaseRequest(_Model):
    leases: list[ReleaseRef] = Field(min_length=1, max_length=MAX_REFS)
    egress: str | None = Field(default=None, max_length=64, description="Recorded with cooldowns")


class FailTask(Ref):
    reason: str = Field(min_length=1, max_length=1000)
    event_id: UUID | None = None
    retry_after_s: float | None = Field(
        default=None, gt=0, le=30 * 86400, description="Override the backoff"
    )
    permanent: bool = Field(default=False, description="Mark dead now; no more attempts")
    cooldown: bool = Field(
        default=False,
        description="The target pushed back: no egress gets it for retry_after_s (target leases)",
    )
    cause: Cause | None = None
    bytes: int | None = Field(default=None, ge=0)
    proxy_error: ProxyErrorIn | None = Field(
        default=None, description="A gateway answer (502/503/504) on the route"
    )
    route: RouteRefIn | None = None


class FailRequest(_Model):
    tasks: list[FailTask] = Field(min_length=1, max_length=MAX_REFS)
    egress: str | None = Field(default=None, max_length=64, description="Recorded with cooldowns")


class AppliedResponse(_Model):
    applied: int
    unknown: list[Ref] = Field(description="No matching lease (already acked, re-leased, ...)")


class PauseRequest(_Model):
    reason: str = Field(min_length=1, max_length=500)


class ClearRequest(_Model):
    source: Literal["health", "classifier", "admin"]
    reason: str = Field(min_length=1, max_length=500)
    evidence: Any = None


class EscalateRequest(_Model):
    task_type: str
    reason: str = Field(min_length=1, max_length=500)
    evidence: Any = None


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
        c = lambda name, doc, labels: Counter(name, doc, ["env", *labels], registry=self.registry)  # noqa: E731
        p = lambda name, doc, labels: Gauge(name, doc, ["env", *labels], registry=self.registry)  # noqa: E731
        self.clears = c("leasehold_target_clears", "Cooldowns lifted", ["source", "cause"])
        self.clears_refused = c(
            "leasehold_target_clears_refused", "Clears refused by cause", ["source", "cause"]
        )
        self.recooled = c(
            "leasehold_target_recooled_after_clear",
            "Targets cooled again within an hour of a clear (clears that were wrong)",
            ["source", "cause"],
        )
        self.escalations = c(
            "leasehold_target_escalations", "Targets moved to a higher tier", ["task_type", "tier"]
        )
        self.waiting = c(
            "leasehold_dispatch_waiting",
            "Targets the dispatcher couldn't grant, by why (no egress on the tier, cooling, ...)",
            ["reason"],
        )
        self.proxy_bytes = c(
            "leasehold_proxy_bytes", "Bytes through proxy accounts", ["account", "task_type"]
        )
        self.proxy_errors = c(
            "leasehold_proxy_errors", "Proxy errors reported by workers", ["account", "kind"]
        )
        self.proxy_disabled = c(
            "leasehold_proxy_auto_disabled",
            "Exits and pools disabled by connect failures",
            ["account"],
        )
        self.proxy_moves = c(
            "leasehold_proxy_exit_moves", "Fixed exits seen at a new address by the probe", []
        )
        self.proxy_state = p(
            "leasehold_proxy_account_state",
            "1 for the account's current state",
            ["account", "vendor", "state"],
        )
        self.proxy_status = p(
            "leasehold_proxy_account_status", "1 for the account's status", ["account", "status"]
        )
        self.proxy_remaining = p(
            "leasehold_proxy_remaining_ratio", "Share of the binding quota left", ["account"]
        )
        self.proxy_remaining_bytes = p(
            "leasehold_proxy_remaining_bytes", "Bytes of the binding quota left", ["account"]
        )
        self.proxy_warn = p(
            "leasehold_proxy_quota_warning",
            "1 when an account is at the warning share used or below warn_remaining_gb",
            ["account", "vendor"],
        )
        self.proxy_paid_days = p(
            "leasehold_proxy_paid_until_days", "Days until paid_until", ["account"]
        )
        self.proxy_paid_warn = p(
            "leasehold_proxy_paid_until_warning",
            "1 when paid_until is within proxies.paid_until_warn",
            ["account", "vendor"],
        )
        self.proxy_drift_warn = p(
            "leasehold_proxy_usage_drift_warning",
            "1 when our byte count and the vendor's differ by more than proxies.drift_warn",
            ["account", "vendor"],
        )
        self.proxy_poll_failing = p(
            "leasehold_proxy_usage_poll_failing",
            "1 when the vendor's usage API has failed for longer than the warning time",
            ["account"],
        )
        self.proxy_drift = p(
            "leasehold_proxy_usage_drift_ratio",
            "(our byte count - the vendor's) / the vendor's limit",
            ["account"],
        )
        self.proxy_stopped = p(
            "leasehold_proxy_stopped",
            "1 while a task type's stop_at_used keeps it off an account",
            ["task_type", "account"],
        )
        self.proxy_egresses = p(
            "leasehold_proxy_egresses",
            "Exits and pools per account and state",
            ["account", "kind", "state"],
        )
        part = ["group", "topic", "partition", "service"]
        self.lag = p("leasehold_kafka_group_lag", "Messages a group hasn't read", part)
        self.lag_age = p(
            "leasehold_kafka_group_oldest_unread_seconds", "Age of the oldest unread message", part
        )
        self.lag_retention = p(
            "leasehold_kafka_group_retention_used_ratio",
            "Age of the oldest unread message / the topic's retention",
            part,
        )
        self.lag_share = p(
            "leasehold_kafka_group_unread_share",
            "Unread share of the messages the partition holds",
            part,
        )
        self.lag_skipped = p(
            "leasehold_kafka_group_skipped_messages",
            "Messages deleted before the group read them",
            part,
        )
        self.lag_success = p(
            "leasehold_kafka_lag_last_success_timestamp_seconds",
            "When a group's lag was last measured",
            ["group", "topic"],
        )
        self.retention_bytes = p(
            "leasehold_kafka_retention_bytes", "retention.bytes of a topic that has one", ["topic"]
        )
        self.dead_letters = p(
            "leasehold_dead_letters_total_offset",
            "Next offset of a service's dead-letter topic: every event ever moved there",
            ["service"],
        )
        job = ["service", "component"]
        self.run_failed = p(
            "leasehold_scheduled_last_run_failed", "1 when the last run failed", job
        )
        self.run_success = p(
            "leasehold_scheduled_last_success_timestamp_seconds", "The last finished run", job
        )
        self.run_due = p(
            "leasehold_scheduled_success_due_timestamp_seconds",
            "When a success is overdue (schedule and timeout)",
            job,
        )
        self.logs_newest = p(
            "leasehold_logs_newest_row_timestamp_seconds", "Newest row of the log table", []
        )
        self.logs_read = p(
            "leasehold_logs_last_success_timestamp_seconds",
            "When the log table was last read",
            [],
        )


# --- app --------------------------------------------------------------------------------------


def load_registry(settings: Settings) -> Registry:
    specs: list[ServiceSpec] = [load_spec(p) for p in find_specs(list(settings.services))]
    return Registry(specs)


def create_app(
    settings: Settings,
    registry: Registry | None = None,
    authenticator: Authenticator | None = None,
    keyring: Keyring | None = None,
    adapters: dict | None = None,
    probe_transport=None,
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
    keyring = keyring if keyring is not None else Keyring.from_env()
    dispatcher = Dispatcher(
        settings.env, frontier.schema, settings.egress_schema, keyring, settings.exit_key
    )
    planner = Planner(settings.egress_schema)
    admin = ProxyAdmin(settings.egress_schema, settings.env, keyring)
    adapters = adapters if adapters is not None else load_adapters()
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

    async def background(name: str, interval: float, fn, wait_first: bool = False) -> None:
        if wait_first:
            await asyncio.sleep(interval)
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

    async def purge_once() -> None:
        async with pool.connection() as conn:
            got = (
                await (
                    await conn.execute(
                        "SELECT pg_try_advisory_lock(hashtext(%s))", (lock_key + ":purge",)
                    )
                ).fetchone()
            )[0]
            if not got:
                return
            try:
                n = await frontier.purge_done(conn)
                if n:
                    log.info("deleted %d done tasks past keep_done", n)
            finally:
                await conn.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s))", (lock_key + ":purge",)
                )

    async def egress_sweep_once() -> None:
        # Only the egress schema's owner deletes; one API instance at a time.
        if not settings.egress_owner:
            return
        key = f"leasehold-egress-sweeper:{settings.egress_schema}"
        async with pool.connection() as conn:
            got = (
                await (
                    await conn.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (key,))
                ).fetchone()
            )[0]
            if not got:
                return
            try:
                await dispatcher.sweep(conn)
                await planner.sweep(conn)
            except EgressNotReady:
                pass
            finally:
                await conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))

    async def plan_once() -> None:
        # Demand from every environment; plans only from the schema's owner (prod), so
        # code deployed to dev never plans the budgets prod uses.
        async with pool.connection() as conn:
            try:
                budgets = await dispatcher.budgets(conn)
            except Exception:  # noqa: BLE001 - the egress schema isn't there yet
                return
            classes = {
                tt: effective_class(k, settings.env, budgets)
                for tt, k in registry.egress_classes.items()
            }
            await publish_demand(
                conn,
                settings.env,
                frontier.schema,
                classes,
                budgets.planning.horizon,
                settings.egress_schema,
            )
            if settings.egress_owner:
                n = await planner.plan(conn)
                if n:
                    log.info("planned egress allocations", extra={"fields": {"rows": n}})

    async def stats_once() -> None:
        async with pool.connection() as conn:
            for s in await frontier.stats(conn):
                metrics.eligible.labels(settings.env, s.task_type).set(s.eligible)
                metrics.leased.labels(settings.env, s.task_type).set(s.leased)
                metrics.dead.labels(settings.env, s.task_type).set(s.dead)
                metrics.paused.labels(settings.env, s.task_type).set(1 if s.paused else 0)
                metrics.rows.labels(settings.env, s.task_type).set(s.rows_estimate)
            await proxy_stats(conn)

    async def proxy_stats(conn) -> None:
        if not await proxy_tables_exist(conn, settings.egress_schema):
            return
        env = settings.env
        accounts = await load_accounts(conn, settings.egress_schema)
        for name, a in accounts.items():
            for st in PROXY_STATES:
                metrics.proxy_state.labels(env, name, a.vendor, st).set(1 if a.state == st else 0)
            for st in ("active", "drain", "disabled"):
                metrics.proxy_status.labels(env, name, st).set(1 if a.status == st else 0)
            if a.remaining.ratio is not None:
                metrics.proxy_remaining.labels(env, name).set(a.remaining.ratio)
            if a.remaining.bytes is not None:
                metrics.proxy_remaining_bytes.labels(env, name).set(a.remaining.bytes)
            h = quota_health.get(name)
            if h is not None:
                metrics.proxy_warn.labels(env, name, a.vendor).set(1 if h.warn else 0)
                metrics.proxy_poll_failing.labels(env, name).set(1 if h.poll_failing else 0)
                metrics.proxy_paid_warn.labels(env, name, a.vendor).set(
                    1 if h.paid_until_warn else 0
                )
                metrics.proxy_drift_warn.labels(env, name, a.vendor).set(1 if h.drift_warn else 0)
                if h.paid_until_days is not None:
                    metrics.proxy_paid_days.labels(env, name).set(h.paid_until_days)
                if h.drift_ratio is not None:
                    metrics.proxy_drift.labels(env, name).set(h.drift_ratio)
        for tt, spec in registry.egress_specs.items():
            if spec is None:
                continue
            resolved = spec.resolve(env)
            for tier, stop in resolved["stop_at_used"].items():
                for n in tiers.names_for(resolved, tier):
                    a = accounts.get(n)
                    if a is None or a.remaining.used_ratio is None:
                        continue
                    stopped = a.remaining.used_ratio >= stop
                    g = metrics.proxy_stopped.labels(env, tt, n)
                    was = g._value.get()  # noqa: SLF001
                    g.set(1 if stopped else 0)
                    if stopped != bool(was):
                        log.info(
                            "task type %s %s account %s (stop at %.0f%% used, %.0f%% used)",
                            tt,
                            "stopped on" if stopped else "resumed on",
                            n,
                            stop * 100,
                            a.remaining.used_ratio * 100,
                        )
        for r in await admin._rows(
            conn,
            "SELECT account, 'exit' AS kind, coalesce(override_status, status) AS state, "
            "count(*) AS n FROM {e}.egresses WHERE account IS NOT NULL AND removed_at IS NULL "
            "GROUP BY 1, 2, 3 UNION ALL SELECT account, 'pool', "
            "coalesce(override_status, status), count(*) FROM {e}.proxy_pools "
            "WHERE removed_at IS NULL GROUP BY 1, 2, 3",
        ):
            metrics.proxy_egresses.labels(env, r["account"], r["kind"], r["state"]).set(r["n"])

    quota_health: dict = {}
    proxy_due: dict[str, float] = {}

    async def proxies_once() -> None:
        # Usage polls, exit address checks and probes: the schema owner only, one API
        # instance at a time.
        if not settings.egress_owner or not keyring.configured:
            return
        key = f"leasehold-proxies:{settings.egress_schema}"
        async with pool.connection() as conn:
            if not await proxy_tables_exist(conn, settings.egress_schema):
                return
            got = (
                await (
                    await conn.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (key,))
                ).fetchone()
            )[0]
            if not got:
                return
            try:
                cfg = (await dispatcher.budgets(conn)).proxies
                keeper = QuotaKeeper(
                    admin,
                    adapters,
                    cfg.warn_used,
                    cfg.usage_poll_warn_after,
                    cfg.paid_until_warn,
                    cfg.drift_warn,
                )
                prober = Prober(admin, cfg.echo_url, transport=probe_transport)
                now = asyncio.get_running_loop().time()

                def due(job: str, every: timedelta) -> bool:
                    if now < proxy_due.get(job, 0):
                        return False
                    proxy_due[job] = now + every.total_seconds()
                    return True

                if due("usage", cfg.usage_poll):
                    await keeper.poll(conn)
                ev = await keeper.evaluate(conn)
                quota_health.clear()
                quota_health.update(ev.health)
                if due("exits", min(cfg.exit_recheck, timedelta(minutes=5))):
                    counts = await prober.recheck_exits(
                        conn, due_after_s=cfg.exit_recheck.total_seconds()
                    )
                    if counts.get("moved"):
                        metrics.proxy_moves.labels(settings.env).inc(counts["moved"])
                if due("probe", cfg.probe_interval):
                    await prober.probe_unhealthy(conn)
            finally:
                await conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))

    # The specs of every service are in the image; only the deployed ones have topics.
    monitored = [
        s
        for s in registry.specs.values()
        if settings.monitor_services is None or s.name in settings.monitor_services
    ]
    unknown = (settings.monitor_services or frozenset()) - set(registry.specs)
    unknown |= settings.paused_runs - {f"{s}.{c}" for s, c in registry.components}
    if unknown:
        raise ValueError(f"monitoring settings name unknown services: {sorted(unknown)}")
    run_log = (
        RunLog(
            settings.clickhouse_url,
            settings.clickhouse_user,
            settings.clickhouse_password,
            settings.logs_table,
            settings.env,
            [
                j
                for j in scheduled_jobs(monitored)
                if f"{j.service}.{j.component}" not in settings.paused_runs
            ],
        )
        if settings.clickhouse_url and settings.logs_table
        else None
    )

    async def runs_once() -> None:
        if run_log is None:
            return
        await run_log.refresh()
        env = settings.env
        metrics.logs_read.labels(env).set(time.time())
        if run_log.newest_s is not None:
            metrics.logs_newest.labels(env).set(run_log.newest_s)
        for (service, component), st in run_log.states.items():
            job = run_log.jobs[(service, component)]
            metrics.run_failed.labels(env, service, component).set(1 if st.last_failed else 0)
            metrics.run_due.labels(env, service, component).set(st.due_s(job.allowed_s))
            if st.last_success_s is not None:
                metrics.run_success.labels(env, service, component).set(st.last_success_s)

    lag_targets = kafka_lag.targets(monitored, settings.env)
    lag_monitor = (
        kafka_lag.LagMonitor(
            admin_client(
                settings.kafka_bootstrap,
                settings.kafka_username or naming.monitor_principal(settings.env),
                settings.kafka_password,
            ),
            lag_targets,
            kafka_lag.dead_letter_topics(monitored, settings.env),
        )
        if settings.kafka_bootstrap and settings.kafka_password
        else None
    )

    async def lag_once() -> None:
        if lag_monitor is None:
            return
        r = await asyncio.to_thread(lag_monitor.measure)
        env, now = settings.env, time.time()
        for t, readings in r.readings.items():
            for x in readings:
                labels = (env, t.group, t.topic, str(x.partition), t.service)
                metrics.lag.labels(*labels).set(x.lag)
                metrics.lag_age.labels(*labels).set(x.oldest_unread_s)
                metrics.lag_retention.labels(*labels).set(x.oldest_unread_s / t.retention_s)
                metrics.lag_share.labels(*labels).set(x.unread_share)
                metrics.lag_skipped.labels(*labels).set(x.skipped)
            metrics.lag_success.labels(env, t.group, t.topic).set(now)
        for service, offset in r.dead_letters.items():
            metrics.dead_letters.labels(env, service).set(offset)
        if r.errors:
            t, why = next(iter(r.errors.items()))
            log.warning(
                "consumer lag: %d groups not measured (%s %s: %s)",
                len(r.errors), t.group, t.topic, why,
            )  # fmt: skip

    if lag_monitor is not None:
        # From now on a group that stays unmeasured counts as such.
        for t in lag_targets:
            metrics.lag_success.labels(settings.env, t.group, t.topic).set(time.time())
            if t.retention_bytes is not None:
                metrics.retention_bytes.labels(settings.env, t.topic).set(t.retention_bytes)
    if run_log is not None:
        metrics.logs_read.labels(settings.env).set(time.time())

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
            asyncio.create_task(background("purge", settings.purge_interval, purge_once)),
            asyncio.create_task(
                background("egress sweeper", settings.purge_interval, egress_sweep_once)
            ),
            # The first run waits an interval: the window it plans is a minute ahead anyway.
            asyncio.create_task(
                background("egress planner", settings.plan_interval, plan_once, wait_first=True)
            ),
            asyncio.create_task(background("proxies", settings.proxy_interval, proxies_once)),
            asyncio.create_task(inbox_table()),
            asyncio.create_task(background("scheduled runs", settings.runs_interval, runs_once)),
            asyncio.create_task(background("consumer lag", settings.lag_interval, lag_once)),
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
            if run_log is not None:
                await run_log.close()

    app = FastAPI(title="leasehold control API", version="1", lifespan=lifespan)
    app.state.settings = settings
    app.state.registry = registry
    app.state.metrics = metrics
    app.state.sweep_once = sweep_once
    app.state.stats_once = stats_once
    app.state.purge_once = purge_once
    app.state.egress_sweep_once = egress_sweep_once
    app.state.plan_once = plan_once
    app.state.proxies_once = proxies_once
    app.state.dispatcher = dispatcher
    app.state.proxy_admin = admin
    app.state.inbox = inbox
    app.state.runs_once = runs_once
    app.state.lag_once = lag_once

    @app.exception_handler(AuthError)
    async def _auth_error(request: Request, exc: AuthError):
        return JSONResponse(
            {"detail": str(exc)}, status_code=401, headers={"WWW-Authenticate": "Bearer"}
        )

    @app.exception_handler(Forbidden)
    async def _forbidden(request: Request, exc: Forbidden):
        return JSONResponse({"detail": str(exc)}, status_code=403)

    @app.exception_handler(EgressRefused)
    async def _egress_refused(request: Request, exc: EgressRefused):
        return JSONResponse({"detail": str(exc)}, status_code=403)

    @app.exception_handler(EgressNotReady)
    async def _not_ready(request: Request, exc: EgressNotReady):
        return JSONResponse({"detail": str(exc)}, status_code=503)

    @app.exception_handler(ClearRefused)
    async def _clear_refused(request: Request, exc: ClearRefused):
        return JSONResponse({"detail": str(exc), "causes": exc.causes}, status_code=409)

    @app.exception_handler(UnknownTaskType)
    async def _unknown(request: Request, exc: UnknownTaskType):
        return JSONResponse({"detail": f"unknown task type {exc.args[0]!r}"}, status_code=404)

    async def any_caller(authorization: Annotated[str | None, Header()] = None) -> Caller:
        return await auth.authenticate(authorization)

    async def caller(
        c: Annotated[Caller, Depends(any_caller)],
        x_leasehold_for: Annotated[str | None, Header()] = None,
    ) -> Caller:
        if c.alerts:
            raise Forbidden(f"{c.name} may only post alert notifications")
        if c.agent:
            if x_leasehold_for is None:
                return c
            # An agent calling for a worker on its machine: the worker's own credential,
            # verified like any other, and the worker's permissions.
            w = await auth.authenticate(x_leasehold_for)
            if w.agent or w.alerts or w.admin:
                raise Forbidden(f"{c.name} calls only on behalf of workers")
            return dataclasses.replace(w, via=c.name)
        if x_leasehold_for is not None:
            raise Forbidden(f"{c.name} may not call on behalf of others")
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
                t.provider,
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
        holder = leased_by = f"{c.name}:{instance}"
        if c.via:
            # The machine's agent holds the target leases for all its workers.
            holder = f"{c.via}@{req.egress or 'routed'}"
        targeted = types & registry.target_leased
        plain = types - targeted
        direct = targeted & registry.needs_egress
        if direct and not req.egress:
            # Fail closed: without its egress, a worker can't be given datacenter targets.
            raise HTTPException(400, f"{', '.join(sorted(direct))} need the worker's egress")
        rows: list[dict] = []
        targets = []
        async with pool.connection() as conn:
            if targeted:
                await egress_ready(conn)
                grant = await dispatcher.lease(
                    conn,
                    sorted(targeted),
                    req.max,
                    req.egress,
                    holder,
                    registry.egress_classes,
                    leased_by=leased_by,
                )
                rows += grant.tasks
                targets = grant.targets
                for why, n in grant.waiting.items():
                    metrics.waiting.labels(settings.env, why).inc(n)
            if plain and len(rows) < req.max:
                rows += await frontier.lease(
                    conn, sorted(plain), req.max - len(rows), leased_by, req.per_group, req.held
                )
        for r in rows:
            count(r["task_type"], "leased")
        return LeaseResponse(
            tasks=[LeasedTask(**r) for r in rows],
            targets=[TargetLeaseOut(**t.__dict__) for t in targets],
        )

    egress_checked: list[float] = [0.0]

    async def egress_ready(conn) -> None:
        """The egress schema is migrated (checked once a minute): the dispatcher's queries
        need the latest version."""
        now = asyncio.get_running_loop().time()
        if now - egress_checked[0] < 60:
            return
        if await egress_version(conn, settings.egress_schema) < EGRESS_LATEST:
            raise EgressNotReady(
                f"schema {settings.egress_schema} is not migrated; run `leasehold egress migrate`"
            )
        egress_checked[0] = now

    @app.post("/v1/targets/renew", response_model=TargetRenewResponse)
    async def renew_targets(req: TargetRenewRequest, c: CallerDep):
        if c.agent:
            holder = f"{c.name}@{req.egress or 'routed'}"
        elif registry.leasable(c) & registry.target_leased:
            holder = f"{c.name}:{c.instance or req.worker_id or 'unknown'}"
        else:
            raise Forbidden(f"{c.name} consumes no task types with target leases")
        routes: dict = {}
        async with pool.connection() as conn:
            ok, lost = await dispatcher.renew(
                conn, req.egress, holder, req.leases, req.release, routes=routes
            )
        return TargetRenewResponse(
            renewed=[RenewedTarget(lease_id=i, expires_at=e) for i, e in ok.items()],
            lost=[LostTarget(lease_id=i, reason=r) for i, r in lost.items()],
            routes=[RouteUpdate(lease_id=i, route=r) for i, r in routes.items()],
        )

    @app.post("/v1/targets/spend", response_model=SpendResponse)
    async def spend(req: SpendRequest, c: CallerDep):
        if not c.agent:
            raise Forbidden(f"{c.name} is not an egress agent")
        if req.task_type not in registry.target_leased:
            raise UnknownTaskType(req.task_type)
        async with pool.connection() as conn:
            granted = await dispatcher.spend(
                conn, req.egress, req.provider, registry.egress_classes[req.task_type], req.n
            )
        return SpendResponse(granted=granted)

    @app.post("/v1/targets/{target}/revoke")
    async def revoke_target(target: str, req: RevokeRequest, c: CallerDep):
        require_admin(c)
        async with pool.connection() as conn:
            revoked = await dispatcher.revoke(conn, target, req.reason)
        return {"revoked": revoked}

    @app.post("/v1/targets/{target}/clear")
    async def clear_target(target: str, req: ClearRequest, c: CallerDep):
        if not c.may("targets:clear"):
            raise Forbidden(f"{c.name} may not clear cooldowns (scope targets:clear)")
        if (req.source == "admin") != c.admin:
            raise Forbidden("source admin is for admin tokens, and admins clear as admin")
        async with pool.connection() as conn:
            await egress_ready(conn)
            try:
                cleared = await dispatcher.clear(
                    conn, target, req.source, req.reason, req.evidence, c.name
                )
            except ClearRefused as e:
                for cause in e.causes:
                    metrics.clears_refused.labels(settings.env, req.source, str(cause)).inc()
                raise
        for r in cleared:
            metrics.clears.labels(settings.env, req.source, str(r["cause"])).inc()
        log.info(
            "cooldown cleared",
            extra={
                "fields": {
                    "target": target,
                    "source": req.source,
                    "by": c.name,
                    "cleared": len(cleared),
                }
            },
        )
        if not cleared:
            return {"cleared": 0, "detail": "the target is not cooling down"}
        return {
            "cleared": len(cleared),
            "cooldowns": [
                {"tier": r["tier"], "cause": r["cause"], "until": r["until"], "reason": r["reason"]}
                for r in cleared
            ],
        }

    @app.post("/v1/targets/{target}/escalate")
    async def escalate_target(target: str, req: EscalateRequest, c: CallerDep):
        if not c.may("targets:escalate"):
            raise Forbidden(f"{c.name} may not escalate targets (scope targets:escalate)")
        if req.task_type not in registry.target_leased:
            raise UnknownTaskType(req.task_type)
        async with pool.connection() as conn:
            await egress_ready(conn)
            was, now = await dispatcher.escalate(
                conn, target, req.task_type, f"{c.name}: {req.reason}", req.evidence
            )
        if now is not None:
            metrics.escalations.labels(settings.env, req.task_type, now).inc()
        return {"target": target, "from": was, "to": now}

    async def _cool(conn, items: list[tuple], egress: str | None) -> None:
        """items: (task_type, key, seconds, reason, cause) of tasks whose target pushed back."""
        items = [i for i in items if i[0] in registry.target_leased and i[2] > 0]
        if not items:
            return
        targets = await dispatcher.targets_of(conn, [(i[0], i[1]) for i in items])
        budgets = await dispatcher.budgets(conn)
        cooling = [
            Cooling(targets[(tt, key)], timedelta(seconds=s), reason, cause, tt)
            for tt, key, s, reason, cause in items
            if (tt, key) in targets
        ]
        escalated: list = []
        await dispatcher.cool(conn, cooling, egress, budgets.cooldown.max, escalated=escalated)
        for r in await dispatcher.recent_clears(conn, [x.target for x in cooling]):
            metrics.recooled.labels(settings.env, r["source"], str(r["cooldown_cause"])).inc()
        for _target, tier, task_type in escalated:
            metrics.escalations.labels(settings.env, task_type, tier).inc()

    async def _outcomes(conn, c: Caller, items: list) -> None:
        """Bytes and proxy errors that came with acks, fails and releases of routed
        tasks: usage per account, connect failures, account states.

        Each is charged to the route the task ran on, as the worker reports it (target
        lease, account, egress), not to what the target holds by now. An error from a route
        that is no longer current (the exit or pool removed, or the account's credentials
        rotated since the route was rendered) says nothing about the exit or account: it
        isn't counted, and the task goes back for a fresh route."""
        routed = [
            i
            for i in items
            if i.bytes is not None
            or getattr(i, "proxy_error", None)
            or getattr(i, "route", None) is not None
        ]
        routed = [i for i in routed if i.task_type in registry.target_leased]
        routed = await _exit_outcomes(conn, routed)
        if not routed or not await proxy_tables_exist(conn, settings.egress_schema):
            return
        targets = await dispatcher.targets_of(conn, [(i.task_type, i.key) for i in routed])
        legacy = [i for i in routed if getattr(i, "route", None) is None]
        leases = await dispatcher.leases_of(
            conn,
            sorted(
                {targets[(i.task_type, i.key)] for i in legacy if (i.task_type, i.key) in targets}
            ),
        )
        states = await admin.route_states(
            conn,
            {(i.route.account, i.route.egress) for i in routed if getattr(i, "route", None)},
        )
        usage = []
        ok_accounts: set[str] = set()
        release: list[tuple[str, UUID]] = []
        cfg = None
        for i in routed:
            t = targets.get((i.task_type, i.key))
            r = getattr(i, "route", None)
            stale = False
            if r is not None:
                st = states.get((r.account, r.egress))
                if st is None:
                    log.warning(
                        "outcome for an unknown route",
                        extra={"fields": {"account": r.account, "egress": r.egress}},
                    )
                    continue
                account, key, lease_id = r.account, r.egress, r.lease_id
                stale = not st["current"] or (
                    r.credentials_fp is not None and r.credentials_fp != st["fp"]
                )
            else:
                lease = leases.get(t) if t else None
                if lease is None or lease["account"] is None:
                    continue
                account, key, lease_id = lease["account"], lease["egress"], lease["lease_id"]
            if i.bytes:
                usage.append(UsageRow(account, key, i.task_type, i.bytes))
                metrics.proxy_bytes.labels(settings.env, account, i.task_type).inc(i.bytes)
            err = getattr(i, "proxy_error", None)
            if err is None:
                if not stale:
                    ok_accounts.add(account)
                    await admin.record_success(conn, account, key)
                continue
            if stale:
                metrics.proxy_errors.labels(settings.env, account, "stale_route").inc()
                if t and err.kind != "gateway":
                    release.append((t, lease_id))
                continue
            cfg = cfg or (await dispatcher.budgets(conn)).proxies
            what = await admin.record_proxy_error(
                conn,
                account,
                key,
                err.status,
                redact_text(err.excerpt),
                kind=err.kind,
                target=t,
                max_failures=cfg.max_connect_failures,
                unreachable_refs=cfg.unreachable_exits,
                window_s=cfg.unreachable_window.total_seconds(),
                gateway_targets=cfg.gateway_targets,
                gateway_window_s=cfg.gateway_window.total_seconds(),
            )
            metrics.proxy_errors.labels(settings.env, account, what).inc()
            if what == "disabled":
                metrics.proxy_disabled.labels(settings.env, account).inc()
            # A refusal or connect failure: released, not cooled (the target did nothing
            # wrong). A gateway answer keeps the lease: the task failed with its backoff.
            if t and err.kind != "gateway":
                release.append((t, lease_id))
        await dispatcher.release_targets(conn, release)
        await admin.record_usage(conn, usage)
        await admin.clear_failures(conn, ok_accounts)

    async def _exit_outcomes(conn, items: list) -> list:
        """Outcomes of routes through registry exits (no proxy account): bytes and errors
        are counted per exit; a refusal or connect failure releases the target (not
        cooled), as for proxies. Returns the other items."""
        rest, release = [], []
        exit_items = []
        for i in items:
            r = getattr(i, "route", None)
            if r is not None and r.account == EXIT_ACCOUNT:
                exit_items.append(i)
            else:
                rest.append(i)
        if not exit_items:
            return rest
        targets = await dispatcher.targets_of(conn, [(i.task_type, i.key) for i in exit_items])
        for i in exit_items:
            if i.bytes:
                metrics.proxy_bytes.labels(settings.env, EXIT_ACCOUNT, i.task_type).inc(i.bytes)
            err = getattr(i, "proxy_error", None)
            if err is None:
                continue
            metrics.proxy_errors.labels(settings.env, EXIT_ACCOUNT, err.kind).inc()
            log.warning(
                "exit error",
                extra={
                    "fields": {"egress": i.route.egress, "kind": err.kind, "status": err.status}
                },
            )
            t = targets.get((i.task_type, i.key))
            if t and err.kind != "gateway":
                release.append((t, i.route.lease_id))
        await dispatcher.release_targets(conn, release)
        return rest

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
            await _outcomes(conn, c, req.tasks)
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
        cool = [
            (t.task_type, t.key, t.retry_after_s or 0, t.reason, t.cause)
            for t in req.tasks
            if t.cooldown
        ]
        async with pool.connection() as conn:
            n, unknown = await frontier.fail(conn, items)
            await _cool(conn, cool, req.egress)
            await _outcomes(conn, c, req.tasks)
        _count_applied(req.tasks, unknown, "failed")
        return AppliedResponse(applied=n, unknown=_refs(unknown))

    @app.post("/v1/tasks/release", response_model=AppliedResponse)
    async def release(req: ReleaseRequest, c: CallerDep):
        _check_refs(c, req.leases)
        after = {r.to_store(): r.after_s for r in req.leases if r.after_s is not None}
        cool = [
            (r.task_type, r.key, r.after_s or 0, r.reason or "pushback", r.cause)
            for r in req.leases
            if r.cooldown and r.proxy_error is None
        ]
        async with pool.connection() as conn:
            n, unknown = await frontier.release(
                conn, [r.to_store() for r in req.leases], after=after
            )
            await _cool(conn, cool, req.egress)
            await _outcomes(conn, c, req.leases)
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

    # --- egress ---

    @app.get("/v1/egress/providers")
    async def egress_providers(c: CallerDep):
        async with pool.connection() as conn:
            try:
                return await provider_rules(conn, settings.egress_schema)
            except EgressNotReady as e:
                raise HTTPException(503, str(e)) from None

    @app.get("/v1/egress/accounts")
    async def egress_accounts(c: CallerDep):
        async with pool.connection() as conn:
            if not await proxy_tables_exist(conn, settings.egress_schema):
                raise HTTPException(503, "the egress schema has no proxy tables yet")
            accounts = await load_accounts(conn, settings.egress_schema)
        return {
            "accounts": [
                account_view(a) for name, a in sorted(accounts.items()) if settings.env in a.envs
            ]
        }

    register_proxy_routes(
        app, pool, admin, dispatcher, registry, settings, caller, keyring, probe_transport, adapters
    )

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
