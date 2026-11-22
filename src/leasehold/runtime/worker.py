"""The task-worker loop.

    from leasehold.runtime import Retry, run_worker

    def handle(task, ctx):
        data = fetch(task.key)                      # may raise Retry(...) or Permanent(...)
        ctx.emit("entries", {"log_name": ..., ...})

    if __name__ == "__main__":
        raise SystemExit(run_worker(handle, concurrency=8))

What the loop does:

- Leases tasks of the component's ``consumes_tasks`` in batches, keeping up to
  ``concurrency + prefetch`` leases, and runs ``handle`` for up to ``concurrency``
  of them at a time in threads.
- With ``group_limit``, holds at most that many tasks per task group (``NewTask.group``,
  e.g. the server a task talks to), so one group's backlog can't take every slot while
  other groups wait.
- Renews all held leases before half their duration has passed.
- After ``handle`` returns, waits until Kafka confirmed every event it emitted,
  then acks with the first event's ID. ``Retry`` fails the attempt (optionally
  with a retry time), ``Defer`` gives the task back untried, due again after a
  delay and with no attempt counted, ``Permanent`` marks the task dead, any other exception
  fails the attempt with the exception as reason. If a produce isn't
  confirmed, the attempt fails.
- A lost lease (expired, task type paused, or the control API unreachable past
  the lease's expiry) is dropped if it hasn't started; if it's running,
  ``task.lost`` is set so long-running handlers can stop early.
- Acks, fails and deferrals are sent in batches and kept until the control API accepts
  them.
- On SIGTERM: stop leasing, give back leases that haven't started, let running
  tasks finish for up to ``LEASEHOLD_DRAIN_TIMEOUT_S``, send the outcomes,
  give back what's still running, flush the producer.

Every attempt should emit one event, also when it fails: emit it before raising
``Retry`` or ``Permanent``. An attempt that emitted nothing is acked with a
generated event ID.

Task types with ``target_leases`` (tasks that talk to sites) lease with the machine's
egress (``LEASEHOLD_EGRESS_IP``; without it the worker doesn't start) and get an
exclusive lease on each task's target (``leasehold.egress.dispatch``). Per target the
worker runs one task at a time, at most one per the lease's interval, and starts none
after a safety margin before the lease expires (on its own monotonic clock). Target
leases are renewed with the task leases and given back when their tasks are done; a
lost one (expired, revoked, cooling down) stops its target at once: queued tasks go
back untried, running ones get ``task.lost``. ``Retry(..., cooldown=True)`` and
``Defer(..., cooldown=True)`` report pushback: the target cools down for every egress
for ``after``.

With the machine's egress agent (``LEASEHOLD_AGENT_SOCKET``; ``leasehold.agent``), the
worker talks only to the agent: it holds the target leases for every worker on the
machine, and the worker asks it for a token before each task's first request. Requests
after the first inside one task (a retry, a fallback, a page load costing N) call
``ctx.token(cost)``, which waits for the agent's leave; a lost target raises
``TargetLost`` (the task goes back untried). If the agent can't be reached the worker
waits and tries again; it never falls back to the control API.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from collections import Counter as Tally
from collections import deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from prometheus_client import Counter, Gauge, Histogram

from leasehold.client import (
    AckItem,
    ClientError,
    FailItem,
    LeasedTask,
    LeaseRef,
    ReleaseItem,
    TargetLease,
    Unavailable,
)
from leasehold.runtime import log as lh_log
from leasehold.runtime.config import ConfigError
from leasehold.runtime.context import Context, build
from leasehold.runtime.events import Delivery, ProduceError

log = logging.getLogger("leasehold")

TASKS = Counter("leasehold_worker_tasks", "Finished task attempts", ["task_type", "outcome"])
DURATION = Histogram(
    "leasehold_worker_task_seconds",
    "Time in the handler",
    ["task_type"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600),
)
RUNNING = Gauge("leasehold_worker_running", "Tasks in the handler now")
QUEUED = Gauge("leasehold_worker_queued", "Leased tasks waiting for a free slot")
LOST = Counter(
    "leasehold_worker_leases_lost", "Leases lost before the task finished", ["task_type"]
)
API_ERRORS = Counter("leasehold_control_api_errors", "Failed control API calls", ["call"])

PRODUCE_TIMEOUT = 120.0
MAX_IDLE_DELAY = 10.0


class Retry(Exception):
    """Fail this attempt; try again later (after ``after`` seconds, if given)."""

    def __init__(
        self, reason: str, after: float | timedelta | None = None, *, cooldown: bool = False
    ):
        super().__init__(reason)
        self.reason = reason
        self.after = timedelta(seconds=after) if isinstance(after, int | float) else after
        self.cooldown = cooldown  # the target pushed back: cool it fleet-wide for ``after``


class Defer(Exception):
    """Give this task back without trying it: no attempt is counted, and it is due
    again after ``after``. For tasks the worker chose not to start, e.g. because
    their target is cooling down; emit no event for them."""

    def __init__(self, reason: str, after: float | timedelta, *, cooldown: bool = False):
        super().__init__(reason)
        self.reason = reason
        self.after = timedelta(seconds=after) if isinstance(after, int | float) else after
        self.cooldown = cooldown  # the target pushed back: cool it fleet-wide for ``after``


class TargetLost(Defer):
    """The task's target may not be contacted any more (lease lost, revoked, cooling
    down): the task goes back untried and is due again at once."""

    def __init__(self, reason: str):
        super().__init__(f"target lost: {reason}", after=0)


class Permanent(Exception):
    """Fail this task for good: it becomes dead, with no more attempts."""


@dataclass
class Task:
    """A leased task, as the handler sees it."""

    task_type: str
    key: str
    payload: Any
    attempts: int
    priority: int
    country: str | None
    ref: LeaseRef
    group: str = ""
    provider: str = ""
    target: str = ""  # with target leases: the target this task's requests go to
    lost: threading.Event = field(default_factory=threading.Event)

    @property
    def is_lost(self) -> bool:
        return self.lost.is_set()


class TaskContext:
    """Passed to the handler: emit events about this task, log with its context."""

    def __init__(self, ctx: Context, task: Task, token_fn: Callable[[int], None] | None = None):
        self._ctx = ctx
        self.task = task
        self.log = ctx.log
        self.deliveries: list[Delivery] = []
        self._token_fn = token_fn

    def token(self, cost: int = 1) -> None:
        """Wait for leave to send one more request to this task's target (after the first,
        which the lease covers). ``cost``: tokens it uses, e.g. N for a browser page load.
        Raises ``TargetLost`` when the target may not be contacted any more. Without
        target leases it returns at once."""
        if self._token_fn is not None:
            self._token_fn(cost)

    def emit(self, topic: str, payload: Any, **kw) -> uuid.UUID:
        kw.setdefault("task_type", self.task.task_type)
        kw.setdefault("task_key", self.task.key)
        d = self._ctx.emit(topic, payload, **kw)
        self.deliveries.append(d)
        return d.event_id

    def upsert(self, *a, **kw):
        return self._ctx.upsert(*a, **kw)

    @property
    def first_event_id(self) -> uuid.UUID | None:
        return self.deliveries[0].event_id if self.deliveries else None


Handler = Callable[[Task, TaskContext], None]


@dataclass
class _Held:
    leased: LeasedTask
    expires: datetime
    duration: timedelta
    task: Task


TARGET_MARGIN_S = 5.0  # stop starting tasks on a target this long before its lease expires
EXTRA_TOKEN_TIMEOUT_S = 300.0  # ctx.token() gives up (TargetLost) after waiting this long


@dataclass
class _Target:
    lease: TargetLease
    expires: datetime
    duration: timedelta
    stop_at: float  # monotonic: no task on this target starts after this
    next_start: float = 0.0  # monotonic: the next task may start then
    busy: bool = False
    lost: str | None = None


def _stop_at(expires: datetime, now: datetime) -> float:
    return time.monotonic() + (expires - now).total_seconds() - TARGET_MARGIN_S


def _now() -> datetime:
    return datetime.now(UTC)


class Worker:
    def __init__(
        self,
        ctx: Context,
        handler: Handler,
        concurrency: int = 4,
        prefetch: int | None = None,
        group_limit: int | None = None,
    ):
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if group_limit is not None and group_limit < 1:
            raise ValueError("group_limit must be >= 1")
        comp = ctx.cfg.component_spec
        self.ctx = ctx
        self.client = ctx.client
        self.handler = handler
        self.concurrency = concurrency
        self.prefetch = concurrency if prefetch is None else prefetch
        self.group_limit = group_limit
        self.task_types = sorted(comp.consumes_tasks)
        own = ctx.cfg.spec.frontier.task_types
        self.targeted = {t for t in self.task_types if t in own and own[t].target_leases}
        self.egress = ctx.cfg.egress_ip
        self.via_agent = bool(getattr(self.client, "via_agent", False))
        if self.targeted and not self.egress and not self.via_agent:
            raise ConfigError(
                f"{', '.join(sorted(self.targeted))} use target leases: set LEASEHOLD_EGRESS_IP "
                "(the node's meta.egress_ip)"
            )
        self.targets: dict[uuid.UUID, _Target] = {}
        self.release_targets: set[uuid.UUID] = set()
        self.worker_id = ctx.cfg.instance
        self.queued: deque[_Held] = deque()
        self.running: dict[LeaseRef, _Held] = {}
        self.done: queue.Queue[tuple[_Held, AckItem | FailItem, str]] = queue.Queue()
        self.acks: list[AckItem] = []
        self.fails: list[FailItem] = []
        self.defers: list[ReleaseItem] = []
        self.wake = threading.Event()
        self.pool = ThreadPoolExecutor(concurrency, thread_name_prefix="leasehold-task")
        self.idle_delay = 0.0
        self.next_lease = 0.0
        self._warned_api = 0.0

    # --- main loop ----------------------------------------------------------------------------

    def run(self) -> int:
        log.info(
            "worker started",
            extra={
                "fields": {
                    "task_types": self.task_types,
                    "concurrency": self.concurrency,
                    "prefetch": self.prefetch,
                    "group_limit": self.group_limit,
                }
            },
        )
        while not self.ctx.stopping.is_set():
            self.step()
        self.drain()
        return 0

    def step(self, wait: float = 0.5) -> None:
        """One pass of the loop. Public for tests."""
        self.ctx.liveness.beat()
        self._collect()
        self._send_outcomes()
        self._renew()
        self._renew_targets()
        if not self.ctx.stopping.is_set():
            self._lease()
            self._start()
        self._tidy_targets()
        QUEUED.set(len(self.queued))
        RUNNING.set(len(self.running))
        self.wake.wait(wait)
        self.wake.clear()

    def drain(self) -> None:
        deadline = time.monotonic() + self.ctx.cfg.drain_timeout
        self._release([h.leased.ref for h in self.queued])
        self.queued.clear()
        while self.running and time.monotonic() < deadline:
            self.step(wait=0.2)
        self._collect()
        if self.running:
            log.warning(
                "tasks still running at shutdown; giving their leases back",
                extra={"fields": {"running": len(self.running)}},
            )
            self._release(list(self.running))
            self.running.clear()
        for _ in range(3):
            self._send_outcomes()
            if not self.acks and not self.fails and not self.defers:
                break
            time.sleep(1)
        if self.targets:
            self.release_targets |= {i for i, t in self.targets.items() if not t.lost}
            self._renew_targets()
        if self.acks or self.fails or self.defers:
            log.error(
                "outcomes not delivered at shutdown",
                extra={
                    "fields": {
                        "acks": len(self.acks),
                        "fails": len(self.fails),
                        "defers": len(self.defers),
                    }
                },
            )
        self.pool.shutdown(wait=False, cancel_futures=True)
        log.info("worker stopped")

    # --- steps --------------------------------------------------------------------------------

    def _api_error(self, call: str, e: Exception) -> None:
        API_ERRORS.labels(call).inc()
        if time.monotonic() - self._warned_api > 30:
            self._warned_api = time.monotonic()
            log.warning(
                "control API call failed", extra={"fields": {"call": call, "error": str(e)}}
            )

    def _lease(self) -> None:
        want = self.concurrency + self.prefetch - len(self.queued) - len(self.running)
        if want <= 0 or len(self.queued) >= self.concurrency or time.monotonic() < self.next_lease:
            return
        targets: list[TargetLease] = []
        try:
            if self.targeted:
                leased, targets = self.client.lease_with_targets(
                    want,
                    task_types=self.task_types,
                    worker_id=self.worker_id,
                    per_group=self.group_limit,
                    held=self.held_per_group() if self.group_limit else None,
                    egress=self.egress,
                )
            elif self.group_limit is None:
                leased = self.client.lease(
                    want, task_types=self.task_types, worker_id=self.worker_id
                )
            else:
                leased = self.client.lease(
                    want,
                    task_types=self.task_types,
                    worker_id=self.worker_id,
                    per_group=self.group_limit,
                    held=self.held_per_group(),
                )
        except (Unavailable, ClientError) as e:
            self._api_error("lease", e)
            self._idle()
            return
        if not leased:
            self._idle()
            return
        self.idle_delay = 0.0
        now = _now()
        for tl in targets:
            have = self.targets.get(tl.lease_id)
            if have is None:
                self.targets[tl.lease_id] = _Target(
                    lease=tl,
                    expires=tl.expires_at,
                    duration=max(tl.expires_at - now, timedelta(seconds=10)),
                    stop_at=_stop_at(tl.expires_at, now),
                )
            elif not have.lost:
                have.expires = max(have.expires, tl.expires_at)
                have.stop_at = _stop_at(have.expires, now)
        for t in leased:
            self.queued.append(
                _Held(
                    leased=t,
                    expires=t.lease_expires_at,
                    duration=max(t.lease_expires_at - now, timedelta(seconds=10)),
                    task=Task(
                        t.task_type,
                        t.key,
                        t.payload,
                        t.attempts,
                        t.priority,
                        t.country,
                        t.ref,
                        t.group,
                        t.provider,
                        t.target,
                    ),
                )
            )

    def held_per_group(self) -> dict[str, int]:
        """Leases held now (queued and running), per task group."""
        held = list(self.queued) + list(self.running.values())
        return dict(Tally(h.task.group for h in held))

    def _idle(self) -> None:
        self.idle_delay = min(max(self.idle_delay * 2, 0.5), MAX_IDLE_DELAY)
        self.next_lease = time.monotonic() + self.idle_delay

    def _start(self) -> None:
        if not self.targets:
            while self.queued and len(self.running) < self.concurrency:
                self._launch(self.queued.popleft())
            return
        now = time.monotonic()
        kept: deque[_Held] = deque()
        while self.queued:
            held = self.queued.popleft()
            tid = held.leased.target_lease_id
            if tid is None:
                if len(self.running) < self.concurrency:
                    self._launch(held)
                else:
                    kept.append(held)
                continue
            t = self.targets.get(tid)
            if t is None or t.lost or now >= t.stop_at:
                # Not started, and the target may not be contacted: back untried.
                self.defers.append(ReleaseItem(held.leased.ref))
                TASKS.labels(held.task.task_type, "target_lost").inc()
                continue
            if len(self.running) >= self.concurrency or t.busy or now < t.next_start:
                kept.append(held)
                continue
            if self.via_agent:
                # The agent paces the target for every worker on the machine.
                try:
                    r = self.client.agent_token(tid, held.task.task_type, first=True)
                except (Unavailable, ClientError) as e:
                    self._api_error("agent_token", e)
                    kept.append(held)
                    t.next_start = now + 1.0
                    continue
                if r.get("lost"):
                    self._target_lost(tid, r["lost"])
                    self.defers.append(ReleaseItem(held.leased.ref))
                    TASKS.labels(held.task.task_type, "target_lost").inc()
                    continue
                if not r.get("granted"):
                    t.next_start = now + float(r.get("wait_s") or 0.5)
                    kept.append(held)
                    continue
                t.busy = True
                self._launch(held)
                continue
            t.busy = True
            t.next_start = now + t.lease.interval_s
            self._launch(held)
        self.queued = kept

    def _extra_token(self, held: _Held, cost: int) -> None:
        """Runs in the handler's thread: wait for leave to send another request."""
        tid = held.leased.target_lease_id
        deadline = time.monotonic() + EXTRA_TOKEN_TIMEOUT_S
        while True:
            if held.task.lost.is_set():
                raise TargetLost("lease lost")
            t = self.targets.get(tid)  # type: ignore[arg-type]
            if t is None or t.lost:
                raise TargetLost(t.lost if t else "unknown target lease")
            if self.via_agent:
                try:
                    r = self.client.agent_token(tid, held.task.task_type, cost=cost)
                except (Unavailable, ClientError) as e:
                    self._api_error("agent_token", e)
                    r = {"granted": False, "wait_s": 1.0}
                if r.get("lost"):
                    raise TargetLost(r["lost"])
                if r.get("granted"):
                    return
                wait = float(r.get("wait_s") or 0.5)
            else:
                now = time.monotonic()
                if now >= t.stop_at:
                    raise TargetLost("target lease ends")
                if now >= t.next_start:
                    t.next_start = now + t.lease.interval_s
                    return
                wait = t.next_start - now
            if time.monotonic() + wait > deadline:
                raise TargetLost("no leave to send within the time limit")
            if self.ctx.stopping.wait(min(wait, 5.0)):
                raise TargetLost("worker stopping")

    def _launch(self, held: _Held) -> None:
        self.running[held.leased.ref] = held
        self.pool.submit(self._run_one, held)

    def _run_one(self, held: _Held) -> None:
        task = held.task
        token_fn = None
        if held.leased.target_lease_id is not None:
            token_fn = lambda cost: self._extra_token(held, cost)  # noqa: E731
        tctx = TaskContext(self.ctx, task, token_fn)
        t0 = time.monotonic()
        with lh_log.scope(task_type=task.task_type, task_key=task.key):
            outcome, label = self._attempt(task, tctx)
        DURATION.labels(task.task_type).observe(time.monotonic() - t0)
        self.done.put((held, outcome, label))
        self.wake.set()

    def _attempt(
        self, task: Task, tctx: TaskContext
    ) -> tuple[AckItem | FailItem | ReleaseItem, str]:
        try:
            self.handler(task, tctx)
            for d in tctx.deliveries:
                d.wait(PRODUCE_TIMEOUT)
            event_id = tctx.first_event_id or uuid.uuid4()
            return AckItem(task.ref, event_id), "acked"
        except Defer as e:
            self._confirm_quietly(tctx)
            return ReleaseItem(task.ref, e.after, e.cooldown, e.reason), "deferred"
        except Retry as e:
            self._confirm_quietly(tctx)
            return FailItem(
                task.ref, e.reason, tctx.first_event_id, e.after, cooldown=e.cooldown
            ), "retry"
        except Permanent as e:
            self._confirm_quietly(tctx)
            return FailItem(
                task.ref, str(e) or "permanent", tctx.first_event_id, permanent=True
            ), "permanent"
        except ProduceError as e:
            log.error("event not delivered", extra={"fields": {"error": str(e)}})
            return FailItem(task.ref, f"produce failed: {e}"), "produce_failed"
        except Exception as e:  # noqa: BLE001 - a handler bug fails the attempt, not the worker
            log.exception("handler failed")
            self._confirm_quietly(tctx)
            return FailItem(task.ref, f"{type(e).__name__}: {e}", tctx.first_event_id), "error"

    @staticmethod
    def _confirm_quietly(tctx: TaskContext) -> None:
        for d in tctx.deliveries:
            try:
                d.wait(PRODUCE_TIMEOUT)
            except ProduceError:
                pass

    def _collect(self) -> None:
        while True:
            try:
                held, outcome, label = self.done.get_nowait()
            except queue.Empty:
                return
            self.running.pop(held.leased.ref, None)
            TASKS.labels(held.task.task_type, label).inc()
            tid = held.leased.target_lease_id
            if tid is not None and tid in self.targets:
                t = self.targets[tid]
                t.busy = False
                if getattr(outcome, "cooldown", False) and not t.lost:
                    # Stop here at once; the API cools the target for every egress.
                    self._target_lost(tid, "pushback")
            if isinstance(outcome, AckItem):
                self.acks.append(outcome)
            elif isinstance(outcome, ReleaseItem):
                self.defers.append(outcome)
            else:
                self.fails.append(outcome)

    def _send_outcomes(self) -> None:
        if self.acks:
            try:
                r = self.client.ack(self.acks)
                if r.unknown:
                    log.info(
                        "acks for leases that were gone",
                        extra={"fields": {"count": len(r.unknown)}},
                    )
                self.acks = []
            except (Unavailable, ClientError) as e:
                self._api_error("ack", e)
        if self.fails:
            try:
                self.client.fail(self.fails, **self._egress_kw())
                self.fails = []
            except (Unavailable, ClientError) as e:
                self._api_error("fail", e)
        if self.defers:
            try:
                self.client.release(self.defers, **self._egress_kw())
                self.defers = []
            except (Unavailable, ClientError) as e:
                self._api_error("release", e)

    def _renew(self) -> None:
        held = list(self.queued) + list(self.running.values())
        if not held:
            return
        now = _now()
        if all(h.expires - now > h.duration / 2 for h in held):
            return
        try:
            renewed, lost = self.client.renew([h.leased.ref for h in held])
        except (Unavailable, ClientError) as e:
            self._api_error("renew", e)
            # Fail closed: past its expiry a lease is gone, whatever the API says later.
            self._mark_lost([h.leased.ref for h in held if h.expires <= now])
            return
        for h in held:
            if h.leased.ref in renewed:
                h.expires = renewed[h.leased.ref]
        self._mark_lost(lost)

    def _egress_kw(self) -> dict:
        return {"egress": self.egress} if self.targeted else {}

    def _renew_targets(self) -> None:
        if not self.targets or self.egress is None or self.via_agent:
            return  # with an agent, the agent renews and gives back target leases
        now = _now()
        active = {
            i: t for i, t in self.targets.items() if not t.lost and i not in self.release_targets
        }
        due = any(t.expires - now <= t.duration / 2 for t in active.values())
        if not due and not self.release_targets:
            return
        release = sorted(self.release_targets)
        try:
            renewed, lost = self.client.renew_targets(
                self.egress, sorted(active), release, worker_id=self.worker_id
            )
        except (Unavailable, ClientError) as e:
            self._api_error("renew_targets", e)
            # Fail closed: past the stop time a target is not contacted, whatever comes later.
            mono = time.monotonic()
            for i, t in active.items():
                if mono >= t.stop_at:
                    self._target_lost(i, "not renewed in time")
            return
        for i in release:
            self.targets.pop(i, None)
        self.release_targets.difference_update(release)
        for i, expires in renewed.items():
            t = self.targets.get(i)
            if t is not None and not t.lost:
                t.expires = expires
                t.stop_at = _stop_at(expires, now)
        for i, why in lost.items():
            self._target_lost(i, why)

    def _target_lost(self, lease_id: uuid.UUID, why: str) -> None:
        t = self.targets.get(lease_id)
        if t is None or t.lost:
            return
        t.lost = why
        for h in self.running.values():
            if h.leased.target_lease_id == lease_id and not h.task.lost.is_set():
                h.task.lost.set()
                LOST.labels(h.task.task_type).inc()
        log.info(
            "target lease lost",
            extra={"fields": {"target": t.lease.target, "reason": why}},
        )

    def _tidy_targets(self) -> None:
        """Give back target leases whose tasks are all done; forget lost ones."""
        if not self.targets:
            return
        busy = {
            h.leased.target_lease_id
            for h in list(self.queued) + list(self.running.values())
            if h.leased.target_lease_id is not None
        }
        for i, t in list(self.targets.items()):
            if i in busy:
                continue
            if t.lost or self.via_agent:
                del self.targets[i]
            else:
                self.release_targets.add(i)

    def _mark_lost(self, refs: list[LeaseRef]) -> None:
        if not refs:
            return
        gone = set(refs)
        kept: deque[_Held] = deque()
        for h in self.queued:
            if h.leased.ref in gone:
                LOST.labels(h.task.task_type).inc()
            else:
                kept.append(h)
        self.queued = kept
        for ref in gone:
            h = self.running.get(ref)
            if h is not None and not h.task.lost.is_set():
                h.task.lost.set()
                LOST.labels(h.task.task_type).inc()
        log.info("leases lost", extra={"fields": {"count": len(gone)}})

    def _release(self, refs: list[LeaseRef]) -> None:
        if not refs:
            return
        try:
            self.client.release(refs)
        except (Unavailable, ClientError) as e:
            self._api_error("release", e)


def run_worker(
    handler: Handler,
    *,
    concurrency: int = 4,
    prefetch: int | None = None,
    group_limit: int | None = None,
    ctx: Context | None = None,
) -> int:
    """Run a task-worker until SIGTERM. Returns the process exit code.

    ``group_limit``: hold at most this many tasks per task group (see the module doc)."""
    ctx = ctx or build("task-worker")
    ctx.install_signal_handlers()
    ctx.serve_http()
    try:
        return Worker(ctx, handler, concurrency, prefetch, group_limit).run()
    finally:
        ctx.close()
