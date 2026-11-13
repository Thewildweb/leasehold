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
  with a retry time), ``Permanent`` marks the task dead, any other exception
  fails the attempt with the exception as reason. If a produce isn't
  confirmed, the attempt fails.
- A lost lease (expired, task type paused, or the control API unreachable past
  the lease's expiry) is dropped if it hasn't started; if it's running,
  ``task.lost`` is set so long-running handlers can stop early.
- Acks and fails are sent in batches and kept until the control API accepts
  them.
- On SIGTERM: stop leasing, give back leases that haven't started, let running
  tasks finish for up to ``LEASEHOLD_DRAIN_TIMEOUT_S``, send the outcomes,
  give back what's still running, flush the producer.

Every attempt should emit one event, also when it fails: emit it before raising
``Retry`` or ``Permanent``. An attempt that emitted nothing is acked with a
generated event ID.
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

from leasehold.client import AckItem, ClientError, FailItem, LeasedTask, LeaseRef, Unavailable
from leasehold.runtime import log as lh_log
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

    def __init__(self, reason: str, after: float | timedelta | None = None):
        super().__init__(reason)
        self.reason = reason
        self.after = timedelta(seconds=after) if isinstance(after, int | float) else after


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
    lost: threading.Event = field(default_factory=threading.Event)

    @property
    def is_lost(self) -> bool:
        return self.lost.is_set()


class TaskContext:
    """Passed to the handler: emit events about this task, log with its context."""

    def __init__(self, ctx: Context, task: Task):
        self._ctx = ctx
        self.task = task
        self.log = ctx.log
        self.deliveries: list[Delivery] = []

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
        self.worker_id = ctx.cfg.instance
        self.queued: deque[_Held] = deque()
        self.running: dict[LeaseRef, _Held] = {}
        self.done: queue.Queue[tuple[_Held, AckItem | FailItem, str]] = queue.Queue()
        self.acks: list[AckItem] = []
        self.fails: list[FailItem] = []
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
        if not self.ctx.stopping.is_set():
            self._lease()
            self._start()
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
            if not self.acks and not self.fails:
                break
            time.sleep(1)
        if self.acks or self.fails:
            log.error(
                "outcomes not delivered at shutdown",
                extra={"fields": {"acks": len(self.acks), "fails": len(self.fails)}},
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
        try:
            if self.group_limit is None:
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
        while self.queued and len(self.running) < self.concurrency:
            held = self.queued.popleft()
            self.running[held.leased.ref] = held
            self.pool.submit(self._run_one, held)

    def _run_one(self, held: _Held) -> None:
        task = held.task
        tctx = TaskContext(self.ctx, task)
        t0 = time.monotonic()
        with lh_log.scope(task_type=task.task_type, task_key=task.key):
            outcome, label = self._attempt(task, tctx)
        DURATION.labels(task.task_type).observe(time.monotonic() - t0)
        self.done.put((held, outcome, label))
        self.wake.set()

    def _attempt(self, task: Task, tctx: TaskContext) -> tuple[AckItem | FailItem, str]:
        try:
            self.handler(task, tctx)
            for d in tctx.deliveries:
                d.wait(PRODUCE_TIMEOUT)
            event_id = tctx.first_event_id or uuid.uuid4()
            return AckItem(task.ref, event_id), "acked"
        except Retry as e:
            self._confirm_quietly(tctx)
            return FailItem(task.ref, e.reason, tctx.first_event_id, e.after), "retry"
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
            if isinstance(outcome, AckItem):
                self.acks.append(outcome)
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
                self.client.fail(self.fails)
                self.fails = []
            except (Unavailable, ClientError) as e:
                self._api_error("fail", e)

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
