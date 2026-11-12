"""Scheduled components: one run per Nomad periodic launch.

    from leasehold.runtime import run_scheduled

    def plan(ctx):
        ctx.upsert("certlog.chunk", new_chunks())

    if __name__ == "__main__":
        raise SystemExit(run_scheduled(plan))

The run gets the same Context as other components (``emit``, ``upsert``,
``log``). It ends with exit code 0 when ``fn`` returned and every emitted event
was confirmed, and 1 otherwise. ``LEASEHOLD_TIMEOUT_S`` (the ``timeout`` in
service.yml) ends a run that takes too long with exit code 1. One log line
summarises each run (``run finished`` / ``run failed``) with its duration, the
container's peak memory and CPU time (periodic runs are too short to scrape).
"""

from __future__ import annotations

import logging
import signal
import time
from collections.abc import Callable
from typing import Any

from leasehold.runtime import resources
from leasehold.runtime.context import Context, build

log = logging.getLogger("leasehold")


class RunTimeout(Exception):
    pass


class _RunContext:
    def __init__(self, ctx: Context):
        self._ctx = ctx
        self.cfg = ctx.cfg
        self.log = ctx.log
        self.client = ctx.client
        self.stopping = ctx.stopping
        self.deliveries = []

    def emit(self, topic, payload, **kw):
        d = self._ctx.emit(topic, payload, **kw)
        self.deliveries.append(d)
        return d.event_id

    def upsert(self, *a, **kw):
        return self._ctx.upsert(*a, **kw)


def run_scheduled(fn: Callable[[Any], Any], *, ctx: Context | None = None) -> int:
    ctx = ctx or build("scheduled")
    ctx.install_signal_handlers()
    run = _RunContext(ctx)
    timeout = ctx.cfg.run_timeout

    def on_alarm(_signum, _frame):
        raise RunTimeout(f"run exceeded {timeout:g}s")

    if timeout:
        signal.signal(signal.SIGALRM, on_alarm)
        signal.setitimer(signal.ITIMER_REAL, timeout)
    t0 = time.monotonic()
    try:
        result = fn(run)
        for d in run.deliveries:
            d.wait(120)
        if timeout:
            signal.setitimer(signal.ITIMER_REAL, 0)
        fields = {
            "duration_s": round(time.monotonic() - t0, 2),
            "events": len(run.deliveries),
            **resources.summary(),
        }
        if isinstance(result, dict):
            fields |= result
        log.info("run finished", extra={"fields": fields})
        return 0
    except BaseException as e:  # noqa: BLE001 - includes RunTimeout and KeyboardInterrupt
        if timeout:
            signal.setitimer(signal.ITIMER_REAL, 0)
        log.exception(
            "run failed",
            extra={
                "fields": {
                    "duration_s": round(time.monotonic() - t0, 2),
                    "error_type": type(e).__name__,
                    **resources.summary(),
                }
            },
        )
        return 1
    finally:
        ctx.close()
