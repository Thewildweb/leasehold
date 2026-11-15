"""Scheduled components: one run per Nomad periodic launch.

    from leasehold.runtime import run_scheduled

    def plan(ctx):
        ctx.upsert("certlog.chunk", new_chunks())

    if __name__ == "__main__":
        raise SystemExit(run_scheduled(plan))

The run gets the same Context as other components (``emit``, ``upsert``,
``log``). A component with ``reads`` in service.yml also gets ``run.clickhouse``
(its read-only ClickHouse user, created on first use) and ``run.table(ref)``, which
names a declared table in this environment (``'classifier.verdicts'`` →
``dev_classifier.verdicts``). It ends with exit code 0 when ``fn`` returned and every emitted event
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

from leasehold import naming
from leasehold.runtime import resources
from leasehold.runtime.config import ConfigError
from leasehold.runtime.context import Context, build

log = logging.getLogger("leasehold")


class RunTimeout(Exception):
    pass


class _RunContext:
    def __init__(self, ctx: Context, clickhouse=None):
        self._ctx = ctx
        self.cfg = ctx.cfg
        self.log = ctx.log
        self.client = ctx.client
        self.stopping = ctx.stopping
        self.deliveries = []
        self._clickhouse = clickhouse

    def _reads(self) -> list[str]:
        comp = self.cfg.component_spec
        if not getattr(comp, "reads_clickhouse", False):
            raise ConfigError(f"{self.cfg.name} declares no reads in service.yml")
        return comp.clickhouse_reads

    @property
    def clickhouse(self):
        """Read-only ClickHouse client (``query(sql, **params)`` → rows as dicts)."""
        if self._clickhouse is None:
            from leasehold.runtime.planner import ClickHouse

            self._reads()
            self._clickhouse = ClickHouse.from_env(
                naming.planner_clickhouse_user(self.cfg.env, self.cfg.service, self.cfg.component)
            )
        return self._clickhouse

    def table(self, ref: str) -> str:
        from leasehold.runtime.planner import resolve_table

        spec = self.cfg.spec
        if spec.clickhouse is None:
            raise ConfigError(f"{spec.name} has no clickhouse section")
        return resolve_table(
            ref,
            service=self.cfg.service,
            env=self.cfg.env,
            database=naming.clickhouse_database(self.cfg.env, spec.clickhouse.database),
            reads=self._reads(),
            who=f"{self.cfg.name} (reads)",
        )

    def emit(self, topic, payload, **kw):
        d = self._ctx.emit(topic, payload, **kw)
        self.deliveries.append(d)
        return d.event_id

    def upsert(self, *a, **kw):
        return self._ctx.upsert(*a, **kw)


def run_scheduled(fn: Callable[[Any], Any], *, ctx: Context | None = None, clickhouse=None) -> int:
    """``clickhouse``: a client to use instead of the one from the environment (tests)."""
    ctx = ctx or build("scheduled")
    ctx.install_signal_handlers()
    run = _RunContext(ctx, clickhouse)
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
