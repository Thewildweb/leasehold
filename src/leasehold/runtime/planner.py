"""Planners: scheduled components that top up a task type's backlog from ClickHouse.

    from leasehold.runtime import run_planner

    def select(run):
        rows = run.clickhouse.query(
            f'''
            SELECT base_domain, toString(first_ingested) AS mark
            FROM {run.table("certlog.base_domains")}
            WHERE tld = 'nl' AND toString(first_ingested) > {{watermark:String}}
            ORDER BY first_ingested
            LIMIT {{limit:UInt32}}
            ''',
            watermark=run.watermark or "",
            limit=run.limit,
        )
        if rows:
            run.advance(rows[-1]["mark"])
        return [r["base_domain"] for r in rows]

    if __name__ == "__main__":
        raise SystemExit(run_planner(select))

The component's ``planner`` block in ``service.yml`` sets the task type, the backlog
target, the batch size, the priority and the tables the selection may read. One run:

1. Reads the task type's backlog (eligible + leased) from the control API. At or above
   ``backlog_target``: records a ``skipped`` run and exits 0.
2. Calls ``select(run)``. ``run.limit`` is ``min(batch_max, backlog_target - backlog)``;
   ``run.watermark`` is where the last successful run stopped (``None`` on the first run
   or with ``LEASEHOLD_PLANNER_FULL=1``). ``select`` returns keys (``str``) or
   ``NewTask``s (for a payload or group); the planner's priority always applies.
3. Upserts them in chunks with ``reopen``, so a key whose ``done`` row is still in the
   frontier is queued again (the selection decides what is due, not the frontier).
4. Emits one plan-log event per chunk (the keys) and one per run (counts, watermark),
   to ``<service>.planned.v1``; ClickHouse keeps them in ``planned`` and
   ``planner_runs`` in the service's database.

The watermark is an opaque string the selection sets with ``run.advance(value)``: an
ingest time, or a cursor in a table's sort order. A run that fails records no
watermark, so the next run starts where the last successful one stopped.

ClickHouse access: ``LEASEHOLD_CLICKHOUSE_URL`` (HTTP interface),
``LEASEHOLD_CLICKHOUSE_USER`` (default: the planner's user from ``leasehold.naming``) and
``LEASEHOLD_CLICKHOUSE_PASSWORD``. The user is read-only and may read only the tables
in ``reads`` and the service's own tables; ``run.table()`` refuses anything else, so a
missing declaration shows up in tests, not as a grant error in production.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field

from leasehold import naming
from leasehold.client import NewTask
from leasehold.runtime.config import ConfigError
from leasehold.runtime.context import Context, build
from leasehold.runtime.scheduled import run_scheduled
from leasehold.spec.models import Planner, Scheduled

log = logging.getLogger("leasehold")

CHUNK = 1000  # keys per upsert and per plan-log event


class PlanLogEntry(BaseModel):
    """Payload of ``<service>.planned.v1``: the keys of one chunk, or one run's record."""

    kind: Literal["keys", "run"]
    planner: str
    task_type: str
    run_id: uuid.UUID
    priority: int | None = None
    keys: list[str] = Field(default_factory=list)
    status: Literal["ok", "skipped", "failed"] | None = None
    full: bool = False
    backlog_before: int | None = None
    backlog_target: int | None = None
    selected: int | None = None
    inserted: int | None = None
    updated: int | None = None
    watermark_from: str | None = None
    watermark: str | None = None
    duration_ms: int | None = None
    error_short: str | None = None


class ClickHouseError(RuntimeError):
    pass


class ClickHouse:
    """Read-only queries over ClickHouse's HTTP interface.

    Parameters use ClickHouse's own syntax (``{name:Type}`` in the SQL, passed as
    keyword arguments), so values are never formatted into the query text. Each
    parameter may be up to 128 KiB (ClickHouse's form field limit), e.g. ~3,000 domains.
    """

    def __init__(self, url: str, user: str, password: str, *, timeout: float = 300.0):
        self._http = httpx.Client(base_url=url, auth=(user, password), timeout=timeout)

    @classmethod
    def from_env(cls, default_user: str, environ: dict[str, str] | None = None) -> ClickHouse:
        e = os.environ if environ is None else environ
        url = e.get("LEASEHOLD_CLICKHOUSE_URL")
        if not url:
            raise ConfigError("set LEASEHOLD_CLICKHOUSE_URL")
        return cls(
            url,
            e.get("LEASEHOLD_CLICKHOUSE_USER") or default_user,
            e.get("LEASEHOLD_CLICKHOUSE_PASSWORD", ""),
        )

    def query(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        """Rows as dicts (``FORMAT JSONEachRow`` is added)."""
        # Parameters go in the body (multipart form fields), not the URL: a list of a few
        # thousand keys is too long for a URL. ClickHouse takes up to 128 KiB per field.
        fields = {"query": (None, sql)} | {
            f"param_{k}": (None, _param(v)) for k, v in params.items()
        }
        resp = self._http.post("/", params={"default_format": "JSONEachRow"}, files=fields)
        if resp.status_code != 200:
            raise ClickHouseError(resp.text.strip()[:500])
        return [json.loads(line) for line in resp.text.splitlines() if line]

    def close(self) -> None:
        self._http.close()


def _quoted(text: str) -> str:
    """A string inside an array parameter: ClickHouse's quoting, not JSON's."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _param(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, list | tuple):
        return "[" + ",".join(_quoted(v) if isinstance(v, str) else str(v) for v in value) + "]"
    return str(value)


class PlannerRun:
    """What ``select`` gets: limits, watermark, ClickHouse and table names."""

    def __init__(
        self,
        *,
        name: str,
        planner: Planner,
        env: str,
        service: str,
        database: str,
        clickhouse: ClickHouse,
        limit: int,
        watermark: str | None,
        full: bool,
        now: datetime,
        log: logging.Logger,
    ):
        self.name = name
        self.planner = planner
        self.clickhouse = clickhouse
        self.limit = limit
        self.watermark = watermark
        self.full = full
        self.now = now
        self.log = log
        self._env = env
        self._service = service
        self._database = database
        self._advanced: str | None = None

    @property
    def exclude_since(self) -> datetime | None:
        """Keys planned after this time are still in flight (``exclude_planned_for``)."""
        w = self.planner.exclude_planned_for
        return self.now - w if w else None

    @property
    def plan_log(self) -> str:
        """This service's plan-log table (one row per key queued)."""
        return f"{self._database}.{naming.PLAN_LOG_KEYS_SINK}"

    def recently_planned(self) -> str:
        """A subquery of the keys this planner queued within ``exclude_planned_for``, for
        ``AND key NOT IN (...)``. Empty result when the window isn't set."""
        since = self.exclude_since
        if since is None:
            return "SELECT '' AS key WHERE 0"
        return (
            f"SELECT key FROM {self.plan_log} WHERE planner = '{self.name}' "
            f"AND planned_at > toDateTime64('{_param(since)}', 3, 'UTC')"
        )

    def table(self, ref: str) -> str:
        """The environment's name of a declared table: ``'certlog.base_domains'`` →
        ``dev_certlog.base_domains`` in dev. Own tables may be given without the service."""
        return resolve_table(
            ref,
            service=self._service,
            env=self._env,
            database=self._database,
            reads=self.planner.reads,
            who=f"{self.name} (planner.reads)",
        )

    def advance(self, watermark: str) -> None:
        """Where the next run starts. Set it to the last row the selection returned."""
        self._advanced = watermark


def resolve_table(
    ref: str, *, service: str, env: str, database: str, reads: Sequence[str], who: str
) -> str:
    """The environment's name of a table a component declared in its reads. Own tables
    may be given without the service; the plan log's tables are always allowed."""
    if "." not in ref:
        ref = f"{service}.{ref}"
    owner, table = ref.split(".", 1)
    own = owner == service
    allowed = ref in reads or (
        own and table in (naming.PLAN_LOG_KEYS_SINK, naming.PLAN_LOG_RUNS_SINK)
    )
    if not allowed:
        raise ConfigError(f"{who} doesn't declare {ref!r}")
    if own:
        return f"{database}.{table}"
    # Other services' databases are named after the service's `clickhouse.database`,
    # which may differ from the service name; reads name the service, so the job gets
    # the mapping from LEASEHOLD_PLANNER_DATABASES (set by the Nomad job).
    # LEASEHOLD_PLANNER_ENVS names services read in another environment (reads_from).
    db = _read_databases().get(owner, owner)
    read_env = _read_json("LEASEHOLD_PLANNER_ENVS").get(owner, env)
    return f"{naming.clickhouse_database(read_env, db)}.{table}"


def _read_databases() -> dict[str, str]:
    return _read_json("LEASEHOLD_PLANNER_DATABASES")


def _read_json(var: str) -> dict[str, str]:
    raw = os.environ.get(var) or "{}"
    try:
        return json.loads(raw)
    except ValueError as e:
        raise ConfigError(f"{var} is not JSON: {e}") from None


def _short(e: BaseException) -> str:
    return f"{type(e).__name__}: {e}"[:300]


def _backlog(ctx: Context, task_type: str) -> int:
    for t in ctx.client.task_types():
        if t["name"] == task_type:
            return int(t.get("eligible") or 0) + int(t.get("leased") or 0)
    raise ConfigError(f"the control API doesn't know task type {task_type!r}")


def _last_watermark(ch: ClickHouse, database: str, planner: str) -> str | None:
    rows = ch.query(
        f"SELECT watermark FROM {database}.{naming.PLAN_LOG_RUNS_SINK} "
        "WHERE planner = {planner:String} AND status = 'ok' AND watermark != '' "
        "ORDER BY finished_at DESC LIMIT 1",
        planner=planner,
    )
    return rows[0]["watermark"] if rows else None


def _as_tasks(items: Iterable[str | NewTask], priority: int) -> list[NewTask]:
    out = []
    for item in items:
        if isinstance(item, str):
            out.append(NewTask(key=item, priority=priority))
        else:
            out.append(
                NewTask(
                    key=item.key,
                    priority=priority,
                    payload=item.payload,
                    group=item.group,
                    country=item.country,
                )
            )
    return out


def _chunks(items: Sequence[NewTask], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def run_planner(
    select: Callable[[PlannerRun], Iterable[str | NewTask]],
    *,
    ctx: Context | None = None,
    clickhouse: ClickHouse | None = None,
    full: bool | None = None,
) -> int:
    """Run one planner pass. Exit code 0 on success (also when the backlog is full)."""
    ctx = ctx or build("scheduled")
    cfg = ctx.cfg
    comp = cfg.component_spec
    if not isinstance(comp, Scheduled) or comp.planner is None:
        raise ConfigError(f"{cfg.name} has no planner block in service.yml")
    planner = comp.planner
    database = naming.clickhouse_database(cfg.env, cfg.spec.clickhouse.database)
    if full is None:
        full = os.environ.get("LEASEHOLD_PLANNER_FULL", "") in ("1", "true", "yes")

    def plan(run_ctx) -> dict[str, Any]:
        ch = clickhouse or ClickHouse.from_env(
            naming.planner_clickhouse_user(cfg.env, cfg.service, cfg.component)
        )
        run_id = uuid.uuid4()
        t0 = time.monotonic()
        record: dict[str, Any] = {
            "kind": "run",
            "planner": cfg.component,
            "task_type": planner.task_type,
            "run_id": run_id,
            "full": full,
            "backlog_target": planner.backlog_target,
        }

        def finish(status: str, **fields: Any) -> dict[str, Any]:
            record.update(status=status, duration_ms=int((time.monotonic() - t0) * 1000), **fields)
            run_ctx.emit(naming.PLAN_LOG_TOPIC, record)
            return {k: v for k, v in record.items() if k not in ("kind", "run_id")}

        try:
            backlog = _backlog(ctx, planner.task_type)
            record["backlog_before"] = backlog
            room = planner.backlog_target - backlog
            if room <= 0:
                return finish("skipped", selected=0, inserted=0, updated=0)
            watermark = None if full else _last_watermark(ch, database, cfg.component)
            record["watermark_from"] = watermark or ""
            run = PlannerRun(
                name=cfg.component,
                planner=planner,
                env=cfg.env,
                service=cfg.service,
                database=database,
                clickhouse=ch,
                limit=min(planner.batch_max, room),
                watermark=watermark,
                full=full,
                now=datetime.now(UTC),
                log=ctx.log,
            )
            tasks = _as_tasks(select(run), planner.priority)
            if len(tasks) > run.limit:
                raise ValueError(f"select returned {len(tasks)} keys, more than limit {run.limit}")
            inserted = updated = 0
            for chunk in _chunks(tasks, CHUNK):
                ins, upd = run_ctx.upsert(planner.task_type, chunk, reopen=True)
                inserted, updated = inserted + ins, updated + upd
                run_ctx.emit(
                    naming.PLAN_LOG_TOPIC,
                    {
                        "kind": "keys",
                        "planner": cfg.component,
                        "task_type": planner.task_type,
                        "run_id": run_id,
                        "priority": planner.priority,
                        "keys": [t.key for t in chunk],
                    },
                )
            # Without a new watermark, keep the old one so the next run starts there too.
            new_mark = run._advanced if run._advanced is not None else (watermark or "")
            return finish(
                "ok",
                selected=len(tasks),
                inserted=inserted,
                updated=updated,
                watermark=new_mark,
            )
        except Exception as e:
            try:
                finish("failed", error_short=_short(e))
            except Exception:  # noqa: BLE001 - the original error matters more
                log.exception("could not record the failed run")
            raise
        finally:
            if clickhouse is None:
                ch.close()

    return run_scheduled(plan, ctx=ctx)
