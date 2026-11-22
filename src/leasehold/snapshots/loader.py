"""One run of a snapshot dataset's loader (``leasehold snapshot load``).

The loader only sends SQL: ClickHouse lists and reads the bucket itself, through the
dataset's named collection. One run:

1. **Window and busy check.** Outside the load window, or when the environment's busy
   query returns more than 0 (a backup is running), exit 0: the next run in the window
   tries again. ``force`` skips both.
2. **Another run?** A ``started`` row of this dataset younger than the timeout without
   an end: exit 0.
3. **Clean up** what a crashed run left: ``_staging_*`` tables of the dataset's tables,
   and partitions of snapshots that never became current.
4. **Find work:** the complete manifests in the bucket newer than the current snapshot,
   not failed ``max_attempts`` times. A dataset with only replace tables loads the
   newest. One with append tables loads them in order, so a snapshot that fails holds
   back the ones after it until it has failed ``max_attempts`` times and is skipped
   (``leasehold snapshot retry`` lets it be tried again).
5. **Check the files** before touching anything: the manifest's tables are the
   dataset's, every file exists with its size, its columns are the manifest's.
6. **Load** each table into a new ``_staging_*`` table, compare its row count with the
   manifest and run the checks; then ``REPLACE PARTITION`` it into ``__snapshots`` (a
   partition no view shows yet) and drop the staging table.
7. **Switch:** one ``current`` row in ``snapshot_loads``; every view follows.
8. **Retention:** drop partitions of replace tables beyond ``keep`` snapshots.

Until step 7 nothing a reader sees has changed. On an error the run drops its staging
tables and its partitions, records ``failed`` and exits 1.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

from leasehold import naming
from leasehold.clickhouse.apply import ClickHouse, ClickHouseError
from leasehold.clickhouse.plan import _quote
from leasehold.snapshots.manifest import Manifest

log = logging.getLogger("leasehold.snapshots")


class LoadError(RuntimeError):
    pass


@dataclass
class Window:
    start: str = "00:00"  # HH:MM, inclusive
    end: str = "24:00"  # HH:MM, exclusive
    timezone: str = "UTC"
    skip_first_sunday: bool = False

    def allows(self, now: datetime) -> bool:
        local = now.astimezone(ZoneInfo(self.timezone))
        if self.skip_first_sunday and local.weekday() == 6 and local.day <= 7:
            return False
        hm = local.strftime("%H:%M")
        return self.start <= hm < self.end

    def cron(self) -> str:
        """Every 20 minutes in the window's hours (the job also checks minutes)."""
        sh = int(self.start[:2])
        eh = int(self.end[:2]) - (1 if self.end[3:] == "00" else 0)
        hours = f"{sh}-{eh}" if eh > sh else str(sh)
        return f"*/20 {hours} * * *"


@dataclass
class Settings:
    service: str
    dataset: str
    database: str
    bucket: str
    collection: str
    keep: int
    max_attempts: int
    timeout_s: int
    tables: dict[str, dict]  # name -> {mode, create, select}
    checks: list[list] = field(default_factory=list)  # [table, min_rows, max_change]
    window: Window = field(default_factory=Window)
    busy_query: str | None = None

    @classmethod
    def from_json(cls, text: str) -> Settings:
        d = json.loads(text)
        d["window"] = Window(**(d.get("window") or {}))
        return cls(**d)

    @property
    def replace_only(self) -> bool:
        return all(t["mode"] == "replace" for t in self.tables.values())


def _q(v: str) -> str:
    return _quote(v)


class Loader:
    def __init__(self, ch: ClickHouse, s: Settings, loader: str = "", now=None):
        self.ch = ch
        self.s = s
        self.loader = loader
        self._now = now or (lambda: datetime.now(ZoneInfo("UTC")))
        self.run_id = uuid.uuid4().hex
        self.loads = f"{s.database}.{naming.SNAPSHOT_LOADS_TABLE}"

    # --- small helpers ------------------------------------------------------------------

    def _scalar(self, sql: str) -> str:
        out = self.ch.execute(sql + " FORMAT TSV").strip()
        return out.splitlines()[0] if out else ""

    def _record(
        self,
        snapshot: str,
        status: str,
        *,
        rows=None,
        manifest: str = "",
        error: str = "",
        source_date: str | None = None,
    ) -> None:
        rows = rows or {}
        rmap = "map(" + ", ".join(f"{_q(k)}, {int(v)}" for k, v in sorted(rows.items())) + ")"
        sd = f"toDate({_q(source_date)})" if source_date else "NULL"
        self.ch.execute(
            f"INSERT INTO {self.loads} (dataset, snapshot, status, run_id, source_date, rows, "
            "manifest, error, loader) VALUES "
            f"({_q(self.s.dataset)}, {_q(snapshot)}, {_q(status)}, {_q(self.run_id)}, {sd}, "
            f"{rmap}, {_q(manifest)}, {_q(error[:4000])}, {_q(self.loader)})"
        )

    def source(self, key_or_glob: str, fmt: str) -> str:
        return f"s3({self.s.collection}, filename = {_q(key_or_glob)}, format = {_q(fmt)})"

    def current(self) -> str:
        return self._scalar(
            f"SELECT snapshot FROM {self.s.database}.{naming.SNAPSHOT_CURRENT_VIEW} "
            f"WHERE dataset = {_q(self.s.dataset)}"
        )

    def ever_current(self) -> set[str]:
        out = self.ch.rows(
            f"SELECT DISTINCT snapshot FROM {self.loads} WHERE dataset = {_q(self.s.dataset)} "
            "AND status = 'current'"
        )
        return {r[0] for r in out}

    def held(self) -> str:
        """The snapshot a rollback went back to, while the dataset is held; "" otherwise.
        A rollback holds the dataset: the loader loads nothing new until ``release``
        (or an explicit ``load --snapshot``, whose ``current`` row ends the hold)."""
        out = self.ch.rows(
            f"SELECT argMax(status, at), argMax(snapshot, at) FROM {self.loads} "
            f"WHERE dataset = {_q(self.s.dataset)} "
            "AND status IN ('current', 'rolled_back', 'released')"
        )
        if out and out[0][0] == "rolled_back":
            return out[0][1]
        return ""

    def failures(self) -> dict[str, int]:
        """Failed loads per snapshot since its last ``retry`` row."""
        rows = self.ch.rows(
            f"SELECT snapshot, countIf(status = 'failed' AND at > retry_at) FROM ("
            f"SELECT snapshot, status, at, maxIf(at, status = 'retry') "
            f"OVER (PARTITION BY snapshot) AS retry_at FROM {self.loads} "
            f"WHERE dataset = {_q(self.s.dataset)}) GROUP BY snapshot"
        )
        return {r[0]: int(r[1]) for r in rows}

    def partitions(self, table: str) -> set[str]:
        st = naming.snapshot_table(table)
        rows = self.ch.rows(
            "SELECT DISTINCT partition FROM system.parts WHERE active "
            f"AND database = {_q(self.s.database)} AND table = {_q(st)}"
        )
        return {r[0] for r in rows}

    # --- steps --------------------------------------------------------------------------

    def other_run_active(self) -> bool:
        n = self._scalar(
            f"SELECT count() FROM (SELECT run_id, groupArray(status) AS s, min(at) AS t "
            f"FROM {self.loads} WHERE dataset = {_q(self.s.dataset)} "
            f"AND at > now64(3, 'UTC') - INTERVAL {int(self.s.timeout_s)} SECOND "
            "GROUP BY run_id) WHERE has(s, 'started') AND NOT hasAny(s, ['current', 'failed'])"
        )
        return int(n or 0) > 0

    def clean_up(self) -> None:
        prefix = naming.SNAPSHOT_STAGING_PREFIX
        for t in self.s.tables:
            rows = self.ch.rows(
                f"SELECT name FROM system.tables WHERE database = {_q(self.s.database)} "
                f"AND startsWith(name, {_q(f'{prefix}{t}_')})"
            )
            for (name,) in rows:
                log.info("dropping a staging table left behind", extra={"fields": {"table": name}})
                self.ch.execute(f"DROP TABLE IF EXISTS {self.s.database}.{name} SYNC")
        ever = self.ever_current()
        for t in self.s.tables:
            for p in self.partitions(t) - ever:
                log.info(
                    "dropping a partition that never became current",
                    extra={"fields": {"table": t, "snapshot": p}},
                )
                self._drop_partition(t, p)

    def _drop_partition(self, table: str, snapshot: str) -> None:
        self.ch.execute(
            f"ALTER TABLE {self.s.database}.{naming.snapshot_table(table)} "
            f"DROP PARTITION {_q(snapshot)}"
        )

    def manifests(self) -> list[Manifest]:
        pattern = f"{self.s.dataset}/*/manifest.json"
        out = self.ch.execute(
            f"SELECT _path, json FROM {self.source(pattern, 'JSONAsString')} FORMAT JSONEachRow"
        )
        found = []
        for line in out.splitlines():
            row = json.loads(line)
            try:
                m = Manifest.model_validate_json(row["json"])
            except ValueError as e:
                log.warning(
                    "unreadable manifest", extra={"fields": {"path": row["_path"], "error": str(e)}}
                )
                continue
            expected = naming.snapshot_manifest_key(self.s.dataset, m.snapshot)
            if m.dataset != self.s.dataset or not row["_path"].endswith("/" + expected):
                log.warning("manifest in the wrong place", extra={"fields": {"path": row["_path"]}})
                continue
            found.append(m)
        return sorted(found, key=lambda m: m.snapshot)

    def pending(self, snapshot: str | None = None) -> list[Manifest]:
        manifests = self.manifests()
        if snapshot is not None:
            return [m for m in manifests if m.snapshot == snapshot]
        held = self.held()
        if held:
            log.info(
                f"held after a rollback to {held}; `leasehold snapshot release` loads again",
                extra={"fields": {"snapshot": held}},
            )
            return []
        current = self.current()
        failures = self.failures()
        ever = self.ever_current()
        todo = []
        for m in manifests:
            if current and m.snapshot <= current:
                continue
            if m.snapshot in ever:  # rolled back from: only an explicit load brings it back
                continue
            if failures.get(m.snapshot, 0) >= self.s.max_attempts:
                log.warning(
                    "not loaded: failed too often",
                    extra={"fields": {"snapshot": m.snapshot, "failures": failures[m.snapshot]}},
                )
                continue
            todo.append(m)
        if self.s.replace_only and todo:
            return todo[-1:]
        return todo

    def verify(self, m: Manifest) -> None:
        problems = m.check_keys()
        missing = sorted(set(self.s.tables) - set(m.tables))
        if missing:
            problems.append(f"tables missing from the manifest: {', '.join(missing)}")
        for t in self.s.tables:
            entry = m.tables.get(t)
            if entry is None:
                continue
            prefix = naming.snapshot_file_prefix(m.dataset, m.snapshot, t)
            listed = {
                r[0].split("/", 1)[1] if "/" in r[0] else r[0]: int(r[1])
                for r in self.ch.rows(
                    f"SELECT _path, _size FROM {self.source(prefix + '*', 'One')}"
                )
            }
            for f in entry.files:
                size = listed.get(f.key)
                if size is None:
                    problems.append(f"{t}: {f.key} is missing")
                elif size != f.bytes:
                    problems.append(f"{t}: {f.key} has {size} bytes, the manifest says {f.bytes}")
            if entry.files and not problems:
                cols = [
                    r[0]
                    for r in self.ch.rows(f"DESCRIBE {self.source(entry.files[0].key, 'Parquet')}")
                ]
                want = [c[0] for c in entry.columns]
                if cols != want:
                    problems.append(f"{t}: the file's columns {cols} aren't the manifest's {want}")
        if problems:
            raise LoadError("; ".join(problems))

    def load(self, m: Manifest) -> None:
        s = self.s
        sid = m.snapshot
        manifest_json = m.model_dump_json()
        self._record(sid, "started", manifest=manifest_json, source_date=m.source_date)
        staged: list[str] = []
        attached: list[str] = []
        current = self.current()
        try:
            self.verify(m)
            rows: dict[str, int] = {}
            for t, spec in sorted(s.tables.items()):
                staging = f"{s.database}.{naming.snapshot_staging_table(t, sid)}"
                self.ch.execute(f"DROP TABLE IF EXISTS {staging} SYNC")
                self.ch.execute(
                    f"CREATE TABLE {staging} AS {s.database}.{naming.snapshot_table(t)}"
                )
                staged.append(staging)
                files = [f.key for f in m.tables[t].files]
                if files:
                    glob = files[0] if len(files) == 1 else _brace(files)
                    select = (
                        spec["select"]
                        .replace("{source}", self.source(glob, "Parquet"), 1)
                        .replace("{snapshot}", _q(sid))
                    )
                    t0 = time.monotonic()
                    self.ch.execute(f"INSERT INTO {staging}\n{select}")
                    log.info(
                        "loaded into staging",
                        extra={"fields": {"table": t, "seconds": round(time.monotonic() - t0, 1)}},
                    )
                n = int(self._scalar(f"SELECT count() FROM {staging}") or 0)
                if n != m.tables[t].rows:
                    raise LoadError(f"{t}: {n} rows loaded, the manifest says {m.tables[t].rows}")
                rows[t] = n
            self.check(rows, current)
            for t in sorted(s.tables):
                staging = f"{s.database}.{naming.snapshot_staging_table(t, sid)}"
                self.ch.execute(
                    f"ALTER TABLE {s.database}.{naming.snapshot_table(t)} "
                    f"REPLACE PARTITION {_q(sid)} FROM {staging}"
                )
                attached.append(t)
                self.ch.execute(f"DROP TABLE IF EXISTS {staging} SYNC")
                staged.remove(staging)
            self._record(sid, "loaded", rows=rows, source_date=m.source_date)
        except Exception as e:
            self._undo(sid, staged, attached)
            self._record(sid, "failed", error=str(e), source_date=m.source_date)
            raise
        self._record(sid, "current", rows=rows, source_date=m.source_date)
        log.info("snapshot is current", extra={"fields": {"snapshot": sid, "rows": rows}})

    def check(self, rows: dict[str, int], current: str) -> None:
        problems = []
        for table, min_rows, max_change in self.s.checks:
            n = rows.get(table, 0)
            if min_rows is not None and n < min_rows:
                problems.append(f"{table}: {n} rows, fewer than {min_rows}")
            if max_change is not None and current:
                before = int(
                    self._scalar(
                        f"SELECT count() FROM {self.s.database}.{naming.snapshot_table(table)} "
                        f"WHERE snapshot = {_q(current)}"
                    )
                    or 0
                )
                if before and abs(n - before) / before > max_change:
                    problems.append(
                        f"{table}: {n} rows against {before} in {current}, a change of more "
                        f"than {max_change:.0%}"
                    )
        if problems:
            raise LoadError("checks failed: " + "; ".join(problems))

    def _undo(self, sid: str, staged: list[str], attached: list[str]) -> None:
        for name in staged:
            try:
                self.ch.execute(f"DROP TABLE IF EXISTS {name} SYNC")
            except ClickHouseError as e:
                log.error(
                    "couldn't drop a staging table",
                    extra={"fields": {"table": name, "error": str(e)}},
                )
        if sid in self.ever_current():
            return  # a reload of a snapshot that was current: its partitions stay
        for t in attached:
            try:
                self._drop_partition(t, sid)
            except ClickHouseError as e:
                log.error(
                    "couldn't drop a partition", extra={"fields": {"table": t, "error": str(e)}}
                )

    def retention(self) -> None:
        """Drop partitions of replace tables beyond ``keep`` snapshots that were current."""
        ever = sorted(self.ever_current())
        keep = set(ever[-self.s.keep :]) | {self.current()}
        dropped = set()
        for t, spec in self.s.tables.items():
            if spec["mode"] != "replace":
                continue
            for p in sorted((self.partitions(t) & set(ever)) - keep):
                log.info("dropping an old snapshot", extra={"fields": {"table": t, "snapshot": p}})
                self._drop_partition(t, p)
                dropped.add(p)
        for p in sorted(dropped):
            self._record(p, "dropped")

    def run(self, force: bool = False, snapshot: str | None = None) -> int:
        now = self._now()
        if not force and not self.s.window.allows(now):
            log.info("outside the load window")
            return 0
        if not force and self.s.busy_query:
            busy = int(self._scalar(self.s.busy_query) or 0)
            if busy > 0:
                log.info("busy: a heavy job is running", extra={"fields": {"busy": busy}})
                return 0
        if self.other_run_active():
            log.info("another load of this dataset is running")
            return 0
        self.clean_up()
        todo = self.pending(snapshot)
        if not todo:
            log.info("nothing to load", extra={"fields": {"current": self.current()}})
            self.retention()
            return 0
        for m in todo:
            log.info("loading", extra={"fields": {"snapshot": m.snapshot}})
            try:
                self.load(m)
            except Exception as e:  # noqa: BLE001 - recorded, then the run fails
                log.error(
                    "load failed; the current snapshot stays",
                    extra={"fields": {"snapshot": m.snapshot, "error": str(e)}},
                )
                return 1
        self.retention()
        return 0


def _brace(keys: list[str]) -> str:
    """One glob for several keys with the same directory: ``dir/{a,b}``."""
    dirs = {k.rsplit("/", 1)[0] for k in keys}
    if len(dirs) != 1:
        raise LoadError("files of one table must share a directory")
    names = [k.rsplit("/", 1)[1] for k in keys]
    for n in names:
        if not re.match(r"^[0-9A-Za-z._-]+$", n):
            raise LoadError(f"unsafe file name {n!r}")
    return f"{dirs.pop()}/{{{','.join(names)}}}"


def rollback(ch: ClickHouse, s: Settings, to: str, loader: str = "") -> None:
    """Make an earlier snapshot current again: one ``rolled_back`` row."""
    naming.check_snapshot_id(to)
    ld = Loader(ch, s, loader)
    if to not in ld.ever_current():
        raise LoadError(f"{to} was never current; load it instead")
    missing = [t for t in s.tables if to not in ld.partitions(t)]
    if missing:
        raise LoadError(
            f"{to} is no longer kept in {', '.join(missing)}; load it again with "
            "`leasehold snapshot load --now --snapshot`"
        )
    ld._record(to, "rolled_back")


def release(ch: ClickHouse, s: Settings, loader: str = "") -> str:
    """End the hold a rollback put on the dataset; the loader loads new snapshots again
    (never one that was current before: those only with ``load --snapshot``)."""
    ld = Loader(ch, s, loader)
    held = ld.held()
    if not held:
        raise LoadError(f"{s.dataset} isn't held")
    ld._record(held, "released")
    return held


def retry(ch: ClickHouse, s: Settings, snapshot: str, loader: str = "") -> None:
    """Let the loader try a snapshot again that failed ``max_attempts`` times."""
    Loader(ch, s, loader)._record(naming.check_snapshot_id(snapshot), "retry")


def status(ch: ClickHouse, s: Settings) -> dict:
    ld = Loader(ch, s)
    last = ch.rows(
        f"SELECT snapshot, status, toString(at), error FROM {ld.loads} "
        f"WHERE dataset = {_q(s.dataset)} ORDER BY at DESC LIMIT 10"
    )
    return {
        "current": ld.current(),
        "kept": {t: sorted(ld.partitions(t)) for t in s.tables},
        "recent": last,
    }


def main(force: bool = False, snapshot: str | None = None) -> int:
    from leasehold.runtime import log as lh_log

    env = os.environ
    s = Settings.from_json(env["LEASEHOLD_SNAPSHOT"])
    component = env.get("LEASEHOLD_COMPONENT", naming.snapshot_loader_component(s.dataset))
    lh_log.setup(
        env.get("LEASEHOLD_SERVICE", s.service),
        component,
        env.get("LEASEHOLD_ENV", ""),
        env.get("LEASEHOLD_VERSION", ""),
        env.get("NOMAD_ALLOC_ID", "local"),
    )
    ch = ClickHouse(
        env["LEASEHOLD_CLICKHOUSE_URL"],
        env["LEASEHOLD_CLICKHOUSE_USER"],
        env["LEASEHOLD_CLICKHOUSE_PASSWORD"],
        timeout=float(s.timeout_s),
    )
    try:
        return Loader(ch, s, loader=f"{component}@{env.get('LEASEHOLD_VERSION', '')}").run(
            force=force, snapshot=snapshot
        )
    except Exception as e:  # noqa: BLE001 - logged for the operator
        log.error("loader failed", extra={"fields": {"error": str(e)}})
        return 1
    finally:
        ch.close()


if __name__ == "__main__":
    sys.exit(main())
