"""What a processor is handling now, kept outside the process.

A handler that holds the GIL (a runaway regex), runs out of memory or segfaults takes the
process with it: nothing in the process can log what it was doing. So the processor writes
the batch to a small file in the allocation directory before ``handle`` and clears it
after. A process that finds an uncleared record at start died while handling that batch.

The file survives the process and its container (the page cache and the allocation
directory do), not a new allocation: then the count starts at 0 again.

``Journal`` keeps the count of such deaths (*kills*) per batch. Two batches are the same
when one holds the event that was being handled at the death, or else the first offset of
one of its partitions. An unrelated batch doesn't reset the count.
"""

from __future__ import annotations

import faulthandler
import json
import logging
import os
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

log = logging.getLogger("leasehold")

BATCH_BYTES = 16384  # the batch record; the current event's record follows it
CURRENT_BYTES = 512
FILE_NAME = "leasehold-inflight.json"

Key = tuple[str, int, int]  # topic, partition, offset


def journal_path(environ: dict[str, str] | None = None) -> Path | None:
    """In the allocation directory; None outside Nomad (then nothing is recorded)."""
    e = os.environ if environ is None else environ
    base = e.get("NOMAD_ALLOC_DIR")
    if not base:
        return None
    data = Path(base) / "data"
    task = e.get("NOMAD_TASK_NAME", "")
    return (data if data.is_dir() else Path(base)) / (f"{task}-{FILE_NAME}" if task else FILE_NAME)


class Died:
    """The batch a previous process died on."""

    def __init__(self, rec: dict, current: Key | None, event_id: str | None):
        self.firsts = {(t, p): o for t, p, o in rec["first"]}
        self.lasts = {(t, p): o for t, p, o in rec["last"]}
        self.kills: int = int(rec["kills"])
        self.isolated: bool = bool(rec["isolated"])
        self.current = current
        self.event_id = event_id

    def matches(self, keys: set[Key]) -> bool:
        if self.current is not None:
            return self.current in keys
        return any((t, p, o) in keys for (t, p), o in self.firsts.items())

    def record(self) -> dict:
        return {
            "first": [[t, p, o] for (t, p), o in self.firsts.items()],
            "last": [[t, p, o] for (t, p), o in self.lasts.items()],
            "kills": self.kills,
            "isolated": self.isolated,
        }

    def log_fields(self) -> dict:
        out: dict = {
            "kills": self.kills,
            "isolated": self.isolated,
            "batch": [f"{t}[{p}]@{o}-{self.lasts[(t, p)]}" for (t, p), o in self.firsts.items()],
        }
        if self.current is not None:
            t, p, o = self.current
            out |= {"topic": t, "partition": p, "offset": o, "event_id": self.event_id}
        return out


class Journal:
    """``died``: the batch to watch for, until it is handled (``resolve``)."""

    def __init__(self, path: Path | None):
        self._fd: int | None = None
        self.died: Died | None = None
        if path is None:
            return
        try:
            self._fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        except OSError as e:
            log.warning("can't record the batch in flight", extra={"fields": {"error": str(e)}})
            return
        self._load()

    def _load(self) -> None:
        raw = os.pread(self._fd, BATCH_BYTES + CURRENT_BYTES, 0)
        try:
            rec = json.loads(raw[:BATCH_BYTES])
            cur = json.loads(raw[BATCH_BYTES:] or b"null")
            key = (cur[0], cur[1], cur[2]) if cur else None
            died = Died(rec, key, cur[3] if cur else None)
        except (ValueError, KeyError, TypeError, IndexError):
            return  # empty, or not a complete record
        if rec.get("active"):
            died.kills += 1
            log.error("the previous process died while handling a batch",
                      extra={"fields": died.log_fields()})  # fmt: skip
        self.died = died
        self._save_died()

    def _write(self, rec: dict, current: list | None) -> None:
        if self._fd is None:
            return
        body = json.dumps(rec, separators=(",", ":")).encode()
        if len(body) > BATCH_BYTES:  # very many partitions: keep what fits
            n = max(1, len(rec["first"]) // 2)
            return self._write(rec | {"first": rec["first"][:n], "last": rec["last"][:n]}, current)
        cur = json.dumps(current, separators=(",", ":")).encode()
        if len(cur) > CURRENT_BYTES:
            cur = b"null"
        # One write of a fixed size: a kill can't leave a mix of two records.
        self._pwrite(body.ljust(BATCH_BYTES) + cur.ljust(CURRENT_BYTES), 0)

    def _pwrite(self, data: bytes, offset: int) -> None:
        try:
            os.pwrite(self._fd, data, offset)
        except OSError as e:  # e.g. disk full: handling events matters more
            log.warning("can't record the batch in flight", extra={"fields": {"error": str(e)}})

    def _save_died(self) -> None:
        d = self.died
        if self._fd is None:
            return
        if d is None:
            try:
                os.ftruncate(self._fd, 0)
            except OSError:
                pass
            return
        cur = [*d.current, d.event_id] if d.current else None
        self._write(d.record() | {"active": False}, cur)

    def matching(self, keys: set[Key]) -> Died | None:
        return self.died if self.died is not None and self.died.matches(keys) else None

    def begin(self, events, isolated_kills: int | None = None) -> None:
        """``isolated_kills``: the events are handled alone; the kills of their batch."""
        if self._fd is None:
            return
        firsts: dict = {}
        lasts: dict = {}
        for e in events:
            firsts.setdefault((e.topic, e.partition), e.offset)
            lasts[(e.topic, e.partition)] = e.offset
        d = self.matching({(e.topic, e.partition, e.offset) for e in events})
        self._write(
            {
                "active": True,
                "first": [[t, p, o] for (t, p), o in firsts.items()],
                "last": [[t, p, o] for (t, p), o in lasts.items()],
                "kills": isolated_kills if isolated_kills is not None else d.kills if d else 0,
                "isolated": isolated_kills is not None,
            },
            None,
        )

    def current(self, event) -> None:
        if self._fd is None:
            return
        at = None
        if event is not None:
            at = [event.topic, event.partition, event.offset, event.event_id]
        cur = json.dumps(at, separators=(",", ":")).encode()
        if len(cur) <= CURRENT_BYTES:
            self._pwrite(cur.ljust(CURRENT_BYTES), BATCH_BYTES)

    def end(self) -> None:
        """``handle`` returned or raised: the process is alive."""
        self._save_died()

    def resolve(self, keys: set[Key]) -> None:
        """The events were committed."""
        if self.matching(keys) is not None:
            self.died = None
            self._save_died()

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None


class Watched(list):
    """The batch given to the handler; reports the event the handler's loop is at."""

    def __init__(self, events, on_event: Callable):
        super().__init__(events)
        self._on_event = on_event

    def __iter__(self):
        for e in super().__iter__():
            self._on_event(e)
            yield e
        self._on_event(None)


class SlowWatch:
    """Reports a ``handle`` call that runs longer than ``limit`` seconds, once per call.

    Two ways, because the thread can't run while the handler holds the GIL:
    a thread logs the event (waiting on I/O, slow Python code), and ``faulthandler``, which
    needs no GIL, writes every thread's stack to stderr (a runaway regex).
    """

    def __init__(self, limit: float):
        self.limit = limit
        self._lock = threading.Lock()
        self._started: float | None = None
        self._logged = False
        self._event = None
        self._first = None
        self._size = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def age(self) -> float:
        started = self._started
        return 0.0 if started is None else time.monotonic() - started

    def start(self) -> None:
        if self.limit > 0 and self._thread is None:
            self._thread = threading.Thread(target=self._run, name="leasehold-slow", daemon=True)
            self._thread.start()

    def begin(self, events) -> None:
        with self._lock:
            self._started, self._logged = time.monotonic(), False
            self._event, self._first, self._size = None, events[0], len(events)
        if self._thread is not None:
            try:
                faulthandler.dump_traceback_later(self.limit, file=sys.__stderr__)
            except (OSError, ValueError, AttributeError, RuntimeError):
                pass  # no usable stderr

    def current(self, event) -> None:
        self._event = event

    def end(self) -> None:
        with self._lock:
            self._started = None
        if self._thread is not None:
            faulthandler.cancel_dump_traceback_later()

    def check(self) -> None:
        with self._lock:
            age = self.age()
            if self._started is None or self._logged or age < self.limit:
                return
            self._logged = True
            e, first, size = self._event, self._first, self._size
        at = e or first
        log.error(
            "a batch is slow",
            extra={
                "fields": {
                    "running_s": round(age, 1),
                    "topic": at.topic,
                    "partition": at.partition,
                    "offset": at.offset,
                    "event_id": at.event_id,
                    "at": "event" if e is not None else "batch start",
                    "batch_size": size,
                }
            },
        )

    def _run(self) -> None:
        while not self._stop.wait(min(1.0, self.limit / 2)):
            self.check()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            faulthandler.cancel_dump_traceback_later()
            self._thread = None
