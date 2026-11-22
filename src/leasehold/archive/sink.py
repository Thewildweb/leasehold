"""The archive sink: one Kafka topic written to Parquet files (zstd) in a bucket.

Runs as the platform component ``archive-<sink>`` (``leasehold archive run``); its
settings come from ``LEASEHOLD_ARCHIVE`` (see ``leasehold.nomad.render``), its Kafka and
storage credentials from its Nomad Variable.

Files::

    <topic ref>/dt=YYYY-MM-DD/hh=HH/p<partition:03>-<first offset:020>-<last offset:020>.parquet

One file holds a contiguous run of offsets of one partition. ``dt``/``hh`` is the hour
of the Kafka timestamp of the file's first message: a file is closed when a message of a
later hour arrives, and a late message (an earlier hour) stays in the open file, so
queries on ``ts`` should look one hour beyond their range.

Delivery: at least once, without lasting duplicates.

- A partition's offset is committed only after its file is uploaded, so a crash replays
  from the first offset of the file that wasn't committed.
- After an upload, other files of the same partition and first offset in the same hour
  are deleted. A replay starts at that same offset (and the same hour, which depends
  only on that message), so its file replaces what an earlier run uploaded but didn't
  commit, even when the two runs cut the file at a different offset.

Memory: one open file per assigned partition. Rows are buffered up to ``row_group_mb``
per file, then written as a row group to a local temporary file, which is uploaded
(multipart) when it reaches ``file_size_mb`` or has been open ``max_age``. A rebalance
drops the open files without committing; the new owner rewrites them.

Payloads behind a claim check (``payload_ref``) are resolved and written as columns:
claims expire with the topic, the archive is kept.

Metrics: ``leasehold_archive_rows_total``, ``_bytes_total`` (uploaded file bytes),
``_files_total``, ``_upload_failures_total``, ``_invalid_events_total``,
``_claim_failures_total``, ``_open_file_age_seconds``, and
``leasehold_kafka_consumer_lag`` (as processors).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client import Counter, Gauge

from leasehold import naming
from leasehold.archive import layout
from leasehold.runtime import claims
from leasehold.runtime import log as lh_log
from leasehold.runtime.http import Liveness, serve
from leasehold.runtime.processor import (
    DEFAULT_BATCH_MAX_BYTES,
    LAG,
    LAG_INTERVAL,
    prefetch_conf,
)
from leasehold.spec.models import ArchiveSink
from leasehold.storage.client import ObjectStore, StorageError

log = logging.getLogger("leasehold")

ROWS = Counter("leasehold_archive_rows", "Rows written to uploaded files", ["topic"])
BYTES = Counter("leasehold_archive_bytes", "Bytes of uploaded files", ["topic"])
FILES = Counter("leasehold_archive_files", "Files uploaded", ["topic"])
UPLOAD_FAILURES = Counter("leasehold_archive_upload_failures", "Failed uploads", ["topic"])
INVALID = Counter(
    "leasehold_archive_invalid_events",
    "Messages that weren't JSON envelopes (archived with the raw value in payload)",
    ["topic"],
)
CLAIM_FAILURES = Counter(
    "leasehold_archive_claim_failures",
    "Claims that couldn't be resolved (archived with payload_ref only)",
    ["topic"],
)
OPEN_AGE = Gauge(
    "leasehold_archive_open_file_age_seconds", "Age of the oldest open file", ["topic"]
)

CONTENT_TYPE = "application/vnd.apache.parquet"
UPLOAD_ATTEMPTS = 5
COMMIT_ATTEMPTS = 5


class ArchiveError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    env: str
    service: str
    component: str
    sink_name: str
    sink: ArchiveSink
    topic: str  # actual topic name
    topic_ref: str
    bucket: str  # actual bucket name
    claims_bucket: str | None
    version: str = "unknown"
    instance: str = "local"
    kafka_bootstrap: str = ""
    kafka_username: str | None = None
    kafka_password: str | None = None
    http_port: int = 0
    tmp_dir: str | None = None

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> Settings:
        e = os.environ if environ is None else environ
        missing = [
            k
            for k in (
                "LEASEHOLD_ENV",
                "LEASEHOLD_SERVICE",
                "LEASEHOLD_COMPONENT",
                "LEASEHOLD_ARCHIVE",
                "LEASEHOLD_KAFKA_BOOTSTRAP",
            )  # fmt: skip
            if not e.get(k)
        ]
        if missing:
            raise ArchiveError(f"missing environment variables: {', '.join(missing)}")
        a = json.loads(e["LEASEHOLD_ARCHIVE"])
        return cls(
            env=naming.check_env(e["LEASEHOLD_ENV"]),
            service=e["LEASEHOLD_SERVICE"],
            component=e["LEASEHOLD_COMPONENT"],
            sink_name=a["sink"],
            sink=ArchiveSink.model_validate(a["spec"]),
            topic=a["topic"],
            topic_ref=a["topic_ref"],
            bucket=a["bucket"],
            claims_bucket=a.get("claims_bucket"),
            version=e.get("LEASEHOLD_VERSION", "unknown"),
            instance=e.get("NOMAD_ALLOC_ID") or os.uname().nodename,
            kafka_bootstrap=e["LEASEHOLD_KAFKA_BOOTSTRAP"],
            kafka_username=e.get("LEASEHOLD_KAFKA_USERNAME") or None,
            kafka_password=e.get("LEASEHOLD_KAFKA_PASSWORD") or None,
            http_port=int(e.get("NOMAD_PORT_http") or e.get("LEASEHOLD_HTTP_PORT") or "0"),
            tmp_dir=e.get("LEASEHOLD_ARCHIVE_TMP") or e.get("NOMAD_TASK_DIR") or None,
        )

    @property
    def group(self) -> str:
        return naming.consumer_group(self.env, self.service, self.component)

    def kafka_conf(self) -> dict[str, Any]:
        conf: dict[str, Any] = {"bootstrap.servers": self.kafka_bootstrap}
        if self.kafka_password:
            conf |= {
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": self.kafka_username
                or naming.kafka_principal(self.env, self.service, self.component),
                "sasl.password": self.kafka_password,
            }
        return conf


def hour_of(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC).replace(minute=0, second=0, microsecond=0)


def file_key(topic_ref: str, hour: datetime, partition: int, first: int, last: int) -> str:
    return f"{hour_prefix(topic_ref, hour)}{file_stem(partition, first)}{last:020d}.parquet"


def hour_prefix(topic_ref: str, hour: datetime) -> str:
    return f"{topic_ref}/dt={hour:%Y-%m-%d}/hh={hour:%H}/"


def file_stem(partition: int, first: int) -> str:
    """Name prefix shared by every file of one partition starting at one offset."""
    return f"p{partition:03d}-{first:020d}-"


@dataclass
class OpenFile:
    """One partition's file being written: rows in memory, row groups on local disk."""

    partition: int
    hour: datetime
    first: int
    path: Path
    schema: Any
    opened: float = field(default_factory=time.monotonic)
    last: int = -1
    rows: list[dict] = field(default_factory=list)
    buffered: int = 0
    count: int = 0
    _writer: Any = None

    def add(self, row: dict, offset: int, row_group_bytes: int) -> None:
        self.rows.append(row)
        self.last = offset
        self.count += 1
        self.buffered += layout.row_size(row)
        if self.buffered >= row_group_bytes:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        if self._writer is None:
            self._writer = pq.ParquetWriter(
                str(self.path), self.schema, compression="zstd", compression_level=3
            )
        self._writer.write_table(pa.Table.from_pylist(self.rows, schema=self.schema))
        self.rows, self.buffered = [], 0

    def size_on_disk(self) -> int:
        return self.path.stat().st_size if self.path.exists() else 0

    def close(self) -> Path:
        self.flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        return self.path

    def discard(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:  # noqa: BLE001 - the file is thrown away
                pass
            self._writer = None
        self.path.unlink(missing_ok=True)


class Sink:
    def __init__(
        self,
        settings: Settings,
        objects: ObjectStore,
        *,
        consumer: Consumer | None = None,
        batch_size: int = 500,
        batch_wait: float = 1.0,
        liveness: Liveness | None = None,
        stopping=None,
    ):
        import threading

        self.s = settings
        self.objects = objects
        self.schema = layout.arrow_schema(settings.sink)
        self.batch_size = batch_size
        self.batch_wait = batch_wait
        self.liveness = liveness or Liveness()
        self.stopping = stopping or threading.Event()
        self.row_group_bytes = settings.sink.row_group_mb * 1024 * 1024
        self.file_bytes = settings.sink.file_size_mb * 1024 * 1024
        self.max_age = settings.sink.max_age.total_seconds()
        self.tmp = Path(tempfile.mkdtemp(prefix="archive-", dir=settings.tmp_dir))
        self.open: dict[int, OpenFile] = {}
        self.next_lag = 0.0
        self.consumer = consumer or Consumer(
            settings.kafka_conf()
            | {
                "group.id": settings.group,
                "client.id": f"{settings.service}.{settings.component}:{settings.instance}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "enable.partition.eof": False,
            }
            # Prefetch bounded like a processor's with 64 MB batches.
            | prefetch_conf(DEFAULT_BATCH_MAX_BYTES)
        )

    # --- partitions -----------------------------------------------------------------------------

    def _on_assign(self, consumer, partitions) -> None:
        log.info(
            "partitions assigned",
            extra={"fields": {"partitions": [p.partition for p in partitions]}},
        )

    def _on_revoke(self, consumer, partitions) -> None:
        """Drop open files of revoked partitions; their next owner replays from the commit."""
        for tp in partitions:
            f = self.open.pop(tp.partition, None)
            if f is not None:
                f.discard()
        log.info(
            "partitions revoked",
            extra={"fields": {"partitions": [p.partition for p in partitions]}},
        )

    # --- loop ---------------------------------------------------------------------------------

    def run(self) -> int:
        self.consumer.subscribe(
            [self.s.topic], on_assign=self._on_assign, on_revoke=self._on_revoke
        )
        log.info(
            "archive sink started",
            extra={
                "fields": {"topic": self.s.topic, "bucket": self.s.bucket, "group": self.s.group}
            },
        )
        try:
            while not self.stopping.is_set():
                self.liveness.beat()
                self.step()
            # Draining: upload what's open, so a deploy doesn't redo up to max_age of work.
            for p in sorted(self.open):
                self._finish(p)
            return 0
        finally:
            for f in self.open.values():
                f.discard()
            self.consumer.close()
            log.info("archive sink stopped")

    def step(self) -> None:
        msgs = self.consumer.consume(self.batch_size, self.batch_wait)
        for m in msgs:
            if m.error():
                if m.error().fatal():
                    raise KafkaException(m.error())
                log.warning("consumer error", extra={"fields": {"error": str(m.error())}})
                continue
            self._add(m)
        now = time.monotonic()
        oldest = 0.0
        for p in sorted(self.open):
            age = now - self.open[p].opened
            if age >= self.max_age:
                self._finish(p)
            else:
                oldest = max(oldest, age)
        OPEN_AGE.labels(self.s.topic).set(oldest)
        self._lag()

    def _add(self, m) -> None:
        p, offset = m.partition(), m.offset()
        ts_type, ts_ms = m.timestamp()
        row = self._row(m, ts_ms if ts_type else None)
        if ts_type == 0 or ts_ms is None or ts_ms < 0:
            ts = row.get("ts") or datetime.now(UTC)
            ts_ms = int(ts.timestamp() * 1000)
        hour = hour_of(ts_ms)
        f = self.open.get(p)
        if f is not None and hour > f.hour:
            self._finish(p)
            f = None
        if f is None:
            f = OpenFile(
                partition=p,
                hour=hour,
                first=offset,
                path=self.tmp / f"p{p}-{offset}.parquet",
                schema=self.schema,
            )
            self.open[p] = f
        f.add(row, offset, self.row_group_bytes)
        if f.size_on_disk() >= self.file_bytes:
            self._finish(p)

    def _row(self, m, kafka_ts_ms: int | None) -> dict:
        raw = m.value()
        try:
            env = json.loads(raw)
            if not isinstance(env, dict):
                raise ValueError("not an object")
        except (ValueError, TypeError):
            INVALID.labels(self.s.topic).inc()
            text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
            return layout.to_row(
                self.s.sink, {}, text, partition=m.partition(), offset=m.offset(),
                kafka_ts_ms=kafka_ts_ms,
            )  # fmt: skip
        payload = env.get("payload")
        ref = env.get("payload_ref")
        if ref is not None and payload is None:
            payload = self._resolve(ref, m)
        return layout.to_row(
            self.s.sink, env, payload, partition=m.partition(), offset=m.offset(),
            kafka_ts_ms=kafka_ts_ms,
        )  # fmt: skip

    def _resolve(self, ref: Any, m) -> Any:
        delay = 1.0
        for attempt in range(1, UPLOAD_ATTEMPTS + 1):
            try:
                return claims.resolve(self.objects, self.s.env, ref, self.s.component)
            except (claims.ClaimMissing, claims.ClaimNotReadable) as e:
                # Not going to get better: archive the reference without the payload.
                CLAIM_FAILURES.labels(self.s.topic).inc()
                log.error(
                    "claim not resolved; archived with payload_ref only",
                    extra={
                        "fields": {
                            "error": str(e),
                            "partition": m.partition(),
                            "offset": m.offset(),
                        }
                    },  # fmt: skip
                )
                return None
            except claims.ClaimError as e:
                if attempt == UPLOAD_ATTEMPTS:
                    raise ArchiveError(f"claim at {m.partition()}@{m.offset()}: {e}") from None
                log.warning("claim not resolved, retrying", extra={"fields": {"error": str(e)}})
                self._sleep(delay)
                delay = min(delay * 2, 30)
        return None

    # --- closing a file -----------------------------------------------------------------------

    def _finish(self, p: int) -> None:
        """Close, upload, replace other files of the same start, then commit."""
        f = self.open.pop(p)
        path = f.close()
        key = file_key(self.s.topic_ref, f.hour, p, f.first, f.last)
        size = path.stat().st_size
        self._upload(path, key)
        prefix = hour_prefix(self.s.topic_ref, f.hour) + file_stem(p, f.first)
        for o in self.objects.list(self.s.bucket, prefix):
            if o.key != key:
                self.objects.delete(self.s.bucket, o.key)
                log.info("replaced an uncommitted file", extra={"fields": {"key": o.key}})
        path.unlink(missing_ok=True)
        self._commit(p, f.last + 1)
        ROWS.labels(self.s.topic).inc(f.count)
        BYTES.labels(self.s.topic).inc(size)
        FILES.labels(self.s.topic).inc()
        log.info(
            "file uploaded",
            extra={"fields": {"key": key, "rows": f.count, "bytes": size}},
        )

    def _upload(self, path: Path, key: str) -> None:
        delay = 1.0
        for attempt in range(1, UPLOAD_ATTEMPTS + 1):
            try:
                self.objects.put_file(self.s.bucket, key, str(path), CONTENT_TYPE)
                return
            except StorageError as e:
                UPLOAD_FAILURES.labels(self.s.topic).inc()
                log.warning(
                    "upload failed",
                    extra={"fields": {"key": key, "attempt": attempt, "error": str(e)}},
                )
                if attempt == UPLOAD_ATTEMPTS:
                    raise ArchiveError(f"upload of {key} failed: {e}") from None
                self._sleep(delay)
                delay = min(delay * 2, 30)

    def _commit(self, p: int, offset: int) -> None:
        tp = TopicPartition(self.s.topic, p, offset)
        for attempt in range(1, COMMIT_ATTEMPTS + 1):
            try:
                self.consumer.commit(offsets=[tp], asynchronous=False)
                return
            except KafkaException as e:
                if attempt == COMMIT_ATTEMPTS:
                    # The file is uploaded; a restart replays and replaces it.
                    raise ArchiveError(f"commit of partition {p} failed: {e}") from None
                self._sleep(1.0)

    def _sleep(self, seconds: float) -> None:
        self.liveness.beat()
        self.stopping.wait(seconds)

    def _lag(self) -> None:
        if time.monotonic() < self.next_lag:
            return
        self.next_lag = time.monotonic() + LAG_INTERVAL
        try:
            for tp in self.consumer.assignment():
                low, high = self.consumer.get_watermark_offsets(tp, timeout=5, cached=False)
                committed = self.consumer.committed([tp], timeout=5)[0].offset
                pos = committed if committed >= 0 else low
                LAG.labels(self.s.group, tp.topic, str(tp.partition)).set(max(high - pos, 0))
        except KafkaException as e:
            log.warning("couldn't read consumer lag", extra={"fields": {"error": str(e)}})


def run(settings: Settings | None = None) -> int:
    """``leasehold archive run``: the sink until SIGTERM. Returns the exit code."""
    import signal
    import threading

    from leasehold.runtime import resources

    try:
        s = settings or Settings.from_env()
    except (ArchiveError, ValueError, KeyError) as e:
        print(f"leasehold archive run: {e}", file=sys.stderr)
        return 1
    lh_log.setup(s.service, s.component, s.env, s.version, s.instance)
    stopping = threading.Event()

    def stop(signum, _frame):
        if not stopping.is_set():
            log.info("stopping", extra={"fields": {"signal": signal.Signals(signum).name}})
        stopping.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    liveness = Liveness()
    resources.register(s.version)
    http = serve(s.http_port, liveness) if s.http_port else None
    try:
        sink = Sink(s, ObjectStore.from_env(), liveness=liveness, stopping=stopping)
        return sink.run()
    except (ArchiveError, StorageError) as e:
        log.error("archive sink failed", extra={"fields": {"error": str(e)}})
        return 1
    finally:
        if http is not None:
            http.shutdown()
