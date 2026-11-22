"""The processor loop: a Kafka consumer group.

    from leasehold.client import NewTask
    from leasehold.runtime import run_processor

    def handle(events, ctx):
        tasks = [NewTask(e.payload["domain"]) for e in events]
        ctx.upsert("resolver.a_record", tasks)

    if __name__ == "__main__":
        raise SystemExit(run_processor(handle))

What the loop does:

- Subscribes to the component's ``consumes`` topics as consumer group
  ``<service>.<component>`` (``dev.`` prefixed in dev).
- Reads batches of up to ``batch_size`` events or ``batch_max_bytes`` bytes, whichever
  comes first (or what arrived within ``batch_wait`` seconds), and calls
  ``handle(events, ctx)`` once per batch. Bytes are message value sizes, plus the
  payload of a resolved claim. A message that would take a batch over the byte limit
  starts the next batch; one larger than the limit is a batch of its own.
  ``LEASEHOLD_BATCH_MAX_BYTES`` overrides the byte limit (default 64 MB). The consumer's
  prefetch queue is limited to half of it (see ``prefetch_conf``), so a processor holds
  about 1.5 x ``batch_max_bytes`` of messages, plus their parsed form.
- Payloads behind a claim check (``payload_ref``, see ``leasehold.runtime.claims``)
  are fetched from object storage before validation, so ``handle`` sees them inline. A
  component without a read grant on the claim's bucket stops with an error naming the
  missing ``reads_buckets`` entry; a claim that is gone (expired) is skipped and counted
  as invalid.
- Commits the batch's offsets only after ``handle`` returned and every event it
  emitted was confirmed by Kafka. Offsets are the processor's progress, so a
  crash replays the batch: ``handle`` must be idempotent (upserts are; dedup
  emitted events downstream on ``event_id``).
- A batch that fails is retried with backoff, ``max_attempts`` times; then the
  process exits non-zero and Nomad restarts it (the crashloop alert fires if it
  keeps failing). The offsets stay where they were.
- Messages that aren't valid envelopes, or whose payload doesn't fit the topic's
  payload model, are skipped and counted (``leasehold_processor_invalid_events_total``);
  there is no dead-letter topic yet. ``Event.payload`` is the model's JSON (defaults
  filled in); the models come from ``LEASEHOLD_PAYLOAD_MODELS``.
- Exports ``leasehold_kafka_consumer_lag{group,topic,partition}`` every 30s.
- On SIGTERM: finish the current batch, commit, leave the group.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import time
from collections import deque
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client import Counter, Gauge, Histogram
from pydantic import BaseModel, ValidationError

from leasehold import naming
from leasehold.runtime import claims, payloads
from leasehold.runtime import log as lh_log
from leasehold.runtime.context import Context, build
from leasehold.runtime.events import Event, InvalidEvent, ProduceError, parse

log = logging.getLogger("leasehold")

EVENTS_IN = Counter("leasehold_processor_events", "Events handled and committed", ["topic"])
INVALID = Counter(
    "leasehold_processor_invalid_events", "Messages that weren't valid events", ["topic"]
)
BATCH_SECONDS = Histogram(
    "leasehold_processor_batch_seconds",
    "Time to handle one batch",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
BATCH_FAILURES = Counter("leasehold_processor_batch_failures", "Failed batch attempts")
BATCH_BYTES = Histogram(
    "leasehold_processor_batch_bytes",
    "Bytes in one batch (message values plus resolved claims)",
    buckets=(1e4, 1e5, 1e6, 4e6, 16e6, 32e6, 64e6, 128e6, 256e6),
)
BATCH_CLOSED = Counter(
    "leasehold_processor_batches",
    "Batches read, by what closed them: count, bytes, or wait (batch_wait ran out)",
    ["closed_by"],
)
CLAIMS = Counter("leasehold_processor_claims_resolved", "Payloads fetched by claim check")
LAG = Gauge(
    "leasehold_kafka_consumer_lag",
    "Messages behind the end of the partition",
    ["group", "topic", "partition"],
)

LAG_INTERVAL = 30.0
PRODUCE_TIMEOUT = 120.0
DEFAULT_BATCH_MAX_BYTES = 64 * 1024 * 1024
# Messages taken from the consumer's queue per call; the rest of a call's messages wait
# in ``_pending`` when a batch closes on bytes.
CONSUME_CHUNK = 50
CLAIM_ATTEMPTS = 5


def batch_max_bytes_from_env(default: int | None) -> int:
    value = os.environ.get("LEASEHOLD_BATCH_MAX_BYTES")
    if value:
        n = int(value)
        if n <= 0:
            raise ValueError("LEASEHOLD_BATCH_MAX_BYTES must be > 0")
        return n
    return default or DEFAULT_BATCH_MAX_BYTES


def prefetch_conf(batch_max_bytes: int) -> dict[str, int]:
    """librdkafka's prefetch limits for a batch byte limit.

    The consumer fetches ahead into a local queue; with large messages that queue can
    hold as much as a batch. ``queued.max.messages.kbytes`` caps it at half the batch
    limit (at least 4 MB), and one fetch response (``fetch.max.bytes``) at that size
    too (librdkafka's default is 50 MB). A message larger than these limits is still
    fetched, one at a time, so nothing gets stuck.
    """
    queued_kb = max(batch_max_bytes // 2 // 1024, 4096)
    return {
        "queued.max.messages.kbytes": queued_kb,
        "fetch.max.bytes": min(queued_kb * 1024, 50 * 1024 * 1024),
    }


class ClaimsNotReadable(RuntimeError):
    """The component can't read a claim's bucket: a service.yml change, not a retry."""


BatchHandler = Callable[[list[Event], Context], None]


class Processor:
    def __init__(
        self,
        ctx: Context,
        handler: BatchHandler,
        *,
        batch_size: int = 500,
        batch_wait: float = 1.0,
        max_attempts: int = 5,
        batch_max_bytes: int | None = None,
        consumer: Consumer | None = None,
        models: dict[str, type[BaseModel]] | None = None,
        objects=None,
    ):
        """``models``: per consumed topic reference, instead of LEASEHOLD_PAYLOAD_MODELS.
        ``batch_max_bytes``: LEASEHOLD_BATCH_MAX_BYTES wins over it. ``objects``: the
        object store for claims (default: from the environment when the component
        declares buckets)."""
        cfg = ctx.cfg
        comp = cfg.component_spec
        self.ctx = ctx
        self.handler = handler
        self.batch_size = batch_size
        self.batch_wait = batch_wait
        self.batch_max_bytes = batch_max_bytes_from_env(batch_max_bytes)
        self.max_attempts = max_attempts
        self._pending: deque = deque()  # read but not yet in a batch: (msg, event, bytes)
        self._objects = objects
        self._objects_loaded = objects is not None
        self.group = naming.consumer_group(cfg.env, cfg.service, cfg.component)
        refs = [naming.TopicRef.parse(r) for r in comp.consumes]
        self.refs = {naming.topic_for(cfg.env, r): r for r in refs}
        if models is None:
            missing = sorted(str(r) for r in refs if str(r) not in cfg.payload_models)
            if missing:
                raise payloads.PayloadModelError(
                    f"no payload model for {', '.join(missing)} in LEASEHOLD_PAYLOAD_MODELS"
                )
            models = payloads.load_models({str(r): cfg.payload_models[str(r)] for r in refs})
        self.models = models
        self.consumer = consumer or Consumer(
            cfg.kafka_conf()
            | {
                "group.id": self.group,
                "client.id": f"{cfg.name}:{cfg.instance}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "enable.partition.eof": False,
            }
            | prefetch_conf(self.batch_max_bytes)
        )
        self.next_lag = 0.0

    def run(self) -> int:
        self.consumer.subscribe(sorted(self.refs), on_revoke=self._on_revoke)
        log.info(
            "processor started",
            extra={"fields": {"group": self.group, "topics": sorted(self.refs)}},
        )
        try:
            while not self.ctx.stopping.is_set():
                self.ctx.liveness.beat()
                if not self.step():
                    return 1
        except ClaimsNotReadable as e:
            log.error("can't read claimed payloads", extra={"fields": {"error": str(e)}})
            return 1
        finally:
            self.consumer.close()
            log.info("processor stopped")
        return 0

    def step(self) -> bool:
        """Read and handle one batch. False when the batch kept failing."""
        batch = self._read_batch()
        if batch is None:
            return False
        msgs, events = batch
        if events and not self._handle(events):
            return False
        if msgs:
            self.consumer.commit(offsets=_next_offsets(msgs), asynchronous=False)
            for e in events:
                EVENTS_IN.labels(e.topic).inc()
        self._lag()
        return True

    def _read_batch(self):
        """(messages, events) of the next batch; None when a claim kept failing."""
        deadline = time.monotonic() + self.batch_wait
        msgs: list = []
        events: list[Event] = []
        size = 0
        closed_by = "wait"
        while len(msgs) < self.batch_size:
            if not self._pending:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                n = min(self.batch_size - len(msgs), CONSUME_CHUNK)
                got = self.consumer.consume(n, left)
                if not got:
                    break
                self._pending.extend((m, None, None) for m in got)
            m, event, nbytes = self._pending.popleft()
            if m.error():
                if m.error().fatal():
                    raise KafkaException(m.error())
                log.warning("consumer error", extra={"fields": {"error": str(m.error())}})
                continue
            if nbytes is None:
                try:
                    event, nbytes = self._event_sized(m)
                except InvalidEvent as e:
                    INVALID.labels(m.topic()).inc()
                    log.error("skipping invalid event", extra={"fields": {"error": str(e)}})
                    event, nbytes = None, len(m.value() or b"")
                except claims.ClaimError as e:
                    log.error("claim not resolved", extra={"fields": {"error": str(e)}})
                    self._pending.appendleft((m, None, None))
                    return None
            if msgs and size + nbytes > self.batch_max_bytes:
                # Starts the next batch (already parsed and resolved).
                self._pending.appendleft((m, event, nbytes))
                closed_by = "bytes"
                break
            msgs.append(m)
            size += nbytes
            if event is not None:
                events.append(event)
            if size >= self.batch_max_bytes:
                closed_by = "bytes"
                break
        else:
            closed_by = "count"
        if msgs:
            BATCH_BYTES.observe(size)
            BATCH_CLOSED.labels(closed_by).inc()
        return msgs, events

    def _on_revoke(self, consumer, partitions) -> None:
        """Messages read ahead from a revoked partition go back to Kafka (not committed)."""
        gone = {(tp.topic, tp.partition) for tp in partitions}
        self._pending = deque(
            item for item in self._pending if (item[0].topic(), item[0].partition()) not in gone
        )

    def _event_sized(self, m) -> tuple[Event, int]:
        """The event, with a claimed payload fetched, and its size for the byte limit."""
        nbytes = len(m.value() or b"")
        ref = self.refs[m.topic()]
        e = parse(m, ref)
        payload_ref = e.envelope.get("payload_ref")
        if payload_ref is not None and e.payload is None:
            payload = self._resolve(payload_ref, e)
            if payload is None:
                raise InvalidEvent(f"{e.topic}[{e.partition}]@{e.offset}: claim is gone")
            nbytes += len(json.dumps(payload, separators=(",", ":")))
            e = dataclasses.replace(e, payload=payload)
        return self._validated(e), nbytes

    def _store(self):
        if not self._objects_loaded:
            self._objects_loaded = True
            comp = self.ctx.cfg.component_spec
            if comp.reads_buckets or comp.writes_buckets:
                from leasehold.storage.client import ObjectStore

                self._objects = ObjectStore.from_env()
        return self._objects

    def _resolve(self, payload_ref, e: Event):
        """The claimed payload; None when it's gone. Retries what may be transient."""
        delay = 1.0
        for attempt in range(1, CLAIM_ATTEMPTS + 1):
            try:
                payload = claims.resolve(self._store(), self.ctx.cfg.env, payload_ref,
                                         self.ctx.cfg.name)  # fmt: skip
                CLAIMS.inc()
                return payload
            except claims.ClaimNotReadable as err:
                raise ClaimsNotReadable(str(err)) from None
            except claims.ClaimMissing as err:
                log.error("claim is gone", extra={"fields": {"error": str(err)}})
                return None
            except claims.ClaimError:
                if attempt == CLAIM_ATTEMPTS or self.ctx.stopping.is_set():
                    raise
                self.ctx.liveness.beat()
                time.sleep(delay)
                delay = min(delay * 2, 30)
        return None

    def _event(self, m) -> Event:
        return self._event_sized(m)[0]

    def _validated(self, e: Event) -> Event:
        try:
            body = payloads.validated(self.models[str(e.ref)], e.payload)
        except ValidationError as err:
            where = f"{e.topic}[{e.partition}]@{e.offset}"
            raise InvalidEvent(f"{where}: payload: {payloads.error_summary(err)}") from None
        return dataclasses.replace(e, payload=body)

    def _handle(self, events: list[Event]) -> bool:
        delay = 1.0
        for attempt in range(1, self.max_attempts + 1):
            ctx = _BatchContext(self.ctx)
            t0 = time.monotonic()
            try:
                with lh_log.scope(batch_size=len(events)):
                    self.handler(events, ctx)
                for d in ctx.deliveries:
                    d.wait(PRODUCE_TIMEOUT)
                BATCH_SECONDS.observe(time.monotonic() - t0)
                return True
            except Exception as e:  # noqa: BLE001
                BATCH_FAILURES.inc()
                log.exception(
                    "batch failed",
                    extra={
                        "fields": {
                            "attempt": attempt,
                            "max_attempts": self.max_attempts,
                            "first": f"{events[0].topic}[{events[0].partition}]@{events[0].offset}",
                            "error_type": type(e).__name__,
                        }
                    },
                )
                if attempt == self.max_attempts or self.ctx.stopping.is_set():
                    break
                self.ctx.liveness.beat()
                time.sleep(delay)
                delay = min(delay * 2, 30)
        return False

    def _lag(self) -> None:
        if time.monotonic() < self.next_lag:
            return
        self.next_lag = time.monotonic() + LAG_INTERVAL
        try:
            for tp in self.consumer.assignment():
                _low, high = self.consumer.get_watermark_offsets(tp, timeout=5, cached=False)
                pos = self.consumer.position([tp])[0].offset
                if pos < 0:  # nothing consumed yet on this partition
                    committed = self.consumer.committed([tp], timeout=5)[0].offset
                    pos = committed if committed >= 0 else _low
                LAG.labels(self.group, tp.topic, str(tp.partition)).set(max(high - pos, 0))
        except KafkaException as e:
            log.warning("couldn't read consumer lag", extra={"fields": {"error": str(e)}})


class _BatchContext:
    """The processor's Context for one batch: tracks emitted events so the batch is only
    committed once they're confirmed."""

    def __init__(self, ctx: Context):
        self._ctx = ctx
        self.cfg = ctx.cfg
        self.log = ctx.log
        self.client = ctx.client
        self.deliveries = []

    def emit(self, topic, payload, **kw):
        d = self._ctx.emit(topic, payload, **kw)
        self.deliveries.append(d)
        return d.event_id

    def upsert(self, *a, **kw):
        return self._ctx.upsert(*a, **kw)


def _next_offsets(msgs) -> list[TopicPartition]:
    """The offset to commit per partition: one past the last message read."""
    last: dict[tuple[str, int], int] = {}
    for m in msgs:
        k = (m.topic(), m.partition())
        last[k] = max(last.get(k, -1), m.offset())
    return [TopicPartition(t, p, o + 1) for (t, p), o in last.items()]


def run_processor(
    handler: BatchHandler,
    *,
    batch_size: int = 500,
    batch_wait: float = 1.0,
    max_attempts: int = 5,
    batch_max_bytes: int | None = None,
    models: dict[str, type[BaseModel]] | None = None,
    ctx: Context | None = None,
) -> int:
    """Run a processor until SIGTERM. Returns the process exit code.

    ``models``: per consumed topic reference, the payload model to validate with,
    instead of the producer's model named in LEASEHOLD_PAYLOAD_MODELS (a reader of
    several services' topics that validates them all with one shared model)."""
    ctx = ctx or build("processor")
    ctx.install_signal_handlers()
    ctx.serve_http()
    try:
        return Processor(
            ctx,
            handler,
            batch_size=batch_size,
            batch_wait=batch_wait,
            max_attempts=max_attempts,
            batch_max_bytes=batch_max_bytes,
            models=models,
        ).run()
    except ProduceError:
        return 1
    finally:
        ctx.close()
