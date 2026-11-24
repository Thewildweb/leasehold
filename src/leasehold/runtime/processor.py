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
- Records the batch in flight in the allocation directory (``leasehold.runtime.inflight``).
  After a death in ``handle`` (killed by the health check, OOM, segfault) the next process
  logs the batch and the event, and counts the kill. A ``handle`` that runs longer than
  ``LEASEHOLD_SLOW_HANDLE_S`` (default 20; 0 = off) is logged while it runs.
- A batch that was killed ``MAX_KILLS`` times is handled one event at a time, each
  committed on its own. An event whose handling alone kills the process goes, as it was
  read, to the service's dead-letter topic (``naming.dead_letter_topic``), with headers
  saying where it came from, and is committed. An exception is never a reason to
  dead-letter: it may be an outage.
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

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from prometheus_client import Counter, Gauge, Histogram
from pydantic import BaseModel, ValidationError

from leasehold import naming
from leasehold.runtime import claims, payloads
from leasehold.runtime import log as lh_log
from leasehold.runtime.context import Context, build
from leasehold.runtime.events import Event, InvalidEvent, ProduceError, parse
from leasehold.runtime.inflight import Journal, SlowWatch, Watched, journal_path

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
BATCH_AGE = Gauge(
    "leasehold_processor_batch_age_seconds", "How long the current handle call has run (0: none)"
)
KILLS = Gauge(
    "leasehold_processor_batch_kills",
    "Times a process died on the batch that is now being retried (0: none)",
)
DEAD_LETTERS = Counter(
    "leasehold_processor_dead_letters", "Events moved to the dead-letter topic", ["topic"]
)
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
MAX_KILLS = 5
DEFAULT_SLOW_HANDLE_S = 20.0  # the health check kills a process that holds the GIL after ~30


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
        journal: Journal | None = None,
        dead_letter_producer=None,
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
        self._dlq = dead_letter_producer
        self.journal = journal if journal is not None else Journal(journal_path())
        self.slow = SlowWatch(
            float(os.environ.get("LEASEHOLD_SLOW_HANDLE_S") or DEFAULT_SLOW_HANDLE_S)
        )
        BATCH_AGE.set_function(self.slow.age)
        KILLS.set(self.journal.died.kills if self.journal.died else 0)

    def run(self) -> int:
        self.consumer.subscribe(sorted(self.refs), on_revoke=self._on_revoke)
        self.slow.start()
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
            self.slow.stop()
            self.consumer.close()
            self.journal.close()
            log.info("processor stopped")
        return 0

    def step(self) -> bool:
        """Read and handle one batch. False when the batch kept failing."""
        batch = self._read_batch()
        if batch is None:
            return False
        msgs, events = batch
        keys = {(e.topic, e.partition, e.offset) for e in events}
        died = self.journal.matching(keys)
        if died is not None and died.kills >= MAX_KILLS:
            return self._isolate(msgs, events)
        if events and not self._handle(events):
            return False
        if msgs:
            self.consumer.commit(offsets=_next_offsets(msgs), asynchronous=False)
            for e in events:
                EVENTS_IN.labels(e.topic).inc()
            self._resolve_kills(keys)
        self._lag()
        return True

    def _resolve_kills(self, keys) -> None:
        self.journal.resolve(keys)
        if self.journal.died is None:
            KILLS.set(0)

    def _isolate(self, msgs, events: list[Event]) -> bool:
        """The batch one event at a time, each committed on its own. The event an earlier
        process died on while it was handled alone goes to the dead-letter topic."""
        died = self.journal.died
        culprit = died.current if died.isolated else None
        log.error("handling a batch one event at a time", extra={"fields": died.log_fields()})
        by_key = {(e.topic, e.partition, e.offset): e for e in events}
        for m in msgs:
            if self.ctx.stopping.is_set():
                break  # the rest isn't committed
            self.ctx.liveness.beat()
            key = (m.topic(), m.partition(), m.offset())
            e = by_key.get(key)
            if e is not None:
                if key == culprit:
                    self._dead_letter(m, e, died)
                elif not self._handle([e], died.kills):
                    return False
            self.consumer.commit(offsets=_next_offsets([m]), asynchronous=False)
            if e is not None:
                EVENTS_IN.labels(e.topic).inc()
                if key == culprit:
                    self._resolve_kills({key})
        else:
            # Every event was handled or moved, also when the death left no event behind.
            self._resolve_kills(set(by_key))
        self._lag()
        return True

    def _dead_letter(self, m, e: Event, died) -> None:
        """The message as it was read, in the dead-letter topic; raises when Kafka doesn't
        confirm it (nothing is committed then)."""
        topic = naming.dead_letter_topic(self.ctx.cfg.env, self.ctx.cfg.service)
        if self._dlq is None:
            self._dlq = Producer(
                self.ctx.cfg.kafka_conf()
                | {
                    "client.id": f"{self.ctx.cfg.name}:{self.ctx.cfg.instance}",
                    "enable.idempotence": True,
                    "acks": "all",
                    "compression.type": "zstd",
                    "message.max.bytes": 2 * 1024 * 1024,
                    "delivery.timeout.ms": 60_000,
                }
            )
        headers = {
            "group": self.group,
            "topic": e.topic,
            "partition": e.partition,
            "offset": e.offset,
            "event_id": e.event_id,
            "kills": died.kills,
            "version": self.ctx.cfg.version,
            "reason": "killed the process",
        }
        errors: list = []
        self._dlq.produce(
            topic,
            value=m.value(),
            key=m.key(),
            headers=[(k, str(v).encode()) for k, v in headers.items()],
            on_delivery=lambda err, _msg: errors.append(err),
        )
        self._dlq.flush(PRODUCE_TIMEOUT)
        if not errors or errors[0] is not None:
            why = errors[0] if errors else "no delivery report"
            raise ProduceError(f"{topic}: {why}")
        DEAD_LETTERS.labels(e.topic).inc()
        log.error(
            "event moved to the dead-letter topic",
            extra={"fields": headers | {"dead_letter_topic": topic}},
        )

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

    def _call(self, events: list[Event], ctx, isolated_kills: int | None) -> None:
        def at(e) -> None:
            self.journal.current(e)
            self.slow.current(e)

        self.journal.begin(events, isolated_kills)
        self.slow.begin(events)
        try:
            if isolated_kills is not None:
                at(events[0])
                self.handler(list(events), ctx)
            else:
                self.handler(Watched(events, at), ctx)
        finally:
            self.slow.end()
            self.journal.end()

    def _handle(self, events: list[Event], isolated_kills: int | None = None) -> bool:
        delay = 1.0
        for attempt in range(1, self.max_attempts + 1):
            ctx = _BatchContext(self.ctx)
            t0 = time.monotonic()
            try:
                with lh_log.scope(batch_size=len(events)):
                    self._call(events, ctx, isolated_kills)
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
    except ProduceError as e:
        log.error("produce failed", extra={"fields": {"error": str(e)}})
        return 1
    finally:
        ctx.close()
