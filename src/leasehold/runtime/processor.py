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
- Reads batches of up to ``batch_size`` events (or what arrived within
  ``batch_wait`` seconds) and calls ``handle(events, ctx)`` once per batch.
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
import logging
import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client import Counter, Gauge, Histogram
from pydantic import BaseModel, ValidationError

from leasehold import naming
from leasehold.runtime import log as lh_log
from leasehold.runtime import payloads
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
LAG = Gauge(
    "leasehold_kafka_consumer_lag",
    "Messages behind the end of the partition",
    ["group", "topic", "partition"],
)

LAG_INTERVAL = 30.0
PRODUCE_TIMEOUT = 120.0

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
        consumer: Consumer | None = None,
        models: dict[str, type[BaseModel]] | None = None,
    ):
        """``models``: per consumed topic reference, instead of LEASEHOLD_PAYLOAD_MODELS."""
        cfg = ctx.cfg
        comp = cfg.component_spec
        self.ctx = ctx
        self.handler = handler
        self.batch_size = batch_size
        self.batch_wait = batch_wait
        self.max_attempts = max_attempts
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
        )
        self.next_lag = 0.0

    def run(self) -> int:
        self.consumer.subscribe(sorted(self.refs))
        log.info(
            "processor started",
            extra={"fields": {"group": self.group, "topics": sorted(self.refs)}},
        )
        try:
            while not self.ctx.stopping.is_set():
                self.ctx.liveness.beat()
                if not self.step():
                    return 1
        finally:
            self.consumer.close()
            log.info("processor stopped")
        return 0

    def step(self) -> bool:
        """Read and handle one batch. False when the batch kept failing."""
        msgs = self.consumer.consume(self.batch_size, self.batch_wait)
        events: list[Event] = []
        for m in msgs:
            if m.error():
                if m.error().fatal():
                    raise KafkaException(m.error())
                log.warning("consumer error", extra={"fields": {"error": str(m.error())}})
                continue
            try:
                events.append(self._event(m))
            except InvalidEvent as e:
                INVALID.labels(m.topic()).inc()
                log.error("skipping invalid event", extra={"fields": {"error": str(e)}})
        if events and not self._handle(events):
            return False
        valid = [m for m in msgs if not m.error()]
        if valid:
            self.consumer.commit(offsets=_next_offsets(valid), asynchronous=False)
            for e in events:
                EVENTS_IN.labels(e.topic).inc()
        self._lag()
        return True

    def _event(self, m) -> Event:
        ref = self.refs[m.topic()]
        e = parse(m, ref)
        try:
            body = payloads.validated(self.models[str(ref)], e.payload)
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
    ctx: Context | None = None,
) -> int:
    """Run a processor until SIGTERM. Returns the process exit code."""
    ctx = ctx or build("processor")
    ctx.install_signal_handlers()
    ctx.serve_http()
    try:
        return Processor(
            ctx, handler, batch_size=batch_size, batch_wait=batch_wait, max_attempts=max_attempts
        ).run()
    except ProduceError:
        return 1
    finally:
        ctx.close()
