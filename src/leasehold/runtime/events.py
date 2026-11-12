"""Events: the shared envelope, a producer that confirms delivery, and parsing.

Every message on every topic is one JSON object:

    {
      "event_id": "…",            uuid4, unique per event; consumers dedup on it
      "schema_version": 1,         the topic's major version
      "service": "certlog", "component": "worker", "env": "prod",
      "worker": "<instance>",      Nomad allocation ID
      "task_type": "…", "task_key": "…",     when produced while handling a task
      "target": "…", "egress_ip": "…",       when the event is about a fetch
      "ts": "2026-11-11T12:00:00.123Z",
      "payload": { … },
      "payload_ref": null          reserved for large payloads in object storage
    }

The Kafka message key is the payload field named by the topic's ``key`` in
``service.yml``, unless a key is passed explicitly.

A component may only produce to the topics in its ``produces`` list; the
broker's ACLs enforce the same thing.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from confluent_kafka import KafkaError, Producer
from prometheus_client import Counter
from pydantic import BaseModel

from leasehold import naming
from leasehold.runtime.config import RuntimeConfig

log = logging.getLogger("leasehold")

# Below the broker's message.max.bytes (2 MiB), leaving room for the envelope.
MAX_PAYLOAD_BYTES = 1_900_000

EVENTS = Counter("leasehold_events_produced", "Events confirmed by Kafka", ["topic"])
PRODUCE_ERRORS = Counter("leasehold_produce_errors", "Events Kafka didn't accept", ["topic"])


class ProduceError(RuntimeError):
    pass


class PayloadTooLarge(ProduceError):
    pass


def _ts() -> str:
    t = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int(t * 1000) % 1000:03d}Z"


def _payload_dict(payload: Any) -> Any:
    if isinstance(payload, BaseModel):
        return payload.model_dump(mode="json")
    return payload


class Delivery:
    """The outcome of one produce; ``wait`` blocks until Kafka confirmed or refused it."""

    def __init__(self, event_id: uuid.UUID, topic: str):
        self.event_id = event_id
        self.topic = topic
        self._done = threading.Event()
        self.error: str | None = None

    def _set(self, err) -> None:
        if err is not None:
            self.error = str(err)
            PRODUCE_ERRORS.labels(self.topic).inc()
        else:
            EVENTS.labels(self.topic).inc()
        self._done.set()

    def wait(self, timeout: float = 60.0) -> None:
        if not self._done.wait(timeout):
            raise ProduceError(f"{self.topic}: no delivery report within {timeout}s")
        if self.error:
            raise ProduceError(f"{self.topic}: {self.error}")


class EventProducer:
    """One per process. Thread-safe: ``emit`` may be called from worker threads."""

    def __init__(self, cfg: RuntimeConfig, producer: Producer | None = None):
        self.cfg = cfg
        spec = cfg.spec
        self.allowed = set(cfg.component_spec.produces)
        self.topics = {
            name: (naming.topic_for(cfg.env, spec.topic_ref(name)), t)
            for name, t in spec.kafka.produces.items()
        }
        self._producer = producer or Producer(
            cfg.kafka_conf()
            | {
                "client.id": f"{cfg.name}:{cfg.instance}",
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 50,
                "message.max.bytes": 2 * 1024 * 1024,
                "delivery.timeout.ms": 120_000,
            }
        )
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll, name="leasehold-producer", daemon=True)
        self._poller.start()

    def _poll(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.1)

    def emit(
        self,
        topic: str,
        payload: Any,
        *,
        key: str | None = None,
        task_type: str | None = None,
        task_key: str | None = None,
        target: str | None = None,
        egress_ip: str | None = None,
        event_id: uuid.UUID | None = None,
    ) -> Delivery:
        """Produce one event to one of this service's topics (by its name in
        service.yml, e.g. ``entries``)."""
        if topic not in self.allowed:
            raise ProduceError(
                f"{self.cfg.name} may not produce to {topic!r}; add it to `produces`"
            )
        actual, spec = self.topics[topic]
        body = _payload_dict(payload)
        if key is None and spec.key and isinstance(body, dict) and spec.key in body:
            key = str(body[spec.key])
        event_id = event_id or uuid.uuid4()
        envelope = {
            "event_id": str(event_id),
            "schema_version": spec.version,
            "service": self.cfg.service,
            "component": self.cfg.component,
            "env": self.cfg.env,
            "worker": self.cfg.instance,
            "task_type": task_type,
            "task_key": task_key,
            "target": target,
            "egress_ip": egress_ip,
            "ts": _ts(),
            "payload": body,
            "payload_ref": None,
        }
        value = json.dumps(envelope, separators=(",", ":"), default=str).encode()
        if len(value) > MAX_PAYLOAD_BYTES:
            raise PayloadTooLarge(f"{topic}: event is {len(value)} bytes (max {MAX_PAYLOAD_BYTES})")
        d = Delivery(event_id, actual)
        while True:
            try:
                self._producer.produce(
                    actual,
                    value=value,
                    key=key.encode() if key else None,
                    on_delivery=lambda err, msg: d._set(err),
                )
                return d
            except BufferError:
                # Local queue full: wait for deliveries to drain it.
                self._producer.poll(0.5)

    def flush(self, timeout: float = 30.0) -> int:
        """Wait for outstanding deliveries; returns how many are still pending."""
        return self._producer.flush(timeout)

    def close(self, timeout: float = 30.0) -> None:
        left = self.flush(timeout)
        if left:
            log.error("events not delivered at shutdown", extra={"fields": {"pending": left}})
        self._stop.set()
        self._poller.join(timeout=2)


@dataclass(frozen=True)
class Event:
    """A consumed message."""

    topic: str  # actual topic name, e.g. dev.certlog.entries.v1
    ref: naming.TopicRef  # environment-independent, e.g. certlog.entries.v1
    partition: int
    offset: int
    key: str | None
    event_id: str
    schema_version: int
    service: str
    component: str
    ts: str
    payload: Any
    envelope: dict

    @property
    def task_type(self) -> str | None:
        return self.envelope.get("task_type")

    @property
    def task_key(self) -> str | None:
        return self.envelope.get("task_key")


class InvalidEvent(ValueError):
    pass


def parse(msg, ref: naming.TopicRef) -> Event:
    """Parse a confluent_kafka.Message into an Event; InvalidEvent if it isn't one."""
    if msg.error():
        raise InvalidEvent(str(msg.error()))
    try:
        env = json.loads(msg.value())
        return Event(
            topic=msg.topic(),
            ref=ref,
            partition=msg.partition(),
            offset=msg.offset(),
            key=msg.key().decode() if msg.key() else None,
            event_id=env["event_id"],
            schema_version=int(env["schema_version"]),
            service=env["service"],
            component=env["component"],
            ts=env["ts"],
            payload=env.get("payload"),
            envelope=env,
        )
    except (ValueError, KeyError, TypeError) as e:
        raise InvalidEvent(f"{msg.topic()}[{msg.partition()}]@{msg.offset()}: {e}") from e


def is_fatal(err: KafkaError | None) -> bool:
    return bool(err is not None and err.fatal())
