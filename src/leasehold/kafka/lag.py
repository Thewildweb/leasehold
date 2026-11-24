"""How far each consumer group is behind, measured from outside the consumers.

A consumer that hangs or keeps restarting can't report its own lag, and ClickHouse's Kafka
tables don't report any. The control API measures it for every group the services declare
(processors, archive sinks, ClickHouse sinks), with admin requests only: committed offsets,
the partition's first and next offset, and offsets by timestamp. No message is read.

The age of the oldest unread message is found by bisecting time: ``ListOffsets`` with a
timestamp gives the first offset at or after it. That needs one request per step for all
partitions together, and gives the age to about half a percent of the retention.

A partition without a committed offset counts from its first message: every consumer
starts at the earliest offset.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from leasehold import naming
from leasehold.spec.models import ArchiveComponent, Processor, ServiceSpec

log = logging.getLogger("leasehold")

TIMEOUT_S = 15.0
STEPS = 9
# The window searched, as a share of retention: Kafka deletes whole segments, so messages
# stay a while past it.
WINDOW = 1.5


@dataclass(frozen=True, order=True)
class Target:
    group: str
    topic: str
    service: str
    retention_s: float
    retention_bytes: int | None = None


@dataclass(frozen=True)
class Reading:
    partition: int
    low: int
    high: int
    committed: int | None
    oldest_unread_s: float = 0.0

    @property
    def lag(self) -> int:
        return max(0, self.high - max(self.position, self.low))

    @property
    def position(self) -> int:
        return self.low if self.committed is None else self.committed

    @property
    def skipped(self) -> int:
        """Messages deleted before this group read them."""
        return max(0, self.low - self.position)

    @property
    def unread_share(self) -> float:
        """The share of the messages the partition still holds that is unread."""
        held = self.high - self.low
        return self.lag / held if held > 0 else 0.0


@dataclass
class Result:
    readings: dict[Target, list[Reading]] = field(default_factory=dict)
    errors: dict[Target, str] = field(default_factory=dict)
    dead_letters: dict[str, int] = field(default_factory=dict)  # service → next offset


def targets(specs: Iterable[ServiceSpec], env: str) -> list[Target]:
    specs = list(specs)
    topics = {s.topic_ref(n): t for s in specs for n, t in s.topics().items()}
    out: set[Target] = set()

    def add(group: str, ref: naming.TopicRef, service: str) -> None:
        t = topics.get(ref)
        if t is not None:
            out.add(
                Target(
                    group,
                    naming.topic_for(env, ref),
                    service,
                    t.retention.total_seconds(),
                    t.retention_bytes,
                )
            )

    for s in specs:
        for cname, c in s.all_components().items():
            if isinstance(c, Processor | ArchiveComponent):
                group = naming.consumer_group(env, s.name, cname)
                for ref in c.consumes:
                    add(group, naming.TopicRef.parse(ref), s.name)
        if s.clickhouse:
            for sname, sink in s.sinks().items():
                group = naming.clickhouse_sink_group(env, s.name, sname)
                add(group, s.sink_topic_ref(sink.topic), s.name)
    return sorted(out)


def dead_letter_topics(specs: Iterable[ServiceSpec], env: str) -> dict[str, str]:
    """service → its dead-letter topic."""
    return {
        s.name: naming.dead_letter_topic(env, s.name)
        for s in specs
        if any(isinstance(c, Processor) for c in s.components.values())
    }


class LagMonitor:
    def __init__(
        self,
        admin: AdminClient,
        wanted: list[Target],
        dead_letters: dict[str, str] | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.admin = admin
        self.targets = wanted
        self.dead_letters = dead_letters or {}
        self.clock = clock

    def measure(self) -> Result:
        """One measurement. Raises ``KafkaException`` when Kafka can't be reached; a group
        or topic that can't be read is in ``errors``."""
        r = Result()
        live = self.admin.list_topics(timeout=TIMEOUT_S).topics
        partitions: dict[str, list[int]] = {}
        for name, t in live.items():
            if t.error is None and t.partitions:
                partitions[name] = sorted(t.partitions)

        wanted: list[Target] = []
        for t in self.targets:
            if t.topic in partitions:
                wanted.append(t)
            else:
                # Also what a missing DESCRIBE looks like.
                r.errors[t] = "topic not found"
        tps = {(t.topic, p) for t in wanted for p in partitions[t.topic]}
        dlq = {s: t for s, t in self.dead_letters.items() if t in partitions}
        tps |= {(t, p) for t in dlq.values() for p in partitions[t]}
        low, _ = self._offsets(tps, lambda _k: OffsetSpec.earliest())
        high, _ = self._offsets(tps, lambda _k: OffsetSpec.latest())
        r.dead_letters = {
            s: sum(high.get((t, p), 0) for p in partitions[t]) for s, t in dlq.items()
        }

        committed, errors = self._committed(wanted, partitions)
        r.errors.update(errors)
        for t in wanted:
            if t in r.errors:
                continue
            missing = [
                p
                for p in partitions[t.topic]
                if (t.topic, p) not in low or (t.topic, p) not in high
            ]
            if missing:
                r.errors[t] = f"no offsets for partitions {missing}"
                continue
            r.readings[t] = [
                Reading(p, low[(t.topic, p)], high[(t.topic, p)], committed[t].get(p))
                for p in partitions[t.topic]
            ]
        self._ages(r)
        return r

    def _offsets(self, tps, spec) -> tuple[dict[tuple[str, int], int], set]:
        """(offsets, failed keys). The offset is -1 when no message is at or after a time."""
        if not tps:
            return {}, set()
        request = {TopicPartition(t, p): spec((t, p)) for t, p in tps}
        out: dict[tuple[str, int], int] = {}
        failed = set()
        for tp, fut in self.admin.list_offsets(request, request_timeout=TIMEOUT_S).items():
            try:
                out[(tp.topic, tp.partition)] = fut.result().offset
            except KafkaException:
                failed.add((tp.topic, tp.partition))
        return out, failed

    def _committed(self, wanted: list[Target], partitions):
        by_group: dict[str, list[Target]] = {}
        for t in wanted:
            by_group.setdefault(t.group, []).append(t)
        futures = {}
        for group, ts in by_group.items():
            tps = [TopicPartition(t.topic, p) for t in ts for p in partitions[t.topic]]
            request = [ConsumerGroupTopicPartitions(group, tps)]
            futures[group] = self.admin.list_consumer_group_offsets(
                request, request_timeout=TIMEOUT_S
            )[group]
        out: dict[Target, dict[int, int]] = {}
        errors: dict[Target, str] = {}
        for group, fut in futures.items():
            try:
                got = fut.result().topic_partitions
                bad = next((tp.error for tp in got if tp.error), None)
                if bad is not None:
                    raise KafkaException(bad)
            except KafkaException as e:
                for t in by_group[group]:
                    errors[t] = str(e)
                continue
            for t in by_group[group]:
                out[t] = {
                    tp.partition: tp.offset for tp in got if tp.topic == t.topic and tp.offset >= 0
                }
        return out, errors

    def _ages(self, r: Result) -> None:
        """Find the largest time T with a message at or after T at or before the first
        unread one: that is when the first unread message was written."""
        now = self.clock()
        # (topic, partition, first unread offset) → [lo, hi]; groups at the same offset share.
        spans: dict[tuple[str, int, int], list[float]] = {}
        for t, readings in r.readings.items():
            for x in readings:
                if x.lag:
                    key = (t.topic, x.partition, max(x.position, x.low))
                    spans[key] = [now - t.retention_s * WINDOW, now]
        skip: set[tuple[str, int]] = set()
        for _ in range(STEPS):
            # One request holds one time per partition, so keys that share a partition
            # (several groups) take separate requests.
            rounds: list[dict[tuple[str, int], tuple]] = []
            for key in spans:
                tp = (key[0], key[1])
                if tp in skip:
                    continue
                for rnd in rounds:
                    if tp not in rnd:
                        rnd[tp] = key
                        break
                else:
                    rounds.append({tp: key})
            for rnd in rounds:
                mids = {tp: sum(spans[key]) / 2 for tp, key in rnd.items()}
                got, failed = self._offsets(
                    set(rnd), lambda k, m=mids: OffsetSpec.for_timestamp(int(m[k] * 1000))
                )
                skip |= failed
                for tp, offset in got.items():
                    key = rnd[tp]
                    if 0 <= offset <= key[2]:
                        spans[key][0] = mids[tp]
                    else:
                        spans[key][1] = mids[tp]
        for t in list(r.readings):
            out = []
            for x in r.readings[t]:
                if not x.lag:
                    out.append(x)
                    continue
                if (t.topic, x.partition) in skip:
                    r.errors[t] = f"no offsets by time for partition {x.partition}"
                    break
                lo, _hi = spans[(t.topic, x.partition, max(x.position, x.low))]
                out.append(Reading(x.partition, x.low, x.high, x.committed, max(0.0, now - lo)))
            else:
                r.readings[t] = out
                continue
            del r.readings[t]


__all__ = [
    "LagMonitor", "Reading", "Result", "Target", "dead_letter_topics",
    "targets",
]  # fmt: skip
