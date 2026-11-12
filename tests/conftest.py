"""Shared test helpers: a small service spec for runtime tests, and fakes."""

from __future__ import annotations

import threading
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from leasehold.client import Applied, LeasedTask, LeaseRef, Unavailable
from leasehold.runtime.config import RuntimeConfig
from leasehold.spec import ServiceSpec

RUNTIME_SPEC = {
    "spec_version": 1,
    "name": "jobs",
    "description": "runtime test service",
    "owner": "test",
    "components": {
        "worker": {
            "kind": "task-worker",
            "consumes_tasks": ["jobs.item"],
            "produces": ["results"],
            "node_class": "processing",
            "resources": {"cpu": 100, "memory": 64},
            "count": {"min": 1, "max": 2},
        },
        "router": {
            "kind": "processor",
            "consumes": ["jobs.results.v1"],
            "creates_tasks": ["jobs.item"],
            "produces": ["routed"],
            "node_class": "services",
            "resources": {"cpu": 100, "memory": 64},
        },
        "planner": {
            "kind": "scheduled",
            "schedule": "*/5 * * * *",
            "creates_tasks": ["jobs.item"],
            "node_class": "services",
            "resources": {"cpu": 100, "memory": 64},
        },
    },
    "kafka": {
        "produces": {
            "results": {
                "payload_model": "payload_models:R",
                "partitions": 3,
                "retention": "1h",
                "key": "id",
            },
            "routed": {"payload_model": "payload_models:S", "partitions": 1, "retention": "1h"},
        }
    },
    "frontier": {"task_types": {"jobs.item": {"key": "id", "lease_duration": "30s"}}},
}


def runtime_spec() -> ServiceSpec:
    return ServiceSpec.model_validate(RUNTIME_SPEC)


def runtime_cfg(component: str, env: str = "dev", **kw) -> RuntimeConfig:
    kw.setdefault("payload_models", {"jobs.results.v1": "payload_models:R"})
    return RuntimeConfig(
        env=env, service="jobs", component=component, spec=runtime_spec(), instance="alloc-1", **kw
    )


@pytest.fixture
def spec():
    return runtime_spec()


class FakeKafkaProducer:
    """Stands in for confluent_kafka.Producer; deliveries are reported on poll()."""

    def __init__(self, fail_topics=()):
        self.fail_topics = set(fail_topics)
        self.messages = []
        self._pending = []
        self.lock = threading.Lock()

    def produce(self, topic, value=None, key=None, on_delivery=None):
        with self.lock:
            self.messages.append((topic, key, value))
            self._pending.append((topic, on_delivery))

    def poll(self, timeout=0):
        with self.lock:
            pending, self._pending = self._pending, []
        for topic, cb in pending:
            cb("broker said no" if topic in self.fail_topics else None, None)
        return len(pending)

    def flush(self, timeout=0):
        self.poll()
        return 0


class FakeControl:
    """An in-memory control API with the client's interface."""

    def __init__(self, keys=(), lease_s=30.0, task_type="jobs.item"):
        self.task_type = task_type
        self.lease_s = lease_s
        self.pending = list(keys)
        self.leases: dict[LeaseRef, datetime] = {}
        self.acked: list = []
        self.failed: list = []
        self.released: list = []
        self.upserts: list = []
        self.down = False
        self.lost_on_renew: set[str] = set()
        self.lock = threading.Lock()

    def _check(self):
        if self.down:
            raise Unavailable("down")

    def lease(self, max, task_types=None, worker_id=None):
        self._check()
        with self.lock:
            out = []
            while self.pending and len(out) < max:
                key = self.pending.pop(0)
                ref = LeaseRef(self.task_type, key, uuid.uuid4())
                exp = datetime.now(UTC) + timedelta(seconds=self.lease_s)
                self.leases[ref] = exp
                out.append(LeasedTask(ref, exp, 50, 0, {"id": key}))
            return out

    def renew(self, refs):
        self._check()
        renewed, lost = {}, []
        for r in refs:
            if r in self.leases and r.key not in self.lost_on_renew:
                exp = datetime.now(UTC) + timedelta(seconds=self.lease_s)
                self.leases[r] = exp
                renewed[r] = exp
            else:
                self.leases.pop(r, None)
                lost.append(r)
        return renewed, lost

    def ack(self, items):
        self._check()
        self.acked.extend(items)
        return Applied(len(items))

    def fail(self, items):
        self._check()
        self.failed.extend(items)
        return Applied(len(items))

    def release(self, refs):
        self._check()
        self.released.extend(refs)
        for r in refs:
            self.leases.pop(r, None)
        return Applied(len(refs))

    def upsert(self, task_type, tasks, reopen=False):
        self._check()
        self.upserts.append((task_type, list(tasks)))
        return len(tasks), 0

    def close(self):
        pass
