import copy
import json

import pytest
from conftest import RUNTIME_SPEC, FakeControl, FakeKafkaProducer

from leasehold.client import NewTask
from leasehold.runtime.config import ConfigError, RuntimeConfig
from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer
from leasehold.runtime.planner import PlannerRun, run_planner
from leasehold.spec import ServiceSpec


def _spec(**planner) -> ServiceSpec:
    data = copy.deepcopy(RUNTIME_SPEC)
    data["components"]["plan-new"] = {
        "kind": "scheduled",
        "schedule": "*/5 * * * *",
        "node_class": "services",
        "resources": {"cpu": 100, "memory": 64},
        "planner": {
            "task_type": "jobs.item",
            "reads": ["other.domains"],
            "backlog_target": 100,
            "batch_max": 30,
            "priority": 70,
            "exclude_planned_for": "6h",
            **planner,
        },
    }
    data["clickhouse"] = {"database": "jobs"}
    return ServiceSpec.model_validate(data)


class FakeCH:
    def __init__(self, watermark=None):
        self.watermark = watermark
        self.queries = []

    def query(self, sql, **params):
        self.queries.append((sql, params))
        if "planner_runs" in sql:
            return [{"watermark": self.watermark}] if self.watermark else []
        return []

    def close(self):
        pass


class Control(FakeControl):
    def __init__(self, eligible=0, leased=0):
        super().__init__()
        self.eligible, self.leased = eligible, leased

    def task_types(self):
        return [{"name": "jobs.item", "eligible": self.eligible, "leased": self.leased}]


def _run(select, *, eligible=0, leased=0, watermark=None, full=False, **planner):
    spec = _spec(**planner)
    cfg = RuntimeConfig(env="dev", service="jobs", component="plan-new", spec=spec)
    kafka = FakeKafkaProducer()
    control = Control(eligible, leased)
    ctx = Context(cfg, client=control, producer=EventProducer(cfg, producer=kafka))
    ch = FakeCH(watermark)
    code = run_planner(select, ctx=ctx, clickhouse=ch, full=full)
    events = [json.loads(v) for _, _, v in kafka.messages]
    return code, control, events, ch


def _run_record(events):
    (rec,) = [e["payload"] for e in events if e["payload"]["kind"] == "run"]
    return rec


def test_tops_up_to_the_limit_with_priority_and_reopen():
    seen = {}

    def select(run: PlannerRun):
        seen["limit"] = run.limit
        seen["watermark"] = run.watermark
        run.advance("2026-11-14 10:00:00")
        return [f"d{i}.nl" for i in range(run.limit)]

    code, control, events, _ = _run(select, eligible=60, leased=20, watermark="w0")
    assert code == 0
    assert seen == {"limit": 20, "watermark": "w0"}  # min(batch_max 30, 100 - 80)
    ((task_type, tasks),) = control.upserts
    assert task_type == "jobs.item" and len(tasks) == 20
    assert {t.priority for t in tasks} == {70}
    rec = _run_record(events)
    assert rec["status"] == "ok" and rec["selected"] == 20 and rec["backlog_before"] == 80
    assert rec["watermark_from"] == "w0" and rec["watermark"] == "2026-11-14 10:00:00"
    (keys,) = [e for e in events if e["payload"]["kind"] == "keys"]
    assert keys["payload"]["keys"][:2] == ["d0.nl", "d1.nl"]
    assert all(e.get("payload") and e["schema_version"] == 1 for e in events)


def test_backlog_full_skips_without_selecting():
    def select(run):
        raise AssertionError("not called")

    code, control, events, _ = _run(select, eligible=100)
    assert code == 0 and control.upserts == []
    assert _run_record(events)["status"] == "skipped"


def test_keeps_watermark_when_select_does_not_advance():
    code, _, events, _ = _run(lambda run: [], watermark="w0")
    assert code == 0 and _run_record(events)["watermark"] == "w0"


def test_full_run_ignores_watermark():
    seen = {}

    def select(run):
        seen["w"] = run.watermark
        return []

    code, _, _, ch = _run(select, full=True, watermark="w0")
    assert code == 0 and seen["w"] is None
    assert not any("planner_runs" in q for q, _ in ch.queries)


def test_select_over_the_limit_fails_and_is_recorded():
    code, control, events, _ = _run(lambda run: ["a"] * (run.limit + 1))
    assert code == 1 and control.upserts == []
    rec = _run_record(events)
    assert rec["status"] == "failed" and "more than limit" in rec["error_short"]


def test_payload_and_group_pass_through_priority_overridden():
    code, control, _, _ = _run(
        lambda run: [NewTask("a.nl", priority=5, payload={"p": 1}, group="g")]
    )
    (task,) = control.upserts[0][1]
    assert (task.priority, task.payload, task.group) == (70, {"p": 1}, "g")


def test_chunks_of_1000():
    code, control, events, _ = _run(
        lambda run: [f"d{i}" for i in range(run.limit)], backlog_target=2500, batch_max=2500
    )
    assert [len(t) for _, t in control.upserts] == [1000, 1000, 500]
    assert len([e for e in events if e["payload"]["kind"] == "keys"]) == 3


def test_table_names_only_for_declared_reads(monkeypatch):
    monkeypatch.setenv("LEASEHOLD_PLANNER_DATABASES", '{"other": "other_db"}')
    out = {}

    def select(run):
        out["other"] = run.table("other.domains")
        out["own"] = run.table("planned")
        out["sub"] = run.recently_planned()
        with pytest.raises(ConfigError, match="planner.reads"):
            run.table("third.secret")
        return []

    assert _run(select)[0] == 0
    assert out["other"] == "dev_other_db.domains"
    assert out["own"] == "dev_jobs.planned"
    assert "dev_jobs.planned" in out["sub"] and "planner = 'plan-new'" in out["sub"]


def test_component_without_planner_block():
    cfg = RuntimeConfig(env="dev", service="jobs", component="planner", spec=_spec())
    with pytest.raises(ConfigError, match="no planner block"):
        run_planner(lambda run: [], ctx=Context(cfg, client=Control()), clickhouse=FakeCH())
