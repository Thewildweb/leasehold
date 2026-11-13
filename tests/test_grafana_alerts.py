"""Alert rules rendered from service.yml (no Grafana needed)."""

from __future__ import annotations

import pytest

from leasehold.grafana.alerts import AlertSpecError, render_when, service_rules
from leasehold.spec import ServiceSpec

BASE = {
    "spec_version": 1,
    "name": "jobs",
    "description": "test",
    "owner": "test",
    "components": {
        "worker": {
            "kind": "task-worker",
            "consumes_tasks": ["jobs.item"],
            "node_class": "processing",
            "resources": {"cpu": 100, "memory": 64},
            "count": {"min": 2, "max": 4},
        },
        "planner": {
            "kind": "scheduled",
            "schedule": "*/5 * * * *",
            "creates_tasks": ["jobs.item"],
            "node_class": "services",
            "resources": {"cpu": 100, "memory": 64},
        },
    },
    "frontier": {"task_types": {"jobs.item": {"key": "id", "max_attempts": 3}}},
}


def spec(**extra) -> ServiceSpec:
    return ServiceSpec.model_validate(BASE | extra)


def rules(s: ServiceSpec, env: str = "prod"):
    return {r.rule["labels"]["rule"]: r for r in service_rules(s, env, "f", "prom", "ch")}


def test_when_functions():
    s = spec()
    assert render_when("dead_tasks(jobs.item) > 5", s, "dev") == (
        'max(leasehold_frontier_dead_tasks{env="dev", task_type="jobs.item"}) > 5'
    )
    assert render_when("delta(dead_tasks(jobs.item)[1h]) > 0", s, "prod") == (
        'delta(max(leasehold_frontier_dead_tasks{env="prod", task_type="jobs.item"})[1h:1m]) > 0'
    )
    ratio = render_when("task_failure_ratio( jobs.item ) > 0.2", s, "prod")
    assert 'op="failed"' in ratio and 'op=~"acked|failed"' in ratio and ratio.endswith("> 0.2")
    with pytest.raises(AlertSpecError, match="not a task type of this service"):
        render_when("dead_tasks(other.item) > 0", s, "prod")


def test_default_rules():
    r = rules(spec())
    assert set(r) == {"worker-crashloop", "worker-below-min", "worker-no-progress-jobs-item"}
    assert "< 2" in r["worker-below-min"].rule["data"][0]["model"]["expr"]
    assert r["worker-below-min"].rule["for"] == "15m"
    labels = r["worker-crashloop"].rule["labels"]
    assert labels == {
        "managed_by": "leasehold",
        "env": "prod",
        "service": "jobs",
        "rule": "worker-crashloop",
        "priority": "high",
        "component": "worker",
    }
    # Overrides: priority, for, off.
    s = spec(
        alerts={
            "defaults": {
                "crashloop": {"enabled": False},
                "no_progress": {"for": "6h", "priority": "warning"},
            }
        }
    )
    r = rules(s)
    assert "worker-crashloop" not in r
    np = r["worker-no-progress-jobs-item"]
    assert np.rule["labels"]["priority"] == "warning"
    assert "[6h]" in np.rule["data"][0]["model"]["expr"]


def test_escalation_and_page():
    s = spec(
        alerts={
            "rules": [
                {
                    "name": "failing",
                    "when": "task_failure_ratio(jobs.item) > 0.2",
                    "for": "1h",
                    "component": "worker",
                    "escalate": [
                        {"after": 0, "priority": "info"},
                        {"after": "6h", "priority": "page"},
                    ],
                }
            ]
        }
    )
    r = rules(s)
    assert (r["failing"].rule["for"], r["failing"].rule["labels"]["priority"]) == ("1h", "info")
    late = r["failing-after-6h"].rule
    assert (late["for"], late["labels"]["priority"], late["labels"]["page"]) == (
        "7h",
        "page",
        "true",
    )
    assert "page" not in r["failing"].rule["labels"]
    assert late["title"] == "jobs: failing for 7h"
    assert rules(s, "dev")["failing"].rule["title"] == "jobs: failing (dev)"


def test_expects_on_clickhouse():
    s = spec(
        clickhouse={"database": "jobs"},
        expects=[
            {
                "name": "rows",
                "sql": "SELECT count() FROM {database}.t",
                "min": 10,
                "max": 100,
                "every": "30m",
                "priority": "high",
            }
        ],
    )
    r = rules(s, "dev")["expect-rows"]
    assert (r.group, r.interval_s) == ("leasehold-every-30m", 1800)
    a, _, c = r.rule["data"]
    assert a["datasourceUid"] == "ch"
    assert a["model"]["rawSql"] == "SELECT count() FROM dev_jobs.t"
    assert c["model"]["expression"] == "$B < 10 || $B > 100"
    assert r.rule["noDataState"] == "Alerting"


def test_hash_follows_content():
    a = rules(spec())["worker-below-min"].rule["annotations"]["leasehold_hash"]
    b = rules(spec())["worker-below-min"].rule["annotations"]["leasehold_hash"]
    changed = BASE["components"]["worker"] | {"count": {"min": 3, "max": 4}}
    c = rules(spec(components=BASE["components"] | {"worker": changed}))["worker-below-min"]
    assert a == b != c.rule["annotations"]["leasehold_hash"]
