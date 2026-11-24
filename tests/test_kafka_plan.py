from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from leasehold import kafka
from leasehold.kafka import AclPlan
from leasehold.spec import ServiceSpec, find_specs, load_spec

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def specs():
    return [load_spec(p) for p in find_specs([FIXTURES])]


def test_topics_with_retention_in_ms(specs):
    p = kafka.plan(specs, "prod")
    topics = {t.name: t for t in p.topics}
    assert set(topics) == {
        "certwatch.entries.v1",
        "resolver.results.v1",
        "resolver.dead-letters.v1",
    }
    entries = topics["certwatch.entries.v1"]
    assert entries.partitions == 12
    assert entries.config_dict() == {"cleanup.policy": "delete", "retention.ms": "86400000"}


def test_retention_bytes_only_when_set(specs):
    data = yaml.safe_load((FIXTURES / "certwatch" / "service.yml").read_text())
    data["kafka"]["produces"]["entries"]["retention_bytes"] = 1024
    p = kafka.plan([ServiceSpec.model_validate(data)], "prod")
    assert p.topics[0].config_dict()["retention.bytes"] == "1024"


def test_exact_acls(specs):
    p = kafka.plan(specs, "dev")
    assert {a for a in p.acls if a.principal != "User:dev.leasehold.monitor"} == {
        # certwatch worker writes its own topic
        AclPlan("User:dev.certwatch.worker", "TOPIC", "dev.certwatch.entries.v1", "WRITE"),
        AclPlan("User:dev.certwatch.worker", "TOPIC", "dev.certwatch.entries.v1", "DESCRIBE"),
        # resolver's bridge processor reads certwatch's topic with its own group
        AclPlan("User:dev.resolver.new-domains", "TOPIC", "dev.certwatch.entries.v1", "READ"),
        AclPlan("User:dev.resolver.new-domains", "TOPIC", "dev.certwatch.entries.v1", "DESCRIBE"),
        AclPlan("User:dev.resolver.new-domains", "GROUP", "dev.resolver.new-domains", "READ"),
        # ... and may write its service's dead-letter topic
        AclPlan("User:dev.resolver.new-domains", "TOPIC", "dev.resolver.dead-letters.v1", "WRITE"),
        AclPlan(
            "User:dev.resolver.new-domains", "TOPIC", "dev.resolver.dead-letters.v1", "DESCRIBE"
        ),
        # resolver worker writes its own topic
        AclPlan("User:dev.resolver.worker", "TOPIC", "dev.resolver.results.v1", "WRITE"),
        AclPlan("User:dev.resolver.worker", "TOPIC", "dev.resolver.results.v1", "DESCRIBE"),
        # ClickHouse: one principal per service, one group per sink
        AclPlan("User:dev.certwatch.clickhouse", "TOPIC", "dev.certwatch.entries.v1", "READ"),
        AclPlan("User:dev.certwatch.clickhouse", "TOPIC", "dev.certwatch.entries.v1", "DESCRIBE"),
        AclPlan(
            "User:dev.certwatch.clickhouse", "GROUP", "dev.certwatch.clickhouse.domains", "READ"
        ),
        AclPlan(
            "User:dev.certwatch.clickhouse",
            "GROUP",
            "dev.certwatch.clickhouse.chunk_attempts",
            "READ",
        ),
    }


def test_components_without_kafka_get_no_principal(specs):
    # The planner only creates tasks; it never talks to Kafka.
    p = kafka.plan(specs, "prod")
    assert "certwatch.planner" not in p.principals
    assert p.principals == [
        "certwatch.clickhouse",
        "certwatch.worker",
        "leasehold.monitor",
        "resolver.new-domains",
        "resolver.worker",
    ]


def test_prod_and_dev_never_share_names(specs):
    prod, dev = kafka.plan(specs, "prod"), kafka.plan(specs, "dev")
    assert not {t.name for t in prod.topics} & {t.name for t in dev.topics}
    assert not set(prod.principals) & set(dev.principals)
    assert not {a.resource_name for a in prod.acls} & {a.resource_name for a in dev.acls}


def test_component_name_clickhouse_is_reserved():
    data = yaml.safe_load((FIXTURES / "certwatch" / "service.yml").read_text())
    data["components"]["clickhouse"] = data["components"].pop("planner")
    with pytest.raises(ValidationError, match="reserved"):
        ServiceSpec.model_validate(data)


def _with_planner() -> ServiceSpec:
    data = yaml.safe_load((FIXTURES / "certwatch" / "service.yml").read_text())
    data["frontier"]["task_types"]["certwatch.recheck"] = {"key": "domain"}
    data["components"]["worker"]["consumes_tasks"].append("certwatch.recheck")
    data["components"]["plan-recheck"] = {
        "kind": "scheduled",
        "schedule": "@hourly",
        "node_class": "services",
        "resources": {"cpu": 100, "memory": 64},
        "planner": {
            "task_type": "certwatch.recheck",
            "reads": ["certwatch.domains"],
            "backlog_target": 10,
            "priority": 50,
        },
    }
    return ServiceSpec.model_validate(data)


def test_plan_log_topic_and_acls():
    p = kafka.plan([_with_planner()], "dev")
    assert "dev.certwatch.planned.v1" in {t.name for t in p.topics}
    planner = "User:dev.certwatch.plan-recheck"
    sink = "User:dev.certwatch.clickhouse"
    assert AclPlan(planner, "TOPIC", "dev.certwatch.planned.v1", "WRITE") in p.acls
    assert AclPlan(sink, "TOPIC", "dev.certwatch.planned.v1", "READ") in p.acls
    for group in ("planned", "planner_runs"):
        assert AclPlan(sink, "GROUP", f"dev.certwatch.clickhouse.{group}", "READ") in p.acls
    # Other components don't get the plan log.
    assert not any(
        a.principal == "User:dev.certwatch.worker" and "planned" in a.resource_name for a in p.acls
    )


def test_monitor_may_only_describe_every_topic_and_group(specs):
    p = kafka.plan(specs, "dev")
    acls = [a for a in p.acls if a.principal == "User:dev.leasehold.monitor"]
    assert {a.operation for a in acls} == {"DESCRIBE"}
    assert {a.resource_name for a in acls if a.resource_type == "TOPIC"} == {
        t.name for t in p.topics
    }
    assert {a.resource_name for a in acls if a.resource_type == "GROUP"} == {
        "dev.resolver.new-domains",
        "dev.certwatch.clickhouse.domains",
        "dev.certwatch.clickhouse.chunk_attempts",
    }


def test_dead_letters_are_kept_30_days(specs):
    (t,) = [t for t in kafka.plan(specs, "dev").topics if "dead-letters" in t.name]
    assert (t.name, t.partitions) == ("dev.resolver.dead-letters.v1", 1)
    assert t.config_dict()["retention.ms"] == str(30 * 86400 * 1000)
