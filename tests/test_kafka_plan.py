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
    assert set(topics) == {"certwatch.entries.v1", "resolver.results.v1"}
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
    assert set(p.acls) == {
        # certwatch worker writes its own topic
        AclPlan("User:dev.certwatch.worker", "TOPIC", "dev.certwatch.entries.v1", "WRITE"),
        AclPlan("User:dev.certwatch.worker", "TOPIC", "dev.certwatch.entries.v1", "DESCRIBE"),
        # resolver's bridge processor reads certwatch's topic with its own group
        AclPlan("User:dev.resolver.new-domains", "TOPIC", "dev.certwatch.entries.v1", "READ"),
        AclPlan("User:dev.resolver.new-domains", "TOPIC", "dev.certwatch.entries.v1", "DESCRIBE"),
        AclPlan("User:dev.resolver.new-domains", "GROUP", "dev.resolver.new-domains", "READ"),
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
