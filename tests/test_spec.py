import copy
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from leasehold.spec import ServiceSpec, check_registry, load_spec, resource_names
from leasehold.spec.load import SpecError
from leasehold.types import format_duration, parse_duration

FIXTURES = Path(__file__).parent / "fixtures"
CERTWATCH = FIXTURES / "certwatch" / "service.yml"
RESOLVER = FIXTURES / "resolver" / "service.yml"


@pytest.fixture
def certwatch() -> dict:
    return yaml.safe_load(CERTWATCH.read_text())


def errors_of(data: dict) -> str:
    with pytest.raises(ValidationError) as exc:
        ServiceSpec.model_validate(data)
    return str(exc.value)


# --- durations -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("30s", timedelta(seconds=30)),
        ("15m", timedelta(minutes=15)),
        ("24h", timedelta(days=1)),
        ("7d", timedelta(days=7)),
        ("2w", timedelta(weeks=2)),
        (0, timedelta(0)),
    ],
)
def test_parse_duration(text, expected):
    assert parse_duration(text) == expected


@pytest.mark.parametrize("bad", [60, "60", "1.5h", "1h30m", "", True])
def test_parse_duration_rejects_ambiguous(bad):
    with pytest.raises(ValueError):
        parse_duration(bad)


def test_format_duration_picks_largest_unit():
    assert format_duration(timedelta(hours=24)) == "1d"
    assert format_duration(timedelta(minutes=90)) == "90m"


# --- valid specs -----------------------------------------------------------------------------


def test_fixtures_load():
    cw = load_spec(CERTWATCH)
    assert cw.name == "certwatch"
    assert cw.kafka.produces["entries"].retention == timedelta(hours=24)
    assert load_spec(RESOLVER).components["new-domains"].kind == "processor"


def test_effective_default_alerts(certwatch):
    spec = ServiceSpec.model_validate(certwatch)
    defaults = spec.alerts.effective_defaults()
    assert defaults["no_progress"] == ("high", timedelta(hours=6), True)  # overridden
    assert defaults["lag_growing"] == ("warning", timedelta(hours=1), True)  # built-in
    assert all(priority != "page" for priority, _, _ in defaults.values())


def test_resource_names_dev():
    rows = {(k, d): a for k, d, a in resource_names(load_spec(CERTWATCH), "dev")}
    assert rows[("topic", "entries")] == "dev.certwatch.entries.v1"
    assert rows[("clickhouse database", "certwatch")] == "dev_certwatch"
    assert rows[("nomad job", "worker")] == "certwatch-worker"
    assert rows[("nomad namespace", "-")] == "dev"


# --- strictness ------------------------------------------------------------------------------


def test_unknown_field_is_an_error(certwatch):
    certwatch["components"]["worker"]["replicas"] = 3
    assert "replicas" in errors_of(certwatch)


def test_unknown_component_kind(certwatch):
    certwatch["components"]["worker"]["kind"] = "daemon"
    assert "kind" in errors_of(certwatch)


def test_secrets_in_env_are_rejected(certwatch):
    certwatch["components"]["worker"]["env"] = {"KAFKA_PASSWORD": "hunter2"}
    assert "secrets" in errors_of(certwatch)


def test_bad_cron(certwatch):
    certwatch["components"]["planner"]["schedule"] = "every 5 minutes"
    assert "cron" in errors_of(certwatch)


# --- ownership rules -------------------------------------------------------------------------


def test_task_types_must_carry_service_prefix(certwatch):
    tt = certwatch["frontier"]["task_types"].pop("certwatch.chunk")
    certwatch["frontier"]["task_types"]["other.chunk"] = tt
    msg = errors_of(certwatch)
    assert "must start with 'certwatch.'" in msg


def test_cannot_create_another_services_tasks(certwatch):
    certwatch["components"]["planner"]["creates_tasks"].append("resolver.a_record")
    assert "that service's own processor" in errors_of(certwatch)


def test_component_may_only_produce_declared_topics(certwatch):
    certwatch["components"]["worker"]["produces"].append("attempts")
    assert "unknown topic 'attempts'" in errors_of(certwatch)


def test_task_type_without_worker(certwatch):
    certwatch["frontier"]["task_types"]["certwatch.backfill"] = {"key": "log_name:chunk_start"}
    assert "no task-worker consumes this task type" in errors_of(certwatch)


def test_topic_without_producer(certwatch):
    certwatch["components"]["worker"]["produces"] = []
    assert "no component produces this topic" in errors_of(certwatch)


def test_scaling_metric_must_fit_kind(certwatch):
    certwatch["components"]["worker"]["scaling"]["metric"] = "consumer_lag"
    assert "eligible_tasks" in errors_of(certwatch)


def test_sink_on_unknown_own_topic(certwatch):
    certwatch["clickhouse"]["sinks"]["domains"]["topic"] = "certwatch.entries.v2"
    assert "refers to version 2" in errors_of(certwatch)


def test_several_problems_reported_at_once(certwatch, tmp_path):
    certwatch["components"]["worker"]["produces"] = ["nope"]
    certwatch["components"]["planner"]["creates_tasks"] = ["certwatch.other"]
    path = tmp_path / "service.yml"
    path.write_text(yaml.safe_dump(certwatch))
    with pytest.raises(SpecError) as exc:
        load_spec(path)
    joined = "\n".join(exc.value.messages)
    assert "unknown topic 'nope'" in joined
    assert "'certwatch.other' is not one of this service's task types" in joined


# --- alerts ----------------------------------------------------------------------------------


def test_rule_needs_priority_or_escalation(certwatch):
    rule = certwatch["alerts"]["rules"][0]
    rule["priority"] = "high"
    assert "exactly one of" in errors_of(certwatch)


def test_escalation_must_start_at_zero_and_not_lower(certwatch):
    data = copy.deepcopy(certwatch)
    data["alerts"]["rules"][0]["escalate"][0]["after"] = "5m"
    assert "after: 0" in errors_of(data)
    data = copy.deepcopy(certwatch)
    data["alerts"]["rules"][0]["escalate"] = [
        {"after": 0, "priority": "high"},
        {"after": "6h", "priority": "info"},
    ]
    assert "may not lower" in errors_of(data)


def test_rule_component_must_exist(certwatch):
    certwatch["alerts"]["rules"][0]["component"] = "ghost"
    assert "unknown component 'ghost'" in errors_of(certwatch)


# --- registry --------------------------------------------------------------------------------


def test_registry_consistent():
    assert check_registry([load_spec(CERTWATCH), load_spec(RESOLVER)]) == []


def test_registry_unresolved_reference():
    errors = check_registry([load_spec(RESOLVER)])
    assert errors == ["resolver: consumes certwatch.entries.v1, which no service produces"]
    assert check_registry([load_spec(RESOLVER)], partial=True) == []


def test_registry_duplicate_database(certwatch):
    other = copy.deepcopy(certwatch)
    other["name"] = "certwatch-two"
    other["frontier"]["task_types"] = {
        "certwatch-two.chunk": other["frontier"]["task_types"]["certwatch.chunk"]
    }
    for c in other["components"].values():
        if "creates_tasks" in c:
            c["creates_tasks"] = ["certwatch-two.chunk"]
        if "consumes_tasks" in c:
            c["consumes_tasks"] = ["certwatch-two.chunk"]
    other["alerts"]["rules"] = []
    errors = check_registry([load_spec(CERTWATCH), ServiceSpec.model_validate(other)])
    assert errors == ["ClickHouse database 'certwatch' is claimed by 2 services"]


# --- expects ---------------------------------------------------------------------------------


def test_expect_sql_must_not_hardcode_database(certwatch):
    certwatch["expects"][0]["sql"] = "SELECT count() FROM certwatch.chunk_attempts"
    assert "hardcodes database 'certwatch'" in errors_of(certwatch)


def test_expect_sql_renders_per_environment():
    from leasehold import naming

    spec = load_spec(CERTWATCH)
    db = naming.clickhouse_database("dev", spec.clickhouse.database)
    assert "FROM dev_certwatch.chunk_attempts" in spec.expects[0].render_sql(db)


def test_expects_need_clickhouse(certwatch):
    del certwatch["clickhouse"]
    assert "no clickhouse section" in errors_of(certwatch)
