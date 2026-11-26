import copy
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from leasehold.spec import ServiceSpec, check_registry, load_spec, resource_names
from leasehold.spec.load import SpecError
from leasehold.spec.models import Derived
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


# --- planners and shared tables --------------------------------------------------------------


def _resolver_with_planner(**planner) -> dict:
    data = yaml.safe_load(RESOLVER.read_text())
    data["components"]["plan-new"] = {
        "kind": "scheduled",
        "schedule": "*/5 * * * *",
        "node_class": "services",
        "resources": {"cpu": 200, "memory": 256},
        "planner": {
            "task_type": "resolver.a_record",
            "reads": ["certwatch.domains", "resolver.known"],
            "backlog_target": 20000,
            "priority": 60,
            **planner,
        },
    }
    data["clickhouse"] = {
        "database": "resolver",
        "sinks": {"known": {"topic": "results", "schema_file": "schema/known.sql"}},
    }
    return data


def _certwatch_sharing(certwatch: dict, **shared) -> ServiceSpec:
    certwatch["clickhouse"]["shared"] = shared or {"domains": {"description": "One row per domain"}}
    return ServiceSpec.model_validate(certwatch)


def test_planner_valid_and_creates_its_task_type():
    spec = ServiceSpec.model_validate(_resolver_with_planner())
    comp = spec.components["plan-new"]
    assert comp.planner.batch_max == 50_000
    assert comp.task_types_created() == {"resolver.a_record"}
    assert spec.plan_log_ref() is not None and str(spec.plan_log_ref()) == "resolver.planned.v1"


def test_planner_task_type_must_be_own():
    assert "a planner only queues its own service's work" in errors_of(
        _resolver_with_planner(task_type="certwatch.chunk")
    )


def test_planner_needs_clickhouse():
    data = _resolver_with_planner(reads=[])
    del data["clickhouse"]
    assert "a planner needs a clickhouse section" in errors_of(data)


def test_planner_reads_unknown_own_table():
    assert "this service has no sink or shared table 'nope'" in errors_of(
        _resolver_with_planner(reads=["resolver.nope"])
    )


def test_planner_may_read_own_plan_log():
    ServiceSpec.model_validate(_resolver_with_planner(reads=["resolver.planned"]))


def _resolver_with_reader(**component) -> dict:
    data = _resolver_with_planner()
    del data["components"]["plan-new"]
    data["components"]["baseline"] = {
        "kind": "scheduled",
        "schedule": "17 * * * *",
        "node_class": "services",
        "resources": {"cpu": 200, "memory": 256},
        "reads": ["resolver.known", "certwatch.domains"],
        **component,
    }
    return data


def test_scheduled_reads_without_a_planner():
    spec = ServiceSpec.model_validate(_resolver_with_reader())
    comp = spec.components["baseline"]
    assert comp.reads_clickhouse and comp.planner is None
    assert spec.clickhouse_readers() == {"baseline": ["resolver.known", "certwatch.domains"]}
    assert spec.plan_log_ref() is None  # no planner, no plan log


def test_scheduled_reads_checked_like_a_planners():
    assert "this service has no sink or shared table 'nope'" in errors_of(
        _resolver_with_reader(reads=["resolver.nope"])
    )
    data = _resolver_with_reader()
    del data["clickhouse"]
    assert "reads: needs a clickhouse section" in errors_of(data)


def test_planner_reads_belong_in_the_planner_block():
    data = _resolver_with_planner()
    data["components"]["plan-new"]["reads"] = ["resolver.known"]
    assert "a planner declares its reads in planner.reads" in errors_of(data)


def test_reserved_plan_log_names(certwatch):
    certwatch["clickhouse"]["sinks"]["planned"] = {
        "topic": "entries",
        "schema_file": "schema/domains.sql",
    }
    assert "the table name 'planned' is reserved" in errors_of(certwatch)


def test_shared_must_be_a_sink(certwatch):
    certwatch["clickhouse"]["shared"] = {"nope": {"description": "x"}}
    assert "only those can be shared" in errors_of(certwatch)


def test_registry_planner_reads_shared(certwatch):
    resolver = ServiceSpec.model_validate(_resolver_with_planner())
    errors = check_registry([ServiceSpec.model_validate(certwatch), resolver])
    assert any("which certwatch doesn't share" in e for e in errors)
    assert check_registry([_certwatch_sharing(copy.deepcopy(certwatch)), resolver]) == []
    # Without certwatch in the set: an error, unless partial.
    assert any("no service 'certwatch'" in e for e in check_registry([resolver]))
    assert not any("planner" in e for e in check_registry([resolver], partial=True))


def test_resource_names_planner():
    rows = resource_names(ServiceSpec.model_validate(_resolver_with_planner()), "dev")
    assert ("clickhouse user", "plan-new", "dev.resolver.plan-new") in rows
    assert ("topic", "planned", "dev.resolver.planned.v1") in rows
    assert ("clickhouse sink", "(plan log)", "dev_resolver.planner_runs") in rows


def test_scheduled_reader_gets_a_clickhouse_user_and_job_env():
    import dataclasses

    from leasehold.clickhouse import users
    from leasehold.nomad.render import render_job

    reader = ServiceSpec.model_validate(_resolver_with_reader())
    certwatch = load_spec(CERTWATCH)
    (u,) = users.plan([reader, certwatch], "dev")
    assert (u.component, u.user) == ("baseline", "dev.resolver.baseline")
    assert ("dev_resolver", "*") in u.grants
    assert any(table == "domains" for _, table in u.grants)
    rows = resource_names(reader, "dev")
    assert ("clickhouse user", "baseline", "dev.resolver.baseline") in rows

    from test_nomad import PLATFORM, SHA

    platform = dataclasses.replace(
        PLATFORM, clickhouse_url="http://ch:8123", databases={"certwatch": "certwatch"}
    )
    job = render_job(reader, "baseline", "dev", SHA, platform).job
    env = job["TaskGroups"][0]["Tasks"][0]["Env"]
    assert env["LEASEHOLD_CLICKHOUSE_URL"] == "http://ch:8123"
    assert env["LEASEHOLD_PLANNER_DATABASES"] == '{"certwatch":"certwatch"}'


def test_reads_from_points_reader_grants_and_job_env_at_another_environment():
    import dataclasses

    from leasehold.clickhouse import users
    from leasehold.nomad.render import render_job

    reader = ServiceSpec.model_validate(_resolver_with_reader())
    certwatch = load_spec(CERTWATCH)
    (u,) = users.plan([reader, certwatch], "dev", {"certwatch": "prod"})
    assert ("dev_resolver", "*") in u.grants  # own tables stay in dev
    assert ("certwatch", "domains") in u.grants  # prod's database has no prefix
    assert not any(db == "dev_certwatch" for db, _ in u.grants)

    from test_nomad import PLATFORM, SHA

    platform = dataclasses.replace(
        PLATFORM,
        clickhouse_url="http://ch:8123",
        databases={"certwatch": "certwatch"},
        reads_from={"certwatch": "prod"},
    )
    env = render_job(reader, "baseline", "dev", SHA, platform).job["TaskGroups"][0]["Tasks"][0][
        "Env"
    ]
    assert env["LEASEHOLD_PLANNER_ENVS"] == '{"certwatch":"prod"}'


def test_derived_offset():
    d = Derived.model_validate({"schema_file": "s.sql", "refresh": "1d", "offset": "9h"})
    assert d.offset == timedelta(hours=9)
    assert Derived.model_validate({"schema_file": "s.sql", "refresh": "1d"}).offset is None
    for offset, message in (("1d", "less than refresh"), ("2d", "less than refresh"), (0, "0")):
        with pytest.raises(ValidationError, match=message):
            Derived.model_validate({"schema_file": "s.sql", "refresh": "1d", "offset": offset})
    with pytest.raises(ValidationError, match="duration"):
        Derived.model_validate({"schema_file": "s.sql", "refresh": "1d", "offset": "9"})
