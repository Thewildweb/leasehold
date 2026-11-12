"""`leasehold apply`: the environment file, step selection and how failures propagate."""

from __future__ import annotations

from pathlib import Path

import pytest

from leasehold import environment, steps
from leasehold.cli import main

FIXTURES = Path(__file__).parent / "fixtures"
SECRETS = {
    "LEASEHOLD_KAFKA_PASSWORD": "k",
    "LEASEHOLD_DSN": "postgresql://x",
    "LEASEHOLD_CLICKHOUSE_PASSWORD": "c",
    "LEASEHOLD_GRAFANA_TOKEN": "g",
}
FULL = """
env: dev
kafka: {bootstrap: "localhost:19092", client_bootstrap: "kafka:9092"}
postgres: {}
nomad: {url: "http://nomad:4646"}
clickhouse: {url: "http://ch:8123"}
grafana: {url: "http://grafana:3000"}
jobs: {registry: "registry.example", api_url: "http://api:8400"}
"""


@pytest.fixture
def envfile(tmp_path):
    p = tmp_path / "dev.yml"
    p.write_text(FULL)
    return p


@pytest.fixture
def secrets(monkeypatch):
    for k, v in SECRETS.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("NOMAD_TOKEN", raising=False)


@pytest.fixture
def fake_steps(monkeypatch):
    """Replace every step; record calls. Set ``results[name]`` to a Result or exception."""
    calls: list[tuple[str, dict]] = []
    results: dict[str, object] = {}

    def make(name):
        def step(out, *args, **kw):
            calls.append((name, kw))
            r = results.get(name, steps.Result())
            if isinstance(r, Exception):
                raise r
            return r

        return step

    for name in steps.ORDER:
        monkeypatch.setattr(steps, name, make(name))
    return calls, results


def _run(envfile, *extra):
    return main(["apply", str(FIXTURES), "-e", str(envfile), "--sha", "abc1234", *extra])


def test_load_rejects_unknown_keys_and_bad_env(tmp_path):
    p = tmp_path / "e.yml"
    p.write_text("env: dev\nkafka: {bootstrap: x, pasword_env: Y}\n")
    with pytest.raises(environment.EnvironmentFileError, match="pasword_env"):
        environment.load(p)
    p.write_text("env: Staging\n")
    with pytest.raises(environment.EnvironmentFileError):
        environment.load(p)


def test_runs_all_steps_in_order_with_environment_values(envfile, secrets, fake_steps, capsys):
    calls, _ = fake_steps
    assert _run(envfile) == 0
    assert [n for n, _ in calls] == list(steps.ORDER)
    kw = dict(calls)
    assert kw["kafka"]["bootstrap"] == "localhost:19092"
    assert kw["clickhouse"]["kafka_bootstrap"] == "kafka:9092"  # client address
    assert kw["nomad"]["kafka_bootstrap"] == "kafka:9092"
    assert kw["nomad"]["sha"] == "abc1234" and kw["nomad"]["token"] is None
    assert all(not k.get("yes") for k in kw.values())
    assert "no changes" in capsys.readouterr().out


def test_missing_secrets_sections_and_sha_stop_before_any_step(
    tmp_path, secrets, fake_steps, monkeypatch, capsys
):
    calls, _ = fake_steps
    monkeypatch.delenv("LEASEHOLD_CLICKHOUSE_PASSWORD")
    p = tmp_path / "e.yml"
    p.write_text("env: dev\nkafka: {bootstrap: x}\nnomad: {url: y}\nclickhouse: {url: z}\n")
    assert main(["apply", str(FIXTURES), "-e", str(p)]) == 1
    err = capsys.readouterr().err
    assert "step frontier needs the 'postgres' section" in err
    assert "step nomad needs --sha" in err
    assert "$LEASEHOLD_CLICKHOUSE_PASSWORD" in err
    assert calls == []


def test_only_and_skip(envfile, secrets, fake_steps):
    calls, _ = fake_steps
    assert _run(envfile, "--only", "nomad", "--only", "kafka") == 0
    assert [n for n, _ in calls] == ["kafka", "nomad"]
    calls.clear()
    assert main(["apply", str(FIXTURES), "-e", str(envfile), "--skip", "nomad"]) == 0  # no --sha
    assert "nomad" not in [n for n, _ in calls]


def test_only_needs_only_its_secrets(tmp_path, fake_steps, monkeypatch):
    calls, _ = fake_steps
    for k in SECRETS:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("LEASEHOLD_GRAFANA_TOKEN", "g")
    p = tmp_path / "e.yml"
    p.write_text("env: dev\ngrafana: {url: http://g}\n")
    assert main(["apply", str(FIXTURES), "-e", str(p), "--only", "grafana"]) == 0
    assert [n for n, _ in calls] == ["grafana"]


def test_dry_run_passes_new_passwords_to_clickhouse(envfile, secrets, fake_steps):
    calls, results = fake_steps
    results["credentials"] = steps.Result(changes=1, new_passwords={"dev.certwatch.clickhouse"})
    _run(envfile)
    assert dict(calls)["clickhouse"]["assume_passwords"] == {"dev.certwatch.clickhouse"}


def test_failure_stops_later_steps_when_applying(envfile, secrets, fake_steps, capsys):
    calls, results = fake_steps
    results["credentials"] = RuntimeError("nomad unreachable")
    assert _run(envfile, "--yes") == 1
    assert [n for n, _ in calls] == ["kafka", "frontier", "credentials"]
    out = capsys.readouterr().out
    assert "failed: RuntimeError: nomad unreachable" in out
    assert "clickhouse   not run" in out


def test_failure_in_dry_run_continues(envfile, secrets, fake_steps, capsys):
    calls, results = fake_steps
    results["kafka"] = RuntimeError("broker down")
    assert _run(envfile) == 1
    assert [n for n, _ in calls] == list(steps.ORDER)


def test_skipped_sinks_fail_the_run_but_later_steps_still_apply(envfile, secrets, fake_steps):
    calls, results = fake_steps
    results["clickhouse"] = steps.Result(changes=1, applied=True, skipped=["dev_x.t"])
    assert _run(envfile, "--yes") == 1
    assert [n for n, _ in calls][-2:] == ["grafana", "nomad"]


def test_single_step_command_reports_dry_run(secrets, fake_steps, capsys, monkeypatch):
    _, results = fake_steps
    results["kafka"] = steps.Result(changes=3)
    assert main(["kafka", "apply", str(FIXTURES), "--env", "dev", "--bootstrap", "x"]) == 0
    out = capsys.readouterr().out
    assert "3 changes" in out and "dry run; pass --yes" in out
