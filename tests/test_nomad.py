"""Nomad jobs rendered from service.yml, and the plan/apply diff against a fake Nomad."""

from __future__ import annotations

import copy
from pathlib import Path

import pytest

from leasehold.cli import main
from leasehold.nomad import Platform, plan, render_job
from leasehold.nomad.apply import Nomad, NomadError, diff, diff_lines
from leasehold.nomad.render import with_count
from leasehold.spec import load_spec

FIXTURES = Path(__file__).parent / "fixtures"
SHA = "0123abc"
PLATFORM = Platform(
    registry="ghcr.io/example", api_url="http://api:8400", kafka_bootstrap="kafka:9092"
)


@pytest.fixture
def spec():
    return load_spec(FIXTURES / "certwatch" / "service.yml")


def _task(job):
    return job["TaskGroups"][0]["Tasks"][0]


def test_task_worker_job(spec):
    jp = render_job(spec, "worker", "prod", SHA, PLATFORM)
    job = jp.job
    assert (job["ID"], job["Namespace"], job["Type"]) == ("certwatch-worker", "default", "service")
    assert job["Constraints"] == [
        {"LTarget": "${node.class}", "RTarget": "processing", "Operand": "="}
    ]
    assert job["Meta"] == {
        "leasehold_env": "prod",
        "leasehold_service": "certwatch",
        "leasehold_component": "worker",
        "leasehold_version": SHA,
    }
    group = job["TaskGroups"][0]
    assert group["Count"] == 2
    assert group["Networks"] == [{"DynamicPorts": [{"Label": "http"}]}]
    svc = group["Services"][0]
    assert svc["Provider"] == "nomad"
    assert svc["Tags"] == ["leasehold", "env=prod", "service=certwatch", "component=worker"]
    assert svc["Checks"][0]["Path"] == "/health"
    task = _task(job)
    assert task["Config"] == {
        "image": "ghcr.io/example/certwatch-worker:0123abc",
        "ports": ["http"],
    }
    assert task["Resources"] == {"CPU": 500, "MemoryMB": 256, "MemoryMaxMB": 512}
    assert task["KillTimeout"] == 30 * 10**9
    assert task["Identities"][0]["Name"] == "leasehold"
    assert task["Identities"][0]["Audience"] == ["leasehold"]
    assert task["Identities"][0]["File"] is True
    env = task["Env"]
    assert env["LEASEHOLD_ENV"] == "prod"
    assert env["LEASEHOLD_KAFKA_BOOTSTRAP"] == "kafka:9092"
    assert env["LEASEHOLD_DRAIN_TIMEOUT_S"] == "25"
    assert "LEASEHOLD_TIMEOUT_S" not in env
    tmpl = task["Templates"][0]
    assert tmpl["Envvars"] is True
    assert 'nomadVar "nomad/jobs/certwatch-worker"' in tmpl["EmbeddedTmpl"]


def test_scheduled_job(spec):
    job = render_job(spec, "planner", "dev", SHA, PLATFORM).job
    assert job["Namespace"] == "dev"
    assert job["Type"] == "batch"
    assert job["Periodic"]["Specs"] == ["*/5 * * * *"]
    assert job["Periodic"]["ProhibitOverlap"] is True
    group = job["TaskGroups"][0]
    assert "Services" not in group and "Networks" not in group
    assert group["RestartPolicy"]["Attempts"] == 0
    task = _task(job)
    assert "ports" not in task["Config"]
    assert task["Env"]["LEASEHOLD_TIMEOUT_S"] == "600"
    assert "LEASEHOLD_KAFKA_BOOTSTRAP" not in task["Env"]
    assert task["Templates"] == []  # no Kafka, no declared secrets


def test_kafka_bootstrap_required(spec):
    with pytest.raises(ValueError, match="uses Kafka"):
        render_job(spec, "worker", "prod", SHA, Platform("r", "http://api"))


def test_spec_env_cannot_override_platform_env(spec):
    worker = spec.components["worker"].model_copy(update={"env": {"LEASEHOLD_ENV": "x"}})
    bad = spec.model_copy(update={"components": {**spec.components, "worker": worker}})
    with pytest.raises(ValueError, match="may not set LEASEHOLD_ENV"):
        render_job(bad, "worker", "prod", SHA, PLATFORM)


def test_running_count_is_kept_within_range(spec):
    jp = render_job(spec, "worker", "prod", SHA, PLATFORM)
    assert with_count(jp, 7).job["TaskGroups"][0]["Count"] == 7
    assert with_count(jp, 50).job["TaskGroups"][0]["Count"] == 10
    assert with_count(jp, 0).job["TaskGroups"][0]["Count"] == 2
    assert jp.job["TaskGroups"][0]["Count"] == 2  # original untouched


def test_diff_lines():
    d = {
        "Type": "Edited",
        "Fields": [{"Type": "Edited", "Name": "Priority", "Old": "50", "New": "60"}],
        "Objects": [
            {
                "Type": "Edited",
                "Name": "Meta",
                "Fields": [{"Type": "None", "Name": "a", "Old": "1", "New": "1"}],
            }
        ],
        "TaskGroups": [
            {
                "Type": "Edited",
                "Name": "worker",
                "Fields": [{"Type": "Edited", "Name": "Count", "Old": "2", "New": "3"}],
                "Tasks": [
                    {
                        "Type": "Edited",
                        "Name": "worker",
                        "Objects": [
                            {
                                "Type": "Added",
                                "Name": "Config",
                                "Fields": [{"Type": "Added", "Name": "x", "Old": "", "New": "1"}],
                            }
                        ],
                    }
                ],
            }
        ],
    }
    assert diff_lines(d) == [
        "Priority: 50 → 60",
        "group[worker].Count: 2 → 3",
        "task[worker].Config.x: ∅ → 1",
    ]


class FakeNomad(Nomad):
    def __init__(self, jobs=None, namespaces=("default", "dev")):
        self.jobs_by_id = {j["ID"]: j for j in (jobs or [])}
        self.namespaces = set(namespaces)
        self.registered = []
        self.stopped = []

    def namespace_exists(self, namespace):
        return namespace in self.namespaces

    def job(self, job_id, namespace):
        return self.jobs_by_id.get(job_id)

    def jobs(self, namespace):
        return list(self.jobs_by_id.values())

    def plan(self, job):
        current = self.jobs_by_id.get(job["ID"])
        if current is None:
            return {"Diff": {"Type": "Added"}, "JobModifyIndex": 0}
        lines = []
        old, new = _task(current)["Config"]["image"], _task(job)["Config"]["image"]
        if old != new:
            lines.append({"Type": "Edited", "Name": "image", "Old": old, "New": new})
        count_old, count_new = current["TaskGroups"][0]["Count"], job["TaskGroups"][0]["Count"]
        if count_old != count_new:
            lines.append(
                {"Type": "Edited", "Name": "Count", "Old": str(count_old), "New": str(count_new)}
            )
        return {"Diff": {"Type": "Edited" if lines else "None", "Fields": lines},
                "JobModifyIndex": 42}  # fmt: skip

    def register(self, job, modify_index):
        self.registered.append((job["ID"], modify_index, job["TaskGroups"][0]["Count"]))
        return {"EvalID": "e"}

    def stop(self, job_id, namespace):
        self.stopped.append(job_id)
        return {"EvalID": "s"}


def _running(spec, cname, sha=SHA, count=None):
    job = copy.deepcopy(render_job(spec, cname, "prod", sha, PLATFORM).job)
    if count is not None:
        job["TaskGroups"][0]["Count"] = count
    return job


def test_diff_creates_missing_jobs(spec):
    p = plan([spec], "prod", SHA, PLATFORM)
    d = diff(FakeNomad(), p, ["certwatch"])
    assert [(c.kind, c.job.job_id) for c in d.changes] == [
        ("create", "certwatch-planner"),
        ("create", "certwatch-worker"),
    ]


def test_diff_unchanged_and_scaled_jobs_have_no_changes(spec):
    nomad = FakeNomad([_running(spec, "planner"), _running(spec, "worker", count=7)])
    d = diff(nomad, plan([spec], "prod", SHA, PLATFORM), ["certwatch"])
    assert d.empty


def test_diff_update_keeps_count(spec):
    nomad = FakeNomad([_running(spec, "planner"), _running(spec, "worker", "fff0000", count=7)])
    from leasehold.nomad.apply import apply

    d = diff(nomad, plan([spec], "prod", SHA, PLATFORM), ["certwatch"])
    assert [c.job.job_id for c in d.changes] == ["certwatch-worker"]
    assert d.changes[0].diff_lines[0].startswith("image: ghcr.io/example/certwatch-worker:fff0000")
    apply(nomad, d)
    assert nomad.registered == [("certwatch-worker", 42, 7)]


def test_diff_skips_jobs_leasehold_does_not_manage(spec):
    foreign = {"ID": "certwatch-worker", "Meta": None, "TaskGroups": [{"Count": 3}]}
    d = diff(FakeNomad([foreign]), plan([spec], "prod", SHA, PLATFORM), ["certwatch"])
    assert [c.job.job_id for c in d.changes] == ["certwatch-planner"]
    assert any("isn't managed by leasehold" in w for w in d.warnings)


def test_diff_reports_and_prunes_stale_jobs(spec):
    from leasehold.nomad.apply import apply

    old = {
        "ID": "certwatch-old",
        "Status": "running",
        "Meta": {"leasehold_service": "certwatch", "leasehold_env": "prod"},
    }
    other = {"ID": "other-x", "Status": "running", "Meta": {"leasehold_service": "other"}}
    run = {  # one run of the periodic planner: its parent's Meta, not stale
        "ID": "certwatch-planner/periodic-1794486600",
        "ParentID": "certwatch-planner",
        "Status": "running",
        "Meta": {"leasehold_service": "certwatch", "leasehold_env": "prod"},
    }
    nomad = FakeNomad([_running(spec, "planner"), _running(spec, "worker"), old, other, run])
    d = diff(nomad, plan([spec], "prod", SHA, PLATFORM), ["certwatch"])
    assert d.stale == [("default", "certwatch-old")]
    apply(nomad, d)
    assert nomad.stopped == []
    apply(nomad, d, prune=True)
    assert nomad.stopped == ["certwatch-old"]


def test_diff_needs_the_namespace(spec):
    with pytest.raises(NomadError, match="namespace 'dev'"):
        diff(FakeNomad(namespaces=["default"]), plan([spec], "dev", SHA, PLATFORM), ["certwatch"])


def test_cli_plan(tmp_path, capsys):
    rc = main(
        ["nomad", "plan", str(FIXTURES / "certwatch"), "--sha", SHA, "--api-url", "http://a",
         "--kafka-bootstrap", "k:9092", "--out", str(tmp_path)]
    )  # fmt: skip
    assert rc == 0
    out = capsys.readouterr().out
    assert "certwatch-worker  service  count 2-10" in out
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "certwatch-planner.json",
        "certwatch-worker.json",
    ]


def test_cli_plan_rejects_a_bad_sha(capsys):
    rc = main(["nomad", "plan", str(FIXTURES / "certwatch"), "--sha", "latest", "--api-url", "a"])
    assert rc == 1
    assert "git SHA" in capsys.readouterr().err


def test_max_per_node_and_node_pool(spec):
    job = render_job(spec, "worker", "prod", SHA, PLATFORM).job
    assert "Constraints" not in job["TaskGroups"][0] and "NodePool" not in job
    spec.components["worker"] = spec.components["worker"].model_copy(update={"max_per_node": 1})
    pool = Platform(**{**PLATFORM.__dict__, "node_pool": "dev"})
    job = render_job(spec, "worker", "dev", SHA, pool).job
    assert job["TaskGroups"][0]["Constraints"] == [
        {"LTarget": "${node.unique.id}", "RTarget": "1", "Operand": "distinct_property"}
    ]
    assert job["NodePool"] == "dev"


def test_max_per_node_must_be_positive(spec):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        type(spec.components["worker"]).model_validate(
            spec.components["worker"].model_dump(by_alias=True) | {"max_per_node": 0}
        )


def test_placement_failures_are_summarised():
    from leasehold.nomad.apply import _placement_failures

    full = {
        f"distinct_property: ${{node.unique.id}}=node-{i} used by 1 allocs": 1 for i in range(41)
    }
    result = {
        "FailedTGAllocs": {
            "worker": {
                "NodesEvaluated": 44,
                "CoalescedFailures": 3,
                "ConstraintFiltered": full | {"computed class ineligible": 3},
            }
        }
    }
    assert _placement_failures(result) == [
        "group worker: 4 can't be placed (44 nodes evaluated): max_per_node reached on 41 "
        "machines; computed class ineligible (3)"
    ]
