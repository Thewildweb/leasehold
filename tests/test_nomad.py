"""Nomad jobs rendered from service.yml, and the plan/apply diff against a fake Nomad."""

from __future__ import annotations

import copy
import json
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
    assert env["LEASEHOLD_EGRESS_IP"] == "${meta.egress_ip}"  # task worker
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


def test_host_network(spec):
    job = render_job(spec, "worker", "prod", SHA, PLATFORM).job
    assert job["TaskGroups"][0]["Networks"] == [{"DynamicPorts": [{"Label": "http"}]}]
    ts = Platform(**{**PLATFORM.__dict__, "host_network": "tailscale"})
    job = render_job(spec, "worker", "prod", SHA, ts).job
    assert job["TaskGroups"][0]["Networks"] == [
        {"DynamicPorts": [{"Label": "http", "HostNetwork": "tailscale"}]}
    ]


def test_node_classes(spec):
    job = render_job(spec, "worker", "dev", SHA, PLATFORM).job
    original = job["Constraints"][0]["RTarget"]
    mapped = Platform(**{**PLATFORM.__dict__, "node_classes": {original: "other"}})
    job = render_job(spec, "worker", "dev", SHA, mapped).job
    assert job["Constraints"] == [{"LTarget": "${node.class}", "RTarget": "other", "Operand": "="}]
    unrelated = Platform(**{**PLATFORM.__dict__, "node_classes": {"nope": "other"}})
    job = render_job(spec, "worker", "dev", SHA, unrelated).job
    assert job["Constraints"][0]["RTarget"] == original


def test_planner_job_env():
    import dataclasses

    from test_kafka_plan import _with_planner

    from leasehold.nomad import plan as nomad_plan

    s = _with_planner()
    with pytest.raises(ValueError, match="ClickHouse URL"):
        nomad_plan([s], "dev", SHA, PLATFORM)
    p = nomad_plan(
        [s],
        "dev",
        SHA,
        dataclasses.replace(PLATFORM, clickhouse_url="http://ch:8123", databases={"x": "y"}),
    )
    (job,) = [j for j in p.jobs if j.component == "plan-recheck"]
    env = job.job["TaskGroups"][0]["Tasks"][0]["Env"]
    assert env["LEASEHOLD_CLICKHOUSE_URL"] == "http://ch:8123"
    assert env["LEASEHOLD_PLANNER_DATABASES"] == "{}"  # reads only its own tables
    assert "LEASEHOLD_EGRESS_IP" not in env  # only task workers send requests out
    # The job reads its Nomad Variable (Kafka for the plan log, ClickHouse password).
    assert job.job["TaskGroups"][0]["Tasks"][0]["Templates"]


def test_scheduled_job_with_only_clickhouse_reads_gets_its_variable():
    # A scheduled job with `reads` and no planner uses no Kafka or storage, but its
    # ClickHouse password is in its own variable: without the template it logs in
    # without one.
    import dataclasses

    import yaml
    from test_kafka_plan import FIXTURES

    from leasehold.nomad import plan as nomad_plan
    from leasehold.spec import ServiceSpec

    data = yaml.safe_load((FIXTURES / "certwatch" / "service.yml").read_text())
    data["components"]["report"] = {
        "kind": "scheduled",
        "schedule": "@hourly",
        "node_class": "services",
        "resources": {"cpu": 100, "memory": 64},
        "reads": ["certwatch.domains"],
    }
    s = ServiceSpec.model_validate(data)
    p = nomad_plan(
        [s],
        "dev",
        SHA,
        dataclasses.replace(PLATFORM, clickhouse_url="http://ch:8123", databases={"x": "y"}),
    )
    (job,) = [j for j in p.jobs if j.component == "report"]
    (template,) = job.job["TaskGroups"][0]["Tasks"][0]["Templates"]
    assert f"nomad/jobs/{job.job['ID']}" in template["EmbeddedTmpl"]


def test_agent_socket_for_target_leasing_workers(spec):
    from dataclasses import replace

    from leasehold.spec import ServiceSpec

    data = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    data["frontier"]["task_types"]["certwatch.chunk"]["target_leases"] = True
    targeted = ServiceSpec.model_validate(data)
    with_agent = replace(PLATFORM, agent_volume="leasehold-agent")

    job = render_job(targeted, "worker", "dev", SHA, with_agent).job
    group = job["TaskGroups"][0]
    assert group["Volumes"] == {
        "leasehold-agent": {"Name": "leasehold-agent", "Type": "host", "Source": "leasehold-agent"}
    }
    task = _task(job)
    assert task["VolumeMounts"] == [
        {"Volume": "leasehold-agent", "Destination": "/run/leasehold-agent", "ReadOnly": False}
    ]
    assert task["Env"]["LEASEHOLD_AGENT_SOCKET"] == "/run/leasehold-agent/dev.sock"

    # Without target leases, or without the volume configured: no mount, no socket.
    for s, p in ((spec, with_agent), (targeted, PLATFORM)):
        job = render_job(s, "worker", "dev", SHA, p).job
        assert "Volumes" not in job["TaskGroups"][0]
        assert "VolumeMounts" not in _task(job)
        assert "LEASEHOLD_AGENT_SOCKET" not in _task(job)["Env"]


# --- sandbox ---------------------------------------------------------------------------------


def _sandboxed(spec, cname="worker"):
    from test_spec import sandbox_block

    from leasehold.spec import ServiceSpec

    data = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    data["components"][cname]["sandbox"] = sandbox_block()
    return ServiceSpec.model_validate(data)


HARDENED = {"readonly_rootfs": True, "cap_drop": ["ALL"]}


def test_sandbox_group_and_main_task(spec):
    job = render_job(_sandboxed(spec), "worker", "prod", SHA, PLATFORM).job
    group = job["TaskGroups"][0]
    assert group["Networks"] == [{"Mode": "bridge", "DynamicPorts": [{"Label": "http"}]}]
    assert group["Services"][0]["PortLabel"] == "http"
    assert [t["Name"] for t in group["Tasks"]] == ["worker", "netfilter", "gateway", "browser"]

    main = group["Tasks"][0]
    assert main["User"] == "10000:10000"
    assert main["Leader"] is True
    assert main["Config"] == {
        "image": "ghcr.io/example/certwatch-worker:0123abc",  # no ports: the bridge maps them
        **HARDENED,
        "security_opt": ["no-new-privileges", "seccomp=/etc/leasehold/seccomp/chromium.json"],
        "mount": [
            {
                "type": "tmpfs",
                "target": "/tmp",
                "readonly": False,
                "tmpfs_options": {"size": 256 * 1024**2},
            }
        ],
    }
    assert main["Resources"] == {"Cores": 2, "MemoryMB": 256, "MemoryMaxMB": 512}
    # The component's own task keeps what it gets today.
    assert main["Identities"][0]["Name"] == "leasehold"
    assert 'nomadVar "nomad/jobs/certwatch-worker"' in main["Templates"][0]["EmbeddedTmpl"]
    assert main["Env"]["LEASEHOLD_KAFKA_BOOTSTRAP"] == "kafka:9092"
    assert "Lifecycle" not in main


def test_sandbox_netfilter_prestart_is_the_only_task_with_a_capability(spec):
    tasks = render_job(_sandboxed(spec), "worker", "prod", SHA, PLATFORM).job["TaskGroups"][0][
        "Tasks"
    ]
    nf = next(t for t in tasks if t["Name"] == "netfilter")
    assert nf["Lifecycle"] == {"Hook": "prestart", "Sidecar": False}
    assert nf["User"] == "0:0"
    assert nf["Config"] == {
        "image": "ghcr.io/example/certwatch-worker:0123abc",  # the component's image
        "command": "/app/netfilter.sh",
        "args": ["10001", "20000"],
        **HARDENED,
        "cap_add": ["NET_ADMIN"],
        "security_opt": ["no-new-privileges"],
    }
    assert nf["Resources"] == {"CPU": 50, "MemoryMB": 64}
    # What its rules are about: the users and the component's addresses; no secrets.
    assert nf["Env"] == {
        "LEASEHOLD_API_URL": PLATFORM.api_url,
        "LEASEHOLD_KAFKA_BOOTSTRAP": "kafka:9092",
        "LEASEHOLD_SANDBOX_USERS": '{"worker":10000,"gateway":10001,"browser":20000}',
    }
    for t in tasks:
        assert t["Config"]["readonly_rootfs"] is True
        assert t["Config"]["cap_drop"] == ["ALL"]
        assert "no-new-privileges" in t["Config"]["security_opt"]
        assert ("cap_add" in t["Config"]) == (t["Name"] == "netfilter")
        assert "Lifecycle" not in t or t["Name"] == "netfilter"


def test_sandbox_extra_tasks_get_no_secrets(spec):
    tasks = render_job(_sandboxed(spec), "worker", "prod", SHA, PLATFORM).job["TaskGroups"][0][
        "Tasks"
    ]
    by_name = {t["Name"]: t for t in tasks}
    for name in ("netfilter", "gateway", "browser"):
        t = by_name[name]
        for absent in ("Templates", "Identities", "VolumeMounts", "Leader"):
            assert absent not in t, (name, absent)
        assert "ports" not in t["Config"]
    for name in ("gateway", "browser"):
        assert not any(k.startswith("LEASEHOLD_") for k in by_name[name]["Env"])
    secretish = ("PASSWORD", "TOKEN", "SECRET", "KEY", "DSN")
    assert not any(w in k for k in by_name["netfilter"]["Env"] for w in secretish)

    gw = by_name["gateway"]
    assert gw["User"] == "10001:10001"
    assert gw["Env"] == {"GATEWAY_LOG_LEVEL": "info"}
    assert gw["Config"]["command"] == "/app/gateway"
    assert gw["Config"]["args"] == ["--listen", "127.0.0.1:3128"]
    assert gw["Config"]["security_opt"] == ["no-new-privileges"]  # seccomp is per task
    assert gw["Resources"] == {"CPU": 500, "MemoryMB": 256}

    br = by_name["browser"]
    assert br["User"] == "20000:20000"
    assert br["Config"]["image"] == "ghcr.io/example/chromium:131.0"
    assert br["Config"]["shm_size"] == 2 * 1024**3
    assert br["Config"]["runtime"] == "runsc"
    assert br["Config"]["mount"] == [
        {"type": "tmpfs", "target": "/tmp", "readonly": False, "tmpfs_options": {"size": 1024**3}},
        {
            "type": "tmpfs",
            "target": "/home/pwuser",
            "readonly": False,
            "tmpfs_options": {"size": 512 * 1024**2},
        },
    ]
    assert br["Resources"] == {"CPU": 2000, "MemoryMB": 2048}


def test_sandbox_keeps_the_host_network_and_agent_socket(spec):
    from dataclasses import replace

    from leasehold.spec import ServiceSpec

    data = _sandboxed(spec).model_dump(mode="json", exclude_none=True, by_alias=True)
    data["frontier"]["task_types"]["certwatch.chunk"]["target_leases"] = True
    targeted = ServiceSpec.model_validate(data)
    platform = replace(PLATFORM, agent_volume="leasehold-agent", host_network="tailscale")
    group = render_job(targeted, "worker", "dev", SHA, platform).job["TaskGroups"][0]
    assert group["Networks"] == [
        {"Mode": "bridge", "DynamicPorts": [{"Label": "http", "HostNetwork": "tailscale"}]}
    ]
    assert "VolumeMounts" in group["Tasks"][0]
    assert all("VolumeMounts" not in t for t in group["Tasks"][1:])


def test_sandbox_on_a_scheduled_component(spec):
    group = render_job(_sandboxed(spec, "planner"), "planner", "prod", SHA, PLATFORM).job[
        "TaskGroups"
    ][0]
    assert group["Networks"] == [{"Mode": "bridge"}]
    assert group["Tasks"][0]["Leader"] is True
    assert "ports" not in group["Tasks"][0]["Config"]


def test_sandbox_minimal(spec):
    from leasehold.spec import ServiceSpec

    data = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    data["components"]["worker"]["sandbox"] = {"user": 10000, "group": 10500}
    job = render_job(ServiceSpec.model_validate(data), "worker", "prod", SHA, PLATFORM).job
    (task,) = job["TaskGroups"][0]["Tasks"]
    assert task["User"] == "10000:10500"
    assert "Leader" not in task
    assert task["Config"] == {
        "image": "ghcr.io/example/certwatch-worker:0123abc",
        **HARDENED,
        "security_opt": ["no-new-privileges"],
    }
    assert task["Resources"] == {"CPU": 500, "MemoryMB": 256, "MemoryMaxMB": 512}


def test_no_sandbox_renders_as_before(spec):
    job = render_job(spec, "worker", "prod", SHA, PLATFORM).job
    group = job["TaskGroups"][0]
    assert group["Networks"] == [{"DynamicPorts": [{"Label": "http"}]}]
    (task,) = group["Tasks"]
    assert not {"User", "Leader", "Lifecycle"} & set(task)
    assert task["Config"] == {
        "image": "ghcr.io/example/certwatch-worker:0123abc",
        "ports": ["http"],
    }


def test_sandbox_netfilter_users_come_from_the_spec_not_its_env(spec):
    from leasehold.spec import ServiceSpec

    s = _sandboxed(spec)
    data = s.model_dump(mode="json", exclude_none=True, by_alias=True)
    data["components"]["worker"]["sandbox"]["netfilter"]["env"] = {
        "LEASEHOLD_SANDBOX_USERS": '{"worker":0}',
        "EXIT_PORT": "3130",
    }
    tasks = render_job(ServiceSpec.model_validate(data), "worker", "prod", SHA, PLATFORM).job[
        "TaskGroups"
    ][0]["Tasks"]
    nf = next(t for t in tasks if t["Name"] == "netfilter")
    assert nf["Env"]["EXIT_PORT"] == "3130"
    users = json.loads(nf["Env"]["LEASEHOLD_SANDBOX_USERS"])
    assert users == {"worker": 10000, "gateway": 10001, "browser": 20000}
