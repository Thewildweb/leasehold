"""Nomad jobs from ``service.yml``: one job per component, as Nomad API JSON.

What each job gets:

- ID ``<service>-<component>`` in the environment's namespace (``default`` for prod),
  ``Meta`` with ``leasehold_env``, ``leasehold_service``, ``leasehold_component`` and
  ``leasehold_version`` (how ``apply`` finds the jobs it manages).
- A constraint on ``node_class``, the spec's priority and resources (``memory_max``
  becomes ``MemoryMaxMB``), and ``kill_timeout``. ``max_per_node`` becomes a
  ``distinct_property`` constraint on ``${node.unique.id}``: at most that many
  instances per machine, so instances that limit themselves per egress IP don't
  share one. The environment's node pool (``Platform.node_pool``), when set, keeps
  its jobs on that pool's machines (e.g. dev on dedicated nodes with clean IPs).
  ``Platform.node_classes`` maps a spec's node class to the class used in this
  environment, for a pool that has no machine of that class (dev's pool has only
  ``processing`` machines, so ``services`` maps to ``processing`` there).
  ``Platform.host_network`` binds the http port to a Nomad host network (e.g. the
  nodes' ``tailscale`` network), so it isn't published on their public address.
- The image ``<registry>/<service>-<component>:<git sha>``, never ``latest``.
- Env: ``LEASEHOLD_ENV``, ``_SERVICE``, ``_COMPONENT``, ``_VERSION``, ``_API_URL``,
  ``_KAFKA_BOOTSTRAP`` (components that use Kafka), ``_DRAIN_TIMEOUT_S`` (5 s under
  ``kill_timeout``), ``_TIMEOUT_S`` (scheduled), plus the spec's ``env``.
- A workload identity ``leasehold`` (audience ``leasehold``) written to
  ``$NOMAD_SECRETS_DIR/nomad_leasehold.jwt``, which the control API client reads.
- Secrets: components that use Kafka render the Nomad Variable ``nomad/jobs/<job>``
  (written by ``apply``'s credentials step) into env vars; every path in the spec's
  ``secrets`` is rendered the same way. A missing variable keeps the task from starting.
- Long-running components (task-worker, processor, api): a dynamic port ``http``, a
  Nomad service ``<job>`` with an HTTP check on ``/health`` that restarts a stuck
  process, and tags ``leasehold``, ``env=..``, ``service=..``, ``component=..``. The
  Prometheus ``nomad_sd`` scrape keeps targets tagged ``leasehold`` and turns the other
  tags into the ``env``, ``service`` and ``component`` labels the dashboards select on.
- Scheduled components: a periodic batch job (cron in UTC, no overlapping runs), no
  port, no restarts inside a run; the next firing is the retry.

Counts: the spec gives ``min``/``max``. ``apply`` keeps the running count (clamped to
that range) so a redeploy doesn't undo scaling; a new job starts at ``min``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any

from leasehold import naming
from leasehold.spec.models import Api, Component, Processor, Scheduled, ServiceSpec, TaskWorker

IDENTITY_NAME = "leasehold"
IDENTITY_AUDIENCE = "leasehold"
HTTP_PORT = "http"
MANAGED_META = "leasehold_service"
DRAIN_MARGIN = timedelta(seconds=5)

_S = 1_000_000_000  # Nomad API durations are nanoseconds


def _ns(d: timedelta) -> int:
    return int(d.total_seconds() * _S)


@dataclass(frozen=True)
class Platform:
    """Addresses that are the same for every service in an environment."""

    registry: str  # e.g. ghcr.io/example
    api_url: str  # control API, e.g. http://control-api.service:8400
    kafka_bootstrap: str | None = None  # e.g. kafka.service:9092
    node_pool: str | None = None  # Nomad node pool for the environment's jobs (default pool)
    # Nomad host network for the http port (/health, /metrics), e.g. "tailscale": the
    # port is published only on that network's address, which is also the address the
    # service registers (and Prometheus scrapes). Unset: the node's default network.
    host_network: str | None = None
    # Spec node class → node class in this environment; classes not listed are kept.
    node_classes: dict[str, str] = field(default_factory=dict, hash=False)
    # Topic reference → payload model, for every topic; ``plan`` fills it in.
    payload_models: dict[str, str] = field(default_factory=dict, hash=False)


@dataclass(frozen=True)
class JobPlan:
    service: str
    component: str
    job_id: str
    namespace: str
    count_min: int | None  # None for scheduled
    count_max: int | None
    job: dict[str, Any]

    @property
    def periodic(self) -> bool:
        return self.count_min is None


@dataclass(frozen=True)
class NomadPlan:
    env: str
    namespace: str
    jobs: list[JobPlan]


def uses_kafka(c: Component) -> bool:
    return bool(c.produces) or isinstance(c, Processor)


def job_variable_path(job_id: str) -> str:
    """The Nomad Variable a job reads without extra ACLs (Nomad's implicit policy)."""
    return f"nomad/jobs/{job_id}"


def _secrets_template(paths: list[str]) -> str:
    """Every key of each Nomad Variable as KEY="value" lines (rendered as env vars)."""
    parts = []
    for p in paths:
        parts.append(
            f'{{{{ with nomadVar "{p}" }}}}{{{{ range $k, $v := . }}}}'
            "{{ $k }}={{ $v | toJSON }}\n{{ end }}{{ end }}"
        )
    return "".join(parts)


def _env(
    spec: ServiceSpec, cname: str, c: Component, env: str, version: str, platform: Platform
) -> dict[str, str]:
    out = {
        "LEASEHOLD_ENV": env,
        "LEASEHOLD_SERVICE": spec.name,
        "LEASEHOLD_COMPONENT": cname,
        "LEASEHOLD_VERSION": version,
        "LEASEHOLD_API_URL": platform.api_url,
        "LEASEHOLD_DRAIN_TIMEOUT_S": str(
            max(int((c.kill_timeout - DRAIN_MARGIN).total_seconds()), 1)
        ),
    }
    if uses_kafka(c):
        if not platform.kafka_bootstrap:
            raise ValueError(f"{spec.name}.{cname} uses Kafka; set the Kafka bootstrap address")
        out["LEASEHOLD_KAFKA_BOOTSTRAP"] = platform.kafka_bootstrap
    if isinstance(c, Processor):
        missing = sorted(r for r in c.consumes if r not in platform.payload_models)
        if missing:
            raise ValueError(f"{spec.name}.{cname} consumes {', '.join(missing)}: no producer")
        models = {r: platform.payload_models[r] for r in sorted(c.consumes)}
        out["LEASEHOLD_PAYLOAD_MODELS"] = json.dumps(models, separators=(",", ":"))
    if isinstance(c, Scheduled):
        out["LEASEHOLD_TIMEOUT_S"] = str(int(c.timeout.total_seconds()))
    # The spec's own env can't override the platform variables.
    clash = sorted(k for k in c.env if k in out)
    if clash:
        raise ValueError(f"{spec.name}.{cname}: env may not set {', '.join(clash)}")
    return out | dict(c.env)


def _task(
    spec: ServiceSpec, cname: str, c: Component, env: str, version: str, platform: Platform
) -> dict[str, Any]:
    job_id = naming.nomad_job(spec.name, cname)
    config: dict[str, Any] = {
        "image": naming.image_ref(platform.registry, spec.name, cname, version),
    }
    if c.command:
        config["command"] = c.command[0]
        if c.command[1:]:
            config["args"] = list(c.command[1:])
    if not isinstance(c, Scheduled):
        config["ports"] = [HTTP_PORT]

    secret_paths = ([job_variable_path(job_id)] if uses_kafka(c) else []) + list(spec.secrets)
    templates = []
    if secret_paths:
        templates.append(
            {
                "EmbeddedTmpl": _secrets_template(secret_paths),
                "DestPath": "secrets/leasehold.env",
                "Envvars": True,
                "ChangeMode": "restart",
            }
        )

    resources: dict[str, Any] = {"CPU": c.resources.cpu, "MemoryMB": c.resources.memory}
    if c.resources.memory_max:
        resources["MemoryMaxMB"] = c.resources.memory_max

    return {
        "Name": cname,
        "Driver": "docker",
        "Config": config,
        "Env": _env(spec, cname, c, env, version, platform),
        "Templates": templates,
        "Resources": resources,
        "KillTimeout": _ns(c.kill_timeout),
        "Identities": [
            {
                "Name": IDENTITY_NAME,
                "Audience": [IDENTITY_AUDIENCE],
                "File": True,
                "TTL": _ns(timedelta(hours=1)),
            }
        ],
    }


def _service(spec: ServiceSpec, cname: str, env: str) -> dict[str, Any]:
    return {
        "Name": naming.nomad_job(spec.name, cname),
        "Provider": "nomad",
        "PortLabel": HTTP_PORT,
        "Tags": ["leasehold", f"env={env}", f"service={spec.name}", f"component={cname}"],
        "Checks": [
            {
                "Name": "health",
                "Type": "http",
                "Path": "/health",
                "PortLabel": HTTP_PORT,
                "Interval": _ns(timedelta(seconds=10)),
                "Timeout": _ns(timedelta(seconds=3)),
                "CheckRestart": {"Limit": 3, "Grace": _ns(timedelta(seconds=60))},
            }
        ],
    }


def render_job(
    spec: ServiceSpec, cname: str, env: str, version: str, platform: Platform
) -> JobPlan:
    c = spec.components[cname]
    job_id = naming.nomad_job(spec.name, cname)
    namespace = naming.nomad_namespace(env)
    task = _task(spec, cname, c, env, version, platform)
    group: dict[str, Any] = {"Name": cname, "Tasks": [task]}
    if c.max_per_node is not None:
        group["Constraints"] = [
            {
                "LTarget": "${node.unique.id}",
                "RTarget": str(c.max_per_node),
                "Operand": "distinct_property",
            }
        ]
    node_class = platform.node_classes.get(c.node_class, c.node_class)
    job: dict[str, Any] = {
        "ID": job_id,
        "Name": job_id,
        "Namespace": namespace,
        "Datacenters": ["*"],
        "Priority": c.priority,
        "Constraints": [{"LTarget": "${node.class}", "RTarget": node_class, "Operand": "="}],
        "Meta": {
            "leasehold_env": env,
            MANAGED_META: spec.name,
            "leasehold_component": cname,
            "leasehold_version": version,
        },
        "TaskGroups": [group],
    }
    if platform.node_pool:
        job["NodePool"] = platform.node_pool

    if isinstance(c, Scheduled):
        job["Type"] = "batch"
        job["Periodic"] = {
            "Enabled": True,
            "SpecType": "cron",
            "Specs": [c.schedule],
            "ProhibitOverlap": True,
            "TimeZone": "UTC",
        }
        group["Count"] = 1
        group["RestartPolicy"] = {"Attempts": 0, "Mode": "fail"}
        group["ReschedulePolicy"] = {"Attempts": 0, "Unlimited": False}
        return JobPlan(spec.name, cname, job_id, namespace, None, None, job)

    assert isinstance(c, TaskWorker | Processor | Api)
    job["Type"] = "service"
    group["Count"] = c.count.min
    port: dict[str, Any] = {"Label": HTTP_PORT}
    if platform.host_network:
        port["HostNetwork"] = platform.host_network
    group["Networks"] = [{"DynamicPorts": [port]}]
    group["Services"] = [_service(spec, cname, env)]
    group["RestartPolicy"] = {
        "Attempts": 10,
        "Interval": _ns(timedelta(minutes=5)),
        "Delay": _ns(timedelta(seconds=15)),
        "Mode": "delay",
    }
    group["Update"] = {
        "MaxParallel": 1,
        "HealthCheck": "checks",
        "MinHealthyTime": _ns(timedelta(seconds=10)),
        "HealthyDeadline": _ns(timedelta(minutes=3)),
        "ProgressDeadline": _ns(timedelta(minutes=10)),
        "AutoRevert": True,
    }
    return JobPlan(spec.name, cname, job_id, namespace, c.count.min, c.count.max, job)


def plan(specs: list[ServiceSpec], env: str, version: str, platform: Platform) -> NomadPlan:
    """Every component's job. ``version`` is the git SHA the images are tagged with."""
    naming.check_env(env)
    platform = replace(
        platform,
        payload_models={
            str(s.topic_ref(name)): t.payload_model
            for s in specs
            for name, t in s.kafka.produces.items()
        }
        | platform.payload_models,
    )
    jobs = [
        render_job(spec, cname, env, version, platform)
        for spec in sorted(specs, key=lambda s: s.name)
        for cname in spec.components
    ]
    return NomadPlan(env=env, namespace=naming.nomad_namespace(env), jobs=jobs)


def with_count(jp: JobPlan, running: int | None) -> JobPlan:
    """The job with the running count kept, clamped to the spec's range."""
    if jp.periodic or running is None or jp.count_min is None or jp.count_max is None:
        return jp
    count = min(max(running, jp.count_min), jp.count_max)
    group = dict(jp.job["TaskGroups"][0], Count=count)
    job = dict(jp.job, TaskGroups=[group])
    return JobPlan(jp.service, jp.component, jp.job_id, jp.namespace, jp.count_min,
                   jp.count_max, job)  # fmt: skip
