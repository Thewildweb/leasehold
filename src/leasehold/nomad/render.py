"""Nomad jobs from ``service.yml``: one job per component, as Nomad API JSON.

What each job gets:

- ID ``<service>-<component>`` in the environment's namespace (``default`` for prod),
  ``Meta`` with ``leasehold_env``, ``leasehold_service``, ``leasehold_component`` and
  ``leasehold_version`` (how ``apply`` finds the jobs it manages).
- A constraint on ``node_class``, the spec's priority and resources (``memory_max``
  becomes ``MemoryMaxMB``, ``disk`` the group's ``EphemeralDisk``), and
  ``kill_timeout``. ``max_per_node`` becomes a ``distinct_property`` constraint on
  ``${node.unique.id}``: at most that many instances per machine, so instances that
  limit themselves per egress IP don't share one. The environment's node pool
  (``Platform.node_pool``), when set, keeps its jobs on that pool's machines (e.g. dev
  on dedicated nodes with clean IPs).
  ``Platform.node_classes`` maps a spec's node class to the class used in this
  environment, for a pool that has no machine of that class (dev's pool has only
  ``processing`` machines, so ``services`` maps to ``processing`` there).
  ``Platform.host_network`` binds the http port to a Nomad host network (e.g. the
  nodes' ``tailscale`` network), so it isn't published on their public address.
- The image ``<registry>/<service>-<component>:<git sha>``, never ``latest``.
- Env: ``LEASEHOLD_ENV``, ``_SERVICE``, ``_COMPONENT``, ``_VERSION``, ``_API_URL``,
  ``_KAFKA_BOOTSTRAP`` (components that use Kafka), ``_DRAIN_TIMEOUT_S`` (5 s under
  ``kill_timeout``), ``_TIMEOUT_S`` (scheduled), ``_EGRESS_IP`` (task workers:
  the node's ``meta.egress_ip``), plus the spec's ``env``.
- Task workers of task types with target leases, when the environment names the agent's
  host volume (``Platform.agent_volume``): that volume mounted at ``/run/leasehold-agent``
  and ``LEASEHOLD_AGENT_SOCKET`` set to ``<env>.sock`` in it, so they talk to the
  machine's egress agent (``leasehold.agent``) instead of the control API.
- A workload identity ``leasehold`` (audience ``leasehold``) written to
  ``$NOMAD_SECRETS_DIR/nomad_leasehold.jwt``, which the control API client reads.
- Components that read or write buckets: ``LEASEHOLD_S3_ENDPOINT`` (and
  ``LEASEHOLD_S3_REGION`` when set); their keys are in the job's variable (storage step).
- Archive sinks (``archives`` in the spec): a job ``<service>-archive-<sink>`` like a
  processor's, running ``leasehold archive run`` in ``Platform.archive_image``, with the
  sink's settings in ``LEASEHOLD_ARCHIVE`` (the image has no service.yml). Its
  ephemeral disk holds one open file per partition: (topic partitions) x
  (``file_size_mb`` + ``row_group_mb``) + 256 MB, so one instance can hold them all.
- Snapshot loaders (``snapshots`` in the spec): a periodic job ``<service>-load-<dataset>``
  running ``leasehold snapshot load`` in ``Platform.snapshot_image``, every 20 minutes in
  the environment's load window (cron in the window's time zone), with the dataset's
  settings and schema files in ``LEASEHOLD_SNAPSHOT`` (``Platform.snapshot_settings``),
  ``LEASEHOLD_CLICKHOUSE_URL`` and its ClickHouse password from the job's variable.
- Secrets: components that use Kafka or buckets render the Nomad Variable
  ``nomad/jobs/<job>`` (written by ``apply``'s credentials and storage steps) into env
  vars; every path in the spec's ``secrets`` is rendered the same way. A missing
  variable keeps the task from starting.
- Long-running components (task-worker, processor, api): a dynamic port ``http``, a
  Nomad service ``<job>`` with an HTTP check on ``/health`` that restarts a stuck
  process, and tags ``leasehold``, ``env=..``, ``service=..``, ``component=..``. The
  Prometheus ``nomad_sd`` scrape keeps targets tagged ``leasehold`` and turns the other
  tags into the ``env``, ``service`` and ``component`` labels the dashboards select on.
- Components with a ``sandbox``: the group's network in bridge mode (its own network
  namespace; the http port is mapped there, so the container publishes none). Every
  task runs as its numeric ``user:group``, with ``readonly_rootfs``, ``cap_drop =
  ["ALL"]``, ``no-new-privileges`` (and ``seccomp=<path>`` when set), its tmpfs
  mounts, ``shm_size``, ``runtime`` and ``cores`` (instead of ``cpu``). The extra tasks
  run next to the component's own (the leader) with only their own env: no template,
  Nomad Variable or workload identity. ``netfilter`` is a prestart task (not a
  sidecar), root with ``NET_ADMIN`` as its only capability, that installs the packet
  filter rules in the shared namespace and exits. Besides its own env it gets what its
  rules are about, no secrets: ``LEASEHOLD_SANDBOX_USERS`` (JSON, task name -> user ID,
  the component's own task included) and the addresses the component's own task talks
  to (``LEASEHOLD_API_URL``, and ``LEASEHOLD_KAFKA_BOOTSTRAP`` / ``LEASEHOLD_S3_ENDPOINT``
  when it has them). No host volume is mounted into the
  extra tasks; the component's own task keeps the egress agent's socket when it uses it.
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
from leasehold.spec.models import (
    NETFILTER_TASK,
    Api,
    ArchiveComponent,
    Component,
    Netfilter,
    Processor,
    Resources,
    Sandbox,
    SandboxTask,
    Scheduled,
    ServiceSpec,
    SnapshotLoaderComponent,
    TaskWorker,
)

IDENTITY_NAME = "leasehold"
IDENTITY_AUDIENCE = "leasehold"
HTTP_PORT = "http"
MANAGED_META = "leasehold_service"
DRAIN_MARGIN = timedelta(seconds=5)
AGENT_DIR = "/run/leasehold-agent"
AGENT_VOLUME = "leasehold-agent"


def agent_socket(env: str) -> str:
    """Where the environment's egress agent listens, inside the host volume."""
    return f"{AGENT_DIR}/{naming.check_env(env)}.sock"


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
    # ClickHouse HTTP interface, for planners.
    clickhouse_url: str | None = None
    # Service → its ClickHouse database (without environment prefix); ``plan`` fills it in.
    databases: dict[str, str] = field(default_factory=dict, hash=False)
    # Service → environment whose tables this environment's readers use (``reads_from``).
    reads_from: dict[str, str] = field(default_factory=dict, hash=False)
    # Nomad host volume with the egress agents' sockets, on every machine that runs
    # target-leasing workers. Unset: those workers lease from the control API directly.
    agent_volume: str | None = None
    # S3 endpoint as jobs reach it (e.g. http://minio.service:9000), for bucket users.
    s3_endpoint: str | None = None
    s3_region: str = ""
    # Image of the archive sinks (leasehold's ``archive`` image target), with its tag.
    archive_image: str | None = None
    # Topic reference → bucket reference of its claims (any service); ``plan`` fills it in.
    claim_buckets: dict[str, str] = field(default_factory=dict, hash=False)
    # Topic reference → partitions (any service); ``plan`` fills it in.
    topic_partitions: dict[str, int] = field(default_factory=dict, hash=False)
    # Image of the snapshot loaders (leasehold's base image), with its tag.
    snapshot_image: str | None = None
    # Loader job ID → its settings (``leasehold.snapshots.plan.loader_settings``).
    snapshot_settings: dict[str, dict] = field(default_factory=dict, hash=False)


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


AnyComponent = Component | ArchiveComponent | SnapshotLoaderComponent


def uses_kafka(c: AnyComponent) -> bool:
    return bool(c.topics_produced()) or isinstance(c, Processor | ArchiveComponent)


def uses_storage(c: AnyComponent) -> bool:
    return bool(c.reads_buckets or c.writes_buckets)


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


def _archive_env(spec: ServiceSpec, c: ArchiveComponent, env: str, platform: Platform) -> str:
    """The archive sink's settings: the sink as declared, with the actual names."""
    sink = spec.archives[c.sink]
    ref = c.consumes[0]
    claims = platform.claim_buckets.get(ref)
    settings = {
        "sink": c.sink,
        "spec": sink.model_dump(mode="json", by_alias=True),
        "topic": naming.topic_for(env, naming.TopicRef.parse(ref)),
        "topic_ref": ref,
        "bucket": naming.bucket(env, spec.name, sink.bucket),
        "claims_bucket": (
            naming.bucket_for(env, naming.BucketRef.parse(claims)) if claims else None
        ),
    }
    return json.dumps(settings, separators=(",", ":"), sort_keys=True)


ARCHIVE_DISK_MARGIN_MB = 256


def archive_disk_mb(spec: ServiceSpec, c: ArchiveComponent, platform: Platform) -> int:
    """Local disk for an archive sink's open files: every partition on one instance."""
    sink = spec.archives[c.sink]
    ref = c.consumes[0]
    if ref not in platform.topic_partitions:
        raise ValueError(f"{spec.name}.{c.sink}: archives {ref}, which no service produces")
    per_file = sink.file_size_mb + sink.row_group_mb
    return platform.topic_partitions[ref] * per_file + ARCHIVE_DISK_MARGIN_MB


def _env(
    spec: ServiceSpec, cname: str, c: AnyComponent, env: str, version: str, platform: Platform
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
    if uses_storage(c):
        if not platform.s3_endpoint:
            raise ValueError(f"{spec.name}.{cname} uses buckets; set the S3 endpoint (storage)")
        out["LEASEHOLD_S3_ENDPOINT"] = platform.s3_endpoint
        if platform.s3_region:
            out["LEASEHOLD_S3_REGION"] = platform.s3_region
    if isinstance(c, ArchiveComponent):
        out["LEASEHOLD_ARCHIVE"] = _archive_env(spec, c, env, platform)
    if isinstance(c, TaskWorker):
        # Nomad interpolates the node's meta; a node without it leaves the text as is,
        # which the runtime reads as no egress address.
        out["LEASEHOLD_EGRESS_IP"] = "${meta.egress_ip}"
        if _uses_agent(spec, c, platform):
            out["LEASEHOLD_AGENT_SOCKET"] = agent_socket(env)
    if isinstance(c, SnapshotLoaderComponent):
        settings = platform.snapshot_settings.get(naming.nomad_job(spec.name, cname))
        if settings is None:
            raise ValueError(f"{spec.name}.{cname}: no loader settings (snapshot_settings)")
        if not platform.clickhouse_url:
            raise ValueError(f"{spec.name}.{cname} loads into ClickHouse; set the ClickHouse URL")
        out["LEASEHOLD_SNAPSHOT"] = json.dumps(settings, separators=(",", ":"), sort_keys=True)
        out["LEASEHOLD_CLICKHOUSE_URL"] = platform.clickhouse_url
        out["LEASEHOLD_TIMEOUT_S"] = str(int(c.timeout.total_seconds()))
    if isinstance(c, Scheduled):
        out["LEASEHOLD_TIMEOUT_S"] = str(int(c.timeout.total_seconds()))
    if isinstance(c, Scheduled) and c.reads_clickhouse:
        if not platform.clickhouse_url:
            raise ValueError(f"{spec.name}.{cname} reads ClickHouse; set the ClickHouse URL")
        out["LEASEHOLD_CLICKHOUSE_URL"] = platform.clickhouse_url
        dbs = {
            s: platform.databases[s]
            for s in sorted({r.split(".", 1)[0] for r in c.clickhouse_reads} - {spec.name})
            if s in platform.databases
        }
        out["LEASEHOLD_PLANNER_DATABASES"] = json.dumps(dbs, separators=(",", ":"))
        envs = {s: platform.reads_from[s] for s in dbs if s in platform.reads_from}
        if envs:
            out["LEASEHOLD_PLANNER_ENVS"] = json.dumps(envs, separators=(",", ":"))
    # The spec's own env can't override the platform variables.
    clash = sorted(k for k in c.env if k in out)
    if clash:
        raise ValueError(f"{spec.name}.{cname}: env may not set {', '.join(clash)}")
    return out | dict(c.env)


def _uses_agent(spec: ServiceSpec, c: Component, platform: Platform) -> bool:
    if not platform.agent_volume or not isinstance(c, TaskWorker):
        return False
    types = spec.frontier.task_types
    return any(t in types and types[t].target_leases for t in c.consumes_tasks)


def _task(
    spec: ServiceSpec, cname: str, c: AnyComponent, env: str, version: str, platform: Platform
) -> dict[str, Any]:
    job_id = naming.nomad_job(spec.name, cname)
    if isinstance(c, ArchiveComponent):
        if not platform.archive_image:
            raise ValueError(f"{spec.name}.{cname}: set the archive image (jobs.archive_image)")
        config: dict[str, Any] = {
            "image": platform.archive_image,
            "command": "leasehold",
            "args": ["archive", "run"],
        }
    elif isinstance(c, SnapshotLoaderComponent):
        if not platform.snapshot_image:
            raise ValueError(f"{spec.name}.{cname}: set the snapshot image (jobs.snapshot_image)")
        config = {
            "image": platform.snapshot_image,
            "command": "leasehold",
            "args": ["snapshot", "load"],
        }
    else:
        config = {"image": naming.image_ref(platform.registry, spec.name, cname, version)}
    if c.command:
        config["command"] = c.command[0]
        if c.command[1:]:
            config["args"] = list(c.command[1:])
    if not isinstance(c, Scheduled | SnapshotLoaderComponent):
        config["ports"] = [HTTP_PORT]

    # The job's own variable holds its Kafka, storage and ClickHouse credentials: a
    # scheduled component that only reads ClickHouse needs it too.
    own = (
        uses_kafka(c)
        or uses_storage(c)
        or isinstance(c, SnapshotLoaderComponent)
        or (isinstance(c, Scheduled) and bool(c.reads_clickhouse))
    )
    secret_paths = ([job_variable_path(job_id)] if own else []) + (
        [] if isinstance(c, ArchiveComponent | SnapshotLoaderComponent) else list(spec.secrets)
    )
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

    sandbox = c.sandbox
    if sandbox is not None:
        # The group's bridge network maps the port; the container itself publishes none.
        config.pop("ports", None)
        config = _hardened(config, sandbox)

    task: dict[str, Any] = {}
    if sandbox is not None:
        task["User"] = _user(sandbox.user, sandbox.group)
        if sandbox.tasks:
            task["Leader"] = True  # the other tasks stop when the component's own does
    if _uses_agent(spec, c, platform):
        task["VolumeMounts"] = [
            {"Volume": AGENT_VOLUME, "Destination": AGENT_DIR, "ReadOnly": False}
        ]

    return task | {
        "Name": cname,
        "Driver": "docker",
        "Config": config,
        "Env": _env(spec, cname, c, env, version, platform),
        "Templates": templates,
        "Resources": _resources(c.resources, sandbox.cores if sandbox else None),
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


def _resources(r: Resources, cores: int | None = None) -> dict[str, Any]:
    """Nomad task resources; ``cores`` (whole cores) replaces the ``cpu`` reservation."""
    out: dict[str, Any] = {"Cores": cores} if cores else {"CPU": r.cpu}
    out["MemoryMB"] = r.memory
    if r.memory_max:
        out["MemoryMaxMB"] = r.memory_max
    return out


def _user(user: int, group: int | None) -> str:
    # Docker gives a bare UID group 0 (root) when the image has no such user.
    return f"{user}:{group if group is not None else user}"


def _hardened(
    config: dict[str, Any],
    s: Sandbox | SandboxTask | Netfilter,
    cap_add: list[str] | None = None,
) -> dict[str, Any]:
    """A sandboxed task's Docker settings: read-only root file system, no capabilities
    (but ``cap_add``), no-new-privileges, the tmpfs mounts, seccomp, /dev/shm, runtime."""
    out = dict(config)
    out["readonly_rootfs"] = True
    out["cap_drop"] = ["ALL"]
    if cap_add:
        out["cap_add"] = list(cap_add)
    out["security_opt"] = ["no-new-privileges"] + ([f"seccomp={s.seccomp}"] if s.seccomp else [])
    if s.tmpfs:
        out["mount"] = [
            {"type": "tmpfs", "target": t, "readonly": False, "tmpfs_options": {"size": size}}
            for t, size in s.tmpfs.items()
        ]
    if getattr(s, "shm_size", None):
        out["shm_size"] = s.shm_size
    if s.runtime:
        out["runtime"] = s.runtime
    return out


def _command(config: dict[str, Any], command: list[str]) -> dict[str, Any]:
    out = dict(config, command=command[0])
    if command[1:]:
        out["args"] = list(command[1:])
    return out


# The component's addresses the netfilter task may need for its rules (never secrets).
NETFILTER_ADDRESSES = ("LEASEHOLD_API_URL", "LEASEHOLD_KAFKA_BOOTSTRAP", "LEASEHOLD_S3_ENDPOINT")


def _sandbox_tasks(
    sandbox: Sandbox, cname: str, leader_env: dict[str, str], image: str, kill_timeout: timedelta
) -> list[dict[str, Any]]:
    """The netfilter prestart task and the extra tasks of a sandboxed group. They get
    no template, no Nomad Variables, no workload identity; the extra tasks only their
    own env, netfilter also the users and addresses its rules are about."""
    tasks = []
    if sandbox.netfilter is not None:
        nf = sandbox.netfilter
        users = {cname: sandbox.user} | {name: t.user for name, t in sandbox.tasks.items()}
        nf_env = {k: leader_env[k] for k in NETFILTER_ADDRESSES if k in leader_env}
        nf_env["LEASEHOLD_SANDBOX_USERS"] = json.dumps(users, separators=(",", ":"))
        config = _command({"image": nf.image or image}, nf.command)
        tasks.append(
            {
                "Name": NETFILTER_TASK,
                "Driver": "docker",
                "User": "0:0",  # iptables needs root; NET_ADMIN is its only capability
                "Lifecycle": {"Hook": "prestart", "Sidecar": False},
                "Config": _hardened(config, nf, cap_add=["NET_ADMIN"]),
                "Env": dict(nf.env) | nf_env,
                "Resources": _resources(nf.resources),
                "KillTimeout": _ns(kill_timeout),
            }
        )
    for name, t in sandbox.tasks.items():
        config = _command({"image": t.image or image}, t.command)
        tasks.append(
            {
                "Name": name,
                "Driver": "docker",
                "User": _user(t.user, t.group),
                "Config": _hardened(config, t),
                "Env": dict(t.env),
                "Resources": _resources(t.resources, t.cores),
                "KillTimeout": _ns(kill_timeout),
            }
        )
    return tasks


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
    c = spec.all_components()[cname]
    job_id = naming.nomad_job(spec.name, cname)
    namespace = naming.nomad_namespace(env)
    task = _task(spec, cname, c, env, version, platform)
    group: dict[str, Any] = {"Name": cname, "Tasks": [task]}
    sandbox = c.sandbox
    if sandbox is not None:
        group["Tasks"] += _sandbox_tasks(
            sandbox, cname, task["Env"], task["Config"]["image"], c.kill_timeout
        )
        # Every task in its own containers, sharing one network namespace (127.0.0.1).
        group["Networks"] = [{"Mode": "bridge"}]
    if _uses_agent(spec, c, platform):
        group["Volumes"] = {
            AGENT_VOLUME: {"Name": AGENT_VOLUME, "Type": "host", "Source": platform.agent_volume}
        }
    if isinstance(c, ArchiveComponent):
        group["EphemeralDisk"] = {"SizeMB": archive_disk_mb(spec, c, platform)}
    elif c.resources.disk:
        group["EphemeralDisk"] = {"SizeMB": c.resources.disk}
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

    if isinstance(c, Scheduled | SnapshotLoaderComponent):
        if isinstance(c, Scheduled):
            cron, tz = c.schedule, "UTC"
        else:
            window = platform.snapshot_settings[job_id]["window"]
            cron, tz = window_cron(window), window.get("timezone", "UTC")
        job["Type"] = "batch"
        job["Periodic"] = {
            "Enabled": True,
            "SpecType": "cron",
            "Specs": [cron],
            "ProhibitOverlap": True,
            "TimeZone": tz,
        }
        group["Count"] = 1
        group["RestartPolicy"] = {"Attempts": 0, "Mode": "fail"}
        group["ReschedulePolicy"] = {"Attempts": 0, "Unlimited": False}
        return JobPlan(spec.name, cname, job_id, namespace, None, None, job)

    assert isinstance(c, TaskWorker | Processor | Api | ArchiveComponent)
    job["Type"] = "service"
    group["Count"] = c.count.min
    port: dict[str, Any] = {"Label": HTTP_PORT}
    if platform.host_network:
        port["HostNetwork"] = platform.host_network
    network: dict[str, Any] = {"Mode": "bridge"} if sandbox is not None else {}
    group["Networks"] = [network | {"DynamicPorts": [port]}]
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


def window_cron(window: dict) -> str:
    """A loader's schedule: every 20 minutes in the window's hours (it checks minutes)."""
    from leasehold.snapshots.loader import Window

    return Window(**window).cron()


def plan(specs: list[ServiceSpec], env: str, version: str, platform: Platform) -> NomadPlan:
    """Every component's job. ``version`` is the git SHA the images are tagged with."""
    naming.check_env(env)
    platform = replace(
        platform,
        payload_models={
            str(s.topic_ref(name)): t.payload_model for s in specs for name, t in s.topics().items()
        }
        | platform.payload_models,
        databases={s.name: s.clickhouse.database for s in specs if s.clickhouse}
        | platform.databases,
        claim_buckets={str(t): str(b) for s in specs for t, b in s.large_payload_buckets().items()}
        | platform.claim_buckets,
        topic_partitions={
            str(s.topic_ref(name)): t.partitions for s in specs for name, t in s.topics().items()
        }
        | platform.topic_partitions,
    )
    jobs = [
        render_job(spec, cname, env, version, platform)
        for spec in sorted(specs, key=lambda s: s.name)
        for cname in spec.all_components()
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
