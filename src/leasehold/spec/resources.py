"""The concrete resource names a service gets in one environment."""

from __future__ import annotations

from leasehold import naming
from leasehold.spec.models import Processor, Scheduled, ServiceSpec

FRONTIER_SCHEMA = "frontier"


def resource_names(spec: ServiceSpec, env: str) -> list[tuple[str, str, str]]:
    """Rows of (kind, declared as, actual name) for everything ``spec`` owns in ``env``."""
    rows: list[tuple[str, str, str]] = [
        ("nomad namespace", "-", naming.nomad_namespace(env)),
    ]
    for cname, c in spec.components.items():
        rows.append(("nomad job", cname, naming.nomad_job(spec.name, cname)))
        rows.append(("image", cname, naming.image_name(spec.name, cname)))
        rows.append(("kafka principal", cname, naming.kafka_principal(env, spec.name, cname)))
        if isinstance(c, Processor):
            rows.append(("consumer group", cname, naming.consumer_group(env, spec.name, cname)))
        if isinstance(c, Scheduled) and c.reads_clickhouse:
            rows.append(
                ("clickhouse user", cname, naming.planner_clickhouse_user(env, spec.name, cname))
            )
    for tname in spec.topics():
        rows.append(("topic", tname, naming.topic_for(env, spec.topic_ref(tname))))
    for ref in sorted(spec.consumed_topic_refs()):
        rows.append(("reads topic", str(ref), naming.topic_for(env, ref)))
    for tt in spec.frontier.task_types:
        rows.append(("task type", tt, f"{naming.postgres_schema(env, FRONTIER_SCHEMA)}: {tt}"))
    if spec.clickhouse:
        db = naming.clickhouse_database(env, spec.clickhouse.database)
        rows.append(("clickhouse database", spec.clickhouse.database, db))
        for sname in spec.clickhouse.sinks:
            rows.append(("clickhouse sink", sname, f"{db}.{sname}"))
        if spec.plan_log_ref() is not None:
            for sname in (naming.PLAN_LOG_KEYS_SINK, naming.PLAN_LOG_RUNS_SINK):
                rows.append(("clickhouse sink", "(plan log)", f"{db}.{sname}"))
        for name in spec.clickhouse.shared:
            rows.append(("shared table", name, f"{db}.{name}"))
    for bname in spec.buckets:
        rows.append(("bucket", bname, naming.bucket(env, spec.name, bname)))
    for cname, c in spec.all_components().items():
        if c.reads_buckets or c.writes_buckets:
            rows.append(("storage user", cname, naming.storage_user(env, spec.name, cname)))
    for dname, d in spec.snapshots.items():
        cname = naming.snapshot_loader_component(dname)
        rows.append(("snapshot loader", dname, naming.nomad_job(spec.name, cname)))
        rows.append(("clickhouse user", cname, naming.snapshot_loader_user(env, spec.name, dname)))
        rows.append(("named collection", dname,
                     naming.snapshot_collection(env, spec.name, d.bucket)))  # fmt: skip
        if spec.clickhouse:
            db = naming.clickhouse_database(env, spec.clickhouse.database)
            for t in d.tables:
                rows.append(("snapshot table", t, f"{db}.{t} ({db}.{naming.snapshot_table(t)})"))
    if spec.snapshots and spec.clickhouse:
        db = naming.clickhouse_database(env, spec.clickhouse.database)
        rows.append(("clickhouse table", "(snapshot loads)", f"{db}.{naming.SNAPSHOT_LOADS_TABLE}"))
        rows.append(
            ("clickhouse user", "(view definer)", naming.clickhouse_definer_user(env, spec.name))
        )
        rows.append(
            ("storage user", "(clickhouse)", naming.clickhouse_storage_user(env, spec.name))
        )
    for aname in spec.archives:
        cname = naming.archive_component(aname)
        rows.append(("archive sink", aname, naming.nomad_job(spec.name, cname)))
        rows.append(("kafka principal", cname, naming.kafka_principal(env, spec.name, cname)))
        rows.append(("consumer group", cname, naming.consumer_group(env, spec.name, cname)))
    return rows
