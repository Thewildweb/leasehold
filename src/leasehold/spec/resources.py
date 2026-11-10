"""The concrete resource names a service gets in one environment."""

from __future__ import annotations

from leasehold import naming
from leasehold.spec.models import Processor, ServiceSpec

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
    for tname in spec.kafka.produces:
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
    rows.append(("object prefix", "-", naming.object_prefix(env, spec.name)))
    return rows
