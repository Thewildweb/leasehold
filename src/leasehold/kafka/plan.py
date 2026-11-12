"""What Kafka should contain for a set of services in one environment.

``plan()`` is a pure function from ``service.yml`` specs to topics, principals
and ACLs. ``leasehold.kafka.apply`` compares the plan with a live cluster.

ACL rules, per environment:

- A component that produces a topic gets WRITE and DESCRIBE on it.
- A processor gets READ and DESCRIBE on each topic it consumes, and READ on its
  consumer group.
- ClickHouse reads each sink's topic as the service's ``<service>.clickhouse``
  principal, with one consumer group per sink.

Everything is a LITERAL resource pattern: no prefix or wildcard ACLs, so each
grant can be traced to one line in one ``service.yml``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from leasehold import naming
from leasehold.spec.models import Processor, ServiceSpec


@dataclass(frozen=True, order=True)
class TopicPlan:
    name: str
    partitions: int
    configs: tuple[tuple[str, str], ...]

    def config_dict(self) -> dict[str, str]:
        return dict(self.configs)


@dataclass(frozen=True, order=True)
class AclPlan:
    principal: str  # e.g. "User:dev.certlog.worker"
    resource_type: str  # TOPIC | GROUP
    resource_name: str
    operation: str  # READ | WRITE | DESCRIBE

    def __str__(self) -> str:
        return f"{self.principal} {self.operation} {self.resource_type}:{self.resource_name}"


@dataclass
class KafkaPlan:
    env: str
    topics: list[TopicPlan] = field(default_factory=list)
    principals: list[str] = field(default_factory=list)  # SCRAM user names, without "User:"
    acls: list[AclPlan] = field(default_factory=list)


def _topic_configs(spec: ServiceSpec, topic_name: str) -> tuple[tuple[str, str], ...]:
    t = spec.kafka.produces[topic_name]
    configs = {
        "cleanup.policy": "delete",
        "retention.ms": str(int(t.retention.total_seconds() * 1000)),
    }
    if t.retention_bytes is not None:
        configs["retention.bytes"] = str(t.retention_bytes)
    return tuple(sorted(configs.items()))


def plan(specs: Iterable[ServiceSpec], env: str) -> KafkaPlan:
    naming.check_env(env)
    result = KafkaPlan(env=env)
    principals: set[str] = set()
    acls: set[AclPlan] = set()

    def grant(user: str, resource_type: str, resource: str, *operations: str) -> None:
        principals.add(user)
        for op in operations:
            acls.add(AclPlan(f"User:{user}", resource_type, resource, op))

    for spec in specs:
        for tname, t in spec.kafka.produces.items():
            result.topics.append(
                TopicPlan(
                    name=naming.topic_for(env, spec.topic_ref(tname)),
                    partitions=t.partitions,
                    configs=_topic_configs(spec, tname),
                )
            )

        for cname, c in spec.components.items():
            user = naming.kafka_principal(env, spec.name, cname)
            for tname in c.produces:
                topic = naming.topic_for(env, spec.topic_ref(tname))
                grant(user, "TOPIC", topic, "WRITE", "DESCRIBE")
            if isinstance(c, Processor):
                for ref_str in c.consumes:
                    topic = naming.topic_for(env, naming.TopicRef.parse(ref_str))
                    grant(user, "TOPIC", topic, "READ", "DESCRIBE")
                group = naming.consumer_group(env, spec.name, cname)
                grant(user, "GROUP", group, "READ")

        if spec.clickhouse:
            user = naming.clickhouse_sink_principal(env, spec.name)
            for sname, sink in spec.clickhouse.sinks.items():
                ref = spec.sink_topic_ref(sink.topic)
                grant(user, "TOPIC", naming.topic_for(env, ref), "READ", "DESCRIBE")
                grant(user, "GROUP", naming.clickhouse_sink_group(env, spec.name, sname), "READ")

    result.topics.sort()
    result.principals = sorted(principals)
    result.acls = sorted(acls)
    return result
