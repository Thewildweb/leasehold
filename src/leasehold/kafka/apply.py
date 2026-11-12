"""Compare a ``KafkaPlan`` with a live cluster, and apply the difference.

``diff()`` only reads. ``apply()`` makes the changes ``diff()`` listed.

What is changed automatically:

- missing topics are created;
- topic configs that differ from the plan are set;
- missing ACLs are created;
- ACLs of managed principals that aren't in the plan are deleted, only with ``prune=True``.

What is only reported, never changed automatically:

- topics that no service declares (deleting a topic deletes its data);
- partition counts that differ from the plan (adding partitions changes which
  partition a key maps to, so it's a deliberate operation);
- principals without a SCRAM credential (credentials are created by whoever
  stores the password, not by this module).

A principal is *managed* if it belongs to one of the planned services in this
environment: ``User:<env prefix><service>.<...>``.

ACL changes propagate through the KRaft metadata log, so a ``diff()`` directly
after ``apply()`` can still list ACLs that were just created. Creating an ACL
that already exists is a no-op in Kafka, so re-applying is harmless.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from confluent_kafka import KafkaException
from confluent_kafka.admin import (
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourcePatternType,
    ResourceType,
)

from leasehold import naming
from leasehold.kafka.plan import AclPlan, KafkaPlan, TopicPlan

TIMEOUT_S = 30


@dataclass(frozen=True)
class Change:
    action: str  # "create topic" | "set config" | "create acl" | "delete acl"
    target: str
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.action:<12} {self.target}" + (f"  {self.detail}" if self.detail else "")


@dataclass
class Diff:
    changes: list[Change] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    # Internal: what apply() needs to make the changes.
    _new_topics: list[TopicPlan] = field(default_factory=list)
    _config_updates: dict[str, dict[str, str]] = field(default_factory=dict)
    _new_acls: list[AclPlan] = field(default_factory=list)
    _stale_acls: list[AclBinding] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.changes


def admin_client(bootstrap: str, username: str, password: str) -> AdminClient:
    return AdminClient(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": username,
            "sasl.password": password,
        }
    )


def _managed_prefixes(plan: KafkaPlan, services: list[str]) -> tuple[str, ...]:
    env_prefix = "" if plan.env == naming.PROD else f"{plan.env}."
    return tuple(f"User:{env_prefix}{s}." for s in services)


def _topic_prefixes(plan: KafkaPlan, services: list[str]) -> tuple[str, ...]:
    env_prefix = "" if plan.env == naming.PROD else f"{plan.env}."
    return tuple(f"{env_prefix}{s}." for s in services)


def _to_binding(acl: AclPlan) -> AclBinding:
    return AclBinding(
        ResourceType[acl.resource_type],
        acl.resource_name,
        ResourcePatternType.LITERAL,
        acl.principal,
        "*",
        AclOperation[acl.operation],
        AclPermissionType.ALLOW,
    )


def _from_binding(b: AclBinding) -> AclPlan | None:
    """The plan form of a live ACL, or None if it isn't of the shape plans produce."""
    if (
        b.resource_pattern_type != ResourcePatternType.LITERAL
        or b.permission_type != AclPermissionType.ALLOW
        or b.host != "*"
        or b.restype not in (ResourceType.TOPIC, ResourceType.GROUP)
    ):
        return None
    return AclPlan(b.principal, b.restype.name, b.name, b.operation.name)


def diff(admin: AdminClient, plan: KafkaPlan, services: list[str]) -> Diff:
    """What it takes to make the cluster match ``plan``. ``services``: the planned service names."""
    d = Diff()
    planned = {t.name: t for t in plan.topics}

    # --- topics ---
    live = admin.list_topics(timeout=TIMEOUT_S).topics
    for name, t in planned.items():
        if name not in live:
            d._new_topics.append(t)
            d.changes.append(Change("create topic", name, f"partitions={t.partitions}"))
        elif (n := len(live[name].partitions)) != t.partitions:
            d.warnings.append(
                f"topic {name}: has {n} partitions, service.yml says {t.partitions}; "
                "not changed automatically (changes the key → partition mapping)"
            )

    existing = [n for n in planned if n in live]
    if existing:
        resources = [ConfigResource(ResourceType.TOPIC, n) for n in existing]
        for res, fut in admin.describe_configs(resources, request_timeout=TIMEOUT_S).items():
            current = {k: v.value for k, v in fut.result().items()}
            wanted = planned[res.name].config_dict()
            updates = {k: v for k, v in wanted.items() if current.get(k) != v}
            if updates:
                d._config_updates[res.name] = updates
                for k, v in sorted(updates.items()):
                    d.changes.append(
                        Change("set config", res.name, f"{k}={v} (was {current.get(k)})")
                    )

    topic_prefixes = _topic_prefixes(plan, services)
    for name in sorted(live):
        if name.startswith(topic_prefixes) and name not in planned:
            d.warnings.append(f"topic {name}: not declared by any service.yml; not deleted")

    # --- ACLs ---
    any_filter = AclBindingFilter(
        ResourceType.ANY,
        None,
        ResourcePatternType.ANY,
        None,
        None,
        AclOperation.ANY,
        AclPermissionType.ANY,
    )
    live_bindings = admin.describe_acls(any_filter, request_timeout=TIMEOUT_S).result()
    managed = _managed_prefixes(plan, services)
    live_acls: set[AclPlan] = set()
    for b in live_bindings:
        if not b.principal.startswith(managed):
            continue
        as_plan = _from_binding(b)
        if as_plan is not None:
            live_acls.add(as_plan)
        if as_plan is None or as_plan not in plan.acls:
            d._stale_acls.append(b)

    for acl in plan.acls:
        if acl not in live_acls:
            d._new_acls.append(acl)
            d.changes.append(Change("create acl", str(acl)))
    for b in d._stale_acls:
        d.changes.append(
            Change(
                "delete acl",
                f"{b.principal} {b.operation.name} {b.restype.name}:{b.name}",
                "(only with --prune)",
            )
        )

    # --- SCRAM credentials ---
    users = admin.describe_user_scram_credentials(request_timeout=TIMEOUT_S).result()
    for principal in plan.principals:
        if principal not in users:
            d.warnings.append(f"principal {principal}: has no SCRAM credential yet")

    return d


def apply(admin: AdminClient, d: Diff, *, prune: bool = False) -> None:
    """Make the changes in ``d``. Raises ``KafkaException`` on the first failure."""
    if d._new_topics:
        new = [
            NewTopic(
                t.name, num_partitions=t.partitions, replication_factor=1, config=t.config_dict()
            )
            for t in d._new_topics
        ]
        for fut in admin.create_topics(new, request_timeout=TIMEOUT_S).values():
            fut.result()

    if d._config_updates:
        resources = [
            ConfigResource(
                ResourceType.TOPIC,
                topic,
                incremental_configs=[
                    ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET)
                    for k, v in sorted(updates.items())
                ],
            )
            for topic, updates in d._config_updates.items()
        ]
        for fut in admin.incremental_alter_configs(resources, request_timeout=TIMEOUT_S).values():
            fut.result()

    if d._new_acls:
        bindings = [_to_binding(a) for a in d._new_acls]
        for fut in admin.create_acls(bindings, request_timeout=TIMEOUT_S).values():
            fut.result()

    if prune and d._stale_acls:
        filters = [
            AclBindingFilter(
                b.restype,
                b.name,
                b.resource_pattern_type,
                b.principal,
                b.host,
                b.operation,
                b.permission_type,
            )
            for b in d._stale_acls
        ]
        for fut in admin.delete_acls(filters, request_timeout=TIMEOUT_S).values():
            fut.result()


__all__ = ["Change", "Diff", "KafkaException", "admin_client", "apply", "diff"]
