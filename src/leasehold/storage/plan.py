"""What object storage should contain for a set of services in one environment.

``plan()`` is a pure function from ``service.yml`` specs to buckets and users;
``leasehold.storage.apply`` compares it with a live MinIO.

- Every declared bucket (``naming.bucket``), with lifecycle rules:
  ``leasehold-retention`` (whole bucket, the bucket's ``retention``) and, for each topic
  whose large payloads go to the bucket, ``leasehold-claims-<topic>`` on that topic's
  claims prefix, expiring one day after the topic's retention (a claim only has to
  outlive the event that points at it).
- One user per component that reads or writes buckets (``naming.storage_user``, the same
  name as its Kafka user), with one policy (``naming.storage_policy``) that allows only
  its buckets: read = list and get; write = read + put, delete and multipart uploads.
  Archive sinks write their bucket and read the claims of the topic they archive.
- The user's keys go into the job's Nomad Variable (``nomad/jobs/<job>``) as
  ``LEASEHOLD_S3_ACCESS_KEY`` / ``LEASEHOLD_S3_SECRET_KEY``.
- A service with ``snapshots`` also gets a user for ClickHouse
  (``naming.clickhouse_storage_user``), read-only on its snapshot buckets. Its keys go
  into ``leasehold/<service>/clickhouse-storage``, a variable no job reads; the
  ClickHouse step puts them into the datasets' named collections.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from leasehold import naming
from leasehold.nomad.render import job_variable_path
from leasehold.spec.models import ArchiveComponent, ServiceSpec

ACCESS_KEY = "LEASEHOLD_S3_ACCESS_KEY"
SECRET_KEY = "LEASEHOLD_S3_SECRET_KEY"
RULE_PREFIX = "leasehold-"
RETENTION_RULE = "leasehold-retention"
CLAIM_MARGIN_DAYS = 1

READ_ACTIONS = ("s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject")
WRITE_ACTIONS = READ_ACTIONS + (
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:AbortMultipartUpload",
    "s3:ListMultipartUploadParts",
    "s3:ListBucketMultipartUploads",
)


@dataclass(frozen=True, order=True)
class LifecycleRule:
    id: str
    prefix: str  # "" = whole bucket
    days: int


@dataclass(frozen=True)
class BucketPlan:
    name: str
    service: str
    bucket: str  # as declared
    versioning: bool
    rules: tuple[LifecycleRule, ...]

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class UserPlan:
    service: str
    component: str
    user: str
    policy: str
    namespace: str
    variable: str
    read: frozenset[str]  # bucket names, without those in write
    write: frozenset[str]

    def policy_document(self) -> dict:
        statements = []
        for actions, buckets in ((READ_ACTIONS, self.read), (WRITE_ACTIONS, self.write)):
            if not buckets:
                continue
            bucket_actions = [a for a in actions if a in _BUCKET_ACTIONS]
            object_actions = [a for a in actions if a not in _BUCKET_ACTIONS]
            names = sorted(buckets)
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": bucket_actions,
                    "Resource": [f"arn:aws:s3:::{b}" for b in names],
                }
            )
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": object_actions,
                    "Resource": [f"arn:aws:s3:::{b}/*" for b in names],
                }
            )
        return {"Version": "2012-10-17", "Statement": statements}

    def __str__(self) -> str:
        return self.user


_BUCKET_ACTIONS = {"s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"}


@dataclass
class StoragePlan:
    env: str
    buckets: list[BucketPlan] = field(default_factory=list)
    users: list[UserPlan] = field(default_factory=list)


def _days(seconds: float) -> int:
    return max(1, math.ceil(seconds / 86400))


def plan(specs: list[ServiceSpec], env: str) -> StoragePlan:
    naming.check_env(env)
    result = StoragePlan(env=env)
    claims = {t: b for s in specs for t, b in s.large_payload_buckets().items()}

    for spec in specs:
        claim_rules: dict[str, list[LifecycleRule]] = {}
        for tname, topic in spec.kafka.produces.items():
            if topic.large_payloads:
                ref = spec.topic_ref(tname)
                claim_rules.setdefault(topic.large_payloads, []).append(
                    LifecycleRule(
                        id=f"{RULE_PREFIX}claims-{ref}",
                        prefix=naming.claims_prefix(ref),
                        days=_days(topic.retention.total_seconds()) + CLAIM_MARGIN_DAYS,
                    )
                )
        for bname, b in spec.buckets.items():
            rules = list(claim_rules.get(bname, []))
            if b.retention is not None:
                rules.append(LifecycleRule(RETENTION_RULE, "", _days(b.retention.total_seconds())))
            result.buckets.append(
                BucketPlan(
                    name=naming.bucket(env, spec.name, bname),
                    service=spec.name,
                    bucket=bname,
                    versioning=b.versioning,
                    rules=tuple(sorted(rules)),
                )
            )

        for cname, c in spec.all_components().items():
            read, write = c.bucket_refs(spec.name)
            if isinstance(c, ArchiveComponent):
                bucket = claims.get(naming.TopicRef.parse(c.consumes[0]))
                if bucket is not None and bucket not in write:
                    read = read | {bucket}
            if not read and not write:
                continue
            result.users.append(
                UserPlan(
                    service=spec.name,
                    component=cname,
                    user=naming.storage_user(env, spec.name, cname),
                    policy=naming.storage_policy(env, spec.name, cname),
                    namespace=naming.nomad_namespace(env),
                    variable=job_variable_path(naming.nomad_job(spec.name, cname)),
                    read=frozenset(naming.bucket_for(env, r) for r in read),
                    write=frozenset(naming.bucket_for(env, r) for r in write),
                )
            )

        snapshot_buckets = {d.bucket for d in spec.snapshots.values()}
        if snapshot_buckets:
            result.users.append(
                UserPlan(
                    service=spec.name,
                    component="clickhouse",
                    user=naming.clickhouse_storage_user(env, spec.name),
                    policy=naming.storage_policy(env, spec.name, "clickhouse"),
                    namespace=naming.nomad_namespace(env),
                    variable=naming.clickhouse_storage_variable(spec.name),
                    read=frozenset(naming.bucket(env, spec.name, b) for b in snapshot_buckets),
                    write=frozenset(),
                )
            )

    result.buckets.sort(key=lambda b: b.name)
    result.users.sort(key=lambda u: u.user)
    return result
