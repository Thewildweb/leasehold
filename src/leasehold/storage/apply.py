"""Compare a ``StoragePlan`` with a live MinIO and apply the differences.

Buckets, lifecycle rules and versioning go through the plain S3 API; users and policies
through MinIO's admin API (the only MinIO-specific part).

``diff`` reports:

- buckets to create, versioning to turn on (or suspend), lifecycle rules to set. Rules
  whose ID starts with ``leasehold-`` are ours; other rules on a bucket are kept;
- users to create or give a new secret (the user or the key in its Nomad Variable is
  missing, or ``rotate``), policies to create or update, policies to attach;
- stale, never removed without ``prune``: buckets of the given services that no spec
  declares (with size and last write; buckets are never deleted, not even with
  ``prune``), users and ``leasehold.`` policies of the given services that the plan no
  longer has, and the storage keys in Nomad Variables of jobs that no longer use storage.

Secrets are never printed. Nomad's state is the only copy of a secret; ``--rotate``
sets new ones.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urlsplit

from minio import Minio
from minio.commonconfig import ENABLED, Filter
from minio.credentials import StaticProvider
from minio.error import MinioAdminException, S3Error
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule
from minio.minioadmin import MinioAdmin
from minio.versioningconfig import SUSPENDED, VersioningConfig

from leasehold import naming
from leasehold.credentials import new_password
from leasehold.nomad.apply import Nomad
from leasehold.storage.plan import (
    ACCESS_KEY,
    RULE_PREFIX,
    SECRET_KEY,
    BucketPlan,
    LifecycleRule,
    StoragePlan,
    UserPlan,
)

# Stale-bucket report: stop counting objects after this many.
SIZE_SCAN_LIMIT = 100_000


@dataclass
class Storage:
    """S3 + MinIO admin clients for one endpoint (``http://host:port``)."""

    s3: Minio
    admin: MinioAdmin

    @classmethod
    def connect(cls, endpoint: str, access_key: str, secret_key: str, region: str = "") -> Storage:
        u = urlsplit(endpoint if "://" in endpoint else f"https://{endpoint}")
        secure = u.scheme == "https"
        creds = StaticProvider(access_key, secret_key)
        return cls(
            s3=Minio(u.netloc, credentials=creds, secure=secure, region=region or None),
            admin=MinioAdmin(endpoint=u.netloc, credentials=creds, secure=secure, region=region),
        )


def _admin_missing(e: MinioAdminException) -> bool:
    return getattr(e, "_code", None) == "404" or str(getattr(e, "_code", "")) == "404"


# --- Buckets ---------------------------------------------------------------------------------


@dataclass
class BucketChange:
    bucket: BucketPlan
    create: bool = False
    versioning: bool | None = None  # True: enable, False: suspend, None: leave
    rules: tuple[LifecycleRule, ...] | None = None  # set our rules to these
    _foreign_rules: list[Rule] = field(default_factory=list, repr=False)

    def __str__(self) -> str:
        what = []
        if self.create:
            what.append("create")
        if self.versioning is True:
            what.append("versioning on")
        elif self.versioning is False:
            what.append("versioning suspended")
        if self.rules is not None:
            rules = (
                ", ".join(f"{r.prefix or '*'} expires after {r.days}d" for r in self.rules)
                or "no expiry"
            )
            what.append(f"lifecycle: {rules}")
        return f"bucket   {self.bucket.name}: {'; '.join(what)}"


@dataclass(frozen=True)
class StaleBucket:
    name: str
    objects: int
    size: int
    last_write: datetime | None
    truncated: bool

    def __str__(self) -> str:
        n = f"{'>=' if self.truncated else ''}{self.objects}"
        last = self.last_write.strftime("%Y-%m-%d %H:%M") if self.last_write else "never"
        return f"bucket {self.name} ({n} objects, {self.size / 1e6:.1f} MB, last write {last})"


def _our_rules(config: LifecycleConfig | None) -> tuple[list[LifecycleRule], list[Rule]]:
    ours, foreign = [], []
    for r in (config.rules if config else []) or []:
        if r.rule_id and r.rule_id.startswith(RULE_PREFIX):
            prefix = (r.rule_filter.prefix if r.rule_filter else None) or ""
            days = r.expiration.days if r.expiration and r.expiration.days else 0
            if r.status == ENABLED:
                ours.append(LifecycleRule(r.rule_id, prefix, days))
        else:
            foreign.append(r)
    return sorted(ours), foreign


def _diff_bucket(st: Storage, b: BucketPlan, exists: bool) -> BucketChange | None:
    if not exists:
        return BucketChange(
            b, create=True, versioning=True if b.versioning else None, rules=b.rules or None
        )
    ch = BucketChange(b)
    status = st.s3.get_bucket_versioning(b.name).status  # "Off", "Enabled", "Suspended"
    if b.versioning and status != ENABLED:
        ch.versioning = True
    elif not b.versioning and status == ENABLED:
        ch.versioning = False
    try:
        config = st.s3.get_bucket_lifecycle(b.name)
    except S3Error as e:
        if e.code != "NoSuchLifecycleConfiguration":
            raise
        config = None
    ours, foreign = _our_rules(config)
    if tuple(ours) != b.rules:
        ch.rules = b.rules
        ch._foreign_rules = foreign
    if ch.versioning is None and ch.rules is None:
        return None
    return ch


def _apply_bucket(st: Storage, ch: BucketChange) -> None:
    name = ch.bucket.name
    if ch.create:
        st.s3.make_bucket(name)
    if ch.versioning is not None:
        st.s3.set_bucket_versioning(name, VersioningConfig(ENABLED if ch.versioning else SUSPENDED))
    if ch.rules is not None:
        rules = list(ch._foreign_rules) + [
            Rule(
                ENABLED,
                rule_filter=Filter(prefix=r.prefix),
                rule_id=r.id,
                expiration=Expiration(days=r.days),
            )
            for r in ch.rules
        ]
        if rules:
            st.s3.set_bucket_lifecycle(name, LifecycleConfig(rules))
        else:
            st.s3.delete_bucket_lifecycle(name)


def _stale_bucket(st: Storage, name: str) -> StaleBucket:
    n = size = 0
    last = None
    for obj in st.s3.list_objects(name, recursive=True):
        n += 1
        size += obj.size or 0
        if obj.last_modified and (last is None or obj.last_modified > last):
            last = obj.last_modified
        if n >= SIZE_SCAN_LIMIT:
            return StaleBucket(name, n, size, last, True)
    return StaleBucket(name, n, size, last, False)


def _owner(name: str, prefix: str, services: list[str]) -> str | None:
    """The managed service a bucket name belongs to (longest matching service name)."""
    if not name.startswith(prefix):
        return None
    rest = name[len(prefix) :]
    owners = [s for s in services if rest.startswith(f"{s}-")]
    return max(owners, key=len) if owners else None


# --- Users -----------------------------------------------------------------------------------


@dataclass
class UserChange:
    user: UserPlan
    reason: str | None = None  # new secret: "new", "user missing", "variable missing", "rotate"
    policy: str | None = None  # "create" | "update"
    attach: bool = False
    _items: dict[str, str] = field(default_factory=dict, repr=False)
    _modify_index: int = 0

    def __str__(self) -> str:
        what = []
        if self.reason:
            what.append(f"set secret ({self.reason}) → {self.user.namespace}/{self.user.variable}")
        if self.policy:
            what.append(f"{self.policy} policy {self.user.policy}")
        if self.attach:
            what.append("attach policy")
        return f"user     {self.user.user}: {', '.join(what)}"


@dataclass
class StorageDiff:
    buckets: list[BucketChange] = field(default_factory=list)
    users: list[UserChange] = field(default_factory=list)
    stale_buckets: list[StaleBucket] = field(default_factory=list)
    stale_users: list[str] = field(default_factory=list)
    stale_policies: list[str] = field(default_factory=list)
    stale_variables: list[tuple[str, str]] = field(default_factory=list)  # (namespace, path)

    @property
    def changes(self) -> int:
        return len(self.buckets) + len(self.users)

    @property
    def prunable(self) -> int:
        return len(self.stale_users) + len(self.stale_policies) + len(self.stale_variables)


def _users(st: Storage) -> dict[str, dict]:
    return json.loads(st.admin.user_list() or "{}") or {}


def _policies(st: Storage) -> dict[str, dict]:
    return json.loads(st.admin.policy_list() or "{}") or {}


def _attached(info: dict) -> set[str]:
    names = info.get("policyName") or ""
    return {p for p in names.split(",") if p}


def diff(
    st: Storage,
    nomad: Nomad,
    p: StoragePlan,
    services: list[str],
    rotate: bool = False,
) -> StorageDiff:
    d = StorageDiff()
    existing = {b.name for b in st.s3.list_buckets()}
    for b in p.buckets:
        ch = _diff_bucket(st, b, b.name in existing)
        if ch is not None:
            d.buckets.append(ch)
    bucket_prefix = "" if p.env == naming.PROD else f"{p.env}-"
    planned_buckets = {b.name for b in p.buckets}
    for name in sorted(existing - planned_buckets):
        if _owner(name, bucket_prefix, services):
            d.stale_buckets.append(_stale_bucket(st, name))

    users = _users(st)
    policies = _policies(st)
    for u in p.users:
        var = nomad.var_get(u.variable, u.namespace)
        items = dict((var or {}).get("Items") or {})
        has_var = bool(items.get(SECRET_KEY)) and items.get(ACCESS_KEY) == u.user
        has_user = u.user in users
        ch = UserChange(u, _items=items, _modify_index=int((var or {}).get("ModifyIndex") or 0))
        if rotate and has_var and has_user:
            ch.reason = "rotate"
        elif not has_var and not has_user:
            ch.reason = "new"
        elif not has_user:
            ch.reason = "user missing"
        elif not has_var:
            ch.reason = "variable missing"
        want = u.policy_document()
        have = policies.get(u.policy)
        if have is None:
            ch.policy = "create"
        elif _normal(have) != _normal(want):
            ch.policy = "update"
        attached = _attached(users.get(u.user) or {})
        ch.attach = u.policy not in attached
        if ch.reason or ch.policy or ch.attach:
            d.users.append(ch)

    prefix = "" if p.env == naming.PROD else f"{p.env}."
    managed = tuple(f"{prefix}{s}." for s in services)
    planned_users = {u.user for u in p.users}
    d.stale_users = sorted(n for n in users if n.startswith(managed) and n not in planned_users)
    planned_policies = {u.policy for u in p.users}
    policy_prefixes = tuple(f"{naming.STORAGE_POLICY_PREFIX}{m}" for m in managed)
    d.stale_policies = sorted(
        n for n in policies if n.startswith(policy_prefixes) and n not in planned_policies
    )

    namespace = naming.nomad_namespace(p.env)
    planned_paths = {u.variable for u in p.users}
    for s in services:
        for path in nomad.var_list(f"nomad/jobs/{s}-", namespace):
            if path in planned_paths:
                continue
            var = nomad.var_get(path, namespace) or {}
            if SECRET_KEY in (var.get("Items") or {}):
                d.stale_variables.append((namespace, path))
    return d


def _normal(policy: dict) -> str:
    """A policy document in a comparable form (MinIO may reorder or add fields)."""

    def norm(v):
        if isinstance(v, dict):
            return {k: norm(x) for k, x in sorted(v.items())}
        if isinstance(v, list):
            return sorted((norm(x) for x in v), key=lambda x: json.dumps(x, sort_keys=True))
        return v

    statements = [
        {k: (x if isinstance(x, list) else [x]) for k, x in s.items() if k != "Sid"}
        for s in policy.get("Statement", [])
    ]
    return json.dumps(norm(statements), sort_keys=True)


def apply(st: Storage, nomad: Nomad, d: StorageDiff, prune: bool = False) -> None:
    for ch in d.buckets:
        _apply_bucket(st, ch)
    for ch in d.users:
        u = ch.user
        if ch.policy:
            st.admin.policy_add(u.policy, policy=u.policy_document())
        if ch.reason:
            secret = new_password()
            st.admin.user_add(u.user, secret)  # creates the user, or sets a new secret
            items = dict(ch._items) | {ACCESS_KEY: u.user, SECRET_KEY: secret}
            nomad.var_put(u.variable, u.namespace, items, ch._modify_index)
        if ch.attach:
            try:
                st.admin.attach_policy([u.policy], user=u.user)
            except MinioAdminException as e:
                if "AlreadyApplied" not in str(e):
                    raise
    if not prune:
        return
    for name in d.stale_users:
        st.admin.user_remove(name)
    for name in d.stale_policies:
        try:
            st.admin.policy_remove(name)
        except MinioAdminException as e:
            if not _admin_missing(e):
                raise
    for namespace, path in d.stale_variables:
        var = nomad.var_get(path, namespace)
        if var is None:
            continue
        items = {
            k: v for k, v in (var.get("Items") or {}).items() if k not in (ACCESS_KEY, SECRET_KEY)
        }
        if items:
            nomad.var_put(path, namespace, items, int(var["ModifyIndex"]))
        else:
            nomad.var_delete(path, namespace)
