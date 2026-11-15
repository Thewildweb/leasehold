"""ClickHouse users for planners and other scheduled readers: read-only, with fixed
limits, grants from ``reads``.

Per planner component (or scheduled component with ``reads``), one user
(``naming.planner_clickhouse_user``, e.g. ``dev.dns.plan-new-nl``):

- in the settings profile ``leasehold_planner``: ``readonly = 2`` (reads only; the
  client may still set formats) and ``CONST`` limits on query time, memory and threads,
  so a bad planner query can't slow ClickHouse down for everything else;
- ``SELECT`` on its own service's database (its sinks and plan log) and on each table in
  ``planner.reads`` of another service, in the environment's databases. Grants the plan
  doesn't have any more are revoked;
- its password in the job's Nomad Variable (``nomad/jobs/<job>``, keys
  ``LEASEHOLD_CLICKHOUSE_USER`` and ``LEASEHOLD_CLICKHOUSE_PASSWORD``), next to the Kafka
  credentials. A missing password is generated; a user missing in ClickHouse is created
  with the stored password.

Stale: users in the profile, of the given services and environment, that the plan no
longer has. Reported; dropped only with ``prune``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from leasehold import naming
from leasehold.clickhouse.apply import ClickHouse
from leasehold.credentials import new_password
from leasehold.nomad.apply import Nomad
from leasehold.nomad.render import job_variable_path
from leasehold.spec.models import ServiceSpec

PROFILE = "leasehold_planner"
USER_KEY = "LEASEHOLD_CLICKHOUSE_USER"
PASSWORD_KEY = "LEASEHOLD_CLICKHOUSE_PASSWORD"

# Limits for every planner query. A planner that needs more should select less per run.
LIMITS = {
    "max_execution_time": "300",
    "max_memory_usage": str(8 * 1024**3),
    "max_threads": "8",
}


def _q(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


@dataclass(frozen=True)
class PlannerUser:
    service: str
    component: str
    user: str
    namespace: str
    variable: str
    # (database, table); table "*" is the whole database
    grants: frozenset[tuple[str, str]]


def plan(
    specs: list[ServiceSpec], env: str, reads_from: dict[str, str] | None = None
) -> list[PlannerUser]:
    """``reads_from``: service -> environment whose tables readers of it are granted."""
    reads_from = reads_from or {}
    databases = {s.name: s.clickhouse.database for s in specs if s.clickhouse}
    out = []
    for spec in specs:
        for cname, reads in spec.clickhouse_readers().items():
            own = naming.clickhouse_database(env, databases[spec.name])
            grants = {(own, "*")}
            for ref in reads:
                service, table = ref.split(".", 1)
                if service == spec.name:
                    continue
                db = databases.get(service)
                if db is None:
                    raise ValueError(
                        f"{spec.name}.{cname} reads {ref}: service {service!r} isn't in the set"
                    )
                grants.add((naming.clickhouse_database(reads_from.get(service, env), db), table))
            out.append(
                PlannerUser(
                    service=spec.name,
                    component=cname,
                    user=naming.planner_clickhouse_user(env, spec.name, cname),
                    namespace=naming.nomad_namespace(env),
                    variable=job_variable_path(naming.nomad_job(spec.name, cname)),
                    grants=frozenset(grants),
                )
            )
    return sorted(out, key=lambda u: u.user)


@dataclass
class UserChange:
    user: PlannerUser
    create: bool = False
    new_password: bool = False
    grant: list[tuple[str, str]] = field(default_factory=list)
    revoke: list[tuple[str, str]] = field(default_factory=list)
    _items: dict[str, str] = field(default_factory=dict, repr=False)
    _modify_index: int = 0

    def __str__(self) -> str:
        what = []
        if self.create:
            what.append("create")
        if self.new_password:
            what.append("new password")
        what += [f"grant SELECT {d}.{t}" for d, t in self.grant]
        what += [f"revoke SELECT {d}.{t}" for d, t in self.revoke]
        return f"user     {self.user.user}: {', '.join(what)}"


@dataclass
class UsersDiff:
    profile_missing: bool = False
    changes: list[UserChange] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not (self.profile_missing or self.changes)


def _profile_sql() -> str:
    limits = ", ".join(f"{k} = {v} CONST" for k, v in LIMITS.items())
    return f"CREATE SETTINGS PROFILE OR REPLACE {PROFILE} SETTINGS readonly = 2, {limits}"


def _current_grants(ch: ClickHouse, user: str) -> set[tuple[str, str]]:
    rows = ch.rows(
        "SELECT ifNull(database, '*'), ifNull(table, '*') FROM system.grants "
        f"WHERE user_name = {_q(user)} AND access_type = 'SELECT' AND NOT is_partial_revoke"
    )
    return {(r[0], r[1]) for r in rows}


def _profile_users(ch: ClickHouse) -> set[str]:
    rows = ch.rows(
        "SELECT user_name FROM system.settings_profile_elements "
        f"WHERE inherit_profile = {_q(PROFILE)} AND user_name IS NOT NULL"
    )
    return {r[0] for r in rows}


def diff(
    ch: ClickHouse, nomad: Nomad, users: list[PlannerUser], env: str, services: list[str]
) -> UsersDiff:
    d = UsersDiff()
    d.profile_missing = not ch.rows(
        f"SELECT 1 FROM system.settings_profiles WHERE name = {_q(PROFILE)}"
    )
    existing = {r[0] for r in ch.rows("SELECT name FROM system.users")}
    for u in users:
        var = nomad.var_get(u.variable, u.namespace)
        items = dict((var or {}).get("Items") or {})
        c = UserChange(u, _items=items, _modify_index=int((var or {}).get("ModifyIndex") or 0))
        c.create = u.user not in existing
        c.new_password = not items.get(PASSWORD_KEY) or items.get(USER_KEY) != u.user
        have = set() if c.create else _current_grants(ch, u.user)
        c.grant = sorted(u.grants - have)
        c.revoke = sorted(have - u.grants)
        if c.create or c.new_password or c.grant or c.revoke:
            d.changes.append(c)

    planned = {u.user for u in users}
    prefix = "" if env == naming.PROD else f"{env}."
    managed = tuple(f"{prefix}{s}." for s in services)
    d.stale = sorted(u for u in _profile_users(ch) if u.startswith(managed) and u not in planned)
    return d


def _on(db: str, table: str) -> str:
    return f"{_ident(db)}.*" if table == "*" else f"{_ident(db)}.{_ident(table)}"


def apply(ch: ClickHouse, nomad: Nomad, d: UsersDiff, prune: bool = False) -> None:
    if d.profile_missing:
        ch.execute(_profile_sql())
    for c in d.changes:
        u = c.user
        if c.new_password:
            password = new_password()
            items = dict(c._items) | {USER_KEY: u.user, PASSWORD_KEY: password}
        else:
            password = c._items[PASSWORD_KEY]
            items = None
        auth = f"IDENTIFIED WITH sha256_password BY {_q(password)}"
        if c.create:
            ch.execute(
                f"CREATE USER {_ident(u.user)} {auth} SETTINGS PROFILE {_q(PROFILE)}",
                secret=password,
            )
        elif c.new_password:
            ch.execute(f"ALTER USER {_ident(u.user)} {auth}", secret=password)
        if items is not None:
            # After ClickHouse accepted it: a job restarted by the variable change
            # finds a working password.
            nomad.var_put(u.variable, u.namespace, items, c._modify_index)
        for db, table in c.grant:
            ch.execute(f"GRANT SELECT ON {_on(db, table)} TO {_ident(u.user)}")
        for db, table in c.revoke:
            ch.execute(f"REVOKE SELECT ON {_on(db, table)} FROM {_ident(u.user)}")
    if prune:
        for name in d.stale:
            ch.execute(f"DROP USER IF EXISTS {_ident(name)}")
