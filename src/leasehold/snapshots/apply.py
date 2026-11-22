"""Compare the snapshot plan with ClickHouse and apply it (part of the ClickHouse step).

- **``__snapshots`` tables** hold data: created when missing; a changed schema file
  skips the dataset with a warning until the table is changed by hand and
  ``--accept-table <database.table>`` records it, as for sinks. A table of that name
  without the ``leasehold:`` mark is never touched.
- **Views** hold no data: created when missing, dropped and created again when their
  definition changed (``CREATE OR REPLACE`` leaves the definer user with dependencies
  ClickHouse doesn't clear). Readers see an error for that moment.
- **``snapshot_loads``** is created once and never changed or dropped.
- **Named collection**: created when missing, altered when the bucket's URL or the
  ClickHouse storage keys changed. ClickHouse doesn't show a collection's secrets, so
  the fingerprint of what was set is kept in the keys' Nomad Variable.
- **Definer and loader users**: created when missing; the loader's password in its job's
  variable (``LEASEHOLD_CLICKHOUSE_USER`` / ``_PASSWORD``), its grants made to match.

Stale: loader users in the ``leasehold_loader`` profile of the given services that the
plan doesn't have; dropped only with ``prune``. Data tables are never dropped.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from leasehold.clickhouse.apply import ClickHouse
from leasehold.clickhouse.plan import MARK, _quote
from leasehold.credentials import new_password
from leasehold.naming import SNAPSHOT_STAGING_PREFIX
from leasehold.nomad.apply import Nomad
from leasehold.snapshots.plan import (
    LOADER_LIMITS,
    LOADER_PROFILE,
    DatasetPlan,
    Grant,
    ServicePlan,
    TablePlan,
    collection_fingerprint,
    collection_url,
    ident,
)
from leasehold.storage.plan import ACCESS_KEY, SECRET_KEY

USER_KEY = "LEASEHOLD_CLICKHOUSE_USER"
PASSWORD_KEY = "LEASEHOLD_CLICKHOUSE_PASSWORD"


def _fp_key(collection: str) -> str:
    return f"clickhouse_collection_{collection}"


@dataclass
class DatasetChange:
    dataset: DatasetPlan
    create_tables: list[TablePlan] = field(default_factory=list)
    accept_tables: list[TablePlan] = field(default_factory=list)
    views: list[TablePlan] = field(default_factory=list)  # create or recreate
    collection: str | None = None  # "create" | "update"
    user_create: bool = False
    user_password: bool = False
    grant: list[Grant] = field(default_factory=list)
    revoke: list[Grant] = field(default_factory=list)
    _items: dict[str, str] = field(default_factory=dict, repr=False)
    _modify_index: int = 0

    def lines(self) -> list[str]:
        d = self.dataset
        out = [f"table    {t.storage_table}: create" for t in self.create_tables]
        out += [f"table    {t.storage_table}: accept changed table" for t in self.accept_tables]
        out += [f"view     {t.view}: create" for t in self.views]
        if self.collection:
            out.append(f"named collection {d.collection}: {self.collection}")
        what = (["create"] if self.user_create else []) + (
            ["new password"] if self.user_password else []
        )
        what += [f"grant {g.access} ON {g.on()}" for g in self.grant]
        what += [f"revoke {g.access} ON {g.on()}" for g in self.revoke]
        if what:
            out.append(f"user     {d.loader_user}: {', '.join(what)}")
        return out

    @property
    def empty(self) -> bool:
        return not self.lines()


@dataclass
class ServiceChange:
    service: ServicePlan
    loads_table: bool = False
    current_view: bool = False
    definer_create: bool = False
    definer_grant: bool = False
    datasets: list[DatasetChange] = field(default_factory=list)
    _storage_items: dict[str, str] = field(default_factory=dict, repr=False)
    _storage_index: int = 0

    def lines(self) -> list[str]:
        s = self.service
        out = []
        if self.loads_table:
            out.append(f"table    {s.database}.snapshot_loads: create")
        if self.current_view:
            out.append(f"view     {s.database}.snapshot_current: create")
        if self.definer_create:
            out.append(f"user     {s.definer}: create (definer of the snapshot views)")
        if self.definer_grant:
            out.append(f"user     {s.definer}: grant SELECT ON {s.database}.*")
        for d in self.datasets:
            out += d.lines()
        return out


@dataclass
class SnapshotDiff:
    changes: list[ServiceChange] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    stale_users: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return sum(len(c.lines()) for c in self.changes)


def _current_grants(ch: ClickHouse, user: str) -> set[tuple]:
    """Grants as ``Grant.row()`` tuples (TSV shows NULL as ``\\N``)."""
    rows = ch.rows(
        "SELECT access_type, database, table, access_object FROM system.grants "
        f"WHERE user_name = {_quote(user)} AND NOT is_partial_revoke"
    )

    def val(v: str) -> str | None:
        return None if v in ("\\N", "") else v

    return {tuple(val(v) for v in (r + [""] * (4 - len(r)))[:4]) for r in rows}


def _profile_users(ch: ClickHouse) -> set[str]:
    rows = ch.rows(
        "SELECT user_name FROM system.settings_profile_elements "
        f"WHERE inherit_profile = {_quote(LOADER_PROFILE)} AND user_name IS NOT NULL"
    )
    return {r[0] for r in rows}


def profile_sql() -> str:
    limits = ", ".join(f"{k} = {v} CONST" for k, v in LOADER_LIMITS.items())
    return f"CREATE SETTINGS PROFILE OR REPLACE {LOADER_PROFILE} SETTINGS {limits}"


def diff(
    ch: ClickHouse,
    nomad: Nomad,
    plans: list[ServicePlan],
    *,
    env: str,
    services: list[str],
    clickhouse_endpoint: str | None,
    busy_tables: tuple[str, ...] = (),
    accept_tables: set[str] = frozenset(),  # type: ignore[assignment]
) -> tuple[SnapshotDiff, bool]:
    """The changes, and whether the loader profile is missing."""
    d = SnapshotDiff()
    existing_users = {r[0] for r in ch.rows("SELECT name FROM system.users")}
    collections = {r[0] for r in ch.rows("SELECT name FROM system.named_collections")}
    profile_missing = not ch.rows(
        f"SELECT 1 FROM system.settings_profiles WHERE name = {_quote(LOADER_PROFILE)}"
    )
    for sp in plans:
        sc = ServiceChange(sp)
        exists = ch.database_exists(sp.database)
        tables = ch.tables(sp.database) if exists else {}
        sc.loads_table = "snapshot_loads" not in tables
        current = tables.get("snapshot_current")
        sc.current_view = current is None
        if current is not None and current[1] != sp.current_comment():
            d.warnings.append(f"{sp.database}.snapshot_current: changed by hand; left as it is")
        sc.definer_create = sp.definer not in existing_users
        if sc.definer_create:
            sc.definer_grant = True
        else:
            want = ("SELECT", sp.database, None, None)
            sc.definer_grant = want not in _current_grants(ch, sp.definer)

        storage_var = nomad.var_get(sp.storage_variable, sp.datasets[0].namespace) or {}
        sc._storage_items = dict(storage_var.get("Items") or {})
        sc._storage_index = int(storage_var.get("ModifyIndex") or 0)

        for dp in sp.datasets:
            dc = DatasetChange(dp)
            skip = False
            for t in dp.tables:
                name = t.storage_table.split(".", 1)[1]
                cur = tables.get(name)
                if cur is None:
                    dc.create_tables.append(t)
                elif not cur[1].startswith(MARK):
                    d.warnings.append(
                        f"{t.storage_table} exists and wasn't created by leasehold; "
                        f"dataset {dp.dataset} skipped"
                    )
                    skip = True
                elif cur[1] != t.storage_comment():
                    if t.storage_table in accept_tables:
                        dc.accept_tables.append(t)
                    else:
                        d.warnings.append(
                            f"{t.storage_table}: the schema file's CREATE TABLE changed. ALTER "
                            f"the table to match, then run with --accept-table "
                            f"{t.storage_table}; dataset {dp.dataset} skipped"
                        )
                        skip = True
                view = tables.get(t.name)
                if view is None or view[1] != t.view_comment(sp.definer):
                    if view is not None and not view[1].startswith(MARK):
                        d.warnings.append(
                            f"{t.view} exists and wasn't created by leasehold; "
                            f"dataset {dp.dataset} skipped"
                        )
                        skip = True
                    else:
                        dc.views.append(t)
            if skip:
                d.skipped.append(f"{sp.service}.{dp.dataset}")
                continue

            if not clickhouse_endpoint:
                d.warnings.append(
                    f"{sp.service}.{dp.dataset}: no storage endpoint for ClickHouse "
                    "(storage.clickhouse_endpoint); named collection left out"
                )
            else:
                items = sc._storage_items
                key, secret = items.get(ACCESS_KEY), items.get(SECRET_KEY)
                url = collection_url(clickhouse_endpoint, dp.bucket)
                if not key or not secret:
                    dc.collection = "create" if dp.collection not in collections else "update"
                    dc.collection += " (once the storage step has made the keys)"
                else:
                    fp = collection_fingerprint(dp.collection, url, key, secret)
                    if dp.collection not in collections:
                        dc.collection = "create"
                    elif items.get(_fp_key(dp.collection)) != fp:
                        dc.collection = "update"

            var = nomad.var_get(dp.loader_variable, dp.namespace)
            items = dict((var or {}).get("Items") or {})
            dc._items, dc._modify_index = items, int((var or {}).get("ModifyIndex") or 0)
            dc.user_create = dp.loader_user not in existing_users
            dc.user_password = not items.get(PASSWORD_KEY) or items.get(USER_KEY) != dp.loader_user
            want = {g.row(): g for g in dp.grants(busy_tables)}
            have = set() if dc.user_create else _current_grants(ch, dp.loader_user)
            dc.grant = sorted(g for r, g in want.items() if r not in have)
            dc.revoke = sorted(_grant_of(r) for r in have if r not in want)
            if not dc.empty:
                sc.datasets.append(dc)
        if sc.lines():
            d.changes.append(sc)

    planned = {dp.loader_user for sp in plans for dp in sp.datasets}
    prefix = "" if env == "prod" else f"{env}."
    managed = tuple(f"{prefix}{s}." for s in services)
    d.stale_users = sorted(
        u for u in _profile_users(ch) if u.startswith(managed) and u not in planned
    )
    return d, profile_missing


def _grant_of(row: tuple) -> Grant:
    access, db, table, obj = row
    if table == SNAPSHOT_STAGING_PREFIX:  # a prefix grant, listed without its "*"
        table = f"{SNAPSHOT_STAGING_PREFIX}*"
    return Grant(access, db, table, obj)


def apply(
    ch: ClickHouse,
    nomad: Nomad,
    d: SnapshotDiff,
    profile_missing: bool,
    *,
    clickhouse_endpoint: str | None,
    prune: bool = False,
) -> None:
    if profile_missing:
        ch.execute(profile_sql())
    for sc in d.changes:
        sp = sc.service
        comment = _quote(f"{MARK} service={sp.service}")
        ch.execute(f"CREATE DATABASE IF NOT EXISTS {sp.database} COMMENT {comment}")
        if sc.loads_table:
            ch.execute(sp.loads_sql())
        if sc.current_view:
            ch.execute(sp.current_sql())
        if sc.definer_create:
            ch.execute(
                f"CREATE USER IF NOT EXISTS {ident(sp.definer)} "
                "IDENTIFIED WITH no_password HOST NONE"
            )
        if sc.definer_grant:
            ch.execute(f"GRANT SELECT ON {ident(sp.database)}.* TO {ident(sp.definer)}")
        storage_items = dict(sc._storage_items)
        storage_changed = False
        for dc in sc.datasets:
            dp = dc.dataset
            for t in dc.create_tables:
                ch.execute(t.create_storage_sql())
            for t in dc.accept_tables:
                ch.execute(
                    f"ALTER TABLE {t.storage_table} MODIFY COMMENT {_quote(t.storage_comment())}"
                )
            for t in dc.views:
                ch.execute(f"DROP VIEW IF EXISTS {t.view} SYNC")
                ch.execute(t.create_view_sql(sp.definer))
            if dc.collection and clickhouse_endpoint:
                key, secret = storage_items.get(ACCESS_KEY), storage_items.get(SECRET_KEY)
                if key and secret:
                    url = collection_url(clickhouse_endpoint, dp.bucket)
                    kv = (
                        f"url = {_quote(url)} NOT OVERRIDABLE, "
                        f"access_key_id = {_quote(key)} NOT OVERRIDABLE, "
                        f"secret_access_key = {_quote(secret)} NOT OVERRIDABLE"
                    )
                    verb = "CREATE" if dc.collection.startswith("create") else "ALTER"
                    if verb == "CREATE":
                        ch.execute(
                            f"CREATE NAMED COLLECTION {ident(dp.collection)} AS {kv}", secret=secret
                        )
                    else:
                        ch.execute(
                            f"ALTER NAMED COLLECTION {ident(dp.collection)} SET {kv}", secret=secret
                        )
                    storage_items[_fp_key(dp.collection)] = collection_fingerprint(
                        dp.collection, url, key, secret
                    )
                    storage_changed = True
            _apply_user(ch, nomad, dc)
        if storage_changed:
            nomad.var_put(
                sp.storage_variable, sp.datasets[0].namespace, storage_items, sc._storage_index
            )
    if prune:
        for name in d.stale_users:
            ch.execute(f"DROP USER IF EXISTS {ident(name)}")


def _apply_user(ch: ClickHouse, nomad: Nomad, dc: DatasetChange) -> None:
    dp = dc.dataset
    user = ident(dp.loader_user)
    items = None
    if dc.user_password or dc.user_create:
        if dc.user_password:
            password = new_password()
            items = dict(dc._items) | {USER_KEY: dp.loader_user, PASSWORD_KEY: password}
        else:
            password = dc._items[PASSWORD_KEY]
        auth = f"IDENTIFIED WITH sha256_password BY {_quote(password)}"
        if dc.user_create:
            ch.execute(
                f"CREATE USER {user} {auth} SETTINGS PROFILE {_quote(LOADER_PROFILE)}",
                secret=password,
            )
        else:
            ch.execute(f"ALTER USER {user} {auth}", secret=password)
        if items is not None:
            nomad.var_put(dp.loader_variable, dp.namespace, items, dc._modify_index)
    for g in dc.revoke:
        ch.execute(f"REVOKE {g.access} ON {g.on()} FROM {user}")
    for g in dc.grant:
        ch.execute(f"GRANT {g.access} ON {g.on()} TO {user}")
