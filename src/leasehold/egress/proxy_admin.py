"""Changing proxy accounts, pools and fixed exits; worker reports; the audit trail.

Everything here runs in the control API (async). The CLI (``leasehold proxy ...``) calls
the admin endpoints, so only the API ever holds the secrets key. Proxy rows are shared by
all environments; operators change them through the schema owner's API (prod), while
every environment's API records what its workers report: bytes, proxy errors, connect
failures (``record_*``).

Every change writes a row to ``egress.proxy_audit``: who (the token's name, or
``control-api`` for automatic changes), the action, the object, and the changed fields
with old and new values. A credentials change records only fingerprints.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from leasehold.egress import proxies as px
from leasehold.egress import secrets as sec

log = logging.getLogger("leasehold.egress.proxies")

SYSTEM = "control-api"

ACCOUNT_FIELDS = {
    "vendor": str,
    "adapter": str,
    "adapter_config": dict,
    "cap_gb": float,
    "cap_period": str,
    "period_anchor": int,
    "paid_until": date,
    "concurrency": int,
    "envs": list,
    "warn_remaining_gb": float,
}
POOL_FIELDS = (
    "kind",
    "endpoint",
    "username_template",
    "sticky",
    "max_sticky_ttl_s",
    "countries",
    "cost_per_gb",
    "concurrency",
    "egress_weight",
    "exit_ip_header",
    "task_types",
)
EXIT_FIELDS = ("country", "task_types", "kind", "asn", "subnet")


def _require_limit(account: dict) -> None:
    """An account in use needs a limit: our own ``cap_gb``, or one the vendor's usage
    API reported. Without one its remaining share is unknown, so no ``stop_at_used``
    triggers and no quota warning fires."""
    if account.get("cap_gb") is not None:
        return
    if account.get("vendor_limit_bytes") is not None or account.get("vendor_remaining_bytes"):
        return
    raise Conflict(
        f"proxy account {account['name']!r} has no limit: set cap_gb (an account without a "
        "usage API, or before its first poll), or wait for the usage poll to report one"
    )


class ProxyAdminError(ValueError):
    """A change that can't be made (unknown name, conflict, bad value): HTTP 4xx."""

    def __init__(self, message: str, status: int = 422):
        super().__init__(message)
        self.status = status


class NotFound(ProxyAdminError):
    def __init__(self, message: str):
        super().__init__(message, 404)


class Conflict(ProxyAdminError):
    def __init__(self, message: str):
        super().__init__(message, 409)


def _jsonable(v: Any) -> Any:
    if isinstance(v, datetime | date):
        return v.isoformat()
    if isinstance(v, bytes | memoryview):
        return None
    if isinstance(v, int | float | str | bool | list | dict) or v is None:
        return v
    return str(v)


class ProxyAdmin:
    def __init__(self, schema: str, env: str, keyring: sec.Keyring):
        self.schema = schema
        self.env = env
        self.keyring = keyring
        self._e = sql.Identifier(schema)

    def _q(self, stmt: str) -> sql.Composed:
        return sql.SQL(stmt).format(e=self._e)

    async def _rows(self, conn: AsyncConnection, stmt: str, params: Any = None) -> list[dict]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(self._q(stmt), params)
            return await cur.fetchall()

    async def audit(
        self,
        conn: AsyncConnection,
        actor: str,
        action: str,
        object_type: str,
        obj: str,
        changes: dict | None = None,
    ) -> None:
        await conn.execute(
            self._q(
                "INSERT INTO {e}.proxy_audit (actor, env, action, object_type, object, changes) "
                "VALUES (%s, %s, %s, %s, %s, %s)"
            ),
            (
                actor,
                self.env,
                action,
                object_type,
                obj,
                json.dumps({k: _jsonable(v) for k, v in (changes or {}).items()}),
            ),
        )

    # --- accounts -----------------------------------------------------------------------------

    async def account(self, conn: AsyncConnection, name: str, lock: bool = False) -> dict:
        rows = await self._rows(
            conn,
            "SELECT * FROM {e}.proxy_accounts WHERE name = %s AND removed_at IS NULL"
            + (" FOR UPDATE" if lock else ""),
            (name,),
        )
        if not rows:
            raise NotFound(f"no proxy account {name!r}")
        return rows[0]

    async def add_account(self, conn: AsyncConnection, actor: str, name: str, **fields) -> None:
        fields = _check_account_fields(fields)
        fields.setdefault("vendor", name)
        async with conn.transaction():
            have = await self._rows(
                conn, "SELECT removed_at FROM {e}.proxy_accounts WHERE name = %s", (name,)
            )
            if have:
                raise Conflict(f"proxy account {name!r} exists")
            cols = ["name", *fields]
            await conn.execute(
                sql.SQL("INSERT INTO {e}.proxy_accounts ({cols}) VALUES ({vals})").format(
                    e=self._e,
                    cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
                    vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
                ),
                [name, *(_db(v) for v in fields.values())],
            )
            await self.audit(conn, actor, "add", "account", name, fields)

    async def update_account(self, conn: AsyncConnection, actor: str, name: str, **fields) -> dict:
        fields = _check_account_fields(fields, update=True)
        async with conn.transaction():
            have = await self.account(conn, name, lock=True)
            changed = {k: v for k, v in fields.items() if _norm(have[k]) != _norm(v)}
            if have["status"] != "disabled":
                _require_limit({**have, **changed})
            if changed:
                await self._update(conn, "proxy_accounts", "name", name, changed)
                await self.audit(
                    conn,
                    actor,
                    "update",
                    "account",
                    name,
                    {k: [_jsonable(have[k]), _jsonable(v)] for k, v in changed.items()},
                )
            return changed

    async def set_credentials(
        self, conn: AsyncConnection, actor: str, name: str, username: str, password: str
    ) -> str:
        """Store the proxy credentials, encrypted. Returns the new fingerprint."""
        if not username or not password:
            raise ProxyAdminError("username and password are required")
        plain = px.encode_credentials(username, password)
        sealed = self.keyring.seal(plain, sec.context("proxy_accounts", name, "credentials"))
        async with conn.transaction():
            have = await self.account(conn, name, lock=True)
            await conn.execute(
                self._q(
                    "UPDATE {e}.proxy_accounts SET credentials = %s, key_id = %s, "
                    "credentials_fp = %s, credentials_updated_at = now(), updated_at = now() "
                    "WHERE name = %s"
                ),
                (sealed.ciphertext, sealed.key_id, sealed.fingerprint, name),
            )
            await self.audit(
                conn,
                actor,
                "set-credentials",
                "account",
                name,
                {"credentials_fp": [have["credentials_fp"], sealed.fingerprint]},
            )
        return sealed.fingerprint

    async def set_usage_key(self, conn: AsyncConnection, actor: str, name: str, key: bytes) -> str:
        if not key:
            raise ProxyAdminError("the usage key is empty")
        sealed = self.keyring.seal(key, sec.context("proxy_accounts", name, "usage_credentials"))
        async with conn.transaction():
            have = await self.account(conn, name, lock=True)
            await conn.execute(
                self._q(
                    "UPDATE {e}.proxy_accounts SET usage_credentials = %s, usage_key_id = %s, "
                    "usage_fp = %s, usage_updated_at = now(), updated_at = now() "
                    "WHERE name = %s"
                ),
                (sealed.ciphertext, sealed.key_id, sealed.fingerprint, name),
            )
            await self.audit(
                conn,
                actor,
                "set-usage-key",
                "account",
                name,
                {"usage_fp": [have["usage_fp"], sealed.fingerprint]},
            )
        return sealed.fingerprint

    async def account_status(self, conn: AsyncConnection, actor: str, name: str, action: str):
        """drain, disable, or enable (``active``; also sets ``state = 'ok'`` without a
        probe: the operator's call)."""
        status = {"drain": "drain", "disable": "disabled", "enable": "active"}.get(action)
        if status is None:
            raise ProxyAdminError(f"unknown action {action!r}")
        async with conn.transaction():
            have = await self.account(conn, name, lock=True)
            if status != "disabled":
                _require_limit(have)
            changes: dict[str, Any] = {}
            if have["status"] != status:
                changes["status"] = [have["status"], status]
            if action == "enable" and have["state"] != "ok":
                changes["state"] = [have["state"], "ok"]
            await conn.execute(
                self._q(
                    "UPDATE {e}.proxy_accounts SET status = %s, updated_at = now() WHERE name = %s"
                ),
                (status, name),
            )
            if action == "enable" and have["state"] != "ok":
                await self.set_state(conn, name, "ok", f"enabled by {actor}", actor=None)
            await self.audit(conn, actor, action, "account", name, changes)
            return changes

    async def set_state(
        self,
        conn: AsyncConnection,
        name: str,
        state: str,
        reason: str,
        actor: str | None = SYSTEM,
        only_from: tuple[str, ...] | None = None,
        reason_prefix: str | None = None,
        unless_reason: tuple[str, ...] = (),
    ) -> bool:
        """Set an account's state (with its evidence). ``only_from``: only when the
        current state is one of these; ``reason_prefix``: only when the current reason
        starts with it; ``unless_reason``: not when it starts with one of these. Returns
        whether it changed."""
        assert state in px.STATES
        cond = ""
        params: list[Any] = [state, reason[:500], name, state]
        if only_from is not None:
            cond += " AND a.state = ANY(%s)"
            params.append(list(only_from))
        if reason_prefix is not None:
            cond += " AND starts_with(coalesce(a.state_reason, ''), %s)"
            params.append(reason_prefix)
        for p in unless_reason:
            cond += " AND NOT starts_with(coalesce(a.state_reason, ''), %s)"
            params.append(p)
        rows = await self._rows(
            conn,
            "UPDATE {e}.proxy_accounts a SET state = %s, state_reason = %s, "
            "state_since = now(), updated_at = now() "
            "FROM (SELECT name, state AS old FROM {e}.proxy_accounts WHERE name = %s "
            "FOR UPDATE) o WHERE a.name = o.name AND a.state <> %s" + cond + " RETURNING o.old",
            params,
        )
        if not rows:
            return False
        log.warning(
            "proxy account state changed",
            extra={
                "fields": {
                    "account": name,
                    "from": rows[0]["old"],
                    "to": state,
                    "reason": reason[:200],
                }
            },
        )
        if actor is not None:
            await self.audit(
                conn,
                actor,
                "state",
                "account",
                name,
                {"state": [rows[0]["old"], state], "reason": reason[:500]},
            )
        return True

    # --- pools --------------------------------------------------------------------------------

    async def pool(self, conn: AsyncConnection, name: str) -> dict:
        rows = await self._rows(
            conn, "SELECT * FROM {e}.proxy_pools WHERE name = %s AND removed_at IS NULL", (name,)
        )
        if not rows:
            raise NotFound(f"no proxy pool {name!r}")
        return rows[0]

    async def add_pool(
        self, conn: AsyncConnection, actor: str, name: str, account: str, **fields
    ) -> None:
        fields = _check_pool_fields(fields)
        if "endpoint" not in fields or "kind" not in fields:
            raise ProxyAdminError("a pool needs an endpoint and a kind")
        async with conn.transaction():
            await self.account(conn, account)
            if await self._rows(conn, "SELECT 1 FROM {e}.proxy_pools WHERE name = %s", (name,)):
                raise Conflict(f"proxy pool {name!r} exists")
            cols = ["name", "account", *fields]
            await conn.execute(
                sql.SQL("INSERT INTO {e}.proxy_pools ({cols}) VALUES ({vals})").format(
                    e=self._e,
                    cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
                    vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
                ),
                [name, account, *(_db(v) for v in fields.values())],
            )
            await self.audit(conn, actor, "add", "pool", name, {"account": account, **fields})

    async def update_pool(self, conn: AsyncConnection, actor: str, name: str, **fields) -> dict:
        fields = _check_pool_fields(fields)
        async with conn.transaction():
            have = await self.pool(conn, name)
            changed = {k: v for k, v in fields.items() if _norm(have[k]) != _norm(v)}
            if changed:
                await self._update(conn, "proxy_pools", "name", name, changed)
                await self.audit(
                    conn,
                    actor,
                    "update",
                    "pool",
                    name,
                    {k: [_jsonable(have[k]), _jsonable(v)] for k, v in changed.items()},
                )
            return changed

    async def pool_status(self, conn: AsyncConnection, actor: str, name: str, action: str):
        async with conn.transaction():
            have = await self.pool(conn, name)
            if action == "remove":
                await conn.execute(
                    self._q(
                        "UPDATE {e}.proxy_pools SET status = 'disabled', removed_at = now(), "
                        "updated_at = now() WHERE name = %s"
                    ),
                    (name,),
                )
                await self._revoke(conn, px.pool_key(name), "pool removed")
            else:
                status = {"drain": "drain", "disable": "disabled", "enable": "active"}.get(action)
                if status is None:
                    raise ProxyAdminError(f"unknown action {action!r}")
                await conn.execute(
                    self._q(
                        "UPDATE {e}.proxy_pools SET status = %s, override_status = NULL, "
                        "override_reason = NULL, consecutive_failures = 0, updated_at = now() "
                        "WHERE name = %s"
                    ),
                    (status, name),
                )
                if status == "disabled":
                    await self._revoke(conn, px.pool_key(name), "pool disabled")
            await self.audit(
                conn,
                actor,
                action,
                "pool",
                name,
                {"status": have["status"], "override_status": have["override_status"]},
            )

    # --- fixed exits --------------------------------------------------------------------------

    async def egress_row(self, conn: AsyncConnection, address: str) -> dict:
        rows = await self._rows(
            conn,
            "SELECT *, host(address) AS addr FROM {e}.egresses "
            "WHERE address = %s AND removed_at IS NULL",
            (address,),
        )
        if not rows:
            raise NotFound(f"no egress {address}")
        return rows[0]

    async def add_exit(
        self,
        conn: AsyncConnection,
        actor: str,
        account: str,
        endpoint: str,
        kind: str = "isp",
        country: str | None = None,
        task_types: list[str] | None = None,
        address: str | None = None,
        subnet: str | None = None,
        asn: int | None = None,
    ) -> int:
        """One endpoint of a fixed exit; with ``address`` the egress row is made at once,
        else the probe learns it. Returns the exit's id."""
        scheme, host, port = px.parse_endpoint(endpoint)
        ep = px.endpoint_text(scheme, host, port)
        if kind not in px.KINDS:
            raise ProxyAdminError(f"kind: one of {', '.join(px.KINDS)}")
        async with conn.transaction():
            await self.account(conn, account)
            if await self._rows(conn, "SELECT 1 FROM {e}.proxy_exits WHERE endpoint = %s", (ep,)):
                raise Conflict(f"exit endpoint {ep} exists")
            rows = await self._rows(
                conn,
                "INSERT INTO {e}.proxy_exits (account, endpoint, kind, country, task_types) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (account, ep, kind, country, task_types),
            )
            exit_id = rows[0]["id"]
            if address:
                await self.attach_address(conn, exit_id, address, subnet, asn, actor=actor)
            await self.audit(
                conn,
                actor,
                "add",
                "exit",
                ep,
                {"account": account, "kind": kind, "country": country, "address": address},
            )
            return exit_id

    async def attach_address(
        self,
        conn: AsyncConnection,
        exit_id: int,
        address: str,
        subnet: str | None,
        asn: int | None,
        actor: str = SYSTEM,
        country: str | None = None,
    ) -> None:
        """Make the egress row for an exit whose address is known. An address that is
        already an egress (a VPS, or another exit) is refused; a removed exit row with
        that address (an exit moving back to an earlier address, A → B → A) is reused,
        since the address is the egress table's key."""
        ex = (await self._rows(conn, "SELECT * FROM {e}.proxy_exits WHERE id = %s", (exit_id,)))[0]
        taken = await self._rows(
            conn,
            "SELECT source, account, exit_id, removed_at FROM {e}.egresses WHERE address = %s "
            "FOR UPDATE",
            (address,),
        )
        revive = False
        if taken:
            t = taken[0]
            if t["exit_id"] == exit_id and t["removed_at"] is None:
                return
            if not (t["source"] == "live" and t["removed_at"] is not None):
                raise Conflict(
                    f"{address} is already an egress ({t['source']}"
                    + (f", account {t['account']}" if t["account"] else "")
                    + ")"
                )
            revive = True
        net = subnet or _slash24(address)
        values = (
            net,
            asn,
            country or ex["country"],
            ex["kind"],
            ex["task_types"],
            ex["account"],
            ex["endpoint"],
            exit_id,
        )
        if revive:
            await conn.execute(
                self._q(
                    """
                    UPDATE {e}.egresses SET subnet = %s, asn = %s, country = %s, kind = %s,
                      status = 'active', task_types = %s, account = %s, endpoint = %s,
                      exit_id = %s, override_status = NULL, override_reason = NULL,
                      consecutive_failures = 0, removed_at = NULL, updated_at = now()
                    WHERE address = %s
                    """
                ),
                (*values, address),
            )
        else:
            await conn.execute(
                self._q(
                    """
                    INSERT INTO {e}.egresses
                      (subnet, asn, country, kind, status, task_types, source, account,
                       endpoint, exit_id, address)
                    VALUES (%s, %s, %s, %s, 'active', %s, 'live', %s, %s, %s, %s)
                    """
                ),
                (*values, address),
            )
        await conn.execute(
            self._q(
                "UPDATE {e}.proxy_exits SET address = %s, address_seen_at = now(), "
                "last_error = NULL, updated_at = now() WHERE id = %s"
            ),
            (address, exit_id),
        )
        await self.audit(
            conn,
            actor,
            "address",
            "exit",
            ex["endpoint"],
            {"address": address, "subnet": net, "asn": asn},
        )

    async def exit_status(self, conn: AsyncConnection, actor: str, address: str, action: str):
        """drain, disable, enable (registry rows too: an override the registry apply keeps),
        or remove (live rows only)."""
        async with conn.transaction():
            row = await self.egress_row(conn, address)
            if action == "remove":
                if row["source"] != "live":
                    raise Conflict(f"{address} is in the registry; remove it from the file")
                await conn.execute(
                    self._q(
                        "UPDATE {e}.egresses SET status = 'disabled', removed_at = now(), "
                        "updated_at = now() WHERE address = %s"
                    ),
                    (address,),
                )
                await conn.execute(
                    self._q(
                        "UPDATE {e}.proxy_exits SET removed_at = now(), updated_at = now() "
                        "WHERE id = %s"
                    ),
                    (row["exit_id"],),
                )
                await self._revoke(conn, address, "exit removed")
            else:
                override = {"drain": "drain", "disable": "disabled", "enable": None}
                if action not in override:
                    raise ProxyAdminError(f"unknown action {action!r}")
                await conn.execute(
                    self._q(
                        "UPDATE {e}.egresses SET override_status = %s, override_reason = %s, "
                        "consecutive_failures = 0, updated_at = now() WHERE address = %s"
                    ),
                    (
                        override[action],
                        f"{action} by {actor}" if override[action] else None,
                        address,
                    ),
                )
                if action == "disable":
                    await self._revoke(conn, address, "egress disabled")
            await self.audit(
                conn,
                actor,
                action,
                "egress",
                address,
                {"override_status": row["override_status"], "source": row["source"]},
            )

    async def update_exit(self, conn: AsyncConnection, actor: str, address: str, **fields):
        bad = set(fields) - set(EXIT_FIELDS)
        if bad:
            raise ProxyAdminError(f"unknown fields: {', '.join(sorted(bad))}")
        async with conn.transaction():
            row = await self.egress_row(conn, address)
            if row["source"] != "live":
                raise Conflict(f"{address} is in the registry; change it in the file")
            changed = {k: v for k, v in fields.items() if _norm(row[k]) != _norm(v)}
            if changed:
                await self._update(conn, "egresses", "address", address, changed)
                await self.audit(
                    conn,
                    actor,
                    "update",
                    "egress",
                    address,
                    {k: [_jsonable(row[k]), _jsonable(v)] for k, v in changed.items()},
                )
            return changed

    async def _revoke(self, conn: AsyncConnection, key: str, reason: str) -> None:
        await conn.execute(
            self._q(
                "UPDATE {e}.target_leases SET revoked_at = now(), revoke_reason = %s "
                "WHERE egress = %s AND expires_at > now() AND revoked_at IS NULL"
            ),
            (reason, key),
        )

    async def _update(self, conn, table: str, key_col: str, key: str, changed: dict) -> None:
        sets = sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in changed)
        await conn.execute(
            sql.SQL("UPDATE {e}.{t} SET {sets}, updated_at = now() WHERE {k} = %s").format(
                e=self._e, t=sql.Identifier(table), sets=sets, k=sql.Identifier(key_col)
            ),
            [*(_db(v) for v in changed.values()), key],
        )

    # --- import -------------------------------------------------------------------------------

    async def import_list(
        self,
        conn: AsyncConnection,
        actor: str,
        account: str,
        kind: str,
        lines: list[ImportLine],
        *,
        vendor: str | None = None,
        adapter: str | None = None,
        country: str | None = None,
        pool: str | None = None,
        countries: list[str] | None = None,
        task_types: list[str] | None = None,
    ) -> ImportResult:
        """Import a vendor's list: ``host:port:username:password`` lines (parsed by the
        caller with ``parse_lines``). One account has one credential pair. Fixed exits get
        one exit per line; ``pool`` makes one pool from a single gateway line. Running it
        again changes only what differs (credentials, new endpoints)."""
        if kind not in px.KINDS:
            raise ProxyAdminError(f"kind: one of {', '.join(px.KINDS)}")
        if not lines:
            raise ProxyAdminError("no lines")
        pairs = {(ln.username, ln.password) for ln in lines}
        if len(pairs) != 1:
            raise ProxyAdminError(
                "the lines have different username/password pairs; one account has one pair, "
                "so split the list per pair"
            )
        if pool is not None and len(lines) != 1:
            raise ProxyAdminError("a pool is one gateway: give one line")
        ((username, password),) = pairs
        result = ImportResult(account=account)
        async with conn.transaction():
            have = await self._rows(
                conn, "SELECT * FROM {e}.proxy_accounts WHERE name = %s", (account,)
            )
            if not have:
                fields: dict[str, Any] = {"vendor": vendor or account}
                if adapter:
                    fields["adapter"] = adapter
                await self.add_account(conn, actor, account, **fields)
                result.account_created = True
                fp = None
            else:
                if have[0]["removed_at"] is not None:
                    raise Conflict(f"proxy account {account!r} was removed")
                fp = have[0]["credentials_fp"]
            if fp != sec.fingerprint(px.encode_credentials(username, password)):
                await self.set_credentials(conn, actor, account, username, password)
                result.credentials_changed = True
            if pool is not None:
                ln = lines[0]
                ep = px.endpoint_text(ln.scheme, ln.host, ln.port)
                existing = await self._rows(
                    conn, "SELECT * FROM {e}.proxy_pools WHERE name = %s", (pool,)
                )
                if not existing:
                    await self.add_pool(
                        conn,
                        actor,
                        pool,
                        account,
                        kind=kind,
                        endpoint=ep,
                        countries=[c.upper() for c in countries or []],
                        task_types=task_types,
                    )
                    result.pools_created.append(pool)
                elif existing[0]["account"] != account:
                    raise Conflict(f"pool {pool!r} belongs to account {existing[0]['account']}")
                elif existing[0]["endpoint"] != ep:
                    await self.update_pool(conn, actor, pool, endpoint=ep)
                    result.pools_updated.append(pool)
            else:
                for ln in lines:
                    ep = px.endpoint_text(ln.scheme, ln.host, ln.port)
                    rows = await self._rows(
                        conn, "SELECT account FROM {e}.proxy_exits WHERE endpoint = %s", (ep,)
                    )
                    if rows:
                        if rows[0]["account"] != account:
                            raise Conflict(
                                f"line {ln.number}: endpoint belongs to account "
                                f"{rows[0]['account']}"
                            )
                        result.exits_existing += 1
                        continue
                    await self.add_exit(
                        conn,
                        actor,
                        account,
                        ep,
                        kind=kind,
                        country=country,
                        task_types=task_types,
                    )
                    result.exits_created += 1
        return result

    # --- listing and rekey --------------------------------------------------------------------

    async def listing(self, conn: AsyncConnection, account: str | None = None) -> dict:
        """Accounts (no credentials), pools and fixed exits."""
        accounts = await px.load_accounts(conn, self.schema)
        out_accounts = []
        for name, a in sorted(accounts.items()):
            if account and name != account:
                continue
            out_accounts.append(account_view(a, detail=True))
        cond = " AND account = %s" if account else ""
        params = (account,) if account else None
        pools = await self._rows(
            conn,
            "SELECT name, account, kind, endpoint, username_template, sticky, max_sticky_ttl_s, "
            "countries, cost_per_gb, concurrency, egress_weight, exit_ip_header, status, "
            "override_status, override_reason, consecutive_failures, task_types "
            "FROM {e}.proxy_pools WHERE removed_at IS NULL" + cond + " ORDER BY name",
            params,
        )
        exits = await self._rows(
            conn,
            "SELECT p.id, p.account, p.endpoint, p.kind, p.country, host(p.address) AS address, "
            "p.address_seen_at, p.last_error, x.status, x.override_status, x.override_reason, "
            "x.consecutive_failures, x.asn, x.subnet::text AS subnet, x.task_types "
            "FROM {e}.proxy_exits p LEFT JOIN {e}.egresses x "
            "ON x.exit_id = p.id AND x.removed_at IS NULL "
            "WHERE p.removed_at IS NULL"
            + cond.replace("account", "p.account")
            + " ORDER BY p.account, p.endpoint",
            params,
        )
        return {
            "accounts": out_accounts,
            "pools": [{k: _jsonable(v) for k, v in p.items()} for p in pools],
            "exits": [{k: _jsonable(v) for k, v in x.items()} for x in exits],
        }

    async def rekey(self, conn: AsyncConnection, actor: str) -> int:
        """Re-encrypt every credential that isn't under the newest key. Returns the count."""
        if not self.keyring.configured:
            raise ProxyAdminError("no secrets key configured")
        n = 0
        async with conn.transaction():
            rows = await self._rows(
                conn,
                "SELECT name, credentials, key_id, usage_credentials, usage_key_id "
                "FROM {e}.proxy_accounts WHERE (credentials IS NOT NULL AND key_id <> %s) "
                "OR (usage_credentials IS NOT NULL AND usage_key_id <> %s) FOR UPDATE",
                (self.keyring.current, self.keyring.current),
            )
            for r in rows:
                for col, kcol in (("credentials", "key_id"), ("usage_credentials", "usage_key_id")):
                    if r[col] is None or r[kcol] == self.keyring.current:
                        continue
                    ctx = sec.context("proxy_accounts", r["name"], col)
                    plain = self.keyring.open(bytes(r[col]), r[kcol], ctx)
                    sealed = self.keyring.seal(plain, ctx)
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {e}.proxy_accounts SET {c} = %s, {k} = %s WHERE name = %s"
                        ).format(e=self._e, c=sql.Identifier(col), k=sql.Identifier(kcol)),
                        (sealed.ciphertext, sealed.key_id, r["name"]),
                    )
                    n += 1
                await self.audit(
                    conn, actor, "rekey", "account", r["name"], {"key_id": self.keyring.current}
                )
        return n

    # --- what workers report ------------------------------------------------------------------

    async def record_usage(self, conn: AsyncConnection, rows: list[UsageRow]) -> None:
        if not rows:
            return
        agg: dict[tuple[str, str, str], list[int]] = {}
        for r in rows:
            a = agg.setdefault((r.account, r.ref, r.task_type), [0, 0])
            a[0] += max(0, r.bytes)
            a[1] += 1
        await conn.execute(
            self._q(
                """
                INSERT INTO {e}.proxy_usage AS u
                  (account, ref, env, task_type, day, bytes, requests)
                SELECT x.account, x.ref, %(env)s, x.task_type, (now() AT TIME ZONE 'UTC')::date,
                       x.bytes, x.requests
                FROM jsonb_to_recordset(%(rows)s::jsonb)
                  AS x(account text, ref text, task_type text, bytes bigint, requests bigint)
                ON CONFLICT (account, day, ref, env, task_type) DO UPDATE SET
                  bytes = u.bytes + EXCLUDED.bytes, requests = u.requests + EXCLUDED.requests
                """
            ),
            {
                "env": self.env,
                "rows": json.dumps(
                    [
                        {"account": a, "ref": ref, "task_type": tt, "bytes": b, "requests": n}
                        for (a, ref, tt), (b, n) in agg.items()
                    ]
                ),
            },
        )

    async def route_states(
        self, conn: AsyncConnection, pairs: set[tuple[str, str]]
    ) -> dict[tuple[str, str], dict]:
        """For (account, egress key) pairs reported with outcomes: whether each is still
        current (neither the exit or pool nor the account removed) and the account's
        credentials fingerprint now. A pair that never existed is missing."""
        if not pairs:
            return {}
        rows = await self._rows(
            conn,
            """
            SELECT x.account, x.key, a.credentials_fp AS fp,
                   x.removed_at IS NULL AND a.removed_at IS NULL AS current
            FROM (SELECT account, host(address) AS key, removed_at FROM {e}.egresses
                  WHERE account IS NOT NULL
                  UNION ALL
                  SELECT account, 'pool:' || name, removed_at FROM {e}.proxy_pools) x
            JOIN {e}.proxy_accounts a ON a.name = x.account
            WHERE x.account = ANY(%s) AND x.key = ANY(%s)
            """,
            (sorted({a for a, _ in pairs}), sorted({k for _, k in pairs})),
        )
        return {(r["account"], r["key"]): r for r in rows if (r["account"], r["key"]) in pairs}

    async def record_success(self, conn: AsyncConnection, account: str, key: str) -> None:
        """A request through this exit or pool worked: its failure count and its gateway
        answers start over."""
        await conn.execute(
            self._q("DELETE FROM {e}.proxy_failures WHERE ref = %s AND kind = 'gateway'"),
            (key,),
        )
        await conn.execute(
            self._q(
                "UPDATE {e}.egresses SET consecutive_failures = 0 "
                "WHERE host(address) = %s AND consecutive_failures > 0"
            ),
            (key,),
        )
        await conn.execute(
            self._q(
                "UPDATE {e}.proxy_pools SET consecutive_failures = 0 "
                "WHERE 'pool:' || name = %s AND consecutive_failures > 0"
            ),
            (key,),
        )

    async def record_proxy_error(
        self,
        conn: AsyncConnection,
        account: str,
        key: str,
        status: int | None,
        excerpt: str,
        *,
        max_failures: int,
        unreachable_refs: int,
        window_s: float,
        kind: str = "connect",
        target: str | None = None,
        gateway_targets: int = 3,
        gateway_window_s: float = 600,
    ) -> str:
        """A proxy refused or failed before the target answered. Returns what it was:
        an account state (the account's error rules matched), ``gateway`` (a 502/503/504
        not counted yet), ``connect`` (counted against the exit or pool), ``disabled``
        (that made it the ``max_failures``-th in a row) or ``unreachable`` (several of the
        account's exits or pools failing together).

        A gateway answer counts as a connect failure only once ``gateway_targets``
        distinct targets got one on this exit or pool within ``gateway_window_s``: one
        dead site retried across exits is the site, not the exits."""
        rows = await self._rows(
            conn, "SELECT adapter_config FROM {e}.proxy_accounts WHERE name = %s", (account,)
        )
        if not rows:
            return "unknown_account"
        state = px.match_error(px.error_rules(rows[0]["adapter_config"]), status, excerpt)
        if state is not None:
            await self.set_state(
                conn,
                account,
                state,
                f"proxy answered {status}: {excerpt[:200]}" if status else excerpt[:200],
            )
            return state
        if kind == "gateway":
            await conn.execute(
                self._q(
                    "INSERT INTO {e}.proxy_failures (account, ref, env, kind, target) "
                    "VALUES (%s, %s, %s, 'gateway', %s)"
                ),
                (account, key, self.env, target),
            )
            sites = await self._rows(
                conn,
                "SELECT count(DISTINCT target) AS n FROM {e}.proxy_failures "
                "WHERE ref = %s AND kind = 'gateway' AND at > now() - %s * interval '1 second'",
                (key, gateway_window_s),
            )
            if not sites or sites[0]["n"] < gateway_targets:
                return "gateway"
        failed = await self._rows(
            conn,
            "UPDATE {e}.egresses SET consecutive_failures = consecutive_failures + 1 "
            "WHERE host(address) = %s RETURNING consecutive_failures AS n",
            (key,),
        ) or await self._rows(
            conn,
            "UPDATE {e}.proxy_pools SET consecutive_failures = consecutive_failures + 1 "
            "WHERE 'pool:' || name = %s RETURNING consecutive_failures AS n",
            (key,),
        )
        await conn.execute(
            self._q("INSERT INTO {e}.proxy_failures (account, ref, env) VALUES (%s, %s, %s)"),
            (account, key, self.env),
        )
        refs = await self._rows(
            conn,
            "SELECT count(DISTINCT ref) AS n FROM {e}.proxy_failures "
            "WHERE account = %s AND kind = 'connect' AND at > now() - %s * interval '1 second'",
            (account, window_s),
        )
        if refs and refs[0]["n"] >= unreachable_refs:
            await self.set_state(
                conn,
                account,
                "unreachable",
                f"connect failures through {refs[0]['n']} exits or pools within {window_s:.0f}s",
                only_from=("ok",),
            )
            return "unreachable"
        if failed and failed[0]["n"] >= max_failures:
            reason = f"{failed[0]['n']} connect failures in a row"
            if px.is_pool(key):
                done = await self._rows(
                    conn,
                    "UPDATE {e}.proxy_pools SET override_status = 'disabled', "
                    "override_reason = %s, updated_at = now() "
                    "WHERE 'pool:' || name = %s AND override_status IS NULL RETURNING name",
                    (reason, key),
                )
            else:
                done = await self._rows(
                    conn,
                    "UPDATE {e}.egresses SET override_status = 'disabled', "
                    "override_reason = %s, updated_at = now() "
                    "WHERE host(address) = %s AND override_status IS NULL RETURNING address",
                    (reason, key),
                )
            if done:
                await self._revoke(conn, key, reason)
                await self.audit(
                    conn,
                    SYSTEM,
                    "auto-disable",
                    "pool" if px.is_pool(key) else "egress",
                    key,
                    {"reason": reason},
                )
                log.warning(
                    "proxy egress disabled", extra={"fields": {"egress": key, "reason": reason}}
                )
                return "disabled"
        return "connect"

    async def clear_failures(self, conn: AsyncConnection, accounts: set[str]) -> None:
        """A success through the account: its connect failures no longer correlate.
        Gateway answers are per exit or pool and stay until one of those works
        (``record_success``) or they age out."""
        if accounts:
            await conn.execute(
                self._q(
                    "DELETE FROM {e}.proxy_failures WHERE account = ANY(%s) AND kind = 'connect'"
                ),
                (sorted(accounts),),
            )


@dataclass(frozen=True)
class UsageRow:
    account: str
    ref: str  # exit address or pool:<name>
    task_type: str
    bytes: int


@dataclass
class ImportResult:
    account: str
    account_created: bool = False
    credentials_changed: bool = False
    exits_created: int = 0
    exits_existing: int = 0
    pools_created: list[str] = field(default_factory=list)
    pools_updated: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ImportLine:
    number: int
    scheme: str
    host: str
    port: int
    username: str = field(repr=False)
    password: str = field(repr=False)


def parse_lines(text: str, scheme: str = "http") -> list[ImportLine]:
    """``host:port:username:password`` per line (blank lines and ``#`` comments skipped).
    Errors name the line number only, never its content."""
    out = []
    for i, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":", 3)
        if len(parts) != 4 or not all(parts):
            raise ProxyAdminError(f"line {i}: expected host:port:username:password")
        host, port_s, user, password = parts
        if not port_s.isdigit() or not 0 < int(port_s) < 65536:
            raise ProxyAdminError(f"line {i}: the port is not a number between 1 and 65535")
        try:
            s, h, p = px.parse_endpoint(f"{scheme}://{host}:{port_s}")
        except ValueError:
            raise ProxyAdminError(f"line {i}: not a host name") from None
        out.append(ImportLine(i, s, h, p, user, password))
    return out


def account_view(a: px.Account, detail: bool = False) -> dict:
    """An account as listings and ``GET /v1/egress/accounts`` show it: no credentials."""
    r = a.row
    out = {
        "name": a.name,
        "vendor": a.vendor,
        "state": a.state,
        "status": a.status,
        "remaining_ratio": a.remaining.ratio,
        "remaining_gb": None if a.remaining.bytes is None else round(a.remaining.bytes / px.GB, 3),
        "limit_gb": None
        if a.remaining.limit_bytes is None
        else round(a.remaining.limit_bytes / px.GB, 3),
        "limit_source": a.remaining.source,
        "period_end": a.period_end.isoformat(),
        "envs": a.envs,
    }
    if detail:
        out |= {
            "adapter": a.adapter,
            "adapter_config": a.adapter_config,
            "state_reason": r.get("state_reason"),
            "state_since": _jsonable(r.get("state_since")),
            "cap_gb": _jsonable(float(r["cap_gb"]) if r.get("cap_gb") is not None else None),
            "cap_period": r.get("cap_period"),
            "period_anchor": r.get("period_anchor"),
            "paid_until": _jsonable(r.get("paid_until")),
            "concurrency": r.get("concurrency"),
            "warn_remaining_gb": _jsonable(
                float(r["warn_remaining_gb"]) if r.get("warn_remaining_gb") is not None else None
            ),
            "our_gb": round(a.our_bytes / px.GB, 3),
            "vendor_used_gb": None
            if r.get("vendor_used_bytes") is None
            else round(r["vendor_used_bytes"] / px.GB, 3),
            "vendor_seen_at": _jsonable(r.get("vendor_seen_at")),
            "vendor_error": r.get("vendor_error"),
            "credentials_fp": a.credentials_fp,
            "credentials_updated_at": _jsonable(r.get("credentials_updated_at")),
            "key_id": a.key_id,
            "usage_fp": r.get("usage_fp"),
        }
    return out


def _check_account_fields(fields: dict, update: bool = False) -> dict:
    out = {}
    for k, v in fields.items():
        if k not in ACCOUNT_FIELDS:
            raise ProxyAdminError(f"unknown field {k!r}")
        if v is None and k in (
            "vendor",
            "adapter",
            "cap_period",
            "period_anchor",
            "envs",
            "adapter_config",
        ):
            continue
        if k == "cap_period" and v not in ("month", "day"):
            raise ProxyAdminError("cap_period: month or day")
        if k == "period_anchor" and not 1 <= int(v) <= 28:
            raise ProxyAdminError("period_anchor: a day between 1 and 28")
        if k in ("cap_gb", "warn_remaining_gb") and v is not None and float(v) <= 0:
            raise ProxyAdminError(f"{k} must be above 0")
        if k == "concurrency" and v is not None and int(v) < 1:
            raise ProxyAdminError("concurrency must be at least 1")
        if k == "adapter_config":
            if not isinstance(v, dict):
                raise ProxyAdminError("adapter_config must be an object")
            if "errors" in v:
                try:
                    px.check_error_rules(v["errors"])
                except ValueError as e:
                    raise ProxyAdminError(f"adapter_config.{e}") from None
        if k == "envs" and (not isinstance(v, list) or not v):
            raise ProxyAdminError("envs: a list of environments")
        out[k] = v
    return out


def _check_pool_fields(fields: dict) -> dict:
    out = {}
    for k, v in fields.items():
        if k not in POOL_FIELDS:
            raise ProxyAdminError(f"unknown field {k!r}")
        if v is None and k not in (
            "max_sticky_ttl_s",
            "cost_per_gb",
            "concurrency",
            "exit_ip_header",
            "task_types",
        ):
            continue
        if k == "kind" and v not in px.KINDS:
            raise ProxyAdminError(f"kind: one of {', '.join(px.KINDS)}")
        if k == "endpoint":
            try:
                s, h, p = px.parse_endpoint(v)
            except ValueError as e:
                raise ProxyAdminError(str(e)) from None
            v = px.endpoint_text(s, h, p)
        if k == "countries":
            v = [c.upper() for c in v]
        if k == "egress_weight" and int(v) < 1:
            raise ProxyAdminError("egress_weight must be at least 1")
        out[k] = v
    if out.get("sticky") and "{session}" not in out.get("username_template", "{session}"):
        raise ProxyAdminError("a sticky pool's username_template needs {session}")
    return out


def _db(v: Any) -> Any:
    if isinstance(v, dict):
        return json.dumps(v)
    return v


def _norm(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, list | tuple):
        return sorted(str(x) for x in v)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True)
    if isinstance(v, float | int) and not isinstance(v, bool):
        return float(v)
    try:
        return float(v)  # numeric from Postgres
    except (TypeError, ValueError):
        return str(v)


def _slash24(address: str) -> str:
    import ipaddress

    ip = ipaddress.ip_address(address)
    prefix = 24 if ip.version == 4 else 48
    return str(ipaddress.ip_network(f"{address}/{prefix}", strict=False))


def now() -> datetime:
    return datetime.now(UTC)
