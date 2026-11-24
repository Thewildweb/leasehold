"""The account probe: what exit address a proxy endpoint gives, and whether an account
works again.

- **Exit addresses.** A vendor's list of fixed exits gives endpoints (host:port), not
  addresses. The probe sends one IP-echo request (``proxies.echo_url``) through each
  endpoint; the answer is the exit's address. Its subnet (the /24) and ASN come from
  public routing data (``AsnLookup``); then the egress row is made. Every endpoint is
  checked again every ``proxies.exit_recheck``: when its address changed, the old egress
  row is disabled (``exit moved to <new>``), its leases are revoked, and a new row is
  made. Cooldowns and pair budgets stay with the old address, which is right: the new
  address has its own history at the targets.
- **Recovery.** An account that isn't ``ok`` (except a quota stop from our own count,
  which only the period's end, a larger cap or ``enable`` clears) is probed every
  ``proxies.probe_interval``: one echo request through one of its exits or pools with the
  current credentials. Success sets ``ok``.

Only the control API runs the probe: it needs the credentials.
"""

from __future__ import annotations

import ipaddress
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx
from psycopg import AsyncConnection

from leasehold.egress import proxies as px
from leasehold.egress import secrets as sec
from leasehold.egress.proxy_admin import SYSTEM, Conflict, ProxyAdmin

log = logging.getLogger("leasehold.egress.probe")

TIMEOUT = httpx.Timeout(20.0, connect=10.0)
_STATUS = re.compile(r"\b([1-5]\d\d)\b")


@dataclass(frozen=True)
class Echo:
    address: str | None = None
    status: int | None = None  # the proxy's refusal status, when it refused
    error: str | None = None  # short, never with credentials


@dataclass(frozen=True)
class Network:
    asn: int | None
    prefix: str | None
    country: str | None = None


AsnLookup = Callable[[str], Awaitable[Network]]
Transport = httpx.AsyncBaseTransport | None


async def echo(
    scheme: str,
    host: str,
    port: int,
    username: str,
    password: str,
    url: str,
    transport: Transport = None,
) -> Echo:
    """One IP-echo request through a proxy. The proxy resolves the echo host."""
    proxy = px.proxy_url(scheme, host, port, username, password)
    try:
        async with httpx.AsyncClient(
            proxy=proxy if transport is None else None,
            transport=transport,
            trust_env=False,
            timeout=TIMEOUT,
            follow_redirects=False,
        ) as client:
            r = await client.get(url, headers={"Accept": "text/plain"})
    except httpx.ProxyError as e:
        text = _clean(str(e), username, password)
        m = _STATUS.search(text)
        return Echo(status=int(m.group(1)) if m else None, error=text)
    except httpx.HTTPError as e:
        return Echo(error=_clean(f"{type(e).__name__}: {e}", username, password))
    if r.status_code in (402, 407):
        return Echo(status=r.status_code, error=_clean(r.text[:200], username, password))
    if r.status_code != 200:
        return Echo(error=f"echo answered {r.status_code}")
    text = r.text.strip()
    if text.startswith("{"):
        try:
            text = str(r.json().get("ip", ""))
        except ValueError:
            pass
    try:
        return Echo(address=str(ipaddress.ip_address(text)))
    except ValueError:
        return Echo(error="echo answer is not an address")


def _clean(text: str, *secrets_: str) -> str:
    for s in secrets_:
        if s:
            text = text.replace(s, "***")
    return text[:200]


async def ripestat_lookup(address: str, transport: Transport = None) -> Network:
    """ASN and announced prefix from RIPEstat's network-info (public routing data)."""
    url = "https://stat.ripe.net/data/network-info/data.json"
    try:
        async with httpx.AsyncClient(transport=transport, timeout=TIMEOUT, trust_env=False) as c:
            r = await c.get(url, params={"resource": address})
            r.raise_for_status()
            data = r.json().get("data", {})
    except (httpx.HTTPError, ValueError) as e:
        log.warning("ASN lookup failed", extra={"fields": {"address": address, "error": str(e)}})
        return Network(None, None)
    asns = data.get("asns") or []
    return Network(int(asns[0]) if asns else None, data.get("prefix"))


class Prober:
    def __init__(
        self,
        admin: ProxyAdmin,
        echo_url: str,
        lookup: AsnLookup = ripestat_lookup,
        transport: Transport = None,
    ):
        self.admin = admin
        self.keyring = admin.keyring
        self.echo_url = echo_url
        self.lookup = lookup
        self.transport = transport  # tests: a transport that plays the proxy

    async def _creds(self, conn: AsyncConnection, account: str) -> tuple[str, str] | None:
        row = await self.admin.account(conn, account)
        if row["credentials"] is None:
            return None
        plain = self.keyring.open(
            bytes(row["credentials"]),
            row["key_id"],
            sec.context("proxy_accounts", account, "credentials"),
        )
        return px.decode_credentials(plain)

    async def _echo(self, endpoint: str, user: str, password: str) -> Echo:
        scheme, host, port = px.parse_endpoint(endpoint)
        return await echo(scheme, host, port, user, password, self.echo_url, self.transport)

    async def recheck_exits(
        self, conn: AsyncConnection, account: str | None = None, due_after_s: float = 3600
    ) -> dict[str, int]:
        """Probe the fixed exits that are due (no address yet, or not seen for
        ``due_after_s``; all of ``account``'s when one is given). Returns counts."""
        cond = "p.removed_at IS NULL AND a.removed_at IS NULL AND a.credentials IS NOT NULL"
        params: list = []
        if account:
            cond += " AND p.account = %s"
            params.append(account)
        else:
            cond += (
                " AND a.status <> 'disabled' AND (p.address_seen_at IS NULL "
                "OR p.address_seen_at < now() - %s * interval '1 second')"
            )
            params.append(due_after_s)
        exits = await self.admin._rows(
            conn,
            "SELECT p.id, p.account, p.endpoint, host(p.address) AS address, p.country "
            "FROM {e}.proxy_exits p JOIN {e}.proxy_accounts a ON a.name = p.account "
            f"WHERE {cond} ORDER BY p.account, p.id",
            params,
        )
        counts = {"checked": 0, "learned": 0, "moved": 0, "failed": 0}
        creds: dict[str, tuple[str, str] | None] = {}
        for ex in exits:
            if ex["account"] not in creds:
                creds[ex["account"]] = await self._creds(conn, ex["account"])
            c = creds[ex["account"]]
            if c is None:
                continue
            result = await self._echo(ex["endpoint"], *c)
            counts["checked"] += 1
            async with conn.transaction():
                outcome = await self._apply_echo(conn, ex, result)
            counts[outcome] = counts.get(outcome, 0) + 1
        return counts

    async def _apply_echo(self, conn: AsyncConnection, ex: dict, result: Echo) -> str:
        admin = self.admin
        if result.address is None:
            await conn.execute(
                admin._q(
                    "UPDATE {e}.proxy_exits SET last_error = %s, updated_at = now() WHERE id = %s"
                ),
                (result.error or "no answer", ex["id"]),
            )
            if result.status in (402, 407):
                rules = px.error_rules((await admin.account(conn, ex["account"]))["adapter_config"])
                state = px.match_error(rules, result.status, result.error or "")
                if state:
                    await admin.set_state(
                        conn, ex["account"], state, f"probe: proxy answered {result.status}"
                    )
            return "failed"
        if ex["address"] == result.address:
            await conn.execute(
                admin._q(
                    "UPDATE {e}.proxy_exits SET address_seen_at = now(), last_error = NULL, "
                    "updated_at = now() WHERE id = %s"
                ),
                (ex["id"],),
            )
            return "same"
        net = await self.lookup(result.address)
        moved = ex["address"] is not None
        if moved:
            old = ex["address"]
            await conn.execute(
                admin._q(
                    "UPDATE {e}.egresses SET override_status = 'disabled', "
                    "override_reason = %s, removed_at = now(), updated_at = now() "
                    "WHERE host(address) = %s AND exit_id = %s"
                ),
                (f"exit moved to {result.address}", old, ex["id"]),
            )
            await admin._revoke(conn, old, f"exit moved to {result.address}")
            log.info(
                "proxy exit moved",
                extra={"fields": {"account": ex["account"], "from": old, "to": result.address}},
            )
        subnet = _slash24(result.address)
        try:
            await admin.attach_address(
                conn,
                ex["id"],
                result.address,
                subnet,
                net.asn,
                actor=SYSTEM,
                country=net.country,
            )
        except Conflict as e:
            await conn.execute(
                admin._q(
                    "UPDATE {e}.proxy_exits SET last_error = %s, updated_at = now() WHERE id = %s"
                ),
                (str(e)[:200], ex["id"]),
            )
            return "failed"
        return "moved" if moved else "learned"

    async def probe_account(self, conn: AsyncConnection, account: str) -> Echo:
        """One echo request through one of the account's exits or pools. Success sets the
        account ``ok``; a refusal sets the state its rules give."""
        c = await self._creds(conn, account)
        if c is None:
            return Echo(error="no credentials")
        user, password = c
        targets = await self.admin._rows(
            conn,
            "SELECT endpoint, NULL::text AS template, NULL::text[] AS countries FROM {e}.egresses "
            "WHERE account = %s AND removed_at IS NULL AND override_status IS NULL "
            "UNION ALL SELECT endpoint, NULL::text, NULL::text[] FROM {e}.proxy_exits "
            "WHERE account = %s AND removed_at IS NULL "
            "UNION ALL SELECT endpoint, username_template, countries FROM {e}.proxy_pools "
            "WHERE account = %s AND removed_at IS NULL LIMIT 1",
            (account, account, account),
        )
        if not targets:
            return Echo(error="the account has no exits or pools")
        t = targets[0]
        username = user
        if t["template"]:
            countries = t["countries"] or []
            username = px.render_username(
                t["template"], user, countries[0] if countries else None, None, None
            )
        result = await self._echo(t["endpoint"], username, password)
        async with conn.transaction():
            if result.address is not None:
                changed = await self.admin.set_state(
                    conn, account, "ok", "probe succeeded", unless_reason=px.NOT_PROBED
                )
                await self.admin.clear_failures(conn, {account})
                if changed:
                    log.info("proxy account recovered", extra={"fields": {"account": account}})
            elif result.status is not None:
                rules = px.error_rules((await self.admin.account(conn, account))["adapter_config"])
                state = px.match_error(rules, result.status, result.error or "")
                if state:
                    await self.admin.set_state(
                        conn, account, state, f"probe: proxy answered {result.status}"
                    )
        return result

    async def probe_unhealthy(self, conn: AsyncConnection) -> int:
        """Probe every account that isn't ``ok``, except states a probe can't disprove
        (a quota stop from the numbers, a state the usage API reports)."""
        rows = await self.admin._rows(
            conn,
            "SELECT name, state, state_reason FROM {e}.proxy_accounts "
            "WHERE removed_at IS NULL AND state <> 'ok' AND status <> 'disabled' "
            "AND credentials IS NOT NULL",
        )
        n = 0
        for r in rows:
            if (r["state_reason"] or "").startswith(px.NOT_PROBED):
                continue
            await self.probe_account(conn, r["name"])
            n += 1
        return n


def _slash24(address: str) -> str:
    ip = ipaddress.ip_address(address)
    return str(ipaddress.ip_network(f"{address}/{24 if ip.version == 4 else 48}", strict=False))
