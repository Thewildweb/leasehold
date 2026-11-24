"""Proxy accounts, fixed exits and rotating pools: reading them for dispatch, quota,
routes and error rules.

- An **account** (``egress.proxy_accounts``) is one contract with one vendor: its
  credentials (encrypted, ``leasehold.egress.secrets``), its plan and quota, its state
  (what it does: ``ok``, ``quota_exhausted``, ``auth_failed``, ...) and status (what the
  operator wants: ``active``, ``drain``, ``disabled``). Routes go through an account only
  when it is ``active`` and ``ok``, allowed in the environment and has credentials.
- A **fixed exit** is an egress row like a VPS (keyed on its exit address), with
  ``account`` and ``endpoint``: reached through the proxy instead of a local bind.
- A **rotating pool** (``egress.proxy_pools``) has no address before the request; its
  egress key is ``pool:<name>``.

Remaining quota (``remaining``) is the lowest of what exists: the vendor's limit minus
its used bytes, the vendor's own remaining, and the account's ``cap_gb`` minus our byte
count this period. The share is taken of the limit the lowest came from.

Changes (admin endpoints, import, the probe) are in ``leasehold.egress.proxy_admin``.
"""

from __future__ import annotations

import calendar
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlsplit

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from leasehold.egress import secrets as sec

POOL_PREFIX = "pool:"
GB = 10**9  # cap_gb and warn_remaining_gb are decimal gigabytes

STATES = ("ok", "quota_exhausted", "payment_failed", "suspended", "auth_failed", "unreachable")
STATUSES = ("active", "drain", "disabled")
KINDS = ("datacenter", "residential", "isp", "mobile")

# Rules every account has; adapter_config.errors adds the vendor's.
GENERIC_ERRORS: list[dict[str, Any]] = [
    {"status": 407, "state": "auth_failed"},
    {"status": 402, "state": "payment_failed"},
]
ERROR_STATES = ("quota_exhausted", "payment_failed", "suspended", "auth_failed")

# state_reason prefixes of states a probe can't disprove: a quota stop from the numbers,
# and a state the vendor's usage API reports. A working IP echo says nothing about either;
# they clear by the numbers, by the usage API, or by an operator's `enable`.
QUOTA_REASON = "quota:"
VENDOR_REASON = "vendor:"
NOT_PROBED = (QUOTA_REASON, VENDOR_REASON)


def pool_key(name: str) -> str:
    return POOL_PREFIX + name


def is_pool(key: str) -> bool:
    return key.startswith(POOL_PREFIX)


# --- endpoints and credentials ----------------------------------------------------------------

_ENDPOINT = re.compile(r"^(http|https|socks5|socks5h)://([A-Za-z0-9.\-\[\]:]+):(\d{1,5})$")


def parse_endpoint(text: str) -> tuple[str, str, int]:
    """``scheme://host:port`` (or ``host:port``, HTTP) → (scheme, host, port)."""
    t = text.strip()
    if "://" not in t:
        t = "http://" + t
    m = _ENDPOINT.match(t)
    if not m:
        raise ValueError(f"not an endpoint (scheme://host:port): {text!r}")
    scheme, host, port = m.group(1), m.group(2), int(m.group(3))
    if not 0 < port < 65536:
        raise ValueError(f"port out of range in {text!r}")
    if scheme == "socks5":
        scheme = "socks5h"  # the proxy resolves the target's name, never we
    return scheme, host, port


def endpoint_text(scheme: str, host: str, port: int) -> str:
    return f"{scheme}://{host}:{port}"


def encode_credentials(username: str, password: str) -> bytes:
    return json.dumps({"username": username, "password": password}).encode()


def decode_credentials(data: bytes) -> tuple[str, str]:
    d = json.loads(data)
    return d["username"], d["password"]


def redact_url(url: str) -> str:
    """``scheme://user:pass@host:port`` → ``scheme://***@host:port``."""
    parts = urlsplit(url)
    if parts.username is None and parts.password is None:
        return url
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return f"{parts.scheme}://***@{host}{parts.path}"


def proxy_url(scheme: str, host: str, port: int, username: str, password: str) -> str:
    return f"{scheme}://{quote(username, safe='')}:{quote(password, safe='')}@{host}:{port}"


# --- periods and remaining quota ----------------------------------------------------------------


def _month_start(now: datetime, anchor: int) -> datetime:
    y, m = now.year, now.month
    if now.day < anchor:
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    day = min(anchor, calendar.monthrange(y, m)[1])
    return datetime(y, m, day, tzinfo=UTC)


def period_start(now: datetime, cap_period: str, anchor: int) -> datetime:
    """Start of the account's current period (UTC days; ``anchor`` day of the month)."""
    if cap_period == "day":
        return datetime(now.year, now.month, now.day, tzinfo=UTC)
    return _month_start(now, anchor)


def period_end(now: datetime, cap_period: str, anchor: int) -> datetime:
    start = period_start(now, cap_period, anchor)
    if cap_period == "day":
        return start + timedelta(days=1)
    y, m = (start.year + 1, 1) if start.month == 12 else (start.year, start.month + 1)
    return datetime(y, m, min(anchor, calendar.monthrange(y, m)[1]), tzinfo=UTC)


@dataclass(frozen=True)
class Remaining:
    bytes: int | None  # None: no limit known at all
    ratio: float | None  # remaining / the binding limit
    limit_bytes: int | None  # the binding limit
    source: str | None  # "vendor" or "cap"

    @property
    def used_ratio(self) -> float | None:
        return None if self.ratio is None else 1 - self.ratio


def remaining(acc: dict, our_bytes: int) -> Remaining:
    """The lowest remaining of the vendor's numbers and our cap against our count."""
    options: list[tuple[int, int | None, str]] = []  # (remaining, its limit, source)
    vl, vu = acc.get("vendor_limit_bytes"), acc.get("vendor_used_bytes")
    vr = acc.get("vendor_remaining_bytes")
    if vl is not None and vu is not None:
        options.append((int(vl) - int(vu), int(vl), "vendor"))
    if vr is not None:
        options.append((int(vr), int(vl) if vl is not None else None, "vendor"))
    if acc.get("cap_gb") is not None:
        cap = int(float(acc["cap_gb"]) * GB)
        options.append((cap - our_bytes, cap, "cap"))
    if not options:
        return Remaining(None, None, None, None)
    left, limit, source = min(options, key=lambda o: o[0])
    left = max(left, 0)
    ratio = None if not limit else max(0.0, min(1.0, left / limit))
    return Remaining(left, ratio, limit, source)


# --- error rules (data, applied by workers) -----------------------------------------------------


def error_rules(adapter_config: dict | None) -> list[dict[str, Any]]:
    """The account's rules for proxy refusals: the generic ones, then the vendor's."""
    extra = (adapter_config or {}).get("errors") or []
    return GENERIC_ERRORS + [r for r in extra if r.get("state") in ERROR_STATES]


def check_error_rules(rules: Any) -> list[dict[str, Any]]:
    """Validate ``adapter_config.errors``: ``{status?, body_regex?, state}``.

    A refused CONNECT exposes only its status and reason line (httpx gives no headers or
    body), so rules match those; ``body_regex`` sees the full body only on forwarded
    ``http://`` requests and in the probe. Header rules could never match and are refused."""
    if not isinstance(rules, list):
        raise ValueError("errors must be a list")
    out = []
    for i, r in enumerate(rules):
        if isinstance(r, dict) and "header" in r:
            raise ValueError(
                f"errors[{i}]: header rules aren't supported (a refused CONNECT has only "
                "its status line); use status and body_regex"
            )
        if not isinstance(r, dict) or set(r) - {"status", "body_regex", "state"}:
            raise ValueError(f"errors[{i}]: keys are status, body_regex, state")
        if r.get("state") not in ERROR_STATES:
            raise ValueError(f"errors[{i}].state: one of {', '.join(ERROR_STATES)}")
        if "status" not in r and "body_regex" not in r:
            raise ValueError(f"errors[{i}]: needs status or body_regex")
        if "body_regex" in r:
            try:
                re.compile(r["body_regex"])
            except re.error as e:
                raise ValueError(f"errors[{i}].body_regex: {e}") from None
        if "status" in r and not isinstance(r["status"], int):
            raise ValueError(f"errors[{i}].status must be a number")
        out.append(r)
    return out


def match_error(rules: list[dict], status: int | None, text: str) -> str | None:
    """The account state a proxy refusal means, by the first rule that matches all its
    fields; None: not an account failure (a connect failure)."""
    for r in rules:
        if "header" in r:
            continue  # stored before header rules were refused; never matches
        if "status" in r and r["status"] != status:
            continue
        if "body_regex" in r and not re.search(r["body_regex"], text or "", re.IGNORECASE):
            continue
        return r["state"]
    return None


# --- reading for dispatch -------------------------------------------------------------------


@dataclass
class Account:
    name: str
    vendor: str
    adapter: str
    adapter_config: dict
    state: str
    status: str
    envs: list[str]
    concurrency: int | None
    credentials: bytes | None
    key_id: str | None
    credentials_fp: str | None
    remaining: Remaining
    our_bytes: int
    period_end: datetime
    row: dict = field(repr=False, default_factory=dict)

    @property
    def usable(self) -> bool:
        return self.status == "active" and self.state == "ok" and self.credentials is not None

    def open_credentials(self, keyring: sec.Keyring) -> tuple[str, str]:
        assert self.credentials is not None and self.key_id is not None
        plain = keyring.open(
            self.credentials, self.key_id, sec.context("proxy_accounts", self.name, "credentials")
        )
        return decode_credentials(plain)


@dataclass(frozen=True)
class ProxyEgress:
    """A fixed exit or a pool that may carry a route."""

    key: str  # exit address, or pool:<name>
    kind: str
    account: str
    pool: str | None
    endpoint: str
    country: str | None = None  # fixed exits
    countries: tuple[str, ...] = ()  # pools
    task_types: tuple[str, ...] | None = None
    sticky: bool = False
    max_sticky_ttl_s: int | None = None
    concurrency: int | None = None
    weight: int = 1
    template: str = "{user}"
    exit_ip_header: str | None = None

    def serves(self, task_type: str) -> bool:
        return self.task_types is None or task_type in self.task_types

    def country_for(self, wanted: list[str]) -> str | None:
        """The exit country for a task type's ``countries`` (empty: any); None if this
        egress can't exit in any of them."""
        if self.pool is None:
            if not wanted or (self.country and self.country.upper() in wanted):
                return self.country
            return None
        have = [c.upper() for c in self.countries]
        if not wanted:
            return have[0] if have else ""
        for c in wanted:
            if c in have:
                return c
        return None


async def _rows(conn: AsyncConnection, schema: str, stmt: str, params: Any = None) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql.SQL(stmt).format(e=sql.Identifier(schema)), params)
        return await cur.fetchall()


async def proxy_tables_exist(conn: AsyncConnection, schema: str) -> bool:
    cur = await conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'proxy_accounts'",
        (schema,),
    )
    return await cur.fetchone() is not None


async def load_accounts(
    conn: AsyncConnection, schema: str, now: datetime | None = None
) -> dict[str, Account]:
    """Every account not removed, with our byte count this period and its remaining."""
    now = now or datetime.now(UTC)
    rows = await _rows(conn, schema, "SELECT * FROM {e}.proxy_accounts WHERE removed_at IS NULL")
    if not rows:
        return {}
    starts = {
        r["name"]: period_start(now, r["cap_period"], r["period_anchor"]).date() for r in rows
    }
    oldest = min(starts.values())
    used = await _rows(
        conn,
        schema,
        "SELECT account, day, sum(bytes) AS bytes FROM {e}.proxy_usage "
        "WHERE day >= %s GROUP BY account, day",
        (oldest,),
    )
    ours: dict[str, int] = {}
    for u in used:
        if u["day"] >= starts.get(u["account"], date.max):
            ours[u["account"]] = ours.get(u["account"], 0) + int(u["bytes"])
    out = {}
    for r in rows:
        mine = ours.get(r["name"], 0)
        out[r["name"]] = Account(
            name=r["name"],
            vendor=r["vendor"],
            adapter=r["adapter"],
            adapter_config=r["adapter_config"] or {},
            state=r["state"],
            status=r["status"],
            envs=list(r["envs"] or []),
            concurrency=r["concurrency"],
            credentials=bytes(r["credentials"]) if r["credentials"] is not None else None,
            key_id=r["key_id"],
            credentials_fp=r["credentials_fp"],
            remaining=remaining(r, mine),
            our_bytes=mine,
            period_end=r["vendor_period_end"]
            or period_end(now, r["cap_period"], r["period_anchor"]),
            row=r,
        )
    return out


async def load_proxy_egresses(conn: AsyncConnection, schema: str) -> list[ProxyEgress]:
    """Fixed exits and pools that are in service (not disabled, not removed). The
    accounts' own state is checked separately (``Account.usable``)."""
    exits = await _rows(
        conn,
        schema,
        """
        SELECT host(address) AS key, kind, account, endpoint, country, task_types
        FROM {e}.egresses
        WHERE account IS NOT NULL AND status = 'active' AND override_status IS NULL
          AND removed_at IS NULL
        """,
    )
    pools = await _rows(
        conn,
        schema,
        """
        SELECT * FROM {e}.proxy_pools
        WHERE status = 'active' AND override_status IS NULL AND removed_at IS NULL
        """,
    )
    out = [
        ProxyEgress(
            key=r["key"],
            kind=r["kind"],
            account=r["account"],
            pool=None,
            endpoint=r["endpoint"],
            country=r["country"],
            task_types=tuple(r["task_types"]) if r["task_types"] is not None else None,
        )
        for r in exits
    ]
    out += [
        ProxyEgress(
            key=pool_key(r["name"]),
            kind=r["kind"],
            account=r["account"],
            pool=r["name"],
            endpoint=r["endpoint"],
            countries=tuple(r["countries"] or ()),
            task_types=tuple(r["task_types"]) if r["task_types"] is not None else None,
            sticky=r["sticky"],
            max_sticky_ttl_s=r["max_sticky_ttl_s"],
            concurrency=r["concurrency"],
            weight=r["egress_weight"],
            template=r["username_template"],
            exit_ip_header=r["exit_ip_header"],
        )
        for r in pools
    ]
    return out


# --- routes ---------------------------------------------------------------------------------


def session_id(lease_id) -> str:
    return "s" + str(lease_id).replace("-", "")[:16]


def render_username(
    template: str, user: str, country: str | None, session: str | None, ttl_s: int | None
) -> str:
    """The vendor's username syntax, as data: ``{user}``, ``{country}`` (lower case),
    ``{COUNTRY}``, ``{session}``, ``{ttl_min}``, ``{ttl_s}``."""
    values = {
        "user": user,
        "country": (country or "").lower(),
        "COUNTRY": (country or "").upper(),
        "session": session or "",
        "ttl_min": str(max(1, (ttl_s or 60) // 60)),
        "ttl_s": str(ttl_s or 0),
    }
    out = template
    for k, v in values.items():
        out = out.replace("{" + k + "}", v)
    return out


def render_route(
    eg: ProxyEgress,
    acc: Account,
    keyring: sec.Keyring,
    *,
    country: str | None,
    session: str | None,
    session_ttl_s: int | None,
    expires_at: datetime,
) -> dict:
    """The route a worker gets with a target lease. Holds credentials: never log it."""
    user, password = acc.open_credentials(keyring)
    scheme, host, port = parse_endpoint(eg.endpoint)
    username = user
    if eg.pool is not None:
        username = render_username(eg.template, user, country, session, session_ttl_s)
    return {
        "egress": eg.key,
        "kind": eg.kind,
        "country": country,
        "account": acc.name,
        "pool": eg.pool,
        "proxy": {
            "scheme": scheme,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
        },
        "session": {"id": session, "ttl_s": session_ttl_s} if session else None,
        "exit_ip": None if eg.pool is not None else eg.key,
        "exit_ip_header": eg.exit_ip_header,
        "account_remaining": acc.remaining.ratio,
        "errors": error_rules(acc.adapter_config),
        "credentials_fp": acc.credentials_fp,
        "expires_at": expires_at.isoformat(),
    }
