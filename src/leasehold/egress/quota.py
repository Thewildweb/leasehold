"""Usage polling and the quota states of proxy accounts.

The schema owner's control API runs ``QuotaKeeper.poll`` every ``proxies.usage_poll``
(one instance at a time, under an advisory lock) and ``evaluate`` after it:

- **Poll:** each account's usage adapter (``leasehold.egress.usage``) is asked for the
  vendor's numbers; they are stored on the account (``vendor_*``). A failing poll is kept
  in ``vendor_error``; after ``proxies.usage_poll_warn_after`` it is a warning, and
  dispatch goes on with our own count. A vendor-reported state (``suspended``, ...) sets
  the account's state like a refusal would.
- **Evaluate:** remaining = the lowest of the vendor's and our cap against our count
  (``leasehold.egress.proxies.remaining``).
  - at 100% used the account goes to ``quota_exhausted`` (reason ``quota: ...``), before
    the vendor refuses; it clears when the period resets, the cap is raised or the vendor
    reports a larger limit;
  - at ``proxies.warn_used`` (90%) used, or fewer than ``warn_remaining_gb`` left: a
    warning ("add GB"), logged once per period and exported as a metric;
  - ``paid_until`` within 7 days: a warning;
  - our count and the vendor's more than 10% of the plan apart: an info message.

The egress system doesn't stop services short of 100%: task types stop themselves with
``stop_at_used`` (``leasehold.egress.dispatch``).
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from psycopg import AsyncConnection

from leasehold.egress import proxies as px
from leasehold.egress import secrets as sec
from leasehold.egress.proxy_admin import ProxyAdmin
from leasehold.egress.usage import GENERIC, AccountInfo, UsageAdapter, UsageError

log = logging.getLogger("leasehold.egress.quota")

QUOTA_REASON = px.QUOTA_REASON
VENDOR_REASON = px.VENDOR_REASON


@dataclass
class Notice:
    level: str  # info, warning, page
    account: str
    kind: str  # warn_used, warn_gb, exhausted, resumed, paid_until, drift, poll_failing
    message: str


@dataclass
class AccountHealth:
    """What the metrics export per account after an evaluation."""

    warn: bool = False
    paid_until_days: float | None = None
    paid_until_warn: bool = False
    drift_ratio: float | None = None
    drift_warn: bool = False
    poll_failing: bool = False


@dataclass
class Evaluation:
    notices: list[Notice] = field(default_factory=list)
    health: dict[str, AccountHealth] = field(default_factory=dict)


class QuotaKeeper:
    def __init__(
        self,
        admin: ProxyAdmin,
        adapters: dict[str, UsageAdapter],
        warn_used: float = 0.9,
        poll_warn_after: timedelta = timedelta(hours=1),
        paid_until_warn: timedelta = timedelta(days=7),
        drift_warn: float = 0.10,
    ):
        """The levels come from the egress settings (``budgets.proxies``)."""
        self.admin = admin
        self.keyring = admin.keyring
        self.adapters = adapters
        self.warn_used = warn_used
        self.poll_warn_after = poll_warn_after
        self.paid_until_warn = paid_until_warn
        self.drift_warn = drift_warn

    async def poll(
        self, conn: AsyncConnection, now: datetime | None = None, account: str | None = None
    ) -> dict[str, str]:
        """Read every active account's usage. Returns account → ``ok``, ``generic`` (no
        usage API) or the error.

        ``account``: only that account, whatever its status (``leasehold proxy account
        poll``). A new account starts disabled, and drain and enable need a limit; this
        poll stores the vendor's plan size first, so no placeholder ``cap_gb`` is needed."""
        now = now or datetime.now(UTC)
        out: dict[str, str] = {}
        if account is None:
            rows = await self.admin._rows(
                conn,
                "SELECT * FROM {e}.proxy_accounts WHERE removed_at IS NULL "
                "AND status <> 'disabled' ORDER BY name",
            )
        else:
            rows = await self.admin._rows(
                conn,
                "SELECT * FROM {e}.proxy_accounts WHERE removed_at IS NULL AND name = %s",
                (account,),
            )
        kinds = {
            r["account"]: r["kind"]
            for r in await self.admin._rows(
                conn,
                "SELECT DISTINCT ON (account) account, kind FROM ("
                "SELECT account, kind FROM {e}.proxy_exits WHERE removed_at IS NULL "
                "UNION ALL SELECT account, kind FROM {e}.proxy_pools WHERE removed_at IS NULL) k",
            )
        }
        for r in rows:
            name = r["name"]
            if r["adapter"] == GENERIC:
                out[name] = GENERIC
                continue
            adapter = self.adapters.get(r["adapter"])
            if adapter is None:
                await self._poll_failed(conn, name, f"usage adapter {r['adapter']} not installed")
                out[name] = "adapter missing"
                continue
            try:
                secret, username = self._secret(r)
                info = AccountInfo(
                    name=name,
                    vendor=r["vendor"],
                    kind=kinds.get(name, ""),
                    config={k: v for k, v in (r["adapter_config"] or {}).items() if k != "errors"},
                    username=username,
                )
                usage = await asyncio.to_thread(adapter.usage, info, secret, now)
            except (UsageError, sec.SecretsError) as e:
                await self._poll_failed(conn, name, str(e)[:300])
                out[name] = str(e)[:300]
                continue
            except Exception as e:  # noqa: BLE001 - an adapter bug must not stop the poller
                log.exception("usage adapter failed", extra={"fields": {"account": name}})
                await self._poll_failed(conn, name, f"{type(e).__name__} in the adapter")
                out[name] = f"{type(e).__name__} in the adapter"
                continue
            if usage is None:
                out[name] = GENERIC
                continue
            await conn.execute(
                self.admin._q(
                    "UPDATE {e}.proxy_accounts SET vendor_used_bytes = %s, "
                    "vendor_limit_bytes = %s, vendor_remaining_bytes = %s, "
                    "vendor_period_end = %s, vendor_seen_at = now(), vendor_error = NULL, "
                    "vendor_error_since = NULL WHERE name = %s"
                ),
                (
                    usage.used_bytes,
                    usage.limit_bytes,
                    usage.remaining_bytes,
                    usage.period_end,
                    name,
                ),
            )
            if usage.state is not None:
                await self.admin.set_state(
                    conn, name, usage.state, f"{VENDOR_REASON} the usage API reports {usage.state}"
                )
            else:
                await self.admin.set_state(
                    conn,
                    name,
                    "ok",
                    f"{VENDOR_REASON} the usage API reports no state any more",
                    reason_prefix=VENDOR_REASON,
                )
            out[name] = "ok"
        return out

    def _secret(self, r: dict) -> tuple[bytes, str | None]:
        username = None
        if r["credentials"] is not None:
            plain = self.keyring.open(
                bytes(r["credentials"]),
                r["key_id"],
                sec.context("proxy_accounts", r["name"], "credentials"),
            )
            username = px.decode_credentials(plain)[0]
        if r["usage_credentials"] is not None:
            secret = self.keyring.open(
                bytes(r["usage_credentials"]),
                r["usage_key_id"],
                sec.context("proxy_accounts", r["name"], "usage_credentials"),
            )
            return secret, username
        if r["credentials"] is None:
            raise UsageError("the account has no credentials")
        return plain, username

    async def _poll_failed(self, conn: AsyncConnection, name: str, error: str) -> None:
        await conn.execute(
            self.admin._q(
                "UPDATE {e}.proxy_accounts SET vendor_error = %s, "
                "vendor_error_since = coalesce(vendor_error_since, now()) WHERE name = %s"
            ),
            (error, name),
        )
        log.warning("usage poll failed", extra={"fields": {"account": name, "error": error}})

    async def evaluate(self, conn: AsyncConnection, now: datetime | None = None) -> Evaluation:
        """Quota states and notices from the stored numbers."""
        now = now or datetime.now(UTC)
        ev = Evaluation()
        for name, a in (await px.load_accounts(conn, self.admin.schema, now)).items():
            r = a.row
            h = AccountHealth()
            ev.health[name] = h
            rem = a.remaining
            period = px.period_start(now, r["cap_period"], r["period_anchor"]).isoformat()
            notified = dict(r.get("notified") or {})
            seen = notified.get(period, [])

            def once(kind: str, level: str, msg: str, _seen=seen, _name=name) -> None:
                if kind not in _seen:
                    _seen.append(kind)
                    ev.notices.append(Notice(level, _name, kind, msg))

            if rem.bytes is not None and rem.bytes <= 0 and a.state == "ok":
                if await self.admin.set_state(
                    conn, name, "quota_exhausted", f"{QUOTA_REASON} {rem.source} limit used up"
                ):
                    ev.notices.append(
                        Notice(
                            "page",
                            name,
                            "exhausted",
                            f"{a.vendor} account {name}: quota used up ({rem.source})",
                        )
                    )
            elif (
                a.state == "quota_exhausted"
                and (r.get("state_reason") or "").startswith(QUOTA_REASON)
                and rem.bytes is not None
                and rem.bytes > 0
            ):
                if await self.admin.set_state(conn, name, "ok", "quota: room again"):
                    ev.notices.append(
                        Notice(
                            "page",
                            name,
                            "resumed",
                            f"{a.vendor} account {name}: quota available again",
                        )
                    )
            used = rem.used_ratio
            if used is not None and used >= self.warn_used:
                h.warn = True
                once(
                    "warn_used",
                    "warning",
                    f"{a.vendor} account {name}: {used:.0%} used, "
                    f"{(rem.bytes or 0) / px.GB:.2f} GB left; add GB",
                )
            warn_gb = r.get("warn_remaining_gb")
            if warn_gb is not None and rem.bytes is not None and rem.bytes < float(warn_gb) * px.GB:
                h.warn = True
                once(
                    "warn_gb",
                    "warning",
                    f"{a.vendor} account {name}: {rem.bytes / px.GB:.2f} GB left "
                    f"(warning below {float(warn_gb):g} GB); add GB",
                )
            if r.get("paid_until") is not None:
                until = datetime.combine(r["paid_until"], datetime.min.time(), tzinfo=UTC)
                days = (until - now) / timedelta(days=1)
                h.paid_until_days = days
                if days <= self.paid_until_warn / timedelta(days=1):
                    h.paid_until_warn = True
                    once(
                        "paid_until",
                        "warning",
                        f"{a.vendor} account {name}: paid until {r['paid_until']}",
                    )
            vl, vu = r.get("vendor_limit_bytes"), r.get("vendor_used_bytes")
            if vl and vu is not None:
                h.drift_ratio = (a.our_bytes - int(vu)) / int(vl)
                if abs(h.drift_ratio) > self.drift_warn:
                    h.drift_warn = True
                    once(
                        "drift",
                        "info",
                        f"{a.vendor} account {name}: our count {a.our_bytes / px.GB:.2f} GB, "
                        f"the vendor's {int(vu) / px.GB:.2f} GB",
                    )
            since = r.get("vendor_error_since")
            if since is not None and now - since >= self.poll_warn_after:
                h.poll_failing = True
                once(
                    "poll_failing",
                    "warning",
                    f"{a.vendor} account {name}: usage poll failing since {since:%H:%M} UTC",
                )
            if seen != notified.get(period, []) or period not in notified:
                await conn.execute(
                    self.admin._q("UPDATE {e}.proxy_accounts SET notified = %s WHERE name = %s"),
                    (json.dumps({period: seen}), name),
                )
        for n in ev.notices:
            level = {"page": logging.ERROR, "warning": logging.WARNING}.get(n.level, logging.INFO)
            log.log(level, n.message, extra={"fields": {"account": n.account, "notice": n.kind}})
        return ev
