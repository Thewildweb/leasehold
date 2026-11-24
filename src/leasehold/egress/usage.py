"""Usage adapters: how much of a proxy account's plan is used, from the vendor's API.

The control API that owns the egress schema polls each active account's adapter every
``proxies.usage_poll`` and stores the answer on the account (``vendor_used_bytes``,
``vendor_limit_bytes``, ``vendor_remaining_bytes``, ``vendor_period_end``). The
dispatcher and the quota states use the lowest remaining of the vendor's numbers and the
account's own cap against our byte count (``leasehold.egress.proxies.remaining``).

leasehold ships only ``generic``: no usage API, our own byte count is all there is.
Vendor adapters are installed as packages that register an entry point in the group
``leasehold.proxy_adapters``::

    [project.entry-points."leasehold.proxy_adapters"]
    example = "example_adapters.example:ExampleAdapter"

The entry point names a class (or a factory) that takes no arguments and returns an
object with this interface. ``usage`` runs in a worker thread of the control API, may
make HTTP calls and should raise ``UsageError`` with a short message when it can't
answer; that message is stored and logged, so it must not contain the secret.

Error patterns (CONNECT refusals that mean "quota used up" and the like) are not part
of an adapter: they are data on the account (``adapter_config.errors``), because workers
apply them and workers run no vendor code.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from importlib.metadata import entry_points
from typing import Any, Protocol, runtime_checkable

log = logging.getLogger("leasehold.egress.usage")

ENTRY_POINT_GROUP = "leasehold.proxy_adapters"
GENERIC = "generic"

# Account states an adapter may report (a subset of the account's states).
VENDOR_STATES = ("quota_exhausted", "payment_failed", "suspended", "auth_failed")


class UsageError(Exception):
    """The vendor's usage could not be read (auth, timeout, unexpected response)."""


@dataclass(frozen=True)
class AccountInfo:
    """What an adapter knows about the account it reads."""

    name: str
    vendor: str
    kind: str  # the kind of its exits and pools: isp, residential, ...
    config: dict[str, Any] = field(default_factory=dict)  # adapter_config minus "errors"
    username: str | None = None  # the proxy username, when the usage API needs it


@dataclass(frozen=True)
class Usage:
    used_bytes: int  # in the current period
    limit_bytes: int | None = None  # the plan, if the vendor reports it
    period_end: datetime | None = None
    state: str | None = None  # one of VENDOR_STATES, if the vendor reports one
    remaining_bytes: int | None = None  # if the vendor reports it separately

    def __post_init__(self) -> None:
        if self.used_bytes < 0:
            raise ValueError("used_bytes must be >= 0")
        if self.state is not None and self.state not in VENDOR_STATES:
            raise ValueError(f"state must be one of {', '.join(VENDOR_STATES)}")


@runtime_checkable
class UsageAdapter(Protocol):
    name: str

    def usage(self, account: AccountInfo, secret: bytes, now: datetime) -> Usage | None:
        """The account's usage now. ``secret`` is the account's usage credential (or its
        proxy credentials as JSON when it has none). ``None``: the vendor has no usage
        API for this account; our own count applies."""
        ...


class GenericAdapter:
    """No usage API: the account is limited by its own ``cap_gb`` alone."""

    name = GENERIC

    def usage(self, account: AccountInfo, secret: bytes, now: datetime) -> Usage | None:
        return None


def load_adapters() -> dict[str, UsageAdapter]:
    """``generic`` plus every installed adapter, by entry point name. An adapter that
    fails to load is logged and left out: its accounts then run on our own count."""
    out: dict[str, UsageAdapter] = {GENERIC: GenericAdapter()}
    for ep in entry_points(group=ENTRY_POINT_GROUP):
        if ep.name == GENERIC:
            log.warning("ignoring a usage adapter named %r: the name is reserved", ep.name)
            continue
        try:
            adapter = ep.load()()
        except Exception:  # noqa: BLE001 - one broken package must not stop the API
            log.exception("usage adapter %s failed to load", ep.name)
            continue
        if not isinstance(adapter, UsageAdapter):
            log.error("usage adapter %s has no usage() method; ignored", ep.name)
            continue
        out[ep.name] = adapter
    return out
