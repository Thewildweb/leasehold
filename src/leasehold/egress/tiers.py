"""Egress tiers: which kinds of egress a task type may use, in escalation order.

``datacenter`` is our own addresses (the registry's VPSes, reached by binding the source
address). ``isp`` is fixed proxy exits, ``residential`` and ``mobile`` rotating proxy
pools (or fixed exits of that kind). A task type lists its tiers in ``service.yml``
(``egress.tiers``), each proxy tier with the accounts or pools it may use, in preference
order; without an ``egress`` block it uses ``datacenter`` only, as before proxies.

Every target starts on the task type's first tier. A block (pushback with cause
``challenge`` or ``block``, or an outside verdict such as ``unreachable_from_dc``) moves
the target to the next tier the task type lists (``egress.target_tiers``, shared across
environments). After ``proxies.tier_ttl`` (30 days) the target starts at the first tier
again. A tier with nothing eligible leaves the target waiting on it: escalation never
skips a tier, and a task type without ``datacenter`` never reaches a target from our
own addresses.

``datacenter`` is reached in one of two ways. By default the worker binds its own
address (it runs on the VPS). With ``via: exit`` on the tier (``datacenter_via: exit``
in ``service.yml``) the worker has no address of its own: the dispatcher picks a
registry egress that runs an exit (``leasehold.exit``) and the route goes through it
with an exit token. Such a task type is never served from a worker's own address.

The resolved block is stored per environment in ``task_types.egress`` (frontier schema):

    {"tiers": [{"tier": "datacenter", "names": [], "via": "exit"},
               {"tier": "isp", "names": ["isp-a"]}],
     "countries": ["NL"], "stop_at_used": {"isp": 0.7},
     "session": {"mode": "sticky", "ttl_s": 600} | null}
"""

from __future__ import annotations

from typing import Any

DATACENTER = "datacenter"
VIA_EXIT = "exit"
TIERS = ("datacenter", "isp", "residential", "mobile")
PROXY_TIERS = TIERS[1:]
ALL_TIERS = "*"  # a cooldown on every tier (rate pushback)

# Pushback causes. A block escalates and cools only its tier; rate pushback cools every
# tier and never escalates (moving to another address after it is not allowed).
CAUSES = ("challenge", "block", "rate", "retry_after")
ESCALATING = ("challenge", "block")

DEFAULT = {"tiers": [{"tier": DATACENTER, "names": []}], "countries": [], "stop_at_used": {}}


def resolved(egress: dict[str, Any] | None) -> dict[str, Any]:
    """The stored block, or the default (datacenter only) for a task type without one."""
    return egress or DEFAULT


def tier_names(egress: dict[str, Any] | None) -> list[str]:
    return [t["tier"] for t in resolved(egress)["tiers"]]


def names_for(egress: dict[str, Any] | None, tier: str) -> list[str]:
    for t in resolved(egress)["tiers"]:
        if t["tier"] == tier:
            return list(t["names"])
    return []


def uses_proxies(egress: dict[str, Any] | None) -> bool:
    return any(t in PROXY_TIERS for t in tier_names(egress))


def uses_datacenter(egress: dict[str, Any] | None) -> bool:
    """Served on the datacenter tier from the worker's own address."""
    return DATACENTER in tier_names(egress) and not via_exit(egress)


def via_exit(egress: dict[str, Any] | None) -> bool:
    """Served on the datacenter tier through registry exits."""
    for t in resolved(egress)["tiers"]:
        if t["tier"] == DATACENTER:
            return t.get("via") == VIA_EXIT
    return False


def rank(tier: str) -> int:
    return TIERS.index(tier) if tier in TIERS else 0


def effective(egress: dict[str, Any] | None, target_tier: str | None) -> str:
    """The tier a task type serves a target on: the first tier it lists that is not
    below the target's tier; the task type's last tier when the target is above all."""
    tiers = tier_names(egress)
    if target_tier is None:
        return tiers[0]
    for t in tiers:
        if rank(t) >= rank(target_tier):
            return t
    return tiers[-1]


def next_tier(egress: dict[str, Any] | None, current: str) -> str | None:
    """The tier after ``current`` in the task type's list; None on its last tier."""
    tiers = tier_names(egress)
    if current not in tiers:
        later = [t for t in tiers if rank(t) > rank(current)]
        return later[0] if later else None
    i = tiers.index(current)
    return tiers[i + 1] if i + 1 < len(tiers) else None
