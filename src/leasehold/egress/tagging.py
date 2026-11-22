"""Which provider a target belongs to, and what a target lease is keyed on.

Planners tag each task when they queue it::

    tagger = run.tagger(shared_ips="dns.target_ips")
    tag = tagger.tag(domain, ip, cnames)
    NewTask(key=domain, group=tag.target, provider=tag.provider)

Rules, first hit wins:

1. A name in the CNAME chain equals a provider's suffix or ends with ``.`` + suffix.
2. The target IP is in a provider's range; the longest prefix over all providers wins.
3. The target IP serves at least ``min_domains`` domains (the shared count) and no
   provider rule matched: provider ``ip:<address>``, with a fleet-wide cap as well as a
   budget per egress. Known providers (rules 1 and 2) take precedence, so their budget
   applies, not that cap.
4. Otherwise no provider: plain hosting, limited by its target IP alone.

A target of a cdn or platform provider, or a shared IP, is leased per domain (the
target is the domain): its rate across domains is held by the provider budget. Every
other target is leased per IP, so the target limit holds for the server: plain hosting
and parking providers (no budget; their domains need few fetches). A task without a
provider is never unlimited: the dispatcher limits it by its target.
"""

from __future__ import annotations

import bisect
import ipaddress
from collections.abc import Iterable
from dataclasses import dataclass

from leasehold.egress.config import PER_DOMAIN_KINDS, SHARED_IP_PREFIX, ProviderRules

_Net = ipaddress.IPv4Network | ipaddress.IPv6Network


@dataclass(frozen=True)
class Tag:
    provider: str  # '' for plain hosting
    target: str  # what a target lease is keyed on: the domain or the IP
    source: str  # cname | range | shared_ip | none
    kind: str = ""  # the provider's kind (cdn, platform, parking); shared_ip; '' for none


class _Ranges:
    """Longest-prefix match over a few thousand networks, per IP version."""

    def __init__(self, ranges: dict[str, str]):
        by_version: dict[int, list[tuple[int, int, int, str]]] = {4: [], 6: []}
        for net_s, provider in ranges.items():
            net: _Net = ipaddress.ip_network(net_s, strict=False)
            by_version[net.version].append(
                (int(net.network_address), int(net.broadcast_address), net.prefixlen, provider)
            )
        self._lists = {v: sorted(rows) for v, rows in by_version.items()}
        self._starts = {v: [r[0] for r in rows] for v, rows in self._lists.items()}
        # Longest prefix in the table, per version: how far back a containing range can start.
        self._maxlen = {
            v: max((r[1] - r[0] for r in rows), default=0) for v, rows in self._lists.items()
        }

    def match(self, ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str | None:
        rows = self._lists[ip.version]
        if not rows:
            return None
        x = int(ip)
        i = bisect.bisect_right(self._starts[ip.version], x) - 1
        best: tuple[int, str] | None = None
        lowest = x - self._maxlen[ip.version]
        while i >= 0 and rows[i][0] >= lowest:
            start, end, plen, provider = rows[i]
            if start <= x <= end and (best is None or plen > best[0]):
                best = (plen, provider)
            i -= 1
        return best[1] if best else None


class Tagger:
    def __init__(
        self,
        rules: ProviderRules,
        shared_ips: Iterable[str] = (),
        min_domains: int | None = None,
    ):
        """``shared_ips``: addresses that serve at least ``min_domains`` domains (the
        caller selects them from the shared count)."""
        self._cnames = dict(rules.cnames)
        self._kinds = dict(rules.providers)
        self._ranges = _Ranges(rules.ranges)
        self._shared = {_canon(ip) for ip in shared_ips}
        self._shared.discard(None)
        self.min_domains = min_domains

    def provider_of_name(self, name: str) -> str | None:
        n = name.lower().rstrip(".")
        while n:
            p = self._cnames.get(n)
            if p is not None:
                return p
            dot = n.find(".")
            if dot < 0:
                return None
            n = n[dot + 1 :]
        return None

    def tag(self, domain: str, ip: str | None, cnames: Iterable[str] = ()) -> Tag:
        addr = _parse(ip)
        for name in cnames:
            if name and (p := self.provider_of_name(name)) is not None:
                return self._provider_tag(p, domain, addr, "cname")
        if addr is not None:
            if (p := self._ranges.match(addr)) is not None:
                return self._provider_tag(p, domain, addr, "range")
            if str(addr) in self._shared:
                return Tag(SHARED_IP_PREFIX + str(addr), domain, "shared_ip", "shared_ip")
            return Tag("", str(addr), "none")
        return Tag("", domain, "none")

    def _provider_tag(self, provider: str, domain: str, addr, source: str) -> Tag:
        kind = self._kinds.get(provider, "")
        per_domain = kind in PER_DOMAIN_KINDS or addr is None
        return Tag(provider, domain if per_domain else str(addr), source, kind)


def _parse(ip: str | None):
    if not ip:
        return None
    try:
        return ipaddress.ip_address(ip)
    except ValueError:
        return None


def _canon(ip: str) -> str | None:
    a = _parse(ip)
    return str(a) if a is not None else None
