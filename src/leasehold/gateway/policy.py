"""Which hosts a browser context may reach: the per-lease host policy of the gateway.

A context is opened by the controller for one page load of one target lease. It may
reach:

- **first party**, at any time while the context is open: for a ``domain`` target,
  every host under the target's registrable domain; for an ``ip`` target, the page's
  own host (the one the controller named when it opened the context), the target
  address itself, and the hosts the controller added as redirects;
- **third party**, only while a page load of that context is in progress (between
  ``page_start`` and ``page_end``, at most ``page_timeout`` after ``page_start``), never
  a host that is itself a target with a live lease held by someone else (the foreign
  targets the controller keeps up to date), and at most ``third_party_tunnels`` tunnels
  and ``third_party_bytes`` bytes per context.

Never, for either party: an IPv6 address, an IPv4 address that isn't public (loopback,
private, link-local such as the cloud metadata address, the tailnet's 100.64.0.0/10) or a
``localhost`` name (``private_address``); the exit refuses those too.

Everything else is refused with a reason (``Refusal.reason``). No I/O here; the server
(``leasehold.gateway.server``) applies it.
"""

from __future__ import annotations

import ipaddress
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache

from leasehold.client import Route
from leasehold.keys import InvalidKey, normalize_domain

DOMAIN = "domain"
IP = "ip"
KINDS = (DOMAIN, IP)

_LABEL = re.compile(r"^(?!-)[a-z0-9_-]{1,63}(?<!-)$")


class Refusal(Exception):
    """A request the gateway does not forward: ``status`` for the browser, ``reason``
    for the metrics and the context's stats."""

    def __init__(self, status: int, reason: str):
        super().__init__(reason)
        self.status = status
        self.reason = reason


class ControlError(ValueError):
    """A control message the gateway refuses; the message goes back to the controller."""


def normalize_host(text: str) -> str:
    """A host as the policy compares it: lowercase, no trailing dot, IPv6 without
    brackets. ``ValueError`` for anything that is neither an IP address nor a DNS name
    in ASCII (browsers send IDNs as punycode)."""
    host = text.strip().lower().rstrip(".")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if not host:
        raise ValueError("empty host")
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        pass
    if len(host) > 253:
        raise ValueError("host too long")
    labels = host.split(".")
    if not all(_LABEL.match(lbl) for lbl in labels):
        raise ValueError(f"invalid host {text!r}")
    last = labels[-1]
    if last.isdigit() or (last.startswith("0x") and all(c in "0123456789abcdef" for c in last[2:])):
        # 2130706433, 127.1, 0x7f.1, 017700000001: numeric IPv4 forms a resolver may turn
        # into an address; no top-level domain is numeric.
        raise ValueError(f"numeric host {text!r}")
    return host


def is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


@lru_cache(maxsize=8192)
def registrable(host: str) -> str | None:
    """The registrable domain of a host (public suffix list), None for IP addresses and
    hosts without one."""
    if is_ip(host):
        return None
    try:
        return normalize_domain(host)
    except InvalidKey:
        return None


def under(host: str, domain: str) -> bool:
    return host == domain or host.endswith("." + domain)


@dataclass(frozen=True)
class Limits:
    third_party_tunnels: int = 200
    third_party_bytes: int = 30 * 2**20
    page_timeout: float = 60.0  # seconds after page_start

    _KEYS = {
        "third_party_tunnels": int,
        "third_party_bytes": int,
        "page_timeout_s": float,
    }

    def merged(self, d: dict | None) -> Limits:
        """These limits with the ones in ``d`` (a control message's ``limits``) applied.
        Unknown keys and non-positive values are refused."""
        if not d:
            return self
        if not isinstance(d, dict):
            raise ControlError("limits must be an object")
        unknown = set(d) - set(self._KEYS)
        if unknown:
            raise ControlError(f"unknown limits: {', '.join(sorted(unknown))}")
        values = {}
        for k, v in d.items():
            if isinstance(v, bool) or not isinstance(v, int | float) or v <= 0:
                raise ControlError(f"limit {k} must be a positive number")
            values["page_timeout" if k == "page_timeout_s" else k] = self._KEYS[k](v)
        return Limits(
            third_party_tunnels=values.get("third_party_tunnels", self.third_party_tunnels),
            third_party_bytes=values.get("third_party_bytes", self.third_party_bytes),
            page_timeout=values.get("page_timeout", self.page_timeout),
        )

    def json(self) -> dict:
        return {
            "third_party_tunnels": self.third_party_tunnels,
            "third_party_bytes": self.third_party_bytes,
            "page_timeout_s": self.page_timeout,
        }


@dataclass
class Foreign:
    """Targets with a live lease held by someone else (from the dispatcher's
    ``leases_of``, through the controller). Third-party hosts are refused while the list
    is older than ``max_age`` seconds, or was never set (fail closed)."""

    targets: frozenset[str] = frozenset()
    updated: float | None = None  # monotonic

    def set(self, targets: list, now: float | None = None) -> tuple[int, int]:
        """Replace the list; (targets kept, entries skipped). An entry that isn't a host
        is skipped and counted, not a reason to keep the old list."""
        out = set()
        skipped = 0
        for t in targets:
            try:
                if not isinstance(t, str):
                    raise ValueError
                out.add(normalize_host(t))
            except ValueError:
                skipped += 1
        self.targets = frozenset(out)
        self.updated = time.monotonic() if now is None else now
        return len(self.targets), skipped

    def fresh(self, now: float, max_age: float) -> bool:
        if max_age <= 0:
            return True
        return self.updated is not None and now - self.updated <= max_age

    def holds(self, host: str) -> bool:
        if host in self.targets:
            return True
        reg = registrable(host)
        return reg is not None and reg in self.targets


@dataclass
class Hosts:
    """The first-party hosts of a context."""

    kind: str
    target: str
    page_host: str
    domain: str | None = None  # the target's registrable domain (domain targets)
    redirects: set[str] = field(default_factory=set)

    @classmethod
    def create(cls, kind: str, target: str, page_host: str) -> Hosts:
        if kind not in KINDS:
            raise ControlError(f"target_kind must be one of {', '.join(KINDS)}")
        try:
            t = normalize_host(target)
            page = normalize_host(page_host)
        except ValueError as e:
            raise ControlError(str(e)) from None
        if kind == IP:
            addr = ipaddress.ip_address(t) if is_ip(t) else None
            if addr is None or addr.version != 4:
                raise ControlError("an ip target must be an IPv4 address")
            return cls(IP, t, page)
        domain = registrable(t)
        if domain is None:
            raise ControlError(f"no registrable domain in target {target!r}")
        if not under(page, domain):
            raise ControlError(f"page host {page_host!r} is not under {domain}")
        return cls(DOMAIN, t, page, domain)

    def add_redirect(self, host: str) -> str:
        if self.kind != IP:
            raise ControlError("redirects are first party only for ip targets")
        try:
            h = normalize_host(host)
        except ValueError as e:
            raise ControlError(str(e)) from None
        self.redirects.add(h)
        return h

    def party(self, host: str) -> str:
        """``target`` (the leased target or the page host), ``redirect`` (a redirect
        the controller added) or ``third``."""
        if self.kind == DOMAIN:
            assert self.domain is not None
            return "target" if under(host, self.domain) else "third"
        if host in (self.target, self.page_host):
            return "target"
        if host in self.redirects:
            return "redirect"
        return "third"


def parse_route(d: object, lease_id: str) -> tuple[Route, float | None]:
    """The route a context reaches its exit by, and when its token expires (unix
    seconds, None if the route doesn't say). The exit's credentials are the lease id and
    the exit token, so the route must be for this lease."""
    if not isinstance(d, dict):
        raise ControlError("route must be an object")
    try:
        route = Route.parse(d)
    except (KeyError, TypeError, ValueError):
        raise ControlError("route is missing proxy host, port or credentials") from None
    p = route.proxy
    if p.scheme != "http":
        raise ControlError("the exit is reached over http (CONNECT)")
    if not p.host or not 0 < p.port < 65536:
        raise ControlError("route has no exit endpoint")
    if p.username != lease_id:
        raise ControlError("route is for another lease")
    if not p.password:
        raise ControlError("route has no exit token")
    expires = None
    if d.get("expires_at"):
        try:
            # whole seconds, as in the exit token: both see it expire at the same time
            expires = float(int(datetime.fromisoformat(str(d["expires_at"])).timestamp()))
        except ValueError:
            raise ControlError("route expires_at is not an ISO timestamp") from None
    return route, expires


@dataclass
class Context:
    """One browser context: its credentials on the gateway, its lease's route, its
    first-party hosts, the page window and what it used."""

    name: str
    password: str = field(repr=False)
    lease_id: str
    route: Route = field(repr=False)
    route_expires: float | None
    hosts: Hosts
    limits: Limits
    page_deadline: float | None = None  # monotonic; None: no page load in progress
    closed: bool = False
    bytes_up: int = 0
    bytes_down: int = 0
    tunnels: int = 0
    third_party_tunnels: int = 0
    third_party_bytes: int = 0
    refused: Counter = field(default_factory=Counter)
    exit_errors: Counter = field(default_factory=Counter)

    def page_open(self, now: float) -> bool:
        return self.page_deadline is not None and now < self.page_deadline

    def admit(
        self,
        host: str,
        port: int,
        ports: tuple[int, ...],
        foreign: Foreign,
        foreign_max_age: float,
        now: float | None = None,
        wall: float | None = None,
        own: Foreign | None = None,
    ) -> bool:
        """Whether a tunnel to ``host:port`` may open; True when it is third party (and
        then counted against the context's third-party tunnels). ``Refusal`` if not."""
        now = time.monotonic() if now is None else now
        wall = time.time() if wall is None else wall
        if self.closed:
            raise Refusal(403, "closed")
        if port not in ports:
            raise Refusal(403, "port")
        if self.route_expires is not None and self.route_expires <= wall:
            raise Refusal(403, "route_expired")
        if is_ip(host):
            addr = ipaddress.ip_address(host)
            if addr.version != 4:
                raise Refusal(403, "ipv6")
            if not addr.is_global:
                # Loopback, private, link-local (metadata), CGNAT (the tailnet): the exit
                # refuses them too, but they never leave the machine as a request.
                raise Refusal(403, "private_address")
        elif host == "localhost" or host.endswith(".localhost"):
            raise Refusal(403, "private_address")
        party = self.hosts.party(host)
        if party == "target":
            return False
        # ``own``: the targets of the gateway's other open contexts. The same browser
        # holds their leases, so they aren't in the foreign list, but a page must not load
        # them outside their own pacing either.
        if own is not None and own.holds(host):
            raise Refusal(403, "own_target")
        if party == "redirect":
            if not foreign.fresh(now, foreign_max_age):
                raise Refusal(403, "foreign_stale")
            if foreign.holds(host):
                raise Refusal(403, "foreign_target")
            return False
        if not self.page_open(now):
            raise Refusal(403, "no_page")
        if not foreign.fresh(now, foreign_max_age):
            raise Refusal(403, "foreign_stale")
        if foreign.holds(host):
            raise Refusal(403, "foreign_target")
        if self.third_party_tunnels >= self.limits.third_party_tunnels:
            raise Refusal(403, "third_party_tunnels")
        if self.third_party_bytes >= self.limits.third_party_bytes:
            raise Refusal(403, "third_party_bytes")
        self.third_party_tunnels += 1
        return True

    def stats(self, open_tunnels: int, now: float | None = None) -> dict:
        now = time.monotonic() if now is None else now
        return {
            "lease_id": self.lease_id,
            "target": self.hosts.target,
            "bytes_up": self.bytes_up,
            "bytes_down": self.bytes_down,
            "bytes": self.bytes_up + self.bytes_down,
            "tunnels": self.tunnels,
            "tunnels_open": open_tunnels,
            "third_party_tunnels": self.third_party_tunnels,
            "third_party_bytes": self.third_party_bytes,
            "page_in_progress": self.page_open(now),
            "refused": dict(self.refused),
            "exit_errors": dict(self.exit_errors),
            "limits": self.limits.json(),
        }


# --- what a tunnel carries -------------------------------------------------------------------

TLS_RECORD_MAX = 16384 + 2048  # a record's payload, with room for compression/expansion
TLS_PORTS = (443,)  # tunnels whose first bytes must be a ClientHello for the host
HTTP_PORTS = (80,)  # tunnels whose first request must name the host


def client_hello_sni(record: bytes) -> str | None:
    """The server name in a TLS ClientHello record (the first record of a tunnel), or
    None without the extension. ``ValueError`` when it isn't a ClientHello."""

    def take(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(record):
            raise ValueError("truncated ClientHello")
        out = record[pos : pos + n]
        pos += n
        return out

    def num(n: int) -> int:
        return int.from_bytes(take(n), "big")

    pos = 0
    if num(1) != 0x16:
        raise ValueError("not a TLS handshake record")
    take(2)  # record version
    length = num(2)
    if length != len(record) - 5:
        raise ValueError("record length mismatch")
    if num(1) != 1:
        raise ValueError("not a ClientHello")
    take(3)  # handshake length (the hello may continue in later records: we read the first)
    take(2 + 32)  # client version, random
    take(num(1))  # session id
    take(num(2))  # cipher suites
    take(num(1))  # compression methods
    if pos >= len(record):
        return None  # no extensions
    end = min(len(record), pos + 2 + num(2))
    while pos + 4 <= end:
        ext, size = num(2), num(2)
        data = take(size)
        if ext == 0:  # server_name
            if len(data) < 5 or data[2] != 0:
                raise ValueError("bad server_name extension")
            n = int.from_bytes(data[3:5], "big")
            name = data[5 : 5 + n]
            if len(name) != n:
                raise ValueError("bad server_name extension")
            return normalize_host(name.decode("ascii"))
    return None


def check_tunnel_start(host: str, port: int, first: bytes) -> None:
    """The first bytes the browser sends into a CONNECT tunnel must be for the host it
    named (no fronting: an admitted host carrying a request for another). Port 443: the
    ClientHello's SNI is that host (none for an IP host); port 80: the request's Host is.
    ``Refusal`` otherwise."""
    if port in TLS_PORTS:
        try:
            sni = client_hello_sni(first)
        except (ValueError, UnicodeDecodeError):
            raise Refusal(403, "not_tls") from None
        if is_ip(host):
            if sni is not None:
                raise Refusal(403, "sni_mismatch")
        elif sni != host:
            raise Refusal(403, "sni_mismatch")
    elif port in HTTP_PORTS:
        value = None
        for line in first.split(b"\r\n")[1:]:
            name, sep, rest = line.partition(b":")
            if sep and name.strip().lower() == b"host":
                if value is not None:
                    raise Refusal(403, "host_mismatch")  # two Host headers
                value = rest.strip().decode("latin1")
        if value is None:
            raise Refusal(403, "host_mismatch")
        name, _, p = value.rpartition(":") if value.count(":") == 1 else (value, "", "")
        try:
            if normalize_host(name) != host or p not in ("", "80"):
                raise Refusal(403, "host_mismatch")
        except ValueError:
            raise Refusal(403, "host_mismatch") from None
