"""The exit: a CONNECT proxy on a VPS that lets a routed target lease leave from the
VPS's registry address.

One per VPS (a Nomad system job), listening on the VPS's private (tailnet) address
only. For each connection it:

1. reads one request head (size and time limited, ``head_timeout``); anything but
   ``CONNECT host:port`` is refused with 405. There is no forward-proxy mode: a client
   that needs plain ``http://`` opens ``CONNECT host:80`` and speaks HTTP inside;
2. verifies ``Proxy-Authorization`` (Basic, lease id and exit token,
   ``leasehold.exit.token``) offline: its own egress, an environment it serves, not
   expired. 407 otherwise;
3. checks the lease's limits: at most ``lease_tunnels`` open tunnels and
   ``lease_bytes`` bytes relayed per lease (429 when reached);
4. checks the port against the allowed list (default 80 and 443). 403 otherwise;
5. resolves the name itself (``dns_timeout``), IPv4 only, and keeps only public unicast
   addresses outside the deny list: the configured networks (the operator's own hosts:
   registry addresses, the control API, Kafka) and always the exit's own egress. It
   connects to the address it checked, so a name that changes between check and connect
   (DNS rebinding) can't reach a private address. 403 when nothing is left;
6. connects from the egress address (the socket is bound to it, so the source can't
   drift to another address or to IPv6), answers 200 and relays bytes both ways until
   either side closes, the tunnel is idle for ``idle_timeout``, the lease's byte limit
   is reached, or the token expires: no tunnel outlives the token it was opened with.

Before authentication a connection holds one of ``max_pending`` slots, at most
``per_source_pending`` from one source address; an authenticated tunnel holds one of
``max_connections``. A full server answers 503.

No TLS is terminated: the client's TLS handshake reaches the site unchanged.

Settings come from the environment (``ExitSettings.from_env``). Metrics on
``/metrics`` and liveness on ``/health`` (``LEASEHOLD_EXIT_METRICS_PORT``).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import dataclasses
import ipaddress
import logging
import os
import signal
import socket
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from prometheus_client import Counter, Gauge

from leasehold.exit import token as tk

log = logging.getLogger("leasehold.exit")

HEAD_LIMIT = 8192
DEFAULT_PORTS = (80, 443)
CHUNK = 65536

CONNECTIONS = Counter(
    "leasehold_exit_connections_total",
    "Connections by result (ok, or why refused)",
    ["result"],
)
BYTES = Counter(
    "leasehold_exit_bytes_total",
    "Bytes relayed (up: client to site, down: site to client)",
    ["direction"],
)
CLOSED = Counter(
    "leasehold_exit_tunnels_closed_total",
    "Tunnels closed by the exit, by reason (idle, expired, lease_bytes)",
    ["reason"],
)
OPEN = Gauge("leasehold_exit_open_connections", "Tunnels open now")

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
Network = ipaddress.IPv4Network | ipaddress.IPv6Network
Resolver = Callable[[str, int], Awaitable[list[str]]]


class Refused(Exception):
    def __init__(self, status: int, reason: str, text: str):
        super().__init__(reason)
        self.status = status
        self.reason = reason
        self.text = text


@dataclass(frozen=True)
class Destinations:
    """Which addresses and ports a tunnel may open to."""

    ports: tuple[int, ...] = DEFAULT_PORTS
    deny: tuple[Network, ...] = ()

    def refusal(self, addr: IPAddress) -> str:
        """'' when ``addr`` may be reached, else why not."""
        if addr.version != 4:
            return "ipv6"
        if not addr.is_global or addr.is_multicast:
            return "not_public"
        if any(addr in n for n in self.deny):
            return "denied"
        return ""


@dataclass
class ExitSettings:
    listen_host: str
    listen_port: int
    egress: str  # the registry address: the tunnels' source address
    keys: list[bytes] = field(repr=False)  # this exit's derived keys, current first
    envs: tuple[str, ...]  # the environments whose tokens it accepts
    destinations: Destinations = Destinations()
    metrics_port: int | None = None
    connect_timeout: float = 10.0
    head_timeout: float = 5.0
    dns_timeout: float = 5.0
    idle_timeout: float = 120.0
    max_connections: int = 1024
    max_pending: int = 64
    per_source_pending: int = 16
    lease_tunnels: int = 64
    lease_bytes: int = 256 * 2**20

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> ExitSettings:
        """Required: ``LEASEHOLD_EXIT_LISTEN`` (host:port, the private address),
        ``LEASEHOLD_EGRESS_IP``, ``LEASEHOLD_EXIT_KEYS`` (this exit's derived keys,
        comma-separated, current first: ``leasehold exit key``), ``LEASEHOLD_EXIT_ENVS``
        (comma-separated), ``LEASEHOLD_EXIT_DENY`` (comma-separated networks: the
        operator's own public addresses). Optional: ``LEASEHOLD_EXIT_PORTS``,
        ``LEASEHOLD_EXIT_METRICS_PORT``, ``LEASEHOLD_EXIT_IDLE_TIMEOUT``,
        ``LEASEHOLD_EXIT_MAX_CONNECTIONS``, ``LEASEHOLD_EXIT_LEASE_TUNNELS``,
        ``LEASEHOLD_EXIT_LEASE_MB``."""
        e = os.environ if environ is None else environ
        required = (
            "LEASEHOLD_EXIT_LISTEN",
            "LEASEHOLD_EGRESS_IP",
            "LEASEHOLD_EXIT_KEYS",
            "LEASEHOLD_EXIT_ENVS",
            "LEASEHOLD_EXIT_DENY",
        )
        missing = [k for k in required if not e.get(k) or e[k].startswith("${")]
        if missing:
            raise ValueError(f"missing environment variables: {', '.join(missing)}")
        host, _, port = e["LEASEHOLD_EXIT_LISTEN"].rpartition(":")
        if not host or not port.isdigit():
            raise ValueError("LEASEHOLD_EXIT_LISTEN must be host:port")
        if host in ("0.0.0.0", "::", ""):
            raise ValueError("the exit listens on one private address, not on all of them")
        egress = ipaddress.ip_address(e["LEASEHOLD_EGRESS_IP"])
        if egress.version != 4:
            raise ValueError("LEASEHOLD_EGRESS_IP must be an IPv4 address")
        envs = tuple(x.strip() for x in e["LEASEHOLD_EXIT_ENVS"].split(",") if x.strip())
        if not envs:
            raise ValueError("LEASEHOLD_EXIT_ENVS names no environment")
        ports = DEFAULT_PORTS
        if e.get("LEASEHOLD_EXIT_PORTS"):
            ports = tuple(int(p) for p in e["LEASEHOLD_EXIT_PORTS"].split(",") if p.strip())
        deny = tuple(
            ipaddress.ip_network(n.strip(), strict=False)
            for n in e["LEASEHOLD_EXIT_DENY"].split(",")
            if n.strip()
        )
        if not deny:
            raise ValueError("LEASEHOLD_EXIT_DENY names no network")
        metrics = e.get("LEASEHOLD_EXIT_METRICS_PORT")
        return cls(
            listen_host=host,
            listen_port=int(port),
            egress=str(egress),
            keys=tk.parse_keys(e["LEASEHOLD_EXIT_KEYS"]),
            envs=envs,
            destinations=Destinations(ports=ports, deny=deny),
            metrics_port=int(metrics) if metrics else None,
            idle_timeout=float(e.get("LEASEHOLD_EXIT_IDLE_TIMEOUT") or 120),
            max_connections=int(e.get("LEASEHOLD_EXIT_MAX_CONNECTIONS") or 1024),
            lease_tunnels=int(e.get("LEASEHOLD_EXIT_LEASE_TUNNELS") or 64),
            lease_bytes=int(float(e.get("LEASEHOLD_EXIT_LEASE_MB") or 256) * 2**20),
        )


async def resolve_ipv4(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(
        host, port, family=socket.AF_INET, type=socket.SOCK_STREAM
    )
    out: list[str] = []
    for info in infos:
        a = info[4][0]
        if a not in out:
            out.append(a)
    return out


def parse_authority(text: str) -> tuple[str, int]:
    """``host:port`` or ``[v6]:port`` of a CONNECT request."""
    if text.startswith("["):
        host, sep, rest = text[1:].partition("]")
        if not sep or not rest.startswith(":"):
            raise ValueError(text)
        port = rest[1:]
    else:
        host, sep, port = text.rpartition(":")
        if not sep:
            raise ValueError(text)
    if not host or not port.isdigit() or not 0 < int(port) < 65536:
        raise ValueError(text)
    return host.lower().rstrip("."), int(port)


def _credentials(headers: dict[str, str]) -> tuple[str, str] | None:
    value = headers.get("proxy-authorization", "")
    scheme, _, data = value.partition(" ")
    if scheme.lower() != "basic":
        return None
    try:
        user, sep, password = base64.b64decode(data.strip(), validate=True).decode().partition(":")
    except (binascii.Error, UnicodeDecodeError):
        return None
    return (user, password) if sep else None


@dataclass
class _LeaseUse:
    expires_at: float
    open: int = 0
    bytes: int = 0


class Exit:
    def __init__(self, settings: ExitSettings, resolver: Resolver = resolve_ipv4):
        self.s = settings
        self.resolve = resolver
        own = ipaddress.ip_network(f"{settings.egress}/32")
        d = settings.destinations
        self.destinations = dataclasses.replace(d, deny=(*d.deny, own))
        self.tunnels = asyncio.Semaphore(settings.max_connections)
        self.pending = asyncio.Semaphore(settings.max_pending)
        self.pending_by_source: dict[str, int] = defaultdict(int)
        self.leases: dict[str, _LeaseUse] = {}
        self.server: asyncio.Server | None = None

    async def start(self) -> asyncio.Server:
        self.server = await asyncio.start_server(
            self.handle, self.s.listen_host, self.s.listen_port, limit=HEAD_LIMIT
        )
        return self.server

    @property
    def port(self) -> int:
        assert self.server is not None
        return self.server.sockets[0].getsockname()[1]

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = (writer.get_extra_info("peername") or ("?",))[0]
        if self.pending.locked() or self.pending_by_source[peer] >= self.s.per_source_pending:
            CONNECTIONS.labels("busy").inc()
            await self._answer(writer, 503, "busy")
            return
        self.pending_by_source[peer] += 1
        try:
            async with self.pending:
                opened = await self._authorize(reader)
        except Refused as r:
            CONNECTIONS.labels(r.reason).inc()
            await self._answer(writer, r.status, r.text, auth=r.status == 407)
            return
        except Exception:
            CONNECTIONS.labels("error").inc()
            log.exception("exit: request failed")
            await self._answer(writer, 500, "error")
            return
        finally:
            self.pending_by_source[peer] -= 1
            if self.pending_by_source[peer] <= 0:
                del self.pending_by_source[peer]
        claims, use, upstream = opened
        try:
            await self._tunnel(writer, reader, claims, use, upstream)
        finally:
            use.open -= 1

    async def _authorize(self, reader: asyncio.StreamReader):
        """Head, token, lease limits, destination; returns the open upstream."""
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), self.s.head_timeout)
        except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            raise Refused(400, "bad_request", "bad request") from None
        lines = head.decode("latin-1").split("\r\n")
        parts = lines[0].split(" ")
        if len(parts) != 3 or not parts[2].startswith("HTTP/1."):
            raise Refused(400, "bad_request", "bad request")
        method, authority, _ = parts
        if method != "CONNECT":
            raise Refused(405, "method", "only CONNECT")
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        creds = _credentials(headers)
        if creds is None:
            raise Refused(407, "no_auth", "proxy authentication required")
        try:
            claims = tk.verify(self.s.keys, creds[0], creds[1], self.s.egress, self.s.envs)
        except tk.TokenError as e:
            log.info("exit: token refused", extra={"fields": {"why": str(e)}})
            raise Refused(407, "bad_token", "proxy authentication required") from None
        try:
            host, port = parse_authority(authority)
        except ValueError:
            raise Refused(400, "bad_request", "bad authority") from None
        if port not in self.destinations.ports:
            raise Refused(403, "port", "port not allowed")
        use = self._lease(claims)
        if use.open >= self.s.lease_tunnels:
            raise Refused(429, "lease_tunnels", "too many tunnels for this lease")
        if use.bytes >= self.s.lease_bytes:
            raise Refused(429, "lease_bytes", "byte limit of this lease reached")
        addr = await self._address(host, port)
        if self.tunnels.locked():
            raise Refused(503, "busy", "busy")
        use.open += 1
        try:
            upstream = await asyncio.wait_for(
                asyncio.open_connection(addr, port, local_addr=(self.s.egress, 0)),
                self.s.connect_timeout,
            )
        except TimeoutError:
            use.open -= 1
            raise Refused(504, "connect_timeout", "connect timed out") from None
        except OSError:
            use.open -= 1
            raise Refused(502, "connect_failed", "connect failed") from None
        return claims, use, upstream

    def _lease(self, claims: tk.Claims) -> _LeaseUse:
        now = time.time()
        for k in [k for k, u in self.leases.items() if u.expires_at <= now and u.open <= 0]:
            del self.leases[k]
        use = self.leases.get(claims.lease_id)
        if use is None:
            use = self.leases[claims.lease_id] = _LeaseUse(claims.expires_at)
        use.expires_at = max(use.expires_at, claims.expires_at)
        return use

    async def _address(self, host: str, port: int) -> str:
        try:
            literal: IPAddress | None = ipaddress.ip_address(host)
        except ValueError:
            literal = None
        if literal is not None:
            candidates = [literal]
        else:
            try:
                found = await asyncio.wait_for(self.resolve(host, port), self.s.dns_timeout)
                candidates = [ipaddress.ip_address(a) for a in found]
            except TimeoutError:
                raise Refused(504, "dns_timeout", "name lookup timed out") from None
            except (OSError, UnicodeError, ValueError):
                raise Refused(502, "dns", "name not resolved") from None
        why = "dns"
        for a in candidates:
            refusal = self.destinations.refusal(a)
            if not refusal:
                return str(a)
            why = refusal
        if why == "dns":
            raise Refused(502, "dns", "name not resolved")
        raise Refused(403, why, "destination not allowed")

    async def _tunnel(self, writer, reader, claims: tk.Claims, use: _LeaseUse, upstream) -> None:
        up_reader, up_writer = upstream
        async with self.tunnels:
            CONNECTIONS.labels("ok").inc()
            OPEN.inc()
            try:
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
                reason = await self._relay(reader, writer, up_reader, up_writer, claims, use)
                CLOSED.labels(reason).inc()
            except (ConnectionError, OSError):
                pass
            finally:
                OPEN.dec()
                for w in (up_writer, writer):
                    w.close()

    async def _relay(self, cr, cw, ur, uw, claims: tk.Claims, use: _LeaseUse) -> str:
        """Relay until done; returns why it stopped."""
        last = [time.monotonic()]
        over = asyncio.Event()

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter, direction: str):
            counter = BYTES.labels(direction)
            try:
                while True:
                    data = await src.read(CHUNK)
                    if not data:
                        break
                    last[0] = time.monotonic()
                    counter.inc(len(data))
                    use.bytes += len(data)
                    if use.bytes > self.s.lease_bytes:
                        over.set()
                        break
                    dst.write(data)
                    await dst.drain()
            finally:
                if dst.can_write_eof():
                    try:
                        dst.write_eof()
                    except OSError:
                        pass

        tasks = [
            asyncio.create_task(pipe(cr, uw, "up")),
            asyncio.create_task(pipe(ur, cw, "down")),
        ]
        try:
            while True:
                left = claims.expires_at - time.time()
                if left <= 0:
                    return "expired"
                done, _ = await asyncio.wait(
                    tasks,
                    timeout=min(1.0, left, self.s.idle_timeout),
                    return_when=asyncio.ALL_COMPLETED,
                )
                if over.is_set():
                    return "lease_bytes"
                if len(done) == len(tasks):
                    return "done"
                quiet = time.monotonic() - last[0]
                if any(t.done() for t in tasks) and quiet > 5.0:
                    return "half_closed"
                if quiet > self.s.idle_timeout:
                    return "idle"
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    async def _answer(
        writer: asyncio.StreamWriter, status: int, text: str, auth: bool = False
    ) -> None:
        body = (text + "\n").encode()
        head = [f"HTTP/1.1 {status} {text}", "Connection: close", f"Content-Length: {len(body)}"]
        if auth:
            head.append('Proxy-Authenticate: Basic realm="exit"')
        try:
            writer.write(("\r\n".join(head) + "\r\n\r\n").encode() + body)
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()


def serve(settings: ExitSettings) -> None:
    """Run until SIGTERM/SIGINT."""
    from leasehold.runtime import http as lh_http
    from leasehold.runtime import log as lh_log

    lh_log.setup("leasehold", "exit", ",".join(settings.envs), "", settings.egress)
    liveness = lh_http.Liveness(stall_after=60)
    if settings.metrics_port is not None:
        lh_http.serve(settings.metrics_port, liveness, host=settings.listen_host)

    async def main() -> None:
        ex = Exit(settings)
        server = await ex.start()
        log.info(
            "exit listening",
            extra={
                "fields": {
                    "listen": f"{settings.listen_host}:{ex.port}",
                    "egress": settings.egress,
                }
            },
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        async with server:
            while not stop.is_set():
                liveness.beat()
                try:
                    await asyncio.wait_for(stop.wait(), 10)
                except TimeoutError:
                    pass

    asyncio.run(main())
