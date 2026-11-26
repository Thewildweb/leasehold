"""The exit: a CONNECT proxy on a VPS that lets a routed target lease leave from the
VPS's registry address.

One per VPS (a Nomad system job), listening on the VPS's private (tailnet) address
only. For each connection it:

1. reads one request head (size and time limited); anything but ``CONNECT host:port``
   is refused with 405. There is no forward-proxy mode: a client that needs plain
   ``http://`` opens ``CONNECT host:80`` and speaks HTTP inside the tunnel;
2. verifies ``Proxy-Authorization`` (Basic, lease id and exit token,
   ``leasehold.exit.token``) offline: right egress, not expired. 407 otherwise;
3. checks the port against the allowed list (default 80 and 443). 403 otherwise;
4. resolves the name itself, IPv4 only, and keeps only public unicast addresses outside
   the configured deny list (the operator's own hosts). It connects to the address it
   checked, so a name that changes between check and connect (DNS rebinding) can't
   reach a private address. 403 when nothing is left;
5. connects from the egress address (the socket is bound to it, so the source can't
   drift to another address or to IPv6), answers 200 and relays bytes both ways until
   either side closes or the connection is idle for ``idle_timeout``.

No TLS is terminated: the client's TLS handshake reaches the site unchanged.

Settings come from the environment (``ExitSettings.from_env``). Metrics on
``/metrics`` and liveness on ``/health`` (``LEASEHOLD_EXIT_METRICS_PORT``).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import ipaddress
import logging
import os
import signal
import socket
import time
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
OPEN = Gauge("leasehold_exit_open_connections", "Tunnels open now")

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
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
    deny: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = ()

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
    keys: list[bytes] = field(repr=False)
    destinations: Destinations = Destinations()
    metrics_port: int | None = None
    env: str = ""
    connect_timeout: float = 10.0
    head_timeout: float = 10.0
    idle_timeout: float = 120.0
    max_connections: int = 1024

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> ExitSettings:
        """``LEASEHOLD_EXIT_LISTEN`` (host:port, the private address),
        ``LEASEHOLD_EGRESS_IP``, ``LEASEHOLD_EXIT_KEYS`` (comma-separated, current first),
        and optionally ``LEASEHOLD_EXIT_PORTS`` (comma-separated), ``LEASEHOLD_EXIT_DENY``
        (comma-separated networks), ``LEASEHOLD_EXIT_METRICS_PORT``,
        ``LEASEHOLD_EXIT_IDLE_TIMEOUT``, ``LEASEHOLD_EXIT_MAX_CONNECTIONS``."""
        e = os.environ if environ is None else environ
        missing = [
            k
            for k in ("LEASEHOLD_EXIT_LISTEN", "LEASEHOLD_EGRESS_IP", "LEASEHOLD_EXIT_KEYS")
            if not e.get(k) or e[k].startswith("${")
        ]
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
        ports = DEFAULT_PORTS
        if e.get("LEASEHOLD_EXIT_PORTS"):
            ports = tuple(int(p) for p in e["LEASEHOLD_EXIT_PORTS"].split(",") if p.strip())
        deny = tuple(
            ipaddress.ip_network(n.strip(), strict=False)
            for n in (e.get("LEASEHOLD_EXIT_DENY") or "").split(",")
            if n.strip()
        )
        metrics = e.get("LEASEHOLD_EXIT_METRICS_PORT")
        return cls(
            listen_host=host,
            listen_port=int(port),
            egress=str(egress),
            keys=tk.parse_keys(e["LEASEHOLD_EXIT_KEYS"]),
            destinations=Destinations(ports=ports, deny=deny),
            metrics_port=int(metrics) if metrics else None,
            env=e.get("LEASEHOLD_ENV", ""),
            idle_timeout=float(e.get("LEASEHOLD_EXIT_IDLE_TIMEOUT") or 120),
            max_connections=int(e.get("LEASEHOLD_EXIT_MAX_CONNECTIONS") or 1024),
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


class Exit:
    def __init__(self, settings: ExitSettings, resolver: Resolver = resolve_ipv4):
        self.s = settings
        self.resolve = resolver
        self.slots = asyncio.Semaphore(settings.max_connections)
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
        if self.slots.locked():
            CONNECTIONS.labels("busy").inc()
            await self._answer(writer, 503, "busy")
            return
        async with self.slots:
            try:
                upstream = await self._open(reader)
            except Refused as r:
                CONNECTIONS.labels(r.reason).inc()
                await self._answer(writer, r.status, r.text, auth=r.status == 407)
                return
            except Exception:
                CONNECTIONS.labels("error").inc()
                log.exception("exit: request failed")
                await self._answer(writer, 500, "error")
                return
            CONNECTIONS.labels("ok").inc()
            up_reader, up_writer = upstream
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            OPEN.inc()
            try:
                await writer.drain()
                await self._relay(reader, writer, up_reader, up_writer)
            except (ConnectionError, OSError):
                pass
            finally:
                OPEN.dec()
                for w in (up_writer, writer):
                    w.close()

    async def _open(self, reader: asyncio.StreamReader):
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
            claims = tk.verify(self.s.keys, creds[0], creds[1], self.s.egress)
        except tk.TokenError as e:
            log.info("exit: token refused", extra={"fields": {"why": str(e)}})
            raise Refused(407, "bad_token", "proxy authentication required") from None
        if self.s.env and claims.env and claims.env != self.s.env:
            # One exit serves every environment by default (egress is shared); an exit
            # given LEASEHOLD_ENV serves only that one.
            raise Refused(407, "bad_token", "proxy authentication required")
        try:
            host, port = parse_authority(authority)
        except ValueError:
            raise Refused(400, "bad_request", "bad authority") from None
        if port not in self.s.destinations.ports:
            raise Refused(403, "port", "port not allowed")
        addr = await self._address(host, port)
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(addr, port, local_addr=(self.s.egress, 0)),
                self.s.connect_timeout,
            )
        except TimeoutError:
            raise Refused(504, "connect_timeout", "connect timed out") from None
        except OSError:
            raise Refused(502, "connect_failed", "connect failed") from None

    async def _address(self, host: str, port: int) -> str:
        try:
            literal: IPAddress | None = ipaddress.ip_address(host)
        except ValueError:
            literal = None
        if literal is not None:
            candidates = [literal]
        else:
            try:
                candidates = [ipaddress.ip_address(a) for a in await self.resolve(host, port)]
            except (OSError, UnicodeError):
                raise Refused(502, "dns", "name not resolved") from None
        why = "dns"
        for a in candidates:
            refusal = self.s.destinations.refusal(a)
            if not refusal:
                return str(a)
            why = refusal
        if why == "dns":
            raise Refused(502, "dns", "name not resolved")
        raise Refused(403, why, "destination not allowed")

    async def _relay(self, cr, cw, ur, uw) -> None:
        last = [time.monotonic()]

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter, direction: str):
            counter = BYTES.labels(direction)
            try:
                while True:
                    data = await src.read(CHUNK)
                    if not data:
                        break
                    last[0] = time.monotonic()
                    counter.inc(len(data))
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
                done, _ = await asyncio.wait(
                    tasks, timeout=min(5.0, self.s.idle_timeout), return_when=asyncio.ALL_COMPLETED
                )
                if len(done) == len(tasks):
                    break
                if any(t.done() for t in tasks) and time.monotonic() - last[0] > 5.0:
                    break  # one side finished and the other went quiet: half-closed tunnel
                if time.monotonic() - last[0] > self.s.idle_timeout:
                    break
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

    lh_log.setup("leasehold", "exit", settings.env or "shared", "", settings.egress)
    liveness = lh_http.Liveness(stall_after=60)
    if settings.metrics_port is not None:
        lh_http.serve(settings.metrics_port, liveness, host=settings.listen_host)

    async def main() -> None:
        ex = Exit(settings)
        server = await ex.start()
        log.info(
            "exit listening",
            extra={
                "fields": {"listen": f"{settings.listen_host}:{ex.port}", "egress": settings.egress}
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
