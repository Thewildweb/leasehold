"""The gateway: the local CONNECT proxy between a sandboxed browser and the registry
exits (``leasehold.exit``).

It runs next to the browser, as its own user, and is the only process that holds exit
tokens. The browser reaches only the gateway's listener (on a loopback address); the
controller tells the gateway over a Unix socket which route belongs to which browser
context.

Data plane (``LEASEHOLD_GATEWAY_LISTEN``). For each connection it:

1. reads one request head (size and time limited, ``head_timeout``):
   ``CONNECT host:port``, or an absolute-form plain-HTTP request
   (``GET http://host/path HTTP/1.1``). Anything else is refused (400/405);
2. checks ``Proxy-Authorization`` (Basic: the context name and the password the
   controller set). 407 when missing or unknown;
3. applies the context's host policy (``leasehold.gateway.policy``). 403 with the reason
   in ``X-Leasehold-Refused`` when refused;
4. opens ``CONNECT host:port`` at the context's exit with the lease id and exit token.
   An exit that refuses is relayed: 403, 429, 502, 503 and 504 as they are, anything
   else (407, 405, 400) as 502, with the exit's status in ``X-Leasehold-Exit-Status``.
   An exit that can't be reached is 502 (``exit_unreachable``) or 504
   (``exit_timeout``);
5. for CONNECT answers 200 and relays bytes both ways; for absolute form sends the
   request in origin form (``GET /path``, hop-by-hop and proxy headers removed,
   ``Connection: close``) through the tunnel (``CONNECT host:80``) with its
   ``Content-Length`` body, relays the response and closes: one request per tunnel.

A tunnel ends when either side closes, it is idle for ``idle_timeout``, its context is
closed, or (third party) the page window ends or the context's third-party bytes are
used up. The exit closes it when the token it was opened with expires.

Bytes are counted per context, both directions, exactly as relayed between the browser
and the site (TLS records included; the CONNECT heads to the exit are not counted).

Control socket (``LEASEHOLD_GATEWAY_CONTROL``, a Unix socket, mode 0600 by default).
Only peers whose uid is in ``LEASEHOLD_GATEWAY_CONTROL_UIDS`` (default: the gateway's
own uid) may talk to it (``SO_PEERCRED``). JSON lines: one object per line, one answer
per line, ``{"ok": true, ...}`` or ``{"ok": false, "error": "..."}``; an ``id`` in a
request is echoed in its answer. Operations (``op``):

- ``open_context``: ``ctx`` (name), ``password`` (16+ characters), ``lease_id``,
  ``target``, ``target_kind`` (``domain`` or ``ip``), ``page_host``, ``route`` (the
  lease's route dict, ``leasehold.client.Route``; its proxy username must be the lease
  id), ``limits`` (optional: ``third_party_tunnels``, ``third_party_bytes``,
  ``page_timeout_s``). A name that is already open is refused;
- ``add_redirect``: ``ctx``, ``host``: a first-party host for an ip target;
- ``page_start`` / ``page_end``: ``ctx``: the third-party window. ``page_end`` (and the
  page timeout) close the context's third-party tunnels;
- ``update_route``: ``route`` and ``ctx`` or ``lease_id`` (every context of the lease):
  the renewed route with its fresh token, for new tunnels;
- ``set_foreign_targets``: ``targets``: the targets with a live lease held by someone
  else, replacing the previous list. Third-party hosts are refused while the list is
  older than ``LEASEHOLD_GATEWAY_FOREIGN_MAX_AGE`` seconds or was never set;
- ``close_context``: ``ctx``: forgets its credentials, closes its tunnels and answers
  with its final ``stats``;
- ``stats``: ``ctx`` optional: per context, bytes up/down, tunnels (total, open, third
  party), refusals by reason and exit errors by status.

Settings come from the environment (``GatewaySettings.from_env``). Metrics on
``/metrics`` and liveness on ``/health`` (``LEASEHOLD_GATEWAY_METRICS_PORT``).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import hmac
import ipaddress
import json
import logging
import os
import re
import signal
import socket
import stat
import struct
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from prometheus_client import Counter, Gauge

from leasehold.exit.server import parse_authority
from leasehold.gateway import policy
from leasehold.gateway.policy import (
    KINDS,
    TLS_RECORD_MAX,
    Context,
    ControlError,
    Foreign,
    Hosts,
    Limits,
    Refusal,
    check_tunnel_start,
    normalize_host,
    parse_route,
)

log = logging.getLogger("leasehold.gateway")

HEAD_LIMIT = 65536  # browser request heads carry cookies
CONTROL_LIMIT = 16 * 2**20  # set_foreign_targets: ~4 MB at the API's 200k held targets
DEFAULT_PORTS = (80, 443)
CHUNK = 65536
REASON_HEADER = "X-Leasehold-Refused"
EXIT_STATUS_HEADER = "X-Leasehold-Exit-Status"
RELAYED_EXIT_STATUS = (403, 429, 502, 503, 504)
_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
# Hop-by-hop and proxy headers, not sent to the site in origin form.
_DROP = frozenset(
    {
        "proxy-authorization",
        "proxy-connection",
        "proxy-authenticate",
        "connection",
        "keep-alive",
        "te",
        "trailer",
        "upgrade",
        "host",
    }
)

REFUSED = Counter(
    "leasehold_gateway_refused_total",
    "Requests the gateway refused, by reason",
    ["reason"],
)
EXIT_ERRORS = Counter(
    "leasehold_gateway_exit_errors_total",
    "Tunnels the exit refused or could not open, by status",
    ["status"],
)
TUNNELS = Counter(
    "leasehold_gateway_tunnels_total",
    "Tunnels opened through an exit (party: first, third)",
    ["party"],
)
CLOSED = Counter(
    "leasehold_gateway_tunnels_closed_total",
    "Tunnels ended, by reason",
    ["reason"],
)
BYTES = Counter(
    "leasehold_gateway_bytes_total",
    "Bytes relayed (up: browser to site, down: site to browser)",
    ["direction"],
)
OPEN = Gauge("leasehold_gateway_open_tunnels", "Tunnels open now")
CONTEXTS = Gauge("leasehold_gateway_contexts", "Browser contexts open now")


@dataclass
class GatewaySettings:
    listen_host: str
    listen_port: int
    control_path: str
    control_mode: int = 0o600
    control_uids: tuple[int, ...] | None = None  # None: the gateway's own uid
    ports: tuple[int, ...] = DEFAULT_PORTS
    limits: Limits = Limits()  # per-context defaults
    foreign_max_age: float = 120.0  # 0: never stale
    metrics_host: str = "127.0.0.1"
    metrics_port: int | None = None
    head_timeout: float = 5.0
    connect_timeout: float = 10.0
    exit_timeout: float = 20.0  # the exit's answer to CONNECT (it resolves and connects)
    idle_timeout: float = 120.0
    max_connections: int = 1024
    max_pending: int = 256

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> GatewaySettings:
        """Required: ``LEASEHOLD_GATEWAY_LISTEN`` (host:port on a loopback address),
        ``LEASEHOLD_GATEWAY_CONTROL`` (the control socket's path). Optional:
        ``LEASEHOLD_GATEWAY_CONTROL_MODE`` (octal, default 600),
        ``LEASEHOLD_GATEWAY_CONTROL_UIDS`` (comma-separated, default: own uid),
        ``LEASEHOLD_GATEWAY_PORTS`` (default 80,443),
        ``LEASEHOLD_GATEWAY_THIRD_PARTY_TUNNELS`` (200), ``LEASEHOLD_GATEWAY_THIRD_PARTY_MB``
        (30), ``LEASEHOLD_GATEWAY_PAGE_TIMEOUT`` (60 s),
        ``LEASEHOLD_GATEWAY_FOREIGN_MAX_AGE`` (120 s), ``LEASEHOLD_GATEWAY_IDLE_TIMEOUT``,
        ``LEASEHOLD_GATEWAY_MAX_CONNECTIONS``, ``LEASEHOLD_GATEWAY_METRICS_PORT``,
        ``LEASEHOLD_GATEWAY_METRICS_HOST`` (default 127.0.0.1)."""
        e = os.environ if environ is None else environ
        required = ("LEASEHOLD_GATEWAY_LISTEN", "LEASEHOLD_GATEWAY_CONTROL")
        missing = [k for k in required if not e.get(k) or e[k].startswith("${")]
        if missing:
            raise ValueError(f"missing environment variables: {', '.join(missing)}")
        host, _, port = e["LEASEHOLD_GATEWAY_LISTEN"].rpartition(":")
        if not host or not port.isdigit():
            raise ValueError("LEASEHOLD_GATEWAY_LISTEN must be host:port")
        try:
            loopback = ipaddress.ip_address(host.strip("[]")).is_loopback
        except ValueError:
            loopback = False
        if not loopback:
            raise ValueError("the gateway listens on a loopback address only")
        uids = None
        if e.get("LEASEHOLD_GATEWAY_CONTROL_UIDS"):
            uids = tuple(
                int(u) for u in e["LEASEHOLD_GATEWAY_CONTROL_UIDS"].split(",") if u.strip()
            )
        ports = DEFAULT_PORTS
        if e.get("LEASEHOLD_GATEWAY_PORTS"):
            ports = tuple(int(p) for p in e["LEASEHOLD_GATEWAY_PORTS"].split(",") if p.strip())
        defaults = Limits()
        limits = Limits(
            third_party_tunnels=int(
                e.get("LEASEHOLD_GATEWAY_THIRD_PARTY_TUNNELS") or defaults.third_party_tunnels
            ),
            third_party_bytes=int(float(e.get("LEASEHOLD_GATEWAY_THIRD_PARTY_MB") or 30) * 2**20),
            page_timeout=float(e.get("LEASEHOLD_GATEWAY_PAGE_TIMEOUT") or defaults.page_timeout),
        )
        metrics = e.get("LEASEHOLD_GATEWAY_METRICS_PORT")
        return cls(
            listen_host=host.strip("[]"),
            listen_port=int(port),
            control_path=e["LEASEHOLD_GATEWAY_CONTROL"],
            control_mode=int(e.get("LEASEHOLD_GATEWAY_CONTROL_MODE") or "600", 8),
            control_uids=uids,
            ports=ports,
            limits=limits,
            foreign_max_age=float(e.get("LEASEHOLD_GATEWAY_FOREIGN_MAX_AGE") or 120),
            metrics_host=e.get("LEASEHOLD_GATEWAY_METRICS_HOST") or "127.0.0.1",
            metrics_port=int(metrics) if metrics else None,
            idle_timeout=float(e.get("LEASEHOLD_GATEWAY_IDLE_TIMEOUT") or 120),
            max_connections=int(e.get("LEASEHOLD_GATEWAY_MAX_CONNECTIONS") or 1024),
        )


def basic_credentials(value: str) -> tuple[str, str] | None:
    scheme, _, data = value.partition(" ")
    if scheme.lower() != "basic":
        return None
    try:
        user, sep, password = base64.b64decode(data.strip(), validate=True).decode().partition(":")
    except (binascii.Error, UnicodeDecodeError):
        return None
    return (user, password) if sep else None


@dataclass
class Request:
    method: str
    host: str
    port: int
    version: str
    headers: list[tuple[str, str]]  # as sent, in order
    path: str = ""  # origin form, absolute-form requests only

    @property
    def absolute(self) -> bool:
        return self.method != "CONNECT"

    def header(self, name: str) -> str | None:
        name = name.lower()
        for k, v in self.headers:
            if k.lower() == name:
                return v
        return None

    def origin_form(self) -> bytes:
        """The request as the site gets it: origin form, our own Host and
        ``Connection: close``, no proxy or hop-by-hop headers."""
        named = {
            t.strip().lower()
            for k, v in self.headers
            if k.lower() == "connection"
            for t in v.split(",")
        }
        authority = self.host if self.port == 80 else f"{self.host}:{self.port}"
        lines = [f"{self.method} {self.path} {self.version}", f"Host: {authority}"]
        for k, v in self.headers:
            if k.lower() in _DROP or k.lower() in named:
                continue
            lines.append(f"{k}: {v}")
        lines.append("Connection: close")
        return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")


def parse_head(head: bytes) -> Request:
    text = head.decode("latin-1")
    if text.endswith("\r\n\r\n"):
        text = text[:-4]
    # Bare CR or LF, or NUL: a head another parser could split differently (smuggling).
    if "\n" in text.replace("\r\n", "") or "\r" in text.replace("\r\n", "") or "\0" in text:
        raise Refusal(400, "bad_request")
    lines = text.split("\r\n")
    parts = lines[0].split(" ")
    if len(parts) != 3 or not parts[2].startswith("HTTP/1."):
        raise Refusal(400, "bad_request")
    method, target, version = parts
    headers = []
    for line in lines[1:]:
        if not line:
            continue
        if line[0] in " \t":
            raise Refusal(400, "bad_request")  # obs-fold (RFC 9112 5.2)
        k, sep, v = line.partition(":")
        if not sep or not k or k != k.strip():
            raise Refusal(400, "bad_request")
        headers.append((k, v.strip()))
    lengths = [v for k, v in headers if k.lower() == "content-length"]
    if len(lengths) > 1 or (lengths and any(k.lower() == "transfer-encoding" for k, _ in headers)):
        raise Refusal(400, "bad_request")  # two lengths, or a length and chunked
    if method == "CONNECT":
        try:
            host, port = parse_authority(target)
            host = normalize_host(host)
        except ValueError:
            raise Refusal(400, "bad_request") from None
        return Request(method, host, port, version, headers)
    if not re.fullmatch(r"[A-Z]{1,16}", method):
        raise Refusal(400, "bad_request")
    if not target.lower().startswith("http://"):
        # origin form (not a proxy request) or https:// (browsers CONNECT for that)
        raise Refusal(405, "method")
    try:
        url = urlsplit(target)
        port = url.port or 80
        if url.username is not None or url.password is not None:
            raise ValueError("user info")
        host = normalize_host(url.hostname or "")
    except ValueError:
        raise Refusal(400, "bad_request") from None
    path = url.path or "/"
    if url.query:
        path += "?" + url.query
    return Request(method, host, port, version, headers, path)


class _Tunnel:
    def __init__(self, ctx: Context, third: bool):
        self.ctx = ctx
        self.third = third
        self.stop = asyncio.Event()
        self.reason = ""
        self.done = asyncio.Event()

    def end(self, reason: str) -> None:
        if not self.stop.is_set():
            self.reason = reason
            self.stop.set()


@dataclass
class _Open:
    ctx: Context
    tunnels: set[_Tunnel] = field(default_factory=set)
    page_timer: asyncio.TimerHandle | None = None


class Gateway:
    def __init__(self, settings: GatewaySettings):
        self.s = settings
        self.contexts: dict[str, _Open] = {}
        self.foreign = Foreign()
        self.tunnel_slots = asyncio.Semaphore(settings.max_connections)
        self.pending = asyncio.Semaphore(settings.max_pending)
        self.server: asyncio.Server | None = None
        self.control_server: asyncio.Server | None = None
        uids = settings.control_uids
        self.control_uids = frozenset(uids if uids is not None else (os.getuid(),))
        self._ops: dict[str, Callable] = {
            "open_context": self._open_context,
            "add_redirect": self._add_redirect,
            "page_start": self._page_start,
            "page_end": self._page_end,
            "update_route": self._update_route,
            "set_foreign_targets": self._set_foreign,
            "close_context": self._close_context,
            "stats": self._stats,
        }

    # --- lifecycle ----------------------------------------------------------------------

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self.handle, self.s.listen_host, self.s.listen_port, limit=HEAD_LIMIT
        )
        path = self.s.control_path
        with contextlib.suppress(FileNotFoundError):
            if stat.S_ISSOCK(os.lstat(path).st_mode):
                os.unlink(path)  # left by a previous run
        old = os.umask(0o177)  # no window in which the socket is open to others
        try:
            self.control_server = await asyncio.start_unix_server(
                self.control, path, limit=CONTROL_LIMIT
            )
        finally:
            os.umask(old)
        os.chmod(path, self.s.control_mode)

    async def stop(self) -> None:
        for srv in (self.server, self.control_server):
            if srv is not None:
                srv.close()
        for name in list(self.contexts):
            await self._close(name, "shutdown")
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.s.control_path)

    @property
    def port(self) -> int:
        assert self.server is not None
        return self.server.sockets[0].getsockname()[1]

    # --- control ------------------------------------------------------------------------

    async def control(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            if not self._peer_allowed(writer):
                writer.write(b'{"ok": false, "error": "not allowed"}\n')
                await writer.drain()
                return
            while True:
                try:
                    line = await reader.readline()
                except (ValueError, asyncio.LimitOverrunError):
                    # Say so before closing: a silent close looks like a gateway crash.
                    writer.write(b'{"ok": false, "error": "message over the control limit"}\n')
                    await writer.drain()
                    break
                if not line:
                    break
                answer = await self.command(line)
                writer.write(json.dumps(answer).encode() + b"\n")
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    def _peer_allowed(self, writer: asyncio.StreamWriter) -> bool:
        sock = writer.get_extra_info("socket")
        opt = getattr(socket, "SO_PEERCRED", None)
        if sock is None or opt is None:
            return False  # no way to tell who is asking: refuse
        size = struct.calcsize("3i")
        _pid, uid, _gid = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, opt, size))
        return uid in self.control_uids

    async def command(self, line: bytes | str | dict) -> dict:
        """One control message, already parsed or as a JSON line; the answer."""
        msg_id = None
        try:
            msg = line if isinstance(line, dict) else json.loads(line)
            if not isinstance(msg, dict):
                raise ControlError("a message is a JSON object")
            msg_id = msg.get("id")
            op = self._ops.get(msg.get("op"))
            if op is None:
                raise ControlError(f"unknown op {msg.get('op')!r}")
            answer = {"ok": True, **(await op(msg))}
        except ControlError as e:
            answer = {"ok": False, "error": str(e)}
        except json.JSONDecodeError:
            answer = {"ok": False, "error": "not JSON"}
        except Exception:
            log.exception("gateway: control message failed")
            answer = {"ok": False, "error": "internal error"}
        if msg_id is not None:
            answer["id"] = msg_id
        return answer

    def _get(self, msg: dict) -> _Open:
        name = msg.get("ctx")
        o = self.contexts.get(name) if isinstance(name, str) else None
        if o is None:
            raise ControlError(f"no context {name!r}")
        return o

    @staticmethod
    def _str(msg: dict, key: str) -> str:
        v = msg.get(key)
        if not isinstance(v, str) or not v:
            raise ControlError(f"{key} is required")
        return v

    async def _open_context(self, msg: dict) -> dict:
        name = self._str(msg, "ctx")
        if not _NAME.match(name):
            raise ControlError("ctx: letters, digits, '.', '_' and '-' only")
        if name in self.contexts:
            raise ControlError(f"context {name!r} is already open")
        password = self._str(msg, "password")
        if len(password) < 16:
            raise ControlError("password: at least 16 characters")
        lease_id = self._str(msg, "lease_id")
        kind = self._str(msg, "target_kind")
        if kind not in KINDS:
            raise ControlError(f"target_kind must be one of {', '.join(KINDS)}")
        hosts = Hosts.create(kind, self._str(msg, "target"), self._str(msg, "page_host"))
        route, expires = parse_route(msg.get("route"), lease_id)
        limits = self.s.limits.merged(msg.get("limits"))
        ctx = Context(name, password, lease_id, route, expires, hosts, limits)
        self.contexts[name] = _Open(ctx)
        CONTEXTS.set(len(self.contexts))
        return {"ctx": name, "limits": limits.json()}

    async def _add_redirect(self, msg: dict) -> dict:
        o = self._get(msg)
        host = o.ctx.hosts.add_redirect(self._str(msg, "host"))
        return {"host": host}

    async def _page_start(self, msg: dict) -> dict:
        o = self._get(msg)
        if o.page_timer is not None:
            o.page_timer.cancel()
        timeout = o.ctx.limits.page_timeout
        o.ctx.page_deadline = time.monotonic() + timeout
        o.page_timer = asyncio.get_running_loop().call_later(
            timeout, self._end_page, o, "page_timeout"
        )
        return {"page_timeout_s": timeout}

    async def _page_end(self, msg: dict) -> dict:
        o = self._get(msg)
        return {"closed": self._end_page(o, "page_end")}

    def _end_page(self, o: _Open, reason: str) -> int:
        """The page window is over: no new third-party tunnels, the open ones close."""
        if o.page_timer is not None:
            o.page_timer.cancel()
            o.page_timer = None
        o.ctx.page_deadline = None
        n = 0
        for t in list(o.tunnels):
            if t.third and not t.stop.is_set():
                t.end(reason)
                n += 1
        return n

    async def _update_route(self, msg: dict) -> dict:
        if msg.get("ctx") is not None:
            targets = [self._get(msg)]
        else:
            lease_id = self._str(msg, "lease_id")
            targets = [o for o in self.contexts.values() if o.ctx.lease_id == lease_id]
            if not targets:
                raise ControlError(f"no context for lease {lease_id!r}")
        lease_id = targets[0].ctx.lease_id
        route, expires = parse_route(msg.get("route"), lease_id)
        for o in targets:
            o.ctx.route, o.ctx.route_expires = route, expires
        return {"updated": len(targets)}

    async def _set_foreign(self, msg: dict) -> dict:
        targets = msg.get("targets")
        if not isinstance(targets, list):
            raise ControlError("targets must be a list")
        if msg.get("complete", True) is not True:
            # A cut list would let the missing targets through: keep the old one, which
            # goes stale (third parties refused) unless a complete one follows.
            return {"applied": False, "count": len(self.foreign.targets), "skipped": 0}
        count, skipped = self.foreign.set(targets)
        if skipped:
            log.warning("foreign targets: %d entries skipped (not hosts)", skipped)
        return {"applied": True, "count": count, "skipped": skipped}

    async def _close_context(self, msg: dict) -> dict:
        self._get(msg)
        return {"stats": await self._close(msg["ctx"], "closed")}

    async def _close(self, name: str, reason: str) -> dict:
        o = self.contexts.pop(name)
        CONTEXTS.set(len(self.contexts))
        o.ctx.closed = True
        if o.page_timer is not None:
            o.page_timer.cancel()
        tunnels = list(o.tunnels)
        for t in tunnels:
            t.end(reason)
        if tunnels:
            await asyncio.wait([asyncio.create_task(t.done.wait()) for t in tunnels], timeout=5)
        return o.ctx.stats(len(o.tunnels))

    async def _stats(self, msg: dict) -> dict:
        if msg.get("ctx") is not None:
            items = [self._get(msg)]
        else:
            items = list(self.contexts.values())
        return {
            "contexts": {o.ctx.name: o.ctx.stats(len(o.tunnels)) for o in items},
            "foreign_targets": len(self.foreign.targets),
        }

    # --- data plane ---------------------------------------------------------------------

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.pending.locked():
            REFUSED.labels("busy").inc()
            await self._answer(writer, 503, "busy")
            return
        ctx: Context | None = None
        try:
            async with self.pending:
                req, o = await self._authorize(reader)
                ctx = o.ctx
                third = ctx.admit(
                    req.host,
                    req.port,
                    self.s.ports,
                    self.foreign,
                    self.s.foreign_max_age,
                    own=self._own_targets(ctx),
                )
                if req.absolute and req.header("transfer-encoding") is not None:
                    raise Refusal(411, "chunked_body")
                if self.tunnel_slots.locked():
                    raise Refusal(503, "busy")
                upstream = await self._open_exit(ctx, req)
        except Refusal as r:
            if r.reason.startswith("exit_"):
                status = r.reason.removeprefix("exit_")
                EXIT_ERRORS.labels(status).inc()
                if ctx is not None:
                    ctx.exit_errors[status] += 1
                await self._answer(writer, r.status, r.reason, exit_status=status)
            else:
                REFUSED.labels(r.reason).inc()
                if ctx is not None:
                    ctx.refused[r.reason] += 1
                await self._answer(writer, r.status, r.reason, auth=r.status == 407)
            return
        except Exception:
            REFUSED.labels("error").inc()
            log.exception("gateway: request failed")
            await self._answer(writer, 500, "error")
            return
        ur, uw = upstream
        if o.ctx.closed:  # closed while the exit was answering
            uw.close()
            REFUSED.labels("closed").inc()
            await self._answer(writer, 403, "closed")
            return
        t = _Tunnel(ctx, third)
        o.tunnels.add(t)
        ctx.tunnels += 1
        TUNNELS.labels("third" if third else "first").inc()
        try:
            async with self.tunnel_slots:
                OPEN.inc()
                try:
                    if req.absolute:
                        body = int(req.header("content-length") or 0)
                        await self._send(t, uw, req.origin_form(), up=True)
                        reason = await self._relay(t, reader, writer, ur, uw, up_limit=body)
                    else:
                        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                        await writer.drain()
                        try:
                            first = await asyncio.wait_for(
                                self._first_bytes(reader, req.port), self.s.head_timeout
                            )
                            check_tunnel_start(req.host, req.port, first)
                        except Refusal as r:
                            REFUSED.labels(r.reason).inc()
                            ctx.refused[r.reason] += 1
                            reason = r.reason
                        except (TimeoutError, asyncio.IncompleteReadError,
                                asyncio.LimitOverrunError):  # fmt: skip
                            reason = "no_start"
                        else:
                            await self._send(t, uw, first, up=True)
                            reason = await self._relay(t, reader, writer, ur, uw)
                    CLOSED.labels(reason).inc()
                except (ConnectionError, OSError, ValueError):
                    CLOSED.labels("error").inc()
                finally:
                    OPEN.dec()
                    for w in (uw, writer):
                        w.close()
        finally:
            o.tunnels.discard(t)
            t.done.set()

    async def _first_bytes(self, reader: asyncio.StreamReader, port: int) -> bytes:
        """What the browser sends first into a CONNECT tunnel, for check_tunnel_start:
        the first TLS record on 443, the request head on 80, nothing on other ports."""
        if port in policy.TLS_PORTS:
            head = await reader.readexactly(5)
            size = int.from_bytes(head[3:5], "big")
            if head[0] != 0x16 or size > TLS_RECORD_MAX:
                return head  # check_tunnel_start refuses it as not_tls
            return head + await reader.readexactly(size)
        if port in policy.HTTP_PORTS:
            return await reader.readuntil(b"\r\n\r\n")
        return b""

    def _own_targets(self, ctx: Context) -> Foreign:
        """The targets of the other open contexts (not this one's own target)."""
        own = Foreign()
        own.targets = frozenset(
            o.ctx.hosts.target
            for o in self.contexts.values()
            if o.ctx is not ctx and o.ctx.hosts.target != ctx.hosts.target
        )
        return own

    async def _authorize(self, reader: asyncio.StreamReader) -> tuple[Request, _Open]:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), self.s.head_timeout)
        except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            raise Refusal(400, "bad_request") from None
        req = parse_head(head)
        creds = basic_credentials(req.header("proxy-authorization") or "")
        if creds is None:
            raise Refusal(407, "no_auth")
        o = self.contexts.get(creds[0])
        if o is None or not _same(o.ctx.password, creds[1]):
            raise Refusal(407, "bad_auth")
        if req.absolute:
            cl = req.header("content-length")
            if cl is not None and (not cl.isdigit() or len(cl) > 15):
                raise Refusal(400, "bad_request")
        return req, o

    async def _open_exit(self, ctx: Context, req: Request):
        """A tunnel to ``req.host:req.port`` through the context's exit."""
        p = ctx.route.proxy
        try:
            ur, uw = await asyncio.wait_for(
                asyncio.open_connection(p.host, p.port, limit=HEAD_LIMIT),
                self.s.connect_timeout,
            )
        except TimeoutError:
            raise Refusal(504, "exit_timeout") from None
        except OSError:
            raise Refusal(502, "exit_unreachable") from None
        cred = base64.b64encode(f"{p.username}:{p.password}".encode()).decode()
        authority = f"{req.host}:{req.port}"
        try:
            uw.write(
                (
                    f"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n"
                    f"Proxy-Authorization: Basic {cred}\r\n\r\n"
                ).encode()
            )
            await uw.drain()
            head = await asyncio.wait_for(ur.readuntil(b"\r\n\r\n"), self.s.exit_timeout)
        except TimeoutError:
            uw.close()
            raise Refusal(504, "exit_timeout") from None
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, OSError):
            uw.close()
            raise Refusal(502, "exit_unreachable") from None
        parts = head.split(b" ", 2)
        status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        if status == 200:
            return ur, uw
        uw.close()
        raise Refusal(status if status in RELAYED_EXIT_STATUS else 502, f"exit_{status}")

    async def _send(self, t: _Tunnel, w: asyncio.StreamWriter, data: bytes, up: bool) -> bool:
        """Relay and count ``data``. A third-party tunnel relays only what is left of
        its context's third-party bytes and then ends (False)."""
        ctx = t.ctx
        ok = True
        if t.third:
            left = ctx.limits.third_party_bytes - ctx.third_party_bytes
            if len(data) > left:
                data = data[: max(0, left)]
                t.end("third_party_bytes")
                ok = False
            ctx.third_party_bytes += len(data)
        if up:
            ctx.bytes_up += len(data)
        else:
            ctx.bytes_down += len(data)
        BYTES.labels("up" if up else "down").inc(len(data))
        if data:
            w.write(data)
            await w.drain()
        return ok

    async def _relay(self, t: _Tunnel, cr, cw, ur, uw, up_limit: int | None = None) -> str:
        """Relay until done; returns why it stopped. With ``up_limit`` (absolute form)
        only that many bytes go up after the head, and the tunnel is done when the site
        has answered and closed."""
        last = [time.monotonic()]

        async def pipe(src, dst, up: bool, limit: int | None):
            left = limit
            try:
                while left is None or left > 0:
                    data = await src.read(CHUNK if left is None else min(CHUNK, left))
                    if not data:
                        break
                    last[0] = time.monotonic()
                    if not await self._send(t, dst, data, up):
                        break
                    if left is not None:
                        left -= len(data)
            finally:
                if limit is None and dst.can_write_eof():
                    with contextlib.suppress(OSError):
                        dst.write_eof()

        up_pipe = asyncio.create_task(pipe(cr, uw, True, up_limit))
        down_pipe = asyncio.create_task(pipe(ur, cw, False, None))
        stopper = asyncio.create_task(t.stop.wait())
        tasks = [up_pipe, down_pipe, stopper]
        try:
            while True:
                waiting = [x for x in tasks if not x.done()]
                await asyncio.wait(
                    waiting,
                    timeout=min(1.0, self.s.idle_timeout),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if t.stop.is_set():
                    return t.reason
                for p in (up_pipe, down_pipe):
                    if p.done() and p.exception() is not None:
                        return "error"
                if down_pipe.done() and (up_limit is not None or up_pipe.done()):
                    return "done"
                quiet = time.monotonic() - last[0]
                if up_limit is None and (up_pipe.done() or down_pipe.done()) and quiet > 5.0:
                    return "half_closed"
                if quiet > self.s.idle_timeout:
                    return "idle"
        finally:
            for x in tasks:
                x.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    async def _answer(
        writer: asyncio.StreamWriter,
        status: int,
        reason: str,
        auth: bool = False,
        exit_status: str | None = None,
    ) -> None:
        body = (reason + "\n").encode()
        head = [
            f"HTTP/1.1 {status} {reason}",
            "Connection: close",
            f"Content-Length: {len(body)}",
            f"{REASON_HEADER}: {reason}",
        ]
        if exit_status is not None:
            head.append(f"{EXIT_STATUS_HEADER}: {exit_status}")
        if auth:
            head.append('Proxy-Authenticate: Basic realm="gateway"')
        try:
            writer.write(("\r\n".join(head) + "\r\n\r\n").encode() + body)
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()


def _same(a: str, b: str) -> bool:
    # compare_digest on bytes: on str it needs ASCII, and a password may not be
    return hmac.compare_digest(a.encode(), b.encode())


def serve(settings: GatewaySettings) -> None:
    """Run until SIGTERM/SIGINT."""
    from leasehold.runtime import http as lh_http
    from leasehold.runtime import log as lh_log

    lh_log.setup("leasehold", "gateway", "", "", f"{settings.listen_host}:{settings.listen_port}")
    liveness = lh_http.Liveness(stall_after=60)
    if settings.metrics_port is not None:
        lh_http.serve(settings.metrics_port, liveness, host=settings.metrics_host)

    async def main() -> None:
        gw = Gateway(settings)
        await gw.start()
        log.info(
            "gateway listening",
            extra={
                "fields": {
                    "listen": f"{settings.listen_host}:{gw.port}",
                    "control": settings.control_path,
                }
            },
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            while not stop.is_set():
                liveness.beat()
                try:
                    await asyncio.wait_for(stop.wait(), 10)
                except TimeoutError:
                    pass
        finally:
            await gw.stop()

    asyncio.run(main())
