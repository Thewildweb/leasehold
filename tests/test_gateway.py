"""The browser gateway: control socket, per-lease host policy and the relay, through the
real exit (``leasehold.exit.server.Exit``) to local echo and HTTP servers (local sockets
only)."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import stat
import time
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client import REGISTRY

from leasehold.exit import token as tk
from leasehold.exit.server import Destinations, Exit, ExitSettings
from leasehold.gateway.policy import (
    Context,
    ControlError,
    Foreign,
    Hosts,
    Limits,
    Refusal,
    normalize_host,
    parse_route,
    registrable,
)
from leasehold.gateway.server import Gateway, GatewaySettings, parse_head

MASTER = bytes(range(32))
EGRESS = "127.0.0.2"
KEY = tk.exit_key(MASTER, EGRESS)
LEASE = "6f1c2a7e-0000-4000-8000-000000000001"
PASSWORD = "p" * 24


# --- policy -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "host, want",
    [("WWW.Example.NL.", "www.example.nl"), ("[::1]", "::1"), ("93.184.216.34", "93.184.216.34")],
)
def test_normalize_host(host, want):
    assert normalize_host(host) == want


@pytest.mark.parametrize("host", ["", "exa mple.nl", "a..b", "-a.nl", "bäcker.nl"])
def test_normalize_host_refuses(host):
    with pytest.raises(ValueError):
        normalize_host(host)


def test_registrable_uses_the_public_suffix_list():
    assert registrable("cdn.shop.example.co.uk") == "example.co.uk"
    assert registrable("www.example.nl") == "example.nl"
    assert registrable("co.uk") is None
    assert registrable("93.184.216.34") is None


def test_domain_target_first_party_is_its_registrable_domain():
    h = Hosts.create("domain", "example.nl", "www.example.nl")
    assert h.party("example.nl") == "target"
    assert h.party("img.cdn.example.nl") == "target"
    assert h.party("badexample.nl") == "third"
    assert h.party("example.nl.evil.com") == "third"
    with pytest.raises(ControlError, match="redirects"):
        h.add_redirect("other.nl")


def test_ip_target_first_party_is_the_page_host_and_its_redirects():
    h = Hosts.create("ip", "93.184.216.34", "shop.example.nl")
    assert h.party("shop.example.nl") == "target"
    assert h.party("93.184.216.34") == "target"
    assert h.party("www.shop.example.nl") == "third"  # not the whole domain
    h.add_redirect("WWW.shop.example.nl.")
    assert h.party("www.shop.example.nl") == "redirect"


@pytest.mark.parametrize(
    "kind, target, page, match",
    [
        ("dns", "example.nl", "example.nl", "target_kind"),
        ("domain", "co.uk", "x.co.uk", "registrable"),
        ("domain", "example.nl", "www.other.nl", "not under"),
        ("ip", "example.nl", "example.nl", "IPv4"),
        ("ip", "2001:db8::1", "example.nl", "IPv4"),
    ],
)
def test_hosts_refusals(kind, target, page, match):
    with pytest.raises(ControlError, match=match):
        Hosts.create(kind, target, page)


def test_limits_merge_and_refuse_unknown_or_bad_values():
    lim = Limits().merged({"third_party_tunnels": 5, "page_timeout_s": 2})
    assert (lim.third_party_tunnels, lim.third_party_bytes, lim.page_timeout) == (
        5,
        30 * 2**20,
        2.0,
    )
    for bad in ({"tunnels": 1}, {"third_party_bytes": 0}, {"page_timeout_s": True}, [1]):
        with pytest.raises(ControlError):
            Limits().merged(bad)


def test_foreign_targets_match_hosts_and_their_registrable_domain():
    f = Foreign()
    assert not f.fresh(0.0, 120)  # never set: stale
    f.set(["Shop.NL", "198.51.100.9"], now=100.0)
    assert f.fresh(150.0, 120) and not f.fresh(300.0, 120) and f.fresh(1e9, 0)
    assert f.holds("shop.nl") and f.holds("www.shop.nl") and f.holds("198.51.100.9")
    assert not f.holds("myshop.nl")


def _route(lease=LEASE, master=MASTER, expires=None, port=1, host="127.0.0.1") -> dict:
    exp = expires or datetime.now(UTC) + timedelta(minutes=5)
    user, pw = tk.sign_lease(master, lease, EGRESS, "example.nl", "dev", exp)
    return {
        "egress": EGRESS,
        "kind": "datacenter",
        "account": "exit",
        "proxy": {"scheme": "http", "host": host, "port": port, "username": user, "password": pw},
        "expires_at": exp.isoformat(),
    }


def test_parse_route_checks_the_lease_and_scheme():
    route, expires = parse_route(_route(), LEASE)
    assert route.proxy.username == LEASE and expires is not None
    with pytest.raises(ControlError, match="another lease"):
        parse_route(_route(lease="other"), LEASE)
    bad = _route()
    bad["proxy"]["scheme"] = "socks5h"
    with pytest.raises(ControlError, match="http"):
        parse_route(bad, LEASE)
    with pytest.raises(ControlError):
        parse_route({"egress": EGRESS}, LEASE)
    assert "password" not in repr(route) and route.proxy.password not in repr(route)


def _ctx(kind="domain", target="example.nl", page="www.example.nl", **limits) -> Context:
    route, exp = parse_route(_route(), LEASE)
    return Context(
        "ctx-1",
        PASSWORD,
        LEASE,
        route,
        exp,
        Hosts.create(kind, target, page),
        Limits().merged(limits),
    )


def _reason(fn) -> str:
    try:
        fn()
    except Refusal as r:
        return r.reason
    return ""


def test_admit_rules_in_order():
    ctx = _ctx(third_party_tunnels=2)
    fresh = Foreign()
    fresh.set(["leased.nl"], now=0.0)
    ports = (80, 443)

    def admit(host, port=443, foreign=fresh, now=1.0):
        return lambda: ctx.admit(host, port, ports, foreign, 120, now=now)

    assert ctx.admit("www.example.nl", 443, ports, Foreign(), 120, now=1.0) is False
    assert _reason(admit("www.example.nl", 22)) == "port"
    assert _reason(admit("::1")) == "ipv6"
    for private in ("127.0.0.1", "10.0.0.1", "169.254.169.254", "100.83.247.12", "localhost",
                    "x.localhost", "0.0.0.0"):  # fmt: skip
        assert _reason(admit(private)) == "private_address", private
    assert _reason(admit("cdn.other.nl")) == "no_page"
    ctx.page_deadline = 10.0
    assert _reason(admit("cdn.other.nl", foreign=Foreign())) == "foreign_stale"
    assert _reason(admit("cdn.other.nl", now=200.0)) == "no_page"  # past the deadline
    assert _reason(admit("www.leased.nl")) == "foreign_target"
    assert ctx.admit("cdn.other.nl", 443, ports, fresh, 120, now=1.0) is True
    assert ctx.admit("fonts.other.nl", 443, ports, fresh, 120, now=1.0) is True
    assert _reason(admit("cdn.other.nl")) == "third_party_tunnels"
    ctx.route_expires = time.time() - 1
    assert _reason(admit("www.example.nl")) == "route_expired"
    ctx.closed = True
    assert _reason(admit("www.example.nl")) == "closed"


def test_redirect_hosts_are_checked_against_foreign_targets():
    ctx = _ctx("ip", "93.184.216.34", "shop.nl")
    ctx.hosts.add_redirect("leased.nl")
    f = Foreign()
    f.set(["leased.nl"], now=0.0)
    assert _reason(lambda: ctx.admit("leased.nl", 443, (443,), f, 120, now=1.0)) == (
        "foreign_target"
    )
    assert ctx.admit("shop.nl", 443, (443,), f, 120, now=1.0) is False


@pytest.mark.parametrize(
    "head, reason",
    [
        (b"CONNECT", "bad_request"),
        (b"CONNECT example.nl HTTP/1.1\r\n\r\n", "bad_request"),
        (b"GET / HTTP/1.1\r\nHost: x\r\n\r\n", "method"),
        (b"GET https://example.nl/ HTTP/1.1\r\n\r\n", "method"),
        (b"GET http://u:p@example.nl/ HTTP/1.1\r\n\r\n", "bad_request"),
        (b"GET http://example.nl/ HTTP/1.1\r\n folded\r\n\r\n", "bad_request"),
    ],
)
def test_parse_head_refusals(head, reason):
    with pytest.raises(Refusal) as e:
        parse_head(head)
    assert e.value.reason == reason


def test_origin_form_drops_proxy_and_hop_by_hop_headers():
    req = parse_head(
        b"GET http://Example.NL:80/a?b=1 HTTP/1.1\r\nHost: wrong\r\nUser-Agent: UA\r\n"
        b"Proxy-Authorization: Basic eA==\r\nProxy-Connection: keep-alive\r\n"
        b"Connection: keep-alive, X-Hop\r\nX-Hop: 1\r\nCookie: a=b\r\n\r\n"
    )
    assert (req.host, req.port, req.path) == ("example.nl", 80, "/a?b=1")
    assert req.origin_form() == (
        b"GET /a?b=1 HTTP/1.1\r\nHost: example.nl\r\nUser-Agent: UA\r\nCookie: a=b\r\n"
        b"Connection: close\r\n\r\n"
    )


# --- settings ---------------------------------------------------------------------------

BASE_ENV = {
    "LEASEHOLD_GATEWAY_LISTEN": "127.0.0.1:3128",
    "LEASEHOLD_GATEWAY_CONTROL": "/alloc/gateway.sock",
}


def test_settings_from_env():
    s = GatewaySettings.from_env(
        {
            **BASE_ENV,
            "LEASEHOLD_GATEWAY_THIRD_PARTY_MB": "5",
            "LEASEHOLD_GATEWAY_CONTROL_UIDS": "1,2",
        }
    )
    assert (s.listen_host, s.listen_port, s.control_path) == (
        "127.0.0.1",
        3128,
        BASE_ENV["LEASEHOLD_GATEWAY_CONTROL"],
    )
    assert s.control_mode == 0o600 and s.control_uids == (1, 2)
    assert s.limits.third_party_bytes == 5 * 2**20 and s.limits.third_party_tunnels == 200
    assert s.ports == (80, 443) and s.metrics_host == "127.0.0.1"


@pytest.mark.parametrize(
    "env, match",
    [
        ({"LEASEHOLD_GATEWAY_CONTROL": ""}, "LEASEHOLD_GATEWAY_CONTROL"),
        ({"LEASEHOLD_GATEWAY_LISTEN": "0.0.0.0:3128"}, "loopback"),
        ({"LEASEHOLD_GATEWAY_LISTEN": "10.0.0.1:3128"}, "loopback"),
        ({"LEASEHOLD_GATEWAY_LISTEN": "3128"}, "host:port"),
    ],
)
def test_settings_refusals(env, match):
    with pytest.raises(ValueError, match=match):
        GatewaySettings.from_env({**BASE_ENV, **env})


# --- the stack: gateway -> real exit -> local servers ------------------------------------


class _Loopback(Destinations):
    def refusal(self, addr):
        return "" if str(addr) == "127.0.0.1" else super().refusal(addr)


async def _any_name_is_local(host, port):
    return ["127.0.0.1"]


class Stack:
    """Echo server, HTTP server, the exit and the gateway, all on loopback."""

    async def start(self, tmp_path, exit_ports=None, **settings):
        self.http_heads: list[bytes] = []
        self.echo = await asyncio.start_server(self._echo, "127.0.0.1", 0)
        self.http = await asyncio.start_server(self._http, "127.0.0.1", 0)
        self.echo_port = self.echo.sockets[0].getsockname()[1]
        self.http_port = self.http.sockets[0].getsockname()[1]
        ports = (self.echo_port, self.http_port)
        self.exit = Exit(
            ExitSettings(
                listen_host="127.0.0.1",
                listen_port=0,
                egress=EGRESS,
                keys=[KEY],
                envs=("dev",),
                destinations=_Loopback(ports=exit_ports or ports),
            ),
            resolver=_any_name_is_local,
        )
        await self.exit.start()
        self.sock = str(tmp_path / "gateway.sock")
        settings.setdefault("ports", (*ports, 1))
        self.gw = Gateway(
            GatewaySettings(
                listen_host="127.0.0.1", listen_port=0, control_path=self.sock, **settings
            )
        )
        await self.gw.start()
        await self.ctl({"op": "set_foreign_targets", "targets": ["leased.nl"]})
        return self

    async def stop(self):
        await self.gw.stop()
        for s in (self.exit.server, self.echo, self.http):
            s.close()

    async def _echo(self, r, w):
        try:
            while data := await r.read(65536):
                w.write(data)
                await w.drain()
        except ConnectionError:
            pass
        w.close()

    async def _http(self, r, w):
        head = await r.readuntil(b"\r\n\r\n")
        self.http_heads.append(head)
        cl = [ln for ln in head.split(b"\r\n") if ln.lower().startswith(b"content-length:")]
        body = await r.readexactly(int(cl[0].split(b":")[1])) if cl else b""
        payload = b"got " + body
        w.write(
            b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(payload)
        )
        w.write(payload)
        await w.drain()
        w.close()

    async def ctl(self, msg: dict) -> dict:
        return await self.gw.command(msg)

    def route(self, **kw) -> dict:
        return _route(port=self.exit.port, **kw)

    async def open(self, ctx="ctx-1", target="example.nl", kind="domain", page=None, **kw):
        msg = {
            "op": "open_context",
            "ctx": ctx,
            "password": PASSWORD,
            "lease_id": LEASE,
            "target": target,
            "target_kind": kind,
            "page_host": page or (f"www.{target}" if kind == "domain" else "shop.nl"),
            "route": kw.pop("route", None) or self.route(),
        }
        msg.update(kw)
        a = await self.ctl(msg)
        assert a["ok"], a
        return a

    async def connect(self, host, port=None, auth=("ctx-1", PASSWORD), line=None, extra=b""):
        r, w = await asyncio.open_connection("127.0.0.1", self.gw.port)
        port = port or self.echo_port
        head = [line or f"CONNECT {host}:{port} HTTP/1.1"]
        if auth:
            cred = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
            head.append(f"Proxy-Authorization: Basic {cred}")
        w.write(("\r\n".join(head) + "\r\n\r\n").encode() + extra)
        await w.drain()
        status = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 5)
        return status, r, w


def run(tmp_path, body, **settings):
    async def main():
        s = await Stack().start(tmp_path, **settings)
        try:
            return await body(s)
        finally:
            await s.stop()

    return asyncio.run(main())


def _header(status: bytes, name: str) -> str | None:
    for line in status.decode().split("\r\n"):
        k, _, v = line.partition(":")
        if k.lower() == name.lower():
            return v.strip()
    return None


def test_connect_first_party_relays_and_counts_bytes_exactly(tmp_path):
    async def body(s: Stack):
        await s.open()
        status, r, w = await s.connect("www.example.nl")
        assert status.startswith(b"HTTP/1.1 200")
        w.write(b"x" * 5000)
        await w.drain()
        assert await asyncio.wait_for(r.readexactly(5000), 5) == b"x" * 5000
        w.close()
        await asyncio.sleep(0.1)
        return (await s.ctl({"op": "stats", "ctx": "ctx-1"}))["contexts"]["ctx-1"]

    st = run(tmp_path, body)
    assert (st["bytes_up"], st["bytes_down"], st["bytes"]) == (5000, 5000, 10000)
    assert st["tunnels"] == 1 and st["tunnels_open"] == 0 and st["third_party_tunnels"] == 0


def test_absolute_form_get_goes_through_a_connect_tunnel_in_origin_form(tmp_path):
    async def body(s: Stack):
        await s.open()
        line = f"POST http://www.example.nl:{s.http_port}/form?a=1 HTTP/1.1"
        r, w = await asyncio.open_connection("127.0.0.1", s.gw.port)
        cred = base64.b64encode(f"ctx-1:{PASSWORD}".encode()).decode()
        req = (
            f"{line}\r\nHost: www.example.nl\r\nProxy-Authorization: Basic {cred}\r\n"
            "Proxy-Connection: keep-alive\r\nContent-Length: 5\r\n\r\nhello"
            # pipelined: never reaches the site
            "GET http://elsewhere.nl/ HTTP/1.1\r\n\r\n"
        )
        w.write(req.encode())
        await w.drain()
        resp = await asyncio.wait_for(r.read(), 5)
        w.close()
        await asyncio.sleep(0.05)
        st = (await s.ctl({"op": "stats", "ctx": "ctx-1"}))["contexts"]["ctx-1"]
        return resp, s.http_heads, st, s.http_port

    resp, heads, st, port = run(tmp_path, body)
    assert resp.startswith(b"HTTP/1.1 200 OK") and resp.endswith(b"got hello")
    assert heads == [
        f"POST /form?a=1 HTTP/1.1\r\nHost: www.example.nl:{port}\r\nContent-Length: 5\r\n"
        "Connection: close\r\n\r\n".encode()
    ]
    assert st["bytes_up"] == len(heads[0]) + 5
    assert st["bytes_down"] == len(resp)


def test_absolute_form_with_a_chunked_body_is_refused(tmp_path):
    async def body(s: Stack):
        await s.open()
        status, _, w = await s.connect(
            None,
            line=f"POST http://www.example.nl:{s.http_port}/ HTTP/1.1\r\n"
            "Transfer-Encoding: chunked",
        )
        w.close()
        return status

    assert run(tmp_path, body).startswith(b"HTTP/1.1 411")


@pytest.mark.parametrize(
    "auth", [None, ("ctx-1", "wrong-password-xxxxxxx"), ("ctx-9", PASSWORD), ("ctx-1", "")]
)
def test_unknown_or_missing_credentials_are_407(tmp_path, auth):
    async def body(s: Stack):
        await s.open()
        status, _, w = await s.connect("www.example.nl", auth=auth)
        w.close()
        return status

    status = run(tmp_path, body)
    assert status.startswith(b"HTTP/1.1 407")
    assert b"Proxy-Authenticate: Basic" in status


def _refused_count(reason: str) -> float:
    return REGISTRY.get_sample_value("leasehold_gateway_refused_total", {"reason": reason}) or 0


def test_third_party_only_during_the_page_window(tmp_path):
    async def body(s: Stack):
        await s.open()
        before = _refused_count("no_page")
        out = []
        status, _, w = await s.connect("cdn.other.nl")
        out.append((status[:12], _header(status, "X-Leasehold-Refused")))
        w.close()
        await s.ctl({"op": "page_start", "ctx": "ctx-1"})
        third, r3, w3 = await s.connect("cdn.other.nl")
        first, r1, w1 = await s.connect("www.example.nl")
        out.append((third[:12], first[:12]))
        end = await s.ctl({"op": "page_end", "ctx": "ctx-1"})
        # the third-party tunnel is closed, the first-party one stays
        out.append(await asyncio.wait_for(r3.read(), 3))
        w1.write(b"still")
        await w1.drain()
        out.append(await asyncio.wait_for(r1.readexactly(5), 3))
        status, _, w = await s.connect("cdn.other.nl")
        out.append(status[:12])
        for x in (w, w1, w3):
            x.close()
        st = (await s.ctl({"op": "stats", "ctx": "ctx-1"}))["contexts"]["ctx-1"]
        return out, end, st, _refused_count("no_page") - before

    out, end, st, counted = run(tmp_path, body)
    assert out[0] == (b"HTTP/1.1 403", "no_page")
    assert out[1] == (b"HTTP/1.1 200", b"HTTP/1.1 200")
    assert end == {"ok": True, "closed": 1}
    assert out[2] == b"" and out[3] == b"still" and out[4] == b"HTTP/1.1 403"
    assert st["refused"] == {"no_page": 2} and st["third_party_tunnels"] == 1
    assert counted == 2


def test_page_timeout_ends_the_window(tmp_path):
    async def body(s: Stack):
        await s.open(limits={"page_timeout_s": 0.3})
        await s.ctl({"op": "page_start", "ctx": "ctx-1"})
        status, r, w = await s.connect("cdn.other.nl")
        rest = await asyncio.wait_for(r.read(), 3)
        status2, _, w2 = await s.connect("cdn.other.nl")
        w.close()
        w2.close()
        return status, rest, status2

    status, rest, status2 = run(tmp_path, body)
    assert status.startswith(b"HTTP/1.1 200") and rest == b""
    assert _header(status2, "X-Leasehold-Refused") == "no_page"


def test_foreign_targets_and_a_stale_list_are_refused(tmp_path):
    async def body(s: Stack):
        await s.open()
        await s.ctl({"op": "page_start", "ctx": "ctx-1"})
        out = []
        for host in ("www.leased.nl", "leased.nl", "notleased.nl"):
            status, _, w = await s.connect(host)
            out.append(_header(status, "X-Leasehold-Refused") or status[:12].decode())
            w.close()
        s.gw.foreign.updated = time.monotonic() - 1000
        status, _, w = await s.connect("notleased.nl")
        out.append(_header(status, "X-Leasehold-Refused"))
        w.close()
        return out

    assert run(tmp_path, body) == [
        "foreign_target",
        "foreign_target",
        "HTTP/1.1 200",
        "foreign_stale",
    ]


def test_ip_target_page_host_and_redirects(tmp_path):
    async def body(s: Stack):
        await s.open(target="93.184.215.14", kind="ip", page="shop.nl")
        out = []
        for host in ("shop.nl", "93.184.215.14", "www.shop.nl"):
            status, _, w = await s.connect(host)
            out.append(_header(status, "X-Leasehold-Refused") or status[:12].decode())
            w.close()
        a = await s.ctl({"op": "add_redirect", "ctx": "ctx-1", "host": "www.shop.nl"})
        status, _, w = await s.connect("www.shop.nl")
        out.append(status[:12].decode())
        w.close()
        await s.ctl({"op": "add_redirect", "ctx": "ctx-1", "host": "leased.nl"})
        status, _, w = await s.connect("leased.nl")
        out.append(_header(status, "X-Leasehold-Refused"))
        w.close()
        return a, out

    a, out = run(tmp_path, body)
    assert a == {"ok": True, "host": "www.shop.nl"}
    # The target address itself passes the gateway; the test exit then refuses it (it
    # reaches only loopback, which the gateway refuses as a host: private_address).
    assert out == ["HTTP/1.1 200", "exit_502", "no_page", "HTTP/1.1 200", "foreign_target"]


def test_port_and_ipv6_refused_at_the_gateway(tmp_path):
    async def body(s: Stack):
        await s.open()
        out = []
        lines = ("CONNECT www.example.nl:22 HTTP/1.1", f"CONNECT [::1]:{s.echo_port} HTTP/1.1")
        for line in lines:
            status, _, w = await s.connect(None, line=line)
            out.append((status[:12], _header(status, "X-Leasehold-Refused")))
            w.close()
        return out

    assert run(tmp_path, body) == [(b"HTTP/1.1 403", "port"), (b"HTTP/1.1 403", "ipv6")]


def test_third_party_tunnel_cap(tmp_path):
    async def body(s: Stack):
        await s.open(limits={"third_party_tunnels": 2})
        await s.ctl({"op": "page_start", "ctx": "ctx-1"})
        out, ws = [], []
        for host in ("a.other.nl", "b.other.nl", "c.other.nl", "www.example.nl"):
            status, _, w = await s.connect(host)
            out.append(_header(status, "X-Leasehold-Refused") or status[:12].decode())
            ws.append(w)
        for w in ws:
            w.close()
        return out

    assert run(tmp_path, body) == ["HTTP/1.1 200"] * 2 + ["third_party_tunnels", "HTTP/1.1 200"]


def test_third_party_byte_cap_closes_the_tunnel_and_refuses_new_ones(tmp_path):
    async def body(s: Stack):
        await s.open(limits={"third_party_bytes": 1000})
        await s.ctl({"op": "page_start", "ctx": "ctx-1"})
        status, r, w = await s.connect("cdn.other.nl")
        assert status.startswith(b"HTTP/1.1 200")
        w.write(b"y" * 600)
        await w.drain()
        got = await asyncio.wait_for(r.readexactly(400), 3)  # 600 up + 400 down = the cap
        rest = await asyncio.wait_for(r.read(), 3)
        w.close()
        status2, _, w2 = await s.connect("cdn.other.nl")
        w2.close()
        # first party is not capped by the gateway
        status3, r3, w3 = await s.connect("www.example.nl")
        w3.write(b"z" * 3000)
        await w3.drain()
        await asyncio.wait_for(r3.readexactly(3000), 3)
        w3.close()
        await asyncio.sleep(0.05)
        st = (await s.ctl({"op": "stats", "ctx": "ctx-1"}))["contexts"]["ctx-1"]
        return got, rest, status2, status3, st

    got, rest, status2, status3, st = run(tmp_path, body)
    assert len(got) == 400 and rest == b""
    assert _header(status2, "X-Leasehold-Refused") == "third_party_bytes"
    assert status3.startswith(b"HTTP/1.1 200")
    assert st["third_party_bytes"] == 1000
    assert st["bytes"] == st["third_party_bytes"] + 6000


def test_exit_refusals_are_relayed(tmp_path):
    async def body(s: Stack):
        await s.open(route=s.route(master=bytes(range(5, 37))))  # a token the exit refuses
        out = []
        status, _, w = await s.connect("www.example.nl")
        out.append((status[:12], _header(status, "X-Leasehold-Exit-Status")))
        w.close()
        a = await s.ctl({"op": "update_route", "lease_id": LEASE, "route": s.route()})
        status, _, w = await s.connect("www.example.nl")
        out.append(status[:12])
        w.close()
        # port 1 passes the gateway (test settings) but not the exit
        status, _, w = await s.connect("www.example.nl", port=1)
        out.append((status[:12], _header(status, "X-Leasehold-Exit-Status")))
        w.close()
        st = (await s.ctl({"op": "stats", "ctx": "ctx-1"}))["contexts"]["ctx-1"]
        return out, a, st

    out, a, st = run(tmp_path, body)
    assert out == [(b"HTTP/1.1 502", "407"), b"HTTP/1.1 200", (b"HTTP/1.1 403", "403")]
    assert a == {"ok": True, "updated": 1}
    assert st["exit_errors"] == {"407": 1, "403": 1}


def test_update_route_refuses_another_lease_and_an_expired_route_is_refused(tmp_path):
    async def body(s: Stack):
        await s.open()
        bad = await s.ctl({"op": "update_route", "ctx": "ctx-1", "route": s.route(lease="x")})
        soon = datetime.fromtimestamp(time.time() + 1, UTC)
        await s.ctl({"op": "update_route", "ctx": "ctx-1", "route": s.route(expires=soon)})
        status, r, w = await s.connect("www.example.nl")
        rest = await asyncio.wait_for(r.read(), 4)  # the exit ends it at the token's expiry
        w.close()
        status2, _, w2 = await s.connect("www.example.nl")
        w2.close()
        return bad, status, rest, status2

    bad, status, rest, status2 = run(tmp_path, body)
    assert not bad["ok"] and "another lease" in bad["error"]
    assert status.startswith(b"HTTP/1.1 200") and rest == b""
    assert _header(status2, "X-Leasehold-Refused") == "route_expired"


def test_exit_unreachable(tmp_path):
    async def body(s: Stack):
        probe = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = probe.sockets[0].getsockname()[1]
        probe.close()
        await probe.wait_closed()
        await s.open(route=_route(port=port))
        status, _, w = await s.connect("www.example.nl")
        w.close()
        return status

    status = run(tmp_path, body)
    assert status.startswith(b"HTTP/1.1 502")
    assert _header(status, "X-Leasehold-Refused") == "exit_unreachable"


def test_close_context_ends_its_tunnels_and_forgets_its_credentials(tmp_path):
    async def body(s: Stack):
        await s.open()
        await s.open(ctx="ctx-2")
        status, r, w = await s.connect("www.example.nl")
        status_b, r2, w2 = await s.connect("www.example.nl", auth=("ctx-2", PASSWORD))
        w.write(b"abc")
        await w.drain()
        await r.readexactly(3)
        closed = await s.ctl({"op": "close_context", "ctx": "ctx-1"})
        rest = await asyncio.wait_for(r.read(), 3)
        w2.write(b"other")
        await w2.drain()
        other = await asyncio.wait_for(r2.readexactly(5), 3)
        status2, _, w3 = await s.connect("www.example.nl")
        again = await s.ctl({"op": "close_context", "ctx": "ctx-1"})
        for x in (w, w2, w3):
            x.close()
        return status, closed, rest, other, status2, again

    status, closed, rest, other, status2, again = run(tmp_path, body)
    assert status.startswith(b"HTTP/1.1 200") and rest == b"" and other == b"other"
    st = closed["stats"]
    assert (st["bytes_up"], st["bytes_down"], st["tunnels_open"]) == (3, 3, 0)
    assert status2.startswith(b"HTTP/1.1 407")
    assert not again["ok"]


# --- the control socket -----------------------------------------------------------------


async def _socket_ctl(path: str, *msgs: dict) -> list[dict]:
    r, w = await asyncio.open_unix_connection(path)
    out = []
    for m in msgs:
        w.write(json.dumps(m).encode() + b"\n")
        await w.drain()
        out.append(json.loads(await asyncio.wait_for(r.readline(), 3)))
    w.write(b"not json\n")
    await w.drain()
    out.append(json.loads(await asyncio.wait_for(r.readline(), 3)))
    w.close()
    return out


def test_control_socket_json_lines(tmp_path):
    async def body(s: Stack):
        mode = stat.S_IMODE(os.stat(s.sock).st_mode)
        msg = {
            "op": "open_context",
            "id": 7,
            "ctx": "ctx-3",
            "password": PASSWORD,
            "lease_id": LEASE,
            "target": "example.nl",
            "target_kind": "domain",
            "page_host": "example.nl",
            "route": s.route(),
            "limits": {"third_party_tunnels": 10},
        }
        out = await _socket_ctl(
            s.sock,
            msg,
            msg,
            {"op": "page_start", "ctx": "ctx-3"},
            {"op": "bogus"},
            {"op": "open_context", "ctx": "ctx-4", "password": "short"},
            {"op": "set_foreign_targets", "targets": ["a.nl", "b.nl"]},
            {"op": "stats"},
        )
        return mode, out

    mode, out = run(tmp_path, body)
    assert mode == 0o600
    assert out[0]["ok"] and out[0]["id"] == 7 and out[0]["limits"]["third_party_tunnels"] == 10
    assert not out[1]["ok"] and "already open" in out[1]["error"]
    assert out[2] == {"ok": True, "page_timeout_s": 60.0}
    assert not out[3]["ok"] and "unknown op" in out[3]["error"]
    assert not out[4]["ok"] and "16" in out[4]["error"]
    assert out[5] == {"ok": True, "count": 2}
    st = out[6]["contexts"]["ctx-3"]
    assert st["page_in_progress"] and st["bytes"] == 0 and out[6]["foreign_targets"] == 2
    assert out[7] == {"ok": False, "error": "not JSON"}
    assert "password" not in json.dumps(out[6]) and PASSWORD not in json.dumps(out)


def test_control_socket_refuses_other_users(tmp_path):
    async def body(s: Stack):
        r, w = await asyncio.open_unix_connection(s.sock)
        w.write(b'{"op": "stats"}\n')
        await w.drain()
        line = await asyncio.wait_for(r.readline(), 3)
        w.close()
        return json.loads(line)

    answer = run(tmp_path, body, control_uids=(os.getuid() + 1,))
    assert answer == {"ok": False, "error": "not allowed"}


def test_control_socket_replaces_a_stale_socket_file(tmp_path):
    async def main():
        s1 = await Stack().start(tmp_path)
        s1.gw.control_server.close()  # crashed: the file stays
        s2 = await Stack().start(tmp_path)
        out = await _socket_ctl(s2.sock, {"op": "stats"})
        await s2.stop()
        await s1.stop()
        return out

    assert asyncio.run(main())[0]["ok"]
