"""The registry exit: tokens, the destination filter and the CONNECT proxy (local
sockets only; the site side is a local echo server)."""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import time
from datetime import UTC, datetime, timedelta

import pytest

from leasehold.exit import token as tk
from leasehold.exit.server import (
    Destinations,
    Exit,
    ExitSettings,
    parse_authority,
    resolve_ipv4,
)

MASTER = bytes(range(32))
OLD_MASTER = bytes(range(1, 33))
EGRESS = "127.0.0.2"  # the tunnels' source address; all of 127/8 is local on Linux
KEY = tk.exit_key(MASTER, EGRESS)  # what the exit holds
LEASE = "6f1c2a7e-0000-4000-8000-000000000001"
ENVS = ("dev", "prod")


def token(master=MASTER, lease=LEASE, egress=EGRESS, env="dev", expires=None):
    exp = expires or datetime.now(UTC) + timedelta(minutes=1)
    return tk.sign_lease(master, lease, egress, "example.nl", env, exp)


# --- tokens -----------------------------------------------------------------------------


def test_token_round_trip():
    user, pw = token()
    assert user == LEASE
    c = tk.verify([KEY], user, pw, EGRESS, ENVS)
    assert (c.lease_id, c.egress, c.target, c.env) == (LEASE, EGRESS, "example.nl", "dev")


def test_each_exit_holds_only_its_own_key():
    user, pw = token()
    for keys in ([MASTER], [tk.exit_key(MASTER, "127.0.0.3")]):
        with pytest.raises(tk.TokenError, match="signature"):
            tk.verify(keys, user, pw, EGRESS, ENVS)
    assert tk.exit_key(MASTER, "192.0.2.1") != tk.exit_key(MASTER, "192.0.2.2")


def test_token_previous_key_still_verifies_during_rotation():
    user, pw = token(master=OLD_MASTER)
    tk.verify([KEY, tk.exit_key(OLD_MASTER, EGRESS)], user, pw, EGRESS, ENVS)
    with pytest.raises(tk.TokenError, match="signature"):
        tk.verify([KEY], user, pw, EGRESS, ENVS)


@pytest.mark.parametrize(
    "kw, user, egress, match",
    [
        ({}, "other-lease", EGRESS, "another lease"),
        ({"env": "staging"}, LEASE, EGRESS, "another environment"),
        ({"expires": datetime.now(UTC) - timedelta(seconds=1)}, LEASE, EGRESS, "expired"),
    ],
)
def test_token_refusals(kw, user, egress, match):
    _, pw = token(**kw)
    with pytest.raises(tk.TokenError, match=match):
        tk.verify([KEY], user, pw, egress, ENVS)


def test_token_for_another_egress_fails():
    user, pw = token(egress="198.51.100.7")
    with pytest.raises(tk.TokenError):
        tk.verify([KEY], user, pw, EGRESS, ENVS)
    # Even with that egress's key, this exit refuses it: it is not its address.
    with pytest.raises(tk.TokenError, match="another egress"):
        tk.verify([tk.exit_key(MASTER, "198.51.100.7")], user, pw, EGRESS, ENVS)


def test_env_is_mandatory():
    with pytest.raises(ValueError, match="environment"):
        token(env="")
    user, pw = token()
    with pytest.raises(tk.TokenError, match="another environment"):
        tk.verify([KEY], user, pw, EGRESS, ())


def test_token_tampered_claims_fail_the_signature():
    user, pw = token()
    v, claims, mac = pw.split(".")
    body = base64.urlsafe_b64decode(claims + "==").replace(b"example.nl", b"example.de")
    forged = f"{v}.{base64.urlsafe_b64encode(body).rstrip(b'=').decode()}.{mac}"
    with pytest.raises(tk.TokenError, match="signature"):
        tk.verify([KEY], user, forged, EGRESS, ENVS)


@pytest.mark.parametrize("pw", ["", "v1.abc", "v2.a.b", "v1.!!.??", "plain-password"])
def test_token_malformed(pw):
    with pytest.raises(tk.TokenError):
        tk.verify([KEY], LEASE, pw, EGRESS, ENVS)


def test_keys_parse_hex_and_base64_and_refuse_short_keys():
    assert tk.parse_keys(f"{KEY.hex()}, {base64.urlsafe_b64encode(OLD_MASTER).decode()}") == [
        KEY,
        OLD_MASTER,
    ]
    with pytest.raises(ValueError, match="32 bytes"):
        tk.parse_key("00ff")
    with pytest.raises(ValueError):
        tk.parse_keys(" , ")


# --- destinations and settings ----------------------------------------------------------


@pytest.mark.parametrize(
    "addr, why",
    [
        ("93.184.216.34", ""),
        ("10.1.2.3", "not_public"),
        ("172.16.0.1", "not_public"),
        ("192.168.1.1", "not_public"),
        ("100.81.80.15", "not_public"),  # tailnet (100.64.0.0/10)
        ("127.0.0.1", "not_public"),
        ("169.254.169.254", "not_public"),
        ("0.0.0.0", "not_public"),
        ("224.0.0.1", "not_public"),
        ("192.0.0.8", "not_public"),  # 192.0.0.0/24, IETF protocol assignments
        ("198.18.0.1", "not_public"),  # benchmarking
        ("198.51.100.20", "not_public"),  # documentation
        ("240.0.0.1", "not_public"),  # reserved
        ("255.255.255.255", "not_public"),
        ("2606:4700::1111", "ipv6"),
        ("85.17.145.10", "denied"),
    ],
)
def test_destination_filter(addr, why):
    d = Destinations(deny=(ipaddress.ip_network("85.17.145.0/24"),))
    assert d.refusal(ipaddress.ip_address(addr)) == why


@pytest.mark.parametrize(
    "text, want",
    [
        ("example.nl:443", ("example.nl", 443)),
        ("Example.NL.:80", ("example.nl", 80)),
        ("93.184.216.34:443", ("93.184.216.34", 443)),
        ("[2001:db8::1]:443", ("2001:db8::1", 443)),
    ],
)
def test_parse_authority(text, want):
    assert parse_authority(text) == want


@pytest.mark.parametrize("text", ["example.nl", "example.nl:", ":443", "a:99999", "[::1]443"])
def test_parse_authority_refuses(text):
    with pytest.raises(ValueError):
        parse_authority(text)


BASE_ENV = {
    "LEASEHOLD_EXIT_LISTEN": "100.64.0.5:3130",
    "LEASEHOLD_EGRESS_IP": "192.0.2.10",
    "LEASEHOLD_EXIT_KEYS": KEY.hex(),
    "LEASEHOLD_EXIT_ENVS": "dev,prod",
    "LEASEHOLD_EXIT_DENY": "85.17.145.0/24, 192.0.2.11",
}


def test_settings_from_env():
    s = ExitSettings.from_env({**BASE_ENV, "LEASEHOLD_EXIT_METRICS_PORT": "3131"})
    assert (s.listen_host, s.listen_port, s.egress, s.metrics_port) == (
        "100.64.0.5",
        3130,
        "192.0.2.10",
        3131,
    )
    assert s.envs == ("dev", "prod")
    assert s.destinations.ports == (80, 443)
    assert len(s.destinations.deny) == 2
    assert s.head_timeout <= 5
    assert "keys" not in repr(s) and KEY.hex() not in repr(s)


@pytest.mark.parametrize(
    "env, match",
    [
        ({"LEASEHOLD_EXIT_ENVS": ""}, "LEASEHOLD_EXIT_ENVS"),
        ({"LEASEHOLD_EXIT_DENY": ""}, "LEASEHOLD_EXIT_DENY"),
        ({"LEASEHOLD_EXIT_DENY": " , "}, "names no network"),
        ({"LEASEHOLD_EXIT_LISTEN": "0.0.0.0:3130"}, "one private address"),
        ({"LEASEHOLD_EGRESS_IP": "2001:db8::1"}, "IPv4"),
        ({"LEASEHOLD_EXIT_KEYS": "00ff"}, "32 bytes"),
    ],
)
def test_settings_refusals(env, match):
    with pytest.raises(ValueError, match=match):
        ExitSettings.from_env({**BASE_ENV, **env})


def test_own_egress_is_always_denied():
    ex = Exit(_settings())
    assert ex.destinations.refusal(ipaddress.ip_address("127.0.0.2")) == "not_public"
    ex = Exit(_settings(egress="93.184.216.34"))
    assert ex.destinations.refusal(ipaddress.ip_address("93.184.216.34")) == "denied"


@pytest.mark.parametrize("host", ["127.1", "2130706433", "0x7f000001", "localhost"])
def test_numeric_host_forms_through_the_real_resolver_are_refused(host):
    async def main():
        ex = Exit(_settings(), resolver=resolve_ipv4)
        try:
            return await ex._address(host, 443)
        except Exception as e:
            return e

    r = asyncio.run(main())
    assert getattr(r, "status", None) in (403, 502), r


# --- the proxy --------------------------------------------------------------------------


class _Loopback(Destinations):
    """The real filter, except that the local echo server's address is allowed."""

    def refusal(self, addr):
        if str(addr) == "127.0.0.1":
            return ""
        return super().refusal(addr)


async def _echo_server(peers: list):
    async def handle(r, w):
        peers.append(w.get_extra_info("peername")[0])
        try:
            while data := await r.read(1024):
                w.write(data)
                await w.drain()
        except ConnectionError:
            pass
        w.close()

    return await asyncio.start_server(handle, "127.0.0.1", 0)


def _settings(**kw) -> ExitSettings:
    kw.setdefault("egress", EGRESS)
    return ExitSettings(listen_host="127.0.0.1", listen_port=0, keys=[KEY], envs=ENVS, **kw)


async def _request(port: int, line: str, auth: tuple[str, str] | None) -> tuple[bytes, tuple]:
    r, w = await asyncio.open_connection("127.0.0.1", port)
    head = [line]
    if auth:
        cred = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        head.append(f"Proxy-Authorization: Basic {cred}")
    w.write(("\r\n".join(head) + "\r\n\r\n").encode())
    await w.drain()
    status = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 5)
    return status, (r, w)


def test_tunnel_leaves_from_the_egress_address_and_relays_bytes():
    async def main():
        peers: list[str] = []
        echo = await _echo_server(peers)
        echo_port = echo.sockets[0].getsockname()[1]
        ex = Exit(_settings(destinations=_Loopback(ports=(echo_port,))))
        await ex.start()
        status, (r, w) = await _request(ex.port, f"CONNECT 127.0.0.1:{echo_port} HTTP/1.1", token())
        assert status.startswith(b"HTTP/1.1 200")
        w.write(b"hello through the exit")
        await w.drain()
        assert await asyncio.wait_for(r.readexactly(22), 5) == b"hello through the exit"
        w.close()
        ex.server.close()
        echo.close()
        return peers

    assert asyncio.run(main()) == [EGRESS]


def test_tunnel_closes_when_its_token_expires():
    async def main():
        echo = await _echo_server([])
        echo_port = echo.sockets[0].getsockname()[1]
        ex = Exit(_settings(destinations=_Loopback(ports=(echo_port,))))
        await ex.start()
        soon = datetime.fromtimestamp(time.time() + 2, UTC)
        status, (r, w) = await _request(
            ex.port, f"CONNECT 127.0.0.1:{echo_port} HTTP/1.1", token(expires=soon)
        )
        assert status.startswith(b"HTTP/1.1 200")
        t0 = time.monotonic()
        rest = await asyncio.wait_for(r.read(), 6)  # EOF when the exit closes the tunnel
        elapsed = time.monotonic() - t0
        w.close()
        ex.server.close()
        echo.close()
        return rest, elapsed

    rest, elapsed = asyncio.run(main())
    assert rest == b"" and elapsed < 4


def test_lease_byte_limit_closes_the_tunnel_and_refuses_new_ones():
    async def main():
        echo = await _echo_server([])
        echo_port = echo.sockets[0].getsockname()[1]
        ex = Exit(_settings(destinations=_Loopback(ports=(echo_port,)), lease_bytes=1000))
        await ex.start()
        line = f"CONNECT 127.0.0.1:{echo_port} HTTP/1.1"
        status, (r, w) = await _request(ex.port, line, token())
        assert status.startswith(b"HTTP/1.1 200")
        w.write(b"x" * 2000)
        await w.drain()
        await asyncio.wait_for(r.read(), 5)
        w.close()
        status2, (_, w2) = await _request(ex.port, line, token())
        w2.close()
        ex.server.close()
        echo.close()
        return status2

    assert asyncio.run(main()).startswith(b"HTTP/1.1 429")


def test_lease_tunnel_limit():
    async def main():
        echo = await _echo_server([])
        echo_port = echo.sockets[0].getsockname()[1]
        ex = Exit(_settings(destinations=_Loopback(ports=(echo_port,)), lease_tunnels=2))
        await ex.start()
        line = f"CONNECT 127.0.0.1:{echo_port} HTTP/1.1"
        out, writers = [], []
        for _ in range(3):
            status, (_, w) = await _request(ex.port, line, token())
            out.append(status[:12])
            writers.append(w)
        # Another lease is not affected.
        other = token(lease="6f1c2a7e-0000-4000-8000-000000000002")
        status, (_, w) = await _request(ex.port, line, other)
        out.append(status[:12])
        writers.append(w)
        for w in writers:
            w.close()
        ex.server.close()
        echo.close()
        return out

    assert asyncio.run(main()) == [b"HTTP/1.1 200"] * 2 + [b"HTTP/1.1 429", b"HTTP/1.1 200"]


def test_pending_slots_per_source_and_slow_heads():
    async def main():
        ex = Exit(_settings(per_source_pending=2, head_timeout=0.5))
        await ex.start()
        idle = [await asyncio.open_connection("127.0.0.1", ex.port) for _ in range(2)]
        await asyncio.sleep(0.05)
        status, (_, w) = await _request(ex.port, "CONNECT example.nl:443 HTTP/1.1", token())
        w.close()
        # The slow heads time out and their slots come back.
        slow = [await asyncio.wait_for(r.read(), 3) for r, _ in idle]
        status2, (_, w2) = await _request(ex.port, "CONNECT example.nl:22 HTTP/1.1", token())
        w2.close()
        for _, iw in idle:
            iw.close()
        ex.server.close()
        return status, slow, status2

    status, slow, status2 = asyncio.run(main())
    assert status.startswith(b"HTTP/1.1 503")
    assert all(s.startswith(b"HTTP/1.1 400") for s in slow)
    assert status2.startswith(b"HTTP/1.1 403")  # served again (port refused)


def test_max_connections_answers_busy():
    async def main():
        echo = await _echo_server([])
        echo_port = echo.sockets[0].getsockname()[1]
        ex = Exit(_settings(destinations=_Loopback(ports=(echo_port,)), max_connections=1))
        await ex.start()
        line = f"CONNECT 127.0.0.1:{echo_port} HTTP/1.1"
        s1, (_, w1) = await _request(ex.port, line, token())
        s2, (_, w2) = await _request(ex.port, line, token())
        for w in (w1, w2):
            w.close()
        ex.server.close()
        echo.close()
        return s1, s2

    s1, s2 = asyncio.run(main())
    assert s1.startswith(b"HTTP/1.1 200") and s2.startswith(b"HTTP/1.1 503")


def test_dns_timeout():
    async def slow(host, port):
        await asyncio.sleep(10)
        return ["93.184.216.34"]

    status = _refused("CONNECT slow.example:443 HTTP/1.1", token(), resolver=slow, dns_timeout=0.2)
    assert status.startswith(b"HTTP/1.1 504")


def test_name_is_resolved_by_the_exit_and_the_checked_address_is_used():
    async def main():
        echo = await _echo_server([])
        echo_port = echo.sockets[0].getsockname()[1]
        asked = []

        async def resolver(host, port):
            asked.append(host)
            return ["127.0.0.1"]

        ex = Exit(_settings(destinations=_Loopback(ports=(echo_port,))), resolver=resolver)
        await ex.start()
        status, (_, w) = await _request(
            ex.port, f"CONNECT site.example:{echo_port} HTTP/1.1", token()
        )
        w.close()
        ex.server.close()
        echo.close()
        return status, asked

    status, asked = asyncio.run(main())
    assert status.startswith(b"HTTP/1.1 200")
    assert asked == ["site.example"]


def _refused(line: str, auth, resolver=None, **kw) -> bytes:
    async def main():
        async def none(host, port):
            raise OSError("no such name")

        ex = Exit(_settings(**kw), resolver=resolver or none)
        await ex.start()
        status, (_, w) = await _request(ex.port, line, auth)
        w.close()
        ex.server.close()
        return status

    return asyncio.run(main())


def test_refuses_without_credentials():
    status = _refused("CONNECT example.nl:443 HTTP/1.1", None)
    assert status.startswith(b"HTTP/1.1 407")
    assert b"Proxy-Authenticate: Basic" in status


def test_refuses_a_token_for_another_egress_or_env_or_an_expired_one():
    for t in (
        token(egress="198.51.100.7"),
        token(env="staging"),
        token(expires=datetime.fromtimestamp(time.time() - 5, UTC)),
    ):
        assert _refused("CONNECT example.nl:443 HTTP/1.1", t).startswith(b"HTTP/1.1 407")


def test_refuses_forward_proxying():
    status = _refused("GET http://example.nl/ HTTP/1.1", token())
    assert status.startswith(b"HTTP/1.1 405")


def test_refuses_other_ports():
    status = _refused("CONNECT example.nl:22 HTTP/1.1", token())
    assert status.startswith(b"HTTP/1.1 403")


@pytest.mark.parametrize(
    "authority",
    [
        "127.0.0.1:443",
        "10.0.0.1:443",
        "100.81.80.15:443",
        "169.254.169.254:80",
        "127.0.0.2:443",  # the exit's own egress
        "[::1]:443",
    ],
)
def test_refuses_private_own_and_ipv6_literals(authority):
    status = _refused(f"CONNECT {authority} HTTP/1.1", token())
    assert status.startswith(b"HTTP/1.1 403")


def test_refuses_a_name_that_resolves_to_a_private_address():
    async def rebinding(host, port):
        return ["10.0.0.5", "100.83.247.12"]

    status = _refused("CONNECT evil.example:443 HTTP/1.1", token(), resolver=rebinding)
    assert status.startswith(b"HTTP/1.1 403")


def test_refuses_the_operators_own_addresses():
    async def own(host, port):
        return ["85.17.145.10"]

    status = _refused(
        "CONNECT worker.example:443 HTTP/1.1",
        token(),
        resolver=own,
        destinations=Destinations(deny=(ipaddress.ip_network("85.17.145.0/24"),)),
    )
    assert status.startswith(b"HTTP/1.1 403")


def test_unresolvable_name_is_a_gateway_error():
    status = _refused("CONNECT nowhere.example:443 HTTP/1.1", token())
    assert status.startswith(b"HTTP/1.1 502")


def test_connect_failure_is_a_gateway_error():
    async def main():
        probe = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = probe.sockets[0].getsockname()[1]
        probe.close()
        await probe.wait_closed()
        ex = Exit(_settings(destinations=_Loopback(ports=(port,))))
        await ex.start()
        status, (_, w) = await _request(ex.port, f"CONNECT 127.0.0.1:{port} HTTP/1.1", token())
        w.close()
        ex.server.close()
        return status

    assert asyncio.run(main()).startswith(b"HTTP/1.1 502")


def test_bad_request_head():
    status = _refused("CONNECT", token())
    assert status.startswith(b"HTTP/1.1 400")
