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
from leasehold.exit.server import Destinations, Exit, ExitSettings, parse_authority

KEY = bytes(range(32))
OLD_KEY = bytes(range(1, 33))
EGRESS = "127.0.0.2"  # the tunnels' source address; all of 127/8 is local on Linux
LEASE = "6f1c2a7e-0000-4000-8000-000000000001"


def token(key=KEY, lease=LEASE, egress=EGRESS, env="dev", expires=None):
    exp = expires or datetime.now(UTC) + timedelta(minutes=1)
    return tk.sign_lease(key, lease, egress, "example.nl", env, exp)


# --- tokens -----------------------------------------------------------------------------


def test_token_round_trip():
    user, pw = token()
    assert user == LEASE
    c = tk.verify([KEY], user, pw, EGRESS)
    assert (c.lease_id, c.egress, c.target, c.env) == (LEASE, EGRESS, "example.nl", "dev")


def test_token_previous_key_still_verifies_during_rotation():
    user, pw = token(key=OLD_KEY)
    tk.verify([KEY, OLD_KEY], user, pw, EGRESS)
    with pytest.raises(tk.TokenError, match="signature"):
        tk.verify([KEY], user, pw, EGRESS)


@pytest.mark.parametrize(
    "kw, user, egress, match",
    [
        ({}, "other-lease", EGRESS, "another lease"),
        ({}, LEASE, "198.51.100.7", "another egress"),
        ({"expires": datetime.now(UTC) - timedelta(seconds=1)}, LEASE, EGRESS, "expired"),
    ],
)
def test_token_refusals(kw, user, egress, match):
    _, pw = token(**kw)
    with pytest.raises(tk.TokenError, match=match):
        tk.verify([KEY], user, pw, egress)


def test_token_tampered_claims_fail_the_signature():
    user, pw = token()
    v, claims, mac = pw.split(".")
    body = base64.urlsafe_b64decode(claims + "==").replace(b"example.nl", b"example.de")
    forged = f"{v}.{base64.urlsafe_b64encode(body).rstrip(b'=').decode()}.{mac}"
    with pytest.raises(tk.TokenError, match="signature"):
        tk.verify([KEY], user, forged, EGRESS)


@pytest.mark.parametrize("pw", ["", "v1.abc", "v2.a.b", "v1.!!.??", "plain-password"])
def test_token_malformed(pw):
    with pytest.raises(tk.TokenError):
        tk.verify([KEY], LEASE, pw, EGRESS)


def test_keys_parse_hex_and_base64_and_refuse_short_keys():
    assert tk.parse_keys(f"{KEY.hex()}, {base64.urlsafe_b64encode(OLD_KEY).decode()}") == [
        KEY,
        OLD_KEY,
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
        ("2606:4700::1111", "ipv6"),
        ("198.51.100.20", "not_public"),  # documentation range
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


def test_settings_from_env():
    s = ExitSettings.from_env(
        {
            "LEASEHOLD_EXIT_LISTEN": "100.64.0.5:3130",
            "LEASEHOLD_EGRESS_IP": "192.0.2.10",
            "LEASEHOLD_EXIT_KEYS": KEY.hex(),
            "LEASEHOLD_EXIT_DENY": "85.17.145.0/24, 192.0.2.10",
            "LEASEHOLD_EXIT_METRICS_PORT": "3131",
        }
    )
    assert (s.listen_host, s.listen_port, s.egress, s.metrics_port) == (
        "100.64.0.5",
        3130,
        "192.0.2.10",
        3131,
    )
    assert s.destinations.ports == (80, 443)
    assert len(s.destinations.deny) == 2
    assert "keys" not in repr(s)


@pytest.mark.parametrize(
    "env, match",
    [
        ({}, "missing"),
        ({"LEASEHOLD_EXIT_LISTEN": "0.0.0.0:3130"}, "one private address"),
        ({"LEASEHOLD_EGRESS_IP": "2001:db8::1"}, "IPv4"),
        ({"LEASEHOLD_EXIT_KEYS": "00ff"}, "32 bytes"),
    ],
)
def test_settings_refusals(env, match):
    base = {
        "LEASEHOLD_EXIT_LISTEN": "100.64.0.5:3130",
        "LEASEHOLD_EGRESS_IP": "192.0.2.10",
        "LEASEHOLD_EXIT_KEYS": KEY.hex(),
    }
    with pytest.raises(ValueError, match=match):
        ExitSettings.from_env({**base, **env} if env else {})


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
        while data := await r.read(1024):
            w.write(data)
            await w.drain()
        w.close()

    return await asyncio.start_server(handle, "127.0.0.1", 0)


def _settings(**kw) -> ExitSettings:
    return ExitSettings(
        listen_host="127.0.0.1",
        listen_port=0,
        egress=EGRESS,
        keys=[KEY],
        **kw,
    )


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


def _run(coro):
    return asyncio.run(coro)


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

    assert _run(main()) == [EGRESS]


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

    status, asked = _run(main())
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

    return _run(main())


def test_refuses_without_credentials():
    status = _refused("CONNECT example.nl:443 HTTP/1.1", None)
    assert status.startswith(b"HTTP/1.1 407")
    assert b"Proxy-Authenticate: Basic" in status


def test_refuses_a_token_for_another_egress_or_an_expired_one():
    other = token(egress="198.51.100.7")
    assert _refused("CONNECT example.nl:443 HTTP/1.1", other).startswith(b"HTTP/1.1 407")
    old = token(expires=datetime.fromtimestamp(time.time() - 5, UTC))
    assert _refused("CONNECT example.nl:443 HTTP/1.1", old).startswith(b"HTTP/1.1 407")


def test_refuses_a_token_of_another_environment_when_the_exit_has_one():
    status = _refused("CONNECT example.nl:443 HTTP/1.1", token(env="prod"), env="dev")
    assert status.startswith(b"HTTP/1.1 407")


def test_refuses_forward_proxying():
    status = _refused("GET http://example.nl/ HTTP/1.1", token())
    assert status.startswith(b"HTTP/1.1 405")


def test_refuses_other_ports():
    status = _refused("CONNECT example.nl:22 HTTP/1.1", token())
    assert status.startswith(b"HTTP/1.1 403")


@pytest.mark.parametrize(
    "authority",
    ["127.0.0.1:443", "10.0.0.1:443", "100.81.80.15:443", "169.254.169.254:80", "[::1]:443"],
)
def test_refuses_private_and_ipv6_literals(authority):
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
        # A port nothing listens on: bind and close to find one.
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

    assert _run(main()).startswith(b"HTTP/1.1 502")


def test_bad_request_head():
    status = _refused("CONNECT", token())
    assert status.startswith(b"HTTP/1.1 400")
