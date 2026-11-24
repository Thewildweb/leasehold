"""Proxies without a database: encryption, quota arithmetic, error rules, routes, tiers,
the egress block in service.yml, import parsing, adapters, the CLI's secret handling."""

from __future__ import annotations

import base64
import io
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from leasehold.egress import proxies as px
from leasehold.egress import secrets as sec
from leasehold.egress import tiers as tr
from leasehold.egress.proxy_admin import ProxyAdminError, parse_lines
from leasehold.egress.usage import GENERIC, Usage, load_adapters
from leasehold.spec.models import TaskEgress, TaskType

# --- secrets ------------------------------------------------------------------------------------


def test_seal_and_open():
    k = sec.new_key()
    ring = sec.Keyring.from_env({"LEASEHOLD_SECRETS_KEY": k})
    s = ring.seal(b"hunter2", "proxy_accounts/a/credentials")
    assert b"hunter2" not in s.ciphertext and s.key_id == ring.current
    assert s.fingerprint == sec.fingerprint(b"hunter2") and len(s.fingerprint) == 12
    assert ring.open(s.ciphertext, s.key_id, "proxy_accounts/a/credentials") == b"hunter2"
    # Bound to its row: copied into another account's row, it doesn't open.
    with pytest.raises(sec.SecretsError):
        ring.open(s.ciphertext, s.key_id, "proxy_accounts/b/credentials")
    assert k not in repr(ring)


def test_rotation():
    old, new = sec.new_key(), sec.new_key()
    ring_old = sec.Keyring.from_env({"LEASEHOLD_SECRETS_KEYS": f"k1:{old}"})
    s = ring_old.seal(b"x", "c")
    ring = sec.Keyring.from_env({"LEASEHOLD_SECRETS_KEYS": f"k1:{old},k2:{new}"})
    assert ring.current == "k2" and ring.open(s.ciphertext, "k1", "c") == b"x"
    assert ring.seal(b"x", "c").key_id == "k2"
    with pytest.raises(sec.SecretsError):
        sec.Keyring.from_env({"LEASEHOLD_SECRETS_KEYS": f"k2:{new}"}).open(s.ciphertext, "k1", "c")


def test_bad_keys():
    with pytest.raises(sec.SecretsError):
        sec.Keyring.from_env({"LEASEHOLD_SECRETS_KEY": base64.b64encode(b"short").decode()})
    with pytest.raises(sec.SecretsError):
        sec.Keyring.from_env({"LEASEHOLD_SECRETS_KEY": sec.new_key(),
                              "LEASEHOLD_SECRETS_KEYS": "a:" + sec.new_key()})  # fmt: skip
    empty = sec.Keyring.from_env({})
    assert not empty.configured
    with pytest.raises(sec.SecretsError):
        empty.seal(b"x", "c")


# --- quota ------------------------------------------------------------------------------------


def test_remaining_is_the_lowest():
    gb = px.GB
    # Our cap only.
    r = px.remaining({"cap_gb": 100}, 30 * gb)
    assert r.bytes == 70 * gb and r.ratio == pytest.approx(0.7) and r.source == "cap"
    # The vendor's numbers are lower: they bind, as a share of the vendor's limit.
    acc = {"cap_gb": 100, "vendor_limit_bytes": 50 * gb, "vendor_used_bytes": 45 * gb}
    r = px.remaining(acc, 10 * gb)
    assert r.bytes == 5 * gb and r.ratio == pytest.approx(0.1) and r.source == "vendor"
    assert r.used_ratio == pytest.approx(0.9)
    # Nothing known: no quota at all.
    assert px.remaining({}, 10).ratio is None
    # Over the limit: zero, not negative.
    assert px.remaining({"cap_gb": 1}, 2 * gb).bytes == 0


def test_periods():
    now = datetime(2026, 3, 3, 12, tzinfo=UTC)
    assert px.period_start(now, "month", 5) == datetime(2026, 2, 5, tzinfo=UTC)
    assert px.period_end(now, "month", 5) == datetime(2026, 3, 5, tzinfo=UTC)
    assert px.period_start(now, "month", 1) == datetime(2026, 3, 1, tzinfo=UTC)
    assert px.period_start(now, "day", 1) == datetime(2026, 3, 3, tzinfo=UTC)
    jan = datetime(2026, 1, 2, tzinfo=UTC)
    assert px.period_start(jan, "month", 15) == datetime(2025, 12, 15, tzinfo=UTC)


def test_error_rules():
    rules = px.error_rules({"errors": [
        {"status": 403, "body_regex": "traffic limit", "state": "quota_exhausted"},
    ]})  # fmt: skip
    assert px.match_error(rules, 407, "") == "auth_failed"
    assert px.match_error(rules, 402, "") == "payment_failed"
    assert px.match_error(rules, 403, "Traffic limit reached") == "quota_exhausted"
    assert px.match_error(rules, 403, "forbidden") is None
    assert px.match_error(rules, 502, "bad gateway") is None
    with pytest.raises(ValueError, match="header rules"):
        px.check_error_rules([{"header": "X-Account-Suspended", "state": "suspended"}])
    with pytest.raises(ValueError):
        px.check_error_rules([{"status": 403, "state": "ok"}])
    with pytest.raises(ValueError):
        px.check_error_rules([{"body_regex": "(", "state": "suspended"}])


def test_endpoints_and_usernames():
    assert px.parse_endpoint("gw.example:9000") == ("http", "gw.example", 9000)
    assert px.parse_endpoint("socks5://gw.example:1080")[0] == "socks5h"
    with pytest.raises(ValueError):
        px.parse_endpoint("gw.example")
    t = "{user}-country-{country}-session-{session}-lifetime-{ttl_min}"
    assert px.render_username(t, "u", "NL", "s1", 600) == "u-country-nl-session-s1-lifetime-10"
    assert px.redact_url("http://u:p@h:1") == "http://***@h:1"
    assert px.session_id(uuid4()).startswith("s")


def test_route_has_credentials_only_inside():
    key = sec.new_key()
    ring = sec.Keyring.from_env({"LEASEHOLD_SECRETS_KEY": key})
    sealed = ring.seal(px.encode_credentials("user-x", "pw-y"),
                       sec.context("proxy_accounts", "a", "credentials"))  # fmt: skip
    acc = px.Account(
        name="a", vendor="v", adapter="generic", adapter_config={}, state="ok",
        status="active", envs=["dev"], concurrency=None, credentials=sealed.ciphertext,
        key_id=sealed.key_id, credentials_fp=sealed.fingerprint,
        remaining=px.Remaining(None, None, None, None), our_bytes=0,
        period_end=datetime.now(UTC),
    )  # fmt: skip
    assert "pw-y" not in repr(acc)
    eg = px.ProxyEgress("pool:p", "residential", "a", "p", "http://gw.example:9000",
                        countries=("NL",), template="{user}-country-{country}")  # fmt: skip
    r = px.render_route(eg, acc, ring, country="NL", session=None, session_ttl_s=None,
                        expires_at=datetime.now(UTC))  # fmt: skip
    assert r["proxy"]["username"] == "user-x-country-nl" and r["proxy"]["password"] == "pw-y"
    assert r["exit_ip"] is None and r["credentials_fp"] == sealed.fingerprint


# --- tiers and the spec -------------------------------------------------------------------------


def test_tiers():
    spec = TaskEgress.model_validate(
        {"tiers": ["datacenter", {"isp": ["isp-a"]}, {"residential": ["r1", "r2"]}]}
    ).resolve("prod")
    assert tr.effective(spec, None) == "datacenter"
    assert tr.effective(spec, "isp") == "isp"
    assert tr.next_tier(spec, "datacenter") == "isp"
    assert tr.next_tier(spec, "residential") is None
    assert tr.names_for(spec, "residential") == ["r1", "r2"]
    only_isp = TaskEgress.model_validate({"tiers": [{"isp": ["isp-a"]}]}).resolve("prod")
    assert tr.effective(only_isp, None) == "isp" and not tr.uses_datacenter(only_isp)
    assert tr.effective(only_isp, "residential") == "isp"  # above its tiers: its last
    assert tr.effective(None, "isp") == "datacenter"  # no egress block: datacenter only


def test_egress_block_validation():
    ok = TaskEgress.model_validate({
        "tiers": ["datacenter", {"isp": ["isp-a"]}],
        "countries": ["NL"],
        "stop_at_used": {"isp": "70%"},
        "per_env": {"dev": {"stop_at_used": {"isp": "20%"}}},
    })  # fmt: skip
    assert ok.resolve("dev")["stop_at_used"] == {"isp": 0.2}
    assert ok.resolve("prod")["stop_at_used"] == {"isp": 0.7}
    assert ok.names() == {"isp-a"}
    bad = [
        {"tiers": [{"isp": []}]},  # a proxy tier names nothing
        {"tiers": [{"isp": ["a"]}, "datacenter"]},  # out of order
        {"tiers": ["datacenter", "datacenter"]},
        {"tiers": ["datacenter"], "stop_at_used": {"isp": "50%"}},  # tier not listed
        {"tiers": [{"isp": ["a"]}], "stop_at_used": {"isp": "150%"}},
        {"tiers": [{"isp": ["a"]}], "stop_at_used": {"isp": "50"}},
        {"tiers": ["datacenter"], "session": {"mode": "sticky", "ttl": "10m"}},
        {"tiers": [{"residential": ["r"]}], "session": {"mode": "sticky"}},
        {"tiers": [{"isp": ["a", "a"]}]},
    ]
    for b in bad:
        with pytest.raises(ValidationError):
            TaskEgress.model_validate(b)
    with pytest.raises(ValidationError, match="target_leases"):
        TaskType.model_validate({"key": "domain", "egress": {"tiers": ["datacenter"]}})
    s = TaskEgress.model_validate(
        {"tiers": [{"residential": ["r"]}], "session": {"mode": "sticky", "ttl": "10m"}}
    ).resolve("prod")
    assert s["session"] == {"mode": "sticky", "ttl_s": 600}


# --- import parsing -----------------------------------------------------------------------------


def test_parse_lines_never_echoes_content():
    lines = parse_lines("# list\nisp.example:10001:user:pa:ss\n\nisp.example:10002:user:pa:ss\n")
    assert [ln.port for ln in lines] == [10001, 10002]
    assert lines[0].password == "pa:ss"  # a colon in the password is kept
    assert "pa:ss" not in repr(lines[0])
    for text in ("isp.example:x:user:secretpw", "isp.example:10001:secretpw", "bad host:1:u:p"):
        with pytest.raises(ProxyAdminError) as e:
            parse_lines(text)
        assert "secretpw" not in str(e.value) and "line 1" in str(e.value)


# --- adapters ---------------------------------------------------------------------------------


def test_generic_adapter_and_usage():
    adapters = load_adapters()
    assert GENERIC in adapters
    assert adapters[GENERIC].usage(None, b"", datetime.now(UTC)) is None
    with pytest.raises(ValueError):
        Usage(used_bytes=-1)
    with pytest.raises(ValueError):
        Usage(used_bytes=1, state="fine")


# --- CLI ------------------------------------------------------------------------------------------


def test_cli_credentials_from_stdin(monkeypatch, capsys):
    from leasehold import proxy_cli

    sent = []
    monkeypatch.setattr(proxy_cli, "_call", lambda m, p, b=None: sent.append((m, p, b)) or
                        {"credentials_fp": "abc"})  # fmt: skip
    monkeypatch.setattr("sys.stdin", io.StringIO("user-x:pw-y\n"))
    from leasehold.cli import main

    assert main(["proxy", "account", "set-credentials", "isp-a"]) == 0
    (m, p, b) = sent[0]
    assert p == "/v1/admin/proxies/accounts/isp-a/credentials"
    assert b == {"username": "user-x", "password": "pw-y"}
    out = capsys.readouterr().out
    assert "pw-y" not in out and json.loads(out) == {"credentials_fp": "abc"}

    monkeypatch.setattr("sys.stdin", io.StringIO("h.example:1:user-x:pw-y\n"))
    assert main(["proxy", "import", "--account", "isp-a", "--kind", "isp", "--country", "NL"]) == 0
    assert sent[1][2]["lines"] == "h.example:1:user-x:pw-y\n" and sent[1][2]["country"] == "NL"
    assert "pw-y" not in capsys.readouterr().out


def test_planner_gives_proxies_their_capacity():
    from leasehold.egress.config import Budgets
    from leasehold.egress.planner import EgressRow, plan_proxies

    budgets = Budgets.model_validate({"network": {"per_subnet": 30}})
    exits = [EgressRow("203.0.113.1", "203.0.113.0/24", 64501),
             EgressRow("203.0.113.2", "203.0.113.0/24", 64501)]  # fmt: skip
    out = plan_proxies(exits, {"pool:resi": 4}, {"cdn-a": 20, "cdn-b": 20},
                       {("cdn-a", "normal"): 5}, budgets)  # fmt: skip
    got = {(a.egress, a.provider, a.klass): a.tokens for a in out}
    # Only providers with demand; the /24 cap splits between the two exits; the pool
    # counts as four egresses and has no network level.
    assert got == {("203.0.113.1", "cdn-a", "*"): 15, ("203.0.113.2", "cdn-a", "*"): 15,
                   ("pool:resi", "cdn-a", "*"): 80}  # fmt: skip
