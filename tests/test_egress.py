"""Egress configuration files and provider tagging."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from leasehold.egress import config
from leasehold.egress.config import ConfigError, ProviderRules
from leasehold.egress.tagging import Tagger

REGISTRY = """
defaults: { asn: 64500, country: NL }
egresses:
  - { address: 192.0.2.10, host: worker-1, ipv6: "2001:db8:1::5/64" }
  - { address: 192.0.2.11, host: worker-2, status: drain }
  - { address: 198.51.100.7, host: worker-3, asn: 64501, subnet: 198.51.100.0/25 }
"""

PROVIDERS = """
providers:
  example-cdn:
    kind: cdn
    cname: [cdn.example.net]
    cidr: [203.0.113.0/24]
    cidr_files: [ranges/cdn.txt]
  example-shop:
    kind: platform
    cname: [shops.example.com]
    cidr: [203.0.113.128/25]
  example-parking:
    kind: parking
    cidr: [198.18.0.1/32]
"""

BUDGETS = """
planning: { window: 60s, horizon: 15m, lend_after: 30s }
classes: { order: [critical, normal, bulk], dev_offset: 1, min_share: { bulk: 0.1 } }
providers:
  default: 30
  per_kind: { parking: null }
  overrides: { example-shop: 20 }
shared_ip: { min_domains: 1000, per_egress: 30, fleet_per_second: 2 }
network: { per_subnet: 600, per_asn: 1200 }
"""


def write(tmp: Path, registry=REGISTRY, providers=PROVIDERS, budgets=BUDGETS) -> Path:
    (tmp / "ranges").mkdir(exist_ok=True)
    (tmp / "ranges" / "cdn.txt").write_text("# extra\n198.51.100.128/25\n\n")
    (tmp / "registry.yml").write_text(textwrap.dedent(registry))
    (tmp / "providers.yml").write_text(textwrap.dedent(providers))
    (tmp / "budgets.yml").write_text(textwrap.dedent(budgets))
    return tmp


def test_load(tmp_path):
    cfg = config.load(write(tmp_path))
    by = {e.address: e for e in cfg.egresses}
    assert by["192.0.2.10"].subnet == "192.0.2.0/24"
    assert by["192.0.2.10"].ipv6 == "2001:db8:1::/64"
    assert by["192.0.2.10"].asn == 64500 and by["192.0.2.10"].country == "NL"
    assert by["192.0.2.11"].status == "drain"
    assert by["198.51.100.7"].subnet == "198.51.100.0/25" and by["198.51.100.7"].asn == 64501
    assert cfg.rules.ranges["198.51.100.128/25"] == "example-cdn"  # from the range file
    assert cfg.rules.providers == {
        "example-cdn": "cdn",
        "example-parking": "parking",
        "example-shop": "platform",
    }
    assert cfg.provider_budget("example-cdn") == 30
    assert cfg.provider_budget("example-shop") == 20
    assert cfg.provider_budget("example-parking") is None
    assert cfg.provider_budget("ip:192.0.2.99") == 30
    assert cfg.provider_budget("") is None
    assert cfg.budgets.network.per_asn == 1200


@pytest.mark.parametrize(
    "registry, message",
    [
        ("egresses: [{address: 192.0.2.1}, {address: 192.0.2.1}]", "listed twice"),
        ("egresses: [{address: nope}]", "not an IP address"),
        ("egresses: [{address: 192.0.2.1, subnet: 10.0.0.0/8}]", "not in its subnet"),
        ("egresses: [{address: 192.0.2.1, ipv6: 10.0.0.0/8}]", "not an IPv6 network"),
    ],
)
def test_registry_errors(tmp_path, registry, message):
    with pytest.raises(ConfigError, match=message):
        config.load(write(tmp_path, registry=registry))


def test_per_domain_providers_need_a_budget(tmp_path):
    budgets = BUDGETS.replace("default: 30", "default: null")
    with pytest.raises(ConfigError, match="no budget for example-cdn"):
        config.load(write(tmp_path, budgets=budgets))


def test_unknown_override(tmp_path):
    budgets = BUDGETS.replace("example-shop: 20", "missing: 20")
    with pytest.raises(ConfigError, match="unknown provider 'missing'"):
        config.load(write(tmp_path, budgets=budgets))


def test_rule_listed_for_two_providers(tmp_path):
    providers = PROVIDERS + "  other:\n    kind: cdn\n    cname: [cdn.example.net]\n"
    with pytest.raises(ConfigError, match="listed for example-cdn and other"):
        config.load(write(tmp_path, providers=providers))


def test_missing_range_file(tmp_path):
    write(tmp_path)
    (tmp_path / "ranges" / "cdn.txt").unlink()
    with pytest.raises(ConfigError, match="does not exist"):
        config.load(tmp_path)


def test_bad_planning(tmp_path):
    budgets = BUDGETS.replace("lend_after: 30s", "lend_after: 2m")
    with pytest.raises(ConfigError, match="lend_after"):
        config.load(write(tmp_path, budgets=budgets))


# --- tagging ---------------------------------------------------------------------------------

RULES = ProviderRules(
    cnames={"cdn.example.net": "example-cdn", "shops.example.com": "example-shop"},
    ranges={
        "203.0.113.0/24": "example-cdn",
        "203.0.113.128/25": "example-shop",
        "198.18.0.1/32": "example-parking",
        "2001:db8:ff::/48": "example-cdn",
    },
    providers={"example-cdn": "cdn", "example-shop": "platform", "example-parking": "parking"},
)


@pytest.fixture
def tagger():
    return Tagger(RULES, shared_ips=["192.0.2.50", "not-an-ip"], min_domains=1000)


def test_cname_first(tagger):
    t = tagger.tag("a.example", "192.0.2.1", ["www.a.example", "x.shops.example.com"])
    assert (t.provider, t.target, t.source, t.kind) == (
        "example-shop",
        "a.example",
        "cname",
        "platform",
    )


def test_cname_matches_on_label_boundaries(tagger):
    assert tagger.tag("a.example", "192.0.2.1", ["cdn.example.net."]).provider == "example-cdn"
    assert tagger.tag("a.example", "192.0.2.1", ["Edge.CDN.example.net"]).provider == "example-cdn"
    # 'notcdn.example.net' is not under 'cdn.example.net'
    t = tagger.tag("a.example", "192.0.2.1", ["notcdn.example.net"])
    assert (t.provider, t.target) == ("", "192.0.2.1")


def test_longest_prefix(tagger):
    assert tagger.tag("a.example", "203.0.113.5", []).provider == "example-cdn"
    assert tagger.tag("a.example", "203.0.113.200", []).provider == "example-shop"
    assert tagger.tag("a.example", "2001:db8:ff::1", []).provider == "example-cdn"


def test_known_provider_before_shared_ip():
    t = Tagger(RULES, shared_ips=["203.0.113.9"], min_domains=1000)
    assert t.tag("a.example", "203.0.113.9", []).provider == "example-cdn"


def test_shared_ip(tagger):
    t = tagger.tag("a.example", "192.0.2.50", [])
    assert (t.provider, t.target, t.source) == ("ip:192.0.2.50", "a.example", "shared_ip")


def test_parking_is_leased_per_ip(tagger):
    """Parking has no budget, so its domains must share the server's target limit."""
    t = tagger.tag("a.example", "198.18.0.1", [])
    assert (t.provider, t.target, t.kind) == ("example-parking", "198.18.0.1", "parking")


def test_plain_hosting_is_leased_per_ip(tagger):
    t = tagger.tag("a.example", "192.0.2.1", [])
    assert (t.provider, t.target, t.source) == ("", "192.0.2.1", "none")


def test_no_ip(tagger):
    assert tagger.tag("a.example", "", []).target == "a.example"
    assert tagger.tag("a.example", None, ["cdn.example.net"]).target == "a.example"


def test_range_with_host_bits_is_an_error(tmp_path):
    providers = PROVIDERS.replace("203.0.113.0/24", "203.0.113.5/24")
    with pytest.raises(ConfigError, match="host bits set"):
        config.load(write(tmp_path, providers=providers))


TARGETS = (
    BUDGETS
    + """
targets:
  interval: 5s
  per_kind: { cdn: 8s }
  overrides:
    api.example.com: { interval: 3s }
    "*.example.com": { interval: 10s, per_hour: 100 }
    "*.deep.example.com": { per_hour: 50 }
    192.0.2.7: { interval: 2s }
    "2001:db8::7": { interval: 4s }
pair: { per_hour: 600 }
"""
)


@pytest.mark.parametrize(
    "target, kind, seconds, per_hour",
    [
        ("api.example.com", "cdn", 3, 600),  # exact, not merged with the pattern
        ("www.example.com", "cdn", 10, 100),
        ("a.b.example.com", "none", 10, 100),
        ("x.deep.example.com", "cdn", 8, 50),  # the longest pattern only
        ("example.com", "none", 5, 600),  # a pattern leaves out the host itself
        ("notexample.com", "cdn", 8, 600),  # label boundaries
        ("192.0.2.7", "none", 2, 600),
        ("2001:db8::7", "none", 4, 600),
        ("192.0.2.8", "none", 5, 600),
        ("no-dots", "none", 5, 600),
    ],
)
def test_target_overrides(tmp_path, target, kind, seconds, per_hour):
    loaded = config.load(write(tmp_path, budgets=TARGETS)).budgets
    # As stored in Postgres and read back by the dispatcher.
    for b in (loaded, config.Budgets.model_validate(loaded.model_dump(mode="json"))):
        assert b.targets.interval_for(kind, target).total_seconds() == seconds
        assert b.pair_per_hour(target) == per_hour


@pytest.mark.parametrize(
    "override",
    [
        '"api.*.com": { interval: 3s }',
        '"*": { interval: 3s }',
        "API.example.com: { interval: 3s }",
        '"https://api.example.com": { interval: 3s }',
        "api.example.com: {}",
        "api.example.com: { interval: 0s }",
        "api.example.com: { per_hour: 0 }",
        "api.example.com: { intervall: 3s }",
    ],
)
def test_bad_target_overrides(tmp_path, override):
    with pytest.raises(ConfigError, match="overrides"):
        config.load(write(tmp_path, budgets=f"targets:\n  overrides:\n    {override}\n"))
