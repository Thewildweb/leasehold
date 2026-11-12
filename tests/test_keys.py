"""normalize_domain, normalize_host and normalize_url."""

from __future__ import annotations

import pytest

from leasehold.keys import InvalidKey, normalize_domain, normalize_host, normalize_url


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("example.nl", "example.nl"),
        ("Example.NL", "example.nl"),
        (" www.example.nl ", "example.nl"),
        ("example.nl.", "example.nl"),
        ("shop.example.nl", "example.nl"),
        ("*.shop.example.nl", "example.nl"),
        ("www.example.co.uk/contact", "example.co.uk"),
        ("example.nl:8080/a?b#c", "example.nl"),
        ("https://user@Shop.Example.nl:8443/x", "example.nl"),
        ("info@Bakker.nl", "bakker.nl"),
        ("münchen.de", "xn--mnchen-3ya.de"),
        ("xn--mnchen-3ya.de", "xn--mnchen-3ya.de"),
        ("foo.github.io", "foo.github.io"),  # private section of the suffix list
        ("_dmarc.example.nl", "example.nl"),
        ("gemeente.amsterdam", "gemeente.amsterdam"),
    ],
)
def test_normalize_domain(value, expected):
    assert normalize_domain(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        "localhost",
        "nl",
        "co.uk",
        "github.io",
        "example.notatld",
        "192.168.1.1",
        "[2001:db8::1]",
        "exa mple.nl",
        "-example.nl",
        "example-.nl",
        "a" * 64 + ".nl",
        "http://",
    ],
)
def test_normalize_domain_rejects(value):
    with pytest.raises(InvalidKey):
        normalize_domain(value)


def test_normalize_host_keeps_subdomains():
    assert normalize_host("WWW.Shop.Example.nl.") == "www.shop.example.nl"
    assert normalize_host("*.example.nl") == "example.nl"
    with pytest.raises(InvalidKey):
        normalize_host("10.0.0.1")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # scheme, host, default ports, empty path
        ("HTTP://Example.NL", "http://example.nl/"),
        ("https://example.nl:443", "https://example.nl/"),
        ("http://example.nl:80/", "http://example.nl/"),
        ("https://example.nl:8443/", "https://example.nl:8443/"),
        ("https://example.nl./a", "https://example.nl/a"),
        # kept distinct on purpose
        ("http://example.nl/", "http://example.nl/"),
        ("https://www.example.nl/", "https://www.example.nl/"),
        ("https://example.nl/a/", "https://example.nl/a/"),
        ("https://example.nl/a", "https://example.nl/a"),
        # user info and fragment dropped
        ("https://user:pw@example.nl/a#top", "https://example.nl/a"),
        # dot segments
        ("https://example.nl/a/./b/../c", "https://example.nl/a/c"),
        ("https://example.nl/a/..", "https://example.nl/"),
        ("https://example.nl/../../a", "https://example.nl/a"),
        # percent-encoding
        ("https://example.nl/%7euser/%2f", "https://example.nl/~user/%2F"),
        ("https://example.nl/caf%c3%a9", "https://example.nl/caf%C3%A9"),
        ("https://example.nl/café", "https://example.nl/caf%C3%A9"),
        ("https://example.nl/a b", "https://example.nl/a%20b"),
        ("https://example.nl/100%", "https://example.nl/100%25"),
        # IDN
        ("https://München.de/", "https://xn--mnchen-3ya.de/"),
        # IP hosts
        ("http://[2001:DB8::0:1]:8080/x", "http://[2001:db8::1]:8080/x"),
        ("http://192.168.1.1/", "http://192.168.1.1/"),
        # query: sorted, tracking removed, empty dropped, values kept as written
        ("https://example.nl/?b=2&a=1", "https://example.nl/?a=1&b=2"),
        ("https://example.nl/?a=2&a=1", "https://example.nl/?a=1&a=2"),
        ("https://example.nl/?a=&a", "https://example.nl/?a&a="),
        ("https://example.nl/?q=a+b&x=%7e", "https://example.nl/?q=a+b&x=~"),
        ("https://example.nl/?", "https://example.nl/"),
        ("https://example.nl/?&&a=1&", "https://example.nl/?a=1"),
        (
            "https://example.nl/p?utm_source=nl&UTM_Medium=x&utm%5Fcampaign=y&id=3"
            "&gclid=1&fbclid=2&_ga=3&mc_eid=4",
            "https://example.nl/p?id=3",
        ),
        ("https://example.nl/?a=1=2", "https://example.nl/?a=1=2"),
    ],
)
def test_normalize_url(value, expected):
    assert normalize_url(value) == expected


def test_normalize_url_is_idempotent():
    for value in [
        "HTTPS://user@Shop.München.de:443/a/../b/%7e?utm_source=x&z=1&a=caf%c3%a9#f",
        "http://[2001:DB8::1]/x y",
    ]:
        once = normalize_url(value)
        assert normalize_url(once) == once


def test_normalize_url_default_scheme():
    assert normalize_url("www.example.nl/a", default_scheme="https") == "https://www.example.nl/a"
    with pytest.raises(InvalidKey, match="no scheme"):
        normalize_url("www.example.nl/a")


@pytest.mark.parametrize(
    "value",
    [
        "ftp://example.nl/",
        "mailto:info@example.nl",
        "https://",
        "https://example.nl:99999/",
        "https://exa mple.nl/",
        "https://localhost/",
    ],
)
def test_normalize_url_rejects(value):
    with pytest.raises(InvalidKey):
        normalize_url(value)
