"""The mock CT log: RFC 6962 entries, paging, growth, rate limits and test controls."""

from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.request

import pytest
from cryptography import x509

from leasehold.mocks.ctlog import (
    PRECERT_ENTRY,
    X509_ENTRY,
    Log,
    MockCTLog,
    domains_for,
    entry,
    entry_type_for,
)

LOG = "mock_argon_2026h2"


def _cert(e: dict) -> tuple[int, x509.Certificate]:
    """Parse an entry the way a CT client does."""
    leaf = base64.b64decode(e["leaf_input"])
    assert leaf[0] == 0 and leaf[1] == 0  # v1, timestamped_entry
    etype = int.from_bytes(leaf[10:12], "big")
    if etype == X509_ENTRY:
        n = int.from_bytes(leaf[12:15], "big")
        der = leaf[15 : 15 + n]
    else:
        extra = base64.b64decode(e["extra_data"])
        n = int.from_bytes(extra[0:3], "big")
        der = extra[3 : 3 + n]
        tbs_len = int.from_bytes(leaf[44:47], "big")  # after the 32-byte issuer key hash
        assert tbs_len > 0
    return etype, x509.load_der_x509_certificate(der)


def test_entries_hold_the_domains_of_their_index():
    for i in range(12):
        etype, cert = _cert(entry(LOG, i))
        assert etype == entry_type_for(i)
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        assert san.get_values_for_type(x509.DNSName) == domains_for(LOG, i)
    assert {entry_type_for(i) for i in range(3)} == {X509_ENTRY, PRECERT_ENTRY}
    assert entry(LOG, 1) != entry("other_log", 1)


def test_domains_cover_the_parser_cases():
    names = [n for i in range(0, 56) for n in domains_for(LOG, i)]
    assert any(n.startswith("*.") for n in names)
    assert any(n.startswith("www.") for n in names)
    assert any(n != n.lower() for n in names)


@pytest.fixture
def mock():
    m = MockCTLog(
        [Log("a_log", "OpA", 50), Log("b_log", "OpA", 10), Log("c_log", "OpB", 10)],
        rps=0,
        max_entries=20,
    )
    server = m.serve()
    m.base = f"http://127.0.0.1:{server.server_address[1]}"
    yield m
    server.shutdown()


def _get(m, path, client=None):
    req = urllib.request.Request(m.base + path, headers={"X-Mock-Client": client} if client else {})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.load(r), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e), dict(e.headers)


def _post(m, path, body):
    req = urllib.request.Request(m.base + path, data=json.dumps(body).encode(), method="POST")
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def test_sth_paging_and_bounds(mock):
    status, sth, _ = _get(mock, "/logs/a_log/ct/v1/get-sth")
    assert status == 200 and sth["tree_size"] == 50
    _, page, _ = _get(mock, "/logs/a_log/ct/v1/get-entries?start=0&end=49")
    assert len(page["entries"]) == 20  # capped, the client must ask again
    _, page, _ = _get(mock, "/logs/a_log/ct/v1/get-entries?start=45&end=99")
    assert len(page["entries"]) == 5  # up to the tree size
    assert _get(mock, "/logs/a_log/ct/v1/get-entries?start=50&end=60")[0] == 400
    assert _get(mock, "/logs/a_log/ct/v1/get-entries?start=5&end=1")[0] == 400
    assert _get(mock, "/logs/nope/ct/v1/get-sth")[0] == 404


def test_log_list_points_at_the_mock(mock):
    _, ll, _ = _get(mock, "/log_list.json")
    ops = {o["name"]: [lg["url"] for lg in o["logs"]] for o in ll["operators"]}
    assert ops["OpA"] == [f"{mock.base}/logs/a_log/", f"{mock.base}/logs/b_log/"]
    assert all("usable" in lg["state"] for o in ll["operators"] for lg in o["logs"])


def test_grow_fail_reset(mock):
    assert _post(mock, "/_mock/grow", {"log": "b_log", "n": 5})["tree_size"] == 15
    _post(mock, "/_mock/fail", {"log": "b_log", "status": 503, "count": 2})
    codes = [_get(mock, "/logs/b_log/ct/v1/get-sth")[0] for _ in range(3)]
    assert codes == [503, 503, 200]
    _post(mock, "/_mock/reset", {})
    assert _get(mock, "/logs/b_log/ct/v1/get-sth")[1]["tree_size"] == 10
    _, d, _ = _get(mock, "/_mock/domains?log=b_log&start=2&end=3")
    assert d == {"2": domains_for("b_log", 2), "3": domains_for("b_log", 3)}


def test_growth_over_time():
    lg = Log("g", "Op", 100, growth=1000.0)
    time.sleep(0.05)
    assert lg.size() > 100


def test_rate_limit_per_client_and_operator(mock):
    mock.rps = 3
    codes = [_get(mock, "/logs/a_log/ct/v1/get-sth", "ip1")[0] for _ in range(3)]
    status, _, headers = _get(mock, "/logs/b_log/ct/v1/get-sth", "ip1")  # same operator
    assert codes == [200, 200, 200] and status == 429 and headers["Retry-After"] == "1"
    assert _get(mock, "/logs/c_log/ct/v1/get-sth", "ip1")[0] == 200  # other operator
    assert _get(mock, "/logs/a_log/ct/v1/get-sth", "ip2")[0] == 200  # other client
    _, stats, _ = _get(mock, "/_mock/stats")
    assert stats["throttled"] == {"ip1": {"b_log": 1}}
    assert stats["requests"]["ip1"] == {"a_log": 3, "b_log": 1, "c_log": 1}
    assert stats["peak_per_second"]["ip1"]["OpA"] <= 3
