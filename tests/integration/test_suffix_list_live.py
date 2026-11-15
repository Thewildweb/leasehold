"""ClickHouse's cutToFirstSignificantSubdomainCustom with the exported suffix list
against normalize_domain, for names built from every rule (the dev stack)."""

from __future__ import annotations

import os

import httpx
import pytest

from leasehold.keys import (
    CLICKHOUSE_SUFFIX_LIST,
    InvalidKey,
    clickhouse_suffix_list,
    normalize_domain,
)

CH = os.environ.get("LEASEHOLD_TEST_CLICKHOUSE")  # e.g. http://localhost:18123
pytestmark = pytest.mark.skipif(not CH, reason="LEASEHOLD_TEST_CLICKHOUSE not set")


def _py(name: str) -> str:
    try:
        return normalize_domain(name)
    except InvalidKey:
        return ""


def _names() -> list[str]:
    names: set[str] = set()
    for rule in clickhouse_suffix_list().splitlines():
        if rule.startswith("//") or not rule.isascii():
            continue
        if rule.startswith("!"):
            names |= {rule[1:], "a." + rule[1:]}
        elif rule.startswith("*."):
            names |= {"b." + rule[2:], "a.b." + rule[2:]}
        else:
            names |= {rule, "x." + rule, "y.x." + rule}
    return sorted(names)


def _clickhouse(names: list[str]) -> dict[str, str]:
    user = os.environ.get("LEASEHOLD_TEST_CLICKHOUSE_USER", "admin")
    password = os.environ.get("LEASEHOLD_TEST_CLICKHOUSE_PASSWORD", "dev-admin")
    q = (
        f"SELECT n, cutToFirstSignificantSubdomainCustom(n, '{CLICKHOUSE_SUFFIX_LIST}') AS c "
        "FROM (SELECT arrayJoin({ns:Array(String)}) AS n) FORMAT TSV"
    )
    out: dict[str, str] = {}
    for i in range(0, len(names), 3000):
        chunk = "[" + ",".join(f"'{n}'" for n in names[i : i + 3000]) + "]"
        r = httpx.post(CH, auth=(user, password), timeout=60,
                       files={"query": (None, q), "param_ns": (None, chunk)})  # fmt: skip
        r.raise_for_status()
        out.update(line.split("\t", 1) for line in r.text.splitlines())
    return out


def test_clickhouse_cuts_like_normalize_domain():
    names = _names()
    got = _clickhouse(names)
    differ = []
    for n in names:
        c = got[n]
        # A bare public suffix comes back as itself; normalize_domain rejects it.
        ch = c if _py(c) == c else ""
        if ch != _py(n):
            differ.append(n)
    # Known: a name that is itself a public suffix (a.prod.fastly.net, or aa.crm.dev
    # under *.aa.crm.dev) has no registrable domain to normalize_domain; ClickHouse
    # returns the domain above it (fastly.net). And www.ck: ClickHouse strips "www.".
    unexplained = [
        n for n in differ
        if n != "www.ck" and not (_py(n) == "" and n.endswith("." + got[n]))
    ]  # fmt: skip
    assert len(names) > 30000 and unexplained == []
