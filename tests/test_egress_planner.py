"""The budget planner's allocation of one window, without a database."""

from __future__ import annotations

from leasehold.egress.config import Budgets
from leasehold.egress.planner import (
    ANY_CLASS,
    EgressRow,
    capacities,
    effective_class,
    plan_window,
)


def budgets(**kw) -> Budgets:
    data = {
        "planning": {"window": "60s", "horizon": "15m"},
        "classes": {"order": ["critical", "normal", "bulk"], "min_share": {"bulk": 0.1}},
        "shared_ip": {"min_domains": 1000, "per_egress": 30, "fleet_per_second": 2},
        "network": {"per_subnet": 600, "per_asn": 1200},
    }
    data.update(kw)
    return Budgets.model_validate(data)


# 10 egresses in one /24, 2 in another, all in one ASN.
EGRESSES = [EgressRow(f"192.0.2.{i}", "192.0.2.0/24", 64500) for i in range(1, 11)] + [
    EgressRow(f"198.51.100.{i}", "198.51.100.0/24", 64500) for i in range(1, 3)
]


def by(allocs, **match):
    return sum(a.tokens for a in allocs if all(getattr(a, k) == v for k, v in match.items()))


def test_effective_class():
    b = budgets()
    assert effective_class("critical", "prod", b) == "critical"
    assert effective_class("critical", "dev", b) == "normal"
    assert effective_class("bulk", "dev", b) == "bulk"
    assert effective_class("unknown", "prod", b) == "bulk"


def test_network_level_caps_each_egress():
    caps = capacities(EGRESSES, 100, budgets())
    # /24 of 10: 600/10 = 60; ASN of 12: 1200/12 = 100; per egress 100 → 60.
    assert caps["192.0.2.1"] == 60
    # /24 of 2: 300; ASN 100; per egress 100 → 100.
    assert caps["198.51.100.1"] == 100
    assert sum(caps.values()) <= 1200


def test_classes_in_order_with_minimum_for_the_last():
    # 12 egresses × 30/min = 360 tokens a window. Demand over 15 windows.
    demand = {
        ("example-cdn", "critical"): 15 * 100,
        ("example-cdn", "normal"): 15 * 300,
        ("example-cdn", "bulk"): 15 * 300,
    }
    allocs = plan_window(EGRESSES, {"example-cdn": 30}, demand, budgets())
    assert by(allocs, provider="example-cdn") == 360
    assert by(allocs, klass="critical") == 100
    assert by(allocs, klass="bulk") == 36  # its 10% minimum
    assert by(allocs, klass="normal") == 224
    assert by(allocs, klass=ANY_CLASS) == 0
    # No egress gets more than its capacity.
    per_egress = {}
    for a in allocs:
        per_egress[a.egress] = per_egress.get(a.egress, 0) + a.tokens
    assert max(per_egress.values()) <= 30


def test_unused_capacity_goes_to_any_class():
    allocs = plan_window(EGRESSES, {"example-cdn": 30}, {("example-cdn", "normal"): 150},
                         budgets())  # fmt: skip
    assert by(allocs, klass="normal") == 10
    assert by(allocs, klass=ANY_CLASS) == 350
    assert by(allocs, klass="bulk") == 0  # no demand: no minimum set aside


def test_no_demand_still_allocates_to_any():
    allocs = plan_window(EGRESSES, {"example-cdn": 30}, {}, budgets())
    assert by(allocs) == 360 and {a.klass for a in allocs} == {ANY_CLASS}


def test_shared_ip_fleet_cap_on_few_egresses():
    demand = {("ip:203.0.113.9", "normal"): 10_000}
    allocs = plan_window(EGRESSES, {}, demand, budgets())
    assert by(allocs, provider="ip:203.0.113.9") == 120  # 2/s for a minute
    assert len({a.egress for a in allocs}) == 4  # 30 each
    # Without demand a shared IP isn't planned.
    assert plan_window(EGRESSES, {}, {}, budgets()) == []


def test_no_egresses():
    assert plan_window([], {"example-cdn": 30}, {}, budgets()) == []
