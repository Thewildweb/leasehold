"""Proxy routes in the runtime: the proxied client against a local proxy, the split
between proxy failures and target results, no connection and no name lookup except to
the proxy, redaction, and the worker's handling of routes."""

from __future__ import annotations

import copy
import json
import socket
import threading
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from conftest import RUNTIME_SPEC, FakeKafkaProducer
from proxyserver import LocalProxy, tls_target
from test_runtime_targets import _WORKERS, TargetControl, run_until

from leasehold.client import LeasedTask, LeaseRef, ProxySettings, Route, TargetLease
from leasehold.runtime.config import ConfigError, RuntimeConfig
from leasehold.runtime.context import Context
from leasehold.runtime.events import EventProducer
from leasehold.runtime.proxy import (
    ByteCount,
    ProxyFailure,
    classify_proxy_error,
    playwright_proxy,
    proxied_client,
    redact,
)
from leasehold.runtime.worker import Retry, Worker
from leasehold.spec import ServiceSpec

PASSWORD = "secret-pass"


@pytest.fixture
def proxy():
    p = LocalProxy("user", PASSWORD)
    yield p
    p.close()


def route_for(proxy: LocalProxy, password: str = PASSWORD, port: int | None = None) -> Route:
    return Route(
        egress="203.0.113.10",
        kind="isp",
        account="isp-a",
        proxy=ProxySettings("http", "127.0.0.1", port or proxy.port, "user", password),
        exit_ip="203.0.113.10",
        credentials_fp="aaaaaaaaaaaa",
    )


class _Watch:
    """Connections and name lookups made by one thread."""

    def __init__(self, monkeypatch):
        self.thread = threading.get_ident()
        self.connects: list = []
        self.lookups: list = []
        real_connect = socket.socket.connect
        real_getaddrinfo = socket.getaddrinfo
        watch = self

        def connect(sock, address):
            if threading.get_ident() == watch.thread:
                watch.connects.append(address)
            return real_connect(sock, address)

        def getaddrinfo(host, *a, **kw):
            if threading.get_ident() == watch.thread:
                watch.lookups.append(host)
            return real_getaddrinfo(host, *a, **kw)

        monkeypatch.setattr(socket.socket, "connect", connect)
        monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)


def test_nothing_leaves_except_to_the_proxy(proxy, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://198.51.100.99:1")  # must be ignored
    monkeypatch.setenv("NO_PROXY", "target.test")  # must be ignored
    with tls_target("target.test") as port:
        proxy.hosts["target.test"] = ("127.0.0.1", port)
        watch = _Watch(monkeypatch)
        count = ByteCount()
        with proxied_client(route_for(proxy), count, verify=False) as c:
            r = c.get(f"https://target.test:{port}/")
        assert r.status_code == 200 and r.text == "hello from the target"
    # Every connection went to the proxy, and target.test was never looked up here.
    assert watch.connects and all(a == ("127.0.0.1", proxy.port) for a in watch.connects)
    assert "target.test" not in watch.lookups
    assert proxy.requests == [f"CONNECT target.test:{port}"]
    assert proxy.users == ["user"]
    assert count.total > len("hello from the target")


def test_proxy_refusals_are_proxy_failures(proxy):
    with tls_target("target.test") as port:
        proxy.hosts["target.test"] = ("127.0.0.1", port)
        url = f"https://target.test:{port}/"
        with proxied_client(route_for(proxy, password="wrong"), verify=False) as c:
            with pytest.raises(ProxyFailure) as e:
                c.get(url)
        assert e.value.error.kind == "refused" and e.value.error.status == 407
        assert "wrong" not in str(e.value) and PASSWORD not in str(e.value)

        proxy.mode = "402"
        with proxied_client(route_for(proxy), verify=False) as c:
            with pytest.raises(ProxyFailure) as e:
                c.get(url)
        assert e.value.error.status == 402

        proxy.mode = "quota"
        with proxied_client(route_for(proxy), verify=False) as c:
            with pytest.raises(ProxyFailure) as e:
                c.get(url)
        assert e.value.error.status == 403 and e.value.error.kind == "refused"


def test_unreachable_proxy_is_a_connect_failure():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # nothing listens there now
    r = Route("pool:x", "residential", "a", ProxySettings("http", "127.0.0.1", port, "u", "p"))
    with proxied_client(r, timeout=2) as c:
        with pytest.raises(ProxyFailure) as e:
            c.get("https://target.test/")
    assert e.value.error.kind == "connect"


def test_plain_http_407_is_the_proxy(proxy):
    with proxied_client(route_for(proxy, password="wrong")) as c:
        with pytest.raises(ProxyFailure) as e:
            c.get("http://target.test/")
    assert e.value.error.status == 407


def test_client_settings_the_route_owns(proxy):
    for k in ("proxy", "mounts", "transport", "trust_env"):
        with pytest.raises(ValueError):
            proxied_client(route_for(proxy), **{k: None})


def test_redaction():
    assert redact("via http://user:pw@host:8000/x failed") == "via http://***@host:8000/x failed"
    r = Route("pool:x", "residential", "a", ProxySettings("http", "h", 1, "user-x", "pw-y"))
    assert "pw-y" not in repr(r) and "user-x" not in repr(r)
    assert r.proxy.redacted_url() == "http://***@h:1"
    e = classify_proxy_error(httpx.ProxyError("407 Proxy Authentication Required"))
    assert e.status == 407 and e.kind == "refused"
    e = classify_proxy_error(httpx.ProxyError("502 Bad Gateway"))
    assert e.kind == "gateway" and e.status == 502
    e = classify_proxy_error(httpx.ProxyError("500 Internal Server Error"))
    assert e.kind == "connect"
    assert playwright_proxy(r) == {"server": "http://h:1", "username": "user-x",
                                   "password": "pw-y"}  # fmt: skip


# --- the worker ---------------------------------------------------------------------------------


def spec(tiers) -> ServiceSpec:
    data = copy.deepcopy(RUNTIME_SPEC)
    tt = data["frontier"]["task_types"]["jobs.item"]
    tt["target_leases"] = True
    tt["egress"] = {"tiers": tiers}
    return ServiceSpec.model_validate(data)


class RouteControl(TargetControl):
    """Target leases that come with a route (or not, with ``route=None``)."""

    def __init__(self, keys, route):
        super().__init__(keys)
        self.route = route

    def lease_with_targets(self, max, task_types=None, worker_id=None, per_group=None,
                           held=None, egress=None):  # fmt: skip
        tasks, targets = super().lease_with_targets(max, task_types, worker_id, per_group, held,
                                                    egress)  # fmt: skip
        targets = [
            TargetLease(
                t.target, t.lease_id, t.expires_at, t.interval_s, tier="isp", route=self.route
            )  # fmt: skip
            for t in targets
        ]
        return tasks, targets


def make(handler, control, tiers, egress=None):
    cfg = RuntimeConfig(
        env="dev",
        service="jobs",
        component="worker",
        spec=spec(tiers),
        instance="alloc-1",
        egress_ip=egress,
        payload_models={"jobs.results.v1": "payload_models:R"},
    )
    producer = FakeKafkaProducer()
    ctx = Context(cfg, client=control, producer=EventProducer(cfg, producer=producer))
    w = Worker(ctx, handler, 2)
    w.kafka = producer
    _WORKERS.append(w)
    return w


def test_proxy_only_worker_needs_no_egress_but_mixed_does():
    make(lambda t, c: None, RouteControl([], None), [{"isp": ["isp-a"]}])
    with pytest.raises(ConfigError, match="LEASEHOLD_EGRESS_IP"):
        make(lambda t, c: None, RouteControl([], None), ["datacenter", {"isp": ["isp-a"]}])


def test_worker_fetches_through_the_route(proxy):
    with tls_target("target.test") as port:
        proxy.hosts["target.test"] = ("127.0.0.1", port)
        route = route_for(proxy)

        def handle(task, ctx):
            assert task.route is route and task.tier == "isp"
            with ctx.http(verify=False) as c:
                r = c.get(f"https://target.test:{port}/")
            ctx.emit("results", {"id": task.key, "n": len(r.text)})

        control = RouteControl(["a:1"], route)
        w = make(handle, control, [{"isp": ["isp-a"]}])
        run_until(w, lambda: len(control.acked) == 1)
    (ack,) = control.acked
    assert ack.bytes and ack.bytes > 0
    assert control.egresses[0] is None  # no own egress sent
    (_, _, value) = w.kafka.messages[0]
    env = json.loads(value)
    assert env["egress"] == "203.0.113.10" and env["egress_ip"] == "203.0.113.10"
    assert env["proxy_account"] == "isp-a" and PASSWORD not in value.decode()


def test_proxy_failure_goes_back_untried_and_stops_the_target(proxy):
    route = route_for(proxy, password="wrong")

    def handle(task, ctx):
        with ctx.http(verify=False) as c:
            c.get("https://target.test/")

    control = RouteControl(["a:1"], route)
    w = make(handle, control, [{"isp": ["isp-a"]}])
    run_until(w, lambda: len(control.released) == 1)
    (rel,) = control.released
    assert rel.proxy_error.status == 407 and not rel.cooldown and rel.after is None
    assert rel.route.egress == "203.0.113.10" and rel.json()["route"]["credentials_fp"]
    assert not control.failed
    assert PASSWORD not in json.dumps(rel.json())
    assert all(t.lost == "proxy error" for t in w.targets.values())


def test_gateway_answer_is_an_attempt_and_keeps_the_target(proxy):
    proxy.mode = "502"
    route = route_for(proxy)

    def handle(task, ctx):
        with ctx.http(verify=False) as c:
            c.get("https://dead.test/")

    control = RouteControl(["a:1"], route)
    w = make(handle, control, [{"isp": ["isp-a"]}])
    run_until(w, lambda: len(control.failed) == 1)
    (f,) = control.failed
    assert f.proxy_error.kind == "gateway" and f.proxy_error.status == 502
    assert not f.cooldown and not f.permanent and not control.released
    assert f.route.account == "isp-a" and f.route.egress == "203.0.113.10"
    assert f.route.credentials_fp == "aaaaaaaaaaaa" and f.route.lease_id is not None
    assert f.json()["route"]["account"] == "isp-a"
    assert all(t.lost != "proxy error" for t in w.targets.values())


def test_task_without_route_never_runs_on_a_proxy_only_type():
    ran = []
    control = RouteControl(["a:1"], None)
    w = make(lambda t, c: ran.append(t.key), control, [{"isp": ["isp-a"]}])
    run_until(w, lambda: len(control.released) == 1)
    assert ran == [] and "only use proxies" in control.released[0].reason


def test_pushback_cause_is_reported():
    def handle(task, ctx):
        ctx.emit("results", {"id": task.key, "n": 1})
        raise Retry("challenge page", 600, cooldown=True, cause="challenge")

    control = TargetControl(["a:1"])
    cfg_spec = copy.deepcopy(RUNTIME_SPEC)
    cfg_spec["frontier"]["task_types"]["jobs.item"]["target_leases"] = True
    w = make(handle, control, ["datacenter", {"isp": ["isp-a"]}], egress="192.0.2.7")
    run_until(w, lambda: len(control.failed) == 1)
    (f,) = control.failed
    assert f.cooldown and f.cause == "challenge" and f.json()["cause"] == "challenge"
    with pytest.raises(ValueError):
        Retry("x", 1, cause="challenge")  # a cause needs cooldown


def test_route_parse_round_trip():
    d = {
        "egress": "pool:resi", "kind": "residential", "country": "NL", "account": "resi-a",
        "pool": "resi", "proxy": {"scheme": "http", "host": "gw.example", "port": 9000,
                                  "username": "u-nl", "password": "p"},
        "session": {"id": "s1", "ttl_s": 600}, "exit_ip": None, "exit_ip_header": "X-Exit",
        "account_remaining": 0.5, "errors": [{"status": 407, "state": "auth_failed"}],
        "credentials_fp": "abc", "expires_at": datetime.now(UTC).isoformat(),
    }  # fmt: skip
    r = Route.parse(d)
    assert r.session == "s1" and r.session_ttl_s == 600 and r.pool == "resi"
    assert r.proxy.url() == "http://u-nl:p@gw.example:9000"
    _ = LeasedTask(LeaseRef("jobs.item", "k", uuid.uuid4()), datetime.now(UTC) +
                   timedelta(seconds=5), 50, 0)  # fmt: skip
