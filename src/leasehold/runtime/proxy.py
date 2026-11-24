"""Reaching a target through its proxy route, with no way around it.

A target on a proxy tier comes with a ``Route`` (``task.route``). For such a task:

- ``proxied_client(route)`` is the only client to use: every request goes through the
  route's proxy. Environment proxy settings (``HTTP_PROXY``, ``NO_PROXY``) are ignored.
  The target's name is resolved by the proxy (HTTP ``CONNECT host:port``, or ``socks5h``),
  never by this machine's resolver.
- A failure of the proxy itself is a ``ProxyFailure``: the task goes back untried (no
  attempt, no backoff, no cooldown, no event), and the control API counts it against the
  exit, pool or account. It is never retried without the proxy.
- ``ByteCount`` counts request and response bytes on the wire for the account's usage;
  the worker reports them with the outcome.
- Browsers: ``playwright_proxy(route)`` for ``browser.new_context(proxy=...)`` and
  ``FIREFOX_NO_DIRECT`` prefs (Camoufox/Firefox) so WebRTC and prefetching don't open
  direct connections.

How a failure is split: an error before the tunnel is up is the proxy's (connect refused
or timed out to the proxy, a ``CONNECT`` answered with an error status); anything after
it is the target's. A ``CONNECT`` refusal whose status (407, 402) or text matches the
account's rules (``route.errors``) means an account state; the control API decides
which. For plain ``http://`` targets the proxy forwards the request instead of tunnelling,
so only a 407 answer (which no site sends) is taken as the proxy's.

Logs and errors never show credentials: ``redact`` turns ``scheme://user:pass@host`` into
``scheme://***@host``.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

import httpx

from leasehold.client import ProxyError, Route
from leasehold.runtime.bytecount import ByteCount
from leasehold.runtime.worker import NoRoute, ProxyFailure

__all__ = [
    "ByteCount",
    "NoRoute",
    "ProxyFailure",
    "classify_proxy_error",
    "exit_ip",
    "playwright_proxy",
    "proxied_client",
    "redact",
]

_URL_CREDS = re.compile(r"(\w+://)[^/@\s]+@")
GATEWAY_STATUSES = (502, 503, 504)
_STATUS = re.compile(r"\b([1-5]\d\d)\b")

# Firefox preferences that keep a browser from leaving outside the proxy.
FIREFOX_NO_DIRECT = {
    "media.peerconnection.enabled": False,  # WebRTC
    "media.peerconnection.ice.default_address_only": True,
    "media.peerconnection.ice.no_host": True,
    "network.dns.disablePrefetch": True,
    "network.prefetch-next": False,
    "network.predictor.enabled": False,
    "network.http.speculative-parallel-limit": 0,
    "network.proxy.socks_remote_dns": True,
    "network.trr.mode": 5,  # no DNS over HTTPS of its own
}
# Chromium switches with the same purpose.
CHROMIUM_NO_DIRECT = [
    "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
    "--webrtc-ip-handling-policy=disable_non_proxied_udp",
    "--disable-features=DnsOverHttps,NetworkPrediction",
]


def redact(text: str) -> str:
    """Drop ``user:pass@`` from every URL in a text."""
    return _URL_CREDS.sub(r"\1***@", text or "")


def _request_bytes(request: httpx.Request) -> int:
    head = len(request.method) + len(request.url.raw_path) + 12
    head += sum(len(k) + len(v) + 4 for k, v in request.headers.raw)
    body = request.headers.get("content-length")
    return head + (int(body) if body and body.isdigit() else 0)


class _CountingStream(httpx.SyncByteStream):
    def __init__(self, inner: Any, count: ByteCount):
        self._inner = inner
        self._count = count

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self._inner:
            self._count.add(len(chunk))
            yield chunk

    def close(self) -> None:
        close = getattr(self._inner, "close", None)
        if close is not None:
            close()


class _RouteTransport(httpx.BaseTransport):
    """The proxy transport, counting bytes and turning proxy failures into
    ``ProxyFailure``."""

    def __init__(self, route: Route, count: ByteCount, **kw: Any):
        self.route = route
        self.count = count
        proxy = httpx.Proxy(route.proxy.url())
        self._inner = httpx.HTTPTransport(proxy=proxy, trust_env=False, **kw)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.count.add(_request_bytes(request))
        try:
            response = self._inner.handle_request(request)
        except httpx.ProxyError as e:
            raise ProxyFailure(classify_proxy_error(e)) from None
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            # With a proxy every connection is to the proxy: it wasn't reachable.
            raise ProxyFailure(ProxyError("connect", None, redact(str(e))[:200])) from None
        head = sum(len(k) + len(v) + 4 for k, v in response.headers.raw) + 16
        self.count.add(head)
        if request.url.scheme == "http" and response.status_code == 407:
            response.close()
            raise ProxyFailure(ProxyError("refused", 407, "proxy authentication required"))
        response.stream = _CountingStream(response.stream, self.count)
        return response

    def close(self) -> None:
        self._inner.close()


def classify_proxy_error(e: Exception) -> ProxyError:
    """A failed ``CONNECT``. A 4xx is a refusal (an account matter); 502/503/504 is
    ``gateway``: the proxy couldn't reach the target, which says as much about the target
    as about the exit; anything else is a connect failure."""
    text = redact(str(e))[:200]
    m = _STATUS.search(text)
    status = int(m.group(1)) if m else None
    if status is not None and status < 500:
        kind = "refused"
    elif status in GATEWAY_STATUSES:
        kind = "gateway"
    else:
        kind = "connect"
    return ProxyError(kind, status, text)


def proxied_client(route: Route, count: ByteCount | None = None, **kw: Any) -> httpx.Client:
    """An httpx client whose every request goes through ``route``'s proxy. ``kw`` goes to
    ``httpx.Client`` (timeout, headers, follow_redirects, ...); ``proxy``, ``mounts``,
    ``transport`` and ``trust_env`` are fixed here."""
    for k in ("proxy", "proxies", "mounts", "transport", "trust_env"):
        if k in kw:
            raise ValueError(f"{k} is set by the route")
    transport_kw = {k: kw.pop(k) for k in ("verify", "cert", "http2", "retries") if k in kw}
    return httpx.Client(
        transport=_RouteTransport(route, count or ByteCount(), **transport_kw),
        trust_env=False,
        **kw,
    )


def playwright_proxy(route: Route) -> dict[str, str]:
    """``proxy=`` for Playwright (browser launch or context). Holds credentials."""
    p = route.proxy
    scheme = "socks5" if p.scheme.startswith("socks5") else p.scheme
    return {
        "server": f"{scheme}://{p.host}:{p.port}",
        "username": p.username,
        "password": p.password,
    }


def exit_ip(route: Route, response: httpx.Response | None = None) -> str | None:
    """The exit address of a request through ``route``: the fixed exit's, or the one the
    vendor reports in ``exit_ip_header``; None when unknown."""
    if route.exit_ip:
        return route.exit_ip
    if response is not None and route.exit_ip_header:
        return response.headers.get(route.exit_ip_header)
    return None
