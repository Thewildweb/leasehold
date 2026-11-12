"""Normalised domains and URLs for task keys and shared columns.

Task keys are compared as strings, so ``http://Example.nl`` and ``https://example.nl/``
would be two rows and the work would be done twice. Every service that creates tasks
keyed by a domain or URL, or writes a ``domain`` / ``url`` column, uses these
functions. Rules are in ODC-infra-v3 ``docs/foundations.md`` section 5.

``VERSION`` changes whenever a rule changes, since that changes keys.
"""

from __future__ import annotations

import ipaddress
import re
from functools import lru_cache
from urllib.parse import urlsplit

import idna
from publicsuffixlist import PublicSuffixList

VERSION = 1

# Query parameters that only track the visitor, matched case-insensitively, plus
# every parameter starting with one of _TRACKING_PREFIXES.
TRACKING_PARAMS = frozenset(
    {
        "gclid",
        "gclsrc",
        "gbraid",
        "wbraid",
        "dclid",
        "fbclid",
        "msclkid",
        "yclid",
        "twclid",
        "ttclid",
        "igshid",
        "li_fat_id",
        "mc_cid",
        "mc_eid",
        "mkt_tok",
        "_ga",
        "_gl",
        "_hsenc",
        "_hsmi",
        "__hssc",
        "__hstc",
        "__hsfp",
        "hsctatracking",
        "oly_anon_id",
        "oly_enc_id",
        "vero_id",
    }
)
_TRACKING_PREFIXES = ("utm_",)

# Underscores are invalid in IDNA but common in DNS names (_dmarc, _domainkey).
_LABEL = re.compile(r"^(?!-)[a-z0-9_-]{1,63}(?<!-)$")
_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
_SUB_DELIMS = "!$&'()*+,;="
_PATH_SAFE = frozenset(_UNRESERVED | set(_SUB_DELIMS) | set(":@/"))
# Inside a query key or value; '&' never appears here because the query is split on it.
_QUERY_SAFE = frozenset(_UNRESERVED | set(_SUB_DELIMS) | set(":@/?"))
_DEFAULT_PORTS = {"http": 80, "https": 443}


class InvalidKey(ValueError):
    """The value isn't a usable domain or URL."""


@lru_cache(maxsize=1)
def _psl() -> PublicSuffixList:
    # The list ships inside the package (no network). Unknown TLDs are rejected.
    # The private section is included, so foo.github.io is its own domain.
    return PublicSuffixList(accept_unknown=False)


def normalize_host(value: str) -> str:
    """A hostname in lowercase ASCII (IDN as punycode), without a trailing dot.

    Accepts a bare host or one with a leading ``*.`` (certificate wildcards). IP
    addresses are rejected; ``normalize_url`` handles those itself.
    """
    host = value.strip().rstrip(".")
    if host.startswith("*."):
        host = host[2:]
    if not host:
        raise InvalidKey(f"empty host: {value!r}")
    if _is_ip(host):
        raise InvalidKey(f"IP address, not a hostname: {value!r}")
    try:
        host = idna.encode(host, uts46=True).decode("ascii")
    except idna.IDNAError as e:
        if not host.isascii():
            raise InvalidKey(f"invalid hostname {value!r}: {e}") from None
        host = host.lower()
    if len(host) > 253:
        raise InvalidKey(f"hostname longer than 253 characters: {value[:60]!r}…")
    labels = host.split(".")
    if len(labels) < 2 or not all(_LABEL.match(lbl) for lbl in labels):
        raise InvalidKey(f"invalid hostname: {value!r}")
    return host


def normalize_domain(value: str) -> str:
    """The registrable domain of a host, URL or e-mail address.

    ``www.Example.co.uk/contact`` → ``example.co.uk``, ``info@Bakker.nl`` → ``bakker.nl``,
    ``https://shop.bakker.nl:8443/`` → ``bakker.nl``. Raises ``InvalidKey`` for values
    without a registrable domain (``localhost``, a bare public suffix, unknown TLDs,
    IP addresses).
    """
    raw = value.strip()
    if not raw:
        raise InvalidKey("empty value")
    if "://" in raw:
        host = urlsplit(raw).hostname or ""
    else:
        if "@" in raw and "/" not in raw.split("@", 1)[0]:
            raw = raw.rsplit("@", 1)[1]  # e-mail address
        host = re.split(r"[/?#]", raw, maxsplit=1)[0]
        if host.count(":") == 1:
            host = host.split(":", 1)[0]  # port
    host = normalize_host(host)
    domain = _psl().privatesuffix(host)
    if domain is None:
        raise InvalidKey(f"no registrable domain in {value!r}")
    return domain


def normalize_url(value: str, default_scheme: str | None = None) -> str:
    """An http(s) URL in one canonical spelling.

    Lowercases the scheme and host (IDN as punycode), drops default ports, user info
    and the fragment, resolves ``.``/``..`` segments, writes percent-encoding one way,
    sorts query parameters and drops tracking ones. Keeps ``http`` vs ``https``,
    ``www.`` and trailing slashes, since those can serve different content.

    ``default_scheme`` (``"https"`` or ``"http"``) is used when the value has none,
    e.g. ``www.example.nl/a``; without it such values are rejected.
    """
    raw = value.strip()
    if "://" not in raw:
        if default_scheme is None:
            raise InvalidKey(f"no scheme in {value!r}")
        raw = f"{default_scheme}://{raw}"
    try:
        parts = urlsplit(raw)
        port = parts.port
    except ValueError as e:
        raise InvalidKey(f"invalid URL {value!r}: {e}") from None
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS:
        raise InvalidKey(f"not an http(s) URL: {value!r}")
    hostname = parts.hostname
    if not hostname:
        raise InvalidKey(f"no host in {value!r}")
    if _is_ip(hostname):
        ip = ipaddress.ip_address(hostname)
        host = f"[{ip.compressed}]" if ip.version == 6 else ip.compressed
    else:
        host = normalize_host(hostname)
    netloc = host if port in (None, _DEFAULT_PORTS[scheme]) else f"{host}:{port}"
    path = _remove_dot_segments(_percent(parts.path, _PATH_SAFE)) or "/"
    query = _query(parts.query)
    return f"{scheme}://{netloc}{path}" + (f"?{query}" if query else "")


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return False
    return True


def _percent(text: str, safe: frozenset[str]) -> str:
    """Decode escapes of unreserved characters, upper-case the others, and escape
    characters that aren't allowed raw (as UTF-8)."""
    out: list[str] = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "%" and re.fullmatch(r"[0-9A-Fa-f]{2}", text[i + 1 : i + 3]):
            byte = int(text[i + 1 : i + 3], 16)
            decoded = chr(byte)
            out.append(decoded if decoded in _UNRESERVED else f"%{byte:02X}")
            i += 3
            continue
        if ch in safe:
            out.append(ch)
        else:
            out.extend(f"%{b:02X}" for b in ch.encode("utf-8"))
        i += 1
    return "".join(out)


def _remove_dot_segments(path: str) -> str:
    """RFC 3986 section 5.2.4."""
    if not path:
        return path
    out: list[str] = []
    segments = path.split("/")
    for i, seg in enumerate(segments):
        last = i == len(segments) - 1
        if seg == ".":
            if last:
                out.append("")
        elif seg == "..":
            if len(out) > 1:
                out.pop()
            if last:
                out.append("")
        else:
            out.append(seg)
    result = "/".join(out)
    return result if result.startswith("/") else "/" + result


def _is_tracking(key: str) -> bool:
    k = key.lower()
    return k in TRACKING_PARAMS or k.startswith(_TRACKING_PREFIXES)


def _query(query: str) -> str:
    pairs: list[tuple[str, str | None]] = []
    for part in query.split("&"):
        if not part:
            continue
        key, sep, val = part.partition("=")
        key = _percent(key, _QUERY_SAFE - {"="})
        if _is_tracking(key):
            continue
        pairs.append((key, _percent(val, _QUERY_SAFE) if sep else None))
    pairs.sort(key=lambda kv: (kv[0], kv[1] is not None, kv[1] or ""))
    return "&".join(k if v is None else f"{k}={v}" for k, v in pairs)
