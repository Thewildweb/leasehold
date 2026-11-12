"""A fake Certificate Transparency log server (RFC 6962), for tests and the dev stack.

    leasehold mock ctlog --port 8500

Serves several logs, each under ``/logs/<name>/``:

- ``ct/v1/get-sth``: the tree size grows while the server runs (``growth`` entries
  per second per log), so a client has to keep up;
- ``ct/v1/get-entries?start=&end=``: real RFC 6962 entries: ``x509_entry`` and
  ``precert_entry`` leaves with DER certificates signed by a mock CA. Responses are
  capped at ``max_entries`` like real logs, so a client must loop on short pages;
  ``start`` at or past the tree size is a 400, as on real logs.

``/log_list.json`` lists the logs in Chrome's log list v3 format, with URLs
pointing back at this server.

Every entry is generated from (log name, index), so tests know in advance which
domains an index holds: ``domains_for(log, index)``, or ``GET /_mock/domains``.
Domains are ``<hash>.test`` names; some entries add a ``www.`` name, a wildcard, an
upper-case duplicate or an IP address in the SAN, to exercise the parser.

Rate limit: per client and log operator, a token bucket of ``rps`` requests per
second; over it, ``429`` with ``Retry-After: 1``. The client is the source IP, or
the ``X-Mock-Client`` header so one test can act as several egress IPs.
``GET /_mock/stats`` reports requests, 429s and the highest number of requests per
client and operator in any one second, so a test can assert that a crawler stays
under the limit.

Test controls (JSON bodies):

- ``POST /_mock/grow {"log": "...", "n": 100}``: add entries now;
- ``POST /_mock/fail {"log": "...", "status": 503, "count": 2}``: the log's next
  ``count`` requests fail with ``status``;
- ``POST /_mock/reset``: back to the start sizes, stats cleared.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import ipaddress
import json
import struct
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

# Entries are timestamped from here, one millisecond apart.
EPOCH_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
X509_ENTRY, PRECERT_ENTRY = 0, 1


@dataclass
class Log:
    name: str
    operator: str
    initial_size: int
    growth: float = 0.0  # entries per second
    _extra: int = 0
    _started: float = field(default_factory=time.monotonic)

    def size(self) -> int:
        grown = int((time.monotonic() - self._started) * self.growth)
        return self.initial_size + grown + self._extra

    def reset(self) -> None:
        self._extra = 0
        self._started = time.monotonic()


DEFAULT_LOGS = (
    Log("mock_argon_2026h2", "Google", 20_000, 5.0),
    Log("mock_xenon_2026h2", "Google", 12_000, 2.0),
    Log("mock_elephant_2026h2", "Sectigo", 8_000, 1.0),
)


# --- entries ----------------------------------------------------------------------------------


def _hash(log: str, index: int) -> str:
    return hashlib.sha256(f"{log}:{index}".encode()).hexdigest()[:12]


def domains_for(log: str, index: int) -> list[str]:
    """The DNS names in entry ``index`` of ``log``, as they appear in the certificate."""
    base = f"{_hash(log, index)}.test"
    names = [base]
    if index % 2 == 0:
        names.append(f"www.{base}")
    if index % 5 == 0:
        names.append(f"*.{base}")
    if index % 11 == 0:
        names.append(base.upper())  # same name again, other case
    return names


def entry_type_for(index: int) -> int:
    return PRECERT_ENTRY if index % 3 == 1 else X509_ENTRY


@lru_cache(maxsize=1)
def _ca() -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.derive_private_key(0x1EA5E401D, ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "leasehold mock CA")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(dt.datetime(2026, 1, 1, tzinfo=dt.UTC))
        .not_valid_after(dt.datetime(2036, 1, 1, tzinfo=dt.UTC))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _leaf_cert(log: str, index: int, precert: bool) -> x509.Certificate:
    ca_key, ca_cert = _ca()
    names = domains_for(log, index)
    san: list[x509.GeneralName] = [x509.DNSName(n) for n in names]
    if index % 7 == 0:
        san.append(x509.IPAddress(ipaddress.ip_address(f"192.0.2.{index % 250 + 1}")))
    not_before = dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(minutes=index % 100_000)
    b = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[0])]))
        .issuer_name(ca_cert.subject)
        .public_key(ca_key.public_key())  # one key for all leaves: generation stays fast
        .serial_number(int(_hash(log, index), 16) or 1)
        .not_valid_before(not_before)
        .not_valid_after(not_before + dt.timedelta(days=90))
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
    )
    if precert:
        b = b.add_extension(x509.PrecertPoison(), critical=True)
    return b.sign(ca_key, hashes.SHA256())


def _u24(data: bytes) -> bytes:
    return len(data).to_bytes(3, "big") + data


@lru_cache(maxsize=50_000)
def entry(log: str, index: int) -> dict[str, str]:
    """One get-entries item: ``leaf_input`` and ``extra_data``, base64."""
    _, ca_cert = _ca()
    ca_der = ca_cert.public_bytes(serialization.Encoding.DER)
    precert = entry_type_for(index) == PRECERT_ENTRY
    cert = _leaf_cert(log, index, precert)
    der = cert.public_bytes(serialization.Encoding.DER)
    head = struct.pack(">BBQH", 0, 0, EPOCH_MS + index, entry_type_for(index))
    if precert:
        issuer_key_hash = hashlib.sha256(
            ca_cert.public_key().public_bytes(
                serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
            )
        ).digest()
        signed = head + issuer_key_hash + _u24(cert.tbs_certificate_bytes)
        extra = _u24(der) + _u24(_u24(ca_der))
    else:
        signed = head + _u24(der)
        extra = _u24(_u24(ca_der))
    leaf = signed + b"\x00\x00"  # no CT extensions
    return {
        "leaf_input": base64.b64encode(leaf).decode(),
        "extra_data": base64.b64encode(extra).decode(),
    }


# --- server -----------------------------------------------------------------------------------


@dataclass
class _Bucket:
    tokens: float
    at: float


class MockCTLog:
    def __init__(self, logs=DEFAULT_LOGS, *, rps: float = 5.0, max_entries: int = 1000):
        self.logs = {lg.name: Log(lg.name, lg.operator, lg.initial_size, lg.growth) for lg in logs}
        self.rps = rps
        self.max_entries = max_entries
        self._lock = threading.Lock()
        self._buckets: dict[tuple[str, str], _Bucket] = {}
        self._fail: dict[str, tuple[int, int]] = {}
        self.reset_stats()

    def reset_stats(self) -> None:
        with self._lock:
            self.requests: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
            self.throttled: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
            self._per_second: dict[tuple[str, str, int], int] = defaultdict(int)
            self._buckets.clear()
            self._fail.clear()

    def reset(self) -> None:
        for lg in self.logs.values():
            lg.reset()
        self.reset_stats()

    def stats(self) -> dict:
        with self._lock:
            peak: dict[str, dict[str, int]] = defaultdict(dict)
            for (client, op, _), n in self._per_second.items():
                peak[client][op] = max(peak[client].get(op, 0), n)
            return {
                "requests": {c: dict(v) for c, v in self.requests.items()},
                "throttled": {c: dict(v) for c, v in self.throttled.items()},
                "peak_per_second": dict(peak),
                "sizes": {name: lg.size() for name, lg in self.logs.items()},
            }

    def admit(self, client: str, log: Log) -> int | None:
        """None when the request may go ahead, else the status to answer with."""
        now = time.monotonic()
        with self._lock:
            self.requests[client][log.name] += 1
            failing = self._fail.get(log.name)
            if failing:
                status, left = failing
                if left > 1:
                    self._fail[log.name] = (status, left - 1)
                else:
                    del self._fail[log.name]
                return status
            if self.rps > 0:
                key = (client, log.operator)
                b = self._buckets.get(key) or _Bucket(self.rps, now)
                b.tokens = min(self.rps, b.tokens + (now - b.at) * self.rps)
                b.at = now
                self._buckets[key] = b
                if b.tokens < 1:
                    self.throttled[client][log.name] += 1
                    return 429
                b.tokens -= 1
            self._per_second[(client, log.operator, int(time.time()))] += 1
        return None

    def fail(self, log: str, status: int, count: int) -> None:
        with self._lock:
            self._fail[log] = (status, count)

    def log_list(self, base_url: str) -> dict:
        operators: dict[str, list] = defaultdict(list)
        for lg in self.logs.values():
            log_id = base64.b64encode(hashlib.sha256(lg.name.encode()).digest()).decode()
            operators[lg.operator].append(
                {
                    "description": f"leasehold mock log {lg.name}",
                    "log_id": log_id,
                    "key": "",
                    "url": f"{base_url}/logs/{lg.name}/",
                    "mmd": 86400,
                    "state": {"usable": {"timestamp": "2026-01-01T00:00:00Z"}},
                    "temporal_interval": {
                        "start_inclusive": "2026-07-01T00:00:00Z",
                        "end_exclusive": "2027-01-01T00:00:00Z",
                    },
                }
            )
        return {
            "version": "mock",
            "log_list_timestamp": "2026-01-01T00:00:00Z",
            "operators": [
                {"name": op, "email": [], "logs": logs, "tiled_logs": []}
                for op, logs in sorted(operators.items())
            ],
        }

    def sth(self, log: Log) -> dict:
        size = log.size()
        root = hashlib.sha256(f"{log.name}:{size}".encode()).digest()
        return {
            "tree_size": size,
            "timestamp": int(time.time() * 1000),
            "sha256_root_hash": base64.b64encode(root).decode(),
            "tree_head_signature": base64.b64encode(b"\x04\x03\x00\x00").decode(),
        }

    def entries(self, log: Log, start: int, end: int) -> tuple[int, dict]:
        size = log.size()
        if start < 0 or end < start:
            return 400, {"error_message": "start and end must satisfy 0 <= start <= end"}
        if start >= size:
            return 400, {"error_message": f"start {start} >= tree size {size}"}
        last = min(end, size - 1, start + self.max_entries - 1)
        return 200, {"entries": [entry(log.name, i) for i in range(start, last + 1)]}

    def handler(self) -> type[BaseHTTPRequestHandler]:
        mock = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args) -> None:  # quiet
                pass

            def _send(self, status: int, body: dict | None = None, headers=None) -> None:
                data = json.dumps(body or {}).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                url = urlparse(self.path)
                q = {k: v[0] for k, v in parse_qs(url.query).items()}
                parts = url.path.strip("/").split("/")
                if url.path == "/log_list.json":
                    host = self.headers.get("Host", "localhost")
                    return self._send(200, mock.log_list(f"http://{host}"))
                if url.path == "/_mock/stats":
                    return self._send(200, mock.stats())
                if url.path == "/_mock/domains":
                    start, end = int(q["start"]), int(q["end"])
                    return self._send(
                        200, {str(i): domains_for(q["log"], i) for i in range(start, end + 1)}
                    )
                if len(parts) == 5 and parts[0] == "logs" and parts[2:4] == ["ct", "v1"]:
                    log = mock.logs.get(parts[1])
                    if log is None:
                        return self._send(404, {"error_message": "no such log"})
                    client = self.headers.get("X-Mock-Client") or self.client_address[0]
                    status = mock.admit(client, log)
                    if status == 429:
                        return self._send(429, {"error_message": "rate limited"},
                                          {"Retry-After": "1"})  # fmt: skip
                    if status:
                        return self._send(status, {"error_message": "injected failure"})
                    if parts[4] == "get-sth":
                        return self._send(200, mock.sth(log))
                    if parts[4] == "get-entries":
                        try:
                            start, end = int(q["start"]), int(q["end"])
                        except (KeyError, ValueError):
                            return self._send(400, {"error_message": "need start and end"})
                        return self._send(*mock.entries(log, start, end))
                return self._send(404, {"error_message": "not found"})

            def do_POST(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}")
                if self.path == "/_mock/reset":
                    mock.reset()
                    return self._send(200, {})
                log = mock.logs.get(body.get("log", ""))
                if log is None:
                    return self._send(404, {"error_message": "no such log"})
                if self.path == "/_mock/grow":
                    log._extra += int(body["n"])
                    return self._send(200, {"tree_size": log.size()})
                if self.path == "/_mock/fail":
                    mock.fail(log.name, int(body.get("status", 503)), int(body.get("count", 1)))
                    return self._send(200, {})
                return self._send(404, {"error_message": "not found"})

        return Handler

    def serve(self, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
        """Start in a background thread; ``server.server_address`` has the port."""
        server = ThreadingHTTPServer((host, port), self.handler())
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server


def parse_log(spec: str) -> Log:
    """``NAME:OPERATOR:SIZE[:GROWTH]``."""
    parts = spec.split(":")
    if len(parts) not in (3, 4):
        raise argparse.ArgumentTypeError("expected NAME:OPERATOR:SIZE[:GROWTH]")
    return Log(parts[0], parts[1], int(parts[2]), float(parts[3]) if len(parts) == 4 else 0.0)


def main(args: argparse.Namespace) -> int:
    mock = MockCTLog(args.log or DEFAULT_LOGS, rps=args.rps, max_entries=args.max_entries)
    server = ThreadingHTTPServer((args.host, args.port), mock.handler())
    print(f"mock CT log on http://{args.host}:{args.port} with {', '.join(mock.logs)}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0
