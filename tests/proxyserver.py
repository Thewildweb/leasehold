"""A small HTTP proxy and targets for tests: CONNECT tunnels and forwarded requests,
Basic auth, and the refusals vendors send (407, 402, a quota body), plus an IP echo.

Names are resolved by the proxy from its own table (``hosts``), never by DNS, so a test
can check that the client never resolves the target's name itself.
"""

from __future__ import annotations

import base64
import datetime as dt
import http.server
import socket
import socketserver
import ssl
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path


class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class LocalProxy:
    """``mode``: ok, 402, quota (403 with a quota body), 502 (the target unreachable from
    the proxy), down (closes every connection).
    ``exit_ip``: what the echo host answers. ``users`` collects the usernames seen."""

    ECHO_HOST = "echo.test"

    def __init__(self, username: str = "user", password: str = "secret-pass"):
        self.username = username
        self.password = password
        self.mode = "ok"
        self.exit_ip = "203.0.113.10"
        self.hosts: dict[str, tuple[str, int]] = {}
        self.users: list[str] = []
        self.requests: list[str] = []
        self.accept_prefix = False  # accept usernames that start with self.username
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _auth_ok(self) -> bool:
                h = self.headers.get("Proxy-Authorization", "")
                if not h.startswith("Basic "):
                    return False
                user, _, pw = base64.b64decode(h[6:]).decode().partition(":")
                proxy.users.append(user)
                ok_user = user == proxy.username or (
                    proxy.accept_prefix and user.startswith(proxy.username)
                )
                return ok_user and pw == proxy.password

            def _refuse(self) -> bool:
                if proxy.mode == "down":
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return True
                if not self._auth_ok():
                    self._send(
                        407,
                        b"proxy authentication required",
                        {"Proxy-Authenticate": 'Basic realm="test"'},
                    )
                    return True
                if proxy.mode == "402":
                    self._send(402, b"payment required")
                    return True
                if proxy.mode == "quota":
                    self._send(403, b"traffic limit reached")
                    return True
                if proxy.mode == "502":
                    self._send(502, b"bad gateway")
                    return True
                return False

            def _send(self, status: int, body: bytes, headers: dict | None = None) -> None:
                self.send_response(status)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_CONNECT(self):  # noqa: N802
                proxy.requests.append(f"CONNECT {self.path}")
                if self._refuse():
                    return
                host, _, port = self.path.rpartition(":")
                if host not in proxy.hosts:
                    self._send(502, b"unknown host")
                    return
                up = socket.create_connection(proxy.hosts[host], timeout=5)
                self.send_response(200, "Connection established")
                self.end_headers()
                self.close_connection = True
                _pipe(self.connection, up)

            def do_GET(self):  # noqa: N802
                proxy.requests.append(f"GET {self.path}")
                if self._refuse():
                    return
                if self.path.startswith(f"http://{proxy.ECHO_HOST}"):
                    self._send(200, proxy.exit_ip.encode(), {"Content-Type": "text/plain"})
                    return
                self._send(200, b"forwarded")

        self.server = _Server(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def _pipe(a: socket.socket, b: socket.socket) -> None:
    def copy(src, dst):
        try:
            while data := src.recv(65536):
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    t = threading.Thread(target=copy, args=(b, a), daemon=True)
    t.start()
    copy(a, b)
    t.join(5)


def _self_signed(host: str, directory: Path) -> tuple[Path, Path]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)])
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(host)]), critical=False)
        .sign(key, hashes.SHA256())
    )
    c, k = directory / "cert.pem", directory / "key.pem"
    c.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    k.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return c, k


@contextmanager
def tls_target(host: str = "target.test", body: bytes = b"hello from the target"):
    """An HTTPS site for ``host`` on 127.0.0.1 (self-signed). Yields its port."""

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with tempfile.TemporaryDirectory() as d:
        cert, key = _self_signed(host, Path(d))
        server = _Server(("127.0.0.1", 0), Handler)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            yield server.server_address[1]
        finally:
            server.shutdown()
            server.server_close()
