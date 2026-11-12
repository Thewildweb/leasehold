"""``/health`` and ``/metrics`` for long-running components.

``/health`` is liveness, not dependency health: it answers 200 while the main
loop has made progress within ``stall_after`` seconds, and 503 otherwise, so
Nomad's ``check_restart`` restarts a stuck process. Kafka or the control API
being down is not a reason to restart; the loop keeps running (and retrying)
then, and stays healthy.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest


class Liveness:
    def __init__(self, stall_after: float = 120.0):
        self.stall_after = stall_after
        self.last = time.monotonic()

    def beat(self) -> None:
        self.last = time.monotonic()

    def age(self) -> float:
        return time.monotonic() - self.last

    def ok(self) -> bool:
        return self.age() < self.stall_after


def serve(port: int, liveness: Liveness, host: str = "0.0.0.0") -> ThreadingHTTPServer:
    """Start the server in a daemon thread. ``port`` 0 picks a free port."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path == "/health":
                ok = liveness.ok()
                body = json.dumps(
                    {
                        "status": "ok" if ok else "stalled",
                        "last_progress_s": round(liveness.age(), 1),
                    }
                ).encode()
                self._send(200 if ok else 503, "application/json", body)
            elif self.path == "/metrics":
                self._send(200, CONTENT_TYPE_LATEST, generate_latest(REGISTRY))
            else:
                self._send(404, "text/plain", b"not found\n")

        def _send(self, status: int, ctype: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # no access log on stdout
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, name="leasehold-http", daemon=True).start()
    return server
