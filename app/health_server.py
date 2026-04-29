"""Tiny HTTP health endpoint so the deployment system can verify liveness.

Binds to PORT env (or 8090) and serves /healthz returning JSON. Runs in a
daemon thread; the main bot continues unaffected. Requires zero deps.
"""

from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .utils import get_logger

log = get_logger("health")

_BOOT_TS = time.time()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 — http.server API
        if self.path in ("/healthz", "/", "/__market-bot", "/__market-bot/healthz"):
            payload = json.dumps({
                "ok": True,
                "service": "market-news-bot",
                "uptime_seconds": int(time.time() - _BOOT_TS),
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_a, **_kw):  # quiet access logs
        pass


def start_health_server(default_port: int = 8090) -> threading.Thread:
    port = int(os.environ.get("PORT") or default_port)

    def _serve() -> None:
        try:
            srv = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
            log.info("Health server listening on 0.0.0.0:%d", port)
            srv.serve_forever()
        except Exception as exc:
            log.exception("Health server failed: %s", exc)

    t = threading.Thread(target=_serve, daemon=True, name="health-server")
    t.start()
    return t
