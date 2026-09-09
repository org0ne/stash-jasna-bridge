"""Minimal Stash GraphQL stand-in: findScene for known ids, optional cookie check."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCENES = {
    "1": "/media/a/one.mp4",
    "2": "/media/b/two.mp4",
}


def start(require_cookie: str | None = None) -> tuple[ThreadingHTTPServer, list]:
    seen: list[dict] = []

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def reply(self, status, obj):
            body = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            req = json.loads(self.rfile.read(length))
            seen.append({"cookie": self.headers.get("Cookie"), "query": req["query"]})
            if require_cookie and self.headers.get("Cookie") != require_cookie:
                return self.reply(401, {"errors": [{"message": "unauthorized"}]})
            q = req["query"]
            if "findScene" in q:
                sid = str(req["variables"]["id"])
                if sid not in SCENES:
                    return self.reply(200, {"data": {"findScene": None}})
                return self.reply(200, {"data": {"findScene": {"id": sid, "files": [{"path": SCENES[sid], "duration": 40.0}]}}})
            return self.reply(200, {"data": {"configuration": {"general": {"databasePath": "/x"}}}})

    server = ThreadingHTTPServer(("127.0.0.1", 0), H)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, seen
