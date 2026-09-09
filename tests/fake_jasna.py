#!/usr/bin/env python3
"""Stand-in for `jasna --stream`: the five endpoints the bridge uses, with a
configurable /open delay. Runnable as a process (accepts and ignores any
Jasna flags except --stream-port) so ProcessManager can be tested too.

    python3 tests/fake_jasna.py --stream --no-browser --stream-port 8765 [--open-delay 0.5]
"""
from __future__ import annotations

import json
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEGMENT_SECONDS = 4.0


class FakeJasna:
    def __init__(self, open_delay: float = 0.0, duration: float = 40.0):
        self.open_delay = open_delay
        self.duration = duration
        self.path: str | None = None
        self.opens: list[str] = []
        self.stops = 0
        self.segments: list[str] = []
        self.lock = threading.Lock()

    def playlist(self) -> str:
        n = int(self.duration // SEGMENT_SECONDS) + 1
        lines = ["#EXTM3U", "#EXT-X-VERSION:3", f"#EXT-X-TARGETDURATION:{int(SEGMENT_SECONDS)}",
                 "#EXT-X-PLAYLIST-TYPE:VOD", "#EXT-X-MEDIA-SEQUENCE:0"]
        for i in range(n):
            lines += [f"#EXTINF:{SEGMENT_SECONDS:.6f},", f"seg_{i:05d}.ts"]
        lines.append("#EXT-X-ENDLIST")
        return "\n".join(lines) + "\n"


def make_handler(state: FakeJasna):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def reply(self, status, body: bytes, ctype="application/json"):
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/status":
                return self.reply(200, json.dumps({"streaming": state.path is not None, "path": state.path}).encode())
            if self.path.startswith("/stream.m3u8"):
                if state.path is None:
                    return self.reply(404, b"no stream", "text/plain")
                return self.reply(200, state.playlist().encode(), "application/vnd.apple.mpegurl")
            if self.path.startswith("/seg_") and self.path.endswith(".ts"):
                if state.path is None:
                    return self.reply(404, b"no stream", "text/plain")
                with state.lock:
                    state.segments.append(self.path[1:])
                body = (self.path[1:] + ":").encode() + b"\x47" * 188 * 8
                return self.reply(200, body, "video/mp2t")
            self.reply(404, b"not found", "text/plain")

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            if self.path == "/open":
                data = json.loads(raw or b"{}")
                path = data.get("path")
                if not path:
                    return self.reply(400, b'{"error":"path required"}')
                time.sleep(state.open_delay)
                with state.lock:
                    state.path = path
                    state.opens.append(path)
                return self.reply(200, b'{"ok":true}')
            if self.path == "/stop":
                with state.lock:
                    state.path = None
                    state.stops += 1
                return self.reply(200, b'{"ok":true}')
            self.reply(404, b"not found", "text/plain")

    return H


def start(port: int = 0, open_delay: float = 0.0) -> tuple[ThreadingHTTPServer, FakeJasna]:
    state = FakeJasna(open_delay)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(state))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state


if __name__ == "__main__":
    port, delay = 8765, 0.0
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--stream-port":
            port = int(args[i + 1])
        if a == "--open-delay":
            delay = float(args[i + 1])
    server, _ = start(port, delay)
    print(f"fake jasna on {port}", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
