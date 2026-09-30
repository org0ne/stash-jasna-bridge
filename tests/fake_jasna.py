#!/usr/bin/env python3
"""Stand-in for `jasna --stream`: the five endpoints the bridge uses, with a
configurable /open delay. Runnable as a process (accepts and ignores any
Jasna flags except --stream-port) so ProcessManager can be tested too.

    python3 tests/fake_jasna.py --stream --no-browser --stream-port 8765 [--open-delay 0.5]
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEGMENT_SECONDS = 4.0

# Segments are real (minimal) MPEG-TS so the bridge's span parser sees them:
# one video PES per frame, PTS only. The timeline copies what Jasna 0.10.0
# does (measured 2026-09-30): the first segment of a pass starts PASS_LEAD_S
# early and runs to where the *next* segment's nominal start + MUX_DELAY_S is,
# every later segment starts at 4N + MUX_DELAY_S + DRIFT_S per segment since
# the pass began. A request for anything but the next sequential segment
# starts a new pass there, like Jasna's "seek requested, cancelling pass".
FRAMES_PER_SEGMENT = 10
MUX_DELAY_S = 1.4
PASS_LEAD_S = 2.0
DRIFT_S = 0.004
SEG_BYTES = 188 * FRAMES_PER_SEGMENT


def _ts_packet(pid: int, payload: bytes, pusi: bool, cc: int) -> bytes:
    pad = 184 - len(payload)
    hdr = bytes([0x47, (0x40 if pusi else 0) | (pid >> 8), pid & 0xFF])
    if pad:
        af = bytes([pad - 1]) + (b"\x00" + b"\xff" * (pad - 2) if pad >= 2 else b"")
        return hdr + bytes([0x30 | (cc & 0xF)]) + af + payload
    return hdr + bytes([0x10 | (cc & 0xF)]) + payload


def _pes_video(pts90k: int, body: bytes) -> bytes:
    p = pts90k
    pts = bytes([0x21 | ((p >> 29) & 0x0E), (p >> 22) & 0xFF, 0x01 | ((p >> 14) & 0xFE), (p >> 7) & 0xFF,
                 0x01 | ((p << 1) & 0xFE)])
    return b"\x00\x00\x01\xe0\x00\x00\x80\x80\x05" + pts + body


def ts_segment(start_s: float, end_s: float, label: bytes = b"") -> bytes:
    """A segment whose video PTS run from start_s to (just under) end_s."""
    frame = (end_s - start_s) / FRAMES_PER_SEGMENT
    out = []
    for i in range(FRAMES_PER_SEGMENT):
        pts = int(round((start_s + i * frame) * 90000))
        out.append(_ts_packet(0x100, _pes_video(pts, label[:150]), True, i))
    return b"".join(out)


def jasna_like_span(index: int, pass_start: int) -> tuple[float, float]:
    """Where Jasna would put segment `index` of a pass that began at `pass_start`."""
    nxt = (index + 1) * SEGMENT_SECONDS + MUX_DELAY_S
    if index == pass_start:
        return max(0.0, index * SEGMENT_SECONDS - PASS_LEAD_S), nxt
    drift = DRIFT_S * (index - pass_start)
    return index * SEGMENT_SECONDS + MUX_DELAY_S + drift, nxt + drift


class FakeJasna:
    def __init__(self, open_delay: float = 0.0, duration: float = 40.0, hang_file: str | None = None,
                 stall_file: str | None = None):
        self.open_delay = open_delay
        # If hang_file exists and contains this process's pid, every request
        # blocks forever - simulates a wedged Jasna whose server stops answering.
        self.hang_file = hang_file
        # If stall_file exists and contains this pid, /status and the playlist
        # still answer but segment requests fail fast - simulates a wedged
        # render PASS behind a healthy HTTP server (the 2026-09-09 pipeline stall).
        self.stall_file = stall_file
        self.duration = duration
        self.path: str | None = None
        self.opens: list[str] = []
        self.stops = 0
        self.segments: list[str] = []
        self.passes: list[int] = []     # segment index each render pass started at
        self.last_seg: int | None = None
        self.lock = threading.Lock()

    def render(self, name: str) -> bytes:
        """Serve seg_NNNNN.ts the way Jasna's pass would, starting a new pass
        when the request is not the next sequential segment."""
        index = int(name[4:9])
        with self.lock:
            if self.last_seg is None or index != self.last_seg + 1:
                self.passes.append(index)
            self.last_seg = index
            self.segments.append(name)
            start, end = jasna_like_span(index, self.passes[-1])
        return ts_segment(start, end, name.encode() + b":")

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

        def _maybe_hang(self):
            import os
            hf = state.hang_file
            if hf and os.path.exists(hf):
                try:
                    if open(hf).read().strip() == str(os.getpid()):
                        time.sleep(3600)
                except OSError:
                    pass

        def reply(self, status, body: bytes, ctype="application/json"):
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self._maybe_hang()
            if self.path == "/status":
                return self.reply(200, json.dumps({"streaming": state.path is not None, "path": state.path}).encode())
            if self.path.startswith("/stream.m3u8"):
                if state.path is None:
                    return self.reply(404, b"no stream", "text/plain")
                return self.reply(200, state.playlist().encode(), "application/vnd.apple.mpegurl")
            if self.path.startswith("/seg_") and self.path.endswith(".ts"):
                if state.path is None:
                    return self.reply(404, b"no stream", "text/plain")
                sf = state.stall_file
                if sf and os.path.exists(sf):
                    try:
                        if open(sf).read().strip() == str(os.getpid()):
                            return self.reply(503, b"segment not ready", "text/plain")
                    except OSError:
                        pass
                return self.reply(200, state.render(self.path[1:]), "video/mp2t")
            self.reply(404, b"not found", "text/plain")

        def do_POST(self):
            self._maybe_hang()
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
                    state.last_seg = None  # a new file: the next request starts a pass
                return self.reply(200, b'{"ok":true}')
            if self.path == "/stop":
                with state.lock:
                    state.path = None
                    state.stops += 1
                return self.reply(200, b'{"ok":true}')
            self.reply(404, b"not found", "text/plain")

    return H


def start(port: int = 0, open_delay: float = 0.0, hang_file: str | None = None,
          stall_file: str | None = None) -> tuple[ThreadingHTTPServer, FakeJasna]:
    state = FakeJasna(open_delay, hang_file=hang_file, stall_file=stall_file)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(state))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state


if __name__ == "__main__":
    port, delay, hang, stall = 8765, 0.0, None, None
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--stream-port":
            port = int(args[i + 1])
        if a == "--open-delay":
            delay = float(args[i + 1])
        if a == "--hang-file":
            hang = args[i + 1]
        if a == "--stall-file":
            stall = args[i + 1]
    server, _ = start(port, delay, hang, stall)
    print(f"fake jasna on {port}", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
