"""HTTP front end: routing, auth, CORS and HLS proxying. Stdlib http.server."""
from __future__ import annotations

import json
import logging
import re
import secrets
import shutil
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import VERSION
from .jasna import JasnaError
from .sessions import Busy, NoSession
from .stash import CookieValidator, StashClient, StashError

log = logging.getLogger("bridge.http")

TOKEN_RE = r"(?P<token>[A-Za-z0-9_-]{16,64})"
ROUTES = [
    ("GET", re.compile(r"^/health$"), "health", False),
    ("GET", re.compile(r"^/presets$"), "presets", True),
    ("GET", re.compile(r"^/session$"), "session_get", True),
    ("POST", re.compile(r"^/session$"), "session_post", True),
    ("POST", re.compile(rf"^/session/{TOKEN_RE}/heartbeat$"), "heartbeat", False),
    ("POST", re.compile(rf"^/session/{TOKEN_RE}/end$"), "end", False),
    ("DELETE", re.compile(rf"^/session/{TOKEN_RE}$"), "end", False),
    ("GET", re.compile(rf"^/hls/{TOKEN_RE}/stream\.m3u8$"), "playlist", False),
    ("GET", re.compile(rf"^/hls/{TOKEN_RE}/(?P<seg>seg_[0-9]{{5}}\.ts)$"), "segment", False),
]


class Bridge:
    """Shared state handed to every request handler."""

    def __init__(self, cfg, sessions, stash: StashClient):
        self.cfg = cfg
        self.sessions = sessions
        self.stash = stash
        self.cookies = CookieValidator(stash, cfg.auth_cache_s) if cfg.auth_mode == "stash_cookie" else None
        self.started = time.time()

    def map_path(self, path: str) -> str:
        for stash_root, jasna_root in self.cfg.path_map:
            if path.startswith(stash_root):
                return jasna_root + path[len(stash_root):]
        return path


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = f"stash-jasna-bridge/{VERSION}"
    bridge: Bridge  # set on the server class

    # ----- plumbing -----
    def log_message(self, fmt, *args):  # route through logging, quieter
        log.debug("%s " + fmt, self.client_ip(), *args)

    def client_ip(self) -> str:
        fwd = self.headers.get("X-Forwarded-For")
        return fwd.split(",")[0].strip() if fwd else self.client_address[0]

    def route_path(self) -> str:
        path = urllib.parse.urlsplit(self.path).path
        prefix = self.bridge.cfg.path_prefix
        if prefix and (path == prefix or path.startswith(prefix + "/")):
            path = path[len(prefix):] or "/"
        return path

    def cors_headers(self) -> dict:
        origin = self.headers.get("Origin")
        allowed = self.bridge.cfg.cors_origins
        if not origin or not allowed:
            return {}
        if "*" in allowed or origin in allowed:
            return {
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Bridge-Token",
                "Access-Control-Max-Age": "600",
                "Vary": "Origin",
            }
        return {}

    def send(self, status: int, body: bytes = b"", content_type: str = "application/json", extra: dict | None = None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in {**self.cors_headers(), **(extra or {})}.items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def send_json(self, status: int, payload: dict):
        self.send(status, json.dumps(payload).encode())

    def error(self, status: int, message: str, **extra):
        self.send_json(status, {"error": message, **extra})

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > 65536:
            raise ValueError("body too large")
        raw = self.rfile.read(length)
        if not raw.strip():
            return {}
        data = json.loads(raw.decode())
        if not isinstance(data, dict):
            raise ValueError("body must be a JSON object")
        return data

    def authorized(self) -> bool:
        cfg = self.bridge.cfg
        if cfg.auth_mode == "none":
            return True
        if cfg.auth_mode == "token":
            given = self.headers.get("X-Bridge-Token") or ""
            auth = self.headers.get("Authorization") or ""
            if auth.lower().startswith("bearer "):
                given = auth[7:].strip()
            return bool(given) and secrets.compare_digest(given, cfg.auth_token)
        return self.bridge.cookies.valid(self.headers.get("Cookie"))

    # ----- dispatch -----
    def do_OPTIONS(self):
        self.send(HTTPStatus.NO_CONTENT)

    def do_HEAD(self):
        self.dispatch("GET")

    def do_GET(self):
        self.dispatch("GET")

    def do_POST(self):
        self.dispatch("POST")

    def do_DELETE(self):
        self.dispatch("DELETE")

    def dispatch(self, method: str):
        path = self.route_path()
        methods_here = set()
        for m, pattern, name, needs_auth in ROUTES:
            match = pattern.match(path)
            if not match:
                continue
            methods_here.add(m)
            if m != method:
                continue
            try:
                if needs_auth and not self.authorized():
                    return self.error(HTTPStatus.UNAUTHORIZED, "unauthorized")
                return getattr(self, "h_" + name)(**match.groupdict())
            except NoSession:
                return self.error(HTTPStatus.GONE, "no such session")
            except (ValueError, json.JSONDecodeError) as err:
                return self.error(HTTPStatus.BAD_REQUEST, f"bad request: {err}")
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as err:  # never let a handler take the thread down silently
                log.exception("unhandled error in %s", name)
                return self.error(HTTPStatus.INTERNAL_SERVER_ERROR, f"internal error: {err}")
        if methods_here:
            return self.error(HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed")
        self.error(HTTPStatus.NOT_FOUND, "not found")

    # ----- handlers -----
    def h_health(self):
        b = self.bridge
        status = b.sessions.jasna.status(timeout=3.0)
        procs = b.sessions.procs
        self.send_json(HTTPStatus.OK, {
            "version": VERSION,
            "uptime_seconds": round(time.time() - b.started),
            "jasna": {
                "reachable": status is not None,
                "streaming": bool(status and status.get("streaming")),
                "managed": procs.managed,
                "running": procs.alive(),
                "pid": procs.pid(),
                "preset": procs.running_preset if procs.managed else None,
                "warm": b.sessions.is_warm(),
            },
            **b.sessions.snapshot(),
        })

    def h_presets(self):
        cfg = self.bridge.cfg
        self.send_json(HTTPStatus.OK, {
            "default": cfg.default_preset,
            "running": self.bridge.sessions.procs.running_preset if cfg.manage_process else None,
            "warm": self.bridge.sessions.is_warm(),
            "manage_process": cfg.manage_process,
            "presets": [{"name": p.name, "description": p.description} for p in cfg.presets.values()],
        })

    def h_session_get(self):
        self.send_json(HTTPStatus.OK, self.bridge.sessions.snapshot())

    def h_session_post(self):
        b = self.bridge
        body = self.read_json()
        scene_id = str(body.get("scene_id") or "").strip()
        if not scene_id.isdigit():
            raise ValueError("scene_id must be a numeric Stash scene id")
        preset = body.get("preset") or b.cfg.default_preset
        if preset not in b.cfg.presets:
            return self.error(HTTPStatus.BAD_REQUEST, f"unknown preset {preset!r}")
        if b.cfg.manage_process is False and preset != b.cfg.default_preset:
            return self.error(HTTPStatus.BAD_REQUEST, "preset switching needs jasna.manage_process = true")
        time_s = float(body.get("time") or 0)
        force = bool(body.get("force"))
        try:
            stash_path, duration = b.stash.scene_path(scene_id)
        except StashError as err:
            return self.error(HTTPStatus.NOT_FOUND, str(err))
        path = b.map_path(stash_path)
        t0 = time.monotonic()
        try:
            session, info = b.sessions.create(scene_id, path, preset, time_s, self.client_ip(), force=force)
        except Busy as busy:
            return self.send_json(HTTPStatus.CONFLICT, {"error": "busy", **busy.payload})
        except JasnaError as err:
            return self.error(HTTPStatus.BAD_GATEWAY, str(err))
        self.send_json(HTTPStatus.OK, {
            "token": session.token,
            "playlist_path": f"/hls/{session.token}/stream.m3u8",
            "scene_id": scene_id,
            "preset": preset,
            "duration": duration,
            "heartbeat_seconds": max(5, int(b.cfg.heartbeat_idle_s // 3)),
            "ready_seconds": round(time.monotonic() - t0, 2),
            **info,
        })

    def h_heartbeat(self, token: str):
        body = self.read_json()
        t = body.get("time")
        paused = body.get("paused")
        s = self.bridge.sessions.heartbeat(
            token,
            float(t) if isinstance(t, (int, float)) else None,
            bool(paused) if isinstance(paused, bool) else None,
        )
        self.send_json(HTTPStatus.OK, {"ok": True, "idle_limit": self.bridge.cfg.heartbeat_idle_s,
                                       "segments": s.segments})

    def h_end(self, token: str):
        try:
            self.bridge.sessions.end(token)
        except NoSession:
            pass  # idempotent: a beacon after an idle release is fine
        self.send_json(HTTPStatus.OK, {"ok": True})

    def h_playlist(self, token: str):
        self.bridge.sessions.get(token)
        data = self.bridge.sessions.jasna.playlist()
        if data is None:
            return self.error(HTTPStatus.BAD_GATEWAY, "Jasna has no stream open")
        self.send(HTTPStatus.OK, data, "application/vnd.apple.mpegurl")

    def h_segment(self, token: str, seg: str):
        self.bridge.sessions.touch_segment(token)
        jasna = self.bridge.sessions.jasna
        try:
            upstream = jasna.open_segment(seg)
        except OSError as err:
            return self.error(HTTPStatus.BAD_GATEWAY, f"segment fetch failed: {err}")
        try:
            if upstream.status != 200:
                upstream.read()
                return self.error(HTTPStatus.BAD_GATEWAY, f"Jasna returned HTTP {upstream.status} for {seg}")
            length = upstream.getheader("Content-Length")
            if length is None:
                body = upstream.read()
                return self.send(HTTPStatus.OK, body, "video/mp2t")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "video/mp2t")
            self.send_header("Content-Length", length)
            self.send_header("Cache-Control", "no-store")
            for k, v in self.cors_headers().items():
                self.send_header(k, v)
            self.end_headers()
            shutil.copyfileobj(upstream, self.wfile, 65536)
        finally:
            upstream.close()


def serve(cfg, sessions, stash: StashClient) -> ThreadingHTTPServer:
    bridge = Bridge(cfg, sessions, stash)
    handler = type("BridgeHandler", (Handler,), {"bridge": bridge})
    server = ThreadingHTTPServer((cfg.host, cfg.port), handler)
    server.daemon_threads = True
    server.bridge = bridge
    thread = threading.Thread(target=server.serve_forever, name="http", daemon=True)
    thread.start()
    log.info("listening on http://%s:%d%s", cfg.host, server.server_address[1], cfg.path_prefix or "")
    return server
