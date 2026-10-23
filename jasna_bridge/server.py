"""HTTP front end: routing, auth, CORS and HLS proxying. Stdlib http.server."""
from __future__ import annotations

import json
import logging
import os
import re
import secrets
import sys
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import VERSION
from .config import map_path, validate_custom_preset
from .jasna import JasnaError
from .jobs import ACTIVE as JOB_ACTIVE, STATES as JOB_STATES, JobError
from .mpegts import JASNA_MUX_DELAY_S, looks_partial, restamp, track_spans, trim_leading_audio
from .playlist import retime
from .sessions import Busy, NoSession
from .stash import CookieValidator, StashClient, StashError

log = logging.getLogger("bridge.http")

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "queue_page.html"), "rb") as _fh:
    QUEUE_PAGE = _fh.read()

TOKEN_RE = r"(?P<token>[A-Za-z0-9_-]{16,64})"
JOB_RE = r"(?P<job_id>[0-9a-f]{12})"
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
    # Batch jobs (jobs.py), used by the Stash jasna-batch plugin.
    ("GET", re.compile(r"^/queue$"), "queue_page", True),
    ("GET", re.compile(r"^/batch$"), "batch", True),
    ("GET", re.compile(r"^/batch/check$"), "batch_check", True),
    ("GET", re.compile(r"^/jobs$"), "jobs_list", True),
    ("POST", re.compile(r"^/jobs$"), "jobs_post", True),
    ("GET", re.compile(rf"^/jobs/{JOB_RE}$"), "job_get", True),
    ("DELETE", re.compile(rf"^/jobs/{JOB_RE}$"), "job_delete", True),
    ("POST", re.compile(rf"^/jobs/{JOB_RE}/ack$"), "job_ack", True),
    ("GET", re.compile(rf"^/jobs/{JOB_RE}/log$"), "job_log", True),
]


class Bridge:
    """Shared state handed to every request handler."""

    def __init__(self, cfg, sessions, stash: StashClient, jobs=None, gpu=None):
        self.cfg = cfg
        self.sessions = sessions
        self.cache = getattr(sessions, "cache", None)
        self.stash = stash
        self.jobs = jobs  # JobQueue, or None when batch.enabled = false
        self.gpu = gpu    # GpuSampler, or None
        self.cookies = CookieValidator(stash, cfg.auth_cache_s) if cfg.auth_mode == "stash_cookie" else None
        self.started = time.time()

    def map_path(self, path: str) -> str:
        return map_path(self.cfg, path)


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
            except JobError as err:
                return self.error(err.status, str(err), **err.extra)
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
            "cache": b.cache.snapshot() if b.cache else {"enabled": False},
            "batch": b.jobs.snapshot() if b.jobs else {"enabled": False},
            "gpu": b.gpu.snapshot() if b.gpu else None,
            **b.sessions.snapshot(),
        })

    # ----- batch jobs -----
    def jobs_or_404(self):
        jobs = self.bridge.jobs
        if jobs is None:
            raise JobError("batch jobs are disabled on this bridge (batch.enabled = false)", HTTPStatus.NOT_FOUND)
        return jobs

    def h_queue_page(self):
        """The queue as a page for a phone or desktop browser: polls
        ./health and ./jobs (relative, so any path_prefix works), names
        scenes through Stash's GraphQL on the same origin, cancels with
        DELETE ./jobs/{id}."""
        self.jobs_or_404()
        self.send(HTTPStatus.OK, QUEUE_PAGE, "text/html; charset=utf-8")

    def h_batch(self):
        jobs = self.jobs_or_404()
        self.send_json(HTTPStatus.OK, {**jobs.snapshot(), "jasna_version": self.bridge.sessions.procs.version_string(),
                                       "output_roots": self.bridge.cfg.batch_output_roots,
                                       "grace_s": self.bridge.cfg.batch_grace_s,
                                       "min_free_gb": self.bridge.cfg.batch_min_free_gb})

    def h_batch_check(self):
        self.send_json(HTTPStatus.OK, self.jobs_or_404().check())

    def h_jobs_list(self):
        jobs = self.jobs_or_404()
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        states = None
        if query.get("state"):
            states = {s for s in query["state"][0].split(",") if s}
            bad = states - set(JOB_STATES)
            if bad:
                raise ValueError(f"unknown state(s) {sorted(bad)}; choose from {', '.join(JOB_STATES)}")
        unacked = query.get("unacknowledged", ["0"])[0] in ("1", "true", "yes")
        found = jobs.list(states, unacked_only=unacked)
        self.send_json(HTTPStatus.OK, {"jobs": [j.public() for j in found], "count": len(found)})

    def h_jobs_post(self):
        jobs = self.jobs_or_404()
        body = self.read_json()
        scene_id = str(body.get("scene_id") or "").strip()
        if not scene_id.isdigit():
            raise ValueError("scene_id must be a numeric Stash scene id")
        preset = body.get("preset")
        flags = body.get("flags")
        if not isinstance(preset, str) or not isinstance(flags, list):
            raise ValueError("preset (string) and flags (list) are required")
        job = jobs.submit(scene_id, str(body.get("file_path") or ""), str(body.get("output_path") or ""),
                          preset, flags, self.client_ip())
        self.send_json(HTTPStatus.CREATED, {"job": job.public(), "position": len(jobs.queued())})

    def h_job_get(self, job_id: str):
        job = self.jobs_or_404().get(job_id)
        if job is None:
            raise JobError("no such job", HTTPStatus.NOT_FOUND)
        self.send_json(HTTPStatus.OK, {"job": job.public()})

    def h_job_delete(self, job_id: str):
        job = self.jobs_or_404().cancel(job_id)
        self.send_json(HTTPStatus.OK, {"job": job.public(), "cancelling": job.state in JOB_ACTIVE})

    def h_job_ack(self, job_id: str):
        job = self.jobs_or_404().ack(job_id)
        self.send_json(HTTPStatus.OK, {"job": job.public()})

    def h_job_log(self, job_id: str):
        text = self.jobs_or_404().log_tail(job_id)
        self.send(HTTPStatus.OK, text.encode(), "text/plain; charset=utf-8")

    def h_presets(self):
        cfg = self.bridge.cfg
        self.send_json(HTTPStatus.OK, {
            "default": cfg.default_preset,
            "running": self.bridge.sessions.procs.running_preset if cfg.manage_process else None,
            "warm": self.bridge.sessions.is_warm(),
            "manage_process": cfg.manage_process,
            "custom_allowed": bool(cfg.custom_presets and cfg.manage_process),
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
        if body.get("flags") is not None:
            # A custom preset: the plugin's "Custom presets" setting sends the
            # flags along. Validated, then registered under its name so the
            # rest of the bridge resolves it like a configured one.
            if not b.cfg.manage_process:
                return self.error(HTTPStatus.BAD_REQUEST, "custom presets need jasna.manage_process = true")
            if not b.cfg.custom_presets:
                return self.error(HTTPStatus.BAD_REQUEST, "custom presets are disabled (jasna.custom_presets = false)")
            if preset in b.cfg.presets:
                return self.error(HTTPStatus.BAD_REQUEST,
                                  f"custom preset {preset!r} collides with a configured preset; rename it")
            custom = validate_custom_preset(preset, body["flags"])
            preset = custom.name
        else:
            custom = None
        if custom is None and not b.cfg.has_preset(preset):
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
            session, info = b.sessions.create(scene_id, path, preset, time_s, self.client_ip(), force=force,
                                              custom=custom)
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
        b = self.bridge
        s = b.sessions.get(token)
        if b.cache and s.cache_key:
            data = b.cache.manifest(s.cache_key)
            if data is not None:
                return self.send_playlist(s, data)
        data = b.sessions.jasna.playlist()
        if data is None and s.lazy:
            try:
                b.sessions.ensure_stream(token)
            except JasnaError as err:
                return self.error(HTTPStatus.BAD_GATEWAY, str(err))
            data = b.sessions.jasna.playlist()
        if data is None:
            return self.error(HTTPStatus.BAD_GATEWAY, "Jasna has no stream open")
        if b.cache and s.cache_key:
            b.cache.store_manifest(s.cache_key, s.path, s.preset, b.cfg.preset(s.preset).flags, data)
        self.send_playlist(s, data)

    def send_playlist(self, s, data: bytes) -> None:
        """Jasna's playlist with every cached segment's entry moved to where
        its video really starts (playlist.retime): hls.js pins the first
        fragment it loads to its playlist position, so a session that begins
        on a cached mid-pass segment would otherwise keep the picture seconds
        away from the clock - and subtitles with it."""
        b = self.bridge
        extra = {}
        if b.cache and s.cache_key:
            starts = b.cache.video_starts(s.cache_key)
            if starts:
                text, moved = retime(data.decode(errors="replace"), starts, JASNA_MUX_DELAY_S)
                if moved:
                    data = text.encode()
                    extra["X-Bridge-Retimed"] = str(moved)
        self.send(HTTPStatus.OK, data, "application/vnd.apple.mpegurl", extra)

    # Adjacent segments whose video does not meet within this many seconds are
    # from different Jasna passes (measured seams: 1.7-7s; same-pass joins: <0.1s).
    SEAM_TOLERANCE_S = 0.5
    # Within a pass audio leads video by one AAC frame at most (<= 25ms).
    AUDIO_LEAD_TOLERANCE_S = 0.1

    # Jasna serves a segment file as soon as it exists, so at the live edge of
    # a pass the first read is often of a file ffmpeg is still writing
    # (mpegts.partial_size). Re-read until it is whole; never serve or cache a
    # partial one - its cut-off last frame and the hole before the next
    # segment are what the viewer saw as strobing / a frozen picture.
    PARTIAL_POLL_S = 0.3
    PARTIAL_WAIT_S = 45.0

    def fetch_complete(self, token: str, seq: int, seg: str) -> bytes | None:
        """A whole segment from Jasna, in memory (a few MB: the cache write is
        atomic and the splice needs it). On failure sends the error response
        and returns None."""
        b = self.bridge
        deadline = time.monotonic() + self.PARTIAL_WAIT_S
        rereads = 0
        while True:
            try:
                upstream = b.sessions.jasna.open_segment(seg)
            except OSError as err:
                self.error(HTTPStatus.BAD_GATEWAY, f"segment fetch failed: {err}")
                return None
            try:
                if upstream.status != 200:
                    upstream.read()
                    self.error(HTTPStatus.BAD_GATEWAY, f"Jasna returned HTTP {upstream.status} for {seg}")
                    return None
                body = upstream.read()
            finally:
                upstream.close()
            if not looks_partial(body):
                if rereads:
                    log.info("%s was still being written; complete after %d re-read(s) (%d bytes)",
                             seg, rereads, len(body))
                return body
            if rereads == 0:
                b.sessions.note_partial_read()
            if time.monotonic() >= deadline or b.sessions.superseded(token, seq):
                self.error(HTTPStatus.SERVICE_UNAVAILABLE, f"{seg} is still being written ({len(body)} bytes)")
                return None
            rereads += 1
            time.sleep(self.PARTIAL_POLL_S)

    def h_segment(self, token: str, seg: str):
        b = self.bridge
        s, st = b.sessions.touch_segment(token)
        index = int(seg[4:9])
        key = s.cache_key if b.cache else ""
        body, tracks = None, {}
        if key:
            fn = b.cache.hit(key, seg)
            if fn:
                try:
                    with open(fn, "rb") as fh:
                        body = fh.read() or None
                except OSError:
                    body = None
            if body is not None:
                tracks = b.cache.tracks(key, seg, body)
                source = "cache"
            elif s.lazy:
                try:
                    b.sessions.ensure_stream(token)
                except JasnaError as err:
                    return self.error(HTTPStatus.BAD_GATEWAY, str(err))
        if body is None:
            if not b.sessions.seek_gate(token, st["seq"], index):
                # The viewer moved on while this seek waited its turn.
                return self.error(HTTPStatus.SERVICE_UNAVAILABLE, "superseded by a newer seek")
            with b.sessions.jasna_fetch():
                body = self.fetch_complete(token, st["seq"], seg)
            if body is None:
                return  # error response already sent
            source = "jasna"
            tracks = b.cache.put(key, seg, body) if key else track_spans(body)
        span = tracks.get("video") or tracks.get("audio")
        # The first segment of a Jasna pass carries 1.4-8s of audio from before
        # its first video frame (ffmpeg -ss with -c:a copy keeps audio from the
        # preceding keyframe on). Players anchor the stream's clock to the
        # earliest sample of the first fragment they load (measured 2026-10-12
        # with hls.js 1.7.2: video landed 7.9s late in media time), which put
        # subtitles and every later toggle/seek that many seconds off for the
        # whole session. Drop it, so the clock starts at the picture.
        audio = tracks.get("audio")
        lead = span[0] - audio[0] if span and audio and "video" in tracks else 0.0
        if lead > self.AUDIO_LEAD_TOLERANCE_S:
            body = trim_leading_audio(body, span[0])
            b.sessions.note_audio_trimmed()
            log.info("session %s: %s carried %.2fs of audio before its first frame; dropped", s.token[:8], seg, lead)
        # Splice. Jasna's PTS are honest source time, but segments from
        # different render passes (or a cache holding several) overlap or gap
        # by the drift between passes, and hls.js mangles video against audio
        # at such a join when the fragment is contiguous. A sequential segment
        # is shifted to continue the player's timeline, and the shift is kept
        # for the rest of the run. A repeat of the last segment (hls.js retry)
        # gets the same shift; any other request is a seek, placed by real PTS.
        sequential = st["last_seg"] == index - 1
        repeat = st["last_seg"] == index
        offset = st["ts_offset"] if (sequential or repeat) else 0.0
        seam = end = None
        if span is not None:
            if sequential and st["last_seg_end"] is not None:
                gap = st["last_seg_end"] - (span[0] + offset)
                if abs(gap) > self.SEAM_TOLERANCE_S:
                    seam = gap
                    offset += gap
            if offset:
                body = restamp(body, offset)
            end = span[1] + offset
        if repeat:
            # Already recorded; only keep the stall clock alive.
            b.sessions.segment_served(token, seq=st["seq"])
        else:
            b.sessions.segment_served(token, index, end, ts_offset=offset, seam=seam, seq=st["seq"])
        extra = {}
        if key:
            extra["X-Bridge-Cache"] = "hit" if source == "cache" else "miss"
        if offset:
            extra["X-Bridge-Offset"] = f"{offset:.3f}"
        if lead > self.AUDIO_LEAD_TOLERANCE_S:
            extra["X-Bridge-Audio-Trimmed"] = f"{lead:.3f}"
        self.send(HTTPStatus.OK, body, "video/mp2t", extra)


class BridgeServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        # A browser dropping a keep-alive connection is routine, not an error:
        # without this every one logged a 25-line traceback to the journal.
        err = sys.exc_info()[1]
        if isinstance(err, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, TimeoutError)):
            log.debug("client %s dropped the connection: %s", client_address[0], err)
            return
        super().handle_error(request, client_address)


def serve(cfg, sessions, stash: StashClient, jobs=None, gpu=None) -> ThreadingHTTPServer:
    bridge = Bridge(cfg, sessions, stash, jobs, gpu)
    handler = type("BridgeHandler", (Handler,), {"bridge": bridge})
    server = BridgeServer((cfg.host, cfg.port), handler)
    server.bridge = bridge
    thread = threading.Thread(target=server.serve_forever, name="http", daemon=True)
    thread.start()
    log.info("listening on http://%s:%d%s", cfg.host, server.server_address[1], cfg.path_prefix or "")
    return server
