"""Session ownership, idle timeout, stream linger and process idle shutdown.

State machine (one Jasna process, one stream, at most one owner):

  session: None ──POST /session──▶ preparing ──▶ active ──┐
     ▲                                                     │ DELETE, or no
     │                                                     │ heartbeat for
     └──────────────────── released ◀──────────────────────┘ heartbeat_idle_s

  stream: open while a session is active; after release it lingers for
  stream_linger_s so ON→OFF→ON on the same file is warm, then /stop.
  process (managed only): terminated process_idle_minutes after the last
  stream closed, restarted on the next session (cold start).
"""
from __future__ import annotations

import logging
import secrets
import threading
import time
from dataclasses import dataclass, field

from .jasna import JasnaClient, JasnaError, ProcessManager

log = logging.getLogger("bridge.sessions")


class Busy(Exception):
    def __init__(self, payload: dict):
        super().__init__("busy")
        self.payload = payload


class NoSession(Exception):
    pass


@dataclass
class Session:
    token: str
    scene_id: str
    path: str
    preset: str
    client: str
    created: float = field(default_factory=time.monotonic)
    last_activity: float = field(default_factory=time.monotonic)
    # Last sign of actual watching: a segment fetch or an unpaused heartbeat.
    # A paused tab keeps heartbeating (last_activity fresh, so it is not
    # idle-released) but should count as idle for takeover.
    last_watch: float = field(default_factory=time.monotonic)
    last_heartbeat: float | None = None
    last_seg_request: float = field(default_factory=time.monotonic)  # client asked for a segment
    last_seg_served: float = field(default_factory=time.monotonic)   # a segment was actually delivered (cache or Jasna)
    time: float = 0.0
    paused: bool = False
    segments: int = 0
    cache_key: str = ""   # segment-cache key for (path, preset flags, jasna version); "" = cache off
    lazy: bool = False    # served from a complete cache; Jasna is opened only on a miss

    def idle_seconds(self, now: float | None = None) -> float:
        return (now or time.monotonic()) - self.last_activity

    def watch_idle_seconds(self, now: float | None = None) -> float:
        return (now or time.monotonic()) - self.last_watch

    def public(self, idle_limit: float) -> dict:
        now = time.monotonic()
        return {
            "scene_id": self.scene_id,
            "path": self.path,
            "preset": self.preset,
            "owner_since": round(now - self.created, 1),
            "idle_seconds": round(self.idle_seconds(now), 1),
            "watch_idle_seconds": round(self.watch_idle_seconds(now), 1),
            "idle_limit": idle_limit,
            "time": self.time,
            "paused": self.paused,
            "segments": self.segments,
            "from_cache": self.lazy,
        }


class SessionManager:
    def __init__(self, cfg, jasna: JasnaClient, procs: ProcessManager, cache=None):
        self.cfg = cfg
        self.jasna = jasna
        self.procs = procs
        self.cache = cache  # SegmentCache or None
        self.lock = threading.RLock()
        self._open_lock = threading.Lock()  # serialises lazy opens (network I/O, never under self.lock)
        self.current: Session | None = None
        self.preparing: dict | None = None  # {scene_id, since} while /open is in flight
        self.stream_path: str | None = None
        self.stream_preset: str | None = None
        self.stream_idle_since: float | None = None
        self.process_idle_since: float | None = time.monotonic()
        self.stats = {"sessions": 0, "opens": 0, "reuses": 0, "idle_releases": 0, "cached_sessions": 0}
        self._stop = threading.Event()
        self._reaper = threading.Thread(target=self._reap_loop, name="reaper", daemon=True)

    # ----- lifecycle -----
    def start(self) -> None:
        self.adopt_existing_stream()
        self._reaper.start()

    def adopt_existing_stream(self) -> None:
        """If Jasna already has a stream open (started by hand, or left over
        from a previous bridge run), track it as an unowned lingering stream so
        health reports warm, a matching session reuses it, and the linger
        timer closes it instead of leaking it."""
        status = self.jasna.status()
        if status and status.get("streaming") and status.get("path"):
            with self.lock:
                self.stream_path = status["path"]
                self.stream_preset = self.procs.running_preset or self.cfg.default_preset
                self.stream_idle_since = time.monotonic()
                self.process_idle_since = None
            log.info("adopted open Jasna stream: %s", status["path"])

    def shutdown(self) -> None:
        self._stop.set()
        with self.lock:
            if self.current:
                self._release(self.current, "shutdown")
            if self.stream_path:
                self._close_stream("shutdown")
        self.procs.stop()

    # ----- queries -----
    def snapshot(self) -> dict:
        with self.lock:
            now = time.monotonic()
            return {
                "active": self.current is not None,
                "preparing": dict(self.preparing) if self.preparing else None,
                "session": self.current.public(self.cfg.heartbeat_idle_s) if self.current else None,
                "stream": {
                    "path": self.stream_path,
                    "preset": self.stream_preset,
                    "lingering_seconds": round(now - self.stream_idle_since, 1) if self.stream_idle_since else None,
                    "linger_limit": self.cfg.stream_linger_s,
                },
                "stats": dict(self.stats),
            }

    def is_warm(self) -> bool:
        return self.stream_path is not None or (self.procs.managed and self.procs.alive())

    def get(self, token: str) -> Session:
        with self.lock:
            s = self.current
            if s is None or not secrets.compare_digest(s.token, token):
                raise NoSession()
            return s

    # ----- commands -----
    def create(self, scene_id: str, path: str, preset: str, time_s: float, client: str,
               force: bool = False) -> tuple[Session, dict]:
        with self.lock:
            now = time.monotonic()
            if self.preparing:
                raise Busy({"reason": "preparing", "scene_id": self.preparing["scene_id"],
                            "owner_since": round(now - self.preparing["since"], 1), "idle_seconds": 0,
                            "takeover_available": False})
            if self.current:
                idle = self.current.idle_seconds(now)
                # Takeover keys off watching, not heartbeats: seen 2026-09-09, a
                # paused phone heartbeating every 30s was only "idle" for the last
                # 10s of each cycle, so TAKE OVER? appeared or not by luck.
                watch_idle = self.current.watch_idle_seconds(now)
                if idle >= self.cfg.heartbeat_idle_s:
                    self._release(self.current, "idle (pre-empted)")
                elif force and watch_idle >= self.cfg.takeover_idle_s:
                    self._release(self.current, f"taken over by {client} (owner not watching for {watch_idle:.0f}s"
                                  f"{', paused' if self.current.paused else ''})")
                else:
                    payload = {"reason": "active", **self.current.public(self.cfg.heartbeat_idle_s)}
                    payload["takeover_available"] = watch_idle >= self.cfg.takeover_idle_s
                    payload["takeover_idle_s"] = self.cfg.takeover_idle_s
                    raise Busy(payload)
            self.preparing = {"scene_id": scene_id, "since": now}
        key = self.cache.key(path, preset, self.cfg.presets[preset].flags) if self.cache else ""
        try:
            if key and self.cache.is_complete(key):
                # Every segment is on disk: no Jasna, no GPU. A miss (eviction
                # made a hole) opens Jasna on demand via ensure_stream().
                with self.lock:
                    session = Session(secrets.token_urlsafe(24), scene_id, path, preset, client, time=time_s,
                                      cache_key=key, lazy=True)
                    self.current = session
                    self.stats["sessions"] += 1
                    self.stats["cached_sessions"] += 1
                log.info("session %s: scene %s preset %s for %s (from cache, Jasna not opened)",
                         session.token[:8], scene_id, preset, client)
                return session, {"reused": False, "cold": False, "switched": False, "cached": True}
            cold = self.procs.ensure(preset)
            reused = False
            with self.lock:
                switched = self.stream_path is not None and self.stream_path != path
                candidate = (not cold and self.stream_path == path and self.stream_preset == preset)
            if candidate:
                # Network I/O deliberately outside self.lock: a slow or wedged Jasna
                # must not stall heartbeats, /session and /health behind it.
                status = self.jasna.status() or {}
                reused = bool(status.get("streaming")) and status.get("path") == path
            if not reused:
                if self.stream_path and self.stream_path != path:
                    log.info("switching stream %s -> %s", self.stream_path, path)
                self.jasna.open(path)
                self.stats["opens"] += 1
                with self.lock:
                    self.stream_path, self.stream_preset, self.stream_idle_since = path, preset, None
                    self.process_idle_since = None
                if not self.jasna.wait_ready(self.cfg.jasna_open_timeout_s, cancelled=self._stop.is_set):
                    raise JasnaError("stream did not become ready in time")
            else:
                self.stats["reuses"] += 1
            with self.lock:
                session = Session(secrets.token_urlsafe(24), scene_id, path, preset, client, time=time_s,
                                  cache_key=key)
                self.current = session
                self.stream_idle_since = None
                self.stats["sessions"] += 1
            log.info("session %s: scene %s preset %s for %s (%s)", session.token[:8], scene_id, preset, client,
                     "reused" if reused else "cold" if cold else "switched" if switched else "warm")
            return session, {"reused": reused, "cold": cold, "switched": switched, "cached": False}
        except Exception:
            with self.lock:
                if self.stream_path == path and self.jasna.playlist() is None:
                    self.stream_path = self.stream_preset = None
                    self.stream_idle_since = None
                    self.process_idle_since = time.monotonic()
            raise
        finally:
            with self.lock:
                self.preparing = None

    def heartbeat(self, token: str, time_s: float | None, paused: bool | None) -> Session:
        with self.lock:
            s = self.get(token)
            now = time.monotonic()
            s.last_activity = s.last_heartbeat = now
            if time_s is not None:
                s.time = time_s
            if paused is not None:
                s.paused = paused
            if not s.paused:
                s.last_watch = now
            return s

    def ensure_stream(self, token: str) -> Session:
        """Lazy open: a session served from cache hit a segment the cache does
        not have (or lost to eviction). Open Jasna for its file now."""
        with self.lock:
            s = self.get(token)
        with self._open_lock:
            with self.lock:
                s = self.get(token)
                have = self.stream_path == s.path and self.stream_preset == s.preset
            if have and not self.procs.managed:
                return s
            if have and self.procs.alive() and (self.jasna.status() or {}).get("path") == s.path:
                return s
            log.info("session %s: cache miss, opening Jasna for %s", s.token[:8], s.path)
            self.procs.ensure(s.preset)
            self.jasna.open(s.path)
            self.stats["opens"] += 1
            with self.lock:
                self.stream_path, self.stream_preset, self.stream_idle_since = s.path, s.preset, None
                self.process_idle_since = None
            if not self.jasna.wait_ready(self.cfg.jasna_open_timeout_s, cancelled=self._stop.is_set):
                raise JasnaError("stream did not become ready in time")
            with self.lock:
                s.lazy = False
            return s

    def touch_segment(self, token: str) -> Session:
        with self.lock:
            s = self.get(token)
            now = time.monotonic()
            s.last_activity = s.last_watch = s.last_seg_request = now
            s.segments += 1
            return s

    def segment_served(self, token: str) -> None:
        """A segment was actually delivered to the client, from cache or from
        Jasna. Keeps the pipeline-stall clock alive: a slow-but-live pass still
        serves one every few seconds, only a real stall goes quiet."""
        with self.lock:
            s = self.current
            if s is not None and secrets.compare_digest(s.token, token):
                s.last_seg_served = time.monotonic()

    def end(self, token: str) -> None:
        with self.lock:
            s = self.get(token)
            self._release(s, "ended by client")

    # ----- internals (call with lock held) -----
    def _release(self, s: Session, why: str) -> None:
        log.info("session %s released: %s (%d segments, %.0fs)", s.token[:8], why, s.segments,
                 time.monotonic() - s.created)
        if self.current is s:
            self.current = None
        if self.stream_path is not None:
            self.stream_idle_since = time.monotonic()

    def _close_stream(self, why: str) -> None:
        log.info("closing stream %s: %s", self.stream_path, why)
        self.jasna.stop()
        self.stream_path = self.stream_preset = None
        self.stream_idle_since = None
        self.process_idle_since = time.monotonic()

    def _reap_loop(self) -> None:
        while not self._stop.wait(self.cfg.reaper_interval_s):
            try:
                self._reap_once()
            except Exception:  # keep the reaper alive no matter what
                log.exception("reaper error")

    def _reap_once(self) -> None:
        now = time.monotonic()
        idle_check = False
        with self.lock:
            s = self.current
            if s and s.idle_seconds(now) >= self.cfg.heartbeat_idle_s:
                self.stats["idle_releases"] += 1
                self._release(s, f"idle for {s.idle_seconds(now):.0f}s")
            if (self.current is None and self.stream_path is not None and self.preparing is None
                    and self.stream_idle_since is not None
                    and now - self.stream_idle_since >= self.cfg.stream_linger_s):
                self._close_stream("linger expired")
            if (self.procs.managed and self.stream_path is None and self.preparing is None
                    and self.process_idle_since is not None and self.procs.alive()
                    and now - self.process_idle_since >= self.cfg.process_idle_minutes * 60):
                log.info("Jasna idle for %.0f min, stopping process", self.cfg.process_idle_minutes)
                self.procs.stop()
                self.process_idle_since = None
            idle_check = (self.procs.managed and self.preparing is None and self.procs.alive())
            # Pipeline stall: Jasna's HTTP server still answers /status (so the
            # liveness probe below is happy) but the render pass has stopped
            # producing segments. Seen 2026-09-09: after grinding a long file the
            # pass went quiet at one segment, GPU 0%, /status still {streaming:
            # true}, and the viewer sat forever. Distinct from a server wedge.
            # Fire only for an active, playing (not paused), Jasna-served (not
            # lazy/cache-only) session that has asked for a segment it has not
            # been given for pipeline_stall_s.
            stall = bool(s and not s.paused and not s.lazy and self.procs.managed
                         and self.stream_path is not None and s is self.current
                         and s.last_seg_request > s.last_seg_served
                         and now - s.last_seg_served >= self.cfg.pipeline_stall_s
                         and now - s.last_seg_request <= self.cfg.pipeline_stall_s)
        # Liveness probe outside the lock (network). Runs whether or not a session is
        # active: seen 2026-09-08, a seek wedged Jasna mid-session (ffmpeg child hung,
        # server stopped accepting) and the viewer sat stalled until they toggled.
        if stall:
            log.warning("Jasna answering /status but no segment served for %.0fs while playing; "
                        "treating as a pipeline stall", self.cfg.pipeline_stall_s)
            self._unresponsive = 0
            return self._recover_wedged()
        if idle_check:
            if self.procs.responsive(timeout=3.0):
                self._unresponsive = 0
            else:
                self._unresponsive = getattr(self, "_unresponsive", 0) + 1
                if self._unresponsive >= 2:
                    self._unresponsive = 0
                    self._recover_wedged()

    def _recover_wedged(self) -> None:
        """Jasna stopped answering. Restart it; if a session is active, re-open its
        file so the viewer's hls.js retries resume on the same token. Called from
        the reaper thread, network I/O outside the lock."""
        with self.lock:
            s = self.current if (self.current and not self.current.lazy) else None
            path, preset = (s.path, s.preset) if s else (self.stream_path, self.stream_preset)
        log.warning("Jasna not answering /status; restarting it%s",
                    f" and re-opening {path} for session {s.token[:8]}" if s else "")
        self.procs.stop(graceful=False)  # its graceful teardown is the broken path; kill outright
        with self.lock:
            self.stream_path = self.stream_preset = None
            self.stream_idle_since = None
            self.process_idle_since = None if s else time.monotonic()
        if not s:
            return
        try:
            self.procs.ensure(preset)
            self.jasna.open(path)
            self.stats["opens"] += 1
            with self.lock:
                self.stream_path, self.stream_preset = path, preset
            if not self.jasna.wait_ready(self.cfg.jasna_open_timeout_s, cancelled=self._stop.is_set):
                raise JasnaError("stream did not become ready after restart")
            with self.lock:
                if self.current is s:
                    now = time.monotonic()
                    # do not idle-release, and give the fresh pass a full stall
                    # window before the detector can fire again.
                    s.last_activity = s.last_seg_served = s.last_seg_request = now
            log.info("Jasna restarted and %s re-opened; session %s continues", path, s.token[:8])
        except JasnaError as err:
            log.error("recovery failed (%s); releasing session %s", err, s.token[:8])
            with self.lock:
                if self.current is s:
                    self._release(s, "jasna recovery failed")
