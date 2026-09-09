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
    last_heartbeat: float | None = None
    time: float = 0.0
    paused: bool = False
    segments: int = 0

    def idle_seconds(self, now: float | None = None) -> float:
        return (now or time.monotonic()) - self.last_activity

    def public(self, idle_limit: float) -> dict:
        now = time.monotonic()
        return {
            "scene_id": self.scene_id,
            "path": self.path,
            "preset": self.preset,
            "owner_since": round(now - self.created, 1),
            "idle_seconds": round(self.idle_seconds(now), 1),
            "idle_limit": idle_limit,
            "time": self.time,
            "paused": self.paused,
            "segments": self.segments,
        }


class SessionManager:
    def __init__(self, cfg, jasna: JasnaClient, procs: ProcessManager):
        self.cfg = cfg
        self.jasna = jasna
        self.procs = procs
        self.lock = threading.RLock()
        self.current: Session | None = None
        self.preparing: dict | None = None  # {scene_id, since} while /open is in flight
        self.stream_path: str | None = None
        self.stream_preset: str | None = None
        self.stream_idle_since: float | None = None
        self.process_idle_since: float | None = time.monotonic()
        self.stats = {"sessions": 0, "opens": 0, "reuses": 0, "idle_releases": 0}
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
    def create(self, scene_id: str, path: str, preset: str, time_s: float, client: str) -> tuple[Session, dict]:
        with self.lock:
            now = time.monotonic()
            if self.preparing:
                raise Busy({"reason": "preparing", "scene_id": self.preparing["scene_id"],
                            "owner_since": round(now - self.preparing["since"], 1), "idle_seconds": 0})
            if self.current:
                if self.current.idle_seconds(now) < self.cfg.heartbeat_idle_s:
                    raise Busy({"reason": "active", **self.current.public(self.cfg.heartbeat_idle_s)})
                self._release(self.current, "idle (pre-empted)")
            self.preparing = {"scene_id": scene_id, "since": now}
        try:
            cold = self.procs.ensure(preset)
            reused = False
            switched = self.stream_path is not None and self.stream_path != path
            with self.lock:
                if (not cold and self.stream_path == path and self.stream_preset == preset):
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
                session = Session(secrets.token_urlsafe(24), scene_id, path, preset, client, time=time_s)
                self.current = session
                self.stream_idle_since = None
                self.stats["sessions"] += 1
            log.info("session %s: scene %s preset %s for %s (%s)", session.token[:8], scene_id, preset, client,
                     "reused" if reused else "cold" if cold else "switched" if switched else "warm")
            return session, {"reused": reused, "cold": cold, "switched": switched}
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
            return s

    def touch_segment(self, token: str) -> Session:
        with self.lock:
            s = self.get(token)
            s.last_activity = time.monotonic()
            s.segments += 1
            return s

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
