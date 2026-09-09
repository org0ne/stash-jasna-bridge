"""Jasna --stream HTTP client and optional process supervision.

Jasna facts this relies on (verified against 0.10.0, see notes):
  POST /open {"path"}      one video per process; a new path pre-empts
  POST /stop               tears the stream down; /stream.m3u8 then 404s
  GET  /status             {"streaming": bool, "path": str|null}
  GET  /stream.m3u8        static VOD playlist for the whole file
  GET  /seg_NNNNN.ts       rendered on demand, blocks up to ~30s
Settings are launch flags only, so a preset change means a restart.
"""
from __future__ import annotations

import http.client
import json
import logging
import os
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger("bridge.jasna")


class JasnaError(Exception):
    pass


class JasnaClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        parsed = urllib.parse.urlsplit(self.base_url)
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or 80
        self.timeout = timeout

    def _request(self, method: str, path: str, body: bytes | None = None, timeout: float | None = None):
        headers = {"Content-Type": "application/json"} if body is not None else {}
        req = urllib.request.Request(self.base_url + path, data=body, headers=headers, method=method)
        return urllib.request.urlopen(req, timeout=timeout or self.timeout)

    def status(self) -> dict | None:
        """Jasna's /status, or None when the server is unreachable."""
        try:
            with self._request("GET", "/status") as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, OSError, ValueError):
            return None

    def open(self, path: str) -> None:
        body = json.dumps({"path": path}).encode()
        try:
            with self._request("POST", "/open", body, timeout=max(self.timeout, 30)) as resp:
                resp.read()
        except urllib.error.HTTPError as err:
            raise JasnaError(f"/open returned HTTP {err.code}") from err
        except (urllib.error.URLError, OSError) as err:
            raise JasnaError(f"/open failed: {err}") from err

    def stop(self) -> bool:
        try:
            with self._request("POST", "/stop", b"{}") as resp:
                resp.read()
            return True
        except (urllib.error.URLError, OSError) as err:
            log.warning("/stop failed: %s", err)
            return False

    def playlist(self) -> bytes | None:
        """The manifest bytes, or None if Jasna has no stream open."""
        try:
            with self._request("GET", "/stream.m3u8") as resp:
                return resp.read()
        except urllib.error.HTTPError:
            return None
        except (urllib.error.URLError, OSError):
            return None

    def wait_ready(self, timeout: float, cancelled=lambda: False) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not cancelled():
            if self.playlist() is not None:
                return True
            time.sleep(0.25)
        return False

    def open_segment(self, name: str, timeout: float = 45.0) -> http.client.HTTPResponse:
        """Open a streaming GET for a segment. Caller must close the response
        (which also closes the connection)."""
        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        conn.request("GET", "/" + name)
        resp = conn.getresponse()
        resp._bridge_conn = conn  # keep the connection alive with the response
        return resp


class ProcessManager:
    """Owns the `jasna --stream` process when manage_process is on.

    With manage_process off every method is a no-op that reports the
    externally-run Jasna as already running; the bridge then only talks to
    the configured URL.
    """

    def __init__(self, cfg, client: JasnaClient):
        self.cfg = cfg
        self.client = client
        self.proc: subprocess.Popen | None = None
        self.running_preset: str | None = None
        self.started_at: float | None = None
        self.prewarmed = False
        self._lock = threading.RLock()

    @property
    def managed(self) -> bool:
        return bool(self.cfg.manage_process)

    def alive(self) -> bool:
        if not self.managed:
            return self.client.status() is not None
        with self._lock:
            return self.proc is not None and self.proc.poll() is None

    def pid(self) -> int | None:
        with self._lock:
            return self.proc.pid if self.proc and self.proc.poll() is None else None

    def command(self, preset: str) -> list[str]:
        p = self.cfg.presets[preset]
        return [
            self.cfg.jasna_binary, "--stream", "--no-browser",
            "--stream-port", str(self.cfg.jasna_stream_port),
            *self.cfg.jasna_common_flags, *p.flags,
        ]

    def ensure(self, preset: str) -> bool:
        """Make sure a Jasna serving `preset` is up. Returns True if this call
        had to (re)start the process (a cold start)."""
        if not self.managed:
            if self.client.status() is None:
                raise JasnaError(f"Jasna is not reachable at {self.client.base_url}")
            return False
        with self._lock:
            if self.proc is not None and self.proc.poll() is None:
                if self.running_preset == preset:
                    return False
                log.info("preset change %s -> %s: restarting Jasna", self.running_preset, preset)
                self.stop()
            elif self.proc is not None:
                log.warning("Jasna exited with code %s; restarting", self.proc.returncode)
                self.proc = None
            self._start(preset)
            return True

    def _start(self, preset: str) -> None:
        cmd = self.command(preset)
        log.info("starting Jasna: %s", " ".join(cmd[:6]) + (" ..." if len(cmd) > 6 else ""))
        cwd = self.cfg.jasna_workdir or os.path.dirname(os.path.abspath(self.cfg.jasna_binary)) or None
        try:
            self.proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, start_new_session=True, cwd=cwd)
        except OSError as err:
            raise JasnaError(f"could not start Jasna: {err}") from err
        self.running_preset = preset
        self.started_at = time.monotonic()
        deadline = self.started_at + self.cfg.jasna_start_timeout_s
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise JasnaError(f"Jasna exited during startup with code {self.proc.returncode}")
            if self.client.status() is not None:
                log.info("Jasna up (pid %s, preset %s) after %.1fs", self.proc.pid, preset,
                         time.monotonic() - self.started_at)
                if self.cfg.prewarm_path and not self.prewarmed:
                    self._prewarm()
                return
            time.sleep(0.5)
        self.stop()
        raise JasnaError("Jasna did not come up within jasna.start_timeout_s")

    def _prewarm(self) -> None:
        """Open the configured clip once so TensorRT engine caches exist."""
        try:
            self.client.open(self.cfg.prewarm_path)
            if self.client.wait_ready(self.cfg.jasna_open_timeout_s):
                # touch the first segment so the pipeline really spins up
                resp = self.client.open_segment("seg_00000.ts", timeout=120)
                resp.read()
                resp.close()
            self.client.stop()
            self.prewarmed = True
            log.info("prewarm done")
        except (JasnaError, OSError) as err:
            log.warning("prewarm failed: %s", err)

    def stop(self) -> None:
        with self._lock:
            proc = self.proc
            if proc is None:
                return
            if proc.poll() is None:
                log.info("stopping Jasna pid %s", proc.pid)
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    log.warning("Jasna ignored SIGTERM, killing")
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    proc.wait(timeout=5)
            self.proc = None
            self.running_preset = None
            self.started_at = None
