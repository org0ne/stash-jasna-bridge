"""Jasna --stream HTTP client and optional process supervision.

Jasna facts this relies on (verified against 0.10.0, see notes):
  POST /open {"path"}      one video per process; a new path pre-empts
  POST /stop               tears the stream down; /stream.m3u8 then 404s
  GET  /status             {"streaming": bool, "path": str|null}
  GET  /stream.m3u8        static VOD playlist for the whole file
  GET  /seg_NNNNN.ts       rendered on demand, blocks up to ~30s
Settings are launch flags only, so a preset change means a restart.

Jasna 0.11.1 (verified 2026-10-16) keeps all of that except:
  DELETE /open             replaces POST /stop (which now 404s)
  GET  /status             omits "path" while idle
  playlist entries read seg_NNNNN.ts?epoch=N; the epoch bumps on every
  /open. A segment request must carry the current epoch to make Jasna
  seek: a bare or stale-epoch request for a segment that is not on disk
  is an immediate 404 and starts no pass (measured: bare seg_00300 stayed
  404 for 90s; seg_00900?epoch=1 seeked and served in 5s). Requests near
  the live edge wait either way. So the client learns the epoch from the
  playlist after every open and sends it with each segment fetch.
"""
from __future__ import annotations

import http.client
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger("bridge.jasna")



def jasna_license_candidates() -> list[str]:
    """Ordered candidate paths for Jasna's license.json across platforms.
    Jasna stores it via platformdirs user_config_dir('jasna'):
      Linux:   ~/.config/jasna/license.json (XDG_CONFIG_HOME honored)
      macOS:   ~/Library/Application Support/jasna/license.json
      Windows: %LOCALAPPDATA%/jasna/jasna or %APPDATA%/jasna (author dir
               varies with how platformdirs was called), so try both.
    First existing file wins; used for reading and for `doctor`."""
    home = os.path.expanduser("~")
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA", os.path.join(home, "AppData", "Local"))
        roaming = os.environ.get("APPDATA", os.path.join(home, "AppData", "Roaming"))
        dirs = [os.path.join(local, "jasna", "jasna"), os.path.join(local, "jasna"),
                os.path.join(roaming, "jasna", "jasna"), os.path.join(roaming, "jasna")]
    elif sys.platform == "darwin":
        dirs = [os.path.join(home, "Library", "Application Support", "jasna"),
                os.path.join(home, ".config", "jasna")]
    else:
        xdg = os.environ.get("XDG_CONFIG_HOME") or os.path.join(home, ".config")
        dirs = [os.path.join(xdg, "jasna"), os.path.join(home, ".config", "jasna")]
    out, seen = [], set()
    for d in dirs:
        f = os.path.join(d, "license.json")
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


# Jasna 0.11 replaced the supporter email + key (``--license-email`` /
# ``--license-key``, stored in license.json) with a per-machine activation
# (``--machine-key`` / ``--license-code`` / ``--activation-key``) that Jasna
# keeps in its own config folder. Passing the old flags to 0.11 is a fatal
# argparse error ("unrecognized arguments", exit 2; verified on 0.11.1), so
# the bridge must know which Jasna it is starting.
LICENSE_FLAGS_DROPPED_IN = (0, 11, 0)
_EPOCH_RE = re.compile(rb"^seg_[0-9]{5}\.ts\?epoch=([A-Za-z0-9_-]+)\s*$", re.M)
_VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")


def jasna_version_from_path(binary: str) -> tuple[int, int, int] | None:
    """The version encoded in the binary's (resolved) path, e.g.
    '.../jasna-linux-nvidia-0.11.1/jasna' -> (0, 11, 1); None if absent."""
    if not binary:
        return None
    m = _VERSION_RE.search(os.path.realpath(binary))
    return tuple(int(g) for g in m.groups()) if m else None  # type: ignore[return-value]


def jasna_accepts_license_flags(binary: str, timeout: float = 120.0) -> bool | None:
    """Ask ``binary --help`` whether it still takes --license-key. True/False
    when the help text settles it, None when it names neither flag (not a
    Jasna help text, or the probe failed). Needs no GPU or port; 0.11 first
    prints its installation check, which can take a few seconds."""
    if not binary:
        return None
    cwd = os.path.dirname(os.path.abspath(binary)) or None
    try:
        out = subprocess.run([binary, "--help"], capture_output=True, text=True, timeout=timeout,
                             stdin=subprocess.DEVNULL, cwd=cwd)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None
    text = (out.stdout or "") + (out.stderr or "")
    if "--license-key" in text:
        return True
    if "--activation-key" in text:
        return False
    return None


class JasnaError(Exception):
    pass


class JasnaClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        parsed = urllib.parse.urlsplit(self.base_url)
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or 80
        self.timeout = timeout
        self.epoch: str | None = None  # Jasna 0.11's per-open stream epoch, from the playlist; None on 0.10

    def _request(self, method: str, path: str, body: bytes | None = None, timeout: float | None = None):
        headers = {"Content-Type": "application/json"} if body is not None else {}
        req = urllib.request.Request(self.base_url + path, data=body, headers=headers, method=method)
        return urllib.request.urlopen(req, timeout=timeout or self.timeout)

    def status(self, timeout: float | None = None) -> dict | None:
        """Jasna's /status, or None when the server is unreachable or does not
        answer within `timeout` (a wedged Jasna accepts nothing at all)."""
        try:
            with self._request("GET", "/status", timeout=timeout or 5.0) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, OSError, ValueError):
            return None

    def open(self, path: str) -> None:
        body = json.dumps({"path": path}).encode()
        self.epoch = None  # every /open starts a new epoch; wait_ready()/playlist() picks it up
        try:
            with self._request("POST", "/open", body, timeout=max(self.timeout, 30)) as resp:
                resp.read()
        except urllib.error.HTTPError as err:
            err.close()  # an HTTPError holds the response socket open until closed
            raise JasnaError(f"/open returned HTTP {err.code}") from err
        except (urllib.error.URLError, OSError) as err:
            raise JasnaError(f"/open failed: {err}") from err

    def stop(self) -> bool:
        """Close the open stream: DELETE /open (Jasna 0.11+), falling back to
        POST /stop (0.10) when the server does not know the new route."""
        try:
            with self._request("DELETE", "/open") as resp:
                resp.read()
            return True
        except urllib.error.HTTPError as err:
            err.close()
            if err.code not in (404, 405, 501):
                log.warning("DELETE /open failed: HTTP %s", err.code)
                return False
        except (urllib.error.URLError, OSError) as err:
            log.warning("DELETE /open failed: %s", err)
            return False
        try:
            with self._request("POST", "/stop", b"{}") as resp:
                resp.read()
            return True
        except urllib.error.HTTPError as err:
            err.close()
            log.warning("/stop failed: HTTP %s", err.code)
            return False
        except (urllib.error.URLError, OSError) as err:
            log.warning("/stop failed: %s", err)
            return False

    def playlist(self) -> bytes | None:
        """The manifest bytes, or None if Jasna has no stream open."""
        try:
            with self._request("GET", "/stream.m3u8", timeout=5.0) as resp:
                data = resp.read()
            m = _EPOCH_RE.search(data)
            self.epoch = m.group(1).decode() if m else None
            return data
        except urllib.error.HTTPError as err:
            err.close()  # wait_ready() polls this every 0.25s; each 404 leaked a socket
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
        """Open a GET for a segment. The caller must close the response;
        closing it also closes the connection (HTTPResponse.close() alone
        leaves an HTTP/1.1 connection's socket open: one leak per segment)."""
        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        try:
            conn.request("GET", "/" + name + (f"?epoch={self.epoch}" if self.epoch else ""))
            resp = conn.getresponse()
        except BaseException:
            conn.close()
            raise
        close_resp = resp.close

        def close() -> None:
            try:
                close_resp()
            finally:
                conn.close()

        resp.close = close
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
        self.running_flags: list[str] | None = None  # so a custom preset re-sent with new flags restarts
        self.started_at: float | None = None
        self.prewarmed = False
        self._lock = threading.RLock()

    @property
    def managed(self) -> bool:
        return bool(self.cfg.manage_process)

    def alive(self) -> bool:
        if not self.managed:
            return self.client.status() is not None
        # No lock: _start() holds it for the whole startup (up to
        # start_timeout_s plus prewarm), and the reaper asks this while holding
        # the session lock, which used to freeze every heartbeat behind a cold
        # start. Reading one reference is atomic; poll() is safe concurrently.
        proc = self.proc
        return proc is not None and proc.poll() is None

    def pid(self) -> int | None:
        proc = self.proc  # no lock, see alive()
        return proc.pid if proc is not None and proc.poll() is None else None

    def matches(self, preset: str) -> bool:
        """True if the running process was started for `preset` with the flags
        that name currently resolves to (a custom preset can be re-sent with
        different flags under the same name)."""
        with self._lock:
            return (self.running_preset == preset
                    and self.running_flags == list(self.cfg.preset(preset).flags))

    def command(self, preset: str) -> list[str]:
        p = self.cfg.preset(preset)
        flags = [*self.cfg.jasna_common_flags, *p.flags]
        return [
            self.cfg.jasna_binary, "--stream", "--no-browser",
            "--stream-port", str(self.cfg.jasna_stream_port),
            *flags, *self._license_flags(flags),
        ]

    def license_flags_supported(self) -> bool:
        """True if the configured Jasna takes --license-email/--license-key
        (0.10 and earlier), False for 0.11+, which activates per machine.
        Decided once per process: from the version in the binary's path, else
        from a `--help` probe; a binary that answers neither way is assumed
        old (the behaviour before 0.11 existed)."""
        cached = getattr(self, "_license_flags_supported", None)
        if cached is not None:
            return cached
        version = jasna_version_from_path(self.cfg.jasna_binary)
        if version is not None:
            supported = version < LICENSE_FLAGS_DROPPED_IN
            how = "version %s from the binary path" % ".".join(map(str, version))
        else:
            probe = jasna_accepts_license_flags(self.cfg.jasna_binary)
            supported = True if probe is None else probe
            how = "--help probe" if probe is not None else "no version in the path and --help was inconclusive; assuming the pre-0.11 flags"
        log.info("Jasna %s --license-email/--license-key (%s)", "takes" if supported else "does not take", how)
        self._license_flags_supported = supported
        return supported

    def _license_flags(self, existing: list[str]) -> list[str]:
        """--license-email/--license-key read from Jasna's own store, so the
        key need not be duplicated in bridge.toml. Skipped if already supplied
        in the flags, if the store is missing/unreadable, or if this Jasna is
        0.11+ (activated per machine; the flags would abort it). Cached; logged once."""
        if "--license-email" in existing or "--license-key" in existing:
            if not getattr(self, "_warned_manual_license", False) and not self.license_flags_supported():
                self._warned_manual_license = True
                log.warning("common_flags/preset flags carry --license-email/--license-key, which this Jasna "
                            "(0.11+) rejects at startup; remove them and activate Jasna once with "
                            "`jasna --activation-key ...` (or --license-code)")
            return []
        cached = getattr(self, "_license_cache", "unset")
        if cached != "unset":
            return cached
        if self.cfg.jasna_license_file:
            paths = [os.path.expanduser(self.cfg.jasna_license_file)]
        else:
            paths = jasna_license_candidates()
        flags: list[str] = []
        found = None
        for path in paths:
            try:
                with open(path) as fh:
                    data = json.load(fh)
            except FileNotFoundError:
                continue
            except (OSError, ValueError) as err:
                log.warning("cannot read Jasna license %s: %s", path, err)
                continue
            found = path
            email, key = data.get("email"), data.get("key")
            if not self.license_flags_supported():
                # 0.11+ keeps its per-machine activation in this same file, without
                # email/key; nothing to pass, and no warning: a missing activation
                # shows up in Jasna's own log ("unet-4x is a supporter feature").
                log.info("Jasna 0.11+ activates per machine (%s); run `jasna --activation-key ...` "
                         "once if supporter models (unet-4x) are missing", path)
            elif email and key:
                flags = ["--license-email", str(email), "--license-key", str(key)]
                log.info("using Jasna license for %s from %s", email, path)
            else:
                log.warning("license file %s has no email/key; unet-4x will be disabled", path)
            break
        if found is None:
            log.info("no Jasna license file found (looked in %s); running without a license",
                     ", ".join(paths))
        self._license_cache = flags
        return flags

    def ensure(self, preset: str) -> bool:
        """Make sure a Jasna serving `preset` is up. Returns True if this call
        had to (re)start the process (a cold start)."""
        if not self.managed:
            if self.client.status() is None:
                raise JasnaError(f"Jasna is not reachable at {self.client.base_url}")
            return False
        with self._lock:
            if self.proc is not None and self.proc.poll() is None:
                if self.matches(preset):
                    if self.responsive():
                        return False
                    # Seen 2026-09-08: an ffmpeg child wedged, Jasna's single-threaded
                    # HTTP server stopped accepting, and every request hung. Treat an
                    # unanswered /status as dead and restart rather than block callers.
                    log.warning("Jasna pid %s is alive but not answering /status; restarting", self.proc.pid)
                    self.stop(graceful=False)  # wedged: no point waiting on the broken teardown
                else:
                    log.info("preset change %s -> %s: restarting Jasna", self.running_preset, preset)
                    # A preset change kills a running pass; its graceful teardown blocks
                    # ~30s (the 2026-09-09 finding), so kill outright for a snappy switch.
                    self.stop(graceful=False)
            elif self.proc is not None:
                log.warning("Jasna exited with code %s; restarting", self.proc.returncode)
                self.proc = None
            self._start(preset)
            return True

    def _start(self, preset: str) -> None:
        cmd = self.command(preset)
        log.info("starting Jasna: %s", " ".join(cmd[:6]) + (" ..." if len(cmd) > 6 else ""))
        cwd = self.cfg.jasna_workdir or os.path.dirname(os.path.abspath(self.cfg.jasna_binary)) or None
        out = None
        if self.cfg.jasna_log_file:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.cfg.jasna_log_file)), exist_ok=True)
                out = open(self.cfg.jasna_log_file, "ab", buffering=0)
                out.write(f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S')} start preset={preset} =====\n".encode())
            except OSError as err:
                log.warning("cannot open jasna log_file %s: %s", self.cfg.jasna_log_file, err)
                out = None
        try:
            self.proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, start_new_session=True, cwd=cwd,
                                         stdout=out, stderr=subprocess.STDOUT if out else None)
        except OSError as err:
            raise JasnaError(f"could not start Jasna: {err}") from err
        finally:
            if out is not None:
                out.close()  # the child holds its own descriptor
        self.running_preset = preset
        self.running_flags = list(self.cfg.preset(preset).flags)
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

    def responsive(self, timeout: float = 5.0) -> bool:
        """True if Jasna answers /status within `timeout`."""
        return self.client.status(timeout=timeout) is not None

    def stop(self, graceful: bool = True) -> None:
        """Stop the process group. graceful=True gives Jasna a SIGTERM and 20s
        to exit on its own (used for idle/preset-change stops). graceful=False
        goes straight to SIGKILL: on the recovery paths Jasna is wedged or its
        render pass is stalled, and its graceful shutdown is exactly the broken
        thing (a cancelled pass blocks its pipeline threads ~30s each; seen
        2026-09-09), so waiting on SIGTERM just adds that delay to recovery."""
        with self._lock:
            proc = self.proc
            if proc is None:
                return
            if proc.poll() is None:
                if graceful:
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
                else:
                    log.info("killing Jasna pid %s (fast recovery, no graceful wait)", proc.pid)
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
            # Jasna's ffmpeg children share the session/group and have been seen
            # to survive SIGTERM (systemd had to SIGKILL "vf#0:0" on unit stop).
            # The main process is gone by now; sweep the rest of the group.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            self.proc = None
            self.running_preset = None
            self.running_flags = None
            self.started_at = None
