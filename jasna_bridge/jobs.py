"""Batch job queue: offline `jasna --input --output` runs, one at a time, on
the GPU the stream shares.

A job is submitted by the Stash jasna-batch plugin with the source file,
the preset's flags and where the finished file must land. The worker waits
until no live session holds the GPU (plus `batch.grace_s` after the last
one ended), stops the stream process, runs Jasna with its output in the
local scratch directory, moves the finished file to the output path, and
tells Stash by running the plugin's Sync task. While a job runs,
POST /session answers 409 {"reason": "batch"}.

Jobs are persisted to a JSON file after every change, so a bridge restart
keeps the queue; a job that was running at the restart is re-queued (its
partial output is discarded) up to `batch.max_attempts` times.
"""
from __future__ import annotations

import json
import logging
import os
import re
import secrets
import shutil
import signal
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field

from .config import validate_custom_preset
from .stash import StashError

log = logging.getLogger("bridge.jobs")

STATES = ("queued", "running", "done", "failed", "cancelled")
ACTIVE = ("queued", "running")
OUTPUT_EXT = ".mp4"


class JobError(Exception):
    """A submit that cannot be accepted; `status` is the HTTP code to answer."""

    def __init__(self, message: str, status: int = 400, **extra):
        super().__init__(message)
        self.status = status
        self.extra = extra


@dataclass
class Job:
    id: str
    scene_id: str
    stash_source: str      # the file as Stash sees it
    source: str            # the same file as Jasna sees it ([[paths.map]] applied)
    stash_output: str      # where the finished file goes, Stash-side
    output: str            # ... and Jasna-side
    preset: str
    flags: list[str] = field(default_factory=list)
    state: str = "queued"
    client: str = ""
    created: float = field(default_factory=time.time)
    started: float | None = None
    finished: float | None = None
    duration_s: float | None = None
    exit_code: int | None = None
    error: str = ""
    command: list[str] = field(default_factory=list)
    jasna_version: str = ""
    source_bytes: int | None = None
    output_bytes: int | None = None
    log_file: str = ""
    attempts: int = 0
    ingested: bool = False   # the plugin acknowledged it (copied sidecars, scanned, ...)
    result_scene_id: str | None = None   # the Stash scene made from the output, as the plugin reported it on ack
    notes: list[str] = field(default_factory=list)
    pid: int | None = None   # Jasna's pid while running (so a restarted bridge can adopt it)
    # Parsed from Jasna's progress bar in the log: {"percent", "eta_s", "rate", "detail", "updated"}
    progress: dict | None = None

    def public(self) -> dict:
        d = asdict(self)
        d["running_s"] = round(time.time() - self.started, 1) if self.state == "running" and self.started else None
        return d

    def brief(self) -> dict:
        return {"id": self.id, "scene_id": self.scene_id, "preset": self.preset, "state": self.state,
                "started": self.started, "running_s": round(time.time() - self.started, 1) if self.started and self.state == "running" else None,
                "source": os.path.basename(self.stash_source)}


class JobQueue:
    POLL_S = 2.0

    def __init__(self, cfg, sessions, stash, map_path):
        self.cfg = cfg
        self.sessions = sessions
        self.stash = stash
        self.map_path = map_path
        self.scratch_dir = os.path.expanduser(cfg.batch_scratch_dir) or os.path.expanduser(
            "~/.cache/stash-jasna-bridge/batch")
        self.state_file = os.path.expanduser(cfg.batch_state_file) or os.path.join(self.scratch_dir, "jobs.json")
        self.log_dir = os.path.join(self.scratch_dir, "logs")
        self.jobs: dict[str, Job] = {}
        self.lock = threading.RLock()
        self.current: Job | None = None
        self.proc: subprocess.Popen | None = None
        self._cancel_requested: set[str] = set()
        self._adopt: list[tuple[Job, int]] = []   # running jobs found alive at start, (job, pid)
        self._save_lock = threading.Lock()        # one writer of the state file at a time
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._worker = threading.Thread(target=self._loop, name="batch", daemon=True)
        self.stats = {"submitted": 0, "completed": 0, "failed": 0, "cancelled": 0, "notify_failures": 0}

    # ----- lifecycle -----
    def start(self) -> None:
        os.makedirs(self.log_dir, exist_ok=True)
        self._load()
        for job, pid in self._adopt:
            threading.Thread(target=self._adopt_running, args=(job, pid), name=f"adopt-{job.id}", daemon=True).start()
        self._worker.start()
        log.info("batch queue on (%d queued, %d adopted), scratch %s", len(self.queued()), len(self._adopt),
                 self.scratch_dir)

    def shutdown(self) -> None:
        self._stop.set()
        self._wake.set()
        with self.lock:
            job, proc = self.current, self.proc
        if proc is not None and proc.poll() is None:
            if self.cfg.batch_keep_running_on_restart:
                log.info("job %s: bridge shutting down; Jasna (pid %s) keeps running and the next bridge start "
                         "adopts it", job.id, proc.pid)
            else:
                log.info("job %s: bridge shutting down, killing Jasna; the job is re-queued at the next start",
                         job.id)
                self._kill(proc)
        self._worker.join(timeout=15)

    # ----- persistence -----
    def _load(self) -> None:
        try:
            with open(self.state_file) as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return
        except (OSError, ValueError) as err:
            log.warning("cannot read %s: %s (starting with an empty queue)", self.state_file, err)
            return
        fields = {f for f in Job.__dataclass_fields__}
        for raw in data.get("jobs", []):
            try:
                job = Job(**{k: v for k, v in raw.items() if k in fields})
            except TypeError as err:
                log.warning("skipping unreadable job record: %s", err)
                continue
            if job.state == "running":
                pid = job.pid if job.pid and self._is_job_process(job.pid, job) else self._find_job_pid(job)
                if pid:
                    # Still running from the previous bridge: watch it to the end.
                    job.pid = pid
                    job.notes.append("bridge restarted while running; adopted")
                    self._adopt.append((job, pid))
                    self.jobs[job.id] = job
                    continue
                # Interrupted by the last shutdown. Its scratch output is
                # incomplete; run it again, or give up after max_attempts.
                if job.attempts >= self.cfg.batch_max_attempts:
                    job.state, job.finished = "failed", time.time()
                    job.error = f"interrupted by a bridge restart {job.attempts} time(s); giving up"
                else:
                    job.state, job.started = "queued", None
                    job.notes.append("interrupted by a bridge restart; re-queued")
                self._discard_scratch(job)
            self.jobs[job.id] = job
        self.stats.update({k: v for k, v in data.get("stats", {}).items() if k in self.stats})

    def _save(self) -> None:
        with self.lock:
            finished = [j for j in self.jobs.values() if j.state not in ACTIVE]
            if len(finished) > self.cfg.batch_keep_jobs:
                # Oldest acknowledged first, then oldest of the rest.
                finished.sort(key=lambda j: (not j.ingested, j.finished or j.created))
                for j in finished[: len(finished) - self.cfg.batch_keep_jobs]:
                    del self.jobs[j.id]
            data = {"jobs": [asdict(j) for j in self.jobs.values()], "stats": dict(self.stats)}
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        tmp = self.state_file + ".tmp"
        with self._save_lock:
            with open(tmp, "w") as fh:
                json.dump(data, fh, indent=1)
            os.replace(tmp, self.state_file)

    # ----- queries -----
    def get(self, job_id: str) -> Job | None:
        with self.lock:
            return self.jobs.get(job_id)

    def list(self, states: set[str] | None = None, unacked_only: bool = False) -> list[Job]:
        with self.lock:
            out = [j for j in self.jobs.values()
                   if (states is None or j.state in states) and (not unacked_only or not j.ingested)]
        return sorted(out, key=lambda j: j.created)

    def queued(self) -> list[Job]:
        return self.list({"queued"})

    def snapshot(self) -> dict:
        with self.lock:
            cur = self.current
            return {
                "enabled": True,
                "running": cur.brief() if cur else None,
                "queued": len([j for j in self.jobs.values() if j.state == "queued"]),
                "unacknowledged": len([j for j in self.jobs.values()
                                       if j.state in ("done", "failed") and not j.ingested]),
                "scratch_dir": self.scratch_dir,
                "scratch_free_gb": _free_gb(self.scratch_dir),
                "stats": dict(self.stats),
            }

    # ----- commands -----
    def submit(self, scene_id: str, stash_source: str, stash_output: str, preset: str,
               flags: list[str], client: str) -> Job:
        if not stash_source or not stash_output:
            raise JobError("file_path and output_path are required")
        if not os.path.isabs(stash_output) or not stash_output.lower().endswith(OUTPUT_EXT):
            raise JobError(f"output_path must be an absolute path ending in {OUTPUT_EXT}")
        roots = self.cfg.batch_output_roots
        if roots and not any(stash_output.startswith(r + "/") for r in roots):
            raise JobError(f"output_path must be under one of {roots}", 403)
        try:
            validate_custom_preset(preset, flags)
        except ValueError as err:
            raise JobError(str(err))
        try:
            files = self.stash.scene_files(scene_id)
        except StashError as err:
            raise JobError(str(err), 404)
        if stash_source not in files:
            raise JobError(f"file_path is not one of scene {scene_id}'s files", 404)
        source = self.map_path(stash_source)
        if not os.path.isfile(source):
            raise JobError(f"source file not visible to the bridge: {source} (check [[paths.map]])", 404)
        output = self.map_path(stash_output)
        with self.lock:
            for j in self.jobs.values():
                if j.state in ACTIVE and j.source == source and j.preset == preset:
                    raise JobError("an identical job is already queued or running", 409, job=j.public())
            job = Job(id=secrets.token_hex(6), scene_id=str(scene_id), stash_source=stash_source, source=source,
                      stash_output=stash_output, output=output, preset=preset, flags=list(flags), client=client,
                      source_bytes=os.path.getsize(source))
            self.jobs[job.id] = job
            self.stats["submitted"] += 1
        self._save()
        log.info("job %s: queued scene %s preset %s (%s) for %s", job.id, scene_id, preset,
                 os.path.basename(source), client)
        self._wake.set()
        return job

    def cancel(self, job_id: str) -> Job:
        with self.lock:
            job = self.jobs.get(job_id)
            if job is None:
                raise JobError("no such job", 404)
            if job.state == "queued":
                job.state, job.finished = "cancelled", time.time()
                self.stats["cancelled"] += 1
                self._save()
                log.info("job %s: cancelled while queued", job.id)
                self._notify(job, "cancelled")
                return job
            if job.state == "running":
                self._cancel_requested.add(job.id)
                proc = self.proc
                if proc is not None and proc.poll() is None:
                    log.info("job %s: cancel requested, killing Jasna", job.id)
                    self._kill(proc)
                return job
            raise JobError(f"job is already {job.state}", 409)

    def ack(self, job_id: str, result_scene_id: str | None = None) -> Job:
        """The plugin has dealt with a finished job; result_scene_id is the
        new Stash scene when it made one. Repeating an ack is fine (it can
        add the scene id later)."""
        with self.lock:
            job = self.jobs.get(job_id)
            if job is None:
                raise JobError("no such job", 404)
            if job.state in ACTIVE:
                raise JobError(f"job is still {job.state}", 409)
            job.ingested = True
            if result_scene_id:
                job.result_scene_id = str(result_scene_id)
        self._save()
        return job

    def check(self) -> dict:
        """What the plugin's Check Settings task shows: is this bridge able
        to run a batch job right now. Read-only apart from creating the
        scratch directory."""
        cfg = self.cfg
        checks: list[dict] = []

        def add(label: str, ok: bool, detail: str = "") -> None:
            checks.append({"label": label, "ok": bool(ok), "detail": detail})

        binary = cfg.jasna_binary
        add("Jasna binary", bool(binary) and os.path.isfile(binary) and os.access(binary, os.X_OK), binary)
        version = self.sessions.procs.version_string()
        add("Jasna version", bool(version), version or "could not determine (path has no version, --version failed)")
        models: list[str] = []
        weights = os.path.join(os.path.dirname(os.path.abspath(binary)), "model_weights") if binary else ""
        if weights and os.path.isdir(weights):
            try:
                models = sorted(os.listdir(weights))
            except OSError:
                models = []
            add("model_weights", True, f"{len(models)} entries")
            add("LTX model (ltx-restore)", "ltx-restore" in models,
                "present" if "ltx-restore" in models else "missing: an LTX preset would stop at Jasna's download prompt")
        else:
            add("model_weights", False, f"{weights or '?'} not found")
        activation = _activation_file()
        add("Jasna activation", activation is not None,
            activation or "no license.json in Jasna's config dir; supporter models (unet-4x) will be refused")
        try:
            os.makedirs(self.scratch_dir, exist_ok=True)
            free = _free_gb(self.scratch_dir)
            add("scratch dir writable", os.access(self.scratch_dir, os.W_OK), self.scratch_dir)
            add("scratch free space", free is not None and free >= cfg.batch_min_free_gb,
                f"{free} GB free, need {cfg.batch_min_free_gb:g} GB" if free is not None else "unknown")
        except OSError as err:
            add("scratch dir", False, f"{self.scratch_dir}: {err}")
        for root in cfg.batch_output_roots:
            mapped = self.map_path(root)
            add(f"output root {root}", os.path.isdir(mapped) and os.access(mapped, os.W_OK),
                "writable" if os.path.isdir(mapped) and os.access(mapped, os.W_OK) else f"{mapped} not a writable directory")
        gpu = shutil.which("nvidia-smi")
        if gpu:
            try:
                name = subprocess.run([gpu, "--query-gpu=name,memory.used,memory.total", "--format=csv,noheader"],
                                      capture_output=True, text=True, timeout=10).stdout.strip().splitlines()
                add("NVIDIA GPU", bool(name), name[0] if name else "nvidia-smi listed no GPU")
            except (OSError, subprocess.SubprocessError):
                add("NVIDIA GPU", False, "nvidia-smi did not answer")
        else:
            add("NVIDIA GPU", False, "nvidia-smi not found")
        try:
            self.stash.graphql("{ version { version } }")
            add("Stash reachable from the bridge", True, cfg.stash_url)
        except StashError as err:
            add("Stash reachable from the bridge", False, f"{cfg.stash_url}: {err}")
        add("Stash callback", bool(cfg.batch_notify_plugin and cfg.batch_notify_task),
            f"{cfg.batch_notify_plugin} / {cfg.batch_notify_task}" if cfg.batch_notify_task else "off: the plugin's Sync task must be run by hand")
        return {"ok": all(c["ok"] for c in checks), "checks": checks, "jasna_version": version, "models": models,
                **self.snapshot()}

    def log_tail(self, job_id: str, limit: int = 65536) -> str:
        job = self.get(job_id)
        if job is None:
            raise JobError("no such job", 404)
        if not job.log_file:
            return ""
        try:
            size = os.path.getsize(job.log_file)
            with open(job.log_file, "rb") as fh:
                if size > limit:
                    fh.seek(size - limit)
                return fh.read().decode(errors="replace")
        except OSError:
            return ""

    # ----- worker -----
    def _loop(self) -> None:
        while not self._stop.is_set():
            job = None
            with self.lock:
                for j in self.jobs.values():
                    if j.state == "queued":
                        job = j
                        break
            if job is None or not self._acquire_gpu(job):
                self._wake.wait(self.POLL_S)
                self._wake.clear()
                continue
            try:
                self._run(job)
            except Exception as err:  # never let the worker die
                log.exception("job %s: unexpected error", job.id)
                self._finish(job, "failed", f"bridge error: {err}")
            finally:
                self.sessions.batch_release()

    def _acquire_gpu(self, job: Job) -> bool:
        return self.sessions.batch_acquire(job.brief(), self.cfg.batch_grace_s)

    def _run(self, job: Job) -> None:
        cfg = self.cfg
        out_tmp = os.path.join(self.scratch_dir, f"{job.id}{OUTPUT_EXT}")
        work = os.path.join(self.scratch_dir, f"work-{job.id}")
        job.log_file = os.path.join(self.log_dir, f"{job.id}.log")
        cmd = [cfg.jasna_binary, "--input", job.source, "--output", out_tmp, "--working-directory", work,
               *([] if cfg.batch_progress else ["--no-progress"]),
               *cfg.jasna_common_flags, *cfg.batch_common_flags, *job.flags]
        with self.lock:
            job.state, job.started, job.attempts = "running", time.time(), job.attempts + 1
            job.command, job.error, job.exit_code, job.progress, job.pid = cmd, "", None, None, None
            job.jasna_version = self.sessions.procs.version_string() if hasattr(self.sessions.procs, "version_string") else ""
            self.current = job
        self._save()
        self._notify(job, "started")
        log.info("job %s: starting (attempt %d): %s", job.id, job.attempts, " ".join(cmd[:5]) + " ...")

        problem = self._preflight(job, out_tmp)
        if problem:
            return self._finish(job, "failed", problem)
        os.makedirs(work, exist_ok=True)
        cwd = cfg.jasna_workdir or os.path.dirname(os.path.abspath(cfg.jasna_binary)) or None
        try:
            with open(job.log_file, "ab", buffering=0) as logfh:
                logfh.write(f"===== {time.strftime('%Y-%m-%d %H:%M:%S')} job {job.id} scene {job.scene_id} "
                            f"preset {job.preset}\n{' '.join(cmd)}\n\n".encode())
                proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=logfh, stderr=subprocess.STDOUT,
                                        start_new_session=True, cwd=cwd)
        except OSError as err:
            return self._finish(job, "failed", f"could not start Jasna: {err}")
        with self.lock:
            self.proc = proc
            job.pid = proc.pid
        self._save()
        deadline = time.monotonic() + cfg.batch_job_timeout_h * 3600 if cfg.batch_job_timeout_h else None
        timed_out = False
        last_progress = 0.0
        while proc.poll() is None:
            if self._stop.is_set() and cfg.batch_keep_running_on_restart:
                return  # shutdown: Jasna runs on, the record stays "running" with its pid; adopted next start
            if deadline and time.monotonic() > deadline:
                timed_out = True
                log.warning("job %s: over batch.job_timeout_h (%.1fh), killing", job.id, cfg.batch_job_timeout_h)
                self._kill(proc)
                break
            if time.monotonic() - last_progress >= self.PROGRESS_POLL_S:
                last_progress = time.monotonic()
                self._update_progress(job)
            time.sleep(0.5)
        proc.wait()
        with self.lock:
            self.proc = None
            cancelled = job.id in self._cancel_requested
            self._cancel_requested.discard(job.id)
        job.exit_code = proc.returncode
        shutil.rmtree(work, ignore_errors=True)
        if cancelled:
            self._discard(out_tmp)
            return self._finish(job, "cancelled", "cancelled while running")
        if timed_out:
            self._discard(out_tmp)
            return self._finish(job, "failed", f"killed after batch.job_timeout_h ({cfg.batch_job_timeout_h}h)")
        if self._stop.is_set() and not cfg.batch_keep_running_on_restart:
            return  # shutdown killed it: left as "running" on disk, re-queued at the next start
        if proc.returncode != 0:
            self._discard(out_tmp)
            return self._finish(job, "failed", f"Jasna exited with code {proc.returncode} (see the job log)")
        self._finalize_output(job, out_tmp)

    PROGRESS_POLL_S = 3.0

    def _finalize_output(self, job: Job, out_tmp: str) -> None:
        """Jasna is gone without being killed: move a complete output into
        place, or fail the job."""
        self._update_progress(job)
        if not os.path.isfile(out_tmp) or os.path.getsize(out_tmp) == 0:
            self._discard(out_tmp)
            return self._finish(job, "failed", "Jasna exited but wrote no output file")
        job.output_bytes = os.path.getsize(out_tmp)
        if os.path.exists(job.output):
            # The plugin checks this before submitting; a file appearing
            # meanwhile is not ours to replace. Keep the result where a human
            # can find it.
            aside = os.path.join(self.scratch_dir, f"{job.id}-unmoved{OUTPUT_EXT}")
            os.replace(out_tmp, aside)
            return self._finish(job, "failed", f"output already exists: {job.output}; result left at {aside}")
        try:
            os.makedirs(os.path.dirname(job.output), exist_ok=True)
            moved = _move(out_tmp, job.output)
        except OSError as err:
            self._discard(out_tmp)
            return self._finish(job, "failed", f"could not move the output to {job.output}: {err}")
        if not moved:
            return self._finish(job, "failed", f"output copy to {job.output} came up short; result left at {out_tmp}")
        self._finish(job, "done", "")

    def _adopt_running(self, job: Job, pid: int) -> None:
        """Watch a Jasna left running by the previous bridge until it exits,
        then finish the job from its output file (the exit code of a
        process that is not our child cannot be read)."""
        out_tmp = os.path.join(self.scratch_dir, f"{job.id}{OUTPUT_EXT}")
        work = os.path.join(self.scratch_dir, f"work-{job.id}")
        while not self.sessions.batch_acquire(job.brief(), 0.0):
            if self._stop.wait(1.0):
                return
        log.info("job %s: adopted running Jasna pid %s", job.id, pid)
        with self.lock:
            self.current = job
        self._save()
        try:
            last_progress = 0.0
            while self._is_job_process(pid, job):
                if self._stop.is_set():
                    return  # another restart: still running, adopted again next time
                if job.id in self._cancel_requested:
                    log.info("job %s: cancel requested on an adopted job, killing pid %s", job.id, pid)
                    for sig in (signal.SIGTERM, signal.SIGKILL):
                        try:
                            os.killpg(pid, sig)
                        except ProcessLookupError:
                            break
                        if not self._is_job_process(pid, job):
                            break
                        time.sleep(5)
                    break
                if time.monotonic() - last_progress >= self.PROGRESS_POLL_S:
                    last_progress = time.monotonic()
                    self._update_progress(job)
                time.sleep(1.0)
            with self.lock:
                cancelled = job.id in self._cancel_requested
                self._cancel_requested.discard(job.id)
            shutil.rmtree(work, ignore_errors=True)
            if cancelled:
                self._discard(out_tmp)
                return self._finish(job, "cancelled", "cancelled while running (adopted)")
            job.notes.append("finished under a restarted bridge; exit code unknown, judged by the output file")
            self._finalize_output(job, out_tmp)
        except Exception as err:
            log.exception("job %s: error while adopted", job.id)
            self._finish(job, "failed", f"bridge error while adopted: {err}")
        finally:
            self.sessions.batch_release()

    def _is_job_process(self, pid: int, job: Job) -> bool:
        """True if `pid` is alive and is this job's Jasna (its command line
        names the job's scratch output), so a reused pid is not mistaken."""
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                argv = fh.read().split(b"\0")
        except OSError:
            return False
        out_tmp = os.path.join(self.scratch_dir, f"{job.id}{OUTPUT_EXT}").encode()
        return out_tmp in argv

    def _find_job_pid(self, job: Job) -> int | None:
        """The pid of a Jasna writing this job's scratch output, if one is
        running (a record from an older bridge may lack the pid)."""
        try:
            pids = [int(d) for d in os.listdir("/proc") if d.isdigit()]
        except OSError:
            return None
        for pid in pids:
            if self._is_job_process(pid, job):
                return pid
        return None

    def _update_progress(self, job: Job) -> None:
        """Read Jasna's latest progress-bar line from the job log."""
        if not job.log_file:
            return
        try:
            size = os.path.getsize(job.log_file)
            with open(job.log_file, "rb") as fh:
                if size > 16384:
                    fh.seek(size - 16384)
                tail = fh.read().decode(errors="replace")
        except OSError:
            return
        parsed = parse_progress(tail)
        if parsed is not None:
            parsed["updated"] = time.time()
            with self.lock:
                job.progress = parsed

    def _preflight(self, job: Job, out_tmp: str) -> str:
        cfg = self.cfg
        if not os.path.isfile(job.source):
            return f"source file is gone: {job.source}"
        try:
            os.makedirs(self.scratch_dir, exist_ok=True)
        except OSError as err:
            return f"scratch dir {self.scratch_dir}: {err}"
        need = max(cfg.batch_min_free_gb, 2 * (job.source_bytes or 0) / 1e9)
        free = _free_gb(self.scratch_dir)
        if free is not None and free < need:
            return f"only {free:.1f} GB free on scratch {self.scratch_dir}; need {need:.1f} GB"
        out_dir = os.path.dirname(job.output)
        probe = out_dir
        while probe and not os.path.isdir(probe):
            probe = os.path.dirname(probe)
        if not probe:
            return f"output volume for {out_dir} does not exist"
        free = _free_gb(probe)
        if free is not None and free < need:
            return f"only {free:.1f} GB free under {probe} for the output; need {need:.1f} GB"
        self._discard(out_tmp)
        return ""

    def _finish(self, job: Job, state: str, error: str) -> None:
        with self.lock:
            job.state, job.finished, job.error = state, time.time(), error
            if job.started:
                job.duration_s = round(job.finished - job.started, 1)
            if self.current is job:
                self.current = None
            key = {"done": "completed", "failed": "failed", "cancelled": "cancelled"}[state]
            self.stats[key] += 1
        self._save()
        if state == "done":
            log.info("job %s: done in %.0fs, %s (%.2f GB)", job.id, job.duration_s or 0, job.output,
                     (job.output_bytes or 0) / 1e9)
        else:
            log.warning("job %s: %s: %s", job.id, state, error)
        self._notify(job, state)

    def _notify(self, job: Job, event: str) -> None:
        """Run the plugin's Sync task in Stash so the scene's status tag
        follows the job; a failure here is logged and the plugin's own task
        catches up later."""
        if not self.cfg.batch_notify_plugin or not self.cfg.batch_notify_task:
            return
        try:
            self.stash.run_plugin_task(self.cfg.batch_notify_plugin, self.cfg.batch_notify_task,
                                       {"job_id": job.id, "event": event})
        except StashError as err:
            with self.lock:
                self.stats["notify_failures"] += 1
            log.warning("job %s: could not run Stash task %s/%s: %s", job.id, self.cfg.batch_notify_plugin,
                        self.cfg.batch_notify_task, err)

    def _discard_scratch(self, job: Job) -> None:
        self._discard(os.path.join(self.scratch_dir, f"{job.id}{OUTPUT_EXT}"))
        shutil.rmtree(os.path.join(self.scratch_dir, f"work-{job.id}"), ignore_errors=True)

    @staticmethod
    def _discard(path: str) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as err:
            log.warning("could not remove %s: %s", path, err)

    @staticmethod
    def _kill(proc: subprocess.Popen) -> None:
        for sig, wait in ((signal.SIGTERM, 10), (signal.SIGKILL, 5)):
            try:
                os.killpg(proc.pid, sig)
            except ProcessLookupError:
                return
            try:
                proc.wait(timeout=wait)
                return
            except subprocess.TimeoutExpired:
                continue


_PERCENT_RE = re.compile(r"(\d{1,3})%")
_ETA_RE = re.compile(r"<\s*(\d+):(\d{2})(?::(\d{2}))?")
_RATE_RE = re.compile(r"([\d.]+)\s*(frames?/s|fps|it/s|s/it|steps?/s|s/step)")
# "LTX 4/4 compose:  33%|..." -> pass 4 of 4, stage "compose" (Jasna 0.11.3)
_PASS_RE = re.compile(r"(\d+)\s*/\s*(\d+)\s+([A-Za-z][\w-]*)")
# tqdm postfix Jasna adds on multi-pass runs: whole-run time remaining
_RUN_LEFT_RE = re.compile(r"run\s+(?:(\d+):)?(\d+):(\d{2})\s+left")


def _hms(h: str | None, m: str, s: str) -> int:
    return int(h or 0) * 3600 + int(m) * 60 + int(s)


def parse_progress(text: str) -> dict | None:
    """The last progress-bar update in a chunk of Jasna's output. tqdm
    rewrites its line with carriage returns, so the chunk is split on both
    CR and LF and the last piece with a percentage wins. Returns
    {"percent", "eta_s", "rate", "detail", "label", "pass", "passes", "stage",
    "run_left_s"} or None when there is none. percent and eta_s are for the
    current pass (the bar restarts per pass: an LTX run is "LTX 1/4 scan",
    "2/4 encode", "3/4 denoise", "4/4 compose"); run_left_s is Jasna's own
    whole-run estimate from the bar's postfix, when it prints one."""
    for piece in reversed(re.split(r"[\r\n]+", text)):
        m = _PERCENT_RE.search(piece)
        if not m or "%" not in piece:
            continue
        percent = min(100, int(m.group(1)))
        eta = _ETA_RE.search(piece)
        eta_s = None
        if eta:
            h, mnt, sec = eta.groups()
            eta_s = _hms(h, mnt, sec) if sec is not None else _hms(None, h, mnt)
        rate = _RATE_RE.search(piece)
        detail = piece[m.end():].strip(" |,")
        label = piece[:m.start()].split("|")[0].strip(" :|\t")   # drop a bar drawn before the percent
        passes = _PASS_RE.search(label)
        run_left = _RUN_LEFT_RE.search(piece)
        return {"percent": percent, "eta_s": eta_s, "rate": rate.group(0) if rate else None,
                "detail": detail[:120], "label": label[:60] or None,
                "pass": int(passes.group(1)) if passes else None,
                "passes": int(passes.group(2)) if passes else None,
                "stage": passes.group(3) if passes else None,
                "run_left_s": _hms(*run_left.groups()) if run_left else None}
    return None


def _activation_file() -> str | None:
    from .jasna import jasna_license_candidates
    for path in jasna_license_candidates():
        if os.path.isfile(path):
            return path
    return None


def _free_gb(path: str) -> float | None:
    try:
        st = os.statvfs(path)
    except OSError:
        return None
    return round(st.f_bavail * st.f_frsize / 1e9, 1)


def _move(src: str, dst: str) -> bool:
    """Move a finished output into place. Same volume: a rename. Otherwise a
    copy to a temporary name beside the destination, a size check, then a
    rename, so a reader never sees a half-written file at the final name."""
    try:
        os.rename(src, dst)
        return True
    except OSError:
        pass
    tmp = dst + ".part"
    shutil.copyfile(src, tmp)
    if os.path.getsize(tmp) != os.path.getsize(src):
        os.remove(tmp)
        return False
    os.replace(tmp, dst)
    os.remove(src)
    return True
