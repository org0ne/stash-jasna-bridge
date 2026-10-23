"""Whole-GPU load sampler for the queue page's sparkline.

A background thread runs `nvidia-smi` at a slow cadence (default 3 s) and
keeps a short ring buffer of utilization. The reading is whole-GPU: it sums
the batch job, the live stream and anything else on the card, so it is a
load gauge, not per-process accounting. One query reads driver counters
(~20 ms, no GPU compute). Where nvidia-smi is absent or fails the sampler
reports None and the page shows no graphic. Ported from the whisperjav_fleet
agent's sampler.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
import threading
import time

log = logging.getLogger("bridge.gpu")


class GpuSampler:
    def __init__(self, cfg):
        self.enabled = bool(cfg.gpu_sample)
        self.interval = max(0.1, float(cfg.gpu_sample_seconds))
        self.history = max(2, int(cfg.gpu_history))
        self.bin = (cfg.gpu_nvidia_smi_bin or "").strip() or shutil.which("nvidia-smi") or ""
        self.available = False
        self._util = 0
        self._mem_used = 0
        self._mem_total = 0
        self._name = ""
        self._hist: list[int] = []
        self._sampled_at = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._warned = False

    def start(self) -> None:
        if not (self.enabled and self.bin):
            if self.enabled:
                log.info("nvidia-smi not found; the queue page shows no GPU graphic")
            return
        threading.Thread(target=self._loop, name="gpu-sample", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def _query(self) -> tuple[int, int, int, str] | None:
        try:
            out = subprocess.run(
                [self.bin, "--query-gpu=utilization.gpu,memory.used,memory.total,name",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=8,
            )
            if out.returncode != 0:
                return None
            # First GPU only. "87, 11009, 24564, NVIDIA GeForce RTX 4090"
            first = out.stdout.strip().splitlines()[0]
            util, mem_used, mem_total, name = (p.strip() for p in first.split(",", 3))
            return int(util), int(mem_used), int(mem_total), name
        except Exception:
            return None

    def _loop(self) -> None:
        while not self._stop.is_set():
            row = self._query()
            with self._lock:
                if row is None:
                    self.available = False
                    if not self._warned:
                        log.warning("nvidia-smi query failed; hiding the GPU graphic until it answers again")
                        self._warned = True
                else:
                    util, mem_used, mem_total, name = row
                    self.available = True
                    self._warned = False
                    self._util, self._mem_used = util, mem_used
                    self._mem_total, self._name = mem_total, name
                    self._sampled_at = time.time()
                    self._hist.append(util)
                    if len(self._hist) > self.history:
                        del self._hist[:-self.history]
            self._stop.wait(self.interval)

    def snapshot(self) -> dict | None:
        """The `gpu` block of /health: util %, VRAM in MiB, name, the recent
        util history and the age of the sample; None when there is no GPU or
        no sample yet."""
        if not (self.enabled and self.bin):
            return None
        with self._lock:
            if not self.available or not self._hist:
                return None
            return {
                "util": self._util,
                "mem_used": self._mem_used,
                "mem_total": self._mem_total,
                "name": self._name,
                "history": list(self._hist),
                "interval_s": self.interval,
                "sampled_ago": round(time.time() - self._sampled_at, 1),
            }
