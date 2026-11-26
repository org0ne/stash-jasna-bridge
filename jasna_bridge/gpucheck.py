"""Does the GPU still run work? Asked before every batch job.

nvidia-smi only reads driver counters, and it kept answering for two
minutes after the GPU locked up on 2026-11-25 (Xid 38 at the end of one job,
then the RC watchdog's "GPU is probably locked!"): the next job was started
into the dead card and hung in the kernel for 12 hours. The probe does what
that Jasna did first, through the CUDA driver API via ctypes: create a
context, set 1 MiB of device memory and wait for the GPU to finish. A locked
GPU fails it or never answers.

The probe runs in its own process, so a call that hangs in the kernel only
strands that process, never the bridge. A stranded probe cannot be killed
(SIGKILL stays pending, as with Jasna); it is reaped if it ever exits.
"""
from __future__ import annotations

import logging
import subprocess
import sys
import threading

log = logging.getLogger("bridge.gpucheck")

PROBE = r"""
import ctypes, sys
try:
    cu = ctypes.CDLL("libcuda.so.1")
except OSError as err:
    print(f"libcuda.so.1 not loadable: {err}"); sys.exit(3)
def check(name, *args):
    rc = getattr(cu, name)(*args)
    if rc:
        text = ctypes.c_char_p()
        cu.cuGetErrorName(rc, ctypes.byref(text))
        print(f"{name} failed: {(text.value or b'').decode() or rc}"); sys.exit(2)
dev, ctx, ptr = ctypes.c_int(), ctypes.c_void_p(), ctypes.c_uint64()
check("cuInit", 0)
check("cuDeviceGet", ctypes.byref(dev), 0)
check("cuCtxCreate_v2", ctypes.byref(ctx), 0, dev)
check("cuMemAlloc_v2", ctypes.byref(ptr), ctypes.c_size_t(1 << 20))
check("cuMemsetD8_v2", ptr, ctypes.c_ubyte(0x5A), ctypes.c_size_t(1 << 20))
check("cuCtxSynchronize")
check("cuMemFree_v2", ptr)
check("cuCtxDestroy_v2", ctx)
print("ok")
"""


def probe(timeout_s: float = 30.0) -> tuple[bool, str]:
    """(True, "ok") when the GPU ran the probe in time, else (False, why)."""
    try:
        proc = subprocess.Popen([sys.executable, "-I", "-c", PROBE], stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, start_new_session=True)
    except OSError as err:
        return False, f"could not start the GPU probe: {err}"
    try:
        out, _ = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        # Never wait on it here: a probe stuck in the kernel does not die.
        threading.Thread(target=proc.wait, name="gpu-probe-reaper", daemon=True).start()
        return False, (f"the GPU did not finish a 1 MiB CUDA memset within {timeout_s:g}s "
                       f"(GPU locked? see `journalctl -k` for Xid / RC watchdog lines)")
    text = (out or "").strip().splitlines()
    last = text[-1] if text else ""
    if proc.returncode == 0 and last == "ok":
        return True, "ok"
    return False, f"CUDA probe failed (exit {proc.returncode}): {last or 'no output'}"
