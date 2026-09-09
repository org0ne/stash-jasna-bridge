"""Phase C: on-disk segment cache.

Layout: <dir>/<key>/seg_NNNNN.ts plus <dir>/<key>/meta.json, where key is
sha256(path, preset name, preset flags, jasna version). Jasna's fixed 4s
segmentation keeps segment indices stable, so (key, segment name) is safe.

- A hit is served from disk and bumps the entry's LRU time.
- A miss is fetched from Jasna, stored atomically (tmp + rename), served.
- meta.json keeps Jasna's own manifest for the stream and the segment count
  it lists; when every segment is on disk the stream is "complete" and a new
  session for it is served entirely from cache (no Jasna, no GPU) until a
  miss - eviction can make holes - opens Jasna on demand.
- Eviction is LRU by total bytes; evicting from a complete stream makes it
  incomplete again.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import threading
import time

log = logging.getLogger("bridge.cache")

SEG_RE = re.compile(r"^seg_[0-9]{5}\.ts$")


def version_from_binary(binary: str) -> str:
    """'…/jasna-linux-nvidia-0.10.0/jasna' -> '0.10.0'; symlinks are resolved."""
    if not binary:
        return ""
    real = os.path.realpath(binary)
    m = re.search(r"(\d+\.\d+\.\d+)", real)
    return m.group(1) if m else ""


class SegmentCache:
    def __init__(self, cfg):
        self.dir = os.path.expanduser(cfg.cache_dir) or os.path.expanduser("~/.cache/stash-jasna-bridge/segments")
        self.max_bytes = int(cfg.cache_max_gb * 1_000_000_000)
        self.version = cfg.cache_version or version_from_binary(cfg.jasna_binary) or "v0"
        self.lock = threading.RLock()
        self.entries: dict[tuple[str, str], tuple[int, float]] = {}  # (key, seg) -> (bytes, last_used)
        self.segs: dict[str, set[str]] = {}                            # key -> cached segment names
        self.metas: dict[str, dict] = {}                               # key -> meta.json contents
        self.total = 0
        self.stats = {"hits": 0, "misses": 0, "stored": 0, "evictions": 0, "bytes_from_cache": 0}
        os.makedirs(self.dir, exist_ok=True)
        self._scan()
        log.info("segment cache at %s: %d segments, %.2f GB of %.2f GB, jasna version %s",
                 self.dir, len(self.entries), self.total / 1e9, self.max_bytes / 1e9, self.version)

    # ----- keys -----
    def key(self, path: str, preset: str, flags: list[str]) -> str:
        raw = json.dumps([path, preset, list(flags), self.version], ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def _key_dir(self, key: str) -> str:
        return os.path.join(self.dir, key)

    def file_path(self, key: str, seg: str) -> str:
        return os.path.join(self._key_dir(key), seg)

    # ----- startup -----
    def _scan(self) -> None:
        for key in os.listdir(self.dir):
            kd = self._key_dir(key)
            if not os.path.isdir(kd):
                continue
            meta_fn = os.path.join(kd, "meta.json")
            if os.path.exists(meta_fn):
                try:
                    with open(meta_fn) as fh:
                        self.metas[key] = json.load(fh)
                except (OSError, ValueError):
                    pass
            for name in os.listdir(kd):
                fn = os.path.join(kd, name)
                if name.endswith(".tmp"):
                    os.unlink(fn)
                    continue
                if not SEG_RE.match(name):
                    continue
                st = os.stat(fn)
                self.entries[(key, name)] = (st.st_size, st.st_mtime)
                self.segs.setdefault(key, set()).add(name)
                self.total += st.st_size
        for key in list(self.metas):
            self._refresh_complete(key, persist=False)

    # ----- manifest / completeness -----
    def manifest(self, key: str) -> bytes | None:
        with self.lock:
            meta = self.metas.get(key)
            return meta["playlist"].encode() if meta and meta.get("playlist") else None

    def store_manifest(self, key: str, path: str, preset: str, flags: list[str], playlist: bytes) -> None:
        text = playlist.decode(errors="replace")
        n = len([ln for ln in text.splitlines() if SEG_RE.match(ln.strip())])
        with self.lock:
            meta = self.metas.get(key) or {}
            if meta.get("playlist") == text:
                return
            meta.update({"path": path, "preset": preset, "flags": list(flags), "version": self.version,
                         "playlist": text, "segments": n, "stored_at": time.time()})
            self.metas[key] = meta
            self._refresh_complete(key, persist=True)

    def is_complete(self, key: str) -> bool:
        with self.lock:
            meta = self.metas.get(key)
            return bool(meta and meta.get("complete"))

    def _refresh_complete(self, key: str, persist: bool) -> None:
        meta = self.metas.get(key)
        if not meta:
            return
        n = meta.get("segments") or 0
        complete = n > 0 and len(self.segs.get(key, ())) >= n
        if meta.get("complete") != complete or persist:
            meta["complete"] = complete
            self._write_meta(key)

    def _write_meta(self, key: str) -> None:
        kd = self._key_dir(key)
        os.makedirs(kd, exist_ok=True)
        tmp = os.path.join(kd, "meta.json.tmp")
        try:
            with open(tmp, "w") as fh:
                json.dump(self.metas[key], fh)
            os.replace(tmp, os.path.join(kd, "meta.json"))
        except OSError as err:
            log.warning("cannot write %s: %s", kd, err)

    # ----- segments -----
    def hit(self, key: str, seg: str) -> str | None:
        """Path of a cached segment (LRU time bumped), or None."""
        with self.lock:
            entry = self.entries.get((key, seg))
            if entry is None:
                self.stats["misses"] += 1
                return None
            fn = self.file_path(key, seg)
            if not os.path.exists(fn):  # removed behind our back
                self._forget(key, seg)
                self.stats["misses"] += 1
                return None
            self.entries[(key, seg)] = (entry[0], time.time())
            self.stats["hits"] += 1
            self.stats["bytes_from_cache"] += entry[0]
            return fn

    def put(self, key: str, seg: str, data: bytes) -> None:
        if not SEG_RE.match(seg) or not data:
            return
        kd = self._key_dir(key)
        fn = self.file_path(key, seg)
        tmp = fn + ".tmp"
        try:
            os.makedirs(kd, exist_ok=True)
            with open(tmp, "wb") as fh:
                fh.write(data)
            os.replace(tmp, fn)
        except OSError as err:
            log.warning("cannot cache %s/%s: %s", key, seg, err)
            try:
                os.unlink(tmp)
            except OSError:
                pass
            return
        with self.lock:
            old = self.entries.get((key, seg))
            if old:
                self.total -= old[0]
            self.entries[(key, seg)] = (len(data), time.time())
            self.segs.setdefault(key, set()).add(seg)
            self.total += len(data)
            self.stats["stored"] += 1
            self._evict()
            self._refresh_complete(key, persist=False)

    def _forget(self, key: str, seg: str) -> None:
        entry = self.entries.pop((key, seg), None)
        if entry:
            self.total -= entry[0]
        self.segs.get(key, set()).discard(seg)

    def _evict(self) -> None:
        if self.total <= self.max_bytes:
            return
        touched: set[str] = set()
        for (key, seg), (size, _) in sorted(self.entries.items(), key=lambda kv: kv[1][1]):
            if self.total <= self.max_bytes:
                break
            try:
                os.unlink(self.file_path(key, seg))
            except OSError:
                pass
            self._forget(key, seg)
            self.stats["evictions"] += 1
            touched.add(key)
        for key in touched:
            self._refresh_complete(key, persist=False)
            if not self.segs.get(key) and key not in self.metas:
                shutil.rmtree(self._key_dir(key), ignore_errors=True)

    # ----- reporting -----
    def snapshot(self) -> dict:
        with self.lock:
            return {
                "enabled": True,
                "dir": self.dir,
                "version": self.version,
                "segments": len(self.entries),
                "bytes": self.total,
                "max_bytes": self.max_bytes,
                "streams": len(self.metas),
                "complete_streams": sum(1 for m in self.metas.values() if m.get("complete")),
                **self.stats,
            }
