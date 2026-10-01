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
- Each segment's real PTS span (mpegts.segment_span) is remembered so the
  server can splice segments from different Jasna passes (which disagree
  about where segment N starts by seconds) into one timeline.
- meta.json records the source file's size and mtime; a changed file at the
  same path drops its cached segments.
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

from .mpegts import segment_span

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
        self.spans: dict[tuple[str, str], tuple[float, float] | None] = {}  # (key, seg) -> PTS span, parsed lazily
        self.total = 0
        self.stats = {"hits": 0, "misses": 0, "stored": 0, "evictions": 0, "bytes_from_cache": 0,
                      "source_changed": 0}
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
            if "source" not in meta:
                sig = self._source_sig(path)
                if sig is not None:
                    meta["source"] = sig
            self.metas[key] = meta
            self._refresh_complete(key, persist=True)

    @staticmethod
    def _source_sig(path: str) -> list | None:
        try:
            st = os.stat(path)
        except OSError:
            return None  # not visible from the bridge host: cannot check
        return [st.st_size, int(st.st_mtime)]

    def check_source(self, key: str, path: str) -> None:
        """Drop a stream's cached segments if its source file changed (same
        path, different size or mtime: re-encoded or replaced)."""
        sig = self._source_sig(path)
        if sig is None:
            return
        with self.lock:
            meta = self.metas.get(key)
            if meta is None:
                return
            old = meta.get("source")
            if old is None:
                meta["source"] = sig
                self._write_meta(key)
                return
            if old == sig:
                return
            log.info("source %s changed since it was cached; dropping %d segments", path,
                     len(self.segs.get(key, ())))
            self.stats["source_changed"] += 1
            for seg in list(self.segs.get(key, ())):
                try:
                    os.unlink(self.file_path(key, seg))
                except OSError:
                    pass
                self._forget(key, seg)
            self.metas.pop(key, None)
            shutil.rmtree(self._key_dir(key), ignore_errors=True)

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

    def span(self, key: str, seg: str, data: bytes | None = None) -> tuple[float, float] | None:
        """(start, end) seconds of a cached segment's video, parsed once.
        Pass the bytes when already read, to avoid reading the file again."""
        with self.lock:
            if (key, seg) in self.spans:
                return self.spans[(key, seg)]
            if (key, seg) not in self.entries:
                return None
        if data is None:
            try:
                with open(self.file_path(key, seg), "rb") as fh:
                    data = fh.read()
            except OSError:
                return None
        span = segment_span(data)
        with self.lock:
            self.spans[(key, seg)] = span
        return span

    def put(self, key: str, seg: str, data: bytes) -> tuple[float, float] | None:
        """Store a segment; returns its PTS span (None if not parseable)."""
        if not SEG_RE.match(seg) or not data:
            return None
        span = segment_span(data)
        if span is None and not getattr(self, "_warned_unparseable", False):
            self._warned_unparseable = True
            log.warning("segment %s/%s is not a parseable MPEG-TS; seam checks are off for such segments", key, seg)
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
            return span
        with self.lock:
            old = self.entries.get((key, seg))
            if old:
                self.total -= old[0]
            self.entries[(key, seg)] = (len(data), time.time())
            self.spans[(key, seg)] = span
            self.segs.setdefault(key, set()).add(seg)
            self.total += len(data)
            self.stats["stored"] += 1
            self._evict()
            self._refresh_complete(key, persist=False)
        return span

    def _forget(self, key: str, seg: str) -> None:
        self.spans.pop((key, seg), None)
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
