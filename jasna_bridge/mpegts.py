"""Just enough MPEG-TS parsing to learn where a segment's content sits in time.

Jasna's --stream segments do not sit on the playlist's fixed 4s grid (measured
2026-09-30 on 0.10.0): segments are cut every 120 frames (4.004s at 29.97fps)
while every playlist entry says 4.000s, the first segment of a render pass is
5-12s long and starts up to ~7s before its slot, and passes started at
different points disagree about where segment N begins by seconds. The bridge
therefore reads each segment's real PTS span and refuses to hand hls.js two
adjacent segments that do not line up (see server.h_segment).
"""
from __future__ import annotations

PACKET = 188
SYNC = 0x47


def _pts(b: bytes, i: int) -> int:
    return (((b[i] & 0x0E) << 29) | (b[i + 1] << 22) | ((b[i + 2] & 0xFE) << 14)
            | (b[i + 3] << 7) | (b[i + 4] >> 1))


def track_spans(data: bytes) -> dict[str, tuple[float, float]]:
    """{"video": (start, end), "audio": (start, end)} in seconds for the
    tracks present, from PES PTS. `end` is the last PTS plus one frame."""
    n = len(data) - len(data) % PACKET
    if n < PACKET or data[0] != SYNC:
        return {}
    video: dict[int, list[int]] = {}
    audio: dict[int, list[int]] = {}
    for off in range(0, n, PACKET):
        if data[off] != SYNC:
            return {}  # lost sync; not worth resyncing for our purposes
        b1 = data[off + 1]
        if not b1 & 0x40:  # payload_unit_start_indicator: only PES starts carry a PTS
            continue
        afc = data[off + 3] & 0x30
        if not afc & 0x10:
            continue
        p = off + 4
        if afc & 0x20:
            p += 1 + data[off + 4]
        end = off + PACKET
        if p + 14 > end or data[p] or data[p + 1] or data[p + 2] != 1:
            continue  # PSI (PAT/PMT) or a PES header split across packets
        stream_id = data[p + 3]
        if not data[p + 7] & 0x80:
            continue  # no PTS
        pid = ((b1 & 0x1F) << 8) | data[off + 2]
        pts = _pts(data, p + 9)
        if 0xE0 <= stream_id <= 0xEF:
            video.setdefault(pid, []).append(pts)
        elif 0xC0 <= stream_id <= 0xDF:
            audio.setdefault(pid, []).append(pts)
    out = {}
    for name, tracks in (("video", video), ("audio", audio)):
        if tracks:
            pts_list = max(tracks.values(), key=len)
            lo, hi = min(pts_list), max(pts_list)
            frame = (hi - lo) / (len(pts_list) - 1) if len(pts_list) > 1 else 0
            out[name] = (lo / 90000.0, (hi + frame) / 90000.0)
    return out


def segment_span(data: bytes) -> tuple[float, float] | None:
    """(start, end) in seconds of the video track's presentation span
    (audio if there is no video), or None if the bytes are not a parseable
    transport stream. `end` is the last frame's PTS plus one frame duration,
    matching what ffprobe reports as start_time + duration within ~1 frame."""
    spans = track_spans(data)
    return spans.get("video") or spans.get("audio")


def trim_leading_audio(data: bytes, before_s: float) -> bytes:
    """Drop audio PES units whose PTS is below before_s (whole TS packets, so
    nothing is re-encoded). Jasna's first segment of a pass carries up to
    ~1.7s of audio ahead of its first video frame; spliced after another
    pass that audio would play over the previous pass's picture."""
    cutoff = int(before_s * 90000)
    n = len(data) - len(data) % PACKET
    keep = bytearray()
    dropping: dict[int, bool] = {}  # audio pid -> currently inside a dropped PES unit
    for off in range(0, n, PACKET):
        pkt = data[off:off + PACKET]
        pid = ((pkt[1] & 0x1F) << 8) | pkt[2]
        if pkt[1] & 0x40 and pkt[3] & 0x10:
            p = 4 + (1 + pkt[4] if pkt[3] & 0x20 else 0)
            if p + 14 <= PACKET and not pkt[p] and not pkt[p + 1] and pkt[p + 2] == 1 and 0xC0 <= pkt[p + 3] <= 0xDF:
                dropping[pid] = bool(pkt[p + 7] & 0x80) and _pts(pkt, p + 9) < cutoff
        if dropping.get(pid):
            continue
        keep += pkt
    return bytes(keep) + data[n:]


WRAP = 1 << 33  # PTS/DTS/PCR base are 33-bit 90kHz counters


def _write_ts(b: bytearray, i: int, v: int) -> None:
    b[i] = (b[i] & 0xF0) | ((v >> 29) & 0x0E) | 1
    b[i + 1] = (v >> 22) & 0xFF
    b[i + 2] = ((v >> 14) & 0xFE) | 1
    b[i + 3] = (v >> 7) & 0xFF
    b[i + 4] = ((v << 1) & 0xFE) | 1


def restamp(data: bytes, delta_s: float) -> bytes:
    """Shift every PTS, DTS, PCR and OPCR in a transport stream by delta_s.

    A byte-level splice, the same thing an MPEG-TS splicer or ffmpeg's
    -output_ts_offset does: headers only, payload untouched, no re-encode.
    Used to make a segment from a new Jasna pass continue the timeline of
    the segment served before it (Jasna's PTS are honest source time + 1.4s,
    so a pass boundary is an overlap or hole of a few seconds, which hls.js
    mishandles when the fragment is contiguous). ~3ms per 3MB segment.
    """
    delta = int(round(delta_s * 90000))
    if not delta:
        return data
    b = bytearray(data)
    n = len(b) - len(b) % PACKET
    for off in range(0, n, PACKET):
        if b[off] != SYNC:
            break
        afc = b[off + 3] & 0x30
        p = off + 4
        if afc & 0x20:
            al = b[off + 4]
            if al >= 7:
                flags = b[off + 5]
                q = off + 6
                for fl in (0x10, 0x08):  # PCR, OPCR: 33-bit base, 6 reserved, 9-bit extension
                    if flags & fl and q + 6 <= off + 5 + al:
                        base = (b[q] << 25) | (b[q + 1] << 17) | (b[q + 2] << 9) | (b[q + 3] << 1) | (b[q + 4] >> 7)
                        base = (base + delta) % WRAP
                        b[q] = (base >> 25) & 0xFF
                        b[q + 1] = (base >> 17) & 0xFF
                        b[q + 2] = (base >> 9) & 0xFF
                        b[q + 3] = (base >> 1) & 0xFF
                        b[q + 4] = (b[q + 4] & 0x7F) | ((base & 1) << 7)
                        q += 6
            p += 1 + al
        if not b[off + 1] & 0x40 or not afc & 0x10:
            continue
        if p + 14 > off + PACKET or b[p] or b[p + 1] or b[p + 2] != 1:
            continue
        if not 0xC0 <= b[p + 3] <= 0xEF:
            continue
        fl = b[p + 7]
        if fl & 0x80:
            _write_ts(b, p + 9, (_pts(b, p + 9) + delta) % WRAP)
            if fl & 0x40:
                _write_ts(b, p + 14, (_pts(b, p + 14) + delta) % WRAP)
    return bytes(b)


def pcr_values(data: bytes) -> list[float]:
    """PCR base values in seconds, in stream order (for tests and diagnostics)."""
    out = []
    n = len(data) - len(data) % PACKET
    for off in range(0, n, PACKET):
        if data[off + 3] & 0x20 and data[off + 4] >= 7 and data[off + 5] & 0x10:
            q = off + 6
            base = (data[q] << 25) | (data[q + 1] << 17) | (data[q + 2] << 9) | (data[q + 3] << 1) | (data[q + 4] >> 7)
            out.append(base / 90000.0)
    return out
