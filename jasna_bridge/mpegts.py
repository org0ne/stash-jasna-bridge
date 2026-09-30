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


def segment_span(data: bytes) -> tuple[float, float] | None:
    """(start, end) in seconds of the video track's presentation span
    (audio if there is no video), or None if the bytes are not a parseable
    transport stream. `end` is the last frame's PTS plus one frame duration,
    matching what ffprobe reports as start_time + duration within ~1 frame."""
    n = len(data) - len(data) % PACKET
    if n < PACKET or data[0] != SYNC:
        return None
    video: dict[int, list[int]] = {}
    audio: dict[int, list[int]] = {}
    for off in range(0, n, PACKET):
        if data[off] != SYNC:
            return None  # lost sync; not worth resyncing for our purposes
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
    tracks = video or audio
    if not tracks:
        return None
    pts_list = max(tracks.values(), key=len)
    lo, hi = min(pts_list), max(pts_list)
    frame = (hi - lo) / (len(pts_list) - 1) if len(pts_list) > 1 else 0
    return lo / 90000.0, (hi + frame) / 90000.0
