"""Re-time Jasna's static playlist to the segments the bridge actually has.

Jasna's VOD playlist calls every segment 4.000s, while its segments are
120 frames (4.004s at 29.97fps) and, at scene cuts, anything from 0.5s to
7s: within a pass the content drifts seconds away from the 4N slot its name
implies (measured 2026-10-12: -3.5s after 85 segments). hls.js pins the
*first* fragment it loads to that fragment's playlist position and places
everything after it by timestamp, so a session that starts on a cached
mid-pass segment runs the whole time with the picture that many seconds
away from the clock - and subtitles, keyed to the clock, with it. The
bridge knows where every cached segment's video really starts, so it
rewrites the durations such that each known segment's position is its real
start (the first frame's PTS minus Jasna's mux delay) and each unknown one
stays at its nominal slot, which is where a fresh Jasna pass will start it.
"""
from __future__ import annotations

import math
import re

# Jasna 0.11 appends ?epoch=N (bumped on every /open) to each entry; the
# bridge serves segments by name alone, so the query is matched and ignored.
SEG_RE = re.compile(r"^seg_([0-9]{5})\.ts(?:\?[^\s]*)?$")
EXTINF_RE = re.compile(r"^#EXTINF:([0-9.]+)")
MIN_DURATION_S = 0.01


def retime(text: str, video_starts: dict[int, float], mux_delay_s: float) -> tuple[str, int]:
    """Return (playlist, number of entries moved). `video_starts` maps a
    segment index to the PTS (seconds) of its first video frame."""
    lines = text.splitlines()
    entries: list[tuple[int, int, float]] = []  # (extinf line, index, nominal duration)
    pending = None
    for i, line in enumerate(lines):
        m = EXTINF_RE.match(line)
        if m:
            pending = (i, float(m.group(1)))
            continue
        m = SEG_RE.match(line.strip())
        if m and pending is not None:
            entries.append((pending[0], int(m.group(1)), pending[1]))
            pending = None
    if not entries:
        return text, 0
    nominal = []
    t = 0.0
    for _, _, dur in entries:
        nominal.append(t)
        t += dur
    starts = [video_starts[idx] - mux_delay_s if idx in video_starts else nominal[k]
              for k, (_, idx, _) in enumerate(entries)]
    moved = sum(1 for k, (_, idx, _) in enumerate(entries) if idx in video_starts and abs(starts[k] - nominal[k]) > 0.0005)
    if not moved:
        return text, 0
    longest = 0.0
    for k, (line_no, _, dur) in enumerate(entries):
        if k + 1 < len(entries):
            dur = max(MIN_DURATION_S, starts[k + 1] - starts[k])
        longest = max(longest, dur)
        lines[line_no] = f"#EXTINF:{dur:.3f},"
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-TARGETDURATION:"):
            lines[i] = f"#EXT-X-TARGETDURATION:{max(int(line.split(':', 1)[1] or 0), math.ceil(longest))}"
    return "\n".join(lines) + "\n", moved
