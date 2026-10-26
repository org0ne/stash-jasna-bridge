"""Audio fix-up for finished batch outputs.

Jasna copies the source's audio stream into its output, so an MP3 or AC3
source comes out as an MP4 with MP3 or AC3 audio, which browsers (and so
Stash's player) will not play from an MP4. When a job asks for it, the
finished file is probed and any non-matching audio is re-encoded with
ffmpeg while the video stream is copied untouched. This runs on the local
scratch copy before the move to the output share.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time

ALLOWED_CODECS = {"aac"}


class AudioError(Exception):
    pass


def find_tools(ffmpeg: str = "") -> tuple[str, str]:
    """(ffmpeg, ffprobe) paths; ffprobe is looked for beside a configured
    ffmpeg first. Either may be "" when not found."""
    ff = (ffmpeg or "").strip() or shutil.which("ffmpeg") or ""
    probe = ""
    if ff:
        beside = os.path.join(os.path.dirname(ff), "ffprobe")
        probe = beside if os.path.isfile(beside) else ""
    return ff, probe or shutil.which("ffprobe") or ""


def probe_audio(path: str, ffprobe: str) -> list[dict]:
    """[{codec, channels, sample_rate}] for every audio stream."""
    try:
        out = subprocess.run([ffprobe, "-v", "error", "-select_streams", "a", "-show_entries",
                              "stream=codec_name,channels,sample_rate", "-of", "json", path],
                             capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired) as err:
        raise AudioError(f"ffprobe failed: {err}")
    if out.returncode != 0:
        raise AudioError(f"ffprobe failed: {(out.stderr or '').strip()[-300:]}")
    try:
        streams = json.loads(out.stdout or "{}").get("streams", [])
    except ValueError:
        raise AudioError("ffprobe answered no JSON")
    return [{"codec": s.get("codec_name") or "?", "channels": int(s.get("channels") or 0),
             "sample_rate": int(s.get("sample_rate") or 0)} for s in streams]


def ensure_codec(path: str, codec: str, ffmpeg: str, ffprobe: str, log_file: str = "") -> dict | None:
    """Re-encode the audio of `path` to `codec` in place when any audio
    stream is something else. Video and other streams are stream-copied.
    Returns a summary dict when a conversion happened, None when the audio
    already matched or there is none. Raises AudioError on failure, leaving
    `path` untouched."""
    if codec not in ALLOWED_CODECS:
        raise AudioError(f"audio codec {codec!r} is not supported (one of {sorted(ALLOWED_CODECS)})")
    if not ffmpeg or not ffprobe:
        raise AudioError("ffmpeg/ffprobe not found on the bridge host")
    streams = probe_audio(path, ffprobe)
    if not streams or all(s["codec"] == codec for s in streams):
        return None
    channels = max((s["channels"] for s in streams), default=2) or 2
    bitrate = f"{min(96 * channels, 512)}k"           # 192k for stereo
    tmp = f"{path}.audio{os.path.splitext(path)[1]}"
    cmd = [ffmpeg, "-y", "-nostdin", "-hide_banner", "-loglevel", "error", "-i", path,
           "-map", "0:v", "-map", "0:a", "-c:v", "copy", "-c:a", codec, "-b:a", bitrate,
           "-movflags", "+faststart", tmp]
    started = time.monotonic()
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=6 * 3600)
    except (OSError, subprocess.TimeoutExpired) as err:
        _discard(tmp)
        raise AudioError(f"ffmpeg failed: {err}")
    if log_file:
        try:
            with open(log_file, "a") as fh:
                fh.write(f"\n===== audio -> {codec}: {' '.join(cmd)}\n{out.stderr or ''}")
        except OSError:
            pass
    if out.returncode != 0 or not os.path.isfile(tmp) or os.path.getsize(tmp) == 0:
        _discard(tmp)
        raise AudioError(f"ffmpeg exited {out.returncode}: {(out.stderr or '').strip()[-300:]}")
    after = probe_audio(tmp, ffprobe)
    if not after or any(s["codec"] != codec for s in after):
        _discard(tmp)
        raise AudioError(f"ffmpeg output still has {[s['codec'] for s in after]} audio")
    os.replace(tmp, path)
    return {"from": "+".join(sorted({s["codec"] for s in streams})), "to": codec, "channels": channels,
            "bitrate": bitrate, "seconds": round(time.monotonic() - started, 1)}


def _discard(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
