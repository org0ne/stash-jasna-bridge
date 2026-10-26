"""Container fix-ups for finished batch outputs, on the local scratch copy
before the move to the output share. One ffmpeg pass handles both:

- Audio codec. Jasna copies the source's audio stream into its output, so
  an MP3 or AC3 source comes out as an MP4 with MP3 or AC3 audio, which
  browsers (and so Stash's player) will not play from an MP4. When a job
  asks for a codec, any other audio is re-encoded; video is copied.
- Edit list. Jasna 0.11.3 sometimes writes a video edit list (elst) whose
  media_time does not match the first keyframe's PTS (it carries over the
  source's start delay), and players that honour it show the video a
  constant ~100 ms off the audio for the whole film. ffprobe warns about
  it in a fraction of a second; a stream-copy remux makes ffmpeg write a
  clean edit list. Checked on every output, whatever the audio.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time

ALLOWED_CODECS = {"aac"}
# ffprobe's complaints (stderr, -v warning) about an edit list whose media_time is off a keyframe
EDIT_LIST_WARNINGS = ("Missing key frame while searching for timestamp", "Cannot find an index entry before timestamp")


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


def broken_edit_list(path: str, ffprobe: str) -> bool:
    """True when ffprobe warns that the file's edit list points off a
    keyframe (~0.1 s even on a 7 GB file: it only reads the index)."""
    try:
        out = subprocess.run([ffprobe, "-v", "warning", "-show_entries", "format=duration", "-of", "csv=p=0", path],
                             capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired) as err:
        raise AudioError(f"ffprobe failed: {err}")
    if out.returncode != 0:
        raise AudioError(f"ffprobe failed: {(out.stderr or '').strip()[-300:]}")
    return any(w in (out.stderr or "") for w in EDIT_LIST_WARNINGS)


def fix_output(path: str, codec: str, ffmpeg: str, ffprobe: str, log_file: str = "") -> dict | None:
    """Repair `path` in place with one stream-copy remux when its audio is
    not `codec` (if one is asked for; "" = leave the audio alone) or its
    edit list is broken. Video is always copied; audio is re-encoded only
    when it has to change. Returns a summary when something was done:
    {"edit_list": bool, "seconds": float} plus "from", "to", "channels",
    "bitrate" when the audio changed. None when the file was fine. Raises
    AudioError on failure, leaving `path` untouched."""
    if codec and codec not in ALLOWED_CODECS:
        raise AudioError(f"audio codec {codec!r} is not supported (one of {sorted(ALLOWED_CODECS)})")
    if not ffmpeg or not ffprobe:
        raise AudioError("ffmpeg/ffprobe not found on the bridge host")
    streams = probe_audio(path, ffprobe)
    convert = bool(codec) and bool(streams) and any(s["codec"] != codec for s in streams)
    bad_elst = broken_edit_list(path, ffprobe)
    if not convert and not bad_elst:
        return None
    channels = max((s["channels"] for s in streams), default=2) or 2
    bitrate = f"{min(96 * channels, 512)}k"           # 192k for stereo
    audio_args = ["-c:a", codec, "-b:a", bitrate] if convert else ["-c:a", "copy"]
    tmp = f"{path}.fix{os.path.splitext(path)[1]}"
    cmd = [ffmpeg, "-y", "-nostdin", "-hide_banner", "-loglevel", "error", "-i", path,
           "-map", "0:v", "-map", "0:a", "-map_metadata", "0", "-c:v", "copy", *audio_args,
           "-movflags", "+faststart", tmp]
    what = " + ".join((["audio -> " + codec] if convert else []) + (["edit list"] if bad_elst else []))
    started = time.monotonic()
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=6 * 3600)
    except (OSError, subprocess.TimeoutExpired) as err:
        _discard(tmp)
        raise AudioError(f"ffmpeg failed: {err}")
    if log_file:
        try:
            with open(log_file, "a") as fh:
                fh.write(f"\n===== {what}: {' '.join(cmd)}\n{out.stderr or ''}")
        except OSError:
            pass
    if out.returncode != 0 or not os.path.isfile(tmp) or os.path.getsize(tmp) == 0:
        _discard(tmp)
        raise AudioError(f"ffmpeg exited {out.returncode}: {(out.stderr or '').strip()[-300:]}")
    after = probe_audio(tmp, ffprobe)
    if convert and (not after or any(s["codec"] != codec for s in after)):
        _discard(tmp)
        raise AudioError(f"ffmpeg output still has {[s['codec'] for s in after]} audio")
    if broken_edit_list(tmp, ffprobe):
        _discard(tmp)
        raise AudioError("ffmpeg output still has a broken edit list")
    os.replace(tmp, path)
    result = {"edit_list": bad_elst, "seconds": round(time.monotonic() - started, 1)}
    if convert:
        result.update({"from": "+".join(sorted({s["codec"] for s in streams})), "to": codec,
                       "channels": channels, "bitrate": bitrate})
    return result


ensure_codec = fix_output   # name used until 0.3.3


def _discard(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
