#!/usr/bin/env python3
"""Measure a video's colour cast and pick / generate a .cube LUT for Jasna's --lut.

Standalone experiment, not part of the bridge. For each video it:

  1. samples --frames frames evenly across the middle 90% (downscaled),
  2. estimates the illuminant from the brightest unclipped, not-too-saturated
     pixels of each frame (white-patch style), with grey-edge as a second opinion,
  3. applies every candidate LUT (numpy, trilinear) to the samples and re-measures,
  4. writes a per-video LUT: --base (Bombadil MIGD-358) blended with identity by
     how warm the video measured (none at --warm-lo, full at --warm-hi), scaled
     down again where skin is already red (full at --skin-full, none at
     --skin-none). A sidecar marked "manual" overrides the measured blend.
     A straight white-balance (per-channel gains) was tried first and looked
     washed out: it takes the red out of skin, which MIGD-358 keeps,
  5. renders comparison grids with ffmpeg's lut3d filter: original, every
     candidate, and the per-video LUT,

and writes index.html in --out with the numbers and the strips.

    uv run --with numpy tools/lut_probe.py --out /tmp/probe video1.mp4 video2.avi ...

Cast numbers are log-ratios (on gamma-encoded values) of the estimated illuminant:
rg = ln(R/G), bg = ln(B/G).
Yellow = bg negative (blue missing); red/orange = rg positive. 0.05 is about 5%.
"""
import argparse
import datetime
import html
import json
import math
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np

DEFAULT_LUTS = Path.home() / "Applications/JasnaCubemaps/Bombadil_LUTs"
# The colour-cast LUTs; "Blacks Fix" ones only lift/crush shadows, so leave them out.
DEFAULT_CANDIDATES = ["Bombadil Subtle.cube", "Bombadil.cube",
                      "Bombadil MIGD-358.cube"]
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
SAMPLE_W = 320
GAMMA = 2.2


# ---------------------------------------------------------------- .cube I/O

def read_cube(path: Path) -> np.ndarray:
    """3D .cube -> array [b][g][r][3] (file order: red changes fastest)."""
    size, lo, hi, rows = None, np.zeros(3), np.ones(3), []
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key = line.split()[0].upper()
        if key == "LUT_3D_SIZE":
            size = int(line.split()[1])
        elif key == "LUT_1D_SIZE":
            raise ValueError(f"{path.name}: 1D LUTs not supported here")
        elif key == "DOMAIN_MIN":
            lo = np.array([float(x) for x in line.split()[1:4]])
        elif key == "DOMAIN_MAX":
            hi = np.array([float(x) for x in line.split()[1:4]])
        elif key in ("TITLE", "LUT_3D_INPUT_RANGE"):
            continue
        else:
            rows.append([float(x) for x in line.split()[:3]])
    if size is None or len(rows) != size ** 3:
        raise ValueError(f"{path.name}: bad LUT ({len(rows)} rows, size {size})")
    if not (np.allclose(lo, 0) and np.allclose(hi, 1)):
        raise ValueError(f"{path.name}: non-default DOMAIN not supported")
    return np.array(rows, dtype=np.float32).reshape(size, size, size, 3)


def write_cube(path: Path, fn, size=33, title=""):
    """fn maps an (N,3) array of RGB in 0..1 to (N,3)."""
    i = np.linspace(0, 1, size, dtype=np.float32)
    b, g, r = np.meshgrid(i, i, i, indexing="ij")
    rgb = np.stack([r, g, b], -1).reshape(-1, 3)
    out = np.clip(fn(rgb), 0, 1)
    with path.open("w") as f:
        if title:
            f.write(f'TITLE "{title}"\n')
        f.write(f"LUT_3D_SIZE {size}\n")
        np.savetxt(f, out, fmt="%.6f")


def apply_lut(lut: np.ndarray, rgb: np.ndarray) -> np.ndarray:
    """Trilinear lookup; rgb (N,3) in 0..1."""
    n = lut.shape[0]
    x = np.clip(rgb, 0, 1) * (n - 1)
    i0 = np.minimum(x.astype(np.int32), n - 2)
    f = x - i0
    r0, g0, b0 = i0[:, 0], i0[:, 1], i0[:, 2]
    fr, fg, fb = f[:, 0:1], f[:, 1:2], f[:, 2:3]
    out = 0
    for db, wb in ((0, 1 - fb), (1, fb)):
        for dg, wg in ((0, 1 - fg), (1, fg)):
            for dr, wr in ((0, 1 - fr), (1, fr)):
                out = out + lut[b0 + db, g0 + dg, r0 + dr] * (wr * wg * wb)
    return np.clip(out, 0, 1)


# ---------------------------------------------------------------- frames

def probe(video: Path) -> dict:
    out = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                          "stream=width,height:format=duration", "-of", "json", str(video)],
                         capture_output=True, text=True, check=True)
    j = json.loads(out.stdout)
    s = j["streams"][0]
    return {"w": s["width"], "h": s["height"], "duration": float(j["format"]["duration"])}


def grab(video: Path, t: float, w: int, h: int) -> np.ndarray | None:
    """One frame at t seconds, scaled to SAMPLE_W wide, as float RGB (H,W,3) 0..1."""
    sh = max(2, round(h * SAMPLE_W / w / 2) * 2)
    p = subprocess.run(["ffmpeg", "-v", "error", "-ss", f"{t:.3f}", "-i", str(video), "-frames:v", "1",
                        "-vf", f"scale={SAMPLE_W}:{sh}:flags=area", "-f", "rawvideo", "-pix_fmt", "rgb24", "-"],
                       capture_output=True)
    if p.returncode or len(p.stdout) != SAMPLE_W * sh * 3:
        return None
    return np.frombuffer(p.stdout, np.uint8).reshape(sh, SAMPLE_W, 3).astype(np.float32) / 255


# ---------------------------------------------------------------- cast estimate

def estimate(rgb: np.ndarray, top=0.10, max_sat=0.5):
    """Illuminant estimate from the brightest unclipped pixels (white-patch style).

    rgb: (N,3) gamma-encoded 0..1. Returns (rg, bg, n_used): ln(R/G), ln(B/G)
    of the median of the top `top` fraction by luminance, ignoring clipped
    channels, near-black pixels and strongly saturated colours. Bright surfaces
    (walls, sheets, clothing, window light) are mostly near-neutral; an
    "iterate towards the grey cluster" estimate was tried first and latched on
    to skin, which fills much of these frames.
    """
    mx, mn = rgb.max(1), rgb.min(1)
    ok = (mn > 0.06) & (mx < 0.96) & ((mx - mn) / np.maximum(mx, 1e-3) < max_sat)
    p = rgb[ok]
    if len(p) < 300:
        return None
    y = p @ np.array([0.2126, 0.7152, 0.0722], np.float32)
    p = p[y >= np.quantile(y, 1 - top)]
    u = np.log(p[:, 0] / p[:, 1])
    v = np.log(p[:, 2] / p[:, 1])
    return float(np.median(u)), float(np.median(v)), len(p)


def grey_edge(frame: np.ndarray, p=6):
    """Second opinion: grey-edge (van de Weijer) illuminant, as gamma-domain log-ratios."""
    lin = np.power(frame, GAMMA)
    g = np.hypot(np.abs(np.diff(lin, axis=1))[:-1], np.abs(np.diff(lin, axis=0))[:, :-1]).reshape(-1, 3)
    e = np.power((g ** p).mean(0), 1 / p) + 1e-9
    return float(np.log(e[0] / e[1]) / GAMMA), float(np.log(e[2] / e[1]) / GAMMA)


def measure(frames: list[np.ndarray], lut: np.ndarray | None = None) -> dict:
    per = []
    for fr in frames:
        px = fr.reshape(-1, 3)
        if lut is not None:
            px = apply_lut(lut, px)
        e = estimate(px)
        if e:
            per.append(e)
    if not per:
        return {"ok": False}
    a = np.array(per)
    w = a[:, 2]
    # Weight frames by how many neutral pixels backed them.
    rg = float(np.average(a[:, 0], weights=w))
    bg = float(np.average(a[:, 1], weights=w))
    ge = np.array([grey_edge(fr if lut is None else apply_lut(lut, fr.reshape(-1, 3)).reshape(fr.shape))
                   for fr in frames])
    return {"ok": True, "rg": rg, "bg": bg, "mag": math.hypot(rg, bg),
            "ge_rg": float(np.median(ge[:, 0])), "ge_bg": float(np.median(ge[:, 1])),
            "rg_sd": float(a[:, 0].std()), "bg_sd": float(a[:, 1].std()),
            "frames_used": len(per), "neutral_px": int(w.sum())}


def warmth(m: dict) -> float:
    """How much warmer than neutral (red over blue) the bright surfaces are."""
    return m["rg"] - m["bg"]


def ramp(x: float, zero_at: float, one_at: float) -> float:
    return min(1.0, max(0.0, (x - zero_at) / (one_at - zero_at)))


def skin_redness(frames: list[np.ndarray]) -> float | None:
    """Median ln(R/G) of skin-like pixels (R>G>B, hue 5-45 deg, moderate
    saturation, not dark or clipped). MIGD-358 adds red to skin, so where skin
    is already red the result reads too warm even when the whiteish surfaces
    are yellow (MEYD-057)."""
    px = np.concatenate([f.reshape(-1, 3) for f in frames])
    r, g, b = px.T
    mx, mn = px.max(1), px.min(1)
    sat = (mx - mn) / np.maximum(mx, 1e-3)
    y = px @ np.array([0.2126, 0.7152, 0.0722], np.float32)
    hue = np.degrees(np.arctan2(np.sqrt(3) * (g - b), 2 * r - g - b))
    skin = (r > g) & (g > b) & (sat > 0.15) & (sat < 0.65) & (hue > 5) & (hue < 45) & (y > 0.2) & (mx < 0.97)
    if skin.sum() < 1000:
        return None
    return float(np.median(np.log(px[skin, 0] / px[skin, 1])))


def blend_strength(m: dict, skin: float | None, a) -> float:
    """Warmth ramp (none at --warm-lo, full at --warm-hi) times a skin ramp
    (full at --skin-full or less red, none at --skin-none or more)."""
    k = ramp(warmth(m), a.warm_lo, a.warm_hi)
    if skin is not None:
        k *= ramp(skin, a.skin_none, a.skin_full)
    return k


def blend_lut(base: np.ndarray, k: float) -> np.ndarray:
    """identity + k * (base - identity), on base's own grid."""
    n = base.shape[0]
    i = np.linspace(0, 1, n, dtype=np.float32)
    b, g, r = np.meshgrid(i, i, i, indexing="ij")
    ident = np.stack([r, g, b], -1)
    return np.clip(ident + k * (base - ident), 0, 1)


def save_cube(path: Path, lut: np.ndarray, title=""):
    with path.open("w") as f:
        if title:
            f.write(f'TITLE "{title}"\n')
        f.write(f"LUT_3D_SIZE {lut.shape[0]}\n")
        np.savetxt(f, lut.reshape(-1, 3), fmt="%.6f")


# ---------------------------------------------------------------- render

def render_grid(video: Path, t: float, h: int, columns: list[tuple[str, Path | None]], out: Path, per_row=3):
    """ffmpeg: one frame at t, once per (label, LUT or None), labelled, tiled per_row wide."""
    oh = min(h, 360)
    n = len(columns)
    parts = [f"[0:v]scale=-2:{oh}:flags=lanczos,format=rgb24,split={n}" + "".join(f"[s{i}]" for i in range(n))]
    for i, (label, lut) in enumerate(columns):
        chain = f"[s{i}]"
        if lut is not None:
            chain += f"lut3d=file={lut.as_posix()}:interp=tetrahedral,"
        safe = label.replace("\\", "").replace(":", "\\:").replace("'", "")
        chain += (f"drawtext=fontfile={FONT}:text='{safe}':x=8:y=8:fontsize=16:fontcolor=white:"
                  f"expansion=none:box=1:boxcolor=black@0.6:boxborderw=4[c{i}]")
        parts.append(chain)
    def off(k, dim):
        return "+".join([dim] * k) if k else "0"
    layout = "|".join(f"{off(i % per_row, 'w0')}_{off(i // per_row, 'h0')}" for i in range(n))
    parts.append("".join(f"[c{i}]" for i in range(n)) + f"xstack=inputs={n}:layout={layout}:fill=black")
    subprocess.run(["ffmpeg", "-v", "error", "-y", "-ss", f"{t:.3f}", "-i", str(video), "-frames:v", "1",
                    "-filter_complex", ";".join(parts), str(out)], check=True)


# ---------------------------------------------------------------- main

def fmt_cast(m: dict) -> str:
    if not m.get("ok"):
        return "n/a"
    return f"rg {m['rg']:+.3f}  bg {m['bg']:+.3f}  |{m['mag']:.3f}|   grey-edge rg {m['ge_rg']:+.3f} bg {m['ge_bg']:+.3f}"


def describe(m: dict) -> str:
    if not m.get("ok"):
        return "no neutral pixels found"
    if m["mag"] < 0.04:
        return "neutral"
    hue = math.degrees(math.atan2(m["bg"], m["rg"]))  # 0 = red, -90 = yellow-ish (blue missing)
    if -135 < hue < -45:
        kind = "yellow"
    elif -45 <= hue < 30:
        kind = "orange/red" if m["rg"] > 0 else "?"
    elif hue >= 30 and hue < 150:
        kind = "blue/magenta"
    else:
        kind = "green/cyan"
    return f"{kind} cast, {'strong' if m['mag'] > 0.15 else 'moderate' if m['mag'] > 0.08 else 'mild'}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("videos", nargs="+", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--luts", type=Path, default=DEFAULT_LUTS, help="folder of candidate .cube files")
    ap.add_argument("--candidates", nargs="*", default=DEFAULT_CANDIDATES,
                    help="file names in --luts to try (default: the Bombadil colour-cast LUTs)")
    ap.add_argument("--frames", type=int, default=80,
                    help="frames sampled for measurement (under ~30 the skin measurement is unreliable)")
    ap.add_argument("--strips", type=int, default=4, help="comparison strips rendered per video")
    ap.add_argument("--base", default="Bombadil MIGD-358.cube",
                    help="LUT in --luts the per-video LUT is a partial blend of")
    ap.add_argument("--warm-lo", type=float, default=0.06,
                    help="warmth (rg - bg) at or below which a video gets no LUT")
    ap.add_argument("--warm-hi", type=float, default=0.18,
                    help="warmth at or above which a video gets the full --base LUT")
    ap.add_argument("--skin-full", type=float, default=0.28,
                    help="skin ln(R/G) at or below which the warmth-based strength applies in full")
    ap.add_argument("--skin-none", type=float, default=0.30,
                    help="skin ln(R/G) at or above which a video gets no LUT (skin already red enough)")
    ap.add_argument("--save-beside", action="store_true",
                    help="also write <video>.lut.cube and <video>.lut.json next to each source "
                         "(no .cube when the video needs no LUT; the .json says so)")
    args = ap.parse_args()
    if args.frames < 30:
        print(f"warning: --frames {args.frames} is too few for a stable skin measurement; "
              "use 30+ for LUTs you keep", file=sys.stderr)

    args.out.mkdir(parents=True, exist_ok=True)
    lutdir = args.out / "luts"
    lutdir.mkdir(exist_ok=True)
    # Copy candidates to filtergraph-safe names (lut3d=file= doesn't like spaces/quotes).
    cands = []
    for i, name in enumerate(args.candidates):
        src = args.luts / name
        if not src.exists():
            print(f"skip missing LUT {src}", file=sys.stderr)
            continue
        dst = lutdir / f"cand{i}.cube"
        shutil.copyfile(src, dst)
        cands.append({"name": name, "src": str(src), "path": dst, "lut": read_cube(src)})

    base_lut = read_cube(args.luts / args.base)
    report = []
    for vi, video in enumerate(args.videos):
        print(f"[{vi + 1}/{len(args.videos)}] {video.name}", flush=True)
        info = probe(video)
        d = info["duration"]
        times = [d * (0.05 + 0.9 * (k + 0.5) / args.frames) for k in range(args.frames)]
        frames = [f for f in (grab(video, t, info["w"], info["h"]) for t in times) if f is not None]
        base = measure(frames)
        rows = [{"name": "none", "m": base}]
        for c in cands:
            rows.append({"name": c["name"], "m": measure(frames, c["lut"]), "c": c})

        generated, k, skin = [], 0.0, skin_redness(frames)
        sidecar = video.with_name(video.stem + ".lut.json")
        manual = None
        if sidecar.exists():
            try:
                old = json.loads(sidecar.read_text())
                if old.get("manual"):
                    manual = float(old.get("blend") or 0)
            except (ValueError, TypeError) as e:
                print(f"  ignoring unreadable {sidecar.name}: {e}", file=sys.stderr)
        if base.get("ok") or manual is not None:
            k = manual if manual is not None else blend_strength(base, skin, args)
            if k > 0:
                gen_path = lutdir / f"v{vi}-{video.stem}.cube"
                lut = blend_lut(base_lut, k)
                save_cube(gen_path, lut, title=f"{args.base.removesuffix('.cube')} x{k:.2f} for {video.stem}")
                rows.append({"name": f"per-video: {args.base.removesuffix('.cube')} x{k:.2f}",
                             "m": measure(frames, lut), "gen": gen_path})
                generated.append(str(gen_path))
        print(f"  warmth {warmth(base) if base.get('ok') else float('nan'):+.3f}  skin R/G "
              f"{skin if skin is not None else float('nan'):+.3f} -> {args.base} x{k:.2f}"
              + ("  (manual, from .lut.json)" if manual is not None else ""))

        if args.save_beside and base.get("ok"):
            beside = video.with_name(video.stem + ".lut.cube")
            if generated:
                shutil.copyfile(generated[0], beside)
            elif beside.exists():
                beside.unlink()
            sidecar.write_text(json.dumps({
                "lut": beside.name if generated else None,
                "base_lut": args.base, "blend": round(k, 4),
                "manual": manual is not None,
                "_manual_help": 'set "manual": true and your own "blend" (0 = no LUT, 1 = full base LUT), '
                                'then rerun with --save-beside to rebuild the .cube; manual entries are kept',
                "made_by": "stash-jasna-bridge tools/lut_probe.py",
                "date": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
                "source": video.name, "source_size": f"{info['w']}x{info['h']}",
                "estimator": "bright-pixel white patch (top 10% luminance, unclipped, sat<0.5), gamma-domain",
                "frames": args.frames, "warmth": warmth(base), "warm_lo": args.warm_lo, "warm_hi": args.warm_hi,
                "skin_rg": skin, "skin_full": args.skin_full, "skin_none": args.skin_none,
                "measured_cast": {k2: base[k2] for k2 in ("rg", "bg", "mag", "ge_rg", "ge_bg", "rg_sd", "bg_sd")},
            }, indent=2) + "\n")
            print(f"  saved {sidecar}" + (f" and {beside.name}" if generated else ""))

        pick = next((r for r in rows if "gen" in r), rows[0])

        cols = [("original", None)]
        for r in rows[1:]:
            label = r["name"].removesuffix(".cube") + ("  <- pick" if r is pick else "")
            cols.append((label, r["c"]["path"] if "c" in r else r["gen"]))
        strips = []
        for k in range(args.strips):
            t = d * (0.15 + 0.7 * k / max(1, args.strips - 1))
            p = args.out / f"v{vi}-strip{k}.png"
            try:
                render_grid(video, t, info["h"], cols, p)
                strips.append(p.name)
            except subprocess.CalledProcessError as e:
                print(f"  strip at {t:.0f}s failed: {e}", file=sys.stderr)

        print(f"  {describe(base)}: {fmt_cast(base)}  (sd rg {base.get('rg_sd', 0):.3f} bg {base.get('bg_sd', 0):.3f}, "
              f"{base.get('frames_used', 0)}/{len(frames)} frames)")
        for r in rows[1:]:
            print(f"    {r['name']:<28} -> {fmt_cast(r['m'])}")
        print(f"  pick: {pick['name']}")
        report.append({"video": str(video), "size": f"{info['w']}x{info['h']}", "describe": describe(base),
                       "rows": [{"name": r["name"], **r["m"]} for r in rows],
                       "pick": pick["name"], "pick_src": pick.get("c", {}).get("src"),
                       "generated": generated, "strips": strips})

    (args.out / "report.json").write_text(json.dumps(report, indent=2))
    write_html(args.out / "index.html", report)
    print(f"\nwrote {args.out / 'index.html'}")


def write_html(path: Path, report: list[dict]):
    e = html.escape
    body = []
    for r in report:
        trs = []
        for row in r["rows"]:
            mark = " class=pick" if row["name"] == r["pick"] else ""
            cast = fmt_cast(row) if row.get("ok") else "n/a"
            extra = ""
            trs.append(f"<tr{mark}><td>{e(row['name'])}</td><td><code>{e(cast)}</code>{e(extra)}</td></tr>")
        imgs = "".join(f'<img src="{e(s)}" loading=lazy>' for s in r["strips"])
        body.append(f"""<section><h2>{e(Path(r['video']).name)}</h2>
<p>{e(r['size'])} — <b>{e(r['describe'])}</b> — pick: <b>{e(r['pick'])}</b></p>
<p class=path>{e(r['video'])}<br>generated LUTs: {'<br>'.join(e(g) for g in r['generated']) or '-'}</p>
<table><tr><th>LUT</th><th>residual cast after LUT</th></tr>{''.join(trs)}</table>{imgs}</section>""")
    path.write_text(f"""<!DOCTYPE html><html><head><meta charset=utf-8><title>LUT probe</title><style>
body{{font:14px system-ui,sans-serif;background:#181818;color:#ddd;margin:16px}}
section{{border-top:1px solid #444;padding:8px 0 24px}} img{{display:block;max-width:100%;margin:6px 0}}
table{{border-collapse:collapse}} td,th{{padding:2px 10px;text-align:left}} tr.pick td{{color:#8f8;font-weight:bold}}
.path{{color:#888;font-size:12px}} code{{white-space:pre}}</style></head><body>
<h1>LUT probe</h1><p>Cast = illuminant log-ratios from the brightest unclipped pixels (grey-edge shown as a second opinion): rg = ln(R/G), bg = ln(B/G).
Yellow shows as negative bg. Per-video LUT = base LUT scaled by warmth (rg - bg). Strips: rendered with ffmpeg lut3d.</p>
{''.join(body)}</body></html>""")


if __name__ == "__main__":
    main()
