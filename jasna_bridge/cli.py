"""`install` and `doctor` subcommands: set the bridge up with as little typing
as possible, and check a deployment. Stdlib only."""
from __future__ import annotations

import glob
import json
import os
import shutil
import socket
import subprocess
import sys
import urllib.error
import urllib.request

from . import config

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNIT_NAME = "stash-jasna-bridge.service"

# A sensible starting preset set, so a fresh install has more than "defaults".
DEFAULT_PRESETS = {
    "hq": {
        "description": "rfdetr-v6-large + unet-4x, 180s clips",
        "flags": ["--detection-model", "rfdetr-v6-large", "--detection-score-threshold", "0.4",
                  "--secondary-restoration", "unet-4x", "--max-clip-size", "180", "--batch-size", "4"],
    },
    "fast": {"description": "Jasna defaults (faster, no unet)", "flags": []},
}


def _find_jasna_binary() -> str:
    env = os.environ.get("JASNA_BIN")
    if env and os.path.isfile(env):
        return env
    which = shutil.which("jasna")
    if which:
        return os.path.realpath(which)
    # newest ~/Applications/jasna*/jasna (a symlink like ~/Applications/jasna wins by mtime)
    cands = sorted(glob.glob(os.path.expanduser("~/Applications/jasna*/jasna")),
                   key=lambda p: os.path.getmtime(p), reverse=True)
    for c in cands:
        if os.path.isfile(c):
            return os.path.realpath(c)
    return ""


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _pick_stream_port() -> int:
    return 8765 if _port_free(8765) else 8766


def _lan_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("192.168.1.1", 1))  # no packet sent; just reads the chosen source IP
            return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def _toml_list(xs) -> str:
    return "[" + ", ".join('"' + str(x).replace('"', '\\"') + '"' for x in xs) + "]"


def _write_config(path: str, stash_url: str, binary: str, port: int, presets: dict, default_preset: str) -> None:
    lines = [
        "# stash-jasna-bridge configuration (written by `python3 -m jasna_bridge install`).",
        "# The Jasna license is read from ~/.config/jasna/license.json automatically.",
        "",
        "[server]",
        'host = "0.0.0.0"           # reachable from the reverse-proxy host',
        "port = 8770",
        'path_prefix = "/jasna"',
        "",
        "[stash]",
        f'url = "{stash_url}"',
        "",
        "[jasna]",
        f'url = "http://127.0.0.1:{port}"',
        "manage_process = true",
        f'binary = "{binary}"',
        f"stream_port = {port}",
        f'default_preset = "{default_preset}"',
        "",
    ]
    for name, p in presets.items():
        lines += [f"[presets.{name}]", f'description = "{p["description"]}"',
                  f"flags = {_toml_list(p['flags'])}", ""]
    lines += [
        "[auth]",
        'mode = "stash_cookie"      # the browser\'s Stash cookie is validated against Stash',
        "",
        "[cache]",
        "enabled = true",
        "max_gb = 5",
        "",
    ]
    with open(path, "w") as fh:
        fh.write("\n".join(lines))
    os.chmod(path, 0o600)


def _write_unit() -> str:
    unit_dir = os.path.expanduser("~/.config/systemd/user")
    os.makedirs(unit_dir, exist_ok=True)
    unit = f"""[Unit]
Description=Bridge between Stash jasna-switch and jasna --stream
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={REPO_DIR}
ExecStart={sys.executable} -m jasna_bridge -c {REPO_DIR}/bridge.toml
Restart=on-failure
RestartSec=5
KillMode=mixed
TimeoutStopSec=30

[Install]
WantedBy=default.target
"""
    path = os.path.join(unit_dir, UNIT_NAME)
    with open(path, "w") as fh:
        fh.write(unit)
    return path


def _systemctl_user(*args) -> bool:
    try:
        subprocess.run(["systemctl", "--user", *args], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return True
    except (OSError, subprocess.CalledProcessError) as err:
        print(f"  ! systemctl --user {' '.join(args)} failed: {err}", file=sys.stderr)
        return False


def install(args) -> int:
    cfg_path = os.path.join(REPO_DIR, "bridge.toml")
    print(f"stash-jasna-bridge install (repo {REPO_DIR})\n")

    binary = args.jasna_binary or _find_jasna_binary()
    if not binary:
        print("! Could not find the Jasna binary. Pass --jasna-binary /path/to/jasna.", file=sys.stderr)
        return 2
    print(f"  Jasna binary: {binary}")
    port = args.stream_port or _pick_stream_port()
    print(f"  Jasna stream port: {port}" + ("" if port == 8765 else "  (8765 was busy)"))
    print(f"  Stash URL: {args.stash_url}")

    presets = DEFAULT_PRESETS if args.presets == "hq" else {"default": {"description": "Jasna defaults", "flags": []}}
    default_preset = "hq" if args.presets == "hq" else "default"

    if os.path.exists(cfg_path) and not args.force:
        print(f"\n  {cfg_path} exists; leaving it (pass --force to overwrite).")
    else:
        _write_config(cfg_path, args.stash_url, binary, port, presets, default_preset)
        print(f"\n  Wrote {cfg_path} (chmod 600).")

    if args.no_unit:
        print("  Skipping the systemd unit (--no-unit).")
    else:
        unit_path = _write_unit()
        print(f"  Wrote {unit_path}.")
        _systemctl_user("daemon-reload")
        try:
            subprocess.run(["loginctl", "enable-linger", os.environ.get("USER", "")],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except OSError:
            pass
        if _systemctl_user("enable", "--now", UNIT_NAME):
            print("  Enabled and started stash-jasna-bridge (user unit, linger on).")

    ip = _lan_ip()
    print("\nTwo steps left, on the hosts the bridge cannot reach itself:\n")
    print("  1. Firewall (needs sudo on this host): keep Jasna localhost-only,")
    print("     let the reverse-proxy host reach 8770. Ready-made rules:")
    print(f"       sudo nft -f {REPO_DIR}/deploy/nftables-jasna.conf")
    print(f"       sudo cp {REPO_DIR}/deploy/nftables-jasna.conf /etc/nftables.d/")
    print("  2. Reverse proxy: add a /jasna location on your Stash host pointing at")
    print(f"       http://{ip}:8770")
    print("     (Nginx Proxy Manager / nginx / Caddy / Cloudflare Tunnel snippets:")
    print(f"       {REPO_DIR}/deploy/reverse-proxy.md )")
    print("\nThen open a scene in Stash: the plugin auto-detects the bridge at <stash>/jasna.")
    print("Check with:  python3 -m jasna_bridge doctor")
    return 0


def _check(label, ok, detail=""):
    mark = "OK  " if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f" - {detail}" if detail else ""))
    return ok


def doctor(args) -> int:
    print("stash-jasna-bridge doctor\n")
    cfg_path = args.config
    if not os.path.exists(cfg_path):
        alt = os.path.join(REPO_DIR, "bridge.toml")
        if os.path.exists(alt):
            cfg_path = alt
    try:
        cfg = config.load(cfg_path)
    except (OSError, ValueError) as err:
        _check(f"config {cfg_path}", False, str(err))
        return 1
    _check(f"config {cfg_path}", True)
    _check(f"Python {sys.version.split()[0]} (need 3.11+)", sys.version_info >= (3, 11))

    ok = True
    if cfg.manage_process:
        ok &= _check(f"Jasna binary {cfg.jasna_binary}",
                     bool(cfg.jasna_binary) and os.path.isfile(cfg.jasna_binary) and os.access(cfg.jasna_binary, os.X_OK))
        from .jasna import jasna_license_candidates
        lic_paths = [os.path.expanduser(cfg.jasna_license_file)] if cfg.jasna_license_file else jasna_license_candidates()
        lic_found = None
        for lp in lic_paths:
            try:
                with open(lp) as fh:
                    d = json.load(fh)
                lic_found = (lp, bool(d.get("email") and d.get("key")))
                break
            except (OSError, ValueError):
                continue
        if lic_found:
            _check(f"Jasna license {lic_found[0]}", lic_found[1],
                   "unet-4x enabled" if lic_found[1] else "no key: unet-4x disabled")
        else:
            _check("Jasna license", False,
                   f"not found (looked in {', '.join(lic_paths)}); unet-4x disabled, hq still works")
        free = _port_free(cfg.jasna_stream_port)
        running = subprocess.run(["pgrep", "-f", f"stream-port {cfg.jasna_stream_port}"],
                                 stdout=subprocess.DEVNULL).returncode == 0 if shutil.which("pgrep") else False
        _check(f"Jasna stream port {cfg.jasna_stream_port}", free or running,
               "in use by our Jasna" if running else "free" if free else "busy (another process?)")

    gpu = shutil.which("nvidia-smi")
    if gpu:
        try:
            name = subprocess.run([gpu, "--query-gpu=name", "--format=csv,noheader"],
                                  capture_output=True, text=True, timeout=10).stdout.strip().splitlines()
            _check("NVIDIA GPU", bool(name), name[0] if name else "")
        except (OSError, subprocess.SubprocessError):
            _check("NVIDIA GPU", False, "nvidia-smi did not answer")
    else:
        _check("NVIDIA GPU", False, "nvidia-smi not found")

    # Stash reachable + a sample media path visible from here (after path mapping)
    try:
        body = json.dumps({"query": "{ findScenes(filter:{per_page:1}){ scenes { files { path } } } }"}).encode()
        req = urllib.request.Request(cfg.stash_url + "/graphql", data=body,
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode()).get("data") or {}
        _check(f"Stash GraphQL {cfg.stash_url}", True)
        scenes = (data.get("findScenes") or {}).get("scenes") or []
        if scenes and scenes[0].get("files"):
            stash_path = scenes[0]["files"][0]["path"]
            mapped = stash_path
            for sr, jr in cfg.path_map:
                if stash_path.startswith(sr):
                    mapped = jr + stash_path[len(sr):]
                    break
            _check("sample media path visible to the bridge", os.path.exists(mapped),
                   mapped if os.path.exists(mapped) else f"{mapped} not found (check [[paths.map]])")
    except (urllib.error.URLError, OSError, ValueError) as err:
        _check(f"Stash GraphQL {cfg.stash_url}", False, str(err))

    cache_dir = os.path.expanduser(cfg.cache_dir) or os.path.expanduser("~/.cache/stash-jasna-bridge/segments")
    try:
        os.makedirs(cache_dir, exist_ok=True)
        _check(f"cache dir {cache_dir} writable", os.access(cache_dir, os.W_OK))
    except OSError as err:
        _check(f"cache dir {cache_dir}", False, str(err))

    # Is the bridge itself answering?
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{cfg.port}{cfg.path_prefix}/health", timeout=5) as resp:
            json.loads(resp.read().decode())
        _check(f"bridge answering on :{cfg.port}{cfg.path_prefix}", True)
    except (urllib.error.URLError, OSError, ValueError):
        _check(f"bridge answering on :{cfg.port}", False, "not running? start the unit: systemctl --user start stash-jasna-bridge")
    return 0
