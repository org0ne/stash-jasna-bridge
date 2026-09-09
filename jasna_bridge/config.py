"""TOML config loading with defaults. See bridge.toml.example."""
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field


@dataclass
class Preset:
    name: str
    flags: list[str] = field(default_factory=list)
    description: str = ""


@dataclass
class Config:
    # [server]
    host: str = "127.0.0.1"
    port: int = 8770
    # Stripped from incoming request paths when present, so the bridge can sit
    # under a reverse-proxy location like /jasna/ without rewrite rules.
    path_prefix: str = "/jasna"
    # Origins allowed to call the bridge cross-origin (plain-HTTP Stash on
    # another origin, or a dev setup). "*" allows any origin. Same-origin
    # deployments (bridge under the Stash domain) need nothing here.
    cors_origins: list[str] = field(default_factory=list)

    # [stash]
    stash_url: str = "http://127.0.0.1:9999"
    stash_api_key: str = ""
    # [[paths.map]]: (stash_root, jasna_root) prefix rewrites, first match wins.
    path_map: list[tuple[str, str]] = field(default_factory=list)

    # [jasna]
    jasna_url: str = "http://127.0.0.1:8765"
    manage_process: bool = False
    jasna_binary: str = ""
    jasna_workdir: str = ""  # cwd for the spawned process; default: the binary's directory
    jasna_log_file: str = ""  # append Jasna's stdout/stderr here (managed only); empty = inherit
    jasna_stream_port: int = 8765
    jasna_common_flags: list[str] = field(default_factory=list)
    jasna_start_timeout_s: float = 180.0
    jasna_open_timeout_s: float = 60.0
    process_idle_minutes: float = 15.0
    prewarm_path: str = ""
    default_preset: str = "default"

    # [presets.<name>]
    presets: dict[str, Preset] = field(default_factory=dict)

    # [session]
    heartbeat_idle_s: float = 90.0
    stream_linger_s: float = 120.0
    reaper_interval_s: float = 5.0
    takeover_idle_s: float = 20.0   # a forced request may pre-empt an owner idle at least this long

    # [auth]
    auth_mode: str = "none"  # none | token | stash_cookie
    auth_token: str = ""
    auth_cache_s: float = 300.0

    # [cache]
    cache_enabled: bool = True
    cache_dir: str = ""          # default: ~/.cache/stash-jasna-bridge/segments
    cache_max_gb: float = 5.0
    cache_version: str = ""      # cache-key salt; default: parsed from jasna.binary path, else "v0"


def _get(table: dict, key: str, default):
    value = table.get(key, default)
    if default is not None and value is not None and not isinstance(value, type(default)):
        # allow int where float expected
        if isinstance(default, float) and isinstance(value, int):
            return float(value)
        raise ValueError(f"config key {key!r}: expected {type(default).__name__}, got {type(value).__name__}")
    return value


def from_dict(data: dict) -> Config:
    cfg = Config()
    server = data.get("server", {})
    cfg.host = _get(server, "host", cfg.host)
    cfg.port = _get(server, "port", cfg.port)
    cfg.path_prefix = _get(server, "path_prefix", cfg.path_prefix).rstrip("/")
    cfg.cors_origins = list(_get(server, "cors_origins", []))

    stash = data.get("stash", {})
    cfg.stash_url = _get(stash, "url", cfg.stash_url).rstrip("/")
    cfg.stash_api_key = _get(stash, "api_key", cfg.stash_api_key)

    paths = data.get("paths", {})
    for entry in paths.get("map", []):
        cfg.path_map.append((entry["stash"], entry["jasna"]))

    jasna = data.get("jasna", {})
    cfg.jasna_url = _get(jasna, "url", cfg.jasna_url).rstrip("/")
    cfg.manage_process = _get(jasna, "manage_process", cfg.manage_process)
    cfg.jasna_binary = _get(jasna, "binary", cfg.jasna_binary)
    cfg.jasna_workdir = _get(jasna, "workdir", cfg.jasna_workdir)
    cfg.jasna_log_file = _get(jasna, "log_file", cfg.jasna_log_file)
    cfg.jasna_stream_port = _get(jasna, "stream_port", cfg.jasna_stream_port)
    cfg.jasna_common_flags = list(_get(jasna, "common_flags", []))
    cfg.jasna_start_timeout_s = _get(jasna, "start_timeout_s", cfg.jasna_start_timeout_s)
    cfg.jasna_open_timeout_s = _get(jasna, "open_timeout_s", cfg.jasna_open_timeout_s)
    cfg.process_idle_minutes = _get(jasna, "process_idle_minutes", cfg.process_idle_minutes)
    cfg.prewarm_path = _get(jasna, "prewarm_path", cfg.prewarm_path)
    cfg.default_preset = _get(jasna, "default_preset", cfg.default_preset)

    for name, table in data.get("presets", {}).items():
        if not isinstance(table, dict):
            raise ValueError(f"[presets.{name}] must be a table")
        cfg.presets[name] = Preset(name, list(table.get("flags", [])), table.get("description", ""))
    if not cfg.presets:
        cfg.presets["default"] = Preset("default", [], "Jasna defaults")
    if cfg.default_preset not in cfg.presets:
        raise ValueError(f"default_preset {cfg.default_preset!r} is not a defined preset")

    session = data.get("session", {})
    cfg.heartbeat_idle_s = _get(session, "heartbeat_idle_s", cfg.heartbeat_idle_s)
    cfg.stream_linger_s = _get(session, "stream_linger_s", cfg.stream_linger_s)
    cfg.reaper_interval_s = _get(session, "reaper_interval_s", cfg.reaper_interval_s)
    cfg.takeover_idle_s = _get(session, "takeover_idle_s", cfg.takeover_idle_s)

    auth = data.get("auth", {})
    cfg.auth_mode = _get(auth, "mode", cfg.auth_mode)
    if cfg.auth_mode not in ("none", "token", "stash_cookie"):
        raise ValueError(f"auth.mode must be none, token or stash_cookie, not {cfg.auth_mode!r}")
    cfg.auth_token = _get(auth, "token", cfg.auth_token)
    if cfg.auth_mode == "token" and not cfg.auth_token:
        raise ValueError("auth.mode = token requires auth.token")
    cfg.auth_cache_s = _get(auth, "cache_s", cfg.auth_cache_s)

    cache = data.get("cache", {})
    cfg.cache_enabled = _get(cache, "enabled", cfg.cache_enabled)
    cfg.cache_dir = _get(cache, "dir", cfg.cache_dir)
    cfg.cache_max_gb = _get(cache, "max_gb", cfg.cache_max_gb)
    cfg.cache_version = _get(cache, "version", cfg.cache_version)

    if cfg.manage_process and not cfg.jasna_binary:
        raise ValueError("jasna.manage_process = true requires jasna.binary")
    return cfg


def load(path: str) -> Config:
    with open(path, "rb") as fh:
        return from_dict(tomllib.load(fh))
