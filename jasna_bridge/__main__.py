"""python -m jasna_bridge -c bridge.toml"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading

from . import VERSION, config
from .cache import SegmentCache
from .jasna import JasnaClient, ProcessManager
from .server import serve
from .sessions import SessionManager
from .stash import StashClient


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="jasna_bridge", description="Bridge between the Stash jasna-switch plugin and jasna --stream")
    ap.add_argument("-c", "--config", default="bridge.toml")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--version", action="version", version=VERSION)
    sub = ap.add_subparsers(dest="cmd")
    ins = sub.add_parser("install", help="write bridge.toml + a systemd user unit and start it")
    ins.add_argument("--stash-url", default="http://127.0.0.1:9999")
    ins.add_argument("--presets", choices=["hq", "minimal"], default="hq",
                     help="hq: hq+fast presets (default); minimal: Jasna defaults only")
    ins.add_argument("--jasna-binary", default="")
    ins.add_argument("--stream-port", type=int, default=0)
    ins.add_argument("--no-unit", action="store_true", help="write config only, no systemd unit")
    ins.add_argument("--force", action="store_true", help="overwrite an existing bridge.toml")
    sub.add_parser("doctor", help="check a deployment and print pass/fail")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.cmd == "install":
        from .cli import install
        return install(args)
    if args.cmd == "doctor":
        from .cli import doctor
        return doctor(args)
    try:
        cfg = config.load(args.config)
    except (OSError, ValueError) as err:
        print(f"config error: {err}", file=sys.stderr)
        return 2

    jasna = JasnaClient(cfg.jasna_url)
    procs = ProcessManager(cfg, jasna)
    cache = SegmentCache(cfg) if cfg.cache_enabled else None
    sessions = SessionManager(cfg, jasna, procs, cache)
    stash = StashClient(cfg.stash_url, cfg.stash_api_key)
    sessions.start()
    server = serve(cfg, sessions, stash)

    done = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: done.set())
    done.wait()
    logging.getLogger("bridge").info("shutting down")
    server.shutdown()
    sessions.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
