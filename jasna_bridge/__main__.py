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
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
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
