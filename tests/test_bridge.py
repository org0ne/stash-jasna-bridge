"""End-to-end tests: real bridge HTTP server against fake Stash and fake Jasna.
Run: python3 -m unittest -v tests.test_bridge"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jasna_bridge import config  # noqa: E402
from jasna_bridge.cache import SegmentCache  # noqa: E402
from jasna_bridge.jasna import JasnaClient, ProcessManager  # noqa: E402
from jasna_bridge.jobs import JobQueue  # noqa: E402
from jasna_bridge.mpegts import (JASNA_MUX_DELAY_S, looks_partial, partial_size, pcr_values, restamp,  # noqa: E402
                                 segment_span, track_spans, trim_leading_audio)
from jasna_bridge.playlist import retime  # noqa: E402
from jasna_bridge.server import serve  # noqa: E402
from jasna_bridge.sessions import SessionManager  # noqa: E402
from jasna_bridge.stash import StashClient  # noqa: E402
from tests import fake_jasna, fake_stash  # noqa: E402

FAKE_JASNA_BIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_jasna.py")


def free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class BridgeHarness:
    def __init__(self, overrides: dict | None = None, require_cookie: str | None = None, managed=False):
        self.stash_srv, self.stash_seen = fake_stash.start(require_cookie)
        if managed:
            self.jasna_srv, self.jasna = None, None
            jasna_port = free_port()
        else:
            self.jasna_srv, self.jasna = fake_jasna.start()
            jasna_port = self.jasna_srv.server_address[1]
        data = {
            "server": {"host": "127.0.0.1", "port": 0, "path_prefix": "/jasna", "cors_origins": ["http://stash.test"]},
            "stash": {"url": f"http://127.0.0.1:{self.stash_srv.server_address[1]}"},
            "jasna": {"url": f"http://127.0.0.1:{jasna_port}", "stream_port": jasna_port,
                      "open_timeout_s": 5, "start_timeout_s": 10, "default_preset": "a",
                      "manage_process": managed, "binary": FAKE_JASNA_BIN if managed else ""},
            "presets": {"a": {"flags": ["--x", "1"]}, "b": {"flags": ["--x", "2"]}},
            "session": {"heartbeat_idle_s": 0.6, "stream_linger_s": 0.6, "reaper_interval_s": 0.1,
                        "seek_spacing_s": 0},
            "cache": {"enabled": True, "dir": tempfile.mkdtemp(prefix="bridge-cache-"), "max_gb": 1.0},
        }
        for section, values in (overrides or {}).items():
            data.setdefault(section, {}).update(values)
        self.cfg = config.from_dict(data)
        self.cache_dir = self.cfg.cache_dir
        client = JasnaClient(self.cfg.jasna_url, timeout=3)
        self.procs = ProcessManager(self.cfg, client)
        self.cache = SegmentCache(self.cfg) if self.cfg.cache_enabled else None
        self.sessions = SessionManager(self.cfg, client, self.procs, self.cache)
        self.sessions.start()
        stash = StashClient(self.cfg.stash_url)
        self.jobs = None
        if self.cfg.batch_enabled:
            self.jobs = JobQueue(self.cfg, self.sessions, stash, lambda p: config.map_path(self.cfg, p))
            self.jobs.POLL_S = 0.2
            self.jobs.start()
        self.server = serve(self.cfg, self.sessions, stash, self.jobs)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown(); self.server.server_close()
        if self.jobs:
            self.jobs.shutdown()
        self.sessions.shutdown()
        self.stash_srv.shutdown(); self.stash_srv.server_close()
        if self.jasna_srv:
            self.jasna_srv.shutdown(); self.jasna_srv.server_close()
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def call(self, method, path, body=None, headers=None, prefix="/jasna"):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + prefix + path, data=data, method=method,
                                     headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if resp.headers.get_content_type() == "application/json" else raw), resp.headers
        except urllib.error.HTTPError as err:
            with err:
                raw = err.read()
            try:
                return err.code, json.loads(raw), err.headers
            except ValueError:
                return err.code, raw, err.headers


class SessionFlow(unittest.TestCase):
    def setUp(self):
        self.h = BridgeHarness()

    def tearDown(self):
        self.h.close()

    def test_create_stream_heartbeat_end_linger(self):
        h = self.h
        st, body, _ = h.call("POST", "/session", {"scene_id": "1", "time": 12.5})
        self.assertEqual(st, 200, body)
        self.assertFalse(body["reused"]); self.assertFalse(body["cold"])
        self.assertEqual(h.jasna.opens, ["/media/a/one.mp4"])
        token = body["token"]
        st, pl, hdr = h.call("GET", body["playlist_path"])
        self.assertEqual(st, 200)
        self.assertIn(b"seg_00000.ts", pl)
        self.assertEqual(hdr["Content-Type"], "application/vnd.apple.mpegurl")
        st, seg, hdr = h.call("GET", f"/hls/{token}/seg_00003.ts")
        self.assertEqual(st, 200)
        self.assertIn(b"seg_00003.ts:", seg)
        self.assertEqual(hdr["Content-Type"], "video/mp2t")
        st, hb, _ = h.call("POST", f"/session/{token}/heartbeat", {"time": 20, "paused": False})
        self.assertEqual(st, 200); self.assertEqual(hb["segments"], 1)
        st, snap, _ = h.call("GET", "/session")
        self.assertTrue(snap["active"]); self.assertEqual(snap["session"]["time"], 20)
        st, _, _ = h.call("DELETE", f"/session/{token}")
        self.assertEqual(st, 200)
        st, _, _ = h.call("GET", f"/hls/{token}/stream.m3u8")
        self.assertEqual(st, 410)
        st, snap, _ = h.call("GET", "/session")
        self.assertFalse(snap["active"]); self.assertEqual(snap["stream"]["path"], "/media/a/one.mp4")
        # same file within the linger window: reused, no second /open
        st, body2, _ = h.call("POST", "/session", {"scene_id": "1"})
        self.assertEqual(st, 200); self.assertTrue(body2["reused"])
        self.assertEqual(len(h.jasna.opens), 1)
        h.call("POST", f"/session/{body2['token']}/end")
        time.sleep(1.2)
        self.assertEqual(h.jasna.stops, 1)
        st, snap, _ = h.call("GET", "/session")
        self.assertIsNone(snap["stream"]["path"])

    def test_takeover_when_owner_idle(self):
        self.h.close()
        self.h = BridgeHarness({"session": {"heartbeat_idle_s": 5, "takeover_idle_s": 0.4, "stream_linger_s": 0.6, "reaper_interval_s": 0.1}})
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        # fresh owner: not takeover-able yet
        st, busy, _ = h.call("POST", "/session", {"scene_id": "2"})
        self.assertEqual(st, 409); self.assertFalse(busy["takeover_available"])
        st, denied, _ = h.call("POST", "/session", {"scene_id": "2", "force": True})
        self.assertEqual(st, 409, denied)  # force too early is still refused
        time.sleep(0.6)  # owner now idle past takeover_idle_s but below heartbeat_idle_s
        st, busy2, _ = h.call("POST", "/session", {"scene_id": "2"})
        self.assertEqual(st, 409); self.assertTrue(busy2["takeover_available"])
        st, b, _ = h.call("POST", "/session", {"scene_id": "2", "force": True})
        self.assertEqual(st, 200, b)
        self.assertEqual(h.jasna.opens, ["/media/a/one.mp4", "/media/b/two.mp4"])
        st, _, _ = h.call("POST", f"/session/{a['token']}/heartbeat", {})
        self.assertEqual(st, 410)  # old owner was released

    def test_takeover_of_paused_owner_despite_heartbeats(self):
        # A paused tab keeps heartbeating (so it is not idle-released) but must
        # still become takeover-able; an unpaused heartbeat resets that clock.
        self.h.close()
        self.h = BridgeHarness({"session": {"heartbeat_idle_s": 5, "takeover_idle_s": 0.4, "stream_linger_s": 0.6, "reaper_interval_s": 0.1}})
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        for _ in range(3):
            time.sleep(0.2)
            st, _, _ = h.call("POST", f"/session/{a['token']}/heartbeat", {"time": 1, "paused": True})
            self.assertEqual(st, 200)
        st, busy, _ = h.call("POST", "/session", {"scene_id": "2"})
        self.assertEqual(st, 409)
        self.assertTrue(busy["takeover_available"], busy)  # paused 0.6s > takeover_idle_s, heartbeats notwithstanding
        self.assertTrue(busy["paused"]); self.assertLess(busy["idle_seconds"], 0.4)
        # an unpaused heartbeat means the owner is watching again: no takeover
        st, _, _ = h.call("POST", f"/session/{a['token']}/heartbeat", {"time": 2, "paused": False})
        st, busy2, _ = h.call("POST", "/session", {"scene_id": "2"})
        self.assertEqual(st, 409); self.assertFalse(busy2["takeover_available"], busy2)
        st, denied, _ = h.call("POST", "/session", {"scene_id": "2", "force": True})
        self.assertEqual(st, 409, denied)
        # pause again, wait, and the takeover goes through
        h.call("POST", f"/session/{a['token']}/heartbeat", {"time": 2, "paused": True})
        time.sleep(0.5)
        st, b, _ = h.call("POST", "/session", {"scene_id": "2", "force": True})
        self.assertEqual(st, 200, b)
        st, _, _ = h.call("POST", f"/session/{a['token']}/heartbeat", {})
        self.assertEqual(st, 410)

    def test_presets_reports_warmth(self):
        st, p, _ = self.h.call("GET", "/presets")
        self.assertEqual(st, 200); self.assertFalse(p["warm"])
        _, a, _ = self.h.call("POST", "/session", {"scene_id": "1"})
        st, p, _ = self.h.call("GET", "/presets")
        self.assertTrue(p["warm"]); self.assertIn("a", [x["name"] for x in p["presets"]])

    def test_busy_then_idle_preempt(self):
        h = self.h
        st, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        self.assertEqual(st, 200)
        st, busy, _ = h.call("POST", "/session", {"scene_id": "2"})
        self.assertEqual(st, 409); self.assertEqual(busy["scene_id"], "1"); self.assertEqual(busy["reason"], "active")
        h.call("POST", f"/session/{a['token']}/heartbeat", {})
        time.sleep(0.9)  # no heartbeat -> idle -> released by reaper
        st, hb, _ = h.call("POST", f"/session/{a['token']}/heartbeat", {})
        self.assertEqual(st, 410)
        st, b, _ = h.call("POST", "/session", {"scene_id": "2"})
        self.assertEqual(st, 200); self.assertFalse(b["reused"])
        self.assertEqual(h.jasna.opens, ["/media/a/one.mp4", "/media/b/two.mp4"])
        st, _, _ = h.call("GET", f"/hls/{a['token']}/seg_00000.ts")
        self.assertEqual(st, 410)  # stale token cannot pull the new owner's segments

    def test_segment_fetch_counts_as_activity(self):
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        for _ in range(4):
            time.sleep(0.3)
            st, _, _ = h.call("GET", f"/hls/{a['token']}/seg_00001.ts")
            self.assertEqual(st, 200)
        st, _, _ = h.call("POST", f"/session/{a['token']}/heartbeat", {})
        self.assertEqual(st, 200)

    def test_unknown_scene_and_bad_input(self):
        st, body, _ = self.h.call("POST", "/session", {"scene_id": "999"})
        self.assertEqual(st, 404)
        st, body, _ = self.h.call("POST", "/session", {"scene_id": "abc"})
        self.assertEqual(st, 400)
        st, body, _ = self.h.call("POST", "/session", {"scene_id": "1", "preset": "zzz"})
        self.assertEqual(st, 400)
        st, _, _ = self.h.call("GET", "/nope")
        self.assertEqual(st, 404)
        st, _, _ = self.h.call("DELETE", "/session")
        self.assertEqual(st, 405)

    def test_prefix_optional_and_cors(self):
        st, body, _ = self.h.call("GET", "/health", prefix="")
        self.assertEqual(st, 200); self.assertTrue(body["jasna"]["reachable"])
        st, body, hdr = self.h.call("GET", "/health", headers={"Origin": "http://stash.test"})
        self.assertEqual(hdr["Access-Control-Allow-Origin"], "http://stash.test")
        self.assertEqual(hdr["Access-Control-Allow-Credentials"], "true")
        st, body, hdr = self.h.call("GET", "/health", headers={"Origin": "http://evil.test"})
        self.assertIsNone(hdr.get("Access-Control-Allow-Origin"))
        req = urllib.request.Request(self.h.base + "/jasna/session", method="OPTIONS", headers={"Origin": "http://stash.test"})
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 204)
            self.assertIn("DELETE", resp.headers["Access-Control-Allow-Methods"])

    def test_path_map(self):
        self.h.close()
        self.h = BridgeHarness({"paths": {"map": [{"stash": "/media", "jasna": "/mnt/pool"}]}})
        st, body, _ = self.h.call("POST", "/session", {"scene_id": "2"})
        self.assertEqual(st, 200)
        self.assertEqual(self.h.jasna.opens, ["/mnt/pool/b/two.mp4"])


class Jasna011Dialect(unittest.TestCase):
    """Jasna 0.11 changed three things the bridge touches (verified live 2026-10-16):
    playlist entries read seg_NNNNN.ts?epoch=N, POST /stop is gone in favour of
    DELETE /open, and /status omits "path" while idle."""

    def setUp(self):
        self.h = BridgeHarness()
        self.h.jasna.epoch_urls = True
        self.h.jasna.delete_only = True

    def tearDown(self):
        self.h.close()

    def test_epoch_playlist_is_counted_retimed_and_served(self):
        h = self.h
        st, body, _ = h.call("POST", "/session", {"scene_id": "1", "time": 0})
        self.assertEqual(st, 200, body)
        token = body["token"]
        st, pl, _ = h.call("GET", body["playlist_path"])
        self.assertEqual(st, 200)
        self.assertIn(b"seg_00000.ts?epoch=1", pl)
        # The cache counted the entries despite the query (a 0 count would never mark a stream complete).
        key = h.sessions.get(token).cache_key
        self.assertGreater(h.cache.metas[key]["segments"], 0)
        # hls.js asks for the entry as written; the bridge serves it by name.
        st, seg, hdr = h.call("GET", f"/hls/{token}/seg_00000.ts?epoch=1")
        self.assertEqual(st, 200)
        self.assertIn(b"seg_00000.ts:", seg)
        self.assertEqual(hdr["X-Bridge-Cache"], "miss")
        # Re-timing still finds the entries: the playlist is re-served with the
        # fetched segment's real start and the header that says so.
        st, pl2, hdr2 = h.call("GET", body["playlist_path"])
        self.assertEqual(st, 200)
        self.assertIn(b"seg_00000.ts?epoch=1", pl2)
        self.assertEqual(retime(pl2.decode(), {0: 1.4 + 0.25}, 1.4)[1], 1)

    def test_segment_fetches_carry_the_current_epoch(self):
        # The fake 404s any segment request without ?epoch=<current>, as 0.11
        # does for segments not yet rendered (and it never seeks for them).
        h = self.h
        st, body, _ = h.call("POST", "/session", {"scene_id": "1", "time": 30})
        self.assertEqual(st, 200, body)
        token = body["token"]
        self.assertEqual(h.sessions.jasna.epoch, "1")
        st, seg, _ = h.call("GET", f"/hls/{token}/seg_00007.ts?epoch=1")
        self.assertEqual(st, 200, seg)
        self.assertIn(b"seg_00007.ts:", seg)
        # A second /open (another file) bumps the epoch and the client follows.
        h.call("POST", f"/session/{token}/end")
        st, body2, _ = h.call("POST", "/session", {"scene_id": "2", "time": 0})
        self.assertEqual(st, 200, body2)
        self.assertEqual(h.sessions.jasna.epoch, "2")
        st, seg, _ = h.call("GET", f"/hls/{body2['token']}/seg_00001.ts?epoch=2")
        self.assertEqual(st, 200, seg)

    def test_stream_closed_with_delete_open(self):
        h = self.h
        st, body, _ = h.call("POST", "/session", {"scene_id": "1", "time": 0})
        self.assertEqual(st, 200, body)
        h.call("POST", f"/session/{body['token']}/end")
        time.sleep(1.2)
        self.assertEqual(h.jasna.stops, 1)  # closed via DELETE /open; POST /stop 404s on this dialect
        st, snap, _ = h.call("GET", "/session")
        self.assertIsNone(snap["stream"]["path"])

    def test_stop_falls_back_to_post_stop_on_0_10(self):
        # A 0.10 server knows no DELETE /open (404); the client must then POST /stop.
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        seen = []

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _reply(self, code):
                self.send_response(code); self.send_header("Content-Length", "2"); self.end_headers(); self.wfile.write(b"{}")

            def do_DELETE(self):
                seen.append(("DELETE", self.path)); self._reply(404)

            def do_POST(self):
                seen.append(("POST", self.path)); self._reply(200)

        srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        import threading
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            self.assertTrue(JasnaClient(f"http://127.0.0.1:{srv.server_address[1]}").stop())
        finally:
            srv.shutdown()
        self.assertEqual(seen, [("DELETE", "/open"), ("POST", "/stop")])


class Auth(unittest.TestCase):
    def test_token(self):
        h = BridgeHarness({"auth": {"mode": "token", "token": "s3cret"}})
        try:
            st, _, _ = h.call("POST", "/session", {"scene_id": "1"})
            self.assertEqual(st, 401)
            st, body, _ = h.call("POST", "/session", {"scene_id": "1"}, headers={"Authorization": "Bearer s3cret"})
            self.assertEqual(st, 200)
            # token-in-path routes need no header (sendBeacon cannot set one)
            st, _, _ = h.call("POST", f"/session/{body['token']}/heartbeat", {})
            self.assertEqual(st, 200)
            st, _, _ = h.call("GET", "/health")
            self.assertEqual(st, 200)
        finally:
            h.close()

    def test_stash_cookie(self):
        h = BridgeHarness({"auth": {"mode": "stash_cookie", "cache_s": 60}}, require_cookie="session=good")
        try:
            st, _, _ = h.call("GET", "/session")
            self.assertEqual(st, 401)
            st, _, _ = h.call("GET", "/session", headers={"Cookie": "session=bad"})
            self.assertEqual(st, 401)
            st, _, _ = h.call("GET", "/session", headers={"Cookie": "session=good"})
            self.assertEqual(st, 200)
            n = len(h.stash_seen)
            h.call("GET", "/session", headers={"Cookie": "session=good"})
            self.assertEqual(len(h.stash_seen), n)  # cached, no second GraphQL call
        finally:
            h.close()


class License(unittest.TestCase):
    """The bridge reads the Jasna license from its store so bridge.toml need not hold it."""

    def _pm(self, overrides=None):
        cfg = config.from_dict({"jasna": {"binary": "/bin/true", "manage_process": True,
                                          "stream_port": 8765, "default_preset": "a", **(overrides or {})},
                                "presets": {"a": {"flags": ["--x", "1"]}}})
        from jasna_bridge.jasna import ProcessManager, JasnaClient
        return ProcessManager(cfg, JasnaClient(cfg.jasna_url))

    def test_license_flags_appended_from_file(self):
        import tempfile
        d = tempfile.mkdtemp()
        lic = os.path.join(d, "license.json")
        with open(lic, "w") as fh:
            json.dump({"email": "a@b.c", "key": "SECRET"}, fh)
        pm = self._pm({"license_file": lic})
        cmd = pm.command("a")
        self.assertEqual(cmd[cmd.index("--license-email") + 1], "a@b.c")
        self.assertEqual(cmd[cmd.index("--license-key") + 1], "SECRET")

    def test_license_not_appended_when_in_common_flags(self):
        import tempfile
        d = tempfile.mkdtemp()
        lic = os.path.join(d, "license.json")
        with open(lic, "w") as fh:
            json.dump({"email": "a@b.c", "key": "SECRET"}, fh)
        pm = self._pm({"license_file": lic, "common_flags": ["--license-email", "manual@x.y", "--license-key", "MANUAL"]})
        cmd = pm.command("a")
        self.assertEqual(cmd.count("--license-email"), 1)
        self.assertEqual(cmd[cmd.index("--license-email") + 1], "manual@x.y")

    def test_missing_license_file_is_harmless(self):
        pm = self._pm({"license_file": "/no/such/license.json"})
        cmd = pm.command("a")
        self.assertNotIn("--license-email", cmd)

    def _binary(self, dirname: str, help_text: str) -> str:
        """An executable in a directory named `dirname` that prints `help_text` for --help."""
        d = os.path.join(tempfile.mkdtemp(prefix="bridge-lic-"), dirname)
        os.makedirs(d)
        bin_path = os.path.join(d, "jasna")
        with open(bin_path, "w") as fh:
            fh.write("#!/bin/sh\nprintf '%s\\n' \"" + help_text + "\"\n")
        os.chmod(bin_path, 0o755)
        return bin_path

    def _license_file(self) -> str:
        d = tempfile.mkdtemp(prefix="bridge-lic-")
        lic = os.path.join(d, "license.json")
        with open(lic, "w") as fh:
            json.dump({"email": "a@b.c", "key": "SECRET"}, fh)
        return lic

    def test_license_flags_skipped_for_jasna_0_11_by_path(self):
        # 0.11 rejects --license-email/--license-key outright (argparse exit 2);
        # the version in the install folder's name settles it without a probe.
        bin_path = self._binary("jasna-linux-nvidia-0.11.1", "usage: never probed")
        pm = self._pm({"license_file": self._license_file(), "binary": bin_path})
        cmd = pm.command("a")
        self.assertNotIn("--license-email", cmd)
        self.assertNotIn("--license-key", cmd)
        self.assertFalse(pm.license_flags_supported())

    def test_license_flags_kept_for_jasna_0_10_by_path(self):
        bin_path = self._binary("jasna-linux-nvidia-0.10.0", "usage: never probed")
        pm = self._pm({"license_file": self._license_file(), "binary": bin_path})
        cmd = pm.command("a")
        self.assertEqual(cmd[cmd.index("--license-key") + 1], "SECRET")

    def test_license_flags_decided_by_help_probe_without_path_version(self):
        from jasna_bridge import jasna as jm
        new = self._binary("jasna", "usage: jasna [--machine-key] [--activation-key KEY]")
        old = self._binary("jasna", "usage: jasna [--license-email EMAIL] [--license-key KEY]")
        self.assertIs(jm.jasna_accepts_license_flags(new), False)
        self.assertIs(jm.jasna_accepts_license_flags(old), True)
        self.assertIsNone(jm.jasna_accepts_license_flags("/bin/true"))  # says neither
        lic = self._license_file()
        self.assertNotIn("--license-key", self._pm({"license_file": lic, "binary": new}).command("a"))
        self.assertIn("--license-key", self._pm({"license_file": lic, "binary": old}).command("a"))
        # Inconclusive: keep the pre-0.11 behaviour.
        self.assertIn("--license-key", self._pm({"license_file": lic, "binary": "/bin/true"}).command("a"))

    def test_manual_license_flags_not_duplicated_but_warned_on_0_11(self):
        bin_path = self._binary("jasna-linux-nvidia-0.11.1", "usage: never probed")
        pm = self._pm({"license_file": self._license_file(), "binary": bin_path,
                       "common_flags": ["--license-email", "m@x.y", "--license-key", "MANUAL"]})
        with self.assertLogs("bridge.jasna", level="WARNING") as logs:
            cmd = pm.command("a")
        self.assertEqual(cmd.count("--license-key"), 1)  # the user's own flag is passed through as before
        self.assertTrue(any("rejects" in line for line in logs.output))

    def test_license_candidates_are_platform_appropriate(self):
        import importlib, jasna_bridge.jasna as jm
        orig = sys.platform
        try:
            sys.platform = "win32"
            os.environ["LOCALAPPDATA"] = r"C:\Users\bob\AppData\Local"
            os.environ["APPDATA"] = r"C:\Users\bob\AppData\Roaming"
            importlib.reload(jm)
            cands = jm.jasna_license_candidates()
            self.assertTrue(all(c.endswith("license.json") for c in cands))
            self.assertTrue(any("Local" in c and "jasna" in c for c in cands))
            self.assertGreaterEqual(len(cands), 2)
            sys.platform = "darwin"
            importlib.reload(jm)
            self.assertTrue(any("Application Support" in c for c in jm.jasna_license_candidates()))
        finally:
            sys.platform = orig
            importlib.reload(jm)


class Cache(unittest.TestCase):
    """Phase C: segments are cached on disk; a complete stream replays with no Jasna."""

    SEG_BYTES = fake_jasna.SEG_BYTES

    def setUp(self):
        self.h = BridgeHarness()

    def tearDown(self):
        self.h.close()

    def fetch_all(self, h, token, n):
        for i in range(n):
            st, _, hdr = h.call("GET", f"/hls/{token}/seg_{i:05d}.ts")
            self.assertEqual(st, 200, i)
        return hdr

    def test_segment_served_from_cache_on_second_request(self):
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        self.assertFalse(a["cached"])
        st, body1, hdr1 = h.call("GET", f"/hls/{a['token']}/seg_00002.ts")
        st, body2, hdr2 = h.call("GET", f"/hls/{a['token']}/seg_00002.ts")
        self.assertEqual(body1, body2)
        self.assertEqual((hdr1["X-Bridge-Cache"], hdr2["X-Bridge-Cache"]), ("miss", "hit"))
        self.assertEqual(h.jasna.segments, ["seg_00002.ts"])  # Jasna asked once
        snap = h.cache.snapshot()
        self.assertEqual((snap["segments"], snap["hits"], snap["stored"]), (1, 1, 1))
        key = h.sessions.current.cache_key
        self.assertTrue(os.path.exists(h.cache.file_path(key, "seg_00002.ts")))

    def test_complete_stream_replays_without_jasna(self):
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        st, pl, _ = h.call("GET", a["playlist_path"])
        n = pl.count(b"seg_")
        self.assertEqual(n, 11)  # fake_jasna: 40s / 4s + 1
        self.fetch_all(h, a["token"], n)
        key = h.sessions.current.cache_key
        self.assertTrue(h.cache.is_complete(key))
        h.call("DELETE", f"/session/{a['token']}")
        time.sleep(1.0)  # linger expires, stream closed
        self.assertEqual(h.jasna.stops, 1)
        segs_before = list(h.jasna.segments)
        # rewatch: no /open, playlist and every segment from disk
        st, b, _ = h.call("POST", "/session", {"scene_id": "1"})
        self.assertEqual(st, 200, b); self.assertTrue(b["cached"])
        self.assertEqual(len(h.jasna.opens), 1)
        st, pl2, hdr = h.call("GET", b["playlist_path"])
        self.assertEqual(st, 200)
        names = lambda p: [l for l in p.decode().splitlines() if l.endswith(".ts")]
        self.assertEqual(names(pl2), names(pl))   # same segments; durations are re-timed to the cached video
        self.assertIn("X-Bridge-Retimed", hdr)
        hdr = self.fetch_all(h, b["token"], n)
        self.assertEqual(hdr["X-Bridge-Cache"], "hit")
        self.assertEqual(h.jasna.segments, segs_before)
        st, snap, _ = h.call("GET", "/session")
        self.assertTrue(snap["session"]["from_cache"]); self.assertEqual(snap["stats"]["cached_sessions"], 1)
        self.assertIsNone(snap["stream"]["path"])  # Jasna never opened for this session

    def test_cache_hole_opens_jasna_lazily(self):
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        h.call("GET", a["playlist_path"])
        self.fetch_all(h, a["token"], 11)
        key = h.sessions.current.cache_key
        h.call("DELETE", f"/session/{a['token']}")
        time.sleep(1.0)
        os.unlink(h.cache.file_path(key, "seg_00005.ts"))  # a hole the index does not know about
        _, b, _ = h.call("POST", "/session", {"scene_id": "1"})
        self.assertTrue(b["cached"]); self.assertEqual(len(h.jasna.opens), 1)
        st, _, hdr = h.call("GET", f"/hls/{b['token']}/seg_00004.ts")
        self.assertEqual(hdr["X-Bridge-Cache"], "hit")
        st, body, hdr = h.call("GET", f"/hls/{b['token']}/seg_00005.ts")
        self.assertEqual(st, 200); self.assertEqual(hdr["X-Bridge-Cache"], "miss")
        self.assertIn(b"seg_00005.ts:", body)
        self.assertEqual(len(h.jasna.opens), 2)  # opened on demand, for the same file
        self.assertEqual(h.jasna.opens[-1], "/media/a/one.mp4")
        st, snap, _ = h.call("GET", "/session")
        self.assertFalse(snap["session"]["from_cache"]); self.assertEqual(snap["stream"]["path"], "/media/a/one.mp4")
        self.assertTrue(h.cache.is_complete(key))  # hole filled


    # --- seams: Jasna's passes disagree about where segment N starts ---
    # (measured 2026-09-30: first-of-pass segments start seconds early and run
    # long; later ones drift). fake_jasna copies that, so cached segments from
    # two passes do not line up and hls.js would strobe/stall at the join.

    def seg(self, h, token, i):
        st, body, hdr = h.call("GET", f"/hls/{token}/seg_{i:05d}.ts")
        self.assertEqual(st, 200, body)
        return hdr["X-Bridge-Cache"], segment_span(body)

    def test_segment_span_parser(self):
        self.assertIsNone(segment_span(b"not a transport stream" * 20))
        self.assertIsNone(segment_span(b""))
        start, end = segment_span(fake_jasna.ts_segment(10.0, 14.0))
        self.assertAlmostEqual(start, 10.0, places=3); self.assertAlmostEqual(end, 14.0, places=3)
        self.assertEqual(segment_span(fake_jasna.ts_segment(10.0, 14.0) + b"\x47trailing junk"), (start, end))

    def test_restamp_and_trim(self):
        seg = fake_jasna.ts_segment(18.0, 25.4, b"x", audio_start_s=17.0)
        self.assertEqual(track_spans(seg), {"video": (18.0, 25.4), "audio": (17.0, 25.4)})
        self.assertEqual(pcr_values(seg), [18.0])
        shifted = restamp(seg, 3.416)
        self.assertEqual(len(shifted), len(seg))
        sp = track_spans(shifted)
        self.assertAlmostEqual(sp["video"][0], 21.416, places=3); self.assertAlmostEqual(sp["audio"][0], 20.416, places=3)
        self.assertAlmostEqual(pcr_values(shifted)[0], 21.416, places=3)
        self.assertAlmostEqual(track_spans(restamp(seg, -2.0))["video"][0], 16.0, places=3)
        self.assertIs(restamp(seg, 0.0), seg)
        trimmed = trim_leading_audio(seg, 18.0)
        self.assertLess(len(trimmed), len(seg))
        self.assertEqual(track_spans(trimmed)["video"], (18.0, 25.4))
        self.assertGreaterEqual(track_spans(trimmed)["audio"][0], 18.0)
        self.assertEqual(trim_leading_audio(seg, 16.0), seg)  # nothing before the cutoff

    def test_seam_is_spliced_into_a_continuous_timeline(self):
        """Cached 0-4 from a pass at 0, then 5 has to come from a new Jasna
        pass: its first segment starts 3.4s before 4 ends and carries 1s of
        lead-in audio. The player must see one continuous timeline."""
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        t = a["token"]
        for i in range(5):
            self.seg(h, t, i)
        self.seg(h, t, 8)                                # park Jasna's pass elsewhere
        h.call("DELETE", f"/session/{t}")
        _, b, _ = h.call("POST", "/session", {"scene_id": "1"})
        t = b["token"]
        st, body4, hdr4 = h.call("GET", f"/hls/{t}/seg_00004.ts")
        st, body5, hdr5 = h.call("GET", f"/hls/{t}/seg_00005.ts")
        st, body6, hdr6 = h.call("GET", f"/hls/{t}/seg_00006.ts")
        self.assertEqual((hdr4["X-Bridge-Cache"], hdr5["X-Bridge-Cache"], hdr6["X-Bridge-Cache"]), ("hit", "miss", "miss"))
        self.assertNotIn("X-Bridge-Offset", hdr4)
        self.assertAlmostEqual(float(hdr5["X-Bridge-Offset"]), 3.416, places=2)
        self.assertEqual(hdr6["X-Bridge-Offset"], hdr5["X-Bridge-Offset"])  # the run keeps the offset
        s4, s5, s6 = track_spans(body4), track_spans(body5), track_spans(body6)
        self.assertLess(abs(s5["video"][0] - s4["video"][1]), 0.01)   # 4 -> 5 meets exactly
        self.assertLess(abs(s6["video"][0] - s5["video"][1]), 0.01)   # 5 -> 6 too
        self.assertGreaterEqual(s5["audio"][0], s5["video"][0] - 0.01)  # lead-in audio trimmed
        self.assertAlmostEqual(pcr_values(body5)[0], s5["video"][0], places=3)
        raw5 = h.cache.span(h.sessions.current.cache_key, "seg_00005.ts")
        self.assertAlmostEqual(raw5[0], 18.0, places=3)                 # the cache keeps Jasna's bytes
        st, snap, _ = h.call("GET", "/session")
        self.assertEqual(snap["stats"]["seams_restamped"], 1)
        self.assertNotIn("seam_rejects", h.cache.snapshot())
        # A seek is non-contiguous for hls.js: served as-is, offset dropped.
        st, body2, hdr2 = h.call("GET", f"/hls/{t}/seg_00002.ts")
        self.assertNotIn("X-Bridge-Offset", hdr2)
        self.assertAlmostEqual(track_spans(body2)["video"][0], 9.408, places=3)
        st, body3, hdr3 = h.call("GET", f"/hls/{t}/seg_00003.ts")
        self.assertNotIn("X-Bridge-Offset", hdr3)              # same pass as 2: nothing to splice
        self.assertEqual(h.jasna.passes, [0, 8, 5])

    def test_cached_segments_from_two_passes_are_spliced_not_rerendered(self):
        """The cache holds 0-4 from a pass at 0 and 5-6 from a pass at 5 (5 is
        first-of-pass: early and long). Replaying 4 -> 5 -> 6 comes entirely
        from the cache, spliced into one continuous timeline, with no new
        Jasna pass (re-rendering would cancel Jasna's pass and make a seam)."""
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        t = a["token"]
        self.seg(h, t, 5); self.seg(h, t, 6)
        for i in range(5):
            self.seg(h, t, i)
        h.call("DELETE", f"/session/{t}")
        _, b, _ = h.call("POST", "/session", {"scene_id": "1"})
        t = b["token"]
        got = {}
        for i in (4, 5, 6):
            st, body, hdr = h.call("GET", f"/hls/{t}/seg_{i:05d}.ts")
            self.assertEqual((st, hdr["X-Bridge-Cache"]), (200, "hit"), i)
            got[i] = (track_spans(body), hdr.get("X-Bridge-Offset"))
        self.assertIsNone(got[4][1])
        self.assertIsNotNone(got[5][1])
        self.assertEqual(got[6][1], got[5][1])
        self.assertLess(abs(got[5][0]["video"][0] - got[4][0]["video"][1]), 0.01)
        self.assertLess(abs(got[6][0]["video"][0] - got[5][0]["video"][1]), 0.01)
        self.assertEqual(h.jasna.passes, [5, 0])  # nothing re-rendered

    def test_stale_request_does_not_overwrite_seek_state(self):
        """An abandoned request finishing after a newer one (the viewer
        seeked while Jasna was rendering) must not record its segment."""
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        t = a["token"]
        sm = h.sessions
        self.seg(h, t, 0)
        s, old = sm.touch_segment(t)            # request for 1 starts...
        s, new = sm.touch_segment(t)            # ...then a seek to 7 starts
        sm.segment_served(t, 7, 33.0, ts_offset=0.0, seq=new["seq"])
        sm.segment_served(t, 1, 9.4, ts_offset=2.0, seq=old["seq"])   # the stale one lands last
        self.assertEqual((s.last_seg, s.last_seg_end, s.ts_offset), (7, 33.0, 0.0))

    def test_repeat_of_last_segment_keeps_the_offset(self):
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        t = a["token"]
        for i in range(5):
            self.seg(h, t, i)
        self.seg(h, t, 8)
        h.call("DELETE", f"/session/{t}")
        _, b, _ = h.call("POST", "/session", {"scene_id": "1"})
        t = b["token"]
        self.seg(h, t, 4)
        st, body5, hdr5 = h.call("GET", f"/hls/{t}/seg_00005.ts")     # spliced: offset
        st, again, hdr5b = h.call("GET", f"/hls/{t}/seg_00005.ts")    # hls.js retry
        self.assertIn("X-Bridge-Offset", hdr5)
        self.assertEqual(hdr5b.get("X-Bridge-Offset"), hdr5["X-Bridge-Offset"])
        self.assertEqual(track_spans(again)["video"], track_spans(body5)["video"])
        st, body6, hdr6 = h.call("GET", f"/hls/{t}/seg_00006.ts")     # and the run continues
        self.assertLess(abs(track_spans(body6)["video"][0] - track_spans(body5)["video"][1]), 0.01)

    def test_changed_source_file_drops_its_cache(self):
        h = self.h
        src = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        src.write(b"v1"); src.close()
        self.addCleanup(os.unlink, src.name)
        key = h.cache.key(src.name, "a", ["--x", "1"])
        h.cache.put(key, "seg_00000.ts", fake_jasna.ts_segment(0, 4))
        h.cache.store_manifest(key, src.name, "a", ["--x", "1"], b"#EXTM3U\nseg_00000.ts\n")
        self.assertTrue(h.cache.is_complete(key))
        h.cache.check_source(key, src.name)                 # unchanged: kept
        self.assertTrue(h.cache.is_complete(key))
        with open(src.name, "wb") as fh:
            fh.write(b"v2 is longer")
        h.cache.check_source(key, src.name)
        self.assertFalse(h.cache.is_complete(key))
        self.assertIsNone(h.cache.hit(key, "seg_00000.ts"))
        self.assertEqual(h.cache.snapshot()["source_changed"], 1)

    def test_concurrent_puts_of_one_segment(self):
        # A scrub can fetch the same segment twice at once (from two passes, so
        # different bytes). Seen 2026-10-06: a shared tmp name made the second
        # rename fail with ENOENT and left size/span describing the wrong bytes.
        import threading
        h = self.h
        key = h.cache.key("/x.mp4", "a", [])
        bodies = [fake_jasna.ts_segment(0, 4 + i / 10) for i in range(8)]
        barrier = threading.Barrier(len(bodies))

        def put(body):
            barrier.wait()
            h.cache.put(key, "seg_00000.ts", body)

        threads = [threading.Thread(target=put, args=(b,)) for b in bodies]
        with self.assertNoLogs("bridge.cache", level="WARNING"):
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        fn = h.cache.hit(key, "seg_00000.ts")
        with open(fn, "rb") as fh:
            on_disk = fh.read()
        self.assertIn(on_disk, bodies)
        self.assertEqual(h.cache.entries[(key, "seg_00000.ts")][0], len(on_disk))
        self.assertEqual(h.cache.span(key, "seg_00000.ts"), segment_span(on_disk))
        self.assertEqual(h.cache.snapshot()["bytes"], len(on_disk))
        self.assertEqual([n for n in os.listdir(os.path.dirname(fn)) if n.endswith(".tmp")], [])

    def test_scrub_burst_reaches_jasna_as_one_seek(self):
        # Every seek starts a Jasna pass and a pass start can deadlock Jasna,
        # so positions the viewer scrubbed past are never forwarded.
        import threading
        self.h.close()
        self.h = h = BridgeHarness({"session": {"seek_spacing_s": 0.5}})
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        tok = a["token"]
        st, _, _ = h.call("GET", f"/hls/{tok}/seg_00000.ts")  # a seek: Jasna was never asked anything
        self.assertEqual(st, 200)
        results = {}

        def get(i):
            results[i] = h.call("GET", f"/hls/{tok}/seg_{i:05d}.ts")[0]

        threads = []
        for i in (5, 7, 9):  # scrubbing within the spacing window
            t = threading.Thread(target=get, args=(i,)); t.start(); threads.append(t)
            time.sleep(0.05)
        for t in threads:
            t.join()
        self.assertEqual(results, {5: 503, 7: 503, 9: 200})
        self.assertEqual(h.jasna.segments, ["seg_00000.ts", "seg_00009.ts"])
        # sequential playback is never held back
        t0 = time.monotonic()
        self.assertEqual(h.call("GET", f"/hls/{tok}/seg_00010.ts")[0], 200)
        self.assertLess(time.monotonic() - t0, 0.3)
        # a lone seek after a quiet spell goes straight through
        time.sleep(0.6)
        t0 = time.monotonic()
        self.assertEqual(h.call("GET", f"/hls/{tok}/seg_00003.ts")[0], 200)
        self.assertLess(time.monotonic() - t0, 0.3)
        snap = h.call("GET", "/session")[1]["stats"]
        self.assertEqual((snap["seeks_forwarded"], snap["seeks_coalesced"]), (3, 2))

    def test_playlist_retime(self):
        pl = ("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-MEDIA-SEQUENCE:0\n"
              + "".join(f"#EXTINF:4.000,\nseg_{i:05d}.ts\n" for i in range(5)) + "#EXT-X-ENDLIST\n")
        # segments 1 and 2 are cached: their video really starts at 2.8s and 5.4s
        out, moved = retime(pl, {1: 2.8 + 1.4, 2: 5.4 + 1.4}, 1.4)
        durs = [float(l.split(":")[1].rstrip(",")) for l in out.splitlines() if l.startswith("#EXTINF")]
        self.assertEqual(moved, 2)
        self.assertEqual(durs, [2.8, 2.6, 6.6, 4.0, 4.0])      # 0->2.8, 2.8->5.4, 5.4->12 (nominal), 12->16, last kept
        self.assertIn("#EXT-X-TARGETDURATION:7", out)
        self.assertEqual(out.count("seg_0000"), 5)
        # nothing known, or everything already on its slot: untouched
        self.assertEqual(retime(pl, {}, 1.4), (pl, 0))
        self.assertEqual(retime(pl, {3: 12.0 + 1.4}, 1.4), (pl, 0))
        # a segment that starts after the next nominal slot never yields a negative duration
        out, _ = retime(pl, {1: 9.0 + 1.4}, 1.4)
        self.assertTrue(all(d > 0 for d in [float(l.split(":")[1].rstrip(",")) for l in out.splitlines() if l.startswith("#EXTINF")]))

    def test_playlist_positions_match_cached_segments(self):
        # hls.js pins its first fragment to the playlist position, so the
        # playlist must put every cached segment where its video really starts.
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        tok = a["token"]
        st, pl0, hdr = h.call("GET", f"/hls/{tok}/stream.m3u8")
        self.assertEqual(st, 200); self.assertNotIn("X-Bridge-Retimed", hdr)
        real = {}
        for seg in (3, 4, 5):  # a pass from 3: its first segment starts PASS_LEAD_S early
            st, body, _ = h.call("GET", f"/hls/{tok}/seg_{seg:05d}.ts")
            self.assertEqual(st, 200)
            real[seg] = track_spans(body)["video"][0] - JASNA_MUX_DELAY_S
        self.assertAlmostEqual(real[3], 12.0 - fake_jasna.PASS_LEAD_S - JASNA_MUX_DELAY_S, places=2)  # fake's first segment has no mux delay
        st, pl, hdr = h.call("GET", f"/hls/{tok}/stream.m3u8")
        self.assertEqual(st, 200)
        self.assertIn("X-Bridge-Retimed", hdr)
        durs = [float(l.split(":")[1].rstrip(",")) for l in pl.decode().splitlines() if l.startswith("#EXTINF")]
        pos = [0.0]
        for d in durs:
            pos.append(pos[-1] + d)
        for seg in (3, 4, 5):
            self.assertAlmostEqual(pos[seg], real[seg], places=2, msg=f"seg {seg}")
        self.assertAlmostEqual(pos[2], 8.0, places=3)     # untouched before the cached run
        self.assertAlmostEqual(pos[6], 24.0, places=2)    # back on the nominal grid after it
        # the timing survives a bridge restart without re-reading the segments
        key = h.sessions.current.cache_key
        fresh = SegmentCache(h.cfg)
        self.assertEqual(fresh.video_starts(key), h.cache.video_starts(key))
        self.assertEqual(sorted(fresh.video_starts(key)), [3, 4, 5])

    def test_lead_in_audio_is_dropped_from_every_pass_start(self):
        # A pass's first segment has audio from before its first frame (the fake
        # mimics Jasna's 1s; real passes carry 1.4-8s). hls.js anchors the
        # session clock to the earliest sample, so left in, the picture and all
        # subtitle cues sit that far apart for the whole session.
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        tok = a["token"]
        for seg, note in ((3, "first request = pass start"), (4, "sequential, same pass"), (8, "a seek = new pass")):
            st, body, hdr = h.call("GET", f"/hls/{tok}/seg_{seg:05d}.ts")
            self.assertEqual(st, 200, note)
            tr = track_spans(body)
            self.assertGreaterEqual(tr["audio"][0], tr["video"][0] - 0.05, f"{note}: audio must not start before video")
            if seg in (3, 8):
                self.assertAlmostEqual(float(hdr["X-Bridge-Audio-Trimmed"]), fake_jasna.AUDIO_LEAD_S, delta=0.1, msg=note)
            else:
                self.assertNotIn("X-Bridge-Audio-Trimmed", hdr, note)
        self.assertEqual(h.call("GET", "/health")[1]["stats"]["audio_leads_trimmed"], 2)
        # the cache keeps Jasna's bytes; a hit is trimmed again on the way out
        st, body, hdr = h.call("GET", f"/hls/{tok}/seg_00008.ts")
        self.assertEqual((st, hdr["X-Bridge-Cache"]), (200, "hit"))
        tr = track_spans(body)
        self.assertGreaterEqual(tr["audio"][0], tr["video"][0] - 0.05)

    def test_partial_segment_is_reread_until_complete(self):
        # Jasna serves a segment file as soon as it exists; at the live edge the
        # first read is of a file ffmpeg is still writing (seen 2026-10-11:
        # seg_01811 cached at 182 x 32 KiB, 87 of 120 frames). The bridge must
        # re-read until it is whole and never cache the short one.
        h = self.h
        h.jasna.partial_first = True
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        tok = a["token"]
        st, body, hdr = h.call("GET", f"/hls/{tok}/seg_00000.ts")
        self.assertEqual(st, 200)
        self.assertEqual(len(body) % 188, 0)
        self.assertFalse(looks_partial(body))
        self.assertEqual(h.jasna.segments, ["seg_00000.ts", "seg_00000.ts"], "one re-read")
        self.assertEqual(hdr["X-Bridge-Cache"], "miss")
        key = h.sessions.current.cache_key
        fn = h.cache.hit(key, "seg_00000.ts")
        self.assertIsNotNone(fn)
        self.assertEqual(os.path.getsize(fn), len(body))
        self.assertEqual(h.call("GET", "/health")[1]["stats"]["partial_reads"], 1)
        # a hit afterwards is the complete file, no Jasna
        st, body2, hdr = h.call("GET", f"/hls/{tok}/seg_00000.ts")
        self.assertEqual((st, hdr["X-Bridge-Cache"], len(h.jasna.segments)), (200, "hit", 2))

    def test_partial_size_signature(self):
        # ffmpeg writes through a 32 KiB buffer: an unfinished file is a whole
        # number of blocks; a finished one is a whole number of 188-byte packets
        # and (almost) never a block multiple.
        self.assertTrue(partial_size(0))
        self.assertTrue(partial_size(187))
        self.assertTrue(partial_size(5963776))          # 182 x 32768, the real one
        self.assertTrue(partial_size(32768 * 47))        # also 188-aligned, still a block multiple
        self.assertFalse(partial_size(188 * 100))
        self.assertFalse(partial_size(188 * 31730))           # 5965240: packet-aligned, not a block multiple
        self.assertTrue(looks_partial(b""))
        self.assertFalse(looks_partial(fake_jasna.ts_segment(0, 4)))

    def test_scan_drops_cached_partials(self):
        h = self.h
        key = h.cache.key("/p.mp4", "a", [])
        kd = os.path.join(h.cache.dir, key)
        os.makedirs(kd)
        with open(os.path.join(kd, "seg_00000.ts"), "wb") as fh:
            fh.write(fake_jasna.ts_segment(0, 4))
        with open(os.path.join(kd, "seg_00001.ts"), "wb") as fh:
            fh.write(b"\x47" * 32768)                   # a file ffmpeg never closed
        with open(os.path.join(kd, "seg_00002.ts"), "wb") as fh:
            fh.write(b"\x47" * (188 * 10 + 40))         # cut mid-packet
        fresh = SegmentCache(h.cfg)
        self.assertEqual(sorted(fresh.segs[key]), ["seg_00000.ts"])
        self.assertEqual(fresh.stats["partials_dropped"], 2)
        self.assertEqual(sorted(os.listdir(kd)), ["seg_00000.ts"])
        # and put() refuses one outright
        self.assertIsNone(fresh.hit(key, "seg_00001.ts"))
        fresh.put(key, "seg_00003.ts", b"\x47" * 32768)
        self.assertIsNone(fresh.hit(key, "seg_00003.ts"))

    def test_seek_lands_on_cache_even_after_live_pass(self):
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        t = a["token"]
        for i in range(4):
            self.seg(h, t, i)
        self.assertEqual(self.seg(h, t, 8)[0], "miss")   # new pass at 8 (live)
        self.assertEqual(self.seg(h, t, 9)[0], "miss")   # sequential on the live pass
        self.assertEqual(self.seg(h, t, 1)[0], "hit")    # a seek back is served from cache
        self.assertEqual(self.seg(h, t, 2)[0], "hit")    # and continues on cache while continuous
        self.assertEqual(h.jasna.passes, [0, 8])

    def test_lru_eviction_by_size(self):
        self.h.close()
        self.h = BridgeHarness({"cache": {"max_gb": (2 * self.SEG_BYTES + 10) / 1e9}})
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        h.call("GET", a["playlist_path"])
        key = h.sessions.current.cache_key
        for i in (0, 1):
            h.call("GET", f"/hls/{a['token']}/seg_{i:05d}.ts")
        h.call("GET", f"/hls/{a['token']}/seg_00000.ts")  # seg 0 is now the most recently used
        h.call("GET", f"/hls/{a['token']}/seg_00002.ts")  # third does not fit: evict LRU = seg 1
        snap = h.cache.snapshot()
        self.assertEqual((snap["segments"], snap["evictions"]), (2, 1))
        self.assertFalse(os.path.exists(h.cache.file_path(key, "seg_00001.ts")))
        self.assertTrue(os.path.exists(h.cache.file_path(key, "seg_00000.ts")))
        self.assertFalse(h.cache.is_complete(key))

    def test_key_depends_on_preset_flags_and_version(self):
        c = self.h.cache
        k1 = c.key("/m/a.mp4", "a", ["--x", "1"])
        self.assertEqual(k1, c.key("/m/a.mp4", "a", ["--x", "1"]))
        self.assertNotEqual(k1, c.key("/m/a.mp4", "a", ["--x", "2"]))
        self.assertNotEqual(k1, c.key("/m/b.mp4", "a", ["--x", "1"]))
        c.version = "0.11.0"
        self.assertNotEqual(k1, c.key("/m/a.mp4", "a", ["--x", "1"]))

    def test_cache_survives_restart_of_bridge(self):
        h = self.h
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        h.call("GET", a["playlist_path"])
        self.fetch_all(h, a["token"], 11)
        key = h.sessions.current.cache_key
        fresh = SegmentCache(h.cfg)  # rescans the same directory
        self.assertTrue(fresh.is_complete(key))
        self.assertEqual(fresh.snapshot()["segments"], 11)
        self.assertIsNotNone(fresh.manifest(key))

    def test_cache_disabled(self):
        self.h.close()
        self.h = BridgeHarness({"cache": {"enabled": False}})
        h = self.h
        self.assertIsNone(h.cache)
        _, a, _ = h.call("POST", "/session", {"scene_id": "1"})
        st, _, hdr = h.call("GET", f"/hls/{a['token']}/seg_00001.ts")
        self.assertEqual(st, 200); self.assertNotIn("X-Bridge-Cache", hdr)
        h.call("GET", f"/hls/{a['token']}/seg_00001.ts")
        self.assertEqual(h.jasna.segments, ["seg_00001.ts", "seg_00001.ts"])


class Managed(unittest.TestCase):
    def test_wedged_jasna_is_restarted(self):
        import tempfile
        hang = os.path.join(tempfile.mkdtemp(), "hang")
        h = BridgeHarness({"jasna": {"common_flags": ["--hang-file", hang], "process_idle_minutes": 60},
                           "session": {"heartbeat_idle_s": 5, "stream_linger_s": 0.3, "reaper_interval_s": 0.2}},
                          managed=True)
        try:
            st, a, _ = h.call("POST", "/session", {"scene_id": "1"})
            self.assertEqual(st, 200, a)
            pid1 = h.procs.pid(); self.assertIsNotNone(pid1)
            h.call("DELETE", f"/session/{a['token']}")
            time.sleep(0.6)  # linger expires -> /stop -> process idle (still alive)
            # wedge the running process: every request now blocks forever
            with open(hang, "w") as fh:
                fh.write(str(pid1))
            # on-demand path: ensure() probes, sees no answer, restarts
            t0 = time.time()
            st, b, _ = h.call("POST", "/session", {"scene_id": "2"})
            self.assertEqual(st, 200, b); self.assertTrue(b["cold"])
            self.assertNotEqual(h.procs.pid(), pid1)
            self.assertLess(time.time() - t0, 20, "recovery should not wait on the wedged process")
            st, _, _ = h.call("GET", f"/hls/{b['token']}/seg_00000.ts")
            self.assertEqual(st, 200)
            h.call("DELETE", f"/session/{b['token']}")
            time.sleep(0.6)
            # idle path: wedge the new process while nobody is watching; the reaper stops it
            pid2 = h.procs.pid()
            with open(hang, "w") as fh:
                fh.write(str(pid2))
            deadline = time.time() + 15
            while time.time() < deadline and h.procs.alive():
                time.sleep(0.3)
            self.assertFalse(h.procs.alive(), "reaper should stop a wedged idle Jasna")
            st, h2, _ = h.call("GET", "/health")
            self.assertFalse(h2["jasna"]["running"])
        finally:
            h.close()

    def test_wedged_jasna_mid_session_is_reopened(self):
        import tempfile
        hang = os.path.join(tempfile.mkdtemp(), "hang")
        h = BridgeHarness({"jasna": {"common_flags": ["--hang-file", hang], "process_idle_minutes": 60},
                           "session": {"heartbeat_idle_s": 60, "stream_linger_s": 5, "reaper_interval_s": 0.2}},
                          managed=True)
        try:
            st, a, _ = h.call("POST", "/session", {"scene_id": "1"})
            self.assertEqual(st, 200, a)
            pid1 = h.procs.pid()
            st, _, _ = h.call("GET", f"/hls/{a['token']}/seg_00001.ts")
            self.assertEqual(st, 200)
            with open(hang, "w") as fh:  # wedge Jasna while the viewer is watching
                fh.write(str(pid1))
            deadline = time.time() + 25
            while time.time() < deadline and (h.procs.pid() in (None, pid1) or not h.sessions.stream_path):
                time.sleep(0.3)
            self.assertNotEqual(h.procs.pid(), pid1, "reaper should restart the wedged process")
            self.assertEqual(h.sessions.stream_path, "/media/a/one.mp4", "stream re-opened")
            # same token keeps working: heartbeat and segments
            st, hb, _ = h.call("POST", f"/session/{a['token']}/heartbeat", {"time": 9, "paused": False})
            self.assertEqual(st, 200, hb)
            st, _, _ = h.call("GET", f"/hls/{a['token']}/seg_00002.ts")
            self.assertEqual(st, 200)
            st, snap, _ = h.call("GET", "/session")
            self.assertTrue(snap["active"]); self.assertEqual(snap["stats"]["opens"], 2)
        finally:
            h.close()

    def test_wedge_during_segment_fetch_restarts_on_first_failed_probe(self):
        import tempfile
        import threading
        hang = os.path.join(tempfile.mkdtemp(), "hang")
        h = BridgeHarness({"jasna": {"common_flags": ["--hang-file", hang], "process_idle_minutes": 60},
                           "session": {"heartbeat_idle_s": 60, "stream_linger_s": 5, "reaper_interval_s": 0.2}},
                          managed=True)
        try:
            st, a, _ = h.call("POST", "/session", {"scene_id": "1"})
            self.assertEqual(st, 200, a)
            pid1 = h.procs.pid()
            with open(hang, "w") as fh:
                fh.write(str(pid1))
            fetch = threading.Thread(target=h.call, args=("GET", f"/hls/{a['token']}/seg_00001.ts"), daemon=True)
            t0 = time.time()
            fetch.start()  # a viewer now waits on the hung Jasna
            while time.time() - t0 < 15 and h.procs.pid() in (None, pid1):
                time.sleep(0.1)
            self.assertNotEqual(h.procs.pid(), pid1)
            # one 3s probe, not two: the viewer is not left waiting the extra round
            self.assertLess(time.time() - t0, 5.5)
            fetch.join(10)
            deadline = time.time() + 10
            while time.time() < deadline and not h.sessions.stream_path:
                time.sleep(0.1)
            self.assertEqual(h.sessions.stream_path, "/media/a/one.mp4", "stream re-opened")
            st, _, _ = h.call("GET", f"/hls/{a['token']}/seg_00002.ts")
            self.assertEqual(st, 200)
        finally:
            h.close()

    def test_pipeline_stall_is_recovered_while_status_ok(self):
        # Jasna's /status keeps answering, but the render pass stops producing
        # segments (the 2026-09-09 stall). The responsive-probe recovery never
        # fires here; pipeline-stall detection must.
        import tempfile
        stall = os.path.join(tempfile.mkdtemp(), "stall")
        h = BridgeHarness({"jasna": {"common_flags": ["--stall-file", stall], "process_idle_minutes": 60},
                           "session": {"heartbeat_idle_s": 60, "stream_linger_s": 5,
                                       "reaper_interval_s": 0.1, "pipeline_stall_s": 0.6}},
                          managed=True)
        try:
            st, a, _ = h.call("POST", "/session", {"scene_id": "1"})
            self.assertEqual(st, 200, a)
            pid1 = h.procs.pid()
            st, _, _ = h.call("GET", f"/hls/{a['token']}/seg_00001.ts")
            self.assertEqual(st, 200)
            self.assertTrue(h.procs.responsive(), "status still answers")
            with open(stall, "w") as fh:  # pass goes quiet; /status stays healthy
                fh.write(str(pid1))
            # keep asking for a segment we never get, as a playing hls.js would
            deadline = time.time() + 20
            while time.time() < deadline and (h.procs.pid() in (None, pid1) or not h.sessions.stream_path):
                h.call("GET", f"/hls/{a['token']}/seg_00002.ts")  # 502 fast while stalled
                time.sleep(0.1)
            self.assertNotEqual(h.procs.pid(), pid1, "reaper should restart the stalled pass")
            self.assertEqual(h.sessions.stream_path, "/media/a/one.mp4", "stream re-opened")
            # the restarted process has a new pid, so it no longer stalls
            st, _, hdr = h.call("GET", f"/hls/{a['token']}/seg_00002.ts")
            self.assertEqual(st, 200)
            st, snap, _ = h.call("GET", "/session")
            self.assertTrue(snap["active"]); self.assertEqual(snap["stats"]["opens"], 2)
        finally:
            h.close()

    def test_no_stall_recovery_when_paused(self):
        # A paused tab stops pulling segments; that is not a stall.
        import tempfile
        stall = os.path.join(tempfile.mkdtemp(), "stall")
        h = BridgeHarness({"jasna": {"common_flags": ["--stall-file", stall], "process_idle_minutes": 60},
                           "session": {"heartbeat_idle_s": 60, "stream_linger_s": 5,
                                       "reaper_interval_s": 0.1, "pipeline_stall_s": 0.4}},
                          managed=True)
        try:
            st, a, _ = h.call("POST", "/session", {"scene_id": "1"})
            pid1 = h.procs.pid()
            h.call("GET", f"/hls/{a['token']}/seg_00001.ts")
            with open(stall, "w") as fh:
                fh.write(str(pid1))
            # request once, then pause and stop pulling
            h.call("GET", f"/hls/{a['token']}/seg_00002.ts")
            h.call("POST", f"/session/{a['token']}/heartbeat", {"time": 4, "paused": True})
            time.sleep(1.2)  # well past pipeline_stall_s
            self.assertEqual(h.procs.pid(), pid1, "paused session must not trigger a restart")
            self.assertEqual(h.sessions.stats["opens"], 1)
        finally:
            h.close()

    def test_spawn_preset_switch_idle_stop(self):
        h = BridgeHarness({"jasna": {"process_idle_minutes": 1 / 60}}, managed=True)
        try:
            st, health, _ = h.call("GET", "/health")
            self.assertFalse(health["jasna"]["running"]); self.assertTrue(health["jasna"]["managed"])
            st, a, _ = h.call("POST", "/session", {"scene_id": "1"})
            self.assertEqual(st, 200, a); self.assertTrue(a["cold"])
            pid1 = h.procs.pid(); self.assertIsNotNone(pid1)
            self.assertEqual(h.procs.running_preset, "a")
            st, _, _ = h.call("GET", f"/hls/{a['token']}/seg_00000.ts")
            self.assertEqual(st, 200)
            h.call("DELETE", f"/session/{a['token']}")
            # preset switch while the stream lingers restarts the process
            st, b, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "b"})
            self.assertEqual(st, 200, b); self.assertTrue(b["cold"]); self.assertFalse(b["reused"])
            self.assertNotEqual(h.procs.pid(), pid1); self.assertEqual(h.procs.running_preset, "b")
            st, presets, _ = h.call("GET", "/presets")
            self.assertEqual(presets["running"], "b")
            h.call("DELETE", f"/session/{b['token']}")
            deadline = time.time() + 6
            while time.time() < deadline and h.procs.alive():
                time.sleep(0.2)
            self.assertFalse(h.procs.alive(), "process should stop after linger + idle")
            st, c, _ = h.call("POST", "/session", {"scene_id": "2"})
            self.assertEqual(st, 200, c); self.assertTrue(c["cold"])
        finally:
            h.close()


class CustomPresets(unittest.TestCase):
    """POST /session {preset, flags}: ad-hoc presets from the plugin's setting."""

    def test_custom_presets_are_pruned_oldest_first_but_never_in_use(self):
        h = BridgeHarness()
        try:
            sm = h.sessions
            sm.MAX_CUSTOM_PRESETS = 3
            with sm.lock:
                sm.stream_preset = "c0"           # pretend c0 is what Jasna is streaming
                for i in range(6):
                    sm._register_custom(config.Preset(f"c{i}", ["--x", str(i)], "custom"))
            self.assertEqual(list(h.cfg.custom), ["c0", "c4", "c5"])
        finally:
            h.close()

    def test_validation(self):
        from jasna_bridge.config import validate_custom_preset
        p = validate_custom_preset("my hq", ["--detection-model", "rfdetr-v6-large", "--cq", "30"])
        self.assertEqual((p.name, p.flags, p.description), ("my hq", ["--detection-model", "rfdetr-v6-large", "--cq", "30"], "custom"))
        for name, flags in [
            ("", ["--cq", "30"]), ("x" * 41, ["--cq", "30"]), ("bad/name", ["--cq", "30"]),
            ("ok", []), ("ok", "--cq 30"), ("ok", ["cq", "30"]), ("ok", [""]), ("ok", ["--cq", 30]),
            ("ok", ["--stream-port", "1"]), ("ok", ["--license-key=abc"]), ("ok", ["--output", "/tmp/x"]),
            ("ok", ["--cq", "3\n0"]), ("ok", ["--cq"] * 65),
        ]:
            with self.assertRaises(ValueError, msg=f"{name!r} {flags!r}"):
                validate_custom_preset(name, flags)

    def test_rejected_unless_enabled_and_managed(self):
        h = BridgeHarness()  # unmanaged, custom off
        try:
            st, p, _ = h.call("GET", "/presets")
            self.assertFalse(p["custom_allowed"])
            st, body, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "mine", "flags": ["--cq", "30"]})
            self.assertEqual(st, 400); self.assertIn("manage_process", body["error"])
        finally:
            h.close()
        h = BridgeHarness(managed=True)  # managed, custom off
        try:
            st, p, _ = h.call("GET", "/presets")
            self.assertFalse(p["custom_allowed"])
            st, body, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "mine", "flags": ["--cq", "30"]})
            self.assertEqual(st, 400); self.assertIn("disabled", body["error"])
            self.assertFalse(h.procs.alive())
        finally:
            h.close()

    def test_custom_preset_runs_and_restarts_on_flag_change(self):
        def cq(cmd):  # the license flags may follow the preset's, so look --cq up by name
            return cmd[cmd.index("--cq") + 1]
        h = BridgeHarness({"jasna": {"custom_presets": True}}, managed=True)
        try:
            st, p, _ = h.call("GET", "/presets")
            self.assertTrue(p["custom_allowed"])
            self.assertEqual([x["name"] for x in p["presets"]], ["a", "b"])  # customs are never listed
            # bad requests never touch the process
            st, body, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "a", "flags": ["--cq", "30"]})
            self.assertEqual(st, 400); self.assertIn("collides", body["error"])
            st, body, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "mine", "flags": ["--stream-port", "9"]})
            self.assertEqual(st, 400); self.assertIn("not allowed", body["error"])
            self.assertFalse(h.procs.alive())

            st, a, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "mine", "flags": ["--cq", "30"]})
            self.assertEqual(st, 200, a); self.assertTrue(a["cold"]); self.assertEqual(a["preset"], "mine")
            pid1 = h.procs.pid()
            self.assertEqual(h.procs.running_preset, "mine")
            self.assertEqual(cq(h.procs.command("mine")), "30")
            st, _, _ = h.call("GET", f"/hls/{a['token']}/seg_00000.ts"); self.assertEqual(st, 200)
            st, p, _ = h.call("GET", "/presets"); self.assertEqual(p["running"], "mine")
            h.call("DELETE", f"/session/{a['token']}")

            # same name, same flags: the lingering stream is reused, no restart
            st, b, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "mine", "flags": ["--cq", "30"]})
            self.assertEqual(st, 200, b); self.assertFalse(b["cold"]); self.assertTrue(b["reused"])
            self.assertEqual(h.procs.pid(), pid1)
            h.call("DELETE", f"/session/{b['token']}")

            # same name, edited flags: Jasna restarts on the new flags and the cache key differs
            st, c, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "mine", "flags": ["--cq", "20"]})
            self.assertEqual(st, 200, c); self.assertTrue(c["cold"]); self.assertFalse(c["reused"])
            self.assertNotEqual(h.procs.pid(), pid1)
            self.assertEqual(cq(h.procs.command("mine")), "20")
            self.assertEqual(h.sessions.current.cache_key, h.cache.key("/media/a/one.mp4", "mine", ["--cq", "20"]))
            self.assertNotEqual(h.sessions.current.cache_key, h.cache.key("/media/a/one.mp4", "mine", ["--cq", "30"]))
            # a second viewer sending other flags under the same name is turned
            # away as busy and must not have re-pointed the running preset
            st, busy, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "mine", "flags": ["--cq", "99"]})
            self.assertEqual(st, 409, busy)
            self.assertEqual(h.cfg.preset("mine").flags, ["--cq", "20"])
            self.assertTrue(h.procs.matches("mine"))
            h.call("DELETE", f"/session/{c['token']}")

            # a configured preset still works alongside, and a bare custom name
            # (no flags) resolves to what was last registered under it
            st, d, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "b"})
            self.assertEqual(st, 200, d); self.assertEqual(h.procs.running_preset, "b")
            h.call("DELETE", f"/session/{d['token']}")
            st, e, _ = h.call("POST", "/session", {"scene_id": "1", "preset": "mine"})
            self.assertEqual(st, 200, e); self.assertEqual(cq(h.procs.command("mine")), "20")
        finally:
            h.close()


if __name__ == "__main__":
    unittest.main()


class Batch(unittest.TestCase):
    """Offline jobs: the queue runs the fake Jasna in --input/--output mode."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bridge-batch-")
        self.src_dir = os.path.join(self.tmp, "lib", "CODE-1 Name - Title (2020)")
        self.out_root = os.path.join(self.tmp, "out")
        os.makedirs(self.src_dir); os.makedirs(self.out_root)
        self.src = os.path.join(self.src_dir, "CODE-1.mkv")
        with open(self.src, "wb") as fh:
            fh.write(b"mosaic")
        fake_stash.SCENES["9"] = [self.src, os.path.join(self.src_dir, "CODE-1-pt2.mkv")]
        self.h = BridgeHarness(managed=True, overrides={
            "batch": {"enabled": True, "scratch_dir": os.path.join(self.tmp, "scratch"), "grace_s": 0,
                      "min_free_gb": 0, "output_roots": [self.out_root]},
            "session": {"heartbeat_idle_s": 5, "stream_linger_s": 0.3, "reaper_interval_s": 0.1},
        })
        self.out = os.path.join(self.out_root, "CODE-1 Name - [LTX] Title (2020)", "CODE-1_LTX_jasna.mp4")

    def tearDown(self):
        self.h.close()
        fake_stash.SCENES.pop("9", None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def submit(self, flags=("--fake-delay", "0.2"), preset="LTX", **over):
        body = {"scene_id": "9", "file_path": self.src, "output_path": self.out, "preset": preset,
                "flags": list(flags)}
        body.update(over)
        return self.h.call("POST", "/jobs", body)

    def wait_state(self, job_id, states, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            st, body, _ = self.h.call("GET", f"/jobs/{job_id}")
            self.assertEqual(st, 200, body)
            if body["job"]["state"] in states:
                return body["job"]
            time.sleep(0.1)
        self.fail(f"job {job_id} did not reach {states}: {body['job']['state']} ({body['job']['error']})")

    def tasks_seen(self):
        return [s["task"] for s in self.h.stash_seen if "task" in s]

    def test_job_runs_moves_output_and_notifies_stash(self):
        st, body, _ = self.submit()
        self.assertEqual(st, 201, body)
        job = body["job"]
        self.assertEqual(job["state"], "queued"); self.assertEqual(body["position"], 1)
        job = self.wait_state(job["id"], {"done", "failed"})
        self.assertEqual(job["state"], "done", job["error"])
        with open(self.out, "rb") as fh:
            self.assertEqual(fh.read(), b"restored:mosaic")
        self.assertEqual(job["output_bytes"], len(b"restored:mosaic"))
        self.assertEqual(job["source_bytes"], 6)
        self.assertGreater(job["duration_s"], 0)
        self.assertEqual(job["exit_code"], 0)
        self.assertEqual(job["jasna_version"], "0.10.0")  # probed from the fake's --version
        self.assertIn("--input", job["command"]); self.assertNotIn("--no-progress", job["command"])
        self.assertEqual(job["command"][job["command"].index("--input") + 1], self.src)
        self.assertFalse(job["ingested"])
        st, text, hdr = self.h.call("GET", f"/jobs/{job['id']}/log")
        self.assertEqual(st, 200); self.assertIn(b"fake jasna: done", text)
        self.assertIn("text/plain", hdr["Content-Type"])
        events = [t["args"]["event"] for t in self.tasks_seen() if t["args"].get("job_id") == job["id"]]
        self.assertEqual(events, ["started", "done"])
        self.assertEqual(self.tasks_seen()[0]["plugin_id"], "jasna-batch")
        self.assertEqual(self.tasks_seen()[0]["task_name"], "Sync jobs")
        # scratch is clean afterwards
        self.assertEqual([f for f in os.listdir(self.h.jobs.scratch_dir) if f.endswith(".mp4")], [])
        # unacknowledged until the plugin acks; then listed as ingested
        st, body, _ = self.h.call("GET", "/jobs?state=done&unacknowledged=1")
        self.assertEqual([j["id"] for j in body["jobs"]], [job["id"]])
        st, body, _ = self.h.call("POST", f"/jobs/{job['id']}/ack")
        self.assertEqual(st, 200); self.assertTrue(body["job"]["ingested"])
        st, body, _ = self.h.call("GET", "/jobs?unacknowledged=1")
        self.assertEqual(body["count"], 0)
        st, body, _ = self.h.call("GET", "/health")
        self.assertEqual(body["batch"]["stats"]["completed"], 1)
        self.assertIsNone(body["batch"]["running"]); self.assertIsNone(body["batch_job"])

    def test_session_refused_while_job_runs(self):
        st, body, _ = self.submit(flags=["--fake-delay", "2"])
        job = self.wait_state(body["job"]["id"], {"running"})
        st, busy, _ = self.h.call("POST", "/session", {"scene_id": "1"})
        self.assertEqual(st, 409, busy)
        self.assertEqual(busy["reason"], "batch"); self.assertEqual(busy["job"]["id"], job["id"])
        self.assertFalse(busy["takeover_available"])
        st, health, _ = self.h.call("GET", "/health")
        self.assertEqual(health["batch"]["running"]["id"], job["id"])
        self.assertEqual(health["batch_job"]["scene_id"], "9")
        self.wait_state(job["id"], {"done"})
        st, body, _ = self.h.call("POST", "/session", {"scene_id": "1"})
        self.assertEqual(st, 200, body)

    def test_job_waits_for_the_live_session_to_end(self):
        st, body, _ = self.h.call("POST", "/session", {"scene_id": "1"})
        self.assertEqual(st, 200, body)
        token = body["token"]
        st, body, _ = self.submit()
        job_id = body["job"]["id"]
        time.sleep(1.0)
        st, body, _ = self.h.call("GET", f"/jobs/{job_id}")
        self.assertEqual(body["job"]["state"], "queued")
        self.h.call("DELETE", f"/session/{token}")
        job = self.wait_state(job_id, {"done"})
        self.assertTrue(os.path.exists(self.out))
        # the stream process was stopped for the job and is not restarted by it
        self.assertFalse(self.h.procs.alive())

    def test_duplicate_cancel_queued_and_cancel_running(self):
        st, a, _ = self.submit(flags=["--fake-delay", "3"])
        st, dup, _ = self.submit(flags=["--fake-delay", "3"])
        self.assertEqual(st, 409, dup); self.assertEqual(dup["job"]["id"], a["job"]["id"])
        st, b, _ = self.submit(preset="other", output_path=self.out.replace("LTX", "other"))
        self.assertEqual(st, 201, b)
        st, body, _ = self.h.call("DELETE", f"/jobs/{b['job']['id']}")
        self.assertEqual(st, 200); self.assertEqual(body["job"]["state"], "cancelled")
        self.wait_state(a["job"]["id"], {"running"})
        st, body, _ = self.h.call("DELETE", f"/jobs/{a['job']['id']}")
        self.assertEqual(st, 200); self.assertTrue(body["cancelling"])
        job = self.wait_state(a["job"]["id"], {"cancelled", "failed", "done"}, timeout=20)
        self.assertEqual(job["state"], "cancelled", job["error"])
        self.assertFalse(os.path.exists(self.out))
        st, body, _ = self.h.call("DELETE", f"/jobs/{a['job']['id']}")
        self.assertEqual(st, 409)

    def test_failures_are_reported(self):
        st, body, _ = self.submit(flags=["--fake-fail", "3"])
        job = self.wait_state(body["job"]["id"], {"done", "failed"})
        self.assertEqual(job["state"], "failed"); self.assertIn("code 3", job["error"])
        self.assertEqual(job["exit_code"], 3)
        st, body, _ = self.submit(flags=["--fake-no-output"], preset="noout", output_path=self.out.replace("LTX", "noout"))
        job = self.wait_state(body["job"]["id"], {"done", "failed"})
        self.assertEqual(job["state"], "failed"); self.assertIn("no output", job["error"])
        # an output that appeared meanwhile is never overwritten
        os.makedirs(os.path.dirname(self.out), exist_ok=True)
        with open(self.out, "wb") as fh:
            fh.write(b"someone else's")
        st, body, _ = self.submit(preset="again", output_path=self.out)
        job = self.wait_state(body["job"]["id"], {"done", "failed"})
        self.assertEqual(job["state"], "failed"); self.assertIn("already exists", job["error"])
        with open(self.out, "rb") as fh:
            self.assertEqual(fh.read(), b"someone else's")
        events = [t["args"]["event"] for t in self.tasks_seen()]
        self.assertEqual(events.count("failed"), 3)

    def test_submit_validation(self):
        st, body, _ = self.submit(flags=["--output", "/x"])
        self.assertEqual(st, 400); self.assertIn("--output", body["error"])
        st, body, _ = self.submit(file_path="/nope.mkv")
        self.assertEqual(st, 404, body)
        st, body, _ = self.submit(output_path="/elsewhere/x.mp4")
        self.assertEqual(st, 403, body)
        st, body, _ = self.submit(output_path=self.out + ".mkv")
        self.assertEqual(st, 400, body)
        st, body, _ = self.submit(scene_id="404")
        self.assertEqual(st, 404, body)
        st, body, _ = self.submit(flags=[])
        self.assertEqual(st, 400, body)
        st, body, _ = self.h.call("GET", "/jobs?state=bogus")
        self.assertEqual(st, 400)
        st, body, _ = self.h.call("GET", "/jobs/0123456789ab")
        self.assertEqual(st, 404)

    def test_progress_is_parsed_from_the_log(self):
        from jasna_bridge.jobs import parse_progress
        self.assertIsNone(parse_progress("Jasna: checking the installation\n11:17:20 jasna.vr180 INFO: VR mode"))
        p = parse_progress("x\rProcessing Files: |###       |  30%  00:12<00:28, 2.5frames/s\rProcessing Files: |#####     |  50%  01:02:03<02:00:30, 8.3frames/s")
        self.assertEqual(p["percent"], 50)
        self.assertEqual(p["eta_s"], 2 * 3600 + 30)
        self.assertEqual(p["rate"], "8.3frames/s")
        # a job run with the progress bar on reports percent while running
        st, body, _ = self.submit(flags=["--fake-delay", "3"])
        job_id = body["job"]["id"]
        seen = None
        deadline = time.time() + 15
        while time.time() < deadline:
            st, body, _ = self.h.call("GET", f"/jobs/{job_id}")
            if body["job"]["progress"] and body["job"]["progress"]["percent"] is not None:
                seen = body["job"]["progress"]
            if body["job"]["state"] != "running" and seen:
                break
            time.sleep(0.3)
        self.assertIsNotNone(seen, "no progress reported")
        self.assertIn("frames/s", seen["rate"])
        job = self.wait_state(job_id, {"done"})
        self.assertEqual(job["progress"]["percent"], 100)
        self.assertNotIn("--no-progress", job["command"])

    def test_running_job_survives_a_bridge_restart(self):
        st, body, _ = self.submit(flags=["--fake-delay", "4"])
        job_id = body["job"]["id"]
        pid = None
        deadline = time.time() + 15
        while time.time() < deadline and not pid:
            st, body, _ = self.h.call("GET", f"/jobs/{job_id}")
            pid = body["job"]["pid"]
            time.sleep(0.1)
        self.assertTrue(pid)
        self.h.jobs.shutdown()           # keep_running_on_restart (default): Jasna is left alone
        self.assertTrue(os.path.exists(f"/proc/{pid}"))
        self.h.sessions.batch_release()  # the old bridge is gone; its GPU claim with it
        reloaded = JobQueue(self.h.cfg, self.h.sessions, StashClient(self.h.cfg.stash_url),
                            lambda p: config.map_path(self.h.cfg, p))
        reloaded.POLL_S = 0.2
        reloaded.start()
        try:
            self.assertEqual(reloaded.jobs[job_id].state, "running")
            self.assertIn("adopted", reloaded.jobs[job_id].notes[0])
            st, busy, _ = self.h.call("POST", "/session", {"scene_id": "1"})
            self.assertEqual(st, 409); self.assertEqual(busy["reason"], "batch")  # still holds the GPU
            deadline = time.time() + 15
            while time.time() < deadline and reloaded.jobs[job_id].state == "running":
                time.sleep(0.2)
            job = reloaded.jobs[job_id]
            self.assertEqual(job.state, "done", job.error)
            self.assertTrue(os.path.exists(self.out))
            self.assertIn("exit code unknown", job.notes[-1])
            self.assertEqual(job.progress["percent"], 100)
            self.assertIsNone(self.h.sessions.batch_job)
        finally:
            reloaded.shutdown()
            self.h.jobs = None

    def test_queue_page(self):
        st, body, hdr = self.h.call("GET", "/queue")
        self.assertEqual(st, 200)
        self.assertIn("text/html", hdr["Content-Type"])
        self.assertIn(b"Jasna queue", body)
        self.assertIn(b'fetch("/graphql"', body)       # scene titles from Stash on the same origin
        self.assertIn(b'api("jobs")', body)             # relative, so the path prefix does not matter
        plain = BridgeHarness()
        try:
            st, body, _ = plain.call("GET", "/queue")
            self.assertEqual(st, 404)
        finally:
            plain.close()

    def test_disabled_bridge_answers_404(self):
        plain = BridgeHarness()
        try:
            st, body, _ = plain.call("GET", "/jobs")
            self.assertEqual(st, 404); self.assertIn("batch.enabled", body["error"])
            st, body, _ = plain.call("GET", "/health")
            self.assertEqual(body["batch"], {"enabled": False})
        finally:
            plain.close()

    def test_check_reports_environment(self):
        st, body, _ = self.h.call("GET", "/batch/check")
        self.assertEqual(st, 200, body)
        labels = {c["label"]: c for c in body["checks"]}
        self.assertTrue(labels["Jasna binary"]["ok"])
        self.assertEqual(body["jasna_version"], "0.10.0")
        self.assertTrue(labels["scratch dir writable"]["ok"])
        self.assertTrue(labels[f"output root {self.out_root}"]["ok"])
        self.assertTrue(labels["Stash reachable from the bridge"]["ok"])
        self.assertIn("model_weights", labels)  # the fake has none; reported, not fatal to the call

    def test_interrupted_job_is_requeued_on_restart(self):
        st, body, _ = self.submit(flags=["--fake-delay", "5"])
        job_id = body["job"]["id"]
        pid = None
        deadline = time.time() + 15
        while time.time() < deadline and not pid:
            st, body, _ = self.h.call("GET", f"/jobs/{job_id}")
            pid = body["job"]["pid"]
            time.sleep(0.1)
        self.h.jobs.shutdown()   # a bridge restart leaves Jasna running...
        os.kill(pid, 9)          # ...but here the machine rebooted: the process is gone
        time.sleep(0.5)
        with open(self.h.jobs.state_file) as fh:
            self.assertEqual(json.load(fh)["jobs"][0]["state"], "running")
        reloaded = JobQueue(self.h.cfg, self.h.sessions, None, lambda p: p)
        reloaded._load()
        job = reloaded.jobs[job_id]
        self.assertEqual(job.state, "queued"); self.assertEqual(job.attempts, 1)
        self.assertIn("re-queued", job.notes[0])
        # a second interruption exhausts batch.max_attempts (2)
        job.state, job.attempts = "running", 2
        reloaded._save()
        again = JobQueue(self.h.cfg, self.h.sessions, None, lambda p: p)
        again._load()
        self.assertEqual(again.jobs[job_id].state, "failed")
        self.assertIn("giving up", again.jobs[job_id].error)
        self.h.jobs = None  # already shut down
