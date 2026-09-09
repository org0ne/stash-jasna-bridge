"""End-to-end tests: real bridge HTTP server against fake Stash and fake Jasna.
Run: python3 -m unittest -v tests.test_bridge"""
from __future__ import annotations

import json
import os
import sys
import time
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jasna_bridge import config  # noqa: E402
from jasna_bridge.jasna import JasnaClient, ProcessManager  # noqa: E402
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
            "session": {"heartbeat_idle_s": 0.6, "stream_linger_s": 0.6, "reaper_interval_s": 0.1},
        }
        for section, values in (overrides or {}).items():
            data.setdefault(section, {}).update(values)
        self.cfg = config.from_dict(data)
        client = JasnaClient(self.cfg.jasna_url, timeout=3)
        self.procs = ProcessManager(self.cfg, client)
        self.sessions = SessionManager(self.cfg, client, self.procs)
        self.sessions.start()
        self.server = serve(self.cfg, self.sessions, StashClient(self.cfg.stash_url))
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown(); self.server.server_close()
        self.sessions.shutdown()
        self.stash_srv.shutdown(); self.stash_srv.server_close()
        if self.jasna_srv:
            self.jasna_srv.shutdown(); self.jasna_srv.server_close()

    def call(self, method, path, body=None, headers=None, prefix="/jasna"):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + prefix + path, data=data, method=method,
                                     headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if resp.headers.get_content_type() == "application/json" else raw), resp.headers
        except urllib.error.HTTPError as err:
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
        self.assertTrue(seg.startswith(b"seg_00003.ts:"))
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


class Managed(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
