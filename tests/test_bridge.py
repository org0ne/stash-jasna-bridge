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
from jasna_bridge.mpegts import pcr_values, restamp, segment_span, track_spans, trim_leading_audio  # noqa: E402
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
        self.server = serve(self.cfg, self.sessions, StashClient(self.cfg.stash_url))
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown(); self.server.server_close()
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
        self.assertEqual((st, pl2), (200, pl))
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
