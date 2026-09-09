# stash-jasna-bridge

A small stdlib-only Python service that sits between the Stash
[jasna-switch](https://github.com/org0ne/stash-plugins/tree/main/plugins/jasna-switch)
plugin and an unmodified `jasna --stream` process. It adds what Jasna's
stream server lacks: session ownership, idle timeout, auth, one exposed
origin, and (optionally) process supervision with named presets.

```
Browser (Stash UI + jasna-switch)
   |  /jasna/... same origin as Stash when reverse-proxied
   v
Bridge  (this, 127.0.0.1:8770 or LAN)     owns sessions, policy, the process
   |
Jasna --stream --no-browser --stream-port 8765 [preset flags]
```

Status: Phase A of the bridge plan (foundation) — everything the PoC did,
plus no trampling between viewers and no orphaned streams. Phases B–D
(reverse-proxy deployment, segment cache, preset picker) build on this.

## Run

```sh
cp bridge.toml.example bridge.toml   # edit; chmod 600 if it holds keys
python3 -m jasna_bridge -c bridge.toml [-v]
```

Python 3.11+ (tomllib), no packages. `deploy/stash-jasna-bridge.service`
is a systemd unit; `deploy/nginx-proxy-manager.md` shows the reverse-proxy
location and firewall rules.

Tests (fake Stash + fake Jasna, ~20s): `python3 -m unittest -v tests.test_bridge`

## API (what the plugin calls)

All paths are also accepted under `server.path_prefix` (default `/jasna`).

| Method | Path | Purpose |
|---|---|---|
| POST | `/session` | `{scene_id, time?, preset?, force?}` → `{token, playlist_path, reused, cold, switched, ready_seconds, heartbeat_seconds, duration}`; **409** `{error:"busy", reason, scene_id, idle_seconds, takeover_available, ...}` when someone else owns Jasna |
| POST | `/session/{token}/heartbeat` | `{time, paused}` every ~30s while ON; **410** once the session is gone |
| DELETE | `/session/{token}` | toggle OFF / scene change |
| POST | `/session/{token}/end` | same, for `navigator.sendBeacon` on `pagehide` |
| GET | `/session` | current owner, lingering stream, stats |
| GET | `/hls/{token}/stream.m3u8` | Jasna's VOD manifest, only for the live token |
| GET | `/hls/{token}/seg_NNNNN.ts` | segment proxied from Jasna (blocks while it renders) |
| GET | `/presets` | preset names, default, currently running, warm |
| GET | `/health` | Jasna reachable/streaming/managed/pid/warm + session snapshot |

The browser never sends filesystem paths: the bridge resolves `scene_id`
through Stash GraphQL (`findScene { files { path } }`) and applies the
optional `[[paths.map]]` rewrites.

## Behaviour

- **Ownership.** One session at a time. A second `POST /session` gets 409
  while the owner is alive. The token is the credential for heartbeat, end
  and HLS routes, so a stale tab cannot pull the new owner's segments.
- **Takeover.** A 409 reports `takeover_available: true` once the current
  owner has not been *watching* for `session.takeover_idle_s` (20s):
  no segment fetch and no unpaused heartbeat in that time, so a paused tab
  qualifies even though its heartbeats keep it from being idle-released. A
  `POST /session` with `force: true` then pre-empts that owner (a
  fresh, playing owner is never forced out). This is how a viewer reclaims
  Jasna from a paused or background tab without waiting out the full idle
  timeout.
- **Segment cache** (`[cache]`, on by default). Every segment the bridge
  proxies is written to `cache.dir` under a key of (file path, preset flags,
  Jasna version), LRU-evicted at `cache.max_gb`. A repeat request for a
  segment is served from disk (`X-Bridge-Cache: hit`), so backward seeks and
  OFF/ON near the same spot never touch the GPU. Jasna's manifest is kept
  per stream; once every segment it lists is on disk the stream is
  *complete* and the next session for it is served entirely from cache
  (`cached: true` in the `/session` reply, `from_cache` in the snapshot)
  without opening Jasna at all. A miss on such a session (eviction made a
  hole) opens Jasna on demand for that file. `/health` reports the cache
  under `cache`.
- **Idle.** A session is released after `session.heartbeat_idle_s` (90s)
  without a heartbeat *or* a segment fetch. A released owner's next
  heartbeat gets 410 and the plugin drops back to the Stash source.
- **Linger.** After release the Jasna stream stays open for
  `session.stream_linger_s` (120s); a new session for the same file and
  preset reuses it (`reused: true`, ~0.02s instead of a cold `/open`).
  Then `/stop`. A different file pre-empts a lingering stream immediately.
- **Adoption.** On start, a stream Jasna already has open (started by
  hand, or left by a previous bridge) is tracked as lingering, so it is
  reused or closed rather than leaked.
- **Process (managed mode).** With `jasna.manage_process = true` the bridge
  spawns Jasna with `common_flags + presets.<name>.flags`, waits for
  `/status`, restarts it on a preset change (only when no session is
  active), restarts it after a crash on the next request, and terminates it
  `process_idle_minutes` after the last stream closed. `prewarm_path` opens
  a clip once after each start so TensorRT engine caches exist.
- **Auth.** `none`, `token` (Authorization: Bearer / X-Bridge-Token on
  `/session`, `/presets`), or `stash_cookie` (the browser's Stash cookie is
  forwarded to Stash GraphQL and cached for `cache_s`). Token-in-path
  routes need no extra auth. Jasna itself binds 0.0.0.0 unauthenticated:
  firewall 8765 to localhost or the bridge's auth is decorative.
- **CORS.** Only for origins in `server.cors_origins` (plain-HTTP Stash on
  another origin). Under the Stash domain nothing is needed.

## Measured (2026-09-08, debeast, preset rfdetr-v6-large + unet-4x)

| case | toggle → first frame |
|---|---|
| stream already open for this file (warm `/open`) | ~6s |
| ON after OFF within linger (`reused`) | ~6.5s, `/session` 0.02s |
| switch to another file (`switched`, `/open` 10.8s) | ~16s |

Most of the remaining time is Jasna rendering the first segment; the
plugin now passes `startPosition` to hls.js so segment 0 is no longer
fetched for nothing.
