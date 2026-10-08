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

Status: all four phases of the bridge plan are in: sessions and process
supervision (A), reverse-proxy deployment (B), the segment cache (C) and
preset selection with custom presets (D), plus splicing of Jasna's render
passes into one continuous timeline.

## Install

Python 3.11+ (tomllib), no packages. On the Jasna host:

```sh
git clone <this repo> ~/Projects/stash-jasna-bridge
cd ~/Projects/stash-jasna-bridge
python3 -m jasna_bridge install --stash-url http://127.0.0.1:9999
```

`install` finds the Jasna binary, writes `bridge.toml` and a systemd **user**
unit, enables linger, starts it, and prints the two steps it cannot do for
you: the firewall rule (needs sudo) and the reverse-proxy `/jasna` location.
The Jasna license is read from Jasna's own store, so no key goes in
`bridge.toml` (Linux `~/.config/jasna`, Windows `%LOCALAPPDATA%\jasna`,
macOS `~/Library/Application Support/jasna`; `jasna.license_file` overrides). Then:

```sh
python3 -m jasna_bridge doctor      # pass/fail check of the whole path; exits 1 on a failure
```

Reverse-proxy snippets (NPM, nginx, Caddy, Cloudflare Tunnel):
`deploy/reverse-proxy.md`. Firewall: `deploy/nftables-jasna.conf`. In Stash,
install the **Jasna Switch** plugin; it auto-detects the bridge at
`<stash-origin>/jasna`, so no plugin settings are needed.

### Manual

```sh
cp bridge.toml.example bridge.toml   # edit; chmod 600 if it holds a token
python3 -m jasna_bridge -c bridge.toml [-v]
```

Tests (fake Stash + fake Jasna, ~20s): `python3 -m unittest -v tests.test_bridge`

## API (what the plugin calls)

All paths are also accepted under `server.path_prefix` (default `/jasna`).

| Method | Path | Purpose |
|---|---|---|
| POST | `/session` | `{scene_id, time?, preset?, flags?, force?}` → `{token, playlist_path, reused, cold, switched, ready_seconds, heartbeat_seconds, duration}`; **409** `{error:"busy", reason, scene_id, idle_seconds, takeover_available, ...}` when someone else owns Jasna. `flags` (a list of Jasna CLI tokens) makes `preset` a custom preset, see below |
| POST | `/session/{token}/heartbeat` | `{time, paused}` every ~30s while ON; **410** once the session is gone |
| DELETE | `/session/{token}` | toggle OFF / scene change |
| POST | `/session/{token}/end` | same, for `navigator.sendBeacon` on `pagehide` |
| GET | `/session` | current owner, lingering stream, stats |
| GET | `/hls/{token}/stream.m3u8` | Jasna's VOD manifest, only for the live token |
| GET | `/hls/{token}/seg_NNNNN.ts` | segment proxied from Jasna (blocks while it renders) |
| GET | `/presets` | preset names, default, currently running, warm, `custom_allowed` |
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
  under `cache`. Each stream's `meta.json` records the source file's size
  and mtime; if the file at that path changes (re-encoded, replaced) its
  cached segments are dropped at the next session (`source_changed`).
- **Seams.** Jasna's segments do not sit on its playlist's fixed 4s grid
  (measured on 0.10.0): segment files are cut every 120 frames and at scene
  changes, so which source range lands under `seg_NNNNN.ts` drifts by
  seconds over a pass, while the PTS inside stay honest (source time +
  1.4s). Two adjacent segments from different render passes therefore
  overlap or gap by a few seconds. hls.js copes with that after a seek but
  not on a *contiguous* fragment: it shifts the overlapping video frames
  and resets the audio, so the picture runs seconds behind the sound. The
  bridge reads every segment's real PTS span (`jasna_bridge/mpegts.py`)
  and splices on the fly: a sequential segment that does not meet the one
  just served is re-stamped (PTS/DTS/PCR shifted, header bytes only, ~3ms,
  nothing re-encoded) so the player sees one continuous timeline, the new
  pass's lead-in audio is trimmed, and the shift is kept for the rest of
  the run (`X-Bridge-Offset`, `seams_restamped` in the session stats).
  At an overlap the viewer sees those seconds once more, in sync, instead
  of a desync; at a gap a few seconds are skipped instead of stalling; the player clock then runs ahead of file time by that much until
  the next seek. The cache keeps Jasna's original bytes and is always
  served first: a join between two cached passes is spliced like any
  other, rather than re-rendered (which would cancel Jasna's current pass
  and make a new seam). An hls.js retry of the segment it just got keeps
  the current shift. A seek is placed by real PTS, so it resets the shift.
- **Stall recovery.** Two failures are watched for. If Jasna stops
  answering `/status` at all (a wedged HTTP server), the reaper restarts it
  after two missed probes, or after one while a segment fetch is waiting on
  it (a healthy Jasna answers `/status` at once even with many segment
  requests parked). If `/status` still answers but the render pass
  goes quiet - no segment served to an active, *playing* session for
  `session.pipeline_stall_s` (180s) though it keeps asking - that is a
  pipeline stall (seen 2026-09-09 on a long file), and the reaper restarts
  Jasna and re-opens on the same token too. A paused tab pulls no segments,
  so it never counts as a stall. Keep the timeout above what one
  `--max-clip-size` clip takes to restore: a healthy pass goes quiet that
  long mid-clip (45s fired twice on 2026-09-30), and every restart mid-pass
  is a seam for the viewer.
- **Seek spacing.** Every seek that reaches Jasna starts a render pass, and
  Jasna 0.10.0 can deadlock at a pass start (reproduced 2026-10-07: bursts
  of rapid seeks hang the whole process, `/status` included, within some
  20-70 pass starts). A cache miss that would make Jasna seek is held until
  `session.seek_spacing_s` (1.5s) has passed since the last one; if the
  player asks for another segment meanwhile (it scrubbed on), the held
  request gets 503 and Jasna never sees it. A lone seek and sequential
  playback are not delayed. `/health` counts `seeks_forwarded` and
  `seeks_coalesced`.
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
- **Custom presets (managed mode, opt-in).** With `jasna.custom_presets =
  true`, `POST /session` may carry `flags: ["--cq", "30", ...]` next to a
  `preset` name of the client's choosing (this is what the plugin's *Custom
  presets* setting sends). The bridge validates the pair (a 1-40 character
  name that does not collide with a configured preset; up to 64 string
  tokens, none from a denylist: `--stream`, `--stream-port`,
  `--stream-segment-duration`, `--no-browser`, `--input`, `--output`,
  `--output-pattern`, `--working-directory`, `--segments`,
  `--license-email`, `--license-key`, `--post-export-*`, `--benchmark*`,
  `--help`, `--version`), then treats it like a configured preset for that
  and later sessions: `common_flags + flags`, restart on change, its own
  cache key. Re-sending a name with different flags restarts Jasna on the
  new flags. Custom presets are never listed by `/presets` (the plugin
  keeps that list) and do not survive a bridge restart. Off by default:
  anyone who can create a session then chooses Jasna's launch flags.
- **Auth.** `none`, `token` (Authorization: Bearer / X-Bridge-Token on
  `/session`, `/presets`), or `stash_cookie` (the browser's Stash cookie is
  forwarded to Stash GraphQL and cached for `cache_s`). Token-in-path
  routes need no extra auth. Jasna itself binds 0.0.0.0 unauthenticated:
  firewall 8765 to localhost or the bridge's auth is decorative.
- **CORS.** Only for origins in `server.cors_origins` (plain-HTTP Stash on
  another origin). Under the Stash domain nothing is needed.

## Measured (preset rfdetr-v6-large + unet-4x, RTX 4090)

| case | toggle → first frame |
|---|---|
| stream already open for this file (warm `/open`) | ~6s |
| ON after OFF within linger (`reused`) | ~6.5s, `/session` 0.02s |
| switch to another file (`switched`, `/open` 10.8s) | ~16s |

Most of the remaining time is Jasna rendering the first segment; the
plugin now passes `startPosition` to hls.js so segment 0 is no longer
fetched for nothing.

## License

MIT. See [LICENSE](LICENSE). Jasna itself is a separate program (AGPL) that
this service only talks to over HTTP; nothing from it is bundled here.
