# Exposing the bridge under the Stash domain with Nginx Proxy Manager

Goal: `https://<stash-domain>/jasna/...` -> bridge on the Jasna host, so the
plugin calls a same-origin relative URL (`/jasna`), Stash's session cookie
is sent automatically, no CSP `connect-src` entry or CORS is needed, and
the throwaway TLS proxy goes away.

1. NPM > Hosts > Proxy Hosts > edit the Stash host > **Custom locations** >
   Add location:
   - location: `/jasna`
   - scheme: `http`
   - forward hostname/IP: the Jasna host (e.g. `192.168.11.113`)
   - forward port: `8770`
   NPM keeps the `/jasna` prefix on the forwarded request; the bridge strips
   it (`server.path_prefix = "/jasna"`), so no rewrite is required.
2. In the location's advanced box (gear icon) add, so segment fetches that
   wait on Jasna are not cut off and no buffering delays first frames:

   ```nginx
   proxy_read_timeout 60s;
   proxy_buffering off;
   proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
   ```
3. Bridge config: `server.host` must be reachable from the NPM host (LAN IP
   or `0.0.0.0`), `auth.mode = "stash_cookie"`, `stash.url` pointing at
   Stash directly (e.g. `http://127.0.0.1:7777`).
4. Firewall on the Jasna host: allow 8770 only from the NPM host, and block
   Jasna's own 8765 from everything but localhost (Jasna binds 0.0.0.0 with
   no auth; without this rule the bridge's auth is decorative). Ready-made
   nftables rules: `deploy/nftables-jasna.conf` (apply/persist commands in
   its header).
5. Plugin: Settings > Plugins > Jasna Switch > **Bridge URL** = `/jasna`
   (relative). Leave Jasna URL empty.
