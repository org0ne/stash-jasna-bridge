# Exposing the bridge under the Stash domain

Goal: `https://<stash>/jasna/...` -> the bridge, so the plugin auto-detects it
at `<stash-origin>/jasna`, Stash's session cookie is sent automatically, and no
CSP, CORS or TLS setup is needed. The bridge listens on `0.0.0.0:8770` and
strips the `/jasna` prefix itself (`server.path_prefix`), so no URL rewrite is
required. Replace `BRIDGE_HOST` with the Jasna host's LAN IP.

Verify from a browser after any of these: open `https://<stash>/jasna/health`;
it should return JSON, not the Stash page.

## Nginx Proxy Manager

Hosts > your Stash proxy host > Custom locations > Add location:
- location: `/jasna`   scheme: `http`   forward: `BRIDGE_HOST` port `8770`

Advanced (gear icon), so a segment fetch waiting on Jasna is not cut off:

```nginx
proxy_read_timeout 90s;
proxy_buffering off;
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
```

## plain nginx

Inside the Stash `server { }` block:

```nginx
location /jasna/ {
    proxy_pass http://BRIDGE_HOST:8770;
    proxy_read_timeout 90s;
    proxy_buffering off;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header Host $host;
}
```

## Caddy

```caddy
handle_path /jasna/* {
    reverse_proxy http://BRIDGE_HOST:8770 {
        header_up X-Forwarded-For {remote_host}
        transport http { read_timeout 90s }
    }
}
```
(Caddy's `handle_path` strips `/jasna`; the bridge also tolerates the prefix, so
`handle /jasna/*` with `reverse_proxy` works too.)

## Cloudflare Tunnel

If a tunnel fronts Stash, its ingress goes straight to Stash and bypasses any
proxy that has the `/jasna` location, so add a rule ABOVE the catch-all in the
tunnel config (dashboard: the hostname's path rules; or `config.yml`):

```yaml
ingress:
  - hostname: <stash>
    path: ^/jasna(/.*)?$
    service: http://BRIDGE_HOST:8770
  - hostname: <stash>
    service: http://STASH_HOST:PORT      # the existing catch-all
  - service: http_status:404
```

Then the firewall must allow the tunnel connector's host (or the LAN subnet) to
reach `BRIDGE_HOST:8770`; see `deploy/nftables-jasna.conf`.
