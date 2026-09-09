"""Stash GraphQL client: scene path resolution and session-cookie validation."""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import urllib.error
import urllib.request

log = logging.getLogger("bridge.stash")


class StashError(Exception):
    pass


class StashUnauthorized(StashError):
    pass


class StashClient:
    def __init__(self, base_url: str, api_key: str = "", timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def graphql(self, query: str, variables: dict | None = None, cookie: str | None = None) -> dict:
        body = json.dumps({"query": query, "variables": variables or {}}).encode()
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if cookie:
            headers["Cookie"] = cookie
        elif self.api_key:
            headers["ApiKey"] = self.api_key
        req = urllib.request.Request(self.base_url + "/graphql", data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode())
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                raise StashUnauthorized(f"Stash returned HTTP {err.code}") from err
            raise StashError(f"Stash returned HTTP {err.code}") from err
        except (urllib.error.URLError, OSError, ValueError) as err:
            raise StashError(f"Stash request failed: {err}") from err
        if payload.get("errors"):
            msg = payload["errors"][0].get("message", "unknown GraphQL error")
            if "unauthorized" in msg.lower() or "not authenticated" in msg.lower():
                raise StashUnauthorized(msg)
            raise StashError(msg)
        return payload.get("data") or {}

    def scene_path(self, scene_id: str) -> tuple[str, float | None]:
        """Return (path, duration) of the scene's first file, or raise StashError."""
        data = self.graphql(
            "query BridgeScene($id: ID!) { findScene(id: $id) { id files { path duration } } }",
            {"id": str(scene_id)},
        )
        scene = data.get("findScene")
        if not scene:
            raise StashError(f"scene {scene_id} not found")
        files = scene.get("files") or []
        if not files:
            raise StashError(f"scene {scene_id} has no files")
        return files[0]["path"], files[0].get("duration")


class CookieValidator:
    """Validates a browser's Stash session cookie by making an authenticated
    GraphQL call with it. Results are cached per cookie for `ttl` seconds."""

    QUERY = "{ configuration { general { databasePath } } }"

    def __init__(self, client: StashClient, ttl: float):
        self.client = client
        self.ttl = ttl
        self._cache: dict[str, tuple[float, bool]] = {}
        self._lock = threading.Lock()

    def valid(self, cookie: str | None) -> bool:
        if not cookie:
            # An instance with no credentials configured accepts cookie-less
            # calls; ask Stash rather than assuming.
            cookie = ""
        key = hashlib.sha256(cookie.encode()).hexdigest()
        now = time.monotonic()
        with self._lock:
            hit = self._cache.get(key)
            if hit and hit[0] > now:
                return hit[1]
        try:
            self.client.graphql(self.QUERY, cookie=cookie or None)
            ok = True
        except StashUnauthorized:
            ok = False
        except StashError as err:
            log.warning("cookie validation failed to reach Stash: %s", err)
            return False
        with self._lock:
            self._cache[key] = (now + self.ttl, ok)
            if len(self._cache) > 1000:
                for k in [k for k, (exp, _) in self._cache.items() if exp <= now]:
                    del self._cache[k]
        return ok
