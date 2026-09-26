"""Linear OAuth token providers.

``ConnectOAuth``: 1Password Connect is the source of truth for ``client_id``,
``client_secret`` and the current ``refresh_token``. The profile-local cache holds only
the short-lived access token and, transiently, a rotated refresh token that Connect has
not accepted yet. Delete the cache and the next call rebuilds it from Connect.
``TokenFile``: a private file holding a token (simple single-profile setups).
"""
from __future__ import annotations

import fcntl
import json
import logging
import os
import stat
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

TOKEN_ENDPOINT = "https://api.linear.app/oauth/token"
MARGIN = 60
ROTATED = "rotated_refresh_token"
ALIASES = {"client_id": ("client_id", "username"), "client_secret": ("client_secret", "credential", "password"),
           "refresh_token": ("refresh_token",)}
log = logging.getLogger("linear.oauth")


class ReauthorizationRequired(RuntimeError):
    """Linear rejected the refresh token; an operator must reauthorize this profile's app."""


def read_private(path: Path | str, what: str) -> str:
    """Read a regular, uid-owned, 0600 file without following symlinks."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise RuntimeError(f"{what} must be a regular file owned by this user with mode 600: {path}")
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            fd = -1
            return stream.read(65536)
    finally:
        if fd >= 0:
            os.close(fd)


def load_env(path: Path | str) -> dict[str, str]:
    values = {}
    for line in read_private(path, "Connect env file").splitlines():
        key, sep, value = line.strip().removeprefix("export ").partition("=")
        if sep and not key.startswith("#"):
            values[key.strip()] = value.strip().strip("'\"")
    return values


class ConnectItem:
    """One 1Password Connect item addressed by vault id and item id."""

    def __init__(self, host: str, token: str, vault_id: str, item_id: str, *, transport: Callable | None = None) -> None:
        if not (host.startswith(("http://", "https://")) and token and vault_id and item_id):
            raise RuntimeError("Connect host (http/https), token, vault id and item id are required")
        self.url = f"{host.rstrip('/')}/v1/vaults/{vault_id}/items/{item_id}"
        self.ids = (vault_id, item_id)
        self._token = token
        self._transport = transport or self._http

    @staticmethod
    def _http(method: str, url: str, headers: dict[str, str], body: bytes | None) -> Any:
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=15) as response:  # nosec B310: operator-configured host
            return json.load(response)

    def _call(self, method: str, body: bytes | None = None) -> Any:
        headers = {"Authorization": "Bearer " + self._token, "Content-Type": "application/json"}
        return self._transport(method, self.url, headers, body)

    def fetch(self) -> dict[str, Any]:
        item = self._call("GET")
        if not isinstance(item, dict) or (str((item.get("vault") or {}).get("id")), str(item.get("id"))) != self.ids:
            raise RuntimeError("Connect item does not match the configured vault/item binding")
        return item

    @staticmethod
    def field(item: dict[str, Any], name: str) -> dict[str, Any]:
        fields = [f for f in item.get("fields") or [] if isinstance(f, dict)]
        for alias in ALIASES[name]:
            hits = [f for f in fields if alias in {str(f.get("id", "")).lower(), str(f.get("label", "")).lower()}]
            if len(hits) == 1:
                return hits[0]
        raise RuntimeError(f"Connect item needs exactly one {name} field")

    def credentials(self) -> dict[str, str]:
        item = self.fetch()
        values = {name: str(self.field(item, name).get("value") or "").strip() for name in ALIASES}
        if not all(values.values()):
            raise RuntimeError("Connect item has an empty OAuth field")
        return values

    def write_refresh_token(self, value: str, *, timeout: float = 300.0, interval: float = 15.0) -> None:
        """Replace the whole field object (Connect ignores ``/fields/<id>/value``) and poll the readback."""
        field = self.field(self.fetch(), "refresh_token")
        patch = [{"op": "replace", "path": f"/fields/{field['id']}", "value": {**field, "value": value}}]
        self._call("PATCH", json.dumps(patch).encode())
        deadline = time.monotonic() + timeout
        while self.field(self.fetch(), "refresh_token").get("value") != value:
            if time.monotonic() >= deadline:
                raise RuntimeError("Connect readback does not show the new refresh token")
            time.sleep(interval)


class ConnectOAuth:
    def __init__(self, item: ConnectItem, cache: Path, *, clock: Callable[[], float] = time.time,
                 post: Callable[[dict[str, str]], Any] | None = None) -> None:
        self.item, self.cache, self.clock = item, Path(cache), clock
        self._post = post or self._post_refresh
        self._lock = threading.Lock()
        self._retry_at = 0.0

    def _read(self) -> dict[str, Any]:
        if not self.cache.exists():
            return {}
        try:
            value = json.loads(read_private(self.cache, "OAuth cache"))
        except ValueError:
            return {}
        return value if isinstance(value, dict) else {}

    def _write(self, value: dict[str, Any]) -> None:
        self.cache.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=self.cache.parent, delete=False, encoding="utf-8") as stream:
            os.chmod(stream.fileno(), 0o600)
            json.dump(value, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(stream.name, self.cache)

    def _locked(self):
        lock = self.cache.with_suffix(".lock")
        lock.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(lock, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        return fd

    def __call__(self) -> str:
        fd = self._locked()
        try:
            with self._lock:
                cache = self._read()
                self._store_rotated(cache)
                expires = cache.get("expires_at")
                if cache.get("access_token") and isinstance(expires, (int, float)) and expires > self.clock() + MARGIN:
                    return str(cache["access_token"])
                return self._refresh(cache)
        finally:
            os.close(fd)

    def invalidate(self) -> None:
        with self._lock:
            cache = self._read()
            cache["expires_at"] = 0
            self._write(cache)

    def _store_rotated(self, cache: dict[str, Any]) -> None:
        rotated = cache.get(ROTATED)
        if not rotated or self.clock() < self._retry_at:
            return
        try:
            self.item.write_refresh_token(rotated)
        except Exception as exc:  # noqa: BLE001 - Connect outage must not block Linear calls
            self._retry_at = self.clock() + 300
            log.warning("rotated Linear refresh token not yet stored in Connect: %s", exc)
            return
        cache.pop(ROTATED, None)
        self._write(cache)

    def _refresh(self, cache: dict[str, Any]) -> str:
        creds = self.item.credentials()
        refresh = cache.get(ROTATED) or creds["refresh_token"]
        result = self._post({"grant_type": "refresh_token", "refresh_token": refresh,
                             "client_id": creds["client_id"], "client_secret": creds["client_secret"]})
        if not isinstance(result, dict) or not result.get("access_token") or not result.get("refresh_token"):
            raise RuntimeError("Linear token endpoint returned an unusable response")
        cache = {"access_token": result["access_token"], "expires_at": self.clock() + float(result.get("expires_in", 0)),
                 ROTATED: result["refresh_token"]}
        self._write(cache)
        self._retry_at = 0.0
        self._store_rotated(cache)
        return str(result["access_token"])

    @staticmethod
    def _post_refresh(form: dict[str, str]) -> Any:
        request = urllib.request.Request(TOKEN_ENDPOINT, data=urllib.parse.urlencode(form).encode(), method="POST",
                                         headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(request, timeout=15) as response:  # nosec B310: fixed https endpoint
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code in (400, 401):
                raise ReauthorizationRequired("Linear rejected the refresh token; reauthorize this profile's app") from exc
            raise RuntimeError(f"Linear token endpoint HTTP {exc.code}; will retry") from exc


class TokenFile:
    def __init__(self, path: Path | str) -> None:
        self.path = path

    def __call__(self) -> str:
        return read_private(self.path, "Linear token file").strip()


def token_provider(settings: dict[str, Any], home: Path) -> Callable[[], str]:
    creds = settings.get("credentials") or {}
    if creds.get("mode") == "token_file":
        return TokenFile(Path(creds.get("path") or home / "secrets" / "linear-token"))
    if creds.get("mode") != "connect":
        raise RuntimeError("linear: settings.credentials.mode must be 'connect' or 'token_file'")
    env = load_env(Path(creds.get("connect_env_file") or home / ".op.env"))
    item = ConnectItem(env.get("OP_CONNECT_HOST", ""), env.get("OP_CONNECT_TOKEN", ""),
                       str(creds.get("vault_id", "")), str(creds.get("item_id", "")))
    return ConnectOAuth(item, Path(creds.get("cache_file") or home / "secrets" / "linear-oauth.json"))
