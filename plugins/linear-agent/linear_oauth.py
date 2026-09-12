"""Linear OAuth credential provider with 1Password Connect as the source of truth.

The Connect item holds ``client_id``, ``client_secret`` and the current
``refresh_token``. The profile-local cache file holds only the short-lived
``access_token``, its ``expires_at``, and (transiently) a ``rotated_refresh_token``
that Linear issued but Connect has not yet accepted. Nothing else is durable
locally: delete the cache file and the next ``token()`` rebuilds it from Connect.
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
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_TOKEN_ENDPOINT = "https://api.linear.app/oauth/token"
_REAUTH_SCRIPT = "/opt/hermes-fleet/bin/linear-reauth"
_ACCESS_TOKEN_MARGIN_SECONDS = 60
# The rotated-token key is deliberately NOT the legacy "pending_refresh_token".
# The old format staged that key BEFORE calling Linear, so a stale one may hold a
# token Linear never issued; writing it to Connect would clobber a good credential.
# A new name means legacy files are ignored by construction.
_ROTATED_KEY = "rotated_refresh_token"
_PENDING_RETRY_MIN_SECONDS = 60.0
_PENDING_RETRY_MAX_SECONDS = 3600.0
_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "client_id": ("client_id", "username"),
    "client_secret": ("client_secret", "credential", "password"),
    "refresh_token": ("refresh_token",),
}

log = logging.getLogger("linear-agent.oauth")


class ReauthorizationRequired(RuntimeError):
    """Linear rejected the refresh token; a human must run the reauth script."""


def validate_private_directory(path: Path) -> None:
    """Warn (never refuse) when a profile directory looks more open than expected."""
    try:
        info = path.lstat()
    except OSError as exc:
        log.warning("Linear OAuth directory %s is unavailable: %s", path, exc)
        return
    if not stat.S_ISDIR(info.st_mode):
        log.warning("Linear OAuth path %s is not a directory", path)
    elif info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        log.warning("Linear OAuth directory %s is owned by uid %s with mode %o", path, info.st_uid, stat.S_IMODE(info.st_mode))


def _open_private_file(path: os.PathLike[str] | str, what: str) -> int:
    """Open a regular, uid-owned, 0600-or-tighter file without following a symlink."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError(f"{what} is unavailable: {path}") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError(f"{what} must be a regular file: {path}")
        if info.st_uid != os.geteuid():
            raise RuntimeError(f"{what} must be owned by uid {os.geteuid()}: {path}")
        if stat.S_IMODE(info.st_mode) & 0o077:
            raise RuntimeError(f"{what} must not be group/other readable (chmod 600): {path}")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def load_connect_env(path: os.PathLike[str] | str) -> tuple[str, str]:
    """Read OP_CONNECT_HOST and OP_CONNECT_TOKEN from a private ``.op.env`` file."""
    descriptor = _open_private_file(path, "1Password Connect environment file")
    with os.fdopen(descriptor, "rb") as stream:
        raw = stream.read(65536)
    values: dict[str, str] = {}
    for line in raw.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value.strip()
    host, token = values.get("OP_CONNECT_HOST", ""), values.get("OP_CONNECT_TOKEN", "")
    if not host or not token:
        raise RuntimeError(f"1Password Connect environment file lacks OP_CONNECT_HOST or OP_CONNECT_TOKEN: {path}")
    if not host.startswith(("http://", "https://")):
        raise RuntimeError(f"OP_CONNECT_HOST must be an http(s) URL: {host}")
    return host.rstrip("/"), token


class ConnectItem:
    """One 1Password Connect item, addressed by vault id and item id only."""

    def __init__(
        self,
        host: str,
        token: str,
        vault_id: str,
        item_id: str,
        *,
        transport: Callable[[str, str, dict[str, str], bytes | None], Any] | None = None,
    ) -> None:
        if not host or not token or not vault_id or not item_id:
            raise ValueError("1Password Connect host, token, vault id and item id are all required")
        self.host = host.rstrip("/")
        self.vault_id = vault_id
        self.item_id = item_id
        self._token = token
        self._transport = transport or self._http_transport
        self._path = f"/v1/vaults/{vault_id}/items/{item_id}"

    @staticmethod
    def _http_transport(method: str, url: str, headers: dict[str, str], body: bytes | None = None) -> Any:
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=15) as response:  # nosec B310: host from private .op.env
            return json.load(response)

    def _request(self, method: str, body: bytes | None = None) -> Any:
        headers = {"Authorization": "Bearer " + self._token, "Content-Type": "application/json"}
        try:
            return self._transport(method, self.host + self._path, headers, body)
        except Exception as exc:
            raise RuntimeError(f"1Password Connect {method} {self._path} failed: {exc}") from exc

    def fetch(self) -> dict[str, Any]:
        item = self._request("GET")
        if not isinstance(item, dict):
            raise RuntimeError("1Password Connect returned a non-object item")
        vault = item.get("vault") if isinstance(item.get("vault"), dict) else {}
        if str(item.get("id") or "") != self.item_id or str(vault.get("id") or "") != self.vault_id:
            raise RuntimeError(
                f"1Password Connect item does not match the configured binding "
                f"(expected vault {self.vault_id} item {self.item_id})"
            )
        return item

    @staticmethod
    def resolve_field(item: dict[str, Any], name: str) -> dict[str, Any]:
        """Find the field for ``name`` by id or label, case-insensitively, canonical name first."""
        fields = [field for field in item.get("fields") or [] if isinstance(field, dict)]
        for alias in _FIELD_ALIASES[name]:
            matches = [
                field for field in fields
                if alias in {str(field.get("id") or "").lower(), str(field.get("label") or "").lower()}
            ]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                raise RuntimeError(f"1Password Connect item has {len(matches)} fields named {alias!r}; expected one")
        raise RuntimeError(
            f"1Password Connect item has no {name} field (looked for id or label in {list(_FIELD_ALIASES[name])})"
        )

    def credentials(self, item: dict[str, Any] | None = None) -> dict[str, str]:
        """Return client_id, client_secret and refresh_token; every value must be non-empty."""
        item = self.fetch() if item is None else item
        values: dict[str, str] = {}
        for name in _FIELD_ALIASES:
            value = str(self.resolve_field(item, name).get("value") or "").strip()
            if not value:
                raise RuntimeError(f"1Password Connect item field {name} is empty")
            values[name] = value
        return values

    def write_refresh_token(self, value: str) -> None:
        """PATCH the refresh token field, then read it back once to confirm."""
        if not value:
            raise ValueError("refresh token to store is empty")
        field = self.resolve_field(self.fetch(), "refresh_token")
        field_id = str(field.get("id") or "")
        if not field_id:
            raise RuntimeError("1Password Connect refresh_token field has no id")
        patch = json.dumps(
            [{"op": "replace", "path": f"/fields/{field_id}/value", "value": value}],
            separators=(",", ":"),
        ).encode("utf-8")
        self._request("PATCH", patch)
        stored = str(self.resolve_field(self.fetch(), "refresh_token").get("value") or "").strip()
        if stored != value:
            raise RuntimeError("1Password Connect readback after PATCH does not show the new refresh token")


def _read_cache(path: Path) -> dict[str, Any]:
    if not path.exists() and not path.is_symlink():
        return {}
    descriptor = _open_private_file(path, "Linear OAuth cache file")
    with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
        try:
            value = json.load(stream)
        except json.JSONDecodeError:
            log.warning("Linear OAuth cache %s is not valid JSON; treating as empty", path)
            return {}
    return value if isinstance(value, dict) else {}


def _write_cache(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, prefix=".linear-oauth.", delete=False) as stream:
            temporary = Path(stream.name)
            os.chmod(stream.fileno(), 0o600)
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class LinearOAuth:
    """Access-token provider: cache locally, refresh via Linear, persist the rotation in Connect."""

    def __init__(
        self,
        connect_item: ConnectItem,
        cache_path: Path,
        *,
        profile: str = "",
        now: Callable[[], float] = time.time,
    ) -> None:
        self.connect_item = connect_item
        self.cache_path = Path(cache_path)
        self.profile = profile
        self._now = now
        self._lock = threading.Lock()
        self._next_pending_retry = 0.0
        self._pending_retry_delay = 0.0

    @contextmanager
    def _locked(self):
        lock_path = self.cache_path.with_suffix(".lock")
        lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(lock_path, flags, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            with self._lock:
                yield
        finally:
            os.close(descriptor)

    def __call__(self) -> str:
        return self.token()

    def token(self) -> str:
        with self._locked():
            cache = _read_cache(self.cache_path)
            self._retry_pending(cache)
            access_token = cache.get("access_token")
            expires_at = cache.get("expires_at")
            if (
                isinstance(access_token, str) and access_token
                and isinstance(expires_at, (int, float)) and not isinstance(expires_at, bool)
                and float(expires_at) > self._now() + _ACCESS_TOKEN_MARGIN_SECONDS
            ):
                return access_token
            return self._refresh(cache)

    def invalidate(self) -> None:
        """Force the next ``token()`` to refresh (used after an authenticated 401)."""
        with self._locked():
            cache = _read_cache(self.cache_path)
            cache["expires_at"] = 0
            _write_cache(self.cache_path, cache)

    def install_refresh_token(self, refresh_token: str) -> None:
        """Store a brand-new refresh token in Connect and discard the local cache (reauthorization)."""
        with self._locked():
            self.connect_item.write_refresh_token(refresh_token)
            self.cache_path.unlink(missing_ok=True)

    def _defer_pending_retry(self, exc: Exception) -> None:
        """Back off before touching Connect again, and say when we will."""
        self._pending_retry_delay = min(max(self._pending_retry_delay * 2, _PENDING_RETRY_MIN_SECONDS), _PENDING_RETRY_MAX_SECONDS)
        self._next_pending_retry = self._now() + self._pending_retry_delay
        log.warning(
            "Linear OAuth rotated refresh token for %s not stored in Connect (retrying in %.0fs): %s",
            self.profile or self.cache_path, self._pending_retry_delay, exc,
        )

    def _retry_pending(self, cache: dict[str, Any]) -> None:
        """Push a rotated token that Connect has not accepted yet.

        Rate limited: this runs on every ``token()`` call, and ``token()`` runs on
        every Linear API request. Without a floor, a Connect outage would put a
        GET+PATCH+GET (or a 15s timeout) in front of every GraphQL call.
        """
        rotated = cache.get(_ROTATED_KEY)
        if not isinstance(rotated, str) or not rotated:
            return
        if self._now() < self._next_pending_retry:
            return
        try:
            self.connect_item.write_refresh_token(rotated)
        except Exception as exc:
            self._defer_pending_retry(exc)
            return
        self._pending_retry_delay = 0.0
        self._next_pending_retry = 0.0
        cache.pop(_ROTATED_KEY, None)
        _write_cache(self.cache_path, cache)

    def _refresh(self, cache: dict[str, Any]) -> str:
        pending = cache.get(_ROTATED_KEY)
        credentials = self.connect_item.credentials()
        refresh_token = pending if isinstance(pending, str) and pending else credentials["refresh_token"]
        refreshed = self._post_refresh(credentials["client_id"], credentials["client_secret"], refresh_token)
        access_token, new_refresh_token, expires_in = self._validated(refreshed)
        cache = {
            "access_token": access_token,
            "expires_at": self._now() + expires_in,
            _ROTATED_KEY: new_refresh_token,
        }
        _write_cache(self.cache_path, cache)
        try:
            self.connect_item.write_refresh_token(new_refresh_token)
        except Exception as exc:
            self._defer_pending_retry(exc)
            return access_token
        cache.pop(_ROTATED_KEY, None)
        _write_cache(self.cache_path, cache)
        return access_token

    def _post_refresh(self, client_id: str, client_secret: str, refresh_token: str) -> Any:
        body = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }).encode("utf-8")
        request = urllib.request.Request(
            _TOKEN_ENDPOINT, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:  # nosec B310: fixed HTTPS endpoint
                raw = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code in (400, 401):
                raise ReauthorizationRequired(
                    f"Linear rejected the refresh token for profile {self.profile or '?'} (HTTP {exc.code}); "
                    f"run {_REAUTH_SCRIPT} {self.profile or '<profile>'} to reauthorize"
                ) from exc
            raise RuntimeError(f"Linear token endpoint returned HTTP {exc.code}; will retry later") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"Linear token endpoint unreachable; will retry later: {exc}") from exc
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise RuntimeError("Linear token endpoint returned non-JSON") from exc

    @staticmethod
    def _validated(refreshed: Any) -> tuple[str, str, float]:
        if not isinstance(refreshed, dict):
            raise RuntimeError("Linear token endpoint returned a non-object response")
        access_token = refreshed.get("access_token")
        refresh_token = refreshed.get("refresh_token")
        expires_in = refreshed.get("expires_in")
        if not isinstance(access_token, str) or not access_token:
            raise RuntimeError("Linear token endpoint returned no access_token")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise RuntimeError("Linear token endpoint returned no refresh_token")
        if isinstance(expires_in, bool) or not isinstance(expires_in, (int, float)) or expires_in <= _ACCESS_TOKEN_MARGIN_SECONDS:
            raise RuntimeError(f"Linear token endpoint returned an unusable expires_in: {expires_in!r}")
        return access_token, refresh_token, float(expires_in)


def make_oauth(profile: str, home: Path, vault_id: str, item_id: str) -> LinearOAuth:
    """Build the provider for a profile home: ``home/.op.env`` and ``home/secrets/linear-oauth.json``."""
    host, token = load_connect_env(home / ".op.env")
    item = ConnectItem(host, token, vault_id, item_id)
    return LinearOAuth(item, home / "secrets" / "linear-oauth.json", profile=profile)
