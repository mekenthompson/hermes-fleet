"""Shared Linear API quota cooldown and read-only GraphQL cache.

Linear bills every fleet token against the configured user/workspace cap. After a
rate-limit, every GraphQL caller must stop immediately and honor a cooldown
instead of retrying. Read queries may be reused for a short TTL; mutations
are never cached.
"""
from __future__ import annotations

import fcntl
import json
import math
import os
import sys
import time
import urllib.error
from collections.abc import Callable, Mapping
from pathlib import Path

IDLE_POLL_SECONDS = 5.0
CLOSEOUT_RETRY_SECONDS = 30.0
DEFAULT_COOLDOWN_SECONDS = 3600.0
MALFORMED_COOLDOWN_SECONDS = 60.0
VIEWER_CACHE_SECONDS = 300.0
ISSUE_CACHE_SECONDS = 15.0
MAX_RETRY_AFTER_SECONDS = 86400.0
MAX_CACHE_ITEMS = 128
_LIVE_QUOTA_DIR = Path("/opt/data/shared/linear-quota")


class LinearQuotaExceeded(RuntimeError):
    """Linear GraphQL is cooling down; do not send another request."""

    def __init__(self, retry_after: float) -> None:
        super().__init__("Linear API quota is cooling down")
        self.retry_after = float(retry_after)


def default_state_path() -> Path | None:
    env = os.environ.get("LINEAR_QUOTA_STATE_PATH", "").strip()
    if env:
        return Path(env)
    return Path("/opt/data/shared/linear-quota/quota.json")


def default_cache_path() -> Path | None:
    env = os.environ.get("LINEAR_READ_CACHE_PATH", "").strip()
    if env:
        return Path(env)
    quota = default_state_path()
    if quota is None:
        return None
    return quota.with_name("reads.json")


def _file_io_allowed(path: Path) -> bool:
    """Refuse to clobber the live shared dir from unittest processes."""
    try:
        parent = path.expanduser().resolve().parent
    except OSError:
        parent = path.parent
    if "unittest" in sys.modules and parent == _LIVE_QUOTA_DIR:
        return False
    return True


def cache_ttl_seconds(query: str) -> float | None:
    stripped = query.lstrip()
    lower = stripped.lower()
    if lower.startswith("mutation") or not lower.startswith("query"):
        return None
    if "LinearIssueActor" in query or "LinearAgentReadiness" in query:
        return VIEWER_CACHE_SECONDS
    return ISSUE_CACHE_SECONDS


def _header_retry_after(headers: object) -> float | None:
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if not callable(getter):
        return None
    raw = getter("Retry-After")
    if raw is None:
        raw = getter("retry-after")
    if raw is None:
        return None
    try:
        value = float(str(raw).strip())
    except ValueError:
        return None
    if value <= 0 or not math.isfinite(value):
        return None
    return min(value, MAX_RETRY_AFTER_SECONDS)


def _body_text(body: object) -> str:
    if body is None:
        return ""
    if isinstance(body, bytes):
        return body.decode("utf-8", "replace")
    return str(body)


def rate_limit_cooldown_seconds(status: int, headers: object, body: object) -> float | None:
    if status == 401:
        return None
    text = _body_text(body).lower()
    limited = status in {429, 503} or "rate limit" in text or "ratelimit" in text
    if not limited:
        return None
    return _header_retry_after(headers) or DEFAULT_COOLDOWN_SECONDS


def read_http_error_body(exc: BaseException) -> bytes:
    reader = getattr(exc, "read", None)
    if not callable(reader):
        return b""
    try:
        raw = reader()
    except Exception:  # noqa: BLE001 - error body is diagnostic only
        return b""
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, bytearray):
        return bytes(raw)
    return b""


class LinearQuotaGate:
    """Process memory plus optional flocked file so agents share one cooldown."""

    def __init__(
        self,
        path: Path | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self.path = path
        self.now = now
        self._memory_until = 0.0

    @classmethod
    def shared(
        cls,
        path: Path | None = None,
        now: Callable[[], float] = time.time,
    ) -> "LinearQuotaGate":
        return cls(path=path if path is not None else default_state_path(), now=now)

    def remaining_seconds(self) -> float:
        until = self._memory_until
        if self.path is not None and _file_io_allowed(self.path):
            until = max(until, self._read_until())
        return max(0.0, until - float(self.now()))

    def raise_if_cooling_down(self) -> None:
        remaining = self.remaining_seconds()
        if remaining > 0:
            raise LinearQuotaExceeded(remaining)

    def record_cooldown(self, seconds: float) -> None:
        value = float(seconds)
        if value <= 0 or not math.isfinite(value):
            return
        until = float(self.now()) + min(value, MAX_RETRY_AFTER_SECONDS)
        self._memory_until = max(self._memory_until, until)
        if self.path is not None:
            self._write_until(until)

    def observe_http_error(self, exc: BaseException) -> None:
        status = int(getattr(exc, "code", 0) or 0)
        cooldown = rate_limit_cooldown_seconds(status, getattr(exc, "headers", None), read_http_error_body(exc))
        if cooldown is None:
            return
        self.record_cooldown(cooldown)
        raise LinearQuotaExceeded(cooldown)

    def observe_payload(self, payload: object, raw: bytes | None = None) -> None:
        body: object = raw if raw is not None else json.dumps(payload) if isinstance(payload, (dict, list)) else payload
        cooldown = rate_limit_cooldown_seconds(200, {}, body)
        if cooldown is None:
            return
        self.record_cooldown(cooldown)
        raise LinearQuotaExceeded(cooldown)

    def _read_until(self) -> float:
        assert self.path is not None
        try:
            handle = self.path.open("r+", encoding="utf-8")
        except FileNotFoundError:
            return 0.0
        except OSError:
            return float(self.now()) + MALFORMED_COOLDOWN_SECONDS
        with handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            raw = handle.read()
            if not raw.strip():
                return 0.0
            try:
                payload = json.loads(raw)
                until = payload.get("cooldown_until") if isinstance(payload, dict) else None
                value = float(until) if isinstance(until, (int, float, str)) else float("nan")
            except (TypeError, ValueError, json.JSONDecodeError):
                value = float("nan")
            if math.isfinite(value):
                return value
            healed = float(self.now()) + MALFORMED_COOLDOWN_SECONDS
            handle.seek(0)
            handle.truncate()
            json.dump({"cooldown_until": healed, "reason": "malformed"}, handle)
            return healed

    def _parse_until(self, raw: str) -> float:
        if not raw.strip():
            return 0.0
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return float(self.now()) + MALFORMED_COOLDOWN_SECONDS
        if not isinstance(payload, dict):
            return float(self.now()) + MALFORMED_COOLDOWN_SECONDS
        until = payload.get("cooldown_until")
        if not isinstance(until, (int, float, str)):
            return float(self.now()) + MALFORMED_COOLDOWN_SECONDS
        try:
            value = float(until)
        except (TypeError, ValueError):
            return float(self.now()) + MALFORMED_COOLDOWN_SECONDS
        if not math.isfinite(value):
            return float(self.now()) + MALFORMED_COOLDOWN_SECONDS
        return value

    def _write_until(self, until: float) -> None:
        assert self.path is not None
        if not _file_io_allowed(self.path):
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.seek(0)
            existing = handle.read()
            merged = max(until, self._parse_until(existing) if existing.strip() else 0.0)
            handle.seek(0)
            handle.truncate()
            json.dump({"cooldown_until": merged, "reason": "rate_limit"}, handle)


class LinearReadCache:
    """Short-lived cache for read-only GraphQL. Mutations never enter.

    Process memory plus an optional flocked file so every agent reuses the
    same viewer/issue reads against one configured workspace cap.
    """

    def __init__(
        self,
        path: Path | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self.path = path
        self.now = now
        self._items: dict[str, tuple[float, dict[str, object]]] = {}

    @classmethod
    def shared(
        cls,
        path: Path | None = None,
        now: Callable[[], float] = time.time,
    ) -> "LinearReadCache":
        return cls(path=path if path is not None else default_cache_path(), now=now)

    @staticmethod
    def _key(query: str, variables: Mapping[str, object] | None) -> str:
        blob = json.dumps(
            {"query": query, "variables": dict(variables or {})},
            separators=(",", ":"),
            sort_keys=True,
        )
        return blob

    def get(self, query: str, variables: Mapping[str, object] | None) -> dict[str, object] | None:
        ttl = cache_ttl_seconds(query)
        if ttl is None:
            return None
        key = self._key(query, variables)
        item = self._items.get(key)
        if item is not None:
            expires, payload = item
            if float(self.now()) < expires:
                return payload
            self._items.pop(key, None)
        loaded = self._load_item(key)
        if loaded is None:
            return None
        expires, payload = loaded
        if float(self.now()) >= expires:
            return None
        self._items[key] = (expires, payload)
        return payload

    def put(
        self,
        query: str,
        variables: Mapping[str, object] | None,
        payload: object,
    ) -> None:
        ttl = cache_ttl_seconds(query)
        if ttl is None or not isinstance(payload, dict) or payload.get("errors"):
            return
        key = self._key(query, variables)
        expires = float(self.now()) + ttl
        self._items[key] = (expires, payload)
        self._store_item(key, expires, payload)

    def _load_item(self, key: str) -> tuple[float, dict[str, object]] | None:
        store = self._read_store()
        item = store.get(key)
        if item is None:
            return None
        return item

    def _store_item(self, key: str, expires: float, payload: dict[str, object]) -> None:
        if self.path is None or not _file_io_allowed(self.path):
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.seek(0)
            store = _parse_cache_store(handle.read())
            now = float(self.now())
            store = {
                item_key: value
                for item_key, value in store.items()
                if value[0] > now
            }
            store[key] = (expires, payload)
            if len(store) > MAX_CACHE_ITEMS:
                ordered = sorted(store.items(), key=lambda item: item[1][0])
                store = dict(ordered[-MAX_CACHE_ITEMS:])
            handle.seek(0)
            handle.truncate()
            json.dump(
                {
                    "items": {
                        item_key: {"expires": value[0], "payload": value[1]}
                        for item_key, value in store.items()
                    }
                },
                handle,
            )

    def _read_store(self) -> dict[str, tuple[float, dict[str, object]]]:
        if self.path is None or not _file_io_allowed(self.path):
            return {}
        try:
            handle = self.path.open("r+", encoding="utf-8")
        except FileNotFoundError:
            return {}
        except OSError:
            return {}
        with handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
            return _parse_cache_store(handle.read())


def _parse_cache_store(raw: str) -> dict[str, tuple[float, dict[str, object]]]:
    if not raw.strip():
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, dict):
        return {}
    parsed: dict[str, tuple[float, dict[str, object]]] = {}
    for key, value in items.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        body = value.get("payload")
        if not isinstance(body, dict):
            continue
        raw_expires = value.get("expires")
        if not isinstance(raw_expires, (int, float, str)):
            continue
        try:
            until = float(raw_expires)
        except (TypeError, ValueError):
            continue
        if math.isfinite(until):
            parsed[key] = (until, body)
    return parsed


def run_cached_graphql(
    query: str,
    variables: Mapping[str, object] | None,
    send: Callable[[bytes], bytes],
    *,
    quota: LinearQuotaGate | None = None,
    cache: LinearReadCache | None = None,
) -> dict[str, object]:
    """Honor the shared cooldown and read cache, then call send(encoded)."""
    gate = quota if quota is not None else LinearQuotaGate.shared()
    reads = cache if cache is not None else LinearReadCache.shared()
    gate.raise_if_cooling_down()
    hit = reads.get(query, variables)
    if hit is not None:
        return hit
    encoded = json.dumps(
        {"query": query, "variables": dict(variables or {})},
        separators=(",", ":"),
    ).encode("utf-8")
    try:
        raw = send(encoded)
    except urllib.error.HTTPError as exc:
        gate.observe_http_error(exc)
        raise
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("Linear GraphQL returned a non-object")
    gate.observe_payload(payload, raw)
    reads.put(query, variables, payload)
    return payload

