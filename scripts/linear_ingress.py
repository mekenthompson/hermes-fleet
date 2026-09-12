#!/usr/bin/env python3
"""Shared Linear webhook ingress primitives.

The service wrapper owns HTTP.  This module deliberately owns only strict route
validation, raw-body signature verification, and a committed SQLite inbox row.
"""
from __future__ import annotations

import dataclasses
import errno
import hashlib
import hmac
import json
import os
import sqlite3
import stat
import time
from contextlib import contextmanager
from pathlib import Path

DEFAULT_MAX_PENDING_DELIVERIES = 1_000
DEFAULT_MAX_PENDING_PAYLOAD_BYTES = 16 * 1_024 * 1_024
DEFAULT_RESERVED_STOP_DELIVERIES = 32
DEFAULT_RESERVED_STOP_PAYLOAD_BYTES = 512 * 1_024
DEFAULT_MAX_RETAINED_DELIVERIES = 10_000
DEFAULT_MAX_RETAINED_PAYLOAD_BYTES = 64 * 1_024 * 1_024


@dataclasses.dataclass(frozen=True)
class Route:
    logical_agent: str
    profile: str
    path: str
    secret_file: Path
    inbox: Path
    max_pending_deliveries: int = DEFAULT_MAX_PENDING_DELIVERIES
    max_pending_payload_bytes: int = DEFAULT_MAX_PENDING_PAYLOAD_BYTES
    reserved_stop_deliveries: int = DEFAULT_RESERVED_STOP_DELIVERIES
    reserved_stop_payload_bytes: int = DEFAULT_RESERVED_STOP_PAYLOAD_BYTES
    # Processed payloads are pruned oldest-first; pending work is never pruned.
    max_retained_deliveries: int = DEFAULT_MAX_RETAINED_DELIVERIES
    max_retained_payload_bytes: int = DEFAULT_MAX_RETAINED_PAYLOAD_BYTES


@dataclasses.dataclass(frozen=True)
class IngestResult:
    accepted: bool
    duplicate: bool
    status: int


class IngressAdmissionFull(Exception):
    """A signed novel delivery cannot be committed within its inbox budget."""


def validate_admission_limits(route: Route) -> tuple[int, int, int, int]:
    values = (
        route.max_pending_deliveries,
        route.max_pending_payload_bytes,
        route.reserved_stop_deliveries,
        route.reserved_stop_payload_bytes,
    )
    if any(type(value) is not int for value in values):
        raise ValueError("ingress admission limits must be integers")
    maximum_rows, maximum_bytes, reserved_rows, reserved_bytes = values
    retained_values = (route.max_retained_deliveries, route.max_retained_payload_bytes)
    if any(type(value) is not int for value in retained_values) or any(value < 1 for value in retained_values):
        raise ValueError("ingress retained-history limits must be positive integers")
    if maximum_rows < 1 or maximum_bytes < 1:
        raise ValueError("ingress admission maximums must be positive")
    if not 1 <= reserved_rows <= maximum_rows:
        raise ValueError("reserved stop delivery capacity must be within the maximum")
    if not 1 <= reserved_bytes <= maximum_bytes:
        raise ValueError("reserved stop payload capacity must be within the maximum")
    return values


def _is_stop_control(body: bytes) -> bool:
    try:
        event = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    activity = event.get("agentActivity") if isinstance(event, dict) else None
    return bool(
        isinstance(activity, dict)
        and event.get("type") == "AgentSessionEvent"
        and event.get("action") == "prompted"
        and activity.get("signal") == "stop"
    )


def canonical_route_path(path: str) -> str:
    """Accept one exact, unescaped route path and reject normalisation tricks."""
    if not isinstance(path, str) or not path.startswith("/"):
        raise ValueError("route must be an absolute path")
    if any(marker in path for marker in ("?", "#", "%", "\\")):
        raise ValueError("route must be unescaped path-only text")
    if "//" in path or path.endswith("/"):
        raise ValueError("route must not have empty or trailing components")
    parts = path.split("/")[1:]
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("route contains an invalid component")
    return path


def _read_secret(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ValueError("webhook secret must not be a symlink") from exc
        raise
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("webhook secret must be a regular file")
        if info.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise ValueError("webhook secret permissions are too broad")
        with os.fdopen(descriptor, "r", encoding="utf-8", closefd=False) as stream:
            for line in stream:
                if line.startswith("LINEAR_WEBHOOK_SECRET="):
                    value = line.split("=", 1)[1].strip()
                    if value:
                        return value.encode("utf-8")
    finally:
        os.close(descriptor)
    raise ValueError("webhook secret is missing")


def _valid_signature(secret: bytes, body: bytes, supplied: str) -> bool:
    if not isinstance(supplied, str):
        return False
    try:
        actual = bytes.fromhex(supplied)
    except ValueError:
        return False
    expected = hmac.new(secret, body, hashlib.sha256).digest()
    return hmac.compare_digest(expected, actual)


class IngressStore:
    """A small durable inbox with per-logical-agent delivery idempotency."""

    def __init__(self, database: Path) -> None:
        database.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.database = database
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute(
                """CREATE TABLE IF NOT EXISTS deliveries (
                    logical_agent TEXT NOT NULL,
                    delivery_id TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    received_at INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (logical_agent, delivery_id)
                )"""
            )

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.database, isolation_level=None, timeout=5.0)
        try:
            yield connection
        finally:
            connection.close()

    def _prune_retained(self, conn: sqlite3.Connection, route: Route, incoming_bytes: int) -> None:
        """Bound raw retained ingress without deleting pending deliveries."""
        while True:
            count, byte_count = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(length(payload)), 0) FROM deliveries"
            ).fetchone()
            if count + 1 <= route.max_retained_deliveries and byte_count + incoming_bytes <= route.max_retained_payload_bytes:
                return
            oldest = conn.execute(
                "SELECT logical_agent, delivery_id FROM deliveries WHERE status != 'pending' "
                "ORDER BY received_at, rowid LIMIT 1"
            ).fetchone()
            if oldest is None:
                raise IngressAdmissionFull("retained ingress capacity is exhausted by pending deliveries")
            conn.execute("DELETE FROM deliveries WHERE logical_agent = ? AND delivery_id = ?", oldest)

    def enqueue(self, route: Route, delivery_id: str, body: bytes) -> bool:
        if not delivery_id or len(delivery_id) > 256:
            raise ValueError("delivery id is invalid")
        maximum_rows, maximum_bytes, reserved_rows, reserved_bytes = (
            validate_admission_limits(route)
        )
        digest = hashlib.sha256(body).hexdigest()
        is_stop = _is_stop_control(body)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT payload_sha256 FROM deliveries WHERE logical_agent = ? AND delivery_id = ?",
                (route.logical_agent, delivery_id),
            ).fetchone()
            if existing is not None:
                conn.execute("COMMIT")
                if existing[0] != digest:
                    raise ValueError("delivery id was reused with different content")
                return True
            self._prune_retained(conn, route, len(body))
            pending_rows, pending_bytes = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(length(payload)), 0) "
                "FROM deliveries WHERE status = 'pending'"
            ).fetchone()
            row_limit = maximum_rows if is_stop else maximum_rows - reserved_rows
            byte_limit = maximum_bytes if is_stop else maximum_bytes - reserved_bytes
            if pending_rows >= row_limit or pending_bytes + len(body) > byte_limit:
                conn.execute("ROLLBACK")
                raise IngressAdmissionFull("pending ingress capacity is exhausted")
            conn.execute(
                """INSERT INTO deliveries
                (logical_agent, delivery_id, profile, payload, payload_sha256, received_at)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (route.logical_agent, delivery_id, route.profile, body, digest, int(time.time())),
            )
            conn.execute("COMMIT")
        return False

    def delivery_count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0])


def ingest_delivery(
    store: IngressStore,
    route: Route,
    request_path: str,
    delivery_id: str,
    raw_body: bytes,
    signature: str,
) -> IngestResult:
    """Verify one raw delivery and commit it before returning success."""
    try:
        if canonical_route_path(request_path) != canonical_route_path(route.path):
            return IngestResult(False, False, 404)
        if not _valid_signature(_read_secret(route.secret_file), raw_body, signature):
            return IngestResult(False, False, 401)
        duplicate = store.enqueue(route, delivery_id, raw_body)
    except ValueError:
        return IngestResult(False, False, 400)
    except IngressAdmissionFull:
        return IngestResult(False, False, 503)
    except (OSError, sqlite3.Error):
        return IngestResult(False, False, 503)
    return IngestResult(True, duplicate, 200)
