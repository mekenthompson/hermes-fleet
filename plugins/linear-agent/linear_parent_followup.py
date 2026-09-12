"""Durable, fail-closed parent follow-up queue.

Wakeups are addressed from ParentOwner, never from event-supplied session IDs.
Trusted summaries may contain identifiers and state tokens only.
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

try:
    from .linear_parent_continuation import ParentOwner, decide_parent_wake
except ImportError:  # direct module invocation in the plugin directory
    from linear_parent_continuation import ParentOwner, decide_parent_wake

_SUMMARY = re.compile(r"^[A-Za-z0-9._:-]+ (completed|started|blocked|stale|missing-evidence)$")


def _event_dict(raw: object) -> dict[str, object]:
    event: object
    if isinstance(raw, (bytes, bytearray)):
        try:
            event = json.loads(bytes(raw))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
    elif isinstance(raw, str):
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return {}
    else:
        event = raw
    return event if isinstance(event, dict) else {}


def _session_dict(event: dict[str, object]) -> dict[str, object]:
    session = event.get("agentSession")
    if not isinstance(session, dict):
        data = event.get("data")
        session = data.get("agentSession") if isinstance(data, dict) else None
    return session if isinstance(session, dict) else {}


def _exact_id(value: object) -> str:
    if isinstance(value, str) and value and value.strip() == value:
        return value
    return ""


def parent_id_from_session_event(raw: object) -> str:
    """Return parent issue id from a Linear session event. Missing/malformed is empty."""
    issue = _session_dict(_event_dict(raw)).get("issue")
    parent = issue.get("parent") if isinstance(issue, dict) else None
    return _exact_id(parent.get("id") if isinstance(parent, dict) else None)


def child_id_from_session_event(raw: object) -> str:
    """Return child issue id from a Linear session event. Missing/malformed is empty."""
    session = _session_dict(_event_dict(raw))
    issue = session.get("issue")
    nested = _exact_id(issue.get("id") if isinstance(issue, dict) else None)
    if nested:
        return nested
    return _exact_id(session.get("issueId"))


def lookup_parent_issue_id(graphql: Callable[..., object], child_id: str) -> str:
    """Read-only parent lookup. Errors and malformed ids are empty."""
    if not _exact_id(child_id):
        return ""
    try:
        result = graphql(
            "query($id: String!) { issue(id: $id) { parent { id } } }",
            {"id": child_id},
        )
    except Exception:
        return ""
    data = result.get("data") if isinstance(result, dict) else None
    issue = data.get("issue") if isinstance(data, dict) else None
    parent = issue.get("parent") if isinstance(issue, dict) else None
    return _exact_id(parent.get("id") if isinstance(parent, dict) else None)


def resolve_parent_issue_id(
    raw: object,
    lookup: Callable[[str], str] | None = None,
) -> str:
    """Use nested parent when present. Otherwise fail-closed lookup by child id."""
    nested = parent_id_from_session_event(raw)
    if nested:
        return nested
    child_id = child_id_from_session_event(raw)
    if not child_id or lookup is None:
        return ""
    try:
        found = lookup(child_id)
    except Exception:
        return ""
    return _exact_id(found)


@dataclass(frozen=True)
class EnqueueResult:
    action: str
    reason: str = ""


@dataclass(frozen=True)
class DrainResult:
    action: str
    reason: str = ""
    followup_key: str = ""
    hermes_session_id: str = ""
    trusted_summary: str = ""


@dataclass(frozen=True)
class FollowupRecord:
    followup_key: str
    state: str
    trusted_summary: str


class ParentFollowupQueue:
    def __init__(self, database: Path) -> None:
        database.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.database = Path(database)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS linear_parent_followups (
                    followup_key TEXT PRIMARY KEY,
                    parent_issue_id TEXT NOT NULL,
                    parent_linear_session_id TEXT NOT NULL,
                    parent_hermes_session_key TEXT NOT NULL,
                    child_issue_id TEXT NOT NULL,
                    transition TEXT NOT NULL,
                    trusted_summary TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('queued','dispatched','reconciled','rejected')),
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.database, timeout=5)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _trusted(summary: str) -> bool:
        return isinstance(summary, str) and 1 <= len(summary) <= 120 and _SUMMARY.fullmatch(summary) is not None

    def enqueue(
        self,
        owner: ParentOwner,
        *,
        child_issue_id: str,
        transition: str,
        trusted_summary: str,
        followup_key: str,
        stop_active: bool,
        parent_busy: bool,
    ) -> EnqueueResult:
        del parent_busy
        if not isinstance(followup_key, str) or not followup_key:
            return EnqueueResult(action="rejected", reason="followup_key_unavailable")
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    "SELECT followup_key FROM linear_parent_followups WHERE followup_key = ?",
                    (followup_key,),
                ).fetchone()
                if existing is not None:
                    conn.execute("COMMIT")
                    return EnqueueResult(action="duplicate", reason="already_recorded")
                if not self._trusted(trusted_summary):
                    conn.execute("ROLLBACK")
                    return EnqueueResult(action="rejected", reason="untrusted_summary")
                wake = decide_parent_wake(owner, child_issue_id=child_issue_id, stop_active=stop_active)
                if wake.action != "continue":
                    conn.execute("ROLLBACK")
                    return EnqueueResult(action="rejected", reason=wake.reason)
                conn.execute(
                    """
                    INSERT INTO linear_parent_followups (
                        followup_key, parent_issue_id, parent_linear_session_id,
                        parent_hermes_session_key, child_issue_id, transition,
                        trusted_summary, state, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
                    """,
                    (
                        followup_key,
                        owner.issue_id,
                        wake.linear_session_id,
                        wake.hermes_session_id,
                        child_issue_id,
                        transition,
                        trusted_summary,
                        now,
                        now,
                    ),
                )
                conn.execute("COMMIT")
            except sqlite3.IntegrityError:
                conn.execute("ROLLBACK")
                return EnqueueResult(action="duplicate", reason="already_recorded")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return EnqueueResult(action="queued", reason="child_transition")

    def deliver_queued(self, deliver: Callable[[str, str], bool]) -> DrainResult:
        """Deliver the oldest queued followup to the owning Hermes session.

        Does not mutate Linear. Failed delivery leaves the row queued.
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    """
                    SELECT followup_key, parent_hermes_session_key, trusted_summary
                    FROM linear_parent_followups
                    WHERE state = 'queued'
                    ORDER BY created_at ASC, followup_key ASC
                    LIMIT 1
                    """
                ).fetchone()
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        if row is None:
            return DrainResult(action="empty")
        followup_key = str(row["followup_key"])
        hermes_session_id = str(row["parent_hermes_session_key"])
        trusted_summary = str(row["trusted_summary"])
        if not hermes_session_id or not self._trusted(trusted_summary):
            return DrainResult(
                action="retry",
                reason="untrusted_delivery",
                followup_key=followup_key,
                hermes_session_id=hermes_session_id,
                trusted_summary=trusted_summary,
            )
        try:
            ok = bool(deliver(hermes_session_id, trusted_summary))
        except Exception:
            ok = False
        if not ok:
            return DrainResult(
                action="retry",
                reason="deliver_failed",
                followup_key=followup_key,
                hermes_session_id=hermes_session_id,
                trusted_summary=trusted_summary,
            )
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                updated = conn.execute(
                    """
                    UPDATE linear_parent_followups
                    SET state = 'dispatched', updated_at = ?
                    WHERE followup_key = ? AND state = 'queued'
                    """,
                    (now, followup_key),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        if updated.rowcount != 1:
            return DrainResult(
                action="retry",
                reason="lost_claim",
                followup_key=followup_key,
                hermes_session_id=hermes_session_id,
                trusted_summary=trusted_summary,
            )
        return DrainResult(
            action="dispatched",
            reason="child_transition",
            followup_key=followup_key,
            hermes_session_id=hermes_session_id,
            trusted_summary=trusted_summary,
        )

    def get(self, followup_key: str) -> FollowupRecord:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT followup_key, state, trusted_summary FROM linear_parent_followups WHERE followup_key = ?",
                (followup_key,),
            ).fetchone()
        if row is None:
            raise KeyError(followup_key)
        return FollowupRecord(followup_key=row["followup_key"], state=row["state"], trusted_summary=row["trusted_summary"])

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM linear_parent_followups").fetchone()
        return int(row["n"])
