"""Durable, fail-closed parent follow-up queue.

Wakeups are addressed from ParentOwner, never from event-supplied session IDs.
Trusted summaries may contain identifiers and state tokens only.
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

try:
    from .linear_parent_continuation import ParentOwner, decide_parent_wake
except ImportError:  # direct module invocation in the plugin directory
    from linear_parent_continuation import ParentOwner, decide_parent_wake

_SUMMARY = re.compile(r"^[A-Za-z0-9._:-]+ (completed|started|blocked|stale|missing-evidence)$")


def parent_id_from_session_event(raw: object) -> str:
    """Return parent issue id from a Linear session event. Missing/malformed is empty."""
    event: object
    if isinstance(raw, (bytes, bytearray)):
        try:
            event = json.loads(bytes(raw))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return ""
    elif isinstance(raw, str):
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return ""
    else:
        event = raw
    if not isinstance(event, dict):
        return ""
    session = event.get("agentSession")
    if not isinstance(session, dict):
        data = event.get("data")
        session = data.get("agentSession") if isinstance(data, dict) else None
    issue = session.get("issue") if isinstance(session, dict) else None
    parent = issue.get("parent") if isinstance(issue, dict) else None
    parent_id = parent.get("id") if isinstance(parent, dict) else None
    if isinstance(parent_id, str) and parent_id and parent_id.strip() == parent_id:
        return parent_id
    return ""


@dataclass(frozen=True)
class EnqueueResult:
    action: str
    reason: str = ""


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
