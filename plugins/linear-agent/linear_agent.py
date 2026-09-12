"""Profile-local durable state machine for Linear Agent Sessions."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

try:
    from .linear_completion import accepted_completion_evidence
    from .linear_handoff import (
        EXTERNAL_EFFECTS,
        HandoffDenied,
        SharedIssueAuthority,
        issue_worktree,
    )
    from .linear_limits import LinearLimits
    from .linear_ownership import IssueOwnership
    from .linear_parent_continuation import ParentOwner
    from .linear_parent_followup import ParentFollowupQueue, parent_id_from_session_event
    from .linear_resume import EffectIdentifier, OperatorResumeGate, ResumeDenied, StopEvidence
    from .linear_stop import validate_control_cause
except ImportError:  # Direct script/test import.
    from linear_completion import accepted_completion_evidence
    from linear_handoff import (
        EXTERNAL_EFFECTS,
        HandoffDenied,
        SharedIssueAuthority,
        issue_worktree,
    )
    from linear_limits import LinearLimits
    from linear_ownership import IssueOwnership
    from linear_parent_continuation import ParentOwner
    from linear_parent_followup import ParentFollowupQueue, parent_id_from_session_event
    from linear_resume import EffectIdentifier, OperatorResumeGate, ResumeDenied, StopEvidence
    from linear_stop import validate_control_cause

_SUMMARY_LIMIT = 8_000
_SENSITIVE_KEY = (
    r"(?:access[_ -]?token|refresh[_ -]?token|api[_ -]?key|apikey|token|"
    r"client[_ -]?secret|clientsecret|authorization|password|passwd|secret|"
    r"cookie|session[_ -]?id|sessionid)"
)
_SECRET_ASSIGNMENT_RE = re.compile(
    rf"(?im)([\"']?{_SENSITIVE_KEY}[\"']?\s*[:=]\s*)"
    r"(?:\"[^\"\n]*\"|'[^'\n]*'|[^\s,}\n]+)"
)
_AUTH_HEADER_RE = re.compile(r"(?im)^(\s*authorization\s*:\s*).+$")
_COOKIE_HEADER_RE = re.compile(r"(?im)^(\s*(?:set-)?cookie\s*:\s*).+$")
_URL_USERINFO_RE = re.compile(r"(?i)(https?://)[^/\s:@]+:[^/\s@]+@")
_BEARER_RE = re.compile(r"(?i)\b(?:Bearer|Basic)\s+\S+")
_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN [^-\n]*PRIVATE KEY-----.*?-----END [^-\n]*PRIVATE KEY-----",
    re.DOTALL,
)
_TOKEN_PREFIX_RE = re.compile(r"\b(?:gh[pousr]_|lin_api_|sk-)[A-Za-z0-9_-]{8,}")
_SENSITIVE_LINE_RE = re.compile(_SENSITIVE_KEY, re.IGNORECASE)
_SENSITIVE_BLOCK_START_RE = re.compile(
    rf"^(\s*[\"']?{_SENSITIVE_KEY}[\"']?\s*[:=])\s*(.*)$",
    re.IGNORECASE,
)
_LINEAR_AUTHORIZATION_DENIAL = (
    "This agent is restricted to approved workspace users."
)
ACK_THOUGHT = "Inspecting the issue."
CLAIM_THOUGHT = "Reading source snapshots."
HEARTBEAT_THOUGHT = "Still inspecting."
_TERMINAL_ISSUE_STATUSES = {"done", "review"}
_UNAUTHORIZED_RESPONSE_LIMIT = 8_000


def unauthorized_response_body_from_entry(entry: object) -> str | None:
    if not isinstance(entry, dict) or "unauthorized_response_body" not in entry:
        return None
    if "allowed_linear_user_ids" not in entry:
        raise ValueError("unauthorized_response_body requires allowed_linear_user_ids")
    value = entry["unauthorized_response_body"]
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _UNAUTHORIZED_RESPONSE_LIMIT
    ):
        raise ValueError("unauthorized_response_body must be a non-empty exact string")
    return value


class _LinearAuthorizationRejection(ValueError):
    def __init__(
        self,
        *,
        linear_session_id: str,
        event_key: str,
        requester_user_id: str | None,
        reason: str,
    ) -> None:
        super().__init__(reason)
        self.linear_session_id = linear_session_id
        self.event_key = event_key
        self.requester_user_id = requester_user_id
        self.reason = reason


def _canonical_linear_user_id(value: object) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError("Linear user ID must be a canonical UUID")
    parsed = uuid.UUID(value)
    canonical = str(parsed)
    if canonical != value:
        raise ValueError("Linear user ID must be a canonical UUID")
    return canonical


def _redact_multiline_secret_values(text: str) -> str:
    redacted: list[str] = []
    secret_indent: int | None = None
    for line in text.splitlines():
        stripped = line.strip()
        indentation = len(line) - len(line.lstrip())
        if secret_indent is not None:
            if not stripped:
                redacted.append(line)
                continue
            if indentation > secret_indent or stripped.startswith("-"):
                redacted.append(" " * indentation + "[REDACTED]")
                continue
            secret_indent = None
        match = _SENSITIVE_BLOCK_START_RE.match(line)
        if match and match.group(2).strip() in {"", "|", ">", "|-", ">-"}:
            redacted.append(match.group(1) + " [REDACTED]")
            secret_indent = indentation
        else:
            redacted.append(line)
    return "\n".join(redacted)


def _summary_comment(response: str) -> str:
    redacted = _redact_multiline_secret_values(response)
    redacted = _PRIVATE_KEY_RE.sub("[REDACTED PRIVATE KEY]", redacted)
    redacted = _AUTH_HEADER_RE.sub(r"\1[REDACTED]", redacted)
    redacted = _COOKIE_HEADER_RE.sub(r"\1[REDACTED]", redacted)
    redacted = _URL_USERINFO_RE.sub(r"\1[REDACTED]@", redacted)
    redacted = _SECRET_ASSIGNMENT_RE.sub(r"\1[REDACTED]", redacted)
    redacted = _BEARER_RE.sub("Authorization [REDACTED]", redacted)
    redacted = _TOKEN_PREFIX_RE.sub("[REDACTED]", redacted)
    redacted = "\n".join(
        "[REDACTED SENSITIVE LINE]"
        if _SENSITIVE_LINE_RE.search(line) and "[REDACTED]" not in line
        else line
        for line in redacted.splitlines()
    )
    comment = f"### Agent session summary\n\n{redacted}"
    if len(comment) > _SUMMARY_LIMIT:
        return comment[: _SUMMARY_LIMIT - 1] + "…"
    return comment


def _project_summary(response: str) -> str:
    """Publish only the explicitly project-scoped section, never the full reply."""
    fallback = (
        "Work session finished. Detailed findings and remaining actions are recorded on the issue. "
        "Issue completion and project health are not inferred from session completion."
    )
    sections: list[list[str]] = []
    active: list[str] | None = None
    fence: str | None = None
    for line in response.splitlines():
        stripped = line.strip()
        if stripped.startswith(('```', '~~~')):
            marker = stripped[:3]
            if fence is None:
                fence = marker
            elif marker == fence:
                fence = None
            if active is not None:
                active.append(line)
            continue
        if fence is None and stripped == '### Project status update':
            active = []
            sections.append(active)
            continue
        if fence is None and re.match(r'^#{1,3} ', line):
            active = None
        if active is not None:
            active.append(line)
    summary = '\n'.join(sections[0]).strip() if len(sections) == 1 and fence is None else ''
    return _summary_comment(summary or fallback)


@dataclass(frozen=True)
class QueuedLinearJob:
    delivery_id: str
    linear_session_id: str
    hermes_session_key: str
    prompt: str
    issue_id: str | None = None
    requester_user_id: str | None = None
    handoff_owner_id: str | None = None
    execution_worktree: Path | None = None


@dataclass(frozen=True)
class StopRequest:
    delivery_id: str
    linear_session_id: str
    issue_id: str | None
    event_key: str
    requester_user_id: str | None
    cause: str = "stop"


class LinearWorker:
    """Consumes one profile's inbox and produces an idempotent response outbox."""

    def __init__(
        self,
        database: Path,
        *,
        profile: str,
        workspace: str,
        allowed_linear_user_ids: Iterable[str] | None = None,
        unauthorized_response_body: str | None = None,
        terminal_issue_status: str = "done",
        reassign_to_requester: bool = False,
        max_agent_queue: int = 100,
        max_session_queue: int = 10,
        max_issue_queue: int = 10,
        max_outbox_attempts: int = 3,
        max_concurrent_jobs: int = 5,
        shared_authority_database: Path | None = None,
        worktree_root: Path | None = None,
    ) -> None:
        if not profile or not workspace:
            raise ValueError("profile and workspace are required")
        database.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.database = database
        self.profile = profile
        self.workspace = workspace
        # Fail closed before worker recovery can mutate execution state.
        self.ownership = IssueOwnership(database, profile=profile, workspace=workspace)
        self.shared_authority = (
            SharedIssueAuthority(shared_authority_database)
            if shared_authority_database is not None
            else None
        )
        if worktree_root is not None:
            self.worktree_root = Path(worktree_root)
        elif shared_authority_database is not None:
            self.worktree_root = Path(shared_authority_database).parent / "issue-worktrees"
        else:
            self.worktree_root = None
        if allowed_linear_user_ids is None:
            self.allowed_linear_user_ids = None
        else:
            configured_user_ids = tuple(allowed_linear_user_ids)
            try:
                canonical_user_ids = tuple(
                    _canonical_linear_user_id(value)
                    for value in configured_user_ids
                )
            except (AttributeError, TypeError, ValueError) as exc:
                raise ValueError(
                    "allowed_linear_user_ids must contain canonical UUIDs"
                ) from exc
            if (
                not canonical_user_ids
                or len(set(canonical_user_ids)) != len(canonical_user_ids)
            ):
                raise ValueError(
                    "allowed_linear_user_ids must be non-empty and unique"
                )
            self.allowed_linear_user_ids = frozenset(canonical_user_ids)
        if unauthorized_response_body is None:
            self.authorization_denial = _LINEAR_AUTHORIZATION_DENIAL
        elif (
            not unauthorized_response_body
            or unauthorized_response_body.strip() != unauthorized_response_body
            or len(unauthorized_response_body) > _UNAUTHORIZED_RESPONSE_LIMIT
        ):
            raise ValueError("unauthorized_response_body must be a non-empty exact string")
        else:
            self.authorization_denial = unauthorized_response_body
        if terminal_issue_status not in _TERMINAL_ISSUE_STATUSES:
            raise ValueError("terminal_issue_status must be done or review")
        self.terminal_issue_status = terminal_issue_status
        self.reassign_to_requester = bool(reassign_to_requester)
        self.limits = LinearLimits(
            max_agent_queue=max_agent_queue,
            max_session_queue=max_session_queue,
            max_issue_queue=max_issue_queue,
            max_outbox_attempts=max_outbox_attempts,
            max_concurrent_jobs=max_concurrent_jobs,
        )
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS deliveries (
                    delivery_id TEXT PRIMARY KEY,
                    payload BLOB NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    created_at INTEGER NOT NULL,
                    issue_id TEXT,
                    linear_session_id TEXT,
                    requester_user_id TEXT,
                    rejection_reason TEXT,
                    control_priority INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    profile TEXT NOT NULL,
                    workspace TEXT NOT NULL,
                    linear_session_id TEXT NOT NULL,
                    hermes_session_key TEXT NOT NULL,
                    PRIMARY KEY (profile, workspace, linear_session_id)
                );
                CREATE TABLE IF NOT EXISTS event_receipts (
                    event_key TEXT PRIMARY KEY,
                    delivery_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS outbox (
                    delivery_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    linear_session_id TEXT NOT NULL,
                    body TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    sequence INTEGER NOT NULL DEFAULT 0,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    generation TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (delivery_id, kind)
                );
                CREATE TABLE IF NOT EXISTS dead_letters (
                    source TEXT NOT NULL CHECK (source IN ('delivery', 'outbox')),
                    delivery_id TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY (source, delivery_id, kind)
                );
                CREATE TABLE IF NOT EXISTS outbox_sequence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT
                );
                CREATE TABLE IF NOT EXISTS session_controls (
                    profile TEXT NOT NULL,
                    workspace TEXT NOT NULL,
                    linear_session_id TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state = 'ambiguous'),
                    stop_event_key TEXT NOT NULL UNIQUE,
                    requester_user_id TEXT,
                    issue_id TEXT,
                    cause TEXT NOT NULL DEFAULT 'stop' CHECK (cause IN ('stop', 'reassigned', 'permission_lost')),
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY (profile, workspace, linear_session_id)
                );
                CREATE TABLE IF NOT EXISTS stop_delivery_receipts (
                    profile TEXT NOT NULL,
                    workspace TEXT NOT NULL,
                    linear_session_id TEXT NOT NULL,
                    stop_event_key TEXT NOT NULL,
                    target_delivery_id TEXT NOT NULL,
                    execution_id TEXT,
                    status TEXT NOT NULL CHECK (status IN ('pending_delivery', 'accepted', 'not_delivered', 'not_running', 'stale')),
                    PRIMARY KEY (profile, workspace, linear_session_id, stop_event_key)
                );
                CREATE TABLE IF NOT EXISTS execution_lifecycle_receipts (
                    profile TEXT NOT NULL, workspace TEXT NOT NULL, linear_session_id TEXT NOT NULL,
                    target_delivery_id TEXT NOT NULL, execution_id TEXT NOT NULL, generation INTEGER,
                    state TEXT NOT NULL, occupancy TEXT NOT NULL CHECK (occupancy IN ('occupied', 'released')),
                    receipt_json TEXT NOT NULL, created_at INTEGER NOT NULL,
                    PRIMARY KEY (profile, workspace, linear_session_id, target_delivery_id, execution_id)
                );
                CREATE TABLE IF NOT EXISTS pending_terminal_closeout (
                    profile TEXT NOT NULL,
                    workspace TEXT NOT NULL,
                    delivery_id TEXT NOT NULL,
                    issue_id TEXT NOT NULL,
                    linear_session_id TEXT NOT NULL,
                    hermes_session_key TEXT NOT NULL,
                    execution_id TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY (profile, workspace, delivery_id)
                );
            """)
            delivery_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(deliveries)")
            }
            if "issue_id" not in delivery_columns:
                conn.execute("ALTER TABLE deliveries ADD COLUMN issue_id TEXT")
            if "linear_session_id" not in delivery_columns:
                conn.execute("ALTER TABLE deliveries ADD COLUMN linear_session_id TEXT")
            if "requester_user_id" not in delivery_columns:
                conn.execute(
                    "ALTER TABLE deliveries ADD COLUMN requester_user_id TEXT"
                )
            if "rejection_reason" not in delivery_columns:
                conn.execute(
                    "ALTER TABLE deliveries ADD COLUMN rejection_reason TEXT"
                )
            if "control_priority" not in delivery_columns:
                conn.execute(
                    "ALTER TABLE deliveries ADD COLUMN control_priority INTEGER NOT NULL DEFAULT 0"
                )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS one_running_job_per_issue "
                "ON deliveries(issue_id) "
                "WHERE state = 'running' AND issue_id IS NOT NULL"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS one_running_job_per_session "
                "ON deliveries(linear_session_id) "
                "WHERE state = 'running' AND linear_session_id IS NOT NULL"
            )
            outbox_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(outbox)")
            }
            if "sequence" not in outbox_columns:
                conn.execute(
                    "ALTER TABLE outbox ADD COLUMN sequence INTEGER NOT NULL DEFAULT 0"
                )
            if "attempts" not in outbox_columns:
                conn.execute(
                    "ALTER TABLE outbox ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
                )
            if "generation" not in outbox_columns:
                conn.execute(
                    "ALTER TABLE outbox ADD COLUMN generation TEXT NOT NULL DEFAULT ''"
                )
            control_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(session_controls)")
            }
            if "cause" not in control_columns:
                conn.execute(
                    "ALTER TABLE session_controls ADD COLUMN cause TEXT NOT NULL DEFAULT 'stop'"
                )
            dead_letter_columns = {
                str(row[1]): int(row[3])
                for row in conn.execute("PRAGMA table_info(dead_letters)")
            }
            if dead_letter_columns.get("kind") != 1:
                conn.execute("BEGIN IMMEDIATE")
                duplicates = conn.execute(
                    "SELECT 1 FROM dead_letters GROUP BY source, delivery_id, "
                    "COALESCE(kind, '') HAVING COUNT(*) > 1 LIMIT 1"
                ).fetchone()
                if duplicates is not None:
                    conn.execute("ROLLBACK")
                    raise RuntimeError(
                        "dead_letters contains duplicate nullable kind rows; "
                        "manual reconciliation is required before migration"
                    )
                conn.execute(
                    "CREATE TABLE dead_letters_rebuilt ("
                    "source TEXT NOT NULL CHECK (source IN ('delivery', 'outbox')), "
                    "delivery_id TEXT NOT NULL, kind TEXT NOT NULL DEFAULT '', "
                    "reason TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, "
                    "created_at INTEGER NOT NULL, PRIMARY KEY (source, delivery_id, kind))"
                )
                conn.execute(
                    "INSERT INTO dead_letters_rebuilt "
                    "(source, delivery_id, kind, reason, attempts, created_at) "
                    "SELECT source, delivery_id, COALESCE(kind, ''), reason, attempts, created_at "
                    "FROM dead_letters"
                )
                conn.execute("DROP TABLE dead_letters")
                conn.execute("ALTER TABLE dead_letters_rebuilt RENAME TO dead_letters")
                conn.execute("COMMIT")
            conn.execute("UPDATE outbox SET sequence = rowid WHERE sequence = 0")
            max_sequence = int(
                conn.execute("SELECT COALESCE(MAX(sequence), 0) FROM outbox").fetchone()[0]
            )
            allocator_empty = conn.execute(
                "SELECT 1 FROM outbox_sequence LIMIT 1"
            ).fetchone() is None
            if allocator_empty and max_sequence:
                conn.execute(
                    "INSERT INTO outbox_sequence (id) VALUES (?)",
                    (max_sequence,),
                )
            # Status assignments are idempotent and safe to retry after a crash.
            # Creation operations remain quarantined because repeating them can
            # duplicate user-visible activity or comments.
            conn.execute(
                "UPDATE outbox SET state = CASE "
                "WHEN (kind LIKE 'issue_status_%' OR kind = 'issue_handoff') "
                "AND attempts < ? THEN 'pending' "
                "WHEN kind LIKE 'issue_status_%' OR kind = 'issue_handoff' "
                "THEN 'dead_letter' ELSE 'ambiguous' END WHERE state = 'sending'",
                (self.limits.max_outbox_attempts,),
            )
            for exhausted_delivery, exhausted_kind, attempts in conn.execute(
                "SELECT delivery_id, kind, attempts FROM outbox WHERE state = 'dead_letter'"
            ):
                self._record_dead_letter(
                    conn, "outbox", str(exhausted_delivery), str(exhausted_kind),
                    "outbox_retry_exhausted", int(attempts),
                )
            # A running delivery may have completed agent-side work before the
            # process died. Do not execute it again. Surface the uncertainty to
            # Linear using the supported terminal response activity.
            running = conn.execute(
                "SELECT delivery_id, payload FROM deliveries WHERE state = 'running'"
            ).fetchall()
            for delivery_id, raw in running:
                try:
                    event = json.loads(bytes(raw))
                    if event.get("type") == "AgentSessionEvent":
                        session = event["agentSession"]
                    else:
                        session = event["data"]["agentSession"]
                    session_id = str(session["id"])
                    issue = session.get("issue") if isinstance(session, dict) else None
                    issue_id = (
                        str(issue["id"])
                        if isinstance(issue, dict) and issue.get("id")
                        else None
                    )
                    if session_id and conn.execute(
                        "SELECT 1 FROM session_controls WHERE profile = ? AND workspace = ? "
                        "AND linear_session_id = ?",
                        (self.profile, self.workspace, session_id),
                    ).fetchone() is not None:
                        continue
                    response = (
                        "The agent was interrupted before its status could be "
                        "confirmed. Please retry this request."
                    )
                    if session_id:
                        self._insert_outbox(
                            conn,
                            str(delivery_id),
                            "error",
                            session_id,
                            response,
                        )
                    if issue_id:
                        self._insert_outbox(
                            conn,
                            str(delivery_id),
                            "issue_comment",
                            issue_id,
                            _summary_comment(response),
                        )
                        self._insert_outbox(
                            conn,
                            str(delivery_id),
                            "issue_status_failure",
                            issue_id,
                            "failure",
                        )
                except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
                    pass
            conn.execute("UPDATE deliveries SET state = 'ambiguous' WHERE state = 'running'")

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.database, isolation_level=None, timeout=5.0)
        try:
            yield connection
        finally:
            connection.close()

    def _insert_outbox(
        self,
        conn: sqlite3.Connection,
        delivery_id: str,
        kind: str,
        target_id: str,
        body: str,
    ) -> None:
        sequence_row = conn.execute(
            "INSERT INTO outbox_sequence DEFAULT VALUES"
        )
        if sequence_row.lastrowid is None:
            raise RuntimeError("failed to allocate durable outbox sequence")
        sequence = int(sequence_row.lastrowid)
        generation = ""
        if kind in EXTERNAL_EFFECTS:
            generation = self._authorize_external_outbox(conn, delivery_id, kind, target_id, body)
        conn.execute(
            "INSERT OR IGNORE INTO outbox "
            "(delivery_id, kind, linear_session_id, body, sequence, generation) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (delivery_id, kind, target_id, body, sequence, generation),
        )

    def _authorize_external_outbox(
        self,
        conn: sqlite3.Connection,
        delivery_id: str,
        kind: str,
        target_id: str,
        body: str = "",
    ) -> str:
        """Fail closed before an external Linear mutation is queued."""
        if self.shared_authority is None:
            return ""
        issue_id, session_id = self._outbox_issue_identity(conn, delivery_id, kind, target_id)
        if not issue_id:
            raise HandoffDenied("external effect requires an issue")
        lease = self.shared_authority.get(workspace=self.workspace, issue_id=issue_id)
        if lease is None:
            raise HandoffDenied("stale generation cannot publish")
        owner_id = f"{self.profile}:{session_id}"
        self.shared_authority.authorize_effect(
            workspace=self.workspace,
            issue_id=issue_id,
            owner_id=owner_id,
            generation=lease.generation,
            kind=kind,
        )
        if kind == "deploy":
            self.shared_authority.acquire_resource(
                body,
                workspace=self.workspace,
                issue_id=issue_id,
                owner_id=owner_id,
                generation=lease.generation,
            )
        return lease.generation

    def _outbox_issue_identity(
        self,
        conn: sqlite3.Connection,
        delivery_id: str,
        kind: str,
        target_id: str,
    ) -> tuple[str, str]:
        row = conn.execute(
            "SELECT issue_id, linear_session_id FROM deliveries WHERE delivery_id = ?",
            (delivery_id,),
        ).fetchone()
        issue_id = str(row[0]) if row is not None and row[0] else ""
        session_id = str(row[1]) if row is not None and row[1] else target_id
        if not issue_id:
            if (
                kind in {"issue_comment", "issue_handoff", "deploy", "attachment", "issue_media", "project_update"}
                or kind.startswith("issue_status_")
            ):
                issue_id = target_id
            else:
                raise HandoffDenied("external effect requires an issue")
        return issue_id, session_id

    def _authorize_pending_outbox(self, conn: sqlite3.Connection, row: tuple[object, ...]) -> None:
        """Re-check the queued generation immediately before emit."""
        if self.shared_authority is None:
            return
        kind = str(row[1])
        issue_id, session_id = self._outbox_issue_identity(conn, str(row[0]), kind, str(row[2]))
        generation = str(row[5] or "")
        if not generation:
            raise HandoffDenied("stale generation cannot publish")
        self.shared_authority.authorize_effect(
            workspace=self.workspace,
            issue_id=issue_id,
            owner_id=f"{self.profile}:{session_id}",
            generation=generation,
            kind=kind,
        )

    @staticmethod
    def _record_dead_letter(
        conn: sqlite3.Connection,
        source: str,
        delivery_id: str,
        kind: str | None,
        reason: str,
        attempts: int = 0,
    ) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO dead_letters "
            "(source, delivery_id, kind, reason, attempts, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source, delivery_id, kind or "", reason, attempts, int(time.time())),
        )

    def _queue_limit_rejection(
        self, conn: sqlite3.Connection, job: QueuedLinearJob
    ) -> tuple[str, str] | None:
        active_states = "('queued', 'prepared', 'running', 'ambiguous', 'waiting_handoff')"
        agent_count = int(conn.execute(
            f"SELECT COUNT(*) FROM deliveries WHERE state IN {active_states} "
            "AND delivery_id != ?",
            (job.delivery_id,),
        ).fetchone()[0])
        if agent_count >= self.limits.max_agent_queue:
            return "agent_queue_limit", "The agent queue is at capacity; this request was not started."
        session_count = int(conn.execute(
            f"SELECT COUNT(*) FROM deliveries WHERE linear_session_id = ? "
            f"AND state IN {active_states} AND delivery_id != ?",
            (job.linear_session_id, job.delivery_id),
        ).fetchone()[0])
        if session_count >= self.limits.max_session_queue:
            return "session_queue_limit", "This Linear session queue is at capacity; this request was not started."
        if job.issue_id:
            issue_count = int(conn.execute(
                f"SELECT COUNT(*) FROM deliveries WHERE issue_id = ? "
                f"AND state IN {active_states} AND delivery_id != ?",
                (job.issue_id, job.delivery_id),
            ).fetchone()[0])
            if issue_count >= self.limits.max_issue_queue:
                return "issue_queue_limit", "This issue queue is at capacity; this request was not started."
        return None

    def _queue_terminal_issue(
        self,
        conn: sqlite3.Connection,
        job: QueuedLinearJob,
        *,
        has_follow_up: bool,
    ) -> None:
        if not job.issue_id:
            return
        if has_follow_up:
            self._insert_outbox(
                conn,
                job.delivery_id,
                "issue_status_terminal_waiting",
                job.issue_id,
                "waiting",
            )
            return
        if self.terminal_issue_status == "review":
            requester = job.requester_user_id
            if self.reassign_to_requester and requester:
                self._insert_outbox(
                    conn,
                    job.delivery_id,
                    "issue_handoff",
                    job.issue_id,
                    json.dumps(
                        {
                            "state": "review",
                            "assigneeId": requester,
                            "clearDelegate": True,
                        },
                        separators=(",", ":"),
                    ),
                )
                return
            self._insert_outbox(
                conn,
                job.delivery_id,
                "issue_status_review",
                job.issue_id,
                "review",
            )
            return
        if not self._accepted_done_receipt(conn, job):
            return
        self._insert_outbox(
            conn,
            job.delivery_id,
            "issue_status_done",
            job.issue_id,
            "done",
        )

    def _accepted_done_receipt(
        self,
        conn: sqlite3.Connection,
        job: QueuedLinearJob,
    ) -> bool:
        row = conn.execute(
            "SELECT execution_id, receipt_json FROM execution_lifecycle_receipts "
            "WHERE profile = ? AND workspace = ? AND linear_session_id = ? "
            "AND target_delivery_id = ? ORDER BY rowid DESC LIMIT 1",
            (self.profile, self.workspace, job.linear_session_id, job.delivery_id),
        ).fetchone()
        if row is None:
            return False
        try:
            receipt = json.loads(row[1])
        except (TypeError, json.JSONDecodeError):
            return False
        return accepted_completion_evidence(
            receipt,
            session_key=job.hermes_session_key,
            execution_id=str(row[0]),
        )

    def add_delivery(self, delivery_id: str, payload: bytes) -> None:
        if not delivery_id:
            raise ValueError("delivery id is required")
        control_priority = 0
        try:
            event = json.loads(payload)
            activity = event.get("agentActivity") if isinstance(event, dict) else None
            if (event.get("type") == "AgentSessionEvent" if isinstance(event, dict) else False) and (
                event.get("action") == "prompted" and isinstance(activity, dict)
                and activity.get("signal") in {"stop", "reassigned", "permission_lost"}
            ):
                control_priority = 1
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
            pass
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO deliveries "
                "(delivery_id, payload, payload_sha256, created_at, control_priority) "
                "VALUES (?, ?, ?, ?, ?)",
                (delivery_id, payload, hashlib.sha256(payload).hexdigest(), int(time.time()), control_priority),
            )

    def _resume_gate(self) -> OperatorResumeGate:
        if self.allowed_linear_user_ids is None:
            raise ResumeDenied("authenticated operator authorization is required")
        return OperatorResumeGate(
            self.database,
            profile=self.profile,
            workspace=self.workspace,
            allowed_operator_ids=self.allowed_linear_user_ids,
        )

    def _load_resume_evidence(self, issue_id: str) -> tuple[StopEvidence, tuple[EffectIdentifier, ...]]:
        with self._connect() as conn:
            control = conn.execute(
                "SELECT linear_session_id, stop_event_key FROM session_controls "
                "WHERE profile=? AND workspace=? AND issue_id=? AND state='ambiguous'",
                (self.profile, self.workspace, issue_id),
            ).fetchone()
            if control is None:
                raise ResumeDenied("verified execution-stop evidence is required")
            linear_session_id, stop_event_key = str(control[0]), str(control[1])
            receipt = conn.execute(
                "SELECT target_delivery_id, execution_id FROM stop_delivery_receipts "
                "WHERE profile=? AND workspace=? AND linear_session_id=? AND stop_event_key=? "
                "AND status='accepted'",
                (self.profile, self.workspace, linear_session_id, stop_event_key),
            ).fetchone()
            if receipt is None or not receipt[0] or not receipt[1]:
                raise ResumeDenied("verified execution-stop evidence is required")
            lifecycle = conn.execute(
                "SELECT 1 FROM execution_lifecycle_receipts WHERE profile=? AND workspace=? "
                "AND linear_session_id=? AND target_delivery_id=? AND execution_id=? "
                "AND occupancy='released'",
                (self.profile, self.workspace, linear_session_id, str(receipt[0]), str(receipt[1])),
            ).fetchone()
            if lifecycle is None:
                raise ResumeDenied("verified execution-stop evidence is required")
            effects = tuple(
                EffectIdentifier(str(row[0]), str(row[1]))
                for row in conn.execute(
                    "SELECT o.delivery_id, o.kind FROM outbox o "
                    "JOIN deliveries d ON d.delivery_id=o.delivery_id "
                    "WHERE d.issue_id=? ORDER BY o.delivery_id, o.kind",
                    (issue_id,),
                )
            )
        return StopEvidence(linear_session_id, stop_event_key, str(receipt[0]), str(receipt[1])), effects

    def resume_reconciled_for_job(self, job: QueuedLinearJob) -> object:
        """Rotate from the verified requester on this job; operator id is not a parameter."""
        if not isinstance(job, QueuedLinearJob) or not job.issue_id or not job.requester_user_id:
            raise ResumeDenied("authenticated operator authorization is required")
        if self.allowed_linear_user_ids is None or job.requester_user_id not in self.allowed_linear_user_ids:
            raise ResumeDenied("authenticated operator authorization is required")
        record = self.ownership.get(job.issue_id)
        if record is None or record.mode != "reconcile":
            raise ResumeDenied("reconciled ownership fence does not exactly match")
        authorization = self._resume_gate()._issue(job.requester_user_id)
        stop_evidence, effects = self._load_resume_evidence(job.issue_id)
        return self._resume_gate().rotate(
            issue_id=job.issue_id,
            owner_session_id=record.owner_session_id,
            generation=record.generation,
            authorization=authorization,
            stop_evidence=stop_evidence,
            effects=effects,
        )

    def resume_if_reconciled(self, job: QueuedLinearJob) -> bool:
        """Production runtime hook: rotate only when this issue is fenced in reconcile."""
        if not isinstance(job, QueuedLinearJob) or not job.issue_id:
            return False
        record = self.ownership.get(job.issue_id)
        if record is None or record.mode != "reconcile":
            return False
        self.resume_reconciled_for_job(job)
        return True

    def import_from_ingress_once(self, ingress_database: Path) -> bool:
        """Copy one committed delivery from this profile's ingress inbox."""
        with closing(sqlite3.connect(ingress_database, isolation_level=None, timeout=5.0)) as ingress:
            ingress.execute("BEGIN IMMEDIATE")
            row = ingress.execute(
                "SELECT logical_agent, delivery_id, payload FROM deliveries "
                "WHERE profile = ? AND status = 'pending' "
                "ORDER BY CASE WHEN json_valid(CAST(payload AS TEXT)) THEN "
                "CASE WHEN json_extract(CAST(payload AS TEXT), '$.type') = 'AgentSessionEvent' "
                "AND json_extract(CAST(payload AS TEXT), '$.action') = 'prompted' "
                "AND json_extract(CAST(payload AS TEXT), '$.agentActivity.signal') IN ('stop', 'reassigned', 'permission_lost') "
                "THEN 1 ELSE 0 END ELSE 0 END DESC, received_at, delivery_id LIMIT 1",
                (self.profile,),
            ).fetchone()
            if row is None:
                ingress.execute("COMMIT")
                return False
            try:
                logical_agent, delivery_id, payload = str(row[0]), str(row[1]), bytes(row[2])
                self.add_delivery(delivery_id, payload)
                ingress.execute(
                    "UPDATE deliveries SET status = 'imported' WHERE logical_agent = ? AND delivery_id = ?",
                    (logical_agent, delivery_id),
                )
                ingress.execute("COMMIT")
            except Exception:
                ingress.execute("ROLLBACK")
                raise
        return True

    def _requester_user_id(
        self,
        *,
        event: dict[str, object],
        session: dict[str, object],
        action: object,
        linear_session_id: str,
        event_key: str,
    ) -> str | None:
        if self.allowed_linear_user_ids is None:
            return None
        primary: object = None
        consistency_values: list[object] = []
        if action == "created":
            primary = session.get("creatorId")
            creator = session.get("creator")
            if creator is not None and not isinstance(creator, dict):
                consistency_values.append(creator)
            elif isinstance(creator, dict) and creator.get("id") is not None:
                consistency_values.append(creator.get("id"))
        elif action == "prompted":
            activity = event.get("agentActivity")
            if isinstance(activity, dict):
                primary = activity.get("userId")
                user = activity.get("user")
                if user is not None and not isinstance(user, dict):
                    consistency_values.append(user)
                elif isinstance(user, dict) and user.get("id") is not None:
                    consistency_values.append(user.get("id"))

        requester_user_id: str | None = None
        if primary is None:
            reason = "linear_user_identity_missing"
        else:
            try:
                requester_user_id = _canonical_linear_user_id(primary)
            except (AttributeError, TypeError, ValueError):
                reason = "linear_user_identity_invalid"
            else:
                try:
                    consistency_ids = [
                        _canonical_linear_user_id(value)
                        for value in consistency_values
                    ]
                except (AttributeError, TypeError, ValueError):
                    reason = "linear_user_identity_invalid"
                else:
                    if any(
                        value != requester_user_id
                        for value in consistency_ids
                    ):
                        reason = "linear_user_identity_conflict"
                    elif requester_user_id not in self.allowed_linear_user_ids:
                        reason = "linear_user_not_allowed"
                    else:
                        return requester_user_id
        raise _LinearAuthorizationRejection(
            linear_session_id=linear_session_id,
            event_key=event_key,
            requester_user_id=requester_user_id,
            reason=reason,
        )

    def _record_authorization_rejection(
        self,
        conn: sqlite3.Connection,
        delivery_id: str,
        rejection: _LinearAuthorizationRejection,
    ) -> None:
        receipt = conn.execute(
            "SELECT delivery_id FROM event_receipts WHERE event_key = ?",
            (rejection.event_key,),
        ).fetchone()
        receipt_delivery_id = str(receipt[0]) if receipt is not None else None
        conn.execute(
            "UPDATE outbox SET state = 'suppressed' "
            "WHERE delivery_id = ? AND state IN ('pending', 'sending', 'ambiguous') "
            "AND NOT (kind = 'response' AND body = ?)",
            (delivery_id, self.authorization_denial),
        )
        if receipt_delivery_id is None:
            conn.execute(
                "INSERT INTO event_receipts (event_key, delivery_id) VALUES (?, ?)",
                (rejection.event_key, delivery_id),
            )
        if receipt_delivery_id in (None, delivery_id):
            response = conn.execute(
                "SELECT state, body FROM outbox "
                "WHERE delivery_id = ? AND kind = 'response'",
                (delivery_id,),
            ).fetchone()
            if response is None:
                self._insert_outbox(
                    conn,
                    delivery_id,
                    "response",
                    rejection.linear_session_id,
                    self.authorization_denial,
                )
            elif str(response[1]) == self.authorization_denial:
                if str(response[0]) != "sent":
                    conn.execute(
                        "UPDATE outbox SET state = 'pending' "
                        "WHERE delivery_id = ? AND kind = 'response'",
                        (delivery_id,),
                    )
            elif str(response[0]) != "sent":
                sequence_row = conn.execute(
                    "INSERT INTO outbox_sequence DEFAULT VALUES"
                )
                if sequence_row.lastrowid is None:
                    raise RuntimeError("failed to allocate durable outbox sequence")
                conn.execute(
                    "UPDATE outbox SET linear_session_id = ?, body = ?, "
                    "state = 'pending', sequence = ? "
                    "WHERE delivery_id = ? AND kind = 'response'",
                    (
                        rejection.linear_session_id,
                        self.authorization_denial,
                        int(sequence_row.lastrowid),
                        delivery_id,
                    ),
                )
        conn.execute(
            "UPDATE deliveries SET linear_session_id = ?, state = 'rejected', "
            "requester_user_id = ?, rejection_reason = ? WHERE delivery_id = ?",
            (
                rejection.linear_session_id,
                rejection.requester_user_id,
                rejection.reason,
                delivery_id,
            ),
        )
        self._record_dead_letter(
            conn, "delivery", delivery_id, None, rejection.reason
        )

    def _record_issue_rejection(
        self, conn: sqlite3.Connection, job: QueuedLinearJob, reason: str, body: str
    ) -> None:
        """Reject after receipt reservation without creating a Hermes mapping."""
        try:
            self._insert_outbox(conn, job.delivery_id, "response", job.linear_session_id, body)
        except HandoffDenied:
            pass
        conn.execute(
            "UPDATE deliveries SET issue_id = ?, linear_session_id = ?, requester_user_id = ?, "
            "state = 'rejected', rejection_reason = ? WHERE delivery_id = ?",
            (job.issue_id, job.linear_session_id, job.requester_user_id, reason, job.delivery_id),
        )
        self._record_dead_letter(conn, "delivery", job.delivery_id, None, reason)

    def _record_waiting_handoff(self, conn: sqlite3.Connection, job: QueuedLinearJob) -> None:
        """Park a successor delivery until acknowledged transfer grants ownership."""
        conn.execute(
            "UPDATE deliveries SET issue_id = ?, linear_session_id = ?, requester_user_id = ?, "
            "state = 'waiting_handoff', rejection_reason = NULL WHERE delivery_id = ?",
            (job.issue_id, job.linear_session_id, job.requester_user_id, job.delivery_id),
        )

    def _canonical_issue_job(
        self, conn: sqlite3.Connection, job: QueuedLinearJob
    ) -> tuple[QueuedLinearJob, tuple[str, str] | None]:
        previous = conn.execute(
            "SELECT issue_id FROM deliveries WHERE linear_session_id = ? "
            "AND delivery_id != ? AND issue_id IS NOT NULL ORDER BY rowid LIMIT 1",
            (job.linear_session_id, job.delivery_id),
        ).fetchone()
        if previous is None:
            return job, None
        issue_id = str(previous[0])
        if job.issue_id is not None and job.issue_id != issue_id:
            return job, ("linear_session_issue_conflict", "This session's issue identity changed; reconciliation is required.")
        return replace(job, issue_id=issue_id), None

    def _control_row(self, conn: sqlite3.Connection, linear_session_id: str):
        return conn.execute(
            "SELECT state, issue_id FROM session_controls WHERE profile = ? AND workspace = ? "
            "AND linear_session_id = ?",
            (self.profile, self.workspace, linear_session_id),
        ).fetchone()

    def _delivery_fenced(self, conn: sqlite3.Connection, job: QueuedLinearJob) -> bool:
        row = conn.execute("SELECT state FROM deliveries WHERE delivery_id = ?", (job.delivery_id,)).fetchone()
        return row is None or str(row[0]) in {"canceled", "stop_requested", "rejected", "ambiguous", "completed"} or self._control_row(conn, job.linear_session_id) is not None

    def _suppress_stopped_output(self, conn: sqlite3.Connection, linear_session_id: str) -> None:
        conn.execute(
            "UPDATE outbox SET state = CASE WHEN state = 'sending' THEN 'ambiguous' WHEN state = 'pending' THEN 'suppressed' ELSE state END "
            "WHERE delivery_id IN (SELECT delivery_id FROM deliveries WHERE linear_session_id = ?) "
            "AND state IN ('pending', 'sending', 'ambiguous')",
            (linear_session_id,),
        )

    def _apply_stop(self, conn: sqlite3.Connection, request: StopRequest) -> None:
        """Persist a control tombstone and fence local, not external, work."""
        existing = self._control_row(conn, request.linear_session_id)
        if existing is not None:
            conn.execute("UPDATE deliveries SET state = 'completed' WHERE delivery_id = ?", (request.delivery_id,))
            return
        canonical = conn.execute(
            "SELECT issue_id FROM deliveries WHERE linear_session_id = ? AND issue_id IS NOT NULL "
            "ORDER BY rowid LIMIT 1", (request.linear_session_id,)
        ).fetchone()
        if canonical is not None and request.issue_id is not None and str(canonical[0]) != request.issue_id:
            conn.execute("UPDATE deliveries SET state = 'rejected' WHERE delivery_id = ?", (request.delivery_id,))
            return
        issue_id = str(canonical[0]) if canonical is not None else request.issue_id
        conn.execute(
            "INSERT INTO session_controls (profile, workspace, linear_session_id, state, stop_event_key, requester_user_id, issue_id, cause, created_at) "
            "VALUES (?, ?, ?, 'ambiguous', ?, ?, ?, ?, ?)",
            (self.profile, self.workspace, request.linear_session_id, request.event_key,
             request.requester_user_id, issue_id, validate_control_cause(request.cause), int(time.time())),
        )
        conn.execute(
            "UPDATE deliveries SET state = CASE WHEN state IN ('queued', 'prepared') THEN 'canceled' "
            "WHEN state = 'running' THEN 'ambiguous' ELSE state END "
            "WHERE linear_session_id = ? AND delivery_id != ?",
            (request.linear_session_id, request.delivery_id),
        )
        self._suppress_stopped_output(conn, request.linear_session_id)
        conn.execute("UPDATE deliveries SET linear_session_id = ?, issue_id = ?, requester_user_id = ?, state = 'completed' WHERE delivery_id = ?",
                     (request.linear_session_id, issue_id, request.requester_user_id, request.delivery_id))

    def stop_state(self, linear_session_id: str) -> bool:
        with self._connect() as conn:
            return self._control_row(conn, linear_session_id) is not None

    def control_cause(self, linear_session_id: str) -> str | None:
        """Return the durable cancellation reason without implying work stopped."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT cause FROM session_controls WHERE profile = ? AND workspace = ? "
                "AND linear_session_id = ?",
                (self.profile, self.workspace, linear_session_id),
            ).fetchone()
        return None if row is None else str(row[0])

    def revoke_session(self, linear_session_id: str, *, cause: str) -> bool:
        """Fence a session after reassignment or permission loss.

        This only cancels local queued work and quarantines a running execution.
        It deliberately has no occupancy-release path: a gateway executor, tool,
        child agent, or remote request remains physically ambiguous until its
        exact lifecycle receipt proves otherwise.
        """
        if not isinstance(linear_session_id, str) or not linear_session_id:
            raise ValueError("linear_session_id is required")
        validate_control_cause(cause)
        if cause == "stop":
            raise ValueError("stop controls require their Linear event receipt")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if self._control_row(conn, linear_session_id) is not None:
                conn.execute("COMMIT")
                return False
            canonical = conn.execute(
                "SELECT issue_id FROM deliveries WHERE linear_session_id = ? "
                "AND issue_id IS NOT NULL ORDER BY rowid LIMIT 1",
                (linear_session_id,),
            ).fetchone()
            issue_id = str(canonical[0]) if canonical is not None else None
            conn.execute(
                "INSERT INTO session_controls (profile, workspace, linear_session_id, state, stop_event_key, issue_id, cause, created_at) "
                "VALUES (?, ?, ?, 'ambiguous', ?, ?, ?, ?)",
                (self.profile, self.workspace, linear_session_id,
                 f"{cause}:{uuid.uuid4().hex}", issue_id, cause, int(time.time())),
            )
            conn.execute(
                "UPDATE deliveries SET state = CASE WHEN state IN ('queued', 'prepared') THEN 'canceled' "
                "WHEN state = 'running' THEN 'ambiguous' ELSE state END "
                "WHERE linear_session_id = ?",
                (linear_session_id,),
            )
            self._suppress_stopped_output(conn, linear_session_id)
            conn.execute("COMMIT")
            return True

    def stop_requested(self, job: QueuedLinearJob) -> bool:
        return self.stop_state(job.linear_session_id)

    def begin_stop_delivery(self, job: QueuedLinearJob, execution_id: str) -> bool:
        """Durably reserve one exact gateway interrupt before attempting it."""
        if not isinstance(execution_id, str) or not execution_id:
            raise ValueError("execution_id is required for Stop delivery")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            control = conn.execute(
                "SELECT stop_event_key FROM session_controls WHERE profile = ? AND workspace = ? AND linear_session_id = ?",
                (self.profile, self.workspace, job.linear_session_id),
            ).fetchone()
            if control is None:
                conn.execute("COMMIT")
                return False
            inserted = conn.execute(
                "INSERT OR IGNORE INTO stop_delivery_receipts (profile, workspace, linear_session_id, stop_event_key, target_delivery_id, execution_id, status) VALUES (?, ?, ?, ?, ?, ?, 'pending_delivery')",
                (self.profile, self.workspace, job.linear_session_id, str(control[0]), job.delivery_id, execution_id),
            ).rowcount
            conn.execute("COMMIT")
            return inserted == 1

    def record_stop_delivery(self, job: QueuedLinearJob, execution_id: str, status: str) -> bool:
        """Persist a validated core receipt; failure leaves pending ambiguous."""
        if status not in {"accepted", "not_delivered", "not_running", "stale"}:
            raise ValueError("invalid Stop delivery receipt")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            changed = conn.execute(
                "UPDATE stop_delivery_receipts SET status = ? WHERE profile = ? AND workspace = ? AND linear_session_id = ? AND target_delivery_id = ? AND execution_id = ? AND status = 'pending_delivery'",
                (status, self.profile, self.workspace, job.linear_session_id, job.delivery_id, execution_id),
            ).rowcount
            conn.execute("COMMIT")
            return changed == 1

    def stop_delivery(self, linear_session_id: str) -> dict[str, str] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT target_delivery_id, execution_id, status FROM stop_delivery_receipts WHERE profile = ? AND workspace = ? AND linear_session_id = ? ORDER BY rowid LIMIT 1",
                (self.profile, self.workspace, linear_session_id),
            ).fetchone()
        if row is None or row[1] is None:
            return None
        return {"delivery_id": str(row[0]), "execution_id": str(row[1]), "status": str(row[2])}

    def record_lifecycle_receipt(self, job: QueuedLinearJob, execution_id: str, receipt: dict[str, object], *, released: bool) -> None:
        """Persist exact lifecycle evidence; uncertainty retains occupancy."""
        generation = receipt.get("generation")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT OR REPLACE INTO execution_lifecycle_receipts (profile, workspace, linear_session_id, target_delivery_id, execution_id, generation, state, occupancy, receipt_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.profile, self.workspace, job.linear_session_id, job.delivery_id, execution_id, generation if type(generation) is int else None, str(receipt.get("state", "unknown")), "released" if released else "occupied", json.dumps(receipt, sort_keys=True, separators=(",", ":")), int(time.time())))
            conn.execute("COMMIT")

    def execution_occupancy(self, linear_session_id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT occupancy FROM execution_lifecycle_receipts WHERE profile = ? AND workspace = ? AND linear_session_id = ? ORDER BY rowid DESC LIMIT 1", (self.profile, self.workspace, linear_session_id)).fetchone()
        return None if row is None else str(row[0])


    def _issue_admission_block(
        self, conn: sqlite3.Connection, job: QueuedLinearJob
    ) -> tuple[str, str] | None:
        if not job.issue_id:
            if self.shared_authority is not None:
                return "missing_issue", "This Linear session is missing an issue."
            return None
        try:
            owned = conn.execute(
                "SELECT mode FROM issue_ownership WHERE profile = ? AND workspace = ? AND issue_id = ?",
                (self.profile, self.workspace, job.issue_id),
            ).fetchone()
        except sqlite3.Error as exc:
            raise RuntimeError("issue ownership registry is unavailable") from exc
        if owned is not None:
            if str(owned[0]) == "active":
                return "issue_owned_by_chat", "already in progress from Hermes chat, updates will land on the ticket"
            if str(owned[0]) == "reconcile":
                return "issue_requires_reconciliation", "This issue has a tracking record requiring reconciliation; no new execution was started."
        active = conn.execute(
            "SELECT 1 FROM deliveries WHERE issue_id = ? AND delivery_id != ? "
            "AND state IN ('queued', 'prepared', 'running', 'ambiguous', 'waiting_handoff') "
            "AND (linear_session_id IS NULL OR linear_session_id != ?) LIMIT 1",
            (job.issue_id, job.delivery_id, job.linear_session_id),
        ).fetchone()
        if active is not None:
            return "issue_active_in_linear_session", "This issue is already active in another Linear session."
        if self.shared_authority is not None:
            claimed = self.shared_authority.claim(
                workspace=self.workspace,
                issue_id=job.issue_id,
                owner_id=f"{self.profile}:{job.linear_session_id}",
            )
            if claimed.status == "conflict":
                try:
                    self.shared_authority.request_transfer(
                        workspace=self.workspace,
                        issue_id=job.issue_id,
                        to_owner=f"{self.profile}:{job.linear_session_id}",
                    )
                except HandoffDenied:
                    pass
                return (
                    "issue_owned_by_shared_authority",
                    "This issue is already owned by another agent.",
                )
        return None

    def _standard_job_from_payload(
        self,
        delivery_id: str,
        raw: bytes,
    ) -> tuple[QueuedLinearJob | StopRequest, str, bool]:
        """Parse one standard Linear Agent Session event without side effects."""
        try:
            event = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid Linear event") from exc
        if not isinstance(event, dict) or event.get("type") != "AgentSessionEvent":
            raise ValueError("unsupported Linear event")
        session = event.get("agentSession")
        if not isinstance(session, dict) or not session.get("id"):
            raise ValueError("AgentSessionEvent is missing session")
        linear_session_id = str(session["id"])
        issue = session.get("issue")
        issue_id = (
            str(issue["id"])
            if isinstance(issue, dict) and issue.get("id")
            else None
        )
        action = event.get("action")
        is_created = action == "created"
        if is_created:
            event_key = f"created:{linear_session_id}"
            prompt = str(event.get("promptContext") or "")
            if not prompt:
                comment = session.get("comment")
                if isinstance(comment, dict):
                    prompt = str(comment.get("body") or "")
        elif action == "prompted":
            activity = event.get("agentActivity")
            if not isinstance(activity, dict) or not activity.get("id"):
                raise ValueError("prompted AgentSessionEvent is missing activity")
            event_key = f"prompted:{activity['id']}"
            requester_user_id = self._requester_user_id(
                event=event, session=session, action=action,
                linear_session_id=linear_session_id, event_key=event_key,
            )
            signal = activity.get("signal")
            content = activity.get("content")
            if signal is not None:
                supplied_session_id = activity.get("agentSessionId")
                if supplied_session_id is not None and (
                    not isinstance(supplied_session_id, str)
                    or supplied_session_id != linear_session_id
                ):
                    raise ValueError("Agent Activity session does not match event session")
                if not isinstance(signal, str) or signal not in {"stop", "reassigned", "permission_lost"}:
                    raise ValueError("unsupported Agent Activity signal")
                if content is not None and (
                    not isinstance(content, dict) or content.get("type") != "prompt"
                ):
                    raise ValueError("malformed stop Agent Activity")
                return (
                    StopRequest(
                        delivery_id, linear_session_id, issue_id, event_key, requester_user_id, signal
                    ),
                    event_key, False,
                )
            prompt = str(activity.get("body") or "")
            if not prompt:
                if isinstance(content, dict):
                    prompt = str(content.get("body") or "")
                elif isinstance(content, str):
                    prompt = content
        else:
            raise ValueError("unsupported AgentSessionEvent action")
        if action != "prompted":
            requester_user_id = self._requester_user_id(
                event=event, session=session, action=action,
                linear_session_id=linear_session_id, event_key=event_key,
            )
        if not prompt:
            raise ValueError("AgentSessionEvent is missing prompt")
        return (
            QueuedLinearJob(
                delivery_id=delivery_id,
                linear_session_id=linear_session_id,
                hermes_session_key=f"linear:{self.workspace}:{linear_session_id}",
                prompt=prompt,
                issue_id=issue_id,
                requester_user_id=requester_user_id,
            ),
            event_key,
            is_created,
        )

    def admit_once(self) -> tuple[bool, QueuedLinearJob | None]:
        """Durably admit one event to the FIFO without starting agent work."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            activated = self._try_activate_waiting_handoff(conn)
            if activated is not None:
                conn.execute("COMMIT")
                return True, activated
            row = conn.execute(
                "SELECT delivery_id, payload FROM deliveries "
                "WHERE state = 'pending' ORDER BY control_priority DESC, rowid LIMIT 1"
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return False, None
            delivery_id, raw = str(row[0]), bytes(row[1])
            try:
                job, event_key, _is_created = self._standard_job_from_payload(
                    delivery_id,
                    raw,
                )
            except _LinearAuthorizationRejection as rejection:
                self._record_authorization_rejection(conn, delivery_id, rejection)
                conn.execute("COMMIT")
                return True, None
            except ValueError:
                conn.execute(
                    "UPDATE deliveries SET state = 'rejected' WHERE delivery_id = ?",
                    (delivery_id,),
                )
                conn.execute("COMMIT")
                return True, None

            existing = conn.execute(
                "SELECT delivery_id FROM event_receipts WHERE event_key = ?",
                (event_key,),
            ).fetchone()
            if existing is not None:
                conn.execute(
                    "UPDATE deliveries SET state = 'completed' WHERE delivery_id = ?",
                    (delivery_id,),
                )
                conn.execute("COMMIT")
                return True, None

            conn.execute(
                "INSERT INTO event_receipts (event_key, delivery_id) VALUES (?, ?)",
                (event_key, delivery_id),
            )
            if isinstance(job, StopRequest):
                self._apply_stop(conn, job)
                conn.execute("COMMIT")
                return True, None
            if self._control_row(conn, job.linear_session_id) is not None:
                conn.execute(
                    "UPDATE deliveries SET linear_session_id = ?, issue_id = ?, requester_user_id = ?, state = 'canceled' WHERE delivery_id = ?",
                    (job.linear_session_id, job.issue_id, job.requester_user_id, delivery_id),
                )
                self._suppress_stopped_output(conn, job.linear_session_id)
                conn.execute("COMMIT")
                return True, None
            conn.execute("UPDATE deliveries SET linear_session_id = ? WHERE delivery_id = ?", (job.linear_session_id, delivery_id))
            job, canonical_rejection = self._canonical_issue_job(conn, job)
            if canonical_rejection is not None:
                self._record_issue_rejection(conn, job, *canonical_rejection)
                conn.execute("COMMIT")
                return True, None
            conn.execute(
                "UPDATE deliveries SET issue_id = ?, requester_user_id = ? WHERE delivery_id = ?",
                (job.issue_id, job.requester_user_id, delivery_id),
            )
            issue_rejection = self._issue_admission_block(conn, job)
            if issue_rejection is not None:
                if issue_rejection[0] == "issue_owned_by_shared_authority":
                    self._record_waiting_handoff(conn, job)
                else:
                    self._record_issue_rejection(conn, job, *issue_rejection)
                conn.execute("COMMIT")
                return True, None
            queue_rejection = self._queue_limit_rejection(conn, job)
            if queue_rejection is not None:
                self._record_issue_rejection(conn, job, *queue_rejection)
                conn.execute("COMMIT")
                return True, None
            job = self._queue_admitted_job(conn, job)
            conn.execute("COMMIT")
            return True, job

    def _waiting_handoff_owned(self, job: QueuedLinearJob) -> bool:
        if self.shared_authority is None or not job.issue_id:
            return False
        owner_id = f"{self.profile}:{job.linear_session_id}"
        lease = self.shared_authority.get(workspace=self.workspace, issue_id=job.issue_id)
        if lease is None or lease.mode == "released":
            claimed = self.shared_authority.claim(
                workspace=self.workspace,
                issue_id=job.issue_id,
                owner_id=owner_id,
            )
            return claimed.status != "conflict"
        return lease.mode == "active" and lease.owner_id == owner_id

    def _try_activate_waiting_handoff(self, conn: sqlite3.Connection) -> QueuedLinearJob | None:
        row = conn.execute(
            "SELECT delivery_id, payload, issue_id FROM deliveries "
            "WHERE state = 'waiting_handoff' ORDER BY rowid LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        delivery_id, raw, stored_issue_id = str(row[0]), bytes(row[1]), row[2]
        try:
            job, _event_key, _is_created = self._standard_job_from_payload(delivery_id, raw)
        except (_LinearAuthorizationRejection, ValueError):
            conn.execute(
                "UPDATE deliveries SET state = 'rejected' WHERE delivery_id = ?",
                (delivery_id,),
            )
            return None
        if isinstance(job, StopRequest):
            return None
        if stored_issue_id is not None:
            job = replace(job, issue_id=str(stored_issue_id))
        if self._control_row(conn, job.linear_session_id) is not None:
            conn.execute(
                "UPDATE deliveries SET state = 'canceled' WHERE delivery_id = ?",
                (delivery_id,),
            )
            return None
        if not self._waiting_handoff_owned(job):
            return None
        queue_rejection = self._queue_limit_rejection(conn, job)
        if queue_rejection is not None:
            self._record_issue_rejection(conn, job, *queue_rejection)
            return None
        return self._queue_admitted_job(conn, job)

    def _queue_admitted_job(self, conn: sqlite3.Connection, job: QueuedLinearJob) -> QueuedLinearJob:
        delivery_id = job.delivery_id
        session = conn.execute(
            "SELECT hermes_session_key FROM sessions "
            "WHERE profile = ? AND workspace = ? AND linear_session_id = ?",
            (self.profile, self.workspace, job.linear_session_id),
        ).fetchone()
        if session is None:
            conn.execute(
                "INSERT INTO sessions "
                "(profile, workspace, linear_session_id, hermes_session_key) "
                "VALUES (?, ?, ?, ?)",
                (
                    self.profile,
                    self.workspace,
                    job.linear_session_id,
                    job.hermes_session_key,
                ),
            )
        else:
            job = QueuedLinearJob(
                delivery_id=job.delivery_id,
                linear_session_id=job.linear_session_id,
                hermes_session_key=str(session[0]),
                prompt=job.prompt,
                issue_id=job.issue_id,
                requester_user_id=job.requester_user_id,
                handoff_owner_id=job.handoff_owner_id,
                execution_worktree=job.execution_worktree,
            )
        conn.execute(
            "UPDATE deliveries SET issue_id = ?, requester_user_id = ? "
            "WHERE delivery_id = ?",
            (job.issue_id, job.requester_user_id, delivery_id),
        )
        self._insert_outbox(
            conn,
            delivery_id,
            "thought",
            job.linear_session_id,
            ACK_THOUGHT,
        )
        if job.issue_id:
            issue_is_active = conn.execute(
                "SELECT 1 FROM deliveries "
                "WHERE issue_id = ? AND state = 'running' LIMIT 1",
                (job.issue_id,),
            ).fetchone()
            if issue_is_active is None:
                self._insert_outbox(
                    conn,
                    delivery_id,
                    "issue_status_waiting",
                    job.issue_id,
                    "waiting",
                )
        conn.execute(
            "UPDATE deliveries SET state = 'queued' WHERE delivery_id = ?",
            (delivery_id,),
        )
        return job

    def claim_prepared(
        self, skip_delivery_ids: Iterable[str] = ()
    ) -> QueuedLinearJob | None:
        """Claim the oldest prepared job that is free to occupy a slot."""
        skipped = tuple(dict.fromkeys(str(item) for item in skip_delivery_ids if item))
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            running = conn.execute(
                "SELECT COUNT(*) FROM deliveries WHERE state = 'running'"
            ).fetchone()
            if running is not None and int(running[0]) >= self.limits.max_concurrent_jobs:
                conn.execute("COMMIT")
                return None
            skip_sql = ""
            params: tuple[object, ...] = ()
            if skipped:
                placeholders = ",".join("?" for _ in skipped)
                skip_sql = f"AND d.delivery_id NOT IN ({placeholders})"
                params = skipped
            row = conn.execute(
                "SELECT d.delivery_id, d.payload, d.issue_id FROM deliveries d "
                "WHERE d.state = 'prepared' "
                f"{skip_sql} "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM deliveries r "
                "  WHERE r.state IN ('running', 'ambiguous') AND ("
                "    (d.issue_id IS NOT NULL AND r.issue_id = d.issue_id) OR "
                "    (d.linear_session_id IS NOT NULL "
                "     AND r.linear_session_id = d.linear_session_id)"
                "  )"
                ") "
                "ORDER BY d.rowid LIMIT 1",
                params,
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            delivery_id, raw, stored_issue_id = str(row[0]), bytes(row[1]), row[2]
            try:
                job, _event_key, _is_created = self._standard_job_from_payload(
                    delivery_id,
                    raw,
                )
            except _LinearAuthorizationRejection as rejection:
                self._record_authorization_rejection(conn, delivery_id, rejection)
                conn.execute("COMMIT")
                return None
            if isinstance(job, StopRequest) or self._control_row(conn, job.linear_session_id) is not None:
                conn.execute("UPDATE deliveries SET state = 'canceled' WHERE delivery_id = ?", (delivery_id,))
                self._suppress_stopped_output(conn, job.linear_session_id)
                conn.execute("COMMIT")
                return None
            if stored_issue_id is not None:
                job = replace(job, issue_id=str(stored_issue_id))
            session = conn.execute(
                "SELECT hermes_session_key FROM sessions "
                "WHERE profile = ? AND workspace = ? AND linear_session_id = ?",
                (self.profile, self.workspace, job.linear_session_id),
            ).fetchone()
            if session is None:
                conn.execute("ROLLBACK")
                raise RuntimeError("queued Linear job is missing its Hermes session")
            job = QueuedLinearJob(
                delivery_id=job.delivery_id,
                linear_session_id=job.linear_session_id,
                hermes_session_key=str(session[0]),
                prompt=job.prompt,
                issue_id=job.issue_id,
                requester_user_id=job.requester_user_id,
            )
            try:
                conn.execute(
                    "UPDATE deliveries SET state = 'running' WHERE delivery_id = ?",
                    (delivery_id,),
                )
            except sqlite3.IntegrityError:
                conn.execute("ROLLBACK")
                return None
            if job.issue_id:
                self._insert_outbox(
                    conn,
                    delivery_id,
                    "issue_status_active",
                    job.issue_id,
                    "active",
                )
            conn.execute("COMMIT")
            return job

    def release_claim(self, job: QueuedLinearJob) -> None:
        """Return a claimed job to prepared when its active status is unconfirmed."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = "canceled" if self._control_row(conn, job.linear_session_id) is not None else "prepared"
            conn.execute("UPDATE deliveries SET state = ? WHERE delivery_id = ? AND state = 'running'", (state, job.delivery_id))
            conn.execute("COMMIT")

    def execution_ready(self, job: QueuedLinearJob) -> bool:
        if self.stop_requested(job):
            return False
        required = ["thought"]
        if job.issue_id:
            required.append("issue_status_active")
        placeholders = ",".join("?" for _ in required)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT kind, state FROM outbox WHERE delivery_id = ? "
                f"AND kind IN ({placeholders})",
                (job.delivery_id, *required),
            ).fetchall()
            states = {str(kind): str(state) for kind, state in rows}
            return all(states.get(kind) == "sent" for kind in required)

    def execution_suppressed(self, job: QueuedLinearJob) -> bool:
        if not job.issue_id:
            return False
        with self._connect() as conn:
            row = conn.execute(
                "SELECT state FROM outbox WHERE delivery_id = ? "
                "AND kind = 'issue_status_active'",
                (job.delivery_id,),
            ).fetchone()
        return row is not None and str(row[0]) == "suppressed"

    def quarantine_interrupted(self, job: QueuedLinearJob) -> None:
        """Fence a disappearing executor without claiming its tools stopped."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            changed = conn.execute(
                "UPDATE deliveries SET state = 'ambiguous' WHERE delivery_id = ? "
                "AND linear_session_id = ? AND state = 'running'",
                (job.delivery_id, job.linear_session_id),
            ).rowcount
            if changed:
                conn.execute(
                    "UPDATE outbox SET state = CASE WHEN state = 'sending' "
                    "THEN 'ambiguous' ELSE 'suppressed' END "
                    "WHERE delivery_id = ? AND state IN ('pending', 'sending')",
                    (job.delivery_id,),
                )
            conn.execute("COMMIT")

    def cancel_job(self, job: QueuedLinearJob) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE deliveries SET state = 'canceled' WHERE delivery_id = ? AND state IN ('queued', 'prepared', 'running', 'stop_requested')", (job.delivery_id,))
            conn.execute(
                "UPDATE outbox SET state = CASE WHEN state = 'sending' THEN 'ambiguous' WHEN state = 'pending' THEN 'suppressed' ELSE state END "
                "WHERE delivery_id = ? AND state IN ('pending', 'sending', 'ambiguous')",
                (job.delivery_id,),
            )
            conn.execute("COMMIT")

    def reauthorize_recoverable(self) -> bool:
        """Reject recoverable jobs that no longer satisfy policy."""
        if self.allowed_linear_user_ids is None:
            return False
        rejected = False
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT delivery_id, payload FROM deliveries "
                "WHERE state IN ('queued', 'prepared', 'ambiguous') ORDER BY rowid"
            ).fetchall()
            for delivery_id, raw in rows:
                try:
                    self._standard_job_from_payload(str(delivery_id), bytes(raw))
                except _LinearAuthorizationRejection as rejection:
                    self._record_authorization_rejection(
                        conn, str(delivery_id), rejection
                    )
                    rejected = True
            conn.execute("COMMIT")
        return rejected

    def next_unprepared(self) -> QueuedLinearJob | None:
        """Return the oldest admitted job whose Hermes session needs preparing."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT delivery_id, payload, issue_id FROM deliveries "
                "WHERE state = 'queued' ORDER BY rowid LIMIT 1"
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            delivery_id, raw, stored_issue_id = str(row[0]), bytes(row[1]), row[2]
            try:
                job, _event_key, _is_created = self._standard_job_from_payload(
                    delivery_id,
                    raw,
                )
            except _LinearAuthorizationRejection as rejection:
                self._record_authorization_rejection(conn, delivery_id, rejection)
                conn.execute("COMMIT")
                return None
            if isinstance(job, StopRequest) or self._control_row(conn, job.linear_session_id) is not None:
                conn.execute("UPDATE deliveries SET state = 'canceled' WHERE delivery_id = ?", (delivery_id,))
                self._suppress_stopped_output(conn, job.linear_session_id)
                conn.execute("COMMIT")
                return None
            if stored_issue_id is not None:
                job = replace(job, issue_id=str(stored_issue_id))
            session = conn.execute(
                "SELECT hermes_session_key FROM sessions "
                "WHERE profile = ? AND workspace = ? AND linear_session_id = ?",
                (self.profile, self.workspace, job.linear_session_id),
            ).fetchone()
            if session is None:
                conn.execute("ROLLBACK")
                raise RuntimeError("queued Linear job is missing its session mapping")
            conn.execute("COMMIT")
            return QueuedLinearJob(
                delivery_id=job.delivery_id,
                linear_session_id=job.linear_session_id,
                hermes_session_key=str(session[0]),
                prompt=job.prompt,
                issue_id=job.issue_id,
            )

    def mark_prepared(self, job: QueuedLinearJob) -> None:
        """Record that the real Hermes session exists before execution."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if self._control_row(conn, job.linear_session_id) is not None:
                conn.execute("UPDATE deliveries SET state = 'canceled' WHERE delivery_id = ?", (job.delivery_id,))
                conn.execute("COMMIT")
                return
            updated = conn.execute(
                "UPDATE deliveries SET state = 'prepared' "
                "WHERE delivery_id = ? AND state = 'queued'",
                (job.delivery_id,),
            ).rowcount
            if updated != 1:
                conn.execute("ROLLBACK")
                raise RuntimeError("Linear job is not awaiting session preparation")
            conn.execute("COMMIT")

    def complete_job(
        self,
        job: QueuedLinearJob,
        response: str,
        *,
        closeout_receipt: object | None = None,
        closeout_execution_id: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = conn.execute("SELECT state FROM deliveries WHERE delivery_id = ?", (job.delivery_id,)).fetchone()
            if (state is None or str(state[0]) != "running"
                    or self._control_row(conn, job.linear_session_id) is not None):
                conn.execute("COMMIT")
                return
            self._insert_outbox(
                conn,
                job.delivery_id,
                "response",
                job.linear_session_id,
                response,
            )
            if job.issue_id:
                self._insert_outbox(
                    conn,
                    job.delivery_id,
                    "issue_comment",
                    job.issue_id,
                    _summary_comment(response),
                )
                # The native publisher resolves this one Linear-origin issue
                # immediately before publishing.  Do not give it unrestricted
                # model output or infer a project (or project health) locally.
                self._insert_outbox(
                    conn,
                    job.delivery_id,
                    "project_update",
                    job.issue_id,
                    json.dumps(
                        {
                            "session_key": (
                                f"linear:{self.workspace}:{job.linear_session_id}:"
                                f"{job.delivery_id}:closeout"
                            ),
                            "summary": _project_summary(response),
                        },
                        separators=(",", ":"),
                    ),
                )
                has_follow_up = conn.execute(
                    "SELECT 1 FROM deliveries WHERE issue_id = ? "
                    "AND delivery_id != ? AND state IN ('queued', 'prepared', 'running') "
                    "LIMIT 1",
                    (job.issue_id, job.delivery_id),
                ).fetchone()
                self._queue_terminal_issue(
                    conn,
                    job,
                    has_follow_up=has_follow_up is not None,
                )
            conn.execute(
                "UPDATE deliveries SET state = 'completed' WHERE delivery_id = ?",
                (job.delivery_id,),
            )
            if closeout_receipt is not None and closeout_execution_id:
                self._store_terminal_closeout(
                    conn, job, closeout_receipt, closeout_execution_id
                )
            conn.execute("COMMIT")
        self._enqueue_parent_followup(job)

    def _enqueue_parent_followup(self, job: QueuedLinearJob) -> None:
        """Queue at most one owning-parent wakeup. Never mutates Linear."""
        if not job.issue_id:
            return
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM deliveries WHERE delivery_id = ?",
                (job.delivery_id,),
            ).fetchone()
            parent_id = parent_id_from_session_event(bytes(row[0]) if row is not None else b"")
            if not parent_id:
                return
            parent_session = conn.execute(
                "SELECT linear_session_id FROM deliveries "
                "WHERE issue_id = ? AND linear_session_id IS NOT NULL "
                "ORDER BY rowid DESC LIMIT 1",
                (parent_id,),
            ).fetchone()
            parent_linear = str(parent_session[0]) if parent_session and parent_session[0] else ""
            stop_active = bool(parent_linear) and self._control_row(conn, parent_linear) is not None
            parent_busy = conn.execute(
                "SELECT 1 FROM deliveries WHERE issue_id = ? "
                "AND state IN ('queued', 'prepared', 'running') LIMIT 1",
                (parent_id,),
            ).fetchone() is not None
        record = self.ownership.get(parent_id)
        if record is None or record.mode != "active":
            return
        ParentFollowupQueue(self.database).enqueue(
            ParentOwner(
                issue_id=parent_id,
                hermes_session_id=record.owner_session_id,
                linear_session_id=parent_linear or record.owner_session_id,
                generation=record.generation,
                mode=record.mode,
            ),
            child_issue_id=job.issue_id,
            transition="completed",
            trusted_summary=f"{job.issue_id} completed",
            followup_key=f"{parent_id}:{job.issue_id}:{job.delivery_id}:completed",
            stop_active=stop_active,
            parent_busy=parent_busy,
        )

    def _receipt_json(self, receipt: object) -> str:
        if isinstance(receipt, str):
            return receipt
        return json.dumps(receipt, separators=(",", ":"), sort_keys=True)

    def _store_terminal_closeout(
        self,
        conn: sqlite3.Connection,
        job: QueuedLinearJob,
        receipt: object,
        execution_id: str,
    ) -> None:
        if self.shared_authority is None or not job.issue_id or not execution_id:
            return
        conn.execute(
            "INSERT OR REPLACE INTO pending_terminal_closeout "
            "(profile, workspace, delivery_id, issue_id, linear_session_id, "
            "hermes_session_key, execution_id, receipt_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self.profile,
                self.workspace,
                job.delivery_id,
                job.issue_id,
                job.linear_session_id,
                job.hermes_session_key,
                execution_id,
                self._receipt_json(receipt),
                int(time.time()),
            ),
        )

    def queue_terminal_closeout(
        self,
        job: QueuedLinearJob,
        receipt: object,
        execution_id: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._store_terminal_closeout(conn, job, receipt, execution_id)
            conn.execute("COMMIT")

    def pending_terminal_closeout(self) -> tuple[QueuedLinearJob, object, str] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT delivery_id, issue_id, linear_session_id, hermes_session_key, "
                "execution_id, receipt_json FROM pending_terminal_closeout "
                "WHERE profile = ? AND workspace = ? ORDER BY created_at ASC LIMIT 1",
                (self.profile, self.workspace),
            ).fetchone()
        if row is None:
            return None
        receipt: object
        try:
            receipt = json.loads(str(row[5]))
        except json.JSONDecodeError:
            receipt = str(row[5])
        job = QueuedLinearJob(
            delivery_id=str(row[0]),
            linear_session_id=str(row[2]),
            hermes_session_key=str(row[3]),
            prompt="",
            issue_id=str(row[1]),
        )
        return job, receipt, str(row[4])

    def terminal_outbox_settled(self, delivery_id: str) -> bool:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT kind, state FROM outbox WHERE delivery_id = ?",
                (delivery_id,),
            ).fetchall()
        for kind, state in rows:
            if str(kind) in EXTERNAL_EFFECTS and str(state) != "sent":
                return False
        return True

    def clear_terminal_closeout(self, delivery_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM pending_terminal_closeout "
                "WHERE profile = ? AND workspace = ? AND delivery_id = ?",
                (self.profile, self.workspace, delivery_id),
            )

    def fail_job(self, job: QueuedLinearJob) -> None:
        response = "Unable to complete this request. Please retry."
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = conn.execute("SELECT state FROM deliveries WHERE delivery_id = ?", (job.delivery_id,)).fetchone()
            if (state is None or str(state[0]) not in {"queued", "prepared", "running"}
                    or self._control_row(conn, job.linear_session_id) is not None):
                conn.execute("COMMIT")
                return
            self._insert_outbox(
                conn,
                job.delivery_id,
                "error",
                job.linear_session_id,
                response,
            )
            if job.issue_id:
                self._insert_outbox(
                    conn,
                    job.delivery_id,
                    "issue_comment",
                    job.issue_id,
                    _summary_comment(response),
                )
                self._insert_outbox(
                    conn,
                    job.delivery_id,
                    "issue_status_failure",
                    job.issue_id,
                    "failure",
                )
            conn.execute(
                "UPDATE deliveries SET state = 'completed' WHERE delivery_id = ?",
                (job.delivery_id,),
            )
            conn.execute("COMMIT")

    def bind_issue_worktree(self, job: QueuedLinearJob) -> Path | None:
        """Bind a per-issue checkout before execution. Isolated stores do not fence."""
        if self.shared_authority is None or not job.issue_id:
            return None
        if self.worktree_root is None:
            raise HandoffDenied("worktree root is required")
        owner_id = f"{self.profile}:{job.linear_session_id}"
        lease = self.shared_authority.get(workspace=self.workspace, issue_id=job.issue_id)
        if lease is None or lease.mode != "active" or lease.owner_id != owner_id:
            raise HandoffDenied("stale generation cannot publish")
        path = issue_worktree(
            self.worktree_root,
            workspace=self.workspace,
            issue_id=job.issue_id,
        )
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        return path

    def authorize_live_activity(self, job: QueuedLinearJob, kind: str) -> None:
        """Fail closed before a live Linear activity call that bypasses the outbox."""
        if self.shared_authority is None:
            return
        if not job.issue_id:
            raise HandoffDenied("external effect requires an issue")
        owner_id = f"{self.profile}:{job.linear_session_id}"
        lease = self.shared_authority.get(workspace=self.workspace, issue_id=job.issue_id)
        if lease is None or lease.mode != "active" or lease.owner_id != owner_id:
            raise HandoffDenied("stale generation cannot publish")
        self.shared_authority.authorize_effect(
            workspace=self.workspace,
            issue_id=job.issue_id,
            owner_id=owner_id,
            generation=lease.generation,
            kind=kind,
        )

    def begin_live_activity(
        self, job: QueuedLinearJob, kind: str
    ) -> tuple[str, str, str, str, str] | None:
        """Hold an external-effect ticket across a live Linear mutation."""
        self.authorize_live_activity(job, kind)
        if self.shared_authority is None:
            return None
        if not job.issue_id:
            raise HandoffDenied("external effect requires an issue")
        owner_id = f"{self.profile}:{job.linear_session_id}"
        lease = self.shared_authority.get(workspace=self.workspace, issue_id=job.issue_id)
        if lease is None or lease.mode != "active" or lease.owner_id != owner_id:
            raise HandoffDenied("stale generation cannot publish")
        try:
            ticket = self.shared_authority.begin_external_effect(
                workspace=self.workspace,
                issue_id=job.issue_id,
                owner_id=owner_id,
                generation=lease.generation,
                kind=kind,
            )
        except sqlite3.Error as exc:
            raise HandoffDenied("shared authority is unavailable") from exc
        return (self.workspace, job.issue_id, owner_id, lease.generation, ticket)

    def finish_live_activity(
        self, ticket_ctx: tuple[str, str, str, str, str] | None
    ) -> None:
        if ticket_ctx is None or self.shared_authority is None:
            return
        workspace, issue_id, owner_id, generation, ticket = ticket_ctx
        try:
            self.shared_authority.finish_external_effect(
                workspace=workspace,
                issue_id=issue_id,
                owner_id=owner_id,
                generation=generation,
                ticket=ticket,
            )
        except HandoffDenied:
            return

    def emit_closeout_updates(
        self,
        publish: Callable[[str, Sequence[Mapping[str, object]]], object],
        key: str,
        issues: Sequence[Mapping[str, object]],
    ) -> object:
        """Fence chat closeout project updates against the current shared lease."""
        if self.shared_authority is None:
            return publish(key, issues)
        tickets: list[tuple[str, str, str, str]] = []
        try:
            for source in issues:
                if not isinstance(source, Mapping):
                    raise HandoffDenied("invalid issue summary source")
                issue_id = str(source.get("issue_id") or "")
                if not issue_id:
                    raise HandoffDenied("external effect requires an issue")
                lease = self.shared_authority.get(workspace=self.workspace, issue_id=issue_id)
                if lease is None or lease.mode != "active":
                    raise HandoffDenied("stale generation cannot publish")
                owner_profile = lease.owner_id.split(":", 1)[0]
                if owner_profile != self.profile:
                    raise HandoffDenied("stale generation cannot publish")
                ticket = self.shared_authority.begin_external_effect(
                    workspace=self.workspace,
                    issue_id=issue_id,
                    owner_id=lease.owner_id,
                    generation=lease.generation,
                    kind="project_update",
                )
                tickets.append((issue_id, lease.owner_id, lease.generation, ticket))
            return publish(key, issues)
        finally:
            for issue_id, owner_id, generation, ticket in reversed(tickets):
                try:
                    self.shared_authority.finish_external_effect(
                        workspace=self.workspace,
                        issue_id=issue_id,
                        owner_id=owner_id,
                        generation=generation,
                        ticket=ticket,
                    )
                except HandoffDenied:
                    pass

    def release_shared_after_stop(
        self,
        job: QueuedLinearJob,
        stop_receipt: object,
        *,
        session_key: str,
        execution_id: str,
        to_owner: str | None = None,
    ) -> None:
        """Release or transfer shared ownership after an acknowledged stop."""
        if self.shared_authority is None or not job.issue_id:
            return
        owner_id = f"{self.profile}:{job.linear_session_id}"
        lease = self.shared_authority.get(workspace=self.workspace, issue_id=job.issue_id)
        if lease is None or lease.mode != "active" or lease.owner_id != owner_id:
            return
        successor = to_owner or job.handoff_owner_id
        if not successor:
            successor = self.shared_authority.pending_owner(
                workspace=self.workspace, issue_id=job.issue_id
            )
        if successor:
            self.shared_authority.transfer(
                workspace=self.workspace,
                issue_id=job.issue_id,
                from_owner=owner_id,
                to_owner=successor,
                generation=lease.generation,
                stop_receipt=stop_receipt,
                session_key=session_key,
                execution_id=execution_id,
            )
            return
        self.shared_authority.release(
            workspace=self.workspace,
            issue_id=job.issue_id,
            owner_id=owner_id,
            generation=lease.generation,
            stop_receipt=stop_receipt,
            session_key=session_key,
            execution_id=execution_id,
        )

    def _release_terminal_deploy_lock(
        self,
        kind: str,
        resource_id: str,
        ticket_ctx: tuple[str, str, str, str, str] | None,
    ) -> None:
        if kind != "deploy" or ticket_ctx is None or self.shared_authority is None:
            return
        try:
            self.shared_authority.release_resource(
                resource_id,
                owner_id=ticket_ctx[2],
                generation=ticket_ctx[3],
            )
        except HandoffDenied:
            return

    def dispatch_outbox(self, emit: Callable[[str, str, str], object]) -> bool:
        """Emit one activity once; quarantine ambiguity instead of reposting."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE outbox SET state = 'suppressed' WHERE state = 'pending' AND EXISTS ("
                "SELECT 1 FROM session_controls c WHERE c.profile = ? AND c.workspace = ? "
                "AND c.linear_session_id = (SELECT d.linear_session_id FROM deliveries d "
                "WHERE d.delivery_id = outbox.delivery_id))",
                (self.profile, self.workspace),
            )
            row = conn.execute(
                "SELECT delivery_id, kind, linear_session_id, body, attempts, generation FROM outbox "
                "WHERE state = 'pending' ORDER BY sequence, rowid LIMIT 1"
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return False
            if str(row[1]) in EXTERNAL_EFFECTS:
                try:
                    self._authorize_pending_outbox(conn, row)
                except HandoffDenied:
                    conn.execute(
                        "UPDATE outbox SET state = 'dead_letter' "
                        "WHERE delivery_id = ? AND kind = ? AND state = 'pending'",
                        (row[0], row[1]),
                    )
                    self._record_dead_letter(
                        conn, "outbox", str(row[0]), str(row[1]),
                        "stale_generation", int(row[4]),
                    )
                    conn.execute("COMMIT")
                    return True
            conn.execute(
                "UPDATE outbox SET state = 'sending', attempts = attempts + 1 "
                "WHERE delivery_id = ? AND kind = ?",
                (row[0], row[1]),
            )
            conn.execute("COMMIT")
        ticket_ctx: tuple[str, str, str, str, str] | None = None
        kind = str(row[1])
        if kind in EXTERNAL_EFFECTS and self.shared_authority is not None:
            try:
                with self._connect() as lookup:
                    issue_id, session_id = self._outbox_issue_identity(
                        lookup, str(row[0]), kind, str(row[2])
                    )
                owner_id = f"{self.profile}:{session_id}"
                generation = str(row[5] or "")
                if not issue_id:
                    raise HandoffDenied("external effect requires an issue")
                ticket = self.shared_authority.begin_external_effect(
                    workspace=self.workspace,
                    issue_id=issue_id,
                    owner_id=owner_id,
                    generation=generation,
                    kind=kind,
                )
                ticket_ctx = (self.workspace, issue_id, owner_id, generation, ticket)
            except HandoffDenied:
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        "UPDATE outbox SET state = 'dead_letter' "
                        "WHERE delivery_id = ? AND kind = ? AND state = 'sending'",
                        (row[0], row[1]),
                    )
                    self._record_dead_letter(
                        conn, "outbox", str(row[0]), str(row[1]),
                        "stale_generation", int(row[4]) + 1,
                    )
                    conn.execute("COMMIT")
                return True
        try:
            operation = "issue_status" if kind.startswith("issue_status_") else kind
            try:
                outcome = emit(str(row[2]), operation, str(row[3]))
            except Exception:
                retryable = (
                    kind.startswith("issue_status_") or kind == "issue_handoff"
                )
                attempts = int(row[4]) + 1
                retry_state = (
                    "pending" if retryable and attempts < self.limits.max_outbox_attempts
                    else "dead_letter" if retryable else "ambiguous"
                )
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    transitioned = conn.execute(
                        "UPDATE outbox SET state = ? "
                        "WHERE delivery_id = ? AND kind = ? AND state = 'sending' "
                        "AND NOT EXISTS (SELECT 1 FROM session_controls c WHERE c.profile = ? "
                        "AND c.workspace = ? AND c.linear_session_id = (SELECT d.linear_session_id "
                        "FROM deliveries d WHERE d.delivery_id = outbox.delivery_id))",
                        (retry_state, row[0], row[1], self.profile, self.workspace),
                    ).rowcount
                    if retry_state == "dead_letter" and transitioned == 1:
                        self._record_dead_letter(
                            conn, "outbox", str(row[0]), str(row[1]),
                            "outbox_retry_exhausted", attempts,
                        )
                    conn.execute("COMMIT")
                if retry_state == "dead_letter":
                    self._release_terminal_deploy_lock(kind, str(row[3]), ticket_ctx)
                raise
            final_state = "suppressed" if outcome is False else "sent"
            with self._connect() as conn:
                conn.execute(
                    "UPDATE outbox SET state = ? WHERE delivery_id = ? AND kind = ? AND state = 'sending' "
                    "AND NOT EXISTS (SELECT 1 FROM session_controls c WHERE c.profile = ? "
                    "AND c.workspace = ? AND c.linear_session_id = (SELECT d.linear_session_id "
                    "FROM deliveries d WHERE d.delivery_id = outbox.delivery_id))",
                    (final_state, row[0], row[1], self.profile, self.workspace),
                )
            self._release_terminal_deploy_lock(kind, str(row[3]), ticket_ctx)
            return True
        finally:
            if ticket_ctx is not None and self.shared_authority is not None:
                try:
                    self.shared_authority.finish_external_effect(
                        workspace=ticket_ctx[0],
                        issue_id=ticket_ctx[1],
                        owner_id=ticket_ctx[2],
                        generation=ticket_ctx[3],
                        ticket=ticket_ctx[4],
                    )
                except HandoffDenied:
                    pass

    def outbox(self) -> list[tuple[str, str, str]]:
        with self._connect() as conn:
            return [(str(kind), str(session), str(body)) for kind, session, body in conn.execute(
                "SELECT kind, linear_session_id, body FROM outbox ORDER BY delivery_id, kind"
            )]

    def outbox_state(self) -> list[str]:
        with self._connect() as conn:
            return [str(row[0]) for row in conn.execute("SELECT state FROM outbox ORDER BY delivery_id, kind")]

    def outbox_attempts(self, delivery_id: str, kind: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT attempts FROM outbox WHERE delivery_id = ? AND kind = ?",
                (delivery_id, kind),
            ).fetchone()
        return int(row[0]) if row is not None else None

    def dead_letters(self) -> list[tuple[str, str, str | None, str, int]]:
        with self._connect() as conn:
            return [
                (
                    str(source), str(delivery_id),
                    str(kind) if kind not in (None, "") else None,
                    str(reason), int(attempts),
                )
                for source, delivery_id, kind, reason, attempts in conn.execute(
                    "SELECT source, delivery_id, kind, reason, attempts FROM dead_letters "
                    "ORDER BY source, delivery_id, kind"
                )
            ]

    def delivery_rejection(
        self,
        delivery_id: str,
    ) -> tuple[str | None, str | None] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT requester_user_id, rejection_reason FROM deliveries "
                "WHERE delivery_id = ?",
                (delivery_id,),
            ).fetchone()
            if row is None:
                return None
            return (
                str(row[0]) if row[0] is not None else None,
                str(row[1]) if row[1] is not None else None,
            )

    def delivery_state(self, delivery_id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT state FROM deliveries WHERE delivery_id = ?", (delivery_id,)
            ).fetchone()
            return str(row[0]) if row is not None else None
