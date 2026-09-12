"""Profile-local durable ownership for Linear issues.

The registry deliberately shares the worker SQLite database but only creates its
own ``issue_ownership_*`` tables.  It never constructs a LinearWorker.
"""
from __future__ import annotations

import secrets
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class OwnershipRecord:
    issue_id: str
    owner_session_id: str
    generation: str
    mode: str
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class ClaimResult:
    status: str  # claimed, conflict, tombstoned, worker_active
    record: OwnershipRecord | None


class IssueOwnership:
    """Atomic, profile-local issue admission ownership registry."""

    def __init__(self, database: Path, *, profile: str, workspace: str) -> None:
        if not profile or not workspace:
            raise ValueError("profile and workspace are required")
        database.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.database = database
        self.profile = profile
        self.workspace = workspace
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        """Initialize once with a bounded retry for concurrent first use."""
        deadline = time.monotonic() + 5.0
        while True:
            try:
                with self._connect() as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA synchronous=FULL")
                    conn.executescript("""
                CREATE TABLE IF NOT EXISTS issue_ownership_meta (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS issue_ownership (
                    profile TEXT NOT NULL,
                    workspace TEXT NOT NULL,
                    issue_id TEXT NOT NULL,
                    owner_session_id TEXT NOT NULL,
                    generation TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK (mode IN ('active', 'released', 'reconcile')),
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (profile, workspace, issue_id)
                );
            """)
                    conn.execute(
                        "INSERT OR IGNORE INTO issue_ownership_meta (singleton, schema_version) VALUES (1, ?)",
                        (SCHEMA_VERSION,),
                    )
                    row = conn.execute(
                        "SELECT schema_version FROM issue_ownership_meta WHERE singleton = 1"
                    ).fetchone()
                    if row is None or type(row[0]) is not int or row[0] != SCHEMA_VERSION:
                        raise RuntimeError("unsupported issue ownership schema version")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise RuntimeError("issue ownership registry is unavailable") from exc
                time.sleep(0.05)

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.database, isolation_level=None, timeout=0.25)
        conn.execute("PRAGMA busy_timeout=250")
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _record(row: tuple[object, ...] | None) -> OwnershipRecord | None:
        if row is None:
            return None
        return OwnershipRecord(
            issue_id=str(row[0]), owner_session_id=str(row[1]), generation=str(row[2]),
            mode=str(row[3]), created_at=int(row[4]), updated_at=int(row[5]),
        )

    def schema_version(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT schema_version FROM issue_ownership_meta WHERE singleton = 1").fetchone()
        if row is None or type(row[0]) is not int or row[0] != SCHEMA_VERSION:
            raise RuntimeError("unsupported issue ownership schema version")
        return row[0]


    def get(self, issue_id: str) -> OwnershipRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT issue_id, owner_session_id, generation, mode, created_at, updated_at "
                "FROM issue_ownership WHERE profile = ? AND workspace = ? AND issue_id = ?",
                (self.profile, self.workspace, issue_id),
            ).fetchone()
        return self._record(row)

    def claim(self, issue_id: str, owner_session_id: str) -> ClaimResult:
        """Claim an issue unless chat or an active Linear delivery already owns it."""
        if not issue_id or not owner_session_id:
            raise ValueError("issue_id and owner_session_id are required")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT issue_id, owner_session_id, generation, mode, created_at, updated_at "
                "FROM issue_ownership WHERE profile = ? AND workspace = ? AND issue_id = ?",
                (self.profile, self.workspace, issue_id),
            ).fetchone()
            record = self._record(row)
            if record is not None:
                if record.mode == "released":
                    now = int(time.time())
                    refreshed = OwnershipRecord(
                        issue_id, owner_session_id, secrets.token_urlsafe(24), "active", now, now,
                    )
                    updated = conn.execute(
                        "UPDATE issue_ownership SET owner_session_id = ?, generation = ?, mode = 'active', updated_at = ? "
                        "WHERE profile = ? AND workspace = ? AND issue_id = ? AND mode = 'released'",
                        (refreshed.owner_session_id, refreshed.generation, now, self.profile, self.workspace, issue_id),
                    ).rowcount
                    conn.execute("COMMIT")
                    if updated != 1:
                        return ClaimResult("tombstoned", record)
                    return ClaimResult("claimed", refreshed)
                conn.execute("COMMIT")
                if record.mode != "active":
                    return ClaimResult("tombstoned", record)
                return ClaimResult("claimed" if record.owner_session_id == owner_session_id else "conflict", record)
            delivery_table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'deliveries'"
            ).fetchone()
            if delivery_table is not None:
                active = conn.execute(
                    "SELECT 1 FROM deliveries WHERE issue_id = ? AND state IN ('queued', 'prepared', 'running', 'ambiguous') LIMIT 1",
                    (issue_id,),
                ).fetchone()
                if active is not None:
                    conn.execute("COMMIT")
                    return ClaimResult("worker_active", None)
            now = int(time.time())
            record = OwnershipRecord(issue_id, owner_session_id, secrets.token_urlsafe(24), "active", now, now)
            conn.execute(
                "INSERT INTO issue_ownership (profile, workspace, issue_id, owner_session_id, generation, mode, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (self.profile, self.workspace, record.issue_id, record.owner_session_id, record.generation, record.mode, record.created_at, record.updated_at),
            )
            conn.execute("COMMIT")
            return ClaimResult("claimed", record)

    def takeover(self, issue_id: str, owner_session_id: str) -> ClaimResult:
        """Explicitly steal released or other-session ownership. Reconcile still fences."""
        if not issue_id or not owner_session_id:
            raise ValueError("issue_id and owner_session_id are required")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT issue_id, owner_session_id, generation, mode, created_at, updated_at "
                "FROM issue_ownership WHERE profile = ? AND workspace = ? AND issue_id = ?",
                (self.profile, self.workspace, issue_id),
            ).fetchone()
            record = self._record(row)
            if record is not None and record.mode == "reconcile":
                conn.execute("COMMIT")
                return ClaimResult("tombstoned", record)
            if record is not None and record.mode == "active" and record.owner_session_id == owner_session_id:
                conn.execute("COMMIT")
                return ClaimResult("claimed", record)
            delivery_table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'deliveries'"
            ).fetchone()
            if delivery_table is not None:
                active = conn.execute(
                    "SELECT 1 FROM deliveries WHERE issue_id = ? AND state IN ('queued', 'prepared', 'running', 'ambiguous') LIMIT 1",
                    (issue_id,),
                ).fetchone()
                if active is not None:
                    conn.execute("COMMIT")
                    return ClaimResult("worker_active", None)
            now = int(time.time())
            stolen = OwnershipRecord(issue_id, owner_session_id, secrets.token_urlsafe(24), "active", now, now)
            if record is None:
                conn.execute(
                    "INSERT INTO issue_ownership (profile, workspace, issue_id, owner_session_id, generation, mode, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (self.profile, self.workspace, stolen.issue_id, stolen.owner_session_id, stolen.generation, stolen.mode, stolen.created_at, stolen.updated_at),
                )
            else:
                updated = conn.execute(
                    "UPDATE issue_ownership SET owner_session_id = ?, generation = ?, mode = 'active', updated_at = ? "
                    "WHERE profile = ? AND workspace = ? AND issue_id = ? AND mode IN ('active', 'released')",
                    (stolen.owner_session_id, stolen.generation, now, self.profile, self.workspace, issue_id),
                ).rowcount
                if updated != 1:
                    conn.execute("COMMIT")
                    return ClaimResult("tombstoned", record)
            conn.execute("COMMIT")
            return ClaimResult("claimed", stolen)

    def release(self, issue_id: str, owner_session_id: str, generation: str) -> bool:
        return self._transition(issue_id, owner_session_id, generation, "released")

    def reconcile(self, issue_id: str, owner_session_id: str, generation: str) -> bool:
        return self._transition(issue_id, owner_session_id, generation, "reconcile")

    def activate_reconciled(self, issue_id: str, owner_session_id: str, generation: str) -> OwnershipRecord | None:
        """Unauthenticated generation rotation is refused.

        Use OperatorResumeGate with an authenticated operator, exact effect IDs,
        and verified stop/release evidence. Direct callers cannot rotate.
        """
        return None

    def _transition(self, issue_id: str, owner_session_id: str, generation: str, mode: str) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute(
                "UPDATE issue_ownership SET mode = ?, updated_at = ? WHERE profile = ? AND workspace = ? AND issue_id = ? AND owner_session_id = ? AND generation = ? AND mode = 'active'",
                (mode, int(time.time()), self.profile, self.workspace, issue_id, owner_session_id, generation),
            ).rowcount
            conn.execute("COMMIT")
        return updated == 1
