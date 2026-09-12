"""Shared workspace/issue authority for Linear handoffs (KEN-444).

Profile-local SQLite is not a fleet lock. This store is keyed only by
workspace and issue_id. Isolated store files do not fence each other.
Stop acknowledgement or a physically stopped v2 lifetime is required before
transfer. External effects and deployment-resource locks require the current
generation.
"""
from __future__ import annotations

import re
import secrets
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

try:
    from .linear_stop import acknowledge_handoff_receipt
except ImportError:  # direct module invocation in the plugin directory
    from linear_stop import acknowledge_handoff_receipt

SCHEMA_VERSION = 3
FLEET_AUTHORITY_STORE = Path("/opt/hermes-fleet/shared/linear-authority/shared-issue-authority.db")
EXTERNAL_EFFECTS = frozenset(
    {
        "attachment",
        "deploy",
        "error",
        "issue_comment",
        "issue_handoff",
        "issue_media",
        "issue_status_active",
        "issue_status_done",
        "issue_status_failure",
        "issue_status_review",
        "issue_status_terminal_waiting",
        "issue_status_waiting",
        "project_update",
        "response",
        "thought",
    }
)
_SAFE_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class HandoffDenied(RuntimeError):
    """Cross-agent fencing rejected the requested mutation."""


@dataclass(frozen=True)
class SharedLease:
    workspace: str
    issue_id: str
    owner_id: str
    generation: str
    mode: str


@dataclass(frozen=True)
class ClaimResult:
    status: str
    lease: SharedLease


def issue_worktree(root: Path, *, workspace: str, issue_id: str) -> Path:
    """Return a per-issue worktree path; never reuse a checkout across issues."""
    if not _SAFE_IDENTITY.fullmatch(workspace) or not _SAFE_IDENTITY.fullmatch(issue_id):
        raise HandoffDenied("unsafe worktree identity")
    base = Path(root).resolve()
    path = (base / workspace / issue_id).resolve()
    try:
        path.relative_to(base)
    except ValueError as exc:
        raise HandoffDenied("unsafe worktree identity") from exc
    return path


def require_fleet_authority_store(
    raw: object,
    *,
    state_database: str | Path,
    profile_home: str | Path,
) -> Path:
    """Reject missing or profile-local stores; those are not fleet locks."""
    if not isinstance(raw, str) or not raw or raw.strip() != raw:
        raise HandoffDenied("shared authority store is required")
    path = Path(raw)
    if not path.is_absolute():
        raise HandoffDenied("shared authority store must be an absolute path")
    state = Path(state_database)
    local_state = Path(profile_home) / "linear-agent"
    if path == state:
        raise HandoffDenied("profile-local SQLite is not a fleet lock")
    try:
        path.relative_to(local_state)
    except ValueError:
        if path != FLEET_AUTHORITY_STORE:
            raise HandoffDenied("shared authority store must be the fleet-visible path") from None
        return path
    raise HandoffDenied("profile-local SQLite is not a fleet lock")


class SharedIssueAuthority:
    """Fleet handoff authority keyed by workspace and issue, not profile."""

    def __init__(self, store: Path) -> None:
        store = Path(store)
        store.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.store = store
        self._ensure_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.store, timeout=5, isolation_level=None)
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS shared_authority_meta ("
                "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
                "schema_version INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS shared_issue_authority ("
                "workspace TEXT NOT NULL, "
                "issue_id TEXT NOT NULL, "
                "owner_id TEXT NOT NULL, "
                "generation TEXT NOT NULL, "
                "mode TEXT NOT NULL CHECK (mode IN ('active', 'released')), "
                "emit_ticket TEXT NOT NULL DEFAULT '', "
                "pending_owner_id TEXT NOT NULL DEFAULT '', "
                "created_at INTEGER NOT NULL, "
                "updated_at INTEGER NOT NULL, "
                "PRIMARY KEY (workspace, issue_id))"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS resource_locks ("
                "resource_id TEXT PRIMARY KEY, "
                "workspace TEXT NOT NULL, "
                "issue_id TEXT NOT NULL, "
                "owner_id TEXT NOT NULL, "
                "generation TEXT NOT NULL)"
            )
            row = conn.execute(
                "SELECT schema_version FROM shared_authority_meta WHERE singleton = 1"
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO shared_authority_meta (singleton, schema_version) VALUES (1, ?)",
                    (SCHEMA_VERSION,),
                )
            elif type(row[0]) is not int or row[0] < 1 or row[0] > SCHEMA_VERSION:
                conn.execute("ROLLBACK")
                raise RuntimeError("unsupported shared authority schema version")
            else:
                columns = {str(item[1]) for item in conn.execute("PRAGMA table_info(shared_issue_authority)")}
                if "emit_ticket" not in columns:
                    conn.execute(
                        "ALTER TABLE shared_issue_authority ADD COLUMN emit_ticket TEXT NOT NULL DEFAULT ''"
                    )
                if "pending_owner_id" not in columns:
                    conn.execute(
                        "ALTER TABLE shared_issue_authority ADD COLUMN pending_owner_id TEXT NOT NULL DEFAULT ''"
                    )
                if row[0] != SCHEMA_VERSION:
                    conn.execute(
                        "UPDATE shared_authority_meta SET schema_version = ? WHERE singleton = 1",
                        (SCHEMA_VERSION,),
                    )
            conn.execute("COMMIT")

    def _lease(self, row: tuple[object, ...] | None) -> SharedLease | None:
        if row is None:
            return None
        return SharedLease(
            workspace=str(row[0]),
            issue_id=str(row[1]),
            owner_id=str(row[2]),
            generation=str(row[3]),
            mode=str(row[4]),
        )

    def get(self, *, workspace: str, issue_id: str) -> SharedLease | None:
        self._require_identity(workspace=workspace, issue_id=issue_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT workspace, issue_id, owner_id, generation, mode "
                "FROM shared_issue_authority WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            ).fetchone()
        return self._lease(row)

    def pending_owner(self, *, workspace: str, issue_id: str) -> str | None:
        """Return the recorded successor, if a production claim requested handoff."""
        self._require_identity(workspace=workspace, issue_id=issue_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT pending_owner_id FROM shared_issue_authority "
                "WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            ).fetchone()
        if row is None:
            return None
        owner = str(row[0] or "")
        return owner or None

    def request_transfer(self, *, workspace: str, issue_id: str, to_owner: str) -> None:
        """Record a successor from a conflicting production claim; do not grant yet."""
        self._require_identity(workspace=workspace, issue_id=issue_id, owner_id=to_owner)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT owner_id, mode, pending_owner_id FROM shared_issue_authority "
                "WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            ).fetchone()
            if row is None or str(row[1]) != "active":
                conn.execute("ROLLBACK")
                raise HandoffDenied("no active lease to hand off")
            if str(row[0]) == to_owner:
                conn.execute("COMMIT")
                return
            pending = str(row[2] or "")
            if pending and pending != to_owner:
                conn.execute("ROLLBACK")
                raise HandoffDenied("handoff already requested for another owner")
            conn.execute(
                "UPDATE shared_issue_authority SET pending_owner_id = ?, updated_at = ? "
                "WHERE workspace = ? AND issue_id = ? AND mode = 'active'",
                (to_owner, int(time.time()), workspace, issue_id),
            )
            conn.execute("COMMIT")

    def claim(self, *, workspace: str, issue_id: str, owner_id: str) -> ClaimResult:
        self._require_identity(workspace=workspace, issue_id=issue_id, owner_id=owner_id)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT workspace, issue_id, owner_id, generation, mode "
                "FROM shared_issue_authority WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            ).fetchone()
            current = self._lease(row)
            if current is not None and current.mode == "active":
                conn.execute("COMMIT")
                status = "claimed" if current.owner_id == owner_id else "conflict"
                return ClaimResult(status, current)
            now = int(time.time())
            lease = SharedLease(workspace, issue_id, owner_id, secrets.token_urlsafe(24), "active")
            if current is None:
                conn.execute(
                    "INSERT INTO shared_issue_authority "
                    "(workspace, issue_id, owner_id, generation, mode, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 'active', ?, ?)",
                    (workspace, issue_id, owner_id, lease.generation, now, now),
                )
            else:
                updated = conn.execute(
                    "UPDATE shared_issue_authority SET owner_id = ?, generation = ?, mode = 'active', "
                    "pending_owner_id = '', updated_at = ? WHERE workspace = ? AND issue_id = ? AND mode = 'released'",
                    (owner_id, lease.generation, now, workspace, issue_id),
                ).rowcount
                if updated != 1:
                    refreshed = self._lease(
                        conn.execute(
                            "SELECT workspace, issue_id, owner_id, generation, mode "
                            "FROM shared_issue_authority WHERE workspace = ? AND issue_id = ?",
                            (workspace, issue_id),
                        ).fetchone()
                    )
                    conn.execute("COMMIT")
                    assert refreshed is not None
                    return ClaimResult("conflict", refreshed)
            conn.execute("COMMIT")
            return ClaimResult("claimed", lease)

    def transfer(
        self,
        *,
        workspace: str,
        issue_id: str,
        from_owner: str,
        to_owner: str,
        generation: str,
        stop_receipt: object,
        session_key: str,
        execution_id: str,
    ) -> SharedLease:
        self._require_identity(workspace=workspace, issue_id=issue_id, owner_id=from_owner)
        self._require_identity(workspace=workspace, issue_id=issue_id, owner_id=to_owner)
        if not generation or from_owner == to_owner:
            raise HandoffDenied("transfer requires a new owner and current generation")
        if not acknowledge_handoff_receipt(stop_receipt, session_key, execution_id):
            raise HandoffDenied("acknowledged stop or terminal lifecycle is required before transfer")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT workspace, issue_id, owner_id, generation, mode, emit_ticket "
                "FROM shared_issue_authority WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            ).fetchone()
            current = self._lease(row)
            if (
                current is None
                or current.mode != "active"
                or current.owner_id != from_owner
                or current.generation != generation
            ):
                conn.execute("ROLLBACK")
                raise HandoffDenied("current generation does not authorize transfer")
            if str(row[5] or ""):
                conn.execute("ROLLBACK")
                raise HandoffDenied("external effect is in flight")
            pending = conn.execute(
                "SELECT pending_owner_id FROM shared_issue_authority WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            ).fetchone()
            requested = str(pending[0] or "") if pending is not None else ""
            if requested and requested != to_owner:
                conn.execute("ROLLBACK")
                raise HandoffDenied("transfer target does not match requested successor")
            now = int(time.time())
            rotated = SharedLease(workspace, issue_id, to_owner, secrets.token_urlsafe(24), "active")
            updated = conn.execute(
                "UPDATE shared_issue_authority SET owner_id = ?, generation = ?, emit_ticket = '', "
                "pending_owner_id = '', updated_at = ? "
                "WHERE workspace = ? AND issue_id = ? AND owner_id = ? AND generation = ? AND mode = 'active' "
                "AND emit_ticket = ''",
                (to_owner, rotated.generation, now, workspace, issue_id, from_owner, generation),
            ).rowcount
            if updated != 1:
                conn.execute("ROLLBACK")
                raise HandoffDenied("current generation does not authorize transfer")
            conn.execute(
                "DELETE FROM resource_locks WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            )
            conn.execute("COMMIT")
            return rotated

    def release(
        self,
        *,
        workspace: str,
        issue_id: str,
        owner_id: str,
        generation: str,
        stop_receipt: object,
        session_key: str,
        execution_id: str,
    ) -> SharedLease:
        self._require_identity(workspace=workspace, issue_id=issue_id, owner_id=owner_id)
        if not generation:
            raise HandoffDenied("current generation does not authorize transfer")
        if not acknowledge_handoff_receipt(stop_receipt, session_key, execution_id):
            raise HandoffDenied("acknowledged stop or terminal lifecycle is required before transfer")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT workspace, issue_id, owner_id, generation, mode, emit_ticket "
                "FROM shared_issue_authority WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            ).fetchone()
            current = self._lease(row)
            if (
                current is None
                or current.mode != "active"
                or current.owner_id != owner_id
                or current.generation != generation
            ):
                conn.execute("ROLLBACK")
                raise HandoffDenied("current generation does not authorize transfer")
            if str(row[5] or ""):
                conn.execute("ROLLBACK")
                raise HandoffDenied("external effect is in flight")
            now = int(time.time())
            rotated = SharedLease(workspace, issue_id, owner_id, secrets.token_urlsafe(24), "released")
            updated = conn.execute(
                "UPDATE shared_issue_authority SET generation = ?, mode = 'released', emit_ticket = '', "
                "pending_owner_id = '', updated_at = ? "
                "WHERE workspace = ? AND issue_id = ? AND owner_id = ? AND generation = ? AND mode = 'active' "
                "AND emit_ticket = ''",
                (rotated.generation, now, workspace, issue_id, owner_id, generation),
            ).rowcount
            if updated != 1:
                conn.execute("ROLLBACK")
                raise HandoffDenied("current generation does not authorize transfer")
            conn.execute(
                "DELETE FROM resource_locks WHERE workspace = ? AND issue_id = ?",
                (workspace, issue_id),
            )
            conn.execute("COMMIT")
            return rotated

    def authorize_effect(
        self,
        *,
        workspace: str,
        issue_id: str,
        owner_id: str,
        generation: str,
        kind: str,
    ) -> None:
        if kind not in EXTERNAL_EFFECTS:
            raise HandoffDenied("unknown external effect")
        lease = self.get(workspace=workspace, issue_id=issue_id)
        if (
            lease is None
            or lease.mode != "active"
            or lease.owner_id != owner_id
            or lease.generation != generation
        ):
            raise HandoffDenied("stale generation cannot publish")

    def begin_external_effect(
        self,
        *,
        workspace: str,
        issue_id: str,
        owner_id: str,
        generation: str,
        kind: str,
    ) -> str:
        """Admit one in-flight external effect; transfer fails until it finishes."""
        self.authorize_effect(
            workspace=workspace,
            issue_id=issue_id,
            owner_id=owner_id,
            generation=generation,
            kind=kind,
        )
        ticket = secrets.token_urlsafe(24)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute(
                "UPDATE shared_issue_authority SET emit_ticket = ? "
                "WHERE workspace = ? AND issue_id = ? AND owner_id = ? AND generation = ? "
                "AND mode = 'active' AND emit_ticket = ''",
                (ticket, workspace, issue_id, owner_id, generation),
            ).rowcount
            if updated != 1:
                conn.execute("ROLLBACK")
                raise HandoffDenied("external effect is in flight")
            conn.execute("COMMIT")
        return ticket

    def finish_external_effect(
        self,
        *,
        workspace: str,
        issue_id: str,
        owner_id: str,
        generation: str,
        ticket: str,
    ) -> None:
        if not ticket:
            raise HandoffDenied("external effect ticket is required")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute(
                "UPDATE shared_issue_authority SET emit_ticket = '' "
                "WHERE workspace = ? AND issue_id = ? AND owner_id = ? AND generation = ? "
                "AND emit_ticket = ?",
                (workspace, issue_id, owner_id, generation, ticket),
            ).rowcount
            if updated != 1:
                conn.execute("ROLLBACK")
                raise HandoffDenied("external effect ticket does not match")
            conn.execute("COMMIT")

    def acquire_resource(
        self,
        resource_id: str,
        *,
        workspace: str,
        issue_id: str,
        owner_id: str,
        generation: str,
    ) -> None:
        if not resource_id or resource_id.strip() != resource_id:
            raise HandoffDenied("resource id is required")
        self._require_identity(workspace=workspace, issue_id=issue_id, owner_id=owner_id)
        if not generation:
            raise HandoffDenied("stale generation cannot publish")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = self._lease(
                conn.execute(
                    "SELECT workspace, issue_id, owner_id, generation, mode "
                    "FROM shared_issue_authority WHERE workspace = ? AND issue_id = ?",
                    (workspace, issue_id),
                ).fetchone()
            )
            if (
                lease is None
                or lease.mode != "active"
                or lease.owner_id != owner_id
                or lease.generation != generation
            ):
                conn.execute("ROLLBACK")
                raise HandoffDenied("stale generation cannot publish")
            row = conn.execute(
                "SELECT resource_id, owner_id, generation FROM resource_locks WHERE resource_id = ?",
                (resource_id,),
            ).fetchone()
            if row is not None:
                if str(row[1]) == owner_id and str(row[2]) == generation:
                    conn.execute("COMMIT")
                    return
                conn.execute("ROLLBACK")
                raise HandoffDenied("deployment resource is already locked")
            conn.execute(
                "INSERT INTO resource_locks (resource_id, workspace, issue_id, owner_id, generation) "
                "VALUES (?, ?, ?, ?, ?)",
                (resource_id, workspace, issue_id, owner_id, generation),
            )
            conn.execute("COMMIT")

    def release_resource(self, resource_id: str, *, owner_id: str, generation: str) -> None:
        if not resource_id or not owner_id or not generation:
            raise HandoffDenied("resource release requires current generation")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lock = conn.execute(
                "SELECT resource_id, workspace, issue_id, owner_id, generation "
                "FROM resource_locks WHERE resource_id = ?",
                (resource_id,),
            ).fetchone()
            if lock is None:
                conn.execute("ROLLBACK")
                raise HandoffDenied("stale generation cannot release a resource")
            lease = self._lease(
                conn.execute(
                    "SELECT workspace, issue_id, owner_id, generation, mode "
                    "FROM shared_issue_authority WHERE workspace = ? AND issue_id = ?",
                    (str(lock[1]), str(lock[2])),
                ).fetchone()
            )
            if (
                lease is None
                or lease.mode != "active"
                or lease.owner_id != owner_id
                or lease.generation != generation
                or str(lock[3]) != owner_id
                or str(lock[4]) != generation
            ):
                conn.execute("ROLLBACK")
                raise HandoffDenied("stale generation cannot release a resource")
            deleted = conn.execute(
                "DELETE FROM resource_locks WHERE resource_id = ? AND owner_id = ? AND generation = ?",
                (resource_id, owner_id, generation),
            ).rowcount
            if deleted != 1:
                conn.execute("ROLLBACK")
                raise HandoffDenied("stale generation cannot release a resource")
            conn.execute("COMMIT")

    def _require_identity(self, *, workspace: str, issue_id: str, owner_id: str | None = None) -> None:
        if not workspace or not issue_id or workspace.strip() != workspace or issue_id.strip() != issue_id:
            raise HandoffDenied("workspace and issue_id are required")
        if owner_id is not None and (not owner_id or owner_id.strip() != owner_id):
            raise HandoffDenied("owner_id is required")
