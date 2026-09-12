"""Explicit, authenticated operator gate for rotating a fenced Linear generation.

This module is deliberately separate from :mod:`linear_reconcile`: inspection
is read-only evidence and can never authorize this operation.  Rotation does
not clear a Stop control or replay an external effect.

Caller-constructed authentication flags are not accepted.  The only durable
authorization is a one-time ticket issued only from a verified job
requester via :meth:`LinearWorker.resume_reconciled_for_job`. The gate
does not accept a caller-chosen operator UUID.
against an allowlisted operator identity, persisted before rotation.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

try:
    from .linear_ownership import OwnershipRecord
except ImportError:  # direct module invocation in the plugin directory
    from linear_ownership import OwnershipRecord


class ResumeDenied(RuntimeError):
    """The durable evidence does not permit an operator rotation."""


@dataclass(frozen=True)
class IssuedAuthorization:
    """Server-issued one-time ticket. Homemade instances fail HMAC/ledger checks."""

    operator_id: str
    authorization_id: str
    mac: str


@dataclass(frozen=True)
class StopEvidence:
    linear_session_id: str
    stop_event_key: str
    target_delivery_id: str
    execution_id: str


@dataclass(frozen=True)
class EffectIdentifier:
    delivery_id: str
    kind: str


class OperatorResumeGate:
    """Rotate one reconciled ownership fence after exact, durable operator proof."""

    def __init__(
        self,
        database: Path,
        *,
        profile: str,
        workspace: str,
        allowed_operator_ids: Iterable[str],
    ) -> None:
        if not profile or not workspace:
            raise ValueError("profile and workspace are required")
        allowed = frozenset(allowed_operator_ids)
        if not allowed or any(
            type(item) is not str or not item or item.strip() != item or len(item) > 512
            for item in allowed
        ):
            raise ValueError("allowed_operator_ids must be a non-empty set of exact operator ids")
        self.database = Path(database)
        self.profile = profile
        self.workspace = workspace
        self.allowed_operator_ids = allowed

    @staticmethod
    def _require_text(value: str, name: str) -> None:
        if type(value) is not str or not value or value.strip() != value or len(value) > 512:
            raise ResumeDenied(f"{name} is required")

    @staticmethod
    def _mac(key: bytes, *, operator_id: str, authorization_id: str, profile: str, workspace: str) -> str:
        payload = f"{profile}\0{workspace}\0{operator_id}\0{authorization_id}".encode()
        return hmac.new(key, payload, hashlib.sha256).hexdigest()

    def _ensure_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operator_resume_secrets (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                key TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operator_resume_authorizations (
                authorization_id TEXT PRIMARY KEY,
                operator_id TEXT NOT NULL,
                profile TEXT NOT NULL,
                workspace TEXT NOT NULL,
                mac TEXT NOT NULL,
                used INTEGER NOT NULL CHECK (used IN (0, 1)),
                issue_id TEXT,
                old_generation TEXT,
                new_generation TEXT,
                authorized_at INTEGER
            )
            """
        )

    def _mac_key(self, conn: sqlite3.Connection) -> bytes:
        row = conn.execute("SELECT key FROM operator_resume_secrets WHERE id = 1").fetchone()
        if row is None:
            key = secrets.token_hex(32)
            conn.execute("INSERT INTO operator_resume_secrets VALUES (1, ?)", (key,))
            return bytes.fromhex(key)
        return bytes.fromhex(str(row[0]))

    def _connect(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(self.database, isolation_level=None, timeout=0.25)
        except sqlite3.Error as exc:
            raise ResumeDenied("private worker state is unavailable") from exc
        conn.execute("PRAGMA busy_timeout=250")
        return conn

    def _issue(self, operator_id: str) -> IssuedAuthorization:
        """Persist a one-time authorization for a verified requester only."""
        self._require_text(operator_id, "operator_id")
        if operator_id not in self.allowed_operator_ids:
            raise ResumeDenied("authenticated operator authorization is required")
        authorization_id = secrets.token_urlsafe(32)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._ensure_tables(conn)
            mac = self._mac(
                self._mac_key(conn),
                operator_id=operator_id,
                authorization_id=authorization_id,
                profile=self.profile,
                workspace=self.workspace,
            )
            conn.execute(
                "INSERT INTO operator_resume_authorizations "
                "(authorization_id, operator_id, profile, workspace, mac, used) "
                "VALUES (?, ?, ?, ?, ?, 0)",
                (authorization_id, operator_id, self.profile, self.workspace, mac),
            )
            conn.execute("COMMIT")
            return IssuedAuthorization(operator_id, authorization_id, mac)
        except (sqlite3.Error, ResumeDenied) as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            if isinstance(exc, ResumeDenied):
                raise
            raise ResumeDenied("operator authorization could not be issued") from exc
        finally:
            conn.close()

    def _validate_request(
        self,
        *,
        issue_id: str,
        owner_session_id: str,
        generation: str,
        authorization: IssuedAuthorization,
        stop_evidence: StopEvidence,
        effects: tuple[EffectIdentifier, ...],
    ) -> None:
        for name, value in (("issue_id", issue_id), ("owner_session_id", owner_session_id), ("generation", generation)):
            self._require_text(value, name)
        if not isinstance(authorization, IssuedAuthorization):
            raise ResumeDenied("issued operator authorization is required")
        self._require_text(authorization.operator_id, "operator_id")
        self._require_text(authorization.authorization_id, "authorization_id")
        self._require_text(authorization.mac, "mac")
        if authorization.operator_id not in self.allowed_operator_ids:
            raise ResumeDenied("authenticated operator authorization is required")
        if not isinstance(stop_evidence, StopEvidence):
            raise ResumeDenied("verified execution-stop evidence is required")
        for name, value in vars(stop_evidence).items():
            self._require_text(value, name)
        if type(effects) is not tuple:
            raise ResumeDenied("exact effect identifiers must be a tuple")
        for effect in effects:
            if not isinstance(effect, EffectIdentifier):
                raise ResumeDenied("exact effect identifiers are required")
            self._require_text(effect.delivery_id, "effect delivery_id")
            self._require_text(effect.kind, "effect kind")
        if len(set(effects)) != len(effects):
            raise ResumeDenied("exact effect identifiers must be unique")

    @staticmethod
    def _record(row: tuple[object, ...]) -> OwnershipRecord:
        return OwnershipRecord(
            issue_id=str(row[0]), owner_session_id=str(row[1]), generation=str(row[2]),
            mode=str(row[3]), created_at=int(row[4]), updated_at=int(row[5]),
        )

    def rotate(
        self,
        *,
        issue_id: str,
        owner_session_id: str,
        generation: str,
        authorization: IssuedAuthorization,
        stop_evidence: StopEvidence,
        effects: tuple[EffectIdentifier, ...],
    ) -> OwnershipRecord:
        """Authorize exactly one fresh generation or fail without changing ownership."""
        self._validate_request(
            issue_id=issue_id, owner_session_id=owner_session_id, generation=generation,
            authorization=authorization, stop_evidence=stop_evidence, effects=effects,
        )
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._ensure_tables(conn)
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            required = {
                "issue_ownership", "deliveries", "outbox", "session_controls",
                "stop_delivery_receipts", "execution_lifecycle_receipts",
            }
            if not required.issubset(tables):
                raise ResumeDenied("required reconciliation evidence is unavailable")
            expected_mac = self._mac(
                self._mac_key(conn),
                operator_id=authorization.operator_id,
                authorization_id=authorization.authorization_id,
                profile=self.profile,
                workspace=self.workspace,
            )
            if not hmac.compare_digest(expected_mac, authorization.mac):
                raise ResumeDenied("issued operator authorization is required")
            issued = conn.execute(
                "SELECT used FROM operator_resume_authorizations "
                "WHERE authorization_id=? AND operator_id=? AND profile=? AND workspace=? AND mac=?",
                (
                    authorization.authorization_id, authorization.operator_id,
                    self.profile, self.workspace, authorization.mac,
                ),
            ).fetchone()
            if issued is None:
                raise ResumeDenied("issued operator authorization is required")
            if int(issued[0]) != 0:
                raise ResumeDenied("operator authorization is already used")
            ownership = conn.execute(
                "SELECT issue_id, owner_session_id, generation, mode, created_at, updated_at "
                "FROM issue_ownership WHERE profile=? AND workspace=? AND issue_id=? "
                "AND owner_session_id=? AND generation=? AND mode='reconcile'",
                (self.profile, self.workspace, issue_id, owner_session_id, generation),
            ).fetchone()
            if ownership is None:
                raise ResumeDenied("reconciled ownership fence does not exactly match")
            control = conn.execute(
                "SELECT 1 FROM session_controls WHERE profile=? AND workspace=? AND issue_id=? "
                "AND linear_session_id=? AND stop_event_key=? AND state='ambiguous'",
                (self.profile, self.workspace, issue_id, stop_evidence.linear_session_id, stop_evidence.stop_event_key),
            ).fetchone()
            receipt = conn.execute(
                "SELECT 1 FROM stop_delivery_receipts WHERE profile=? AND workspace=? "
                "AND linear_session_id=? AND stop_event_key=? AND target_delivery_id=? "
                "AND execution_id=? AND status='accepted'",
                (self.profile, self.workspace, stop_evidence.linear_session_id, stop_evidence.stop_event_key,
                 stop_evidence.target_delivery_id, stop_evidence.execution_id),
            ).fetchone()
            lifecycle = conn.execute(
                "SELECT 1 FROM execution_lifecycle_receipts WHERE profile=? AND workspace=? "
                "AND linear_session_id=? AND target_delivery_id=? AND execution_id=? AND occupancy='released'",
                (self.profile, self.workspace, stop_evidence.linear_session_id,
                 stop_evidence.target_delivery_id, stop_evidence.execution_id),
            ).fetchone()
            if not (control and receipt and lifecycle):
                raise ResumeDenied("verified execution-stop evidence does not exactly match")
            unresolved = conn.execute(
                "SELECT 1 FROM deliveries WHERE issue_id=? AND state NOT IN ('completed','failed','canceled','rejected','ignored') LIMIT 1",
                (issue_id,),
            ).fetchone()
            if unresolved:
                raise ResumeDenied("interrupted delivery remains unresolved")
            actual_effects = tuple(EffectIdentifier(str(row[0]), str(row[1])) for row in conn.execute(
                "SELECT o.delivery_id, o.kind FROM outbox o JOIN deliveries d ON d.delivery_id=o.delivery_id "
                "WHERE d.issue_id=? ORDER BY o.delivery_id, o.kind", (issue_id,),
            ))
            if set(actual_effects) != set(effects) or len(actual_effects) != len(effects):
                raise ResumeDenied("exact effect identifiers do not match known API effects")
            unresolved_effect = conn.execute(
                "SELECT 1 FROM outbox o JOIN deliveries d ON d.delivery_id=o.delivery_id "
                "WHERE d.issue_id=? AND o.state NOT IN ('sent','suppressed') LIMIT 1", (issue_id,),
            ).fetchone()
            if unresolved_effect:
                raise ResumeDenied("known API effect remains ambiguous")
            token = secrets.token_urlsafe(24)
            now = int(time.time())
            changed = conn.execute(
                "UPDATE issue_ownership SET mode='active', generation=?, updated_at=? "
                "WHERE profile=? AND workspace=? AND issue_id=? AND owner_session_id=? "
                "AND generation=? AND mode='reconcile'",
                (token, now, self.profile, self.workspace, issue_id, owner_session_id, generation),
            ).rowcount
            if changed != 1:
                raise ResumeDenied("reconciled ownership changed before authorization")
            consumed = conn.execute(
                "UPDATE operator_resume_authorizations "
                "SET used=1, issue_id=?, old_generation=?, new_generation=?, authorized_at=? "
                "WHERE authorization_id=? AND used=0",
                (issue_id, generation, token, now, authorization.authorization_id),
            ).rowcount
            if consumed != 1:
                raise ResumeDenied("operator authorization is already used")
            row = conn.execute(
                "SELECT issue_id, owner_session_id, generation, mode, created_at, updated_at "
                "FROM issue_ownership WHERE profile=? AND workspace=? AND issue_id=?",
                (self.profile, self.workspace, issue_id),
            ).fetchone()
            conn.execute("COMMIT")
            return self._record(row)
        except (sqlite3.Error, ResumeDenied) as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            if isinstance(exc, ResumeDenied):
                raise
            raise ResumeDenied("reconciliation state is unavailable or incompatible") from exc
        finally:
            conn.close()
