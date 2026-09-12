"""Durable, explicit chat-origin project-closeout admission.

This module never inspects a transcript.  Admission is made only by an explicit,
verified chat tracking command and explicit completion summary.  The lifecycle hook only
marks a successful terminal event eligible; a managed service drains it later.
"""
from __future__ import annotations

import sqlite3
import time
import asyncio
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

try:
    from .linear_quota import CLOSEOUT_RETRY_SECONDS
except ImportError:  # Direct script/test import.
    from linear_quota import CLOSEOUT_RETRY_SECONDS


class ChatCloseoutError(RuntimeError):
    pass


QUIET_SECONDS = 1800


class ChatCloseoutRegistry:
    def __init__(
        self,
        database: Path,
        *,
        profile: str,
        workspace: str,
        clock: Callable[[], float] | None = None,
        quiet_seconds: float = QUIET_SECONDS,
    ) -> None:
        if quiet_seconds < 0:
            raise ValueError("quiet_seconds must be non-negative")
        self.database = Path(database)
        self.profile, self.workspace = profile, workspace
        self.clock = clock or time.time
        self.quiet_seconds = quiet_seconds
        self.database.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS chat_closeout_tracking (
                    profile TEXT NOT NULL, workspace TEXT NOT NULL, issue_id TEXT NOT NULL,
                    owner_session_id TEXT NOT NULL, generation TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('active', 'completed', 'canceled')),
                    summary TEXT, PRIMARY KEY (profile, workspace, issue_id)
                );
                CREATE TABLE IF NOT EXISTS chat_closeout_outbox (
                    profile TEXT NOT NULL, workspace TEXT NOT NULL, owner_session_id TEXT NOT NULL,
                    issue_id TEXT NOT NULL, closeout_key TEXT NOT NULL, summary TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('admitted', 'sending', 'sent', 'suppressed')),
                    eligible_at REAL, lease_token TEXT, lease_until REAL,
                    dispatch_authorized_at REAL,
                    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                    PRIMARY KEY (profile, workspace, owner_session_id, issue_id)
                );
                CREATE INDEX IF NOT EXISTS chat_closeout_outbox_owner
                    ON chat_closeout_outbox(profile, workspace, owner_session_id, state);
            """)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(chat_closeout_outbox)")}
            for name, definition in (
                ("eligible_at", "REAL"), ("lease_token", "TEXT"), ("lease_until", "REAL"),
                ("dispatch_authorized_at", "REAL"),
            ):
                if name not in columns:
                    conn.execute(f"ALTER TABLE chat_closeout_outbox ADD COLUMN {name} {definition}")

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.database, isolation_level=None, timeout=1)
        conn.execute("PRAGMA busy_timeout=1000")
        try:
            yield conn
        finally:
            conn.close()

    def track(self, issue_id: str, owner_session_id: str, generation: str) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            owned = conn.execute(
                "SELECT mode FROM issue_ownership WHERE profile=? AND workspace=? AND issue_id=? "
                "AND owner_session_id=? AND generation=?",
                (self.profile, self.workspace, issue_id, owner_session_id, generation),
            ).fetchone()
            if owned != ("active",):
                conn.execute("ROLLBACK")
                raise ChatCloseoutError("issue is not an active tracked chat claim")
            conn.execute(
                """INSERT INTO chat_closeout_tracking
                (profile, workspace, issue_id, owner_session_id, generation, state)
                VALUES (?, ?, ?, ?, ?, 'active')
                ON CONFLICT(profile, workspace, issue_id) DO UPDATE SET
                owner_session_id=excluded.owner_session_id,
                generation=excluded.generation,
                state='active',
                summary=NULL""",
                (self.profile, self.workspace, issue_id, owner_session_id, generation),
            )
            conn.execute("COMMIT")

    def complete(self, issue_id: str, owner_session_id: str, generation: str, summary: str) -> str:
        if not isinstance(summary, str) or not summary.strip() or summary != summary.strip():
            raise ChatCloseoutError("an explicit non-empty closeout summary is required")
        # One deterministic key per session is intentionally shared by every
        # admitted issue; the publisher resolves live project membership and
        # emits exactly one update for each affected project.
        key = f"chat:{self.profile}:{self.workspace}:{owner_session_id}:closeout"
        now = int(time.time())
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            tracked = conn.execute("SELECT state FROM chat_closeout_tracking WHERE profile=? AND workspace=? AND issue_id=? AND owner_session_id=? AND generation=?",
                                   (self.profile, self.workspace, issue_id, owner_session_id, generation)).fetchone()
            owned = conn.execute("SELECT mode FROM issue_ownership WHERE profile=? AND workspace=? AND issue_id=? AND owner_session_id=? AND generation=?",
                                 (self.profile, self.workspace, issue_id, owner_session_id, generation)).fetchone()
            if tracked != ('active',) or owned != ('active',):
                conn.execute("ROLLBACK")
                raise ChatCloseoutError("tracked issue is no longer the active owner claim")
            conn.execute("UPDATE issue_ownership SET mode='released', updated_at=? WHERE profile=? AND workspace=? AND issue_id=?",
                         (now, self.profile, self.workspace, issue_id))
            conn.execute("UPDATE chat_closeout_tracking SET state='completed', summary=? WHERE profile=? AND workspace=? AND issue_id=?",
                         (summary, self.profile, self.workspace, issue_id))
            conn.execute("""INSERT INTO chat_closeout_outbox
                (profile, workspace, owner_session_id, issue_id, closeout_key, summary, state, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'admitted', ?, ?)""",
                (self.profile, self.workspace, owner_session_id, issue_id, key, summary, now, now))
            conn.execute("COMMIT")
        return key

    def cancel(self, session_id: str) -> int:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute("UPDATE chat_closeout_outbox SET state='suppressed', updated_at=? WHERE profile=? AND workspace=? AND owner_session_id=? AND state IN ('admitted', 'sending') AND dispatch_authorized_at IS NULL",
                                   (int(time.time()), self.profile, self.workspace, session_id)).rowcount
            conn.execute("COMMIT")
            return updated

    def mark_eligible(self, session_id: str, *, completed: bool, failed: bool, interrupted: bool) -> int:
        """Durably authorize a completed session for the retry service, without I/O."""
        if not session_id or not completed or failed or interrupted:
            return 0
        now = self.clock()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute(
                "UPDATE chat_closeout_outbox SET eligible_at=?, updated_at=? "
                "WHERE profile=? AND workspace=? AND owner_session_id=? "
                "AND state='admitted'",
                (now + self.quiet_seconds, int(now), self.profile, self.workspace, session_id),
            ).rowcount
            conn.execute("COMMIT")
            return updated

    def _claim(self, session_id: str | None, *, lease_seconds: float) -> tuple[str, str, list[dict[str, str]], str] | None:
        """Atomically lease one eligible session group, recovering expired sends only."""
        now = self.clock()
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            query = (
                "SELECT owner_session_id FROM chat_closeout_outbox WHERE profile=? AND workspace=? "
                "AND eligible_at IS NOT NULL AND eligible_at<=? AND ((state='admitted') OR (state='sending' AND lease_until<?)) "
            )
            args: list[object] = [self.profile, self.workspace, now, now]
            if session_id is not None:
                query += "AND owner_session_id=? "
                args.append(session_id)
            row = conn.execute(query + "ORDER BY eligible_at, owner_session_id LIMIT 1", args).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            owner_session_id = row[0]
            rows = conn.execute(
                "SELECT closeout_key, issue_id, summary FROM chat_closeout_outbox "
                "WHERE profile=? AND workspace=? AND owner_session_id=? AND eligible_at IS NOT NULL "
                "AND state IN ('admitted','sending') ORDER BY created_at, issue_id",
                (self.profile, self.workspace, owner_session_id),
            ).fetchall()
            native_table = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='deliveries'").fetchone()
            if native_table and any(conn.execute(
                "SELECT 1 FROM deliveries WHERE issue_id=? AND state IN ('queued','prepared','running','ambiguous') LIMIT 1",
                (issue_id,),
            ).fetchone() for _, issue_id, _ in rows):
                conn.execute(
                    "UPDATE chat_closeout_outbox SET state='suppressed', lease_token=NULL, lease_until=NULL, updated_at=? "
                    "WHERE profile=? AND workspace=? AND owner_session_id=? AND state IN ('admitted','sending')",
                    (int(now), self.profile, self.workspace, owner_session_id),
                )
                conn.execute("COMMIT")
                return None
            # A live lease means another worker owns this entire grouped payload.
            if any(state == 'sending' for (state,) in conn.execute(
                "SELECT state FROM chat_closeout_outbox WHERE profile=? AND workspace=? AND owner_session_id=? "
                "AND state='sending' AND lease_until>=?", (self.profile, self.workspace, owner_session_id, now)
            )):
                conn.execute("COMMIT")
                return None
            conn.execute(
                "UPDATE chat_closeout_outbox SET state='sending', lease_token=?, lease_until=?, dispatch_authorized_at=NULL, updated_at=? "
                "WHERE profile=? AND workspace=? AND owner_session_id=? AND eligible_at IS NOT NULL "
                "AND (state='admitted' OR (state='sending' AND lease_until<?))",
                (token, now + lease_seconds, int(now), self.profile, self.workspace, owner_session_id, now),
            )
            conn.execute("COMMIT")
        return owner_session_id, rows[0][0], [{"issue_id": issue_id, "summary": summary} for _, issue_id, summary in rows], token

    def claim_next(self, *, lease_seconds: float = 30) -> tuple[str, str, list[dict[str, str]], str] | None:
        return self._claim(None, lease_seconds=lease_seconds)

    def authorize_dispatch(self, session_id: str, lease_token: str) -> bool:
        """Linearize the irreversible remote-send decision against cancellation.

        A cancellation which commits first suppresses the group and this returns
        false.  Once this transaction commits, the send is authorized and a
        later cancellation cannot truthfully claim to retract a remote call.
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            leased = conn.execute(
                "SELECT COUNT(*) FROM chat_closeout_outbox WHERE profile=? AND workspace=? "
                "AND owner_session_id=? AND state='sending' AND lease_token=?",
                (self.profile, self.workspace, session_id, lease_token),
            ).fetchone()[0]
            sendable = conn.execute(
                "SELECT COUNT(*) FROM chat_closeout_outbox WHERE profile=? AND workspace=? "
                "AND owner_session_id=? AND state='sending' AND lease_token=? "
                "AND dispatch_authorized_at IS NULL",
                (self.profile, self.workspace, session_id, lease_token),
            ).fetchone()[0]
            if not leased or sendable != leased:
                conn.execute("COMMIT")
                return False
            authorized = conn.execute(
                "UPDATE chat_closeout_outbox SET dispatch_authorized_at=?, updated_at=? "
                "WHERE profile=? AND workspace=? AND owner_session_id=? AND state='sending' "
                "AND lease_token=? AND dispatch_authorized_at IS NULL",
                (time.time(), int(time.time()), self.profile, self.workspace, session_id, lease_token),
            ).rowcount
            conn.execute("COMMIT")
            return authorized == leased

    def settle(self, session_id: str, lease_token: str, *, succeeded: bool) -> int:
        """Settle only the caller's lease; cancellation wins any race with remote I/O."""
        with self._connect() as conn:
            if succeeded:
                return conn.execute(
                    "UPDATE chat_closeout_outbox SET state='sent', lease_token=NULL, lease_until=NULL, dispatch_authorized_at=NULL, updated_at=? "
                    "WHERE profile=? AND workspace=? AND owner_session_id=? AND state='sending' AND lease_token=?",
                    (int(time.time()), self.profile, self.workspace, session_id, lease_token),
                ).rowcount
            return conn.execute(
                "UPDATE chat_closeout_outbox SET state='admitted', lease_token=NULL, lease_until=NULL, dispatch_authorized_at=NULL, updated_at=? "
                "WHERE profile=? AND workspace=? AND owner_session_id=? AND state='sending' AND lease_token=?",
                (int(time.time()), self.profile, self.workspace, session_id, lease_token),
            ).rowcount

    def drain(self, session_id: str, *, completed: bool, failed: bool, interrupted: bool,
              publish: Callable[[str, list[dict[str, str]]], object]) -> int:
        """Publish a durable session group; failures retain the same retry key."""
        if not session_id or not completed or failed or interrupted:
            return 0
        self.mark_eligible(session_id, completed=completed, failed=failed, interrupted=interrupted)
        claimed = self._claim(session_id, lease_seconds=30)
        if claimed is None:
            return 0
        owner_session_id, key, issues, lease_token = claimed
        if not self.authorize_dispatch(owner_session_id, lease_token):
            return 0
        try:
            publish(key, issues)
        except Exception:  # noqa: BLE001 - publisher transports expose no shared exception base
            # Deterministic project-update IDs reconcile an uncertain mutation.
            # Keep the outbox sendable instead of permanently suppressing it.
            self.settle(owner_session_id, lease_token, succeeded=False)
            return 0
        return self.settle(owner_session_id, lease_token, succeeded=True)


class ChatCloseoutRetryService:
    """One bounded lifecycle worker; SQLite leases make duplicate processes safe."""
    def __init__(self, registry: ChatCloseoutRegistry, publish: Callable[[str, list[dict[str, str]]], object], *, retry_delay: float = CLOSEOUT_RETRY_SECONDS) -> None:
        self.registry, self.publish, self.retry_delay = registry, publish, retry_delay
        self._wake = asyncio.Event()
        self._dispatch_lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def wake(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._wake.set)

    async def run(self, stop_event) -> None:
        self._loop = asyncio.get_running_loop()
        while not stop_event.is_set():
            claimed = await asyncio.to_thread(self.registry.claim_next)
            if claimed is not None:
                session_id, key, issues, lease_token = claimed
                # The dispatch lock spans the final shutdown checks and the
                # irreversible-send authorization.  A stop observed before the
                # authorization returns prevents publication; after it commits,
                # cancellation cannot honestly retract a remote call.
                async with self._dispatch_lock:
                    if stop_event.is_set():
                        return
                    authorized = await asyncio.to_thread(
                        self.registry.authorize_dispatch, session_id, lease_token
                    )
                    if not authorized:
                        continue
                    if stop_event.is_set():
                        return
                    try:
                        await asyncio.to_thread(self.publish, key, issues)
                    except Exception:  # noqa: BLE001 - remote transports have unrelated exception types
                        await asyncio.to_thread(self.registry.settle, session_id, lease_token, succeeded=False)
                    else:
                        await asyncio.to_thread(self.registry.settle, session_id, lease_token, succeeded=True)
                continue
            self._wake.clear()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.retry_delay)
            except TimeoutError:
                pass
