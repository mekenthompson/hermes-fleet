"""Profile-local plugin state: two tables, ``work`` and ``outbox``. No cross-container state.

``work``: one row per issue this profile is actively working (deleted when work ends, so
there are no tombstones). ``outbox``: pending Linear writes. Creates reuse the row ``id``
(a UUID v4 chosen at enqueue) as Linear's client id, so a retry after an unknown outcome
cannot duplicate a comment. States: pending, sent, failed.
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, Iterator

MIN_BACKOFF, MAX_BACKOFF, GIVE_UP_AFTER = 60.0, 3600.0, 86_400.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS work (
  issue_id TEXT PRIMARY KEY, origin TEXT NOT NULL, owner_ref TEXT NOT NULL,
  task_id TEXT, project_id TEXT, last_updated_at REAL NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS outbox (
  id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL, state TEXT NOT NULL DEFAULT 'pending');
"""


class Store:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.executescript(SCHEMA)

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with closing(sqlite3.connect(self.path, timeout=30, isolation_level=None)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise

    # -- work -------------------------------------------------------------
    def get(self, issue_id: str) -> dict[str, Any] | None:
        with self._tx() as db:
            row = db.execute("SELECT * FROM work WHERE issue_id = ?", (issue_id,)).fetchone()
        return dict(row) if row else None

    def put(self, issue_id: str, origin: str, owner_ref: str, *, task_id: str | None = None,
            project_id: str | None = None, last_updated_at: float = 0.0) -> None:
        with self._tx() as db:
            db.execute("INSERT OR REPLACE INTO work VALUES (?, ?, ?, ?, ?, ?)",
                       (issue_id, origin, owner_ref, task_id, project_id, last_updated_at))

    def update(self, issue_id: str, **fields: Any) -> None:
        cols = ", ".join(f"{name} = ?" for name in fields)
        with self._tx() as db:
            db.execute(f"UPDATE work SET {cols} WHERE issue_id = ?", (*fields.values(), issue_id))

    def delete(self, issue_id: str) -> None:
        with self._tx() as db:
            db.execute("DELETE FROM work WHERE issue_id = ?", (issue_id,))

    def active(self, origin: str | None = None) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM work WHERE ? IS NULL OR origin = ?", (origin, origin)).fetchall()
        return [dict(row) for row in rows]

    # -- outbox -----------------------------------------------------------
    def enqueue(self, kind: str, payload: dict[str, Any], *, at: float | None = None) -> str:
        row_id = str(uuid.uuid4())
        now = time.time() if at is None else at
        body = json.dumps({**payload, "enqueued_at": payload.get("enqueued_at", now)})
        with self._tx() as db:
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)", (row_id, kind, body, now))
        return row_id

    def due(self, now: float) -> list[dict[str, Any]]:
        """Oldest pending row per issue, if due: writes for one issue go out in order."""
        with self._tx() as db:
            rows = db.execute(
                "SELECT o.* FROM outbox o WHERE o.state = 'pending' AND o.rowid = ("
                " SELECT MIN(p.rowid) FROM outbox p WHERE p.state = 'pending' AND"
                " json_extract(p.payload, '$.issue_id') IS json_extract(o.payload, '$.issue_id')"
                ") AND o.next_at <= ? ORDER BY o.rowid", (now,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def pending(self, issue_id: str | None = None) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM outbox WHERE state = 'pending' AND (? IS NULL OR "
                              "json_extract(payload, '$.issue_id') = ?) ORDER BY rowid", (issue_id, issue_id)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def mark(self, row_id: str, state: str) -> None:
        with self._tx() as db:
            db.execute("UPDATE outbox SET state = ? WHERE id = ?", (state, row_id))

    def drop(self, row_id: str) -> None:
        with self._tx() as db:
            db.execute("DELETE FROM outbox WHERE id = ?", (row_id,))

    def defer(self, row_id: str, until: float) -> None:
        with self._tx() as db:
            db.execute("UPDATE outbox SET next_at = ? WHERE id = ?", (until, row_id))

    def retry(self, row: dict[str, Any], now: float) -> bool:
        """Back off 1 min doubling to 1 h. Returns True when the row is given up (24 h old)."""
        attempts = int(row["attempts"]) + 1
        if now - float(row["payload"].get("enqueued_at", now)) >= GIVE_UP_AFTER:
            with self._tx() as db:
                db.execute("UPDATE outbox SET state = 'failed', attempts = ? WHERE id = ?", (attempts, row["id"]))
            return True
        delay = min(MIN_BACKOFF * 2 ** (attempts - 1), MAX_BACKOFF)
        with self._tx() as db:
            db.execute("UPDATE outbox SET attempts = ?, next_at = ? WHERE id = ?", (attempts, now + delay, row["id"]))
        return False

    def revive_failed(self, now: float) -> int:
        """After any successful write, give timed-out rows one more try (same client id). A status
        write superseded by a later one for the same issue stays failed, so it cannot land stale."""
        with self._tx() as db:
            return db.execute(
                "UPDATE outbox SET state = 'pending', next_at = ? WHERE state = 'failed' AND attempts > 0 AND NOT ("
                " kind = 'status' AND EXISTS (SELECT 1 FROM outbox l WHERE l.kind = 'status' AND l.rowid > outbox.rowid"
                " AND json_extract(l.payload, '$.issue_id') = json_extract(outbox.payload, '$.issue_id')))",
                (now,)).rowcount

    def project_update(self, session_id: str, project_id: str) -> dict[str, Any] | None:
        with self._tx() as db:
            row = db.execute("SELECT * FROM outbox WHERE kind = 'project_update' AND state = 'pending' AND "
                             "attempts = 0 AND json_extract(payload, '$.session_id') = ? AND "
                             "json_extract(payload, '$.project_id') IS ?",
                             (session_id, project_id)).fetchone()
        return {**dict(row), "payload": json.loads(row["payload"])} if row else None

    def report(self, row_id: str) -> None:
        with self._tx() as db:
            db.execute("UPDATE outbox SET payload = json_set(payload, '$.reported', 1) WHERE id = ?", (row_id,))

    def rewrite(self, row_id: str, payload: dict[str, Any], next_at: float) -> None:
        with self._tx() as db:
            db.execute("UPDATE outbox SET payload = ?, next_at = ? WHERE id = ?", (json.dumps(payload), next_at, row_id))

    def delay_session_updates(self, session_id: str, next_at: float) -> None:
        """Quiet period: every turn in the session pushes its pending project updates back."""
        with self._tx() as db:
            db.execute("UPDATE outbox SET next_at = MAX(next_at, ?) WHERE kind = 'project_update' AND "
                       "state = 'pending' AND json_extract(payload, '$.session_id') = ?", (next_at, session_id))
