"""Profile-local plugin state. No cross-container state.

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
from collections import Counter
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, Iterator

MIN_BACKOFF, MAX_BACKOFF, GIVE_UP_AFTER = 60.0, 3600.0, 86_400.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS work (
  issue_id TEXT PRIMARY KEY, origin TEXT NOT NULL, owner_ref TEXT NOT NULL,
  task_id TEXT, project_id TEXT, last_updated_at REAL NOT NULL DEFAULT 0,
  linear_session_id TEXT, panel_note TEXT, stop_requested_at REAL NOT NULL DEFAULT 0,
  last_event_id INTEGER NOT NULL DEFAULT 0, resume_fence_at REAL NOT NULL DEFAULT 0,
  run_generation INTEGER, ownership_id TEXT, pending_resume TEXT);
CREATE TABLE IF NOT EXISTS outbox (
  id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL, state TEXT NOT NULL DEFAULT 'pending');
CREATE TABLE IF NOT EXISTS chat_stop (
  id TEXT PRIMARY KEY, profile TEXT NOT NULL, issue_id TEXT NOT NULL,
  linear_session_id TEXT NOT NULL, source_activity_id TEXT NOT NULL,
  session_key TEXT NOT NULL, run_generation INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'requested', worker_completion TEXT NOT NULL DEFAULT 'unknown',
  completion_activity_id TEXT, uncertainty_activity_id TEXT,
  UNIQUE(profile, issue_id, linear_session_id, source_activity_id));
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


class Store:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.executescript(SCHEMA)
            columns = {row[1] for row in db.execute("PRAGMA table_info(work)")}
            for name in ("linear_session_id", "panel_note", "pending_resume"):
                if name not in columns:
                    db.execute(f"ALTER TABLE work ADD COLUMN {name} TEXT")
            if "stop_requested_at" not in columns:
                db.execute("ALTER TABLE work ADD COLUMN stop_requested_at REAL NOT NULL DEFAULT 0")
            if "last_event_id" not in columns:
                db.execute("ALTER TABLE work ADD COLUMN last_event_id INTEGER NOT NULL DEFAULT 0")
            if "resume_fence_at" not in columns:
                db.execute("ALTER TABLE work ADD COLUMN resume_fence_at REAL NOT NULL DEFAULT 0")
            if "run_generation" not in columns:
                db.execute("ALTER TABLE work ADD COLUMN run_generation INTEGER")
            if "ownership_id" not in columns:
                db.execute("ALTER TABLE work ADD COLUMN ownership_id TEXT")
            db.execute("UPDATE work SET ownership_id=lower(hex(randomblob(16))) WHERE ownership_id IS NULL")
            db.commit()

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

    def activation_cutoff_ms(self, configured: int | None = None) -> int | None:
        """Persist the first fresh-work cutoff; never replace legacy or differently-cut state."""
        with self._tx() as db:
            row = db.execute("SELECT value FROM metadata WHERE key='activation_cutoff_ms'").fetchone()
            if row:
                stored = int(row["value"])
                if configured is not None and configured != stored:
                    raise ValueError("linear: activation_cutoff_ms cannot change after it is persisted")
                return stored
            if configured is None:
                return None
            for table in ("work", "outbox", "chat_stop"):
                if db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
                    raise ValueError("linear: cannot establish activation_cutoff_ms over existing work state")
            db.execute("INSERT INTO metadata (key, value) VALUES ('activation_cutoff_ms', ?)", (str(configured),))
            return configured

    # -- work -------------------------------------------------------------
    def get(self, issue_id: str) -> dict[str, Any] | None:
        with self._tx() as db:
            row = db.execute("SELECT * FROM work WHERE issue_id = ?", (issue_id,)).fetchone()
        return dict(row) if row else None

    def put(self, issue_id: str, origin: str, owner_ref: str, *, task_id: str | None = None,
            project_id: str | None = None, last_updated_at: float = 0.0,
            run_generation: int | None = None) -> None:
        with self._tx() as db:
            db.execute("INSERT OR REPLACE INTO work (issue_id, origin, owner_ref, task_id, project_id, "
                       "last_updated_at, run_generation, ownership_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (issue_id, origin, owner_ref, task_id, project_id, last_updated_at, run_generation, str(uuid.uuid4())))

    def capture_chat_stop(self, issue_id: str, linear_session_id: str, activity_id: str,
                          profile: str, *, at: float, stamp: float = 0.0) -> dict[str, Any] | None:
        """Commit the exact target and stable Linear activity id before any core request."""
        with self._tx() as db:
            existing = db.execute("SELECT * FROM chat_stop WHERE profile=? AND issue_id=? AND "
                                  "linear_session_id=? AND source_activity_id=?",
                                  (profile, issue_id, linear_session_id, activity_id)).fetchone()
            if existing:
                return dict(existing)
            work = db.execute("SELECT * FROM work WHERE issue_id=?", (issue_id,)).fetchone()
            if not work or work["origin"] != "chat":
                return None
            if stamp:
                db.execute("UPDATE work SET last_updated_at=MAX(last_updated_at, ?), stop_requested_at=? "
                           "WHERE issue_id=?", (stamp, stamp, issue_id))
            status_payload = {"issue_id": issue_id, "state": "blocked", "session_key": work["owner_ref"],
                              "enqueued_at": at}
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, 'status', ?, ?)",
                       (str(uuid.uuid4()), json.dumps(status_payload), at))
            if not activity_id or work["linear_session_id"] != linear_session_id or \
                    not isinstance(work["run_generation"], int) or work["run_generation"] < 1:
                error_payload = {"issue_id": issue_id, "session_id": linear_session_id,
                                 "content": {"type": "error", "body": "Stop recorded, but this chat turn has no "
                                             "verified run binding. Cancellation is unsupported; stop it in chat "
                                             "and reconcile external effects."},
                                 "session_key": work["owner_ref"], "enqueued_at": at}
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, 'activity', ?, ?)",
                           (str(uuid.uuid4()), json.dumps(error_payload), at))
                return None
            stop_id = str(uuid.uuid4())
            db.execute("INSERT INTO chat_stop (id, profile, issue_id, linear_session_id, source_activity_id, "
                       "session_key, run_generation) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (stop_id, profile, issue_id, linear_session_id, activity_id,
                        work["owner_ref"], work["run_generation"]))
            return dict(db.execute("SELECT * FROM chat_stop WHERE id=?", (stop_id,)).fetchone())

    def stop_intents(self) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM chat_stop ORDER BY rowid").fetchall()
        return [dict(row) for row in rows]

    def stop_result(self, stop_id: str, status: str, completion: str, *, at: float) -> None:
        with self._tx() as db:
            row = db.execute("SELECT * FROM chat_stop WHERE id=?", (stop_id,)).fetchone()
            if not row:
                return
            db.execute("UPDATE chat_stop SET status=?, worker_completion=? WHERE id=?",
                       (status, completion, stop_id))
            detail = {"accepted": "Stop request accepted", "unknown": "Stop result unknown; cancellation unconfirmed",
                      "stale": "Saved chat run is stale; cancellation unconfirmed",
                      "not_running": "Saved chat run is not running; cancellation unconfirmed",
                      "unsupported": "Chat cancellation unsupported"}[status]
            body = (f"{detail}; worker {completion}; external effects unknown. "
                    "Keep this issue Blocked until a new instruction explicitly resumes it.")
            payload = {"issue_id": row["issue_id"], "session_id": row["linear_session_id"],
                       "content": {"type": "response" if status == "accepted" else "error", "body": body},
                       "session_key": row["session_key"], "enqueued_at": at}
            db.execute("INSERT OR IGNORE INTO outbox (id, kind, payload, next_at) VALUES (?, 'activity', ?, ?)",
                       (stop_id, json.dumps(payload), at))
            if status == "accepted" and completion == "completed" and row["status"] == "accepted" and \
                    not row["completion_activity_id"]:
                follow_id = str(uuid.uuid4())
                payload["content"] = {"type": "response", "body": "Stopped chat worker completed; external effects unknown. "
                                      "Reconcile before resuming this issue."}
                db.execute("UPDATE chat_stop SET completion_activity_id=? WHERE id=?", (follow_id, stop_id))
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, 'activity', ?, ?)",
                           (follow_id, json.dumps(payload), at))
            if status == "accepted" and completion == "unknown" and row["status"] == "accepted" and \
                    row["worker_completion"] == "pending" and not row["uncertainty_activity_id"]:
                follow_id = str(uuid.uuid4())
                payload["content"] = {"type": "error", "body": "Chat worker completion is now unknown; "
                                      "cancellation and external effects are unconfirmed. Reconcile before resuming."}
                db.execute("UPDATE chat_stop SET uncertainty_activity_id=? WHERE id=?", (follow_id, stop_id))
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, 'activity', ?, ?)",
                           (follow_id, json.dumps(payload), at))

    def update(self, issue_id: str, **fields: Any) -> None:
        cols = ", ".join(f"{name} = ?" for name in fields)
        with self._tx() as db:
            db.execute(f"UPDATE work SET {cols} WHERE issue_id = ?", (*fields.values(), issue_id))

    def delete(self, issue_id: str) -> None:
        with self._tx() as db:
            db.execute("DELETE FROM work WHERE issue_id = ?", (issue_id,))

    def complete_resume(self, issue_id: str, stamp: float, writes: list[tuple[str, dict[str, Any]]], *, at: float) -> None:
        """Clear the transition intent only with its durable acknowledgement and status."""
        with self._tx() as db:
            if not db.execute("SELECT 1 FROM work WHERE issue_id=? AND pending_resume IS NOT NULL", (issue_id,)).fetchone():
                return
            for kind, payload in writes:
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                           (str(uuid.uuid4()), kind, json.dumps({**payload, "enqueued_at": at}), at))
            db.execute("UPDATE work SET pending_resume=NULL, last_updated_at=MAX(last_updated_at, ?), "
                       "resume_fence_at=CASE WHEN origin='kanban' OR stop_requested_at>0 THEN ? ELSE resume_fence_at END, "
                       "stop_requested_at=0 WHERE issue_id=?", (stamp, stamp, issue_id))

    def followup_captured(self, marker: str) -> bool:
        with self._tx() as db:
            return bool(db.execute("SELECT 1 FROM outbox WHERE kind='activity' AND "
                                   "json_extract(payload, '$.followup_marker')=? LIMIT 1", (marker,)).fetchone())

    def finish(self, issue_id: str, writes: list[tuple[str, dict[str, Any]]], *, at: float) -> bool:
        """Capture terminal Linear writes before forgetting work, in one durable commit."""
        with self._tx() as db:
            work = db.execute("SELECT * FROM work WHERE issue_id = ?", (issue_id,)).fetchone()
            if not work:
                return False
            status_id = str(uuid.uuid4())
            for kind, payload in writes:
                payload = {**payload, "work_owner": work["ownership_id"]}
                row_id = status_id if kind == "status" else str(uuid.uuid4())
                if kind != "status":
                    payload = {**payload, "requires_status_id": status_id}
                if kind == "project_update":
                    payload["terminal_lines"] = {ident: status_id for ident in payload["lines"]}
                    existing = db.execute("SELECT * FROM outbox WHERE kind = 'project_update' AND "
                                          "json_extract(payload, '$.session_id') = ? AND "
                                          "json_extract(payload, '$.project_id') IS ? "
                                          "ORDER BY CASE state WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, "
                                          "rowid DESC LIMIT 1",
                                          (payload["session_id"], payload["project_id"])).fetchone()
                    if existing:
                        prior = json.loads(existing["payload"])
                        if existing["state"] in ("pending", "failed") and not prior.get("frozen"):
                            prior["lines"].update(payload["lines"])
                            for key in ("line_issues", "terminal_lines"):
                                prior.setdefault(key, {}).update(payload.get(key, {}))
                            prior.update({key: value for key, value in payload.items()
                                          if key not in ("lines", "line_issues", "terminal_lines")})
                            prior["enqueued_at"] = at
                            db.execute("UPDATE outbox SET payload = ?, next_at = MAX(next_at, ?) WHERE id = ?",
                                       (json.dumps(prior), at + float(payload.get("quiet", 0)), existing["id"]))
                        continue
                body = json.dumps({**payload, "enqueued_at": payload.get("enqueued_at", at)})
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                           (row_id, kind, body, at + float(payload.get("quiet", 0))))
            db.execute("DELETE FROM work WHERE issue_id = ?", (issue_id,))
            return True

    def capture_event(self, issue_id: str, event_id: int, writes: list[tuple[str, dict[str, Any]]],
                      *, at: float, forget: bool = False) -> bool:
        """Advance our Kanban cursor with its Linear writes, independently of core's claim cursor."""
        with self._tx() as db:
            row = db.execute("SELECT last_event_id FROM work WHERE issue_id = ?", (issue_id,)).fetchone()
            if not row or event_id <= row["last_event_id"]:
                return False
            for kind, payload in writes:
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                           (str(uuid.uuid4()), kind, json.dumps({**payload, "enqueued_at": at}), at))
            if forget:
                db.execute("DELETE FROM work WHERE issue_id = ?", (issue_id,))
            else:
                db.execute("UPDATE work SET last_event_id = ? WHERE issue_id = ?", (event_id, issue_id))
            return True

    def active(self, origin: str | None = None) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM work WHERE ? IS NULL OR origin = ?", (origin, origin)).fetchall()
        return [dict(row) for row in rows]

    def captured_task_activities(self, issue_id: str, task_id: str, session_id: str) -> Counter[tuple[str, str]]:
        """Count captured alerts; legacy rows used issue and session without a task id."""
        with self._tx() as db:
            rows = db.execute(
                "SELECT json_extract(payload, '$.content.type'), json_extract(payload, '$.content.body'), "
                "COUNT(*) FROM outbox WHERE kind = 'activity' AND json_extract(payload, '$.issue_id') = ? "
                "AND (json_extract(payload, '$.task_id') = ? OR "
                "(json_extract(payload, '$.task_id') IS NULL AND "
                "json_extract(payload, '$.session_id') = ? AND "
                "(state IN ('sent', 'pending') OR (state = 'failed' AND attempts > 0)))) GROUP BY 1, 2",
                (issue_id, task_id, session_id)).fetchall()
        return Counter({(kind, body): count for kind, body, count in rows})

    def terminal_captured(self, task_id: str) -> bool:
        with self._tx() as db:
            return bool(db.execute("SELECT 1 FROM outbox WHERE kind='status' AND "
                                   "json_extract(payload, '$.task_id')=? AND "
                                   "json_extract(payload, '$.terminal')=1 LIMIT 1", (task_id,)).fetchone())

    # -- outbox -----------------------------------------------------------
    def enqueue(self, kind: str, payload: dict[str, Any], *, at: float | None = None) -> str:
        row_id = str(uuid.uuid4())
        now = time.time() if at is None else at
        body = json.dumps({**payload, "enqueued_at": payload.get("enqueued_at", now)})
        with self._tx() as db:
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)", (row_id, kind, body, now))
        return row_id

    def enqueue_once(self, kind: str, payload: dict[str, Any], marker: str, *, at: float) -> str:
        """Capture an ingress failure alert once across retries and process restarts."""
        with self._tx() as db:
            existing = db.execute("SELECT id FROM outbox WHERE json_extract(payload, '$.parked_delivery') = ?",
                                  (marker,)).fetchone()
            if existing:
                return str(existing["id"])
            row_id = str(uuid.uuid4())
            body = json.dumps({**payload, "parked_delivery": marker, "enqueued_at": at})
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                       (row_id, kind, body, at))
            return row_id

    def due(self, now: float) -> list[dict[str, Any]]:
        """Oldest pending row per issue, if due: writes for one issue go out in order."""
        def eligible(alias: str) -> str:
            dependency = f"json_extract({alias}.payload, '$.requires_status_id')"
            return (f"(NOT EXISTS (SELECT 1 FROM outbox h WHERE h.id={dependency} "
                    "AND json_extract(h.payload, '$.reconcile_required')=1 AND "
                    f"({alias}.kind != 'project_update' OR json_extract({alias}.payload, '$.issue_id')="
                    "json_extract(h.payload, '$.issue_id'))) AND "
                    f"({alias}.kind = 'project_update' OR COALESCE({dependency}, '') = '' OR "
                    f"EXISTS (SELECT 1 FROM outbox s WHERE s.id = {dependency} "
                    "AND (s.state = 'sent' OR (s.state = 'failed' AND (s.attempts = 0 OR EXISTS ("
                    "SELECT 1 FROM outbox l WHERE l.kind = 'status' AND l.state = 'sent' AND l.rowid > s.rowid "
                    "AND json_extract(l.payload, '$.issue_id') = json_extract(s.payload, '$.issue_id'))))))))")

        with self._tx() as db:
            rows = db.execute(
                "SELECT o.* FROM outbox o WHERE o.state = 'pending' AND " + eligible("o") + " AND o.rowid = ("
                " SELECT MIN(p.rowid) FROM outbox p WHERE p.state = 'pending' AND " + eligible("p") +
                " AND json_extract(p.payload, '$.issue_id') IS json_extract(o.payload, '$.issue_id')"
                ") AND o.next_at <= ? ORDER BY o.rowid", (now,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def pending(self, issue_id: str | None = None) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM outbox WHERE state = 'pending' AND (? IS NULL OR "
                              "json_extract(payload, '$.issue_id') = ?) ORDER BY rowid", (issue_id, issue_id)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def unreported_failed(self) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM outbox WHERE state = 'failed' AND "
                              "COALESCE(json_extract(payload, '$.reported'), 0) = 0 ORDER BY rowid").fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def mark(self, row_id: str, state: str) -> None:
        with self._tx() as db:
            db.execute("UPDATE outbox SET state = ? WHERE id = ?", (state, row_id))

    def mark_sent(self, row_id: str, applied: bool) -> None:
        with self._tx() as db:
            db.execute("UPDATE outbox SET state = 'sent', payload = json_set(payload, '$.applied', ?) WHERE id = ?",
                       (int(applied), row_id))

    def terminal_status_applied(self, row_id: str) -> bool:
        with self._tx() as db:
            row = db.execute("SELECT state, kind, json_extract(payload, '$.applied') FROM outbox WHERE id = ?",
                             (row_id,)).fetchone()
        return bool(row and row["state"] == "sent" and row["kind"] == "status" and row[2] == 1)

    def terminal_text(self, status_id: str) -> str:
        with self._tx() as db:
            rows = db.execute("SELECT COALESCE(json_extract(payload, '$.body'), "
                              "json_extract(payload, '$.content.body'), '') FROM outbox WHERE "
                              "json_extract(payload, '$.requires_status_id')=?", (status_id,)).fetchall()
        return " ".join(row[0] for row in rows)

    def superseded(self, row: dict[str, Any]) -> bool:
        """A later local owner fences a predecessor, even after the successor finishes."""
        payload = row["payload"]
        owner = payload.get("work_owner")
        with self._tx() as db:
            work = db.execute("SELECT * FROM work WHERE issue_id=?", (payload["issue_id"],)).fetchone()
            if work:
                return not owner or work["ownership_id"] != owner  # terminal capture already ended the old work
            return bool(db.execute(
                "SELECT 1 FROM outbox WHERE kind='status' AND rowid > (SELECT rowid FROM outbox WHERE id=?) "
                "AND json_extract(payload, '$.issue_id')=? AND json_extract(payload, '$.work_owner') IS NOT NULL "
                "AND (? IS NULL OR json_extract(payload, '$.work_owner') != ?) LIMIT 1",
                (row["id"], payload["issue_id"], owner, owner)).fetchone())

    def hold_terminal(self, row_id: str) -> None:
        with self._tx() as db:
            db.execute("UPDATE outbox SET attempts=MAX(attempts, 1), "
                       "payload=json_set(payload, '$.reconcile_required', 1) WHERE id=?", (row_id,))

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
                "UPDATE outbox SET state = 'pending', next_at = ? WHERE state = 'failed' AND attempts > 0 "
                "AND COALESCE(json_extract(payload, '$.reconcile_required'), 0)=0 AND NOT ("
                " kind = 'status' AND EXISTS (SELECT 1 FROM outbox l WHERE l.kind = 'status' AND l.rowid > outbox.rowid"
                " AND json_extract(l.payload, '$.issue_id') = json_extract(outbox.payload, '$.issue_id')))",
                (now,)).rowcount

    def project_update(self, session_id: str, project_id: str) -> dict[str, Any] | None:
        with self._tx() as db:
            row = db.execute("SELECT * FROM outbox WHERE kind = 'project_update' AND "
                             "json_extract(payload, '$.session_id') = ? AND "
                             "json_extract(payload, '$.project_id') IS ? "
                             "ORDER BY CASE state WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, "
                             "rowid DESC LIMIT 1",
                             (session_id, project_id)).fetchone()
        return {**dict(row), "payload": json.loads(row["payload"])} if row else None

    def outbox_row(self, row_id: str) -> dict[str, Any] | None:
        with self._tx() as db:
            row = db.execute("SELECT * FROM outbox WHERE id = ?", (row_id,)).fetchone()
        return {**dict(row), "payload": json.loads(row["payload"])} if row else None

    def queue_project_update(self, payload: dict[str, Any], *, due: float, quiet: bool) -> None:
        """Merge a session's update atomically with terminal capture in finish()."""
        with self._tx() as db:
            row = db.execute("SELECT * FROM outbox WHERE kind = 'project_update' AND "
                             "json_extract(payload, '$.session_id') = ? AND "
                             "json_extract(payload, '$.project_id') IS ? "
                             "ORDER BY CASE state WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, "
                             "rowid DESC LIMIT 1",
                             (payload["session_id"], payload["project_id"])).fetchone()
            if row:
                prior = json.loads(row["payload"])
                if row["state"] not in ("pending", "failed") or prior.get("frozen"):
                    return
                prior["lines"].update(payload["lines"])
                prior.setdefault("line_issues", {}).update(payload.get("line_issues", {}))
                if payload.get("terminal"):
                    prior["terminal"] = True
                    prior["owner_issue_id"] = payload.get("owner_issue_id")
                    prior["requires_status_id"] = payload.get("requires_status_id")
                    prior.setdefault("terminal_lines", {}).update(payload.get("terminal_lines", {}))
                else:
                    terminal_lines = prior.setdefault("terminal_lines", {})
                    for ident in payload["lines"]:
                        terminal_lines.pop(ident, None)
                    if terminal_lines:
                        ident, status_id = next(reversed(terminal_lines.items()))
                        prior["terminal"] = True
                        prior["owner_issue_id"] = prior["line_issues"].get(ident, "")
                        prior["requires_status_id"] = status_id
                    else:
                        prior["terminal"] = False
                        prior["owner_issue_id"] = ""
                        prior["requires_status_id"] = ""
                prior["resolve"] = payload.get("resolve") or prior.get("resolve")
                next_at = max(due, float(row["next_at"])) if quiet else due
                db.execute("UPDATE outbox SET payload = ?, next_at = ? WHERE id = ?",
                           (json.dumps(prior), next_at, row["id"]))
            else:
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, 'project_update', ?, ?)",
                           (str(uuid.uuid4()), json.dumps({**payload, "enqueued_at": due}), due))

    def freeze_project_update(self, row_id: str, expected: dict[str, Any], now: float,
                              body: str, project_id: str | None) -> bool | None:
        """Freeze the preflighted body only if it is still unchanged and due."""
        with self._tx() as db:
            row = db.execute("SELECT * FROM outbox WHERE id = ? AND kind = 'project_update' AND state = 'pending'",
                             (row_id,)).fetchone()
            if not row:
                return False
            if row["next_at"] > now:
                return False
            payload = json.loads(row["payload"])
            if payload != expected:
                return None
            if not payload.get("frozen"):
                payload["frozen"] = True
                payload["send_body"] = body
                payload["send_project_id"] = project_id
                db.execute("UPDATE outbox SET payload = ? WHERE id = ?", (json.dumps(payload), row_id))
        return True

    def split_project_update(self, row_id: str, expected: dict[str, Any], now: float,
                             waiting: set[str]) -> bool | None:
        """Keep status-blocked lines pending while the eligible lines use this client id."""
        with self._tx() as db:
            row = db.execute("SELECT payload, next_at FROM outbox WHERE id=? AND kind='project_update' "
                             "AND state='pending'", (row_id,)).fetchone()
            if not row or row["next_at"] > now:
                return False
            if json.loads(row["payload"]) != expected:
                return None
            # An older deferred row has no followup marker. Its sibling proves the
            # session/project already split, so it must wait rather than split again.
            sibling = db.execute("SELECT 1 FROM outbox WHERE kind='project_update' AND id != ? AND "
                                 "json_extract(payload, '$.session_id') = ? AND "
                                 "json_extract(payload, '$.project_id') IS ? LIMIT 1",
                                 (row_id, expected["session_id"], expected.get("project_id"))).fetchone()
            if sibling:
                return False
            if not waiting or waiting == set(expected["lines"]):
                return False
            ready, deferred = dict(expected), dict(expected)
            deferred["followup"] = True
            for part, idents in ((ready, set(expected["lines"]) - waiting), (deferred, waiting)):
                part["lines"] = {ident: expected["lines"][ident] for ident in idents}
                part["line_issues"] = {ident: value for ident, value in expected.get("line_issues", {}).items()
                                       if ident in idents}
                part["terminal_lines"] = {ident: value for ident, value in expected.get("terminal_lines", {}).items()
                                          if ident in idents}
                part["resolve"] = next(iter(part["line_issues"].values()), expected.get("resolve"))
                part["terminal"] = bool(part["terminal_lines"])
                ident = next(iter(part["terminal_lines"]), None)
                part["owner_issue_id"] = part["line_issues"].get(ident, "") if ident else ""
                part["requires_status_id"] = part["terminal_lines"].get(ident, "") if ident else ""
            db.execute("UPDATE outbox SET payload=? WHERE id=?", (json.dumps(ready), row_id))
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, 'project_update', ?, ?)",
                       (str(uuid.uuid4()), json.dumps(deferred), row["next_at"]))
        return True

    def drop_unpublished_update(self, row_id: str, expected: dict[str, Any], now: float) -> bool | None:
        """Forget an empty batch only if no valid line merged during preflight."""
        with self._tx() as db:
            row = db.execute("SELECT payload, next_at FROM outbox WHERE id = ? AND kind = 'project_update' "
                             "AND state = 'pending'", (row_id,)).fetchone()
            if not row or row["next_at"] > now:
                return False
            if json.loads(row["payload"]) != expected:
                return None
            db.execute("DELETE FROM outbox WHERE id = ?", (row_id,))
        return True

    def pending_claim(self, issue_id: str) -> bool:
        with self._tx() as db:
            return bool(db.execute(
                "SELECT 1 FROM outbox c WHERE c.kind = 'status' AND "
                "json_extract(c.payload, '$.issue_id') = ? AND json_extract(c.payload, '$.claim') = 1 AND "
                "(c.state = 'pending' OR (c.state = 'failed' AND c.attempts > 0 AND NOT EXISTS ("
                "SELECT 1 FROM outbox l WHERE l.kind = 'status' AND l.rowid > c.rowid AND "
                "json_extract(l.payload, '$.issue_id') = ?))) LIMIT 1", (issue_id, issue_id)).fetchone())

    def status_pending(self, row_id: str) -> bool:
        with self._tx() as db:
            return bool(db.execute("SELECT 1 FROM outbox WHERE id = ? AND kind = 'status' AND "
                                   "(state = 'pending' OR (state = 'failed' AND attempts > 0))",
                                   (row_id,)).fetchone())

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
                       "state = 'pending' AND COALESCE(json_extract(payload, '$.frozen'), 0) = 0 AND "
                       "json_extract(payload, '$.session_id') = ?", (next_at, session_id))
