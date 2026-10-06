"""Profile-local work, permanent scope fences, and an idempotent Linear outbox."""
from __future__ import annotations
import json
import sqlite3
import threading
import time
import uuid
from collections import Counter
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator
MIN_BACKOFF, MAX_BACKOFF, GIVE_UP_AFTER = 60.0, 3600.0, 86_400.0
SCHEMA = """
CREATE TABLE IF NOT EXISTS work (
  issue_id TEXT PRIMARY KEY, origin TEXT NOT NULL, owner_ref TEXT NOT NULL,
  task_id TEXT, project_id TEXT, last_updated_at REAL NOT NULL DEFAULT 0,
  linear_session_id TEXT, panel_note TEXT, stop_requested_at REAL NOT NULL DEFAULT 0,
  last_event_id INTEGER NOT NULL DEFAULT 0, resume_fence_at REAL NOT NULL DEFAULT 0,
  run_generation INTEGER, ownership_id TEXT, pending_resume TEXT, release_pending INTEGER NOT NULL DEFAULT 0);
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
CREATE TABLE IF NOT EXISTS retirement (
  task_id TEXT PRIMARY KEY, issue_id TEXT NOT NULL, snapshot TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS specialist_scope_fence (
  issue_id TEXT PRIMARY KEY, reason TEXT NOT NULL, fenced_at REAL NOT NULL);
"""
class Store:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self._local = threading.local()
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
            if "release_pending" not in columns:
                db.execute("ALTER TABLE work ADD COLUMN release_pending INTEGER NOT NULL DEFAULT 0")
            if "ownership_id" not in columns:
                db.execute("ALTER TABLE work ADD COLUMN ownership_id TEXT")
    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        active = getattr(self._local, "db", None)
        if active is not None:
            yield active
            return
        with closing(sqlite3.connect(self.path, timeout=90, isolation_level=None)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            self._local.db = db
            try:
                yield db
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
            finally:
                self._local.db = None
    @staticmethod
    def _admitted(db: sqlite3.Connection, *issue_ids: str) -> bool:
        return all(not db.execute("SELECT 1 FROM specialist_scope_fence WHERE issue_id=?", (ident,)).fetchone()
                   for ident in issue_ids if ident)
    @staticmethod
    def _unresolved_terminal(db: sqlite3.Connection, issue_id: str, *, except_id: str = "") -> bool:
        """An attempted terminal write has an unknown effect until an explicit durable resolution."""
        if not issue_id or issue_id.startswith("update:"):
            return False
        rows = db.execute("SELECT id, state, attempts, payload FROM outbox WHERE kind='status' AND "
                          "json_extract(payload, '$.issue_id')=?", (issue_id,)).fetchall()
        for row in rows:
            if row["id"] == except_id:
                continue
            payload = json.loads(row["payload"])
            if not payload.get("terminal") or payload.get("reconciliation"):
                continue
            if (payload.get("reconcile_required") or
                    (row["state"] in ("pending", "failed") and
                     (payload.get("write_started") is True or
                      (row["attempts"] > 0 and "write_started" not in payload)))):
                return True
        return False
    @classmethod
    def _effect_admitted(cls, db: sqlite3.Connection, *issue_ids: str, except_id: str = "") -> bool:
        return cls._admitted(db, *issue_ids) and all(
            not cls._unresolved_terminal(db, ident, except_id=except_id) for ident in issue_ids)
    def issue_reconciliation_blocked(self, issue_id: str, *, except_id: str = "") -> bool:
        with self._tx() as db:
            return self._unresolved_terminal(db, issue_id, except_id=except_id)
    @contextmanager
    def guard_issue_work(self, issue_id: str) -> Iterator[bool]:
        with self._tx() as db:
            yield self._effect_admitted(db, issue_id)
    def admit_mutation(self, row_id: str, *, terminal: bool = False) -> bool:
        """Linearize scope admission and the terminal send marker in one short commit."""
        if getattr(self._local, "db", None) is not None:
            raise RuntimeError("mutation admission requires its own commit")
        with self._tx() as db:
            row = db.execute("SELECT kind, state, payload FROM outbox WHERE id=?", (row_id,)).fetchone()
            if not row or row["state"] != "pending":
                return False
            payload = json.loads(row["payload"])
            if not self._effect_admitted(db, *self._targets(payload), except_id=row_id):
                return False
            if terminal:
                return bool(db.execute("UPDATE outbox SET payload=json_set(payload, '$.write_started', json('true')) "
                                       "WHERE id=?", (row_id,)).rowcount)
            return True
    def reconcile_terminal(self, row_id: str, *, outcome: str, evidence: str, at: float) -> bool:
        """Record an operator's verified remote outcome; keep the attempted receipt intact."""
        if outcome not in ("applied", "not_applied") or not evidence.strip():
            raise ValueError("terminal reconciliation requires a verified outcome and evidence")
        with self._tx() as db:
            row = db.execute("SELECT kind, state, attempts, payload FROM outbox WHERE id=?", (row_id,)).fetchone()
            if not row or row["kind"] != "status": return False
            payload = json.loads(row["payload"])
            attempted = payload.get("reconcile_required") or (row["state"] in ("pending", "failed") and
                (payload.get("write_started") is True or (row["attempts"] > 0 and "write_started" not in payload)))
            if not payload.get("terminal") or payload.get("reconciliation") or not attempted: return False
            payload["reconciliation"] = {"outcome": outcome, "evidence": evidence.strip(), "at": at}
            payload["reconcile_required"] = True
            db.execute("UPDATE outbox SET payload=?, state='failed' WHERE id=?", (json.dumps(payload), row_id))
            return True
    @staticmethod
    def _targets(payload: dict[str, Any]) -> set[str]:
        return {ident for ident in (payload.get("owner_issue_id"), payload.get("resolve"),
                                    *(payload.get("line_issues") or {}).values(),
                                    payload.get("issue_id") if not str(payload.get("issue_id", "")).startswith("update:")
                                    else None) if ident}
    def _row_admitted(self, db: sqlite3.Connection, row_id: str, state: str | None = None) -> bool:
        row = db.execute("SELECT state, payload FROM outbox WHERE id=?", (row_id,)).fetchone()
        return bool(row and (state is None or row["state"] == state) and
                    self._admitted(db, *self._targets(json.loads(row["payload"]))))
    @contextmanager
    def guard_outbox(self, row_id: str, state: str = "pending") -> Iterator[bool]:
        with self._tx() as db:
            yield self._row_admitted(db, row_id, state)
    @contextmanager
    def guard_issue(self, issue_id: str) -> Iterator[bool]:
        with self._tx() as db:
            yield self._admitted(db, issue_id)
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
    def retired(self, issue_id: str | None = None) -> list[dict]:
        with self._tx() as db:
            return [dict(row) for row in db.execute("SELECT * FROM retirement WHERE ? IS NULL OR issue_id=?", (issue_id, issue_id))]
    def retire(self, issue_id: str, task_id: str, snapshot: dict) -> None:
        with self._tx() as db:
            db.execute("INSERT OR REPLACE INTO retirement VALUES (?, ?, ?)",
                       (task_id, issue_id, json.dumps(snapshot)))
    def clear_retirement(self, task_id: str, snapshot: str) -> None:
        with self._tx() as db:
            db.execute("DELETE FROM retirement WHERE task_id=? AND snapshot=?", (task_id, snapshot))
    def chat_closeout_ms(self, issue_id: str) -> float:
        """Existing terminal receipts fence pre-closeout chat delegation echoes and prompts."""
        with self._tx() as db:
            row = db.execute("SELECT MAX(json_extract(payload, '$.enqueued_at')) FROM outbox WHERE kind='status' "
                             "AND json_extract(payload, '$.issue_id')=? AND json_extract(payload, '$.terminal')=1 "
                             "AND json_extract(payload, '$.session_key') IS NOT NULL", (issue_id,)).fetchone()
            return float(row[0] or 0) * 1000
    def put(self, issue_id: str, origin: str, owner_ref: str, *, task_id: str | None = None,
            project_id: str | None = None, last_updated_at: float = 0.0,
            run_generation: int | None = None) -> bool:
        with self._tx() as db:
            if not self._effect_admitted(db, issue_id) or self.retired(issue_id): return False
            db.execute("INSERT OR REPLACE INTO work (issue_id, origin, owner_ref, task_id, project_id, "
                       "last_updated_at, run_generation, ownership_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (issue_id, origin, owner_ref, task_id, project_id, last_updated_at, run_generation, str(uuid.uuid4())))
            return True
    def capture_chat_stop(self, issue_id: str, linear_session_id: str, activity_id: str,
                          profile: str, *, at: float, stamp: float = 0.0) -> dict[str, Any] | None:
        """Commit the exact target and stable Linear activity id before any core request."""
        with self._tx() as db:
            if not self._admitted(db, issue_id): return None
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
            if not row or not self._admitted(db, row["issue_id"]):
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
    def update(self, issue_id: str, **fields: Any) -> bool:
        cols = ", ".join(f"{name} = ?" for name in fields)
        with self._tx() as db:
            if not self._admitted(db, issue_id): return False
            admits_work = "owner_ref" in fields or "run_generation" in fields or fields.get("stop_requested_at") == 0
            if admits_work and not self._effect_admitted(db, issue_id): return False
            db.execute(f"UPDATE work SET {cols} WHERE issue_id = ?", (*fields.values(), issue_id))
            return True
    def delete(self, issue_id: str) -> bool:
        with self._tx() as db:
            if not self._admitted(db, issue_id): return False
            db.execute("DELETE FROM work WHERE issue_id = ?", (issue_id,))
            return True
    def scope_fenced(self, issue_id: str) -> bool:
        with self._tx() as db:
            return db.execute("SELECT 1 FROM specialist_scope_fence WHERE issue_id = ?",
                              (issue_id,)).fetchone() is not None
    def activate_task(self, issue_id: str, task_id: str, action: Callable[[], bool]) -> bool:
        with self._tx() as db:
            if not self._effect_admitted(db, issue_id) or not db.execute(
                    "SELECT 1 FROM work WHERE issue_id=? AND task_id=?", (issue_id, task_id)).fetchone(): return False
            return action()
    def complete_resume(self, issue_id: str, stamp: float, writes: list[tuple[str, dict[str, Any]]], *, at: float) -> bool:
        with self._tx() as db:
            work = db.execute("SELECT * FROM work WHERE issue_id=? AND pending_resume IS NOT NULL", (issue_id,)).fetchone()
            if not work or not self._effect_admitted(db, issue_id): return False
            if json.loads(work["pending_resume"]).get("stamp") != stamp or (
                    work["origin"] == "kanban" and work["stop_requested_at"] and
                    stamp <= work["stop_requested_at"]): return False
            if any(not self._effect_admitted(db, *self._targets(payload)) for _, payload in writes): return False
            for kind, payload in writes:
                body = json.dumps({**payload, "work_owner": work["ownership_id"], "enqueued_at": at})
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                           (str(uuid.uuid4()), kind, body, at))
            db.execute("UPDATE work SET pending_resume=NULL, stop_requested_at=0, "
                       "last_updated_at=MAX(last_updated_at, ?), "
                       "resume_fence_at=CASE WHEN origin='kanban' OR stop_requested_at>0 THEN ? ELSE resume_fence_at END "
                       "WHERE issue_id=?",
                       (stamp, stamp, issue_id))
            return True
    def followup_captured(self, marker: str) -> bool:
        with self._tx() as db:
            return bool(db.execute("SELECT 1 FROM outbox WHERE kind='activity' AND "
                                   "(json_extract(payload, '$.followup_marker')=? OR EXISTS "
                                   "(SELECT 1 FROM json_each(outbox.payload, '$.followup_markers') WHERE value=?)) "
                                   "LIMIT 1", (marker, marker)).fetchone())
    def fence_scope(self, issue_id: str, reason: str, *, at: float) -> None:
        """Persist a permanent authorization denial without deleting work or its evidence."""
        with self._tx() as db:
            db.execute("INSERT OR IGNORE INTO specialist_scope_fence (issue_id, reason, fenced_at) "
                       "VALUES (?, ?, ?)", (issue_id, reason[:500], at))
    def finish(self, issue_id: str, writes: list[tuple[str, dict[str, Any]]], *, at: float,
               release: bool = False) -> bool:
        """Capture terminal Linear writes before forgetting work, in one durable commit."""
        with self._tx() as db:
            if not self._effect_admitted(db, issue_id): return False
            work = db.execute("SELECT ownership_id FROM work WHERE issue_id = ?", (issue_id,)).fetchone()
            if not work: return False
            if any(not self._admitted(db, *self._targets(payload)) for _, payload in writes): return False
            for kind, payload in writes:
                if kind == "project_update":
                    prior = db.execute("SELECT payload, state FROM outbox WHERE kind='project_update' AND "
                                       "json_extract(payload, '$.session_id')=? AND "
                                       "json_extract(payload, '$.project_id') IS ? ORDER BY "
                                       "CASE state WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, rowid DESC LIMIT 1",
                                       (payload["session_id"], payload["project_id"])).fetchone()
                    if prior and prior["state"] in ("pending", "failed") and not json.loads(prior["payload"]).get("frozen") \
                            and not self._admitted(db, *self._targets(json.loads(prior["payload"]))): return False
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
            if release:
                db.execute("UPDATE work SET release_pending=1 WHERE issue_id=?", (issue_id,))
            else:
                db.execute("DELETE FROM work WHERE issue_id = ?", (issue_id,))
            return True
    def capture_event(self, issue_id: str, event_id: int, writes: list[tuple[str, dict[str, Any]]],
                      *, at: float, forget: bool = False) -> bool:
        """Advance our Kanban cursor with its Linear writes, independently of core's claim cursor."""
        with self._tx() as db:
            if not self._admitted(db, issue_id) or (writes and not self._effect_admitted(db, issue_id)):
                return False
            row = db.execute("SELECT last_event_id FROM work WHERE issue_id = ?", (issue_id,)).fetchone()
            if not row or event_id <= row["last_event_id"]:
                return False
            if any(not self._admitted(db, *self._targets(payload)) for _, payload in writes): return False
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
    def superseded(self, row: dict[str, Any]) -> bool:
        payload, owner = row["payload"], row["payload"].get("work_owner")
        with self._tx() as db:
            work = db.execute("SELECT ownership_id FROM work WHERE issue_id=?", (payload["issue_id"],)).fetchone()
            if work: return not owner or work["ownership_id"] != owner
            return bool(db.execute("SELECT 1 FROM outbox WHERE kind='status' AND "
                                   "rowid>(SELECT rowid FROM outbox WHERE id=?) AND "
                                   "json_extract(payload, '$.issue_id')=? AND "
                                   "json_extract(payload, '$.work_owner') IS NOT NULL AND "
                                   "(? IS NULL OR json_extract(payload, '$.work_owner')!=?) LIMIT 1",
                                   (row["id"], payload["issue_id"], owner, owner)).fetchone())
    def hold_terminal(self, row_id: str) -> None:
        with self._tx() as db:
            db.execute("UPDATE outbox SET attempts=MAX(attempts, 1), "
                       "payload=json_set(payload, '$.reconcile_required', 1) WHERE id=?", (row_id,))
    # -- outbox -----------------------------------------------------------
    def enqueue(self, kind: str, payload: dict[str, Any], *, at: float | None = None) -> str:
        row_id = str(uuid.uuid4())
        now = time.time() if at is None else at
        with self._tx() as db:
            if not self._effect_admitted(db, *self._targets(payload)): return ""
            work = db.execute("SELECT ownership_id FROM work WHERE issue_id=?", (payload.get("issue_id"),)).fetchone()
            body = json.dumps({**payload, "work_owner": work["ownership_id"] if work else None,
                               "enqueued_at": payload.get("enqueued_at", now)})
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)", (row_id, kind, body, now))
        return row_id
    def enqueue_many(self, writes: list[tuple[str, dict[str, Any]]], *, at: float) -> bool:
        with self._tx() as db:
            if any(not self._effect_admitted(db, *self._targets(payload)) for _, payload in writes): return False
            for kind, payload in writes:
                work = db.execute("SELECT ownership_id FROM work WHERE issue_id=?", (payload.get("issue_id"),)).fetchone()
                if work: payload = {**payload, "work_owner": work["ownership_id"]}
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                           (str(uuid.uuid4()), kind, json.dumps({**payload, "enqueued_at": at}), at))
            return True
    def enqueue_once(self, kind: str, payload: dict[str, Any], marker: str, *, at: float) -> str:
        """Capture an ingress failure alert once across retries and process restarts."""
        with self._tx() as db:
            if not self._admitted(db, *self._targets(payload)): return ""
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
            if not self._row_admitted(db, row_id): return
            db.execute("UPDATE outbox SET state = ? WHERE id = ?", (state, row_id))
    def mark_sent(self, row_id: str, applied: bool) -> bool:
        with self._tx() as db:
            if not self._row_admitted(db, row_id): return False
            db.execute("UPDATE outbox SET state = 'sent', payload = json_set(payload, '$.applied', ?) WHERE id = ?",
                       (int(applied), row_id))
            if applied:
                receipt = db.execute("SELECT payload FROM outbox WHERE id=?", (row_id,)).fetchone()
                payload = json.loads(receipt["payload"])
                if payload.get("release"):
                    db.execute("DELETE FROM work WHERE issue_id=? AND ownership_id=? AND release_pending=1",
                               (payload["issue_id"], payload["work_owner"]))
            return True
    def verified_release(self, row_id: str) -> bool:
        row = self.outbox_row(row_id)
        return bool(row and row["state"] == "sent" and row["kind"] == "status"
                    and row["payload"].get("release") and row["payload"].get("applied"))
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
    def drop(self, row_id: str) -> None:
        with self._tx() as db:
            if not self._row_admitted(db, row_id): return
            db.execute("DELETE FROM outbox WHERE id = ?", (row_id,))
    def defer(self, row_id: str, until: float) -> None:
        with self._tx() as db:
            if not self._row_admitted(db, row_id): return
            db.execute("UPDATE outbox SET next_at = ? WHERE id = ?", (until, row_id))
    def retry(self, row: dict[str, Any], now: float) -> bool:
        """Back off 1 min doubling to 1 h. Returns True when the row is given up (24 h old)."""
        attempts = int(row["attempts"]) + 1
        if now - float(row["payload"].get("enqueued_at", now)) >= GIVE_UP_AFTER:
            with self._tx() as db:
                if not self._row_admitted(db, row["id"]): return False
                db.execute("UPDATE outbox SET state = 'failed', attempts = ? WHERE id = ?", (attempts, row["id"]))
            return True
        delay = min(MIN_BACKOFF * 2 ** (attempts - 1), MAX_BACKOFF)
        with self._tx() as db:
            if not self._row_admitted(db, row["id"]): return False
            db.execute("UPDATE outbox SET attempts = ?, next_at = ? WHERE id = ?", (attempts, now + delay, row["id"]))
        return False
    def revive_failed(self, now: float) -> int:
        """After any successful write, give timed-out rows one more try (same client id). A status
        write superseded by a later one for the same issue stays failed, so it cannot land stale."""
        with self._tx() as db:
            rows = db.execute(
                "SELECT id, payload FROM outbox WHERE state = 'failed' AND attempts > 0 AND "
                "COALESCE(json_extract(payload, '$.reconcile_required'), 0)=0 AND NOT ("
                " kind = 'status' AND EXISTS (SELECT 1 FROM outbox l WHERE l.kind = 'status' AND l.rowid > outbox.rowid"
                " AND json_extract(l.payload, '$.issue_id') = json_extract(outbox.payload, '$.issue_id')))",
                ).fetchall()
            revived = 0
            for row in rows:
                if self._admitted(db, *self._targets(json.loads(row["payload"]))):
                    db.execute("UPDATE outbox SET state='pending', next_at=? WHERE id=?", (now, row["id"]))
                    revived += 1
            return revived
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
    def queue_project_update(self, payload: dict[str, Any], *, due: float, quiet: bool) -> bool:
        """Merge a session's update atomically with terminal capture in finish()."""
        with self._tx() as db:
            if not self._effect_admitted(db, *self._targets(payload)): return False
            row = db.execute("SELECT * FROM outbox WHERE kind = 'project_update' AND "
                             "json_extract(payload, '$.session_id') = ? AND "
                             "json_extract(payload, '$.project_id') IS ? "
                             "ORDER BY CASE state WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, "
                             "rowid DESC LIMIT 1",
                             (payload["session_id"], payload["project_id"])).fetchone()
            if row:
                prior = json.loads(row["payload"])
                if row["state"] not in ("pending", "failed") or prior.get("frozen"):
                    return True
                if not self._admitted(db, *self._targets(prior)): return False
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
            return True
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
            if not self._admitted(db, *self._targets(payload)): return False
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
            if not self._admitted(db, *self._targets(expected)): return False
            # A sibling proves this session/project already split; wait for it.
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
            if not self._admitted(db, *self._targets(expected)): return False
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
    def has_start_ack(self, issue_id: str, task_id: str) -> bool:
        with self._tx() as db:
            return bool(db.execute("SELECT 1 FROM outbox WHERE kind='status' AND "
                                   "json_extract(payload, '$.issue_id')=? AND "
                                   "json_extract(payload, '$.task_id')=? AND "
                                   "json_extract(payload, '$.claim')=1 LIMIT 1", (issue_id, task_id)).fetchone())
    def status_pending(self, row_id: str) -> bool:
        with self._tx() as db:
            return bool(db.execute("SELECT 1 FROM outbox WHERE id = ? AND kind = 'status' AND "
                                   "(state = 'pending' OR (state = 'failed' AND attempts > 0))",
                                   (row_id,)).fetchone())
    def report(self, row_id: str) -> None:
        with self._tx() as db:
            if not self._row_admitted(db, row_id): return
            db.execute("UPDATE outbox SET payload = json_set(payload, '$.reported', 1) WHERE id = ?", (row_id,))
    def rewrite(self, row_id: str, payload: dict[str, Any], next_at: float) -> None:
        with self._tx() as db:
            if not self._row_admitted(db, row_id) or not self._admitted(db, *self._targets(payload)): return
            db.execute("UPDATE outbox SET payload = ?, next_at = ? WHERE id = ?", (json.dumps(payload), next_at, row_id))
    def delay_session_updates(self, session_id: str, next_at: float) -> None:
        """Quiet period: every turn in the session pushes its pending project updates back."""
        with self._tx() as db:
            rows = db.execute("SELECT id, payload FROM outbox WHERE kind='project_update' AND state='pending' AND "
                              "COALESCE(json_extract(payload, '$.frozen'), 0)=0 AND "
                              "json_extract(payload, '$.session_id')=?", (session_id,)).fetchall()
            for row in rows:
                if self._admitted(db, *self._targets(json.loads(row["payload"]))):
                    db.execute("UPDATE outbox SET next_at=MAX(next_at, ?) WHERE id=?", (next_at, row["id"]))
