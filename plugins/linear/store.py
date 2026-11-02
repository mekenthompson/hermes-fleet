"""Issue/task bindings, preserved legacy records and the durable Linear outbox.

The database records reporting and uncertain delivery. It never grants execution.
"""
from __future__ import annotations
import hashlib
import json
import sqlite3
import threading
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
  run_generation INTEGER, ownership_id TEXT, pending_resume TEXT, release_pending INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS outbox (
  id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
  hold_reason TEXT);
CREATE TABLE IF NOT EXISTS chat_stop (
  id TEXT PRIMARY KEY, profile TEXT NOT NULL, issue_id TEXT NOT NULL,
  linear_session_id TEXT NOT NULL, source_activity_id TEXT NOT NULL,
  session_key TEXT NOT NULL, run_generation INTEGER,
  status TEXT NOT NULL DEFAULT 'requested', worker_completion TEXT NOT NULL DEFAULT 'unknown',
  completion_activity_id TEXT, uncertainty_activity_id TEXT,
  UNIQUE(profile, issue_id, linear_session_id, source_activity_id));
CREATE TABLE IF NOT EXISTS retirement (
  task_id TEXT PRIMARY KEY, issue_id TEXT NOT NULL, snapshot TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS specialist_scope_fence (
  issue_id TEXT PRIMARY KEY, reason TEXT NOT NULL, fenced_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS chat_admissions (
  request_id TEXT PRIMARY KEY, issue_id TEXT NOT NULL UNIQUE, profile TEXT NOT NULL,
  home TEXT NOT NULL, parent_session_key TEXT NOT NULL, parent_session_id TEXT NOT NULL,
  run_generation INTEGER NOT NULL, ownership_id TEXT NOT NULL, task_id TEXT,
  state TEXT NOT NULL CHECK(state IN ('prepared','claiming','admitted','rejected','ambiguous')),
  reason_code TEXT, route TEXT);
"""
ADVISORY = ("(kind = 'activity' AND (json_extract(payload, '$.content.type') IN ('thought', 'action') OR "
            "COALESCE(json_extract(payload, '$.one_try'), 0) = 1))")
def advisory(row: dict[str, Any]) -> bool:
    """A thought, action or decline reply changes no issue: it gets one try and never alerts, revives or fences."""
    payload = row["payload"]
    return row["kind"] == "activity" and ((payload.get("content") or {}).get("type") in ("thought", "action")
                                          or payload.get("one_try") is True)


class Store:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self._local = threading.local()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.executescript(SCHEMA)
            outbox_columns = {row[1] for row in db.execute("PRAGMA table_info(outbox)")}
            if "hold_reason" not in outbox_columns:
                db.execute("ALTER TABLE outbox ADD COLUMN hold_reason TEXT")
            db.commit()
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
    def unlinked_content(kind: str, payload: dict[str, Any]) -> bool:
        """An ordinary content request carries no retained execution identity."""
        content_kind = kind in ("comment", "description") or Store.conversation_reply(kind, payload)
        return (content_kind and payload.get("content_action") is True
                and payload.get("unlinked_content") is True and not any(payload.get(key) for key in (
                    "work_owner", "session_key", "admission_id", "claim", "release", "task_id",
                    "stop_intent_id", "requires_status_id", "terminal")))
    @staticmethod
    def conversation_reply(kind: str, payload: dict[str, Any]) -> bool:
        content = payload.get("content") or {}
        return (kind == "activity" and type(payload.get("native_reply_version")) is int
                and payload["native_reply_version"] == 1 and isinstance(content, dict)
                and payload.get("content_action") is True and payload.get("unlinked_content") is True
                and not any(payload.get(key) for key in ("work_owner", "session_key", "admission_id", "claim",
                    "release", "task_id", "stop_intent_id", "requires_status_id", "terminal"))
                and isinstance(payload.get("issue_id"), str) and bool(payload["issue_id"])
                and isinstance(payload.get("session_id"), str) and bool(payload["session_id"])
                and payload.get("requester_sessions") == {payload.get("issue_id"): payload["session_id"]}
                and content.get("type") in ("response", "error") and isinstance(content.get("body"), str))
    @staticmethod
    def reporting_notice(kind: str, payload: dict[str, Any]) -> bool:
        return (kind == "activity" and payload.get("reporting_notice_version") == 1
                and payload.get("one_try") is True and bool(payload.get("session_id"))
                and (payload.get("content") or {}).get("type") in ("response", "error", "thought")
                and not any(payload.get(key) for key in (
                    "work_owner", "session_key", "admission_id", "claim", "release", "stop_intent_id", "task_id", "terminal")))
    @classmethod
    def independent_comment(cls, kind: str, payload: dict[str, Any]) -> bool:
        # Adding a new authorized comment does not resolve or repeat an earlier mutation.
        return kind == "comment" and cls.unlinked_content(kind, payload)
    @classmethod
    def independent_message(cls, kind: str, payload: dict[str, Any]) -> bool:
        return (kind == "comment" or cls.conversation_reply(kind, payload)) and cls.unlinked_content(kind, payload)
    @classmethod
    def _delivery_hold_reason(cls, db: sqlite3.Connection, kind: str, payload: dict[str, Any]) -> str | None:
        if cls.unlinked_content(kind, payload):
            return None
        if any(payload.get(key) for key in ("work_owner", "session_key", "admission_id", "claim", "release", "stop_intent_id")):
            return "Retained execution-derived delivery requires explicit reconciliation; automatic replay is disabled"
        if cls.reporting_notice(kind, payload):
            return None  # A newly authorized refusal is reporting, never task admission.
        targets = cls._targets(payload)
        if not targets or not payload.get("task_id"):
            return "Unverified retained delivery has no native reporting binding; inspect its original effect history"
        matched_task = False
        for issue_id in targets:
            work = db.execute("SELECT origin,task_id FROM work WHERE issue_id=?", (issue_id,)).fetchone()
            binding = db.execute("SELECT value FROM metadata WHERE key=?", (f"native-binding:{issue_id}",)).fetchone()
            if not work or work["origin"] != "native" or not binding:
                return "Retained or missing work cannot authorize automatic reporting replay"
            saved = json.loads(binding[0])
            if (saved.get("issue_id") != issue_id or saved.get("task_id") != work["task_id"]
                    or not saved.get("profile") or not saved.get("board")):
                return "Native reporting binding does not match the retained delivery; reconcile before migration"
            matched_task = matched_task or payload["task_id"] == work["task_id"]
            if kind != "project_update" and payload["task_id"] != work["task_id"]:
                return "Native reporting binding does not match the retained delivery; reconcile before migration"
        return None if matched_task else "Native report has no matching task binding; reconcile before migration"
    @classmethod
    def _hold_legacy_outbox(cls, db: sqlite3.Connection) -> None:
        # Preserve request/effect history exactly: holding changes only this nullable column.
        # Historical sent rows stay sent, but cannot become current native receipts.
        rows = db.execute("SELECT id,kind,payload FROM outbox WHERE state IN ('pending','failed','sent') AND hold_reason IS NULL").fetchall()
        for row in rows:
            reason = cls._delivery_hold_reason(db, row["kind"], json.loads(row["payload"]))
            if reason:
                db.execute("UPDATE outbox SET hold_reason=? WHERE id=?", (reason, row["id"]))
        # A native dependent must not launder an older release/admission receipt.
        changed = True
        while changed:
            changed = False
            for row in db.execute("SELECT id,payload FROM outbox WHERE state IN ('pending','failed','sent') AND hold_reason IS NULL").fetchall():
                payload = json.loads(row["payload"])
                dependencies = {payload.get("requires_status_id"), *(payload.get("terminal_lines") or {}).values()} - {None, ""}
                if any(db.execute("SELECT 1 FROM outbox WHERE id=? AND hold_reason IS NOT NULL", (key,)).fetchone() for key in dependencies):
                    db.execute("UPDATE outbox SET hold_reason=? WHERE id=?",
                               ("Reporting depends on a retained delivery requiring explicit reconciliation", row["id"]))
                    changed = True
    def delivery_allowed(self, row_id: str) -> bool:
        """Fence every consumer, including direct sends and send admission, before effects."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT hold_reason FROM outbox WHERE id=?", (row_id,)).fetchone()
            return bool(row and row[0] is None)
    @staticmethod
    def _admitted(db: sqlite3.Connection, *issue_ids: str) -> bool:
        return all(not db.execute("SELECT 1 FROM specialist_scope_fence WHERE issue_id=?", (ident,)).fetchone()
                   for ident in issue_ids if ident)
    @staticmethod
    def _unresolved_terminal(db: sqlite3.Connection, issue_id: str, *, except_id: str = "") -> list[dict[str, str]]:
        """Attempted terminal writes, oldest first, whose effect stays unknown until an explicit durable resolution."""
        if not issue_id or issue_id.startswith("update:"):
            return []
        rows = db.execute("SELECT id, kind, state, attempts, payload FROM outbox WHERE kind IN ('status','description','delegate') AND "
                          "json_extract(payload, '$.issue_id')=? ORDER BY rowid", (issue_id,)).fetchall()
        return [{"id": row["id"], "kind": row["kind"]} for row in rows
                if row["id"] != except_id and Store.uncertain(row, json.loads(row["payload"]))]
    @staticmethod
    def uncertain(row, payload: dict[str, Any]) -> bool:
        """An attempted terminal or content write whose remote effect stays unknown until resolved."""
        return bool((payload.get("terminal") or payload.get("content_action") or
                     (row["kind"] == "status" and (payload.get("write_started") is True or payload.get("reconcile_required"))))
                    and not payload.get("reconciliation") and (
            payload.get("reconcile_required") or (row["state"] in ("pending", "failed") and (
                payload.get("write_started") is True or (row["attempts"] > 0 and "write_started" not in payload)))))
    @classmethod
    def _effect_admitted(cls, db: sqlite3.Connection, *issue_ids: str, except_id: str = "") -> bool:
        return cls._admitted(db, *issue_ids) and all(
            not cls._unresolved_terminal(db, ident, except_id=except_id)
            and not cls._pending_chat_transfer(db, ident) for ident in issue_ids)
    @staticmethod
    def _pending_chat_transfer(db: sqlite3.Connection, issue_id: str) -> dict | None:
        for row in db.execute("SELECT value FROM metadata WHERE key LIKE 'chat-transfer:%'"):
            transfer = json.loads(row[0])
            if transfer["issue_id"] == issue_id and transfer["state"] in ("prepared", "committed"):
                return transfer
        return None
    @staticmethod
    def _predecessor_transfer_blocks(db: sqlite3.Connection, issue_id: str, payload: dict) -> bool:
        for row in db.execute("SELECT value FROM metadata WHERE key LIKE 'chat-transfer:%'"):
            saved = json.loads(row[0])
            if saved["issue_id"] == issue_id and (
                    payload.get("work_owner") == saved["predecessor"]["ownership_id"] or
                    payload.get("session_key") == saved["predecessor"]["owner_ref"]):
                return True
        return False
    def predecessor_transfer_blocks(self, issue_id: str, payload: dict) -> bool:
        with self._tx() as db:
            return self._predecessor_transfer_blocks(db, issue_id, payload)
    def issue_reconciliation_blocked(self, issue_id: str, *, except_id: str = "") -> list[dict[str, str]]:
        """Falsy when the issue is free; otherwise the held writes, so a refusal can name them."""
        with self._tx() as db:
            return self._unresolved_terminal(db, issue_id, except_id=except_id)
    @contextmanager
    def guard_issue_work(self, issue_id: str) -> Iterator[bool]:
        with self._tx() as db:
            yield self._effect_admitted(db, issue_id)
    def admit_mutation(self, row_id: str, *, terminal: bool = False) -> bool:
        """Persist a remote-send marker without using a Linear record as execution authority."""
        if getattr(self._local, "db", None) is not None:
            raise RuntimeError("mutation admission requires its own commit")
        with self._tx() as db:
            row = db.execute("SELECT kind, state, payload FROM outbox WHERE id=?", (row_id,)).fetchone()
            if not row or row["state"] != "pending":
                return False
            self._hold_legacy_outbox(db)
            if not self.delivery_allowed(row_id):
                return False
            payload = json.loads(row["payload"])
            if (payload.get("native_history") is True or payload.get("native_progress") is True) and payload.get("requires_status_id"):
                dependency = self.outbox_row(payload["requires_status_id"])
                if not self.history_status_settled(dependency):
                    return False
            targets = self._targets(payload)
            if not self.conversation_reply(row["kind"], payload) and not self._admitted(db, *targets):
                return False
            # A normal authorized issue edit/comment is independent of retained chat ownership.
            # An uncertain prior remote mutation still requires external reconciliation.
            if self.reporting_notice(row["kind"], payload):
                pass  # Informational replies do not repeat or resolve any earlier effect.
            elif payload.get("content_action"):
                if not self.independent_message(row["kind"], payload) and any(self._unresolved_terminal(db, target, except_id=row_id) for target in targets):
                    return False
            else:
                if not self._effect_admitted(db, *targets, except_id=row_id):
                    return False
                if any(self._predecessor_transfer_blocks(db, target, payload) for target in targets):
                    return False
                if payload.get("work_owner") and self.superseded({"id": row_id, "payload": payload}):
                    return False
            native_progress = (row["kind"] in ("comment", "activity") and
                               (payload.get("native_progress") is True or payload.get("native_history") is True)
                               and payload.get("task_id"))
            if terminal or native_progress or row["kind"] in ("status", "description", "delegate", "project_update"):
                return bool(db.execute("UPDATE outbox SET payload=json_set(payload, '$.write_started', json('true')) "
                                       "WHERE id=?", (row_id,)).rowcount)
            return True
    def reject_native_message_before_transport(self, row_id: str, *, started_before: bool | None) -> bool:
        """Only the pre-yield guard's proved non-send can restore an unstarted marker."""
        if started_before is not False:
            return False
        with self._tx() as db:
            row = db.execute("SELECT kind,state,attempts,payload FROM outbox WHERE id=?", (row_id,)).fetchone()
            if not row or row["state"] != "pending" or row["attempts"] != 0 or row["kind"] not in ("comment", "activity"):
                return False
            payload = json.loads(row["payload"])
            if (payload.get("write_started") is not True or not payload.get("task_id") or
                    not (payload.get("native_history") is True or payload.get("native_progress") is True)):
                return False
            db.execute("UPDATE outbox SET payload=json_set(payload,'$.write_started',json('false')) WHERE id=?", (row_id,))
            self._sequence_native_history(db, payload["issue_id"])
            return True

    def delegate_not_applied(self, row_id: str, *, started_before: bool | None) -> bool:
        """Only an affirmative HTTP rejection can undo this attempt's send marker."""
        if started_before is not False:
            return False
        with self._tx() as db:
            row = db.execute("SELECT kind,state,payload FROM outbox WHERE id=?", (row_id,)).fetchone()
            if not row or row["kind"] != "delegate" or row["state"] != "pending":
                return False
            payload = json.loads(row["payload"])
            if payload.get("delegate_routing_version") != 1 or payload.get("reconcile_required") or payload.get("delegate_receipt"):
                return False
            db.execute("UPDATE outbox SET payload=json_set(payload, '$.write_started', json('false')) WHERE id=?", (row_id,))
            return True

    def reconcile_terminal(self, row_id: str, *, outcome: str, evidence: str, at: float) -> bool:
        """Record an operator's verified remote outcome; keep the attempted receipt intact."""
        if outcome not in ("applied", "not_applied") or not evidence.strip():
            raise ValueError("terminal reconciliation requires a verified outcome and evidence")
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT kind, state, attempts, payload, hold_reason FROM outbox WHERE id=?", (row_id,)).fetchone()
            if not row or row["kind"] not in ("status", "description", "delegate"): return False
            payload = json.loads(row["payload"])
            if payload.get("reconciliation") or not self.uncertain(row, payload) and not row["hold_reason"]: return False
            payload["reconciliation"] = {"outcome": outcome, "evidence": evidence.strip(), "at": at}
            payload["reconcile_required"], payload["reported"] = True, 1  # the operator acted; no alert
            if payload.get("release"):
                payload["applied"] = int(outcome == "applied")
                # External release verification is not proof that retained execution ended.
                # Keep its checkpoints, Stop and ownership/effect history unchanged.
            state = "sent" if (payload.get("release") and payload.get("applied") or
                               row["kind"] == "delegate" and outcome == "applied") else "failed"
            db.execute("UPDATE outbox SET payload=?, state=? WHERE id=?", (json.dumps(payload), state, row_id))
            return True
    @staticmethod
    def _targets(payload: dict[str, Any]) -> set[str]:
        return {ident for ident in (payload.get("owner_issue_id"), payload.get("resolve"),
                                    *(payload.get("line_issues") or {}).values(),
                                    payload.get("issue_id") if not str(payload.get("issue_id", "")).startswith("update:")
                                    else None) if ident}
    def _row_admitted(self, db: sqlite3.Connection, row_id: str, state: str | None = None) -> bool:
        self._hold_legacy_outbox(db)
        row = db.execute("SELECT kind, state, payload, hold_reason FROM outbox WHERE id=?", (row_id,)).fetchone()
        if not row or row["hold_reason"] is not None or state is not None and row["state"] != state:
            return False
        payload = json.loads(row["payload"])
        return self.conversation_reply(row["kind"], payload) or self._admitted(db, *self._targets(payload))
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
    def native_binding(self, issue_id: str, profile: str | None = None,
                       board: str | None = None) -> dict[str, Any] | None:
        with self._tx() as db:
            row = db.execute("SELECT value FROM metadata WHERE key=?",
                             (f"native-binding:{issue_id}",)).fetchone()
            work = db.execute("SELECT origin,task_id FROM work WHERE issue_id=?", (issue_id,)).fetchone()
        if not row or not work or work["origin"] != "native":
            return None
        binding = json.loads(row[0])
        if (binding["task_id"] != work["task_id"] or
                (profile is not None and binding["profile"] != profile) or
                (board is not None and binding["board"] != board)):
            return None
        return binding
    def bind_native(self, issue_id: str, task_id: str, profile: str, board: str | None,
                    owner_ref: str = "", project_id: str | None = None, *,
                    requester_activity: str | None = None, completion_scope: dict | None = None, at: float) -> bool:
        """Link an inspected task without claiming, resuming or adopting legacy work."""
        if any(not isinstance(value, str) or not value.strip() for value in (issue_id, task_id, profile)):
            raise ValueError("native binding needs an issue, task and profile")
        binding = {"issue_id": issue_id, "task_id": task_id, "profile": profile,
                   "board": board or "default"}
        if completion_scope is not None:
            binding["completion_scope"] = self.validate_completion_scope(completion_scope)
        if requester_activity is not None:
            if not owner_ref or not isinstance(requester_activity, str) or not requester_activity or requester_activity == owner_ref:
                raise ValueError("native reporting needs distinct session and Activity coordinates")
            binding.update(requester_session_id=owner_ref, requester_activity_id=requester_activity)
        with self._tx() as db:
            work = db.execute("SELECT origin,task_id FROM work WHERE issue_id=?", (issue_id,)).fetchone()
            prior = db.execute("SELECT value FROM metadata WHERE key=?", (f"native-binding:{issue_id}",)).fetchone()
            if work or prior:
                return bool(work and work["origin"] == "native" and prior and json.loads(prior[0]) == binding)
            if (not self._effect_admitted(db, issue_id) or self.retired(issue_id) or
                    db.execute("SELECT 1 FROM chat_admissions WHERE issue_id=? AND state IN "
                               "('prepared','claiming','admitted','ambiguous')", (issue_id,)).fetchone()):
                return False
            db.execute("INSERT INTO work (issue_id,origin,owner_ref,task_id,project_id,last_updated_at,linear_session_id) "
                       "VALUES (?,'native',?,?,?,?,?)", (issue_id,owner_ref,task_id,project_id,at,owner_ref or None))
            db.execute("INSERT INTO metadata (key,value) VALUES (?,?)",
                       (f"native-binding:{issue_id}", json.dumps(binding)))
            return True
    @staticmethod
    def validate_completion_scope(value: dict) -> dict:
        """A reporting boundary, never permission to execute or a second task state."""
        if (not isinstance(value, dict) or set(value) != {"mode", "task_spec_sha256", "specified_event_id"}
                or not isinstance(value.get("mode"), str) or value["mode"] not in ("outcome", "progress")
                or not isinstance(value.get("task_spec_sha256"), str)
                or len(value["task_spec_sha256"]) != 64
                or any(c not in "0123456789abcdef" for c in value["task_spec_sha256"])
                or type(value.get("specified_event_id")) is not int or value["specified_event_id"] < 0):
            raise ValueError("Invalid native completion reporting scope")
        return dict(value)
    @staticmethod
    def _binding_snapshot(db: sqlite3.Connection, issue_id: str) -> dict[str, Any]:
        work = db.execute("SELECT * FROM work WHERE issue_id=?", (issue_id,)).fetchone()
        binding = db.execute("SELECT value FROM metadata WHERE key=?", (f"native-binding:{issue_id}",)).fetchone()
        return {"work": dict(work) if work else None, "native_binding": binding[0] if binding else None}
    @staticmethod
    def _binding_revision(issue_id: str, snapshot: dict[str, Any]) -> str:
        # Include absence and the exact stored binding text, not only its parsed target.
        value = json.dumps({"issue_id": issue_id, **snapshot}, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(value.encode()).hexdigest()
    def binding_revision(self, issue_id: str) -> str:
        """Token for an exact inspected reporting projection; it proves no execution state."""
        with self._tx() as db:
            return self._binding_revision(issue_id, self._binding_snapshot(db, issue_id))
    @classmethod
    def _hold_superseded_reports(cls, db: sqlite3.Connection, issue_id: str) -> None:
        # Do not rewrite an attempted request or reinterpret a historical sent receipt.
        # Ordinary content is independent of execution, including uncertain descriptions.
        reason = "Reporting binding was explicitly replaced; retained delivery must not be replayed"
        superseded = set()
        for row in db.execute("SELECT id,kind,payload,hold_reason FROM outbox").fetchall():
            payload = json.loads(row["payload"])
            if issue_id in cls._targets(payload) and not cls.unlinked_content(row["kind"], payload):
                superseded.add(row["id"])
                if row["hold_reason"] is None:
                    db.execute("UPDATE outbox SET hold_reason=? WHERE id=?", (reason, row["id"]))
        changed = True
        while changed:
            changed = False
            for row in db.execute("SELECT id,payload FROM outbox WHERE hold_reason IS NULL").fetchall():
                payload = json.loads(row["payload"])
                dependencies = {payload.get("requires_status_id"), *(payload.get("terminal_lines") or {}).values()} - {None, ""}
                if dependencies & superseded:
                    db.execute("UPDATE outbox SET hold_reason=? WHERE id=?",
                               ("Reporting depends on a superseded delivery; retained history must not be replayed", row["id"]))
                    superseded.add(row["id"])
                    changed = True
    def replace_native_binding(self, issue_id: str, task_id: str, profile: str, board: str | None, *,
                               expected_revision: str, request_id: str, owner_ref: str = "",
                               project_id: str | None = None, requester_activity: str | None = None,
                               completion_scope: dict | None = None, reconcile_complete: bool = False,
                               at: float) -> dict[str, Any] | None:
        """Explicitly replace reporting after caller inspection, never native execution.

        Archive the exact prior projection in the existing immutable intent. Uncertain
        effects, Stop/admission/transfer records and specialist fences remain untouched.
        A stale inspection or scope fence returns None; conflicting request reuse fails.
        """
        if any(not isinstance(value, str) or not value.strip() for value in (issue_id, task_id, profile)):
            raise ValueError("native binding needs an issue, task and profile")
        if (board is not None and (not isinstance(board, str) or not board.strip()) or
                not isinstance(owner_ref, str) or project_id is not None and not isinstance(project_id, str)):
            raise ValueError("native binding has invalid board or reporting scope")
        try:
            parsed = uuid.UUID(request_id)
        except (ValueError, TypeError, AttributeError):
            raise ValueError("reporting replacement needs a canonical request UUIDv4") from None
        if parsed.version != 4 or str(parsed) != request_id:
            raise ValueError("reporting replacement needs a canonical request UUIDv4")
        if (not isinstance(expected_revision, str) or len(expected_revision) != 64 or
                any(char not in "0123456789abcdef" for char in expected_revision)):
            raise ValueError("reporting replacement needs an inspected binding revision")
        binding = {"issue_id": issue_id, "task_id": task_id, "profile": profile, "board": board or "default"}
        if completion_scope is not None:
            binding["completion_scope"] = self.validate_completion_scope(completion_scope)
        if requester_activity is not None:
            if not owner_ref or not isinstance(requester_activity, str) or not requester_activity or requester_activity == owner_ref:
                raise ValueError("native reporting needs distinct session and Activity coordinates")
            binding.update(requester_session_id=owner_ref, requester_activity_id=requester_activity)
        request = {"binding": binding, "expected_revision": expected_revision,
                   "owner_ref": owner_ref, "project_id": project_id}
        if reconcile_complete:
            request["reconcile_complete"] = True
        key = f"track:{issue_id}:{request_id}"
        with self._tx() as db:
            if not self._admitted(db, issue_id):
                return None
            previous = self._binding_snapshot(db, issue_id)
            prior = self.native_intent(key)
            if prior:
                if prior["payload"].get("request") != request:
                    raise ValueError("native intent key reused with conflicting reporting replacement")
                work = previous["work"] or {}
                current = json.loads(previous["native_binding"]) if previous["native_binding"] else None
                return dict(binding) if (prior["state"] == "applied" and current == binding and
                    work.get("origin") == "native" and work.get("task_id") == task_id and
                    work.get("owner_ref") == owner_ref and work.get("project_id") == project_id) else None
            if self._binding_revision(issue_id, previous) != expected_revision:
                return None
            self.native_intent_prepare(key, {"kind": "track", "issue_id": issue_id, "request": request,
                                            "previous": previous, "at": at})
            work = previous["work"] or {}
            old_binding = json.loads(previous["native_binding"]) if previous["native_binding"] else {}
            same_target = (isinstance(old_binding, dict) and work.get("origin") == "native" and work.get("task_id") == task_id and
                           all(old_binding.get(field) == binding[field] for field in ("issue_id", "task_id", "profile", "board")))
            unchanged = (same_target and old_binding == binding and work.get("owner_ref") == owner_ref
                         and work.get("project_id") == project_id)
            if not unchanged:
                self._hold_superseded_reports(db, issue_id)
                if same_target:
                    # Reporting metadata cannot manufacture a newer execution
                    # instruction or suppress an already valid source Stop.
                    db.execute("UPDATE work SET owner_ref=?,project_id=?,linear_session_id=? WHERE issue_id=?",
                               (owner_ref, project_id, owner_ref or None, issue_id))
                else:
                    db.execute("DELETE FROM work WHERE issue_id=?", (issue_id,))
                    db.execute("INSERT INTO work(issue_id,origin,owner_ref,task_id,project_id,last_updated_at,linear_session_id) "
                               "VALUES (?,'native',?,?,?,?,?)", (issue_id, owner_ref, task_id, project_id, at, owner_ref or None))
                db.execute("INSERT INTO metadata(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                           (f"native-binding:{issue_id}", json.dumps(binding)))
            self.native_intent_settle(key, {"state": "applied", "binding": binding,
                "revision": self._binding_revision(issue_id, self._binding_snapshot(db, issue_id))})
            return dict(binding)
    def reconcile_completion(self, issue_id: str, request_id: str, event_id: int,
                             writes: list[tuple[str, dict]], *, at: float,
                             status_required: bool = True) -> bool:
        """Repair missing delivery components without repeating native work or applied effects."""
        key = f"track:{issue_id}:{request_id}"
        with self._tx() as db:
            saved = self.native_intent(key)
            request = (saved or {}).get("payload", {}).get("request", {})
            binding = request.get("binding")
            if (not saved or saved.get("state") != "applied" or request.get("reconcile_complete") is not True
                    or not binding or self.native_binding(issue_id) != binding
                    or binding.get("completion_scope", {}).get("mode") != "outcome"):
                return False
            status = next((payload for kind, payload in writes if kind == "status"
                           and payload.get("terminal") and payload.get("state") == "done"), None)
            proof = (status or {}).get("native_completion") or {}
            if (not status or type(event_id) is not int or event_id <= 0 or proof.get("event_id") != event_id
                    or status.get("issue_id") != issue_id or proof.get("scope") != binding["completion_scope"]
                    or any(proof.get(field) != binding[field] for field in ("task_id", "profile", "board"))):
                return False
            prior = self.outbox_row(request_id)
            if prior:
                if (prior["kind"] != "status" or prior["payload"].get("issue_id") != issue_id
                        or prior["payload"].get("native_completion") != proof
                        or prior["payload"].get("completion_reconcile_request_id") != request_id):
                    raise ValueError("Completion reconciliation UUID conflicts with an existing report")
                if prior["state"] == "sent" and prior["payload"].get("applied", 1) == 0:
                    return False  # Exact requests never acquire a newer reporting timestamp.
            existing = db.execute("SELECT * FROM outbox WHERE kind='status' "
                                  "AND json_extract(payload,'$.issue_id')=? AND json_extract(payload,'$.state')='done' "
                                  "AND NOT (state='sent' AND COALESCE(json_extract(payload,'$.applied'),1)=0) "
                                  "ORDER BY CASE WHEN state='sent' AND json_extract(payload,'$.applied')=1 "
                                  "THEN 0 WHEN hold_reason IS NULL THEN 1 ELSE 2 END, rowid DESC",
                                  (issue_id,)).fetchall()
            reusable = prior
            if reusable is None:
                for raw in existing:
                    candidate = {**dict(raw), "payload": json.loads(raw["payload"])}
                    if candidate["payload"].get("native_completion") != proof:
                        continue
                    applied = candidate["state"] == "sent" and candidate["payload"].get("applied") == 1
                    if status_required and applied:
                        continue  # A reopened issue needs a current authorized status intent.
                    reusable = candidate
                    break
            if reusable is not None:
                applied = reusable["state"] == "sent" and reusable["payload"].get("applied") == 1
                if reusable.get("hold_reason") and not applied:
                    return False  # A held or uncertain status is never a delivery receipt.
                for kind, payload in writes:
                    if kind != "comment" or not payload.get("assessment"):
                        continue
                    if self.assessment_comment_id(issue_id, payload["body"], completion=proof):
                        continue
                    current = self.outbox_row(payload["id"])
                    if current:
                        if (current["kind"] != "comment" or current["payload"].get("issue_id") != issue_id
                                or current["payload"].get("body") != payload["body"]
                                or current["payload"].get("completion_reconcile_request_id") != request_id):
                            raise ValueError("Completion reconciliation UUID conflicts with an existing report")
                        if current["state"] == "sent" and current["payload"].get("applied", 1) == 0:
                            return False
                        continue
                    sibling = {**payload, "native_completion": proof,
                               "completion_reconcile_request_id": request_id,
                               "source_ms": saved["payload"]["at"]}
                    dependency = "reconciled_status_id" if reusable.get("hold_reason") else "requires_status_id"
                    sibling[dependency] = reusable["id"]
                    if not self.enqueue("comment", sibling, at=at):
                        return False
            else:
                writes = [(kind, payload) for kind, payload in writes
                          if not (kind == "comment" and payload.get("assessment")
                                  and self.assessment_comment_id(issue_id, payload["body"], completion=proof))]
                writes = [(kind, {**payload, "completion_reconcile_request_id": request_id} if kind == "status" else payload)
                          for kind, payload in writes]
                if not self.finish(issue_id, writes, at=at, status_id=request_id):
                    return False
            db.execute("UPDATE work SET last_event_id=MAX(last_event_id,?) WHERE issue_id=?", (event_id, issue_id))
            return True
    def native_intent(self, key: str) -> dict[str, Any] | None:
        with self._tx() as db:
            row = db.execute("SELECT value FROM metadata WHERE key=?", (f"native-intent:{key}",)).fetchone()
        return json.loads(row[0]) if row else None
    def record_unbound_stop(self, issue_id: str, source_ms: float, *, session_id: str,
                            activity_id: str, actor: str) -> dict[str, Any]:
        """Keep the newest authenticated Stop even across independent service instances."""
        with self._tx():
            key = f"unbound-stop:{issue_id}"
            saved, _ = self.native_intent_prepare(key, {"kind": "stop", "issue_id": issue_id})
            if source_ms >= float(saved.get("source_ms") or 0):
                return self.native_intent_settle(key, {"state": "reconcile", "source_ms": source_ms,
                    "session_id": session_id, "activity_id": activity_id, "actor": actor,
                    "control_version": "native-event-guard-v1",
                    "reason": "Stop arrived before a native binding; inspect any in-flight task creation"})
            return saved
    def native_intent_prepare(self, key: str, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """Commit a delivery attempt before the native call; only one caller may send.

        Native task creation/comments are not atomic exactly-once APIs. A crash or lost
        response leaves this intent pending reconciliation, never permission to replay.
        """
        if not key or not isinstance(payload, dict):
            raise ValueError("native intent needs a key and immutable payload")
        with self._tx() as db:
            row = db.execute("SELECT value FROM metadata WHERE key=?", (f"native-intent:{key}",)).fetchone()
            if row:
                saved = json.loads(row[0])
                if saved["payload"] != payload:
                    raise ValueError("native intent key reused with conflicting payload")
                return saved, False
            saved = {"state": "attempting", "payload": payload}
            db.execute("INSERT INTO metadata (key,value) VALUES (?,?)", (f"native-intent:{key}", json.dumps(saved)))
            return saved, True
    def native_intent_settle(self, key: str, updates: dict[str, Any]) -> dict[str, Any]:
        if updates.get("state") not in ("applied", "reconcile") or "payload" in updates:
            raise ValueError("native intent must retain its request and delivery evidence")
        with self._tx() as db:
            row = db.execute("SELECT value FROM metadata WHERE key=?", (f"native-intent:{key}",)).fetchone()
            if not row:
                raise ValueError("native intent was not prepared")
            saved = json.loads(row[0])
            if saved["state"] == "applied":
                if any(saved.get(k) != v for k,v in updates.items()):
                    raise ValueError("applied native intent cannot be rewritten")
                return saved
            saved.update(updates)
            db.execute("UPDATE metadata SET value=? WHERE key=?", (json.dumps(saved),f"native-intent:{key}"))
            return saved
    # -- session-free durable chat admission ------------------------------
    def retired(self, issue_id: str | None = None) -> list[dict]:
        with self._tx() as db:
            return [dict(row) for row in db.execute("SELECT * FROM retirement WHERE ? IS NULL OR issue_id=?", (issue_id, issue_id))]
    def put(self, issue_id: str, origin: str, owner_ref: str, *, task_id: str | None = None,
            project_id: str | None = None, last_updated_at: float = 0.0,
            run_generation: int | None = None) -> bool:
        """Import a retained record without adopting/replacing existing work; grants no execution."""
        with self._tx() as db:
            if not self._effect_admitted(db, issue_id) or self.retired(issue_id):
                return False
            return bool(db.execute(
                "INSERT OR IGNORE INTO work (issue_id,origin,owner_ref,task_id,project_id,last_updated_at,run_generation) "
                "VALUES (?,?,?,?,?,?,?)", (issue_id,origin,owner_ref,task_id,project_id,last_updated_at,run_generation)).rowcount)
    def update(self, issue_id: str, **fields: Any) -> bool:
        allowed = {"project_id", "last_updated_at", "linear_session_id", "panel_note", "last_event_id"}
        if not fields or not set(fields) <= allowed:
            raise ValueError("Linear bindings cannot mutate execution ownership or Stop state")
        cols = ", ".join(f"{name} = ?" for name in fields)
        with self._tx() as db:
            if not self._admitted(db, issue_id):
                return False
            return bool(db.execute(f"UPDATE work SET {cols} WHERE issue_id=?", (*fields.values(), issue_id)).rowcount)
    def delete(self, issue_id: str) -> bool:
        with self._tx() as db:
            if not self._admitted(db, issue_id): return False
            db.execute("DELETE FROM work WHERE issue_id = ? AND origin != 'native'", (issue_id,))
            return True
    def retire_untracked(self, issue_id: str, task_id: str | None) -> bool:
        """Stop tracking a record whose native task does not exist; outbox history stays inspectable."""
        with self._tx() as db:
            if not self._admitted(db, issue_id):
                return False
            gone = db.execute("DELETE FROM work WHERE issue_id=? AND task_id IS ?", (issue_id, task_id)).rowcount
            if gone and task_id is not None:  # A fresh link can bind this issue again.
                db.execute("DELETE FROM metadata WHERE key=?", (f"native-binding:{issue_id}",))
            return bool(gone)
    def scope_fenced(self, issue_id: str) -> bool:
        with self._tx() as db:
            return db.execute("SELECT 1 FROM specialist_scope_fence WHERE issue_id = ?",
                              (issue_id,)).fetchone() is not None
    def fence_scope(self, issue_id: str, reason: str, *, at: float) -> None:
        """Persist a permanent authorization denial without deleting work or its evidence."""
        with self._tx() as db:
            db.execute("INSERT OR IGNORE INTO specialist_scope_fence (issue_id, reason, fenced_at) "
                       "VALUES (?, ?, ?)", (issue_id, reason[:500], at))
    def finish(self, issue_id: str, writes: list[tuple[str, dict[str, Any]]], *, at: float,
               release: bool = False, status_id: str | None = None) -> bool:
        """Capture terminal Linear writes before forgetting work, in one durable commit."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            if not self._effect_admitted(db, issue_id): return False
            work = db.execute("SELECT ownership_id FROM work WHERE issue_id = ?", (issue_id,)).fetchone()
            if not work: return False
            if any(not self._admitted(db, *self._targets(payload)) for _, payload in writes): return False
            for kind, payload in writes:
                if kind == "project_update":
                    prior = db.execute("SELECT payload, state FROM outbox WHERE kind='project_update' AND hold_reason IS NULL AND "
                                       "json_extract(payload, '$.session_id')=? AND "
                                       "json_extract(payload, '$.project_id') IS ? ORDER BY "
                                       "CASE state WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, rowid DESC LIMIT 1",
                                       (payload["session_id"], payload["project_id"])).fetchone()
                    if prior and prior["state"] in ("pending", "failed") and not json.loads(prior["payload"]).get("frozen") \
                            and not self._admitted(db, *self._targets(json.loads(prior["payload"]))): return False
            status_id = status_id or str(uuid.uuid4())
            for kind, payload in writes:
                payload = {**payload, "work_owner": work["ownership_id"]}
                row_id = status_id if kind == "status" else (
                    str(payload["id"]) if kind == "comment" and payload.get("id") else str(uuid.uuid4()))
                if kind != "status":
                    payload = {**payload, "requires_status_id": status_id}
                if kind == "project_update":
                    # Project batches have their own queue: an older coalesced batch
                    # may depend on a newer status without blocking that status.
                    payload["issue_id"] = f"update:{payload['session_id']}:{payload['project_id'] or issue_id}"
                    payload["terminal_lines"] = {ident: status_id for ident in payload["lines"]}
                    existing = db.execute("SELECT * FROM outbox WHERE kind = 'project_update' AND hold_reason IS NULL AND "
                                          "json_extract(payload, '$.session_id') = ? AND "
                                          "json_extract(payload, '$.project_id') IS ? "
                                          "ORDER BY CASE state WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, "
                                          "rowid DESC LIMIT 1",
                                          (payload["session_id"], payload["project_id"])).fetchone()
                    if existing and self._requester_conflict(json.loads(existing["payload"]), payload):
                        existing = None
                    if existing:
                        prior = json.loads(existing["payload"])
                        if existing["state"] in ("pending", "failed") and not prior.get("frozen"):
                            prior["lines"].update(payload["lines"])
                            for key in ("line_issues", "terminal_lines"):
                                prior.setdefault(key, {}).update(payload.get(key, {}))
                            self._merge_requesters(prior, payload)
                            prior.update({key: value for key, value in payload.items()
                                          if key not in ("lines", "line_issues", "terminal_lines", "requester_sessions", "requester_activities")})
                            prior["enqueued_at"] = at
                            db.execute("UPDATE outbox SET payload = ?, next_at = MAX(next_at, ?) WHERE id = ?",
                                       (json.dumps(prior), at + float(payload.get("quiet", 0)), existing["id"]))
                        continue
                body = json.dumps({**payload, "enqueued_at": payload.get("enqueued_at", at)})
                db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                           (row_id, kind, body, at + float(payload.get("quiet", 0))))
            self._sequence_native_history(db, issue_id)
            if release:
                db.execute("UPDATE work SET release_pending=1 WHERE issue_id=?", (issue_id,))
            else:
                db.execute("DELETE FROM work WHERE issue_id = ? AND origin != 'native'", (issue_id,))
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
            if any(kind == "status" and payload.get("terminal") for kind, payload in writes):
                # Reuse the established terminal dependency grouping in this same
                # transaction; a crash cannot advance the cursor without its writes.
                if not self.finish(issue_id, writes, at=at):
                    return False
            else:
                for kind, payload in writes:
                    db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                               (str(uuid.uuid4()), kind, json.dumps({**payload, "enqueued_at": at}), at))
            self._sequence_native_history(db, issue_id)
            db.execute("UPDATE work SET last_event_id = ? WHERE issue_id = ?", (event_id, issue_id))
            if forget:
                db.execute("DELETE FROM work WHERE issue_id = ? AND origin != 'native'", (issue_id,))
            return True
    @staticmethod
    def _sequence_native_history(db: sqlite3.Connection, issue_id: str) -> None:
        """Current status precedes only native history durably known not to have started."""
        work = db.execute("SELECT task_id FROM work WHERE issue_id=? AND origin='native'", (issue_id,)).fetchone()
        if not work:
            return
        status = db.execute("SELECT id FROM outbox WHERE kind='status' AND state='pending' AND hold_reason IS NULL "
                            "AND json_extract(payload,'$.issue_id')=? AND json_extract(payload,'$.task_id')=? "
                            "ORDER BY rowid DESC LIMIT 1", (issue_id, work["task_id"])).fetchone()
        if status:
            db.execute("UPDATE outbox SET payload=json_set(payload,'$.requires_status_id',?) "
                       "WHERE state='pending' AND hold_reason IS NULL AND kind IN ('comment','activity') AND attempts=0 "
                       "AND (json_extract(payload,'$.native_history')=1 OR json_extract(payload,'$.native_progress')=1) "
                       "AND json_type(payload,'$.write_started')='false' "
                       "AND json_extract(payload,'$.issue_id')=? AND json_extract(payload,'$.task_id')=?",
                       (status["id"], issue_id, work["task_id"]))

    def active(self, origin: str | None = None) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM work WHERE ? IS NULL OR origin = ?", (origin, origin)).fetchall()
        return [dict(row) for row in rows]
    def captured_task_activities(self, issue_id: str, task_id: str, session_id: str) -> Counter[tuple[str, str]]:
        """Count captured alerts; legacy rows used issue and session without a task id."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            rows = db.execute(
                "SELECT json_extract(payload, '$.content.type'), json_extract(payload, '$.content.body'), "
                "COUNT(*) FROM outbox WHERE hold_reason IS NULL AND kind = 'activity' AND json_extract(payload, '$.issue_id') = ? "
                "AND (json_extract(payload, '$.task_id') = ? OR "
                "(json_extract(payload, '$.task_id') IS NULL AND "
                "json_extract(payload, '$.session_id') = ? AND "
                "(state IN ('sent', 'pending') OR (state = 'failed' AND attempts > 0)))) GROUP BY 1, 2",
                (issue_id, task_id, session_id)).fetchall()
        return Counter({(kind, body): count for kind, body, count in rows})
    def terminal_captured(self, task_id: str, *, issue_id: str | None = None, state: str | None = None) -> bool:
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            return bool(db.execute("SELECT 1 FROM outbox WHERE hold_reason IS NULL AND kind='status' AND "
                                   "json_extract(payload, '$.task_id')=? AND "
                                   "(? IS NULL OR json_extract(payload, '$.issue_id')=?) AND "
                                   "(? IS NULL OR json_extract(payload, '$.state')=?) AND "
                                   "json_extract(payload, '$.terminal')=1 AND "
                                   "json_extract(payload, '$.admission_id') IS NULL LIMIT 1",
                                   (task_id, issue_id, issue_id, state, state)).fetchone())
    def assessment_comment_id(self, issue_id: str, body: str, *, completion: dict | None = None) -> str | None:
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            rows = db.execute("SELECT * FROM outbox WHERE kind='comment' AND "
                              "json_extract(payload, '$.issue_id')=? AND "
                              "json_extract(payload, '$.assessment')=1 AND "
                              "json_extract(payload, '$.body')=? AND state IN ('sent', 'pending', 'failed') "
                              "ORDER BY CASE WHEN state='sent' AND json_extract(payload,'$.applied')=1 "
                              "THEN 0 ELSE 1 END, rowid DESC", (issue_id, body)).fetchall()
            for raw in rows:
                row = {**dict(raw), "payload": json.loads(raw["payload"])}
                payload = row["payload"]
                if row["state"] == "sent" and payload.get("applied", 1) == 0:
                    continue
                if row["state"] == "failed" and not self.uncertain(row, payload):
                    continue
                dependency = self.outbox_row(payload.get("requires_status_id") or payload.get("reconciled_status_id") or "")
                if (dependency and dependency["state"] == "sent"
                        and dependency["payload"].get("applied", 1) == 0
                        and not self.uncertain(row, payload)):
                    continue  # An unattempted sibling cannot publish behind a known-unapplied status.
                if row.get("hold_reason"):
                    source = dependency
                    if (not completion or not source or source["kind"] != "status"
                            or source["payload"].get("issue_id") != issue_id
                            or source["payload"].get("native_completion") != completion):
                        continue
                return str(row["id"])
        return None
    def superseded(self, row: dict[str, Any]) -> bool:
        payload, owner = row["payload"], row["payload"].get("work_owner")
        with self._tx() as db:
            work = db.execute("SELECT origin,task_id,ownership_id FROM work WHERE issue_id=?", (payload["issue_id"],)).fetchone()
            if work and work["origin"] == "native":
                return bool(payload.get("task_id") and payload["task_id"] != work["task_id"])
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
    def first_value(self, key: str, value: str) -> str:
        """Keep the first value recorded for ``key`` and return it; later calls cannot change it."""
        with self._tx() as db:
            db.execute("INSERT OR IGNORE INTO metadata(key,value) VALUES (?,?)", (key, value))
            return db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()[0]
    # -- outbox -----------------------------------------------------------
    def enqueue(self, kind: str, payload: dict[str, Any], *, at: float | None = None) -> str:
        row_id = str(payload.get("id") or uuid.uuid4())
        now = time.time() if at is None else at
        with self._tx() as db:
            existing = db.execute("SELECT kind, payload FROM outbox WHERE id=?", (row_id,)).fetchone()
            if existing:
                saved = json.loads(existing["payload"])
                if kind == "delegate" and any(saved.get(key) != payload.get(key) for key in (
                        "target_delegate_id", "expected_delegate_id", "expected_updated_at",
                        "expected_binding_revision", "native_binding", "task_id", "delegate_routing_version")):
                    raise ValueError("outbox id reused with conflicting delegate routing")
                if existing["kind"] == kind and all(saved.get(k) == payload.get(k) for k in  # a stamped work_owner is not the caller's
                        ("issue_id", "body", "expected_description", "id", "session_key", "work_owner") if k in payload or k != "work_owner"):
                    return row_id
                raise ValueError("outbox id reused with conflicting content")
            targets = self._targets(payload)
            if payload.get("content_action"):
                if (not self.conversation_reply(kind, payload) and not self._admitted(db, *targets)) or (not self.independent_message(kind, payload) and
                        any(self._unresolved_terminal(db, target) for target in targets)): return ""
            elif self.reporting_notice(kind, payload):
                if not self._admitted(db, *targets): return ""
            elif not self._effect_admitted(db, *targets): return ""
            work = db.execute("SELECT ownership_id FROM work WHERE issue_id=?", (payload.get("issue_id"),)).fetchone()
            body = json.dumps({**payload, "work_owner": work["ownership_id"] if work and not payload.get("content_action") and not self.reporting_notice(kind, payload) else None,
                               "enqueued_at": payload.get("enqueued_at", now)})
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)", (row_id, kind, body, now))
        return row_id
    def enqueue_many(self, writes: list[tuple[str, dict[str, Any]]], *, at: float) -> bool:
        with self._tx() as db:
            if any(not self._effect_admitted(db, *self._targets(payload)) for _, payload in writes): return False
            for kind, payload in writes:
                work = db.execute("SELECT ownership_id FROM work WHERE issue_id=?", (payload.get("issue_id"),)).fetchone()
                if work and not self.reporting_notice(kind, payload): payload = {**payload, "work_owner": work["ownership_id"]}
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
        """Oldest pending issue write per issue, if due, plus any due thought: issue writes go out in order."""
        def eligible(alias: str) -> str:
            dependency = f"json_extract({alias}.payload, '$.requires_status_id')"
            return (f"({alias}.hold_reason IS NULL AND NOT EXISTS (SELECT 1 FROM outbox h WHERE h.id={dependency} "
                    "AND json_extract(h.payload, '$.reconcile_required')=1 AND "
                    f"({alias}.kind != 'project_update' OR json_extract({alias}.payload, '$.issue_id')="
                    "json_extract(h.payload, '$.issue_id'))) AND "
                    f"({alias}.kind = 'project_update' OR COALESCE({dependency}, '') = '' OR "
                    f"EXISTS (SELECT 1 FROM outbox s WHERE s.id = {dependency} "
                    "AND (s.state = 'sent' OR (s.state = 'failed' AND (s.attempts = 0 OR EXISTS ("
                    "SELECT 1 FROM outbox l WHERE l.kind = 'status' AND l.state = 'sent' AND l.rowid > s.rowid "
                    "AND json_extract(l.payload, '$.issue_id') = json_extract(s.payload, '$.issue_id'))))))))")
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            rows = db.execute(
                "SELECT o.* FROM outbox o WHERE o.state = 'pending' AND " + eligible("o") +
                " AND (" + ADVISORY + " OR (o.kind='activity' AND json_extract(o.payload, '$.native_reply_version')=1) OR o.rowid = ("  # thoughts never wait for, or hold, issue writes
                " SELECT MIN(p.rowid) FROM outbox p WHERE p.state = 'pending' AND " + eligible("p") +
                " AND NOT " + ADVISORY + " AND json_extract(p.payload, '$.issue_id') IS json_extract(o.payload, '$.issue_id')"
                ")) AND o.next_at <= ? ORDER BY COALESCE(json_extract(o.payload, '$.initial_ack'), 0) DESC, o.rowid", (now,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]
    def pending(self, issue_id: str | None = None) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM outbox WHERE state = 'pending' AND (? IS NULL OR "
                              "json_extract(payload, '$.issue_id') = ?) ORDER BY rowid", (issue_id, issue_id)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]
    def last_activity_at(self, issue_id: str, session_id: str) -> float:
        """When this session last showed, or is about to show, an activity; dropped ones do not count."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT MAX(json_extract(payload, '$.enqueued_at')) FROM outbox WHERE hold_reason IS NULL AND kind = 'activity' "
                             "AND (state = 'pending' OR (state = 'sent' AND COALESCE(json_extract(payload, '$.applied'), 1) = 1)) AND "
                             "json_extract(payload, '$.issue_id') = ? AND json_extract(payload, '$.session_id') = ?",
                             (issue_id, session_id)).fetchone()
        return float(row[0] or 0)
    def unreported_failed(self) -> list[dict[str, Any]]:
        with self._tx() as db:
            rows = db.execute("SELECT * FROM outbox WHERE state = 'failed' AND hold_reason IS NULL AND NOT " + ADVISORY + " AND "
                              "COALESCE(json_extract(payload, '$.reported'), 0) = 0 ORDER BY rowid").fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]
    def retire_advisory(self, row_id: str, now: float) -> None:
        """Retire a due, unsent thought; one a quota pause deferred keeps its place. It changes no issue: no fence applies."""
        with self._tx() as db:
            db.execute("UPDATE outbox SET state = 'failed' WHERE id = ? AND state = 'pending' AND hold_reason IS NULL AND next_at <= ? AND "
                       + ADVISORY, (row_id, now))
    def mark(self, row_id: str, state: str, error: str | None = None) -> None:
        with self._tx() as db:
            if not self._row_admitted(db, row_id): return
            db.execute("UPDATE outbox SET state = ?, payload = CASE WHEN ? IS NULL THEN payload ELSE "
                       "json_set(payload, '$.last_error', ?) END WHERE id = ?", (state, error, error, row_id))
    def mark_sent(self, row_id: str, applied: bool) -> bool:
        with self._tx() as db:
            if not self._row_admitted(db, row_id): return False
            db.execute("UPDATE outbox SET state = 'sent', payload = json_set(payload, '$.applied', ?) WHERE id = ?",
                       (int(applied), row_id))
            return True
    def native_status_receipt(self, issue_id: str, task_id: str) -> dict | None:
        """Read the latest acknowledged issue write for this native binding."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT payload FROM outbox WHERE kind IN ('status','delegate') AND state='sent' AND hold_reason IS NULL "
                             "AND json_extract(payload,'$.applied')=1 AND json_extract(payload,'$.issue_id')=? "
                             "AND json_extract(payload,'$.task_id')=? AND json_type(payload,'$.issue_write_receipt')='object' "
                             "ORDER BY rowid DESC LIMIT 1", (issue_id, task_id)).fetchone()
        return json.loads(row[0])["issue_write_receipt"] if row else None
    # One delegate write at a time; unrelated rows (status, comments, activities,
    # project updates) never starve delegate recovery.
    _DELEGATE_PENDING = ("SELECT 1 FROM outbox WHERE kind='delegate' AND state='pending' "
                         "AND json_extract(payload, '$.issue_id')=? LIMIT 1")
    def delegate_routed(self, issue_id: str, revision: str) -> bool:
        """A delegate intent exists for this binding revision, or another delegate write is pending."""
        with self._tx() as db:
            return bool(db.execute(self._DELEGATE_PENDING, (issue_id,)).fetchone() or db.execute(
                "SELECT 1 FROM outbox WHERE kind='delegate' AND json_extract(payload, '$.issue_id')=? "
                "AND json_extract(payload, '$.expected_binding_revision')=? LIMIT 1", (issue_id, revision)).fetchone())
    def recover_binding_delegate(self, issue_id: str, payload: dict[str, Any], *, at: float) -> bool:
        """Queue one recovery delegate intent; no caller/requester or work_owner fields are invented."""
        with self._tx() as db:
            if not self._effect_admitted(db, issue_id) or not db.execute(
                    "SELECT 1 FROM work WHERE issue_id=? AND origin='native' AND task_id=?",
                    (issue_id, payload["task_id"])).fetchone():
                return False
            if db.execute("SELECT 1 FROM outbox WHERE id=? OR (kind='delegate' AND json_extract(payload, '$.issue_id')=? "
                          "AND json_extract(payload, '$.expected_binding_revision')=?)",
                          (payload["id"], issue_id, payload["expected_binding_revision"])).fetchone():
                return False
            if db.execute(self._DELEGATE_PENDING, (issue_id,)).fetchone():
                return False  # One delegate write at a time; routing history fences the rest.
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                       (payload["id"], "delegate", json.dumps({**payload, "enqueued_at": at}), at))
            return True
    def reconcile_status(self, issue_id: str, payload: dict[str, Any], *, at: float) -> bool:
        """Queue a native-derived state only when the newest intent differs.

        A refused In Progress is retried once per claim. Pending native messages
        are sequenced after it, so people read them under the current state.
        """
        with self._tx() as db:
            if not self._effect_admitted(db, issue_id) or not db.execute(
                    "SELECT 1 FROM work WHERE issue_id=? AND origin='native' AND task_id=?",
                    (issue_id, payload.get("task_id"))).fetchone():
                return False
            latest = db.execute(self._LATEST_STATUS, (issue_id,)).fetchone()
            claim = (payload.get("recovered_claim") or {}).get("event_id")
            retry = (latest and claim is not None and self._refused_in_progress(latest)
                     and latest["claim_event"] != claim)
            if latest and latest["intent"] == payload["state"] and not retry:
                return False
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, ?, ?, ?)",
                       (str(uuid.uuid4()), "status", json.dumps({**payload, "enqueued_at": at}), at))
            self._sequence_native_history(db, issue_id)
            return True
    _LATEST_STATUS = ("SELECT id, state AS row_state, hold_reason, json_extract(payload, '$.state') AS intent, "
                      "json_extract(payload, '$.enqueued_at') AS at, json_extract(payload, '$.applied') AS applied, "
                      "json_extract(payload, '$.recovered_claim.event_id') AS claim_event FROM outbox "
                      "WHERE kind='status' AND json_extract(payload, '$.issue_id')=? "
                      "ORDER BY json_extract(payload, '$.enqueued_at') DESC, rowid DESC LIMIT 1")
    @staticmethod
    def _refused_in_progress(row) -> bool:
        """An In Progress intent that was delivered as a no-op: Linear does not show it."""
        return (row["intent"] == "in_progress" and row["row_state"] == "sent" and not row["applied"]
                and row["hold_reason"] is None)
    def latest_status(self, issue_id: str) -> dict[str, Any] | None:
        """Newest status intent, including a pending or failed closeout.

        ``refused`` marks an In Progress intent that was refused (not applied),
        so it is not mistaken for Linear already showing In Progress;
        ``recovered`` marks a background claim recovery row.
        """
        with self._tx() as db:
            row = db.execute(self._LATEST_STATUS, (issue_id,)).fetchone()
        if not row or not row["intent"]: return None
        return {"id": row["id"], "state": row["intent"], "refused": self._refused_in_progress(row),
                "recovered": row["claim_event"] is not None}
    def verified_release(self, row_id: str) -> bool:
        row = self.outbox_row(row_id)
        return bool(row and not row.get("hold_reason") and row["state"] == "sent" and row["kind"] == "status"
                    and row["payload"].get("release") and row["payload"].get("applied"))
    @staticmethod
    def history_status_settled(row: dict | None) -> bool:
        if not row or row["kind"] != "status" or row.get("hold_reason"):
            return False
        payload = row["payload"]
        if row["state"] == "sent" and payload.get("applied") == 1:
            return True
        return bool(payload.get("write_started") is False and (
            (row["state"] == "sent" and payload.get("applied") == 0) or
            (row["state"] == "failed" and row["attempts"] == 0)))

    def terminal_status_applied(self, row_id: str) -> bool:
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT state, kind, json_extract(payload, '$.applied') FROM outbox WHERE hold_reason IS NULL AND id = ?",
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
    def retry(self, row: dict[str, Any], now: float, error: str | None = None) -> bool:
        """Back off 1 min doubling to 1 h. Returns True when the row is given up (24 h old)."""
        attempts = int(row["attempts"]) + 1
        if now - float(row["payload"].get("enqueued_at", now)) >= GIVE_UP_AFTER:
            with self._tx() as db:
                if not self._row_admitted(db, row["id"]): return False
                db.execute("UPDATE outbox SET state = 'failed', attempts = ?, payload = CASE WHEN ? IS NULL THEN payload "
                           "ELSE json_set(payload, '$.last_error', ?) END WHERE id = ?", (attempts, error, error, row["id"]))
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
            self._hold_legacy_outbox(db)
            rows = db.execute(
                "SELECT id, payload FROM outbox WHERE state = 'failed' AND hold_reason IS NULL AND attempts > 0 AND "
                "COALESCE(json_extract(payload, '$.reconcile_required'), 0)=0 AND NOT " + ADVISORY + " AND NOT ("
                " kind = 'status' AND EXISTS (SELECT 1 FROM outbox l WHERE l.kind = 'status' AND l.rowid > outbox.rowid"
                " AND json_extract(l.payload, '$.issue_id') = json_extract(outbox.payload, '$.issue_id')))",
                ).fetchall()
            revived = 0
            for row in rows:
                if self._admitted(db, *self._targets(json.loads(row["payload"]))):
                    db.execute("UPDATE outbox SET state='pending', next_at=? WHERE id=?", (now, row["id"]))
                    revived += 1
            return revived
    @staticmethod
    def _latest_project_update(db: sqlite3.Connection, session_id: str, project_id: str | None):
        return db.execute("SELECT * FROM outbox WHERE kind = 'project_update' AND hold_reason IS NULL AND "
                          "json_extract(payload, '$.session_id') = ? AND json_extract(payload, '$.project_id') IS ? "
                          "ORDER BY CASE state WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, "
                          "rowid DESC LIMIT 1", (session_id, project_id)).fetchone()
    def project_update(self, session_id: str, project_id: str) -> dict[str, Any] | None:
        with self._tx() as db:
            row = self._latest_project_update(db, session_id, project_id)
        return {**dict(row), "payload": json.loads(row["payload"])} if row else None
    def outbox_row(self, row_id: str) -> dict[str, Any] | None:
        with self._tx() as db:
            row = db.execute("SELECT * FROM outbox WHERE id = ?", (row_id,)).fetchone()
        return {**dict(row), "payload": json.loads(row["payload"])} if row else None
    @staticmethod
    def _merge_requesters(prior, payload):
        # An existing member retains its original evidence coordinates, including
        # legacy creator-based evidence. A later Activity cannot thaw its request.
        if not payload.get("requester_sessions"):
            return
        sessions = prior.setdefault("requester_sessions", {})
        for issue, session in payload.get("requester_sessions", {}).items():
            if issue not in sessions:
                sessions[issue] = session
                activity = (payload.get("requester_activities") or {}).get(issue)
                if activity:
                    prior.setdefault("requester_activities", {})[issue] = activity
    @staticmethod
    def _requester_conflict(prior, payload):
        old, new = prior.get("requester_sessions") or {}, payload.get("requester_sessions") or {}
        return bool(old) != bool(new) or any(key in old and old[key] != value for key, value in new.items())
    def queue_project_update(self, payload: dict[str, Any], *, due: float, quiet: bool) -> bool:
        """Merge a session's update atomically with terminal capture in finish()."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            if not self._effect_admitted(db, *self._targets(payload)): return False
            row = self._latest_project_update(db, payload["session_id"], payload["project_id"])
            if row and self._requester_conflict(json.loads(row["payload"]), payload): row = None
            if row:
                prior = json.loads(row["payload"])
                if (row["state"] not in ("pending", "failed") or prior.get("frozen") or
                        prior.get("write_started") is True or
                        ("write_started" not in prior and prior.get("send_body"))):
                    return True
                if prior.get("followup") and not set(payload["lines"]) <= set(prior["lines"]):
                    # A split remainder contains only its original unpublished
                    # members. Later same-session updates cannot reintroduce a
                    # published line or expand the once-per-project/session batch.
                    return True
                if not self._admitted(db, *self._targets(prior)): return False
                prior["lines"].update(payload["lines"])
                prior.setdefault("line_issues", {}).update(payload.get("line_issues", {}))
                self._merge_requesters(prior, payload)
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
    def project_not_applied(self, row_id: str, *, known_rejection: bool = False, started_before=None) -> bool:
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT payload FROM outbox WHERE id=? AND kind='project_update' AND state='pending' AND hold_reason IS NULL", (row_id,)).fetchone()
            if not row:
                return False
            payload = json.loads(row["payload"])
            if known_rejection:
                prior = payload.get("write_started") if started_before is None else started_before
                legacy_create = prior is None and payload.get("send_body") is not None
                # Already started, or a prepared legacy create: do not thaw and do not retry.
                # A missing marker with no send body has not started, even if attempts > 0.
                if prior is True or legacy_create:
                    return False
                if payload.get("send_body") is None:
                    return True
                payload["frozen"] = True
                payload["write_started"] = False
                db.execute("UPDATE outbox SET payload=? WHERE id=?", (json.dumps(payload), row_id))
                return True
            if payload.get("write_started") is not False:
                return False
            db.execute("UPDATE outbox SET payload=json_set(payload, '$.frozen', json('false'), '$.write_started', json('false')) WHERE id=?", (row_id,))
            return True
    def freeze_project_update(self, row_id: str, expected: dict[str, Any], now: float,
                              body: str, project_id: str | None) -> bool | None:
        """Freeze the preflighted body only if it is still unchanged and due."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT * FROM outbox WHERE id = ? AND kind = 'project_update' AND state = 'pending' AND hold_reason IS NULL",
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
                # write_started true, or a prepared body with no marker, must not be
                # reprojected. Attempts alone are pre-send retries and may still freeze.
                if payload.get("write_started") is True or (
                        "write_started" not in payload and payload.get("send_body")):
                    return False
                payload["frozen"] = True
                payload["write_started"] = False
                payload["send_body"] = body
                payload["send_project_id"] = project_id
                db.execute("UPDATE outbox SET payload = ? WHERE id = ?", (json.dumps(payload), row_id))
        return True
    def split_project_update(self, row_id: str, expected: dict[str, Any], now: float,
                             waiting: set[str]) -> bool | None:
        """Keep status-blocked lines pending while the eligible lines use this client id."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT payload, next_at FROM outbox WHERE id=? AND kind='project_update' "
                             "AND state='pending' AND hold_reason IS NULL", (row_id,)).fetchone()
            if not row or row["next_at"] > now:
                return False
            if json.loads(row["payload"]) != expected:
                return None
            if not self._admitted(db, *self._targets(expected)): return False
            # The exact unstarted row may partition again as original held
            # members regain scope. Sent siblings do not own this request ID.
            if expected.get("write_started") is True or expected.get("frozen") or (
                    "write_started" not in expected and expected.get("send_body")):
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
                if part.get("task_id"):
                    # Each split keeps a native reporting anchor belonging to
                    # its own lines, never the removed sibling's task identity.
                    work = db.execute("SELECT task_id FROM work WHERE issue_id=? AND origin='native'",
                                      (part["resolve"],)).fetchone()
                    if work:
                        part["task_id"] = work["task_id"]
            db.execute("UPDATE outbox SET payload=? WHERE id=?", (json.dumps(ready), row_id))
            db.execute("INSERT INTO outbox (id, kind, payload, next_at) VALUES (?, 'project_update', ?, ?)",
                       (str(uuid.uuid4()), json.dumps(deferred), row["next_at"]))
        return True
    def drop_unpublished_update(self, row_id: str, expected: dict[str, Any], now: float) -> bool | None:
        """Forget an empty batch only if no valid line merged during preflight."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            row = db.execute("SELECT payload, next_at FROM outbox WHERE id = ? AND kind = 'project_update' "
                             "AND state = 'pending' AND hold_reason IS NULL", (row_id,)).fetchone()
            if not row or row["next_at"] > now:
                return False
            if json.loads(row["payload"]) != expected:
                return None
            if not self._admitted(db, *self._targets(expected)): return False
            db.execute("DELETE FROM outbox WHERE id = ?", (row_id,))
        return True
    def pending_claim(self, issue_id: str) -> bool:
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            return bool(db.execute(
                "SELECT 1 FROM outbox c WHERE c.hold_reason IS NULL AND c.kind = 'status' AND "
                "json_extract(c.payload, '$.issue_id') = ? AND json_extract(c.payload, '$.claim') = 1 AND "
                "(c.state = 'pending' OR (c.state = 'failed' AND c.attempts > 0 AND NOT EXISTS ("
                "SELECT 1 FROM outbox l WHERE l.kind = 'status' AND l.rowid > c.rowid AND "
                "json_extract(l.payload, '$.issue_id') = ?))) LIMIT 1", (issue_id, issue_id)).fetchone())
    def progress_reported(self, issue_id: str, task_id: str, action: str) -> bool:
        """A native progress milestone for this binding is queued, published, or may have landed.

        Progress coalesced unsent (sent with applied=0) or given up before any send attempt
        never reached Linear, so it does not count and the next spawn says it again.
        A timed-out send (failed after an attempt) may have landed and is revived with the
        same client id, so it counts.
        """
        with self._tx() as db:
            return bool(db.execute(
                "SELECT 1 FROM outbox WHERE hold_reason IS NULL AND kind IN ('activity', 'comment') AND "
                "(state = 'pending' OR (state = 'sent' AND COALESCE(json_extract(payload, '$.applied'), 1) = 1) "
                "OR (state = 'failed' AND attempts > 0)) AND "
                "json_extract(payload, '$.issue_id') = ? AND json_extract(payload, '$.task_id') = ? AND "
                "json_extract(payload, '$.native_progress') = 1 AND "
                "(json_extract(payload, '$.content.action') = ? OR "
                "substr(json_extract(payload, '$.body'), 1, length(?) + 1) = ? || ':') LIMIT 1",
                (issue_id, task_id, action, action, action)).fetchone())
    def has_start_ack(self, issue_id: str, task_id: str) -> bool:
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            return bool(db.execute("SELECT 1 FROM outbox WHERE hold_reason IS NULL AND kind='status' AND "
                                   "json_extract(payload, '$.issue_id')=? AND "
                                   "json_extract(payload, '$.task_id')=? AND "
                                   "json_extract(payload, '$.claim')=1 LIMIT 1", (issue_id, task_id)).fetchone())
    def status_pending(self, row_id: str) -> bool:
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            return bool(db.execute("SELECT 1 FROM outbox WHERE hold_reason IS NULL AND id = ? AND kind = 'status' AND "
                                   "(state = 'pending' OR (state = 'failed' AND attempts > 0))",
                                   (row_id,)).fetchone())
    def report(self, row_id: str) -> None:
        with self._tx() as db:
            if not self._row_admitted(db, row_id): return
            db.execute("UPDATE outbox SET payload = json_set(payload, '$.reported', 1) WHERE id = ?", (row_id,))
    def rewrite(self, row_id: str, payload: dict[str, Any], next_at: float) -> None:
        with self._tx() as db:
            if not self._row_admitted(db, row_id): return
            row = db.execute("SELECT kind FROM outbox WHERE id=?", (row_id,)).fetchone()
            if not self.conversation_reply(row["kind"], payload) and not self._admitted(db, *self._targets(payload)): return
            db.execute("UPDATE outbox SET payload = ?, next_at = ? WHERE id = ?", (json.dumps(payload), next_at, row_id))
    def delay_session_updates(self, session_id: str, next_at: float, *, turn_id: str = "") -> None:
        """Quiet period: each identified turn retains its first captured deadline across replay."""
        with self._tx() as db:
            self._hold_legacy_outbox(db)
            rows = db.execute("SELECT id, payload FROM outbox WHERE kind='project_update' AND state='pending' AND hold_reason IS NULL AND "
                              "COALESCE(json_extract(payload, '$.frozen'), 0)=0 AND "
                              "json_extract(payload, '$.session_id')=?", (session_id,)).fetchall()
            if turn_id and rows:
                key = json.dumps(["turn_end", session_id, turn_id])
                db.execute("INSERT OR IGNORE INTO metadata(key,value) VALUES (?,?)", (key, str(next_at)))
                next_at = float(db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()[0])
            for row in rows:
                if self._admitted(db, *self._targets(json.loads(row["payload"]))):
                    db.execute("UPDATE outbox SET next_at=MAX(next_at, ?) WHERE id=?", (next_at, row["id"]))
