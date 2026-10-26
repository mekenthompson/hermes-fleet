"""Durable delivery records do not become a second task ownership system."""
import json
import sqlite3
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from linear_fake_api import load_plugin

load_plugin()
from hermes_fleet_linear_plugin.store import Store


class NativeStoreTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "state.db"
        self.store = Store(self.path)

    def bind(self, issue="issue-1", task="task-1"):
        return self.store.bind_native(issue, task, "alpha", "default", at=100)

    def replace(self, task="replacement", *, issue="issue-1", revision=None, request_id=None,
                owner_ref="new-session", project_id="project-1", at=200):
        return self.store.replace_native_binding(
            issue, task, "alpha", "default",
            expected_revision=self.store.binding_revision(issue) if revision is None else revision,
            request_id=request_id or str(uuid.uuid4()), owner_ref=owner_ref, project_id=project_id, at=at)

    def rows(self, table):
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY rowid")]

    def outbox(self, row_id, kind, payload, *, state="pending", attempts=0, next_at=250):
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO outbox(id,kind,payload,attempts,next_at,state) VALUES (?,?,?,?,?,?)",
                       (row_id, kind, json.dumps(payload), attempts, next_at, state))

    def test_binding_is_idempotent_and_cannot_adopt_replace_or_expand_scope(self):
        self.assertTrue(self.bind())
        self.assertTrue(self.bind())
        self.assertFalse(self.bind(task="task-2"))
        self.assertIsNone(self.store.native_binding("issue-1", "beta", "default"))
        self.assertIsNone(self.store.native_binding("issue-1", "alpha", "other-board"))
        self.assertEqual(self.store.native_binding("issue-1")["task_id"], "task-1")
        self.assertIsNone(self.store.get("issue-1")["ownership_id"])
        self.assertTrue(self.store.put("legacy", "chat", "old-conversation", run_generation=3))
        before = self.store.get("legacy")
        self.assertFalse(self.bind("legacy"))
        self.assertEqual(self.store.get("legacy"), before)
        self.assertIsNone(self.store.native_binding("legacy"))

    def test_concurrent_deliveries_reserve_only_one_native_attempt(self):
        payload = {"issue_id": "issue-1", "kind": "create", "key": "stable-key"}
        def prepare(_):
            return Store(self.path).native_intent_prepare("request-1", payload)
        with ThreadPoolExecutor(max_workers=8) as pool:
            attempts = list(pool.map(prepare, range(16)))
        self.assertEqual(sum(fresh for _, fresh in attempts), 1)
        self.assertEqual({saved["state"] for saved, _ in attempts}, {"attempting"})
        # Reopening a process after an uncertain effect cannot grant a new send.
        self.assertFalse(Store(self.path).native_intent_prepare("request-1", payload)[1])
        with self.assertRaises(ValueError):
            self.store.native_intent_prepare("request-1", {**payload, "key": "different"})

    def test_binding_revision_covers_exact_work_binding_and_absence(self):
        absent = self.store.binding_revision("issue-1")
        self.assertEqual(absent, Store(self.path).binding_revision("issue-1"))
        self.assertNotEqual(absent, self.store.binding_revision("other-issue"))
        self.assertTrue(self.bind())
        linked = self.store.binding_revision("issue-1")
        self.assertNotEqual(absent, linked)
        self.store.update("issue-1", last_event_id=4)
        self.assertNotEqual(linked, self.store.binding_revision("issue-1"))
        before = self.store.binding_revision("issue-1")
        with sqlite3.connect(self.path) as db:
            binding = db.execute("SELECT value FROM metadata WHERE key='native-binding:issue-1'").fetchone()[0]
            db.execute("UPDATE metadata SET value=? WHERE key='native-binding:issue-1'", (binding + " ",))
        self.assertNotEqual(before, self.store.binding_revision("issue-1"))

    def test_explicit_replacement_archives_legacy_history_and_preserves_effect_guards(self):
        self.assertTrue(self.store.put("issue-1", "kanban_chat", "old-session", task_id="old-task",
                                       project_id="old-project", last_updated_at=70, run_generation=4))
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE work SET stop_requested_at=75,resume_fence_at=74,ownership_id='old-owner',"
                       "release_pending=1,pending_resume='saved resume',panel_note='saved note' WHERE issue_id='issue-1'")
            db.execute("INSERT INTO retirement VALUES ('old-task','issue-1','original retirement')")
            db.execute("INSERT INTO chat_stop VALUES ('stop-1','alpha','issue-1','old-linear','activity-1',"
                       "'old-key',4,'accepted','unknown',NULL,NULL)")
            db.execute("INSERT INTO chat_admissions VALUES ('admission-1','issue-1','alpha','/old-home',"
                       "'old-key','old-session',4,'old-owner','old-task','ambiguous',NULL,NULL)")
            db.execute("INSERT INTO metadata VALUES ('chat-transfer:old',?)",
                       (json.dumps({"issue_id": "issue-1", "state": "prepared"}),))
        self.outbox("uncertain", "status", {"issue_id": "issue-1", "task_id": "old-task",
                                            "terminal": True, "write_started": True}, state="failed", attempts=3)
        self.outbox("independent", "comment", {"issue_id": "issue-1", "body": "New discussion",
                                                "content_action": True, "unlinked_content": True})
        original = self.store.get("issue-1")
        retained = {table: self.rows(table) for table in ("retirement", "chat_stop", "chat_admissions")}
        uncertain = self.store.issue_reconciliation_blocked("issue-1")
        request_id = str(uuid.uuid4())
        self.assertFalse(self.bind())  # Ordinary tracking still cannot adopt legacy work.
        self.assertEqual(self.replace(request_id=request_id)["task_id"], "replacement")
        receipt = self.store.native_intent(f"track:issue-1:{request_id}")
        self.assertEqual(receipt["payload"]["previous"]["work"], original)
        self.assertIsNone(receipt["payload"]["previous"]["native_binding"])
        self.assertEqual(receipt["state"], "applied")
        for table, before in retained.items():
            self.assertEqual(self.rows(table), before)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(json.loads(db.execute("SELECT value FROM metadata WHERE key='chat-transfer:old'").fetchone()[0]),
                             {"issue_id": "issue-1", "state": "prepared"})
        self.assertEqual(self.store.issue_reconciliation_blocked("issue-1"), uncertain)
        self.assertTrue(self.store.admit_mutation("independent"))
        self.assertEqual(self.store.get("issue-1")["last_event_id"], 0)

    def test_replacement_holds_old_reports_and_sent_receipts_without_rewriting_history(self):
        self.assertTrue(self.bind(task="old-task"))
        self.assertTrue(self.bind("other-issue", "other-task"))
        self.outbox("old-report", "comment", {"issue_id": "issue-1", "task_id": "old-task"}, attempts=2)
        self.outbox("old-sent", "status", {"issue_id": "issue-1", "task_id": "old-task"}, state="sent", attempts=1)
        self.outbox("old-batch", "project_update", {"issue_id": "update:project-1", "task_id": "old-task",
                                                    "line_issues": {"first": "issue-1", "second": "other-issue"}})
        self.outbox("dependent", "comment", {"issue_id": "other-issue", "task_id": "other-task",
                                               "requires_status_id": "old-sent"})
        self.outbox("independent", "comment", {"issue_id": "issue-1", "content_action": True,
                                                 "unlinked_content": True, "body": "Current discussion"})
        self.outbox("description", "description", {"issue_id": "issue-1", "content_action": True,
                                                     "unlinked_content": True, "write_started": True}, attempts=1)
        self.outbox("unrelated", "comment", {"issue_id": "other-issue", "task_id": "other-task"})
        before = {row["id"]: row for row in self.rows("outbox")}
        self.assertTrue(self.replace())
        after = {row["id"]: row for row in self.rows("outbox")}
        for row_id in ("old-report", "old-sent", "old-batch", "dependent"):
            self.assertTrue(after[row_id]["hold_reason"], row_id)
            self.assertEqual({k: v for k, v in after[row_id].items() if k != "hold_reason"},
                             {k: v for k, v in before[row_id].items() if k != "hold_reason"})
            self.assertFalse(self.store.delivery_allowed(row_id))
        for row_id in ("independent", "description", "unrelated"):
            self.assertEqual(after[row_id], before[row_id])
        self.assertTrue(self.store.delivery_allowed("independent"))
        self.assertTrue(self.store.issue_reconciliation_blocked("issue-1"))

    def test_native_history_is_archived_and_same_target_preserves_cursor(self):
        self.assertTrue(self.bind())
        self.store.update("issue-1", last_event_id=12, panel_note="old note")
        original = self.store.get("issue-1")
        with sqlite3.connect(self.path) as db:
            raw_binding = db.execute("SELECT value FROM metadata WHERE key='native-binding:issue-1'").fetchone()[0]
        request_id = str(uuid.uuid4())
        self.assertTrue(self.replace(task="task-1", request_id=request_id))
        self.assertEqual(self.store.get("issue-1")["last_event_id"], 12)
        self.assertEqual(self.store.get("issue-1")["panel_note"], "old note")
        receipt = self.store.native_intent(f"track:issue-1:{request_id}")
        self.assertEqual(receipt["payload"]["previous"], {"work": original, "native_binding": raw_binding})
        self.assertTrue(self.replace(task="new-target"))
        self.assertEqual(self.store.get("issue-1")["last_event_id"], 0)

    def test_same_target_and_reporting_scope_is_a_noop_with_a_retained_receipt(self):
        self.assertTrue(self.bind())
        self.store.update("issue-1", last_event_id=9, last_updated_at=110)
        self.outbox("current-report", "comment", {"issue_id": "issue-1", "task_id": "task-1"})
        before, revision = self.store.get("issue-1"), self.store.binding_revision("issue-1")
        self.assertTrue(self.replace(task="task-1", owner_ref="", project_id=None))
        self.assertEqual(self.store.get("issue-1"), before)
        self.assertEqual(self.store.binding_revision("issue-1"), revision)
        self.assertIsNone(self.store.outbox_row("current-report")["hold_reason"])

    def test_superseded_holds_preserve_existing_reasons_and_leave_unrelated_dependencies_alone(self):
        self.assertTrue(self.bind())
        self.outbox("old-report", "comment", {"issue_id": "issue-1", "task_id": "task-1"})
        self.outbox("old-dependent", "comment", {"issue_id": "other-issue", "requires_status_id": "old-report"})
        self.outbox("unrelated-hold", "comment", {"issue_id": "unrelated"})
        self.outbox("unrelated-dependent", "comment", {"issue_id": "unrelated", "requires_status_id": "unrelated-hold"})
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE outbox SET hold_reason='Original reason' WHERE id IN ('old-report','unrelated-hold')")
        before = {row["id"]: row for row in self.rows("outbox")}
        self.assertTrue(self.replace())
        after = {row["id"]: row for row in self.rows("outbox")}
        self.assertEqual(after["old-report"], before["old-report"])
        self.assertTrue(after["old-dependent"]["hold_reason"])
        self.assertEqual(after["unrelated-hold"], before["unrelated-hold"])
        self.assertEqual(after["unrelated-dependent"], before["unrelated-dependent"])

    def test_stale_revision_and_scope_fence_leave_all_records_unchanged(self):
        self.assertTrue(self.bind())
        revision = self.store.binding_revision("issue-1")
        self.store.update("issue-1", last_event_id=1)
        before = {table: self.rows(table) for table in ("work", "metadata", "outbox")}
        self.assertIsNone(self.replace(revision=revision))
        for table, rows in before.items():
            self.assertEqual(self.rows(table), rows)
        self.store.fence_scope("issue-1", "requester no longer allowed", at=101)
        revision = self.store.binding_revision("issue-1")
        self.assertIsNone(self.replace(revision=revision))
        self.assertTrue(self.store.scope_fenced("issue-1"))
        self.assertEqual(self.store.native_binding("issue-1")["task_id"], "task-1")

    def test_concurrent_replacements_have_one_winner(self):
        self.assertTrue(self.bind())
        revision = self.store.binding_revision("issue-1")
        def replace(index):
            return Store(self.path).replace_native_binding(
                "issue-1", f"task-{index}", "alpha", "default", expected_revision=revision,
                request_id=str(uuid.uuid4()), at=200)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(replace, range(2, 10)))
        self.assertEqual(sum(result is not None for result in results), 1)
        winners = [row for row in self.rows("metadata") if row["key"].startswith("native-intent:track:")]
        self.assertEqual(len(winners), 1)

    def test_replacement_retry_is_idempotent_and_cannot_restore_a_superseded_target(self):
        self.assertTrue(self.bind())
        revision, request_id = self.store.binding_revision("issue-1"), str(uuid.uuid4())
        first = self.replace(revision=revision, request_id=request_id)
        before = {table: self.rows(table) for table in ("work", "metadata", "outbox")}
        self.assertEqual(self.replace(revision=revision, request_id=request_id, at=999), first)
        for table, rows in before.items():
            self.assertEqual(self.rows(table), rows)
        with self.assertRaises(ValueError):
            self.replace(task="conflicting", revision=revision, request_id=request_id)
        self.assertTrue(self.replace(task="latest"))
        self.assertIsNone(self.replace(revision=revision, request_id=request_id))
        self.assertEqual(self.store.native_binding("issue-1")["task_id"], "latest")

    def test_replacement_rolls_back_history_holds_and_projection_on_failure(self):
        self.assertTrue(self.bind())
        self.outbox("old", "comment", {"issue_id": "issue-1", "task_id": "task-1"})
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER refuse_binding BEFORE UPDATE ON metadata "
                       "WHEN OLD.key='native-binding:issue-1' BEGIN SELECT RAISE(ABORT,'injected failure'); END")
        before = {table: self.rows(table) for table in ("work", "metadata", "outbox")}
        with self.assertRaises(sqlite3.IntegrityError):
            self.replace()
        for table, rows in before.items():
            self.assertEqual(self.rows(table), rows)

    def test_uncertain_native_attempt_requires_readback_never_resets_to_ready(self):
        self.store.native_intent_prepare("request-1", {"kind": "comment", "issue_id": "issue-1"})
        self.store.native_intent_settle("request-1", {"state": "reconcile", "reason": "lost response"})
        saved, fresh = self.store.native_intent_prepare("request-1", {"kind": "comment", "issue_id": "issue-1"})
        self.assertFalse(fresh)
        self.assertEqual(saved["state"], "reconcile")
        self.store.native_intent_settle("request-1", {"state": "applied", "comment_id": 2})
        with self.assertRaises(ValueError):
            self.store.native_intent_settle("request-1", {"state": "attempting"})
        with self.assertRaises(ValueError):
            self.store.native_intent_settle("request-1", {"state": "applied", "comment_id": 3})

    def test_native_event_cursor_and_remote_intents_commit_together_and_keep_binding(self):
        self.assertTrue(self.bind())
        writes = [("comment", {"issue_id": "issue-1", "body": "Verified native result", "task_id": "task-1"})]
        self.assertTrue(self.store.capture_event("issue-1", 4, writes, at=101, forget=True))
        self.assertIsNotNone(self.store.native_binding("issue-1"))
        self.assertEqual(self.store.get("issue-1")["last_event_id"], 4)
        self.assertFalse(self.store.capture_event("issue-1", 4, writes, at=102))
        self.assertEqual(len(self.store.pending()), 1)
        with self.assertRaises(TypeError):
            self.store.capture_event("issue-1", 5, [("comment", {"issue_id": "issue-1", "body": object()})], at=103)
        self.assertEqual(self.store.get("issue-1")["last_event_id"], 4)
        self.assertEqual(len(self.store.pending()), 1)

    def test_unlinked_content_ignores_chat_claims_but_preserves_uncertain_effects(self):
        self.store.put("legacy", "chat", "original-chat")
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE work SET stop_requested_at=100,release_pending=1 WHERE issue_id='legacy'")
        before = self.store.get("legacy")
        row = self.store.enqueue("comment", {"issue_id": "legacy", "body": "Status question", "id": "new-comment",
                                             "content_action": True, "unlinked_content": True}, at=101)
        self.assertTrue(self.store.admit_mutation(row))
        self.assertEqual(self.store.get("legacy"), before)
        self.assertIsNone(self.store.outbox_row(row)["payload"].get("work_owner"))
        description = self.store.enqueue("description", {"issue_id": "unlinked", "body": "New text", "id": "edit-1",
                                                         "expected_description": "Old", "content_action": True, "unlinked_content": True}, at=101)
        self.assertTrue(self.store.admit_mutation(description))
        self.assertTrue(self.store.issue_reconciliation_blocked("unlinked"))
        self.assertEqual(self.store.enqueue("description", {"issue_id": "unlinked", "body": "Retry", "id": "edit-2",
                                                            "content_action": True}, at=102), "")

    def test_legacy_records_and_intents_are_preserved_without_activating_protocols(self):
        self.store.put("legacy", "kanban_chat", "original-chat", task_id="legacy-task")
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO retirement VALUES ('legacy-task','legacy','{}')")
            db.execute("INSERT INTO metadata VALUES ('chat-transfer:old',?)", (json.dumps({"issue_id": "legacy", "state": "prepared"}),))
            before = list(db.iterdump())
        Store(self.path)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(list(db.iterdump()), before)
        for removed in ("prepare_chat_transfer", "commit_chat_transfer", "resume_chat_transfer", "capture_chat_stop",
                        "prepare_chat_admission", "activate_task", "mark_recovery_delivered"):
            self.assertFalse(hasattr(self.store, removed), removed)
        self.assertFalse(self.bind("legacy"))


if __name__ == "__main__":
    unittest.main()
