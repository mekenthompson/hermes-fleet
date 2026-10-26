"""Durable delivery records do not become a second task ownership system."""
import json
import sqlite3
import tempfile
import unittest
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
                                             "content_action": True, "session_key": "different-channel"}, at=101)
        self.assertTrue(self.store.admit_mutation(row))
        self.assertEqual(self.store.get("legacy"), before)
        self.assertIsNone(self.store.outbox_row(row)["payload"].get("work_owner"))
        description = self.store.enqueue("description", {"issue_id": "unlinked", "body": "New text", "id": "edit-1",
                                                         "expected_description": "Old", "content_action": True}, at=101)
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
