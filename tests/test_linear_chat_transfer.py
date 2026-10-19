"""Transfer intent survives interruption without granting a successor execution."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

from linear_fake_api import Clock, FakeLinear, SELF, load_plugin

load_plugin()
from hermes_fleet_linear_plugin import chat
from hermes_fleet_linear_plugin.api import LinearAPI
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.store import Store


class ChatTransferTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "linear.db"
        self.clock = Clock()
        self.remote = FakeLinear(self.clock)
        self.addCleanup(self.remote.close)
        self.remote.add_issue("issue-1", "ABC-1")
        self.remote.set_delegate("issue-1", {"id": SELF, "name": "This Agent"})
        self.remote.set_state("issue-1", "Blocked")
        self.store = Store(self.path)
        self.store.put("issue-1", "chat", "old-chat", project_id="project-1",
                       run_generation=3, last_updated_at=self.clock() * 1000)
        self.owner = self.store.get("issue-1")
        self.bridge = Bridge(self.store, LinearAPI(lambda: "fixture", endpoint=self.remote.url),
                             SimpleNamespace(), profile="alpha", clock=self.clock)
        self.bridge.profile_home = self.path.parent
        self.bridge.validate_chat_admission = lambda context: context is self.context
        self.bridge.transfer_user_message = lambda context: "Transfer ABC-1 to this chat"
        self.bridge.retire_chat = lambda key, generation, request: {"status": "blocked", "effects": "unknown"}
        self.bridge.chat_lineage = lambda key: (key,)
        self.context = SimpleNamespace(session_key="new-chat", session_id="new-session",
                                       profile="alpha", platform="slack", run_generation=4)
        self.request_id = str(uuid.uuid4())

    def transfer(self, **changes):
        args = {"action": "transfer", "issue": "ABC-1", "id": self.request_id,
                "expected_owner": self.owner["ownership_id"]}
        return json.loads(chat.handle(self.bridge, {**args, **changes}, self.context))

    def retired(self, key="old-chat", generation=3, request=None):
        return {"kind": "chat-retirement/v1", "status": "retired", "execution_admitted": False,
                "session_key": key, "claim_generation": generation, "recovery_id": request,
                "profile_home": str(self.path.parent), "effects": "unknown"}

    def test_transfer_intent_reopens_without_assuming_the_old_executor_stopped(self):
        reply = self.transfer()
        self.assertEqual(reply.get("transfer_id"), self.request_id)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["code"], "retirement_pending")
        self.assertEqual(self.store.get("issue-1"), self.owner)
        saved = Store(self.path).chat_transfer(self.request_id)
        self.assertEqual(saved["state"], "prepared")
        self.assertEqual(saved["expected_owner"], self.owner["ownership_id"])
        self.assertEqual(saved["session_key"], "new-chat")
        self.assertEqual(saved["issue_id"], "issue-1")
        self.assertFalse(any(q.startswith("mutation") for q in self.remote.requests))

    def test_blocked_status_alone_never_authorizes_transfer(self):
        self.bridge.transfer_user_message = lambda _context: ""
        denied = self.transfer()
        self.assertFalse(denied["ok"])
        self.assertIsNone(self.store.chat_transfer(self.request_id))
        self.assertEqual(self.store.get("issue-1"), self.owner)


    def test_retry_reuses_one_durable_intent(self):
        first = self.transfer()
        before = Store(self.path).chat_transfer(self.request_id)
        self.clock.now += 10
        second = self.transfer()
        self.assertEqual(first, second)
        self.assertEqual(Store(self.path).chat_transfer(self.request_id), before)

    def test_request_id_cannot_move_to_a_different_chat(self):
        self.transfer()
        before = Store(self.path).chat_transfer(self.request_id)
        self.context.session_key = "another-chat"
        reply = self.transfer()
        self.assertFalse(reply["ok"])
        self.assertIn("identity conflicts", reply["message"])
        self.assertEqual(Store(self.path).chat_transfer(self.request_id), before)
        self.assertEqual(self.store.get("issue-1"), self.owner)

    def test_stale_expected_owner_does_not_prepare_a_transfer(self):
        reply = self.transfer(expected_owner=str(uuid.uuid4()))
        self.assertFalse(reply["ok"])
        self.assertIsNone(Store(self.path).chat_transfer(self.request_id))
        self.assertEqual(self.store.get("issue-1"), self.owner)

    def test_shared_predecessor_refuses_before_intent_or_core_retirement(self):
        self.assertTrue(self.store.put("issue-2", "chat", "old-chat", run_generation=3))
        calls = []
        self.bridge.retire_chat = lambda *args: (calls.append(args), self.retired(*args))[1]
        result = self.transfer()
        self.assertEqual(result["code"], "shared_predecessor")
        self.assertIsNone(self.store.chat_transfer(self.request_id))
        self.assertEqual(calls, [])
        self.assertEqual(self.store.get("issue-1"), self.owner)
        self.assertEqual(self.store.get("issue-2")["owner_ref"], "old-chat")

    def test_prepared_transfer_fences_old_mutations_but_not_another_issue(self):
        old_write = self.bridge.status("issue-1", "blocked")
        self.assertTrue(old_write)
        self.transfer()
        self.assertFalse(self.store.admit_mutation(old_write))
        self.context.session_key = "old-chat"
        refused = json.loads(chat.handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context))
        self.assertFalse(refused["ok"])
        self.assertEqual(self.store.get("issue-1"), self.owner)
        self.assertFalse(self.store.put("issue-2", "chat", "old-chat"))
        self.assertTrue(self.store.put("issue-2", "chat", "other-owner"))

    def test_legacy_stop_table_migrates_without_losing_exact_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.db"
            with sqlite3.connect(path) as db:
                db.execute("""CREATE TABLE chat_stop (
                  id TEXT PRIMARY KEY, profile TEXT NOT NULL, issue_id TEXT NOT NULL,
                  linear_session_id TEXT NOT NULL, source_activity_id TEXT NOT NULL,
                  session_key TEXT NOT NULL, run_generation INTEGER NOT NULL,
                  status TEXT NOT NULL DEFAULT 'requested', worker_completion TEXT NOT NULL DEFAULT 'unknown',
                  completion_activity_id TEXT, uncertainty_activity_id TEXT,
                  UNIQUE(profile, issue_id, linear_session_id, source_activity_id))""")
                db.execute("INSERT INTO chat_stop (id,profile,issue_id,linear_session_id,source_activity_id,"
                           "session_key,run_generation) VALUES ('saved','alpha','issue-1','linear-s',"
                           "'stop-a','old-chat',3)")
            reopened = Store(path)
            self.assertEqual(reopened.stop_intents()[0]["run_generation"], 3)
            with sqlite3.connect(path) as db:
                self.assertEqual(next(row[3] for row in db.execute("PRAGMA table_info(chat_stop)")
                                      if row[1] == "run_generation"), 0)

    def test_unsupported_runtime_does_not_prepare_or_fence(self):
        del self.bridge.retire_chat
        result = self.transfer()
        self.assertEqual(result["code"], "capability_unavailable")
        self.assertIsNone(self.store.chat_transfer(self.request_id))
        self.assertEqual(self.store.get("issue-1"), self.owner)
        self.assertTrue(self.bridge.status("issue-1", "blocked"))

    def test_stop_during_retirement_keeps_prepared_fence_and_old_owner(self):
        def retire(key, generation, request):
            self.store.update("issue-1", stop_requested_at=self.clock() * 1000)
            return self.retired(key, generation, request)
        self.bridge.retire_chat = retire
        result = self.transfer()
        self.assertFalse(result["ok"])
        self.assertEqual(self.store.chat_transfer(self.request_id)["state"], "prepared")
        self.assertEqual(self.store.get("issue-1")["owner_ref"], "old-chat")
        self.assertFalse(self.bridge.status("issue-1", "blocked"))

    def test_stop_after_control_handoff_is_durable_without_native_generation(self):
        self.bridge.retire_chat = self.retired
        self.assertTrue(self.store.update("issue-1", linear_session_id="linear-s"))
        self.assertTrue(self.transfer()["ok"])
        self.bridge.handle_webhook(self.remote.session_event(
            "prompted", "issue-1", "other-session", signal="stop", activity_id="wrong"))
        self.assertEqual(self.store.stop_intents(), [])
        self.clock.now += 1
        self.bridge.handle_webhook(self.remote.session_event(
            "prompted", "issue-1", "linear-s", signal="stop", activity_id="stop-a"))
        stops = Store(self.path).stop_intents()
        self.assertEqual(len(stops), 1)
        self.assertEqual(stops[0]["source_activity_id"], "stop-a")
        self.assertIsNone(stops[0]["run_generation"])
        self.assertEqual(stops[0]["status"], "unsupported")
        self.assertTrue(self.store.get("issue-1")["stop_requested_at"])
        self.assertFalse(self.store.chat_transfer(self.request_id)["execution_admitted"])
        notices = [row for row in self.store.pending("issue-1")
                   if row["payload"].get("stop_intent_id") == stops[0]["id"]]
        self.assertEqual({row["kind"] for row in notices}, {"status", "activity"})
        self.assertTrue(all(self.store.admit_mutation(row["id"]) for row in notices))

    def test_stop_after_resume_revokes_execution_admission(self):
        self.bridge.retire_chat = self.retired
        self.assertTrue(self.store.update("issue-1", linear_session_id="linear-s"))
        self.assertTrue(self.transfer()["ok"])
        link = "https://example.test/reconciled"
        self.bridge.transfer_user_message = lambda context: f"I reconciled ABC-1 at {link} and continue here"
        args = {"action": "resume_transfer", "issue": "ABC-1", "id": self.request_id, "evidence": link}
        self.assertTrue(json.loads(chat.handle(self.bridge, args, self.context))["ok"])
        queued = self.bridge.status("issue-1", "in_progress")
        self.assertTrue(queued)
        self.clock.now += 1
        self.bridge.handle_webhook(self.remote.session_event(
            "prompted", "issue-1", "linear-s", signal="stop", activity_id="stop-b"))
        self.assertEqual(len(Store(self.path).stop_intents()), 1)
        self.assertFalse(json.loads(chat.handle(self.bridge, args, self.context))["ok"])
        self.assertFalse(self.store.admit_mutation(queued))
        self.assertFalse(self.store.chat_transfer(self.request_id)["execution_admitted"])
        self.assertFalse(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                                self.context))["ok"])
        self.assertFalse(json.loads(chat.handle(self.bridge, {"action": "done", "issue": "ABC-1"},
                                                self.context))["ok"])

    def test_completed_control_handoff_keeps_effects_unknown_until_user_reconciliation(self):
        self.bridge.retire_chat = self.retired
        old_write = self.bridge.status("issue-1", "blocked")
        first = self.transfer()
        self.assertTrue(first["ok"], first)
        self.assertFalse(first["execution_admitted"])
        self.assertEqual(first["predecessor_effects"], "unknown")
        self.assertEqual(self.transfer(), first)
        self.assertEqual(self.store.get("issue-1")["owner_ref"], "new-chat")
        self.assertEqual(self.store.get("issue-1")["project_id"], "project-1")
        self.assertFalse(self.store.admit_mutation(old_write))
        self.assertFalse(self.bridge.status("issue-1", "blocked"))
        link = "https://example.test/reconciled"
        self.bridge.transfer_user_message = lambda context: f"I reconciled ABC-1 at {link} and continue here"
        resumed = json.loads(chat.handle(self.bridge, {"action": "resume_transfer", "issue": "ABC-1",
            "id": self.request_id, "evidence": link}, self.context))
        self.assertTrue(resumed["ok"], resumed)
        self.assertEqual(self.store.chat_transfer(self.request_id)["retirement"]["effects"], "unknown")
        self.assertFalse(self.store.admit_mutation(old_write))
        self.assertTrue(self.bridge.status("issue-1", "blocked"))

    def test_resume_refuses_evidence_only_supplied_by_model(self):
        self.bridge.retire_chat = self.retired
        self.assertTrue(self.transfer()["ok"])
        self.bridge.transfer_user_message = lambda context: "I reconciled ABC-1; continue here"
        result = json.loads(chat.handle(self.bridge, {"action": "resume_transfer", "issue": "ABC-1",
            "id": self.request_id, "evidence": "https://example.test/invented"}, self.context))
        self.assertFalse(result["ok"])
        self.assertEqual(self.store.chat_transfer(self.request_id)["state"], "committed")

    def test_uncertain_outbox_receipt_moves_control_but_still_blocks_execution(self):
        self.bridge.retire_chat = self.retired
        uncertain = self.store.enqueue("status", {"issue_id": "issue-1", "terminal": True,
            "state": "blocked", "session_key": "old-chat"}, at=self.clock())
        self.assertTrue(self.store.admit_mutation(uncertain, terminal=True))
        first = self.transfer()
        self.assertTrue(first["ok"], first)
        self.assertFalse(first["execution_admitted"])
        self.assertIsNotNone(self.store.outbox_row(uncertain))
        link = "https://example.test/reconciled"
        self.bridge.transfer_user_message = lambda context: f"I reconciled ABC-1 at {link} and continue here"
        refused = json.loads(chat.handle(self.bridge, {"action": "resume_transfer", "issue": "ABC-1",
            "id": self.request_id, "evidence": link}, self.context))
        self.assertEqual(refused["code"], "reconciliation_required")
        self.assertEqual(self.store.chat_transfer(self.request_id)["state"], "committed")

    def test_old_project_update_remains_fenced_after_successor_resume(self):
        self.bridge.retire_chat = self.retired
        old_update = self.store.enqueue("project_update", {
            "issue_id": "update:old-session:project-1", "session_id": "old-session",
            "session_key": "old-chat", "project_id": "project-1", "resolve": "issue-1",
            "lines": {"ABC-1": "Blocked"}, "line_issues": {"ABC-1": "issue-1"}}, at=self.clock())
        self.assertTrue(self.transfer()["ok"])
        link = "https://example.test/reconciled"
        self.bridge.transfer_user_message = lambda context: f"I reconciled ABC-1 at {link} and continue here"
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "resume_transfer", "issue": "ABC-1",
            "id": self.request_id, "evidence": link}, self.context))["ok"])
        self.assertFalse(self.store.admit_mutation(old_update))
        other_update = self.store.enqueue("project_update", {
            "issue_id": "update:old-session:other-project", "session_id": "old-session",
            "session_key": "old-chat", "project_id": "other-project", "resolve": "issue-2",
            "lines": {"ABC-2": "In progress"}, "line_issues": {"ABC-2": "issue-2"}}, at=self.clock())
        self.assertTrue(self.store.admit_mutation(other_update))

    def test_same_chat_reopen_with_rotated_session_id_keeps_transfer_receipt(self):
        self.bridge.retire_chat = self.retired
        first = self.transfer()
        self.context.session_id = "new-transcript-after-compression"
        self.assertEqual(self.transfer(), first)
        link = "https://example.test/reconciled"
        self.bridge.transfer_user_message = lambda context: f"I reconciled ABC-1 at {link} and continue here"
        resumed = json.loads(chat.handle(self.bridge, {"action": "resume_transfer", "issue": "ABC-1",
            "id": self.request_id, "evidence": link}, self.context))
        self.assertTrue(resumed["ok"], resumed)

    def test_authenticated_desktop_key_rotation_rebinds_same_successor(self):
        self.bridge.retire_chat = self.retired
        self.context.platform = "desktop"
        self.context._serialized_transport = True
        self.context.ui_session_id = "stable-ui-tab"
        self.context.browser_control_provider = "ticket"
        self.context.browser_control_subject = "subject"
        self.context.user_message = "Transfer ABC-1 to this chat"
        first = self.transfer()
        self.assertTrue(first["ok"], first)
        self.context.session_key = "new-chat-after-compression"
        self.context.session_id = "new-transcript-after-compression"
        self.assertEqual(self.transfer(), first)
        self.assertEqual(self.store.get("issue-1")["owner_ref"], self.context.session_key)
        self.context.user_message = "I reconciled ABC-1 at https://example.test/reconciled and continue here"
        resumed = json.loads(chat.handle(self.bridge, {"action": "resume_transfer", "issue": "ABC-1",
            "id": self.request_id, "evidence": "https://example.test/reconciled"}, self.context))
        self.assertTrue(resumed["ok"], resumed)

    def test_human_reassignment_or_closed_issue_never_prepares(self):
        self.remote.set_delegate("issue-1", {"id": "other-agent"})
        self.assertFalse(self.transfer()["ok"])
        self.assertIsNone(self.store.chat_transfer(self.request_id))
        self.remote.set_delegate("issue-1", {"id": SELF})
        self.remote.set_state("issue-1", "Canceled")
        self.assertFalse(self.transfer()["ok"])
        self.assertIsNone(self.store.chat_transfer(self.request_id))


if __name__ == "__main__":
    unittest.main()
