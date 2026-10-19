"""Transfer intent survives interruption without granting a successor execution."""
from __future__ import annotations

import json
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

    def test_prepared_transfer_fences_old_mutations_but_not_another_issue(self):
        old_write = self.bridge.status("issue-1", "blocked")
        self.assertTrue(old_write)
        self.transfer()
        self.assertFalse(self.store.admit_mutation(old_write))
        self.context.session_key = "old-chat"
        refused = json.loads(chat.handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context))
        self.assertFalse(refused["ok"])
        self.assertEqual(self.store.get("issue-1"), self.owner)
        self.assertTrue(self.store.put("issue-2", "chat", "other-owner"))

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
