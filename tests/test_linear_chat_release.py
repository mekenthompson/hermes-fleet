"""Explicit unfinished release is a durable relinquishment, not local deletion."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from linear_fake_api import Clock, FakeLinear, SELF, load_plugin
load_plugin()
from hermes_fleet_linear_plugin.api import LinearAPI, LinearError
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.chat import handle
from hermes_fleet_linear_plugin.store import Store


class ChatReleaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = Clock()
        self.remote = FakeLinear(self.clock)
        self.addCleanup(self.remote.close)
        self.issue = self.remote.add_issue("i", "ABC-1")
        self.remote.set_delegate("i", {"id": SELF})
        self.remote.set_state("i", "In Progress")
        self.store = Store(Path(self.tmp.name) / "state.db")
        self.store.put("i", "chat", "s", last_updated_at=self.clock() * 1000)
        self.api = LinearAPI(lambda: "synthetic", endpoint=self.remote.url, clock=self.clock)
        self.bridge = Bridge(self.store, self.api, SimpleNamespace(), profile="test", clock=self.clock)
        self.context = SimpleNamespace(session_key="s", session_id="s", profile="test", run_generation=1)

    def test_release_retains_ownership_until_verified_relinquishment(self):
        reply = json.loads(handle(self.bridge, {"action": "release", "issue": "ABC-1", "note": "unfinished"}, self.context))
        self.assertTrue(reply["ok"])
        self.assertIsNotNone(self.store.get("i"))
        self.assertTrue(any(r["payload"].get("release") for r in self.store.pending("i")))
        self.assertIsNotNone(self.issue["delegate"])

    def test_pending_release_refuses_more_work_from_previous_chat(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1", "note": "unfinished"}, self.context)
        pending = len(self.store.pending())
        reply = json.loads(handle(self.bridge, {"action": "start", "issue": "ABC-1"}, self.context))
        self.assertFalse(reply["ok"])
        self.assertEqual(len(self.store.pending()), pending)

    def test_success_clears_delegate_and_only_then_forgets_owner(self):
        self.issue["assignee"] = {"id": "human"}
        handle(self.bridge, {"action": "release", "issue": "ABC-1", "note": "unfinished"}, self.context)
        self.bridge.flush()
        self.assertIsNone(self.issue["delegate"])
        self.assertEqual(self.issue["state"]["name"], "Blocked")
        self.assertEqual(self.issue["assignee"], {"id": "human"})
        self.assertIsNone(self.store.get("i"))
        self.assertTrue(any("Released unfinished" in c["body"] for c in self.remote.comments))
        self.clock.now += self.bridge.quiet
        self.bridge.flush()
        self.assertEqual(len(self.remote.project_updates), 1)

    def test_response_loss_retains_fence_and_does_not_replay_mutation(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context)
        self.remote.lose_next_response = True
        self.bridge.flush()
        self.assertIsNone(self.issue["delegate"])
        self.assertIsNotNone(self.store.get("i"))
        self.assertTrue(self.store.issue_reconciliation_blocked("i"))
        count = self.remote.requests.count("mutation IssueUpdate")
        self.clock.now += 120
        self.bridge.flush()
        self.assertEqual(self.remote.requests.count("mutation IssueUpdate"), count)

    def test_human_reassignment_wins_before_release_send(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context)
        self.remote.set_delegate("i", {"id": "other"})
        self.bridge.flush()
        self.assertEqual(self.issue["delegate"], {"id": "other"})
        self.assertEqual(self.issue["state"]["name"], "In Progress")

    def test_human_terminal_edit_is_not_reopened(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context)
        self.remote.set_state("i", "Done")
        self.bridge.flush()
        self.assertEqual(self.issue["state"]["name"], "Done")
        self.assertEqual(self.issue["delegate"], {"id": SELF})

    def test_native_response_is_sent_after_verified_release(self):
        self.store.update("i", linear_session_id="ls")
        handle(self.bridge, {"action": "release", "issue": "ABC-1", "note": "unfinished"}, self.context)
        self.bridge.flush()
        self.assertIsNone(self.issue["delegate"])
        self.assertEqual(len(self.remote.activities), 1)
        self.assertEqual(self.remote.activities[0]["agentSessionId"], "ls")
        self.assertEqual(self.remote.activities[0]["content"]["type"], "response")
        self.assertIn("unfinished", self.remote.activities[0]["content"]["body"])

    def test_pending_release_does_not_accept_linear_chat_followup(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context)
        with patch.object(self.bridge, "inject", return_value=True) as inject:
            self.bridge._chat_followup(self.store.get("i"), "ls", "a", self.clock() * 1000,
                                       "ABC-1", "resume")
        inject.assert_not_called()

    def test_success_false_does_not_delete_owner_or_emit_closeout(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context)
        original = self.remote._apply
        def failed(query, variables):
            if "mutation IssueUpdate" in query:
                return 200, {"data": {"issueUpdate": {"success": False}}}
            return original(query, variables)
        with patch.object(self.remote, "_apply", side_effect=failed):
            self.bridge.flush()
        self.assertIsNotNone(self.store.get("i"))
        self.assertEqual(self.issue["delegate"], {"id": SELF})
        self.assertFalse(self.remote.comments)
        self.assertFalse(self.remote.project_updates)

    def test_readback_failure_requires_reconciliation_without_replay(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context)
        original = self.api.issue
        def failing_readback(issue_id):
            if self.issue["delegate"] is None:
                raise LinearError("readback unavailable")
            return original(issue_id)
        with patch.object(self.api, "issue", side_effect=failing_readback):
            self.bridge.flush()
        self.assertIsNone(self.issue["delegate"])
        self.assertIsNotNone(self.store.get("i"))
        self.assertTrue(self.store.issue_reconciliation_blocked("i"))
        self.assertFalse(self.remote.comments)
        count = self.remote.requests.count("mutation IssueUpdate")
        self.clock.now += 120
        self.bridge.flush()
        self.assertEqual(self.remote.requests.count("mutation IssueUpdate"), count)

    def test_specialist_fence_preserves_remote_and_local_ownership(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context)
        self.store.fence_scope("i", "revoked", at=self.clock())
        self.bridge.flush()
        self.assertEqual(self.issue["delegate"], {"id": SELF})
        self.assertIsNotNone(self.store.get("i"))
        self.assertFalse(self.remote.comments)

    def test_restart_keeps_pending_release_owner_fenced(self):
        handle(self.bridge, {"action": "release", "issue": "ABC-1"}, self.context)
        reopened = Store(self.store.path)
        self.assertEqual(reopened.get("i")["release_pending"], 1)
        self.bridge.store = reopened
        self.bridge.flush()
        self.assertIsNone(reopened.get("i"))
        self.assertIsNone(self.issue["delegate"])
