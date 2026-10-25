"""Linear links native work without admitting, moving or resuming an executor."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from linear_fake_api import Clock, FakeLinear, load_plugin

load_plugin()
from hermes_fleet_linear_plugin import chat
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.store import Store


class Board:
    def __init__(self):
        self.task = SimpleNamespace(id="task-1", assignee="alpha", status="running", current_run_id=7)
        self.subscriptions = []

    def get(self, task_id):
        return self.task if task_id == self.task.id else None

    def subscribe(self, task_id, issue_id):
        self.subscriptions.append((task_id, issue_id))

    def create(self, **kwargs):
        raise AssertionError("tracking must not create work")

    def unblock(self, task_id):
        raise AssertionError("tracking must not resume work")


class TaskTrackingTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.clock = Clock()
        self.remote = FakeLinear(self.clock)
        self.addCleanup(self.remote.close)
        self.remote.add_issue("issue-1", "ABC-1")
        from hermes_fleet_linear_plugin.api import LinearAPI
        self.board = Board()
        self.bridge = Bridge(Store(Path(tmp.name) / "state.db"),
            LinearAPI(lambda: "fixture", endpoint=self.remote.url, clock=self.clock), self.board,
            profile="alpha", clock=self.clock)
        self.bridge.validate_chat_admission = lambda context: context.user_id == "allowed"
        self.context = SimpleNamespace(profile="alpha", platform="telegram", session_key="chat-a",
            session_id="session-a", run_generation=1, user_id="allowed")

    def invoke(self, action, **args):
        return json.loads(chat.handle(self.bridge, {"action": action, "issue": "ABC-1", **args}, self.context))

    def test_two_surfaces_link_the_same_worker_without_moving_it(self):
        self.assertTrue(self.invoke("track", task_id="task-1")["ok"])
        before = self.bridge.store.get("issue-1")
        self.context.platform = "desktop"
        self.context.session_key = "chat-b"
        self.context.session_id = "session-b"
        self.assertTrue(self.invoke("track", task_id="task-1")["ok"])
        self.assertEqual(self.bridge.store.get("issue-1"), before)
        self.assertEqual(self.board.task.current_run_id, 7)
        self.assertEqual(self.board.task.status, "running")

    def test_tracking_a_stopped_task_does_not_resume_it(self):
        self.board.task.status = "blocked"
        self.assertTrue(self.invoke("track", task_id="task-1")["ok"])
        self.assertEqual(self.board.task.status, "blocked")

    def test_foreign_user_or_task_profile_cannot_link_work(self):
        self.context.user_id = "other"
        self.assertFalse(self.invoke("track", task_id="task-1")["ok"])
        self.context.user_id = "allowed"
        self.board.task.assignee = "other-profile"
        self.assertFalse(self.invoke("track", task_id="task-1")["ok"])
        self.assertIsNone(self.bridge.store.get("issue-1"))

    def test_legacy_chat_claim_is_preserved_without_phrase_takeover(self):
        self.bridge.store.put("issue-1", "chat", "old-chat")
        before = self.bridge.store.get("issue-1")
        self.context.user_message = "Continue ABC-1 here"
        result = self.invoke("track", task_id="task-1")
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "legacy_reconciliation_required")
        self.assertEqual(self.bridge.store.get("issue-1"), before)

    def test_chat_claim_and_transfer_actions_cannot_admit_execution(self):
        for action in ("start", "delegate", "transfer", "resume_transfer"):
            with self.subTest(action=action):
                self.assertFalse(self.invoke(action)["ok"])
                self.assertIsNone(self.bridge.store.get("issue-1"))

    def test_restart_keeps_legacy_records_without_injection_or_deletion(self):
        self.bridge.store.put("issue-1", "chat", "old-chat")
        before = self.bridge.store.get("issue-1")
        self.bridge.inject = lambda *args: self.fail("restart cannot start a chat turn")
        self.bridge.recover()
        self.assertEqual(self.bridge.store.get("issue-1"), before)


if __name__ == "__main__":
    unittest.main()
