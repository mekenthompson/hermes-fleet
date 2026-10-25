"""Registered producer -> private transport -> native binding -> durable HTTP outbox."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linear_fake_api import Clock, FakeLinear, load_plugin

SOURCE = os.environ.get("HERMES_AGENT_SRC", "")
if SOURCE:
    sys.path.insert(0, SOURCE)
try:
    if not SOURCE:
        raise ImportError("HERMES_AGENT_SRC is not set")
    from gateway.chat_execution_recovery import ChatExecutionStore
    from tools.registry import ToolInvocationContext
    CORE = None
except ImportError as exc:
    CORE = str(exc)

plugin = load_plugin()
from hermes_fleet_linear_plugin import chat, transport
from hermes_fleet_linear_plugin.api import LinearAPI
from hermes_fleet_linear_plugin.bridge import Bridge, Kanban
from hermes_fleet_linear_plugin.store import Store


@unittest.skipIf(CORE is not None, f"integration: native source unavailable ({CORE})")
class TaskTrackingScenarios(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.home = Path(temp.name)
        self.env = patch.dict(os.environ, {"HERMES_HOME": str(self.home),
                                          "HERMES_KANBAN_HOME": str(self.home / "kanban")})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.clock = Clock()
        self.remote = FakeLinear(self.clock)
        self.addCleanup(self.remote.close)
        self.remote.add_issue("issue-1", "ABC-1")
        self.board = Kanban(profile="default", profile_home=self.home)
        self.task = self.board.create(title="remaining work", initial_status="blocked", created_by="default")
        self.store = Store(self.home / "linear.db")
        self.bridge = Bridge(self.store, LinearAPI(lambda: "fixture", endpoint=self.remote.url,
                                                 clock=self.clock), self.board,
                             profile="default", clock=self.clock)
        self.journal = ChatExecutionStore(self.home)
        self.server = transport.Server(self.home, "default",
            lambda args, context: chat.handle(self.bridge, args, context), lambda session: None)
        self.addCleanup(self.server.close)

        class Registration:
            def get_config(self, key, default=None): return True if key == "enabled" else default
            def register_tool(self, **kwargs): self.tool = kwargs["handler"]
            def register_hook(self, *args): pass
            def register_profile_service(self, *args): pass

        registration = Registration()
        plugin.register(registration)
        self.producer = registration.tool

    def context(self, channel, *, platform="desktop"):
        generation = self.journal.start_local_worker(channel, "session-" + channel)
        return ToolInvocationContext(profile="default", platform=platform, session_key=channel,
            session_id="session-" + channel, run_generation=generation,
            ui_session_id=channel if platform == "desktop" else "",
            browser_control_provider="local" if platform == "desktop" else "",
            browser_control_subject="owner" if platform == "desktop" else "",
            api_control_principal="principal:fixture:0123456789abcdef0123456789abcdef" if platform == "api_server" else "",
            api_control_transport_family="local-api" if platform == "api_server" else "",
            api_conversation_key=channel if platform == "api_server" else "",
            user_message="Pick up the remaining work")

    def invoke(self, context, action="track", **changes):
        return json.loads(self.producer({"action": action, "issue": "ABC-1", "task_id": self.task.id,
                                        **changes}, invocation_context=context))

    def test_desktop_and_api_follow_same_task_without_ownership_transfer(self):
        self.assertTrue(self.board.unblock(self.task.id))
        with self.board.conn() as conn:
            self.board.kb.claim_task(conn, self.task.id)
        before = self.board.get(self.task.id)
        first = self.context("desktop-a")
        self.assertTrue(self.invoke(first, profile="foreign", user_id="foreign", run_generation=999)["ok"])
        binding = self.store.get("issue-1")
        self.assertTrue(self.invoke(self.context("api-b", platform="api_server"))["ok"])
        self.assertEqual(self.store.get("issue-1"), binding)
        after = self.board.get(self.task.id)
        self.assertEqual((after.current_run_id, after.claim_lock), (before.current_run_id, before.claim_lock))
        self.bridge.flush()
        self.assertEqual(self.remote.state("issue-1"), "In Progress")

    def test_tracking_stopped_task_and_opening_another_surface_never_resumes_it(self):
        self.assertTrue(self.invoke(self.context("desktop-a"))["ok"])
        self.assertTrue(self.invoke(self.context("api-b", platform="api_server"))["ok"])
        self.assertEqual(self.board.get(self.task.id).status, "blocked")
        self.assertIsNone(self.board.get(self.task.id).current_run_id)

    def test_stale_or_missing_native_host_identity_cannot_link_a_task(self):
        context = self.context("desktop-a")
        self.journal.worker_finished(context.session_key, context.run_generation)
        self.assertEqual(self.invoke(context)["code"], "unverified_host_turn")
        self.assertIsNone(self.store.get("issue-1"))

    def test_outbox_response_loss_does_not_duplicate_native_work(self):
        context = self.context("desktop-a")
        self.assertTrue(self.invoke(context)["ok"])
        self.remote.lose_next_response = True
        self.bridge.flush()
        self.assertTrue(self.invoke(context)["ok"])
        with self.board.conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 1)
        self.assertEqual(self.board.get(self.task.id).status, "blocked")

    def test_linked_task_completion_reports_project_without_an_agent_session(self):
        self.assertTrue(self.invoke(self.context("desktop-project"))["ok"])
        self.bridge.flush()
        self.assertTrue(self.board.unblock(self.task.id))
        evidence = "https://example.test/findings/native-project"
        with self.board.conn() as conn:
            self.assertTrue(self.board.kb.complete_task(conn, self.task.id,
                result="Verified project outcome " + evidence))
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.assertEqual(self.remote.state("issue-1"), "Done")
        self.assertEqual(self.remote.activities, [], "delivery addresses do not determine execution or reporting")
        self.assertEqual(len(self.remote.project_updates), 1)
        self.assertIn(evidence, self.remote.project_updates[0]["body"])
        self.assertTrue(any(evidence in comment["body"] for comment in self.remote.comments))
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.assertEqual(len(self.remote.project_updates), 1)

    def test_native_followups_are_once_only_and_do_not_unblock(self):
        context = self.context("desktop-a")
        self.assertTrue(self.invoke(context)["ok"])
        for _ in range(2):
            self.board.comment(self.task.id, "Keep the checkpoint", marker="followup-1")
        with self.board.conn() as conn:
            self.assertEqual(len(self.board.kb.list_comments(conn, self.task.id)), 1)
        self.assertEqual(self.board.get(self.task.id).status, "blocked")


if __name__ == "__main__":
    unittest.main()
