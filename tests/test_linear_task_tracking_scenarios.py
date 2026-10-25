"""Registered producer -> private transport -> native binding -> durable HTTP outbox."""
import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import uuid
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from linear_fake_api import Clock, load_plugin
from test_linear_content_actions import ContentRemote

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
        self.remote = ContentRemote(self.clock)
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

    def queue_native_content(self, channel="desktop-content"):
        context = self.context(channel)
        self.assertTrue(self.invoke(context)["ok"])
        self.bridge.flush()
        comment_id, description_id = str(uuid.uuid4()), str(uuid.uuid4())
        self.assertTrue(self.invoke(context, action="add_comment", id=comment_id,
                                    body="Verified native progress")["ok"])
        self.assertTrue(self.invoke(context, action="update_description", id=description_id,
                                    description="### Checkpoint\n\nVerified native progress",
                                    expected_description="Synthetic issue body")["ok"])
        return comment_id, description_id

    def test_registered_native_content_reaches_remote_once(self):
        comment_id, description_id = self.queue_native_content()
        self.bridge.flush()
        self.bridge.flush()
        self.assertEqual(sum(comment.get("body") == "Verified native progress"
                             for comment in self.remote.comments), 1)
        self.assertEqual(self.remote.issues["issue-1"]["description"], "### Checkpoint\n\nVerified native progress")
        self.assertTrue(self.store.outbox_row(comment_id)["payload"]["applied"])
        self.assertTrue(self.store.outbox_row(description_id)["payload"]["applied"])

    def test_native_content_cannot_write_through_a_replaced_binding(self):
        self.queue_native_content()
        successor = self.board.create(title="distinct remaining work", initial_status="blocked", created_by="default")
        self.assertTrue(self.store.delete("issue-1"))
        self.assertTrue(self.store.link_task("issue-1", successor.id, "proj-1", self.clock() * 1000))
        self.bridge.flush()
        self.assertFalse(any(comment.get("body") == "Verified native progress" for comment in self.remote.comments))
        self.assertEqual(self.remote.issues["issue-1"]["description"], "Synthetic issue body")
        self.assertEqual(self.store.get("issue-1")["task_id"], successor.id)

    def test_queued_native_content_remains_fenced_by_stop(self):
        self.queue_native_content()
        self.assertTrue(self.store.update("issue-1", stop_requested_at=self.clock() * 1000))
        self.bridge.flush()
        self.assertFalse(any(comment.get("body") == "Verified native progress" for comment in self.remote.comments))
        self.assertEqual(self.remote.issues["issue-1"]["description"], "Synthetic issue body")

    def test_queued_native_content_remains_fenced_by_scope_revocation(self):
        self.queue_native_content()
        self.store.fence_scope("issue-1", "fixture authorization revoked", at=self.clock())
        self.bridge.flush()
        self.assertFalse(any(comment.get("body") == "Verified native progress" for comment in self.remote.comments))
        self.assertEqual(self.remote.issues["issue-1"]["description"], "Synthetic issue body")

    def test_authenticated_track_revalidates_old_binding_without_changing_native_owner(self):
        self.assertTrue(self.board.unblock(self.task.id))
        with self.board.conn() as conn:
            self.assertTrue(self.board.kb.claim_task(conn, self.task.id))
        self.assertTrue(self.invoke(self.context("desktop-before-migration"))["ok"])
        self.bridge.flush()
        before = self.board.get(self.task.id)
        binding = self.store.get("issue-1")
        with sqlite3.connect(self.store.path) as conn:
            conn.execute("ALTER TABLE work DROP COLUMN linked_at")
        self.store = Store(self.store.path)
        self.bridge.store = self.store
        self.assertEqual(self.store.get("issue-1")["linked_at"], 0)
        status = self.invoke(self.context("desktop-migration-status"), action="status")
        self.assertTrue(status["ok"])
        self.assertTrue(status["legacy_reconciliation_required"])
        self.assertEqual(self.store.get("issue-1")["linked_at"], 0, "a status read cannot establish provenance")
        self.clock.now += 10
        self.assertTrue(self.invoke(self.context("desktop-revalidated"))["ok"])
        after = self.board.get(self.task.id)
        updated = self.store.get("issue-1")
        self.assertGreaterEqual(updated["linked_at"], self.clock() * 1000)
        self.assertEqual((updated["task_id"], updated["ownership_id"], updated["owner_ref"]),
                         (binding["task_id"], binding["ownership_id"], binding["owner_ref"]))
        self.assertEqual((after.status, after.current_run_id, after.claim_lock),
                         (before.status, before.current_run_id, before.claim_lock))

    def test_track_cannot_clear_unknown_binding_with_unresolved_delivery(self):
        self.assertTrue(self.invoke(self.context("desktop-unresolved"))["ok"])
        self.bridge.flush()
        pending = json.dumps({"note": "Delivery receipt is unknown"})
        self.assertTrue(self.store.update("issue-1", linked_at=0, pending_resume=pending))
        before = self.store.get("issue-1")
        reply = self.invoke(self.context("desktop-reconcile"))
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["code"], "legacy_reconciliation_required")
        self.assertEqual(self.store.get("issue-1"), before)
        self.assertEqual(self.board.get(self.task.id).status, "blocked")

    def test_new_tracking_link_uses_host_time_floor_for_old_remote_issue(self):
        self.remote.issues["issue-1"]["updatedAt"] = self.clock.iso(-3600)
        self.assertTrue(self.invoke(self.context("desktop-fresh-link"))["ok"])
        self.assertGreaterEqual(self.store.get("issue-1")["linked_at"], self.clock() * 1000)
        self.assertEqual(self.board.get(self.task.id).status, "blocked")

    def test_service_rejects_incompatible_native_runtime_before_credentials_or_state(self):
        from hermes_cli import kanban_db
        plugin.validate_native_task_runtime()  # the control runtime is compatible before narrowing one API
        service_home = self.home / "incompatible-service"
        service_home.mkdir()
        state_path = service_home / "linear" / "state.db"
        services = {}
        values = {"enabled": True, "identity": {"viewer_id": "fixture-app", "organization_id": "fixture-org"},
                  "state_database": str(state_path), "api_url": self.remote.url}
        registration = SimpleNamespace(
            get_config=lambda key, default=None: values.get(key, default),
            register_tool=lambda **kwargs: None, register_hook=lambda *args: None,
            register_profile_service=lambda name, factory: services.update({name: factory}))
        plugin.register(registration)

        def legacy_comment(conn, task_id, author, body):
            raise AssertionError("preflight must not execute native work")

        async def run_service():
            stop = asyncio.Event()
            stop.set()
            runtime = SimpleNamespace(profile_home=service_home, profile_name="default", gateway=object(), stop_event=stop)
            await services["linear"](runtime)

        with patch.object(kanban_db, "add_comment", legacy_comment), \
                patch.object(plugin, "token_provider", side_effect=AssertionError("credential lookup reached")) as tokens:
            with self.assertRaisesRegex(RuntimeError, "native task authority/idempotent-comment runtime"):
                asyncio.run(run_service())
            tokens.assert_not_called()
        self.assertFalse(state_path.exists())
        self.assertFalse((service_home / "linear").exists())
        self.assertEqual(list(service_home.iterdir()), [], "incompatible runtime cannot create profile state")

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
