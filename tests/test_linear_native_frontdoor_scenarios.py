"""Official upstream producer -> registered tool -> private socket -> real native board.

The existing scenario runner verifies the immutable manifest-pinned HERMES_AGENT_SRC.
This suite is also exercised separately against official ff294849385e433dd40a993b4f07673abb95c6f5.
These source/backend tests do not claim a deployed Desktop or messaging client round-trip.
"""
from __future__ import annotations
import contextvars
import json
import inspect
import multiprocessing
import os
from pathlib import Path
from types import SimpleNamespace
import socket
import sys
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import patch

from linear_fake_api import Clock, FakeLinear, load_plugin

SOURCE = os.environ.get("HERMES_AGENT_SRC")
if SOURCE:
    sys.path.insert(0, SOURCE)
plugin = load_plugin()
from hermes_fleet_linear_plugin import chat, transport
from hermes_fleet_linear_plugin.api import LinearAPI
from hermes_fleet_linear_plugin.bridge import Bridge, Kanban
from hermes_fleet_linear_plugin.store import Store


def api_process_invoke(home, pipe):
    """A fresh API plugin host, using the real upstream producer and registrar."""
    from hermes_constants import set_hermes_home_override
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
    from gateway.platforms.api_server import APIServerAdapter
    from tools.registry import registry
    os.environ['HERMES_HOME'] = home
    set_hermes_home_override(home)
    manager = PluginManager(scope_key=home)
    plugin.register(PluginContext(PluginManifest(name='linear'), manager))
    APIServerAdapter._bind_api_server_session(chat_id='desktop-tab', session_key='api:other-tab',
                                            session_id='durable-1', session_history_delivery='1')
    pipe.send(json.loads(registry.get_entry('linear', scope=home).handler({'action': 'status', 'issue': 'ABC-1'})))
    pipe.close()
    manager.unload()


@unittest.skipUnless(SOURCE, "requires explicit official HERMES_AGENT_SRC")
class NativeFrontdoorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from hermes_cli.plugins import PluginContext
        from gateway.platforms.api_server import APIServerAdapter
        for native_type in (PluginContext, APIServerAdapter):
            if not Path(inspect.getfile(native_type)).resolve().is_relative_to(Path(SOURCE).resolve()):
                raise AssertionError("boundary tests must import the runner-verified native source")


    def setUp(self):
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        from gateway.session_context import reset_session_vars
        from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
        from tools.registry import registry
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith("HERMES_KANBAN_") and key != "HERMES_DELEGATED_CHILD_CONTEXT"}
        environment.update(HERMES_HOME=str(self.home), HERMES_KANBAN_HOME=str(self.home / "kanban"), _HERMES_GATEWAY="")
        self.env = patch.dict(os.environ, environment, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        token = set_hermes_home_override(self.home)
        self.addCleanup(reset_hermes_home_override, token)
        reset_session_vars()
        self.addCleanup(reset_session_vars)
        (self.home / "config.yaml").write_text("plugins:\n  entries:\n    linear:\n      settings:\n        enabled: true\n")
        self.manager = PluginManager(scope_key=str(self.home))
        self.addCleanup(self.manager.unload)
        plugin.register(PluginContext(PluginManifest(name="linear"), self.manager))
        entry = registry.get_entry("linear", scope=str(self.home))
        self.assertIsNotNone(entry, "actual upstream PluginContext must register the tool")
        self.tool = entry.handler
        self.clock = Clock()
        from test_linear_content_actions import ContentRemote
        self.remote = ContentRemote(self.clock)
        self.addCleanup(self.remote.close)
        self.issue = self.remote.add_issue("issue-1", "ABC-1")
        api = LinearAPI(lambda: "synthetic-token", endpoint=self.remote.url, clock=self.clock)
        self.board = Kanban(profile="default", profile_home=self.home)
        self.bridge = Bridge(Store(self.home / "state.db"), api, self.board, profile="default", clock=self.clock)
        self.bridge.profile_home = self.home
        self.seen = []
        def consume(args, context):
            self.seen.append(context)
            return chat.handle(self.bridge, args, context)
        self.server = transport.Server(self.home, "default", consume)
        self.addCleanup(self.server.close)

    def bind_api(self, key="api:desktop", profile=""):
        from gateway.platforms.api_server import APIServerAdapter
        return APIServerAdapter._bind_api_server_session(chat_id="desktop-tab", session_key=key,
                                                         session_id="durable-1", profile=profile,
                                                         session_history_delivery="1")

    def call(self, args):
        return json.loads(self.tool(args))

    def bind_messaging(self, platform, *, profile=None, launch="sample-agent"):
        """Execute the real gateway producer without starting a gateway or its adapters."""
        from gateway.config import Platform
        from gateway.run import GatewayRunner
        from gateway.session import SessionContext, SessionSource
        runner = object.__new__(GatewayRunner)
        runner.config = SimpleNamespace(multiplex_profiles=False)
        runner.adapters = {}
        runner._configured_runtime_profile_name = launch
        source = SessionSource(platform=Platform(platform), chat_id="chat-1", user_id="human-1",
                               scope_id="workspace-1", thread_id="thread-1", profile=profile)
        context = SessionContext(source=source, connected_platforms=[], home_channels={},
                                 session_key=f"{platform}:chat-1:thread-1", session_id="messaging-run")
        return runner._set_session_env(context)

    def test_standalone_launch_label_reaches_service_from_real_messaging_producers(self):
        with patch.dict(os.environ, {"HERMES_PROFILE": "sample-agent"}):
            for platform in ("telegram", "slack"):
                for profile in (None, "sample-agent"):
                    with self.subTest(platform=platform, profile=profile):
                        self.bind_messaging(platform, profile=profile)
                        result = self.call({"action": "status", "issue": "ABC-1"})
                        self.assertTrue(result["ok"], result)
                        got = self.seen[-1]
                        self.assertEqual((got.profile, got.home), ("default", str(self.home)))
                        self.assertEqual((got.platform, got.session_id, got.user_id, got.scope_id),
                                         (platform, "messaging-run", "human-1", "workspace-1"))
                        self.assertEqual((got.chat_id, got.thread_id), ("chat-1", "thread-1"))
        self.assertIsNone(self.bridge.store.get("issue-1"), "reading must not start or bind execution")

    def test_api_default_and_launch_label_use_same_native_profile(self):
        with patch.dict(os.environ, {"HERMES_PROFILE": "sample-agent"}):
            for profile in ("", "sample-agent"):
                with self.subTest(profile=profile):
                    self.bind_api(profile=profile)
                    result = self.call({"action": "status", "issue": "ABC-1"})
                    self.assertTrue(result["ok"], result)
                    self.assertEqual(self.seen[-1].profile, "default")

    def test_launch_label_does_not_authorize_foreign_messaging_or_api_profile(self):
        with patch.dict(os.environ, {"HERMES_PROFILE": "sample-agent"}):
            for platform in ("telegram", "slack", "api_server"):
                with self.subTest(platform=platform):
                    if platform == "api_server":
                        self.bind_api(profile="foreign")
                    else:
                        self.bind_messaging(platform, profile="foreign")
                    result = self.call({"action": "status", "issue": "ABC-1"})
                    self.assertFalse(result["ok"], result)
        self.assertEqual(self.seen, [])

    def test_existing_other_native_profile_is_not_a_launch_label_alias(self):
        other = self.home / "profiles" / "sample-agent"
        other.mkdir(parents=True)
        (other / "config.yaml").write_text("model: synthetic\n")
        with patch.dict(os.environ, {"HERMES_PROFILE": "sample-agent"}):
            self.bind_messaging("telegram", profile="sample-agent")
            result = self.call({"action": "status", "issue": "ABC-1"})
        self.assertFalse(result["ok"], result)
        self.assertEqual(self.seen, [])

    def test_launch_label_cannot_alias_a_routed_home(self):
        from hermes_constants import pin_process_hermes_home
        with tempfile.TemporaryDirectory() as process_home, patch.dict(os.environ, {"HERMES_PROFILE": "sample-agent"}):
            pin_process_hermes_home(process_home)
            self.addCleanup(pin_process_hermes_home, None)
            self.bind_messaging("slack", profile="sample-agent")
            result = self.call({"action": "status", "issue": "ABC-1"})
        self.assertFalse(result["ok"], result)
        self.assertEqual(self.seen, [])

    def test_actual_api_producer_registered_handler_socket_and_unlinked_comment(self):
        self.bind_api()
        result = self.call({"action": "add_comment", "issue": "ABC-1", "id": str(uuid.uuid4()), "body": "### Progress\nA bounded finding."})
        self.assertTrue(result["ok"], result)
        self.assertEqual(len(self.seen), 1)
        self.assertEqual(self.seen[0].platform, "api_server")
        self.assertEqual(self.seen[0].profile, "default")
        self.assertEqual(self.seen[0].session_key, "api:desktop")
        self.assertEqual(self.seen[0].session_id, "durable-1")
        self.assertEqual(self.seen[0].user_id, "")
        self.assertIsNone(self.bridge.store.get("issue-1"))
        self.assertEqual(self.bridge.flush(), 1)
        self.assertEqual(len(self.remote.comments), 1)

    def test_optional_conversation_capability_preserves_ordinary_native_reads_and_tracking(self):
        from hermes_fleet_linear_plugin.native_input import native_supported
        available = native_supported()
        self.assertIs(type(available), bool)
        self.bind_api()
        status = self.call({"action": "status", "issue": "ABC-1"})
        self.assertTrue(status["ok"], status)
        task = self.board.create(title="Existing native work", body="Linear issue issue-1: " + self.issue["url"],
            created_by=self.board.executor_profile, initial_status="blocked")
        result = self.call({"action": "track", "issue": "ABC-1", "task_id": task.id})
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.board.get(task.id).status, "blocked")
        self.assertEqual(self.bridge.store.native_binding("issue-1")["task_id"], task.id)
        print("native conversation capability:", available)

    def test_api_process_crosses_real_socket_with_complete_host_context(self):
        parent, child = multiprocessing.Pipe(duplex=False)
        process = multiprocessing.get_context("spawn").Process(target=api_process_invoke, args=(str(self.home), child))
        process.start()
        child.close()
        try:
            self.assertTrue(parent.poll(15), "producer child must receive a service reply")
            result = parent.recv()
        finally:
            process.join(timeout=15)
            if process.is_alive():
                process.terminate()
                process.join()
            parent.close()
        self.assertEqual(process.exitcode, 0)
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.seen[-1].session_key, "api:other-tab")

    def test_messaging_producer_preserves_user_scope_and_route_for_every_action(self):
        from gateway.session_context import set_session_vars
        set_session_vars(platform="slack", source="slack", chat_id="channel-1", chat_type="group",
                         thread_id="thread-1", user_id="human-1", user_id_alt="human-alt",
                         scope_id="workspace-1", session_key="slack:channel-1:thread-1", session_id="slack-run", profile="default")
        result = self.call({"action": "status", "issue": "ABC-1", "profile": "foreign", "user_id": "forged"})
        self.assertTrue(result["ok"], result)
        got = self.seen[-1]
        self.assertEqual((got.user_id, got.user_id_alt, got.scope_id), ("human-1", "human-alt", "workspace-1"))
        self.assertEqual((got.chat_id, got.thread_id), ("channel-1", "thread-1"))
        self.assertEqual(got.profile, "default")

    def test_missing_native_binding_rejects_model_or_environment_identity(self):
        with patch.dict(os.environ, {"HERMES_SESSION_KEY": "forged", "HERMES_SESSION_PROFILE": "default",
                                    "HERMES_SESSION_PLATFORM": "api_server", "HERMES_SESSION_ID": "forged"}):
            result = contextvars.Context().run(self.call, {"action": "status", "issue": "ABC-1", "context": {"session_key": "forged"}})
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "context_or_service_unavailable")
        self.assertEqual(self.seen, [])

    def test_foreign_profile_home_is_refused_before_transport(self):
        self.bind_api(profile="other")
        result = self.call({"action": "status", "issue": "ABC-1"})
        self.assertFalse(result["ok"])
        self.assertEqual(self.seen, [])

    def test_socket_refuses_altered_profile_and_home_context(self):
        self.bind_api()
        context = transport.capture_context().to_wire()
        context["profile"] = "other"
        with self.assertRaises(transport.Uncertain):
            transport.request(self.home, {"op": "chat", "home": str(self.home), "args": {"action": "status", "issue": "ABC-1"}, "context": context})
        self.assertEqual(self.seen, [])

    def test_private_directory_and_socket_are_required(self):
        self.bind_api()
        self.assertEqual(transport.endpoint(self.home).stat().st_mode & 0o777, 0o600)
        transport.endpoint(self.home).parent.chmod(0o755)
        result = self.call({"action": "status", "issue": "ABC-1"})
        self.assertFalse(result["ok"])
        self.assertIn("private", result["message"])
        transport.endpoint(self.home).parent.chmod(0o700)

    def test_tracking_existing_native_task_has_no_execution_side_effect(self):
        self.bind_api()
        task = self.board.create(title="Existing native work", body="Investigate only the remainder")
        before = self.board.get(task.id)
        result = self.call({"action": "track", "issue": "ABC-1", "task_id": task.id})
        self.assertTrue(result["ok"], result)
        after = self.board.get(task.id)
        self.assertEqual((after.status, after.current_run_id, after.assignee),
                         (before.status, before.current_run_id, before.assignee))
        self.assertEqual(self.bridge.store.get("issue-1")["origin"], "native")
        self.assertEqual(self.call({"action": "status", "issue": "ABC-1"})["native"]["task_id"], task.id)

    def test_default_and_project_tasks_link_with_separate_creation_board(self):
        self.bind_api()
        creation = Kanban("sample-linear-v1", profile="default", profile_home=self.home)
        self.bridge.kanban = creation
        for slug in ("default", "project-board"):
            with self.subTest(board=slug):
                board = Kanban(slug, profile="default", profile_home=self.home)
                task = board.create(title="Existing native work", initial_status="blocked")
                issue_id = "issue-" + slug
                self.remote.add_issue(issue_id, "ABC-" + slug)
                args = {"action": "track", "issue": "ABC-" + slug, "task_id": task.id}
                if slug != "default":
                    args["board"] = slug
                result = self.call(args)
                self.assertTrue(result["ok"], result)
                self.assertEqual(self.seen[-1].native_board, slug)
                binding = self.bridge.store.native_binding(issue_id)
                self.assertEqual(binding["board"], slug)
                status = self.call({"action": "status", "issue": "ABC-" + slug})
                self.assertEqual((status["native"]["task_id"], status["native"]["board"]), (task.id, slug))
                after = board.get(task.id)
                self.assertEqual((after.status, after.current_run_id, after.claim_lock, after.assignee),
                                 (task.status, task.current_run_id, task.claim_lock, task.assignee))
        self.assertEqual(self.bridge.kanban.board, "sample-linear-v1")
        with creation.conn() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM tasks").fetchone()[0], 0)

    def test_track_defaults_to_actual_native_current_board(self):
        from hermes_cli import kanban_db as kb
        self.bind_api()
        board = Kanban("current-project", profile="default", profile_home=self.home)
        task = board.create(title="Current project work", initial_status="blocked")
        with kb.scoped_current_board("current-project"):
            result = self.call({"action": "track", "issue": "ABC-1", "task_id": task.id})
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.bridge.store.native_binding("issue-1")["board"], "current-project")

    def test_missing_and_archived_track_boards_are_not_created_or_resurrected(self):
        from hermes_cli import kanban_db as kb
        self.bind_api()
        Kanban("retired-project", profile="default", profile_home=self.home)
        kb.remove_board("retired-project", archive=True)
        for slug in ("missing-project", "retired-project", "../outside"):
            with self.subTest(board=slug):
                before = len(self.seen)
                result = self.call({"action": "track", "issue": "ABC-1", "task_id": "existing", "board": slug})
                self.assertFalse(result["ok"], result)
                self.assertEqual(len(self.seen), before)
        self.assertFalse(kb.board_dir("missing-project").exists())
        self.assertFalse(kb.kanban_db_path(board="retired-project").exists())
        self.assertTrue(kb.read_board_metadata("retired-project")["archived"])

    def test_worker_and_descendant_cannot_retarget_track_before_rpc(self):
        from hermes_cli import kanban_db as kb
        self.bind_api()
        Kanban("other-project", profile="default", profile_home=self.home)
        for marker in ({"HERMES_KANBAN_TASK": "native-worker"},
                       {"HERMES_DELEGATED_CHILD_CONTEXT": str(kb.kanban_home())}):
            with self.subTest(marker=marker), patch.dict(os.environ, {
                    "HERMES_KANBAN_BOARD": "default", "HERMES_KANBAN_DB": str(kb.kanban_db_path(board="default")), **marker}):
                before = len(self.seen)
                result = self.call({"action": "track", "issue": "ABC-1", "task_id": "existing", "board": "other-project"})
                self.assertFalse(result["ok"], result)
                self.assertIn("pin", result["message"])
                self.assertEqual(len(self.seen), before)

    def test_pinned_status_keeps_issue_read_but_cannot_reach_other_board_evidence_or_followup(self):
        from hermes_cli import kanban_db as kb
        self.bind_api()
        board = Kanban("other-project", profile="default", profile_home=self.home)
        task = board.create(title="Other board work")
        self.assertTrue(self.bridge.authorize_specialist_effect("issue-1"))
        self.assertTrue(self.bridge.store.bind_native("issue-1", task.id, "default", "other-project", at=self.clock()))
        with patch.dict(os.environ, {"HERMES_KANBAN_BOARD": "default",
                                   "HERMES_KANBAN_DB": str(kb.kanban_db_path(board="default")),
                                   "HERMES_KANBAN_TASK": "native-worker"}):
            for action in ("status", "followup"):
                result = self.call({"action": action, "issue": "ABC-1", "body": "A new instruction", "id": str(uuid.uuid4())})
                if action == "status":
                    self.assertTrue(result["ok"], result)
                    self.assertEqual(result["issue"]["id"], "issue-1")
                    self.assertEqual(result["native"]["status"], "unavailable")
                    self.assertIn("pin", result["native"]["message"])
                    self.assertNotIn(task.id, json.dumps(result["native"]))
                else:
                    self.assertFalse(result["ok"], result)
                    self.assertIn("pin", result["message"])
                self.assertEqual(self.seen[-1].pinned_board, "default")
        with board.conn() as conn:
            self.assertEqual(kb.list_comments(conn, task.id), [])

    def test_unlinked_content_needs_no_native_board_and_ignores_model_pins(self):
        self.bind_api()
        with patch.object(transport, "native_board_target", side_effect=AssertionError("ordinary content needs no board")):
            result = self.call({"action": "add_comment", "issue": "ABC-1", "id": str(uuid.uuid4()),
                                "body": "Ordinary content", "native_board": "forged", "pinned_db_path": "/forged"})
            self.assertTrue(result["ok"], result)
            self.assertEqual(self.seen[-1].native_board, "")
            self.assertEqual(self.seen[-1].pinned_db_path, "")
            self.assertTrue(self.call({"action": "status", "issue": "ABC-1"})["ok"])

    def test_linked_issue_content_survives_missing_native_storage_in_registered_status(self):
        from hermes_cli import kanban_db as kb
        self.bind_api()
        task = self.board.create(title="Existing native work")
        self.assertTrue(self.call({"action": "track", "issue": "ABC-1", "task_id": task.id})["ok"])
        database = Path(kb.kanban_db_path(board="default"))
        saved = database.with_name("unavailable-original.db")
        database.rename(saved)
        try:
            for marker in ({}, {"HERMES_KANBAN_TASK": "native-worker", "HERMES_KANBAN_BOARD": "default",
                                "HERMES_KANBAN_DB": str(database)}):
                with self.subTest(marker=marker), patch.dict(os.environ, marker):
                    result = self.call({"action": "status", "issue": "ABC-1"})
                    self.assertTrue(result["ok"], result)
                    self.assertEqual(result["issue"]["id"], "issue-1")
                    self.assertFalse(result["native"]["linked"])
                    followup = self.call({"action": "followup", "issue": "ABC-1", "body": "Instruction", "id": str(uuid.uuid4())})
                    self.assertFalse(followup["ok"], followup)
                    self.assertFalse(database.exists())
        finally:
            saved.rename(database)

    def specialist_content_bridge(self, *, bound=True):
        self.specialist_sessions = {"linear-request": {"id": "linear-request", "creator": {"id": "allowed-requester"},
                                                     "appUser": {"id": "app-self"}, "issue": {"id": "issue-1"}}}
        original = self.remote._apply
        def authority(query, variables):
            if "IdentityBinding" in query:
                return 200, {"data": {"viewer": {"id": "app-self"}, "organization": {"id": "org-1"}}}
            if "AgentSessionAuthorization" in query:
                return 200, {"data": {"agentSession": self.specialist_sessions.get(variables["id"])}}
            return original(query, variables)
        self.remote._apply = authority
        api = plugin.BoundLinearAPI(lambda: "synthetic-token", endpoint=self.remote.url,
            identity={"viewer_id": "app-self", "organization_id": "org-1"},
            specialist_scope={"all_teams": True, "all_projects": True,
                              "allowed_requester_ids": ["allowed-requester"], "requester_basis": "agent_session"})
        self.bridge = Bridge(self.bridge.store, api, self.board, profile="default", clock=self.clock)
        self.bridge.profile_home = self.home
        if bound:
            task = self.board.create(title="Existing specialist request", initial_status="blocked")
            self.assertTrue(self.bridge.store.bind_native("issue-1", task.id, "default", "default",
                                                         owner_ref="linear-request", at=self.clock()))
            return task

    def test_specialist_registered_content_persists_verified_requester_and_delivers(self):
        self.bind_api()
        task = self.specialist_content_bridge()
        request = str(uuid.uuid4())
        result = self.call({"action": "add_comment", "issue": "ABC-1", "id": request, "body": "Authorized ordinary comment"})
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.bridge.store.outbox_row(request)["payload"]["requester_sessions"], {"issue-1": "linear-request"})
        self.assertEqual(self.bridge.flush(), 1)
        self.assertEqual(self.remote.comments[0]["body"], "Authorized ordinary comment")
        after = self.board.get(task.id)
        self.assertEqual((after.status, after.current_run_id, after.claim_lock),
                         (task.status, task.current_run_id, task.claim_lock))

    def test_specialist_unbound_cross_issue_or_revoked_requester_refuses_before_queue(self):
        self.bind_api()
        self.specialist_content_bridge(bound=False)
        request = str(uuid.uuid4())
        args = {"action": "add_comment", "issue": "ABC-1", "id": request, "body": "Authorized comment"}
        result = self.call(args)
        self.assertFalse(result["ok"], result)
        self.assertIsNone(self.bridge.store.outbox_row(request))
        # Establish the original native request without fabricating outbox authority.
        task = self.board.create(title="Existing request", initial_status="blocked")
        self.assertTrue(self.bridge.store.bind_native("issue-1", task.id, "default", "default",
                                                     owner_ref="linear-request", at=self.clock()))
        self.specialist_sessions["linear-request"]["issue"] = {"id": "other-issue"}
        result = self.call(args)
        self.assertFalse(result["ok"], result)
        self.assertIsNone(self.bridge.store.outbox_row(request))
        self.specialist_sessions["linear-request"].update(issue={"id": "issue-1"}, creator={"id": "revoked-requester"})
        result = self.call(args)
        self.assertFalse(result["ok"], result)
        self.assertIsNone(self.bridge.store.outbox_row(request))
        self.assertEqual(self.remote.comments, [])

    def test_specialist_delivery_never_borrows_a_later_binding_requester(self):
        self.bind_api()
        self.specialist_content_bridge()
        request = str(uuid.uuid4())
        result = self.call({"action": "add_comment", "issue": "ABC-1", "id": request, "body": "Original authorized comment"})
        self.assertTrue(result["ok"], result)
        self.specialist_sessions["linear-request"]["creator"] = {"id": "revoked-requester"}
        self.specialist_sessions["later-request"] = {"id": "later-request", "creator": {"id": "allowed-requester"},
                                                    "appUser": {"id": "app-self"}, "issue": {"id": "issue-1"}}
        with self.bridge.store._tx() as db:
            db.execute("UPDATE work SET owner_ref='later-request' WHERE issue_id='issue-1'")
        self.assertEqual(self.bridge.flush(), 0)
        self.assertEqual(self.remote.comments, [])
        self.assertEqual(self.bridge.store.outbox_row(request)["payload"]["requester_sessions"], {"issue-1": "linear-request"})

    def test_content_with_retained_stopped_chat_does_not_adopt_or_execute_it(self):
        self.bind_api()
        self.bridge.store.put("issue-1", "chat", "retained-old-chat")
        prior = self.bridge.store.get("issue-1")
        result = self.call({"action": "add_comment", "issue": "ABC-1", "id": str(uuid.uuid4()), "body": "### Reconciliation\nRetain the original execution."})
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.bridge.store.get("issue-1"), prior)
        self.assertEqual(self.bridge.flush(), 1)
        self.assertEqual(len(self.remote.comments), 1)

    def test_description_conflict_and_offline_refusals_preserve_native_tasks(self):
        self.bind_api()
        task = self.board.create(title="Unrelated native task")
        result = self.call({"action": "update_description", "issue": "ABC-1", "id": str(uuid.uuid4()),
                            "description": "New content", "expected_description": "A stale description"})
        self.assertEqual(result["code"], "description_conflict")
        self.remote.down = True
        result = self.call({"action": "status", "issue": "ABC-1"})
        self.assertFalse(result["ok"])
        self.assertEqual(self.board.get(task.id).status, task.status)
        self.assertEqual(self.bridge.store.pending(), [])

    def test_native_delegated_child_guard_survives_registered_handler_rpc(self):
        from agent.delegation_context import delegated_child_context
        self.bind_api()
        task = self.board.create(title="Existing worker")
        self.assertTrue(self.call({"action": "track", "issue": "ABC-1", "task_id": task.id})["ok"])
        with delegated_child_context("native-child"):
            result = self.call({"action": "followup", "issue": "ABC-1", "body": "Instruction", "id": str(uuid.uuid4())})
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "native_scope_refused")
        self.assertTrue(self.seen[-1].delegated_child)
        with self.board.conn() as conn:
            self.assertEqual(self.board.kb.list_comments(conn, task.id), [])

    def test_official_plugin_unload_reload_stops_single_reporting_host(self):
        from hermes_cli.plugins import PluginContext, PluginManifest
        # Use the actual reporting loop and native plugin ownership ledger. No runtime
        # profile-service method is supplied by the test or required by the plugin.
        self.manager.unload()
        self.server.close()
        settings = {"enabled": True, "identity": {"viewer_id": "app-self", "organization_id": "org-1"},
                    "api_url": self.remote.url, "tick_seconds": 0.05}
        (self.home / "config.yaml").write_text(json.dumps({"plugins": {"entries": {"linear": {"settings": settings}}}}))
        original = self.remote._apply
        def identity(query, variables):
            if "IdentityBinding" in query:
                return 200, {"data": {"viewer": {"id": "app-self"}, "organization": {"id": "org-1"}}}
            return original(query, variables)
        self.remote._apply = identity
        def wait_ready():
            until = time.monotonic() + 5
            while time.monotonic() < until:
                try:
                    if transport.request(self.home, {"op": "status"})["ready"]:
                        return
                except transport.Unavailable:
                    threading.Event().wait(0.02)
            self.fail("official plugin reporting host did not become ready")
        with patch.dict(os.environ, {"_HERMES_GATEWAY": "1"}), patch.object(plugin, "token_provider", return_value=lambda: "synthetic-token"):
            for _ in range(2):
                plugin.register(PluginContext(PluginManifest(name="linear"), self.manager))
                wait_ready()
                self.assertEqual(sum(t.name == "linear-reporting" for t in threading.enumerate()), 1)
                self.manager.unload()
                self.assertFalse(transport.endpoint(self.home).exists())
                self.assertFalse(any(t.name == "linear-reporting" for t in threading.enumerate()))

    def test_removed_ceremonies_are_not_registered_actions(self):
        actions = chat.SCHEMA["parameters"]["properties"]["action"]["enum"]
        self.assertTrue({"track", "status", "followup"} <= set(actions))
        self.assertFalse({"start", "claim", "delegate", "transfer", "resume_transfer", "done"} & set(actions))
        self.assertTrue({"on_kanban_worker_spawned", "on_kanban_task_updated"} <= set(self.manager._hooks))
