"""Real Agent journal and registered Linear tool handoff scenarios."""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from linear_fake_api import Clock, FakeLinear, SELF, load_plugin

SOURCE = os.environ.get("HERMES_AGENT_SRC", "")
if SOURCE:
    sys.path.insert(0, SOURCE)
try:
    if not SOURCE:
        raise ImportError("HERMES_AGENT_SRC is not set")
    from gateway.chat_execution_recovery import ChatExecutionStore
    from tools import registry
    CORE = None
except ImportError as exc:
    CORE = str(exc)

plugin = load_plugin()
from hermes_fleet_linear_plugin import chat, transport
from hermes_fleet_linear_plugin.api import LinearAPI
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.store import Store


@unittest.skipIf(CORE is not None, f"integration: Agent source unavailable ({CORE})")
class ChatTransferScenarios(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.home = Path(tmp.name)
        self.clock = Clock()
        self.linear = FakeLinear(self.clock)
        self.addCleanup(self.linear.close)
        self.linear.add_issue("issue-1", "ABC-1")
        self.linear.set_delegate("issue-1", {"id": SELF, "name": "This Agent"})
        self.linear.set_state("issue-1", "Blocked")
        self.store = Store(self.home / "linear.db")
        self.core = ChatExecutionStore(self.home)
        self.old_generation = self.core.start_local_worker("old-chat", "old-real-session")
        self.assertEqual(self.old_generation, 1)
        self.assertTrue(self.core.observe_tool("old-chat", self.old_generation))
        self.assertTrue(self.core.worker_finished("old-chat", self.old_generation))
        self.assertTrue(self.store.put("issue-1", "chat", "old-chat", project_id="project-1",
                                       run_generation=self.old_generation))
        self.previous = self.store.get("issue-1")
        self.bridge = Bridge(self.store, LinearAPI(lambda: "fixture", endpoint=self.linear.url),
                             SimpleNamespace(profile_home=self.home), profile="alpha", clock=self.clock)
        self.bridge.profile_home = self.home
        self.bridge.retire_chat = lambda key, generation, request: self.core.retire_for_handoff(
            key, generation, request)
        self.request_id = str(uuid.uuid4())
        self.args = {"action": "transfer", "issue": "ABC-1", "id": self.request_id,
                     "expected_owner": self.previous["ownership_id"]}

    def test_registered_native_route_retains_unknown_effects_and_requires_explicit_resume(self):
        from hermes_fleet_linear_plugin import admission
        from tools import registry
        successor_generation = self.core.start_local_worker("new-chat", "new-real-session")
        self.assertEqual(successor_generation, 1)
        context = registry.ToolInvocationContext(profile="alpha", platform="telegram", chat_id="chat",
            chat_type="dm", session_key="new-chat", session_id="new-real-session",
            run_generation=successor_generation)
        source = SimpleNamespace(platform=SimpleNamespace(value="telegram"), chat_id="chat",
            thread_id="", chat_type="dm", user_id="", scope_id="")
        event = SimpleNamespace(source=source, internal=False, text="Transfer ABC-1 to this chat")
        turn = SimpleNamespace(session_key=context.session_key, session_id=context.session_id,
                               run_generation=successor_generation, _run_still_current=lambda: True)
        state = SimpleNamespace(persistent=SimpleNamespace(run_generation=successor_generation),
                                turn=SimpleNamespace(agent=object(), ctx=turn, event=event))
        gateway = SimpleNamespace(_peek_session_state=lambda key: state,
            _chat_stop_profile_matches=lambda key, home: Path(home).resolve() == self.home,
            _is_user_authorized_for_source=lambda candidate: candidate is source)
        with sqlite3.connect(self.home / "state.db") as db:
            db.execute("CREATE TABLE sessions(id TEXT PRIMARY KEY, ended_at REAL)")
            db.execute("INSERT INTO sessions VALUES (?, NULL)", (context.session_id,))
        with patch.object(registry, "_current_tool_invocation_context", return_value=context):
            self.assertTrue(admission.bound_to_live_turn(SimpleNamespace(
                gateway=gateway, profile_home=self.home, profile_name="alpha"), context))

        class Registration:
            def get_config(self, key, default=None): return True if key == "enabled" else default
            def register_tool(self, **kwargs): self.tool = kwargs
            def register_hook(self, *args): pass
            def register_profile_service(self, name, factory): self.service = factory

        registration = Registration()
        plugin.register(registration)
        self.assertIn("transfer", registration.tool["schema"]["parameters"]["properties"]["action"]["enum"])
        self.assertIn("resume_transfer", registration.tool["schema"]["parameters"]["properties"]["action"]["enum"])
        captured = []
        def at_recover(*_):
            handler = registration.tool["handler"]
            captured.append(json.loads(handler(self.args, context)))
            captured.append(json.loads(handler(self.args, context)))
            event.text = "I reconciled ABC-1 against https://example.test/effects and continue here"
            captured.append(json.loads(handler({"action": "resume_transfer", "issue": "ABC-1",
                                                 "id": self.request_id, "evidence": "https://example.test/effects"}, context)))
        async def run_service():
            stop = asyncio.Event(); stop.set()
            await registration.service(SimpleNamespace(gateway=gateway, profile_home=self.home,
                                                       profile_name="alpha", stop_event=stop))
        with patch.object(plugin, "BoundLinearAPI", return_value=self.bridge.api), \
                patch.object(plugin, "token_provider", return_value=lambda: "fixture"), \
                patch.object(plugin, "Store", return_value=self.store), \
                patch.object(plugin, "Kanban", return_value=self.bridge.kanban), \
                patch.object(plugin, "Bridge", return_value=self.bridge), \
                patch.object(self.bridge, "chat_profile_matches", return_value=True), \
                patch.object(self.bridge, "recover", side_effect=at_recover), \
                patch.object(registry, "_current_tool_invocation_context", return_value=context):
            asyncio.run(run_service())
        self.assertEqual(captured[0], captured[1])
        self.assertTrue(captured[0]["ok"], captured)
        self.assertEqual(captured[0]["predecessor_effects"], "unknown")
        self.assertFalse(captured[0]["execution_admitted"])
        self.assertTrue(captured[2]["ok"], captured)
        self.assertEqual(self.store.get("issue-1")["owner_ref"], "new-chat")
        self.assertEqual(self.store.get("issue-1")["project_id"], "project-1")
        self.assertFalse(self.core.start("old-chat", 2, "late-old"))
        self.assertFalse(any(query.startswith("mutation") for query in self.linear.requests))

    def test_serialized_desktop_identity_moves_control_without_live_gateway_grant(self):
        from gateway.session_context import clear_session_vars, set_session_vars, set_turn_user_message
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            self.bridge.validate_chat_admission = lambda context: False
            tokens = set_session_vars(platform="desktop", profile="alpha", session_key="desktop-chat",
                session_id="actual-desktop-session", browser_control_provider="synthetic-ticket",
                browser_control_subject="synthetic-user", ui_session_id="stable-desktop-tab")
            try:
                set_turn_user_message("Transfer ABC-1 to this chat")
                context = registry._current_tool_invocation_context()
                server = transport.Server(self.home, "alpha", lambda args, host:
                    chat.handle(self.bridge, args, host), lambda _: None)
                try:
                    result = json.loads(transport.chat_request(self.home, self.args, context))
                finally:
                    server.close()
            finally:
                clear_session_vars(tokens)
        self.assertTrue(result["ok"], result)
        self.assertFalse(result["execution_admitted"])
        self.assertEqual(self.store.get("issue-1")["owner_ref"], "desktop-chat")
        self.assertEqual(self.store.chat_transfer(self.request_id)["retirement"]["effects"], "unknown")
        self.assertFalse(self.core.start_local_worker("old-chat", "late"))
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            tokens = set_session_vars(platform="desktop", profile="alpha", session_key="desktop-after-compress",
                session_id="compressed-session", browser_control_provider="synthetic-ticket",
                browser_control_subject="synthetic-user", ui_session_id="stable-desktop-tab")
            try:
                set_turn_user_message("Transfer ABC-1 to this chat")
                context = registry._current_tool_invocation_context()
                server = transport.Server(self.home, "alpha", lambda args, host:
                    chat.handle(self.bridge, args, host), lambda _: None)
                try:
                    retry = json.loads(transport.chat_request(self.home, self.args, context))
                finally:
                    server.close()
            finally:
                clear_session_vars(tokens)
        self.assertEqual(retry, result)
        self.assertEqual(self.store.get("issue-1")["owner_ref"], "desktop-after-compress")

    def test_serialized_authenticated_api_instruction_moves_control(self):
        from gateway.session_context import (clear_session_vars, set_api_transfer_identity,
                                             set_session_vars, set_turn_user_message)
        self.bridge.validate_chat_admission = lambda context: False
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            tokens = set_session_vars(platform="api_server", profile="alpha", session_key="api-run-1",
                                      session_id="api-conversation")
            try:
                set_turn_user_message("Transfer ABC-1 to this chat")
                set_api_transfer_identity("principal:alpha:verified", "local-api", "api-conversation")
                context = registry._current_tool_invocation_context()
                server = transport.Server(self.home, "alpha", lambda args, host:
                    chat.handle(self.bridge, args, host), lambda _: None)
                try:
                    result = json.loads(transport.chat_request(self.home, self.args, context))
                finally:
                    server.close()
            finally:
                clear_session_vars(tokens)
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.store.get("issue-1")["owner_ref"], "api-run-1")
        self.assertFalse(result["execution_admitted"])
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            tokens = set_session_vars(platform="api_server", profile="alpha", session_key="api-run-2",
                                      session_id="api-conversation")
            try:
                set_turn_user_message("Transfer ABC-1 to this chat")
                set_api_transfer_identity("principal:alpha:verified", "local-api", "api-conversation")
                context = registry._current_tool_invocation_context()
                server = transport.Server(self.home, "alpha", lambda args, host:
                    chat.handle(self.bridge, args, host), lambda _: None)
                try:
                    retry = json.loads(transport.chat_request(self.home, self.args, context))
                finally:
                    server.close()
            finally:
                clear_session_vars(tokens)
        self.assertEqual(retry, result)
        self.assertEqual(self.store.get("issue-1")["owner_ref"], "api-run-2")
