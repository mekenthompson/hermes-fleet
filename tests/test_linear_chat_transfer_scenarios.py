"""Real Agent journal and registered Linear tool handoff scenarios."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import os
import subprocess
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
from hermes_fleet_linear_plugin.chat_transfer import _retirement_sha256
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
        self.bridge.chat_lineage = self.core.lineage_keys
        self.request_id = str(uuid.uuid4())
        self.args = {"action": "transfer", "issue": "ABC-1", "id": self.request_id,
                     "expected_owner": self.previous["ownership_id"]}

    def add_reconciliation(self, link="https://example.test/effects"):
        saved = self.store.chat_transfer(self.request_id)
        self.linear.comments.append({"issueId": "issue-1", "author_id": "human-1",
            "createdAt": self.clock.iso(1), "body": json.dumps({
                "kind": "chat-transfer-reconciliation/v1", "issue_id": "issue-1",
                "transfer_id": self.request_id,
                "predecessor_owner": saved["predecessor"]["ownership_id"],
                "retirement_sha256": _retirement_sha256(saved["retirement"]),
                "decision": "resume_after_review", "evidence": link})})

    def test_registered_native_route_retains_unknown_effects_and_requires_explicit_resume(self):
        from hermes_fleet_linear_plugin import admission
        from tools import registry
        successor_generation = self.core.start_local_worker("new-chat", "new-real-session")
        self.assertEqual(successor_generation, 1)
        context = registry.ToolInvocationContext(profile="alpha", platform="telegram", chat_id="chat",
            chat_type="dm", user_id="alice", session_key="new-chat", session_id="new-real-session",
            run_generation=successor_generation, user_message="Transfer ABC-1 to this chat")
        source = SimpleNamespace(platform=SimpleNamespace(value="telegram"), chat_id="chat",
            thread_id="", chat_type="dm", user_id="alice", scope_id="")
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
        active = [context]
        def at_recover(*_):
            handler = registration.tool["handler"]
            captured.append(json.loads(handler(self.args, context)))
            captured.append(json.loads(handler(self.args, context)))
            self.add_reconciliation()
            event.text = "I reconciled ABC-1 against https://example.test/effects and continue here"
            captured.append(json.loads(handler({"action": "resume_transfer", "issue": "ABC-1",
                                                 "id": self.request_id, "evidence": "https://example.test/effects"}, context)))
            next_context = replace(context, user_message=event.text, message_id="next-turn")
            active[0] = next_context
            captured.append(json.loads(handler({"action": "resume_transfer", "issue": "ABC-1",
                                                 "id": self.request_id, "evidence": "https://example.test/effects"}, next_context)))
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
                patch.object(registry, "_current_tool_invocation_context", side_effect=lambda: active[0]):
            asyncio.run(run_service())
        self.assertEqual(captured[0], captured[1])
        self.assertTrue(captured[0]["ok"], captured)
        self.assertEqual(captured[0]["predecessor_effects"], "unknown")
        self.assertFalse(captured[0]["execution_admitted"])
        self.assertFalse(captured[2]["ok"], captured)
        self.assertTrue(captured[3]["ok"], captured)
        self.assertEqual(self.store.get("issue-1")["owner_ref"], "new-chat")
        self.assertEqual(self.store.get("issue-1")["project_id"], "project-1")
        self.assertFalse(self.core.start("old-chat", 2, "late-old"))
        self.assertFalse(any(query.startswith("mutation") for query in self.linear.requests))

    def test_serialized_desktop_identity_moves_control_without_live_gateway_grant(self):
        from gateway.session_context import (clear_session_vars, set_session_run_generation,
                                             set_session_vars, set_turn_user_message)
        desktop_generation = self.core.start_local_worker("desktop-chat", "actual-desktop-session")
        self.assertEqual(desktop_generation, 1)
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            self.bridge.validate_chat_admission = lambda context: False
            tokens = set_session_vars(platform="desktop", profile="alpha", session_key="desktop-chat",
                session_id="actual-desktop-session", browser_control_provider="synthetic-ticket",
                browser_control_subject="synthetic-user", ui_session_id="stable-desktop-tab")
            try:
                set_session_run_generation(desktop_generation)
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
        compressed_generation = self.core.start_local_worker("desktop-after-compress", "compressed-session")
        self.assertEqual(compressed_generation, 1)
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            tokens = set_session_vars(platform="desktop", profile="alpha", session_key="desktop-after-compress",
                session_id="compressed-session", browser_control_provider="synthetic-ticket",
                browser_control_subject="synthetic-user", ui_session_id="stable-desktop-tab")
            try:
                set_session_run_generation(compressed_generation)
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
        self.assertTrue(self.store.update("issue-1", linear_session_id="linear-s"))
        self.bridge.handle_webhook(self.linear.session_event(
            "prompted", "issue-1", "other-s", signal="stop", activity_id="wrong-target"))
        self.assertEqual(self.store.stop_intents(), [])
        self.clock.now += 1
        self.bridge.handle_webhook(self.linear.session_event(
            "prompted", "issue-1", "linear-s", signal="stop", activity_id="stop-control"))
        saved_stop = Store(self.home / "linear.db").stop_intents()
        self.assertEqual(len(saved_stop), 1)
        self.assertEqual(saved_stop[0]["status"], "unsupported")
        self.assertIsNone(saved_stop[0]["run_generation"])
        class NoNativeStop:
            async def get_chat_run_stop_observation(self, **kwargs):
                raise AssertionError("control Stop has no native generation")
            async def request_chat_run_stop(self, **kwargs):
                raise AssertionError("control Stop has no native generation")
        asyncio.run(plugin.process_chat_stops(self.bridge, SimpleNamespace(
            gateway=NoNativeStop(), profile_home=self.home)))
        self.assertTrue(self.store.get("issue-1")["stop_requested_at"])
        self.bridge.tick()
        self.assertTrue(any("Cancellation is unsupported" in activity["content"]["body"]
                            for activity in self.linear.activities))

    def test_unbound_same_uid_socket_packet_cannot_invent_desktop_instruction(self):
        self.assertEqual(self.core.start_local_worker("forged-chat", "forged-session"), 1)
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            server = transport.Server(self.home, "alpha", lambda args, context:
                chat.handle(self.bridge, args, context), lambda _: None)
            try:
                payload = {"op": "chat", "home": str(self.home), "args": self.args,
                    "context": {"profile": "alpha", "platform": "desktop", "session_key": "forged-chat",
                                "session_id": "forged-session", "run_generation": 1},
                    "transfer_auth": {"browser_control_provider": "forged-provider",
                        "browser_control_subject": "forged-subject", "user_message": "Transfer ABC-1 to this chat",
                        "ui_session_id": "forged-tab", "api_control_principal": "",
                        "api_control_transport_family": "", "api_conversation_key": ""}}
                script = "import socket,sys; s=socket.socket(socket.AF_UNIX); s.connect(sys.argv[1]); s.sendall(sys.stdin.buffer.read()); print(s.recv(65536).decode())"
                child = subprocess.run([sys.executable, "-c", script, str(transport.endpoint(self.home))],
                                       input=(json.dumps(payload) + "\n").encode(), capture_output=True, timeout=10)
            finally:
                server.close()
        self.assertEqual(child.returncode, 0, child.stderr)
        self.assertTrue(child.stdout.strip())
        self.assertEqual(json.loads(json.loads(child.stdout)["result"])["code"], "unverified_host_turn")
        self.assertIsNone(self.store.chat_transfer(self.request_id))
        self.assertEqual(self.store.get("issue-1"), self.previous)
        self.assertEqual(self.core.start_local_worker("old-chat", "late-turn"), 2)

    def test_shared_predecessor_refusal_leaves_other_issue_running(self):
        from gateway.session_context import (clear_session_vars, set_session_run_generation,
                                             set_session_vars, set_turn_user_message)
        self.assertTrue(self.store.put("issue-2", "chat", "old-chat",
                                       run_generation=self.old_generation))
        destination_generation = self.core.start_local_worker("desktop-chat", "actual-desktop-session")
        self.assertEqual(destination_generation, 1)
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            tokens = set_session_vars(platform="desktop", profile="alpha", session_key="desktop-chat",
                session_id="actual-desktop-session", browser_control_provider="synthetic-ticket",
                browser_control_subject="synthetic-user", ui_session_id="stable-desktop-tab")
            try:
                set_session_run_generation(destination_generation)
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
        self.assertEqual(result["code"], "shared_predecessor")
        self.assertIsNone(self.store.chat_transfer(self.request_id))
        self.assertEqual(self.store.get("issue-1"), self.previous)
        self.assertEqual(self.store.get("issue-2")["owner_ref"], "old-chat")
        self.assertEqual(self.core.start_local_worker("old-chat", "next-real-session"), 2)

    def test_compressed_predecessor_refuses_before_intent_or_other_issue_fence(self):
        from gateway.session_context import (clear_session_vars, set_session_run_generation,
                                             set_session_vars, set_turn_user_message)
        self.assertTrue(self.core.link_session_keys("old-chat", "compressed-chat"))
        child_generation = self.core.start_local_worker("compressed-chat", "child-agent")
        self.assertEqual(child_generation, 1)
        self.assertTrue(self.core.worker_finished("compressed-chat", child_generation))
        self.assertTrue(self.store.put("issue-2", "chat", "compressed-chat",
                                       run_generation=child_generation))
        destination_generation = self.core.start_local_worker("desktop-chat", "actual-desktop-session")
        self.assertEqual(destination_generation, 1)
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            tokens = set_session_vars(platform="desktop", profile="alpha", session_key="desktop-chat",
                session_id="actual-desktop-session", browser_control_provider="synthetic-ticket",
                browser_control_subject="synthetic-user", ui_session_id="stable-desktop-tab")
            try:
                set_session_run_generation(destination_generation)
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
        self.assertEqual(result["code"], "compressed_predecessor")
        self.assertIsNone(self.store.chat_transfer(self.request_id))
        self.assertEqual(self.store.get("issue-1"), self.previous)
        self.assertEqual(self.store.get("issue-2")["owner_ref"], "compressed-chat")
        self.assertEqual(self.core.start_local_worker("compressed-chat", "unrelated-worker"), 2)

    def test_serialized_authenticated_api_instruction_moves_control(self):
        from gateway.session_context import (clear_session_vars, set_api_transfer_identity,
                                             set_session_run_generation, set_session_vars, set_turn_user_message)
        self.bridge.validate_chat_admission = lambda context: False
        first_generation = self.core.start_local_worker("api-run-1", "api-conversation")
        self.assertEqual(first_generation, 1)
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            tokens = set_session_vars(platform="api_server", profile="alpha", session_key="api-run-1",
                                      session_id="api-conversation")
            try:
                set_session_run_generation(first_generation)
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
        second_generation = self.core.start_local_worker("api-run-2", "api-conversation")
        self.assertEqual(second_generation, 1)
        with patch.object(self.bridge, "chat_profile_matches", return_value=True):
            tokens = set_session_vars(platform="api_server", profile="alpha", session_key="api-run-2",
                                      session_id="api-conversation")
            try:
                set_session_run_generation(second_generation)
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
