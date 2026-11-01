"""Guarded real Base/authority/SQLite/provider consumer with finite endpoints only.

The provider transport, model decisions and runner settings are explicit test
fixtures. Native input admission, lifecycle, durable receipt, controls, task
storage and scoped outbox are production code. No gateway/model bootstrap.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
from types import MethodType
import unittest
from unittest.mock import patch

SOURCE = os.environ.get("HERMES_AGENT_SRC")
GUARDED_ROOT = os.environ.get("HERMES_LINEAR_INPUT_TEST_ROOT")


@unittest.skipUnless(SOURCE and GUARDED_ROOT, "requires scripts/run-linear-input-scenarios.py")
class NativeInputSeamScenarios(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Reuse Core's finite settings/agent boundary, never GatewayRunner.
        spec = importlib.util.spec_from_file_location("finite_native_retry_fixture",
            Path(SOURCE) / "tests/gateway/test_native_inbound_retry.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.finite_agent_type = module.FiniteAgent
        self.fixture = module.NativeInboundRetryTests()
        await self.fixture.asyncSetUp()
        self.addCleanup(self.fixture.doCleanups)
        self.home, self.runner, self.authority = self.fixture.home, self.fixture.runner, self.fixture.authority
        environment = patch.dict(os.environ, HERMES_HOME=str(self.home),
            HERMES_KANBAN_HOME=str(self.home / "board"), HERMES_PROFILE="default")
        environment.start()
        self.addCleanup(environment.stop)
        from linear_fake_api import load_plugin
        plugin = load_plugin()
        from test_linear_specialist_authorization import Authority, USER, VIEWER, ISSUE, SESSION
        from hermes_fleet_linear_plugin.bridge import Bridge, Kanban
        from hermes_fleet_linear_plugin.store import Store
        from hermes_fleet_linear_plugin import native_input
        from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
        from gateway.config import PlatformConfig
        from gateway.platform_registry import platform_registry
        self.native_input, self.USER, self.ISSUE, self.SESSION = native_input, USER, ISSUE, SESSION
        self.provider = Authority()
        self.provider.issues[ISSUE]["delegate"] = {"id": VIEWER}
        self.fixture.allowed.add(USER)
        self.board = Kanban(profile="default", profile_home=self.home)
        self.bridge = Bridge(Store(self.home / "linear.db"), self.provider.api(scope=None),
            self.board, profile="default", clock=lambda: 1900000000.0)
        self.bridge.profile_home = self.home
        (self.home / "config.yaml").write_text("platforms:\n  linear:\n    enabled: true\nplugins:\n  entries:\n    linear:\n      settings:\n        enabled: true\n")
        self.manager = PluginManager(scope_key=str(self.home))
        self.addCleanup(self.manager.unload)
        self.context = PluginContext(PluginManifest(name="linear", source="user", kind="standalone"), self.manager)
        registered = {}
        register_platform = native_input.register_platform
        def observe_platform(context, running):
            registered["running"] = running
            register_platform(context, running)
        # Observe the factory's real closure; do not replace its route or create
        # a second reporting service in this finite host.
        with patch.object(native_input, "register_platform", side_effect=observe_platform):
            plugin.register(self.context)
        self.running = registered["running"]
        self.running["bridge"] = self.bridge
        self.addCleanup(platform_registry.unregister, "linear", scope=self.manager.scope_key)
        entry = platform_registry.get("linear")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.allowed_users_env, "LINEAR_ALLOWED_USERS")
        self.assertTrue(entry.check_fn())
        self.adapter = entry.adapter_factory(PlatformConfig(enabled=True, typing_indicator=False))
        self.assertIs(self.adapter.requires_durable_input_ack, True)
        self.assertTrue(await self.adapter.connect())
        self.runner.adapters[self.adapter.platform] = self.adapter
        self.runner._intake_adapter_for = lambda source: self.adapter
        self.runner._delivery_adapter_for = lambda source: self.adapter
        self.runner._is_session_running = lambda key: False
        self.receipts, self.events, self.decisions = [], [], []
        admitted = self.adapter.on_message_admitted
        async def receipt(event, value):
            await admitted(event, value)
            self.receipts.append((event, value))
        self.adapter.on_message_admitted = receipt
        async def handle(runner, event):
            from gateway.run_inbound import GatewayInboundMixin
            from gateway.session_ingress import executing_admission
            self.events.append(event)
            if executing_admission.get():
                # The finite host binds only the actual restored native source
                # and real owner session, just as a gateway producer does.
                from gateway.session_context import set_session_vars, clear_session_vars
                source = event.source
                route = self.fixture.store._generate_session_key(source)
                entry = self.fixture.store.lookup_by_session_key(route)
                tokens = set_session_vars(platform=source.platform.value, source=source.platform.value,
                    chat_id=source.chat_id, chat_type=source.chat_type, thread_id=source.thread_id or "",
                    user_id=source.user_id or "", user_id_alt=source.user_id_alt or "",
                    scope_id=source.scope_id or "", session_key=route, session_id=entry.session_id,
                    message_id=event.message_id, profile="default")
                try:
                    return await self.model(event)
                finally:
                    clear_session_vars(tokens)
            _, reply = await GatewayInboundMixin._hm_authority_admission(runner, event,
                event.source, self.fixture.store._generate_session_key(event.source), False)
            return reply
        self.runner._handle_message = MethodType(handle, self.runner)
        self.adapter.set_message_handler(self.runner._handle_message)
        self.adapter.set_session_store(self.fixture.store)
        self.bridge.native_ingress = lambda delivery: native_input.route_delivery(self.running, delivery)
        from linear_ingress_fixture import IngressStore, Route
        self.inbox = IngressStore(self.home / "inbox.db")
        self.inbox_route = Route("provider-fixture", "default", "/unused", self.home / "unused-secret", self.home)
        self.model = self.no_model

    async def asyncTearDown(self):
        for future in self.authority.waiters.values():
            if not future.done():
                future.cancel()
        for future in list(self.adapter._submitted.values()):
            future.cancel()
        await self.adapter.disconnect()
        tasks = [live.task for live in self.authority.sessions.values() if live.task and not live.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for future in self.authority.waiters.values():
            if not future.done():
                future.cancel()

    async def no_model(self, event):
        self.fail("No finite model was enabled for this case")

    async def until(self, predicate):
        try:
            async with asyncio.timeout(10):
                while not predicate():
                    await asyncio.sleep(0.01)
        except TimeoutError:
            import faulthandler
            print("PENDING_NATIVE_INPUT", [(key, f.done(), repr(f.exception()) if f.done() and not f.cancelled() else None)
                for key, f in self.adapter._submitted.items()], flush=True)
            print("ADMITTED_IDS", [event.message_id for event, _ in self.receipts], flush=True)
            faulthandler.dump_traceback()
            raise

    def event(self, identity, body, *, signal=None, action="prompted"):
        from test_linear_specialist_authorization import USER
        self.provider.activities[identity] = {"id": identity, "user": {"id": USER},
                                              "agentSession": {"id": self.SESSION}}
        activity = {"id": identity, "user": {"id": USER}, "content": {"body": body}}
        if signal:
            activity["signal"] = signal
        return {"type": "AgentSessionEvent", "action": action,
                "createdAt": datetime.fromtimestamp(self.bridge.clock(), timezone.utc).isoformat(),
                "agentSession": {"id": self.SESSION, "issue": {"id": self.ISSUE}, "creatorId": USER},
                "agentActivity": activity}

    async def deliver(self, identity, body, **kwargs):
        raw = json.dumps(self.event(identity, body, **kwargs), separators=(",", ":")).encode()
        with patch("linear_ingress_fixture.time.time", return_value=self.bridge.clock()):
            self.inbox.enqueue(self.inbox_route, identity, raw)
        await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database)
        return raw

    async def deliver_raw(self, identity, event):
        raw = json.dumps(event, separators=(",", ":")).encode()
        with patch("linear_ingress_fixture.time.time", return_value=self.bridge.clock()):
            self.inbox.enqueue(self.inbox_route, identity, raw)
        await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database)
        return raw

    def inbox_row(self, identity):
        with self.inbox._connect() as db:
            return db.execute("SELECT profile,payload,status,attempts FROM deliveries WHERE delivery_id=?", (identity,)).fetchone()

    def rows(self):
        from hermes_state_runtime import list_session_admissions
        return [row for sid in self.authority.sessions
                for row in list_session_admissions(self.authority.db, session_id=sid, pending_only=False)]

    def enable_owned_worker(self, task_id):
        """Only the finite model endpoint is fake; claim/drain/steer stay native."""
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        from hermes_cli import kanban_db as kb
        from hermes_state_runtime import get_session_admission
        fixture = self
        self.workers = []
        self.worker_started = asyncio.Event()
        self.instruction_consumed = asyncio.Event()
        class Endpoint(self.finite_agent_type):
            def __init__(self):
                super().__init__()
                self.instructions = []
                self.wake, self.release = asyncio.Event(), asyncio.Event()
                self.stopped = False
            def steer(self, text, *, ticket=None):
                accepted = super().steer(text, ticket=ticket)
                self.instructions.append(ticket)
                self.wake.set()
                return accepted
            def interrupt(self, text=None):
                super().interrupt(text)
                self.stopped = True
                self.release.set()
                self.wake.set()
        async def model(event):
            task = self.board.require_task(task_id)
            with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=self.bridge.clock() - 2):
                if task.status == "blocked":
                    latest = self.board.history(task.id, 0)[-1].id
                    self.assertTrue(kb.unblock_task(conn, task.id, expected_event_id=latest))
                claim = kb.claim_task(conn, task.id)
            self.assertIsNotNone(claim)
            owner = next(row for row in self.rows() if row["request_id"] == event.message_id)
            endpoint = Endpoint()
            endpoint.claim = claim
            self.authority.adopt_agent(owner["target_session_id"], owner["generation"], endpoint)
            self.workers.append(endpoint)
            self.decisions.append((event.message_id, event.text))
            self.worker_started.set()
            while not endpoint.release.is_set():
                await endpoint.wake.wait()
                endpoint.wake.clear()
                while endpoint.instructions:
                    ticket = endpoint.instructions.pop(0)
                    instruction = get_session_admission(self.authority.db, admission_id=ticket)
                    timestamp = datetime.fromisoformat(instruction["payload"]["native_text_v1"]["timestamp"]).timestamp() * 1000
                    await asyncio.to_thread(self.bridge.followup, self.ISSUE,
                        instruction["payload"]["text"], instruction["request_id"], source_ms=timestamp)
                    endpoint.pending.discard(ticket)
                    self.instruction_consumed.set()
            if not endpoint.stopped:
                with self.board.conn() as conn:
                    self.assertTrue(kb.complete_task(conn, task.id, expected_run_id=claim.current_run_id,
                        result="Verified bounded native remainder and original follow-up."))
            execution_result.get()["result"] = {"final_response": "Stopped." if endpoint.stopped else "Verified remainder complete.",
                "interrupted": endpoint.stopped, "completed": not endpoint.stopped}
            return "Stopped." if endpoint.stopped else "Verified remainder complete."
        self.model = model
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)

    def fixture_task(self):
        issue = self.provider.issues[self.ISSUE]
        task = self.board.create(title="Existing bounded remainder",
            body=f"Linear issue {self.ISSUE}: {issue['url']}", created_by=self.board.executor_profile)
        self.bridge.track(self.ISSUE, task.id)
        return task.id

    def specialist_bridge(self, *, session_requester=False, creator_null=False):
        from hermes_fleet_linear_plugin.bridge import Bridge
        from test_linear_specialist_authorization import SCOPE
        if creator_null:
            self.provider.sessions[self.SESSION]["creator"] = None
        scope = {**SCOPE, **({"requester_basis": "agent_session"} if session_requester else {})}
        self.bridge = Bridge(self.bridge.store, self.provider.api(scope=scope), self.board,
            profile="default", clock=lambda: 1900000000.0)
        self.bridge.profile_home = self.home
        self.running["bridge"] = self.bridge
        self.bridge.native_ingress = lambda delivery: self.native_input.route_delivery(self.running, delivery)

    async def test_creator_null_current_activity_stop_controls_linked_ready_task_before_ack(self):
        task_id = self.fixture_task()
        self.specialist_bridge(session_requester=True, creator_null=True)
        event = self.event("creator-null-stop", "stop", signal="stop")
        event["agentSession"].pop("creatorId")
        await self.deliver_raw("creator-null-stop", event)
        await self.until(lambda: self.inbox_row("creator-null-stop")[2] == "imported")
        self.assertEqual(self.board.get(task_id).status, "blocked")
        intent = self.bridge.store.native_intent(f"stop:{self.ISSUE}:creator-null-stop")
        self.assertEqual(intent["state"], "applied")
        self.assertIn("expected_event_id", intent["payload"])
        self.assertIsNone(self.provider.sessions[self.SESSION]["creator"])

    async def test_fresh_authorized_discussion_reads_and_replies_without_thawing_old_effect_fence(self):
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        from hermes_fleet_linear_plugin import chat, transport
        from hermes_fleet_linear_plugin.api import LinearError
        task_id = self.fixture_task()
        self.specialist_bridge()
        old = self.bridge.comment(self.ISSUE, "Original held native finding")
        old_row = self.bridge.store.outbox_row(old)
        denied = self.event("earlier-denied-actor", "Denied older instruction")
        self.provider.activities["earlier-denied-actor"]["user"] = {"id": "not-allowed"}
        denied["agentActivity"]["user"] = {"id": "not-allowed"}
        with self.assertRaises(LinearError):
            self.native_input.project_input(self.bridge, denied)
        self.assertTrue(self.bridge.store.scope_fenced(self.ISSUE))
        native_events = self.board.history(task_id, 0)
        async def decision(event):
            context = transport.capture_context()
            status = json.loads(chat.handle(self.bridge, {"action": "status", "issue": self.ISSUE}, context))
            self.assertTrue(status["ok"], status)
            self.assertEqual(status["issue"]["description"], self.provider.issues[self.ISSUE]["description"])
            self.assertTrue(status["native"]["effects_held"])
            effect = json.loads(chat.handle(self.bridge, {"action": "followup", "issue": self.ISSUE,
                "id": "00000000-0000-4000-8000-000000000001", "body": "Must stay held"}, context))
            self.assertFalse(effect["ok"], effect)
            execution_result.get()["result"] = {"completed": True,
                "final_response": "Current issue read; prior effects remain held."}
            return "Current issue read; prior effects remain held."
        self.model = decision
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        await self.deliver("current-allowed-discussion", "Read the current issue and discuss its status.")
        await self.until(lambda: self.rows() and self.rows()[0]["status"] == "terminal")
        await self.until(lambda: bool(self.provider.mutations))
        replies = [row for row in self.bridge.store.pending() if self.bridge.store.conversation_reply(row["kind"], row["payload"])]
        self.assertEqual(replies, [])
        self.assertTrue(self.bridge.store.scope_fenced(self.ISSUE))
        self.assertEqual(self.bridge.store.outbox_row(old), old_row)
        self.assertEqual(self.board.history(task_id, 0), native_events)
        self.assertEqual(self.board.get(task_id).status, "ready")
        self.assertEqual(self.inbox_row("current-allowed-discussion")[2], "imported")
        self.assertEqual(len(self.provider.mutations), 1)

    async def test_fenced_discussion_reply_rechecks_original_actor_and_does_not_starve_new_allowed_reply(self):
        self.specialist_bridge(session_requester=True, creator_null=True)
        self.bridge.store.fence_scope(self.ISSUE, "retained earlier authorization denial", at=1)
        self.event("original-reply-actor", "Original authorized discussion")
        transport = self.bridge.api.transport
        unavailable = [True]
        def endpoint(url, body, headers):
            if unavailable[0] and json.loads(body)["query"].lstrip().startswith("mutation"):
                return 503, {}, json.dumps({"errors": [{"message": "finite reply outage"}]}).encode()
            return transport(url, body, headers)
        self.bridge.api.transport = endpoint
        first = await self.adapter.send(self.ISSUE, "Frozen original discussion response",
            reply_to="original-reply-actor", metadata={"thread_id": self.SESSION})
        self.assertFalse(first.success)
        original = self.bridge.store.outbox_row(first.message_id)["payload"]
        self.provider.activities["original-reply-actor"]["user"] = {"id": "revoked"}
        unavailable[0] = False
        self.bridge.clock = lambda: 1900001000.0
        self.assertEqual(await asyncio.to_thread(self.bridge.flush), 0)
        self.assertEqual(self.provider.mutations, [])
        self.event("new-allowed-reply", "Independent current authorized discussion")
        second = await self.adapter.send(self.ISSUE, "New independently authorized response",
            reply_to="new-allowed-reply", metadata={"thread_id": self.SESSION})
        self.assertTrue(second.success, second)
        held = self.bridge.store.outbox_row(first.message_id)
        self.assertNotEqual(held["state"], "sent")
        self.assertEqual(held["payload"]["content"], original["content"])
        self.assertEqual(held["payload"]["requester_activities"], original["requester_activities"])
        self.assertTrue(self.bridge.store.scope_fenced(self.ISSUE))
        self.assertEqual(len(self.provider.mutations), 1)
        self.assertEqual(self.rows(), [])

    async def test_rejected_task_stop_remains_pending_and_old_retry_cannot_stop_new_run(self):
        from hermes_cli import kanban_db as kb
        task_id = self.fixture_task()
        with self.board.conn() as conn:
            conn.execute("CREATE TRIGGER fixture_refuse_stop BEFORE UPDATE OF status ON tasks "
                "WHEN NEW.status='blocked' BEGIN SELECT RAISE(ABORT, 'finite native Stop refusal'); END")
        event = self.event("rejected-task-stop", "stop", signal="stop")
        await self.deliver_raw("rejected-task-stop", event)
        key = f"stop:{self.ISSUE}:rejected-task-stop"
        await self.until(lambda: (self.bridge.store.native_intent(key) or {}).get("state") == "reconcile")
        await self.until(lambda: all(future.done() for future in self.adapter._submitted.values()))
        original = self.bridge.store.native_intent(key)
        self.assertEqual(self.inbox_row("rejected-task-stop")[2], "pending")
        self.assertEqual(self.board.get(task_id).status, "ready")
        with self.board.conn() as conn:
            conn.execute("DROP TRIGGER fixture_refuse_stop")
            newer = kb.claim_task(conn, task_id)
        self.assertIsNotNone(newer)
        event_id = self.board.history(task_id, 0)[-1].id
        await self.deliver_raw("rejected-stop-redelivery", event)
        for _ in range(3):
            await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database, retry_now=True)
            await asyncio.sleep(0.02)
        await self.until(lambda: all(future.done() for future in self.adapter._submitted.values()))
        self.assertEqual(self.board.get(task_id).status, "running")
        self.assertEqual(self.board.get(task_id).current_run_id, newer.current_run_id)
        self.assertEqual(self.board.history(task_id, 0)[-1].id, event_id)
        self.assertEqual(self.bridge.store.native_intent(key)["payload"], original["payload"])
        self.assertEqual(self.inbox_row("rejected-task-stop")[2], "pending")
        self.assertEqual(self.inbox_row("rejected-stop-redelivery")[2], "pending")

    async def test_lost_task_stop_response_reconciles_original_receipt_without_stopping_resumed_run(self):
        from hermes_cli import kanban_db as kb
        from hermes_fleet_linear_plugin.api import LinearError
        task_id = self.fixture_task()
        stop = self.board.stop
        def lose_response(*args, **kwargs):
            self.assertTrue(stop(*args, **kwargs))
            raise LinearError("finite response loss after native commit")
        event = self.event("lost-task-stop", "stop", signal="stop")
        with patch.object(self.board, "stop", side_effect=lose_response):
            await self.deliver_raw("lost-task-stop", event)
            key = f"stop:{self.ISSUE}:lost-task-stop"
            await self.until(lambda: (self.bridge.store.native_intent(key) or {}).get("state") == "reconcile")
            await self.until(lambda: all(future.done() for future in self.adapter._submitted.values()))
        original = self.bridge.store.native_intent(key)["payload"]
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.assertEqual(self.inbox_row("lost-task-stop")[2], "pending")
        with self.board.conn() as conn:
            self.assertTrue(kb.unblock_task(conn, task_id, expected_event_id=self.board.history(task_id, 0)[-1].id))
            newer = kb.claim_task(conn, task_id)
        event_id = self.board.history(task_id, 0)[-1].id
        await self.deliver_raw("lost-stop-redelivery", event)
        for _ in range(3):
            await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database, retry_now=True)
            await asyncio.sleep(0.02)
        await self.until(lambda: self.inbox_row("lost-stop-redelivery")[2] == "imported")
        self.assertEqual(self.board.get(task_id).status, "running")
        self.assertEqual(self.board.get(task_id).current_run_id, newer.current_run_id)
        self.assertEqual(self.board.history(task_id, 0)[-1].id, event_id)
        self.assertEqual(self.bridge.store.native_intent(key)["payload"], original)
        self.assertEqual(self.bridge.store.native_intent(key)["state"], "applied")

    async def test_unavailable_first_task_stop_retains_old_binding_and_cannot_control_replacement(self):
        from hermes_cli import kanban_db as kb
        import uuid
        old_task = self.fixture_task()
        database = Path(kb.kanban_db_path(board=self.board.board))
        unavailable = database.with_name("finite-unavailable.db")
        event = self.event("unavailable-task-stop", "stop", signal="stop")
        database.rename(unavailable)
        try:
            await self.deliver_raw("unavailable-task-stop", event)
            key = f"stop:{self.ISSUE}:unavailable-task-stop"
            await self.until(lambda: (self.bridge.store.native_intent(key) or {}).get("state") == "reconcile")
            await self.until(lambda: all(future.done() for future in self.adapter._submitted.values()))
            original = self.bridge.store.native_intent(key)["payload"]
            self.assertEqual(original["task_id"], old_task)
            self.assertIsNone(original["expected_event_id"])
            self.assertEqual(self.inbox_row("unavailable-task-stop")[2], "pending")
        finally:
            unavailable.rename(database)
        issue = self.provider.issues[self.ISSUE]
        replacement = self.board.create(title="Explicitly inspected replacement",
            body=f"Linear issue {self.ISSUE}: {issue['url']}", created_by=self.board.executor_profile)
        self.bridge.track(self.ISSUE, replacement.id, replace=True,
            expected_revision=self.bridge.store.binding_revision(self.ISSUE), request_id=str(uuid.uuid4()))
        with self.board.conn() as conn:
            newer = kb.claim_task(conn, replacement.id)
        event_id = self.board.history(replacement.id, 0)[-1].id
        await self.deliver_raw("unavailable-stop-redelivery", event)
        for _ in range(3):
            await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database, retry_now=True)
            await asyncio.sleep(0.02)
        await self.until(lambda: all(future.done() for future in self.adapter._submitted.values()))
        self.assertEqual(self.board.get(replacement.id).status, "running")
        self.assertEqual(self.board.get(replacement.id).current_run_id, newer.current_run_id)
        self.assertEqual(self.board.history(replacement.id, 0)[-1].id, event_id)
        self.assertEqual(self.bridge.store.native_intent(key)["payload"], original)
        self.assertEqual(self.inbox_row("unavailable-stop-redelivery")[2], "pending")

    async def test_running_worker_consumes_one_followup_without_another_model_or_native_run(self):
        task_id = self.fixture_task()
        self.enable_owned_worker(task_id)
        await self.deliver("active-opening", "Inspect the existing bounded native remainder.")
        await self.until(lambda: len(self.workers) == 1)
        worker = self.workers[0]
        original_run = worker.claim.current_run_id
        await self.deliver("active-followup", "  Preserve the completed effects; do only the remainder.\n")
        await asyncio.wait_for(self.instruction_consumed.wait(), 10)
        with self.board.conn() as conn:
            comments = self.board.kb.list_comments_after(conn, task_id)
        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0].body, "Preserve the completed effects; do only the remainder.")
        self.assertEqual(self.board.get(task_id).current_run_id, original_run)
        self.assertEqual(len(self.decisions), 1)
        self.assertIs(self.workers[0], worker)
        # A distinct webhook delivery can repeat the same immutable Activity.
        await self.deliver_raw("active-followup-redelivery", self.event("active-followup",
            "  Preserve the completed effects; do only the remainder.\n"))
        await self.until(lambda: self.inbox_row("active-followup-redelivery")[2] == "imported")
        with self.board.conn() as conn:
            self.assertEqual(len(self.board.kb.list_comments_after(conn, task_id)), 1)
        self.assertEqual(len([row for row in self.rows() if row["request_id"] == "active-followup"]), 1)
        worker.release.set()
        worker.wake.set()
        await self.until(lambda: all(row["status"] == "terminal" for row in self.rows()))
        self.assertEqual(self.board.get(task_id).status, "done")

    async def test_typed_stop_controls_conversation_and_task_without_late_retry_cancelling_new_work(self):
        from hermes_cli import kanban_db as kb
        task_id = self.fixture_task()
        self.enable_owned_worker(task_id)
        original = self.event("stop-opening", "Execute the bounded native remainder.")
        await self.deliver_raw("stop-opening", original)
        await self.until(lambda: len(self.workers) == 1)
        first = self.workers[0]
        stop = self.event("typed-stop", "stop", signal="stop")
        await self.deliver_raw("typed-stop", stop)
        await self.until(lambda: self.inbox_row("typed-stop")[2] == "imported")
        await self.until(lambda: self.board.get(task_id).status == "blocked")
        await self.until(lambda: any(event.message_id == "typed-stop" for event, _ in self.receipts))
        await self.until(lambda: next(row for row in self.rows() if row["request_id"] == "stop-opening")["status"] == "terminal")
        self.assertTrue(first.stopped)
        controls = [(event, receipt) for event, receipt in self.receipts if event.message_id == "typed-stop"]
        self.assertTrue(controls)
        self.assertEqual(controls[0][1].control.kind, "stop")
        self.assertTrue(controls[0][1].control.request_consumed)
        self.assertNotIn("typed-stop", [identity for identity, _ in self.decisions])
        await self.deliver("after-stop", "Resume only the reconciled native remainder.")
        await self.until(lambda: len(self.workers) == 2)
        second = self.workers[1]
        self.assertNotEqual(first.claim.current_run_id, second.claim.current_run_id)
        await self.deliver_raw("old-stop-redelivery", stop)
        await self.deliver_raw("old-input-redelivery", original)
        await self.until(lambda: all(self.inbox_row(identity)[2] == "imported"
            for identity in ("old-stop-redelivery", "old-input-redelivery")))
        self.assertFalse(second.stopped)
        self.assertEqual(second.effects, [])
        self.assertEqual(self.board.get(task_id).current_run_id, second.claim.current_run_id)
        self.assertEqual(self.board.get(task_id).status, "running")
        self.assertEqual([identity for identity, _ in self.decisions], ["stop-opening", "after-stop"])
        self.assertEqual(len(self.rows()), 3)
        second.release.set()
        second.wake.set()
        await self.until(lambda: all(row["status"] == "terminal" for row in self.rows()))
        self.assertEqual(self.board.get(task_id).status, "done")

    async def test_generic_notifier_retains_authorization_and_deduplicates_replies(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        from gateway.kanban_watchers_notifier import _KanbanNotification
        from hermes_cli import kanban_db as kb, kanban_db_notify as notify
        from hermes_cli.kanban_db_connect import connect

        self.event("notice-source", "A verified fixture request")
        for scenario in ("valid", "absent", "foreign", "stale"):
            with self.subTest(scenario=scenario):
                database = self.home / (scenario + "-notifications.db")
                conn = connect(database)
                try:
                    task_id = kb.create_task(conn, title="Bounded notice", assignee="default")
                    key = dict(task_id=task_id, platform="linear", chat_id=self.ISSUE,
                               thread_id=self.SESSION)
                    anchor = {} if scenario == "absent" else {"reply_to": "notice-source"}
                    notify.add_notify_sub(conn, **key, delivery_mode="notify", delivery_metadata=anchor)
                    sub = notify.list_notify_subs(conn)[0]
                    old_cursor = sub["last_event_id"]
                    cursor = old_cursor + 1
                    notify.advance_notify_cursor(conn, **key, new_cursor=cursor)
                finally:
                    conn.close()

                def operation(_board, name, _sub, **kwargs):
                    connection = connect(database)
                    try:
                        getattr(notify, name)(connection, **key, **kwargs)
                    finally:
                        connection.close()

                runner = SimpleNamespace(
                    _kanban_sub_op=operation,
                    _kanban_rewind=lambda sub, claimed, old, board: operation(
                        board, "rewind_notify_cursor", sub, claimed_cursor=claimed, old_cursor=old),
                    _kanban_unsub=lambda sub, board: operation(board, "remove_notify_sub", sub),
                    _deliver_kanban_artifacts=AsyncMock(),
                )
                event = SimpleNamespace(id=cursor, kind="completed", payload={"summary": "Bounded outcome"})
                notice = _KanbanNotification(runner, dict(sub=sub, task=None, events=[event],
                    cursor=cursor, old_cursor=old_cursor, board="default"),
                    platform_cls=None, sub_fail_counts={})
                notice.adapter = self.adapter
                if scenario == "foreign":
                    self.provider.activities["notice-source"]["agentSession"] = {"id": "other-session"}
                elif scenario == "stale":
                    self.provider.activities.pop("notice-source")
                before = len(self.provider.mutations)
                delivered = await notice._send_pings()
                self.assertEqual(delivered, scenario == "valid")
                # Reconstruct the notice as the next poll would. A real outbox
                # must suppress a replay even with a stale local ping snapshot.
                repeat = _KanbanNotification(runner, notice.d, platform_cls=None, sub_fail_counts={})
                repeat.adapter = self.adapter
                self.assertEqual(await repeat._send_pings(), scenario == "valid")
                self.assertEqual(len(self.provider.mutations) - before, int(scenario == "valid"))
                connection = connect(database)
                try:
                    retained = notify.list_notify_subs(connection)
                finally:
                    connection.close()
                self.assertEqual(len(retained), 1)
                self.assertEqual(retained[0]["delivery_mode"], "notify")
                self.assertEqual(retained[0]["last_event_id"], cursor if delivered else old_cursor)
                self.assertEqual(retained[0]["last_ping_event_id"], cursor if delivered else 0)
                self.assertEqual(self.rows(), [])  # Reporting did not admit engineering work.

    async def test_adapter_reply_outage_keeps_same_outbox_identity_until_actual_sent(self):
        self.event("reply-origin", "A verified fixture request")
        original = self.bridge.api.transport
        unavailable = [True]
        def transport(url, body, headers):
            if unavailable[0] and json.loads(body)["query"].lstrip().startswith("mutation"):
                return 503, {}, json.dumps({"errors": [{"message": "finite reply endpoint unavailable"}]}).encode()
            return original(url, body, headers)
        self.bridge.api.transport = transport
        result = await self.adapter.send(self.ISSUE, "Verified finite response", reply_to="reply-origin",
                                         metadata={"thread_id": self.SESSION})
        self.assertFalse(result.success)
        rows = self.bridge.store.pending(self.ISSUE)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["id"], result.message_id)
        self.assertNotEqual(row["state"], "sent")
        self.assertEqual(self.provider.mutations, [])
        unavailable[0] = False
        self.bridge.clock = lambda: 1900001000.0
        await asyncio.to_thread(self.bridge.flush)
        self.assertEqual(self.bridge.store.outbox_row(row["id"])["state"], "sent")
        retry = await self.adapter.send(self.ISSUE, "Verified finite response", reply_to="reply-origin",
                                        metadata={"thread_id": self.SESSION})
        self.assertTrue(retry.success)
        self.assertEqual(retry.message_id, row["id"])
        self.assertEqual(len(self.provider.mutations), 1)
        self.assertEqual(self.rows(), [])

    async def test_adapter_reply_recovery_rechecks_original_session_scope(self):
        self.event("reply-origin", "A verified fixture request")
        from hermes_fleet_linear_plugin.native_input import queue_reply
        row_id = queue_reply(self.bridge, self.ISSUE, self.SESSION, "reply-origin", "Held original reply")
        before = self.bridge.store.outbox_row(row_id)
        self.provider.sessions[self.SESSION]["appUser"] = {"id": "foreign-app"}
        await asyncio.to_thread(self.bridge.flush)
        after = self.bridge.store.outbox_row(row_id)
        self.assertEqual({key: after["payload"][key] for key in before["payload"]}, before["payload"])
        self.assertNotEqual(after["state"], "sent")
        self.assertEqual(self.provider.mutations, [])
        result = await self.adapter.send(self.ISSUE, "Held original reply", reply_to="reply-origin",
                                         metadata={"thread_id": self.SESSION})
        self.assertFalse(result.success)
        self.assertEqual(self.rows(), [])

    async def test_missing_handler_retains_input_until_receiver_is_actually_bound(self):
        self.adapter.set_message_handler(None)
        raw = await self.deliver("startup-input", "Do not silently consume startup work")
        self.assertEqual(self.inbox_row("startup-input")[:3], ("default", raw, "pending"))
        self.assertEqual(self.rows(), [])
        self.adapter.set_message_handler(self.runner._handle_message)
        await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database, retry_now=True)
        await self.until(lambda: self.inbox_row("startup-input")[2] == "imported")
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.rows()[0]["request_id"], "startup-input")

    async def test_actorless_assignment_scope_fence_retains_original_delivery(self):
        self.provider.sessions[self.SESSION]["creator"] = None
        self.bridge.store.fence_scope(self.ISSUE, "Retained original authorization fence", at=1)
        event = self.event("actorless-fenced", "", action="created")
        event["agentSession"].pop("creatorId")
        raw = await self.deliver_raw("actorless-fenced", event)
        await self.until(lambda: all(f.done() for f in self.adapter._submitted.values()))
        for _ in range(3):
            await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database, retry_now=True)
            await asyncio.sleep(.02)
        self.assertEqual(self.inbox_row("actorless-fenced")[1:3], (raw, "pending"))
        self.assertTrue(self.bridge.store.scope_fenced(self.ISSUE))
        self.assertIsNone(self.bridge.store.get(self.ISSUE))
        self.assertEqual(self.board.discover(self.provider.issues[self.ISSUE]), [])
        self.assertEqual(self.rows(), [])

    async def test_actorless_unknown_creation_retry_cannot_ack_or_repeat_create(self):
        self.provider.sessions[self.SESSION]["creator"] = None
        with self.board.conn() as conn:
            conn.execute("CREATE TRIGGER fixture_refuse_create BEFORE INSERT ON tasks "
                         "BEGIN SELECT RAISE(ABORT, 'finite native create refusal'); END")
        event = self.event("actorless-unknown", "", action="created")
        event["agentSession"].pop("creatorId")
        raw = await self.deliver_raw("actorless-unknown", event)
        await self.until(lambda: all(f.done() for f in self.adapter._submitted.values()))
        self.assertEqual(self.inbox_row("actorless-unknown")[2], "pending")
        with self.board.conn() as conn:
            conn.execute("DROP TRIGGER fixture_refuse_create")
        original = self.bridge.store.native_intent("create:" + self.ISSUE)["payload"]
        for _ in range(5):
            await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database, retry_now=True)
            await asyncio.sleep(.02)
        intent = self.bridge.store.native_intent("create:" + self.ISSUE)
        self.assertEqual(intent["state"], "reconcile")
        self.assertEqual(intent["payload"], original)
        self.assertEqual(self.inbox_row("actorless-unknown")[1:3], (raw, "pending"))
        self.assertIsNone(self.bridge.store.get(self.ISSUE))
        self.assertEqual(self.board.discover(self.provider.issues[self.ISSUE]), [])
        self.assertEqual(self.rows(), [])

    async def test_actorless_committed_create_response_loss_reconciles_before_ack(self):
        self.provider.sessions[self.SESSION]["creator"] = None
        event = self.event("actorless-lost-response", "", action="created")
        event["agentSession"].pop("creatorId")
        create = self.board.create
        created = []
        def lost_response(**kwargs):
            created.append(create(**kwargs))
            raise ConnectionError("finite create response loss")
        with patch.object(self.board, "create", side_effect=lost_response) as request:
            raw = await self.deliver_raw("actorless-lost-response", event)
            await self.until(lambda: all(f.done() for f in self.adapter._submitted.values()))
            self.assertEqual(self.inbox_row("actorless-lost-response")[2], "pending")
            self.assertEqual(request.call_count, 1)
        self.assertIsNone(self.bridge.store.get(self.ISSUE))
        self.assertEqual(self.board.get(created[0].id).status, "blocked")
        with patch.object(self.board, "create", side_effect=AssertionError("must reconcile original create")):
            for _ in range(5):
                await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database, retry_now=True)
                await asyncio.sleep(.02)
            await self.until(lambda: self.inbox_row("actorless-lost-response")[2] == "imported")
        self.assertEqual(self.bridge.store.get(self.ISSUE)["task_id"], created[0].id)
        self.assertEqual(self.bridge.store.native_intent("create:" + self.ISSUE)["state"], "applied")
        self.assertEqual(len(self.board.discover(self.provider.issues[self.ISSUE])), 1)
        self.assertEqual(self.inbox_row("actorless-lost-response")[1], raw)
        self.assertEqual(self.rows(), [])

    async def test_known_pre_cutoff_assignment_is_consumed_without_revival(self):
        self.provider.sessions[self.SESSION]["creator"] = None
        event = self.event("actorless-stale", "", action="created")
        event["agentSession"].pop("creatorId")
        self.bridge.activation_cutoff_ms = 2000000000000
        await self.deliver_raw("actorless-stale", event)
        await self.until(lambda: self.inbox_row("actorless-stale")[2] == "imported")
        self.assertIsNone(self.bridge.store.get(self.ISSUE))
        self.assertIsNone(self.bridge.store.native_intent("create:" + self.ISSUE))
        self.assertEqual(self.rows(), [])

    async def test_ignored_nonwork_webhook_still_acknowledges_without_native_work(self):
        await self.deliver_raw("nonwork-update", {"type": "Issue", "action": "update", "data": {"id": self.ISSUE}})
        await self.until(lambda: self.inbox_row("nonwork-update")[2] == "imported")
        self.assertIsNone(self.bridge.store.get(self.ISSUE))
        self.assertEqual(self.rows(), [])

    async def test_actorless_assignment_uses_one_native_coordination_task_and_unknown_requester(self):
        from hermes_cli import kanban_db as kb
        from hermes_fleet_linear_plugin.native_input import ACTORLESS_COORDINATION
        self.provider.sessions[self.SESSION]["creator"] = None
        event = self.event("unused", "", action="created")
        event["agentSession"].pop("creatorId")
        event.pop("agentActivity")
        raw = await self.deliver_raw("actorless-delivery", event)
        await self.until(lambda: self.inbox_row("actorless-delivery")[2] == "imported")
        binding = self.bridge.store.native_binding(self.ISSUE)
        self.assertIsNotNone(binding)
        task = self.board.get(binding["task_id"])
        self.assertEqual(task.status, "ready")
        self.assertEqual(task.created_by, "linear")
        self.assertIn(ACTORLESS_COORDINATION, task.body)
        self.assertEqual(self.rows(), [])  # No invented human conversation/principal.
        self.assertEqual(self.receipts, [])
        with self.board.conn() as conn:
            claim = kb.claim_task(conn, task.id)
            context = kb.build_worker_context(conn, task.id)
        # Explicit finite task-model endpoint, fed the actual native worker
        # context and owned run; no spawned worker or local-owner principal.
        def task_model(worker_context, owned_run):
            self.assertIn(ACTORLESS_COORDINATION, worker_context)
            self.assertEqual(owned_run.current_run_id, self.board.get(task.id).current_run_id)
            self.decisions.append((task.id, "coordinate-native-remainder", None))
            artifact = self.home / "verified-remainder.txt"
            artifact.write_text("One bounded remainder, requester unknown.\n")
            return "Verified one bounded remainder; artifact: " + str(artifact)
        outcome = task_model(context, claim)
        with self.board.conn() as conn:
            self.assertTrue(kb.complete_task(conn, task.id,
                expected_run_id=claim.current_run_id,
                result=outcome))
        await self.deliver_raw("actorless-redelivery", event)
        await self.until(lambda: self.inbox_row("actorless-redelivery")[2] == "imported")
        self.assertEqual(self.board.get(task.id).status, "done")
        self.assertEqual(len(self.board.discover(self.provider.issues[self.ISSUE])), 1)
        self.assertEqual(len(self.decisions), 1)
        self.assertEqual(self.inbox_row("actorless-delivery")[1], raw)
        self.assertEqual(self.rows(), [])

    async def test_creator_null_later_activity_reaches_model_track_and_scoped_publication(self):
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        from hermes_fleet_linear_plugin import chat, transport
        from hermes_fleet_linear_plugin.bridge import Bridge
        from test_linear_specialist_authorization import SCOPE
        import uuid
        self.provider.sessions[self.SESSION]["creator"] = None
        self.bridge = Bridge(self.bridge.store, self.provider.api(scope={**SCOPE, "requester_basis": "agent_session"}),
            self.board, profile="default", clock=lambda: 1900000000.0)
        self.bridge.profile_home = self.home
        self.running["bridge"] = self.bridge
        self.bridge.native_ingress = lambda delivery: self.native_input.route_delivery(self.running, delivery)
        request_id = str(uuid.uuid4())
        async def decision(event):
            context = transport.capture_context(track=True)
            self.assertEqual(context.message_id, "later-allowed-activity")
            status = json.loads(chat.handle(self.bridge, {"action": "status", "issue": self.ISSUE}, context))
            self.assertTrue(status["ok"], status)
            issue = self.provider.issues[self.ISSUE]
            task = self.board.create(title="Existing authorized native remainder",
                body=f"Linear issue {self.ISSUE}: {issue['url']}", created_by=self.board.executor_profile,
                initial_status="blocked")
            tracked = json.loads(chat.handle(self.bridge, {"action": "track", "issue": self.ISSUE,
                "task_id": task.id}, context))
            self.assertTrue(tracked["ok"], tracked)
            update = json.loads(chat.handle(self.bridge, {"action": "add_comment", "issue": self.ISSUE,
                "id": request_id, "body": "Verified later Activity, original creator remains unknown."}, context))
            self.assertTrue(update["ok"], update)
            execution_result.get()["result"] = {"completed": True, "final_response": "Native reporting linked."}
            return "Native reporting linked."
        self.model = decision
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        event = self.event("later-allowed-activity", "Inspect and link the authorized remainder.")
        event["agentSession"].pop("creatorId")
        raw = await self.deliver_raw("later-allowed-activity", event)
        await self.until(lambda: self.rows() and self.rows()[0]["status"] == "terminal")
        await asyncio.to_thread(self.bridge.flush)
        binding = self.bridge.store.native_binding(self.ISSUE)
        self.assertEqual((binding["requester_session_id"], binding["requester_activity_id"]),
                         (self.SESSION, "later-allowed-activity"))
        self.assertEqual(self.bridge.store.outbox_row(request_id)["state"], "sent")
        self.assertEqual(self.inbox_row("later-allowed-activity"), ("default", raw, "imported", 1))
        self.assertIsNone(self.provider.sessions[self.SESSION]["creator"])
        # Outside the model's requester context, existing native reporting
        # recovers only the paired coordinates and verifies the Activity again.
        self.assertTrue(self.bridge.authorize_specialist_effect(self.ISSUE))
        self.assertFalse(self.bridge.store.scope_fenced(self.ISSUE))
        self.assertEqual(self.board.get(binding["task_id"]).status, "blocked")

    async def test_human_mention_is_acknowledged_first_and_reaches_the_model_as_a_question_with_work_facts(self):
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        task_id = self.fixture_task()
        question = "@agent is the rollout finished, or is anything still waiting?"
        seen = []
        async def answer(event):
            # The acknowledgment is already visible in Linear before any model turn.
            seen.append((event.text, event.channel_context, [query for query in self.provider.mutations
                                                              if "agentActivityCreate" in query]))
            execution_result.get()["result"] = {"completed": True, "final_response": "Still running."}
            return "Still running."
        self.model = answer
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        event = self.event("unused-mention-activity", "", action="created")
        event.pop("agentActivity")
        event["agentSession"]["comment"] = {"id": "mention-comment", "body": question}
        raw = await self.deliver_raw("human-mention", event)
        await self.until(lambda: self.rows() and self.rows()[0]["status"] == "terminal")
        self.assertEqual(len(seen), 1)
        text, context, acknowledged = seen[0]
        self.assertEqual(text, question)
        self.assertIn("@mention", context)
        self.assertIn(f"task {task_id}", context)
        self.assertIn("`linear followup`", context)
        self.assertEqual(len(acknowledged), 1)
        ack = self.bridge.store.outbox_row(self.native_input.stable_uuid4(
            "native-session-ack", "default", self.ISSUE, self.SESSION))
        self.assertEqual((ack["state"], ack["payload"]["content"]["type"]), ("sent", "thought"))
        self.assertEqual(self.inbox_row("human-mention"), ("default", raw, "imported", 1))
        activities = len([query for query in self.provider.mutations if "agentActivityCreate" in query])
        # A redelivered mention neither posts a second acknowledgment nor starts a second model turn.
        await self.deliver_raw("human-mention-redelivery", event)
        await self.until(lambda: self.inbox_row("human-mention-redelivery")[2] == "imported")
        self.assertEqual(len([query for query in self.provider.mutations if "agentActivityCreate" in query]), activities)
        self.assertEqual(len(seen), 1)
        with self.board.conn() as conn:
            self.assertEqual([row[0] for row in conn.execute("SELECT id FROM tasks")], [task_id])

    async def test_distinct_provider_inputs_commit_own_ids_bodies_before_own_inbox_ack(self):
        a = await self.deliver("activity-a", "  First unchanged instruction.\n")
        b = await self.deliver("activity-b", "Second unchanged instruction\nwith detail.")
        await self.until(lambda: {event.message_id for event, _ in self.receipts} >= {"activity-a", "activity-b"})
        expected = [("activity-a", "  First unchanged instruction.\n", a),
                    ("activity-b", "Second unchanged instruction\nwith detail.", b)]
        for identity, text, raw in expected:
            row = next(row for row in self.rows() if row["request_id"] == identity)
            self.assertEqual(row["payload"]["text"], text)
            self.assertEqual(self.inbox_row(identity), ("default", raw, "imported", 1))
            event, receipt = next(pair for pair in self.receipts if pair[0].message_id == identity)
            self.assertEqual(event.source.platform.value, "linear")
            self.assertEqual((event.source.chat_id, event.source.thread_id, event.source.user_id,
                              event.source.scope_id), (self.ISSUE, self.SESSION, self.USER, self.provider.organization))
            self.assertEqual(receipt.admission_id, row["admission_id"])
        self.assertEqual(self.board.discover(self.provider.issues[self.ISSUE]), [])
        self.assertEqual(self.decisions, [])

    async def test_native_commit_with_failed_inbox_update_retries_receipt_without_duplicate_input(self):
        with sqlite3.connect(self.inbox.database) as db:
            db.execute("CREATE TRIGGER fixture_refuse_ack BEFORE UPDATE OF status ON deliveries "
                "WHEN NEW.status='imported' BEGIN SELECT RAISE(ABORT, 'finite inbox write refusal'); END")
        raw = await self.deliver("ack-outage", "Original committed input, original provider ID.")
        await self.until(lambda: len(self.rows()) == 1)
        await self.until(lambda: all(future.done() for future in self.adapter._submitted.values()))
        self.assertEqual(self.inbox_row("ack-outage")[:3], ("default", raw, "pending"))
        self.assertEqual(self.rows()[0]["request_id"], "ack-outage")
        self.assertEqual(self.rows()[0]["payload"]["text"], "Original committed input, original provider ID.")
        with sqlite3.connect(self.inbox.database) as db:
            db.execute("DROP TRIGGER fixture_refuse_ack")
        # Each retry follows the existing inbox loop; the first can report the
        # prior receiver error, and the next reconciles the committed input.
        for _ in range(3):
            await asyncio.to_thread(self.bridge.drain_ingress, self.inbox.database, retry_now=True)
            await asyncio.sleep(0.02)
        await self.until(lambda: self.inbox_row("ack-outage")[2] == "imported")
        self.assertEqual(self.inbox_row("ack-outage")[:3], ("default", raw, "imported"))
        self.assertGreaterEqual(self.inbox_row("ack-outage")[3], 1)
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.decisions, [])

    async def test_inactive_input_reaches_finite_model_and_actual_guarded_native_task_control(self):
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        from hermes_cli import kanban_db as kb
        async def decision(event):
            self.decisions.append((event.message_id, event.text))
            issue = self.provider.issues[self.ISSUE]
            self.assertEqual(self.board.discover(issue), [])
            from hermes_fleet_linear_plugin import chat, transport
            context = transport.capture_context()
            self.assertEqual((context.platform, context.thread_id, context.user_id),
                             ("linear", self.SESSION, self.USER))
            status = json.loads(chat.handle(self.bridge, {"action": "status", "issue": self.ISSUE}, context))
            self.assertTrue(status["ok"], status)
            self.assertFalse(status["native"]["linked"])
            task = self.board.create(title="Bounded native remainder", body=f"Linear issue {self.ISSUE}: {issue['url']}",
                created_by=self.board.executor_profile, initial_status="blocked")
            events = self.board.history(task.id, 0)
            with self.board.conn() as conn:
                self.assertTrue(kb.unblock_task(conn, task.id, expected_event_id=events[-1].id))
            self.bridge.track(self.ISSUE, task.id)
            execution_result.get()["result"] = {"final_response": "Native remainder is ready.", "completed": True}
            return "Native remainder is ready."
        self.model = decision
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        raw = await self.deliver("inactive-activity", "Continue the remaining authorized work.")
        await self.until(lambda: self.bridge.store.native_binding(self.ISSUE) is not None)
        await self.until(lambda: self.rows() and self.rows()[0]["status"] == "terminal")
        self.assertEqual(self.decisions, [("inactive-activity", "Continue the remaining authorized work.")])
        binding = self.bridge.store.native_binding(self.ISSUE)
        self.assertEqual(self.board.get(binding["task_id"]).status, "ready")
        self.assertEqual(self.inbox_row("inactive-activity"), ("default", raw, "imported", 1))
        self.assertEqual(len(self.rows()), 1)
        await self.until(lambda: len(self.provider.mutations) == 1)
        self.assertFalse(self.bridge.store.pending(self.ISSUE))
