"""Mounted origin evidence against the pinned native owner/receipts, without a model or live account."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
import inspect
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from linear_fake_api import load_plugin

SOURCE = os.environ.get("HERMES_AGENT_SRC")
GUARDED_ROOT = os.environ.get("HERMES_LINEAR_ORIGIN_TEST_ROOT")
if SOURCE and GUARDED_ROOT:
    sys.path.insert(0, SOURCE)
plugin = load_plugin()
from hermes_fleet_linear_plugin import origin, transport, work


@unittest.skipUnless(SOURCE and GUARDED_ROOT, "requires the guarded manifest-pinned origin proof entrypoint")
class OriginScenarios(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        guard_root = os.environ.get("HERMES_LINEAR_ORIGIN_TEST_ROOT")
        if not guard_root:
            raise RuntimeError("Use scripts/run-linear-origin-scenarios.py; native imports require the audited isolation guard")
        self.temp = tempfile.TemporaryDirectory(dir=guard_root)
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        isolated = {"PATH": os.defpath, "HOME": str(self.home), "USERPROFILE": str(self.home),
                    "HERMES_HOME": str(self.home), "HERMES_PROFILE": "default",
                    "XDG_CONFIG_HOME": str(self.home / "xdg-config"), "XDG_CACHE_HOME": str(self.home / "xdg-cache"),
                    "CLAUDE_CONFIG_DIR": str(self.home / ".claude"), "GH_CONFIG_DIR": str(self.home / ".config/gh"),
                    "HERMES_AGENT_SRC": SOURCE, "HERMES_LINEAR_ORIGIN_TEST_ROOT": guard_root}
        self.env = patch.dict(os.environ, isolated, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        (self.home / "config.yaml").write_text("model:\n  default: fixture\nplatform_toolsets:\n  cli: []\n  desktop: []\n")
        from gateway.config import GatewayConfig
        from gateway.session import SessionStore
        from gateway.session_authority import initialize_session_authority
        from gateway.session_controls import AuthorityConnection
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        from gateway.session_context import reset_session_vars
        self.assertTrue(Path(inspect.getfile(AuthorityConnection)).resolve().is_relative_to(Path(SOURCE).resolve()))
        token = set_hermes_home_override(self.home)
        self.addCleanup(reset_hermes_home_override, token)
        reset_session_vars()
        self.addCleanup(reset_session_vars)
        self.settings = {"identity": {"organization_id": "workspace", "teams": ["team"]},
                         "specialist_scope": {"allowed_team_ids": ["team"], "allowed_project_ids": ["project"],
                                              "allowed_requester_ids": ["requester"]}}
        self.config = {"model": {"default": "fixture"}, "platform_toolsets": {"cli": [], "desktop": []}}
        self.store = SessionStore(self.home / "sessions", GatewayConfig())
        self.addCleanup(self.store._db.close)
        self.runner = SimpleNamespace(session_store=self.store, _session_db=self.store._db, adapters={},
                                      _draining=False, config=GatewayConfig(), _cached_agent_for=lambda key: None,
                                      _peek_session_state=lambda key: None, _primary_profile_name="default")
        self.runner._adapter_for_source = lambda source: self.runner.adapters.get(source.platform)
        self.runner._adapter_flag = lambda *args: False
        self.runner._adapter_profile_for_source = lambda source: None
        self.runner._is_user_authorized_for_source = lambda *args, **kwargs: True
        self.authority = await initialize_session_authority(self.runner, profile_id=str(self.home), instance_id="fixture")
        self.authority._schedule = lambda ref: None
        self.conn = AuthorityConnection(self.authority, SimpleNamespace(write=lambda frame: None), {
            "provider": "local", "user_id": "uid:1000", "profile_id": str(self.home), "instance_id": "fixture",
            "native_bootstrap": True,
            "capabilities": ["session:create", "session:read", "session:submit", "session:control"]}, operator=True)
        self.addAsyncCleanup(self.conn.close)
        self.loop = asyncio.get_running_loop()
        self.calls = []
        self.before_rpc = None

        async def rpc(method, **params):
            async def on_owner():
                self.assertEqual(method, "prompt.receipt", "origin inspection must never attach or compute runtime inventory")
                self.calls.append((method, params))
                if self.before_rpc:
                    self.before_rpc(method, params)
                result = await self.conn.dispatch({"id": len(self.calls), "method": method, "params": params})
                if "error" in result:
                    raise RuntimeError(result["error"]["message"])
                return result["result"]
            if asyncio.get_running_loop() is self.loop:
                return await on_owner()
            return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(on_owner(), self.loop))

        @asynccontextmanager
        async def client(home):
            self.assertEqual(home, self.home)
            yield SimpleNamespace(rpc=rpc), SimpleNamespace(instance_id=self.authority.instance_id,
                                                            authority_epoch=self.authority.epoch)
        patcher = patch.object(origin, "_client", client)
        patcher.start()
        self.addCleanup(patcher.stop)

    async def local(self, text="Take over ABC-1", *, source="gui", extra=None, work_context=None):
        from gateway.config import Platform
        from gateway.session import SessionEntry, SessionSource
        from gateway.session_contract import SessionRef
        from gateway.session_lifecycle import _now
        from gateway.session_policy import LocalSessionPolicy, SURFACES
        from hermes_state_local import commit_local_session
        from hermes_state_keys import local_identity
        from hermes_state_runtime import admit_session_input
        # Build synthetic fixture policy through the native immutable type/atomic writer.
        # The launch planner also initializes profile credentials; no launch occurs in this proof.
        request_id = ("kanban:" + json.dumps([work_context[k] for k in ("board", "task_id", "run_id")], separators=(",", ":"))
                      if work_context else "origin-local-" + str(len(self.authority.sessions)))
        self.sid = local_identity(str(self.home), self.conn.actor.subject, request_id)
        request = {"source": source, "model": "fixture", "cwd": str(self.home), "toolsets": []}
        if work_context:
            request.update({k: work_context[k] for k in ("board", "task_id", "run_id")})
        policy = LocalSessionPolicy(source=source, platform=SURFACES.get(source, "cli"), cwd=str(self.home), model="fixture",
            toolsets=(), config_json=json.dumps(self.config), request_json=json.dumps(request),
            terminal_json=json.dumps({"TERMINAL_ENV": "local", "TERMINAL_CWD": str(self.home)}),
            safe_mode=True, ignore_user_config=True, kanban_json=json.dumps(work_context) if work_context else None)
        native_source = SessionSource(platform=Platform.LOCAL, chat_id=self.sid,
                                      user_id=self.conn.actor.subject, chat_type="dm")
        route = self.store._generate_session_key(native_source)
        entry = SessionEntry(route, self.sid, _now(), _now(), origin=native_source, platform=Platform.LOCAL)
        commit_local_session(self.authority.db, epoch=self.authority.epoch, receipt={
            "profile_id": str(self.home), "principal_id": self.conn.actor.subject, "request_id": request_id,
            "session_id": self.sid, "route": route, "entry": entry.to_dict(), "policy": asdict(policy)})
        self.ref = self.authority._publish_route(native_source, self.sid, route)
        payload = {"text": text}
        if extra:
            from gateway.session_cli_turn import admit_cli_turn
            payload.update(admit_cli_turn(extra))
        if work_context:
            payload["kanban_card_v1"] = {k: work_context[k] for k in ("board", "task_id", "run_id")}
        row = admit_session_input(self.authority.db, epoch=self.authority.epoch,
            principal_id=self.conn.actor.subject, session_id=self.sid,
            request_id=request_id if work_context else "request-1", payload=payload)
        self.admission = row["admission_id"]
        self.claim()
        self.bind("local", self.sid)
        if work_context:
            self.bind("local", self.sid, source="kanban")

    def claim(self):
        from hermes_state_runtime import claim_session_input
        self.row = claim_session_input(self.authority.db, epoch=self.authority.epoch, session_id=self.sid)
        self.authority.executing[self.sid] = self.row["generation"]

    def bind(self, platform, sid, *, source=None, key=None, user="uid:1000", chat="", thread="", scope=""):
        from gateway.session_context import set_session_vars
        set_session_vars(platform=platform, source=source or platform, session_key=key or self.authority.sessions[self.sid].route,
                         session_id=sid, user_id=user, user_id_alt="", chat_id=chat, chat_type="dm", thread_id=thread,
                         scope_id=scope, profile="default", ui_session_id="tab")

    async def messaging(self, platform):
        from gateway.config import Platform
        from gateway.session import SessionSource
        from gateway.platforms.event import MessageEvent
        from gateway.session_contract import SessionRef
        source = SessionSource(platform=Platform(platform), chat_id="chat", user_id="human", chat_type="dm",
                               thread_id="thread", scope_id="scope")
        self.runner.adapters[source.platform] = SimpleNamespace()
        event = MessageEvent(text="Take over ABC-1", message_id="message-1", source=source)
        receipt = await self.authority.admit_native(event)
        self.sid, self.admission, self.ref = receipt.ref.session_id, receipt.admission_id, receipt.ref
        self.claim()
        self.bind(platform, self.sid, user="human", chat="chat", thread="thread", scope="scope")

    async def api(self):
        from gateway.session_api import bind_api_session
        from gateway.session_contract import SessionRef
        from hermes_state_runtime import admit_session_input
        self.sid = "api-session"
        self.ref = bind_api_session(self.authority, self.sid)
        payload = {"text": "Take over ABC-1", "api_turn_v1": {"history": None, "settings": {},
            "transfer_auth": {"principal": "principal:fixture:" + "a" * 32, "transport_family": "local-api"}}}
        row = admit_session_input(self.authority.db, epoch=self.authority.epoch, principal_id="api",
                                  session_id=self.sid, request_id="api-request", payload=payload)
        self.admission = row["admission_id"]
        self.claim()
        self.bind("api_server", self.sid, user="api_server")

    async def current(self):
        context = transport.capture_context()
        return await asyncio.to_thread(origin.current, context, self.settings)

    async def inspected(self, ref):
        return await asyncio.to_thread(origin.inspect, ref, self.settings)

    async def kanban(self, *, existing=False):
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_connect import _sqlite_connect
        if not existing:
            from agent import runtime_session_store
            # Synthetic successful-adoption process marker: no owner bootstrap or model.
            worker = patch.object(runtime_session_store, "_worker_process", True)
            worker.start()
            self.addCleanup(worker.stop)
            self.board_path = self.home / "work.db"
            kb.init_db(self.board_path)
            self.board = _sqlite_connect(self.board_path)
            self.board.row_factory = __import__("sqlite3").Row
            self.addCleanup(self.board.close)
            self.task_id = kb.create_task(self.board, title="Take over ABC-1", body="Finish the remaining checks",
                assignee="default", created_by="fixture-agent", tenant="tenant", workspace_kind="dir",
                workspace_path=str(self.home), skills=[])
        task = kb.claim_task(self.board, self.task_id, claimer="fixture-host:CLAIM_PRIVATE_SENTINEL")
        self.assertIsNotNone(task)
        self.work_context = {"board": "default", "db": str(self.board_path), "task_id": task.id,
            "run_id": task.current_run_id, "claim_lock": task.claim_lock, "profile": "default",
            "workspace": task.workspace_path, "branch": task.branch_name, "tenant": task.tenant,
            "workspaces_root": str(self.home), "skills": [], "goal_mode": task.goal_mode,
            "goal_max_turns": task.goal_max_turns, "goal_text": task.title + "\n\n" + task.body,
            "context": "UNRELATED_FROZEN_CONTEXT", "max_runtime_seconds": task.max_runtime_seconds, "accept_hooks": True}
        await self.local("work kanban task " + task.id, source="kanban", work_context=self.work_context)
        # Actual managed Kanban workers bind the AIAgent/compression session ID but no chat
        # platform/source/key. Exercise precisely that supported binder, with no env identity.
        from gateway.session_context import reset_session_vars, set_current_session_id
        reset_session_vars()
        set_current_session_id(self.sid)
        with kb.write_txn(self.board):
            kb._append_event(self.board, task.id, "owner_admitted", {"db": str(self.home / "state.db"),
                "session_id": self.sid, "request_id": self.row["request_id"], "pid": os.getpid(), "birth": 123},
                run_id=task.current_run_id)
            kb._append_event(self.board, task.id, "worker_bound", {"pid": os.getpid(), "claim_lock": task.claim_lock},
                run_id=task.current_run_id)

    async def work_current(self):
        return await asyncio.to_thread(work.current, work.capture_context(), self.settings)

    async def work_inspected(self, ref):
        return await asyncio.to_thread(work.inspect, ref, self.settings)

    async def test_work_current_preserves_unknown_requester_and_selective_privacy(self):
        from hermes_cli import kanban_db as kb
        await self.kanban()
        self.authority.db.append_message(self.sid, "user", "UNRELATED_PRIVATE_HISTORY")
        kb.create_task(self.board, title="UNRELATED_PRIVATE_CARD", assignee="default")
        before = [tuple(r) for r in self.board.execute("SELECT * FROM tasks")]
        admission_before = self.authority.db._read_all("SELECT * FROM session_admissions")
        result = await self.work_current()
        evidence = await self.work_inspected(result["reference"])
        self.assertEqual(evidence["requester"], {"kind": "native_dispatcher", "original_requester": "unknown"})
        self.assertEqual(evidence["work"]["instruction"], "Take over ABC-1\n\nFinish the remaining checks")
        self.assertEqual(evidence["scope"]["declared"], origin.scope(self.settings))
        self.assertEqual(evidence["scope"]["workspace"], str(self.home))
        self.assertFalse(evidence["permission_granted"])
        for private in ("CLAIM_PRIVATE_SENTINEL", "UNRELATED_FROZEN_CONTEXT", "UNRELATED_PRIVATE_HISTORY",
                        "UNRELATED_PRIVATE_CARD", str(self.board_path), self.conn.actor.subject):
            self.assertNotIn(private, json.dumps(evidence))
        self.assertEqual([tuple(r) for r in self.board.execute("SELECT * FROM tasks")], before)
        self.assertEqual(self.authority.db._read_all("SELECT * FROM session_admissions"), admission_before)
        with self.assertRaises(transport.Unavailable):
            await self.current()  # Interactive origin rules remain unchanged for producers.

    async def test_work_registered_boundary_and_cli_are_separate_and_selector_free(self):
        from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
        from tools.registry import registry
        from hermes_fleet_linear_plugin import ops
        import argparse
        await self.kanban()
        self.config["plugins"] = {"entries": {"linear": {"settings": {"enabled": True, **self.settings}}}}
        (self.home / "config.yaml").write_text(json.dumps(self.config))
        manager = PluginManager(scope_key=str(self.home))
        self.addCleanup(manager.unload)
        plugin.register(PluginContext(PluginManifest(name="linear"), manager))
        handler = registry.get_entry("linear", scope=str(self.home)).handler
        result = json.loads(await asyncio.to_thread(handler, {"action": "work_reference"}))
        self.assertTrue(result["ok"], result)
        for selector in ("session_id", "task_id", "home", "profile", "body", "issue"):
            denied = json.loads(await asyncio.to_thread(handler, {"action": "work_reference", selector: "forged"}))
            self.assertFalse(denied["ok"])
        parser = argparse.ArgumentParser()
        ops.setup(parser)
        args = parser.parse_args(["inspect-work", "--reference-json", json.dumps(result["reference"])])
        with patch("builtins.print") as printed:
            code = await asyncio.to_thread(ops.run, args, self.home / "unused", self.home / "unused", "default",
                                           origin_settings=self.settings)
        self.assertEqual(code, 0)
        self.assertFalse(json.loads(printed.call_args.args[0])["permission_granted"])

    async def test_work_stale_run_claim_lease_and_scope_are_refused(self):
        await self.kanban()
        ref = (await self.work_current())["reference"]
        for column, value in (("current_run_id", 999), ("claim_lock", "different"), ("claim_expires", 1),
                              ("assignee", "foreign"), ("workspace_path", "/foreign"), ("tenant", "foreign"),
                              ("body", "Different request"), ("skills", '["different"]'), ("project_id", "changed")):
            old = self.board.execute(f"SELECT {column} FROM tasks WHERE id=?", (self.task_id,)).fetchone()[0]
            self.board.execute(f"UPDATE tasks SET {column}=? WHERE id=?", (value, self.task_id))
            with self.subTest(column=column), self.assertRaises(transport.Unavailable):
                await self.work_inspected(ref)
            self.board.execute(f"UPDATE tasks SET {column}=? WHERE id=?", (old, self.task_id))
        self.board.execute("UPDATE task_runs SET claim_expires=1 WHERE id=?", (self.work_context["run_id"],))
        with self.assertRaises(transport.Unavailable):
            await self.work_inspected(ref)

    async def test_work_stop_and_successor_need_a_fresh_reference(self):
        from hermes_cli import kanban_db as kb
        from hermes_state_runtime import settle_session_input
        await self.kanban()
        old = (await self.work_current())["reference"]
        token = kb.list_events(self.board, self.task_id)[-1].id
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="needs_input", reason="Stop", expected_event_id=token))
        with self.assertRaises(transport.Unavailable):
            await self.work_inspected(old)
        settle_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=self.admission,
                             generation=self.row["generation"], outcome="interrupted")
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        await self.kanban(existing=True)
        new = (await self.work_current())["reference"]
        self.assertNotEqual(old["session_id"], new["session_id"])
        self.assertNotEqual(old["work_digest"], new["work_digest"])
        with self.assertRaises(transport.Unavailable):
            await self.work_inspected(old)
        self.assertFalse((await self.work_inspected(new))["permission_granted"])

    async def test_work_spoofed_carriers_and_claim_markers_are_refused(self):
        from agent import runtime_session_store
        from agent.delegation_context import delegated_child_context, non_dispatcher_owned_context
        from gateway.session_context import reset_session_vars, set_current_session_id
        await self.local()
        with self.assertRaises(transport.Unavailable):
            work.capture_context()  # Ordinary local/peer carriers cannot become worker registrations.
        with patch.object(runtime_session_store, "_worker_process", True), self.assertRaises(transport.Unavailable):
            await asyncio.to_thread(work.current, work.capture_context(), self.settings)
        await self.kanban()
        for scope in (delegated_child_context(), non_dispatcher_owned_context()):
            with scope, self.assertRaises(transport.Unavailable):
                work.capture_context()
        with patch.object(runtime_session_store, "_worker_process", False), self.assertRaises(transport.Unavailable):
            work.capture_context()
        reset_session_vars()
        with patch.dict(os.environ, {"HERMES_SESSION_ID": self.sid, "HERMES_KANBAN_TASK": self.task_id,
                                     "HERMES_SESSION_SOURCE": "kanban"}):
            with self.assertRaises(transport.Unavailable):
                work.capture_context()
        set_current_session_id(self.sid)
        self.board.execute("UPDATE task_events SET payload=? WHERE kind='worker_bound' AND task_id=?",
                           (json.dumps({"pid": os.getpid(), "claim_lock": "spoof"}), self.task_id))
        with self.assertRaises(transport.Unavailable):
            await self.work_current()

    async def test_work_tracked_read_transaction_preserves_a_live_board_write_lock(self):
        from hermes_cli.sqlite_safe_read import connect_tracked, has_live_connection
        import sqlite3
        await self.kanban()
        self.assertTrue(has_live_connection(self.board_path))
        self.board.execute("BEGIN IMMEDIATE")
        try:
            evidence = await self.work_inspected((await self.work_current())["reference"])
            self.assertFalse(evidence["permission_granted"])
            competing = connect_tracked(self.board_path, timeout=0.01, isolation_level=None)
            try:
                with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                    competing.execute("BEGIN IMMEDIATE")
            finally:
                competing.close()
            self.assertTrue(self.board.in_transaction)
            self.assertTrue(has_live_connection(self.board_path))
        finally:
            self.board.rollback()

    async def test_stop_checkpoint_capability_distinguishes_unknown_clear_and_pending(self):
        from gateway import session_ancillary
        from hermes_state_runtime import settle_session_input
        await self.local()
        interactive = (await self.current())["reference"]
        # fae has no persisted checkpoint getter. An in-flight direct Stop may still have a
        # started receipt; it must produce UNKNOWN evidence, never an absence-of-Stop claim.
        await self.authority.interrupt(self.conn.actor, self.ref, self.row["generation"])
        evidence = await self.inspected(interactive)
        self.assertEqual(evidence["stop_evidence"]["state"], "unknown")
        self.assertFalse(evidence["stop_evidence"]["quiescence_proven"])
        settle_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=self.admission,
                             generation=self.row["generation"], outcome="interrupted")
        with self.assertRaises(transport.Unavailable):
            await self.inspected(interactive)
        await self.kanban()
        current = (await self.work_current())["reference"]
        self.assertEqual((await self.work_inspected(current))["stop_evidence"]["state"], "unknown")
        # The corrective core's existing getter reads a single native meta checkpoint. Test
        # that optional ABI against real native RO state, without changing the pinned core.
        key = "fixture.pending.stop." + self.sid
        with patch.object(session_ancillary, "process_stop_pending", lambda db, sid: db.get_meta("fixture.pending.stop." + sid), create=True):
            clear = (await self.work_current())["reference"]
            self.assertEqual((await self.work_inspected(clear))["stop_evidence"]["state"], "clear")
            self.authority.db.set_meta(key, "1:1")
            with self.assertRaises(transport.Unavailable):
                await self.work_inspected(clear)
            with self.assertRaises(transport.Unavailable):
                await self.work_current()
            await self.local()
            self.authority.db.set_meta("fixture.pending.stop." + self.sid, "1:1")
            with self.assertRaises(transport.Unavailable):
                await self.current()

    async def test_work_newer_stop_during_inspection_is_rechecked(self):
        from hermes_cli import kanban_db as kb
        await self.kanban()
        ref = (await self.work_current())["reference"]
        count = 0
        def stop(method, params):
            nonlocal count
            count += 1
            if count == 2:
                kb.block_task(self.board, self.task_id, kind="needs_input", reason="new Stop")
        self.before_rpc = stop
        with self.assertRaises(transport.Unavailable):
            await self.work_inspected(ref)

    async def test_work_restart_and_settlement_invalidate_without_invented_requester(self):
        from hermes_state_runtime import begin_runtime_epoch, settle_session_input
        await self.kanban()
        ref = (await self.work_current())["reference"]
        settle_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=self.admission,
                             generation=self.row["generation"], outcome="interrupted")
        with self.assertRaises(transport.Unavailable):
            await self.work_inspected(ref)
        self.authority.epoch = begin_runtime_epoch(self.authority.db, instance_id="new-owner")
        self.authority.instance_id = "new-owner"
        with self.assertRaises(transport.Unavailable):
            await self.work_inspected(ref)

    async def test_surfaces_preserve_original_principal_and_scope_without_grant(self):
        for platform in ("local", "telegram", "slack", "api_server"):
            with self.subTest(platform=platform):
                if platform == "local":
                    await self.local()
                elif platform == "api_server":
                    await self.api()
                else:
                    await self.messaging(platform)
                current = await self.current()
                self.assertEqual(set(current), {"reference", "permission_granted"})
                self.assertFalse(current["permission_granted"])
                evidence = await self.inspected(current["reference"])
                self.assertEqual(evidence["original_message"], "Take over ABC-1")
                self.assertEqual(evidence["scope"], origin.scope(self.settings))
                self.assertFalse(evidence["permission_granted"])
                self.assertEqual(evidence["requester"]["principal_id"], self.row["principal_id"])
                if platform == "api_server":
                    self.assertEqual(evidence["requester"]["kind"], "api_credential")
                    self.assertEqual(evidence["requester"]["principal"], "principal:fixture:" + "a" * 32)
                self.assertEqual(evidence["reference"]["session_id"], self.sid)

    async def test_native_registered_tool_captures_the_actual_current_caller(self):
        from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
        from tools.registry import registry
        await self.local()
        self.config["plugins"] = {"entries": {"linear": {"settings": {"enabled": True, **self.settings}}}}
        (self.home / "config.yaml").write_text(json.dumps(self.config))
        manager = PluginManager(scope_key=str(self.home))
        self.addCleanup(manager.unload)
        plugin.register(PluginContext(PluginManifest(name="linear"), manager))
        handler = registry.get_entry("linear", scope=str(self.home)).handler
        result = json.loads(await asyncio.to_thread(handler, {"action": "origin_reference"}))
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["reference"]["admission_id"], self.admission)
        self.assertFalse(result["permission_granted"])
        self.assertEqual((await self.inspected(result["reference"]))["original_message"], "Take over ABC-1")
        denied = json.loads(await asyncio.to_thread(handler, {"action": "origin_reference", "session_id": "forged"}))
        self.assertFalse(denied["ok"])

    async def test_queued_input_and_other_history_are_not_dumped(self):
        from gateway.session_contract import Submission
        await self.local()
        self.authority.db.append_message(self.sid, "user", "UNRELATED_PRIVATE_HISTORY")
        await self.authority.submit(self.conn.actor, Submission("queued", self.ref, {"text": "QUEUED_PRIVATE_TEXT"}, "queue"))
        evidence = await self.inspected((await self.current())["reference"])
        serialized = json.dumps(evidence)
        self.assertNotIn("UNRELATED_PRIVATE_HISTORY", serialized)
        self.assertNotIn("QUEUED_PRIVATE_TEXT", serialized)
        self.assertEqual(evidence["original_message"], "Take over ABC-1")

    async def test_changed_scope_and_spoofed_reference_are_refused(self):
        await self.local()
        ref = (await self.current())["reference"]
        for key, value in (("profile", "foreign"), ("session_id", "foreign"), ("admission_id", "foreign"),
                           ("input_id", "foreign"), ("execution_generation", ref["execution_generation"] + 1),
                           ("authority_epoch", ref["authority_epoch"] + 1), ("instance_id", "restarted")):
            with self.subTest(key=key), self.assertRaises(transport.Unavailable):
                await self.inspected({**ref, key: value})
        self.settings["identity"]["teams"] = ["different-team"]
        with self.assertRaises(transport.Unavailable):
            await self.inspected(ref)

    async def test_stop_or_settlement_between_reference_and_inspection_invalidates(self):
        from hermes_state_runtime import settle_session_input
        await self.local()
        ref = (await self.current())["reference"]
        settle_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=self.admission,
                             generation=self.row["generation"], outcome="interrupted")
        with self.assertRaises(transport.Unavailable):
            await self.inspected(ref)

    async def test_stop_during_read_is_rechecked(self):
        from hermes_state_runtime import settle_session_input
        await self.local()
        resume_count = 0
        def stop(method, params):
            nonlocal resume_count
            if method == "prompt.receipt":
                resume_count += 1
                if resume_count == 2:
                    settle_session_input(self.authority.db, epoch=self.authority.epoch,
                        admission_id=self.admission, generation=self.row["generation"], outcome="interrupted")
        self.before_rpc = stop
        with self.assertRaises(transport.Unavailable):
            await self.current()

    async def test_child_bot_and_producer_origins_are_refused(self):
        await self.local(extra={"turn_author": {"id": "bot:peer", "is_bot": True}})
        with self.assertRaises(transport.Unavailable):
            await self.current()
        context = transport.capture_context()
        with self.assertRaises(transport.Unavailable):
            await asyncio.to_thread(origin.current, replace(context, delegated_child=True), self.settings)
        await self.local(source="tool")
        with self.assertRaises(transport.Unavailable):
            await self.current()

    async def test_api_without_authenticated_transfer_principal_is_refused(self):
        await self.api()
        payload = self.row["payload"]
        payload["api_turn_v1"].pop("transfer_auth")
        self.authority.db._execute_write(lambda c: c.execute(
            "UPDATE session_admissions SET payload_json=? WHERE admission_id=?", (json.dumps(payload), self.admission)))
        with self.assertRaises(transport.Unavailable):
            await self.current()

    async def test_native_bot_and_caller_identity_mismatch_are_refused(self):
        await self.messaging("telegram")
        context = transport.capture_context()
        with self.assertRaises(transport.Unavailable):
            await asyncio.to_thread(origin.current, replace(context, user_id="forged"), self.settings)
        payload = self.row["payload"]
        payload["native_text_v1"]["source"]["is_bot"] = True
        self.authority.db._execute_write(lambda c: c.execute(
            "UPDATE session_admissions SET payload_json=? WHERE admission_id=?", (json.dumps(payload), self.admission)))
        with self.assertRaises(transport.Unavailable):
            await self.current()

    async def test_compression_tips_keep_the_original_admission_on_each_surface(self):
        for platform in ("local", "telegram", "slack", "api_server"):
            with self.subTest(platform=platform):
                if platform == "local":
                    await self.local()
                elif platform == "api_server":
                    await self.api()
                else:
                    await self.messaging(platform)
                original = (await self.current())["reference"]
                stored = self.authority.db.get_session(self.sid)
                tip = self.sid + "-compressed"
                self.authority.db.publish_compression_child(parent_session_id=self.sid, child_session_id=tip,
                    source=stored["source"], messages=[{"role": "user", "content": "Synthetic compression checkpoint"}],
                    require_compression_lease=False)
                from gateway.session_context import set_current_session_id
                set_current_session_id(tip)
                compressed = (await self.current())["reference"]
                self.assertEqual(compressed, original)
                self.assertEqual((await self.inspected(compressed))["reference"]["session_id"], self.sid)

    async def test_owner_restart_invalidates_a_previously_valid_reference(self):
        from hermes_state_runtime import begin_runtime_epoch
        await self.local()
        ref = (await self.current())["reference"]
        self.authority.epoch = begin_runtime_epoch(self.authority.db, instance_id="restarted")
        self.authority.instance_id = "restarted"
        with self.assertRaises(transport.Unavailable):
            await self.inspected(ref)

    async def test_origin_projection_does_not_change_native_state(self):
        await self.local()
        before = self.authority.db._read_all("SELECT * FROM session_admissions")
        sessions = self.authority.db._read_all("SELECT * FROM sessions")
        evidence = await self.inspected((await self.current())["reference"])
        self.assertEqual(self.authority.db._read_all("SELECT * FROM session_admissions"), before)
        self.assertEqual(self.authority.db._read_all("SELECT * FROM sessions"), sessions)
        self.assertFalse(evidence["permission_granted"])

    async def test_native_steering_transfer_is_ambiguous(self):
        from hermes_state_runtime import admit_session_input, steer_session_input
        await self.local()
        queued = admit_session_input(self.authority.db, epoch=self.authority.epoch, principal_id="operator",
            session_id=self.sid, request_id="steer", payload={"text": "different instruction"})
        steer_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=queued["admission_id"],
                            generation=self.row["generation"])
        with self.assertRaises(transport.Unavailable):
            await self.current()

    async def test_unknown_execution_does_not_become_requester_authority(self):
        await self.local()
        self.authority.db._execute_write(lambda c: c.execute(
            "UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (self.admission,)))
        with self.assertRaises(transport.Unavailable):
            await self.current()

    async def test_native_reconciled_unknown_history_does_not_block_a_new_request(self):
        from hermes_state_runtime import (admit_session_input, begin_runtime_epoch, claim_session_input,
            recover_session_inputs, resolve_unknown_session_input, RuntimeStoreError)
        await self.local()
        old_id, old_generation = self.admission, self.row["generation"]
        self.authority.epoch = begin_runtime_epoch(self.authority.db, instance_id="recovered")
        self.authority.instance_id = "recovered"
        recover_session_inputs(self.authority.db, epoch=self.authority.epoch)
        new = admit_session_input(self.authority.db, epoch=self.authority.epoch,
            principal_id=self.conn.actor.subject, session_id=self.sid, request_id="new-request",
            payload={"text": "Take over a new increment of ABC-1"})
        # The native owner itself prohibits execution behind unresolved uncertainty.
        with self.assertRaisesRegex(RuntimeStoreError, "unknown_execution"):
            claim_session_input(self.authority.db, epoch=self.authority.epoch, session_id=self.sid)
        resolve_unknown_session_input(self.authority.db, epoch=self.authority.epoch,
                                      admission_id=old_id, generation=old_generation)
        self.admission = new["admission_id"]
        self.claim()
        evidence = await self.inspected((await self.current())["reference"])
        self.assertEqual(evidence["original_message"], "Take over a new increment of ABC-1")
        self.assertNotEqual(evidence["reference"]["admission_id"], old_id)

    async def test_old_steering_history_does_not_block_the_current_generation(self):
        from hermes_state_runtime import admit_session_input, steer_session_input, settle_session_input
        await self.local()
        old_generation = self.row["generation"]
        steering = admit_session_input(self.authority.db, epoch=self.authority.epoch,
            principal_id=self.conn.actor.subject, session_id=self.sid, request_id="old-steer",
            payload={"text": "Earlier bounded clarification"})
        steer_session_input(self.authority.db, epoch=self.authority.epoch,
                            admission_id=steering["admission_id"], generation=old_generation)
        settle_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=self.admission,
            generation=old_generation, outcome="completed", _transfers={steering["admission_id"]: True})
        later = admit_session_input(self.authority.db, epoch=self.authority.epoch,
            principal_id=self.conn.actor.subject, session_id=self.sid, request_id="later",
            payload={"text": "Take over the remaining work on ABC-1"})
        self.admission = later["admission_id"]
        self.claim()
        evidence = await self.inspected((await self.current())["reference"])
        self.assertGreater(evidence["reference"]["execution_generation"], old_generation)
        self.assertEqual(evidence["original_message"], "Take over the remaining work on ABC-1")

