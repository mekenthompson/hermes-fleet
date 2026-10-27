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
from hermes_fleet_linear_plugin import origin, transport, work, cleanup


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

    async def api(self, *, session_id="api-session", request_id="api-request"):
        from gateway.session_api import bind_api_session
        from gateway.session_contract import SessionRef
        from hermes_state_runtime import admit_session_input
        self.sid = session_id
        self.ref = bind_api_session(self.authority, self.sid)
        payload = {"text": "Take over ABC-1", "api_turn_v1": {"history": None, "settings": {},
            "transfer_auth": {"principal": "principal:fixture:" + "a" * 32, "transport_family": "local-api"}}}
        row = admit_session_input(self.authority.db, epoch=self.authority.epoch, principal_id="api",
                                  session_id=self.sid, request_id=request_id, payload=payload)
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
            self.board_path = self.home / "kanban.db"
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

    def process_witness(self, start):
        from gateway.drain_control import current_instantiation_epoch
        return f"{current_instantiation_epoch()}|{start}"

    async def stopped_issue(self, *, existing=False):
        from hermes_cli import kanban_db as kb
        from hermes_state_runtime import (register_worker_execution, adopt_worker_execution,
            mutate_worker_execution, settle_session_input)
        from hermes_fleet_linear_plugin.store import Store
        from gateway import session_ancillary
        await self.kanban(existing=existing)
        # Finite native state fixtures: real register/adopt/finish and admission settlement;
        # no actual model, process launch, owner bootstrap or production Stop.
        executor = "admission-worker:" + self.admission
        args = dict(epoch=self.authority.epoch, execution_id=executor, session_id=self.sid,
                    generation=self.row["generation"])
        register_worker_execution(self.authority.db, **args, kind="compute", adoption_secret="fixture-adoption-private")
        adopt_worker_execution(self.authority.db, **args, adoption_secret="fixture-adoption-private")
        mutate_worker_execution(self.authority.db, **args, sequence=1, operation="execution.finish", payload={})
        settle_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=self.admission,
                             generation=self.row["generation"], outcome="interrupted")
        with kb.write_txn(self.board):
            self.board.execute("UPDATE task_events SET payload=? WHERE kind='worker_bound' AND task_id=?",
                (json.dumps({"pid": 99999998, "started_at": self.process_witness(1), "claim_lock": self.work_context["claim_lock"]}), self.task_id))
            kb._append_event(self.board, self.task_id, "spawned", {"pid": 99999999, "started_at": self.process_witness(2)},
                             run_id=self.work_context["run_id"])
            self.board.execute("UPDATE task_runs SET worker_pid=99999999,worker_started_at=? WHERE id=?",
                               (self.process_witness(2), self.work_context["run_id"]))
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="needs_input", reason="fixture Stop"))
        self.stop_id = kb.list_events(self.board, self.task_id)[-1].id
        self.linear_state = self.home / "linear" / "state.db"
        self.linear_store = Store(self.linear_state)
        self.assertTrue(self.linear_store.bind_native("issue-one", self.task_id, "default", "default", at=1))
        # Optional corrective-core getter adapter, explicitly synthetic on pinned fae; it
        # executes a real native read-only meta lookup, not an unconditional success stub.
        getter = patch.object(session_ancillary, "process_stop_pending",
            lambda db, sid: db.get_meta("fixture.pending.stop." + sid), create=True)
        getter.start()
        self.addCleanup(getter.stop)

    async def cleanup_inspected(self, *, stop_id=None):
        from agent import runtime_session_store
        with patch.object(runtime_session_store, "_worker_process", False):
            return await asyncio.to_thread(cleanup.inspect, self.linear_state, "issue-one", "default",
                                           self.stop_id if stop_id is None else stop_id, self.settings)

    def cancelled_control(self):
        """Pinned-fae final-row adapter for candidate c9b8ad4's atomic Stop writer.

        This does not execute the unaccepted writer. Its final native row has the same
        principal/request/payload/status and NULL generation; no prompt or worker exists.
        The target is the logical owner_admitted session, never a physical worker SID.
        """
        from hermes_state_runtime import admit_session_input, cancel_session_input
        identity = json.dumps(["completion", "fixture-process", 123.0], separators=(",", ":"))
        row = admit_session_input(self.authority.db, epoch=self.authority.epoch,
            principal_id="native-process-stop", session_id=self.sid, request_id=identity,
            payload={"process_completion": identity, "route": "fixture-process-route"})
        row = cancel_session_input(self.authority.db, epoch=self.authority.epoch,
                                   admission_id=row["admission_id"])
        self.assertEqual((row["status"], row["outcome"], row["generation"]),
                         ("terminal", "cancelled", None))
        self.assertEqual(set(row["payload"]), {"process_completion", "route"})
        self.assertEqual(row["target_session_id"], self.sid)
        return row

    def reference_through_a2a(self, captured):
        # These are the actual primitives used by tools._send_task and adapter._prepare_task.
        # No SendMessage handler, task store, audit, network or dispatch is invoked.
        from plugins.platforms.a2a import protocol, security
        wire = json.dumps(captured, sort_keys=True, separators=(",", ":"))
        safe = security.redact_outbound(wire)
        self.assertEqual(safe, wire)
        message = protocol.text_message(protocol.ROLE_USER, safe, context_id="fixture-context")
        extracted = protocol.extract_text(json.loads(json.dumps({"message": message})))
        self.assertEqual(extracted, wire)
        framed = security.wrap_inbound("configured-peer", extracted)
        prefix = security.PRIVACY_PREFIX.format(peer="configured-peer")
        self.assertTrue(framed.startswith(prefix))
        decoded = json.loads(framed[len(prefix):])
        self.assertEqual(decoded, captured)
        self.assertFalse(decoded["permission_granted"])
        # Keep the real scrub/framer enabled: preservation must not depend on bypassing them.
        self.assertNotIn("sk-" + "a" * 40, security.redact_outbound("sk-" + "a" * 40))
        self.assertIn("[filtered]", security.wrap_inbound("configured-peer", "<|system|>"))
        return decoded["reference"]

    async def test_a2a_reference_framing_preserves_each_interactive_surface(self):
        for surface in ("local", "api", "telegram", "slack"):
            with self.subTest(surface=surface):
                if surface == "local":
                    await self.local()
                elif surface == "api":
                    await self.api()
                else:
                    await self.messaging(surface)
                captured = await self.current()
                ref = self.reference_through_a2a(captured)
                evidence = await self.inspected(ref)
                self.assertEqual(evidence["reference"], ref)
                self.assertFalse(evidence["permission_granted"])

    async def test_a2a_work_reference_framing_preserves_unknown_requester(self):
        await self.kanban()
        ref = self.reference_through_a2a(await self.work_current())
        evidence = await self.work_inspected(ref)
        self.assertEqual(evidence["requester"], {"kind": "native_dispatcher", "original_requester": "unknown"})
        self.assertFalse(evidence["permission_granted"])

    async def test_a2a_explicit_api_ids_stay_private_and_survive_native_framing(self):
        # Native API permits arbitrary explicit path-safe IDs. They remain private even
        # when the A2A scrub would intentionally alter them if carried in peer text.
        await self.api(session_id="fixture@example.invalid", request_id="sk-" + "a" * 40)
        captured = await self.current()
        self.assertNotIn("session_id", captured["reference"])
        self.assertNotIn("input_id", captured["reference"])
        self.assertNotIn(self.sid, json.dumps(captured))
        self.assertNotIn(self.row["request_id"], json.dumps(captured))
        evidence = await self.inspected(self.reference_through_a2a(captured))
        self.assertEqual(evidence["requester"]["kind"], "api_credential")
        self.assertEqual(evidence["requester"]["principal"], "principal:fixture:" + "a" * 32)
        self.assertFalse(evidence["permission_granted"])

    async def test_stop_control_receipt_is_never_origin_or_requester_authority(self):
        from hermes_state_runtime import settle_session_input
        await self.local()
        control = self.cancelled_control()
        captured = await self.current()
        self.assertEqual(captured["reference"]["admission_id"], self.admission)
        evidence = await self.inspected(captured["reference"])
        self.assertEqual(evidence["original_message"], "Take over ABC-1")
        self.assertNotEqual(evidence["requester"]["principal_id"], control["principal_id"])
        forged = dict(captured["reference"], admission_id=control["admission_id"])
        with self.assertRaises(transport.Unavailable):
            await self.inspected(forged)
        settle_session_input(self.authority.db, epoch=self.authority.epoch,
            admission_id=self.admission, generation=self.row["generation"], outcome="interrupted")
        with self.assertRaises(transport.Unavailable):
            await self.current()

    async def test_cleanup_terminal_stop_controls_are_data_not_execution_or_proof(self):
        await self.stopped_issue()
        control = self.cancelled_control()
        before = self.authority.db._read_all("SELECT * FROM session_admissions")
        evidence = await self.cleanup_inspected()
        self.assertTrue(evidence["quiescence_proven"], evidence)
        self.assertFalse(evidence["permission_granted"])
        self.assertNotIn(control["request_id"], json.dumps(evidence))
        self.assertEqual(self.authority.db._read_all("SELECT * FROM session_admissions"), before)
        # A terminal cancellation is never a substitute for a clear native Stop checkpoint.
        self.authority.db.set_meta("fixture.pending.stop." + self.sid, "pending")
        pending = await self.cleanup_inspected()
        self.assertFalse(pending["quiescence_proven"])
        self.assertEqual(pending["reason"], "native_stop_checkpoint_pending")

    async def test_cleanup_owner_cli_proves_only_bound_stopped_work_without_state_changes(self):
        from hermes_fleet_linear_plugin import ops
        import argparse
        from agent import runtime_session_store
        await self.stopped_issue()
        self.authority.db.append_message(self.sid, "user", "PRIVATE_TRANSCRIPT_SENTINEL")
        before = [tuple(r) for r in self.board.execute("SELECT * FROM task_events")]
        native = self.authority.db._read_all("SELECT * FROM session_admissions")
        evidence = await self.cleanup_inspected()
        self.assertTrue(evidence["quiescence_proven"], evidence)
        self.assertFalse(evidence["permission_granted"])
        self.assertEqual(evidence["stop_event_id"], self.stop_id)
        for private in ("CLAIM_PRIVATE_SENTINEL", "PRIVATE_TRANSCRIPT_SENTINEL", str(self.board_path), "fixture-adoption-private"):
            self.assertNotIn(private, json.dumps(evidence))
        self.assertEqual([tuple(r) for r in self.board.execute("SELECT * FROM task_events")], before)
        self.assertEqual(self.authority.db._read_all("SELECT * FROM session_admissions"), native)
        parser = argparse.ArgumentParser()
        ops.setup(parser)
        args = parser.parse_args(["inspect-cleanup", "issue-one", "--expected-stop-event-id", str(self.stop_id)])
        with patch.object(runtime_session_store, "_worker_process", False), patch("builtins.print") as printed:
            rc = await asyncio.to_thread(ops.run, args, self.linear_state, self.home / "unused", "deployment-routing-alias", origin_settings=self.settings)
        self.assertEqual(rc, 0)
        self.assertTrue(json.loads(printed.call_args.args[0])["quiescence_proven"])
        self.board.execute("ALTER TABLE task_runs DROP COLUMN worker_started_at")
        damaged = await self.cleanup_inspected()
        self.assertFalse(damaged["quiescence_proven"])
        self.assertEqual(damaged["reason"], "cleanup_evidence_unavailable")

    async def test_cleanup_unsupported_pending_and_live_admission_are_unknown(self):
        from gateway import session_ancillary
        await self.stopped_issue()
        with patch.object(session_ancillary, "process_stop_pending", None):
            result = await self.cleanup_inspected()
        self.assertEqual(result["reason"], "native_stop_checkpoint_unsupported")
        self.authority.db.set_meta("fixture.pending.stop." + self.sid, "pending")
        result = await self.cleanup_inspected()
        self.assertFalse(result["quiescence_proven"])
        self.assertEqual(result["reason"], "native_stop_checkpoint_pending")
        self.authority.db._execute_write(lambda c: c.execute("DELETE FROM state_meta WHERE key=?", ("fixture.pending.stop." + self.sid,)))
        self.authority.db._execute_write(lambda c: c.execute("UPDATE session_admissions SET status='started',outcome=NULL WHERE admission_id=?", (self.admission,)))
        result = await self.cleanup_inspected()
        self.assertFalse(result["quiescence_proven"])
        self.assertEqual(result["reason"], "native_admission_not_settled")

    async def test_cleanup_unknown_workers_or_missing_executor_never_become_settlement(self):
        from hermes_state_runtime import register_worker_execution
        await self.stopped_issue()
        executor = "admission-worker:" + self.admission
        self.authority.db._execute_write(lambda c: c.execute("UPDATE worker_executions SET status='unknown' WHERE execution_id=?", (executor,)))
        result = await self.cleanup_inspected()
        self.assertEqual(result["reason"], "native_worker_or_spawner_unsettled")
        self.authority.db._execute_write(lambda c: c.execute("UPDATE worker_executions SET status='terminal' WHERE execution_id=?", (executor,)))
        child = self.sid + "-child"
        self.authority.db.create_session(child, source="subagent", parent_session_id=self.sid)
        register_worker_execution(self.authority.db, epoch=self.authority.epoch, execution_id=executor + "/child:fixture",
            session_id=child, generation=0, kind="child", adoption_secret="fixture-child-private")
        self.assertEqual((await self.cleanup_inspected())["reason"], "native_child_spawner_unsettled")
        self.authority.db._execute_write(lambda c: c.execute("DELETE FROM worker_executions WHERE execution_id=?", (executor + "/child:fixture",)))
        self.authority.db._execute_write(lambda c: (c.execute("DELETE FROM worker_receipts WHERE execution_id=?", (executor,)),
            c.execute("DELETE FROM worker_executions WHERE execution_id=?", (executor,))))
        result = await self.cleanup_inspected()
        self.assertEqual(result["reason"], "native_executor_settlement_missing")

    async def test_cleanup_live_recycled_and_unverifiable_worker_fingerprints(self):
        from hermes_cli.kanban_db_dispatch import _process_fingerprint
        await self.stopped_issue()
        real = _process_fingerprint(os.getpid())
        self.assertIsNotNone(real)
        def bound(fingerprint):
            self.board.execute("UPDATE task_events SET payload=? WHERE task_id=? AND kind='worker_bound'",
                (json.dumps({"pid": os.getpid(), "started_at": fingerprint, "claim_lock": self.work_context["claim_lock"]}), self.task_id))
        bound(real)
        self.assertEqual((await self.cleanup_inspected())["reason"], "old_worker_alive")
        bound(self.process_witness(1))
        evidence = await self.cleanup_inspected()
        self.assertTrue(evidence["quiescence_proven"], evidence)
        self.assertEqual(evidence["attempts"][0]["workers"][0]["fate"], "recycled")
        bound(None)
        self.assertEqual((await self.cleanup_inspected())["reason"], "worker_identity_missing")

    async def test_cleanup_scoped_checkpoint_retains_owned_obligations_and_foreign_records(self):
        from gateway.status import get_process_start_time
        await self.stopped_issue()
        route = self.authority.sessions[self.sid].route
        foreign = {"session_key": "foreign", "owner_task_id": "foreign", "pid": os.getpid(),
                   "host_start_time": get_process_start_time(os.getpid()), "systemd_unit": "FOREIGN_SCOPE_PRIVATE"}
        own = {"session_key": route, "parent_session_id": self.sid, "pid": os.getpid(),
               "host_start_time": get_process_start_time(os.getpid()), "pid_scope": "host"}
        path = self.home / "processes.json"
        for changes in ({}, {"host_start_time": 1}, {"pid": 99999997}, {"systemd_unit": "pending-native-scope"}):
            with self.subTest(changes=changes):
                path.write_text(json.dumps([foreign, {**own, **changes}]))
                before = path.read_bytes()
                evidence = await self.cleanup_inspected()
                self.assertFalse(evidence["quiescence_proven"])
                self.assertEqual(evidence["reason"], "scoped_process_cleanup_unknown")
                self.assertNotIn("FOREIGN_SCOPE_PRIVATE", json.dumps(evidence))
                self.assertEqual(path.read_bytes(), before)
        # A recycled legacy leader cannot discharge native ownership either.
        # Success requires native retirement of the attributed obligation.
        path.write_text(json.dumps([foreign]))
        evidence = await self.cleanup_inspected()
        self.assertTrue(evidence["quiescence_proven"], evidence)
        self.assertEqual(evidence["scoped_checkpoint"], {"scoped_records": 0, "recycled_records": 0})

    async def test_cleanup_new_stop_unblock_foreign_binding_and_late_spawner_invalidate(self):
        from hermes_cli import kanban_db as kb
        await self.stopped_issue()
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="needs_input", expected_event_id=self.stop_id))
        self.assertEqual((await self.cleanup_inspected())["reason"], "stop_event_changed")
        self.stop_id = kb.list_events(self.board, self.task_id)[-1].id
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        with kb.write_txn(self.board):
            kb._append_event(self.board, self.task_id, "worker_bound", {"pid": 99999998,
                "started_at": self.process_witness(1), "claim_lock": self.work_context["claim_lock"]}, run_id=self.work_context["run_id"])
        self.assertEqual((await self.cleanup_inspected())["reason"], "late_worker_binding_after_stop")
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        self.assertEqual((await self.cleanup_inspected())["reason"], "task_not_currently_stopped")

    async def test_cleanup_stop_changes_during_receipt_or_owner_restart_are_unknown(self):
        from hermes_cli import kanban_db as kb
        from hermes_state_runtime import begin_runtime_epoch
        await self.stopped_issue()
        def stop(method, params):
            self.before_rpc = None
            kb.block_task(self.board, self.task_id, kind="needs_input", expected_event_id=self.stop_id)
        self.before_rpc = stop
        self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        self.stop_id = kb.list_events(self.board, self.task_id)[-1].id
        self.authority.epoch = begin_runtime_epoch(self.authority.db, instance_id="different-owner")
        # Endpoint still reports old instance: independently read epoch/instance must refuse.
        self.assertEqual((await self.cleanup_inspected())["reason"], "native_owner_changed")

    async def test_cleanup_missing_reporting_link_discovers_unique_native_work_without_adopting(self):
        from hermes_cli import kanban_db as kb
        await self.stopped_issue()
        with self.linear_store._tx() as db:
            db.execute("DELETE FROM metadata WHERE key='native-binding:issue-one'")
            db.execute("DELETE FROM work WHERE issue_id='issue-one'")
        self.board.execute("UPDATE tasks SET idempotency_key='linear:issue-one' WHERE id=?", (self.task_id,))
        evidence = await self.cleanup_inspected()
        self.assertTrue(evidence["quiescence_proven"], evidence)
        self.assertEqual(evidence["discovery_source"], "native_history_discovery")
        self.assertIsNone(self.linear_store.get("issue-one"))
        kb.create_task(self.board, title="second plausible work", assignee="default", idempotency_key=None,
                       body="Linear issue issue-one: https://linear.example/issue")
        # Marker-only metadata is owner CLI discovery data, not authorization.
        from agent import runtime_session_store
        with patch.object(runtime_session_store, "_worker_process", False):
            result = await asyncio.to_thread(cleanup.inspect, self.linear_state, "issue-one", "default", self.stop_id,
                self.settings, {"id": "issue-one", "identifier": "ABC-1", "url": "https://linear.example/issue"})
        self.assertEqual(result["reason"], "native_history_match_missing_or_ambiguous")
        self.assertIsNone(self.linear_store.get("issue-one"))

    async def test_cleanup_foreign_binding_and_scope_fence_are_not_authority(self):
        await self.stopped_issue()
        with self.linear_store._tx() as db:
            db.execute("UPDATE metadata SET value=? WHERE key='native-binding:issue-one'",
                       (json.dumps({"issue_id": "issue-one", "task_id": self.task_id, "board": "default", "profile": "foreign"}),))
        self.assertEqual((await self.cleanup_inspected())["reason"], "native_history_match_missing_or_ambiguous")
        with self.linear_store._tx() as db:
            db.execute("INSERT INTO specialist_scope_fence(issue_id,reason,fenced_at) VALUES('issue-one','fixture',1)")
        self.assertEqual((await self.cleanup_inspected())["reason"], "issue_scope_fenced")

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
        self.assertNotEqual(old["admission_id"], new["admission_id"])
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
        supported = callable(getattr(session_ancillary, "process_stop_pending", None))
        # An in-flight Stop retains a started receipt. A supported pending
        # checkpoint refuses; the older ABI reports unknown, never clear.
        # Adopt only a finite interrupt endpoint so the older Core does not
        # need its unrelated gateway bootstrap to inspect a cached-agent slot.
        self.authority.adopt_agent(self.sid, self.row["generation"], SimpleNamespace(interrupt=lambda *args: None))
        await self.authority.interrupt(self.conn.actor, self.ref, self.row["generation"])
        if supported:
            with self.assertRaises(transport.Unavailable):
                await self.inspected(interactive)
        else:
            evidence = await self.inspected(interactive)
            self.assertEqual(evidence["stop_evidence"]["state"], "unknown")
            self.assertFalse(evidence["stop_evidence"]["quiescence_proven"])
        settle_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=self.admission,
                             generation=self.row["generation"], outcome="interrupted")
        with self.assertRaises(transport.Unavailable):
            await self.inspected(interactive)
        await self.kanban()
        current = (await self.work_current())["reference"]
        self.assertEqual((await self.work_inspected(current))["stop_evidence"]["state"], "clear" if supported else "unknown")
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
                self.assertEqual(evidence["reference"]["admission_id"], self.admission)
                self.assertNotIn("session_id", evidence["reference"])
                self.assertNotIn("input_id", evidence["reference"])

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
        ref = (await self.current())["reference"]
        for auth in (None, {"principal": "Ken", "transport_family": "local-api"},
                     {"principal": "opaque-unknown", "transport_family": "remote-api"}):
            with self.subTest(auth=auth):
                payload["api_turn_v1"]["transfer_auth"] = auth
                self.authority.db._execute_write(lambda c: c.execute(
                    "UPDATE session_admissions SET payload_json=? WHERE admission_id=?", (json.dumps(payload), self.admission)))
                with self.assertRaises(transport.Unavailable):
                    await self.current()
                with self.assertRaises(transport.Unavailable):
                    await self.inspected(ref)

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
                self.assertEqual((await self.inspected(compressed))["reference"]["admission_id"], self.admission)

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


    async def test_cleanup_partial_conflicting_and_opaque_checkpoint_ownership_is_unknown(self):
        await self.stopped_issue()
        route = self.authority.sessions[self.sid].route
        entries = [{}, {"session_key": route}, {"parent_session_id": self.sid},
                   {"session_key": "foreign", "parent_session_id": self.sid},
                   {"session_key": route, "parent_session_id": "foreign"},
                   {"session_key": "foreign", "parent_session_id": "foreign", "owner_task_id": self.sid}]
        for entry in entries:
            entry.update(pid=os.getpid(), systemd_unit="unresolved-private-scope")
            with self.subTest(entry=entry):
                (self.home / "processes.json").write_text(json.dumps([entry]))
                evidence = await self.cleanup_inspected()
                self.assertFalse(evidence["quiescence_proven"])
                self.assertEqual(evidence["reason"], "checkpoint_ownership_ambiguous")
                self.assertFalse(evidence["permission_granted"])
                self.assertNotIn("unresolved-private-scope", json.dumps(evidence))

    async def test_cleanup_native_spawn_before_admission_stays_unknown(self):
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_dispatch import _set_worker_pid, _process_fingerprint
        from gateway import session_ancillary
        getter = getattr(session_ancillary, "process_stop_pending", None)
        await self.stopped_issue()
        # Use the real candidate getter when supported; the release pin retains
        # the explicitly labelled read-only fixture getter from stopped_issue.
        if getter is not None:
            self.enterContext(patch.object(session_ancillary, "process_stop_pending", getter))
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="transient", reason="temporary retry"))
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        task = kb.claim_task(self.board, self.task_id, claimer="fixture-second-claim")
        self.assertIsNotNone(task)
        _set_worker_pid(self.board, self.task_id, os.getpid(), run_id=task.current_run_id, claim_lock=task.claim_lock)
        self.assertIsNotNone(_process_fingerprint(os.getpid()))
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="needs_input", reason="Stop before native admission"))
        self.stop_id = kb.list_events(self.board, self.task_id)[-1].id
        evidence = await self.cleanup_inspected()
        self.assertFalse(evidence["quiescence_proven"])
        self.assertEqual(evidence["reason"], "old_worker_alive")
        self.assertFalse(evidence["permission_granted"])

    async def test_cleanup_unadmitted_attempt_without_recorded_worker_is_unknown(self):
        from hermes_cli import kanban_db as kb
        await self.stopped_issue()
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="transient"))
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        task = kb.claim_task(self.board, self.task_id, claimer="fixture-unadmitted-claim")
        self.assertIsNotNone(task)
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="needs_input", reason="unadmitted Stop"))
        self.stop_id = kb.list_events(self.board, self.task_id)[-1].id
        evidence = await self.cleanup_inspected()
        self.assertFalse(evidence["quiescence_proven"])
        self.assertEqual(evidence["reason"], "attempt_native_admission_missing_or_ambiguous")

    async def test_cleanup_two_admitted_attempts_check_old_run_state_and_worker(self):
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_dispatch import _process_fingerprint
        await self.stopped_issue()
        old = dict(self.board.execute("SELECT * FROM task_runs WHERE id=?", (self.work_context["run_id"],)).fetchone())
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        # A reason-free parked transient transition resets the native recurrence
        # policy without synthesizing an unexecuted handoff run.
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="transient"))
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        await self.stopped_issue(existing=True)
        evidence = await self.cleanup_inspected()
        self.assertTrue(evidence["quiescence_proven"], evidence)
        self.assertEqual(len(evidence["attempts"]), 2)
        for changes in ({"status": "running"}, {"status": "unknown"}, {"ended_at": None}, {"profile": "foreign"},
                        {"worker_pid": os.getpid(), "worker_started_at": _process_fingerprint(os.getpid())}):
            with self.subTest(changes=changes):
                for key, value in changes.items():
                    self.board.execute("UPDATE task_runs SET " + key + "=? WHERE id=?", (value, old["id"]))
                self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
                for key in changes:
                    self.board.execute("UPDATE task_runs SET " + key + "=? WHERE id=?", (old[key], old["id"]))
        old_binding = dict(self.board.execute("SELECT * FROM task_events WHERE run_id=? AND kind='owner_admitted'",
                                              (old["id"],)).fetchone())
        self.board.execute("DELETE FROM task_events WHERE id=?", (old_binding["id"],))
        self.assertEqual((await self.cleanup_inspected())["reason"], "attempt_native_admission_missing_or_ambiguous")
        columns = ",".join(old_binding)
        self.board.execute("INSERT INTO task_events (" + columns + ") VALUES (" + ",".join("?" for _ in old_binding) + ")",
                           tuple(old_binding.values()))
        def old_run_change(method, params):
            self.before_rpc = None
            self.board.execute("UPDATE task_runs SET summary='changed while inspecting' WHERE id=?", (old["id"],))
        self.before_rpc = old_run_change
        self.assertEqual((await self.cleanup_inspected())["reason"], "cleanup_evidence_changed")

    async def test_cleanup_orphan_and_ambiguous_attempt_events_never_disappear(self):
        from hermes_cli import kanban_db as kb
        await self.stopped_issue()
        for kind in ("owner_admitted", "spawned", "worker_registered", "worker_bound"):
            with self.subTest(kind=kind), kb.write_txn(self.board):
                kb._append_event(self.board, self.task_id, kind,
                    {"pid": 99999999, "started_at": self.process_witness(2)}, run_id=999999)
            evidence = await self.cleanup_inspected()
            self.assertFalse(evidence["quiescence_proven"])
            self.board.execute("DELETE FROM task_events WHERE id=(SELECT MAX(id) FROM task_events WHERE task_id=?)", (self.task_id,))
        binding = self.board.execute("SELECT * FROM task_events WHERE task_id=? AND kind='owner_admitted'", (self.task_id,)).fetchone()
        with kb.write_txn(self.board):
            kb._append_event(self.board, self.task_id, "owner_admitted", json.loads(binding["payload"]), run_id=binding["run_id"])
        self.assertEqual((await self.cleanup_inspected())["reason"], "admission_after_stop_or_missing")
        token = kb.list_events(self.board, self.task_id)[-1].id
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="needs_input", expected_event_id=token))
        self.stop_id = kb.list_events(self.board, self.task_id)[-1].id
        self.assertEqual((await self.cleanup_inspected())["reason"], "attempt_native_admission_missing_or_ambiguous")

    async def test_cleanup_retained_native_job_scope_requires_native_owner_retirement(self):
        from gateway import session_ancillary
        from hermes_cli import kanban_db as kb
        getter = getattr(session_ancillary, "process_stop_pending", None)
        await self.stopped_issue()
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="transient"))
        self.assertTrue(kb.unblock_task(self.board, self.task_id))
        await self.stopped_issue(existing=True)
        if getter is not None:
            self.enterContext(patch.object(session_ancillary, "process_stop_pending", getter))
        entry = {"session_id": "proc_cleanup_fixture", "session_key": self.authority.sessions[self.sid].route,
                 "parent_session_id": self.sid, "owner_task_id": self.sid, "pid": 99999997,
                 "host_start_time": 123, "pid_scope": "host", "native_job_scope": True}
        checkpoint = self.home / "processes.json"
        receipt_path = self.home / "logs" / "process-output" / "proc_cleanup_fixture.scope.exit"
        receipt_path.parent.mkdir(parents=True)
        # Adversarial receipt-like fixtures, never an actual kernel exit claim.
        # Even a structurally affirmative file cannot replace native retirement:
        # pending reader/output ownership settlement also retains the checkpoint.
        positive = {"version": 1, "pid": entry["pid"], "birth": entry["host_start_time"],
                    "quiescent": True, "exit_code": 0}
        receipts = [None, "malformed", {**positive, "quiescent": False}, {**positive, "pid": 99999996},
                    {**positive, "birth": 124}, {**positive, "exit_code": None}, positive]
        for receipt in receipts:
            with self.subTest(receipt=receipt):
                if receipt is None:
                    receipt_path.unlink(missing_ok=True)
                else:
                    receipt_path.write_text(receipt if isinstance(receipt, str) else json.dumps(receipt))
                checkpoint.write_text(json.dumps([entry]))
                before = checkpoint.read_bytes()
                evidence = await self.cleanup_inspected()
                self.assertFalse(evidence["quiescence_proven"])
                self.assertEqual(evidence["reason"], "scoped_process_cleanup_unknown")
                self.assertFalse(evidence["permission_granted"])
                self.assertEqual(checkpoint.read_bytes(), before)
        for flag in (1, 0, None, "false", "true"):
            with self.subTest(flag=flag):
                checkpoint.write_text(json.dumps([{**entry, "native_job_scope": flag,
                                                  "pid": os.getpid(), "host_start_time": 1}]))
                self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        # Model only the owner's later checkpoint retirement in this finite
        # fixture; inspection itself never performs cleanup or writes the file.
        checkpoint.write_text("[]")
        retired = await self.cleanup_inspected()
        self.assertTrue(retired["quiescence_proven"], retired)
        self.assertEqual(len(retired["attempts"]), 2)
        # A positively foreign managed scope is preserved and excluded, never
        # used to discharge the inspected task or exposed in its evidence.
        checkpoint.write_text(json.dumps([{**entry, "session_key": "foreign", "parent_session_id": "foreign",
                                          "owner_task_id": "foreign"}]))
        before = checkpoint.read_bytes()
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        self.assertEqual(checkpoint.read_bytes(), before)
        def newly_retained(method, params):
            self.before_rpc = None
            checkpoint.write_text(json.dumps([entry]))
        self.before_rpc = newly_retained
        raced = await self.cleanup_inspected()
        self.assertFalse(raced["quiescence_proven"])
        self.assertEqual(raced["reason"], "scoped_process_cleanup_unknown")

    async def test_cleanup_saved_manual_binding_cannot_hide_another_matching_live_task(self):
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_dispatch import _set_worker_pid
        await self.stopped_issue()
        other = kb.create_task(self.board, title="Retained work on the same issue", assignee="default",
                               idempotency_key="linear:issue-one")
        claimed = kb.claim_task(self.board, other, claimer="fixture-other-claim")
        _set_worker_pid(self.board, other, os.getpid(), run_id=claimed.current_run_id, claim_lock=claimed.claim_lock)
        self.assertTrue(kb.block_task(self.board, other, kind="needs_input", reason="Stop before admission"))
        evidence = await self.cleanup_inspected()
        self.assertFalse(evidence["quiescence_proven"])
        self.assertEqual(evidence["reason"], "native_history_match_missing_or_ambiguous")
        self.assertFalse(evidence["permission_granted"])

    async def test_cleanup_single_saved_target_and_unrelated_live_work_remain_readable(self):
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_dispatch import _set_worker_pid
        await self.stopped_issue()
        self.board.execute("UPDATE tasks SET idempotency_key='linear:issue-one' WHERE id=?", (self.task_id,))
        other = kb.create_task(self.board, title="Work on a different issue", assignee="default",
                               idempotency_key="linear:another-issue")
        claimed = kb.claim_task(self.board, other, claimer="fixture-unrelated-claim")
        _set_worker_pid(self.board, other, os.getpid(), run_id=claimed.current_run_id, claim_lock=claimed.claim_lock)
        evidence = await self.cleanup_inspected()
        self.assertTrue(evidence["quiescence_proven"], evidence)
        self.assertEqual(evidence["task_id"], self.task_id)
        self.assertEqual(evidence["discovery_source"], "saved_native_binding")

    async def test_cleanup_same_snapshot_rechecks_tasks_created_after_discovery(self):
        from hermes_cli import kanban_db as kb
        from hermes_fleet_linear_plugin.bridge import Kanban
        await self.stopped_issue()
        original = Kanban.discover
        inserted = []
        def racing(reader, issue):
            found = original(reader, issue)
            if not inserted:
                from hermes_cli.kanban_db_connect import _sqlite_connect
                from contextlib import closing
                with closing(_sqlite_connect(self.board_path)) as writer:
                    writer.row_factory = __import__("sqlite3").Row
                    inserted.append(kb.create_task(writer, title="New matching executor", assignee="default",
                                                   idempotency_key="linear:issue-one"))
            return found
        with patch.object(Kanban, "discover", racing):
            evidence = await self.cleanup_inspected()
        self.assertTrue(inserted, evidence)
        self.assertFalse(evidence["quiescence_proven"])
        self.assertEqual(evidence["reason"], "native_history_match_missing_or_ambiguous")

    async def test_cleanup_matching_task_arriving_during_receipt_readback_invalidates(self):
        from hermes_cli import kanban_db as kb
        await self.stopped_issue()
        def arriving(method, params):
            self.before_rpc = None
            kb.create_task(self.board, title="New matching task", assignee="default", idempotency_key="linear:issue-one")
        self.before_rpc = arriving
        self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])

    async def test_cleanup_strict_predicate_keys_and_native_event_normalization_precede_selection(self):
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_dispatch import _process_fingerprint
        await self.stopped_issue()
        cases = [(b"\xff", self.work_context["run_id"], "spawned", False),
                 (self.task_id.encode(), self.work_context["run_id"], "spawned", False),
                 (self.task_id, b"\xff", "spawned", False),
                 (self.task_id, self.work_context["run_id"], b"spawned", False),
                 (self.task_id, self.work_context["run_id"], "spawned", True)]
        for task_key, run_key, kind, negative in cases:
            with self.subTest(task_key=task_key, run_key=run_key, kind=kind, negative=negative):
                with kb.write_txn(self.board):
                    kb._append_event(self.board, task_key, kind, {"pid": os.getpid(),
                        "started_at": _process_fingerprint(os.getpid())}, run_id=run_key)
                    event_id = self.board.execute("SELECT MAX(id) FROM task_events").fetchone()[0]
                    if negative:
                        self.board.execute("UPDATE task_events SET id=-1 WHERE id=?", (event_id,))
                        event_id = -1
                self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
                self.board.execute("DELETE FROM task_events WHERE id=?", (event_id,))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])

    async def test_cleanup_corrupt_retained_run_predicate_key_is_not_foreign_absence(self):
        await self.stopped_issue()
        original = dict(self.board.execute("SELECT * FROM task_runs WHERE id=?", (self.work_context["run_id"],)).fetchone())
        for field, value in (("task_id", b"\xff"), ("status", b"running"), ("ended_at", "not-a-native-epoch")):
            with self.subTest(field=field):
                self.board.execute("UPDATE task_runs SET " + field + "=? WHERE id=?", (value, original["id"]))
                self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
                self.board.execute("UPDATE task_runs SET " + field + "=? WHERE id=?", (original[field], original["id"]))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])

    async def test_cleanup_saved_binding_checks_other_boards_and_archived_matching_work(self):
        from contextlib import closing
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_connect import _sqlite_connect
        from hermes_cli.kanban_db_dispatch import _set_worker_pid
        await self.stopped_issue()
        slug = "retained-project"
        kb.create_board(slug)
        with closing(_sqlite_connect(kb.kanban_db_path(board=slug))) as other_board:
            other_board.row_factory = __import__("sqlite3").Row
            other = kb.create_task(other_board, title="Retained executor in another board", assignee="default",
                                   idempotency_key="linear:issue-one", board=slug)
            claimed = kb.claim_task(other_board, other, claimer="fixture-other-board")
            _set_worker_pid(other_board, other, os.getpid(), run_id=claimed.current_run_id, claim_lock=claimed.claim_lock)
            self.assertTrue(kb.block_task(other_board, other, kind="needs_input", reason="Stop before admission"))
        self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        kb.remove_board(slug, archive=True)
        self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])

    async def test_cleanup_native_birth_validation_refuses_malformed_and_unreadable_witnesses(self):
        from hermes_cli.kanban_db_dispatch import _process_fingerprint
        await self.stopped_issue()
        real = _process_fingerprint(os.getpid())
        self.assertIsNotNone(real)
        epoch, start = real.split("|")
        def bound(pid, fingerprint):
            self.board.execute("UPDATE task_events SET payload=? WHERE task_id=? AND kind='worker_bound'",
                (json.dumps({"pid": pid, "started_at": fingerprint, "claim_lock": self.work_context["claim_lock"]}), self.task_id))
        malformed = [None, True, False, 0, 1, "", "unverified", "|", "|1", "previous-unproved-epoch|1",
                     *(epoch + "|" + value for value in ("", "True", "False", "0", "-1", "nan", "inf", "-inf", "1e9999", "1|2"))]
        for witness in malformed:
            with self.subTest(witness=witness):
                bound(os.getpid(), witness)
                self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        # Invalid identity never becomes a gone-process proof either.
        bound(99999997, "|")
        self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        for pid in (True, False, 0, -1, None, str(os.getpid())):
            with self.subTest(pid=pid):
                bound(pid, real)
                self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        for number in (start, str(float(start)), "+" + start):
            with self.subTest(number=number):
                bound(os.getpid(), epoch + "|" + number)
                self.assertEqual((await self.cleanup_inspected())["reason"], "old_worker_alive")
        bound(os.getpid(), real)
        with patch("hermes_cli.kanban_db_dispatch._process_fingerprint", return_value=None):
            self.assertEqual((await self.cleanup_inspected())["reason"], "worker_identity_unreadable")
        with patch("gateway.drain_control.current_instantiation_epoch", return_value=""):
            self.assertEqual((await self.cleanup_inspected())["reason"], "worker_identity_unreadable")
        # Retained spawn/run records use canonical current-epoch fixture births;
        # the actual native comparator still proves known recycled and gone IDs.
        bound(os.getpid(), self.process_witness(1))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        bound(99999997, self.process_witness(1))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])


    async def test_cleanup_retained_symlink_and_unreadable_checkpoint_are_unknown(self):
        await self.stopped_issue()
        checkpoint = self.home / "processes.json"
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        checkpoint.symlink_to(self.home / "missing-process-records.json")
        self.assertEqual((await self.cleanup_inspected())["reason"], "checkpoint_unreadable")
        checkpoint.unlink()
        checkpoint.write_text("[]")
        original = Path.lstat
        def unreadable(path, *args, **kwargs):
            if path == checkpoint:
                raise PermissionError("finite unreadable checkpoint metadata")
            return original(path, *args, **kwargs)
        with patch.object(Path, "lstat", unreadable):
            self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])

    async def test_cleanup_native_linux_birth_sources_never_prove_live_pid_recycled(self):
        import psutil
        from hermes_cli.kanban_db_dispatch import _process_fingerprint
        from hermes_cli.process_identity import process_birth
        self.assertTrue(psutil.LINUX, "this Linux source-fault fixture requires its actual platform")
        await self.stopped_issue()
        pid = os.getpid()
        direct = _process_fingerprint(pid)
        original = Path.read_text
        def unreadable_stat(path, *args, **kwargs):
            if path == Path("/proc/" + str(pid) + "/stat"):
                raise PermissionError("finite direct-stat read failure")
            return original(path, *args, **kwargs)
        def bound(witness):
            self.board.execute("UPDATE task_events SET payload=? WHERE task_id=? AND kind='worker_bound'",
                (json.dumps({"pid": pid, "started_at": witness, "claim_lock": self.work_context["claim_lock"]}), self.task_id))
        bound(direct)
        with patch.object(Path, "read_text", unreadable_stat):
            fallback = _process_fingerprint(pid)
            self.assertNotEqual(direct, fallback)
            self.assertEqual((await self.cleanup_inspected())["reason"], "old_worker_alive")
        # Persist the actual fallback, then inspect with the actual direct read restored.
        bound(fallback)
        self.assertEqual((await self.cleanup_inspected())["reason"], "old_worker_alive")
        with patch("hermes_cli.process_identity.worker_birth", side_effect=PermissionError("finite unreadable native birth")):
            self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        bound(self.process_witness(1))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        # An honestly old fallback-scale witness can also prove recycling.
        epoch = direct.split("|")[0]
        bound(epoch + "|" + str(round((process_birth(psutil.Process(pid)) - 10) * 100)))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        # Changing the native identity during the independent observation is unknown.
        with patch("hermes_cli.kanban_db_dispatch._process_fingerprint", side_effect=[direct, self.process_witness(1)]):
            self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])


    async def test_cleanup_psutil_identity_errors_are_structured_unknown(self):
        import psutil
        from hermes_cli.kanban_db_dispatch import _process_fingerprint
        await self.stopped_issue()
        pid = os.getpid()
        self.board.execute("UPDATE task_events SET payload=? WHERE task_id=? AND kind='worker_bound'",
            (json.dumps({"pid": pid, "started_at": _process_fingerprint(pid),
                         "claim_lock": self.work_context["claim_lock"]}), self.task_id))
        actual_process = psutil.Process
        for target in ("psutil.Process", "hermes_cli.process_identity.process_birth", "hermes_cli.process_identity.worker_birth"):
            for error in (psutil.AccessDenied(pid), psutil.NoSuchProcess(pid)):
                with self.subTest(target=target, error=type(error).__name__):
                    def fault(*args, **kwargs):
                        # Race only at the independent birth lookup; keep native
                        # liveness probing real so NoSuchProcess is not an earlier
                        # authoritative gone-PID observation.
                        caller = inspect.currentframe().f_back
                        while caller and caller.f_globals.get("__name__") == "unittest.mock":
                            caller = caller.f_back
                        if target == "psutil.Process" and caller.f_code is not cleanup._pid_gone.__code__:
                            return actual_process(*args, **kwargs)
                        raise error
                    with patch(target, side_effect=fault):
                        evidence = await self.cleanup_inspected()
                    self.assertFalse(evidence["quiescence_proven"])
                    self.assertEqual(evidence["state"], "unknown")
                    self.assertEqual(evidence["reason"], "worker_identity_unreadable")
                    self.assertFalse(evidence["permission_granted"])
        self.assertEqual((await self.cleanup_inspected())["reason"], "old_worker_alive")
