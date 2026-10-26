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
from hermes_fleet_linear_plugin import origin, transport


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

    async def local(self, text="Take over ABC-1", *, source="gui", extra=None):
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
        request_id = "origin-local-" + str(len(self.authority.sessions))
        self.sid = local_identity(str(self.home), self.conn.actor.subject, request_id)
        request = {"source": source, "model": "fixture", "cwd": str(self.home), "toolsets": []}
        policy = LocalSessionPolicy(source=source, platform=SURFACES[source], cwd=str(self.home), model="fixture",
            toolsets=(), config_json=json.dumps(self.config), request_json=json.dumps(request),
            terminal_json=json.dumps({"TERMINAL_ENV": "local", "TERMINAL_CWD": str(self.home)}),
            safe_mode=True, ignore_user_config=True)
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
        row = admit_session_input(self.authority.db, epoch=self.authority.epoch,
            principal_id=self.conn.actor.subject, session_id=self.sid, request_id="request-1", payload=payload)
        self.admission = row["admission_id"]
        self.claim()
        self.bind("local", self.sid)

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

