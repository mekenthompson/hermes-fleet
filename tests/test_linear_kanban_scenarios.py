"""Integration scenarios for plugins/linear: a fake Linear over HTTP and a REAL Kanban board.

Needs the pinned Hermes Agent source (and its Python deps) on the path:
    <scenario-venv>/bin/python scripts/run-linear-scenarios.py --agent-source /path/to/hermes-agent
Run it in its own process: older Linear tests stub `agent.*` modules in-process.
General unit discovery skips when Agent source is unavailable. The dedicated CI runner
checks the manifest revision and fails on any skip.
One test per rule in the rebuild plan's flows 1-10.
"""
from __future__ import annotations

import json
import asyncio
from contextlib import contextmanager
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import warnings
from pathlib import Path

from linear_fake_api import OTHER, SELF, Clock, FakeLinear, load_plugin

SOURCE = os.environ.get("HERMES_AGENT_SRC", "")
try:
    if not SOURCE:
        raise ImportError("HERMES_AGENT_SRC is not set")
    os.environ.setdefault("HERMES_HOME", tempfile.mkdtemp(prefix="linear-scenario-home-"))
    sys.path.insert(0, SOURCE)
    warnings.simplefilter("ignore")
    from hermes_cli import kanban_db as kb, kanban_db_dispatch as dispatch  # noqa: E402
    CORE = None
except Exception as exc:  # noqa: BLE001 - any import failure means "no core here"
    CORE = str(exc)

plugin = load_plugin()
from hermes_fleet_linear_plugin.api import LinearAPI  # noqa: E402
from hermes_fleet_linear_plugin.bridge import Bridge, Kanban  # noqa: E402
from hermes_fleet_linear_plugin.store import Store  # noqa: E402
from hermes_fleet_linear_plugin import chat  # noqa: E402

ISSUE = "iss-1"


class Context:
    def __init__(self, session_key: str, session_id: str, generation: int | None = None,
                 profile: str = "alpha") -> None:
        self.session_key, self.session_id = session_key, session_id
        self.run_generation, self.profile = generation, profile


@unittest.skipIf(CORE is not None, f"integration: pinned Agent source unavailable ({CORE})")
class LinearKanbanScenarios(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        (self.dir / "kanban").mkdir(mode=0o700)
        os.environ["HERMES_KANBAN_HOME"] = str(self.dir / "kanban")
        self.clock = Clock()
        self.linear = FakeLinear(self.clock)
        self.addCleanup(self.linear.close)
        self.linear.add_issue(ISSUE, "ABC-1")
        self.injected: list[tuple[str, str]] = []
        self.inject_ok = True
        self.bridge = self.make_bridge()

    def make_bridge(self, settings: dict | None = None) -> Bridge:
        api = LinearAPI(lambda: "synthetic-token", endpoint=self.linear.url, clock=self.clock)
        return Bridge(Store(self.dir / "state.db"), api, Kanban(), profile="alpha", settings=settings or {},
                      inject=self.inject, clock=self.clock)

    def inject(self, key: str, text: str) -> bool:
        self.injected.append((key, text))
        return self.inject_ok

    def test_standalone_delegation_is_eligible_for_real_core_dispatch(self):
        from hermes_cli.profiles import get_profile_dir
        self.bridge.kanban = Kanban(profile="container-label", profile_home=get_profile_dir("default"))
        self.delegate()
        row = self.bridge.store.get(ISSUE)
        task = self.bridge.kanban.get(row["task_id"])
        self.assertEqual(task.assignee, "default")
        self.assertTrue(dispatch._profile_exists_fn()(task.assignee))

    def test_standalone_inbox_binding_creates_one_real_default_executor(self):
        from hermes_cli.profiles import get_profile_dir
        from linear_ingress_fixture import IngressStore, Route
        inbox = self.dir / "bound-ingress.db"
        producer = IngressStore(inbox)
        target = Route("worker-a", "worker-a", "/webhook/worker-a", self.dir / "unused", inbox)
        foreign = Route("worker-b", "worker-b", "/webhook/worker-b", self.dir / "unused", inbox)
        self.bridge = Bridge(self.bridge.store, self.bridge.api,
                             Kanban(profile="default", profile_home=get_profile_dir("default")),
                             profile="default", settings={"ingress_profile": "worker-a"}, clock=self.clock)
        self.linear.set_delegate(ISSUE, {"id": SELF})
        event = json.dumps(self.linear.session_event("created", ISSUE, "bound-session")).encode()
        producer.enqueue(target, "target", event)
        producer.enqueue(foreign, "foreign", event)
        self.bridge.tick(inbox)
        self.assertEqual(len(self.tasks()), 1)
        task = self.bridge.kanban.get(self.bridge.store.get(ISSUE)["task_id"])
        self.assertEqual(task.assignee, "default")
        self.assertTrue(dispatch._profile_exists_fn()(task.assignee))
        with sqlite3.connect(inbox) as db:
            self.assertEqual(dict(db.execute("SELECT delivery_id, status FROM deliveries")),
                             {"target": "imported", "foreign": "pending"})
        self.bridge.tick(inbox)
        self.assertEqual(len(self.tasks()), 1)

    def test_bound_inbox_echo_preserves_existing_default_chat_owner_without_worker(self):
        from hermes_cli.profiles import get_profile_dir
        from linear_ingress_fixture import IngressStore, Route
        inbox = self.dir / "bound-chat-ingress.db"
        producer = IngressStore(inbox)
        route = Route("worker-a", "worker-a", "/webhook/worker-a", self.dir / "unused", inbox)
        self.bridge = Bridge(self.bridge.store, self.bridge.api,
                             Kanban(profile="default", profile_home=get_profile_dir("default")),
                             profile="default", settings={"ingress_profile": "worker-a"}, clock=self.clock)
        owner = Context("owner-key", "owner-transcript", 7, profile="default")
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"}, owner))["ok"])
        self.bridge.flush()
        producer.enqueue(route, "echo", json.dumps(self.linear.session_event("created", ISSUE, "echo-session")).encode())
        self.bridge.tick(inbox)
        row = self.bridge.store.get(ISSUE)
        self.assertEqual((row["origin"], row["owner_ref"], row["run_generation"], row["task_id"]),
                         ("chat", "owner-key", 7, None))
        self.assertEqual(self.tasks(), [])
        with sqlite3.connect(inbox) as db:
            self.assertEqual(db.execute("SELECT status, attempts FROM deliveries").fetchone(), ("imported", 1))

    def test_standalone_chat_uses_default_context_and_retains_stop_generation(self):
        from hermes_cli.profiles import get_profile_dir
        self.bridge.kanban = Kanban(profile="alpha", profile_home=get_profile_dir("default"))
        reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                       Context("chat-owner", "chat-transcript", 7, profile="default")))
        self.assertTrue(reply["ok"], reply)
        row = self.bridge.store.get(ISSUE)
        self.assertEqual(row["origin"], "chat")
        self.assertEqual(row["run_generation"], 7)
        self.assertIsNone(row["task_id"])

    def test_same_executor_name_from_foreign_home_cannot_claim_chat(self):
        from hermes_cli.profiles import get_profile_dir
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        self.bridge.kanban = Kanban(profile="alpha", profile_home=get_profile_dir("default"))
        token = set_hermes_home_override(str(self.dir))
        try:
            reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                           Context("foreign", "foreign-transcript", 7, profile="default")))
        finally:
            reset_hermes_home_override(token)
        self.assertFalse(reply["ok"])
        self.assertIsNone(self.bridge.store.get(ISSUE))

    def test_standalone_chat_without_generation_explains_stop_limit(self):
        from hermes_cli.profiles import get_profile_dir
        self.bridge.kanban = Kanban(profile="alpha", profile_home=get_profile_dir("default"))
        reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                       Context("chat-owner", "chat-transcript", profile="default")))
        self.assertTrue(reply["ok"])
        self.assertIn("Linear Stop cannot interrupt", reply["message"])
        self.assertIsNone(self.bridge.store.get(ISSUE)["run_generation"])

    def test_real_default_api_context_can_track_and_complete_without_a_worker(self):
        from hermes_cli.profiles import get_profile_dir
        from gateway.platforms.api_server import APIServerAdapter
        from gateway.session_context import clear_session_vars
        from tools.registry import _current_tool_invocation_context
        self.bridge.kanban = Kanban(profile="alpha", profile_home=get_profile_dir("default"))
        tokens = APIServerAdapter._bind_api_server_session(
            chat_id="api-chat", session_key="api-owner", session_id="api-transcript", profile="")
        try:
            context = _current_tool_invocation_context()
            self.assertEqual(context.profile, "")
            self.assertEqual(context.platform, "api_server")
            start = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"}, context))
            self.assertTrue(start["ok"], start)
            self.assertIn("Linear Stop cannot interrupt", start["message"])
            self.assertIsNone(self.bridge.store.get(ISSUE)["run_generation"])
            done = json.loads(chat.handle(self.bridge, {
                "action": "done", "issue": "ABC-1", "note": "Verified result",
                "evidence": "https://git.example/org/repo/commit/abc"}, context))
            self.assertTrue(done["ok"], done)
            self.bridge.flush()
            self.assertEqual(self.linear.state(ISSUE), "Done")
            self.assertIsNone(self.bridge.store.get(ISSUE))
            self.assertEqual(self.tasks(), [])
        finally:
            clear_session_vars(tokens)

    def test_blank_api_context_cannot_claim_from_a_foreign_home(self):
        from hermes_cli.profiles import get_profile_dir
        from gateway.platforms.api_server import APIServerAdapter
        from gateway.session_context import clear_session_vars
        from tools.registry import _current_tool_invocation_context
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        self.bridge.kanban = Kanban(profile="alpha", profile_home=get_profile_dir("default"))
        tokens = APIServerAdapter._bind_api_server_session(
            session_key="foreign", session_id="foreign-transcript", profile="")
        override = set_hermes_home_override(self.dir)
        try:
            reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                           _current_tool_invocation_context()))
            self.assertFalse(reply["ok"])
            self.assertIsNone(self.bridge.store.get(ISSUE))
            self.assertEqual(self.tasks(), [])
        finally:
            reset_hermes_home_override(override)
            clear_session_vars(tokens)

    def test_blank_non_api_context_and_named_executor_remain_refused(self):
        from hermes_cli.profiles import get_profile_dir
        from types import SimpleNamespace
        self.bridge.kanban = Kanban(profile="alpha", profile_home=get_profile_dir("default"))
        for platform in ("", "slack", "telegram"):
            context = SimpleNamespace(profile="", platform=platform, session_key="owner", session_id="sid")
            reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"}, context))
            self.assertFalse(reply["ok"])
        self.bridge.kanban.executor_profile = "alpha"
        context = SimpleNamespace(profile="", platform="api_server", session_key="owner", session_id="sid")
        self.assertFalse(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"}, context))["ok"])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.assertEqual(self.tasks(), [])

    def test_named_chat_matches_its_scoped_home_and_retains_generation(self):
        from unittest.mock import patch
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        home = self.dir / "profiles" / "alpha"
        home.mkdir(parents=True)
        (home / "config.yaml").write_text("{}\n")
        with patch.dict(os.environ, {"HERMES_HOME": str(self.dir)}):
            self.bridge.kanban = Kanban(profile="alpha", profile_home=home)
            token = set_hermes_home_override(home)
            try:
                reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("named-owner", "named-transcript", 8, profile="alpha")))
            finally:
                reset_hermes_home_override(token)
        self.assertTrue(reply["ok"], reply)
        self.assertEqual(self.bridge.store.get(ISSUE)["run_generation"], 8)

    def test_desktop_handler_without_local_service_uses_single_gateway_owner(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        from hermes_fleet_linear_plugin import transport
        home = self.dir / "profiles" / "alpha"
        home.mkdir(parents=True)
        (home / "config.yaml").write_text("{}\n")
        class Registration:
            def get_config(self, key, default=None):
                return True if key == "enabled" else default
            def register_tool(self, **kwargs): self.tool = kwargs["handler"]
            def register_hook(self, *args): pass
            def register_profile_service(self, *args): self.service = args
        with patch.dict(os.environ, {"HERMES_HOME": str(self.dir)}):
            self.bridge.kanban = Kanban(profile="alpha", profile_home=home)
            token = set_hermes_home_override(home)
            server = None
            try:
                server = transport.Server(home, "alpha", lambda args, context: chat.handle(self.bridge, args, context),
                                          lambda session: chat.on_turn_end(self.bridge, session))
                client = Registration()
                plugin.register(client)
                context = SimpleNamespace(profile="alpha", platform="api_server", session_key="desktop-owner",
                                          session_id="desktop-transcript", run_generation=12)
                reply = json.loads(client.tool({"action": "start", "issue": "ABC-1"}, context))
                self.assertTrue(reply["ok"], reply)
                row = self.bridge.store.get(ISSUE)
                self.assertEqual((row["owner_ref"], row["run_generation"], row["task_id"]), ("desktop-owner", 12, None))
                with self.bridge.kanban.conn() as conn:
                    self.assertEqual(kb.list_tasks(conn), [])
                foreign = SimpleNamespace(**{**vars(context), "session_key": "other-chat"})
                refusal = json.loads(client.tool({"action": "start", "issue": "ABC-1"}, foreign))
                self.assertFalse(refusal["ok"])
                self.assertEqual(self.bridge.store.get(ISSUE)["owner_ref"], "desktop-owner")
            finally:
                if server is not None: server.close()
                reset_hermes_home_override(token)

    # -- helpers -------------------------------------------------------------
    def deliver(self, event: dict) -> None:
        self.bridge.handle_webhook(event)
        self.bridge.tick()

    def delegate(self, session: str = "s-1", **kw) -> None:
        self.linear.set_delegate(ISSUE, {"id": SELF, "name": "This Agent"})
        self.deliver(self.linear.session_event("created", ISSUE, session, **kw))

    def tasks(self) -> list[tuple[str, str]]:
        with self.bridge.kanban.conn() as conn:
            return [(r["id"], r["status"]) for r in conn.execute("SELECT id, status FROM tasks ORDER BY created_at, id")]

    def task_id(self) -> str:
        return self.bridge.store.get(ISSUE)["task_id"]

    def complete(self, task_id: str, result: str) -> None:
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.complete_task(conn, task_id, result=result))
        self.bridge.tick()

    def chat(self, action: str, session: str = "chat-key", **args) -> dict:
        out = chat.handle(self.bridge, {"action": action, "issue": "ABC-1", **args}, Context(session, session + "-id"))
        self.bridge.tick()
        return json.loads(out)

    def test_chat_mutations_require_the_exact_recorded_owner(self) -> None:
        self.assertTrue(self.chat("start", session="owner-chat")["ok"])

        def snapshot():
            with sqlite3.connect(self.dir / "state.db") as db:
                outbox = db.execute("SELECT id, kind, payload, attempts, next_at, state FROM outbox ORDER BY rowid").fetchall()
            return (
                self.bridge.store.get(ISSUE), tuple(outbox),
                self.linear.issues[ISSUE]["updatedAt"], self.linear.state(ISSUE),
                tuple(json.dumps(row, sort_keys=True) for row in self.linear.comments),
                tuple(json.dumps(row, sort_keys=True) for row in self.linear.activities),
                tuple(json.dumps(row, sort_keys=True) for row in self.linear.project_updates),
                tuple(query for query in self.linear.requests if query.startswith("mutation")),
            )

        actions = {
            "done": {"evidence": "https://docs.example/findings/owner-work"},
            "blocked": {"note": "synthetic cross-session probe"},
            "release": {"note": "synthetic cross-session probe"},
        }
        for action, args in actions.items():
            with self.subTest(action=action, owner="foreign"):
                before = snapshot()
                reply = json.loads(chat.handle(
                    self.bridge, {"action": action, "issue": "ABC-1", **args}, Context("other-chat", "other-chat-id")))
                self.assertFalse(reply["ok"])
                self.assertEqual(snapshot(), before)

            self.bridge.store.update(ISSUE, owner_ref="")
            with self.subTest(action=action, owner="missing"):
                before = snapshot()
                reply = json.loads(chat.handle(
                    self.bridge, {"action": action, "issue": "ABC-1", **args}, Context("owner-chat", "owner-chat-id")))
                self.assertFalse(reply["ok"])
                self.assertEqual(snapshot(), before)
            self.bridge.store.update(ISSUE, owner_ref="owner-chat")

    def types(self) -> list[str]:
        return [a["content"]["type"] for a in self.linear.activities]

    def bind_identity(self) -> None:
        self.bound_actor = SELF
        transport = self.bridge.api.transport

        def bound_transport(url, body, headers):
            query = json.loads(body)["query"]
            if "IdentityBinding" in query:
                self.linear.requests.append("query IdentityBinding { viewer { id } organization { id } }")
                return 200, {}, json.dumps({"data": {
                    "viewer": {"id": self.bound_actor}, "organization": {"id": "fixture-org"},
                }}).encode()
            return transport(url, body, headers)

        self.bridge.api = plugin.BoundLinearAPI(lambda: "synthetic-token", endpoint=self.linear.url,
            identity={"viewer_id": SELF, "organization_id": "fixture-org"}, transport=bound_transport)

    def test_existing_task_resume_refuses_issue_read_outage(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        self.bind_identity()
        self.delegate()
        task = self.task_id()
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.clock.now += 1
        before = self.bridge.store.get(ISSUE)
        self.bridge.api.issue = lambda ref: (_ for _ in ()).throw(LinearError("fixture read outage"))
        self.bridge.handle_webhook(self.linear.session_event("prompted", ISSUE, "s-2", body="continue"))
        self.bridge.tick()
        self.assertEqual(self.tasks(), [(task, "blocked")])
        self.assertEqual(self.bridge.store.get(ISSUE), before)

    def test_existing_task_resume_requires_current_owner_identity_and_scope(self) -> None:
        self.bind_identity()
        self.delegate()
        task = self.task_id()
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.clock.now += 1
        before = self.bridge.store.get(ISSUE)
        for refusal in ("delegate", "unassigned", "actor", "scope"):
            self.bound_actor = OTHER["id"] if refusal == "actor" else SELF
            self.bridge.api.identity.pop("projects", None)
            self.linear.set_delegate(ISSUE, OTHER if refusal == "delegate" else None if refusal == "unassigned"
                                     else {"id": SELF, "name": "This Agent"})
            if refusal == "scope":
                self.bridge.api.identity["projects"] = ["outside-project"]
            with self.subTest(refusal=refusal):
                self.bridge.handle_webhook(self.linear.session_event("prompted", ISSUE, "s-2", body="continue"))
                with self.bridge.kanban.conn() as conn:
                    self.assertIsNone(kb.claim_task(conn, task, claimer="isolated-no-executor"))
                self.assertEqual(self.tasks(), [(task, "blocked")])
                self.assertEqual(self.bridge.store.get(ISSUE), before)
        self.bound_actor = SELF
        self.bridge.api.identity.pop("projects", None)
        self.linear.set_delegate(ISSUE, {"id": SELF, "name": "This Agent"})
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-2", body="authorized continue"))
        self.assertEqual(self.tasks(), [(task, "ready")])

    def test_chat_followup_requires_current_owner_identity_and_scope(self) -> None:
        self.bind_identity()
        self.assertTrue(self.chat("start")["ok"])
        original_issue = self.bridge.api.issue
        before = self.bridge.store.get(ISSUE)
        for refusal in ("delegate", "unassigned", "actor", "scope", "read"):
            self.bridge.api.issue = original_issue
            self.bound_actor = OTHER["id"] if refusal == "actor" else SELF
            self.bridge.api.identity.pop("projects", None)
            self.linear.set_delegate(ISSUE, OTHER if refusal == "delegate" else None if refusal == "unassigned"
                                     else {"id": SELF, "name": "This Agent"})
            if refusal == "scope":
                self.bridge.api.identity["projects"] = ["outside-project"]
            if refusal == "read":
                self.bridge.api.issue = lambda ref: (_ for _ in ()).throw(plugin.LinearError("fixture read outage"))
            with self.subTest(refusal=refusal):
                injected = list(self.injected)
                self.bridge.handle_webhook(self.linear.session_event("prompted", ISSUE, "s-9", body="foreign write"))
                self.assertEqual(self.injected, injected)
                self.assertEqual(self.bridge.store.get(ISSUE), before)
        self.bridge.api.issue = original_issue
        self.bound_actor = SELF
        self.bridge.api.identity.pop("projects", None)
        self.linear.set_delegate(ISSUE, {"id": SELF, "name": "This Agent"})
        self.bridge.handle_webhook(self.linear.session_event("prompted", ISSUE, "s-9", body="authorized follow-up"))
        self.assertTrue(any("authorized follow-up" in body for _, body in self.injected))

    def test_existing_redelegation_refuses_owner_replacement_after_takeover(self) -> None:
        self.bind_identity()
        self.delegate()
        task = self.task_id()
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.clock.now += 1
        before = self.bridge.store.get(ISSUE)
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.handle_webhook(self.linear.session_event("created", ISSUE, "s-2"))
        self.assertEqual(self.tasks(), [(task, "blocked")])
        self.assertEqual(self.bridge.store.get(ISSUE), before)

    def test_chat_recovery_does_not_resume_after_authoritative_takeover(self) -> None:
        self.bind_identity()
        self.assertTrue(self.chat("start")["ok"])
        self.linear.set_delegate(ISSUE, OTHER)
        before = list(self.injected), self.bridge.store.get(ISSUE)
        self.bridge.recover()
        self.assertEqual((self.injected, self.bridge.store.get(ISSUE)), before)

    # -- flows ---------------------------------------------------------------
    def test_delegated_run_reaches_done_with_one_message(self) -> None:
        self.delegate()
        [(task_id, status)] = self.tasks()
        self.assertEqual(status, "ready")
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(self.linear.issues[ISSUE]["delegate"]["id"], SELF)
        self.assertEqual(self.types(), ["thought"])
        with self.bridge.kanban.conn() as conn:
            self.assertIn("Reconcile first", kb.build_worker_context(conn, task_id))
        self.complete(task_id, "Shipped the fix: https://docs.example/fix/7")
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertEqual(self.types(), ["thought", "response"])
        self.assertIn("https://docs.example/fix/7", self.linear.activities[-1]["content"]["body"])
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIsNone(self.bridge.store.get(ISSUE))

    def test_duplicate_and_out_of_order_webhooks(self) -> None:
        from linear_ingress_fixture import IngressStore, Route

        inbox = self.dir / "ingress.db"
        store = IngressStore(inbox)
        route = Route("alpha", "alpha", "/webhook/alpha", self.dir / "secret", inbox)
        self.linear.set_delegate(ISSUE, {"id": SELF, "name": "This Agent"})
        created = json.dumps(self.linear.session_event("created", ISSUE, "s-new")).encode()
        store.enqueue(route, "d-1", created)
        store.enqueue(route, "d-1", created)  # same delivery retried
        store.enqueue(route, "d-2", created)  # same event, new delivery id
        self.bridge.tick(inbox)
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual(self.types(), ["thought"])
        # An older session arriving late must not take the row back or start more work.
        store.enqueue(route, "d-3", json.dumps(self.linear.session_event("created", ISSUE, "s-old", at=-600)).encode())
        self.bridge.tick(inbox)
        self.assertEqual(self.bridge.store.get(ISSUE)["owner_ref"], "s-new")
        self.assertEqual(len(self.tasks()), 1)
        # A follow-up that overtakes its own 'created' event still makes exactly one task.
        self.linear.add_issue("iss-2", "ABC-2")
        self.linear.set_delegate("iss-2", {"id": SELF, "name": "This Agent"})
        self.deliver(self.linear.session_event("prompted", "iss-2", "s-2", body="also check the docs"))
        self.deliver(self.linear.session_event("created", "iss-2", "s-2"))
        self.assertEqual(len(self.tasks()), 2)

    def test_loop_guard_on_our_own_chat_start_echo(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        # Self-delegation fires 'created' at our own webhook (phase 0 finding).
        self.deliver(self.linear.session_event("created", ISSUE, "s-echo", creator=SELF))
        self.deliver(self.linear.session_event("created", ISSUE, "s-human"))
        self.assertEqual(self.tasks(), [])
        self.assertEqual(self.types(), ["response", "response"])
        self.assertTrue(all("Already in progress from Hermes chat" in a["content"]["body"] for a in self.linear.activities))

    def test_chat_closeout_receipt_fences_late_native_echo_and_prompt_across_restart(self):
        from unittest.mock import patch
        for action in ("done", "release"):
            with self.subTest(action=action):
                self.setUp()
                self.assertTrue(self.chat("start")["ok"])
                old_echo = self.linear.session_event("created", ISSUE, "s-echo", creator=None)
                old_prompt = self.linear.session_event("prompted", ISSUE, "s-echo", body="Older instruction")
                self.clock.now += 1
                with patch.object(self.bridge, "flush", return_value=0):
                    self.assertTrue(self.chat(action, evidence="https://example.invalid/merged-result")["ok"])
                if action == "release":
                    self.assertEqual(self.bridge.store.get(ISSUE)["release_pending"], 1)
                else:
                    self.assertIsNone(self.bridge.store.get(ISSUE))
                self.bridge = self.make_bridge()
                self.bridge.recover()
                self.bridge.handle_webhook(old_echo)
                self.bridge.handle_webhook(old_prompt)
                self.assertEqual(self.tasks(), [])
                if action == "release":
                    self.assertEqual(self.bridge.store.get(ISSUE)["release_pending"], 1)
                else:
                    self.assertIsNone(self.bridge.store.get(ISSUE))
                self.bridge.flush()
                self.assertIsNone(self.bridge.store.get(ISSUE))
                if action == "release": self.assertIsNone(self.linear.issues[ISSUE]["delegate"])
                self.bridge.handle_webhook(old_echo)
                self.assertEqual(self.tasks(), [])
                self.clock.now += 1
                if action == "release": self.linear.set_delegate(ISSUE, {"id": SELF})
                self.deliver(self.linear.session_event("prompted", ISSUE, "s-new", body="Explicit new work"))
                self.assertEqual(len(self.tasks()), 1)

    def test_follow_up_is_forwarded_to_the_owner(self) -> None:
        self.delegate()
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", body="Please also update the changelog"))
        with self.bridge.kanban.conn() as conn:
            comments = [c.body for c in kb.list_comments(conn, self.task_id())]
        self.assertTrue(any("update the changelog" in c for c in comments))
        self.assertEqual(len(self.tasks()), 1)
        # Chat-owned work: the follow-up is injected into the owning chat session.
        self.linear.add_issue("iss-2", "ABC-2")
        chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"}, Context("chat-key", "chat-id"))
        self.bridge.flush()  # Work-bearing steering waits for the queued claim to be confirmed.
        self.assertEqual(self.linear.issues["iss-2"]["delegate"]["id"], SELF)
        self.deliver(self.linear.session_event("prompted", "iss-2", "s-9", body="Use the staging data"))
        self.assertIn(("chat-key", "[Linear follow-up on ABC-2] Use the staging data"), self.injected)

    def test_stop_blocks_and_stays_stopped_after_restart(self) -> None:
        self.delegate()
        task_id = self.task_id()
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.assertEqual(self.tasks(), [(task_id, "blocked")])
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertEqual(self.linear.issues[ISSUE]["delegate"]["id"], SELF)
        self.assertIn("Stopped by Pat Example", self.linear.activities[-1]["content"]["body"])
        messages = len(self.linear.bodies(ISSUE))
        # Restart: a fresh bridge over the same state, a dispatcher pass, and ticks.
        self.bridge = self.make_bridge()
        self.bridge.recover()
        with self.bridge.kanban.conn() as conn:
            kb.recompute_ready(conn)
        self.clock.now += 3600
        self.bridge.tick()
        self.assertEqual(self.tasks(), [(task_id, "blocked")])
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertEqual(len(self.linear.bodies(ISSUE)), messages)

    def test_redelegate_after_stop_unblocks_the_same_task(self) -> None:
        self.delegate()
        task_id = self.task_id()
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.clock.now += 60
        self.delegate(session="s-2")
        self.assertEqual(self.tasks(), [(task_id, "ready")])
        self.assertEqual(self.linear.state(ISSUE), "In Progress")

    def test_redelegate_after_done_runs_fresh(self) -> None:
        self.delegate()
        first = self.task_id()
        self.complete(first, "Findings: https://docs.example/findings/1")
        self.clock.now += 60
        self.delegate(session="s-2")
        self.assertEqual(sorted(s for _, s in self.tasks()), ["done", "ready"])
        self.assertNotEqual(self.task_id(), first)

    def _registered_child(self):
        process = subprocess.Popen([sys.executable, "-c", "import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);print('ready',flush=True);time.sleep(120)"],
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.assertEqual(process.stdout.readline().strip(), "ready")
        def cleanup():
            if process.poll() is None: process.kill()
            process.wait(timeout=10)
            process.stdout.close()
        self.addCleanup(cleanup)
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.claim_task(conn, self.task_id()))
            dispatch._set_worker_pid(conn, self.task_id(), process.pid)
        return process

    def test_running_handoff_uses_core_archive_termination(self):
        self.delegate()
        process = self._registered_child()
        # Core escalates the verified worker from TERM to KILL.
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.recheck(force=True)
        process.wait(timeout=10)
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.assertIsNotNone(process.poll())

    def test_blocked_handoff_fences_native_and_chat_until_worker_exit_across_restart(self):
        self.delegate()
        old = self.task_id()
        process = self._registered_child()
        with self.bridge.kanban.conn() as conn:
            kb.block_task(conn, old, reason="waiting for operator", kind="needs_input")
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.recheck(force=True)
        self.assertIsNone(process.poll())
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.bridge = self.make_bridge()
        self.linear.set_delegate(ISSUE, {"id": SELF, "name": "This Agent"})
        event = self.linear.session_event("created", ISSUE, "replacement")
        with self.assertRaisesRegex(Exception, "still exiting"):
            self.bridge.handle_webhook(event)
        self.assertEqual(self.tasks(), [(old, "archived")])
        reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"}, Context("other-chat", "chat")))
        self.assertFalse(reply["ok"])
        self.assertIn("still exiting", reply["message"])
        process.kill(); process.wait(timeout=10)
        self.deliver(event)
        self.assertNotEqual(self.task_id(), old)
        self.assertFalse(self.bridge.store.retired(ISSUE))

    def test_retirement_keeps_spawn_identity_after_core_task_delete(self):
        self.delegate()
        old = self.task_id()
        process = self._registered_child()
        with self.bridge.kanban.conn() as conn:
            kb.block_task(conn, old, reason="parked", kind="needs_input")
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.recheck(force=True)
        with self.bridge.kanban.conn() as conn: kb.delete_archived_task(conn, old)
        self.bridge = self.make_bridge()
        with self.assertRaisesRegex(Exception, "still exiting"): self.bridge.await_retired(ISSUE)
        process.kill(); process.wait(timeout=10)
        self.bridge.await_retired(ISSUE)
        self.assertFalse(self.bridge.store.retired(ISSUE))

    def test_claimed_launch_window_stays_fenced_until_registered_child_exits(self):
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn: self.assertTrue(kb.claim_task(conn, task_id))
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.recheck(force=True)
        self.bridge = self.make_bridge()
        with self.assertRaisesRegex(Exception, "identity is unknown"): self.bridge.await_retired(ISSUE)
        process = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(120)"])
        def cleanup():
            if process.poll() is None: process.kill()
            process.wait(timeout=10)
        self.addCleanup(cleanup)
        with self.bridge.kanban.conn() as conn: dispatch._set_worker_pid(conn, task_id, process.pid)
        with self.assertRaisesRegex(Exception, "still exiting"): self.bridge.await_retired(ISSUE)
        process.kill(); process.wait(timeout=10)
        self.bridge.await_retired(ISSUE)
        self.assertFalse(self.bridge.store.retired(ISSUE))

    def test_board_archive_before_snapshot_and_claim_after_snapshot_hold_replacements(self):
        from unittest.mock import patch
        for interleaving in ("board_archive", "late_claim"):
            with self.subTest(interleaving=interleaving):
                self.setUp(); self.delegate(); old = self.task_id()
                if interleaving == "board_archive":
                    with self.bridge.kanban.conn() as conn:
                        self.assertTrue(kb.claim_task(conn, old))
                        kb.archive_task(conn, old)
                    self.bridge.pump_kanban()
                else:
                    original = self.bridge.kanban.archive
                    def claim_before_archive(task):
                        with self.bridge.kanban.conn() as conn: self.assertTrue(kb.claim_task(conn, task))
                        original(task)
                    self.linear.set_delegate(ISSUE, OTHER)
                    with patch.object(self.bridge.kanban, "archive", side_effect=claim_before_archive):
                        self.bridge.recheck(force=True)
                self.bridge = self.make_bridge(); self.bridge.recover()
                self.linear.set_delegate(ISSUE, {"id": SELF, "name": "This Agent"})
                event = self.linear.session_event("created", ISSUE, "late-registration")
                with self.assertRaisesRegex(Exception, "identity is unknown"): self.bridge.handle_webhook(event)
                reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"}, Context("new-chat", "chat")))
                self.assertFalse(reply["ok"]); self.assertIn("identity is unknown", reply["message"])
                self.assertEqual(self.tasks(), [(old, "archived")])
                process = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(120)"])
                try:
                    with self.bridge.kanban.conn() as conn: dispatch._set_worker_pid(conn, old, process.pid)
                    with self.assertRaisesRegex(Exception, "still exiting"): self.bridge.await_retired(ISSUE)
                    self.assertEqual(self.tasks(), [(old, "archived")])
                    process.kill(); process.wait(timeout=10)
                    self.bridge.await_retired(ISSUE)
                    self.deliver(event)
                    self.assertNotEqual(self.task_id(), old)
                finally:
                    if process.poll() is None: process.kill()
                    process.wait(timeout=10)

    def test_restart_finishes_retirement_committed_before_core_archive(self):
        from unittest.mock import patch
        self.delegate()
        old = self.task_id()
        process = self._registered_child()
        self.linear.set_delegate(ISSUE, OTHER)
        with patch.object(self.bridge.kanban, "archive", side_effect=RuntimeError("crash")):
            with self.assertRaisesRegex(RuntimeError, "crash"): self.bridge.recheck(force=True)
        self.assertTrue(self.bridge.store.retired(ISSUE))
        self.bridge = self.make_bridge()
        self.bridge.recover()
        process.wait(timeout=10)
        self.bridge.tick()
        self.assertEqual(self.bridge.kanban.get(old).status, "archived")
        self.assertIsNone(self.bridge.store.get(ISSUE))

    def test_unreadable_retired_worker_fingerprint_fails_closed(self):
        from unittest.mock import patch
        self.delegate()
        process = self._registered_child()
        with self.bridge.kanban.conn() as conn: kb.block_task(conn, self.task_id(), reason="parked", kind="needs_input")
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.recheck(force=True)
        with patch.object(dispatch, "_process_fingerprint", return_value=None):
            with self.assertRaisesRegex(Exception, "identity is unavailable"): self.bridge.await_retired(ISSUE)
        self.assertTrue(self.bridge.store.retired(ISSUE))
        self.assertFalse(self.bridge.store.put(ISSUE, "chat", "bypass"))

    def test_archive_delete_before_tick_closes_unfinished_and_fences_unknown_identity(self):
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn:
            kb.archive_task(conn, task_id)
            kb.delete_archived_task(conn, task_id)
        self.bridge = self.make_bridge()
        self.bridge.recover()
        self.bridge.tick()
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertIn("unfinished", self.linear.activities[-1]["content"]["body"])
        before = len(self.linear.activities)
        self.bridge.tick()
        self.assertEqual(len(self.linear.activities), before)
        with self.assertRaisesRegex(Exception, "identity is unknown"): self.bridge.await_retired(ISSUE)

    def test_archive_closeout_survives_outage_and_preserves_human_takeover(self):
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn: kb.archive_task(conn, task_id)
        self.bridge.pump_kanban()
        self.assertIsNone(self.bridge.store.get(ISSUE))
        pending = self.bridge.store.pending(ISSUE)
        self.assertEqual([r["kind"] for r in pending], ["status", "activity"])
        self.assertTrue(all(r["payload"]["terminal"] for r in pending))
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertFalse(self.bridge.store.pending(ISSUE))
        self.assertEqual(self.linear.issues[ISSUE]["delegate"], OTHER)

    def test_takeover_by_delegate_change_is_pulled(self) -> None:
        self.delegate()
        task_id = self.task_id()
        self.linear.set_delegate(ISSUE, OTHER)  # no webhook: pull-based
        self.clock.now += 6 * 60
        self.bridge.tick()
        self.assertEqual(self.tasks(), [(task_id, "archived")])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.assertIn("Reassigned to Other Agent", self.linear.comments[-1]["body"])
        self.assertEqual(self.linear.issues[ISSUE]["delegate"], OTHER)

    def test_pending_activity_does_not_hide_takeover(self) -> None:
        self.delegate()
        task_id = self.task_id()
        self.bridge.activity(ISSUE, "s-1", "thought", "still working")
        self.linear.set_delegate(ISSUE, OTHER)
        self.clock.now += 6 * 60
        self.bridge.tick()
        self.assertEqual(self.tasks(), [(task_id, "archived")])
        self.assertIsNone(self.bridge.store.get(ISSUE))

    def test_prompt_older_than_stop_never_unblocks(self) -> None:
        self.delegate()
        task_id = self.task_id()
        self.clock.now += 60
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", body="stale follow-up", at=-120))
        self.assertEqual(self.tasks(), [(task_id, "blocked")])
        self.assertEqual(self.linear.state(ISSUE), "Blocked")

    def test_claimed_completion_reconciles_from_task_after_crash(self) -> None:
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.complete_task(conn, task_id, result="Findings: https://docs.example/findings/2"))
        self.assertEqual(len(self.bridge.kanban.events(task_id, ISSUE)), 1)  # prior process claimed cursor
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertIsNone(self.bridge.store.get(ISSUE))

    def test_claimed_nonterminal_events_recover_after_restart(self) -> None:
        for kind in ("blocked", "gave_up", "unblocked", "archived"):
            with self.subTest(kind=kind):
                self.setUp()
                self.delegate()
                task_id = self.task_id()
                with self.bridge.kanban.conn() as conn:
                    if kind == "gave_up":
                        kb.claim_task(conn, task_id)
                        dispatch._record_task_failure(conn, task_id, "synthetic crash", outcome="crashed",
                                                      failure_limit=1, release_claim=True, end_run=True)
                    elif kind == "unblocked":
                        kb.block_task(conn, task_id, reason="needs review", kind="needs_input")
                        self.bridge.tick()
                        kb.unblock_task(conn, task_id)
                    elif kind == "archived":
                        kb.archive_task(conn, task_id)
                    else:
                        kb.block_task(conn, task_id, reason="needs review", kind="needs_input")
                self.bridge.kanban.events(task_id, ISSUE)  # claimed just before process death
                before = len(self.linear.activities)
                self.bridge = self.make_bridge()
                self.bridge.tick()
                if kind == "archived":
                    self.assertIsNone(self.bridge.store.get(ISSUE))
                else:
                    self.assertEqual(self.linear.state(ISSUE), "In Progress" if kind == "unblocked" else "Blocked")
                    self.assertEqual(len(self.linear.activities), before + (0 if kind == "unblocked" else 1))
                self.bridge.tick()
                self.assertEqual(len(self.linear.activities), before + (0 if kind == "unblocked" else 1))

    def _migrate_old_work_row(self) -> None:
        """Recreate the pre-cursor work table, then let Store upgrade it on restart."""
        with sqlite3.connect(self.bridge.store.path) as db:
            db.execute("CREATE TABLE old_work AS SELECT issue_id, origin, owner_ref, task_id, project_id, "
                       "last_updated_at, linear_session_id, panel_note, stop_requested_at FROM work")
            db.execute("DROP TABLE work")
            db.execute("ALTER TABLE old_work RENAME TO work")
        self.bridge = self.make_bridge()
        self.assertEqual(self.bridge.store.get(ISSUE)["last_event_id"], 0)

    def test_upgraded_work_does_not_replay_old_give_up_after_retry(self) -> None:
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn:
            kb.claim_task(conn, task_id)
            dispatch._record_task_failure(conn, task_id, "old crash", outcome="crashed",
                                          failure_limit=1, release_claim=True, end_run=True)
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.unblock_task(conn, task_id))
        before = len(self.linear.activities)
        self._migrate_old_work_row()
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(len(self.linear.activities), before)
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(len(self.linear.activities), before)

    def test_upgraded_work_keeps_claimed_unprocessed_block(self) -> None:
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.block_task(conn, task_id, reason="new decision", kind="needs_input"))
        self.bridge.kanban.events(task_id, ISSUE)
        self._migrate_old_work_row()
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertEqual(len([a for a in self.linear.activities if a["content"]["type"] == "elicitation"]), 1)
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(len([a for a in self.linear.activities if a["content"]["type"] == "elicitation"]), 1)

    def test_upgraded_work_deduplicates_delivered_current_block(self) -> None:
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.block_task(conn, task_id, reason="needs review", kind="needs_input"))
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        before = len(self.linear.activities)
        self._migrate_old_work_row()
        self.bridge.tick()
        self.assertEqual(len(self.linear.activities), before)
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(len(self.linear.activities), before)

    def test_legacy_sent_alerts_reconcile_by_issue_and_session(self) -> None:
        for kind in ("blocked", "gave_up"):
            with self.subTest(kind=kind):
                self.setUp()
                self.delegate()
                task_id = self.task_id()
                with self.bridge.kanban.conn() as conn:
                    if kind == "blocked":
                        self.assertTrue(kb.block_task(conn, task_id, reason="needs review", kind="needs_input"))
                    else:
                        kb.claim_task(conn, task_id)
                        dispatch._record_task_failure(conn, task_id, "old crash", outcome="crashed",
                                                      failure_limit=1, release_claim=True, end_run=True)
                self.bridge.tick()
                before = len(self.linear.activities)
                with sqlite3.connect(self.bridge.store.path) as db:
                    row = db.execute("SELECT id, payload FROM outbox WHERE kind='activity' ORDER BY rowid DESC LIMIT 1").fetchone()
                    legacy = json.loads(row[1])
                    legacy.pop("task_id")
                    db.execute("UPDATE outbox SET payload=? WHERE id=?", (json.dumps(legacy), row[0]))
                    # Identical text on another session or an explicitly different task is not this task's alert.
                    for changes in ({"session_id": "other-session"}, {"task_id": "other-task"}):
                        other = {**legacy, **changes}
                        db.execute("INSERT INTO outbox (id, kind, payload, next_at, state) VALUES (?, 'activity', ?, 0, 'sent')",
                                   (f"other-{kind}-{len(changes)}-{changes.get('session_id', 'task')}", json.dumps(other)))
                self._migrate_old_work_row()
                self.bridge.tick()
                self.assertEqual(len(self.linear.activities), before)
                self.assertEqual(self.bridge.store.pending(ISSUE), [])
                self.bridge = self.make_bridge()
                self.bridge.tick()
                self.assertEqual(len(self.linear.activities), before)

    def test_pending_legacy_alert_is_not_replayed_after_cursor_upgrade(self) -> None:
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.block_task(conn, task_id, reason="needs review", kind="needs_input"))
        old = self.bridge.store.enqueue("activity", {"issue_id": ISSUE, "session_id": "s-1",
                                                      "content": {"type": "elicitation", "body":
                                                                  "Blocked: needs review. Reply here to unblock."}},
                                        at=self.clock() + 3600)
        before = len(self.linear.activities)
        self._migrate_old_work_row()
        self.bridge.tick()
        pending = [p for p in self.bridge.store.pending(ISSUE) if p["kind"] == "activity"]
        self.assertEqual([p["id"] for p in pending], [old])
        self.assertEqual(len(self.linear.activities), before)
        self.clock.now += 3601
        self.bridge.flush()
        self.assertEqual(len(self.linear.activities), before + 1)

    def test_retryable_failed_legacy_alert_is_not_replayed_after_cursor_upgrade(self) -> None:
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.block_task(conn, task_id, reason="needs review", kind="needs_input"))
        old = self.bridge.store.enqueue("activity", {"issue_id": ISSUE, "session_id": "s-1",
                                                      "content": {"type": "elicitation", "body":
                                                                  "Blocked: needs review. Reply here to unblock."}},
                                        at=self.clock() + 3600)
        with sqlite3.connect(self.bridge.store.path) as db:
            db.execute("UPDATE outbox SET state='failed', attempts=1 WHERE id=?", (old,))
        self._migrate_old_work_row()
        self.bridge.tick()
        with sqlite3.connect(self.bridge.store.path) as db:
            rows = db.execute("SELECT id FROM outbox WHERE kind='activity' AND "
                              "json_extract(payload, '$.issue_id')=? AND "
                              "json_extract(payload, '$.content.type')='elicitation'", (ISSUE,)).fetchall()
        self.assertEqual([row[0] for row in rows], [old])

    def test_legacy_other_session_does_not_hide_unreported_block(self) -> None:
        self.delegate()
        task_id = self.task_id()
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.block_task(conn, task_id, reason="needs review", kind="needs_input"))
        self._migrate_old_work_row()
        other = self.bridge.store.enqueue("activity", {"issue_id": ISSUE, "session_id": "other-session",
                                                        "content": {"type": "elicitation", "body":
                                                                    "Blocked: needs review. Reply here to unblock."}},
                                          at=self.clock())
        self.bridge.store.mark_sent(other, True)
        self.bridge.tick()
        self.assertEqual(len([a for a in self.linear.activities if a["content"]["type"] == "elicitation"]), 1)
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(len([a for a in self.linear.activities if a["content"]["type"] == "elicitation"]), 1)

    def test_chat_terminal_capture_rolls_back_at_each_sql_boundary(self) -> None:
        from unittest.mock import patch
        for action in ("done", "release"):
            for boundary in ("status", "comment", "project_update", "project_update_insert", "ownership_capture"):
                with self.subTest(action=action, boundary=boundary):
                    self.setUp()
                    self.assertTrue(self.chat("start")["ok"])
                    if boundary == "project_update_insert":
                        with sqlite3.connect(self.bridge.store.path) as db:
                            db.execute("DELETE FROM outbox WHERE kind = 'project_update'")
                    before = [(p["id"], p["payload"]) for p in self.bridge.store.pending()]
                    with sqlite3.connect(self.bridge.store.path) as db:
                        if boundary == "ownership_capture":
                            operation = "UPDATE" if action == "release" else "DELETE"
                            db.execute(f"CREATE TRIGGER fail_boundary BEFORE {operation} ON work BEGIN SELECT RAISE(FAIL, 'fault'); END")
                        elif boundary == "project_update":
                            db.execute("CREATE TRIGGER fail_boundary BEFORE UPDATE ON outbox WHEN NEW.kind = 'project_update' "
                                       "BEGIN SELECT RAISE(FAIL, 'fault'); END")
                        elif boundary == "project_update_insert":
                            db.execute("CREATE TRIGGER fail_boundary BEFORE INSERT ON outbox WHEN NEW.kind = 'project_update' "
                                       "BEGIN SELECT RAISE(FAIL, 'fault'); END")
                        else:
                            db.execute(f"CREATE TRIGGER fail_boundary BEFORE INSERT ON outbox WHEN NEW.kind = '{boundary}' "
                                       "BEGIN SELECT RAISE(FAIL, 'fault'); END")
                    with patch.object(self.bridge, "flush", return_value=0), self.assertRaises(sqlite3.IntegrityError):
                        self.chat(action, evidence="https://docs.example/findings/1")
                    self.bridge = self.make_bridge()
                    self.assertIsNotNone(self.bridge.store.get(ISSUE))
                    self.assertEqual([(p["id"], p["payload"]) for p in self.bridge.store.pending()], before)
                    with sqlite3.connect(self.bridge.store.path) as db:
                        db.execute("DROP TRIGGER fail_boundary")
                    with patch.object(self.bridge, "flush", return_value=0):
                        self.assertTrue(self.chat(action, evidence="https://docs.example/findings/1")["ok"])
                    self.bridge = self.make_bridge()
                    self.bridge.tick()
                    self.assertIsNone(self.bridge.store.get(ISSUE))
                    self.assertEqual(self.linear.state(ISSUE), "Done" if action == "done" else "Blocked")
                    self.assertTrue(any("Evidence:" in body if action == "done" else "Released unfinished" in body
                                        for body in self.linear.bodies(ISSUE)))

    def test_failed_chat_stop_injection_keeps_work_and_reports_uncertainty(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        self.inject_ok = False
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.assertIsNotNone(self.bridge.store.get(ISSUE))
        self.assertNotIn("Stopped by", " ".join(self.linear.bodies(ISSUE)))

    def test_unbound_chat_stop_reports_unsupported_without_core_target(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "linear-s", creator=SELF))
        self.deliver(self.linear.session_event("prompted", ISSUE, "linear-s", signal="stop"))
        self.assertEqual(self.bridge.store.stop_intents(), [])
        self.assertIn("cancellation is unsupported", self.linear.activities[-1]["content"]["body"].lower())
        self.assertFalse(self.chat("done", evidence="https://docs.example/findings/1")["ok"])

    def test_bound_chat_stop_runs_on_gateway_loop_after_durable_capture(self) -> None:
        from hermes_fleet_linear_plugin import process_chat_stops
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-id", 7)))["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "linear-s", creator=SELF))
        event = self.linear.session_event("prompted", ISSUE, "linear-s", signal="stop", activity_id="stop-a")
        loop_thread = __import__("threading").get_ident()
        class Gateway:
            calls = 0
            async def get_chat_run_stop_observation(inner, **kwargs):
                self.assertEqual(__import__("threading").get_ident(), loop_thread)
                return {"status": "unknown", "worker_completion": "unknown"} if inner.calls == 0 else {
                    "status": "observed", "stop_status": "accepted", "worker_completion": "completed"}
            async def request_chat_run_stop(inner, **kwargs):
                inner.calls += 1
                self.assertEqual(__import__("threading").get_ident(), loop_thread)
                self.assertEqual(kwargs["session_key"], "chat-key")
                self.assertEqual(kwargs["expected_run_generation"], 7)
                self.assertEqual(kwargs["profile_home"], str(self.dir))
                self.assertEqual(self.bridge.store.stop_intents()[0]["status"], "requested")
                return {"status": "accepted", "worker_completion": "pending"}
        runtime = type("Runtime", (), {"gateway": Gateway(), "profile_home": str(self.dir)})()
        asyncio.run(asyncio.to_thread(self.deliver, event))  # webhook/tick worker differs from gateway loop
        self.assertEqual(len(self.bridge.store.stop_intents()), 1)
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.bridge.tick()
        self.assertIn("worker pending; external effects unknown", self.linear.activities[-1]["content"]["body"])
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.assertEqual(self.bridge.store.stop_intents()[0]["worker_completion"], "completed")
        self.bridge.tick()
        self.assertIn("worker completed; external effects unknown", self.linear.activities[-1]["content"]["body"])
        self.deliver(event)
        self.assertEqual(runtime.gateway.calls, 1)
        self.assertEqual(len(self.bridge.store.stop_intents()), 1)
        self.assertEqual(len([a for a in self.linear.activities if "Stop request accepted" in a["content"]["body"]]), 1)

    def test_stop_crash_before_receipt_does_not_claim_success_or_retarget(self) -> None:
        from hermes_fleet_linear_plugin import process_chat_stops
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-id", 7)))["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "linear-s", creator=SELF))
        self.deliver(self.linear.session_event("prompted", ISSUE, "linear-s", signal="stop"))
        class CrashGateway:
            async def get_chat_run_stop_observation(inner, **kwargs):
                return {"status": "unknown"}
            async def request_chat_run_stop(inner, **kwargs):
                self.assertEqual(kwargs["expected_run_generation"], 7)
                raise RuntimeError("response lost after request")
        runtime = type("Runtime", (), {"gateway": CrashGateway(), "profile_home": str(self.dir)})()
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.assertEqual(self.bridge.store.stop_intents()[0]["status"], "requested")
        self.assertFalse(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        self.bridge = self.make_bridge()
        class RestartGateway(CrashGateway):
            async def request_chat_run_stop(inner, **kwargs):
                self.assertEqual(kwargs["expected_run_generation"], 7)
                return {"status": "stale", "worker_completion": "unknown"}
        runtime.gateway = RestartGateway()
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.bridge.tick()
        self.assertEqual(self.bridge.store.stop_intents()[0]["status"], "stale")
        self.assertIn("external effects unknown", self.linear.activities[-1]["content"]["body"])

    def test_accepted_pending_becomes_visible_unknown_after_restart(self) -> None:
        from hermes_fleet_linear_plugin import process_chat_stops
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-id", 7)))["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "linear-s", creator=SELF))
        self.deliver(self.linear.session_event("prompted", ISSUE, "linear-s", signal="stop"))
        class Gateway:
            async def get_chat_run_stop_observation(inner, **kwargs):
                return {"status": "unknown", "worker_completion": "unknown"}
            async def request_chat_run_stop(inner, **kwargs):
                return {"status": "accepted", "worker_completion": "pending"}
        runtime = type("Runtime", (), {"gateway": Gateway(), "profile_home": str(self.dir)})()
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.bridge.tick()
        self.assertIn("worker pending", self.linear.activities[-1]["content"]["body"])
        self.bridge = self.make_bridge()
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.bridge.tick()
        self.assertEqual(self.bridge.store.stop_intents()[0]["worker_completion"], "unknown")
        self.assertIn("completion is now unknown", self.linear.activities[-1]["content"]["body"])
        before = len(self.linear.activities)
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.bridge.tick()
        self.assertEqual(len(self.linear.activities), before)

    def test_stop_ack_lost_response_retries_same_linear_activity_id(self) -> None:
        from hermes_fleet_linear_plugin import process_chat_stops
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-id", 7)))["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "linear-s", creator=SELF))
        self.deliver(self.linear.session_event("prompted", ISSUE, "linear-s", signal="stop"))
        class Gateway:
            async def get_chat_run_stop_observation(inner, **kwargs):
                return {"status": "unknown"}
            async def request_chat_run_stop(inner, **kwargs):
                return {"status": "accepted", "worker_completion": "pending"}
        runtime = type("Runtime", (), {"gateway": Gateway(), "profile_home": str(self.dir)})()
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.linear.lose_next_response = True
        self.bridge.tick()
        first = [a for a in self.linear.activities if "Stop request accepted" in a["content"]["body"]]
        self.assertEqual(len(first), 1)
        self.clock.now += 61
        self.bridge.tick()
        second = [a for a in self.linear.activities if "Stop request accepted" in a["content"]["body"]]
        self.assertEqual(second, first)
        self.assertFalse(any(r["kind"] == "activity" for r in self.bridge.store.pending(ISSUE)))

    def test_new_chat_turn_does_not_retarget_saved_stop_and_wrong_session_is_ignored(self) -> None:
        from hermes_fleet_linear_plugin import process_chat_stops
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-id", 7)))["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "linear-s", creator=SELF))
        self.deliver(self.linear.session_event("prompted", ISSUE, "other-s", signal="stop"))
        self.assertEqual(self.bridge.store.stop_intents(), [])
        self.deliver(self.linear.session_event("prompted", ISSUE, "linear-s", signal="stop", activity_id="stop-a"))
        self.assertEqual(self.bridge.store.stop_intents()[0]["run_generation"], 7)
        self.assertFalse(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                                Context("chat-key", "chat-id", 7)))["ok"])
        self.assertNotEqual(self.bridge.store.get(ISSUE)["stop_requested_at"], 0)
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-id", 8)))["ok"])
        self.assertEqual(self.bridge.store.get(ISSUE)["run_generation"], 8)
        self.assertFalse(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                                Context("chat-key", "chat-id", 9, "wrong")))["ok"])
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-id", 7)))["ok"])
        self.assertEqual(self.bridge.store.get(ISSUE)["run_generation"], 8)
        class Gateway:
            async def get_chat_run_stop_observation(inner, **kwargs):
                return {"status": "unknown"}
            async def request_chat_run_stop(inner, **kwargs):
                self.assertEqual(kwargs["expected_run_generation"], 7)
                return {"status": "stale", "worker_completion": "unknown"}
        runtime = type("Runtime", (), {"gateway": Gateway(), "profile_home": str(self.dir)})()
        asyncio.run(process_chat_stops(self.bridge, runtime))
        self.assertEqual(self.bridge.store.stop_intents()[0]["status"], "stale")

    def test_chat_stop_cannot_be_marked_done_after_restart_until_newer_prompt(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.assertFalse(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        self.bridge = self.make_bridge()
        self.bridge.recover()
        self.assertFalse(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        self.clock.now += 60
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", body="Resume this work"))
        self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])

    def test_restart_comment_is_fenced_if_chat_issue_changes_before_flush(self) -> None:
        for change in ("takeover", "closure"):
            with self.subTest(change=change):
                self.setUp()
                self.assertTrue(self.chat("start")["ok"])
                self.inject_ok = False
                self.bridge = self.make_bridge()
                self.bridge.recover()
                self.assertIsNone(self.bridge.store.get(ISSUE))
                comment = next(p for p in self.bridge.store.pending(ISSUE) if p["kind"] == "comment")
                self.assertEqual(comment["payload"].get("session_key"), "chat-key")
                if change == "takeover":
                    self.linear.set_delegate(ISSUE, OTHER)
                else:
                    self.linear.set_state(ISSUE, "Done")
                self.bridge.flush()
                self.assertFalse(any("Interrupted by a restart" in c["body"] for c in self.linear.comments))

    def test_project_update_is_once_even_after_first_was_sent(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        self.clock.now += 31 * 60
        self.bridge.tick()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.chat("done", evidence="https://docs.example/findings/1")
        self.clock.now += 31 * 60
        self.bridge.tick()
        self.assertEqual(len(self.linear.project_updates), 1)

    def test_restart_mid_run_resumes_then_breaker_blocks(self) -> None:
        self.delegate()
        task_id = self.task_id()
        for attempt in range(3):
            with self.bridge.kanban.conn() as conn:
                self.assertIsNotNone(kb.claim_task(conn, task_id))
                tripped = dispatch._record_task_failure(conn, task_id, "worker killed by restart", outcome="crashed",
                                                        failure_limit=3, release_claim=True, end_run=True)
            self.bridge.tick()
            if attempt < 2:  # respawned automatically; Linear keeps showing In Progress
                self.assertFalse(tripped)
                self.assertEqual(self.tasks(), [(task_id, "ready")])
                self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertTrue(tripped)
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertEqual(self.types(), ["thought", "error"])
        self.assertIn("repeated failed attempts", self.linear.activities[-1]["content"]["body"])

    def test_queued_claim_never_overrides_a_later_human_close(self) -> None:
        from unittest.mock import patch
        with patch.object(self.bridge, "flush", return_value=0):
            self.delegate()  # verified issue read, but claim remains durably queued
        self.linear.down = True
        self.clock.now += 600
        self.linear.set_state(ISSUE, "Canceled")  # a human cancels while our claim is still queued
        self.linear.down = False
        self.clock.now += 3600
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "Canceled")
        self.assertEqual([s for _, s in self.tasks()], ["archived"])
        self.assertIsNone(self.bridge.store.get(ISSUE))

    def test_queued_claim_never_undoes_a_later_delegate_removal(self) -> None:
        from unittest.mock import patch
        with patch.object(self.bridge, "flush", return_value=0):
            self.delegate()  # verified issue read, but claim remains durably queued
        self.linear.down = True
        self.clock.now += 600
        self.linear.set_delegate(ISSUE, None)  # a human takes the agent off while our claim is queued
        self.linear.down = False
        self.clock.now += 3600
        self.bridge.tick()
        self.assertIsNone(self.linear.issues[ISSUE]["delegate"])
        self.assertEqual([s for _, s in self.tasks()], ["archived"])
        self.assertIsNone(self.bridge.store.get(ISSUE))

    def test_late_stop_for_an_older_session_does_not_stop_newer_work(self) -> None:
        self.delegate()
        self.clock.now += 60
        self.delegate(session="s-2")
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop", at=-120))
        self.assertEqual([s for _, s in self.tasks()], ["ready"])
        self.assertEqual(self.linear.state(ISSUE), "In Progress")

    def test_start_fails_closed_when_viewer_identity_fails_before_kanban_creation(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError

        self.linear.set_delegate(ISSUE, OTHER)
        original_me = self.bridge.me
        calls = 0

        def one_viewer_failure():
            nonlocal calls
            calls += 1
            return None if calls == 2 else original_me()

        self.bridge.me = one_viewer_failure
        event = self.linear.session_event("created", ISSUE, "foreign-session", creator="human-1")
        with self.assertRaisesRegex(LinearError, "viewer identity"):
            self.bridge.handle_webhook(event)

        self.assertEqual(self.tasks(), [])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.assertEqual(self.linear.issues[ISSUE]["delegate"]["id"], OTHER["id"])
        self.assertEqual(self.linear.state(ISSUE), "Todo")
        self.assertEqual(self.bridge.store.pending(), [])
        self.assertEqual([query for query in self.linear.requests if query.startswith("mutation")], [])

    def test_issue_read_outage_does_not_admit_stale_delegation(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError

        original_issue = self.bridge.api.issue
        event = self.linear.session_event("created", ISSUE, "s-retry")
        self.bridge.api.issue = lambda ref: (_ for _ in ()).throw(LinearError("issue read unavailable"))
        with self.assertRaises(LinearError):
            self.bridge.handle_webhook(event)
        self.assertEqual(self.tasks(), [])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.bridge.api.issue = original_issue
        self.deliver(event)
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual(self.linear.state(ISSUE), "In Progress")

    def test_revoked_refresh_token_alerts_chat_on_first_failed_write(self) -> None:
        from hermes_fleet_linear_plugin.oauth import ReauthorizationRequired
        self.assertTrue(self.chat("start")["ok"])
        def revoked():
            raise ReauthorizationRequired("reauthorize this profile's app")
        self.bridge.api.token = revoked
        self.bridge.comment(ISSUE, "queued progress")
        self.bridge.tick()
        alerts = [message for key, message in self.injected if key == "chat-key" and "reauthoriz" in message.lower()]
        self.assertEqual(len(alerts), 1)

    def test_revoked_token_on_terminal_row_alerts_after_work_deletion(self) -> None:
        from hermes_fleet_linear_plugin.oauth import ReauthorizationRequired
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        with patch.object(self.bridge, "flush", return_value=0):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        def revoked():
            raise ReauthorizationRequired("reauthorize this profile's app")
        self.bridge.api.token = revoked
        self.bridge.tick()
        alerts = [message for key, message in self.injected if key == "chat-key" and "reauthoriz" in message.lower()]
        self.assertEqual(len(alerts), 1)

    def test_parked_inbox_failure_posts_one_issue_alert(self) -> None:
        import sqlite3
        from unittest.mock import patch
        from linear_ingress_fixture import IngressStore, Route
        inbox = self.dir / "parked-ingress.db"
        store = IngressStore(inbox)
        route = Route("alpha", "alpha", "/webhook/alpha", self.dir / "secret", inbox)
        event = self.linear.session_event("created", ISSUE, "s-broken")
        store.enqueue(route, "delivery-broken", json.dumps(event).encode())
        with sqlite3.connect(inbox) as db:
            db.execute("UPDATE deliveries SET received_at = ?", (int(self.clock() - 86_401),))
        with patch.object(self.bridge, "handle_webhook", side_effect=RuntimeError("synthetic importer error")):
            self.bridge.tick(inbox)
            self.bridge.tick(inbox)
        with sqlite3.connect(inbox) as db:
            self.assertEqual(db.execute("SELECT status FROM deliveries").fetchone()[0], "failed")
        self.assertEqual(len([c for c in self.linear.comments if "parked" in c["body"]]), 1)

    def test_activation_cutoff_discards_delayed_valid_delivery_before_work_or_writes(self) -> None:
        import sqlite3
        from linear_ingress_fixture import IngressStore, Route

        cutoff_ms = int(self.clock() * 1000)
        self.bridge = self.make_bridge({"activation_cutoff_ms": cutoff_ms})
        inbox = self.dir / "fresh-ingress.db"
        ingress = IngressStore(inbox)
        route = Route("alpha", "alpha", "/webhook/alpha", self.dir / "secret", inbox)
        delayed = self.linear.session_event("created", ISSUE, "s-delayed", at=-1)
        ingress.enqueue(route, "delivery-delayed", json.dumps(delayed).encode())

        self.bridge.tick(inbox)

        db = sqlite3.connect(inbox)
        try:
            self.assertEqual(db.execute("SELECT status FROM deliveries").fetchone()[0], "imported")
        finally:
            db.close()
        self.assertEqual(self.tasks(), [])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.assertEqual(self.bridge.store.pending(), [])
        self.assertEqual(self.linear.requests, [])

    def test_activation_cutoff_rejects_undated_signed_delivery_without_any_effect(self) -> None:
        import sqlite3
        from linear_ingress_fixture import IngressStore, Route

        cutoff_ms = int(self.clock() * 1000)
        self.bridge = self.make_bridge({"activation_cutoff_ms": cutoff_ms})
        inbox = self.dir / "fresh-ingress.db"
        ingress = IngressStore(inbox)
        route = Route("alpha", "alpha", "/webhook/alpha", self.dir / "secret", inbox)
        event = self.linear.session_event("created", ISSUE, "s-undated", at=1)
        for field in ("agentActivity", "agentSession", "data"):
            if isinstance(event.get(field), dict):
                event[field].pop("createdAt", None)
                event[field].pop("updatedAt", None)
        event.pop("createdAt", None)
        # Valid signed delivery time proves freshness, not when the source action happened.
        event["webhookTimestamp"] = cutoff_ms + 1000
        ingress.enqueue(route, "delivery-undated", json.dumps(event).encode())
        self.bridge.tick(inbox)
        with sqlite3.connect(inbox) as db:
            self.assertEqual(db.execute("SELECT status FROM deliveries").fetchone()[0], "imported")
        self.assertEqual(self.tasks(), [])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.assertEqual(self.bridge.store.pending(), [])
        self.assertEqual(self.linear.requests, [])

    def test_activation_cutoff_admits_equal_boundary_and_survives_duplicate_restart(self) -> None:
        import sqlite3
        from linear_ingress_fixture import IngressStore, Route

        cutoff_ms = int(self.clock() * 1000)
        self.bridge = self.make_bridge({"activation_cutoff_ms": cutoff_ms})
        inbox = self.dir / "fresh-ingress.db"
        ingress = IngressStore(inbox)
        route = Route("alpha", "alpha", "/webhook/alpha", self.dir / "secret", inbox)
        event = self.linear.session_event("created", ISSUE, "s-boundary", at=0)
        payload = json.dumps(event).encode()
        ingress.enqueue(route, "delivery-boundary", payload)

        self.bridge.tick(inbox)
        self.assertEqual(len(self.tasks()), 1)
        writes_after_first_admission = [query for query in self.linear.requests if query.startswith("mutation")]

        self.bridge = self.make_bridge()  # The persisted cutoff remains active without config.
        ingress.enqueue(route, "delivery-boundary-duplicate", payload)
        self.bridge.tick(inbox)

        db = sqlite3.connect(inbox)
        try:
            statuses = [row[0] for row in db.execute("SELECT status FROM deliveries ORDER BY delivery_id")]
        finally:
            db.close()
        self.assertEqual(statuses, ["imported", "imported"])
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual([query for query in self.linear.requests if query.startswith("mutation")],
                         writes_after_first_admission)

    def test_activation_cutoff_requires_positive_integer_and_refuses_existing_work(self) -> None:
        cutoff_ms = int(self.clock() * 1000)
        for invalid in (0, -1, True, 1.5, str(cutoff_ms)):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.make_bridge({"activation_cutoff_ms": invalid})

        self.bridge.store.put(ISSUE, "kanban", "legacy-task", task_id="old-task")
        with self.assertRaisesRegex(ValueError, "existing work"):
            self.make_bridge({"activation_cutoff_ms": cutoff_ms})
        self.assertEqual(self.bridge.store.get(ISSUE)["task_id"], "old-task")
        self.assertEqual(self.linear.requests, [])

    def test_activation_cutoff_cannot_change_after_it_is_persisted(self) -> None:
        cutoff_ms = int(self.clock() * 1000)
        self.bridge = self.make_bridge({"activation_cutoff_ms": cutoff_ms})

        with self.assertRaisesRegex(ValueError, "cannot change"):
            self.make_bridge({"activation_cutoff_ms": cutoff_ms + 1})

    def test_day_long_outage_catches_up_without_duplicates(self) -> None:
        self.delegate()  # existing authorized execution survives an API outage
        self.linear.down = True
        self.assertEqual(len(self.tasks()), 1)
        task_id = self.task_id()
        self.complete(task_id, "Deployed and checked: https://status.example/check/1")
        for _ in range(23):
            self.clock.now += 3600
            self.bridge.tick()
        self.assertEqual(self.types(), ["thought"])  # no new effects during outage
        self.linear.down = False
        original_apply = self.linear._apply
        lose_comment = True
        def lose_one_comment_response(query, variables):
            nonlocal lose_comment
            if lose_comment and "commentCreate" in query:
                self.linear.lose_next_response = True
                lose_comment = False
            return original_apply(query, variables)
        self.linear._apply = lose_one_comment_response  # idempotent create lands but response is lost
        for _ in range(6):
            self.clock.now += 3600
            self.bridge.tick()
        self.assertEqual(self.types(), ["thought", "response"])
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertEqual(self.bridge.store.pending(), [])

    def test_refused_terminal_mutation_keeps_recovery_rows_and_truthful_receipt(self) -> None:
        from hermes_fleet_linear_plugin.store import Store
        self.assertTrue(self.chat("start")["ok"])
        original = self.linear._apply
        def rejected(query, variables):
            for mutation in ("issueUpdate", "commentCreate", "projectUpdateCreate"):
                if mutation in query:
                    return 200, {"data": {mutation: {"success": False}}}
            return original(query, variables)
        self.linear._apply = rejected
        result = self.chat("done", evidence="https://docs.example/findings/1")
        self.assertTrue(result["ok"])  # receipt acknowledges durable local capture only
        self.assertIn("queued", result["message"])
        self.assertIn("not yet confirmed", result["message"])
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        with self.bridge.store._tx() as db:
            rows = db.execute("SELECT id, state, payload FROM outbox WHERE kind='status'").fetchall()
        terminal = [r for r in rows if json.loads(r["payload"]).get("terminal")][0]
        self.assertEqual(terminal["state"], "failed")
        self.assertFalse(json.loads(terminal["payload"]).get("applied"))
        status_id = terminal["id"]
        self.bridge.store = Store(self.dir / "state.db")
        self.assertFalse(self.bridge.store.terminal_status_applied(status_id))
        self.assertIsNotNone(self.bridge.store.outbox_row(status_id))
        self.clock.now += self.bridge.quiet
        self.bridge.flush()
        self.assertEqual(self.linear.comments, [])
        self.assertEqual(self.linear.project_updates, [])
        self.assertTrue(any("has not accepted" in message for _, message in self.injected))

    def test_give_up_after_a_day_is_loud_and_retried_later(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        done = chat.handle(self.bridge, {"action": "done", "issue": "ABC-1", "note": "Merged",
                                         "evidence": "https://git.example/org/repo/commit/abc"},
                           Context("chat-key", "chat-key-id"))
        self.assertTrue(json.loads(done)["ok"])
        self.linear.down = True  # Linear goes away before the writes are delivered
        with self.assertLogs("linear", "ERROR"):
            for _ in range(26):
                self.clock.now += 3600
                self.bridge.tick()
        self.assertTrue(any("has not accepted" in text for _, text in self.injected))
        self.linear.down = False
        self.linear.add_issue("iss-2", "ABC-2")
        chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"}, Context("chat-key", "chat-id"))
        self.bridge.tick()  # a successful write revives the given-up rows
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertEqual(len([c for c in self.linear.comments if c["issueId"] == ISSUE]), 1)

    def test_lost_terminal_status_response_reports_uncertainty_once(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        self.linear.lose_next_response = True
        self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.clock.now += 61
        self.bridge.tick()
        self.bridge.tick()
        alerts = [message for key, message in self.injected if key == "chat-key" and "prior status attempt failed" in message]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(self.linear.comments, [])
        self.assertEqual(self.linear.project_updates, [])

    def test_terminal_write_alert_retains_chat_destination_after_work_deletion(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        with patch.object(self.bridge, "flush", return_value=0):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        status = next(p for p in self.bridge.store.pending(ISSUE) if p["kind"] == "status" and
                      p["payload"]["state"] == "done")
        self.bridge._loud(status, LinearError("synthetic outage"))
        self.assertTrue(any(key == "chat-key" and "has not accepted" in message for key, message in self.injected))

    def test_terminal_write_alert_retains_kanban_destination_after_work_deletion(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        self.delegate()
        task_id = self.task_id()
        with patch.object(self.bridge, "flush", return_value=0):
            self.complete(task_id, "Findings: https://docs.example/findings/1")
        status = next(p for p in self.bridge.store.pending(ISSUE) if p["kind"] == "status" and
                      p["payload"]["state"] == "done")
        self.bridge._loud(status, LinearError("synthetic outage"))
        with self.bridge.kanban.conn() as conn:
            comments = [c.body for c in kb.list_comments(conn, task_id)]
        self.assertTrue(any("has not accepted" in body for body in comments))

    def test_done_without_evidence_is_unfinished(self) -> None:
        self.delegate()
        self.complete(self.task_id(), "All good, trust me")
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertEqual(self.types(), ["thought", "error"])
        self.assertIn("unfinished", self.linear.activities[-1]["content"]["body"])

    def test_unconfigured_kanban_pr_requires_exact_head_acceptance(self) -> None:
        from unittest.mock import patch
        self.delegate()
        with patch("hermes_fleet_linear_plugin.bridge.pr_acceptance", return_value=False) as acceptance:
            self.complete(self.task_id(), "PR: https://github.com/example/repo/pull/7")
        acceptance.assert_called_once_with("https://github.com/example/repo/pull/7")
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertIn("PR acceptance", self.linear.activities[-1]["content"]["body"])

    def test_chat_pr_requires_exact_head_acceptance_before_deleting_work(self) -> None:
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        with patch("hermes_fleet_linear_plugin.bridge.pr_acceptance", return_value=False) as acceptance:
            result = self.chat("done", evidence="https://github.com/example/repo/pull/8")
        acceptance.assert_called_once_with("https://github.com/example/repo/pull/8")
        self.assertFalse(result["ok"])
        self.assertIsNotNone(self.bridge.store.get(ISSUE))
        self.assertNotEqual(self.linear.state(ISSUE), "Done")

    def test_exact_pr_contract_is_rechecked_after_core_completion(self) -> None:
        from unittest.mock import patch
        url = "https://github.com/example/repo/pull/8"
        with patch("hermes_fleet_linear_plugin.bridge.pr_acceptance", return_value=False) as acceptance:
            self.assertFalse(self.bridge.accepted_evidence([url], contract=url))
        acceptance.assert_called_once_with(url)

    def test_sentence_period_after_pr_link_does_not_hide_acceptance_check(self) -> None:
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        with patch("hermes_fleet_linear_plugin.bridge.pr_acceptance", return_value=False) as acceptance:
            result = self.chat("done", evidence="PR: https://github.com/example/repo/pull/8.")
        acceptance.assert_called_once_with("https://github.com/example/repo/pull/8")
        self.assertFalse(result["ok"])

    def test_unverified_other_host_pr_links_cannot_mark_chat_done(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        for url in ("https://gitlab.com/example/repo/-/merge_requests/7",
                    "https://bitbucket.org/example/repo/pull-requests/7",
                    "https://dev.azure.com/acme/project/_git/repo/pullrequest/7"):
            with self.subTest(url=url):
                self.assertFalse(self.chat("done", evidence=url)["ok"])
                self.assertIsNotNone(self.bridge.store.get(ISSUE))

    def test_unverified_azure_pr_cannot_mark_kanban_done(self) -> None:
        self.delegate()
        self.complete(self.task_id(), "https://dev.azure.com/acme/project/_git/repo/pullrequest/7")
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertIn("PR acceptance", self.linear.activities[-1]["content"]["body"])

    def test_explicit_chat_resume_fences_equal_timestamp_duplicate_stop(self) -> None:
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-key-id", 7)))["ok"])
        stop = self.linear.session_event("prompted", ISSUE, "s-1", signal="stop")
        self.deliver(stop)
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("chat-key", "chat-key-id", 8)))["ok"])
        self.deliver(stop)
        self.assertEqual(self.bridge.store.get(ISSUE)["stop_requested_at"], 0)
        self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])

    def test_legacy_stop_cannot_be_resumed_from_an_unverified_chat_turn(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.assertFalse(self.chat("start")["ok"])
        self.assertFalse(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                                Context("chat-key", "chat-key-id", 9)))["ok"])
        self.assertFalse(self.chat("done", evidence="https://docs.example/findings/1")["ok"])

    def test_prompt_chat_resume_fences_equal_timestamp_duplicate_stop(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        stop = self.linear.session_event("prompted", ISSUE, "s-1", signal="stop")
        self.deliver(stop)
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", body="Resume work"))
        self.deliver(stop)
        self.assertEqual(self.bridge.store.get(ISSUE)["stop_requested_at"], 0)
        self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])

    def test_failed_exhaustion_alert_remains_retryable(self) -> None:
        from unittest.mock import patch
        def unreported() -> list[tuple[str]]:
            with sqlite3.connect(self.bridge.store.path) as db:
                return db.execute("SELECT kind FROM outbox WHERE state = 'failed' AND "
                                  "COALESCE(json_extract(payload, '$.reported'), 0) = 0").fetchall()
        self.assertTrue(self.chat("start")["ok"])
        with patch.object(self.bridge, "flush", return_value=0):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.inject_ok = False
        self.linear.down = True
        self.clock.now += 86_401
        self.bridge.tick()
        failed = [r for r in unreported() if r[0] == "status"]
        self.assertEqual(len(failed), 1)
        self.bridge = self.make_bridge()
        self.inject_ok = True
        self.bridge.tick()
        self.assertEqual(unreported(), [])
        self.assertTrue(any("has not accepted" in text for _, text in self.injected))

    def test_takeover_fences_queued_progress_at_send_time(self) -> None:
        from unittest.mock import patch
        self.delegate()
        with patch.object(self.bridge, "flush", return_value=0):
            self.bridge.comment(ISSUE, "stale progress")
            self.bridge.activity(ISSUE, "s-1", "thought", "stale activity")
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.flush()
        self.assertFalse(any(c["body"] == "stale progress" for c in self.linear.comments))
        self.assertFalse(any(a["content"]["body"] == "stale activity" for a in self.linear.activities))

    def test_terminal_messages_are_suppressed_after_human_takeover(self) -> None:
        from unittest.mock import patch
        self.delegate()
        task_id = self.task_id()
        with patch.object(self.bridge, "flush", return_value=0):
            self.complete(task_id, "Findings: https://docs.example/findings/7")
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.flush()
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(self.types(), ["thought"])
        self.assertEqual(self.linear.project_updates, [])

    def test_terminal_messages_are_suppressed_after_human_done(self) -> None:
        from unittest.mock import patch
        self.delegate()
        task_id = self.task_id()
        with patch.object(self.bridge, "flush", return_value=0):
            self.complete(task_id, "Findings: https://docs.example/findings/7")
        self.linear.set_state(ISSUE, "Done")
        self.bridge.flush()
        self.assertEqual(self.types(), ["thought"])
        self.assertEqual(self.linear.project_updates, [])

    def test_terminal_project_update_keeps_kanban_alert_route(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        self.delegate()
        task_id = self.task_id()
        with patch.object(self.bridge, "flush", return_value=0):
            self.complete(task_id, "Findings: https://docs.example/findings/7")
        update = next(p for p in self.bridge.store.pending() if p["kind"] == "project_update")
        self.bridge._loud(update, LinearError("synthetic outage"))
        with self.bridge.kanban.conn() as conn:
            comments = [c.body for c in kb.list_comments(conn, task_id)]
        self.assertTrue(any("has not accepted" in body for body in comments))

    def test_rate_limit_reset_header_is_honoured(self) -> None:
        from unittest.mock import patch
        with patch.object(self.bridge, "flush", return_value=0):
            self.delegate()  # pause applies to queued writes, not unverified execution admission
        self.linear.rate_limited_until = self.clock() + 120
        self.bridge.tick()
        calls = len(self.linear.requests)
        self.clock.now += 60
        self.bridge.tick()
        self.assertEqual(len(self.linear.requests), calls)  # paused: no calls before the reset time
        self.clock.now += 61
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(self.types(), ["thought"])

    def test_restart_preserves_quota_pause_and_delivers_the_same_outbox_rows(self) -> None:
        from unittest.mock import patch
        with patch.object(self.bridge, "flush", return_value=0): self.delegate()
        path = self.dir / "rate-limit.json"
        self.bridge.api.rate_limit_path = path
        pending_ids = {row["id"] for row in self.bridge.store.pending()}
        self.linear.rate_limited_until = self.clock() + 120
        self.bridge.tick()
        calls = len(self.linear.requests)
        self.bridge = self.make_bridge()
        self.bridge.api.rate_limit_path = path
        self.bridge.tick()
        self.assertEqual(len(self.linear.requests), calls)
        self.assertEqual({row["id"] for row in self.bridge.store.pending()}, pending_ids)
        self.clock.now += 121
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(self.types(), ["thought"])

    def test_chat_start_refused_when_another_agent_holds_it(self) -> None:
        self.linear.set_delegate(ISSUE, OTHER)
        self.linear.set_state(ISSUE, "In Progress")
        reply = self.chat("start")
        self.assertFalse(reply["ok"])
        self.assertIn("taken by Other Agent", reply["message"])
        self.assertIn("https://linear.example/issue/ABC-1", reply["message"])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.assertEqual(self.linear.issues[ISSUE]["delegate"], OTHER)

    def test_project_update_once_per_session_after_quiet_period(self) -> None:
        self.linear.add_issue("iss-2", "ABC-2")
        self.assertTrue(self.chat("start")["ok"])
        chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"}, Context("chat-key", "chat-key-id"))
        for _ in range(4):  # turns keep the session busy
            self.clock.now += 20 * 60
            chat.on_turn_end(self.bridge, "chat-key-id")
            self.bridge.tick()
        self.chat("done", evidence="https://docs.example/findings/9", note="Findings")
        self.assertEqual(self.linear.project_updates, [])
        self.clock.now += 31 * 60
        self.bridge.tick()
        self.bridge.tick()
        self.assertEqual(len(self.linear.project_updates), 1)
        body = self.linear.project_updates[0]["body"]
        self.assertIn("ABC-1: Done", body)
        self.assertIn("ABC-2: In progress", body)

    def test_pending_project_update_drops_taken_over_issue_at_send(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        self.linear.set_delegate(ISSUE, OTHER)
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(self.linear.project_updates, [])

    def test_project_update_retains_owned_lines_and_terminal_line(self) -> None:
        from unittest.mock import patch
        self.linear.add_issue("iss-2", "ABC-2")
        self.assertTrue(self.chat("start")["ok"])
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"},
                                              Context("chat-key", "chat-key-id")))["ok"])
        self.bridge.tick()
        with patch.object(self.bridge, "flush", return_value=0):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/9")["ok"])
        self.linear.set_delegate("iss-2", OTHER)
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        body = self.linear.project_updates[0]["body"]
        self.assertIn("ABC-1: Done", body)
        self.assertNotIn("ABC-2", body)

    def test_terminal_takeover_does_not_drop_other_project_line(self) -> None:
        from unittest.mock import patch
        self.linear.add_issue("iss-2", "ABC-2")
        self.assertTrue(self.chat("start")["ok"])
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"},
                                              Context("chat-key", "chat-key-id")))["ok"])
        self.bridge.tick()
        with patch.object(self.bridge, "flush", return_value=0):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/9")["ok"])
        self.linear.set_delegate(ISSUE, OTHER)
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        body = self.linear.project_updates[0]["body"]
        self.assertNotIn("ABC-1", body)
        self.assertIn("ABC-2: In progress", body)
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(len(self.linear.project_updates), 1)

    def test_project_update_merge_does_not_erase_concurrent_terminal_line(self) -> None:
        from unittest.mock import patch
        self.bridge.project_update("chat-key-id", "proj-1", "ABC-1", "In progress",
                                   session_key="chat-key", issue_id=ISSUE)
        self.bridge.store.put("iss-2", "chat", "chat-key", project_id="proj-1")
        rewrite = self.bridge.store.rewrite
        merged = False

        def finish_other_issue():
            self.bridge.store.finish("iss-2", [
                ("status", {"issue_id": "iss-2", "state": "done"}),
                ("project_update", {"issue_id": "update:chat-key-id:proj-1", "session_id": "chat-key-id",
                                    "project_id": "proj-1", "resolve": "iss-2",
                                    "lines": {"ABC-2": "Done"}, "line_issues": {"ABC-2": "iss-2"}}),
            ], at=self.clock())

        def finish_before_rewrite(row_id, payload, next_at):
            nonlocal merged
            finish_other_issue()
            merged = True
            rewrite(row_id, payload, next_at)

        with patch.object(self.bridge.store, "rewrite", side_effect=finish_before_rewrite):
            self.bridge.project_update("chat-key-id", "proj-1", "ABC-1", "Still working",
                                       session_key="chat-key", issue_id=ISSUE)
        if not merged:
            finish_other_issue()  # atomic queue path has no read/rewrite gap
        update = self.bridge.store.project_update("chat-key-id", "proj-1")["payload"]
        self.assertEqual(update["lines"], {"ABC-1": "Still working", "ABC-2": "Done"})
        self.assertEqual(update["line_issues"]["ABC-2"], "iss-2")
        self.assertIn("ABC-2", update["terminal_lines"])

    def test_project_update_send_uses_terminal_merge_after_due_snapshot(self) -> None:
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        self.clock.now += 31 * 60
        due = self.bridge.store.due
        merged = False

        def merge_after_snapshot(now):
            nonlocal merged
            rows = due(now)
            if not merged and any(r["kind"] == "project_update" for r in rows):
                merged = True
                result = chat.handle(self.bridge, {"action": "done", "issue": "ABC-1",
                                                   "evidence": "https://docs.example/findings/9"},
                                     Context("chat-key", "chat-key-id"))
                self.assertTrue(json.loads(result)["ok"])
            return rows

        with patch.object(self.bridge.store, "due", side_effect=merge_after_snapshot):
            self.bridge.flush()
        self.assertTrue(merged)
        self.assertEqual(self.linear.project_updates, [])
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIn("ABC-1: Done", self.linear.project_updates[0]["body"])
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(len(self.linear.project_updates), 1)

    def test_project_update_waits_for_retrying_initial_claim(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        with patch.object(self.bridge.api, "update_issue", side_effect=LinearError("synthetic outage")):
            self.assertTrue(self.chat("start")["ok"])
            self.clock.now += 31 * 60
            self.bridge.flush()
        self.assertEqual(self.linear.project_updates, [])
        self.clock.now += 121
        self.bridge.flush()
        self.assertEqual(self.linear.issues[ISSUE]["delegate"]["id"], SELF)
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIn("ABC-1: In progress", self.linear.project_updates[0]["body"])

    def test_project_update_waits_for_failed_retryable_claim(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        with patch.object(self.bridge.api, "update_issue", side_effect=LinearError("synthetic outage")):
            self.assertTrue(self.chat("start")["ok"])
        claim = next(p for p in self.bridge.store.pending(ISSUE) if p["kind"] == "status" and
                     p["payload"].get("claim"))
        self.clock.now += 86_401
        self.assertTrue(self.bridge.store.retry(claim, self.clock()))
        self.assertTrue(self.bridge.store.pending_claim(ISSUE))
        self.bridge.flush()
        self.assertEqual(self.linear.project_updates, [])
        self.bridge.store.revive_failed(self.clock())
        self.bridge.flush()
        self.assertEqual(self.linear.issues[ISSUE]["delegate"]["id"], SELF)
        self.clock.now += 61
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)

    def test_retrying_terminal_status_does_not_hold_other_project_line(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        self.linear.add_issue("iss-2", "ABC-2")
        self.assertTrue(self.chat("start")["ok"])
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"},
                                               Context("chat-key", "chat-key-id")))["ok"])
        self.bridge.tick()
        resolve_state = self.bridge.state_name

        def fail_done(issue, key):
            if issue["id"] == ISSUE and key == "done":
                raise LinearError("synthetic pre-send status resolution outage", retryable=True)
            return resolve_state(issue, key)

        with patch.object(self.bridge, "state_name", side_effect=fail_done):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/9")["ok"])
            self.clock.now += 31 * 60
            self.bridge.flush()
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIn("ABC-2: In progress", self.linear.project_updates[0]["body"])
        self.assertNotIn("ABC-1: Done", self.linear.project_updates[0]["body"])
        self.assertTrue(any("ABC-1" in p["payload"].get("lines", {}) for p in self.bridge.store.pending()
                            if p["kind"] == "project_update"))
        self.bridge = self.make_bridge()
        self.clock.now += 121
        self.bridge.flush()
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertEqual(len(self.linear.project_updates), 2)
        self.assertIn("ABC-1: Done", self.linear.project_updates[1]["body"])
        self.assertNotIn("ABC-2", self.linear.project_updates[1]["body"])
        self.bridge = self.make_bridge()
        self.bridge.tick()
        self.assertEqual(len(self.linear.project_updates), 2)

    def test_staggered_terminal_retries_share_one_followup_project_update(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        self.linear.add_issue("iss-2", "ABC-2")
        self.linear.add_issue("iss-3", "ABC-3")
        for ident in ("ABC-1", "ABC-2", "ABC-3"):
            self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": ident},
                                                   Context("chat-key", "chat-key-id")))["ok"])
        self.bridge.tick()
        with patch.object(self.bridge, "flush", return_value=0):
            for ident in ("ABC-1", "ABC-2", "ABC-3"):
                self.assertTrue(json.loads(chat.handle(
                    self.bridge, {"action": "done", "issue": ident,
                                  "evidence": "https://docs.example/findings/9"},
                    Context("chat-key", "chat-key-id")))["ok"])
        resolve_state = self.bridge.state_name
        failing = {ISSUE, "iss-2"}

        def fail_done(issue, key):
            if issue["id"] in failing and key == "done":
                raise LinearError("synthetic pre-send status resolution outage", retryable=True)
            return resolve_state(issue, key)

        self.clock.now += 31 * 60
        with patch.object(self.bridge, "state_name", side_effect=fail_done):
            self.bridge.flush()
            self.assertEqual(len(self.linear.project_updates), 1)
            self.assertIn("ABC-3: Done", self.linear.project_updates[0]["body"])
            failing.remove(ISSUE)
            self.clock.now += 121
            self.bridge.flush()
            self.assertEqual(len(self.linear.project_updates), 1)
            failing.clear()
            self.clock.now += 121
            self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 2)
        self.assertIn("ABC-1: Done", self.linear.project_updates[1]["body"])
        self.assertIn("ABC-2: Done", self.linear.project_updates[1]["body"])

    def test_unmarked_deferred_update_stays_bounded_after_restart(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        self.linear.add_issue("iss-2", "ABC-2")
        self.linear.add_issue("iss-3", "ABC-3")
        for ident in ("ABC-1", "ABC-2", "ABC-3"):
            self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": ident},
                                                   Context("chat-key", "chat-key-id")))["ok"])
        self.bridge.tick()
        with patch.object(self.bridge, "flush", return_value=0):
            for ident in ("ABC-1", "ABC-2", "ABC-3"):
                self.assertTrue(json.loads(chat.handle(
                    self.bridge, {"action": "done", "issue": ident,
                                  "evidence": "https://docs.example/findings/9"},
                    Context("chat-key", "chat-key-id")))["ok"])
        failing = {ISSUE, "iss-2"}

        def flush_with_failures() -> None:
            resolve_state = self.bridge.state_name

            def fail_done(issue, key):
                if issue["id"] in failing and key == "done":
                    raise LinearError("synthetic pre-send status resolution outage", retryable=True)
                return resolve_state(issue, key)

            with patch.object(self.bridge, "state_name", side_effect=fail_done):
                self.bridge.flush()

        self.clock.now += 31 * 60
        flush_with_failures()
        self.assertEqual(len(self.linear.project_updates), 1)
        with sqlite3.connect(self.bridge.store.path) as db:
            deferred = db.execute("SELECT id, payload FROM outbox WHERE kind='project_update' "
                                  "AND state='pending'").fetchall()
            self.assertEqual(len(deferred), 1)
            self.assertTrue(json.loads(deferred[0][1])["followup"])
            db.execute("UPDATE outbox SET payload=json_remove(payload, '$.followup') WHERE id=?",
                       (deferred[0][0],))  # row persisted by pre-upgrade code
        self.bridge = self.make_bridge()
        failing.remove(ISSUE)
        self.clock.now += 121
        flush_with_failures()
        self.assertEqual(len(self.linear.project_updates), 1)
        failing.clear()
        self.clock.now += 121
        flush_with_failures()
        self.assertEqual(len(self.linear.project_updates), 2)
        self.assertIn("ABC-1: Done", self.linear.project_updates[1]["body"])
        self.assertIn("ABC-2: Done", self.linear.project_updates[1]["body"])

    def test_deferred_terminal_line_is_fenced_after_takeover(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        self.linear.add_issue("iss-2", "ABC-2")
        self.assertTrue(self.chat("start")["ok"])
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"},
                                               Context("chat-key", "chat-key-id")))["ok"])
        self.bridge.tick()
        with patch.object(self.bridge.api, "update_issue", side_effect=LinearError("synthetic outage")):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/9")["ok"])
            self.clock.now += 31 * 60
            self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge = self.make_bridge()
        self.clock.now += 121
        self.bridge.flush()
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertNotIn("ABC-1", self.linear.project_updates[0]["body"])

    def test_pending_claim_does_not_freeze_out_later_terminal_line(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        with patch.object(self.bridge.api, "update_issue", side_effect=LinearError("synthetic outage")):
            self.assertTrue(self.chat("start")["ok"])
            self.clock.now += 31 * 60
            self.bridge.flush()
        done = chat.handle(self.bridge, {"action": "done", "issue": "ABC-1",
                                         "evidence": "https://docs.example/findings/9"},
                           Context("chat-key", "chat-key-id"))
        self.assertTrue(json.loads(done)["ok"])
        self.clock.now += 121 + 31 * 60
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIn("ABC-1: Done", self.linear.project_updates[0]["body"])

    def test_failed_unfrozen_update_absorbs_terminal_line_before_revival(self) -> None:
        from hermes_fleet_linear_plugin.api import LinearError
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        self.clock.now += 31 * 60
        with patch.object(self.bridge.api, "issue", side_effect=LinearError("synthetic read outage")):
            self.bridge.flush()
        update = next(p for p in self.bridge.store.pending() if p["kind"] == "project_update")
        self.assertNotIn("frozen", update["payload"])
        self.clock.now += 86_401
        self.assertTrue(self.bridge.store.retry(update, self.clock()))
        done = chat.handle(self.bridge, {"action": "done", "issue": "ABC-1",
                                         "evidence": "https://docs.example/findings/9"},
                           Context("chat-key", "chat-key-id"))
        self.assertTrue(json.loads(done)["ok"])
        stored = self.bridge.store.project_update("chat-key-id", "proj-1")["payload"]
        self.assertIn("Done", stored["lines"]["ABC-1"])
        self.bridge.flush()
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIn("ABC-1: Done", self.linear.project_updates[0]["body"])

    def test_project_update_respects_quiet_extension_during_send_preflight(self) -> None:
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        self.clock.now += 31 * 60
        freeze = self.bridge.store.freeze_project_update
        delayed = False

        def delay_before_freeze(*args, **kwargs):
            nonlocal delayed
            if not delayed:
                delayed = True
                self.bridge.store.delay_session_updates("chat-key-id", self.clock() + 30 * 60)
            return freeze(*args, **kwargs)

        with patch.object(self.bridge.store, "freeze_project_update", side_effect=delay_before_freeze):
            self.bridge.flush()
        self.assertTrue(delayed)
        self.assertEqual(self.linear.project_updates, [])
        self.clock.now += 30 * 60 + 1
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)

    def test_unpublished_takeover_batch_allows_later_owned_work(self) -> None:
        self.linear.add_issue("iss-2", "ABC-2")
        self.assertTrue(self.chat("start")["ok"])
        self.linear.set_delegate(ISSUE, OTHER)
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(self.linear.project_updates, [])
        started = chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"},
                              Context("chat-key", "chat-key-id"))
        self.assertTrue(json.loads(started)["ok"])
        self.bridge.tick()
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIn("ABC-2: In progress", self.linear.project_updates[0]["body"])

    def test_uncertain_create_retry_does_not_change_body_after_takeover(self) -> None:
        self.linear.add_issue("iss-2", "ABC-2")
        self.assertTrue(self.chat("start")["ok"])
        started = chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"},
                              Context("chat-key", "chat-key-id"))
        self.assertTrue(json.loads(started)["ok"])
        self.bridge.tick()
        self.clock.now += 31 * 60
        self.linear.lose_next_response = True
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        update = next(p for p in self.bridge.store.pending() if p["kind"] == "project_update")
        self.assertIn("ABC-2", update["payload"]["send_body"])
        self.linear.set_delegate("iss-2", OTHER)
        self.clock.now += 61
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertEqual(len([p for p in self.bridge.store.pending() if p["kind"] == "project_update"]), 1)

    def test_quiet_extension_after_send_starts_does_not_move_frozen_batch(self) -> None:
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        self.clock.now += 31 * 60
        create = self.bridge.api.create_project_update

        def end_turn_before_create(*args, **kwargs):
            chat.on_turn_end(self.bridge, "chat-key-id")
            return create(*args, **kwargs)

        with patch.object(self.bridge.api, "create_project_update", side_effect=end_turn_before_create):
            self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        update = self.bridge.store.project_update("chat-key-id", "proj-1")
        self.assertLessEqual(update["next_at"], self.clock())

    def test_reclaimed_issue_replaces_unpublished_terminal_dependency(self) -> None:
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        with patch.object(self.bridge, "flush", return_value=0):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/9")["ok"])
        self.linear.set_delegate(ISSUE, OTHER)
        self.bridge.flush()  # terminal status is declined; quiet update remains queued
        self.linear.set_delegate(ISSUE, None)
        self.assertTrue(self.chat("start")["ok"])
        update = self.bridge.store.project_update("chat-key-id", "proj-1")["payload"]
        self.assertEqual(update["lines"], {"ABC-1": "In progress"})
        self.assertNotIn("ABC-1", update.get("terminal_lines", {}))
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIn("ABC-1: In progress", self.linear.project_updates[0]["body"])

    def test_team_without_blocked_state_keeps_status_and_still_reports(self) -> None:
        self.bridge = self.make_bridge({"team_states": {"ABC": {"blocked": None, "done": "In Review"}}})
        self.linear.issues[ISSUE]["team"]["states"]["nodes"] = [
            s for s in self.linear.issues[ISSUE]["team"]["states"]["nodes"] if s["name"] != "Blocked"] + [
            {"id": "st-review", "name": "In Review", "type": "started"}]
        self.delegate()
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.assertEqual(self.linear.state(ISSUE), "In Progress")  # unchanged, not an error
        self.assertIn("Stopped by", self.linear.activities[-1]["content"]["body"])
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", body="carry on"))
        self.complete(self.task_id(), "Findings: https://docs.example/findings/3")
        self.assertEqual(self.linear.state(ISSUE), "In Review")
        self.assertEqual(self.bridge.store.pending(), [])

    def test_chat_block_on_team_without_blocked_state_is_visible_in_agent_panel(self) -> None:
        self.bridge = self.make_bridge({"team_states": {"ABC": {"blocked": None}}})
        self.linear.issues[ISSUE]["team"]["states"]["nodes"] = [
            s for s in self.linear.issues[ISSUE]["team"]["states"]["nodes"] if s["name"] != "Blocked"]
        self.assertTrue(self.chat("start")["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "chat-linear-session", creator=SELF))
        self.assertTrue(self.chat("start")["ok"])  # idempotent start must preserve the session binding
        self.assertTrue(self.chat("blocked", note="Need a human decision")["ok"])
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(self.linear.activities[-1]["content"]["type"], "elicitation")
        self.assertIn("Need a human decision", self.linear.activities[-1]["content"]["body"])

    def test_older_created_and_stop_cannot_replace_chat_panel_session(self) -> None:
        self.assertTrue(self.chat("start")["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "chat-new", creator=SELF, at=10))
        self.deliver(self.linear.session_event("created", ISSUE, "chat-old", creator=SELF, at=-10))
        self.assertEqual(self.bridge.store.get(ISSUE)["linear_session_id"], "chat-new")
        self.deliver(self.linear.session_event("prompted", ISSUE, "chat-old", signal="stop", at=-5))
        self.assertEqual(self.bridge.store.get(ISSUE)["stop_requested_at"], 0)
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertTrue(self.chat("blocked", note="Need a human decision")["ok"])
        self.assertEqual(self.linear.activities[-1]["agentSessionId"], "chat-new")


    def test_delayed_delegation_respects_cancel_source_time_but_fresh_redelegation_opens(self) -> None:
        self.bind_identity()
        self.linear.set_delegate(ISSUE, {"id": SELF, "name": "This Agent"})
        self.bridge.tick()  # the periodic ownership read is not due on delivery
        old = self.linear.session_event("created", ISSUE, "old-session")
        self.clock.now += 10
        self.linear.set_state(ISSUE, "Canceled")
        self.deliver(old)
        self.assertEqual(self.linear.state(ISSUE), "Canceled")
        self.assertEqual(self.tasks(), [])
        self.assertIsNone(self.bridge.store.get(ISSUE))

        self.clock.now += 1
        fresh = self.linear.session_event("created", ISSUE, "fresh-session")
        self.bridge.handle_webhook(fresh)
        task = self.task_id()
        self.bridge.flush()
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(self.tasks(), [(task, "ready")])
        # Even a delayed enqueue must compare the original source time at send.
        self.clock.now += 1
        self.linear.set_state(ISSUE, "Canceled")
        self.linear.issues[ISSUE]["canceledAt"] = self.clock.iso()
        self.clock.now += 1
        self.bridge.status(ISSUE, "in_progress", claim=True, seen=SELF,
                           source_ms=fresh["webhookTimestamp"])
        self.bridge.flush()
        self.assertEqual(self.linear.state(ISSUE), "Canceled")
        self.assertEqual(self.tasks(), [(task, "archived")])

        self.clock.now += 1
        cold_fresh = self.linear.session_event("created", ISSUE, "cold-fresh-session")
        self.clock.now += 1
        self.linear.set_delegate(ISSUE, {"id": SELF, "name": "This Agent"})  # delegation echo updates issue later
        self.bridge = self.make_bridge()
        self.bind_identity()
        self.deliver(cold_fresh)
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        self.assertEqual(self.bridge.kanban.get(self.task_id()).status, "ready")


    def test_known_unsent_predecessor_can_yield_to_successor_with_receipts(self) -> None:
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        for resolution_failed, capture_first in ((False, True), (True, True), (False, False)):
            with self.subTest(resolution_failed=resolution_failed, capture_first=capture_first):
                case = f"{resolution_failed}-{capture_first}"
                issue_id = f"predecessor-{case}"
                self.linear.add_issue(issue_id, f"OLD-{int(resolution_failed) + 1}")
                self.linear.set_delegate(issue_id, {"id": SELF, "name": "This Agent"})
                self.deliver(self.linear.session_event("created", issue_id, f"old-{case}"))
                old_task = self.bridge.store.get(issue_id)["task_id"]
                with self.bridge.kanban.conn() as conn:
                    kb.complete_task(conn, old_task, summary="Findings https://docs.example/predecessor", metadata={})
                if capture_first:
                    self.bridge.pump_kanban()
                if resolution_failed:
                    with patch.object(self.bridge, "_effect_issue",
                                      side_effect=LinearError("pre-send issue resolution unavailable")):
                        self.bridge.flush()
                    status = next(r for r in self.bridge.store.pending(issue_id)
                                  if r["kind"] == "status" and r["payload"].get("terminal"))
                    self.assertIs(status["payload"]["write_started"], False)
                    self.assertEqual(status["attempts"], 1)
                self.clock.now += 1
                self.bridge.handle_webhook(self.linear.session_event("created", issue_id, f"new-{case}"))
                new_task = self.bridge.store.get(issue_id)["task_id"]
                terminal_rows = [r for r in self.bridge.store.pending(issue_id) if r["kind"] == "status"
                                 and r["payload"].get("terminal") and r["payload"].get("task_id") == old_task]
                self.assertTrue(terminal_rows, "The predecessor result must be captured before replacing its mapping")
                terminal = terminal_rows[0]
                self.clock.now += 61
                self.bridge.tick()
                self.assertEqual(self.bridge.kanban.get(new_task).status, "ready")
                self.assertEqual(self.bridge.kanban.get(old_task).status, "done")
                self.assertEqual(self.linear.state(issue_id), "In Progress")
                self.assertEqual(self.bridge.store.get(issue_id)["task_id"], new_task)
                with self.bridge.store._tx() as db:
                    successor_claim = db.execute("SELECT state, json_extract(payload, '$.applied') FROM outbox "
                                                 "WHERE kind='status' AND json_extract(payload, '$.task_id')=?",
                                                 (new_task,)).fetchone()
                self.assertEqual(tuple(successor_claim), ("sent", 1))
                old_receipts = [a for a in self.linear.activities if a["agentSessionId"] == f"old-{case}"
                                and a["content"]["type"] != "thought"]
                status = self.bridge.store.outbox_row(terminal["id"])
                self.assertNotIn("reconcile_required", status["payload"])
                self.assertFalse(status["payload"]["applied"])
                self.assertEqual(len(old_receipts), 1)
                self.assertIn("superseded", old_receipts[0]["content"]["body"].lower())
                self.assertIn("https://docs.example/predecessor", old_receipts[0]["content"]["body"])
                self.assertFalse(any(f"OLD-{int(resolution_failed) + 1}: Done" in u["body"]
                                     for u in self.linear.project_updates))

    def test_applied_terminal_lost_response_blocks_fresh_admission_and_stale_claim(self) -> None:
        from unittest.mock import patch
        self.delegate("old-session")
        old_task = self.task_id()
        with self.bridge.kanban.conn() as conn:
            kb.complete_task(conn, old_task, summary="Findings https://docs.example/predecessor", metadata={})
        self.bridge.pump_kanban()
        terminal = next(row for row in self.bridge.store.pending(ISSUE)
                        if row["kind"] == "status" and row["payload"].get("terminal"))
        # This successor claim was captured before the predecessor's send outcome was known.
        claim = self.bridge.status(ISSUE, "in_progress", claim=True, seen=SELF,
                                   source_ms=self.clock() * 1000 + 1000)
        stale_comment = self.bridge.comment(ISSUE, "Stale successor comment")
        stale_activity = self.bridge.activity(ISSUE, "old-session", "thought", "Stale successor activity")
        self.linear.lose_next_response = True
        self.bridge.flush()
        self.assertEqual(self.linear.state(ISSUE), "Done")  # FakeLinear applied the mutation before 502.
        self.assertTrue(self.bridge.store.outbox_row(terminal["id"])["payload"]["write_started"])
        self.assertEqual(self.bridge.store.outbox_row(claim)["state"], "pending")
        self.assertEqual([self.bridge.store.outbox_row(row_id)["state"]
                          for row_id in (stale_comment, stale_activity)], ["pending", "pending"])

        self.clock.now += 1
        created = self.linear.session_event("created", ISSUE, "fresh-session")
        before_tasks = self.tasks()
        before_mapping = self.bridge.store.get(ISSUE)
        before_injected = list(self.injected)
        before_mutations = tuple(q for q in self.linear.requests if q.startswith("mutation"))
        self.bridge.handle_webhook(created)
        self.assertEqual(self.tasks(), before_tasks)
        self.assertEqual(self.bridge.store.get(ISSUE), before_mapping)
        self.assertEqual(self.injected, before_injected)
        self.assertFalse(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-1"},
                                               Context("fresh-chat", "fresh-chat-id")))["ok"])
        self.clock.now += 61
        with patch.object(self.bridge.kanban, "comment", side_effect=RuntimeError("alert unavailable")):
            self.bridge.tick()
        self.assertTrue(self.bridge.store.outbox_row(terminal["id"])["payload"]["reconcile_required"])
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertEqual(tuple(q for q in self.linear.requests if q.startswith("mutation")), before_mutations)
        self.assertEqual(self.injected, before_injected)
        self.assertFalse(any("Stale successor" in body for body in self.linear.bodies(ISSUE)))
        self.assertEqual(self.bridge.store.outbox_row(claim)["state"], "pending")
        self.bridge = self.make_bridge()
        self.clock.now += 1
        self.linear.set_state(ISSUE, "In Progress")  # A newer human reopen is not reconciliation.
        self.bridge.handle_webhook(self.linear.session_event("created", ISSUE, "later-session"))
        self.bridge.tick()
        self.assertEqual(self.tasks(), before_tasks)
        self.assertEqual(self.bridge.store.get(ISSUE), before_mapping)
        self.assertEqual(tuple(q for q in self.linear.requests if q.startswith("mutation")), before_mutations)
        self.assertEqual(self.bridge.store.outbox_row(terminal["id"])["state"], "failed")
        self.assertTrue(any(row["payload"].get("requires_status_id") == terminal["id"]
                            for row in self.bridge.store.pending(ISSUE)))
        self.assertTrue(self.bridge.store.reconcile_terminal(
            terminal["id"], outcome="applied", evidence="https://docs.example/verified-remote-result",
            at=self.clock()))
        resolved = self.bridge.store.outbox_row(terminal["id"])
        self.assertEqual((resolved["state"], resolved["payload"]["write_started"]), ("failed", True))
        self.assertTrue(resolved["payload"]["reconcile_required"])
        self.assertTrue(any(row["payload"].get("requires_status_id") == terminal["id"]
                            for row in self.bridge.store.pending(ISSUE)))
        self.bridge = self.make_bridge()
        self.clock.now += 1
        self.deliver(self.linear.session_event("created", ISSUE, "reconciled-session"))
        self.assertEqual(len(self.tasks()), len(before_tasks) + 1)
        self.assertNotEqual(self.bridge.store.get(ISSUE), before_mapping)

    def test_uncertain_terminal_does_not_widen_shared_project_batch(self) -> None:
        self.linear.add_issue("iss-2", "ABC-2")
        self.assertTrue(self.chat("start")["ok"])
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": "ABC-2"},
                                               Context("chat-key", "chat-key-id")))["ok"])
        self.bridge.tick()
        self.linear.lose_next_response = True
        self.assertTrue(self.chat("done", evidence="https://docs.example/findings/9")["ok"])
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.clock.now += 61
        self.bridge.tick()
        self.assertTrue(self.bridge.store.issue_reconciliation_blocked(ISSUE))
        self.clock.now += 31 * 60
        self.bridge.flush()
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertIn("ABC-2: In progress", self.linear.project_updates[0]["body"])
        self.assertNotIn("ABC-1", self.linear.project_updates[0]["body"])
        self.assertTrue(any("ABC-1" in row["payload"].get("lines", {})
                            for row in self.bridge.store.pending() if row["kind"] == "project_update"))


    def test_interrupted_resume_replays_core_unblock_after_restart(self) -> None:
        from unittest.mock import patch
        from linear_ingress_fixture import IngressStore, Route
        self.bind_identity()
        inbox = self.dir / "resume-ingress.db"
        producer = IngressStore(inbox)
        route = Route("alpha", "alpha", "/webhook/alpha", self.dir / "unused-secret", inbox)
        for action in ("created", "prompted"):
            with self.subTest(action=action):
                issue_id = f"resume-issue-{action}"
                self.linear.add_issue(issue_id, f"RESUME-{action}")
                self.linear.set_delegate(issue_id, {"id": SELF, "name": "This Agent"})
                self.deliver(self.linear.session_event("created", issue_id, f"start-{action}"))
                task = self.bridge.store.get(issue_id)["task_id"]
                self.clock.now += 1
                stop = self.linear.session_event("prompted", issue_id, "s-1", signal="stop")
                self.deliver(stop)
                self.clock.now += 1
                session = f"resume-{action}"
                event = self.linear.session_event(action, issue_id, session, body="Continue the same work")
                producer.enqueue(route, session, json.dumps(event).encode())
                with sqlite3.connect(inbox) as db:
                    db.execute("UPDATE deliveries SET received_at=?", (int(self.clock()),))
                with patch.object(self.bridge.kanban, "unblock", side_effect=RuntimeError("interrupted before core unblock")):
                    self.bridge.drain_ingress(inbox)
                with sqlite3.connect(inbox) as db:
                    self.assertEqual(db.execute("SELECT status FROM deliveries WHERE delivery_id=?", (session,)).fetchone()[0], "pending")
                self.assertEqual(self.bridge.kanban.get(task).status, "blocked")
                self.assertEqual(self.bridge.store.get(issue_id)["owner_ref"], session)
                self.bridge = self.make_bridge()
                self.bind_identity()
                self.bridge.recover()
                self.bridge.tick(inbox)
                self.assertEqual(self.bridge.kanban.get(task).status, "ready")
                with self.bridge.kanban.conn() as conn:
                    notes = "\n".join(c.body for c in kb.list_comments(conn, task))
                    self.assertEqual(notes.count("Continue the same work"), 1 if action == "prompted" else 0)
                self.assertEqual(self.linear.state(issue_id), "In Progress")
                self.assertFalse(self.bridge.store.get(issue_id)["stop_requested_at"])
                with sqlite3.connect(inbox) as db:
                    self.assertEqual(db.execute("SELECT status FROM deliveries WHERE delivery_id=?", (session,)).fetchone()[0], "imported")
                receipt_count = len(self.linear.activities)
                self.bridge = self.make_bridge()
                self.bind_identity()
                with self.bridge.kanban.conn() as conn:
                    kb.recompute_ready(conn)  # mirror core's scheduler after reopening the board
                self.bridge.tick(inbox)
                self.bridge.handle_webhook(stop)  # stale Stop cannot undo the recovered resume
                self.assertEqual(self.bridge.kanban.get(task).status, "ready")
                self.assertEqual(len(self.linear.activities), receipt_count)

    def test_false_unblock_retains_resume_intent_while_core_is_blocked(self) -> None:
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        self.delegate()
        task = self.task_id()
        self.clock.now += 1
        self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
        self.clock.now += 1
        event = self.linear.session_event("prompted", ISSUE, "s-1", body="Continue")
        with patch.object(self.bridge.kanban, "unblock", return_value=False):
            with self.assertRaises(LinearError):
                self.bridge.handle_webhook(event)
        self.assertEqual(self.bridge.kanban.get(task).status, "blocked")
        self.assertIsNotNone(self.bridge.store.get(ISSUE)["pending_resume"])
        self.bridge = self.make_bridge()
        self.bridge.recover()
        self.assertEqual(self.bridge.kanban.get(task).status, "ready")

    def test_repeated_stop_resume_uses_supported_same_task_triage_transition(self):
        self.delegate()
        task = self.task_id()
        for cycle in range(3):
            self.clock.now += 1
            self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
            self.assertEqual(self.bridge.kanban.get(task).status, "blocked" if cycle == 0 else "triage")
            self.clock.now += 1
            self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", body=f"Resume cycle {cycle}"))
            self.bridge = self.make_bridge()
            self.bridge.recover()
            self.assertEqual(self.tasks(), [(task, "ready")])
            self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])
        with self.bridge.kanban.conn() as conn:
            self.assertEqual(len([e for e in kb.list_events(conn, task) if e.kind == "specified"]), 2)
            self.assertEqual(self.bridge.kanban.get(task).block_recurrences, 3)

    def test_other_core_triage_preserves_instruction_and_reports_blocker_once(self):
        self.delegate()
        task = self.task_id()
        with self.bridge.kanban.conn() as conn:
            self.assertTrue(kb.block_task(conn, task, kind="capability", reason="Missing required capability"))
            self.assertTrue(kb.unblock_task(conn, task))
            self.assertTrue(kb.block_task(conn, task, kind="capability", reason="Still missing capability"))
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertIn("Kanban triage", self.linear.activities[-1]["content"]["body"])
        self.clock.now += 1
        event = self.linear.session_event("prompted", ISSUE, "s-1", body="Keep this instruction")
        self.deliver(event)
        self.assertEqual(self.bridge.kanban.get(task).status, "triage")
        self.assertIsNotNone(self.bridge.store.get(ISSUE)["pending_resume"])
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertIn("Kanban triage", self.linear.activities[-1]["content"]["body"])
        count = len(self.linear.activities)
        self.bridge = self.make_bridge()
        self.bridge.recover()
        self.deliver(event)
        self.assertEqual(len(self.linear.activities), count)
        with self.bridge.kanban.conn() as conn:
            self.assertFalse(any(e.kind == "specified" for e in kb.list_events(conn, task)))
            self.assertTrue(kb.specify_triage_task(conn, task, author="operator"))
        self.bridge.tick()
        self.assertEqual(self.tasks(), [(task, "ready")])
        self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])
        self.assertEqual(self.linear.state(ISSUE), "In Progress")
        with self.bridge.kanban.conn() as conn:
            self.assertEqual(sum(c.body.count("Keep this instruction") for c in kb.list_comments(conn, task)), 1)

    def test_stop_caused_triage_holds_live_worker_across_restart(self):
        from hermes_fleet_linear_plugin.api import LinearError
        with self.stopped_worker() as (process, task, event):
            process.terminate(); process.wait(timeout=10)
            self.deliver(event)
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            with self.bridge.kanban.conn() as conn:
                self.assertTrue(kb.claim_task(conn, task, claimer=kb._claimer_id()))
                dispatch._set_worker_pid(conn, task, process.pid)
            self.clock.now += 1
            self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
            self.assertEqual(self.bridge.kanban.get(task).status, "triage")
            self.clock.now += 1
            with self.assertRaises(LinearError):
                self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", body="Resume same task"))
            self.bridge = self.make_bridge()
            self.bridge.recover()
            self.assertEqual(self.bridge.kanban.get(task).status, "triage")
            self.assertIsNone(process.poll())
            self.assertIsNotNone(self.bridge.store.get(ISSUE)["pending_resume"])
            process.terminate(); process.wait(timeout=10)
            self.bridge.recover()
            self.assertEqual(self.tasks(), [(task, "ready")])
            self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])
        finally:
            if process.poll() is None: process.terminate()
            process.wait(timeout=10)

    @contextmanager
    def stopped_worker(self):
        self.delegate()
        task = self.task_id()
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            with self.bridge.kanban.conn() as conn:
                self.assertTrue(kb.claim_task(conn, task, claimer=kb._claimer_id()))
                dispatch._set_worker_pid(conn, task, process.pid)
            self.clock.now += 1
            self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
            self.assertEqual(self.bridge.kanban.get(task).status, "blocked")
            self.clock.now += 1
            yield process, task, self.linear.session_event("prompted", ISSUE, "s-1", body="Resume this task")
        finally:
            if process.poll() is None: process.terminate()
            process.wait(timeout=10)

    def test_pending_resume_waits_for_stopped_worker_exit_across_recovery(self):
        from hermes_fleet_linear_plugin.api import LinearError
        with self.stopped_worker() as (process, task, event):
            with self.bridge.kanban.conn() as conn:
                before = len([e for e in kb.list_events(conn, task) if e.kind == "commented"])
            with self.assertRaises(LinearError): self.deliver(event)
            self.bridge = self.make_bridge()
            self.bridge.recover()
            self.assertEqual(self.bridge.kanban.get(task).status, "blocked")
            self.assertIsNotNone(self.bridge.store.get(ISSUE)["pending_resume"])
            self.assertIsNone(process.poll())
            with self.bridge.kanban.conn() as conn:
                self.assertEqual(len([e for e in kb.list_events(conn, task) if e.kind == "commented"]), before)
            process.terminate(); process.wait(timeout=10)
            self.bridge.recover()
            self.assertEqual(self.bridge.kanban.get(task).status, "ready")
            self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])
            self.bridge.recover()
            with self.bridge.kanban.conn() as conn:
                self.assertEqual(len([e for e in kb.list_events(conn, task) if e.kind == "commented"]), before + 1)

    def test_newer_stop_fence_survives_core_failure_and_ingress_retries(self):
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        from linear_ingress_fixture import IngressStore, Route
        with self.stopped_worker() as (process, task, event):
            with self.assertRaises(LinearError): self.deliver(event)
            self.clock.now += 1
            stop = self.linear.session_event("prompted", ISSUE, "s-1", signal="stop")
            stamp = self.clock() * 1000
            inbox = self.dir / "stop-retry.db"
            producer = IngressStore(inbox)
            producer.enqueue(Route("alpha", "alpha", "/webhook/alpha", inbox, inbox), "new-stop", json.dumps(stop).encode())
            with sqlite3.connect(inbox) as db:
                db.execute("UPDATE deliveries SET received_at=?", (self.clock(),))
            with patch.object(self.bridge.kanban, "block", side_effect=LinearError("Temporary core failure")):
                self.bridge.drain_ingress(inbox)
            self.assertEqual(self.bridge.store.get(ISSUE)["stop_requested_at"], stamp)
            self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])
            process.terminate(); process.wait(timeout=10)
            self.bridge = self.make_bridge()
            with patch.object(self.bridge.kanban, "block", side_effect=RuntimeError("Interrupted core call")):
                self.bridge.recover(inbox)
            self.assertEqual(self.tasks(), [(task, "blocked")])
            self.assertEqual(self.bridge.store.get(ISSUE)["stop_requested_at"], stamp)
            self.bridge.tick(inbox)
            self.assertEqual(self.tasks(), [(task, "blocked")])
            self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])
            with sqlite3.connect(inbox) as db:
                self.assertEqual(db.execute("SELECT status FROM deliveries WHERE delivery_id='new-stop'").fetchone()[0], "imported")

    def test_startup_processes_owned_stop_beyond_normal_inbox_batch_before_resume(self):
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        from linear_ingress_fixture import IngressStore, Route
        with self.stopped_worker() as (process, task, event):
            with self.assertRaises(LinearError): self.deliver(event)
            inbox = self.dir / "startup-stop.db"
            producer = IngressStore(inbox)
            route = Route("alpha", "alpha", "/webhook/alpha", inbox, inbox)
            for i in range(101): producer.enqueue(route, f"older-{i:03}", b'{"type":"Unknown"}')
            self.clock.now += 1
            stop = self.linear.session_event("prompted", ISSUE, "s-1", signal="stop")
            producer.enqueue(route, "new-stop", json.dumps(stop).encode())
            with sqlite3.connect(inbox) as db:
                db.execute("UPDATE deliveries SET received_at=?", (self.clock()-1,))
                db.execute("UPDATE deliveries SET received_at=? WHERE delivery_id='new-stop'", (self.clock(),))
            process.terminate(); process.wait(timeout=10)
            self.bridge = self.make_bridge()
            with patch.object(self.bridge.kanban, "unblock", wraps=self.bridge.kanban.unblock) as unblock:
                self.bridge.recover(inbox)
                unblock.assert_not_called()
            self.assertEqual(self.tasks(), [(task, "blocked")])
            self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])
            with sqlite3.connect(inbox) as db:
                self.assertEqual(db.execute("SELECT status FROM deliveries WHERE delivery_id='new-stop'").fetchone()[0], "imported")
                self.assertEqual(db.execute("SELECT count(*) FROM deliveries WHERE status='pending'").fetchone()[0], 51)

    def test_pending_resume_waits_for_stop_beyond_prioritized_batch(self):
        from hermes_fleet_linear_plugin.api import LinearError
        from linear_ingress_fixture import IngressStore, Route
        with self.stopped_worker() as (process, task, event):
            with self.assertRaises(LinearError): self.deliver(event)
            inbox = self.dir / "batched-stops.db"
            producer = IngressStore(inbox)
            route = Route("alpha", "alpha", "/webhook/alpha", inbox, inbox)
            self.clock.now -= 1
            old_stop = self.linear.session_event("prompted", ISSUE, "s-1", signal="stop")
            self.clock.now += 1
            for i in range(50): producer.enqueue(route, f"older-{i:03}", json.dumps(old_stop).encode())
            self.clock.now += 1
            producer.enqueue(route, "new-stop", json.dumps(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop")).encode())
            with sqlite3.connect(inbox) as db:
                db.execute("UPDATE deliveries SET received_at=?", (self.clock()-1,))
                db.execute("UPDATE deliveries SET received_at=? WHERE delivery_id='new-stop'", (self.clock(),))
            process.terminate(); process.wait(timeout=10)
            self.bridge = self.make_bridge()
            self.bridge.tick(inbox)
            self.assertEqual(self.tasks(), [(task, "blocked")])
            self.assertIsNotNone(self.bridge.store.get(ISSUE)["pending_resume"])
            with sqlite3.connect(inbox) as db:
                self.assertEqual(db.execute("SELECT status FROM deliveries WHERE delivery_id='new-stop'").fetchone()[0], "pending")
            self.bridge.tick(inbox)
            self.assertEqual(self.tasks(), [(task, "blocked")])
            self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])

    def test_newer_stop_cancels_pending_resume_across_worker_exit_and_recovery(self):
        from hermes_fleet_linear_plugin.api import LinearError
        with self.stopped_worker() as (process, task, event):
            with self.assertRaises(LinearError): self.deliver(event)
            stale = self.bridge.store.get(ISSUE)
            intent = json.loads(stale["pending_resume"])
            self.clock.now += 1
            self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", signal="stop"))
            stopped = self.bridge.store.get(ISSUE)
            self.assertIsNone(stopped["pending_resume"])
            process.terminate(); process.wait(timeout=10)
            self.bridge = self.make_bridge()
            self.bridge.recover()
            self.bridge._resume(stale, intent["note"], intent["session_id"], intent["stamp"], intent["receipt"])
            self.assertEqual(self.bridge.kanban.get(task).status, "blocked")
            self.assertEqual(self.bridge.store.get(ISSUE)["stop_requested_at"], stopped["stop_requested_at"])
            with self.bridge.kanban.conn() as conn:
                self.assertFalse(kb.claim_task(conn, task, claimer=kb._claimer_id()))
            self.clock.now += 1
            self.deliver(self.linear.session_event("prompted", ISSUE, "s-1", body="Now resume explicitly"))
            self.assertEqual(self.bridge.kanban.get(task).status, "ready")
            self.assertEqual(self.task_id(), task)

    def test_recovery_deduplicates_completed_followups_before_pending_ingress_replay(self):
        from linear_ingress_fixture import IngressStore, Route
        self.bind_identity()
        inbox = self.dir / "queued-followup-ingress.db"
        producer = IngressStore(inbox)
        route = Route("alpha", "alpha", "/webhook/alpha", self.dir / "unused-secret", inbox)
        with self.stopped_worker() as (process, task, first):
            second = self.linear.session_event("prompted", ISSUE, "s-1", body="Use the updated scope", activity_id="act-2")
            for delivery, event in (("first", first), ("second", second)):
                producer.enqueue(route, delivery, json.dumps(event).encode())
            with sqlite3.connect(inbox) as db:
                db.execute("UPDATE deliveries SET received_at=?", (int(self.clock()),))
            self.bridge.drain_ingress(inbox)
            process.terminate(); process.wait(timeout=10)
            self.bridge = self.make_bridge()
            self.bind_identity()
            self.bridge.recover()
            self.bridge.tick(inbox)
            self.bridge = self.make_bridge()
            self.bind_identity()
            self.bridge.recover()
            self.bridge.tick(inbox)
            with self.bridge.kanban.conn() as conn:
                notes = "\n".join(c.body for c in kb.list_comments(conn, task))
            self.assertEqual(notes.count("Resume this task"), 1)
            self.assertEqual(notes.count("Use the updated scope"), 1)
            self.assertEqual(self.bridge.kanban.get(task).status, "ready")
            self.assertEqual(self.task_id(), task)
            with sqlite3.connect(inbox) as db:
                self.assertEqual(db.execute("SELECT status FROM deliveries ORDER BY delivery_id").fetchall(),
                                 [("imported",), ("imported",)])

    def test_newer_prompt_after_uncertain_comment_commit_preserves_each_instruction_once(self):
        from unittest.mock import patch
        with self.stopped_worker() as (process, task, first):
            process.terminate(); process.wait(timeout=10)
            add = kb.add_comment
            def lose_reply(*args, **kwargs):
                add(*args, **kwargs)
                raise RuntimeError("reply lost after core comment commit")
            with patch.object(kb, "add_comment", side_effect=lose_reply):
                with self.assertRaises(RuntimeError): self.deliver(first)
            self.clock.now += 1
            second = self.linear.session_event("prompted", ISSUE, "s-1", body="Use the updated scope")
            self.deliver(second)
            self.bridge = self.make_bridge()
            self.bridge.recover()
            self.deliver(second)
            with self.bridge.kanban.conn() as conn:
                notes = "\n".join(c.body for c in kb.list_comments(conn, task))
            self.assertEqual(notes.count("Resume this task"), 1)
            self.assertEqual(notes.count("Use the updated scope"), 1)
            self.assertEqual(self.bridge.kanban.get(task).status, "ready")

    def test_newer_pending_prompt_preserves_both_instructions_without_retry_duplicates(self):
        from hermes_fleet_linear_plugin.api import LinearError
        for exit_before_prompt in (False, True):
            with self.subTest(exit_before_prompt=exit_before_prompt):
                self.setUp()
                with self.stopped_worker() as (process, task, first):
                    with self.assertRaises(LinearError): self.deliver(first)
                    if exit_before_prompt: process.terminate(); process.wait(timeout=10)
                    self.clock.now += 1
                    second = self.linear.session_event("prompted", ISSUE, "s-1", body="Use the updated scope")
                    if exit_before_prompt:
                        self.deliver(second)
                    else:
                        for _ in range(2):
                            with self.assertRaises(LinearError): self.deliver(second)
                        self.bridge = self.make_bridge()
                        self.bridge.recover()
                        saved = json.loads(self.bridge.store.get(ISSUE)["pending_resume"])
                        self.assertEqual(saved["note"].count("Resume this task"), 1)
                        self.assertEqual(saved["note"].count("Use the updated scope"), 1)
                        process.terminate(); process.wait(timeout=10)
                        self.bridge.recover()
                    self.deliver(first)  # An older ingress retry cannot overwrite the later instruction.
                    self.assertEqual(self.bridge.kanban.get(task).status, "ready")
                    self.assertIsNone(self.bridge.store.get(ISSUE)["pending_resume"])
                    with self.bridge.kanban.conn() as conn:
                        notes = "\n".join(comment.body for comment in kb.list_comments(conn, task))
                    self.assertEqual(notes.count("Resume this task"), 1)
                    self.assertEqual(notes.count("Use the updated scope"), 1)

    def test_recycled_stopped_worker_pid_does_not_hold_resume_or_signal_stranger(self):
        from unittest.mock import patch
        with self.stopped_worker() as (process, task, event):
            with patch.object(dispatch, "_process_fingerprint", return_value="different-instance|0"):
                self.deliver(event)
            self.assertEqual(self.bridge.kanban.get(task).status, "ready")
            self.assertIsNone(process.poll())

    def test_unreadable_stopped_worker_identity_keeps_resume_pending(self):
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        with self.stopped_worker() as (process, task, event):
            with patch.object(dispatch, "_process_fingerprint", return_value=None):
                with self.assertRaises(LinearError): self.deliver(event)
            self.assertEqual(self.bridge.kanban.get(task).status, "blocked")
            self.assertIsNotNone(self.bridge.store.get(ISSUE)["pending_resume"])
            self.assertIsNone(process.poll())

    def test_terminal_sweep_cannot_erase_live_worker_resume_witness(self):
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        with self.stopped_worker() as (process, task, event):
            with patch.object(dispatch, "_process_fingerprint", return_value=None):
                with self.bridge.kanban.conn() as conn:
                    conn.execute("UPDATE task_runs SET ended_at=? WHERE task_id=?",
                                 (int(time.time()) - 121, task))
                    dispatch.reap_terminal_workers(conn)
                    self.assertIsNone(conn.execute("SELECT worker_pid FROM task_runs WHERE task_id=?",
                                                   (task,)).fetchone()[0])
                with self.assertRaises(LinearError): self.deliver(event)
                self.bridge = self.make_bridge()
                self.bridge.recover()
            self.assertEqual(self.bridge.kanban.get(task).status, "blocked")
            self.assertIsNotNone(self.bridge.store.get(ISSUE)["pending_resume"])
            self.assertIsNone(process.poll())
            with self.bridge.kanban.conn() as conn:
                self.assertFalse(kb.claim_task(conn, task, claimer=kb._claimer_id()))
            process.terminate(); process.wait(timeout=10)
            self.bridge.recover()
            self.assertEqual(self.bridge.kanban.get(task).status, "ready")

    def test_fluctuating_worker_identity_cannot_release_a_live_stopped_worker(self):
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        with self.stopped_worker() as (process, task, event):
            with self.bridge.kanban.conn() as conn:
                fingerprint = conn.execute("SELECT worker_started_at FROM task_runs WHERE task_id=? AND worker_pid=?",
                                           (task, process.pid)).fetchone()[0]
            with patch.object(dispatch, "_process_fingerprint", side_effect=[fingerprint, None]) as probe:
                with self.assertRaises(LinearError): self.deliver(event)
                self.assertEqual(probe.call_count, 1)
            with patch.object(dispatch, "_process_fingerprint", side_effect=[fingerprint, None]) as probe:
                with self.assertRaises(LinearError): self.bridge.kanban.unblock(task)
                self.assertEqual(probe.call_count, 1)
            self.assertEqual(self.bridge.kanban.get(task).status, "blocked")
            self.assertIsNotNone(self.bridge.store.get(ISSUE)["pending_resume"])
            self.assertIsNone(process.poll())
            process.terminate(); process.wait(timeout=10)
            self.bridge.recover()
            self.assertEqual(self.bridge.kanban.get(task).status, "ready")


    def test_task_creation_crash_recovers_mapping_and_existing_terminal_evidence_once(self) -> None:
        from unittest.mock import patch
        for outcome in ("evidence", "missing", "active"):
            with self.subTest(outcome=outcome):
                issue_id = f"creation-crash-{outcome}"
                self.linear.add_issue(issue_id, f"CRASH-{outcome}")
                self.linear.set_delegate(issue_id, {"id": SELF, "name": "This Agent"})
                event = self.linear.session_event("created", issue_id, f"creation-{outcome}")
                with patch.object(self.bridge.store, "put", side_effect=RuntimeError("interrupted after core create")):
                    with self.assertRaisesRegex(RuntimeError, "after core create"):
                        self.bridge.handle_webhook(event)
                self.assertIsNone(self.bridge.store.get(issue_id))
                with self.bridge.kanban.conn() as conn:
                    task = conn.execute("SELECT id FROM tasks WHERE idempotency_key=?",
                                        (f"linear:{issue_id}:creation-{outcome}",)).fetchone()[0]
                    if outcome != "active":
                        summary = "Persisted result https://docs.example/durable-evidence" if outcome == "evidence" else "No evidence"
                        self.assertTrue(kb.complete_task(conn, task, summary=summary, metadata={}))
                self.bridge = self.make_bridge()
                self.bridge.recover()
                self.bridge.handle_webhook(event)
                self.bridge.tick()
                expected = {"evidence": "Done", "missing": "Blocked", "active": "In Progress"}[outcome]
                self.assertEqual(self.linear.state(issue_id), expected)
                receipts = [a for a in self.linear.activities if a["agentSessionId"] == f"creation-{outcome}"]
                self.assertEqual(len(receipts), 1)
                self.assertEqual(receipts[0]["content"]["type"], {"evidence": "response", "missing": "error", "active": "thought"}[outcome])
                if outcome == "evidence":
                    self.assertIn("https://docs.example/durable-evidence", receipts[0]["content"]["body"])
                    self.assertTrue(any("https://docs.example/durable-evidence" in u["body"] for u in self.linear.project_updates))
                if outcome == "active":
                    self.assertEqual(self.bridge.store.get(issue_id)["task_id"], task)
                else:
                    self.assertIsNone(self.bridge.store.get(issue_id))
                before = len(self.tasks()), len(self.linear.activities), len(self.linear.project_updates)
                self.bridge = self.make_bridge()
                self.bridge.handle_webhook(event)
                self.bridge.tick()
                self.assertEqual((len(self.tasks()), len(self.linear.activities), len(self.linear.project_updates)), before)


    def test_terminal_pre_send_resolution_failure_can_retry(self) -> None:
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        with patch("hermes_cli.kanban_pr_acceptance.collect_acceptance",
                   return_value={"ok": True, "head_sha": "a" * 40}):
            self.assertTrue(self.chat("start")["ok"])
            result = chat.handle(self.bridge, {"action": "done", "issue": "ABC-1",
                                "evidence": "https://github.com/example/repo/pull/8"},
                                 Context("chat-key", "chat-key-id"))
            self.assertTrue(json.loads(result)["ok"])
            with patch.object(self.bridge, "_effect_issue",
                              side_effect=LinearError("temporary lookup failure", retryable=True)):
                self.bridge.flush()
            self.assertEqual(self.linear.state(ISSUE), "In Progress")
            self.clock.now += 61
            self.bridge.flush()
            self.assertEqual(self.linear.state(ISSUE), "Done")

    def test_retryable_pre_send_then_changed_pr_head_fails_without_reconciliation(self) -> None:
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        url = "https://github.com/example/repo/pull/8"
        with patch("hermes_cli.kanban_pr_acceptance.collect_acceptance",
                   return_value={"ok": True, "head_sha": "a" * 40}) as check:
            self.assertTrue(self.chat("start")["ok"])
            with patch.object(self.bridge, "flush", return_value=0):
                self.assertTrue(self.chat("done", evidence=url)["ok"])
            terminal = next(r for r in self.bridge.store.pending(ISSUE)
                            if r["kind"] == "status" and r["payload"].get("terminal"))
            with patch.object(self.bridge, "_effect_issue",
                              side_effect=LinearError("pre-send issue read failed", retryable=True)):
                self.bridge.flush()
            unsent = self.bridge.store.outbox_row(terminal["id"])
            self.assertEqual(unsent["attempts"], 1)
            self.assertIs(unsent["payload"]["write_started"], False)
            self.assertFalse(self.bridge.store.issue_reconciliation_blocked(ISSUE))
            check.return_value = {"ok": True, "head_sha": "b" * 40}
            self.clock.now += 61
            self.bridge.flush()
            rejected = self.bridge.store.outbox_row(terminal["id"])
            self.assertEqual(rejected["state"], "failed")
            self.assertIs(rejected["payload"]["write_started"], False)
            self.assertNotIn("reconcile_required", rejected["payload"])
            self.assertFalse(self.bridge.store.issue_reconciliation_blocked(ISSUE))
            self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), 1)  # initial claim only

    def test_bound_update_preflight_query_failure_is_known_unsent_and_retries(self) -> None:
        from unittest.mock import patch
        from hermes_fleet_linear_plugin.api import LinearError
        self.bind_identity()
        self.bridge.api.identity["teams"] = ["team-1"]
        self.assertTrue(self.chat("start")["ok"])
        with patch.object(self.bridge, "flush", return_value=0):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        terminal = next(r for r in self.bridge.store.pending(ISSUE)
                        if r["kind"] == "status" and r["payload"].get("terminal"))
        mutations = sum("IssueUpdate" in q for q in self.linear.requests)
        with patch.object(self.bridge.api, "_mutation_issue",
                          side_effect=LinearError("pre-send scope query failed", retryable=True)):
            self.bridge.flush()
        unsent = self.bridge.store.outbox_row(terminal["id"])
        self.assertEqual(unsent["attempts"], 1)
        self.assertIs(unsent["payload"]["write_started"], False)
        self.assertFalse(self.bridge.store.issue_reconciliation_blocked(ISSUE))
        self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), mutations)
        self.clock.now += 61
        self.bridge.flush()
        self.assertEqual(self.bridge.store.outbox_row(terminal["id"])["state"], "sent")
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), mutations + 1)

    def test_terminal_transport_requires_durable_started_marker(self) -> None:
        from unittest.mock import patch
        self.assertTrue(self.chat("start")["ok"])
        with patch.object(self.bridge, "flush", return_value=0):
            self.assertTrue(self.chat("done", evidence="https://docs.example/findings/1")["ok"])
        terminal = next(r for r in self.bridge.store.pending(ISSUE)
                        if r["kind"] == "status" and r["payload"].get("terminal"))
        mutations = sum("IssueUpdate" in q for q in self.linear.requests)
        with patch.object(self.bridge.store, "admit_mutation", return_value=False):
            self.bridge.flush()
        self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), mutations)
        self.assertIs(self.bridge.store.outbox_row(terminal["id"])["payload"]["write_started"], False)
        original = self.bridge.api.transport
        observed = []

        def inspect_marker(url, body, headers):
            if "mutation IssueUpdate" in json.loads(body)["query"]:
                observed.append(self.bridge.store.outbox_row(terminal["id"])["payload"]["write_started"])
            return original(url, body, headers)

        self.bridge.api.transport = inspect_marker
        self.bridge.flush()
        self.assertEqual(observed, [True])
        self.assertEqual(self.bridge.store.outbox_row(terminal["id"])["state"], "sent")

    def test_terminal_admission_commit_is_the_scope_linearization(self) -> None:
        from contextlib import contextmanager
        from unittest.mock import patch
        row_id = self.bridge.store.enqueue("status", {"issue_id": ISSUE, "terminal": True,
                                                       "write_started": False}, at=self.clock())
        store = self.bridge.store
        original_tx = store._tx
        fenced = threading.Event()

        @contextmanager
        def fence_after_commit():
            with original_tx() as db:
                yield db
            # A second connection fences immediately after the marker transaction commits.
            # Admission that already committed must have no later denying transaction.
            if not fenced.is_set():
                with sqlite3.connect(store.path) as db:
                    payload = db.execute("SELECT payload FROM outbox WHERE id=?", (row_id,)).fetchone()[0]
                if json.loads(payload)["write_started"] is True:
                    Store(store.path).fence_scope(ISSUE, "synthetic concurrent denial", at=self.clock())
                    fenced.set()

        before = sum("IssueUpdate" in q for q in self.linear.requests)
        with patch.object(store, "_tx", fence_after_commit):
            self.bridge._mutate_outbox(row_id, lambda: self.bridge.api.update_issue(ISSUE, {}), terminal=True)
        self.assertTrue(fenced.is_set())
        self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), before + 1)
        self.assertIs(store.outbox_row(row_id)["payload"]["write_started"], True)

    def test_terminal_admission_denial_and_failed_marker_never_transport(self) -> None:
        from hermes_fleet_linear_plugin.bridge import ProjectUpdateDeferred
        row_id = self.bridge.store.enqueue("status", {"issue_id": ISSUE, "terminal": True,
                                                       "write_started": False}, at=self.clock())
        before = sum("IssueUpdate" in q for q in self.linear.requests)
        self.bridge.store.fence_scope(ISSUE, "synthetic denial", at=self.clock())
        with self.assertRaises(ProjectUpdateDeferred):
            self.bridge._mutate_outbox(row_id, lambda: self.bridge.api.update_issue(ISSUE, {}), terminal=True)
        self.assertIs(self.bridge.store.outbox_row(row_id)["payload"]["write_started"], False)
        self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), before)

        other_id = self.bridge.store.enqueue("status", {"issue_id": "other-issue", "terminal": True,
                                                         "write_started": False}, at=self.clock())
        with sqlite3.connect(self.bridge.store.path) as db:
            db.execute("CREATE TRIGGER reject_marker BEFORE UPDATE OF payload ON outbox "
                       "WHEN NEW.id='" + other_id + "' AND json_extract(NEW.payload, '$.write_started')=1 "
                       "BEGIN SELECT RAISE(ABORT, 'marker failed'); END")
        with self.assertRaises(sqlite3.DatabaseError):
            self.bridge._mutate_outbox(other_id, lambda: self.bridge.api.update_issue(ISSUE, {}), terminal=True)
        self.assertIs(self.bridge.store.outbox_row(other_id)["payload"]["write_started"], False)
        self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), before)

    def test_stalled_mutation_transport_does_not_hold_store_writer(self) -> None:
        row_id = self.bridge.store.enqueue("status", {"issue_id": ISSUE, "terminal": True,
                                                       "write_started": False}, at=self.clock())
        entered, release = threading.Event(), threading.Event()
        put_started, put_done = threading.Event(), threading.Event()
        failures = []
        original_transport = self.bridge.api.transport

        def stalled_transport(url, body, headers):
            entered.set()
            if not release.wait(10):
                raise AssertionError("test transport was not released")
            return original_transport(url, body, headers)

        def send():
            try:
                self.bridge._mutate_outbox(row_id, lambda: self.bridge.api.update_issue(ISSUE, {}), terminal=True)
            except BaseException as exc:
                failures.append(exc)

        def put_unrelated():
            put_started.set()
            try:
                if not Store(self.bridge.store.path).put("unrelated", "chat", "synthetic-owner"):
                    raise AssertionError("unrelated work refused")
            except BaseException as exc:
                failures.append(exc)
            finally:
                put_done.set()

        self.bridge.api.transport = stalled_transport
        sender = threading.Thread(target=send)
        writer = threading.Thread(target=put_unrelated)
        sender.start()
        try:
            self.assertTrue(entered.wait(5), "mutation did not reach transport")
            writer.start()
            self.assertTrue(put_started.wait(5), "unrelated writer did not start")
            completed_before_release = put_done.wait(2)
        finally:
            release.set()
            sender.join(10)
            if writer.ident is not None:
                writer.join(10)
            self.bridge.api.transport = original_transport
        self.assertTrue(completed_before_release, "SQLite writer stayed locked through HTTP")
        self.assertFalse(sender.is_alive() or writer.is_alive())
        self.assertEqual(failures, [])
        self.assertIs(self.bridge.store.outbox_row(row_id)["payload"]["write_started"], True)

    def test_uncertain_done_never_replays_after_a_human_reopens(self) -> None:
        from unittest.mock import patch
        url = "https://github.com/example/repo/pull/8"
        with patch("hermes_cli.kanban_pr_acceptance.collect_acceptance",
                   return_value={"ok": True, "head_sha": "a" * 40}):
            self.assertTrue(self.chat("start")["ok"])
            self.linear.lose_next_response = True
            self.assertTrue(self.chat("done", evidence=url)["ok"])
            self.assertEqual(self.linear.state(ISSUE), "Done")
            terminal = next(r for r in self.bridge.store.pending(ISSUE)
                            if r["kind"] == "status" and r["payload"].get("state") == "done")
            self.assertTrue(terminal["payload"]["write_started"])
            self.clock.now += 1
            self.linear.set_state(ISSUE, "In Progress")
            mutations = sum("IssueUpdate" in q for q in self.linear.requests)
            self.clock.now += 60
            self.bridge.flush()
            self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), mutations)
            self.assertEqual(self.linear.state(ISSUE), "In Progress")
            held = self.bridge.store.outbox_row(terminal["id"])
            self.assertEqual(held["state"], "failed")
            self.assertTrue(held["payload"]["reconcile_required"])
            self.assertTrue(held["payload"]["write_started"])
            self.assertTrue(any(r["payload"].get("requires_status_id") == terminal["id"]
                                for r in self.bridge.store.pending(ISSUE)))
            self.bridge = self.make_bridge()
            self.bridge.recover()
            self.clock.now += 61
            self.bridge.tick()
            self.assertEqual(sum("IssueUpdate" in q for q in self.linear.requests), mutations)
            self.assertEqual(self.linear.state(ISSUE), "In Progress")
            self.assertTrue(self.bridge.store.outbox_row(terminal["id"])["payload"]["reconcile_required"])

    def test_delayed_terminal_delivery_revalidates_recorded_pr_head_and_keeps_uncertain_send(self) -> None:
        from unittest.mock import patch
        url = "https://github.com/example/repo/pull/8"
        for origin, change in (("chat", "checks"), ("kanban", "head"), ("chat", "same"),
                               ("kanban", "same"), ("chat", "uncertain"), ("kanban", "uncertain"),
                               ("chat", "legacy"), ("kanban", "legacy")):
            with self.subTest(origin=origin, change=change):
                issue_id, session = f"pr-{origin}-{change}", f"session-{origin}-{change}"
                self.linear.add_issue(issue_id, f"PR-{origin}-{change}")
                self.linear.set_delegate(issue_id, {"id": SELF, "name": "This Agent"})
                if origin == "chat":
                    self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "start", "issue": issue_id},
                                                          Context(session, session)))["ok"])
                    self.bridge.tick()
                else:
                    self.deliver(self.linear.session_event("created", issue_id, session))
                accepted = {"ok": True, "head_sha": "a" * 40}
                with patch("hermes_cli.kanban_pr_acceptance.collect_acceptance", return_value=accepted) as check:
                    if origin == "chat":
                        result = json.loads(chat.handle(self.bridge, {"action": "done", "issue": issue_id,
                                                                     "evidence": url}, Context(session, session)))
                        self.assertTrue(result["ok"])
                    else:
                        task = self.bridge.store.get(issue_id)["task_id"]
                        with self.bridge.kanban.conn() as conn:
                            kb.complete_task(conn, task, result=f"PR {url}")
                        self.bridge.pump_kanban()
                    terminal = next(r for r in self.bridge.store.pending(issue_id) if r["kind"] == "status")
                    if change == "legacy":
                        payload = dict(terminal["payload"])
                        for field in ("pr_heads", "evidence", "evidence_contract"):
                            payload.pop(field, None)
                        self.bridge.store.rewrite(terminal["id"], payload, terminal["next_at"])
                    if change == "uncertain":
                        self.linear.lose_next_response = True  # apply, then lose the mutation response
                        self.bridge.flush()
                        self.clock.now += 61
                    check.return_value = ({"ok": False, "head_sha": "a" * 40} if change in ("checks", "uncertain") else
                                          {"ok": True, "head_sha": "b" * 40} if change == "head" else accepted)
                    self.bridge.flush()
                    expected = "Done" if change in ("same", "uncertain") else "In Progress"
                    self.assertEqual(self.linear.state(issue_id), expected)
                    if change == "legacy":
                        status = self.bridge.store.outbox_row(terminal["id"])
                        self.assertTrue(status["payload"]["reconcile_required"])
                        self.assertEqual(status["state"], "failed")
                        self.assertTrue(any(r["payload"].get("requires_status_id") == terminal["id"]
                                            for r in self.bridge.store.pending(issue_id)))
                        continue  # absent historical heads must be reconciled, never inferred from current checks
                    self.assertGreaterEqual(check.call_count, 2)
                    status = self.bridge.store.outbox_row(terminal["id"])
                    self.assertEqual(status["payload"]["pr_heads"], {url: "a" * 40})
                    self.assertEqual(status["payload"]["evidence_contract"], "local-only")
                    if change == "uncertain":
                        self.assertEqual(status["state"], "failed")
                        self.assertTrue(status["payload"]["reconcile_required"])
                        self.bridge = self.make_bridge()
                        self.bridge.tick()
                        self.assertEqual(self.bridge.store.outbox_row(terminal["id"])["state"], "failed")
                    if change != "same":
                        self.assertFalse(any(c["issueId"] == issue_id and c["body"].startswith("Done")
                                             for c in self.linear.comments))
                        self.assertFalse(any(a.get("issueId") == issue_id and a["content"]["body"].startswith("Done")
                                             for a in self.linear.activities))


    def test_chat_followup_ordering_replay_and_failed_or_uncertain_injection_are_durable(self) -> None:
        from unittest.mock import patch
        from linear_ingress_fixture import IngressStore, Route
        self.bind_identity()
        self.assertTrue(self.chat("start")["ok"])
        self.deliver(self.linear.session_event("created", ISSUE, "chat-linear", creator=SELF))
        self.clock.now += 1
        old = self.linear.session_event("prompted", ISSUE, "chat-linear", body="OLD destination A", activity_id="old")
        self.clock.now += 1
        new = self.linear.session_event("prompted", ISSUE, "chat-linear", body="NEW destination B", activity_id="new")
        self.deliver(new)
        self.deliver(old)
        self.assertFalse(any("OLD destination A" in body for _, body in self.injected))
        self.assertEqual(self.bridge.store.get(ISSUE)["last_updated_at"], new["webhookTimestamp"])
        count = len(self.injected)
        self.bridge = self.make_bridge()
        self.bind_identity()
        self.deliver(new)
        self.assertEqual(len(self.injected), count)

        inbox = self.dir / "chat-followup-ingress.db"
        producer = IngressStore(inbox)
        route = Route("alpha", "alpha", "/webhook/alpha", self.dir / "unused-secret", inbox)
        self.clock.now += 1
        retry = self.linear.session_event("prompted", ISSUE, "chat-linear", body="Retry destination C", activity_id="retry")
        producer.enqueue(route, "retry", json.dumps(retry).encode())
        with sqlite3.connect(inbox) as db:
            db.execute("UPDATE deliveries SET received_at=?", (int(self.clock()),))
        self.inject_ok = False  # a definitive refusal is safe to retry
        self.bridge.tick(inbox)
        with sqlite3.connect(inbox) as db:
            self.assertEqual(db.execute("SELECT status FROM deliveries WHERE delivery_id='retry'").fetchone()[0], "pending")
        self.assertEqual(self.bridge.store.get(ISSUE)["last_updated_at"], new["webhookTimestamp"])
        self.bridge = self.make_bridge()
        self.bind_identity()
        self.inject_ok = True
        self.bridge.tick(inbox)
        self.assertEqual(self.bridge.store.get(ISSUE)["last_updated_at"], retry["webhookTimestamp"])
        with sqlite3.connect(inbox) as db:
            self.assertEqual(db.execute("SELECT status FROM deliveries WHERE delivery_id='retry'").fetchone()[0], "imported")
        count = len(self.injected)
        self.deliver(retry)
        self.assertEqual(len(self.injected), count)

        self.clock.now += 1
        unknown = self.linear.session_event("prompted", ISSUE, "chat-linear", body="Uncertain destination D", activity_id="unknown")
        producer.enqueue(route, "unknown", json.dumps(unknown).encode())
        with sqlite3.connect(inbox) as db:
            db.execute("UPDATE deliveries SET received_at=? WHERE delivery_id='unknown'", (int(self.clock()),))
        def interrupted(key, body):
            self.injected.append((key, body))
            raise RuntimeError("response lost after chat accepted")
        with patch.object(self.bridge, "inject", side_effect=interrupted):
            self.bridge.tick(inbox)
        count = len(self.injected)
        self.bridge = self.make_bridge()
        self.bind_identity()
        self.bridge.tick(inbox)
        self.assertEqual(len(self.injected), count)  # uncertain injection must not be repeated
        self.assertEqual(self.bridge.store.get(ISSUE)["last_updated_at"], retry["webhookTimestamp"])
        self.assertTrue(any("unknown" in a["content"]["body"].lower() for a in self.linear.activities))
        self.clock.now += 86_401
        self.bridge.tick(inbox)
        with sqlite3.connect(inbox) as db:
            self.assertEqual(db.execute("SELECT status FROM deliveries WHERE delivery_id='unknown'").fetchone()[0], "failed")
        self.clock.now += 1
        self.deliver(self.linear.session_event("prompted", ISSUE, "chat-linear", body="Fresh explicit destination E", activity_id="fresh"))
        self.assertIn("Fresh explicit destination E", self.injected[-1][1])
        self.assertEqual(self.bridge.store.get(ISSUE)["last_updated_at"], self.clock() * 1000)


if __name__ == "__main__":
    unittest.main()
