"""Integration scenarios for plugins/linear: a fake Linear over HTTP and a REAL Kanban board.

Needs the pinned Hermes Agent source (and its Python deps) on the path:
    HERMES_AGENT_SRC=/path/to/hermes-agent <agent-venv>/bin/python -m unittest tests.test_linear_kanban_scenarios
Run it in its own process: older Linear tests stub `agent.*` modules in-process.
Skipped when the Agent source is unavailable (public CI has no Agent checkout).
One test per rule in the rebuild plan's flows 1-10.
"""
from __future__ import annotations

import json
import asyncio
import os
import sqlite3
import sys
import tempfile
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

    def types(self) -> list[str]:
        return [a["content"]["type"] for a in self.linear.activities]

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
                self.assertEqual(len(self.linear.activities), before + (0 if kind in ("unblocked", "archived") else 1))

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
            for boundary in ("status", "comment", "project_update", "project_update_insert", "delete"):
                with self.subTest(action=action, boundary=boundary):
                    self.setUp()
                    self.assertTrue(self.chat("start")["ok"])
                    if boundary == "project_update_insert":
                        with sqlite3.connect(self.bridge.store.path) as db:
                            db.execute("DELETE FROM outbox WHERE kind = 'project_update'")
                    before = [(p["id"], p["payload"]) for p in self.bridge.store.pending()]
                    with sqlite3.connect(self.bridge.store.path) as db:
                        if boundary == "delete":
                            db.execute("CREATE TRIGGER fail_boundary BEFORE DELETE ON work BEGIN SELECT RAISE(FAIL, 'fault'); END")
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
        self.linear.down = True
        self.delegate()
        self.clock.now += 600
        self.linear.set_state(ISSUE, "Canceled")  # a human cancels while our claim is still queued
        self.linear.down = False
        self.clock.now += 3600
        self.bridge.tick()
        self.assertEqual(self.linear.state(ISSUE), "Canceled")
        self.assertEqual([s for _, s in self.tasks()], ["archived"])
        self.assertIsNone(self.bridge.store.get(ISSUE))

    def test_queued_claim_never_undoes_a_later_delegate_removal(self) -> None:
        self.linear.down = True
        self.delegate()
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

    def test_credential_outage_keeps_the_delegation(self) -> None:
        def broken() -> str:
            raise RuntimeError("Connect unreachable")

        self.bridge.api.token = broken
        self.delegate()
        self.assertEqual(len(self.tasks()), 1)  # the delegation is not dropped
        self.bridge.api.token = lambda: "synthetic-token"
        self.clock.now += 120
        self.bridge.tick()
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

    def test_day_long_outage_catches_up_without_duplicates(self) -> None:
        self.linear.down = True
        self.delegate()
        self.assertEqual(len(self.tasks()), 1)  # work starts even while Linear is unreachable
        task_id = self.task_id()
        self.complete(task_id, "Deployed and checked: https://status.example/check/1")
        for _ in range(23):
            self.clock.now += 3600
            self.bridge.tick()
        self.assertEqual(self.linear.activities, [])
        self.linear.down = False
        self.linear.lose_next_response = True  # a write lands but its response is lost
        for _ in range(6):
            self.clock.now += 3600
            self.bridge.tick()
        self.assertEqual(self.types(), ["thought", "response"])
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertEqual(len(self.linear.project_updates), 1)
        self.assertEqual(self.bridge.store.pending(), [])

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
        self.linear.rate_limited_until = self.clock() + 120
        self.delegate()
        calls = len(self.linear.requests)
        self.clock.now += 60
        self.bridge.tick()
        self.assertEqual(len(self.linear.requests), calls)  # paused: no calls before the reset time
        self.clock.now += 61
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
        update_issue = self.bridge.api.update_issue

        def fail_done(issue_id, fields):
            if issue_id == ISSUE and "stateId" in fields:
                raise LinearError("synthetic terminal outage")
            return update_issue(issue_id, fields)

        with patch.object(self.bridge.api, "update_issue", side_effect=fail_done):
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

        def end_turn_before_create(*args):
            chat.on_turn_end(self.bridge, "chat-key-id")
            return create(*args)

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


if __name__ == "__main__":
    unittest.main()
