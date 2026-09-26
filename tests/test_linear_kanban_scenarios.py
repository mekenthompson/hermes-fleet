"""Integration scenarios for plugins/linear: a fake Linear over HTTP and a REAL Kanban board.

Needs the pinned Hermes Agent source (and its Python deps) on the path:
    HERMES_AGENT_SRC=/path/to/hermes-agent <agent-venv>/bin/python -m unittest tests.test_linear_kanban_scenarios
Run it in its own process: older Linear tests stub `agent.*` modules in-process.
Skipped when the Agent source is unavailable (public CI has no Agent checkout).
One test per rule in the rebuild plan's flows 1-10.
"""
from __future__ import annotations

import json
import os
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
    def __init__(self, session_key: str, session_id: str) -> None:
        self.session_key, self.session_id = session_key, session_id


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
        self.complete(task_id, "Shipped the fix: https://git.example/org/repo/pull/7")
        self.assertEqual(self.linear.state(ISSUE), "Done")
        self.assertEqual(self.types(), ["thought", "response"])
        self.assertIn("https://git.example/org/repo/pull/7", self.linear.activities[-1]["content"]["body"])
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

    def test_done_without_evidence_is_unfinished(self) -> None:
        self.delegate()
        self.complete(self.task_id(), "All good, trust me")
        self.assertEqual(self.linear.state(ISSUE), "Blocked")
        self.assertEqual(self.types(), ["thought", "error"])
        self.assertIn("unfinished", self.linear.activities[-1]["content"]["body"])

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
        self.chat("done", evidence="https://git.example/org/repo/pull/9", note="Merged")
        self.assertEqual(self.linear.project_updates, [])
        self.clock.now += 31 * 60
        self.bridge.tick()
        self.bridge.tick()
        self.assertEqual(len(self.linear.project_updates), 1)
        body = self.linear.project_updates[0]["body"]
        self.assertIn("ABC-1: Done", body)
        self.assertIn("ABC-2: In progress", body)

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
        self.complete(self.task_id(), "PR: https://git.example/org/repo/pull/3")
        self.assertEqual(self.linear.state(ISSUE), "In Review")
        self.assertEqual(self.bridge.store.pending(), [])


if __name__ == "__main__":
    unittest.main()
