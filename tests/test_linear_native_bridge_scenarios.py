"""Real upstream task producer/consumer tests; no fork lifecycle APIs or worker credentials.

Run with HERMES_AGENT_SRC pointing at the clean manifest-pinned Hermes checkout.
These backend tests do not claim Desktop, Telegram or Slack client round-trips.
"""
from __future__ import annotations

import copy
import os
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from linear_fake_api import load_plugin

SOURCE = os.environ.get("HERMES_AGENT_SRC", "")
CORE = None
try:
    if not SOURCE:
        raise ImportError("HERMES_AGENT_SRC is not set")
    sys.path.insert(0, SOURCE)
    from hermes_cli import kanban_db as kb
except Exception as exc:
    CORE = str(exc)

plugin = load_plugin()
from hermes_fleet_linear_plugin.api import LinearError
from hermes_fleet_linear_plugin.bridge import Bridge, Kanban
from hermes_fleet_linear_plugin.store import Store
from test_linear_specialist_authorization import Authority, ISSUE, SESSION, ACTIVITY, USER, VIEWER


@unittest.skipIf(CORE is not None, f"native upstream source unavailable ({CORE})")
class NativeBridgeScenarios(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="linear-native-")
        self.addCleanup(temp.cleanup)
        self.home = Path(temp.name)
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith("HERMES_KANBAN_") and key != "HERMES_DELEGATED_CHILD_CONTEXT"}
        environment.update(HERMES_HOME=str(self.home), HERMES_KANBAN_HOME=str(self.home / "board"), HERMES_PROFILE="default")
        self.env = patch.dict(os.environ, environment, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.authority = Authority()
        self.authority.sessions[SESSION]["appUser"] = {"id": VIEWER}
        self.authority.issues[ISSUE]["delegate"] = {"id": VIEWER}
        self.board = Kanban(profile="default", profile_home=self.home)
        self.store = Store(self.home / "linear.db")
        self.bridge = self.make_bridge()

    def make_bridge(self):
        return Bridge(Store(self.home / "linear.db"), self.authority.api(scope=None), self.board, profile="default")

    def event(self, action="created", *, body="continue the bounded investigation", stamp=None, signal=None):
        from datetime import datetime, timezone
        activity = {"id": ACTIVITY, "user": {"id": USER}, "content": {"body": body}}
        if signal:
            activity["signal"] = signal
        return {"type": "AgentSessionEvent", "action": action,
                "createdAt": datetime.fromtimestamp(stamp or time.time(), timezone.utc).isoformat(),
                "agentSession": {"id": SESSION, "issue": {"id": ISSUE}, "creatorId": USER},
                "agentActivity": activity}

    def create(self, *, activate=True):
        self.bridge.handle_webhook(self.event())
        task_id = self.store.get(ISSUE)["task_id"]
        status = self.board.get(task_id).status
        self.assertEqual(status, "ready" if self.board.guarded_controls else "blocked")
        if activate and status == "blocked":
            # Backend fixture uses the actual native operator path. This is not a
            # fabricated human transport event or an integration execution grant.
            with self.board.conn() as conn:
                self.assertTrue(kb.unblock_task(conn, task_id))
        return task_id

    def comments(self, task_id):
        with self.board.conn() as conn:
            return kb.list_comments_after(conn, task_id)

    def tasks(self):
        with self.board.conn() as conn:
            return list(conn.execute("SELECT id FROM tasks"))

    def test_assignment_duplicate_and_tracking_are_native_only(self):
        event = self.event()
        self.bridge.handle_webhook(event)
        task_id = self.store.get(ISSUE)["task_id"]
        before = self.board.get(task_id)
        self.assertEqual(before.status, "ready" if self.board.guarded_controls else "blocked")
        self.assertIsNone(before.current_run_id)
        self.bridge.handle_webhook(event)
        self.bridge.track(ISSUE, task_id)
        after = self.board.get(task_id)
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual((before.status, before.current_run_id, before.claim_lock),
                         (after.status, after.current_run_id, after.claim_lock))
        self.assertEqual(self.store.get(ISSUE)["origin"], "native")
        self.assertIsNone(self.store.get(ISSUE)["ownership_id"])
        self.assertFalse(hasattr(self.bridge, "inject"))

    def test_assignment_lost_create_response_reconciles_without_second_create(self):
        original = self.board.create
        def lost(**fields):
            self.assertIsNone(getattr(self.store._local, "db", None))
            original(**fields)
            raise ConnectionError("lost native response")
        event = self.event()
        with patch.object(self.board, "create", side_effect=lost) as send:
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(event)
            self.assertEqual(send.call_count, 1)
        restarted = self.make_bridge()
        with patch.object(self.board, "create", side_effect=AssertionError("must inspect instead")):
            restarted.handle_webhook(event)
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual(restarted.store.native_intent(f"create:{ISSUE}")["state"], "applied")

    def test_crash_before_create_stays_ambiguous_without_replay(self):
        event = self.event()
        with patch.object(self.board, "create", side_effect=ConnectionError("before native call")):
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(event)
        with patch.object(self.board, "create", side_effect=AssertionError("ambiguous intent cannot send")):
            self.make_bridge().handle_webhook(event)
        self.assertEqual(self.tasks(), [])
        self.assertEqual(self.store.native_intent(f"create:{ISSUE}")["state"], "reconcile")

    def test_simultaneous_assignment_requests_coalesce_before_native_create(self):
        entered, release = threading.Event(), threading.Event()
        original = self.board.create
        def slow(**fields):
            entered.set()
            self.assertTrue(release.wait(10))
            return original(**fields)
        event = self.event()
        with ThreadPoolExecutor(max_workers=2) as pool, patch.object(self.board, "create", side_effect=slow) as send:
            first = pool.submit(self.bridge.handle_webhook, copy.deepcopy(event))
            self.assertTrue(entered.wait(10))
            second = pool.submit(self.make_bridge().handle_webhook, copy.deepcopy(event))
            second.result(timeout=10)
            release.set()
            first.result(timeout=10)
        self.assertEqual(send.call_count, 1)
        self.assertEqual(len(self.tasks()), 1)

    def test_authoritative_wrong_actor_profile_and_reassignment_refuse_effects(self):
        event = self.event("prompted")
        event["agentActivity"]["user"] = {"id": "forged"}
        self.bridge.handle_webhook(event)
        self.assertEqual(self.tasks(), [])
        self.authority.issues[ISSUE]["delegate"] = {"id": "other-app"}
        self.bridge.handle_webhook(self.event())
        self.assertEqual(self.tasks(), [])
        with self.board.conn() as conn:
            foreign = kb.create_task(conn, title="foreign", assignee="other-profile")
        with self.assertRaises(LinearError):
            self.bridge.track(ISSUE, foreign)
        self.assertIsNone(self.store.get(ISSUE))

    def test_manual_native_external_key_collision_is_not_adopted_or_retired(self):
        with self.board.conn() as conn:
            existing = kb.create_task(conn, title="manual work", assignee="default", idempotency_key=f"linear:{ISSUE}")
        before = self.board.get(existing)
        with self.assertRaises(LinearError):
            self.bridge.handle_webhook(self.event())
        after = self.board.get(existing)
        self.assertEqual((before.status, before.current_run_id, before.assignee), (after.status, after.current_run_id, after.assignee))
        self.assertEqual(len(self.tasks()), 1)
        self.assertIsNone(self.store.get(ISSUE))
        # Discovery rejects the collision before any native create attempt.
        self.assertIsNone(self.store.native_intent(f"create:{ISSUE}"))

    def test_same_profile_followup_reaches_actual_native_injector_same_run(self):
        from tools import kanban_tools as kt
        task_id = self.create()
        with self.board.conn() as conn:
            task = kb.claim_task(conn, task_id, claimer="native-test-worker")
        self.assertIsNotNone(task)
        before = self.board.get(task_id)
        steers = []
        agent = SimpleNamespace(steer=lambda body: steers.append(body) or True)
        kt._comment_watermark.clear()
        kt._comment_poll_last_attempt = 0
        with patch.dict(os.environ, {"HERMES_KANBAN_TASK": task_id, "HERMES_KANBAN_RUN_ID": str(task.current_run_id)}):
            self.assertFalse(kt.inject_new_comments_from_env(agent))
            # Prove native same-profile own notes are skipped, rather than inventing a human author.
            with self.board.conn() as conn:
                kb.add_comment(conn, task_id, "default", "worker's own note")
            kt._comment_poll_last_attempt = 0
            self.assertFalse(kt.inject_new_comments_from_env(agent))
            self.bridge.followup(ISSUE, "use the verified v2 endpoint", "followup-one")
            kt._comment_poll_last_attempt = 0
            self.assertTrue(kt.inject_new_comments_from_env(agent))
            self.assertIn("linear:", steers[0])
            self.assertIn("verified v2 endpoint", steers[0])
            kt._comment_poll_last_attempt = 0
            self.assertFalse(kt.inject_new_comments_from_env(agent))
        after = self.board.get(task_id)
        self.assertEqual((before.current_run_id, before.claim_lock, before.assignee),
                         (after.current_run_id, after.claim_lock, after.assignee))
        self.assertEqual(len(steers), 1)

    def test_lost_followup_response_reconciles_actual_comment_and_dedupes(self):
        task_id = self.create()
        original = self.board.comment
        def lost(*args, **kwargs):
            self.assertIsNone(getattr(self.bridge.store._local, "db", None))
            original(*args, **kwargs)
            raise ConnectionError("native comment response lost")
        with patch.object(self.board, "comment", side_effect=lost):
            with self.assertRaises(ConnectionError):
                self.bridge.followup(ISSUE, "bounded follow-up", "one")
        restarted = self.make_bridge()
        with patch.object(self.board, "comment", side_effect=AssertionError("must inspect instead")):
            restarted.followup(ISSUE, "bounded follow-up", "one")
            restarted.followup(ISSUE, "bounded follow-up", "one")
        self.assertEqual(len(self.comments(task_id)), 1)
        with self.assertRaises(ValueError):
            restarted.followup(ISSUE, "different content", "one")

    def test_ambiguous_followup_without_comment_stays_held(self):
        task_id = self.create()
        with patch.object(self.board, "comment", side_effect=ConnectionError("before native comment")):
            with self.assertRaises(ConnectionError):
                self.bridge.followup(ISSUE, "bounded follow-up", "one")
        with patch.object(self.board, "comment", side_effect=AssertionError("cannot replay")):
            with self.assertRaises(LinearError):
                self.make_bridge().followup(ISSUE, "bounded follow-up", "one")
        self.assertEqual(self.comments(task_id), [])

    def test_stopped_completed_and_legacy_records_do_not_resume_or_create_increment(self):
        task_id = self.create()
        with self.board.conn() as conn:
            kb.block_task(conn, task_id, reason="native operator Stop", kind="needs_input")
        result = self.bridge.followup(ISSUE, "continue", "stopped")
        self.assertIn("stopped", result["message"])
        self.make_bridge().recover()
        self.assertEqual(self.board.get(task_id).status, "blocked")
        with self.board.conn() as conn:
            kb.complete_task(conn, task_id, result="Verified bounded investigation is complete")
        self.bridge.handle_webhook(self.event("prompted"))
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual(self.board.get(task_id).status, "done")
        self.assertEqual(self.comments(task_id), [])
        self.store.put("legacy", "chat", "retained-session")
        status = self.bridge.native_status("legacy")
        self.assertFalse(status["linked"])
        self.assertEqual(status["retained"]["origin"], "chat")
        self.assertFalse(self.bridge.native_status("unlinked")["linked"])

    def test_cursor_and_terminal_reporting_are_atomic_restart_safe(self):
        task_id = self.create()
        with self.board.conn() as conn:
            native_cursor = conn.execute("SELECT last_event_id FROM kanban_notify_subs WHERE task_id=?", (task_id,)).fetchone()[0]
            kb.block_task(conn, task_id, reason="human decision required", kind="needs_input")
        def crash(*args, **kwargs):
            raise RuntimeError("before plugin durable commit")
        pending_before = self.store.pending()
        with patch.object(self.bridge.store, "capture_event", side_effect=crash):
            with self.assertLogs("linear", level="WARNING") as held:
                self.bridge.pump_kanban()
        self.assertTrue(any("RuntimeError" in entry and task_id in entry for entry in held.output))
        self.assertFalse(any("before plugin durable commit" in entry for entry in held.output))
        self.assertEqual(self.store.pending(), pending_before)
        self.assertEqual(self.store.get(ISSUE)["last_event_id"], 0)
        # No native subscription notification cursor was consumed before the durable outbox.
        with self.board.conn() as conn:
            self.assertEqual(conn.execute("SELECT last_event_id FROM kanban_notify_subs WHERE task_id=?", (task_id,)).fetchone()[0], native_cursor)
        self.make_bridge().pump_kanban()
        cursor = self.store.get(ISSUE)["last_event_id"]
        self.assertGreater(cursor, 0)
        count = len(self.store.pending())
        self.make_bridge().pump_kanban()
        self.assertEqual(len(self.store.pending()), count)
        with self.board.conn() as conn:
            kb.complete_task(conn, task_id, result="Verified findings: https://example.test/findings")
        self.make_bridge().pump_kanban()
        self.assertGreater(self.store.get(ISSUE)["last_event_id"], cursor)
        terminal = [row for row in self.store.pending() if row["payload"].get("terminal")]
        status = next(row for row in terminal if row["kind"] == "status")
        self.assertTrue(all(row["payload"].get("requires_status_id") == status["id"] for row in terminal if row["kind"] != "status"))
        count = len(self.store.pending())
        self.make_bridge().recover()
        self.assertEqual(len(self.store.pending()), count)
        self.assertEqual(self.store.get(ISSUE)["task_id"], task_id)

    def test_unlinked_tracker_reports_comments_and_passive_subscriptions(self):
        with self.board.conn() as conn:
            task_id = kb.create_task(conn, title="native task", assignee="default")
        self.bridge.track(ISSUE, task_id)
        self.board.subscribe_parent(task_id, "default", {"platform": "api_server", "chat_id": "authorized-chat"})
        with self.board.conn() as conn:
            self.assertEqual(conn.execute("SELECT delivery_mode FROM kanban_notify_subs WHERE task_id=?", (task_id,)).fetchone()[0], "notify")
            kb.block_task(conn, task_id, reason="operator decision", kind="needs_input")
        self.bridge.pump_kanban()
        self.assertTrue(any(row["kind"] == "comment" for row in self.store.pending()))
        self.assertFalse(any(row["kind"] == "activity" for row in self.store.pending()))

    def test_specialist_native_completion_without_recommendation_stays_unfinished(self):
        from test_linear_specialist_authorization import SCOPE
        scoped = self.authority.api(scope={**SCOPE, "requester_basis": "agent_session"})
        self.bridge = Bridge(self.store, scoped, self.board, profile="default")
        task_id = self.create()
        self.assertIn("metadata.issue_comment", self.board.get(task_id).body)
        with self.board.conn() as conn:
            kb.complete_task(conn, task_id, result="Verified investigation with a sufficiently detailed summary")
        self.bridge.pump_kanban()
        terminal = [row for row in self.store.pending() if row["payload"].get("terminal")]
        self.assertEqual(next(row["payload"]["state"] for row in terminal if row["kind"] == "status"), "blocked")
        self.assertFalse(any(row["payload"].get("assessment") for row in terminal))
        self.assertTrue(all(row["payload"]["requester_sessions"] == {ISSUE: SESSION} for row in terminal))

    def test_specialist_native_assessment_is_durable_and_not_repeated(self):
        from test_linear_specialist_authorization import SCOPE
        scoped = self.authority.api(scope={**SCOPE, "requester_basis": "agent_session"})
        self.bridge = Bridge(self.store, scoped, self.board, profile="default")
        task_id = self.create()
        recommendation = "Recommendation: adopt the verified bounded change.\nThe native investigation supplied the supporting evidence."
        with self.board.conn() as conn:
            kb.complete_task(conn, task_id, result="Verified bounded specialist investigation", metadata={"issue_comment": recommendation})
        self.bridge.pump_kanban()
        assessment = [row for row in self.store.pending() if row["payload"].get("assessment")]
        self.assertEqual(len(assessment), 1)
        self.assertEqual(assessment[0]["payload"]["body"], recommendation)
        self.assertEqual(next(row["payload"]["state"] for row in self.store.pending() if row["kind"] == "status"), "done")
        count = len(self.store.pending())
        self.bridge.recover()
        self.assertEqual(len(self.store.pending()), count)

    def test_simultaneous_followups_share_native_run_and_keep_both_messages(self):
        task_id = self.create()
        with self.board.conn() as conn:
            native = kb.claim_task(conn, task_id, claimer="native-test-worker")
        with ThreadPoolExecutor(max_workers=2) as pool:
            requests = [pool.submit(self.make_bridge().followup, ISSUE, f"instruction {number}", f"followup-{number}")
                        for number in (1, 2)]
            for request in requests:
                request.result(timeout=10)
        self.assertEqual({comment.body for comment in self.comments(task_id)}, {"instruction 1", "instruction 2"})
        self.assertEqual(self.board.get(task_id).current_run_id, native.current_run_id)

    def test_stop_before_binding_holds_older_creation_and_is_visible(self):
        created = self.event(stamp=time.time())
        self.bridge.handle_webhook(self.event("prompted", signal="stop", stamp=time.time() + 2))
        with patch.object(self.board, "create", side_effect=AssertionError("newer Stop must hold creation")):
            self.bridge.handle_webhook(created)
        self.assertEqual(self.tasks(), [])
        self.assertEqual(self.store.native_intent(f"unbound-stop:{ISSUE}")["state"], "reconcile")
        self.assertEqual(self.store.native_intent(f"create:{ISSUE}")["state"], "reconcile")

    def test_stop_during_ambiguous_native_creation_does_not_claim_worker_stopped(self):
        entered, release = threading.Event(), threading.Event()
        original = self.board.create
        def slow(**fields):
            entered.set()
            self.assertTrue(release.wait(10))
            return original(**fields)
        with ThreadPoolExecutor(max_workers=2) as pool, patch.object(self.board, "create", side_effect=slow):
            creation = pool.submit(self.bridge.handle_webhook, self.event())
            self.assertTrue(entered.wait(10))
            self.make_bridge().handle_webhook(self.event("prompted", signal="stop", stamp=time.time() + 2))
            release.set()
            creation.result(timeout=10)
        task_id = self.store.get(ISSUE)["task_id"]
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.assertIsNone(self.board.get(task_id).current_run_id)
        with self.board.conn() as conn:
            self.assertIsNone(kb.claim_task(conn, task_id, claimer="must-not-start"))
        held = self.store.native_intent(f"unbound-stop:{ISSUE}")
        self.assertEqual(held["state"], "reconcile")
        bodies = [row["payload"].get("content", {}).get("body", "") for row in self.store.pending()]
        self.assertTrue(any("in flight" in body for body in bodies))
        self.assertFalse(any("worker stopped" in body for body in bodies))

    def test_native_stop_is_once_and_stale_stop_after_native_resume_is_held(self):
        task_id = self.create()
        with self.board.conn() as conn:
            native = kb.claim_task(conn, task_id, claimer="native-test-worker")
        stop = self.event("prompted", signal="stop", stamp=time.time() + 2)
        self.bridge.handle_webhook(stop)
        self.assertEqual(self.board.get(task_id).status, "blocked")
        with patch.object(self.board, "block", side_effect=AssertionError("Stop cannot replay")):
            self.make_bridge().handle_webhook(stop)
        with self.board.conn() as conn:
            kb.unblock_task(conn, task_id)
            next_run = kb.claim_task(conn, task_id, claimer="native-recovery-worker")
            self.assertNotEqual(native.current_run_id, next_run.current_run_id)
            self.assertFalse(kb.complete_task(conn, task_id, result="stale worker result", expected_run_id=native.current_run_id))
        # A delayed old control cannot target this newer native attempt.
        self.authority.activities["old-stop"] = {"id": "old-stop", "user": {"id": USER}, "agentSession": {"id": SESSION}}
        old = self.event("prompted", signal="stop", stamp=time.time() - 10)
        old["agentActivity"]["id"] = "old-stop"
        self.make_bridge().handle_webhook(old)
        self.assertEqual(self.board.get(task_id).current_run_id, next_run.current_run_id)

    def reporting_fixture(self, name):
        from linear_fake_api import Clock, FakeLinear
        from hermes_fleet_linear_plugin.api import LinearAPI
        clock = Clock()
        remote = FakeLinear(clock)
        self.addCleanup(remote.close)
        remote.add_issue(ISSUE, "OPS-1", project=None)
        path = self.home / (name + ".db")
        reporter = Bridge(Store(path), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock),
                          self.board, profile="default", clock=clock)
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            task_id = kb.create_task(conn, title="Existing native work", assignee="default", initial_status="blocked")
        reporter.track(ISSUE, task_id)
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.unblock_task(conn, task_id)
        reporter.pump_kanban()
        return clock, remote, reporter, task_id

    def complete_reported_task(self, clock, task_id):
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            self.assertTrue(kb.complete_task(conn, task_id, result="Verified investigation found the precise bounded outcome"))

    def test_delayed_progress_and_completion_flush_reaches_done_once(self):
        clock, remote, reporter, task_id = self.reporting_fixture("delayed-completion")
        self.complete_reported_task(clock, task_id)
        reporter.pump_kanban()
        clock.now += 10
        reporter.flush()
        self.assertEqual(self.board.get(task_id).status, "done")
        self.assertEqual(remote.state(ISSUE), "Done")
        self.assertEqual(sum("precise bounded outcome" in body for body in remote.bodies(ISSUE)), 1)
        self.assertFalse(reporter.store.pending())
        reporter.recover()
        reporter.flush()
        self.assertEqual(sum("precise bounded outcome" in body for body in remote.bodies(ISSUE)), 1)

    def native_progress(self, clock, task_id):
        from hermes_cli.kanban_db_dispatch import heartbeat_worker
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            claimed = kb.claim_task(conn, task_id, claimer="reporting-scenario")
            self.assertIsNotNone(claimed)
            # Native dispatcher records spawn before its worker produces heartbeats.
            with kb.write_txn(conn):
                kb._append_event(conn, task_id, "spawned", {"pid": 123456}, run_id=claimed.current_run_id)
            self.assertTrue(heartbeat_worker(conn, task_id, note="Checkpoint B remains absent",
                                           expected_run_id=claimed.current_run_id))

    def halt_reported_task(self, clock, task_id, state):
        for index in range(kb.BLOCK_RECURRENCE_LIMIT if state == "triage" else 1):
            clock.now += 1
            task = self.board.get(task_id)
            with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                self.assertTrue(kb.block_task(conn, task_id, reason="Runtime reconciliation pending",
                                             kind="transient", expected_run_id=task.current_run_id))
                if state == "triage" and index < kb.BLOCK_RECURRENCE_LIMIT - 1:
                    self.assertTrue(kb.unblock_task(conn, task_id))
        self.assertEqual(self.board.get(task_id).status, state)

    def land_reported_task(self, clock, task_id, state):
        if state in ("blocked", "triage"):
            return self.halt_reported_task(clock, task_id, state)
        if state == "done":
            return self.complete_reported_task(clock, task_id)
        clock.now += 1
        task = self.board.get(task_id)
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            if state == "todo":
                parent = kb.create_task(conn, title="Synthetic unfinished prerequisite", assignee="default", initial_status="blocked")
                kb.link_tasks(conn, parent, task_id, expected_child_run_id=task.current_run_id)
                self.assertTrue(kb.block_task(conn, task_id, reason="Dependency remains unfinished", kind="dependency", expected_run_id=task.current_run_id))
            elif state == "scheduled":
                self.assertTrue(kb.schedule_task(conn, task_id, reason="Waiting for the scheduled time", expected_run_id=task.current_run_id))
            elif state == "review":
                self.assertTrue(kb.request_review(conn, task_id, summary="Implementation ready for review", reviewer="default", expected_run_id=task.current_run_id))
            else:
                self.assertTrue(kb.archive_task(conn, task_id))
        self.assertEqual(self.board.get(task_id).status, state)

    def test_mixed_native_history_projects_current_state_before_preserved_comments(self):
        from hermes_fleet_linear_plugin.api import LinearAPI
        for state in ("blocked", "triage", "todo", "scheduled", "done", "archived"):
            for captured in (False, True):
                with self.subTest(state=state, captured=captured):
                    clock, remote, reporter, task_id = self.reporting_fixture("mixed-history-" + state + str(captured))
                    remote.comment_updates_issue = True
                    reporter.flush()
                    self.native_progress(clock, task_id)
                    clock.now += 1
                    with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                        kb.add_comment(conn, task_id, "worker", "Preserve this ordinary native checkpoint")
                        kb.add_comment(conn, task_id, "worker-secondary", "Preserve the second ordinary checkpoint too")
                    if captured:
                        reporter.pump_kanban()
                    self.land_reported_task(clock, task_id, state)
                    native, history = self.board.get(task_id), self.board.history(task_id, 0)
                    clock.now += 10
                    reporter = Bridge(Store(reporter.store.path), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock), self.board, profile="default", clock=clock)
                    reporter.recover()
                    reporter.flush()
                    self.assertEqual(remote.state(ISSUE), "Done" if state == "done" else "Blocked")
                    bodies = remote.bodies(ISSUE)
                    self.assertEqual(sum("Preserve this ordinary native checkpoint" in body for body in bodies), 1)
                    self.assertEqual(sum("Preserve the second ordinary checkpoint too" in body for body in bodies), 1)
                    self.assertFalse(any("Reading the ticket" in body or "Checkpoint B remains absent" in body for body in bodies))
                    self.assertEqual(self.board.get(task_id), native)
                    self.assertEqual(self.board.history(task_id, 0), history)
                    reporter.recover()
                    reporter.flush()
                    self.assertEqual(remote.bodies(ISSUE), bodies)

    def test_mixed_history_preserves_human_versions_and_history_when_status_is_denied(self):
        for phase in ("blocked", "done"):
            for change in ("same_state", "assignee", "delegate", "close", "human_done"):
                with self.subTest(phase=phase, change=change):
                    clock, remote, reporter, task_id = self.reporting_fixture("mixed-human-" + phase + change)
                    remote.comment_updates_issue = True
                    reporter.flush()
                    self.native_progress(clock, task_id)
                    with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                        kb.add_comment(conn, task_id, "worker", "Ordinary history must survive denied status")
                    reporter.pump_kanban()
                    self.land_reported_task(clock, task_id, phase)
                    clock.now += 10
                    if change == "assignee":
                        remote.issues[ISSUE]["assignee"] = {"id": "different-human"}
                        remote.issues[ISSUE]["updatedAt"] = clock.iso()
                    elif change == "delegate":
                        remote.set_delegate(ISSUE, {"id": "different-agent"})
                    else:
                        remote.set_state(ISSUE, "Done" if change == "human_done" else "Canceled" if change == "close" else "In Progress")
                    human = copy.deepcopy(remote.issues[ISSUE])
                    reporter.recover()
                    reporter.flush()
                    self.assertEqual(remote.issues[ISSUE], human)
                    ordinary = [body for body in remote.bodies(ISSUE) if "Ordinary history must survive" in body]
                    self.assertEqual(len(ordinary), int(change in ("same_state", "assignee")))
                    status = reporter.store.outbox_row(reporter.store.latest_status(ISSUE)["id"])
                    self.assertEqual(status["payload"].get("applied"), 0)
                    self.assertIs(status["payload"].get("write_started"), False)
                    reporter.recover()
                    reporter.flush()
                    self.assertEqual(reporter.store.outbox_row(status["id"]), status)

    def test_attempted_ordinary_history_keeps_its_delivery_boundary_and_uuid(self):
        for legacy_marker in (False, True):
            with self.subTest(legacy_marker=legacy_marker):
                clock, remote, reporter, task_id = self.reporting_fixture("ordinary-crash-" + str(legacy_marker))
                remote.comment_updates_issue = True
                reporter.flush()
                clock.now += 1
                with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                    kb.claim_task(conn, task_id, claimer="ordinary-history-worker")
                    kb.add_comment(conn, task_id, "worker", "Original ordinary delivery must not be moved or replayed")
                reporter.pump_kanban()
                ordinary = next(row for row in reporter.store.pending() if row["kind"] == "comment")
                class ProcessCrash(BaseException):
                    pass
                with patch.object(reporter.store, "mark_sent", side_effect=ProcessCrash):
                    with self.assertRaises(ProcessCrash):
                        reporter.flush()
                attempted = reporter.store.outbox_row(ordinary["id"])
                self.assertIs(attempted["payload"].get("write_started"), True)
                if legacy_marker:
                    payload = dict(attempted["payload"])
                    payload.pop("write_started")
                    reporter.store.rewrite(attempted["id"], payload, attempted["next_at"])
                    attempted = reporter.store.outbox_row(attempted["id"])
                self.halt_reported_task(clock, task_id, "blocked")
                reporter.recover()
                self.assertEqual(reporter.store.outbox_row(attempted["id"]), attempted)
                reporter.flush()
                settled = reporter.store.outbox_row(attempted["id"])
                self.assertEqual(settled["payload"].get("applied"), 1)
                self.assertEqual(sum(item["id"] == attempted["id"] for item in remote.comments), 1)
                self.assertIn("query CommentReadback", remote.requests)
                self.assertEqual(remote.state(ISSUE), "Blocked")

    def test_own_done_history_requires_current_scope_and_preserves_late_history(self):
        import uuid
        for replace_scope in (False, True):
            with self.subTest(replace_scope=replace_scope):
                clock, remote, reporter, task_id = self.reporting_fixture("done-late-history-" + str(replace_scope))
                remote.comment_updates_issue = True
                reporter.flush()
                self.complete_reported_task(clock, task_id)
                reporter.recover()
                reporter.flush()
                self.assertEqual(remote.state(ISSUE), "Done")
                if replace_scope:
                    reporter.track(ISSUE, task_id, replace=True, completion_scope="progress",
                                   expected_revision=reporter.store.binding_revision(ISSUE), request_id=str(uuid.uuid4()))
                clock.now += 1
                with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                    kb.add_comment(conn, task_id, "worker", "Late native history after own completion")
                reporter.pump_kanban()
                reporter.flush()
                self.assertEqual(sum("Late native history after own completion" in body for body in remote.bodies(ISSUE)), int(not replace_scope))

    def test_archived_history_does_not_revive_retired_release_or_human_clear(self):
        import uuid
        from linear_fake_api import SELF
        for human_clear in (False, True):
            with self.subTest(human_clear=human_clear):
                clock, remote, reporter, task_id = self.reporting_fixture("release-history-" + str(human_clear))
                reporter.flush()
                remote.set_delegate(ISSUE, {"id": SELF})
                reporter.track(ISSUE, task_id, replace=True, requester_session=SESSION, requester_activity=ACTIVITY,
                               expected_revision=reporter.store.binding_revision(ISSUE), request_id=str(uuid.uuid4()))
                remote.sessions[SESSION] = ISSUE
                clock.now += 1
                with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                    kb.add_comment(conn, task_id, "worker", "Archived history after verified release")
                reporter.pump_kanban()
                self.land_reported_task(clock, task_id, "archived")
                self.assertTrue(reporter.store.finish(ISSUE, [("status", {"issue_id": ISSUE, "task_id": task_id,
                    "state": "blocked", "release": True, "terminal": True, "owner_issue_id": ISSUE,
                    "source_ms": clock() * 1000})], at=clock(), release=True))
                if human_clear:
                    clock.now += 1
                    remote.set_delegate(ISSUE, None)
                reporter.flush()
                status = reporter.store.outbox_row(reporter.store.latest_status(ISSUE)["id"])
                self.assertIn("Retained execution-derived delivery", status["hold_reason"])
                self.assertNotIn("applied", status["payload"])
                self.assertEqual(sum("Archived history after verified release" in body for body in remote.bodies(ISSUE)), 0)
                self.assertEqual(remote.issues[ISSUE]["delegate"], None if human_clear else {"id": SELF})
                self.assertEqual(self.board.get(task_id).status, "archived")

    def session_reporting_fixture(self, name):
        from linear_fake_api import Clock, FakeLinear, SELF
        from hermes_fleet_linear_plugin.api import _http
        from test_linear_specialist_authorization import ORG
        clock = Clock()
        remote = FakeLinear(clock)
        self.addCleanup(remote.close)
        remote.add_issue(ISSUE, "OPS-1", project=None)
        remote.set_delegate(ISSUE, {"id": SELF})
        remote.sessions[SESSION] = ISSUE
        remote.comment_updates_issue = remote.activity_updates_issue = True
        authority = Authority()
        authority.viewer = SELF
        authority.sessions[SESSION]["appUser"] = {"id": SELF}
        def transport(url, body, headers):
            if any(query in body.decode() for query in ("IdentityBinding", "AgentSessionAuthorization", "AgentActivityAuthorization")):
                return authority.transport(url, body, headers)
            return _http(remote.url, body, headers)
        api = plugin.BoundLinearAPI(lambda: "synthetic", identity={"viewer_id": SELF, "organization_id": ORG},
            specialist_scope={"all_teams": True, "all_projects": True, "allowed_requester_ids": [USER], "requester_basis": "agent_session"},
            transport=transport, endpoint=remote.url, clock=clock)
        reporter = Bridge(Store(self.home / (name + ".db")), api, self.board, profile="default", clock=clock)
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            task_id = kb.create_task(conn, title="Existing session-native work", assignee="default", initial_status="blocked")
        # The shared authoritative session/Activity fixture is read independently
        # from provider coordinates; no production session or human grant is created.
        with api.requester_session(SESSION, activity_id=ACTIVITY):
            reporter.track(ISSUE, task_id, requester_session=SESSION, requester_activity=ACTIVITY)
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.unblock_task(conn, task_id)
        reporter.pump_kanban()
        reporter.flush()
        return clock, remote, reporter, task_id

    def test_authenticated_agent_session_history_uses_current_state_first(self):
        for state in ("blocked", "done", "archived"):
            for captured in (False, True):
                with self.subTest(state=state, captured=captured):
                    clock, remote, reporter, task_id = self.session_reporting_fixture("session-history-" + state + str(captured))
                    self.native_progress(clock, task_id)
                    with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                        kb.add_comment(conn, task_id, "worker", "Authenticated session ordinary history")
                    if captured:
                        reporter.pump_kanban()
                    if state == "done":
                        clock.now += 1
                        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                            self.assertTrue(kb.complete_task(conn, task_id, result="Verified session-native bounded outcome",
                                metadata={"issue_comment": "Recommendation: keep the verified bounded result with its supporting evidence."}))
                    else:
                        self.land_reported_task(clock, task_id, state)
                    native, history = self.board.get(task_id), self.board.history(task_id, 0)
                    clock.now += 10
                    reporter.recover()
                    reporter.flush()
                    self.assertEqual(remote.state(ISSUE), "Done" if state == "done" else "Blocked")
                    self.assertEqual(sum("Authenticated session ordinary history" in body for body in remote.bodies(ISSUE)), 1)
                    self.assertTrue(any(item["content"].get("body") == "Authenticated session ordinary history" for item in remote.activities))
                    self.assertFalse(any("Reading the ticket" in body or "Checkpoint B remains absent" in body for body in remote.bodies(ISSUE)))
                    self.assertEqual(self.board.get(task_id), native)
                    self.assertEqual(self.board.history(task_id, 0), history)

    def test_unknown_agent_session_history_activity_stays_unknown_without_replay(self):
        clock, remote, reporter, task_id = self.session_reporting_fixture("session-unknown-history")
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.claim_task(conn, task_id, claimer="session-native-worker")
            kb.add_comment(conn, task_id, "worker", "Session history whose HTTP response is lost")
        reporter.pump_kanban()
        row = next(row for row in reporter.store.pending() if row["payload"].get("native_history"))
        remote.lose_next_response = True
        reporter.flush()
        unknown = reporter.store.outbox_row(row["id"])
        self.assertIs(unknown["payload"].get("write_started"), True)
        self.assertNotIn("applied", unknown["payload"])
        self.halt_reported_task(clock, task_id, "blocked")
        reporter.recover()
        reporter.flush()
        retained = reporter.store.outbox_row(row["id"])
        self.assertNotIn("applied", retained["payload"])
        self.assertEqual(sum(item["id"] == row["id"] for item in remote.activities), 1)
        self.assertEqual(remote.state(ISSUE), "Blocked")

    def test_own_done_history_rechecks_human_takeover_at_http_boundary(self):
        for change in ("delegate", "cancel"):
            with self.subTest(change=change):
                clock, remote, reporter, task_id = self.reporting_fixture("done-history-http-" + change)
                reporter.flush()
                self.complete_reported_task(clock, task_id)
                reporter.recover()
                reporter.flush()
                with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                    kb.add_comment(conn, task_id, "worker", "History held after human change at HTTP boundary")
                reporter.pump_kanban()
                create = reporter.api.create_comment
                def human_change_before_http(*args, **kwargs):
                    if change == "delegate":
                        remote.set_delegate(ISSUE, {"id": "different-agent"})
                    else:
                        remote.set_state(ISSUE, "Canceled")
                    return create(*args, **kwargs)
                before = copy.deepcopy(remote.comments)
                with patch.object(reporter.api, "create_comment", side_effect=human_change_before_http):
                    reporter.flush()
                self.assertEqual(remote.comments, before)

    def test_ordinary_history_waits_for_a_native_halt_arriving_at_http_boundary(self):
        clock, remote, reporter, task_id = self.reporting_fixture("ordinary-halt-http")
        remote.comment_updates_issue = True
        reporter.flush()
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.claim_task(conn, task_id, claimer="ordinary-http-worker")
            kb.add_comment(conn, task_id, "worker", "Ordinary checkpoint preserved after a boundary halt")
        reporter.pump_kanban()
        create = reporter.api.create_comment
        def halt_before_http(*args, **kwargs):
            self.halt_reported_task(clock, task_id, "blocked")
            return create(*args, **kwargs)
        with patch.object(reporter.api, "create_comment", side_effect=halt_before_http):
            reporter.flush()
        self.assertEqual(remote.comments, [])
        reporter.recover()
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "Blocked")
        self.assertEqual(sum("Ordinary checkpoint preserved" in body for body in remote.bodies(ISSUE)), 1)

    def test_session_history_waits_for_a_native_halt_without_spending_its_advisory_attempt(self):
        clock, remote, reporter, task_id = self.session_reporting_fixture("session-halt-http")
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.claim_task(conn, task_id, claimer="session-http-worker")
            kb.add_comment(conn, task_id, "worker", "Session checkpoint preserved across deferred native halt")
        reporter.pump_kanban()
        ordinary = next(row for row in reporter.store.pending() if row["payload"].get("native_history"))
        create = reporter.api.create_activity
        def halt_before_http(*args, **kwargs):
            self.halt_reported_task(clock, task_id, "blocked")
            return create(*args, **kwargs)
        with patch.object(reporter.api, "create_activity", side_effect=halt_before_http):
            reporter.flush()
        deferred = reporter.store.outbox_row(ordinary["id"])
        self.assertEqual(deferred["state"], "pending")
        self.assertIs(deferred["payload"].get("write_started"), False)
        self.assertEqual(remote.activities, [])
        reporter.recover()
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "Blocked")
        self.assertEqual(sum("Session checkpoint preserved" in body for body in remote.bodies(ISSUE)), 1)

    def test_history_dependency_is_rechecked_inside_mutation_admission(self):
        clock, remote, reporter, task_id = self.reporting_fixture("history-admit-race")
        remote.comment_updates_issue = True
        reporter.flush()
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.claim_task(conn, task_id, claimer="admission-race-worker")
            kb.add_comment(conn, task_id, "worker", "Ordinary history protected by the final dependency check")
        reporter.pump_kanban()
        admit = reporter.store.admit_mutation
        def concurrent_capture_before_admission(*args, **kwargs):
            self.halt_reported_task(clock, task_id, "blocked")
            work = reporter.store.get(ISSUE)
            for event in self.board.history(task_id, work["last_event_id"]):
                reporter._task_event(work, event, "blocked", {})
                work = reporter.store.get(ISSUE)
            return admit(*args, **kwargs)
        with patch.object(reporter.store, "admit_mutation", side_effect=concurrent_capture_before_admission):
            reporter.flush()
        self.assertEqual(remote.comments, [])
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "Blocked")
        self.assertEqual(sum("Ordinary history protected" in body for body in remote.bodies(ISSUE)), 1)

    def test_halted_reporting_gap_coalesces_native_progress_before_capture_or_send(self):
        from hermes_fleet_linear_plugin.api import LinearAPI
        for state in ("blocked", "triage"):
            for captured in (False, True):
                with self.subTest(state=state, captured=captured):
                    clock, remote, reporter, task_id = self.reporting_fixture("halted-gap-" + state + str(captured))
                    remote.comment_updates_issue = True
                    reporter.flush()
                    self.native_progress(clock, task_id)
                    if captured:
                        reporter.pump_kanban()
                    self.halt_reported_task(clock, task_id, state)
                    native, history = self.board.get(task_id), self.board.history(task_id, 0)
                    clock.now += 10
                    reporter = Bridge(Store(reporter.store.path), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock),
                                      self.board, profile="default", clock=clock)
                    reporter.recover()
                    reporter.flush()
                    self.assertEqual(remote.state(ISSUE), "Blocked")
                    bodies = remote.bodies(ISSUE)
                    self.assertTrue(any("Runtime reconciliation pending" in body for body in bodies))
                    self.assertFalse(any("Reading the ticket" in body or "Checkpoint B remains absent" in body for body in bodies))
                    self.assertEqual(self.board.get(task_id), native)
                    self.assertEqual(self.board.history(task_id, 0), history)
                    self.assertFalse(reporter.store.pending())
                    reporter.recover()
                    reporter.flush()
                    self.assertEqual(remote.bodies(ISSUE), bodies)

    def test_native_halt_at_progress_http_boundary_skips_obsolete_send(self):
        for state in ("blocked", "triage"):
            with self.subTest(state=state):
                clock, remote, reporter, task_id = self.reporting_fixture("halt-http-" + state)
                remote.comment_updates_issue = True
                reporter.flush()
                self.native_progress(clock, task_id)
                reporter.pump_kanban()
                create = reporter.api.create_comment
                def halt_before_http(*args, **kwargs):
                    self.halt_reported_task(clock, task_id, state)
                    return create(*args, **kwargs)
                with patch.object(reporter.api, "create_comment", side_effect=halt_before_http) as attempted:
                    reporter.flush()
                self.assertEqual(attempted.call_count, 1)
                self.assertEqual(remote.bodies(ISSUE), [])
                reporter.recover()
                reporter.flush()
                self.assertEqual(remote.state(ISSUE), "Blocked")

    def test_halted_progress_keeps_later_human_edit_and_takeover_authoritative(self):
        for change in ("same_state", "assignee", "delegate", "close"):
            with self.subTest(change=change):
                clock, remote, reporter, task_id = self.reporting_fixture("halted-human-" + change)
                remote.comment_updates_issue = True
                reporter.flush()
                self.native_progress(clock, task_id)
                reporter.pump_kanban()
                self.halt_reported_task(clock, task_id, "blocked")
                clock.now += 10
                if change == "same_state":
                    remote.set_state(ISSUE, "In Progress")
                elif change == "assignee":
                    remote.issues[ISSUE]["assignee"] = {"id": "new-human-assignee"}
                    remote.issues[ISSUE]["updatedAt"] = clock.iso()
                elif change == "delegate":
                    remote.set_delegate(ISSUE, {"id": "different-agent"})
                else:
                    remote.set_state(ISSUE, "Canceled")
                human = copy.deepcopy(remote.issues[ISSUE])
                reporter.recover()
                reporter.flush()
                self.assertEqual(remote.issues[ISSUE], human)
                self.assertEqual(self.board.get(task_id).status, "blocked")
                self.assertFalse(any("Reading the ticket" in body or "Checkpoint B remains absent" in body
                                     for body in remote.bodies(ISSUE)))

    def test_other_native_waits_keep_current_milestones_instead_of_old_progress(self):
        for state, milestone in (("todo", "Waiting for a dependency"), ("scheduled", "Scheduled"),
                                 ("review", "Review requested"), ("archived", "unfinished")):
            with self.subTest(state=state):
                clock, remote, reporter, task_id = self.reporting_fixture("stationary-" + state)
                remote.comment_updates_issue = True
                reporter.flush()
                self.native_progress(clock, task_id)
                reporter.pump_kanban()
                clock.now += 1
                task = self.board.get(task_id)
                with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                    if state == "todo":
                        parent = kb.create_task(conn, title="Independent dependency", assignee="default", initial_status="blocked")
                        kb.link_tasks(conn, parent, task_id, expected_child_run_id=task.current_run_id)
                        self.assertTrue(kb.block_task(conn, task_id, reason="Dependency remains unfinished", kind="dependency",
                                                     expected_run_id=task.current_run_id))
                    elif state == "scheduled":
                        self.assertTrue(kb.schedule_task(conn, task_id, reason="Waiting for the scheduled time",
                                                        expected_run_id=task.current_run_id))
                    elif state == "review":
                        self.assertTrue(kb.request_review(conn, task_id, summary="Implementation ready for review", reviewer="default",
                                                         expected_run_id=task.current_run_id))
                    else:
                        self.assertTrue(kb.archive_task(conn, task_id))
                self.assertEqual(self.board.get(task_id).status, state)
                native, history = self.board.get(task_id), self.board.history(task_id, 0)
                clock.now += 10
                reporter.recover()
                reporter.flush()
                bodies = remote.bodies(ISSUE)
                self.assertFalse(any("Reading the ticket" in body or "Checkpoint B remains absent" in body for body in bodies))
                self.assertTrue(any(milestone in body for body in bodies), bodies)
                self.assertEqual(self.board.get(task_id), native)
                self.assertEqual(self.board.history(task_id, 0), history)

    def test_halted_task_preserves_attempted_progress_uuid_and_exact_readback(self):
        for state in ("blocked", "triage"):
            with self.subTest(state=state):
                clock, remote, reporter, task_id, progress_id = self.crash_after_progress_publication("halted-attempt-" + state)
                self.halt_reported_task(clock, task_id, state)
                clock.now += 10
                reporter.recover()
                reporter.flush()
                settled = reporter.store.outbox_row(progress_id)
                self.assertEqual(settled["payload"].get("applied"), 1)
                self.assertIs(settled["payload"].get("write_started"), True)
                self.assertEqual(sum(comment["id"] == progress_id for comment in remote.comments), 1)
                self.assertIn("query CommentReadback", remote.requests)
                self.assertEqual(remote.state(ISSUE), "Blocked")

    def test_running_ready_and_unknown_phases_do_not_silently_drop_progress(self):
        for state in ("running", "ready", "future-native-phase"):
            with self.subTest(state=state):
                clock, remote, reporter, task_id = self.reporting_fixture("active-phase-" + state)
                reporter.flush()
                self.native_progress(clock, task_id)
                native = copy.copy(self.board.get(task_id))
                native.status = state
                # The unrecognized readback is deliberately not written to native storage.
                with patch.object(self.board, "require_task", return_value=native):
                    reporter.pump_kanban()
                    reporter.flush()
                self.assertTrue(any("Reading the ticket" in body for body in remote.bodies(ISSUE)))
                self.assertTrue(any("Checkpoint B remains absent" in body for body in remote.bodies(ISSUE)))

    def test_native_operator_stop_survives_reporting_catchup_without_reactivation(self):
        clock, remote, reporter, task_id = self.reporting_fixture("stopped-progress")
        remote.comment_updates_issue = True
        reporter.flush()
        self.native_progress(clock, task_id)
        reporter.pump_kanban()
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            self.assertTrue(kb.block_task(conn, task_id, reason="native operator Stop", kind="needs_input",
                                         expected_run_id=self.board.get(task_id).current_run_id))
        native, history = self.board.get(task_id), self.board.history(task_id, 0)
        clock.now += 10
        reporter.recover()
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "Blocked")
        self.assertEqual(self.board.get(task_id), native)
        self.assertEqual(self.board.history(task_id, 0), history)
        self.assertFalse(any("Reading the ticket" in body or "Checkpoint B remains absent" in body
                             for body in remote.bodies(ISSUE)))

    def test_completed_reporting_gap_coalesces_native_progress_before_capture_or_send(self):
        from hermes_fleet_linear_plugin.api import LinearAPI
        for captured in (False, True):
            with self.subTest(captured=captured):
                clock, remote, reporter, task_id = self.reporting_fixture("gap-progress-" + str(captured))
                remote.comment_updates_issue = True
                reporter.flush()
                self.native_progress(clock, task_id)
                if captured:
                    reporter.pump_kanban()
                    self.assertTrue(any(row["kind"] == "comment" for row in reporter.store.pending()))
                self.complete_reported_task(clock, task_id)
                native = self.board.get(task_id)
                history = self.board.history(task_id, 0)
                clock.now += 10
                reporter = Bridge(Store(reporter.store.path), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock),
                                  self.board, profile="default", clock=clock)
                reporter.recover()
                reporter.flush()
                self.assertEqual(remote.state(ISSUE), "Done")
                bodies = remote.bodies(ISSUE)
                self.assertFalse(any("Reading the ticket" in body or "Checkpoint B remains absent" in body for body in bodies))
                self.assertEqual(sum("precise bounded outcome" in body for body in bodies), 1)
                self.assertFalse(reporter.store.pending())
                self.assertEqual(self.board.get(task_id), native)
                self.assertEqual(self.board.history(task_id, 0), history)
                reporter.recover()
                reporter.flush()
                self.assertEqual(remote.bodies(ISSUE), bodies)

    def test_native_completion_at_progress_http_boundary_skips_obsolete_send(self):
        clock, remote, reporter, task_id = self.reporting_fixture("progress-http-boundary")
        remote.comment_updates_issue = True
        reporter.flush()
        self.native_progress(clock, task_id)
        reporter.pump_kanban()
        create = reporter.api.create_comment
        def complete_before_http(*args, **kwargs):
            self.complete_reported_task(clock, task_id)
            return create(*args, **kwargs)
        with patch.object(reporter.api, "create_comment", side_effect=complete_before_http) as attempted:
            reporter.flush()
        self.assertEqual(attempted.call_count, 1)
        self.assertEqual(remote.bodies(ISSUE), [])
        reporter.recover()
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "Done")
        self.assertEqual(sum("precise bounded outcome" in body for body in remote.bodies(ISSUE)), 1)

    def test_coalesced_progress_keeps_later_human_changes_authoritative(self):
        for change in ("same_state", "assignee", "delegate", "close"):
            with self.subTest(change=change):
                clock, remote, reporter, task_id = self.reporting_fixture("coalesced-human-" + change)
                remote.comment_updates_issue = True
                reporter.flush()
                self.native_progress(clock, task_id)
                reporter.pump_kanban()
                self.complete_reported_task(clock, task_id)
                clock.now += 10
                if change == "same_state":
                    remote.set_state(ISSUE, "In Progress")
                elif change == "assignee":
                    remote.issues[ISSUE]["assignee"] = {"id": "new-human-assignee"}
                    remote.issues[ISSUE]["updatedAt"] = clock.iso()
                elif change == "delegate":
                    remote.set_delegate(ISSUE, {"id": "different-agent"})
                else:
                    remote.set_state(ISSUE, "Canceled")
                human = copy.deepcopy(remote.issues[ISSUE])
                reporter.recover()
                reporter.flush()
                self.assertEqual(remote.issues[ISSUE], human)
                self.assertEqual(remote.bodies(ISSUE), [])
                self.assertEqual(self.board.get(task_id).status, "done")

    def test_coalescing_keeps_native_comments_and_partial_stage_summary(self):
        import uuid
        clock, remote, reporter, task_id = self.reporting_fixture("progress-partial-stage")
        reporter.flush()
        reporter.track(ISSUE, task_id, replace=True, completion_scope="progress",
                       expected_revision=reporter.store.binding_revision(ISSUE), request_id=str(uuid.uuid4()))
        self.native_progress(clock, task_id)
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.add_comment(conn, task_id, "worker", "Checkpoint evidence retained")
        reporter.pump_kanban()
        reporter.comment(ISSUE, "Started: Reading the ticket — explicitly requested comment")
        self.complete_reported_task(clock, task_id)
        clock.now += 10
        reporter.recover()
        reporter.flush()
        self.assertNotEqual(remote.state(ISSUE), "Done")
        bodies = remote.bodies(ISSUE)
        self.assertTrue(any("Checkpoint evidence retained" in body for body in bodies))
        self.assertTrue(any("explicitly requested comment" in body for body in bodies))
        self.assertTrue(any("precise bounded outcome" in body for body in bodies))
        self.assertFalse(any("Checkpoint B remains absent" in body for body in bodies))

    def test_lost_progress_response_retains_its_uuid_after_native_completion(self):
        clock, remote, reporter, task_id = self.reporting_fixture("uncertain-native-progress")
        remote.comment_updates_issue = True
        reporter.flush()
        self.native_progress(clock, task_id)
        reporter.pump_kanban()
        remote.lose_next_response = True
        self.assertEqual(reporter.flush(), 0)
        self.assertEqual(len(remote.bodies(ISSUE)), 1)
        self.assertIn("Reading the ticket", remote.bodies(ISSUE)[0])
        unknown = next(row for row in reporter.store.pending() if row["attempts"])
        self.complete_reported_task(clock, task_id)
        reporter.recover()
        self.assertEqual(reporter.store.outbox_row(unknown["id"]), unknown)
        self.assertEqual(len(remote.bodies(ISSUE)), 1)
        clock.now += 61
        reporter.flush()
        settled = reporter.store.outbox_row(unknown["id"])
        self.assertEqual(settled["state"], "sent")
        self.assertEqual(settled["payload"].get("applied"), 1)
        self.assertEqual(sum("Reading the ticket" in body for body in remote.bodies(ISSUE)), 1)
        self.assertFalse(any("Checkpoint B remains absent" in body for body in remote.bodies(ISSUE)))
        self.assertEqual(remote.state(ISSUE), "Done")

    def crash_after_progress_publication(self, name):
        clock, remote, reporter, task_id = self.reporting_fixture(name)
        remote.comment_updates_issue = True
        reporter.flush()
        self.native_progress(clock, task_id)
        reporter.pump_kanban()
        progress = next(row for row in reporter.store.pending() if row["payload"].get("native_progress")
                        and not row["payload"].get("ephemeral"))
        mark_sent = reporter.store.mark_sent
        class ProcessCrash(BaseException):
            pass
        def crash_before_settlement(row_id, applied):
            if row_id == progress["id"]:
                raise ProcessCrash
            return mark_sent(row_id, applied)
        with patch.object(reporter.store, "mark_sent", side_effect=crash_before_settlement):
            with self.assertRaises(ProcessCrash):
                reporter.flush()
        self.assertEqual(len(remote.comments), 1)
        self.assertEqual(reporter.store.outbox_row(progress["id"])["attempts"], 0)
        return clock, remote, reporter, task_id, progress["id"]

    def test_crash_after_progress_http_reconciles_same_uuid_instead_of_coalescing(self):
        from hermes_fleet_linear_plugin.api import LinearAPI
        clock, remote, reporter, task_id, progress_id = self.crash_after_progress_publication("crash-progress")
        self.complete_reported_task(clock, task_id)
        clock.now += 10
        reporter = Bridge(Store(reporter.store.path), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock),
                          self.board, profile="default", clock=clock)
        reporter.recover()
        reporter.flush()
        settled = reporter.store.outbox_row(progress_id)
        self.assertEqual(settled["state"], "sent")
        self.assertEqual(settled["payload"].get("applied"), 1)
        self.assertIs(settled["payload"].get("write_started"), True)
        self.assertEqual(sum(comment["id"] == progress_id for comment in remote.comments), 1)
        self.assertIn("query CommentReadback", remote.requests)
        self.assertEqual(remote.state(ISSUE), "Done")

    def test_attempted_progress_does_not_claim_unapplied_after_takeover_or_changed_readback(self):
        for change in ("body", "issue", "delegate", "closed"):
            with self.subTest(change=change):
                clock, remote, reporter, task_id, progress_id = self.crash_after_progress_publication("changed-progress-" + change)
                self.complete_reported_task(clock, task_id)
                clock.now += 10
                if change == "body":
                    remote.comments[0]["body"] = "Human corrected the progress comment"
                elif change == "issue":
                    remote.comments[0]["issueId"] = "different-issue"
                elif change == "delegate":
                    remote.set_delegate(ISSUE, {"id": "different-agent"})
                else:
                    remote.set_state(ISSUE, "Canceled")
                remote.issues[ISSUE]["updatedAt"] = clock.iso()
                before = copy.deepcopy(remote.comments)
                reporter.recover()
                reporter.flush()
                held = reporter.store.outbox_row(progress_id)
                self.assertIn(held["state"], ("pending", "failed"))
                self.assertNotIn("applied", held["payload"])
                self.assertIs(held["payload"].get("write_started"), True)
                self.assertEqual(remote.comments, before)
                self.assertNotEqual(remote.state(ISSUE), "Done")

    def test_legacy_progress_without_explicit_unstarted_marker_is_not_coalesced(self):
        clock, remote, reporter, task_id, progress_id = self.crash_after_progress_publication("legacy-progress")
        row = reporter.store.outbox_row(progress_id)
        payload = dict(row["payload"])
        payload.pop("write_started")
        reporter.store.rewrite(progress_id, payload, row["next_at"])
        self.complete_reported_task(clock, task_id)
        clock.now += 10
        reporter.recover()
        reporter.flush()
        settled = reporter.store.outbox_row(progress_id)
        self.assertEqual(settled["payload"].get("applied"), 1)
        self.assertEqual(sum(comment["id"] == progress_id for comment in remote.comments), 1)
        self.assertIn("query CommentReadback", remote.requests)

    def test_atomic_progress_receipt_survives_restart_before_completion_projection(self):
        from hermes_fleet_linear_plugin.api import LinearAPI
        clock, remote, reporter, task_id = self.reporting_fixture("receipt-restart")
        self.complete_reported_task(clock, task_id)
        clock.now += 10
        self.assertEqual(reporter.flush(), 1)
        progress = next(row for row in reporter.store.active() if row["issue_id"] == ISSUE)
        reporter = Bridge(Store(reporter.store.path), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock),
                          self.board, profile="default", clock=clock)
        reporter.recover()
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "Done")
        self.assertTrue(any("precise bounded outcome" in body for body in remote.bodies(ISSUE)))
        self.assertEqual(reporter.store.get(ISSUE)["task_id"], progress["task_id"])

    def test_later_human_version_is_preserved_after_own_progress_receipt(self):
        for change in ("same_state", "assignee", "delegate", "close"):
            with self.subTest(change=change):
                clock, remote, reporter, task_id = self.reporting_fixture("human-after-own-" + change)
                self.complete_reported_task(clock, task_id)
                clock.now += 10
                self.assertEqual(reporter.flush(), 1)
                clock.now += 1
                if change == "same_state":
                    remote.set_state(ISSUE, "In Progress")
                elif change == "assignee":
                    remote.issues[ISSUE]["assignee"] = {"id": "new-human-assignee"}
                    remote.issues[ISSUE]["updatedAt"] = clock.iso()
                elif change == "delegate":
                    remote.set_delegate(ISSUE, {"id": "different-agent"})
                else:
                    remote.set_state(ISSUE, "Canceled")
                human = copy.deepcopy(remote.issues[ISSUE])
                reporter.recover()
                reporter.flush()
                self.assertEqual(remote.issues[ISSUE], human)
                self.assertEqual(self.board.get(task_id).status, "done")
                self.assertFalse(any("precise bounded outcome" in body for body in remote.bodies(ISSUE)))

    def test_unknown_progress_response_has_no_own_receipt_and_requires_reconciliation(self):
        clock, remote, reporter, task_id = self.reporting_fixture("unknown-progress")
        before = reporter.store.get(ISSUE)["last_event_id"]
        self.complete_reported_task(clock, task_id)
        clock.now += 10
        remote.lose_next_response = True
        self.assertEqual(reporter.flush(), 0)
        self.assertEqual(remote.state(ISSUE), "In Progress")
        pending = next(row for row in reporter.store.pending() if row["kind"] == "status")
        self.assertFalse(pending["payload"].get("issue_write_receipt"))
        reporter.pump_kanban()
        self.assertTrue(reporter.store.issue_reconciliation_blocked(ISSUE))
        self.assertEqual(reporter.store.get(ISSUE)["last_event_id"], before)
        self.assertFalse(any(row["payload"].get("state") == "done" for row in reporter.store.pending()))
        self.assertEqual(self.board.get(task_id).status, "done")

    def test_failed_completion_capture_does_not_consume_later_events_or_other_bindings(self):
        import uuid
        from hermes_fleet_linear_plugin.api import LinearAPI
        clock, remote, reporter, task_id = self.reporting_fixture("blocked-capture")
        reporter.flush()
        before = reporter.store.get(ISSUE)["last_event_id"]
        unknown = reporter.store.enqueue("description", {"issue_id": ISSUE, "id": str(uuid.uuid4()), "body": "Unknown effect",
            "expected_description": "Synthetic issue body", "content_action": True, "unlinked_content": True,
            "write_started": True}, at=clock())
        self.complete_reported_task(clock, task_id)
        with self.board.conn() as conn:
            kb.edit_task(conn, task_id, title="Clarified native task")
        remote.add_issue("unaffected", "OPS-2", project=None)
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            other = kb.create_task(conn, title="Independent work", assignee="default")
        reporter.track("unaffected", other)
        clock.now += 1
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.complete_task(conn, other, result="Verified independent binding outcome")
        reporter.pump_kanban()
        self.assertEqual(reporter.store.get(ISSUE)["last_event_id"], before)
        self.assertTrue(any(row["payload"].get("issue_id") == "unaffected" and row["payload"].get("state") == "done"
                            for row in reporter.store.pending()))
        reporter.store.reconcile_terminal(unknown, outcome="not_applied", evidence="Remote description verified unchanged", at=clock())
        reporter = Bridge(Store(reporter.store.path), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock),
                          self.board, profile="default", clock=clock)
        reporter.recover()
        reporter.flush()
        self.assertNotEqual(remote.state(ISSUE), "Done")
        self.assertEqual(remote.state("unaffected"), "Done")
        self.assertFalse(any(row["payload"].get("issue_id") == ISSUE and row["payload"].get("state") == "done"
                             for row in reporter.store.pending()))
        cursor = reporter.store.get(ISSUE)["last_event_id"]
        self.assertGreater(cursor, before)
        native = self.board.get(task_id)
        self.assertEqual(native.status, "done")
        history = copy.deepcopy(self.board.history(task_id, 0))
        with self.board.conn() as conn:
            tasks = kb.list_tasks(conn, include_archived=True)
        request = dict(replace=True, completion_scope="outcome", reconcile_complete=True,
                       expected_revision=reporter.store.binding_revision(ISSUE), request_id=str(uuid.uuid4()))
        result = reporter.track(ISSUE, task_id, **request)
        self.assertTrue(result["reporting_only"])
        self.assertTrue(result["completion_reporting_reconciled"])
        self.assertEqual(reporter.store.get(ISSUE)["last_event_id"], cursor)
        self.assertEqual(self.board.get(task_id), native)
        self.assertEqual(self.board.history(task_id, 0), history)
        reporter.track(ISSUE, task_id, **request)  # A lost reply reuses the same UUID and inspected CAS preimage.
        with self.board.conn() as conn:
            self.assertEqual(kb.list_tasks(conn, include_archived=True), tasks)
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "Done")
        self.assertEqual(remote.state("unaffected"), "Done")
        self.assertTrue(any("precise bounded outcome" in body for body in remote.bodies(ISSUE)))
        self.assertGreater(reporter.store.get(ISSUE)["last_event_id"], before)

    def test_remote_outage_queues_reporting_and_real_http_delivery_recovers(self):
        from linear_fake_api import Clock, FakeLinear
        from hermes_fleet_linear_plugin.api import LinearAPI
        clock = Clock()
        remote = FakeLinear(clock)
        self.addCleanup(remote.close)
        remote.add_issue(ISSUE, "OPS-1")
        reporter = Bridge(Store(self.home / "remote.db"), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock),
                          self.board, profile="default", clock=clock)
        with self.board.conn() as conn:
            task_id = kb.create_task(conn, title="OPS-1: existing task", assignee="default")
        reporter.track(ISSUE, task_id)
        remote.down = True
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.complete_task(conn, task_id, result="Verified findings: https://example.test/findings")
        reporter.pump_kanban()
        self.assertGreater(reporter.store.get(ISSUE)["last_event_id"], 0)
        self.assertTrue(reporter.store.pending())
        reporter.flush()
        self.assertTrue(reporter.store.pending())
        self.assertEqual(self.board.get(task_id).status, "done")
        remote.down = False
        clock.now += 61
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "Done")
        self.assertTrue(remote.comments)
        self.assertTrue(remote.project_updates)
        self.assertFalse(reporter.store.pending())

    def test_restart_catchup_preserves_human_state_changed_after_native_event(self):
        from linear_fake_api import Clock, FakeLinear
        from hermes_fleet_linear_plugin.api import LinearAPI
        clock = Clock()
        remote = FakeLinear(clock)
        self.addCleanup(remote.close)
        remote.add_issue(ISSUE, "OPS-1")
        reporter = Bridge(Store(self.home / "human-state.db"), LinearAPI(lambda: "synthetic", endpoint=remote.url, clock=clock),
                          self.board, profile="default", clock=clock)
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            task_id = kb.create_task(conn, title="OPS-1: native work", assignee="default")
        reporter.track(ISSUE, task_id)
        with self.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            kb.block_task(conn, task_id, reason="native needs a decision", kind="needs_input")
        clock.now += 5
        remote.set_state(ISSUE, "In Progress")
        clock.now += 5
        reporter.pump_kanban()
        status = next(row for row in reporter.store.pending() if row["kind"] == "status")
        self.assertLess(status["payload"]["source_ms"], clock() * 1000)
        reporter.flush()
        self.assertEqual(remote.state(ISSUE), "In Progress")
        self.assertEqual(self.board.get(task_id).status, "blocked")

    def test_human_state_and_reporting_outage_never_control_native_worker(self):
        task_id = self.create()
        with self.board.conn() as conn:
            task = kb.claim_task(conn, task_id, claimer="native-test-worker")
        before = self.board.get(task_id)
        self.authority.fail_issue = True
        self.bridge.recheck(force=True)
        after = self.board.get(task_id)
        self.assertEqual((before.status, before.current_run_id, before.claim_lock),
                         (after.status, after.current_run_id, after.claim_lock))
        self.authority.fail_issue = False
        self.authority.issues[ISSUE]["state"] = {"type": "canceled", "name": "Canceled"}
        self.bridge.recheck(force=True)
        self.assertEqual(self.board.get(task_id).current_run_id, task.current_run_id)
        with self.assertRaises(LinearError):
            self.bridge.followup(ISSUE, "late instruction", "closed")
        self.assertEqual(self.comments(task_id), [])
