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
