"""Real native guarded admission; no fabricated client or approval context."""
from __future__ import annotations

import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import test_linear_native_bridge_scenarios as scenarios


@unittest.skipUnless(os.environ.get("HERMES_AGENT_SRC"), "requires explicit native source")
class GuardedAdmissionScenarios(unittest.TestCase):
    def setUp(self):
        self.fixture = scenarios.NativeBridgeScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.board, self.bridge, self.store = self.fixture.board, self.fixture.bridge, self.fixture.store
        if not self.board.guarded_controls:
            self.skipTest("native host predates guarded task controls")

    def assign(self):
        self.bridge.handle_webhook(self.fixture.event())
        return self.store.get(scenarios.ISSUE)["task_id"]

    def test_assignment_reaches_native_ready_without_manual_activation(self):
        task_id = self.assign()
        self.assertEqual(self.board.get(task_id).status, "ready")
        self.bridge.handle_webhook(self.fixture.event())
        self.assertEqual(len(self.fixture.tasks()), 1)
        self.assertEqual(sum(event.kind == "unblocked" for event in self.board.history(task_id, 0)), 1)

    def test_assignment_activation_preserves_outcome_snapshot_through_native_completion(self):
        import copy
        task_id = self.assign()
        before = self.board.get(task_id)
        self.assertEqual(before.status, "ready")
        binding = copy.deepcopy(self.store.native_binding(scenarios.ISSUE))
        self.assertEqual(binding["completion_scope"]["mode"], "outcome")
        self.assertFalse(any(event.kind == "specified" for event in self.board.history(task_id, 0)))
        self.assertEqual(sum(event.kind == "unblocked" for event in self.board.history(task_id, 0)), 1)
        with self.board.conn() as conn:
            self.assertTrue(scenarios.kb.complete_task(conn, task_id,
                result="Verified the assigned outcome and retained the final deliverable."))
        after = self.board.get(task_id)
        self.assertEqual((after.title, after.body, after.completion_contract),
                         (before.title, before.body, before.completion_contract))
        self.bridge.pump_kanban()
        self.assertEqual(self.store.native_binding(scenarios.ISSUE), binding)
        self.assertTrue(any(row["kind"] == "status" and row["payload"].get("state") == "done"
                            for row in self.store.pending()))
        self.assertTrue(any(row["kind"] == "project_update" for row in self.store.pending()))
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_stop_during_creation_invalidates_initial_admission(self):
        create = self.board.create
        def stopped(**fields):
            task = create(**fields)
            with self.board.conn() as conn:
                self.assertTrue(scenarios.kb.block_task(conn, task.id, reason="operator Stop", kind="needs_input"))
            return task
        with patch.object(self.board, "create", side_effect=stopped):
            task_id = self.assign()
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.assertFalse(any(event.kind == "unblocked" for event in self.board.history(task_id, 0)))

    def test_stop_between_inspection_and_activation_wins(self):
        activate = self.board.activate
        def stopped(task_id, expected_event_id, **kwargs):
            with self.board.conn() as conn:
                self.assertTrue(scenarios.kb.block_task(conn, task_id, reason="operator Stop", kind="needs_input",
                                                        expected_event_id=expected_event_id))
            return activate(task_id, expected_event_id, **kwargs)
        with patch.object(self.board, "activate", side_effect=stopped):
            task_id = self.assign()
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.fixture.make_bridge().recover()
        self.assertEqual(self.board.get(task_id).status, "blocked")

    def test_restart_finishes_admission_after_binding_without_second_task(self):
        with patch.object(self.bridge, "_assignment_activation", side_effect=ConnectionError("crash after binding")):
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(self.fixture.event())
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.fixture.make_bridge().recover()
        self.assertEqual(self.board.get(task_id).status, "ready")
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_lost_activation_reply_reconciles_same_cas_without_second_transition(self):
        activate = self.board.activate
        def lost(task_id, expected_event_id, **kwargs):
            self.assertTrue(activate(task_id, expected_event_id, **kwargs))
            raise ConnectionError("lost native activation reply")
        with patch.object(self.board, "activate", side_effect=lost):
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(self.fixture.event())
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        self.fixture.make_bridge().recover()
        self.assertEqual(sum(event.kind == "unblocked" for event in self.board.history(task_id, 0)), 1)
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_newer_stop_after_lost_activation_reply_is_never_refreshed_away(self):
        activate = self.board.activate
        def lost(task_id, expected_event_id, **kwargs):
            self.assertTrue(activate(task_id, expected_event_id, **kwargs))
            raise ConnectionError("lost native activation reply")
        with patch.object(self.board, "activate", side_effect=lost):
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(self.fixture.event())
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        with self.board.conn() as conn:
            self.assertTrue(scenarios.kb.block_task(conn, task_id, reason="newer Stop", kind="needs_input"))
        self.fixture.make_bridge().recover()
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.assertEqual(sum(event.kind == "unblocked" for event in self.board.history(task_id, 0)), 1)

    def test_linking_existing_parked_task_does_not_activate_it_on_upgrade(self):
        task = self.board.create(title="Previously parked", initial_status="blocked")
        self.bridge.track(scenarios.ISSUE, task.id)
        self.bridge.recover()
        self.assertEqual(self.board.get(task.id).status, "blocked")

    def test_status_exposes_verified_local_executor_for_chat_origin_creation(self):
        status = self.bridge.native_status(scenarios.ISSUE)
        self.assertFalse(status["linked"])
        self.assertEqual(status["local"], {"profile": "default", "board": "default"})

    def test_current_linear_stop_stops_initial_queued_work_in_same_second(self):
        stamp = time.time()
        event = self.fixture.event(stamp=stamp)
        self.bridge.handle_webhook(event)
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        self.assertEqual(self.board.get(task_id).status, "ready")
        self.bridge.handle_webhook(self.fixture.event("prompted", signal="stop", stamp=stamp + 0.001))
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.fixture.make_bridge().recover()
        self.assertEqual(self.board.get(task_id).status, "blocked")

    def test_delayed_linear_stop_cannot_override_a_later_native_resume(self):
        stamp = time.time()
        self.bridge.handle_webhook(self.fixture.event(stamp=stamp))
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        with self.board.conn() as conn:
            self.assertTrue(scenarios.kb.block_task(conn, task_id, reason="native Stop", kind="needs_input"))
            self.assertTrue(scenarios.kb.unblock_task(conn, task_id))
        self.bridge.handle_webhook(self.fixture.event("prompted", signal="stop", stamp=stamp + 0.001))
        self.assertEqual(self.board.get(task_id).status, "ready")

    def test_no_row_stop_is_carried_into_a_concurrently_committed_native_binding(self):
        stopper = self.fixture.make_bridge()
        saw_none, continue_stop, stop_done = threading.Event(), threading.Event(), threading.Event()
        get, activate = stopper.store.get, self.board.activate
        def snapshot(issue_id):
            row = get(issue_id)
            if threading.current_thread().name.startswith("stop") and not saw_none.is_set():
                self.assertIsNone(row)
                saw_none.set()
                self.assertTrue(continue_stop.wait(5))
            return row
        def after_stop(task_id, token, **kwargs):
            continue_stop.set()
            self.assertTrue(stop_done.wait(5))
            return activate(task_id, token, **kwargs)
        stamp = time.time()
        def stop():
            try:
                stopper.handle_webhook(self.fixture.event("prompted", signal="stop", stamp=stamp + 0.01))
            finally:
                stop_done.set()
        with patch.object(stopper.store, "get", side_effect=snapshot), patch.object(self.board, "activate", side_effect=after_stop):
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="stop") as pool:
                request = pool.submit(stop)
                self.assertTrue(saw_none.wait(5))
                self.bridge.handle_webhook(self.fixture.event(stamp=stamp))
                request.result(5)
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        self.fixture.make_bridge().recover()
        self.assertEqual(self.board.get(task_id).status, "blocked")
        with self.board.conn() as conn:
            self.assertIsNone(scenarios.kb.claim_task(conn, task_id, claimer="must-not-start"))

    def test_unavailable_binding_does_not_abort_other_recovery_or_reporting(self):
        from hermes_cli.kanban_db_boards import create_board, remove_board
        create_board("project")
        board = self.board.for_board("project")
        bridge = scenarios.Bridge(self.store, self.fixture.authority.api(scope=None), board, profile="default")
        bridge.handle_webhook(self.fixture.event())
        remove_board("project")
        restarted = self.fixture.make_bridge()
        with patch.object(restarted, "pump_kanban", wraps=restarted.pump_kanban) as pump:
            restarted.recover()
            self.assertEqual(pump.call_count, 1)

    def test_lost_activation_receipt_still_accepts_current_same_second_stop(self):
        now = int(time.time()) + 0.1
        activate = self.board.activate
        def lost(task_id, token, **kwargs):
            self.assertTrue(activate(task_id, token, **kwargs))
            raise ConnectionError("lost successful native response")
        with patch("time.time", return_value=now), patch.object(self.board, "activate", side_effect=lost):
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(self.fixture.event(stamp=now))
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        with patch("time.time", return_value=now):
            self.fixture.make_bridge().recover()
            self.bridge.handle_webhook(self.fixture.event("prompted", signal="stop", stamp=now + 0.1))
        self.assertEqual(self.board.get(task_id).status, "blocked")

    def test_same_second_linear_followup_reaches_existing_native_run(self):
        now = int(time.time()) + 0.1
        with patch("time.time", return_value=now):
            self.bridge.handle_webhook(self.fixture.event(stamp=now))
            task_id = self.store.get(scenarios.ISSUE)["task_id"]
            with self.board.conn() as conn:
                native = scenarios.kb.claim_task(conn, task_id, claimer="native-test-worker")
            self.bridge.handle_webhook(self.fixture.event("prompted", body="use the v2 endpoint", stamp=now + 0.1))
        self.assertEqual([comment.body for comment in self.fixture.comments(task_id)], ["use the v2 endpoint"])
        self.assertEqual(self.board.get(task_id).current_run_id, native.current_run_id)

    def test_restart_recovers_creation_receipt_lost_after_binding(self):
        settle = self.store.native_intent_settle
        def lost(key, updates):
            if key == f"create:{scenarios.ISSUE}" and updates.get("state") == "applied":
                raise ConnectionError("crash after binding before creation receipt")
            return settle(key, updates)
        with patch.object(self.bridge.store, "native_intent_settle", side_effect=lost):
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(self.fixture.event())
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        self.fixture.make_bridge().recover()
        self.assertEqual(self.board.get(task_id).status, "ready")
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_transient_binding_failure_retries_in_existing_service_loop(self):
        with patch.object(self.bridge, "_assignment_activation", side_effect=ConnectionError("crash after binding")):
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(self.fixture.event())
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        restarted = self.fixture.make_bridge()
        with patch.object(restarted, "_assignment_activation", side_effect=scenarios.LinearError("temporary read outage")):
            restarted.recover()
        self.assertEqual(self.board.get(task_id).status, "blocked")
        restarted.tick()
        self.assertEqual(self.board.get(task_id).status, "ready")

    def test_lost_stop_response_cannot_cancel_a_later_native_resume_on_retry(self):
        stamp = time.time()
        self.bridge.handle_webhook(self.fixture.event(stamp=stamp))
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        event = self.fixture.event("prompted", signal="stop", stamp=stamp + 0.01)
        stop = self.board.stop
        def lost(*args, **kwargs):
            self.assertTrue(stop(*args, **kwargs))
            raise ConnectionError("lost Stop response")
        with patch.object(self.board, "stop", side_effect=lost):
            with self.assertRaises(ConnectionError):
                self.bridge.handle_webhook(event)
        with self.board.conn() as conn:
            self.assertTrue(scenarios.kb.unblock_task(conn, task_id))
        self.fixture.make_bridge().handle_webhook(event)
        self.assertEqual(self.board.get(task_id).status, "ready")

    def test_recorded_no_row_stop_survives_handler_crash_before_native_control(self):
        stamp = time.time()
        self.bridge.handle_webhook(self.fixture.event(stamp=stamp))
        task_id = self.store.get(scenarios.ISSUE)["task_id"]
        stopper = self.fixture.make_bridge()
        get = stopper.store.get
        reads = []
        def stale(issue_id):
            reads.append(issue_id)
            return None if len(reads) == 1 else get(issue_id)
        with patch.object(stopper.store, "get", side_effect=stale):
            with patch.object(stopper, "_pending_assignment_stop", side_effect=ConnectionError("handler crash")):
                with self.assertRaises(ConnectionError):
                    stopper.handle_webhook(self.fixture.event("prompted", signal="stop", stamp=stamp + 0.01))
        self.assertEqual(self.board.get(task_id).status, "ready")
        self.fixture.make_bridge().recover()
        self.assertEqual(self.board.get(task_id).status, "blocked")

    def test_changed_creation_is_reported_once_without_restart_notice_spam(self):
        create = self.board.create
        def stopped(**fields):
            task = create(**fields)
            with self.board.conn() as conn:
                self.assertTrue(scenarios.kb.block_task(conn, task.id, reason="operator Stop", kind="needs_input"))
            return task
        with patch.object(self.board, "create", side_effect=stopped):
            self.assign()
        def notices():
            return [row for row in self.store.pending() if
                    (row["payload"].get("content") or {}).get("body", "").startswith("Native work changed after creation;")]
        before = len(notices())
        self.fixture.make_bridge().recover()
        self.fixture.make_bridge().recover()
        self.assertEqual(len(notices()), before)


if __name__ == "__main__":
    unittest.main()
