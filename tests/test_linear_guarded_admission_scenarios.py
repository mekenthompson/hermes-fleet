"""Real native guarded admission; no fabricated client or approval context."""
from __future__ import annotations

import os
import time
import unittest
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
        def stopped(task_id, expected_event_id):
            with self.board.conn() as conn:
                self.assertTrue(scenarios.kb.block_task(conn, task_id, reason="operator Stop", kind="needs_input",
                                                        expected_event_id=expected_event_id))
            return activate(task_id, expected_event_id)
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
        def lost(task_id, expected_event_id):
            self.assertTrue(activate(task_id, expected_event_id))
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
        def lost(task_id, expected_event_id):
            self.assertTrue(activate(task_id, expected_event_id))
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


if __name__ == "__main__":
    unittest.main()
