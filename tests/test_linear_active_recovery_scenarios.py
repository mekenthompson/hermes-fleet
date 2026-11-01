"""Upgrade regression: retained reporting cursors must not strand active work.

An older consumer could advance the reporting cursor over a native ``claimed``
event without projecting In Progress. Real pinned Core tasks, the plugin Store,
the registered chat RPC and the HTTP fake API exercise the current entrypoints.
"""
import os
import time
import unittest

import test_linear_delegate_routing_scenarios as routing
from linear_fake_api import OTHER, SELF


@unittest.skipUnless(os.environ.get("HERMES_AGENT_SRC"), "requires explicit native source")
class ActiveRecoveryScenarios(unittest.TestCase):
    def setUp(self):
        self.fixture = routing.DelegateRoutingScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def retained_running(self, delegate=None):
        f = self.fixture
        f.fixture.clock.now = time.time() - 2
        f.remote.set_delegate("issue-1", delegate)
        with f.board.conn() as db:
            task = f.board.kb.create_task(db, title="Retained active work", board=f.board.board,
                assignee=f.board.executor_profile, created_by="operator",
                body="Linear issue issue-1: " + f.issue["url"])
            running = f.board.kb.claim_task(db, task, claimer="native-worker")
        # Before automatic chat routing existed, track persisted this binding.
        # The old consumer advanced over claimed without a status projection.
        f.bridge.track("issue-1", task, completion_scope="progress")
        for event in f.board.history(task, 0):
            self.assertTrue(f.bridge.store.capture_event("issue-1", event.id, [], at=f.fixture.clock()))
        from hermes_fleet_linear_plugin.store import Store
        f.bridge.store = Store(f.bridge.store.path)
        return task, running.current_run_id, f.snapshot()

    def later(self):
        self.fixture.fixture.clock.now = time.time() + 2

    def recovered_status_rows(self):
        with self.fixture.bridge.store._tx() as db:
            return db.execute("SELECT id FROM outbox WHERE kind='status' AND "
                              "json_extract(payload, '$.recovered_claim') IS NOT NULL").fetchall()

    def test_other_authorized_chat_follows_existing_run_without_transfer(self):
        import uuid
        from gateway.platforms.api_server import APIServerAdapter
        task, run, before = self.retained_running()
        f = self.fixture
        APIServerAdapter._bind_api_server_session(chat_id="another-chat", session_key="api:another-chat",
            session_id="another-durable-session", profile="default", session_history_delivery="1")
        status = f.call({"action": "status", "issue": "ABC-1"})
        self.assertTrue(status["ok"], status)
        self.assertEqual(f.snapshot(), before, "status reads cannot execute or claim")
        request = {"action": "followup", "issue": "ABC-1", "id": str(uuid.uuid4()),
                   "body": "Please continue the remaining work and keep the current result."}
        result = f.call(request)
        self.assertTrue(result["ok"], result)
        after = f.snapshot()
        retry = f.call(request)
        self.assertTrue(retry["ok"], retry)
        self.assertEqual(f.snapshot(), after)
        self.assertEqual(f.board.get(task).current_run_id, run)
        self.assertEqual(after["tasks"], before["tasks"])
        self.assertEqual(after["task_runs"], before["task_runs"])
        self.assertIsNone(f.bridge.store.native_intent("create:issue-1"))
        self.assertEqual(f.fixture.seen[-1].session_id, "another-durable-session")

    def test_recovery_projects_consumed_running_claim_without_new_execution(self):
        task, run, before = self.retained_running(delegate={"id": SELF})
        f = self.fixture
        f.bridge.recover()
        self.later()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "In Progress")
        self.assertEqual(f.issue["delegate"]["id"], SELF)
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})
        self.assertEqual(f.snapshot(), before)
        self.assertEqual(f.board.get(task).current_run_id, run)
        # Repeated recovery/service loops are idempotent: one intent, one write.
        mutations = len(f.mutations())
        for _ in range(3):
            f.bridge.recover()
            f.bridge.pump_kanban()
            f.bridge.flush()
        self.assertEqual(len(self.recovered_status_rows()), 1)
        self.assertEqual(len(f.mutations()), mutations)
        self.assertEqual(f.snapshot(), before)

    def test_background_recovery_reports_but_never_claims_an_undelegated_issue(self):
        # Existing policy: a session-less native binding may report status on
        # an undelegated issue. Background recovery has no caller identity, so
        # it must never assign itself as delegate or touch the human assignee.
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        f.bridge.recover()
        self.later()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "In Progress")
        self.assertIsNone(f.issue["delegate"])
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})
        self.assertEqual(len(f.mutations()), 1)
        self.assertEqual(f.snapshot(), before)

    def test_human_reassignment_after_claim_wins_over_recovery(self):
        task, run, before = self.retained_running(delegate={"id": SELF})
        f = self.fixture
        self.later()
        f.remote.set_delegate("issue-1", OTHER)
        f.bridge.recover()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "Todo")
        self.assertEqual(f.issue["delegate"]["id"], OTHER["id"])
        self.assertEqual(f.mutations(), [])
        self.assertEqual(f.snapshot(), before)

    def test_human_state_edit_after_claim_wins_over_recovery(self):
        task, run, before = self.retained_running(delegate={"id": SELF})
        f = self.fixture
        self.later()
        f.remote.set_state("issue-1", "Todo")  # a deliberate later human edit
        f.bridge.recover()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "Todo")
        self.assertEqual(f.mutations(), [])

    def test_stopped_native_work_is_not_reported_as_running(self):
        task, run, _ = self.retained_running(delegate={"id": SELF})
        f = self.fixture
        with f.board.conn() as db:
            self.assertTrue(f.board.kb.block_task(db, task, reason="operator stop", expected_run_id=run))
        self.later()
        f.bridge.recover()
        f.bridge.flush()
        self.assertNotEqual(f.remote.state("issue-1"), "In Progress")
        self.assertEqual(self.recovered_status_rows(), [])

    def test_retracking_retained_active_work_recovers_status_without_new_execution(self):
        task, run, before = self.retained_running()
        f = self.fixture
        f.fixture.bind_api()
        linked = f.call({"action": "track", "issue": "ABC-1", "task_id": task,
                         "completion_scope": "progress"})
        self.assertTrue(linked["ok"], linked)
        # A competing second link request for the same work is a no-op.
        again = f.call({"action": "track", "issue": "ABC-1", "task_id": task,
                        "completion_scope": "progress"})
        self.assertTrue(again["ok"], again)
        f.bridge.pump_kanban()
        f.bridge.pump_kanban()
        self.later()
        f.bridge.flush()
        self.assertEqual(f.issue["delegate"]["id"], SELF)
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})
        self.assertEqual(f.remote.state("issue-1"), "In Progress")
        self.assertEqual(len(self.recovered_status_rows()), 1)
        self.assertEqual(f.snapshot(), before)
        self.assertEqual(f.board.get(task).current_run_id, run)
