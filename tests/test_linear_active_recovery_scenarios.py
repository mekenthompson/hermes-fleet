"""Upgrade regression: retained reporting cursors must not strand active work.

An older consumer could advance the reporting cursor over a native ``claimed``
event without projecting In Progress. Real pinned Core tasks, the plugin Store,
the registered chat RPC and the HTTP fake API exercise the current entrypoints.
"""
import json
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
        # Shared setUp stamps issue history on its fixed (2030) clock; real
        # Linear history for this issue precedes the claim made below.
        f.remote.history.pop("issue-1", None)
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

    def running_unconsumed(self, delegate):
        """Current build: the claimed event is captured by the ordinary path."""
        f = self.fixture
        # Shared setUp stamps issue history on its fixed (2030) clock; real
        # Linear history for this issue precedes the claim made below.
        f.remote.history.pop("issue-1", None)
        f.fixture.clock.now = time.time() - 2
        f.remote.set_delegate("issue-1", delegate)
        with f.board.conn() as db:
            task = f.board.kb.create_task(db, title="Fresh active work", board=f.board.board,
                assignee=f.board.executor_profile, created_by="operator",
                body="Linear issue issue-1: " + f.issue["url"])
            f.board.kb.claim_task(db, task, claimer="native-worker")
        f.bridge.track("issue-1", task, completion_scope="progress")
        return task

    def ticks(self, count=3):
        for _ in range(count):
            self.fixture.bridge.recover()
            self.fixture.bridge.pump_kanban()
            self.fixture.bridge.flush()

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
        from hermes_fleet_linear_plugin.store import Store
        for _ in range(3):
            f.bridge.store = Store(f.bridge.store.path)  # restart between ticks
            f.bridge.recover()
            f.bridge.pump_kanban()
            f.bridge.flush()
        self.assertEqual(len(self.recovered_status_rows()), 1)
        self.assertEqual(len(f.mutations()), mutations)
        self.assertEqual(f.snapshot(), before)

    def delegate_rows(self):
        with self.fixture.bridge.store._tx() as db:
            return [json.loads(row[0]) for row in db.execute(
                "SELECT payload FROM outbox WHERE kind='delegate' ORDER BY rowid")]

    def test_recovery_routes_own_delegate_for_linked_undelegated_work_and_echo_keeps_run(self):
        # Authority is the existing session-less reporting link, recorded with
        # recovery provenance and no fabricated caller/requester.
        from hermes_fleet_linear_plugin.store import Store
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        f.bridge.recover()
        self.later()
        f.bridge.flush()
        self.assertEqual(f.issue["delegate"]["id"], SELF)
        self.assertEqual(f.remote.state("issue-1"), "In Progress")
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})
        rows = self.delegate_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["recovered_binding"]["task_id"], task)
        for field in ("caller_context", "requester_sessions", "requester_activities", "work_owner", "content_action"):
            self.assertNotIn(field, rows[0])
        self.assertEqual(f.snapshot(), before)
        mutations = len(f.mutations())
        for _ in range(3):
            f.bridge.store = Store(f.bridge.store.path)
            f.bridge.recover()
            f.bridge.pump_kanban()
            f.bridge.flush()
        self.assertEqual(len(self.delegate_rows()), 1)
        self.assertEqual(len(f.mutations()), mutations)
        # Linear's own-assignment echo reuses the same native task and run.
        session = "recovery-assignment-echo"
        f.remote.session_records[session] = {"id": session, "creator": None,
            "appUser": {"id": SELF}, "issue": {"id": "issue-1"}}
        event = f.remote.session_event("created", "issue-1", session, creator=None)
        event["agentSession"].pop("creatorId")
        self.assertTrue(f.bridge.handle_webhook(event))
        self.assertTrue(f.bridge.handle_webhook(event))  # repeated webhook delivery
        self.assertEqual(f.snapshot(), before)
        self.assertEqual(f.bridge.store.native_binding("issue-1")["task_id"], task)
        self.assertEqual(f.board.get(task).current_run_id, run)
        self.assertIsNone(f.bridge.store.native_intent("create:issue-1"))

    def test_human_delegate_after_recovery_queue_wins_at_delivery(self):
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        f.bridge.recover()
        self.assertEqual(len(self.delegate_rows()), 1)
        self.later()
        f.remote.set_delegate("issue-1", OTHER)
        f.bridge.flush()
        f.bridge.flush()
        self.assertEqual(f.issue["delegate"]["id"], OTHER["id"])
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})
        self.assertEqual(f.mutations(), [])  # neither delegate nor stale status overwrites the human
        self.assertEqual(f.snapshot(), before)

    def test_recovery_holds_delegate_routing_when_scope_requires_requester_session(self):
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        f.bridge.api.specialist_scope = {**f.bridge.api.specialist_scope, "requester_basis": "agent_session"}
        f.bridge.recover()
        self.later()
        f.bridge.flush()
        self.assertIsNone(f.issue["delegate"])
        self.assertEqual(self.delegate_rows(), [])
        self.assertEqual(f.snapshot(), before)

    def test_completed_internal_stage_is_not_routed_and_does_not_close_issue(self):
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        with f.board.conn() as db:
            self.assertTrue(f.board.kb.complete_task(db, task, summary="stage done"))
        self.later()
        f.bridge.recover()
        f.bridge.flush()
        self.assertNotEqual(f.remote.state("issue-1"), "Done")
        self.assertEqual(self.delegate_rows(), [])

    def test_background_recovery_cannot_fabricate_session_delegate_routing(self):
        # Delegate routing is a caller-authorized content action. A session-owned
        # binding on an undelegated issue stays held until an authorized caller
        # links/routes it; recovery neither assigns a delegate nor executes.
        f = self.fixture
        f.fixture.clock.now = time.time() - 2
        f.remote.set_delegate("issue-1", None)
        with f.board.conn() as db:
            task = f.board.kb.create_task(db, title="Session work", board=f.board.board,
                assignee=f.board.executor_profile, created_by="operator",
                body="Linear issue issue-1: " + f.issue["url"])
            f.board.kb.claim_task(db, task, claimer="native-worker")
        f.bridge.track("issue-1", task, completion_scope="progress", requester_session="linear-session-1")
        for event in f.board.history(task, 0):
            f.bridge.store.capture_event("issue-1", event.id, [], at=f.fixture.clock())
        before = f.snapshot()
        f.bridge.recover()
        self.later()
        f.bridge.flush()
        self.assertIsNone(f.issue["delegate"])
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})
        self.assertEqual(f.mutations(), [])
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
        # A deliberate later human move (Linear records no history for a
        # same-state no-op, which is just a touch).
        f.remote.set_state("issue-1", "Blocked")
        f.bridge.recover()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "Blocked")
        self.assertEqual(f.mutations(), [])

    def test_later_comment_or_activity_does_not_strand_delegated_running_work_in_todo(self):
        # Review probe: the issue is already delegated to this agent, then
        # touched after the claim (a comment, attachment, label or agent
        # activity). updatedAt moves but nobody changed routing, so the
        # running work must still show In Progress.
        task, run, before = self.retained_running(delegate={"id": SELF})
        f = self.fixture
        self.later()
        f.remote.touch("issue-1")
        f.remote.comment_updates_issue = f.remote.activity_updates_issue = True
        f.bridge.recover()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "In Progress")
        self.assertEqual(f.issue["delegate"]["id"], SELF)
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})
        self.assertEqual(len(self.recovered_status_rows()), 1)
        self.assertEqual(f.snapshot(), before)

    def test_human_state_edit_after_claim_wins_even_after_later_touches(self):
        task, run, before = self.retained_running(delegate={"id": SELF})
        f = self.fixture
        self.later()
        f.remote.set_state("issue-1", "Blocked")  # a deliberate later human move
        f.fixture.clock.now += 1
        f.remote.touch("issue-1")  # then an unrelated comment
        f.bridge.recover()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "Blocked")
        self.assertEqual(f.mutations(), [])

    def test_delegate_recovery_is_not_starved_by_unrelated_pending_issue_writes(self):
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        f.bridge.store.enqueue("comment", {"issue_id": "issue-1", "body": "unrelated pending note",
                                           "id": "00000000-0000-4000-8000-000000000001"}, at=f.fixture.clock())
        f.bridge.pump_kanban()
        self.assertEqual(len(self.delegate_rows()), 1)

    def test_delegate_recovery_survives_agent_comment_landing_before_it(self):
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        f.bridge.recover()
        self.assertEqual(len(self.delegate_rows()), 1)
        self.later()
        f.remote.touch("issue-1")  # own progress comment lands first
        f.bridge.flush()
        f.bridge.flush()
        self.assertEqual(f.issue["delegate"]["id"], SELF)
        self.assertEqual(f.remote.state("issue-1"), "In Progress")
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})

    def test_human_state_move_after_delegate_recovery_queue_wins(self):
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        f.bridge.recover()
        self.assertEqual(len(self.delegate_rows()), 1)
        self.later()
        f.remote.set_state("issue-1", "Blocked")
        f.bridge.flush()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "Blocked")
        self.assertIsNone(f.issue["delegate"])
        self.assertEqual(f.mutations(), [])

    def test_human_state_move_after_claim_blocks_delegate_recovery(self):
        task, run, before = self.retained_running(delegate=None)
        f = self.fixture
        self.later()
        f.remote.set_state("issue-1", "Blocked")
        for _ in range(2):
            f.bridge.recover()
            f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "Blocked")
        self.assertIsNone(f.issue["delegate"])
        self.assertEqual(self.delegate_rows(), [])
        self.assertEqual(f.mutations(), [])

    def test_status_recovery_runs_even_when_delegate_recovery_fails(self):
        task, run, before = self.retained_running(delegate={"id": SELF})
        f = self.fixture
        def broken(*args, **kwargs):
            raise RuntimeError("delegate recovery unavailable")
        f.bridge._recover_binding_delegate = broken
        f.bridge.recover()
        self.later()
        f.bridge.flush()
        self.assertEqual(f.remote.state("issue-1"), "In Progress")

    def test_comment_before_ordinary_claim_send_still_shows_in_progress(self):
        # Review probe: the claim is not yet projected (worker just claimed, or
        # a restart backlog) and a comment touches the issue before the status
        # send. updatedAt moves; routing does not. Running work is In Progress.
        self.running_unconsumed(delegate={"id": SELF})
        f = self.fixture
        self.later()
        f.remote.touch("issue-1")
        self.ticks()
        self.assertEqual(f.remote.state("issue-1"), "In Progress")
        self.assertEqual(f.issue["delegate"]["id"], SELF)
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})

    def test_human_state_move_before_ordinary_claim_send_wins(self):
        self.running_unconsumed(delegate={"id": SELF})
        f = self.fixture
        self.later()
        f.remote.set_state("issue-1", "Blocked")
        f.fixture.clock.now += 1
        f.remote.touch("issue-1")
        self.ticks()
        self.assertEqual(f.remote.state("issue-1"), "Blocked")
        self.assertEqual(f.mutations(), [])
        # Bounded: at most one recovery retry after the refused claim status.
        self.assertLessEqual(len(self.recovered_status_rows()), 1)

    def test_in_progress_refused_by_older_build_is_recovered(self):
        # Live upgrade case: an older build fenced the claim status on
        # updatedAt, so a comment made it record In Progress as sent but not
        # applied. The current build must not read that as "already shown".
        self.running_unconsumed(delegate={"id": SELF})
        f = self.fixture
        self.later()
        f.remote.touch("issue-1")
        current = f.bridge.may_write
        f.bridge.may_write = lambda issue, claim, queued_at=0.0, seen=None, source_ms=None, history_fence=False: \
            current(issue, claim, queued_at, seen, source_ms, False)  # the older updatedAt-only fence
        self.ticks(1)
        self.assertEqual(f.remote.state("issue-1"), "Todo")
        self.assertTrue(f.bridge.store.latest_status("issue-1")["refused"])
        # The older build's claimed-event row carried no claim provenance.
        refused = f.bridge.store.outbox_row(f.bridge.store.latest_status("issue-1")["id"])
        refused["payload"].pop("recovered_claim")
        f.bridge.store.rewrite(refused["id"], refused["payload"], refused["next_at"])
        f.bridge.may_write = current  # upgrade
        self.ticks()
        self.assertEqual(f.remote.state("issue-1"), "In Progress")
        self.assertEqual(f.issue["assignee"], {"id": "human-assignee"})
        self.assertEqual(len(self.recovered_status_rows()), 1)

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
