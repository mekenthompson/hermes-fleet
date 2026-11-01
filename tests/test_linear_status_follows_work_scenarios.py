"""Linear status follows native execution (job spec: "Linear status follows the work").

One mapping from the bound task's native status to the team's workflow state,
applied after each pump and on the reporting recheck. Real pinned Core tasks,
the plugin Store and the HTTP fake API exercise the current entrypoints.
"""
import json
import os
import time
import unittest

import test_linear_delegate_routing_scenarios as routing
from linear_fake_api import SELF, STATES

IN_REVIEW = {"id": "st-review", "name": "In Review", "type": "started"}


@unittest.skipUnless(os.environ.get("HERMES_AGENT_SRC"), "requires explicit native source")
class StatusFollowsWorkScenarios(unittest.TestCase):
    def setUp(self):
        self.fixture = routing.DelegateRoutingScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        f = self.fixture
        f.remote.history.pop("issue-1", None)  # setUp stamps history on its fixed 2030 clock
        f.fixture.clock.now = time.time() - 5
        f.remote.set_delegate("issue-1", {"id": SELF}, actor=SELF)
        self.states = [*STATES, IN_REVIEW]
        f.issue["team"]["states"]["nodes"] = self.states
        self.remote, self.board, self.bridge = f.remote, f.board, f.bridge

    def set_state(self, name, actor="human-1"):
        """Linear records who moved the issue and when."""
        before = dict(self.fixture.issue)
        self.fixture.issue["state"] = dict(next(s for s in self.states if s["name"] == name))
        self.fixture.issue["updatedAt"] = self.fixture.fixture.clock.iso()
        self.remote.record_history("issue-1", actor, before, self.fixture.issue)

    def task(self, *, scope="outcome", claim=False, **fields):
        with self.board.conn() as db:
            task = self.board.kb.create_task(db, title="Native work", board=self.board.board,
                assignee=self.board.executor_profile, created_by="operator",
                body="Linear issue issue-1: " + self.fixture.issue["url"], **fields)
            run = self.board.kb.claim_task(db, task, claimer="native-worker").current_run_id if claim else None
        self.bridge.track("issue-1", task, completion_scope=scope)
        return task, run

    def native(self, action, *args, **kwargs):
        with self.board.conn() as db:
            return getattr(self.board.kb, action)(db, *args, **kwargs)

    def ticks(self, count=3):
        self.fixture.fixture.clock.now = time.time() + 2  # Linear reads after the native facts
        for _ in range(count):
            self.bridge.pump_kanban()
            self.bridge.flush()

    def state(self):
        return self.remote.state("issue-1")

    def mutations(self):
        return self.fixture.mutations()

    def failed(self):
        with self.bridge.store._tx() as db:
            return db.execute("SELECT kind, payload FROM outbox WHERE state='failed'").fetchall()

    # -- one row of the spec table each ----------------------------------

    def test_queued_work_shows_todo_not_in_progress(self):
        self.set_state("In Progress", actor=SELF)  # a phantom In Progress from an older build
        task, _ = self.task()
        self.assertEqual(self.board.get(task).status, "ready")
        self.ticks()
        self.assertEqual(self.state(), "Todo")

    def test_unblocked_or_promoted_but_only_queued_is_todo(self):
        task, _ = self.task(initial_status="blocked")
        self.ticks()
        self.native("unblock_task", task)
        self.assertEqual(self.board.get(task).status, "ready")
        self.ticks()
        self.assertEqual(self.state(), "Todo")  # was In Progress before this change
        self.assertNotIn("In Progress", [self.remote.state("issue-1")])

    def test_running_work_shows_in_progress(self):
        self.task(claim=True)
        self.ticks()
        self.assertEqual(self.state(), "In Progress")
        self.assertEqual(self.fixture.issue["delegate"]["id"], SELF)
        self.assertEqual(self.fixture.issue["assignee"], {"id": "human-assignee"})

    def test_waiting_on_a_person_is_blocked_with_one_plain_sentence(self):
        task, run = self.task(claim=True)
        self.ticks()
        self.native("block_task", task, reason="Pick the staging or production database", kind="needs_input",
                    expected_run_id=run)
        self.ticks()
        self.assertEqual(self.state(), "Blocked")
        said = [body for body in self.remote.bodies("issue-1") if body.startswith("Waiting on you")]
        self.assertEqual(said, ["Waiting on you: Pick the staging or production database. Once this is answered, "
                                "ask the agent to resume and it continues the same task."])

    def test_team_without_blocked_keeps_its_state_and_still_posts_the_sentence(self):
        self.states[:] = [s for s in self.states if s["name"] not in ("Blocked", "In Review")]
        task, run = self.task(claim=True)
        self.ticks()
        self.native("block_task", task, reason="Grant read access to the billing repo", kind="needs_input",
                    expected_run_id=run)
        self.ticks()
        self.assertEqual(self.state(), "In Progress")
        self.assertTrue(any("Grant read access to the billing repo" in body for body in self.remote.bodies("issue-1")))
        self.assertEqual(self.failed(), [])  # a missing state is not a delivery failure

    def test_finished_stage_awaiting_review_is_in_review(self):
        task, run = self.task(claim=True)
        self.ticks()
        self.assertTrue(self.native("request_review", task, summary="PR is up for review", expected_run_id=run))
        self.assertEqual(self.board.get(task).status, "review")
        self.ticks()
        self.assertEqual(self.state(), "In Review")

    def test_work_held_by_the_open_pr_guard_is_in_review(self):
        task, _ = self.task()
        self.ticks()
        self.assertEqual(self.state(), "Todo")
        with self.board.conn() as db, self.board.kb.write_txn(db):
            self.board.kb._append_event(db, task, "respawn_guarded", {"reason": "active_pr"})
        self.ticks()
        self.assertEqual(self.state(), "In Review")

    def stage_done(self, summary):
        task, run = self.task(scope="progress", claim=True)
        self.ticks()
        self.assertTrue(self.native("complete_task", task, summary=summary, expected_run_id=run))
        self.ticks()
        self.assertTrue(any("This stage is finished" in body for body in self.remote.bodies("issue-1")))
        return self.state()

    def test_internal_stage_done_shows_the_next_step_never_done(self):
        self.assertEqual(self.stage_done("Drafted the migration plan."), "Todo")

    def test_internal_stage_done_with_a_pr_awaits_review(self):
        self.assertEqual(self.stage_done("Opened https://github.com/example/repo/pull/12 for review."), "In Review")

    def test_verified_outcome_is_done(self):
        task, run = self.task(claim=True)
        self.ticks()
        self.assertTrue(self.native("complete_task", task, summary="Rotated the staging certificate and checked it live.",
                                    expected_run_id=run))
        self.ticks()
        self.assertEqual(self.state(), "Done")

    def test_archived_without_outcome_leaves_state_and_says_nothing(self):
        task, _ = self.task(claim=True)
        self.ticks()
        said, writes = list(self.remote.bodies("issue-1")), len(self.mutations())
        self.native("archive_task", task)
        self.ticks()
        self.assertEqual(self.state(), "In Progress")
        self.assertEqual(self.remote.bodies("issue-1"), said)
        self.assertEqual(len(self.mutations()), writes)

    # -- the newer human edit wins (#254's routing-history fence) --------

    def test_human_state_move_after_the_native_change_wins(self):
        task, run = self.task(claim=True)
        self.ticks()
        self.native("block_task", task, reason="Need the API key", kind="needs_input", expected_run_id=run)
        self.fixture.fixture.clock.now = time.time() + 2
        self.set_state("Todo")  # Ken parks it himself after the block
        writes = len(self.mutations())
        self.ticks()
        self.assertEqual(self.state(), "Todo")
        self.assertEqual(len(self.mutations()), writes)

    def test_a_later_comment_does_not_stop_the_status_from_following_the_work(self):
        task, run = self.task(claim=True)
        self.ticks()
        self.native("block_task", task, reason="Need the API key", kind="needs_input", expected_run_id=run)
        self.fixture.fixture.clock.now = time.time() + 2
        self.remote.touch("issue-1")  # a comment moves updatedAt; nobody changed routing
        self.ticks()
        self.assertEqual(self.state(), "Blocked")

    def test_steady_state_is_idempotent_across_restarts(self):
        self.task(claim=True)
        self.ticks()
        writes = len(self.mutations())
        from hermes_fleet_linear_plugin.store import Store
        for _ in range(3):
            self.bridge.store = Store(self.bridge.store.path)
            self.bridge.recover()
            self.ticks(1)
        self.assertEqual(self.state(), "In Progress")
        self.assertEqual(len(self.mutations()), writes)

    # -- stale and legacy records: reconcile once, then stop tracking ----

    def test_binding_whose_native_task_is_gone_is_parked_once_and_untracked(self):
        task, _ = self.task(claim=True)
        with self.board.conn() as db:  # other work on the same board
            self.board.kb.create_task(db, title="Unrelated work", board=self.board.board, created_by="operator")
        self.ticks()
        self.assertEqual(self.state(), "In Progress")
        writes = len(self.mutations())
        self.native("delete_task", task)
        for _ in range(3):
            self.bridge.recheck(force=True)
            self.bridge.flush()
        self.assertEqual(self.state(), "Todo")
        self.assertIsNone(self.bridge.store.get("issue-1"))
        self.assertIsNone(self.bridge.store.native_binding("issue-1"))
        self.assertEqual(len(self.mutations()), writes + 1)  # Todo, once

    def test_an_emptied_board_is_not_evidence_that_bound_work_is_gone(self):
        task, _ = self.task(claim=True)
        self.ticks()
        self.native("delete_task", task)  # the board now has no tasks at all
        self.bridge.recheck(force=True)
        self.assertEqual(self.state(), "In Progress")
        self.assertIsNotNone(self.bridge.store.native_binding("issue-1"))

    def legacy(self):
        self.assertTrue(self.bridge.store.put("issue-1", "chat", "agent:main:slack:dm:legacy"))

    def test_pre_rebuild_chat_record_moves_to_todo_once_and_is_retired(self):
        self.set_state("In Progress", actor=SELF)
        self.legacy()
        self.fixture.fixture.clock.now = time.time() + 2
        self.bridge.recheck(force=True)
        self.assertEqual(self.state(), "Todo")
        self.assertIsNone(self.bridge.store.get("issue-1"))
        writes = len(self.mutations())
        self.legacy()  # rerun safety: an already-parked issue is not written again
        self.bridge.recheck(force=True)
        self.assertEqual(len(self.mutations()), writes)
        self.assertIsNone(self.bridge.store.get("issue-1"))

    def test_pre_rebuild_record_leaves_a_human_choice_alone(self):
        self.set_state("In Progress", actor=SELF)
        self.fixture.fixture.clock.now += 1
        self.set_state("Blocked")  # Ken moved it himself
        self.legacy()
        self.bridge.recheck(force=True)
        self.assertEqual(self.state(), "Blocked")
        self.assertEqual(self.mutations(), [])
        self.assertIsNone(self.bridge.store.get("issue-1"))


if __name__ == "__main__":
    unittest.main()
