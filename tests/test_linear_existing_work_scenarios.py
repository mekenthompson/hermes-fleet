"""Native issue discovery before assignment admission; no client-UAT claim."""
import unittest
from unittest.mock import patch

import test_linear_native_bridge_scenarios as native


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class ExistingWorkScenarios(unittest.TestCase):
    def setUp(self):
        self.fixture = native.NativeBridgeScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.board, self.bridge, self.store = self.fixture.board, self.fixture.bridge, self.fixture.store
        self.issue = self.fixture.authority.issues[native.ISSUE]

    def existing(self, *, board=None, key=None, assignee="default", body=None, contract=None):
        adapter = self.board if board is None else native.Kanban(board, profile="default", profile_home=self.fixture.home)
        with adapter.conn() as conn:
            task_id = native.kb.create_task(conn, board=adapter.board, assignee=assignee,
                title=f"{self.issue['identifier']}: existing work", created_by="operator",
                body=body if body is not None else f"Linear issue {native.ISSUE}: {self.issue['url']}",
                idempotency_key=key or f"chat-work:{native.ISSUE}", completion_contract=contract)
        return adapter, task_id

    def assign(self):
        self.bridge.handle_webhook(self.fixture.event())

    def test_running_unlinked_chat_task_is_reused_and_followup_keeps_its_run(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            original = native.kb.claim_task(conn, task_id)
        self.assign()
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
        self.bridge.followup(native.ISSUE, "Current authorized steering", "existing-followup")
        current = adapter.get(task_id)
        self.assertEqual((current.current_run_id, current.claim_lock), (original.current_run_id, original.claim_lock))
        self.assertEqual(self.fixture.comments(task_id)[-1].body, "Current authorized steering")
        self.assign()
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_exact_native_key_reuses_task_without_requiring_identical_creation_prose(self):
        _, task_id = self.existing(key=f"linear:{native.ISSUE}",
            body=f"Linear issue {native.ISSUE}: {self.issue['url']}\nOriginal approved objective")
        self.assign()
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_key_only_collision_on_an_alternate_board_is_not_adopted_or_duplicated(self):
        adapter, task_id = self.existing(board="project-work", key=f"linear:{native.ISSUE}",
                                        body="Manual work with a colliding retry key")
        with self.assertRaises(native.LinearError):
            self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
        self.assertEqual(self.fixture.tasks(), [])
        self.assertEqual(adapter.get(task_id).status, "ready")

    def test_alternate_existing_board_remains_the_execution_and_followup_board(self):
        adapter, task_id = self.existing(board="project-work")
        self.assign()
        self.assertEqual(self.store.native_binding(native.ISSUE)["board"], "project-work")
        self.assertEqual(self.fixture.tasks(), [])
        self.bridge.followup(native.ISSUE, "Steer existing project work", "project-followup")
        with adapter.conn() as conn:
            self.assertEqual(len(native.kb.list_tasks(conn)), 1)
            self.assertEqual(native.kb.list_comments_after(conn, task_id)[-1].body, "Steer existing project work")

    def test_stopped_work_is_linked_without_activation_even_after_restart(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            native.kb.block_task(conn, task_id, reason="operator Stop", kind="needs_input")
        history = [(event.id, event.kind) for event in adapter.history(task_id, 0)]
        self.assign()
        self.fixture.make_bridge().recover()
        self.assertEqual(adapter.get(task_id).status, "blocked")
        self.assertEqual([(e.id, e.kind) for e in adapter.history(task_id, 0)], history)
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_new_stop_during_discovery_is_not_undone(self):
        adapter, task_id = self.existing()
        discover = self.board.discover
        def stop_after_inspection(issue):
            found = discover(issue)
            with adapter.conn() as conn:
                native.kb.block_task(conn, task_id, reason="new Stop", kind="needs_input")
            return found
        with patch.object(self.board, "discover", side_effect=stop_after_inspection):
            self.assign()
        self.assertEqual(adapter.get(task_id).status, "blocked")
        self.assertFalse(any(e.kind == "unblocked" for e in adapter.history(task_id, 0)))

    def test_foreign_executor_does_not_trigger_replacement_or_implicit_transfer(self):
        adapter, task_id = self.existing(assignee="different-local-profile")
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(len(self.fixture.tasks()), 1)
        self.assertEqual(adapter.get(task_id).assignee, "different-local-profile")

    def test_ambiguous_existing_work_is_not_selected_or_duplicated(self):
        self.existing()
        self.existing(board="other-project")
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))

    def test_incidental_issue_mentions_are_not_implicit_task_links(self):
        _, old_id = self.existing(body=f"Research notes refer to {self.issue['identifier']} and {self.issue['url']}")
        self.assign()
        self.assertEqual(len(self.fixture.tasks()), 2)
        self.assertNotEqual(self.store.get(native.ISSUE)["task_id"], old_id)

    def test_completed_existing_work_stays_history_without_new_execution(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            native.kb.complete_task(conn, task_id, result="Verified original approved outcome")
        self.assign()
        self.assertEqual(adapter.get(task_id).status, "done")
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_identifier_marker_is_accepted_without_a_machine_creation_key(self):
        _, task_id = self.existing(body=f"Linear issue {self.issue['identifier']}: {self.issue['url']}")
        self.assign()
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)

    def test_reassignment_during_discovery_prevents_binding_or_creation(self):
        self.existing()
        discover = self.board.discover
        def changed(issue):
            found = discover(issue)
            self.issue["delegate"] = {"id": "another-app"}
            return found
        with patch.object(self.board, "discover", side_effect=changed):
            self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_unreadable_existing_board_is_not_treated_as_empty(self):
        self.existing(board="project-work")
        original = self.board.for_board
        def denied(board):
            if board == "project-work":
                raise native.LinearError("Existing native target unavailable", retryable=False)
            return original(board)
        with patch.object(self.board, "for_board", side_effect=denied):
            with self.assertRaises(native.LinearError):
                self.assign()
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))

    def test_configured_project_acceptance_is_not_downgraded_by_reuse(self):
        adapter, task_id = self.existing()
        self.bridge.contracts[self.issue["project"]["id"]] = "example/repository"
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(adapter.get(task_id).status, "ready")
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_matching_project_acceptance_reuses_existing_task(self):
        _, task_id = self.existing(contract="example/repository")
        self.bridge.contracts[self.issue["project"]["id"]] = "example/repository"
        self.assign()
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        self.assertEqual(len(self.fixture.tasks()), 1)


if __name__ == "__main__":
    unittest.main()
