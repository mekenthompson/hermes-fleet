"""Actual registered tool and socket: inspect and replace reporting, never execution."""
import json
import os
import sqlite3
import unittest
import uuid

import test_linear_native_frontdoor_scenarios as frontdoor


@unittest.skipUnless(os.environ.get("HERMES_AGENT_SRC"), "requires explicit native source")
class ReportingRelinkScenarios(unittest.TestCase):
    def setUp(self):
        self.fixture = frontdoor.NativeFrontdoorTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.bind_api()
        self.bridge, self.board = self.fixture.bridge, self.fixture.board

    def call(self, args):
        return self.fixture.call(args)

    def test_explicit_inspected_legacy_relink_preserves_history_and_does_not_start_worker(self):
        self.bridge.store.put("issue-1", "chat", "previous-conversation")
        old = self.bridge.store.get("issue-1")
        pending = self.bridge.store.enqueue("status", {"issue_id": "issue-1", "state": "blocked", "claim": False}, at=1)
        original_delivery = self.bridge.store.outbox_row(pending)
        read = self.call({"action": "status", "issue": "ABC-1"})
        self.assertTrue(read["ok"], read)
        self.assertEqual(read["native"]["status"], "reconciliation")
        task = self.board.create(title="Inspected remainder", initial_status="blocked")
        request = {"action": "track", "issue": "ABC-1", "task_id": task.id,
                   "replace": True, "id": str(uuid.uuid4()),
                   "expected_binding_revision": read["native"]["binding_revision"]}
        result = self.call(request)
        self.assertTrue(result["ok"], result)
        self.assertTrue(result["native"]["reporting_only"])
        self.assertEqual(self.board.get(task.id).status, "blocked")
        self.assertEqual(len(self.board.history(task.id, 0)), 2)
        receipt = self.bridge.store.native_intent(f"track:issue-1:{request['id']}")
        self.assertEqual(receipt["payload"]["previous"]["work"], old)
        held = self.bridge.store.outbox_row(pending)
        self.assertTrue(held["hold_reason"])
        self.assertEqual({k: v for k, v in held.items() if k != "hold_reason"},
                         {k: v for k, v in original_delivery.items() if k != "hold_reason"})
        retry = self.call(request)
        self.assertTrue(retry["ok"], retry)

    def test_stale_reporting_inspection_cannot_overwrite_a_new_link(self):
        read = self.call({"action": "status", "issue": "ABC-1"})
        first = self.board.create(title="Current work", initial_status="blocked")
        second = self.board.create(title="Different inspected work", initial_status="blocked")
        self.bridge.track("issue-1", first.id)
        result = self.call({"action": "track", "issue": "ABC-1", "task_id": second.id,
                            "replace": True, "id": str(uuid.uuid4()),
                            "expected_binding_revision": read["native"]["binding_revision"]})
        self.assertFalse(result["ok"])
        self.assertEqual(self.bridge.store.native_binding("issue-1")["task_id"], first.id)

    def test_uncertain_description_remains_held_while_reporting_link_is_reconciled(self):
        self.bridge.store.put("issue-1", "chat", "previous-conversation")
        request_id = str(uuid.uuid4())
        row_id = self.bridge.store.enqueue("description", {"issue_id": "issue-1", "id": request_id,
            "body": "Uncertain previous description", "expected_description": "Old description",
            "content_action": True, "unlinked_content": True}, at=1)
        with sqlite3.connect(self.bridge.store.path) as db:
            db.execute("UPDATE outbox SET payload=json_set(payload,'$.write_started',json('true')),attempts=1 WHERE id=?", (row_id,))
        previous = self.bridge.store.outbox_row(row_id)
        read = self.call({"action": "status", "issue": "ABC-1"})
        self.assertTrue(read["ok"], read)
        task = self.board.create(title="Inspected independent remainder", initial_status="blocked")
        result = self.call({"action": "track", "issue": "ABC-1", "task_id": task.id,
                            "replace": True, "id": str(uuid.uuid4()),
                            "expected_binding_revision": read["native"]["binding_revision"]})
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.bridge.store.outbox_row(row_id), previous)
        self.assertTrue(self.bridge.store.issue_reconciliation_blocked("issue-1"))
        self.assertEqual(self.board.get(task.id).status, "blocked")

    def test_relink_refuses_foreign_native_task_without_changing_prior_history(self):
        self.bridge.store.put("issue-1", "chat", "previous-conversation")
        read = self.call({"action": "status", "issue": "ABC-1"})
        task = self.board.create(title="Foreign task", initial_status="blocked")
        with self.board.conn() as conn:
            self.board.kb.assign_task(conn, task.id, "other-profile")
        result = self.call({"action": "track", "issue": "ABC-1", "task_id": task.id,
                            "replace": True, "id": str(uuid.uuid4()),
                            "expected_binding_revision": read["native"]["binding_revision"]})
        self.assertFalse(result["ok"])
        self.assertEqual(self.bridge.store.get("issue-1")["origin"], "chat")


if __name__ == "__main__":
    unittest.main()
