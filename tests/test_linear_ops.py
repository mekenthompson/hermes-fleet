"""Operators can see every held item and clear it through a supported command, never a database edit."""
import argparse
import io
import json
import sqlite3
import tempfile
import unittest
import uuid
from contextlib import closing, redirect_stdout
from pathlib import Path

from linear_fake_api import load_plugin
from linear_ingress_fixture import IngressStore, Route

load_plugin()
from hermes_fleet_linear_plugin import ops
from hermes_fleet_linear_plugin.store import Store


class LinearOperatorTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.home = Path(tmp.name)
        self.state, self.inbox = self.home / "state.db", self.home / "ingress.db"
        self.store = Store(self.state)

    def cli(self, *argv):
        parser = argparse.ArgumentParser()
        ops.setup(parser)
        out = io.StringIO()
        with redirect_stdout(out):
            code = ops.run(parser.parse_args(argv), self.state, self.inbox, "default")
        return code, out.getvalue()

    def uncertain(self, kind, issue, *, settled=True, **payload):
        if kind == "status":
            self.store.bind_native(issue, f"native-{issue}", "default", "default", at=0)
            payload["task_id"] = f"native-{issue}"
        elif kind == "description":
            payload.update(content_action=True, unlinked_content=True)
        row = self.store.enqueue(kind, {"issue_id": issue, **payload}, at=100)
        saved = self.store.outbox_row(row)
        self.store.rewrite(row, {**saved["payload"], "write_started": True}, 100)
        if settled:  # the gateway has given up on settling it by itself
            self.store.mark(row, "failed")
        return row

    def test_nothing_held_is_said_plainly(self):
        self.assertEqual(self.cli("status"), (0, "Linear: nothing is held.\n"))

    def test_uncertain_status_and_description_are_listed_and_resolved_by_command(self):
        status = self.uncertain("status", "issue-1", state="done", terminal=True)
        description = self.uncertain("description", "issue-5", id=str(uuid.uuid4()), body="New",
                                     expected_description="Old", content_action=True)
        self.assertTrue(self.store.issue_reconciliation_blocked("issue-1"))
        self.assertTrue(self.store.issue_reconciliation_blocked("issue-5"))
        code, text = self.cli("status")
        self.assertEqual(code, 1)
        for row in (status, description):
            self.assertIn(f"hermes linear resolve {row} applied|not_applied", text)
        self.assertEqual(self.cli("resolve", status, "applied", "--evidence", "Issue shows Done")[0], 0)
        self.assertEqual(self.cli("resolve", description, "not_applied", "--evidence", "Old text in Linear")[0], 0)
        self.assertFalse(self.store.issue_reconciliation_blocked("issue-1"))
        self.assertFalse(self.store.issue_reconciliation_blocked("issue-5"))
        self.assertEqual(self.cli("status")[0], 0)
        self.assertEqual(self.cli("resolve", status, "applied", "--evidence", "again")[0], 1)  # already resolved
        self.assertEqual(self.store.unreported_failed(), [])  # the operator acted: no "held" alert follows

    def test_in_flight_write_is_shown_but_cannot_be_resolved_under_the_sender(self):
        row = self.uncertain("status", "issue-6", settled=False, state="done", terminal=True)
        [item] = ops.stuck(self.state, self.inbox, "default", now=160)
        self.assertFalse(item["act"])
        self.assertIn("being sent", item["why"])
        self.assertEqual(self.cli("resolve", row, "not_applied", "--evidence", "Linear shows Todo")[0], 1)
        self.assertTrue(self.store.issue_reconciliation_blocked("issue-6"))
        [late] = ops.stuck(self.state, self.inbox, "default", now=100 + 3601)
        self.assertTrue(late["act"])  # held unsent for an hour: the health check fires

    def test_locked_state_database_is_an_error_not_an_all_clear(self):
        from unittest.mock import patch
        with patch.object(ops, "_read") as read:
            read.return_value.execute.side_effect = sqlite3.OperationalError("database is locked")
            with self.assertRaises(sqlite3.OperationalError):
                ops.stuck(self.state, self.inbox, "default")

    def test_retried_terminal_comment_never_asks_for_resolve(self):
        self.store.bind_native("issue-7", "native-7", "default", "default", at=0)
        comment = self.store.enqueue("comment", {"issue_id": "issue-7", "task_id": "native-7", "body": "Done", "terminal": True}, at=100)
        self.store.retry(self.store.outbox_row(comment), 100)
        self.store.mark(comment, "failed")
        items = ops.stuck(self.state, self.inbox, "default", now=200)
        self.assertEqual([(i["ref"], i["act"]) for i in items], [(comment, False)])
        self.assertNotIn("resolve", items[0]["action"])
        self.assertEqual(ops.stuck(self.state, self.inbox, "default", now=100 + 8 * 86_400), [])

    def test_parked_delivery_is_listed_and_replayed_once(self):
        route = Route("default", "default", "/webhook/default", self.home / "unused", self.inbox)
        event = {"type": "AgentSessionEvent", "agentSession": {"issue": {"id": "issue-2"}}}
        IngressStore(self.inbox).enqueue(route, "parked-1", json.dumps(event).encode())
        with closing(sqlite3.connect(self.inbox)) as db, db:
            db.execute("UPDATE deliveries SET status = 'failed', attempts = 40")
        items = ops.stuck(self.state, self.inbox, "default")
        self.assertEqual([(i["issue"], i["ref"]) for i in items], [("issue-2", "parked-1")])
        self.assertIn("hermes linear replay parked-1", items[0]["action"])
        self.assertFalse(ops.replay(self.inbox, "worker-a", "parked-1"))  # another profile's inbox row
        self.assertEqual(self.cli("replay", "parked-1")[0], 0)
        with closing(sqlite3.connect(self.inbox)) as db:
            self.assertEqual(db.execute("SELECT status, attempts FROM deliveries").fetchone(), ("pending", 0))
        self.assertEqual(self.cli("replay", "parked-1")[0], 1)

    def test_fences_and_undelivered_writes_explain_the_next_step(self):
        self.store.fence_scope("issue-3", "requester outside scope", at=50)
        refused = self.store.enqueue("comment", {"issue_id": "issue-4", "body": "evidence", "content_action": True, "unlinked_content": True}, at=100)
        self.store.mark(refused, "failed")
        self.store.bind_native("issue-4", "native-4", "default", "default", at=0)
        thought = self.store.enqueue("activity", {"issue_id": "issue-4", "task_id": "native-4", "session_id": "s",
                                                  "content": {"type": "thought", "body": "Working"}}, at=100)
        self.store.mark(thought, "failed")
        items = {i["ref"]: i for i in ops.stuck(self.state, self.inbox, "default", now=200)}
        self.assertIn("approved procedure", items["fence"]["action"])
        self.assertIn("redo the step", items[refused]["action"])
        self.assertNotIn(thought, items)  # a dropped thought holds nothing
        self.assertFalse(items["fence"]["act"])  # a policy fence alone is not an alarm
        self.assertNotIn("requester outside scope", ops.report(list(items.values())))  # reasons stay in the database
        self.assertTrue(items[refused]["act"])


if __name__ == "__main__":
    unittest.main()
