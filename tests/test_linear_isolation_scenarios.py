"""One unavailable native binding or original project line cannot starve others."""
from contextlib import closing, nullcontext
import json
import unittest
from unittest.mock import patch

import test_linear_native_bridge_scenarios as native
import test_linear_scope_effect_scenarios as scope
from test_linear_content_actions import ContentRemote


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class IsolationScenarios(unittest.TestCase):
    def setUp(self):
        self.fixture = scope.ScopeEffectScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.board = self.fixture.board

    def reporting(self, name):
        with patch("linear_fake_api.FakeLinear", ContentRemote):
            return self.fixture.reporting(name)

    def queue_content(self, reporter, issue_id):
        from hermes_fleet_linear_plugin import chat, transport
        reporter.profile_home = self.fixture.fixture.fixture.home
        context = transport.NativeContext(home=str(reporter.profile_home), profile="default",
            platform="api", source="api", session_key="isolated-content", session_id="isolated-session")
        identity = "b9e5907c-801d-42f8-924f-eb9c738ac1a7"
        queued = json.loads(chat.handle(reporter, {"action": "add_comment", "issue": issue_id,
            "id": identity, "body": "Authorized independent discussion"}, context))
        self.assertTrue(queued["ok"], queued)
        return identity

    def check_history_isolation(self, *, other_board=False, lazy=False, comments=False, stamp=float("inf")):
        from hermes_cli.sqlite_safe_read import connect_tracked
        clock, remote, reporter, task_id = self.reporting("isolated-history")
        reporter.flush()
        remote.add_issue("independent", "OPS-2", project=None)
        adapter = native.Kanban("independent", profile="default",
            profile_home=self.fixture.fixture.fixture.home) if other_board else self.board
        with adapter.conn() as conn:
            other = native.kb.create_task(conn, title="OPS-2: independent work", assignee="default")
        reporter.track("independent", other, board=adapter.board)
        clock.now += 1
        with adapter.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            native.kb.complete_task(conn, other, result="Verified independent native outcome")
        if comments:
            with self.board.conn() as conn:
                native.kb.add_comment(conn, task_id, "operator", "Native comment requiring inspection")
        before = reporter.store.get(native.ISSUE)
        binding = reporter.store.native_binding(native.ISSUE)
        identity = self.queue_content(reporter, "independent")
        database = native.Path(native.kb.kanban_db_path(board=self.board.board))
        with closing(connect_tracked(database)) as conn:
            table = "task_comments" if comments else "task_events"
            conn.execute(f"UPDATE {table} SET created_at=? WHERE task_id=?", (stamp, task_id))
            conn.commit()
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
        committed = database.read_bytes()
        if not comments:
            if isinstance(stamp, str):
                self.assertTrue(all(event.created_at == 0 for event in self.board.history(task_id, 0)))
            else:
                with self.assertRaises(native.LinearError):
                    self.board.history(task_id, 0)
        history = self.board.history
        def deferred_history(*args, **kwargs):
            yield from history(*args, **kwargs)  # The actual native decoder fails during iteration.
        with patch.object(self.board, "history", side_effect=deferred_history) if lazy else nullcontext():
            reporter.tick()
        self.assertEqual(reporter.store.get(native.ISSUE), before)
        self.assertEqual(reporter.store.native_binding(native.ISSUE), binding)
        self.assertFalse(reporter.store.pending(native.ISSUE))
        self.assertGreater(reporter.store.get("independent")["last_event_id"], 0)
        self.assertEqual(remote.state("independent"), "Done")
        self.assertIn("Authorized independent discussion", remote.bodies("independent"))
        self.assertEqual(reporter.store.outbox_row(identity)["state"], "sent")
        self.assertEqual(database.read_bytes(), committed)

    def test_nonfinite_native_event_history_holds_only_binding_on_same_board(self):
        self.check_history_isolation()

    def test_nonfinite_native_event_history_holds_only_binding_across_boards(self):
        self.check_history_isolation(other_board=True)

    def test_deferred_native_history_iteration_failure_does_not_abort_tick(self):
        self.check_history_isolation(lazy=True)

    def test_native_unknown_timestamp_fallback_keeps_other_work_usable(self):
        self.check_history_isolation(stamp="not-an-epoch")

    def test_native_comment_collection_failure_keeps_other_binding_and_content_usable(self):
        self.check_history_isolation(comments=True)

    def original_batch(self):
        clock, remote, reporter, first = self.reporting("original-partitions")
        reporter.flush()
        ids = {native.ISSUE: first}
        for issue_id, ident in (("independent", "OPS-2"), ("remaining", "OPS-3")):
            remote.add_issue(issue_id, ident)
            with self.board.conn() as conn:
                ids[issue_id] = native.kb.create_task(conn, title=ident + ": original work", assignee="default")
            reporter.track(issue_id, ids[issue_id])
        lines = {"OPS-1": "Original first outcome", "OPS-2": "Original independent outcome",
                 "OPS-3": "Original third outcome"}
        for issue_id, ident in ((native.ISSUE, "OPS-1"), ("independent", "OPS-2"), ("remaining", "OPS-3")):
            reporter.project_update("original-batch", "proj-1", ident, lines[ident],
                                    quiet=False, issue_id=issue_id)
        return clock, remote, reporter, ids, lines

    def check_original_partitions(self, *, malformed_rows=False):
        from hermes_cli.sqlite_safe_read import connect_tracked
        clock, remote, reporter, ids, lines = self.original_batch()
        database = native.Path(native.kb.kanban_db_path(board=self.board.board))
        originals = {}
        def change(issue_id, hold):
            if malformed_rows:
                with closing(connect_tracked(database)) as conn:
                    originals.setdefault(issue_id, conn.execute("SELECT created_at FROM tasks WHERE id=?",
                                                               (ids[issue_id],)).fetchone()[0])
                    conn.execute("UPDATE tasks SET created_at=? WHERE id=?",
                                 (float("inf") if hold else originals[issue_id], ids[issue_id]))
                    conn.commit()
            elif hold:
                self.fixture.edit_scope(ids[issue_id])
            else:
                with self.board.conn() as conn:
                    native.kb.edit_task(conn, ids[issue_id], body="Authorized original manually tracked work")
        change(native.ISSUE, True)
        change("remaining", True)
        reporter.flush()
        self.assertEqual([row["body"] for row in remote.project_updates],
                         ["Agent update\n\n- OPS-2: " + lines["OPS-2"]])
        sent_ids = [row["id"] for row in remote.project_updates]
        remaining = next(row for row in reporter.store.pending() if row["kind"] == "project_update")
        self.assertEqual(remaining["payload"]["lines"], {key: lines[key] for key in ("OPS-1", "OPS-3")})
        change(native.ISSUE, False)
        restarted = native.Bridge(native.Store(reporter.store.path), reporter.api, self.board,
                                  profile="default", clock=clock)
        restarted.flush()
        self.assertEqual(len(remote.project_updates), 2)
        self.assertEqual(remote.project_updates[-1]["body"], "Agent update\n\n- OPS-1: " + lines["OPS-1"])
        self.assertEqual(remote.project_updates[-1]["id"], remaining["id"])
        held = next(row for row in restarted.store.pending() if row["kind"] == "project_update")
        self.assertEqual(held["payload"]["lines"], {"OPS-3": lines["OPS-3"]})
        self.assertNotIn(held["id"], sent_ids + [remaining["id"]])
        # The existing once contract forbids reintroducing an already-published
        # member, or expanding this sealed original remainder with a new member.
        restarted.project_update("original-batch", "proj-1", "OPS-2", "Later same-session outcome",
                                 quiet=False, issue_id="independent")
        restarted.project_update("original-batch", "proj-1", "OPS-NEW", "New same-session member",
                                 quiet=False, issue_id="independent")
        self.assertEqual(restarted.store.outbox_row(held["id"]), held)
        restarted.flush()
        self.assertEqual(len(remote.project_updates), 2)
        change("remaining", False)
        restarted.flush()
        self.assertEqual(len(remote.project_updates), 3)
        self.assertEqual(remote.project_updates[-1]["body"], "Agent update\n\n- OPS-3: " + lines["OPS-3"])
        self.assertEqual(remote.project_updates[-1]["id"], held["id"])
        self.assertEqual(len({row["id"] for row in remote.project_updates}), 3)
        restarted.flush()
        restarted.project_update("original-batch", "proj-1", "OPS-2", "After completed batch",
                                 quiet=False, issue_id="independent")
        restarted.flush()
        self.assertEqual(len(remote.project_updates), 3)
        self.assertFalse(restarted.store.pending())

    def test_original_three_line_batch_repartitions_as_each_native_scope_returns(self):
        self.check_original_partitions()

    def test_original_three_line_batch_repartitions_after_individual_native_row_recovery(self):
        self.check_original_partitions(malformed_rows=True)

    def test_temporary_issue_read_holds_only_original_line_and_can_repartition_again(self):
        _, remote, reporter, _, lines = self.original_batch()
        unavailable = {native.ISSUE, "remaining"}
        apply = remote._apply
        def issue_unavailable(query, variables):
            if "issue(" in query and variables.get("id") in unavailable:
                return 503, {"errors": [{"message": "Issue temporarily unavailable"}]}
            return apply(query, variables)
        with patch.object(remote, "_apply", side_effect=issue_unavailable):
            reporter.flush()
            self.assertEqual([row["body"] for row in remote.project_updates],
                             ["Agent update\n\n- OPS-2: " + lines["OPS-2"]])
            unavailable.remove(native.ISSUE)
            reporter.flush()
            self.assertEqual(remote.project_updates[-1]["body"], "Agent update\n\n- OPS-1: " + lines["OPS-1"])
            unavailable.clear()
            reporter.flush()
        self.assertEqual(len(remote.project_updates), 3)
        self.assertEqual(len({row["id"] for row in remote.project_updates}), 3)
        self.assertFalse(reporter.store.pending())

    def test_global_rate_limit_still_holds_original_batch_before_any_partition(self):
        clock, remote, reporter, _, _ = self.original_batch()
        before = next(row for row in reporter.store.pending() if row["kind"] == "project_update")
        remote.rate_limited_until = clock() + 60
        reporter.flush()
        self.assertEqual(remote.project_updates, [])
        after = reporter.store.outbox_row(before["id"])
        self.assertEqual(after["payload"], before["payload"])
        clock.now += 61
        remote.rate_limited_until = 0
        reporter.flush()
        self.assertEqual(len(remote.project_updates), 1)
        self.assertEqual(remote.project_updates[0]["id"], before["id"])

    def test_uncertain_started_project_request_keeps_identity_content_and_no_repartition(self):
        clock, remote, reporter, ids, _ = self.original_batch()
        remote.lose_next_response = True
        reporter.flush()
        before = next(row for row in reporter.store.pending() if row["kind"] == "project_update")
        self.assertTrue(before["payload"]["write_started"])
        self.assertTrue(before["payload"]["frozen"])
        self.assertEqual(len(remote.project_updates), 1)  # Committed remotely, response lost.
        self.fixture.edit_scope(ids[native.ISSUE])
        reporter.project_update("original-batch", "proj-1", "OPS-2", "Uncertain replacement prose",
                                quiet=False, issue_id="independent")
        self.assertEqual(reporter.store.outbox_row(before["id"]), before)
        clock.now += 600
        reporter.flush()
        self.assertEqual(reporter.store.outbox_row(before["id"]), before)
        self.assertEqual(len(remote.project_updates), 1)

    def test_prepared_legacy_project_request_cannot_merge_or_split_without_send_marker(self):
        clock, remote, reporter, ids, _ = self.original_batch()
        row = next(row for row in reporter.store.pending() if row["kind"] == "project_update")
        legacy = {**row["payload"], "frozen": False, "send_body": "Retained prepared original request",
                  "send_project_id": "proj-1"}
        legacy.pop("write_started", None)
        reporter.store.rewrite(row["id"], legacy, clock())
        before = reporter.store.outbox_row(row["id"])
        reporter.project_update("original-batch", "proj-1", "OPS-2", "New prose cannot replace legacy request",
                                quiet=False, issue_id="independent")
        self.assertEqual(reporter.store.outbox_row(row["id"]), before)
        self.fixture.edit_scope(ids[native.ISSUE])
        reporter.flush()
        self.assertEqual(reporter.store.outbox_row(row["id"]), before)
        self.assertEqual(remote.project_updates, [])


if __name__ == "__main__":
    unittest.main()
