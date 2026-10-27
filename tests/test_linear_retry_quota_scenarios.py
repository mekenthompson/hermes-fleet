"""Native retry selection and app-wide quota semantics survive scoped holds."""
from contextlib import closing
import unittest
from unittest.mock import patch

import test_linear_event_data_scenarios as events
import test_linear_existing_work_scenarios as existing
import test_linear_native_bridge_scenarios as native
from hermes_fleet_linear_plugin.api import RateLimited


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class RetryQuotaScenarios(unittest.TestCase):
    def test_retry_lookup_preserves_native_sql_equality_and_affinity(self):
        for raw, key in (("linear:retry", "linear:retry"), (b"linear:retry", "linear:retry"),
                         (123, "123"), ("123", "123"), (b"123", "123"), (None, "linear:retry")):
            with self.subTest(raw=raw, key=key):
                fixture = existing.ExistingWorkScenarios()
                try:
                    fixture.setUp()
                    adapter, task_id = fixture.existing()
                    with adapter.conn() as conn:
                        conn.execute("UPDATE tasks SET idempotency_key=? WHERE id=?", (raw, task_id))
                        conn.commit()
                    database = native.Path(native.kb.kanban_db_path(board=adapter.board))
                    before = database.read_bytes()
                    with adapter.read_conn() as conn:
                        expected = [row[0] for row in conn.execute(
                            "SELECT id FROM tasks WHERE idempotency_key=?", (key,))]
                    self.assertEqual([task.id for task in adapter.find_created(key)], expected)
                    self.assertEqual(expected, [] if isinstance(raw, bytes) or raw is None else [task_id])
                    self.assertEqual(database.read_bytes(), before)
                    self.assertIsNone(fixture.store.native_binding(native.ISSUE))
                finally:
                    fixture.doCleanups()

    def test_unreadable_retry_key_cannot_disappear_before_native_selection(self):
        fixture = existing.ExistingWorkScenarios()
        try:
            fixture.setUp()
            adapter, task_id = fixture.existing(key="unrelated-retained-key")
            from hermes_cli.sqlite_safe_read import connect_tracked
            database = native.Path(native.kb.kanban_db_path(board=adapter.board))
            with closing(connect_tracked(database)) as conn:
                conn.execute("UPDATE tasks SET idempotency_key=? WHERE id=?", (b"\xff", task_id))
                conn.commit()
                self.assertEqual(conn.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
            before = database.read_bytes()
            with self.assertRaises(native.LinearError):
                adapter.find_created("linear:retry")
            self.assertEqual(database.read_bytes(), before)
            self.assertIsNone(fixture.store.native_binding(native.ISSUE))
        finally:
            fixture.doCleanups()

    def mark_issue(self, case):
        with case.board.conn() as conn:
            native.kb.edit_task(conn, case.task_id,
                body=f"Linear issue {native.ISSUE}: {case.remote.issues[native.ISSUE]['url']}")
        case.committed = case.database.read_bytes()

    def test_final_capture_quota_propagates_without_consuming_event_and_recovers(self):
        fixture = events.EventDataScenarios()
        with fixture.event_case("heartbeat", {"note": "Current progress"}, invalid=False) as case:
            self.mark_issue(case)
            original = case.reporter._native_effect_task
            calls = []
            def read(issue_id, *args, **kwargs):
                if issue_id == native.ISSUE:
                    calls.append(issue_id)
                    if len(calls) == 2:
                        case.remote.rate_limited_until = case.clock() + 60
                return original(issue_id, *args, **kwargs)
            with patch.object(case.reporter, "_native_effect_task", side_effect=read):
                with self.assertRaises(RateLimited):
                    case.reporter.pump_kanban()
            self.assertEqual(len(calls), 2)
            self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
            self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
            self.assertFalse(case.reporter.store.pending(native.ISSUE))
            self.assertEqual(case.reporter.store.outbox_row(case.content_id)["state"], "pending")
            self.assertEqual(case.database.read_bytes(), case.committed)
            case.clock.now += 61
            case.remote.rate_limited_until = 0
            case.reporter.tick()
            self.assertEqual(case.reporter.store.get(native.ISSUE)["last_event_id"],
                             case.board.history(case.task_id, 0)[-1].id)
            fixture.check_independent_delivery(case)

    def test_status_followup_specialist_and_viewer_do_not_downgrade_quota(self):
        for action in ("status", "followup", "specialist", "viewer"):
            with self.subTest(action=action):
                fixture = events.EventDataScenarios()
                with fixture.event_case("heartbeat", {}, invalid=False) as case:
                    self.mark_issue(case)
                    case.remote.rate_limited_until = case.clock() + 60
                    if action == "viewer":
                        case.reporter.api._viewer = None
                    with patch.object(case.reporter, "_specialist_scope_active", return_value=action == "specialist"):
                        with self.assertRaises(RateLimited):
                            if action == "status":
                                case.reporter.native_status(native.ISSUE)
                            elif action == "followup":
                                case.reporter.followup(native.ISSUE, "Current instruction", "quota-request")
                            elif action == "specialist":
                                case.reporter.authorize_specialist_effect(native.ISSUE)
                            else:
                                case.reporter.me()
                    self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
                    self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
                    self.assertIsNone(case.reporter.store.native_intent(f"comment:{native.ISSUE}:quota-request"))
                    self.assertEqual(case.database.read_bytes(), case.committed)
                    case.clock.now += 61
                    case.remote.rate_limited_until = 0
                    case.reporter.tick()
                    fixture.check_independent_delivery(case)

    def test_guarded_stop_final_issue_reads_preserve_quota_and_pending_receipt(self):
        for quota_read in (1, 2):
            with self.subTest(quota_read=quota_read):
                fixture = events.EventDataScenarios()
                with fixture.event_case("heartbeat", {}, invalid=False) as case:
                    self.mark_issue(case)
                    with case.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=case.clock()):
                        native.kb.claim_task(conn, case.task_id)
                    case.committed = case.database.read_bytes()
                    task = case.board.get(case.task_id)
                    issue_read = case.reporter._effect_issue
                    calls = []
                    def read(issue_id):
                        calls.append(issue_id)
                        if len(calls) == quota_read:
                            case.remote.rate_limited_until = case.clock() + 60
                        return issue_read(issue_id)
                    request = f"quota-stop-{quota_read}"
                    with patch.object(case.reporter, "_effect_issue", side_effect=read):
                        with self.assertRaises(RateLimited):
                            case.reporter._guarded_stop(case.before, task, case.board, "session", "operator",
                                                       (case.clock() + 5) * 1000, request)
                    self.assertEqual(len(calls), quota_read)
                    self.assertEqual(case.board.get(case.task_id).current_run_id, task.current_run_id)
                    self.assertEqual(case.board.get(case.task_id).status, "running")
                    intent = case.reporter.store.native_intent(f"stop:{native.ISSUE}:{request}")
                    self.assertNotIn(intent["state"], ("applied", "reconcile"))
                    self.assertEqual(case.database.read_bytes(), case.committed)

    def test_final_project_mutation_guard_quota_is_not_a_scoped_deferral(self):
        fixture = events.EventDataScenarios()
        with fixture.event_case("heartbeat", {}, invalid=False) as case:
            self.mark_issue(case)
            case.reporter.project_update("quota-batch", "proj-1", "OPS-1", "Verified original outcome",
                                         quiet=False, issue_id=native.ISSUE)
            row = next(row for row in case.reporter.store.pending() if row["kind"] == "project_update")
            before = row["payload"]
            scope = case.reporter._require_outbox_native_scope
            def guarded(*args, **kwargs):
                case.remote.rate_limited_until = case.clock() + 60
                return scope(*args, **kwargs)
            with patch.object(case.reporter, "_require_outbox_native_scope", side_effect=guarded):
                with self.assertRaises(RateLimited):
                    case.reporter._mutate_outbox(row["id"], lambda: case.reporter.api.graphql(
                        "mutation Create($input: ProjectUpdateCreateInput!) { projectUpdateCreate(input: $input) { success } }",
                        {"input": {"id": row["id"], "projectId": "proj-1", "body": "Must not publish"}}))
            self.assertEqual(case.remote.project_updates, [])
            self.assertEqual(case.reporter.store.outbox_row(row["id"])["payload"], before)
            self.assertEqual(case.database.read_bytes(), case.committed)


if __name__ == "__main__":
    unittest.main()
