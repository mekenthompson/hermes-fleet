"""Consumed native TEXT is validated before lossy constructor decoding."""
from contextlib import closing
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_linear_event_data_scenarios as events
import test_linear_existing_work_scenarios as existing
import test_linear_native_bridge_scenarios as native
from hermes_fleet_linear_plugin import chat

TASK_TEXT = ("id", "title", "body", "assignee", "status", "created_by", "result",
             "idempotency_key", "completion_contract", "current_run_id", "claim_lock")
COMMENT = "Verified native operator note"


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class NativeTextScenarios(unittest.TestCase):
    def mutate(self, database, table, column, raw, task_id):
        from hermes_cli.sqlite_safe_read import connect_tracked
        with closing(connect_tracked(database)) as conn:
            key = "id" if table == "tasks" else "task_id"
            conn.execute(f"UPDATE {table} SET {column}=? WHERE {key}=?", (raw, task_id))
            conn.commit()
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchall(), [("ok",)])

    def test_unknown_consumed_task_identity_never_becomes_absence_or_a_competing_worker(self):
        for column in TASK_TEXT:
            with self.subTest(column=column):
                fixture = existing.ExistingWorkScenarios()
                try:
                    fixture.setUp()
                    board, task_id = fixture.existing()
                    with board.conn() as conn:
                        original = native.kb.claim_task(conn, task_id)
                    database = native.Path(native.kb.kanban_db_path(board=board.board))
                    raw = (b'Linear iss\xffue ' + native.ISSUE.encode() + b': ' + fixture.issue['url'].encode()
                           if column == "body" else b"native-\xff")
                    self.mutate(database, "tasks", column, raw, task_id)
                    committed = database.read_bytes()
                    if column != "id":
                        with self.assertRaises(native.LinearError):
                            board.get(task_id)
                    with self.assertRaises(native.LinearError):
                        board.discover(fixture.issue)
                    with self.assertRaises(native.LinearError):
                        board.find_created(f"chat-work:{native.ISSUE}")
                    fixture.assign()
                    self.assertIsNone(fixture.store.native_binding(native.ISSUE))
                    self.assertIsNone(fixture.store.native_intent(f"create:{native.ISSUE}"))
                    with board.history_conn(database) as conn:
                        self.assertEqual(conn.execute("SELECT count(*) FROM tasks").fetchone()[0], 1)
                        row = conn.execute("SELECT current_run_id,claim_lock FROM tasks").fetchone()
                        self.assertEqual(row[0], raw if column == "current_run_id" else original.current_run_id)
                        self.assertEqual(row[1], raw if column == "claim_lock" else original.claim_lock)
                    self.assertEqual(database.read_bytes(), committed)
                finally:
                    fixture.doCleanups()

    def check_reporting(self, table, column, *, invalid, raw=None):
        fixture = events.EventDataScenarios()
        comment = table == "task_comments"
        payload = events.GENERATED if comment else {}
        with fixture.event_case("commented" if comment else "heartbeat", payload,
                                invalid=False, comment=comment) as case:
            if table in ("task_runs", "tasks"):
                with case.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=case.clock()):
                    native.kb.claim_task(conn, case.task_id)
                    native.kb.complete_task(conn, case.task_id, result="Verified native outcome")
            if raw is None:
                with case.board.read_conn() as conn:
                    key = "id" if table == "tasks" else "task_id"
                    original = conn.execute(f"SELECT {column} FROM {table} WHERE {key}=?", (case.task_id,)).fetchone()[0]
                raw = b"native-\xff" if invalid else str(original if original is not None else "").encode("utf-8")
            self.mutate(case.database, table, column, raw, case.task_id)
            case.committed = case.database.read_bytes()
            if invalid:
                if table == "task_runs":
                    task = case.board.get(case.task_id)
                    for reader in (case.board.evidence_text, case.board.completion_summary, case.board.issue_comment):
                        with self.assertRaises(native.LinearError):
                            reader(task)
                elif table == "task_comments":
                    with self.assertRaises(native.LinearError):
                        case.board.find_comment(case.task_id, COMMENT, "retained-request")
                elif table == "task_events":
                    with self.assertRaises(native.LinearError):
                        case.board.history(case.task_id, case.before["last_event_id"])
            case.reporter.tick()
            if invalid:
                self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
                self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
                self.assertFalse(case.reporter.store.pending(native.ISSUE))
                self.assertNotEqual(case.remote.state(native.ISSUE), "Done")
            else:
                self.assertGreater(case.reporter.store.get(native.ISSUE)["last_event_id"], case.before["last_event_id"])
                if table in ("task_runs", "tasks"):
                    self.assertEqual(case.remote.state(native.ISSUE), "Done")
                if table == "task_comments":
                    self.assertIn(COMMENT, case.remote.bodies(native.ISSUE))
            fixture.check_independent_delivery(case)
            context = SimpleNamespace(profile="default", home=case.reporter.profile_home, pinned_db_path="")
            status = json.loads(chat.handle(case.reporter, {"action": "status", "issue": native.ISSUE}, context))
            self.assertTrue(status["ok"], status)
            self.assertEqual(status["issue"]["id"], native.ISSUE)
            with case.board.history_conn(case.database) as conn:
                key = "id" if table == "tasks" else "task_id"
                self.assertEqual(conn.execute(f"SELECT {column} FROM {table} WHERE {key}=?", (case.task_id,)).fetchone()[0], raw)

    def test_bound_task_unreadable_text_is_scoped_to_that_binding(self):
        for column in TASK_TEXT[1:]:
            with self.subTest(column=column):
                self.check_reporting("tasks", column, invalid=True)

    def test_run_event_and_comment_text_refuse_lossy_projection_and_keep_cursor(self):
        for table, columns in (("task_runs", ("summary",)),
                               ("task_events", ("kind", "created_at", "run_id")),
                               ("task_comments", ("author", "body", "created_at"))):
            for column in columns:
                with self.subTest(table=table, column=column):
                    self.check_reporting(table, column, invalid=True)

    def test_genuine_utf8_blob_text_and_native_absence_defaults_keep_reporting(self):
        for table, column in (("tasks", "title"), ("tasks", "body"), ("tasks", "result"),
                              ("task_runs", "summary"), ("task_events", "kind"),
                              ("task_comments", "author"), ("task_comments", "body")):
            with self.subTest(table=table, column=column):
                self.check_reporting(table, column, invalid=False)
        self.check_reporting("task_runs", "summary", invalid=False, raw="Verified legitimate \ufffd outcome".encode("utf-8"))

    def test_archived_identity_text_cannot_hide_history_from_admission(self):
        fixture = existing.ExistingWorkScenarios()
        try:
            fixture.setUp()
            board, task_id = fixture.existing(board="project")
            with board.conn() as conn:
                native.kb.complete_task(conn, task_id, result="Verified original outcome")
            archived = native.kb.remove_board("project", archive=True)
            database = native.Path(archived["new_path"]) / "kanban.db"
            self.mutate(database, "tasks", "body", b"Linear iss\xffue retained identity", task_id)
            committed = database.read_bytes()
            fixture.assign()
            self.assertIsNone(fixture.store.native_binding(native.ISSUE))
            self.assertIsNone(fixture.store.native_intent(f"create:{native.ISSUE}"))
            self.assertEqual(database.read_bytes(), committed)
        finally:
            fixture.doCleanups()

    def test_unused_skills_corruption_does_not_create_a_new_task_schema_gate(self):
        fixture = existing.ExistingWorkScenarios()
        try:
            fixture.setUp()
            board, task_id = fixture.existing()
            database = native.Path(native.kb.kanban_db_path(board=board.board))
            self.mutate(database, "tasks", "skills", b"\xff", task_id)
            self.assertEqual(board.get(task_id).id, task_id)
            fixture.assign()
            self.assertEqual(fixture.store.native_binding(native.ISSUE)["task_id"], task_id)
            with board.read_conn() as conn:
                self.assertEqual(conn.execute("SELECT skills FROM tasks WHERE id=?", (task_id,)).fetchone()[0], b"\xff")
        finally:
            fixture.doCleanups()


if __name__ == "__main__":
    unittest.main()
