"""Consumed native JSON cannot silently become evidence or active-board authority."""
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_linear_event_data_scenarios as events
import test_linear_existing_work_scenarios as existing
import test_linear_isolation_scenarios as isolation
import test_linear_native_bridge_scenarios as native
from hermes_fleet_linear_plugin import chat, transport


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class ConsumedMetadataScenarios(unittest.TestCase):
    def check_run(self, raw, *, invalid):
        fixture = events.EventDataScenarios()
        with fixture.event_case("heartbeat", {}, invalid=False) as case:
            with case.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=case.clock()):
                native.kb.claim_task(conn, case.task_id)
                native.kb.complete_task(conn, case.task_id, result="Verified bounded native outcome")
                conn.execute("UPDATE task_runs SET metadata=? WHERE task_id=?", (raw, case.task_id))
                conn.commit()
                self.assertEqual([row[0] for row in conn.execute("PRAGMA integrity_check")], ["ok"])
            case.committed = case.database.read_bytes()
            task = case.board.get(case.task_id)
            if invalid:
                for reader in (case.board.evidence_text, case.board.completion_summary, case.board.issue_comment):
                    with self.assertRaises(native.LinearError):
                        reader(task)
            case.reporter.tick()
            if invalid:
                self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
                self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
                self.assertFalse(case.reporter.store.pending(native.ISSUE))
                self.assertNotEqual(case.remote.state(native.ISSUE), "Done")
            else:
                self.assertGreater(case.reporter.store.get(native.ISSUE)["last_event_id"], case.before["last_event_id"])
                self.assertEqual(case.remote.state(native.ISSUE), "Done")
            fixture.check_independent_delivery(case)
            with case.board.read_conn() as conn:
                self.assertEqual(conn.execute("SELECT metadata FROM task_runs WHERE task_id=?", (case.task_id,)).fetchone()[0], raw)
            context = SimpleNamespace(profile="default", home=case.reporter.profile_home, pinned_db_path="")
            status = json.loads(chat.handle(case.reporter, {"action": "status", "issue": native.ISSUE}, context))
            self.assertTrue(status["ok"], status)
            self.assertEqual(status["issue"]["id"], native.ISSUE)

    def test_unreadable_or_nonobject_run_metadata_holds_only_affected_cursor(self):
        for raw in ('{"evidence":', '[1]', '"unexpected"', '', ' ', 'false', '0', b'\xff',
                    '{}'.encode('utf-16'), b'\xef\xbb\xbf{}'):
            with self.subTest(raw=raw):
                self.check_run(raw, invalid=True)

    def test_run_sql_null_json_null_objects_and_utf8_keep_native_completion(self):
        for raw in (None, 'null', '{}', '{"note":"Verified native evidence"}', b'{"issue_comment":"Native note"}'):
            with self.subTest(raw=raw):
                self.check_run(raw, invalid=False)

    def check_board_admission(self, raw, *, invalid):
        fixture = existing.ExistingWorkScenarios()
        try:
            fixture.setUp()
            adapter, task_id = fixture.existing(board="project")
            metadata = native.kb.board_metadata_path("project")
            metadata.write_bytes(raw)
            database = native.Path(native.kb.kanban_db_path(board="project"))
            committed = database.read_bytes()
            restarted = native.Kanban("project", profile="default", profile_home=fixture.fixture.home)
            self.assertEqual(database.read_bytes(), committed)
            if invalid:
                with self.assertRaises(transport.Unavailable):
                    transport.native_board_target("project")
                with self.assertRaises(native.LinearError):
                    restarted.get(task_id)
            fixture.assign()
            if invalid:
                self.assertIsNone(fixture.store.native_binding(native.ISSUE))
                self.assertIsNone(fixture.store.native_intent(f"create:{native.ISSUE}"))
                self.assertEqual(database.read_bytes(), committed)
            else:
                self.assertEqual(fixture.store.native_binding(native.ISSUE)["task_id"], task_id)
                self.assertEqual(transport.native_board_target("project")[0], "project")
            self.assertEqual(metadata.read_bytes(), raw)
        finally:
            fixture.doCleanups()

    def test_invalid_active_board_metadata_never_binds_subscribes_or_initializes(self):
        for raw in (b'{"archived":', b'[]', b'null', b'', b'\xff', b'{"archived":null}',
                    b'{"archived":[]}', b'{"archived":0}'):
            with self.subTest(raw=raw):
                self.check_board_admission(raw, invalid=True)

    def test_active_board_legacy_defaults_bom_and_directory_identity_remain_supported(self):
        for raw in (b'{}', b'{"archived":false}', b'\xef\xbb\xbf{}', b'{"slug":"other-name"}'):
            with self.subTest(raw=raw):
                self.check_board_admission(raw, invalid=False)

    def test_invalid_configured_board_keeps_other_native_work_and_issue_content_usable(self):
        for raw in (b'{"archived":', b'null', b'\xff', b'{"archived":falsee}'):
            with self.subTest(raw=raw):
                fixture = isolation.IsolationScenarios()
                try:
                    fixture.setUp()
                    clock, remote, reporter, other = fixture.reporting("board-metadata-isolation")
                    reporter.flush()
                    remote.add_issue("affected", "OPS-3", project=None)
                    home = fixture.fixture.fixture.fixture.home
                    board = native.Kanban("project", profile="default", profile_home=home)
                    with board.conn() as conn:
                        task_id = native.kb.create_task(conn, title="OPS-3: affected", assignee="default")
                    reporter.track("affected", task_id, board="project")
                    reporter.flush()
                    identity = fixture.queue_content(reporter, "affected")
                    before = reporter.store.get("affected")
                    binding = reporter.store.native_binding("affected")
                    clock.now += 1
                    for adapter, ident in ((fixture.board, other), (board, task_id)):
                        with adapter.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                            native.kb.complete_task(conn, ident, result="Verified independent native outcome")
                    metadata = native.kb.board_metadata_path("project")
                    original = metadata.read_bytes()
                    metadata.write_bytes(raw)
                    database = native.Path(native.kb.kanban_db_path(board="project"))
                    committed = database.read_bytes()
                    restarted = native.Kanban("project", profile="default", profile_home=home)
                    reporter.kanban = restarted
                    context = SimpleNamespace(profile="default", home=home, pinned_db_path="")
                    status = json.loads(chat.handle(reporter, {"action": "status", "issue": "affected"}, context))
                    self.assertTrue(status["ok"], status)
                    self.assertEqual(status["issue"]["id"], "affected")
                    self.assertFalse(status["native"]["linked"])
                    with patch.object(reporter, "authorize_specialist_effect", return_value=False):
                        refused = json.loads(chat.handle(reporter, {"action": "status", "issue": "affected"}, context))
                    self.assertFalse(refused["ok"])
                    self.assertNotIn("issue", refused)
                    reporter.tick()
                    self.assertEqual(reporter.store.get("affected"), before)
                    self.assertEqual(reporter.store.native_binding("affected"), binding)
                    self.assertEqual(remote.state(native.ISSUE), "Done")
                    self.assertEqual(reporter.store.outbox_row(identity)["state"], "sent")
                    self.assertIn("Authorized independent discussion", remote.bodies("affected"))
                    self.assertEqual(database.read_bytes(), committed)
                    self.assertEqual(metadata.read_bytes(), raw)
                    metadata.write_bytes(original)
                    reporter.tick()
                    self.assertEqual(remote.state("affected"), "Done")
                    self.assertEqual(reporter.store.outbox_row(identity)["state"], "sent")
                finally:
                    fixture.doCleanups()


if __name__ == "__main__":
    unittest.main()
