"""Raw selection keys are inspected before native WHERE/ORDER/LIMIT consume them."""
from contextlib import closing
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

import test_linear_event_data_scenarios as events
import test_linear_native_bridge_scenarios as native

CURRENT = "Verified current native outcome"


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class QueryOrderScenarios(unittest.TestCase):
    def runs(self, case):
        with case.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=case.clock()):
            prior = conn.execute("INSERT INTO task_runs(task_id,profile,status,started_at,summary,metadata) VALUES(?,?,?,?,?,?)",
                (case.task_id, "default", "blocked", int(case.clock())-100,
                 "Earlier failed attempt remains unresolved", "{}")).lastrowid
            task = native.kb.claim_task(conn, case.task_id)
            native.kb.complete_task(conn, case.task_id, result="done")
            conn.execute("UPDATE task_runs SET summary=? WHERE id=?", (CURRENT, task.current_run_id))
            conn.commit()
        return prior, task.current_run_id

    def independent_board(self, case):
        case.remote.add_issue("other-board", "OPS-3", project=None)
        adapter = native.Kanban("independent", profile="default", profile_home=case.reporter.profile_home)
        with adapter.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=case.clock()):
            task_id = native.kb.create_task(conn, title="OPS-3: independent", assignee="default")
            native.kb.complete_task(conn, task_id, result="Verified independent board outcome")
        case.reporter.track("other-board", task_id, board="independent")

    def raw_write(self, case, query, args):
        from hermes_cli.sqlite_safe_read import connect_tracked
        with closing(connect_tracked(case.database)) as conn:
            conn.execute(query, args)
            conn.commit()
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
        case.committed = case.database.read_bytes()

    def test_every_relevant_run_order_key_is_checked_before_native_limit(self):
        for old, raw in ((True, b"\xff"), (False, b"\xff"), (False, float("-inf")), (True, float("inf"))):
            with self.subTest(old=old, raw=raw):
                fixture = events.EventDataScenarios()
                with fixture.event_case("heartbeat", {}, invalid=False) as case:
                    prior, current = self.runs(case)
                    self.independent_board(case)
                    self.raw_write(case, "UPDATE task_runs SET started_at=? WHERE id=?", (raw, prior if old else current))
                    task = case.board.get(case.task_id)
                    for reader in (case.board.evidence_text, case.board.completion_summary, case.board.issue_comment):
                        with self.assertRaises(native.LinearError):
                            reader(task)
                    case.reporter.tick()
                    self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
                    self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
                    self.assertFalse(case.reporter.store.pending(native.ISSUE))
                    self.assertNotEqual(case.remote.state(native.ISSUE), "Done")
                    self.assertEqual(case.remote.state("other-board"), "Done")
                    fixture.check_independent_delivery(case)

    def test_native_legacy_timestamp_order_ties_and_fallback_are_unchanged(self):
        for style in ("integer", "numeric-text", "float", "iso", "empty", "legacy-fallback", "utf8-blob"):
            with self.subTest(style=style):
                fixture = events.EventDataScenarios()
                with fixture.event_case("heartbeat", {}, invalid=False) as case:
                    prior, current = self.runs(case)
                    stamps = [int(case.clock())-100, int(case.clock())]
                    if style == "numeric-text":
                        stamps = [str(value) for value in stamps]
                    elif style == "float":
                        stamps = [value + 0.25 for value in stamps]
                    elif style == "iso":
                        stamps = [datetime.fromtimestamp(value, timezone.utc).isoformat() for value in stamps]
                    elif style == "empty":
                        stamps = ["", ""]
                    elif style == "legacy-fallback":
                        stamps = ["legacy-unknown", "legacy-unknown"]
                    elif style == "utf8-blob":
                        stamps = [b"legacy-unknown", b"legacy-unknown"]
                    with case.board.conn() as conn:
                        for run_id, stamp in zip((prior, current), stamps):
                            conn.execute("UPDATE task_runs SET started_at=? WHERE id=?", (stamp, run_id))
                        conn.commit()
                        expected = native.kb.latest_run(conn, case.task_id)
                    case.committed = case.database.read_bytes()
                    self.assertEqual(expected.id, current)
                    self.assertEqual(case.board.completion_summary(case.board.get(case.task_id)), expected.summary)
                    with case.board.read_conn() as conn:
                        observed = case.board.latest_run(conn, case.task_id)
                    self.assertEqual((observed.id, observed.started_at), (expected.id, expected.started_at))
                    case.reporter.tick()
                    self.assertEqual(case.remote.state(native.ISSUE), "Done")
                    fixture.check_independent_delivery(case)

    def test_unused_earlier_run_summary_and_metadata_do_not_gate_current_evidence(self):
        fixture = events.EventDataScenarios()
        with fixture.event_case("heartbeat", {}, invalid=False) as case:
            prior, _ = self.runs(case)
            self.raw_write(case, "UPDATE task_runs SET summary=?,metadata=? WHERE id=?", (b"\xff", b"\xff", prior))
            self.assertEqual(case.board.completion_summary(case.board.get(case.task_id)), CURRENT)
            case.reporter.tick()
            self.assertEqual(case.remote.state(native.ISSUE), "Done")
            fixture.check_independent_delivery(case)

    def test_unreadable_task_predicate_keys_cannot_disappear_through_where(self):
        for table in ("task_runs", "task_events", "task_comments"):
            with self.subTest(table=table):
                fixture = events.EventDataScenarios()
                with fixture.event_case("commented", events.GENERATED, invalid=False, comment=True) as case:
                    self.runs(case)
                    self.independent_board(case)
                    self.raw_write(case, f"UPDATE {table} SET task_id=? WHERE task_id=?", (b"\xff", case.task_id))
                    reader = (lambda: case.board.completion_summary(case.board.get(case.task_id))) if table == "task_runs" else (
                        (lambda: case.board.history(case.task_id, case.before["last_event_id"])) if table == "task_events" else
                        (lambda: case.board.find_comment(case.task_id, "retained note", "retained-request")))
                    with self.assertRaises(native.LinearError):
                        reader()
                    case.reporter.tick()
                    self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
                    self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
                    self.assertNotEqual(case.remote.state(native.ISSUE), "Done")
                    self.assertEqual(case.remote.state("other-board"), "Done")
                    self.assertEqual(case.reporter.store.outbox_row(case.content_id)["state"], "sent")
                    self.assertEqual(case.database.read_bytes(), case.committed)

    def test_readable_predicate_storage_keeps_native_sqlite_equality_and_affinity(self):
        for raw in ("123", 123, b"other-task", "matching-blob"):
            with self.subTest(raw=raw):
                fixture = events.EventDataScenarios()
                with fixture.event_case("heartbeat", {}, invalid=False) as case:
                    prior, current = self.runs(case)
                    if raw == "matching-blob":
                        raw = case.task_id.encode("utf-8")
                    self.raw_write(case, "UPDATE task_runs SET task_id=? WHERE id=?", (raw, prior))
                    with case.board.read_conn() as conn:
                        expected = native.kb.latest_run(conn, case.task_id)
                        observed = case.board.latest_run(conn, case.task_id)
                    self.assertEqual((observed.id, observed.summary, observed.started_at),
                                     (expected.id, expected.summary, expected.started_at))
                    self.assertEqual(observed.id, current)
                    case.reporter.tick()
                    self.assertEqual(case.remote.state(native.ISSUE), "Done")
                    fixture.check_independent_delivery(case)

    def test_missing_task_read_refuses_unknown_ids_but_other_known_task_still_reports(self):
        fixture = events.EventDataScenarios()
        with fixture.event_case("heartbeat", {}, invalid=False) as case:
            self.raw_write(case, "UPDATE tasks SET id=? WHERE id=?", (b"\xff", case.task_id))
            with self.assertRaises(native.LinearError):
                case.board.get(case.task_id)
            case.reporter.tick()
            self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
            self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
            fixture.check_independent_delivery(case)


if __name__ == "__main__":
    unittest.main()
