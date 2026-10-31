"""Persisted native metadata cannot abort unrelated work or content delivery."""
from contextlib import contextmanager
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_linear_isolation_scenarios as isolation
import test_linear_native_bridge_scenarios as native
from hermes_fleet_linear_plugin.api import LinearError, RateLimited

GENERATED = object()
COMMENT = "Verified native operator note"


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class EventDataScenarios(unittest.TestCase):
    @contextmanager
    def event_case(self, kind, payload, *, invalid=True, comment=False, raw_payload=GENERATED):
        fixture = isolation.IsolationScenarios()
        try:
            fixture.setUp()
            clock, remote, reporter, task_id = fixture.reporting("event-data")
            reporter.flush()
            board = fixture.board
            remote.add_issue("independent", "OPS-2", project=None)
            with board.conn() as conn:
                other = native.kb.create_task(conn, title="OPS-2: independent", assignee="default")
            reporter.track("independent", other)
            content_id = fixture.queue_content(reporter, "independent")
            before = reporter.store.get(native.ISSUE)
            binding = reporter.store.native_binding(native.ISSUE)
            clock.now += 1
            with board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
                native.kb.complete_task(conn, other, result="Verified independent native outcome")
                if comment:
                    native.kb.add_comment(conn, task_id, "operator", COMMENT)
                    event_id = conn.execute("SELECT max(id) FROM task_events WHERE task_id=?", (task_id,)).fetchone()[0]
                    if payload is not GENERATED:
                        conn.execute("UPDATE task_events SET payload=? WHERE id=?", (json.dumps(payload), event_id))
                else:
                    event_id = conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
                                            (task_id, kind, json.dumps(payload) if raw_payload is GENERATED else raw_payload,
                                             int(clock()))).lastrowid
                if invalid:
                    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
                        (task_id, "heartbeat", json.dumps({"note": "Must not publish after invalid event"}), int(clock())))
                conn.commit()
                self.assertEqual([row[0] for row in conn.execute("PRAGMA integrity_check")], ["ok"])
                raw = conn.execute("SELECT payload FROM task_events WHERE id=?", (event_id,)).fetchone()[0]
            database = native.Path(native.kb.kanban_db_path(board=board.board))
            yield SimpleNamespace(fixture=fixture, clock=clock, remote=remote, reporter=reporter,
                board=board, task_id=task_id, event_id=event_id, content_id=content_id,
                before=before, binding=binding, database=database, committed=database.read_bytes(), raw=raw)
        finally:
            fixture.doCleanups()

    def check_independent_delivery(self, case):
        self.assertEqual(case.remote.state("independent"), "Done")
        self.assertIn("Authorized independent discussion", case.remote.bodies("independent"))
        self.assertEqual(case.reporter.store.outbox_row(case.content_id)["state"], "sent")
        self.assertGreater(case.reporter.store.get("independent")["last_event_id"], 0)
        self.assertEqual(case.database.read_bytes(), case.committed)
        with case.board.read_conn() as conn:
            self.assertEqual(conn.execute("SELECT payload FROM task_events WHERE id=?", (case.event_id,)).fetchone()[0], case.raw)

    def check_invalid(self, kind, payload):
        with self.event_case(kind, payload) as case:
            case.reporter.tick()
            self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
            self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
            self.assertFalse(case.reporter.store.pending(native.ISSUE))
            self.assertFalse(any("Must not publish" in body for body in case.remote.bodies(native.ISSUE)))
            self.check_independent_delivery(case)

    def test_nonobject_json_event_payload_shapes_are_retained_without_consumption(self):
        for payload in (["unexpected-list"], [], "unexpected-string", "", 0, False, True):
            with self.subTest(payload=payload):
                self.check_invalid("heartbeat", payload)

    def test_malformed_persisted_json_cannot_become_legacy_absence(self):
        for raw in ('{"note":', 'not-json', '[', '', ' ', b'\xff',
                    '{"note":"wrong encoding"}'.encode('utf-16'), b'\xef\xbb\xbf{}'):
            with self.subTest(raw=raw), self.event_case("heartbeat", None, raw_payload=raw) as case:
                with self.assertRaises(LinearError):
                    case.board.history(case.task_id, case.before["last_event_id"])
                case.reporter.tick()
                self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
                self.assertEqual(case.reporter.store.native_binding(native.ISSUE), case.binding)
                self.assertFalse(case.reporter.store.pending(native.ISSUE))
                self.assertFalse(any("Must not publish" in body for body in case.remote.bodies(native.ISSUE)))
                self.check_independent_delivery(case)

    def test_sql_null_and_valid_json_null_object_records_keep_working(self):
        for raw in (None, 'null', ' {} ', '{"note": "Valid native progress"}', b'{"note":"Valid native UTF-8 progress"}'):
            with self.subTest(raw=raw), self.event_case("heartbeat", None, invalid=False, raw_payload=raw) as case:
                case.reporter.tick()
                self.assertEqual(case.reporter.store.get(native.ISSUE)["last_event_id"], case.event_id)
                self.check_independent_delivery(case)

    def test_ordinary_comment_metadata_is_inert_even_when_legacy_length_is_invalid(self):
        for value in ("not-a-number", "", [], [1], {}, {"value": 1}, float("inf"), float("nan")):
            with self.subTest(numeric=value), self.event_case("commented", {"author": "operator", "len": value}, invalid=False) as case:
                case.reporter.tick()
                self.assertEqual(case.reporter.store.get(native.ISSUE)["last_event_id"], case.event_id)
                self.assertNotIn(COMMENT, case.remote.bodies(native.ISSUE))
                self.check_independent_delivery(case)

    def test_nested_unhashable_metadata_and_formatter_input_failures_are_scoped(self):
        for kind, payload in (("blocked", {"kind": ["unexpected"]}),
                              ("blocked", {"kind": {"unexpected": True}}),
                              ("changes_requested", {"reason": {"unexpected": True}})):
            with self.subTest(kind=kind, payload=payload):
                self.check_invalid(kind, payload)

    def test_legacy_none_empty_mapping_and_valid_heartbeat_payloads_keep_working(self):
        for payload in (None, {}, {"note": "Valid native progress"}):
            with self.subTest(payload=payload), self.event_case("heartbeat", payload, invalid=False) as case:
                case.reporter.tick()
                self.assertEqual(case.reporter.store.get(native.ISSUE)["last_event_id"], case.event_id)
                self.check_independent_delivery(case)
                self.assertFalse(any("Valid native progress" in body for body in case.remote.bodies(native.ISSUE)))

    def test_native_generated_comment_and_legacy_numeric_string_stay_internal(self):
        for payload in (GENERATED, {"author": "operator", "len": str(len(COMMENT))}):
            with self.subTest(payload=payload), self.event_case("commented", payload, invalid=False, comment=True) as case:
                case.reporter.tick()
                self.assertEqual(case.reporter.store.get(native.ISSUE)["last_event_id"], case.event_id)
                self.assertNotIn(COMMENT, case.remote.bodies(native.ISSUE))
                with case.board.read_conn() as conn:
                    self.assertEqual(case.board.comments(conn, case.task_id)[-1].body, COMMENT)
                self.check_independent_delivery(case)

    def test_actual_global_rate_limit_propagates_instead_of_becoming_a_bad_binding(self):
        with self.event_case("heartbeat", {"note": "Valid progress"}, invalid=False) as case:
            with case.board.conn() as conn:
                native.kb.edit_task(conn, case.task_id,
                    body=f"Linear issue {native.ISSUE}: {case.remote.issues[native.ISSUE]['url']}")
            case.remote.rate_limited_until = case.clock() + 60
            with self.assertRaises(RateLimited):
                case.reporter.pump_kanban()
            with self.assertRaises(RateLimited):
                case.reporter.recheck(force=True)
            self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)
            self.assertEqual(case.reporter.store.outbox_row(case.content_id)["state"], "pending")
            case.clock.now += 61
            case.remote.rate_limited_until = 0
            case.reporter.tick()
            self.assertEqual(case.remote.state("independent"), "Done")
            self.assertEqual(case.reporter.store.outbox_row(case.content_id)["state"], "sent")

    def test_keyboard_interrupt_is_not_swallowed_by_the_external_data_boundary(self):
        with self.event_case("heartbeat", {"note": "Valid progress"}, invalid=False) as case:
            with patch.object(case.reporter, "_task_event", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
                case.reporter.pump_kanban()
            self.assertEqual(case.reporter.store.get(native.ISSUE), case.before)


if __name__ == "__main__":
    unittest.main()
