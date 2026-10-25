"""Current native generations fence historical and queued Linear closeouts."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import test_linear_task_tracking_scenarios as tracking_fixtures

CORE, SOURCE = tracking_fixtures.CORE, tracking_fixtures.SOURCE


@unittest.skipIf(CORE is not None, f"integration: native source unavailable ({CORE})")
class NativeGenerationScenarios(unittest.TestCase):
    setUp = tracking_fixtures.TaskTrackingScenarios.setUp
    context = tracking_fixtures.TaskTrackingScenarios.context
    invoke = tracking_fixtures.TaskTrackingScenarios.invoke

    def running(self):
        self.assertTrue(self.board.unblock(self.task.id))
        with self.board.conn() as conn:
            self.assertTrue(self.board.kb.claim_task(conn, self.task.id))
        self.assertTrue(self.invoke(self.context("generation-owner"))["ok"])
        self.bridge.flush()
        self.assertEqual(self.remote.state("issue-1"), "In Progress")
        return self.board.get(self.task.id).current_run_id

    def complete(self, evidence="https://example.test/generation/first"):
        with self.board.conn() as conn:
            run_id = self.board.kb.get_task(conn, self.task.id).current_run_id
            self.assertTrue(self.board.kb.complete_task(conn, self.task.id,
                result="Verified delivery " + evidence, expected_run_id=run_id))

    def reopen(self):
        name = "hf562_native_generation_dashboard"
        if name not in sys.modules:
            source = Path(SOURCE) / "plugins" / "kanban" / "dashboard" / "plugin_api.py"
            spec = importlib.util.spec_from_file_location(name, source)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
        with self.board.conn() as conn:
            self.assertTrue(sys.modules[name]._set_status_direct(conn, self.task.id, "ready"))
            self.assertTrue(self.board.kb.claim_task(conn, self.task.id))
        return self.board.get(self.task.id).current_run_id

    def assert_running_unchanged(self, run_id):
        task = self.board.get(self.task.id)
        self.assertEqual((task.status, task.current_run_id), ("running", run_id))
        self.assertEqual(self.remote.state("issue-1"), "In Progress")
        self.assertIsNotNone(self.store.get("issue-1"), "reporting cannot discard an ongoing native task binding")
        self.assertEqual(self.remote.project_updates, [])
        self.assertFalse(any("Done." in c["body"] for c in self.remote.comments))
        self.assertEqual(self.store.retired("issue-1"), [], "historical closeout cannot retire another native run")

    def test_completed_history_cannot_close_reopened_native_run(self):
        first = self.running()
        self.complete()
        current = self.reopen()
        self.assertNotEqual(first, current)
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.assert_running_unchanged(current)

    def test_terminal_capture_cannot_outlive_the_native_generation(self):
        self.running()
        self.complete()
        self.bridge.pump_kanban()
        current = self.reopen()
        self.bridge.flush()
        self.assert_running_unchanged(current)

    def test_archived_history_cannot_retire_reopened_native_run(self):
        self.running()
        with self.board.conn() as conn:
            self.assertTrue(self.board.kb.archive_task(conn, self.task.id))
        current = self.reopen()
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.assert_running_unchanged(current)

    def test_legacy_retirement_recovery_cannot_archive_a_reopened_worker(self):
        from hermes_fleet_linear_plugin.api import LinearError

        self.running()
        self.store.retire("issue-1", self.task.id, self.board.retirement(self.task.id))
        self.complete()
        current = self.reopen()
        worker = self.registered_worker()
        before = self.board.get(self.task.id)
        self.bridge.recover()
        self.bridge.recheck(force=True)
        self.bridge.flush()
        after = self.board.get(self.task.id)
        self.assertEqual((after.status, after.current_run_id, after.claim_lock, after.worker_pid),
                         ("running", current, before.claim_lock, worker.pid))
        self.assertIsNone(worker.poll())
        retained = self.store.get("issue-1")
        self.assertTrue(retained["release_pending"])
        self.assertIn("native generation proof", retained["panel_note"])
        self.assertTrue(self.store.retired("issue-1"))
        with self.assertRaisesRegex(LinearError, "native reconciliation"):
            self.bridge.await_retired("issue-1")
        self.assignment("cannot-clear-legacy-retirement")
        self.assertTrue(self.store.get("issue-1")["release_pending"])
        self.assertIsNone(worker.poll())
        with self.board.conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 1)

    def test_reopen_during_capture_evidence_read_cannot_commit_closeout(self):
        self.running()
        self.complete()
        accepted = self.bridge.accepted_evidence
        current = []

        def reopen_after_read(*args, **kwargs):
            result = accepted(*args, **kwargs)
            current.append(self.reopen())
            return result

        with patch.object(self.bridge, "accepted_evidence", side_effect=reopen_after_read):
            self.bridge.pump_kanban()
        self.bridge.flush()
        self.assert_running_unchanged(current[0])

    def test_assignment_during_terminal_reconciliation_routes_to_reopened_worker_once(self):
        self.running()
        self.complete()
        accepted = self.bridge.accepted_evidence
        current = []

        def reopen_after_read(*args, **kwargs):
            result = accepted(*args, **kwargs)
            if not current: current.append(self.reopen())
            return result

        with patch.object(self.bridge, "accepted_evidence", side_effect=reopen_after_read):
            event = self.assignment("during-native-reconciliation")
        self.bridge.handle_webhook(event)
        self.bridge.flush()
        self.assert_running_unchanged(current[0])
        with self.board.conn() as conn:
            comments = self.board.kb.list_comments(conn, self.task.id)
            self.assertEqual(sum("Assignment from Linear" in comment.body for comment in comments), 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 1)

    def test_reopen_during_credential_refresh_cannot_send_terminal_mutation(self):
        self.running()
        self.complete()
        self.bridge.pump_kanban()
        transport = self.bridge.api.transport
        terminal_attempts, current = [], []
        owner = self

        class Credential:
            def __call__(self):
                return "fixture"

            def invalidate(self):
                current.append(owner.reopen())

        def reject_once(endpoint, body, headers):
            request = json.loads(body)
            fields = request.get("variables", {}).get("input", {})
            if "issueUpdate" in request["query"] and fields.get("stateId") == "st-done":
                terminal_attempts.append(True)
                if len(terminal_attempts) == 1:
                    return 401, {}, b'{"errors":[{"message":"expired"}]}'
            return transport(endpoint, body, headers)

        self.bridge.api.token = Credential()
        self.bridge.api.transport = reject_once
        self.bridge.flush()
        self.assertEqual(len(terminal_attempts), 1, "a refreshed credential is not permission to finish an obsolete run")
        self.assert_running_unchanged(current[0])

    def test_current_completion_reports_once_with_project_and_comment(self):
        self.running()
        self.complete()
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.assertEqual(self.remote.state("issue-1"), "Done")
        self.assertEqual(len(self.remote.project_updates), 1)
        self.assertEqual(sum("Done." in c["body"] for c in self.remote.comments), 1)
        self.assertEqual(self.board.get(self.task.id).status, "done")
        self.bridge.recheck(force=True)
        self.bridge.recover()
        self.assertEqual(self.board.get(self.task.id).status, "done")
        self.assertIsNotNone(self.store.get("issue-1"))
        self.assertFalse(self.store.get("issue-1")["release_pending"])

    def test_own_done_cannot_cancel_native_generation_reopened_after_delivery(self):
        self.linear_origin_running()
        self.complete()
        self.bridge.pump_kanban()
        self.bridge.flush()
        current = self.reopen()
        self.bridge.recheck(force=True)
        self.bridge.recover()
        task = self.board.get(self.task.id)
        self.assertEqual((task.status, task.current_run_id), ("running", current))
        self.assertIsNotNone(self.store.get("issue-1"))

    def linear_origin_running(self):
        from linear_fake_api import SELF

        self.remote.set_delegate("issue-1", {"id": SELF})
        self.bridge.handle_webhook(self.remote.session_event("created", "issue-1", "linear-origin"))
        self.task = self.board.get(self.store.get("issue-1")["task_id"])
        with self.board.conn() as conn:
            self.assertTrue(self.board.kb.claim_task(conn, self.task.id))
        self.bridge.flush()

    def test_human_close_still_cancels_the_original_linear_execution(self):
        self.linear_origin_running()
        self.clock.now += 10
        self.remote.set_state("issue-1", "Done")
        self.bridge.recheck(force=True)
        self.assertEqual(self.board.get(self.task.id).status, "archived")

    def test_human_reassignment_still_cancels_the_original_linear_execution(self):
        from linear_fake_api import OTHER

        self.linear_origin_running()
        self.clock.now += 10
        self.remote.set_delegate("issue-1", OTHER)
        self.bridge.recheck(force=True)
        self.assertEqual(self.board.get(self.task.id).status, "archived")

    def test_new_native_completion_replaces_obsolete_queued_result_once(self):
        self.running()
        self.complete()
        self.bridge.pump_kanban()
        self.reopen()
        self.complete("https://example.test/generation/second")
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.assertEqual(self.remote.state("issue-1"), "Done")
        self.assertEqual(len(self.remote.project_updates), 1)
        self.assertIn("/second", self.remote.project_updates[0]["body"])
        self.assertEqual(sum("Done." in c["body"] for c in self.remote.comments), 1)
        self.assertTrue(any("/second" in c["body"] for c in self.remote.comments))

    def test_reopen_during_delivery_acceptance_read_cannot_send_done(self):
        self.running()
        url = "https://github.com/example/project/pull/23"
        self.complete(url)
        receipt = {"ok": True, "head_sha": "a" * 40}
        with patch("hermes_fleet_linear_plugin.bridge.pr_acceptance", return_value=receipt):
            self.bridge.pump_kanban()
        current = []

        def reopen_after_acceptance(*args):
            if not current: current.append(self.reopen())
            return receipt

        with patch("hermes_fleet_linear_plugin.bridge.pr_acceptance", side_effect=reopen_after_acceptance):
            self.bridge.flush()
        self.assert_running_unchanged(current[0])

    def registered_worker(self):
        import subprocess
        from hermes_cli.kanban_db_dispatch import adopt_worker_pid

        worker = subprocess.Popen([sys.executable, "-c", "import sys; sys.stdin.read()"], stdin=subprocess.PIPE)

        def cleanup():
            if worker.poll() is None:
                worker.communicate(timeout=10)

        self.addCleanup(cleanup)
        with self.board.conn() as conn:
            task = self.board.kb.get_task(conn, self.task.id)
            self.assertTrue(adopt_worker_pid(conn, self.task.id, task.current_run_id, worker.pid))
        return worker

    def completed_with_worker_exit(self):
        self.running()
        worker = self.registered_worker()
        self.complete()
        worker.communicate(timeout=10)
        self.bridge.pump_kanban()
        self.bridge.flush()

    def assignment(self, session="increment-1"):
        self.clock.now += 10
        event = self.remote.session_event("created", "issue-1", session)
        self.bridge.handle_webhook(event)
        return event

    def test_verified_new_assignment_creates_one_distinct_native_increment(self):
        self.completed_with_worker_exit()
        event = self.assignment()
        binding = self.store.get("issue-1")
        self.assertNotEqual(binding["task_id"], self.task.id)
        with self.board.conn() as conn:
            increment = self.board.kb.get_task(conn, binding["task_id"])
            self.assertEqual(increment.status, "ready")
            self.assertEqual(self.board.kb.parent_ids(conn, increment.id), [self.task.id])
            self.assertTrue(self.board.kb.claim_task(conn, increment.id))
            self.assertEqual(self.board.kb.get_task(conn, self.task.id).status, "done")
        self.bridge.handle_webhook(event)
        with self.board.conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 2)

    def test_parent_reopen_during_increment_creation_cannot_create_a_competing_worker(self):
        self.completed_with_worker_exit()
        create = self.board.create
        current = []

        def reopen_after_creation(**kwargs):
            increment = create(**kwargs)
            if kwargs.get("_terminal_parent"):
                current.append(self.reopen())
            return increment

        with patch.object(self.board, "create", side_effect=reopen_after_creation):
            self.assignment()
        binding = self.store.get("issue-1")
        with self.board.conn() as conn:
            self.assertFalse(self.board.kb.claim_task(conn, binding["task_id"]))
            parent = self.board.kb.get_task(conn, self.task.id)
            self.assertEqual((parent.status, parent.current_run_id), ("running", current[0]))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks WHERE status='running'").fetchone()[0], 1)

    def test_fresh_assignment_routes_reopened_worker_after_reporting_fence(self):
        self.linear_origin_running()
        self.complete()
        self.bridge.pump_kanban()
        self.bridge.flush()
        current = self.reopen()
        self.bridge.recheck(force=True)
        self.assertTrue(self.store.get("issue-1")["release_pending"])
        event = self.assignment("follow-current-generation")
        self.bridge.handle_webhook(event)
        self.bridge.flush()
        task = self.board.get(self.task.id)
        self.assertEqual((task.status, task.current_run_id), ("running", current))
        self.assertEqual(self.store.get("issue-1")["task_id"], self.task.id)
        self.assertFalse(self.store.get("issue-1")["release_pending"])
        with self.board.conn() as conn:
            comments = self.board.kb.list_comments(conn, self.task.id)
            self.assertEqual(sum("Assignment from Linear" in comment.body for comment in comments), 1)

    def test_unknown_terminal_attempt_stays_fenced_after_generation_changes(self):
        self.running()
        self.complete()
        self.bridge.pump_kanban()
        status = next(row for row in self.store.pending("issue-1") if row["kind"] == "status" and row["payload"].get("terminal"))
        self.store.rewrite(status["id"], {**status["payload"], "write_started": True}, status["next_at"])
        current = self.reopen()
        self.bridge.flush()
        held = self.store.outbox_row(status["id"])
        self.assertTrue(held["payload"]["reconcile_required"])
        self.assertTrue(held["payload"]["write_started"])
        self.assertEqual(self.remote.state("issue-1"), "In Progress")
        self.assertEqual(self.board.get(self.task.id).current_run_id, current)
        self.assertIsNotNone(self.store.get("issue-1"))

    def test_human_takeover_settles_unsent_reporting_but_preserves_unknown_effects(self):
        from linear_fake_api import OTHER

        self.running()
        self.complete()
        self.bridge.pump_kanban()
        status = next(row for row in self.store.pending("issue-1") if row["kind"] == "status" and row["payload"].get("terminal"))
        self.store.rewrite(status["id"], {**status["payload"], "write_started": True}, status["next_at"])
        self.remote.set_delegate("issue-1", OTHER)
        self.bridge.recheck(force=True)
        self.bridge.flush()
        held = self.store.outbox_row(status["id"])
        self.assertNotEqual(held["state"], "sent")
        self.assertTrue(held["payload"]["write_started"])
        self.assertTrue(self.store.get("issue-1")["release_pending"])
        self.assertEqual(self.board.get(self.task.id).status, "done")
        self.assertEqual(self.remote.state("issue-1"), "In Progress")
        self.assertFalse(any("Done." in c["body"] for c in self.remote.comments))

    def deleted_native_task(self, change):
        from linear_fake_api import OTHER

        self.running()
        before = self.store.get("issue-1")
        with self.board.conn() as conn:
            self.assertTrue(self.board.kb.archive_task(conn, self.task.id))
            self.assertTrue(self.board.kb.delete_archived_task(conn, self.task.id))
        self.bridge.pump_kanban()
        if change == "closed":
            self.remote.set_state("issue-1", "Done")
        else:
            self.remote.set_delegate("issue-1", OTHER)
        with patch.object(self.board, "archive", side_effect=AssertionError("missing task must not be retired again")):
            self.bridge.recheck(force=True)
            self.bridge.recover()
            self.bridge.flush()
        retained = self.store.get("issue-1")
        self.assertEqual((retained["task_id"], retained["ownership_id"], retained["linked_at"]),
                         (self.task.id, before["ownership_id"], before["linked_at"]))
        self.assertTrue(retained["release_pending"])
        self.assertIn("missing", retained["panel_note"])
        self.assertIsNone(self.board.get(self.task.id))
        self.assertEqual(self.store.retired("issue-1"), [])
        with self.board.conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 0)

    def test_human_close_preserves_missing_native_task_binding(self):
        self.deleted_native_task("closed")

    def test_human_reassignment_preserves_missing_native_task_binding(self):
        self.deleted_native_task("reassigned")

    def test_legacy_closeout_without_native_generation_stays_visible_and_fenced(self):
        self.running()
        self.complete()
        self.bridge.pump_kanban()
        status = next(row for row in self.store.pending("issue-1") if row["kind"] == "status" and row["payload"].get("terminal"))
        payload = {key: value for key, value in status["payload"].items() if key != "native_generation"}
        self.store.rewrite(status["id"], payload, status["next_at"])
        self.bridge.flush()
        held = self.store.outbox_row(status["id"])
        self.assertTrue(held["payload"]["reconcile_required"])
        self.assertEqual(self.remote.state("issue-1"), "In Progress")
        self.assertIsNotNone(self.store.get("issue-1"))
        self.assertTrue(self.store.issue_reconciliation_blocked("issue-1"))

    def test_human_takeover_does_not_settle_unproven_legacy_closeout(self):
        from linear_fake_api import OTHER

        self.running()
        self.complete()
        self.bridge.pump_kanban()
        status = next(row for row in self.store.pending("issue-1") if row["kind"] == "status" and row["payload"].get("terminal"))
        payload = {key: value for key, value in status["payload"].items() if key != "native_generation"}
        self.store.rewrite(status["id"], payload, status["next_at"])
        self.remote.set_delegate("issue-1", OTHER)
        self.bridge.recheck(force=True)
        self.bridge.flush()
        retained = self.store.outbox_row(status["id"])
        self.assertEqual(retained["state"], "pending")
        self.assertEqual(retained["payload"], payload)
        self.assertTrue(self.store.get("issue-1")["release_pending"])
        self.assertEqual(self.board.get(self.task.id).status, "done")
        self.assertEqual(self.remote.state("issue-1"), "In Progress")

    def test_increment_waits_for_actual_prior_worker_exit(self):
        from hermes_fleet_linear_plugin.api import LinearError

        self.running()
        worker = self.registered_worker()
        self.complete()
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.assertIsNone(worker.poll())
        self.clock.now += 10
        event = self.remote.session_event("created", "issue-1", "after-worker-exit")
        with self.assertRaises(LinearError):
            self.bridge.handle_webhook(event)
        self.assertEqual(self.store.get("issue-1")["task_id"], self.task.id)
        worker.communicate(timeout=10)
        self.bridge.handle_webhook(event)
        self.assertNotEqual(self.store.get("issue-1")["task_id"], self.task.id)
        with self.board.conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
