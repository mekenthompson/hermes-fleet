"""Native control requests retain their original admission fence across waits and retries."""
import copy
import os
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import patch

import test_linear_native_continuation_scenarios as fixtures


@unittest.skipIf(fixtures.CORE is not None, f"integration: native source unavailable ({fixtures.CORE})")
class NativeIntentScenarios(unittest.TestCase):
    setUp = fixtures.NativeContinuationScenarios.setUp
    reopen = fixtures.NativeContinuationScenarios.reopen
    notes = fixtures.NativeContinuationScenarios.notes
    prompt = fixtures.NativeContinuationScenarios.prompt
    queue = fixtures.NativeContinuationScenarios.queue

    def stop(self, marker="original-stop"):
        self.bridge.handle_webhook(self.prompt("", marker, signal="stop"))
        self.assertEqual(self.board.get(self.task_id).status, "blocked")

    def resume_and_claim(self):
        with self.board.conn() as conn:
            self.assertTrue(fixtures.kb.unblock_task(conn, self.task_id))
            self.assertTrue(fixtures.kb.claim_task(conn, self.task_id))
        return self.board.get(self.task_id)

    def assert_same_run(self, before):
        after = self.board.get(self.task_id)
        self.assertEqual((after.status, after.current_run_id, after.claim_lock),
                         (before.status, before.current_run_id, before.claim_lock))

    def request_contexts(self):
        with sqlite3.connect(self.store.path) as conn:
            return dict(conn.execute("SELECT key,value FROM metadata WHERE key LIKE 'native-request:%'"))

    def test_old_unapplied_stop_cannot_cancel_new_native_resumption(self):
        event = self.prompt("", "stop-before-native-resume", signal="stop")
        inbox = self.queue(event, "stop-before-native-resume")
        with patch.object(self.board, "block", side_effect=OSError("native write unavailable before commit")):
            self.bridge.drain_ingress(inbox)
        original_context = self.request_contexts()
        self.assertGreater(self.store.get(fixtures.ISSUE)["stop_requested_at"], 0)
        with self.board.conn() as conn:
            self.assertTrue(fixtures.kb.block_task(conn, self.task_id, reason="Newer native Stop"))
        before = self.resume_and_claim()
        self.bridge = self.reopen()
        self.clock.now += 5
        self.bridge.drain_ingress(inbox)
        self.bridge.handle_webhook(event)  # same request under another delivery cannot renew its fence
        altered_delivery = copy.deepcopy(event)
        altered_delivery["agentActivity"]["createdAt"] = self.clock.iso()
        self.bridge.handle_webhook(altered_delivery)
        self.assert_same_run(before)
        self.assertEqual(self.request_contexts(), original_context)

    def test_newer_native_stop_during_assignment_context_wait_stays_stopped(self):
        self.stop()
        self.clock.now += 1
        event = self.remote.session_event("created", fixtures.ISSUE, "assignment-before-stop")
        inbox = self.queue(event, "assignment-before-stop")
        thread = self.bridge._thread

        def stop_during_context(*args, **kwargs):
            result = thread(*args, **kwargs)
            with self.board.conn() as conn:
                self.assertFalse(fixtures.kb.block_task(conn, self.task_id, reason="Newer native Stop during HTTP context"))
            return result

        with patch.object(self.bridge, "_thread", side_effect=stop_during_context):
            self.bridge.drain_ingress(inbox)
        self.bridge = self.reopen()
        self.bridge.handle_webhook(event)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        with sqlite3.connect(inbox) as conn:
            self.assertEqual(conn.execute("SELECT status FROM deliveries").fetchone()[0], "imported")

    def during_issue_read(self, callback):
        issue = self.bridge.api.issue
        called = []

        def after_http_read(*args, **kwargs):
            result = issue(*args, **kwargs)
            if not called:
                called.append(True)
                callback()
            return result

        return patch.object(self.bridge.api, "issue", side_effect=after_http_read)

    def test_assignment_is_anchored_before_the_first_issue_authorization_read(self):
        self.stop()
        self.clock.now += 1
        event = self.remote.session_event("created", fixtures.ISSUE, "before-authorization")

        def newer_stop():
            with self.board.conn() as conn:
                self.assertFalse(fixtures.kb.block_task(conn, self.task_id, reason="Newer Stop during issue authorization"))

        with self.during_issue_read(newer_stop):
            self.bridge.handle_webhook(event)
        original_context = self.request_contexts()
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        # A distinct current request can resume; the old one cannot renew its anchor.
        self.bridge.handle_webhook(event)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        altered_delivery = copy.deepcopy(event)
        self.clock.now += 1
        altered_delivery["agentSession"]["createdAt"] = self.clock.iso()
        self.bridge.handle_webhook(altered_delivery)
        self.assertEqual(self.request_contexts(), original_context)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        self.assertEqual(sum("Re-delegated" in body for body in self.notes()), 1)
        self.clock.now += 1
        self.bridge.handle_webhook(self.remote.session_event("created", fixtures.ISSUE, "after-native-stop"))
        self.assertEqual(self.board.get(self.task_id).status, "ready")

    def test_stop_is_anchored_before_the_first_issue_authorization_read(self):
        event = self.prompt("", "stop-before-authorization", signal="stop")
        current = []

        def native_resume():
            with self.board.conn() as conn:
                self.assertTrue(fixtures.kb.block_task(conn, self.task_id, reason="Newer native Stop"))
            current.append(self.resume_and_claim())

        with self.during_issue_read(native_resume):
            self.bridge.handle_webhook(event)
        self.assert_same_run(current[0])

    def test_assignment_admitted_before_completion_cannot_become_a_new_increment(self):
        self.clock.now += 1
        event = self.remote.session_event("created", fixtures.ISSUE, "before-completion")

        def complete_during_read():
            with self.board.conn() as conn:
                self.assertTrue(fixtures.kb.complete_task(conn, self.task_id,
                    result="Verified prior work https://example.test/before-new-increment"))

        with self.during_issue_read(complete_during_read):
            self.bridge.handle_webhook(event)
        self.assertEqual(self.store.get(fixtures.ISSUE)["task_id"], self.task_id)
        with self.board.conn() as conn:
            self.assertEqual(len(fixtures.kb.list_tasks(conn)), 1)
        self.clock.now += 1
        self.bridge.handle_webhook(self.remote.session_event("created", fixtures.ISSUE, "after-completion"))
        child = self.store.get(fixtures.ISSUE)["task_id"]
        self.assertNotEqual(child, self.task_id)
        with self.board.conn() as conn:
            self.assertEqual(fixtures.kb.parent_ids(conn, child), [self.task_id])

    def test_native_scope_edit_during_increment_authorization_invalidates_old_intent(self):
        with self.board.conn() as conn:
            self.assertTrue(fixtures.kb.complete_task(conn, self.task_id,
                result="Verified prior result https://example.test/native-scope-edit"))
        self.clock.now += 1
        event = self.remote.session_event("created", fixtures.ISSUE, "increment-before-scope-edit")

        def edit_native_scope():
            with self.board.conn() as conn:
                self.assertTrue(fixtures.kb.edit_task(conn, self.task_id, body="Operator changed the remaining native scope"))

        with self.during_issue_read(edit_native_scope):
            self.bridge.handle_webhook(event)
        self.assertEqual(self.store.get(fixtures.ISSUE)["task_id"], self.task_id)
        self.assertEqual(self.board.get(self.task_id).body, "Operator changed the remaining native scope")
        with self.board.conn() as conn:
            self.assertEqual(len(fixtures.kb.list_tasks(conn)), 1)

    def test_task_handoff_during_stop_authorization_cannot_cancel_foreign_work(self):
        event = self.prompt("", "stop-before-handoff", signal="stop")
        with self.during_issue_read(self.foreign_handoff):
            self.bridge.handle_webhook(event)
        task = self.board.get(self.task_id)
        self.assertEqual((task.status, task.assignee), ("ready", "other-specialist"))

    def test_failed_before_commit_stop_retries_the_same_unchanged_native_state(self):
        event = self.prompt("", "unchanged-native-stop", signal="stop")
        inbox = self.queue(event)
        with patch.object(self.board, "block", side_effect=OSError("native write unavailable")):
            self.bridge.drain_ingress(inbox)
        self.bridge = self.reopen()
        self.clock.now += 5
        self.bridge.drain_ingress(inbox)
        self.bridge.handle_webhook(event)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        with self.board.conn() as conn:
            receipts = [e for e in fixtures.kb.list_events(conn, self.task_id) if e.kind == "lifecycle_request"
                        and (e.payload or {}).get("operation") == "block"]
        self.assertEqual(len(receipts), 1)

    def test_legacy_retry_without_original_anchor_is_fenced(self):
        event = self.prompt("", "legacy-attempted-stop", signal="stop")
        inbox = self.queue(event)
        with sqlite3.connect(inbox) as conn:
            conn.execute("UPDATE deliveries SET attempts=1")
        self.clock.now += 5
        before = self.board.get(self.task_id)
        self.bridge.drain_ingress(inbox)
        self.bridge.handle_webhook(event)
        self.assert_same_run(before)
        self.assertEqual(self.store.get(fixtures.ISSUE)["stop_requested_at"], 0)

    def test_lost_resume_before_commit_cannot_cross_newer_native_stop_on_retry(self):
        self.stop()
        self.clock.now += 1
        event = self.remote.session_event("created", fixtures.ISSUE, "resume-before-commit-loss")
        inbox = self.queue(event)
        with patch.object(self.board, "unblock", side_effect=OSError("no native mutation committed")):
            self.bridge.drain_ingress(inbox)
        with self.board.conn() as conn:
            self.assertFalse(fixtures.kb.block_task(conn, self.task_id, reason="Newer native Stop before retry"))
        self.bridge = self.reopen()
        self.clock.now += 5
        self.bridge.drain_ingress(inbox)
        self.bridge.handle_webhook(event)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        self.assertEqual(sum("Re-delegated" in body for body in self.notes()), 1)

    def test_committed_stop_with_lost_receipt_never_replays_into_a_new_run(self):
        event = self.prompt("", "lost-committed-stop", signal="stop")
        inbox = self.queue(event)
        block = self.board.block

        def lose_response(*args, **kwargs):
            self.assertTrue(block(*args, **kwargs))
            raise OSError("native Stop committed but response lost")

        with patch.object(self.board, "block", side_effect=lose_response):
            self.bridge.drain_ingress(inbox)
        before = self.resume_and_claim()
        self.bridge = self.reopen()
        self.clock.now += 5
        with patch.object(self.board, "block", side_effect=AssertionError("committed Stop replayed")):
            self.bridge.drain_ingress(inbox)
        self.assert_same_run(before)

    def test_comments_during_resume_wait_do_not_invalidate_control_intent(self):
        self.stop()
        self.clock.now += 1
        event = self.remote.session_event("created", fixtures.ISSUE, "resume-with-comments")
        thread = self.bridge._thread

        def unrelated_comment(*args, **kwargs):
            result = thread(*args, **kwargs)
            with self.board.conn() as conn:
                fixtures.kb.add_comment(conn, self.task_id, "operator", "Additional status context")
            return result

        with patch.object(self.bridge, "_thread", side_effect=unrelated_comment):
            self.bridge.handle_webhook(event)
        self.assertEqual(self.board.get(self.task_id).status, "ready")

    def test_ordinary_note_survives_native_claim_during_context_wait(self):
        event = self.prompt("Keep the already completed checkpoint", "ordinary-note")
        thread = self.bridge._thread
        current = []

        def claim_during_read(*args, **kwargs):
            result = thread(*args, **kwargs)
            with self.board.conn() as conn:
                self.assertTrue(fixtures.kb.claim_task(conn, self.task_id))
            current.append(self.board.get(self.task_id))
            return result

        with patch.object(self.bridge, "_thread", side_effect=claim_during_read):
            self.bridge.handle_webhook(event)
        self.bridge.handle_webhook(event)
        self.assert_same_run(current[0])
        self.assertEqual(sum("already completed checkpoint" in body for body in self.notes()), 1)

    def foreign_handoff(self):
        with self.board.conn() as conn:
            self.assertTrue(fixtures.kb.assign_task(conn, self.task_id, "other-specialist"))
            return self.board.kn.list_notify_subs(conn, self.task_id)

    def registered_worker(self, task_id=None):
        from hermes_cli.kanban_db_dispatch import adopt_worker_pid

        task_id = task_id or self.task_id
        worker = subprocess.Popen([sys.executable, "-c", "import sys;sys.stdin.read()"], stdin=subprocess.PIPE)
        self.addCleanup(lambda: worker.communicate(timeout=10) if worker.poll() is None else worker.communicate())
        with self.board.conn() as conn:
            if fixtures.kb.get_task(conn, task_id).status != "running":
                self.assertTrue(fixtures.kb.claim_task(conn, task_id))
            task = fixtures.kb.get_task(conn, task_id)
            self.assertTrue(adopt_worker_pid(conn, task_id, task.current_run_id, worker.pid))
        return worker

    def rejected_admission_preserves_native_claim(self, *, scope_denied):
        issue = "staged-native-unit"
        self.remote.add_issue(issue, "STAGE-1")
        self.remote.set_delegate(issue, {"id": fixtures.SELF})
        event = self.remote.session_event("created", issue, "staged-native-session")
        put, create = self.bridge.store.put, self.board.create
        current, workers = [], []

        def native_claim(task):
            with self.board.conn() as conn:
                self.assertTrue(fixtures.kb.unblock_task(conn, task.id))
            workers.append(self.registered_worker(task.id))
            current.append(self.board.get(task.id))

        def rejected_bind(issue_id, *args, **kwargs):
            if issue_id == issue:
                if not current: native_claim(self.board.get(kwargs["task_id"]))
                return False
            return put(issue_id, *args, **kwargs)

        def denied_after_create(**kwargs):
            task = create(**kwargs)
            native_claim(task)
            self.bridge.store.fence_scope(issue, "Explicit external scope denial", at=self.clock())
            return task

        with patch.object(self.board, "create", side_effect=denied_after_create) if scope_denied else \
                patch.object(self.bridge.store, "put", side_effect=rejected_bind):
            self.bridge.handle_webhook(event)
            if not scope_denied: self.bridge.handle_webhook(event)
        self.assertTrue(current)
        task = self.board.get(current[0].id)
        self.assertEqual((task.status, task.current_run_id, task.claim_lock, task.worker_pid),
                         ("running", current[0].current_run_id, current[0].claim_lock, workers[0].pid))
        self.assertIsNone(workers[0].poll())
        self.assertIsNone(self.store.get(issue))
        with self.board.conn() as conn:
            self.assertEqual(len(fixtures.kb.list_tasks(conn)), 2)
            comments = fixtures.kb.list_comments(conn, task.id)
            self.assertEqual(sum("not reconciled" in comment.body for comment in comments), 1)
            self.assertEqual(fixtures.kb.get_task(conn, task.id).idempotency_key,
                             f"linear:{issue}:staged-native-session")

    def test_rejected_binding_preserves_newer_native_claim_instead_of_rollback_archive(self):
        self.rejected_admission_preserves_native_claim(scope_denied=False)

    def test_denied_admission_preserves_newer_native_claim_instead_of_rollback_archive(self):
        self.rejected_admission_preserves_native_claim(scope_denied=True)

    def assert_retained_cancellation(self, before):
        retained = self.store.get(fixtures.ISSUE)
        self.assertEqual((retained["task_id"], retained["ownership_id"], retained["linked_at"]),
                         (before["task_id"], before["ownership_id"], before["linked_at"]))
        self.assertTrue(retained["release_pending"])
        self.assertIn("reconcil", retained["panel_note"].lower())
        self.assertEqual(self.store.retired(fixtures.ISSUE), [])

    def test_fresh_human_close_blocks_owned_worker_and_preserves_actual_history(self):
        worker = self.registered_worker()
        before = self.store.get(fixtures.ISSUE)
        self.remote.set_state(fixtures.ISSUE, "Done")
        with patch.object(self.board.kb, "archive_task", side_effect=AssertionError("reporting cannot archive execution")):
            self.bridge.recheck(force=True)
        worker.wait(timeout=10)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        self.assert_retained_cancellation(before)
        self.assertIn("external effects", self.store.get(fixtures.ISSUE)["panel_note"])

    def test_foreign_human_close_preserves_native_task_worker_and_subscription(self):
        subscribers = self.foreign_handoff()
        worker = self.registered_worker()
        before = self.board.get(self.task_id)
        binding = self.store.get(fixtures.ISSUE)
        self.remote.set_state(fixtures.ISSUE, "Done")
        with patch.object(self.board.kb, "archive_task", side_effect=AssertionError("foreign archive")), \
                patch.object(self.board, "block", side_effect=AssertionError("foreign block")):
            self.bridge.recheck(force=True)
        self.assert_same_run(before)
        self.assertIsNone(worker.poll())
        self.assert_retained_cancellation(binding)
        with self.board.conn() as conn:
            self.assertEqual(self.board.kn.list_notify_subs(conn, self.task_id), subscribers)

    def test_native_handoff_after_last_cancel_scope_read_refuses_actual_consumer(self):
        before = self.store.get(fixtures.ISSUE)
        block = self.board.block
        current, workers = [], []

        def handoff_before_block(*args, **kwargs):
            self.foreign_handoff()
            workers.append(self.registered_worker())
            current.append(self.board.get(self.task_id))
            return block(*args, **kwargs)

        self.remote.set_state(fixtures.ISSUE, "Done")
        with patch.object(self.board, "block", side_effect=handoff_before_block), \
                patch.object(self.board.kb, "archive_task", side_effect=AssertionError("handoff archive")):
            self.bridge.recheck(force=True)
        self.assertTrue(current, "the real native cancellation consumer must be reached")
        self.assert_same_run(current[0])
        self.assertIsNone(workers[0].poll())
        self.assert_retained_cancellation(before)
        self.assertIn("not confirmed", self.store.get(fixtures.ISSUE)["panel_note"])

    def test_unseen_human_close_inside_store_guard_defers_until_snapshot_commits(self):
        worker = self.registered_worker()
        self.remote.set_state(fixtures.ISSUE, "Done")
        with self.bridge.store.guard_issue_work(fixtures.ISSUE) as admitted:
            self.assertTrue(admitted)
            self.assertFalse(self.bridge.may_write(self.bridge._effect_issue(fixtures.ISSUE), False))
            self.assertIsNone(worker.poll())
            self.assertEqual(self.board.get(self.task_id).status, "running")
        original = self.request_contexts()
        self.assertTrue(self.store.get(fixtures.ISSUE)["release_pending"])
        self.bridge.recheck(force=True)
        worker.wait(timeout=10)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        self.assertEqual(self.request_contexts(), original)

    def test_old_human_close_retry_and_timestamp_update_cannot_cancel_native_resume(self):
        self.remote.set_state(fixtures.ISSUE, "Done")
        self.bridge.recheck(force=True)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        before = self.resume_and_claim()
        worker = self.registered_worker()
        original = self.request_contexts()
        self.clock.now += 10
        self.remote.issues[fixtures.ISSUE]["updatedAt"] = self.clock.iso()  # unrelated issue activity
        self.bridge = self.reopen()
        with patch.object(self.board, "block", side_effect=AssertionError("old cancellation replayed")):
            self.bridge.recheck(force=True)
        self.assert_same_run(before)
        self.assertIsNone(worker.poll())
        self.assertEqual(self.request_contexts(), original)

    def test_distinct_assignment_can_resume_reconciled_human_cancellation(self):
        worker = self.registered_worker()
        self.remote.set_state(fixtures.ISSUE, "Done")
        self.bridge.recheck(force=True)
        worker.wait(timeout=10)
        self.clock.now += 1
        self.bridge.handle_webhook(self.remote.session_event("created", fixtures.ISSUE, "new-instruction-after-cancellation"))
        task = self.board.get(self.task_id)
        self.assertEqual(task.status, "ready")
        self.assertEqual(self.store.get(fixtures.ISSUE)["task_id"], self.task_id)
        self.assertFalse(self.store.get(fixtures.ISSUE)["release_pending"])
        self.bridge.flush()
        self.bridge.recheck(force=True)
        self.assertEqual(self.remote.state(fixtures.ISSUE), "In Progress")
        self.assertFalse(self.store.get(fixtures.ISSUE)["release_pending"])
        with self.board.conn() as conn:
            self.assertEqual(len(fixtures.kb.list_tasks(conn)), 1)

    def test_second_human_close_after_verified_reopen_stops_the_current_worker(self):
        first = self.registered_worker()
        self.remote.set_state(fixtures.ISSUE, "Done")
        self.bridge.recheck(force=True)
        first.wait(timeout=10)
        self.clock.now += 1
        self.bridge.handle_webhook(self.remote.session_event("created", fixtures.ISSUE, "between-human-closures"))
        self.bridge.flush()  # verified remote reopen; no subsequent observation before human closes again
        self.assertEqual(self.remote.state(fixtures.ISSUE), "In Progress")
        second = self.registered_worker()
        self.clock.now += 1
        self.remote.set_state(fixtures.ISSUE, "Done")
        self.bridge.recheck(force=True)
        self.assertIn(self.board.get(self.task_id).status, ("blocked", "triage"))
        second.wait(timeout=10)
        self.assertTrue(self.store.get(fixtures.ISSUE)["release_pending"])

    def test_stop_cannot_cancel_a_foreign_native_executor(self):
        subscribers = self.foreign_handoff()
        before = self.board.get(self.task_id)
        self.bridge.handle_webhook(self.prompt("", "foreign-stop", signal="stop"))
        self.assert_same_run(before)
        self.assertEqual(self.store.get(fixtures.ISSUE)["stop_requested_at"], 0)
        with self.board.conn() as conn:
            self.assertEqual(self.board.kn.list_notify_subs(conn, self.task_id), subscribers)

    def test_foreign_stopped_executor_receives_notes_without_resumption(self):
        self.stop()
        subscribers = self.foreign_handoff()
        before = self.board.get(self.task_id)
        stop_at = self.store.get(fixtures.ISSUE)["stop_requested_at"]
        event = self.prompt("Route this instruction to the current native owner", "foreign-note")
        self.bridge.handle_webhook(event)
        self.bridge.handle_webhook(event)
        self.clock.now += 1
        assignment = self.remote.session_event("created", fixtures.ISSUE, "foreign-resume-request")
        self.bridge.handle_webhook(assignment)
        self.bridge.handle_webhook(assignment)
        self.assert_same_run(before)
        self.assertEqual(self.store.get(fixtures.ISSUE)["stop_requested_at"], stop_at)
        self.assertEqual(sum("Route this instruction" in body for body in self.notes()), 1)
        self.assertEqual(sum("Re-delegated" in body for body in self.notes()), 1)
        with self.board.conn() as conn:
            self.assertEqual(self.board.kn.list_notify_subs(conn, self.task_id), subscribers)

    def test_retained_subscriber_reports_foreign_completion_without_native_control(self):
        # Use the real explicitly scoped adapter, whose terminal readback used to
        # confuse executor identity with the existing notification subscription.
        with patch.dict(os.environ, {"HERMES_HOME": str(self.home)}):
            self.board = fixtures.Kanban(profile="default", profile_home=self.home)
        self.bridge = self.reopen()
        subscribers = self.foreign_handoff()
        self.assertTrue(any(sub["platform"] == "linear" and sub["chat_id"] == fixtures.ISSUE for sub in subscribers))
        with self.board.conn() as conn:
            self.assertTrue(fixtures.kb.claim_task(conn, self.task_id))
            task = fixtures.kb.get_task(conn, self.task_id)
            self.assertTrue(fixtures.kb.complete_task(conn, self.task_id,
                result="Verified native handoff https://example.test/handoff/result", expected_run_id=task.current_run_id))
        with patch.object(self.board, "block", side_effect=AssertionError("reporting cannot block")), \
                patch.object(self.board, "unblock", side_effect=AssertionError("reporting cannot resume")), \
                patch.object(self.board.kb, "archive_task", side_effect=AssertionError("reporting cannot archive")):
            self.bridge.recover()
            self.bridge.flush()
            self.bridge.recover()
            self.bridge.flush()
        self.assertEqual(self.remote.state(fixtures.ISSUE), "Done")
        self.assertEqual(sum("Done." in activity["content"].get("body", "") for activity in self.remote.activities), 1)
        self.assertEqual(len(self.remote.project_updates), 1)
        self.assertEqual(self.board.get(self.task_id).assignee, "other-specialist")
        self.assertEqual(self.board.get(self.task_id).status, "done")
        self.assertEqual(self.store.get(fixtures.ISSUE)["task_id"], self.task_id)
        with self.board.conn() as conn:
            self.assertEqual(self.board.kn.list_notify_subs(conn, self.task_id), subscribers)
            self.assertEqual(len(fixtures.kb.list_tasks(conn)), 1)
        self.clock.now += 1
        with patch.object(self.board, "create", side_effect=AssertionError("foreign terminal report cannot claim a new executor")):
            self.bridge.handle_webhook(self.remote.session_event("created", fixtures.ISSUE, "foreign-completed-assignment"))
        self.assertEqual(self.store.get(fixtures.ISSUE)["task_id"], self.task_id)


if __name__ == "__main__":
    unittest.main()
