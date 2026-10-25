"""Verified tracker requests use native admission/idempotency; reporting never recovers a worker."""
from __future__ import annotations

import concurrent.futures
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from linear_fake_api import Clock, FakeLinear, SELF, load_plugin
from linear_ingress_fixture import IngressStore, Route

SOURCE = os.environ.get("HERMES_AGENT_SRC", "")
try:
    if not SOURCE:
        raise ImportError("HERMES_AGENT_SRC is not set")
    sys.path.insert(0, SOURCE)
    from hermes_cli import kanban_db as kb, kanban_db_dispatch as dispatch
    CORE = None
except ImportError as exc:
    CORE = str(exc)

load_plugin()
from hermes_fleet_linear_plugin.api import LinearAPI, LinearError
from hermes_fleet_linear_plugin.bridge import Bridge, Kanban
from hermes_fleet_linear_plugin.store import Store

ISSUE = "native-issue"


@unittest.skipIf(CORE is not None, f"integration: native source unavailable ({CORE})")
class NativeContinuationScenarios(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.home = Path(tmp.name)
        env = patch.dict(os.environ, {"HERMES_KANBAN_HOME": str(self.home / "kanban")})
        env.start()
        self.addCleanup(env.stop)
        self.clock = Clock()
        self.remote = FakeLinear(self.clock)
        self.addCleanup(self.remote.close)
        self.remote.add_issue(ISSUE, "NATIVE-1")
        self.remote.set_delegate(ISSUE, {"id": SELF})
        self.board = Kanban()
        self.store = Store(self.home / "tracking.db")
        self.bridge = self.reopen()
        self.bridge.handle_webhook(self.remote.session_event("created", ISSUE, "original-session"))
        self.bridge.flush()
        self.task_id = self.store.get(ISSUE)["task_id"]

    def reopen(self):
        return Bridge(Store(self.home / "tracking.db"),
                      LinearAPI(lambda: "fixture", endpoint=self.remote.url, clock=self.clock),
                      self.board, profile="default", clock=self.clock)

    def notes(self):
        with self.board.conn() as conn:
            return [comment.body for comment in kb.list_comments(conn, self.task_id)]

    def prompt(self, body, marker, *, signal=None):
        self.clock.now += 1
        return self.remote.session_event("prompted", ISSUE, "original-session", body=body,
                                         activity_id=marker, signal=signal)

    def queue(self, event, delivery="followup"):
        inbox = self.home / "inbox.db"
        IngressStore(inbox).enqueue(Route("default", "default", "/webhook/default", inbox, inbox),
                                   delivery, json.dumps(event).encode())
        with sqlite3.connect(inbox) as conn:
            conn.execute("UPDATE deliveries SET received_at=?", (self.clock(),))
        return inbox

    def test_lost_native_comment_receipt_retries_original_inbox_once(self):
        event = self.prompt("Keep the checkpoint and continue remaining work", "comment-1")
        inbox = self.queue(event)
        add = kb.add_comment

        def lost_receipt(*args, **kwargs):
            add(*args, **kwargs)
            raise OSError("native comment receipt lost")

        with patch.object(kb, "add_comment", side_effect=lost_receipt):
            self.bridge.drain_ingress(inbox)
        self.assertEqual(sum("Keep the checkpoint" in body for body in self.notes()), 1)
        saved = list(self.notes())
        self.remote.comments.append({"issueId": ISSUE, "id": "late-thread", "body": "Thread edited after delivery",
                                     "createdAt": self.clock.iso(), "author": "Pat Example"})
        self.assertIsNone(self.store.get(ISSUE)["pending_resume"])
        self.bridge = self.reopen()
        self.bridge.recover()  # reporting alone cannot replay the instruction
        self.clock.now += 5
        self.bridge.drain_ingress(inbox)
        self.bridge.drain_ingress(inbox)
        self.assertEqual(sum("Keep the checkpoint" in body for body in self.notes()), 1)
        self.assertEqual(self.notes(), saved, "a receipt retry cannot reconstruct a different thread snapshot")
        with sqlite3.connect(inbox) as conn:
            self.assertEqual(conn.execute("SELECT status FROM deliveries").fetchone()[0], "imported")
        with self.board.conn() as conn:
            self.assertEqual(len(kb.list_tasks(conn)), 1)

    def test_inbox_stop_retries_native_cancellation_after_committed_fence(self):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            with self.board.conn() as conn:
                self.assertTrue(kb.claim_task(conn, self.task_id))
                dispatch._set_worker_pid(conn, self.task_id, process.pid)
            event = self.prompt("", "retry-native-stop", signal="stop")
            inbox = self.queue(event, "stop-retry")
            with patch.object(self.board, "block", side_effect=OSError("native DB unavailable")):
                self.bridge.drain_ingress(inbox)
            self.assertGreater(self.store.get(ISSUE)["stop_requested_at"], 0)
            self.assertEqual(self.board.get(self.task_id).status, "running")
            self.assertIsNone(process.poll(), "a recorded fence does not prove process cancellation")
            self.bridge = self.reopen()
            self.clock.now += 5
            self.bridge.drain_ingress(inbox)
            self.bridge.handle_webhook(event)  # the same event may arrive under another delivery id
            self.bridge.recover()
            self.bridge.flush()
            with sqlite3.connect(inbox) as conn:
                self.assertEqual(conn.execute("SELECT status FROM deliveries").fetchone()[0], "imported")
            self.assertEqual(self.board.get(self.task_id).status, "blocked")
            self.assertIsNotNone(process.poll())
            receipts = [activity for activity in self.remote.activities
                        if "Stop recorded" in activity["content"].get("body", "")]
            self.assertEqual(len(receipts), 1)
            with self.board.conn() as conn:
                self.assertEqual(len(kb.list_tasks(conn)), 1)
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)

    def test_lost_native_resume_receipt_reconciles_fence_without_unblock_replay(self):
        self.bridge.handle_webhook(self.prompt("", "prior-stop", signal="stop"))
        old_stop = self.store.get(ISSUE)["stop_requested_at"]
        self.clock.now += 1
        event = self.remote.session_event("created", ISSUE, "resumed-assignment")
        inbox = self.queue(event, "resume-lost-response")
        unblock = self.board.unblock

        def lost_response(task_id, **request):
            self.assertTrue(unblock(task_id, **request))
            raise OSError("native unblock committed, response lost")

        with patch.object(self.board, "unblock", side_effect=lost_response):
            self.bridge.drain_ingress(inbox)
        self.assertEqual(self.board.get(self.task_id).status, "ready")
        self.assertGreater(self.store.get(ISSUE)["stop_requested_at"], 0)
        self.bridge = self.reopen()
        self.clock.now += 5
        with patch.object(self.board, "unblock", side_effect=AssertionError("committed native effect replayed")):
            self.bridge.drain_ingress(inbox)
        row = self.store.get(ISSUE)
        self.assertEqual(row["stop_requested_at"], 0)
        self.assertGreater(row["resume_fence_at"], old_stop)
        delayed = self.remote.session_event("prompted", ISSUE, "original-session",
                                            signal="stop", activity_id="pre-resume-stop")
        delayed["agentActivity"]["createdAt"] = datetime.fromtimestamp(
            (old_stop + 500) / 1000, timezone.utc).isoformat()
        self.bridge.handle_webhook(delayed)
        self.assertEqual(self.board.get(self.task_id).status, "ready")
        with sqlite3.connect(inbox) as conn:
            self.assertEqual(conn.execute("SELECT status FROM deliveries").fetchone()[0], "imported")
        self.assertEqual(sum("Re-delegated in Linear" in body for body in self.notes()), 1)

    def test_old_schema_binding_stays_fenced_and_visible_without_provenance_inference(self):
        current_time = self.clock.now
        self.clock.now -= 10
        prompt = self.remote.session_event("prompted", ISSUE, "prior-task-session",
            body="Old implementation instructions", activity_id="old-generation-comment")
        stop = self.remote.session_event("prompted", ISSUE, "prior-task-session",
            signal="stop", activity_id="old-generation-stop")
        self.clock.now = current_time
        before = self.board.get(self.task_id)
        with sqlite3.connect(self.store.path) as conn:
            conn.execute("ALTER TABLE work DROP COLUMN linked_at")
        self.bridge = self.reopen()  # exercise real pre-column migration, not just a helper's default
        self.bridge.handle_webhook(prompt)
        self.bridge.handle_webhook(stop)
        self.bridge.recover()
        self.assertEqual(self.notes(), [])
        after = self.board.get(self.task_id)
        self.assertEqual((after.status, after.current_run_id, after.claim_lock),
                         (before.status, before.current_run_id, before.claim_lock))
        self.assertEqual(self.store.get(ISSUE)["linked_at"], 0)
        self.assertEqual(self.store.get(ISSUE)["task_id"], self.task_id)
        from hermes_fleet_linear_plugin import ops
        self.assertTrue(any(item["issue"] == ISSUE and "provenance" in item["why"]
                            for item in ops.stuck(self.store.path, self.home / "inbox.db", "default")))

    def test_unknown_completed_binding_cannot_start_an_assignment_increment(self):
        evidence = "Checkpoint https://example.test/findings/migration-checkpoint"
        with self.board.conn() as conn:
            self.assertTrue(kb.complete_task(conn, self.task_id, result=evidence))
        before = self.board.get(self.task_id)
        with sqlite3.connect(self.store.path) as conn:
            conn.execute("ALTER TABLE work DROP COLUMN linked_at")
        self.bridge = self.reopen()
        self.clock.now += 1
        assignment = self.remote.session_event("created", ISSUE, "unreconciled-assignment")
        self.bridge.handle_webhook(assignment)
        self.bridge.handle_webhook(assignment)
        self.assertEqual(self.store.get(ISSUE)["linked_at"], 0)
        self.assertEqual(self.store.get(ISSUE)["task_id"], self.task_id)
        after = self.board.get(self.task_id)
        self.assertEqual((after.status, after.result, after.workspace_path, after.current_run_id),
                         (before.status, before.result, before.workspace_path, before.current_run_id))
        with self.board.conn() as conn:
            self.assertEqual(len(kb.list_tasks(conn)), 1, "unknown prior binding cannot manufacture a new increment")

    def test_simultaneous_followups_share_one_native_execution(self):
        with self.board.conn() as conn:
            self.assertTrue(kb.claim_task(conn, self.task_id))
        before = self.board.get(self.task_id)
        events = [self.prompt("First remaining instruction", "first"),
                  self.prompt("Second remaining instruction", "second")]
        # Equal source time permits both distinct serialized requests rather than making one stale.
        events[0]["agentActivity"]["createdAt"] = events[1]["agentActivity"]["createdAt"]
        events[0]["createdAt"] = events[1]["createdAt"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as workers:
            list(workers.map(self.bridge.handle_webhook, events))
        text = "\n".join(self.notes())
        self.assertEqual(text.count("First remaining instruction"), 1)
        self.assertEqual(text.count("Second remaining instruction"), 1)
        after = self.board.get(self.task_id)
        self.assertEqual((after.current_run_id, after.claim_lock), (before.current_run_id, before.claim_lock))
        with self.board.conn() as conn:
            self.assertEqual(len(kb.list_tasks(conn)), 1)

    def test_distinct_out_of_order_followups_are_not_lost_to_a_reporting_watermark(self):
        older = self.prompt("First authorized instruction", "older-distinct")
        newer = self.prompt("Second authorized instruction", "newer-distinct")
        self.bridge.handle_webhook(newer)
        watermark = self.store.get(ISSUE)["last_updated_at"]
        self.bridge.handle_webhook(older)
        self.bridge.handle_webhook(older)
        self.bridge.handle_webhook(newer)
        text = "\n".join(self.notes())
        self.assertEqual(text.count("First authorized instruction"), 1)
        self.assertEqual(text.count("Second authorized instruction"), 1)
        self.assertGreaterEqual(self.store.get(ISSUE)["last_updated_at"], watermark)
        with self.board.conn() as conn:
            self.assertEqual(len(kb.list_tasks(conn)), 1)

    def test_stop_and_explicit_resume_fence_prior_instructions(self):
        stale = self.prompt("Instruction before Stop", "before-stop")
        self.bridge.handle_webhook(self.prompt("", "stop-generation", signal="stop"))
        self.bridge.handle_webhook(stale)
        self.assertEqual(self.notes(), [])
        self.clock.now += 1
        assignment = self.remote.session_event("created", ISSUE, "resumed-assignment")
        self.bridge.handle_webhook(assignment)
        self.assertEqual(self.board.get(self.task_id).status, "ready")
        self.bridge.handle_webhook(stale)
        self.assertEqual(sum("Instruction before Stop" in body for body in self.notes()), 0)
        self.clock.now += 1
        fresh = self.remote.session_event("prompted", ISSUE, "resumed-assignment",
                                          body="Freshly authorized instruction", activity_id="after-resume")
        self.bridge.handle_webhook(fresh)
        self.assertEqual(sum("Freshly authorized instruction" in body for body in self.notes()), 1)

    def test_new_native_increment_rejects_prior_task_generation_followup(self):
        stale = self.prompt("Instruction for completed work", "old-task-generation")
        old_task = self.task_id
        with self.board.conn() as conn:
            self.assertTrue(kb.complete_task(conn, old_task,
                result="Verified findings https://example.test/findings/old"))
        self.clock.now += 1
        self.bridge.handle_webhook(self.remote.session_event("created", ISSUE, "new-native-increment"))
        new_task = self.store.get(ISSUE)["task_id"]
        self.assertNotEqual(new_task, old_task)
        self.bridge.handle_webhook(stale)
        with self.board.conn() as conn:
            self.assertFalse(any("Instruction for completed work" in comment.body
                                 for comment in kb.list_comments(conn, new_task)))
            self.assertEqual(len(kb.list_tasks(conn)), 2)
        self.assertEqual(self.board.get(old_task).status, "done")
        self.assertEqual(self.board.get(new_task).status, "ready")

    def test_newer_comment_cannot_hide_an_older_distinct_stop_request(self):
        stop = self.prompt("", "delayed-stop", signal="stop")
        comment = self.prompt("Additional context does not resume work", "newer-comment")
        self.bridge.handle_webhook(comment)
        watermark = self.store.get(ISSUE)["last_updated_at"]
        self.assertEqual(self.board.get(self.task_id).status, "ready")
        self.bridge.handle_webhook(stop)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        recorded_stop = self.store.get(ISSUE)["stop_requested_at"]
        self.assertGreater(recorded_stop, 0)
        self.assertLess(recorded_stop, watermark)
        newer_stop = self.prompt("", "later-stop", signal="stop")
        self.bridge.handle_webhook(newer_stop)
        latest_stop = self.store.get(ISSUE)["stop_requested_at"]
        self.assertGreater(latest_stop, recorded_stop)
        self.bridge.handle_webhook(stop)
        self.assertEqual(self.store.get(ISSUE)["stop_requested_at"], latest_stop)
        with self.board.conn() as conn:
            self.assertIsNone(kb.claim_task(conn, self.task_id))

    def test_equal_time_stop_after_initial_assignment_wins(self):
        stop = self.remote.session_event("prompted", ISSUE, "original-session",
                                         signal="stop", activity_id="same-time-stop")
        self.bridge.handle_webhook(stop)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        self.assertGreater(self.store.get(ISSUE)["stop_requested_at"], 0)
        with self.board.conn() as conn:
            self.assertIsNone(kb.claim_task(conn, self.task_id))
        self.bridge = self.reopen()
        self.bridge.recover()
        self.assertEqual(self.board.get(self.task_id).status, "blocked")

    def test_prompt_and_reporting_restart_do_not_resume_stopped_work(self):
        self.bridge.handle_webhook(self.prompt("", "stop", signal="stop"))
        task = self.board.get(self.task_id)
        self.assertEqual(task.status, "blocked")
        self.bridge.handle_webhook(self.prompt("Resume with this extra context", "context"))
        self.bridge = self.reopen()
        self.bridge.recover()
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        self.assertEqual(sum("extra context" in body for body in self.notes()), 1)
        self.assertIsNone(self.store.get(ISSUE)["pending_resume"])

    def test_new_stop_preserves_unknown_legacy_instruction_delivery(self):
        legacy = json.dumps({"note": "Unknown prior delivery", "session_id": "original-session"})
        self.store.update(ISSUE, pending_resume=legacy)
        self.bridge.handle_webhook(self.prompt("", "new-stop", signal="stop"))
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        self.assertEqual(self.store.get(ISSUE)["pending_resume"], legacy)
        self.bridge = self.reopen()
        self.bridge.recover()
        self.assertEqual(self.store.get(ISSUE)["pending_resume"], legacy)
        self.assertFalse(any("Unknown prior delivery" in body for body in self.notes()))

    def test_reconnect_keeps_healthy_worker_claim_and_process(self):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            with self.board.conn() as conn:
                self.assertTrue(kb.claim_task(conn, self.task_id))
                dispatch._set_worker_pid(conn, self.task_id, process.pid)
            before = self.board.get(self.task_id)
            self.bridge = self.reopen()
            self.bridge.recover()
            self.bridge.handle_webhook(self.prompt("Use the existing checkpoint", "live-followup"))
            after = self.board.get(self.task_id)
            self.assertEqual((after.current_run_id, after.claim_lock), (before.current_run_id, before.claim_lock))
            self.assertIsNone(process.poll())
            self.assertEqual(sum("existing checkpoint" in body for body in self.notes()), 1)
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)

    def test_stop_terminates_proven_native_process_before_explicit_resumption(self):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            with self.board.conn() as conn:
                self.assertTrue(kb.claim_task(conn, self.task_id))
                dispatch._set_worker_pid(conn, self.task_id, process.pid)
            self.bridge.handle_webhook(self.prompt("", "stop-proven", signal="stop"))
            self.assertEqual(process.poll(), -15)
            self.assertEqual(self.board.get(self.task_id).status, "blocked")
            event = self.prompt("Preserve this instruction", "after-stop")
            self.bridge.handle_webhook(event)
            self.bridge.handle_webhook(event)
            self.assertEqual(self.board.get(self.task_id).status, "blocked")
            self.assertTrue(self.board.unblock(self.task_id))  # supported explicit native action after reconciliation
            self.assertEqual(self.board.get(self.task_id).status, "ready")
            self.assertEqual(sum("Preserve this instruction" in body for body in self.notes()), 1)
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)

    def test_unknown_process_identity_remains_fenced_after_restart_and_terminal_sweep(self):
        from hermes_cli.kanban_db_dispatch import UNVERIFIED_WORKER_FINGERPRINT
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            with self.board.conn() as conn:
                self.assertTrue(kb.claim_task(conn, self.task_id))
                dispatch._set_worker_pid(conn, self.task_id, process.pid)
                conn.execute("UPDATE tasks SET worker_started_at=? WHERE id=?",
                             (UNVERIFIED_WORKER_FINGERPRINT, self.task_id))
                conn.execute("UPDATE task_runs SET worker_started_at=? WHERE task_id=?",
                             (UNVERIFIED_WORKER_FINGERPRINT, self.task_id))
            self.bridge.handle_webhook(self.prompt("", "stop-unknown", signal="stop"))
            self.assertIsNone(process.poll(), "an unverified process must not be signalled")
            self.bridge.handle_webhook(self.prompt("Additional context", "unknown-followup"))
            self.bridge = self.reopen()
            self.bridge.recover()
            with patch.object(dispatch, "_process_fingerprint", return_value=None):
                with self.board.conn() as conn:
                    conn.execute("UPDATE task_runs SET ended_at=1 WHERE task_id=?", (self.task_id,))
                    dispatch.reap_terminal_workers(conn)
                with self.assertRaises(LinearError):
                    self.board.unblock(self.task_id)
            self.assertEqual(self.board.get(self.task_id).status, "blocked")
            self.assertIsNone(process.poll())
            process.terminate()
            process.wait(timeout=10)
            self.bridge.recover()
            self.assertEqual(self.board.get(self.task_id).status, "blocked", "reporting cannot infer resumption")
            self.assertTrue(self.board.unblock(self.task_id))
            self.assertEqual(self.board.get(self.task_id).status, "ready")
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)

    def test_native_resumption_refusal_never_creates_a_replacement_executor(self):
        self.bridge.handle_webhook(self.prompt("", "stop-existing", signal="stop"))
        self.clock.now += 1
        assignment = self.remote.session_event("created", ISSUE, "new-assignment")
        with patch.object(self.board, "unblock", return_value=False):
            self.bridge.handle_webhook(assignment)
        self.assertEqual(self.store.get(ISSUE)["task_id"], self.task_id)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")
        with self.board.conn() as conn:
            self.assertEqual(len(kb.list_tasks(conn)), 1)
        self.bridge = self.reopen()
        self.bridge.recover()
        self.assertEqual(self.board.get(self.task_id).status, "blocked")

    def test_legacy_pending_instruction_remains_fenced_and_visible(self):
        from hermes_fleet_linear_plugin import ops
        legacy = json.dumps({"note": "Maybe delivered", "session_id": "original-session"})
        self.store.update(ISSUE, pending_resume=legacy)
        self.bridge.handle_webhook(self.prompt("Additional instruction", "new-context"))
        self.bridge = self.reopen()
        self.bridge.recover()
        self.assertEqual(self.store.get(ISSUE)["pending_resume"], legacy)
        self.assertEqual(self.notes(), [])
        self.assertTrue(any(item["why"] == "legacy continuation requires reconciliation"
                            for item in ops.stuck(self.store.path, self.home / "inbox.db", "default")))

    def test_migrated_binding_cannot_release_or_report_terminal_work(self):
        for state in ("running", "done", "archived"):
            with self.subTest(state=state):
                issue = "migrated-" + state
                self.remote.add_issue(issue, "MIG-" + state)
                self.remote.set_delegate(issue, {"id": SELF})
                self.bridge.handle_webhook(self.remote.session_event("created", issue, "session-" + state))
                self.bridge.flush()
                row = self.store.get(issue)
                task_id = row["task_id"]
                with self.board.conn() as conn:
                    if state == "running":
                        self.assertTrue(kb.claim_task(conn, task_id))
                    elif state == "done":
                        self.assertTrue(kb.complete_task(conn, task_id,
                            summary="Substantive retained findings needing reconciliation", metadata={}))
                    else:
                        self.board.archive(task_id)
                # Exercise the actual additive migration, not a mocked provenance predicate.
                with sqlite3.connect(self.store.path) as conn:
                    conn.execute("ALTER TABLE work DROP COLUMN linked_at")
                self.bridge = self.reopen()
                saved = self.store.get(issue)
                pending = self.store.pending(issue)
                self.remote.set_delegate(issue, {"id": "different-human"})
                self.bridge.recheck(force=True)
                self.bridge.recover()
                self.bridge.release(saved, "reassigned")
                self.bridge.flush()
                self.assertEqual(self.store.get(issue), saved)
                self.assertEqual(self.store.pending(issue), pending)
                self.assertEqual(self.board.get(task_id).status, state)
                self.assertEqual(self.remote.state(issue), "In Progress")
                self.assertFalse(self.store.retired(issue))

    def test_assignment_inbox_reconciles_crash_after_binding_or_activation(self):
        for boundary in ("before-activation", "after-activation", "reporting"):
            with self.subTest(boundary=boundary):
                issue, session = "crash-" + boundary, "session-" + boundary
                self.remote.add_issue(issue, "CRASH-" + boundary)
                self.remote.set_delegate(issue, {"id": SELF})
                event = self.remote.session_event("created", issue, session)
                inbox = self.queue(event, delivery=boundary)
                activate = self.bridge.store.activate_task
                def crash_activate(*args):
                    if boundary == "after-activation": activate(*args)
                    raise RuntimeError("process interrupted at native admission")
                target = "enqueue_many" if boundary == "reporting" else "activate_task"
                with patch.object(self.bridge.store, target, side_effect=(
                        RuntimeError("reporting capture interrupted") if boundary == "reporting" else crash_activate)):
                    self.bridge.drain_ingress(inbox)
                row = self.store.get(issue)
                self.assertIsNotNone(row)
                task_id = row["task_id"]
                self.assertEqual(self.board.get(task_id).status,
                                 "blocked" if boundary == "before-activation" else "ready")
                self.bridge = self.reopen()
                self.clock.now += 5
                self.bridge.drain_ingress(inbox)
                self.bridge.drain_ingress(inbox)
                self.bridge.recover()
                self.bridge.recheck(force=True)
                self.bridge.flush()
                self.assertEqual(self.store.get(issue)["task_id"], task_id)
                self.assertEqual(self.board.get(task_id).status, "ready")
                self.assertEqual(self.remote.state(issue), "In Progress")
                with self.board.conn() as conn:
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks WHERE idempotency_key=?",
                        (f"linear:{issue}:{session}",)).fetchone()[0], 1)

    def test_assignment_retry_does_not_resume_native_stop_after_staging_crash(self):
        issue, session = "stopped-initial", "stopped-session"
        self.remote.add_issue(issue, "STOP-INITIAL")
        self.remote.set_delegate(issue, {"id": SELF})
        event = self.remote.session_event("created", issue, session)
        with patch.object(self.bridge.store, "activate_task", side_effect=RuntimeError("crash")):
            with self.assertRaisesRegex(RuntimeError, "crash"):
                self.bridge.handle_webhook(event)
        task_id = self.store.get(issue)["task_id"]
        self.assertTrue(self.board.unblock(task_id))
        self.assertTrue(self.board.block(task_id, "Explicit native Stop after interrupted admission"))
        self.bridge = self.reopen()
        self.bridge.handle_webhook(event)
        self.bridge.recover()
        self.bridge.flush()
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.assertFalse(self.store.has_start_ack(issue, task_id))

    def test_lost_assignment_receipt_cannot_undo_a_later_native_stop(self):
        self.bridge.handle_webhook(self.prompt("", "before-lost-assignment", signal="stop"))
        self.clock.now += 1
        event = self.remote.session_event("created", ISSUE, "lost-assignment")
        inbox = self.queue(event, "lost-assignment")
        unblock = self.board.unblock
        def lose_receipt(task_id, **request):
            self.assertTrue(unblock(task_id, **request))
            raise OSError("unblock receipt lost")
        with patch.object(self.board, "unblock", side_effect=lose_receipt):
            self.bridge.drain_ingress(inbox)
        with self.board.conn() as conn:
            self.assertTrue(kb.block_task(conn, self.task_id, reason="Later explicit native Stop"))
        self.bridge = self.reopen()
        self.clock.now += 5
        with patch.object(self.board, "unblock", side_effect=AssertionError("old assignment replayed")):
            self.bridge.drain_ingress(inbox)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")

    def test_lost_stop_receipt_cannot_block_a_newer_native_resumption(self):
        event = self.prompt("", "lost-stop", signal="stop")
        inbox = self.queue(event, "lost-stop")
        block = self.board.block
        def lose_receipt(task_id, reason, **request):
            self.assertTrue(block(task_id, reason, **request))
            raise OSError("block receipt lost")
        with patch.object(self.board, "block", side_effect=lose_receipt):
            self.bridge.drain_ingress(inbox)
        self.assertTrue(self.board.unblock(self.task_id))
        before = self.board.get(self.task_id)
        self.bridge = self.reopen()
        self.clock.now += 5
        with patch.object(self.board, "block", side_effect=AssertionError("old Stop replayed")):
            self.bridge.drain_ingress(inbox)
        after = self.board.get(self.task_id)
        self.assertEqual((after.status, after.block_recurrences), (before.status, before.block_recurrences))
        self.assertEqual(after.status, "ready")

    def test_native_stop_after_assignment_comment_fences_first_unblock(self):
        self.bridge.handle_webhook(self.prompt("", "before-comment-race", signal="stop"))
        self.clock.now += 1
        event = self.remote.session_event("created", ISSUE, "comment-stop-race")
        inbox = self.queue(event, "comment-stop-race")
        unblock = self.board.unblock
        def stop_before_unblock(task_id, **request):
            with self.board.conn() as conn:
                self.assertFalse(kb.block_task(conn, task_id, reason="Native Stop while already blocked"))
            return unblock(task_id, **request)
        with patch.object(self.board, "unblock", side_effect=stop_before_unblock):
            self.bridge.drain_ingress(inbox)
        self.bridge = self.reopen()
        self.clock.now += 5
        self.bridge.drain_ingress(inbox)
        self.assertEqual(self.board.get(self.task_id).status, "blocked")

    def test_native_stop_while_initially_blocked_fences_staged_admission_retry(self):
        issue, session = "staged-noop-stop", "staged-noop-session"
        self.remote.add_issue(issue, "STAGED-NOOP")
        self.remote.set_delegate(issue, {"id": SELF})
        event = self.remote.session_event("created", issue, session)
        with patch.object(self.bridge.store, "activate_task", side_effect=RuntimeError("staging crash")):
            with self.assertRaisesRegex(RuntimeError, "staging crash"):
                self.bridge.handle_webhook(event)
        task_id = self.store.get(issue)["task_id"]
        with self.board.conn() as conn:
            self.assertFalse(kb.block_task(conn, task_id, reason="Native Stop before initial activation"))
        self.bridge = self.reopen()
        self.bridge.handle_webhook(event)
        self.assertEqual(self.board.get(task_id).status, "blocked")
        self.assertEqual(self.store.get(issue)["task_id"], task_id)
        self.assertFalse(self.store.has_start_ack(issue, task_id))

    def test_assignment_creation_key_cannot_adopt_independently_created_work(self):
        issue, session = "manual-key-collision", "manual-key-session"
        self.remote.add_issue(issue, "MANUAL-KEY")
        self.remote.set_delegate(issue, {"id": SELF})
        task = self.board.create(title="Independent native work", assignee="default", created_by="default",
                                 idempotency_key=f"linear:{issue}:{session}")
        with self.board.conn() as conn:
            self.assertTrue(kb.claim_task(conn, task.id))
        before = self.board.get(task.id)
        inbox = self.queue(self.remote.session_event("created", issue, session), "manual-key")
        self.bridge.drain_ingress(inbox)
        self.assertIsNone(self.store.get(issue))
        after = self.board.get(task.id)
        self.assertEqual((after.status, after.current_run_id, after.claim_lock),
                         (before.status, before.current_run_id, before.claim_lock))


if __name__ == "__main__":
    unittest.main()
