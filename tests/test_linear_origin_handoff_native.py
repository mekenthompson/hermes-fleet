"""Owner side of a takeover against the pinned native owner, Kanban and receipts.

``handoff.hand_off`` runs in the profile that owns the work. It refuses before stopping
anything when the proof cannot hold, stops with one guarded native Stop, proves the stopped
work ended with the existing cleanup inspection, and resumes what it stopped when the proof
fails. A stop made outside this flow (a raw CLI block) is only proven, never changed.
"""
import asyncio
import contextlib
import io
import json
import os
import sqlite3
import subprocess
import unittest
from unittest.mock import patch

import test_linear_origin_native as base
from hermes_fleet_linear_plugin import cleanup, handoff

URL = "https://linear.example/issue/ABC-1"
META = {"id": "issue-one", "identifier": "ABC-1", "url": URL}
MARKER = "Linear issue ABC-1: " + URL
REQUEST = "6f2c9e4a-3b1d-4c7e-9a52-8d0e1f7b4c63"
R2 = "0b8f5a7e-3c1d-4e2f-9a6b-7c8d9e0f1a2b"
R3 = "3d4e5f6a-7b8c-4d9e-8f0a-1b2c3d4e5f6a"


def load_tests(loader, tests, pattern):
    # Only this module's scenarios; the base fixture's own tests run from their module.
    return loader.loadTestsFromNames(sorted(
        f"{__name__}.Handoff.{name}" for name in dir(Handoff) if name.startswith("test_handoff_")))


class Handoff(base.OriginScenarios):
    async def running_issue(self, *, settle=True, body=MARKER + "\nFinish the remaining checks", issue_id="issue-one"):
        """A claimed, admitted native task bound to issue-one, still running; optionally its
        attempt has already wound down natively (as after the Stop reached it)."""
        from hermes_cli import kanban_db as kb
        from hermes_state_runtime import (register_worker_execution, adopt_worker_execution,
            mutate_worker_execution, settle_session_input)
        from hermes_fleet_linear_plugin.store import Store
        from gateway import session_ancillary
        await self.kanban()
        if settle:
            executor = "admission-worker:" + self.admission
            args = dict(epoch=self.authority.epoch, execution_id=executor, session_id=self.sid,
                        generation=self.row["generation"])
            register_worker_execution(self.authority.db, **args, kind="compute", adoption_secret="fixture-handoff")
            adopt_worker_execution(self.authority.db, **args, adoption_secret="fixture-handoff")
            mutate_worker_execution(self.authority.db, **args, sequence=1, operation="execution.finish", payload={})
            settle_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=self.admission,
                                 generation=self.row["generation"], outcome="interrupted")
        with kb.write_txn(self.board):
            self.board.execute("UPDATE task_events SET payload=? WHERE kind='worker_bound' AND task_id=?",
                (json.dumps({"pid": 99999998, "started_at": self.process_witness(1),
                             "claim_lock": self.work_context["claim_lock"]}), self.task_id))
            kb._append_event(self.board, self.task_id, "spawned", {"pid": 99999999, "started_at": self.process_witness(2)},
                             run_id=self.work_context["run_id"])
            self.board.execute("UPDATE task_runs SET worker_pid=99999999,worker_started_at=? WHERE id=?",
                               (self.process_witness(2), self.work_context["run_id"]))
            self.board.execute("UPDATE tasks SET body=? WHERE id=?", (body, self.task_id))
        self.assertEqual(kb.get_task(self.board, self.task_id).status, "running")
        self.linear_state = self.home / "linear" / "state.db"
        self.linear_store = Store(self.linear_state)
        self.assertTrue(self.linear_store.bind_native(issue_id, self.task_id, "default", "default", at=1))
        getter = patch.object(session_ancillary, "process_stop_pending",
            lambda db, sid: db.get_meta("fixture.pending.stop." + sid), create=True)
        getter.start()
        self.addCleanup(getter.stop)

    def snapshot(self):
        return {table: [tuple(row) for row in self.board.execute("SELECT * FROM " + table)]
                for table in ("tasks", "task_runs", "task_events")}

    def status(self):
        from hermes_cli import kanban_db as kb
        return kb.get_task(self.board, self.task_id).status

    async def hand_off(self, *, issue="issue-one", metadata=META, request_id=REQUEST, wait_seconds=0, cancel=False,
                       to="Nova", **extra):
        from agent import runtime_session_store
        from hermes_cli import kanban_process_stop
        def delivered(conn, task_id, event_id):
            self.delivered.append((task_id, event_id))  # The owner's Stop reaches its worker.
        self.delivered = getattr(self, "delivered", [])
        with patch.object(runtime_session_store, "_worker_process", False), \
                patch.object(kanban_process_stop, "request_native_stop", delivered):
            if cancel:
                return await asyncio.to_thread(handoff.cancel, self.linear_state, issue, "default", metadata,
                                               to=to, request_id=request_id, owner_name="Atlas", wait_seconds=0)
            return await asyncio.to_thread(handoff.hand_off, self.linear_state, issue, "default", self.settings,
                                           metadata, to=to, request_id=request_id, owner_name="Atlas",
                                           wait_seconds=wait_seconds, sleep=lambda seconds: None, **extra)

    async def inspected(self, issue, metadata, stop_id):
        from agent import runtime_session_store
        with patch.object(runtime_session_store, "_worker_process", False):
            return await asyncio.to_thread(cleanup.inspect, self.linear_state, issue, "default", stop_id,
                                           self.settings, metadata)

    # -- (a) a coordinated stop is proven and handed over -----------------------------------
    async def test_handoff_coordinated_stop_is_proven_and_handed_over_once(self):
        from hermes_cli import kanban_db as kb
        await self.running_issue()
        result = await self.hand_off()
        self.assertTrue(result["handed_off"], result)
        self.assertEqual(result["state"], "handed_off")
        self.assertTrue(result["stopped_here"])
        self.assertFalse(result["permission_granted"])
        self.assertEqual(result["issue_id"], "issue-one")
        self.assertIn("Atlas's work stopped and is confirmed finished; Nova can continue it.", result["message"])
        self.assertEqual(self.status(), "blocked")
        stop = kb.list_events(self.board, self.task_id)[-1]
        self.assertEqual((stop.kind, stop.id), ("blocked", result["stop_event_id"]))
        self.assertEqual(stop.payload["reason"], handoff.handoff_reason("Nova", REQUEST))
        self.assertEqual(stop.payload["kind"], "needs_input")
        self.assertEqual(self.delivered, [(self.task_id, stop.id)])
        for private in ("CLAIM_PRIVATE_SENTINEL", "fixture-handoff", str(self.board_path)):
            self.assertNotIn(private, json.dumps(result))
        # A retry after a lost reply proves the same Stop and never stops again.
        before = self.snapshot()
        again = await self.hand_off()
        self.assertTrue(again["handed_off"], again)
        self.assertEqual(again["stop_event_id"], result["stop_event_id"])
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(len(self.delivered), 1)

    async def test_handoff_cancel_resumes_only_its_own_pause(self):
        from hermes_cli import kanban_db as kb
        await self.running_issue()
        self.assertTrue((await self.hand_off())["handed_off"])
        other = await self.hand_off(request_id="0b8f5a7e-3c1d-4e2f-9a6b-7c8d9e0f1a2b", cancel=True)
        self.assertEqual(other["state"], "unchanged")
        self.assertEqual(self.status(), "blocked")
        undone = await self.hand_off(cancel=True)
        self.assertEqual(undone["state"], "resumed", undone)
        self.assertEqual(self.status(), "ready")
        resumed = kb.list_events(self.board, self.task_id)[-1]
        self.assertEqual(resumed.kind, "unblocked")
        self.assertEqual(resumed.payload["request_id"], handoff.resume_request_id(REQUEST))
        self.assertEqual((await self.hand_off(cancel=True))["state"], "unchanged")  # Nothing left to undo.

    # -- (b) the proof fails: the owner is left running ---------------------------------------
    async def test_handoff_failed_proof_resumes_the_owner_it_stopped(self):
        from hermes_cli import kanban_db as kb
        await self.running_issue(settle=False)  # The attempt's native admission never settles.
        result = await self.hand_off()
        self.assertFalse(result["handed_off"])
        self.assertEqual(result["state"], "resumed", result)
        self.assertEqual(result["reason"], "proof_timeout")
        self.assertIn("couldn't be confirmed safe", result["message"])
        self.assertIn("Atlas's work was resumed and nothing was handed over", result["message"])
        self.assertNotIn("native_", result["message"])
        self.assertEqual(self.status(), "ready")  # Runnable again; never left stopped with nobody on it.
        kinds = [event.kind for event in kb.list_events(self.board, self.task_id)]
        self.assertEqual(kinds[-2:], ["blocked", "unblocked"])

    async def test_handoff_refuses_before_stopping_when_proof_cannot_hold(self):
        await self.running_issue(body="Linear issue OTHER-2: https://linear.example/issue/OTHER-2")
        before = self.snapshot()
        result = await self.hand_off()
        self.assertEqual((result["handed_off"], result["state"]), (False, "unchanged"))
        self.assertEqual(result["reason"], "native_task_issue_scope_conflict")
        self.assertIn("Nothing was changed", result["message"])
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.delivered, [])
        # No reporting link here: nothing to hand over, nothing touched.
        with self.linear_store._tx() as db:
            db.execute("DELETE FROM metadata WHERE key='native-binding:issue-one'")
            db.execute("DELETE FROM work WHERE issue_id='issue-one'")
        refused = await self.hand_off()
        self.assertEqual((refused["state"], refused["reason"]), ("unchanged", "not_bound_here"))
        self.assertEqual(self.snapshot(), before)
        # An incomplete issue reference never selects or stops anything.
        incomplete = await self.hand_off(metadata={"id": "issue-one"})
        self.assertEqual(incomplete["state"], "unchanged")
        self.assertEqual(self.snapshot(), before)

    async def test_handoff_started_open_tasks_are_left_running(self):
        from hermes_cli import kanban_db as kb
        await self.running_issue()
        kb.create_task(self.board, title="sub-step", assignee="default", created_by="fixture-agent",
                       creator_task_id=self.task_id)
        before = self.snapshot()
        result = await self.hand_off()
        self.assertEqual((result["state"], result["reason"]), ("unchanged", "started_other_tasks"))
        self.assertEqual(self.snapshot(), before)

    # -- a stop made outside this flow (raw CLI) ----------------------------------------------
    async def test_handoff_raw_cli_stop_is_proven_without_another_stop(self):
        from hermes_cli import kanban_db as kb
        await self.stopped_issue()  # Blocked directly with kanban block, like an operator CLI call.
        kb.edit_task(self.board, self.task_id, body=MARKER)
        stop_id = self.stop_id
        before = self.snapshot()
        result = await self.hand_off()
        self.assertTrue(result["handed_off"], result)
        self.assertFalse(result["stopped_here"])
        self.assertEqual(result["stop_event_id"], stop_id)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.delivered, [])

    async def test_handoff_raw_cli_stop_that_cannot_be_proven_is_left_as_it_was(self):
        from hermes_cli import kanban_db as kb
        await self.stopped_issue()
        kb.edit_task(self.board, self.task_id, body=MARKER)
        self.authority.db.set_meta("fixture.pending.stop." + self.sid, "pending")  # Native Stop still in flight.
        before = self.snapshot()
        result = await self.hand_off()
        self.assertEqual((result["handed_off"], result["state"]), (False, "unchanged"))
        self.assertIn("left as it was", result["message"])
        self.assertEqual(self.status(), "blocked")
        self.assertEqual(self.snapshot(), before)

    # -- identifier vs UUID selectors ---------------------------------------------------------
    async def test_handoff_identifier_and_id_select_the_same_work_and_missing_metadata_is_named(self):
        from hermes_cli import kanban_db as kb
        await self.stopped_issue()
        kb.edit_task(self.board, self.task_id, body=MARKER)
        stop_id = self.stop_id
        by_id = await self.inspected("issue-one", META, stop_id)
        by_key = await self.inspected("ABC-1", META, stop_id)
        self.assertTrue(by_id["quiescence_proven"], by_id)
        self.assertTrue(by_key["quiescence_proven"], by_key)
        self.assertEqual(by_key["task_id"], by_id["task_id"])
        self.assertEqual(by_key["issue_id"], "issue-one")
        # Without the issue's link a bridge-written marker is unproved, never a "scope conflict".
        for selector in ("issue-one", "ABC-1"):
            with self.subTest(selector=selector):
                held = await self.inspected(selector, None, stop_id)
                self.assertFalse(held["quiescence_proven"])
                self.assertEqual(held["reason"], "issue_metadata_required")
                self.assertIn("--issue-json", held["needed"])
        # A real conflict with complete metadata is still a conflict.
        kb.edit_task(self.board, self.task_id, body=MARKER + "\nLinear issue OTHER-2: https://linear.example/issue/OTHER-2")
        self.assertEqual((await self.inspected("ABC-1", META, stop_id))["reason"], "native_task_issue_scope_conflict")
        kb.edit_task(self.board, self.task_id, body=MARKER)
        # A key that names a different issue than its metadata is refused before any read.
        wrong = await self.inspected("XYZ-9", META, stop_id)
        self.assertEqual(wrong["reason"], "invalid_issue_discovery_metadata")
        result = await self.hand_off(issue="ABC-1")
        self.assertTrue(result["handed_off"], result)
        self.assertEqual(result["issue_id"], "issue-one")

    async def test_handoff_owner_cli_command(self):
        import argparse
        from agent import runtime_session_store
        from hermes_cli import kanban_process_stop
        from hermes_fleet_linear_plugin import ops
        await self.running_issue()
        parser = argparse.ArgumentParser()
        ops.setup(parser)
        args = parser.parse_args(["hand-off", "ABC-1", "--issue-json", json.dumps(META), "--to", "Nova",
                                  "--from-name", "Atlas", "--request-id", REQUEST, "--wait-seconds", "5"])
        with patch.object(runtime_session_store, "_worker_process", False), \
                patch.object(kanban_process_stop, "request_native_stop", lambda *a: None), \
                patch("builtins.print") as printed:
            rc = await asyncio.to_thread(ops.run, args, self.linear_state, self.home / "unused", "default",
                                         origin_settings=self.settings)
        reply = json.loads(printed.call_args.args[0])
        self.assertEqual(rc, 0, reply)
        self.assertTrue(reply["handed_off"])
        self.assertIn("Atlas's work stopped", reply["message"])

    # -- blocker 2: a failure after the Stop is rechecked and resumed, never "unchanged" -------
    async def test_handoff_unreadable_history_after_the_stop_resumes_never_unchanged(self):
        from hermes_fleet_linear_plugin.bridge import Kanban
        await self.running_issue()
        armed, real_block, real_history = [], handoff._block, Kanban.history
        def block(*args, **kwargs):
            changed = real_block(*args, **kwargs)
            armed.append(True)
            return changed
        def history(adapter, task_id, after):
            if armed:
                armed.clear()
                raise sqlite3.OperationalError("database is locked")  # The read right after the Stop.
            return real_history(adapter, task_id, after)
        with patch.object(handoff, "_block", block), patch.object(Kanban, "history", history):
            result = await self.hand_off()
        self.assertFalse(result["handed_off"])
        self.assertEqual(result["state"], "resumed", result)
        self.assertIn("couldn't be checked after it was asked to stop", result["message"])
        self.assertIn("Atlas's work was resumed", result["message"])
        self.assertEqual(self.status(), "ready")

    # -- should-fix 6: our own Stop in TRIAGE is not "someone else acted" ------------------------
    async def test_handoff_own_stop_left_in_triage_is_reported_for_an_operator(self):
        from hermes_cli import kanban_db as kb
        await self.running_issue(settle=False)  # The proof never holds, so the Stop must be undone.
        real_block = handoff._block
        def block(adapter, task, reason, expected):
            changed = real_block(adapter, task, reason, expected)
            # As an older native's recurrence fence would leave it (this runs on the owner's thread).
            with adapter.conn() as conn, kb.write_txn(conn):
                conn.execute("UPDATE tasks SET status='triage' WHERE id=?", (self.task_id,))
            return changed
        with patch.object(handoff, "_block", block):
            result = await self.hand_off()
        self.assertEqual(self.status(), "triage")
        self.assertEqual(result["state"], "stopped", result)
        self.assertIn("an operator needs to resume it", result["message"])
        self.assertNotIn("Someone resumed", result["message"])

    # -- should-fix 7: never prove another agent's handover pause for a second agent -----------
    async def test_handoff_pause_for_another_agent_is_never_proven_for_this_one(self):
        from hermes_cli import kanban_db as kb
        await self.running_issue()
        vega = "0b8f5a7e-3c1d-4e2f-9a6b-7c8d9e0f1a2b"
        self.assertTrue(kb.block_task(self.board, self.task_id, kind="needs_input",
                                      reason=handoff.handoff_reason("Vega", vega)))
        before = self.snapshot()
        result = await self.hand_off()
        self.assertEqual((result["handed_off"], result["state"], result["reason"]),
                         (False, "unchanged", "handover_in_progress"))
        self.assertIn("already being handed over to another agent", result["message"])
        self.assertEqual(self.snapshot(), before)

    async def test_handoff_busy_issue_lock_changes_nothing(self):
        await self.running_issue()
        before = self.snapshot()
        with patch.object(handoff, "HANDOFF_LOCK_SECONDS", 0), \
                handoff.issue_lock(handoff._handoff_dir(self.linear_state), "issue-one", 0) as held:
            self.assertTrue(held)
            result = await self.hand_off()
        self.assertEqual((result["state"], result["reason"]), ("unchanged", "handover_in_progress"))
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.delivered, [])

    # -- calling a request off: before it ran, and while its proof was still running ------------
    async def test_handoff_called_off_request_never_stops_anything(self):
        await self.running_issue()
        before = self.snapshot()
        self.assertEqual((await self.hand_off(cancel=True))["state"], "unchanged")
        result = await self.hand_off()  # A late or repeated hand-off for the called-off request.
        self.assertEqual((result["handed_off"], result["state"]), (False, "unchanged"))
        self.assertIn("called off earlier", result["message"])
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.delivered, [])

    async def test_handoff_called_off_while_proving_undoes_its_own_stop(self):
        await self.running_issue()
        calls = []
        def inspect(*args, **kwargs):
            calls.append(args)
            if len(calls) == 2:  # The proof after the Stop: the requester gave up waiting and called it off.
                handoff._call_off(self.linear_state, REQUEST)
            return cleanup.inspect(*args, **kwargs)
        result = await self.hand_off(inspect_cleanup=inspect)
        self.assertFalse(result["handed_off"])
        self.assertEqual(result["state"], "resumed", result)
        self.assertIn("called off before it finished", result["message"])
        self.assertEqual(self.status(), "ready")

    # -- review r2 B: only the request holding a pause may resume it ---------------------------
    async def test_handoff_an_older_request_never_resumes_a_pause_a_later_request_handed_over(self):
        await self.running_issue()
        first = await self.hand_off()  # R1 stops and proves; its requester never finished (no call-off).
        self.assertTrue(first["handed_off"], first)
        later = await self.hand_off(request_id=R2)  # R2 proves R1's pause: Nova continues under R2.
        self.assertTrue(later["handed_off"], later)
        self.assertFalse(later["stopped_here"])
        self.assertEqual(later["stop_event_id"], first["stop_event_id"])
        self.assertEqual(len(self.delivered), 1)  # Never stopped twice.
        before = self.snapshot()
        late_cancel = await self.hand_off(cancel=True)  # R1's requester comes back and calls R1 off.
        self.assertEqual(late_cancel["state"], "unchanged", late_cancel)
        self.assertIn("different handover request", late_cancel["message"])
        retry = await self.hand_off()  # And retries R1 as told after takeover_uncertain.
        self.assertEqual((retry["handed_off"], retry["state"]), (False, "unchanged"), retry)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.status(), "blocked")  # Still handed over to Nova: never both running.
        # R2 holds the pause: R2 calling off (its start failed) resumes it, so nobody is left stopped.
        undone = await self.hand_off(request_id=R2, cancel=True)
        self.assertEqual(undone["state"], "resumed", undone)
        self.assertEqual(self.status(), "ready")

    async def test_handoff_a_called_off_pause_is_finished_before_a_new_request_proves_it(self):
        # The reviewer's sequence: R1 stops and proves; R1's call-off is recorded but its cancel
        # hit a busy lock. R2 must not prove that pause (R1's retry would then resume it beside
        # Nova); it finishes R1's call-off instead, and a fresh request takes over cleanly.
        await self.running_issue()
        self.assertTrue((await self.hand_off())["handed_off"])
        handoff._call_off(self.linear_state, REQUEST)
        r2 = await self.hand_off(request_id=R2)
        self.assertEqual((r2["handed_off"], r2["state"], r2["reason"]), (False, "resumed", "earlier_handover_called_off"))
        self.assertIn("Try again", r2["message"])
        self.assertEqual(self.status(), "ready")
        retry = await self.hand_off()  # R1 retried: never stops, never resumes anything.
        self.assertEqual((retry["handed_off"], retry["state"]), (False, "unchanged"), retry)
        self.assertEqual(self.status(), "ready")
        r3 = await self.hand_off(request_id=R3)
        self.assertTrue(r3["handed_off"], r3)
        self.assertTrue(r3["stopped_here"])
        self.assertEqual((await self.hand_off(cancel=True))["state"], "unchanged")  # R1 can't touch R3's pause.
        self.assertEqual(self.status(), "blocked")

    # -- review r2 E: budgets and call-off records ----------------------------------------------
    async def test_handoff_waits_only_briefly_for_a_busy_issue_lock(self):
        import time
        await self.running_issue()
        with patch.object(handoff, "HANDOFF_LOCK_SECONDS", 0.5), \
                handoff.issue_lock(handoff._handoff_dir(self.linear_state), "issue-one", 0):
            started = time.monotonic()
            result = await self.hand_off(wait_seconds=30)
            elapsed = time.monotonic() - started
            self.assertEqual(result["reason"], "handover_in_progress")
            self.assertLess(elapsed, 5)  # Not the wait plus slack: the requester's timeout always outlasts it.
            handoff._call_off(self.linear_state, REQUEST)
            busy = await self.hand_off(wait_seconds=30)
        self.assertEqual(busy["state"], "stopped")
        self.assertIn("puts Atlas's work back by itself", busy["message"])

    async def test_handoff_old_call_off_records_are_removed(self):
        await self.running_issue()
        directory = handoff._handoff_dir(self.linear_state)
        handoff._call_off(self.linear_state, R2)
        stale = handoff._called_off_path(self.linear_state, R2)
        past = __import__("time").time() - handoff.CALLED_OFF_TTL_SECONDS - 60
        os.utime(stale, (past, past))
        self.assertEqual((await self.hand_off(cancel=True))["state"], "unchanged")
        self.assertFalse(stale.exists())
        self.assertTrue(handoff._called_off_path(self.linear_state, REQUEST).exists())
        self.assertEqual(sorted(path.name for path in directory.glob("*.cancelled")),
                         [handoff._called_off_path(self.linear_state, REQUEST).name])

    # -- the requester and the real owner hand-off together -------------------------------------
    async def requester(self):
        """Nova, a second agent with its own Kanban home and Linear state, reaching Atlas (this
        fixture's owner) only through the owner command. Here that command runs Atlas's real
        ``linear hand-off`` CLI entry in-process, against Atlas's own native board and receipts."""
        import argparse
        from agent import runtime_session_store
        from hermes_cli import kanban_process_stop
        from hermes_fleet_linear_plugin import chat, ops
        from hermes_fleet_linear_plugin.bridge import Bridge, Kanban
        from hermes_fleet_linear_plugin.store import Store
        from hermes_fleet_linear_plugin.transport import NativeContext
        from test_linear_chat_entrust_scenarios import ChatAuthority, now_iso
        from test_linear_specialist_authorization import ISSUE
        self.ISSUE = ISSUE
        issue_meta = {"id": ISSUE, "identifier": "OPS-1", "url": "https://linear.example/issue/OPS-1"}
        await self.running_issue(body="Linear issue OPS-1: https://linear.example/issue/OPS-1\nFinish the checks",
                                 issue_id=ISSUE)
        self.nova_board = self.home / "nova-board"
        self.nova_kanban = patch.dict(os.environ, {"HERMES_KANBAN_HOME": str(self.nova_board)})
        self.authority_linear = ChatAuthority()
        issue = self.authority_linear.issues[ISSUE]
        issue.update(delegate={"id": "atlas-app", "name": "Atlas"}, updatedAt=now_iso(),
                     state={"id": "state-progress", "name": "In Progress", "type": "started"})
        command = ["host-bridge", "profile", "atlas", "--", "hermes", "--profile", "default"]
        with self.nova_kanban:
            board = Kanban(profile="default", profile_home=self.home)
            self.assertTrue(board.guarded_controls)
            self.nova = Bridge(Store(self.home / "nova" / "linear.db"), self.authority_linear.api(scope=None), board,
                               profile="default", settings={"takeover": {"name": "Nova", "agents": [
                                   {"name": "Atlas", "delegate_id": "atlas-app", "command": command, "wait_seconds": 5}]}})
            self.nova.profile_home = self.home
        self.nova_board_adapter = board
        self.nova_context = NativeContext(home=str(self.home), profile="default", platform="slack", source="slack",
                                          session_key="slack:chat-1:thread-1", session_id="chat-session",
                                          user_id="human-1", chat_id="chat-1", chat_type="dm", thread_id="thread-1",
                                          scope_id="workspace-1")
        parser = argparse.ArgumentParser()
        ops.setup(parser)
        self.owner_calls, self.lose_reply = [], False
        def owner_cli(argv, **options):
            self.assertEqual(argv[:len(command)], command)
            self.assertNotIn("shell", options)
            args = parser.parse_args(argv[len(command) + 1:])
            self.assertEqual(json.loads(args.issue_json), issue_meta)
            self.owner_calls.append(argv[len(command):])
            out = io.StringIO()
            with patch.dict(os.environ, {}), contextlib.redirect_stdout(out):
                os.environ.pop("HERMES_KANBAN_HOME", None)  # Atlas's own home and board.
                rc = ops.run(args, self.linear_state, self.home / "unused", "default", origin_settings=self.settings)
            if self.lose_reply and not args.cancel:
                return subprocess.CompletedProcess(argv, 255, "Connection to atlas closed by remote host.\n", "")
            return subprocess.CompletedProcess(argv, rc, "1Password: applied 3 secrets\n" + out.getvalue(), "")
        self.delivered = []
        def take_over(request_id=REQUEST):
            with self.nova_kanban, patch.object(subprocess, "run", owner_cli), \
                    patch.object(runtime_session_store, "_worker_process", False), \
                    patch.object(kanban_process_stop, "request_native_stop",
                                 lambda conn, task_id, event_id: self.delivered.append((task_id, event_id))):
                return json.loads(chat.handle(self.nova, {"action": "take_over", "issue": ISSUE, "id": request_id},
                                              self.nova_context))
        self.take_over = take_over

    def nova_tasks(self):
        with self.nova_kanban, self.nova_board_adapter.conn() as conn:
            return [dict(row) for row in conn.execute("SELECT id, status, body FROM tasks")]

    def owner_events(self):
        from hermes_cli import kanban_db as kb
        return kb.list_events(self.board, self.task_id)

    async def test_handoff_with_requester_real_owner_stops_proves_and_nova_runs_one_task(self):
        await self.requester()
        reply = await asyncio.to_thread(self.take_over)
        self.assertTrue(reply["ok"], reply)
        self.assertIn("Took over OPS-1 from Atlas", reply["message"])
        self.assertEqual(reply["handover"], {"from": "Atlas", "state": "handed_off", "stopped_here": True})
        self.assertEqual([call[:2] for call in self.owner_calls], [["linear", "hand-off"]])
        # Atlas: stopped once by the guarded handover Stop, and still paused (nothing resumed it).
        self.assertEqual(self.status(), "blocked")
        stop = [event for event in self.owner_events() if event.kind == "blocked"][-1]
        self.assertEqual(stop.payload["reason"], handoff.handoff_reason("Nova", REQUEST))
        self.assertEqual(self.delivered, [(self.task_id, stop.id)])
        # Nova: exactly one task, runnable, saying where to continue from; Nova replaces Atlas as delegate.
        tasks = self.nova_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertIn(tasks[0]["status"], ("ready", "running"))
        self.assertIn("Atlas worked on this issue before and stopped", tasks[0]["body"])
        with self.nova_kanban:
            self.nova.flush()
        self.assertEqual(self.authority_linear.issues[self.ISSUE]["delegate"]["id"], self.nova.api.viewer_id())

    async def test_handoff_with_requester_failure_after_proof_resumes_the_real_owner(self):
        from datetime import datetime, timedelta, timezone
        from hermes_fleet_linear_plugin.bridge import Kanban
        from test_linear_specialist_authorization import USER
        await self.requester()
        real = Kanban.create
        def create(adapter, *args, **kwargs):
            task = real(adapter, *args, **kwargs)
            # A person moves the issue between Atlas's proof and Nova's admission: the person wins.
            issue = self.authority_linear.issues[self.ISSUE]
            moved = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat().replace("+00:00", "Z")
            issue["updatedAt"] = moved
            self.authority_linear.history.setdefault(self.ISSUE, []).append({
                "createdAt": moved, "actorId": USER, "fromStateId": "state-progress", "toStateId": "state-todo",
                "fromAssigneeId": None, "toAssigneeId": None, "fromDelegate": None, "toDelegate": None})
            return task
        with patch.object(Kanban, "create", create):
            reply = await asyncio.to_thread(self.take_over)
        self.assertFalse(reply["ok"], reply)
        self.assertIn("Atlas's work was resumed and nothing was handed over", reply["message"])
        self.assertEqual([call[-1] == "--cancel" for call in self.owner_calls], [False, True])
        # Atlas runs again; Nova runs nothing. Never stopped with nobody running.
        self.assertEqual(self.status(), "ready")
        self.assertEqual([event.kind for event in self.owner_events()][-2:], ["blocked", "unblocked"])
        self.assertTrue(all(task["status"] not in ("ready", "running") for task in self.nova_tasks()))

    async def test_handoff_with_requester_lost_owner_reply_is_called_off_and_the_real_owner_resumes(self):
        await self.requester()
        self.lose_reply = True  # Atlas stopped and proved it, but its reply never reached Nova.
        reply = await asyncio.to_thread(self.take_over)
        self.assertFalse(reply["ok"], reply)
        self.assertEqual(reply["code"], "takeover_refused")
        self.assertIn("didn't report back", reply["message"])
        self.assertIn("Atlas's work was resumed and nothing was handed over", reply["message"])
        self.assertEqual(self.status(), "ready")
        self.assertEqual(self.nova_tasks(), [])
        # Repeating the same request finishes calling it off: Atlas is never stopped again.
        self.lose_reply = False
        again = await asyncio.to_thread(self.take_over)
        self.assertFalse(again["ok"])
        self.assertIn("called off earlier", again["message"])
        self.assertEqual(self.status(), "ready")
        self.assertEqual(len([event for event in self.owner_events() if event.kind == "blocked"]), 1)


    async def test_handoff_with_requester_failure_after_admission_keeps_the_real_owner_paused(self):
        # Review r2 A against the real owner: Nova's task was admitted, then its chat subscription
        # failed. Atlas must stay paused (never both running); Nova runs the one task.
        from hermes_fleet_linear_plugin.bridge import Kanban
        await self.requester()
        def boom(*args, **kwargs):
            raise OSError("notify db locked")
        with patch.object(Kanban, "subscribe_origin", boom):
            reply = await asyncio.to_thread(self.take_over)
        self.assertTrue(reply["ok"], reply)
        self.assertEqual([call[-1] == "--cancel" for call in self.owner_calls], [False])
        self.assertEqual(self.status(), "blocked")
        (task,) = self.nova_tasks()
        self.assertIn(task["status"], ("ready", "running"))

    async def test_handoff_with_requester_running_task_never_reaches_the_real_owner_again(self):
        # Review r2 B against the real owner: after the takeover, retries of the same or another
        # request (Linear still shows Atlas until routing flushes) never reach Atlas.
        await self.requester()
        self.assertTrue((await asyncio.to_thread(self.take_over))["ok"])
        calls = list(self.owner_calls)
        for request in (REQUEST, R2):
            reply = await asyncio.to_thread(self.take_over, request)
            self.assertTrue(reply["ok"], reply)
        self.assertEqual(self.owner_calls, calls)
        self.assertEqual(self.status(), "blocked")
        self.assertEqual(len(self.nova_tasks()), 1)


if __name__ == "__main__":
    unittest.main()
