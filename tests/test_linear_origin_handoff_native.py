"""Owner side of a takeover against the pinned native owner, Kanban and receipts.

``handoff.hand_off`` runs in the profile that owns the work. It refuses before stopping
anything when the proof cannot hold, stops with one guarded native Stop, proves the stopped
work ended with the existing cleanup inspection, and resumes what it stopped when the proof
fails. A stop made outside this flow (a raw CLI block) is only proven, never changed.
"""
import asyncio
import json
import os
import unittest
from unittest.mock import patch

import test_linear_origin_native as base
from hermes_fleet_linear_plugin import cleanup, handoff

URL = "https://linear.example/issue/ABC-1"
META = {"id": "issue-one", "identifier": "ABC-1", "url": URL}
MARKER = "Linear issue ABC-1: " + URL
REQUEST = "6f2c9e4a-3b1d-4c7e-9a52-8d0e1f7b4c63"


def load_tests(loader, tests, pattern):
    # Only this module's scenarios; the base fixture's own tests run from their module.
    return loader.loadTestsFromNames(sorted(
        f"{__name__}.Handoff.{name}" for name in dir(Handoff) if name.startswith("test_handoff_")))


class Handoff(base.OriginScenarios):
    async def running_issue(self, *, settle=True, body=MARKER + "\nFinish the remaining checks"):
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
        self.assertTrue(self.linear_store.bind_native("issue-one", self.task_id, "default", "default", at=1))
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

    async def hand_off(self, *, issue="issue-one", metadata=META, request_id=REQUEST, wait_seconds=0, cancel=False):
        from agent import runtime_session_store
        from hermes_cli import kanban_process_stop
        def delivered(conn, task_id, event_id):
            self.delivered.append((task_id, event_id))  # The owner's Stop reaches its worker.
        self.delivered = getattr(self, "delivered", [])
        with patch.object(runtime_session_store, "_worker_process", False), \
                patch.object(kanban_process_stop, "request_native_stop", delivered):
            if cancel:
                return await asyncio.to_thread(handoff.cancel, self.linear_state, issue, "default", metadata,
                                               to="Nova", request_id=request_id, owner_name="Atlas")
            return await asyncio.to_thread(handoff.hand_off, self.linear_state, issue, "default", self.settings,
                                           metadata, to="Nova", request_id=request_id, owner_name="Atlas",
                                           wait_seconds=wait_seconds, sleep=lambda seconds: None)

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


if __name__ == "__main__":
    unittest.main()
