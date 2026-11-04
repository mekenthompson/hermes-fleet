"""take_over from chat: real upstream Kanban, the registered chat handler and a synthetic Linear.

The requesting agent is never the old owner. It reaches the owner only through the configured
owner command, simulated here by a recorded runner that returns what the owner's
``hermes linear hand-off`` prints. A takeover ends either with one task here and this app as
delegate (replacing exactly the old delegate), or with nothing changed and a plain reason.
The owner side itself (stop, proof, resume) is proven in test_linear_origin_handoff_native.py.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from linear_fake_api import load_plugin

SOURCE = os.environ.get("HERMES_AGENT_SRC", "")
CORE = None
try:
    if not SOURCE:
        raise ImportError("HERMES_AGENT_SRC is not set")
    sys.path.insert(0, SOURCE)
    from hermes_cli import kanban_db as kb  # noqa: F401
except Exception as exc:
    CORE = str(exc)

plugin = load_plugin()
from hermes_fleet_linear_plugin import chat, guard, handoff
from hermes_fleet_linear_plugin.bridge import Bridge, Kanban
from hermes_fleet_linear_plugin.store import Store
from hermes_fleet_linear_plugin.transport import NativeContext
from test_linear_chat_entrust_scenarios import ChatAuthority, now_iso
from test_linear_specialist_authorization import ISSUE, USER, VIEWER

OWNER = str(uuid.UUID(int=0x99, version=4))
STRANGER = str(uuid.UUID(int=0x77, version=4))
REQUEST = str(uuid.UUID(int=0x5150, version=4))
OWNER_COMMAND = ["host-bridge", "profile", "atlas", "--", "hermes", "--profile", "default"]


class OwnerCommand:
    """Records each owner hand-off call and answers with what the owner would print."""

    def __init__(self, *replies):
        self.replies, self.calls = list(replies), []

    def __call__(self, argv, **options):
        self.calls.append((list(argv), options))
        reply = self.replies.pop(0) if self.replies else None
        if isinstance(reply, BaseException):
            raise reply
        stdout = "1Password: applied 3 secrets\n" + (json.dumps(reply) if isinstance(reply, dict) else (reply or ""))
        code = 0 if isinstance(reply, dict) and reply.get("handed_off") else 1
        return subprocess.CompletedProcess(argv, code, stdout, "")


def handed_off(**extra):
    return {"handed_off": True, "state": "handed_off", "issue_id": ISSUE, "task_id": "t_0wner001", "board": "atlas",
            "stop_event_id": 211, "stopped_here": True, "evidence_digest": "a" * 64, "to": "Nova",
            "permission_granted": False, "message": "Atlas's work stopped and is confirmed finished; Nova can continue it.",
            **extra}


def not_handed_off(state, reason, message):
    return {"handed_off": False, "state": state, "reason": reason, "message": message, "permission_granted": False}


@unittest.skipIf(CORE is not None, f"native upstream source unavailable ({CORE})")
class TakeoverScenarios(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="linear-takeover-")
        self.addCleanup(temp.cleanup)
        self.home = Path(temp.name).resolve()
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith("HERMES_KANBAN_") and key != "HERMES_DELEGATED_CHILD_CONTEXT"}
        environment.update(HERMES_HOME=str(self.home), HERMES_KANBAN_HOME=str(self.home / "board"), HERMES_PROFILE="default")
        self.env = patch.dict(os.environ, environment, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.authority = ChatAuthority()
        self.authority.issues[ISSUE]["delegate"] = {"id": OWNER, "name": "Atlas"}
        self.authority.issues[ISSUE]["state"] = {"id": "state-progress", "name": "In Progress", "type": "started"}
        self.authority.issues[ISSUE]["updatedAt"] = now_iso()
        self.board = Kanban(profile="default", profile_home=self.home)
        if not self.board.guarded_controls:
            self.skipTest("native host predates guarded admission")
        settings = {"takeover": {"name": "Nova", "agents": [
            {"name": "Atlas", "delegate_id": OWNER, "command": OWNER_COMMAND, "wait_seconds": 30}]}}
        self.bridge = Bridge(Store(self.home / "linear.db"), self.authority.api(scope=None), self.board,
                             profile="default", settings=settings)
        self.bridge.profile_home = self.home
        self.context = NativeContext(home=str(self.home), profile="default", platform="slack", source="slack",
                                     session_key="slack:chat-1:thread-1", session_id="chat-session", user_id="human-1",
                                     chat_id="chat-1", chat_type="dm", thread_id="thread-1", scope_id="workspace-1")

    def take_over(self, owner, **extra):
        with patch.object(subprocess, "run", owner):
            return json.loads(chat.handle(self.bridge, {"action": "take_over", "issue": ISSUE, "id": REQUEST, **extra},
                                          self.context))

    def tasks(self):
        with self.board.conn() as conn:
            return [dict(row) for row in conn.execute("SELECT id, status, body FROM tasks")]

    def delegate(self):
        return (self.authority.issues[ISSUE].get("delegate") or {}).get("id")

    def assert_nothing_changed_here(self):
        self.assertEqual(self.tasks(), [])
        self.assertIsNone(self.bridge.store.get(ISSUE))
        self.bridge.flush()
        self.assertEqual(self.delegate(), OWNER)

    # -- (a) owner stopped, proven, requester continues as one task -------------------------
    def test_requester_is_not_the_owner_and_continues_as_one_task_after_proof(self):
        owner = OwnerCommand(handed_off())
        reply = self.take_over(owner)
        self.assertTrue(reply["ok"], reply)
        self.assertIn("Took over OPS-1 from Atlas", reply["message"])
        self.assertIn("confirmed finished", reply["message"])
        # The owner was reached only through the configured command, with the full issue reference.
        (argv, options), = owner.calls
        self.assertEqual(argv[:len(OWNER_COMMAND)], OWNER_COMMAND)
        tail = argv[len(OWNER_COMMAND):]
        self.assertEqual(tail[:3], ["linear", "hand-off", ISSUE])
        self.assertEqual(json.loads(tail[tail.index("--issue-json") + 1]),
                         {"id": ISSUE, "identifier": "OPS-1", "url": "https://linear.example/issue/OPS-1"})
        self.assertEqual(tail[tail.index("--to") + 1], "Nova")
        self.assertEqual(tail[tail.index("--request-id") + 1], REQUEST)
        self.assertNotIn("--cancel", tail)
        self.assertNotIn("shell", options)
        # One task on this agent's board, admitted, saying where to continue from.
        tasks = self.tasks()
        self.assertEqual(len(tasks), 1)
        self.assertIn(tasks[0]["status"], ("ready", "running"))
        self.assertIn("Atlas worked on this issue before and stopped", tasks[0]["body"])
        self.assertEqual(reply["handover"], {"from": "Atlas", "state": "handed_off", "stopped_here": True})
        # This app replaces exactly the old delegate, guarded on it.
        rows = [row for row in self.bridge.store.pending() if row["kind"] == "delegate"]
        self.assertEqual([row["payload"]["expected_delegate_id"] for row in rows], [OWNER])
        self.bridge.flush()
        self.assertEqual(self.delegate(), VIEWER)

    def test_retry_after_success_is_work_on_and_never_asks_the_owner_again(self):
        self.assertTrue(self.take_over(OwnerCommand(handed_off()))["ok"])
        self.bridge.flush()
        again = OwnerCommand()
        reply = self.take_over(again)
        self.assertTrue(reply["ok"], reply)
        self.assertEqual(again.calls, [])
        self.assertEqual(len(self.tasks()), 1)

    # -- (b) nothing changes, plain explanation ----------------------------------------------
    def test_failed_proof_leaves_owner_running_and_starts_nothing(self):
        owner = OwnerCommand(not_handed_off("resumed", "proof_timeout",
            "The handover couldn't be confirmed safe: its run did not finish winding down in time. "
            "Atlas's work was resumed and nothing was handed over."))
        reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["code"], "takeover_refused")
        self.assertIn("Atlas's work was resumed and nothing was handed over", reply["message"])
        self.assertNotIn("proof_timeout", reply["message"])
        self.assertEqual(reply["handover"]["state"], "resumed")
        self.assert_nothing_changed_here()

    def test_raw_stop_that_cannot_be_proven_is_left_as_it_was(self):
        owner = OwnerCommand(not_handed_off("unchanged", "native_admission_not_settled",
            "The handover couldn't be confirmed safe. Atlas's work was left as it was, and nothing was handed over."))
        reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertIn("left as it was", reply["message"])
        self.assert_nothing_changed_here()

    def test_unknown_owner_outcome_calls_the_handover_off_and_starts_nothing(self):
        # Blocker 3: the owner may already be paused for this request, so an unknown reply is called off.
        for outcome in (subprocess.TimeoutExpired(["owner"], 120), OSError("no bridge"), "not json at all"):
            with self.subTest(outcome=type(outcome).__name__):
                owner = OwnerCommand(outcome, not_handed_off("resumed", "handoff_cancelled",
                                                             "The handover was called off and Atlas's work was resumed."))
                reply = self.take_over(owner)
                self.assertFalse(reply["ok"])
                self.assertEqual(reply["code"], "takeover_refused")
                self.assertIn("didn't report back", reply["message"])
                self.assertIn("Atlas's work was resumed and nothing was handed over", reply["message"])
                self.assertEqual(reply["handover"]["state"], "resumed")
                self.assertEqual(["--cancel" in argv for argv, _ in owner.calls], [False, True])
                cancel = owner.calls[1][0]
                self.assertEqual(cancel[cancel.index("--request-id") + 1], REQUEST)
                self.assert_nothing_changed_here()

    def test_unknown_owner_outcome_and_unknown_call_off_say_to_retry_the_same_id(self):
        owner = OwnerCommand(subprocess.TimeoutExpired(["owner"], 120), OSError("bridge gone"))
        reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["code"], "takeover_uncertain")
        self.assertIn("same id", reply["message"])
        self.assertIn("operator", reply["message"])
        self.assertEqual(len(owner.calls), 2)
        self.assert_nothing_changed_here()
        # The retry carries the same request id, which the owner recognises on its own Stop.
        owner = OwnerCommand(handed_off(stopped_here=True))
        self.assertTrue(self.take_over(owner)["ok"])
        argv = owner.calls[0][0]
        self.assertEqual(argv[argv.index("--request-id") + 1], REQUEST)

    # -- blocker 1: earlier local work that entrust would reuse but that would not run -------
    def _own_earlier_task(self, final):
        """This agent worked the issue earlier while it was undelegated; then a person delegated it to Atlas."""
        self.authority.issues[ISSUE]["delegate"] = None
        started = json.loads(chat.handle(self.bridge, {"action": "work_on", "issue": ISSUE}, self.context))
        self.assertTrue(started["ok"], started)
        task_id = self.bridge.store.get(ISSUE)["task_id"]
        with self.board.conn() as conn:
            if final == "done":
                self.board.kb.complete_task(conn, task_id, summary="earlier")
            else:
                self.board.kb.block_task(conn, task_id, reason="paused by a person", kind="needs_input")
        self.bridge.flush()
        self.authority.issues[ISSUE]["delegate"] = {"id": OWNER, "name": "Atlas"}
        self.authority.issues[ISSUE]["updatedAt"] = now_iso()
        return task_id

    def test_earlier_local_work_that_would_not_run_is_refused_before_the_owner_is_asked(self):
        # The reviewer's repro: a finished or paused local task used to be linked after the owner
        # stopped, and the reply said "Took over" while nothing ran anywhere.
        for final, words in (("done", "already finished"), ("paused", "is paused")):
            with self.subTest(final=final):
                self.setUp()
                self._own_earlier_task(final)
                before = self.tasks()
                owner = OwnerCommand(handed_off(), not_handed_off("resumed", "handoff_cancelled", "x"))
                reply = self.take_over(owner)
                self.assertFalse(reply["ok"], reply)
                self.assertEqual(reply["code"], "takeover_refused")
                self.assertIn(words, reply["message"])
                self.assertIn("Atlas keeps working on OPS-1", reply["message"])
                self.assertEqual(owner.calls, [])  # Atlas was never asked to stop.
                self.assertEqual(self.tasks(), before)
                self.bridge.flush()
                self.assertEqual(self.delegate(), OWNER)

    def test_nothing_running_here_after_the_proof_calls_the_handover_off(self):
        # If local work changes between the preflight and the start (here: the preflight is
        # skipped), "linked but not running" is a failure: the owner is resumed, never left stopped.
        self._own_earlier_task("done")
        owner = OwnerCommand(handed_off(), not_handed_off("resumed", "handoff_cancelled",
                                                          "The handover was called off and Atlas's work was resumed."))
        with patch.object(Bridge, "takeover_preflight", lambda *a, **k: ("ask_owner", None)):
            reply = self.take_over(owner)
        self.assertFalse(reply["ok"], reply)
        self.assertIn("couldn't start here", reply["message"])
        self.assertIn("Atlas's work was resumed and nothing was handed over", reply["message"])
        self.assertEqual(["--cancel" in argv for argv, _ in owner.calls], [False, True])
        self.bridge.flush()
        self.assertEqual(self.delegate(), OWNER)

    # -- blocker 3: any exception after the proof calls the handover off and still replies ----
    def test_any_exception_after_the_proof_calls_the_handover_off_and_replies(self):
        from hermes_fleet_linear_plugin import transport
        for error in (transport.Uncertain("lost"), KeyError("native"), TypeError("bad"), OSError("disk")):
            with self.subTest(error=type(error).__name__):
                self.setUp()
                owner = OwnerCommand(handed_off(), not_handed_off("resumed", "handoff_cancelled",
                                                                  "The handover was called off and Atlas's work was resumed."))
                def broken(*args, **kwargs):
                    raise error
                with patch.object(Bridge, "entrust", broken):
                    reply = self.take_over(owner)
                self.assertFalse(reply["ok"])
                self.assertEqual(reply["code"], "takeover_refused")
                self.assertIn("Atlas's work was resumed and nothing was handed over", reply["message"])
                self.assertEqual(["--cancel" in argv for argv, _ in owner.calls], [False, True])

    # -- should-fix 7: one take_over per issue at a time --------------------------------------
    def test_a_second_takeover_of_the_same_issue_waits_for_the_first(self):
        owner = OwnerCommand(handed_off())
        with handoff.issue_lock(self.bridge.store.path.parent / "takeover", ISSUE, 0) as held:
            self.assertTrue(held)
            reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["code"], "takeover_in_progress")
        self.assertEqual(owner.calls, [])
        self.assert_nothing_changed_here()
        self.assertTrue(self.take_over(owner)["ok"])  # Free again once the first one has finished.

    # -- should-fix 9: admission after the proof is checked like work_on's ------------------
    def _routing_change_at_create(self, actor):
        """A routing change by ``actor`` lands right after the task is created, before admission."""
        real = Kanban.create
        def create(adapter, *args, **kwargs):
            task = real(adapter, *args, **kwargs)
            issue = self.authority.issues[ISSUE]
            moved = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat().replace("+00:00", "Z")
            issue["updatedAt"] = moved
            self.authority.history.setdefault(ISSUE, []).append({
                "createdAt": moved, "actorId": actor, "fromStateId": issue["state"]["id"], "toStateId": "state-todo",
                "fromAssigneeId": None, "toAssigneeId": None, "fromDelegate": {"id": OWNER}, "toDelegate": {"id": OWNER}})
            return task
        return patch.object(Kanban, "create", create)

    def test_a_person_moving_the_issue_between_proof_and_admission_wins(self):
        owner = OwnerCommand(handed_off(), not_handed_off("resumed", "handoff_cancelled",
                                                          "The handover was called off and Atlas's work was resumed."))
        with self._routing_change_at_create(USER):
            reply = self.take_over(owner)
        self.assertFalse(reply["ok"], reply)
        self.assertIn("stopped, closed or reassigned", reply["message"])
        self.assertEqual(["--cancel" in argv for argv, _ in owner.calls], [False, True])
        self.assertNotIn(self.tasks()[0]["status"], ("ready", "running"))

    def test_the_old_delegate_reporting_its_pause_is_not_a_person_moving_it(self):
        with self._routing_change_at_create(OWNER):
            reply = self.take_over(OwnerCommand(handed_off()))
        self.assertTrue(reply["ok"], reply)
        self.assertIn(self.tasks()[0]["status"], ("ready", "running"))

    def test_a_reply_for_another_issue_is_not_a_handover(self):
        reply = self.take_over(OwnerCommand(handed_off(issue_id="another-issue")))
        self.assertEqual(reply["code"], "takeover_uncertain")
        self.assert_nothing_changed_here()

    def test_unconfigured_owner_is_refused_before_anything_is_asked(self):
        self.authority.issues[ISSUE]["delegate"] = {"id": STRANGER, "name": "Vega"}
        owner = OwnerCommand(handed_off())
        reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["code"], "takeover_unavailable")
        self.assertIn("isn't set up", reply["message"])
        self.assertIn("Vega keeps working on it", reply["message"])
        self.assertEqual(owner.calls, [])
        self.assertEqual(self.tasks(), [])

    def test_refusals_this_side_would_make_later_are_made_before_the_owner_is_asked(self):
        owner = OwnerCommand(handed_off())
        self.authority.issues[ISSUE]["state"] = {"id": "state-done", "name": "Done", "type": "completed"}
        self.assertEqual(self.take_over(owner)["code"], "takeover_refused")
        self.authority.issues[ISSUE]["state"] = {"id": "state-progress", "name": "In Progress", "type": "started"}
        child = replace(self.context, delegated_child=True)
        with patch.object(subprocess, "run", owner):
            reply = json.loads(chat.handle(self.bridge, {"action": "take_over", "issue": ISSUE, "id": REQUEST}, child))
        self.assertFalse(reply["ok"])
        with patch.object(type(self.board), "guarded_controls", property(lambda self: False)):
            self.assertEqual(self.take_over(owner)["code"], "takeover_refused")
        self.assertEqual(owner.calls, [])
        self.assertEqual(self.tasks(), [])

    def test_no_delegate_is_not_a_takeover(self):
        self.authority.issues[ISSUE]["delegate"] = None
        owner = OwnerCommand(handed_off())
        reply = self.take_over(owner)
        self.assertEqual(reply["code"], "not_delegated")
        self.assertIn("work_on", reply["message"])
        self.assertEqual(owner.calls, [])
        self.assertEqual(self.tasks(), [])

    def test_this_side_failing_after_proof_resumes_the_owner(self):
        owner = OwnerCommand(handed_off(), not_handed_off("resumed", "handoff_cancelled",
                                                          "The handover was called off and Atlas's work was resumed."))
        # Someone re-delegates the issue between the owner's proof and this side's start.
        real = Bridge.entrust_checks
        calls = []
        def moved(bridge, issue_id, **kw):
            calls.append(kw)
            if len(calls) == 2:
                self.authority.issues[ISSUE]["delegate"] = {"id": STRANGER, "name": "Vega"}
                self.authority.issues[ISSUE]["updatedAt"] = now_iso()
            return real(bridge, issue_id, **kw)
        with patch.object(Bridge, "entrust_checks", moved):
            reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertIn("Atlas's work was resumed and nothing was handed over", reply["message"])
        self.assertEqual(len(owner.calls), 2)
        self.assertIn("--cancel", owner.calls[1][0])
        self.assertEqual(owner.calls[1][0][owner.calls[1][0].index("--request-id") + 1], REQUEST)
        self.assertEqual(self.tasks(), [])

    # -- review r2 A: a failure after the task started here never resumes the owner ----------
    def test_a_failure_after_the_task_started_here_never_calls_the_handover_off(self):
        # The reviewer's repro: admission ran, then a later step (the chat subscription, the
        # routing outlook) raised. Calling off would resume Atlas beside the task running here.
        from hermes_fleet_linear_plugin import transport
        def subscription_down(*args, **kwargs):
            raise OSError("notify db locked")
        def outlook_down(*args, **kwargs):
            raise transport.Unavailable("linear down")
        for target, name, broken in ((Kanban, "subscribe_origin", subscription_down),
                                     (Bridge, "chat_route_outlook", outlook_down)):
            with self.subTest(step=name):
                self.setUp()
                owner = OwnerCommand(handed_off(), not_handed_off("resumed", "handoff_cancelled", "resumed"))
                with patch.object(target, name, broken):
                    reply = self.take_over(owner)
                self.assertTrue(reply["ok"], reply)
                self.assertIn("Took over OPS-1 from Atlas", reply["message"])
                self.assertIn("Finishing the setup hit a problem", reply["message"])
                self.assertEqual(["--cancel" in argv for argv, _ in owner.calls], [False])  # Never called off.
                (task,) = self.tasks()
                self.assertIn(task["status"], ("ready", "running"))
                # Asking again (any id) never reaches Atlas and finishes the Linear update.
                again = OwnerCommand()
                retry = self.take_over(again, id=str(uuid.uuid4()))
                self.assertTrue(retry["ok"], retry)
                self.assertIn("already taken over from Atlas", retry["message"])
                self.assertEqual(again.calls, [])
                self.bridge.flush()
                self.assertEqual(self.delegate(), VIEWER)
                self.assertEqual(len(self.tasks()), 1)

    def test_unreadable_local_state_after_a_failed_start_neither_resumes_nor_claims_success(self):
        owner = OwnerCommand(handed_off(), not_handed_off("resumed", "handoff_cancelled", "resumed"))
        def broken(*args, **kwargs):
            raise KeyError("native")
        def unreadable(*args, **kwargs):
            raise OSError("board unreadable")
        with patch.object(Bridge, "entrust", broken), patch.object(Bridge, "takeover_local_status", unreadable):
            reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["code"], "takeover_uncertain")
        self.assertIn("same id", reply["message"])
        self.assertEqual(["--cancel" in argv for argv, _ in owner.calls], [False])
        # The retry with the same id proves the same Stop and starts the work here.
        owner = OwnerCommand(handed_off())
        self.assertTrue(self.take_over(owner)["ok"])
        self.assertIn(self.tasks()[0]["status"], ("ready", "running"))

    # -- review r2 B (requester side): a task running here means the owner is never asked -----
    def test_while_a_task_runs_here_the_owner_is_never_asked_anything(self):
        # R2 took over and Nova runs the work; Linear still shows Atlas (routing not flushed yet).
        # A retry of an older request, or any new one, must not reach Atlas: a hand-off it cannot
        # prove, or a call-off, would resume Atlas beside Nova.
        self.assertTrue(self.take_over(OwnerCommand(handed_off()), id=str(uuid.uuid4()))["ok"])
        self.assertEqual(self.delegate(), OWNER)
        for request in (REQUEST, str(uuid.uuid4())):
            with self.subTest(request=request):
                owner = OwnerCommand(not_handed_off("resumed", "handoff_cancelled", "resumed"))
                reply = self.take_over(owner, id=request)
                self.assertTrue(reply["ok"], reply)
                self.assertEqual(owner.calls, [])
                self.assertEqual(len(self.tasks()), 1)
                self.assertIn(self.tasks()[0]["status"], ("ready", "running"))

    def test_other_work_already_running_here_is_refused_before_the_owner_is_asked(self):
        # This agent worked the issue while it was undelegated and still runs it; a person then
        # delegated it to Atlas. Stopping Atlas cannot make one agent run it: refuse, change nothing.
        self.authority.issues[ISSUE]["delegate"] = None
        self.assertTrue(json.loads(chat.handle(self.bridge, {"action": "work_on", "issue": ISSUE}, self.context))["ok"])
        self.bridge.flush()
        self.authority.issues[ISSUE]["delegate"] = {"id": OWNER, "name": "Atlas"}
        self.authority.issues[ISSUE]["updatedAt"] = now_iso()
        before = self.tasks()
        owner = OwnerCommand(handed_off())
        reply = self.take_over(owner)
        self.assertFalse(reply["ok"], reply)
        self.assertEqual(reply["code"], "takeover_refused")
        self.assertIn("already has work running on this issue", reply["message"])
        self.assertEqual(owner.calls, [])
        self.assertEqual(self.tasks(), before)

    def test_owner_reporting_its_work_may_still_be_paused_is_uncertain_not_refused(self):
        owner = OwnerCommand(not_handed_off("stopped", "handoff_stuck",
                                            "Atlas's work is paused and could not be resumed automatically: an operator "
                                            "needs to resume it on Atlas's board."))
        reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["code"], "takeover_uncertain")
        self.assertIn("same id", reply["message"])
        self.assert_nothing_changed_here()

    # -- a take_over that never reached a final answer is finished by the bridge -------------
    def unfinished(self):
        return sorted((self.bridge.store.path.parent / "takeover").glob("*.inflight.json"))

    def resolve(self, owner):
        with patch.object(subprocess, "run", owner):
            chat.resolve_unfinished_takeovers(self.bridge)

    def test_a_takeover_whose_process_stopped_after_the_proof_is_called_off_later(self):
        # The service stops between Atlas's proof and the start here: Atlas is paused, nothing runs.
        class Stopped(BaseException):
            pass
        def stopped(*args, **kwargs):
            raise Stopped()
        with patch.object(Bridge, "entrust", stopped), self.assertRaises(Stopped):
            self.take_over(OwnerCommand(handed_off()))
        self.assertEqual(len(self.unfinished()), 1)
        owner = OwnerCommand(not_handed_off("resumed", "handoff_cancelled", "resumed"))
        self.resolve(owner)
        self.assertEqual(len(owner.calls), 1)
        argv = owner.calls[0][0]
        self.assertIn("--cancel", argv)
        self.assertEqual(argv[argv.index("--request-id") + 1], REQUEST)
        self.assertEqual(self.unfinished(), [])
        self.resolve(OwnerCommand())  # Nothing left to finish.

    def test_an_uncertain_takeover_is_called_off_later_until_it_is_certain(self):
        reply = self.take_over(OwnerCommand(subprocess.TimeoutExpired(["owner"], 120), OSError("bridge gone")))
        self.assertEqual(reply["code"], "takeover_uncertain")
        self.assertEqual(len(self.unfinished()), 1)
        self.resolve(OwnerCommand(OSError("still gone")))
        self.assertEqual(len(self.unfinished()), 1)  # Still unknown: kept, retried after a back-off.
        key = f"takeover-resolve:{ISSUE}"
        self.assertFalse(self.bridge.store.backoff_ready(key, self.bridge.clock()))
        self.bridge.store.backoff_clear(key)
        owner = OwnerCommand(not_handed_off("resumed", "handoff_cancelled", "resumed"))
        self.resolve(owner)
        self.assertEqual(["--cancel" in argv for argv, _ in owner.calls], [True])
        self.assertEqual(self.unfinished(), [])

    def test_an_unfinished_takeover_with_a_task_running_here_stands_and_the_owner_is_not_asked(self):
        def unreadable(*args, **kwargs):
            raise OSError("board unreadable")
        real = Bridge.takeover_local_status
        def subscription_down(*args, **kwargs):
            raise OSError("notify db locked")
        # Admitted here, then a later step fails and this side can't read what runs here.
        with patch.object(Kanban, "subscribe_origin", subscription_down), \
                patch.object(Bridge, "takeover_local_status", unreadable):
            reply = self.take_over(OwnerCommand(handed_off()))
        self.assertEqual(reply["code"], "takeover_uncertain")
        self.assertEqual(len(self.unfinished()), 1)
        self.assertIn(self.tasks()[0]["status"], ("ready", "running"))
        owner = OwnerCommand()
        with patch.object(Bridge, "takeover_local_status", real):
            self.resolve(owner)
        self.assertEqual(owner.calls, [])
        self.assertEqual(self.unfinished(), [])

    def test_finished_takeovers_leave_no_record(self):
        self.assertTrue(self.take_over(OwnerCommand(handed_off()))["ok"])
        self.assertEqual(self.unfinished(), [])
        self.setUp()
        self.assertEqual(self.take_over(OwnerCommand(not_handed_off("resumed", "proof_timeout", "x")))["code"],
                         "takeover_refused")
        self.assertEqual(self.unfinished(), [])

    # -- review r2 C: the old delegate's own writes never hold this agent's routing ----------
    def test_the_old_delegate_writing_after_the_read_still_routes_this_agent(self):
        # Atlas's bridge posts its pause (a comment: updatedAt moves, no history entry) after this
        # side read the issue for routing. The delegate update still goes out.
        real = Kanban.create
        def create(adapter, *args, **kwargs):
            task = real(adapter, *args, **kwargs)
            self.authority.issues[ISSUE]["updatedAt"] = (
                datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat().replace("+00:00", "Z")
            return task
        with patch.object(Kanban, "create", create):
            reply = self.take_over(OwnerCommand(handed_off()))
        self.assertTrue(reply["ok"], reply)
        self.assertEqual(reply["routing"], "queued")
        self.bridge.flush()
        self.assertEqual(self.delegate(), VIEWER)

    def test_held_takeover_routing_is_retried_by_recovery(self):
        # The delegate update could not be queued at the start (Linear was unavailable): recovery
        # retries it in the background, under the human fence, replacing exactly Atlas.
        from hermes_fleet_linear_plugin.api import LinearError
        def unavailable(*args, **kwargs):
            raise LinearError("Linear unreachable (network error)")
        with patch.object(Bridge, "queue_delegate_self", unavailable):
            reply = self.take_over(OwnerCommand(handed_off()))
        self.assertTrue(reply["ok"], reply)
        self.assertEqual(reply["routing"], "held: will retry automatically")
        self.bridge.flush()
        self.assertEqual(self.delegate(), OWNER)
        self.bridge.pump_kanban()
        self.bridge.flush()
        self.assertEqual(self.delegate(), VIEWER)

    def test_a_person_moving_the_issue_after_the_takeover_still_wins_routing(self):
        self.assertTrue(self.take_over(OwnerCommand(handed_off()))["ok"])
        issue = self.authority.issues[ISSUE]
        moved = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat().replace("+00:00", "Z")
        issue["updatedAt"] = moved
        issue["assignee"] = {"id": USER}
        self.authority.history.setdefault(ISSUE, []).append({
            "createdAt": moved, "actorId": USER, "fromStateId": issue["state"]["id"], "toStateId": issue["state"]["id"],
            "fromAssigneeId": None, "toAssigneeId": USER, "fromDelegate": {"id": OWNER}, "toDelegate": {"id": OWNER}})
        self.bridge.flush()
        self.assertEqual(self.delegate(), OWNER)

    # -- review r2 D: this side's Linear work runs under a deadline ---------------------------
    def test_the_start_here_runs_under_a_linear_deadline(self):
        owner = OwnerCommand(handed_off(), not_handed_off("resumed", "handoff_cancelled",
                                                          "The handover was called off and Atlas's work was resumed."))
        with patch.object(handoff, "ENTRUST_BUDGET_SECONDS", 1.0):
            reply = self.take_over(owner)  # No Linear request fits in one second: none is started.
        self.assertFalse(reply["ok"], reply)
        self.assertIn("too slowly", reply["message"])
        self.assertIn("Atlas's work was resumed", reply["message"])
        self.assertEqual(["--cancel" in argv for argv, _ in owner.calls], [False, True])
        self.assertTrue(all(task["status"] not in ("ready", "running") for task in self.tasks()))

    def test_reads_before_the_owner_is_asked_run_under_a_linear_deadline(self):
        owner = OwnerCommand(handed_off())
        with patch.object(handoff, "BEFORE_OWNER_BUDGET_SECONDS", 1.0):
            reply = self.take_over(owner)
        self.assertFalse(reply["ok"])
        self.assertIn("too slowly", reply["message"])
        self.assertEqual(owner.calls, [])
        self.assertEqual(self.tasks(), [])

    # -- what the agent is told ---------------------------------------------------------------
    def test_work_on_refusal_and_guard_point_to_take_over(self):
        reply = json.loads(chat.handle(self.bridge, {"action": "work_on", "issue": ISSUE}, self.context))
        self.assertFalse(reply["ok"])
        self.assertIn("take_over", reply["message"])
        self.assertIn("was not touched", reply["message"])
        message = guard.block_message("QA-3", 6, 69)
        self.assertIn('"action": "take_over", "issue": "QA-3"', message)
        self.assertIn("Never stop another agent's task yourself", message)
        self.assertIn("take_over", chat.SCHEMA["description"])
        self.assertIn("take_over", chat.SCHEMA["parameters"]["properties"]["action"]["enum"])
        self.assertTrue(guard._started("linear", {"action": "take_over", "issue": "QA-3"}, json.dumps({"ok": True})))
        self.assertFalse(guard._started("linear", {"action": "take_over", "issue": "QA-3"}, json.dumps({"ok": False})))


class TakeoverWording(unittest.TestCase):
    """What people read in Linear when an agent pauses its work for a handover."""

    def test_the_handover_pause_never_reads_as_waiting_on_you(self):
        from hermes_fleet_linear_plugin.bridge import _block_message
        kind, body = _block_message({"kind": "needs_input", "reason": handoff.handoff_reason("Nova", REQUEST)},
                                    "blocked")
        self.assertEqual(kind, "response")
        self.assertEqual(body, "Paused for a handover to Nova. Nothing is needed from you: Nova continues this "
                               "work once the handover is confirmed; if the handover doesn't complete, the agent or "
                               "an operator resumes it.")
        self.assertNotIn(REQUEST, body)
        # Should-fix 8: only the exact guarded pause is relabelled. Any other needs-input pause,
        # even one whose words mention a handover, keeps its ordinary "Waiting on you" wording.
        for reason in ("Ken requested Nova take over QA-3 (OPS-9). Stopping Atlas run for handoff; Nova owns the remainder.",
                       handoff.handoff_reason("Nova", REQUEST) + " please",
                       "Which account should I hand over to?", "Need the staging password before handing off the deploy",
                       "Handover to Atlas pending: confirm the region"):
            with self.subTest(reason=reason):
                kind, body = _block_message({"kind": "needs_input", "reason": reason}, "blocked")
                self.assertEqual(kind, "elicitation")
                self.assertTrue(body.startswith("Waiting on you"), body)


class TakeoverSettings(unittest.TestCase):
    def test_budgets_always_end_inside_the_chat_client_timeout(self):
        # Review r2 D/E: every phase of one take_over is bounded, and the bounds add up.
        wait, overhead = handoff.TAKEOVER_MAX_WAIT_SECONDS, handoff.OWNER_CALL_OVERHEAD_SECONDS
        self.assertGreaterEqual(handoff.CLIENT_TIMEOUT_SECONDS, handoff.BEFORE_OWNER_BUDGET_SECONDS
                                + 2 * (wait + overhead) + handoff.ENTRUST_BUDGET_SECONDS)
        # The owner's hand-off (brief lock wait) and --cancel (lock wait up to the wait plus slack)
        # each answer before the requester's subprocess timeout of the wait plus the overhead.
        self.assertLess(handoff.HANDOFF_LOCK_SECONDS + 30, overhead)
        self.assertLess(handoff.LOCK_SLACK_SECONDS + 20, overhead)
        # Each Linear phase lets at least one request start and finish.
        from hermes_fleet_linear_plugin import api
        self.assertGreater(handoff.BEFORE_OWNER_BUDGET_SECONDS, api.REQUEST_TIMEOUT_SECONDS)
        self.assertGreater(handoff.ENTRUST_BUDGET_SECONDS, api.REQUEST_TIMEOUT_SECONDS)
        seen = []
        def run(argv, **options):
            seen.append(options["timeout"])
            raise subprocess.TimeoutExpired(argv, options["timeout"])
        peer = {"name": "Atlas", "command": ["owner"], "wait_seconds": 30.0}
        self.assertIsNone(handoff.request(peer, {"id": "i", "identifier": "OPS-1", "url": "u"}, to="Nova",
                                          request_id=str(uuid.uuid4()), run=run))
        self.assertEqual(seen, [30.0 + overhead])

    def test_a_linear_deadline_never_starts_a_request_it_cannot_finish(self):
        from hermes_fleet_linear_plugin.api import LinearAPI, LinearError
        sent = []
        def transport(url, body, headers):
            sent.append(body)
            return 200, {}, b'{"data": {"viewer": {"id": "v"}}}'
        api = LinearAPI(lambda: "token", transport=transport)
        with api.deadline(5), self.assertRaisesRegex(LinearError, "too slowly"):
            api.graphql("query Viewer { viewer { id } }")
        self.assertEqual(sent, [])
        with api.deadline(60):
            self.assertEqual(api.graphql("query Viewer { viewer { id } }"), {"viewer": {"id": "v"}})
        self.assertEqual(api.graphql("query Viewer { viewer { id } }"), {"viewer": {"id": "v"}})  # No deadline outside.
        self.assertEqual(len(sent), 2)

    def test_only_complete_entries_are_usable(self):
        parsed = handoff.takeover_settings({"name": "Nova", "agents": [
            {"name": "Atlas", "delegate_id": OWNER, "command": OWNER_COMMAND},
            {"name": "Bad", "delegate_id": STRANGER, "command": "host-bridge atlas"},
            {"name": "Slow", "delegate_id": "slow", "command": ["x"], "wait_seconds": 9999},
            {"delegate_id": "nameless", "command": ["x"]}]}, default_name="nova")
        self.assertEqual(set(parsed["agents"]), {OWNER})
        self.assertEqual(parsed["name"], "Nova")
        # Should-fix 4: a valid but long wait is capped so one take_over ends inside the client timeout.
        capped = handoff.takeover_settings({"agents": [
            {"name": "Atlas", "delegate_id": OWNER, "command": OWNER_COMMAND, "wait_seconds": 300}]}, default_name="nova")
        self.assertEqual(capped["agents"][OWNER]["wait_seconds"], handoff.TAKEOVER_MAX_WAIT_SECONDS)
        worst = 2 * (handoff.TAKEOVER_MAX_WAIT_SECONDS + handoff.OWNER_CALL_OVERHEAD_SECONDS) + handoff.ENTRUST_BUDGET_SECONDS
        self.assertLess(worst, handoff.CLIENT_TIMEOUT_SECONDS)
        seen = []
        handoff.request(capped["agents"][OWNER], {"id": ISSUE, "identifier": "OPS-1", "url": "u"}, to="Nova",
                        request_id=REQUEST, run=lambda argv, **options: seen.append(options["timeout"]))
        self.assertEqual(seen, [handoff.TAKEOVER_MAX_WAIT_SECONDS + handoff.OWNER_CALL_OVERHEAD_SECONDS])
        self.assertEqual(handoff.takeover_settings(None, default_name="nova"), {"name": "nova", "agents": {}})


if __name__ == "__main__":
    unittest.main()
