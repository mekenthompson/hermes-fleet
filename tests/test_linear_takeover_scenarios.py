"""take_over from chat: real upstream Kanban, the registered chat handler and a synthetic Linear.

The requesting agent is never the old owner. It reaches the owner only through the configured
owner command, simulated here by a recorded runner that returns what the owner's
``hermes linear hand-off`` prints. A takeover ends either with one task here and this app as
delegate (replacing exactly the old delegate), or with nothing changed and a plain reason.
The owner side itself (stop, proof, resume) is proven in test_linear_origin_handoff_native.py.
"""
from __future__ import annotations

from dataclasses import replace
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
from test_linear_specialist_authorization import ISSUE, VIEWER

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

    def test_unknown_owner_outcome_starts_nothing_and_retry_reuses_the_same_request(self):
        for outcome in (subprocess.TimeoutExpired(["owner"], 120), OSError("no bridge"), "not json at all"):
            with self.subTest(outcome=type(outcome).__name__):
                owner = OwnerCommand(outcome)
                reply = self.take_over(owner)
                self.assertFalse(reply["ok"])
                self.assertEqual(reply["code"], "takeover_uncertain")
                self.assertIn("same id", reply["message"])
                self.assert_nothing_changed_here()
        # The retry carries the same request id, which the owner recognises on its own Stop.
        owner = OwnerCommand(handed_off(stopped_here=True))
        self.assertTrue(self.take_over(owner)["ok"])
        argv = owner.calls[0][0]
        self.assertEqual(argv[argv.index("--request-id") + 1], REQUEST)

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
                               "work once the handover is confirmed, and if it isn't, this agent picks it back up.")
        self.assertNotIn(REQUEST, body)
        # OPS-9: an agent's own words for a handover it improvised are a note, not a question.
        kind, body = _block_message({"kind": "needs_input", "reason": (
            "Ken requested Nova take over QA-3 (OPS-9). Stopping Atlas run for handoff; "
            "Nova owns the remainder.")}, "blocked")
        self.assertEqual(kind, "response")
        self.assertNotIn("Waiting on you", body)
        self.assertNotIn("ask the agent to resume", body)
        self.assertTrue(body.endswith("Nothing is needed from you."), body)
        # A real question that mentions a handover still asks for the answer.
        for reason in ("Which account should I hand over to?", "Need the staging password before handing off the deploy",
                       "Handover to Atlas pending: confirm the region"):
            with self.subTest(reason=reason):
                kind, body = _block_message({"kind": "needs_input", "reason": reason}, "blocked")
                self.assertEqual(kind, "elicitation")
                self.assertTrue(body.startswith("Waiting on you"), body)


class TakeoverSettings(unittest.TestCase):
    def test_only_complete_entries_are_usable(self):
        parsed = handoff.takeover_settings({"name": "Nova", "agents": [
            {"name": "Atlas", "delegate_id": OWNER, "command": OWNER_COMMAND},
            {"name": "Bad", "delegate_id": STRANGER, "command": "host-bridge atlas"},
            {"name": "Slow", "delegate_id": "slow", "command": ["x"], "wait_seconds": 9999},
            {"delegate_id": "nameless", "command": ["x"]}]}, default_name="nova")
        self.assertEqual(set(parsed["agents"]), {OWNER})
        self.assertEqual(parsed["name"], "Nova")
        self.assertEqual(handoff.takeover_settings(None, default_name="nova"), {"name": "nova", "agents": {}})


if __name__ == "__main__":
    unittest.main()
