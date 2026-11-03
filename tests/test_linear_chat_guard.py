"""Chat guard: Linear-tracked work asked for in chat is started as native work, not done inline.

Live acceptance 2026-11-03: Klanker ran a 247 s research job inline and only then logged it
with record_done; Finn finished a takeover inline. These checks drive the registered hooks the
way core does (pre_llm_call once per turn, pre/post_tool_call per tool, post_llm_call at the end).
"""
from __future__ import annotations

import json
import logging
import os
import unittest
from unittest.mock import patch

from linear_fake_api import load_plugin

plugin = load_plugin()
from hermes_fleet_linear_plugin import diag, guard  # noqa: E402

KLANKER_CASE = ("Please research the 3 cheapest ways to back up 2 TB to cloud storage and write it up "
                "— track it in Linear (project Fleet QA).")
FINN_CASE = "Take over QA-9 from Klanker and finish it."

POSITIVES = [
    KLANKER_CASE,
    FINN_CASE,
    "take over HF-621 please",
    "Can you pick up QA-12 and get it merged?",
    "continue HF-600 where Marko left off",
    "work on FLEET-31 today",
    "Finish HF-12, the tests are flaky",
    "Please do HF-44.",
    "carry on with QA-3",
    "resume KEN-514 now that the broker is fixed",
    "track it",
    "Research cold storage options and track this.",
    "Write up the incident and track the work in Linear.",
    "Investigate the WAL lock loss and put it in Linear.",
    "build the export feature, tracked in linear please",
    "Fix the camofox reaper — log it to Linear and take it from there",
    "Draft the Q4 plan as a Linear project and start on it",
    "implement the drill sheet https://linear.app/acme/issue/HF-9 thanks",
    "can you investigate and file a Linear issue for it",
    "Track progress on the migration",
]

NEGATIVES = [
    "what's the status of HF-12?",
    "what's 15% of 240?",
    "how does linear regression work",
    "Is the Linear issue HF-3 still open?",
    "Fix the bug in the linear plugin's status fence",
    "this runs in linear time, can you write it faster?",
    "explain linear algebra eigenvectors",
    "Has Klanker finished QA-9?",
    "Upgrade to SHA-256 and finish the migration",
    "finish the UTF-8 decoding fix",
    "patch CVE-2024-1234 and finish the deploy",
    "can you track this bug down?",
    "track it down for me",
    "Research the 3 cheapest backup options, don't track it",
    "take over HF-12 but do it here",
    "Look at QA-9 and tell me what's left",
    "hi",
    "",
    "[kanban] Task t_abc123 completed.\nTitle: Take over QA-9 and finish it\nAssignee: @finn\nBoard: default",
    "Here is the log:\n```\ntake over QA-9 finish track it in Linear\n```\nwhy did it fail?",
    "> take over QA-9 and finish it\nwho wrote this?",
    "What does `track it` mean in the docs?",
    "Is Linear down right now?",
    "Write a haiku about linear progressions",
    "Compare linear and logistic regression",
]


class MatcherCorpusTests(unittest.TestCase):
    def test_positive_requests_arm(self) -> None:
        for message in POSITIVES:
            with self.subTest(message=message):
                self.assertIsNotNone(guard.match(message))

    def test_questions_and_non_tracker_linear_do_not_arm(self) -> None:
        for message in NEGATIVES:
            with self.subTest(message=message):
                self.assertIsNone(guard.match(message))

    def test_reasons_and_issue_key(self) -> None:
        self.assertEqual(guard.match(KLANKER_CASE), ("track_request", ""))
        self.assertEqual(guard.match(FINN_CASE), ("issue_work_request", "QA-9"))
        self.assertEqual(guard.match("Investigate the outage and put it in Linear."), ("linear_work_request", ""))

    def test_multimodal_content_parts(self) -> None:
        parts = [{"type": "text", "text": FINN_CASE}, {"type": "image_url", "image_url": {"url": "x"}}]
        self.assertEqual(guard.match(parts), ("issue_work_request", "QA-9"))

    def test_case_variations_and_padding(self) -> None:
        # A light fuzz: casing, whitespace and trailing punctuation never change the decision.
        for message in POSITIVES + NEGATIVES:
            if not message or message.startswith(("[kanban]", ">")) or "```" in message or "`" in message:
                continue
            for variant in (message.lower(), "  " + message + "  ", message + "!!", message.replace(" ", "  ")):
                if variant == message.lower() and guard._ISSUE_KEY.search(message):
                    continue  # issue keys are case-sensitive by design
                with self.subTest(variant=variant):
                    self.assertEqual(guard.match(variant) is None, guard.match(message) is None)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class GuardHookTests(unittest.TestCase):
    def setUp(self) -> None:
        diag.reset()
        self.clock = Clock()
        self.guard = guard.ChatGuard(clock=self.clock)
        env = patch.dict(os.environ, {})
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("HERMES_KANBAN_TASK", None)
        self.records: list[logging.LogRecord] = []
        handler = logging.Handler(logging.INFO)
        handler.emit = self.records.append
        logger = logging.getLogger(diag.LOGGER_NAME)
        logger.addHandler(handler)
        level = logger.level
        logger.setLevel(logging.INFO)
        self.addCleanup(logger.removeHandler, handler)
        self.addCleanup(logger.setLevel, level)

    def _lines(self) -> list[str]:
        return [line for line in (r.getMessage() for r in self.records) if "chat.guard" in line]

    def turn(self, message: str, *, platform: str = "slack", turn: str = "t1", session: str = "s1", **extra) -> None:
        self.guard.pre_llm_call(session_id=session, turn_id=turn, user_message=message, platform=platform,
                                task_id="task", conversation_history=[], is_first_turn=False, model="m",
                                parent_session_id=extra.get("parent", ""), sender_id="u")

    def call(self, tool: str, *, turn: str = "t1", session: str = "s1", args: dict | None = None) -> dict | None:
        return self.guard.pre_tool_call(tool_name=tool, args=args or {}, task_id="task", session_id=session,
                                        tool_call_id="c", turn_id=turn, api_request_id="", middleware_trace=[])

    def result(self, tool: str, args: dict, reply: dict, *, turn: str = "t1", session: str = "s1") -> None:
        self.guard.post_tool_call(tool_name=tool, args=args, result=json.dumps(reply), task_id="task",
                                  session_id=session, tool_call_id="c", turn_id=turn, api_request_id="",
                                  duration_ms=1, status="ok", error_type=None, error_message=None, middleware_trace=[])

    def test_inline_research_is_blocked_after_the_allowance(self) -> None:
        self.turn(KLANKER_CASE)
        for _ in range(guard.GRACE_TOOL_CALLS):
            self.assertIsNone(self.call("web_search"))
        blocked = self.call("web_extract")
        self.assertEqual(blocked["action"], "block")
        self.assertIn('"action": "work_on"', blocked["message"])
        self.assertIn('"start_work": true', blocked["message"])
        self.assertIsNotNone(self.call("terminal"))
        # Coordination stays available while blocked.
        for tool in ("linear", "linear_work_reference", "kanban_show", "kanban_create", "a2a_send",
                     "message_agent", "clarify", "tool_search", "tool_describe"):
            with self.subTest(tool=tool):
                self.assertIsNone(self.call(tool))
        # One blocked line per turn, not per call.
        self.assertEqual(sum("outcome=blocked" in line for line in self._lines()), 1)

    def test_takeover_names_the_issue_and_releases_on_work_on(self) -> None:
        self.turn(FINN_CASE)
        for _ in range(guard.GRACE_TOOL_CALLS):
            self.call("terminal")
        message = self.call("terminal")["message"]
        self.assertIn('"issue": "QA-9"', message)
        self.result("linear", {"action": "work_on", "issue": "QA-9"}, {"ok": True, "message": "One native task"})
        self.assertIsNone(self.call("terminal"))
        lines = self._lines()
        self.assertEqual([l.split("outcome=")[1].split()[0] for l in lines], ["armed", "blocked", "released"])
        self.assertIn("reason=tracked_work_started", lines[-1])
        self.assertIn("issue=QA-9", lines[0])

    def test_create_issue_start_work_releases_but_record_done_and_refusals_do_not(self) -> None:
        self.turn(KLANKER_CASE)
        self.result("linear", {"action": "create_issue", "record_done": True, "summary": "x"}, {"ok": True})
        self.result("linear", {"action": "work_on", "issue": "HF-1"}, {"ok": False, "message": "Not started"})
        self.result("linear", {"action": "status", "issue": "HF-1"}, {"ok": True})
        self.result("terminal", {"action": "work_on"}, {"ok": True})
        for _ in range(guard.GRACE_TOOL_CALLS):
            self.call("terminal")
        self.assertIsNotNone(self.call("terminal"))
        self.result("linear", {"action": "create_issue", "start_work": True, "title": "t", "team": "HF"},
                    {"ok": True})
        self.assertIsNone(self.call("terminal"))

    def test_time_allowance_counts_from_first_tool_call(self) -> None:
        self.turn(FINN_CASE)
        self.clock.now += 300  # model thinking before any tool call does not spend the allowance
        self.assertIsNone(self.call("terminal"))
        self.clock.now += guard.GRACE_SECONDS
        blocked = self.call("terminal")
        self.assertIsNotNone(blocked)
        self.assertTrue(any("reason=grace_time" in line for line in self._lines()))

    def test_turn_end_disarms_and_next_turn_is_judged_alone(self) -> None:
        self.turn(FINN_CASE)
        for _ in range(guard.GRACE_TOOL_CALLS + 1):
            self.call("terminal")
        self.guard.post_llm_call(session_id="s1", turn_id="t1", user_message=FINN_CASE, assistant_response="",
                                 conversation_history=[], model="m", platform="slack", task_id="task")
        self.assertIsNone(self.call("terminal"))
        self.assertIn("reason=turn_ended", self._lines()[-1])
        self.turn(FINN_CASE, turn="t2")
        self.turn("what's the status of QA-9?", turn="t3")  # supersedes t2 in the same session
        for _ in range(guard.GRACE_TOOL_CALLS + 3):
            self.assertIsNone(self.call("terminal", turn="t3"))
        self.assertIsNone(self.call("terminal", turn="t2"))

    def test_status_question_and_unarmed_turns_are_never_blocked(self) -> None:
        self.turn("what's the status of HF-12?")
        for _ in range(50):
            self.assertIsNone(self.call("terminal"))
        self.assertEqual(self._lines(), [])

    def test_linear_platform_cron_subagent_and_workers_never_arm(self) -> None:
        for platform in ("linear", "cron", "subagent", "webhook", ""):
            with self.subTest(platform=platform):
                self.turn(FINN_CASE, platform=platform, turn="p-" + platform)
                for _ in range(guard.GRACE_TOOL_CALLS + 2):
                    self.assertIsNone(self.call("terminal", turn="p-" + platform))
        self.turn(FINN_CASE, turn="child", parent="parent-session")
        for _ in range(guard.GRACE_TOOL_CALLS + 2):
            self.assertIsNone(self.call("terminal", turn="child"))
        with patch.dict(os.environ, {"HERMES_KANBAN_TASK": "t_1"}):
            self.turn(FINN_CASE, turn="worker")
            for _ in range(guard.GRACE_TOOL_CALLS + 2):
                self.assertIsNone(self.call("terminal", turn="worker"))

    def test_desktop_and_telegram_arm(self) -> None:
        for platform in ("tui", "telegram", "cli"):
            with self.subTest(platform=platform):
                self.turn(FINN_CASE, platform=platform, turn="d-" + platform, session="s-" + platform)
                for _ in range(guard.GRACE_TOOL_CALLS):
                    self.call("terminal", turn="d-" + platform, session="s-" + platform)
                self.assertIsNotNone(self.call("terminal", turn="d-" + platform, session="s-" + platform))

    def test_missing_turn_id_falls_back_to_the_sessions_armed_turn(self) -> None:
        self.turn(FINN_CASE)
        for _ in range(guard.GRACE_TOOL_CALLS):
            self.call("terminal", turn="")
        self.assertIsNotNone(self.call("terminal", turn=""))

    def test_fails_open_and_logs_once(self) -> None:
        self.turn(FINN_CASE)
        with patch.object(guard, "coordination", side_effect=RuntimeError("boom")):
            for _ in range(guard.GRACE_TOOL_CALLS + 3):
                self.assertIsNone(self.call("terminal"))
        failed = [line for line in self._lines() if "outcome=failed" in line]
        self.assertEqual(len(failed), 1)
        self.assertIn("reason=exception", failed[0])

    def test_log_lines_are_whitelisted(self) -> None:
        self.turn(FINN_CASE)
        for _ in range(guard.GRACE_TOOL_CALLS + 1):
            self.call("terminal")
        for line in self._lines():
            self.assertNotIn("?", line.split("linear.evt", 1)[1], line)
            self.assertNotIn("Klanker", line)

    def test_bounded_memory(self) -> None:
        for n in range(guard.MAX_TURNS + 50):
            self.turn(FINN_CASE, turn=f"t{n}", session=f"s{n}")
        self.assertLessEqual(len(self.guard._turns), guard.MAX_TURNS)
        self.clock.now += guard.TURN_TTL_SECONDS + 1
        self.turn(FINN_CASE, turn="fresh", session="fresh")
        self.assertEqual(list(self.guard._turns), ["fresh"])


class RegistrationTests(unittest.TestCase):
    class Ctx:
        def __init__(self) -> None:
            self.hooks: list[str] = []

        def register_hook(self, name, callback) -> None:
            self.hooks.append(name)

    def test_registers_the_four_turn_hooks(self) -> None:
        ctx = self.Ctx()
        with patch.dict(os.environ, {}):
            os.environ.pop("HERMES_KANBAN_TASK", None)
            self.assertIsNotNone(guard.register(ctx))
        self.assertEqual(ctx.hooks, ["pre_llm_call", "pre_tool_call", "post_tool_call", "post_llm_call"])

    def test_native_workers_register_nothing(self) -> None:
        ctx = self.Ctx()
        with patch.dict(os.environ, {"HERMES_KANBAN_TASK": "t_1"}):
            self.assertIsNone(guard.register(ctx))
        self.assertEqual(ctx.hooks, [])

    def test_plugin_registers_the_guard_outside_workers(self) -> None:
        from test_linear_plugin import FakeContext
        with patch.dict(os.environ, {}):
            os.environ.pop("HERMES_KANBAN_TASK", None)
            on = FakeContext({"enabled": True})
            plugin.register(on)
        self.assertTrue({"pre_llm_call", "pre_tool_call", "post_tool_call", "post_llm_call"} <= set(on.hooks))

if __name__ == "__main__":
    unittest.main()
