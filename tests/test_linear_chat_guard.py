"""Chat guard: Linear-tracked work asked for in chat is started as native work, not done inline.

Live acceptance 2026-11-03: one agent ran a 247 s research job inline and only then logged it
with record_done; another finished a takeover inline. These checks drive the registered hooks the
way core does (pre_llm_call once per turn, pre/post_tool_call per tool, post_llm_call at the end).
Core fire-sites and the real linear handler are proven in the *_scenarios modules.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import unittest
from unittest.mock import patch

from linear_fake_api import load_plugin, work_on_reply

plugin = load_plugin()
from hermes_fleet_linear_plugin import chat, diag, guard  # noqa: E402

RESEARCH_CASE = ("Please research the 3 cheapest ways to back up 2 TB to cloud storage and write it up "
                 "— track it in Linear (project Fleet QA).")
TAKEOVER_CASE = "Take over QA-9 from Atlas and finish it."
SELF = frozenset({"orion"})
SLACK_THREAD = ("[Thread context \u2014 prior messages in this thread (not yet in conversation history):]\n"
                "[thread parent] Ken: Take over QA-9 from Atlas and finish it.\n[End of thread context]\n\n"
                "[New message]\n[Ken | Slack user <@U1>] {typed}")
DISCORD_HISTORY = ("[Context around the replied-to message]\nKen: take over QA-9 and finish it\n\n"
                   "[Recent channel messages]\nKen: track it in Linear as HF-12\n\n[New message]\n[Ken] {typed}")
REPLY_TO_TRACKED = ('[Replying to your previous message: "Done — I tracked it in Linear as HF-12"]\n\n'
                    "thanks, now fix the CSS bug")

POSITIVES = [
    RESEARCH_CASE,
    TAKEOVER_CASE,
    "take over HF-621 please",
    "Can you pick up QA-12 and get it merged?",
    "continue HF-600 where Atlas left off",
    "work on FLEET-31 today",
    "Finish HF-12, the tests are flaky",
    "Please do HF-44.",
    "carry on with QA-3",
    "resume OPS-514 now that the broker is fixed",
    "Atlas, take over QA-9 please",
    "Ok, take over QA-9.",
    "I need you to finish HF-12",
    "I'd like you to work on HF-3",
    "track it in Linear",
    "Track progress on HF-77",
    "Write up the incident and track the work in Linear.",
    "Investigate the WAL lock loss and put it in Linear.",
    "build the export feature and track it in Linear",
    "Fix the camofox reaper — log it to Linear and take it from there",
    "Draft the Q4 plan as a Linear project and start on it",
    "Take over HF-618 from Atlas and finish it.",
    "hey Orion can you take over HF-618",
    "complete HF-618",
    "Get HF-618 done",
    "Orion: take over HF-9",  # addressed to this agent (self name "orion")
    "Note: take over HF-9",
    SLACK_THREAD.format(typed="ok, take over QA-9 and finish it"),
    DISCORD_HISTORY.format(typed="please finish QA-9"),
    "can you investigate and file a Linear issue for it",
    "[Ken] take over QA-9",
    '[Replying to your previous message: "Here is the plan"]\n\ntake over QA-9 and finish it',
]

NEGATIVES = [
    # Coding on the Linear plugin itself, described by what goes wrong on the Linear side.
    "fix the bug where comments don't post to Linear",
    "Fix the plugin so status updates show up in Linear",
    "fix why the status isn't syncing to linear",
    "Investigate why work_on doesn't show In Progress in Linear",
    "Fix the session-end project update so it posts to Linear",
    "Refactor how we write comments to Linear",
    "write a test that the plugin posts to Linear",
    "Implement a fix so we log errors to Linear only once",
    # Status, arithmetic and non-tracker "linear".
    "what's the status of HF-12?",
    "what's 15% of 240?",
    "how does linear regression work",
    "Is the Linear issue HF-3 still open?",
    "Fix the bug in the linear plugin's status fence",
    "this runs in linear time, can you write it faster?",
    "explain linear algebra eigenvectors",
    "Is Linear down right now?",
    "Write a haiku about linear progressions",
    "Compare linear and logistic regression",
    # Routine coding about the Linear plugin: Linear is the subject, not the destination.
    "fix the linear issue sync bug",
    "Fix the Linear project picker",
    "Fix the bug where the Linear issue HF-3 shows twice",
    "write a test for linear.app/ links",
    # Addressed to someone else, conditional, or retracted.
    "Atlas: take over HF-9",
    "@atlas take over HF-9",
    "<@U2> take over HF-9",
    "If Atlas fails, take over HF-9",
    "When Atlas is done, finish HF-9",
    "Take over HF-9? Actually no, wait",
    "take over HF-9. never mind, Atlas has it",
    # Questions, negations and other subjects about existing work.
    "why did you stop working on KEN-514?",
    "Did you continue HF-12?",
    "Atlas is working on HF-9, leave it",
    "I'll take over HF-9 myself, you stop",
    "don't do it until HF-12 merges",
    "What happened with HF-700? did Atlas finish it",
    "Has Atlas finished QA-9?",
    "take over HF-9?",
    "Do you know if HF-12 is in Linear",
    "Look at QA-9 and tell me what's left",
    # "track" without Linear or an issue, or "track down".
    "we need to track this better",
    "track it",
    "Track progress on the migration",
    "can you track this bug down?",
    "track it down for me",
    # Non-issue keys.
    "Upgrade to SHA-256 and finish the migration",
    "finish the UTF-8 decoding fix",
    "patch CVE-2024-1234 and finish the deploy",
    # Opt-outs.
    "Research the 3 cheapest backup options, don't track it",
    "take over HF-12 but do it here",
    # Text the person did not type this turn.
    REPLY_TO_TRACKED,
    SLACK_THREAD.format(typed="what's the ETA?"),
    DISCORD_HISTORY.format(typed="thanks!"),
    "[Recent channel messages]\nKen: take over QA-9 and finish it",
    '[Replying to: "see [\"x\"]\n\nTake over QA-9 and finish it"]\n\nwhat\'s the ETA?',
    '[Replying to: "Take over QA-9 and finish it, track it in Linear"]\n\nwho asked for this?',
    "[Heartbeat — recurring instruction, fires every 1h]\nTake over QA-9 and finish it.\n\n"
    "If there is nothing meaningful to do or report for this instruction right now, reply briefly.",
    "[kanban] Task t_abc123 completed.\nTitle: Take over QA-9 and finish it\nAssignee: @atlas\nBoard: default",
    "[The user sent a text document: 'plan.md'. Its content has been included below. The file is also saved "
    "at: /tmp/plan.md]\n\n[Content of plan.md]:\nTake over QA-9 and finish it.\n\nwhat do you think of this?",
    "summarize this\n\n--- Attached Context ---\n\nTake over QA-9 and finish it, track it in Linear",
    "[System note: Take over QA-9 and finish it]",
    "Here is the log:\n```\ntake over QA-9 finish track it in Linear\n```\nwhy did it fail?",
    "> take over QA-9 and finish it\nwho wrote this?",
    "What does `track it in Linear` mean in the docs?",
    "hi",
    "",
]


class MatcherCorpusTests(unittest.TestCase):
    def test_positive_requests_arm(self) -> None:
        for message in POSITIVES:
            with self.subTest(message=message):
                self.assertIsNotNone(guard.match(message, SELF))

    def test_questions_negations_and_untyped_text_do_not_arm(self) -> None:
        for message in NEGATIVES:
            with self.subTest(message=message):
                self.assertIsNone(guard.match(message, SELF))

    def test_only_the_typed_text_after_channel_history_counts(self) -> None:
        self.assertEqual(guard.request_text(SLACK_THREAD.format(typed="what's the ETA?")), "what's the ETA?")
        self.assertEqual(guard.request_text(DISCORD_HISTORY.format(typed="thanks!")), "thanks!")
        self.assertEqual(guard.match(DISCORD_HISTORY.format(typed="please finish QA-9")),
                         ("issue_work_request", "QA-9"))

    def test_named_addressee_needs_a_self_name(self) -> None:
        self.assertIsNone(guard.match("Orion: take over HF-9"))
        self.assertIsNotNone(guard.match("Orion: take over HF-9", SELF))

    def test_reasons_and_issue_key(self) -> None:
        self.assertEqual(guard.match(RESEARCH_CASE), ("track_request", ""))
        self.assertEqual(guard.match(TAKEOVER_CASE), ("issue_work_request", "QA-9"))
        self.assertEqual(guard.match("Investigate the outage and put it in Linear."), ("linear_work_request", ""))

    def test_issue_key_comes_from_the_request_not_the_quote(self) -> None:
        message = '[Replying to your previous message: "HF-1 is done"]\n\nNow take over QA-9 and finish it.'
        self.assertEqual(guard.match(message), ("issue_work_request", "QA-9"))
        self.assertEqual(guard.match("HF-1 shipped. Take over QA-9 now."), ("issue_work_request", "QA-9"))

    def test_multimodal_content_parts(self) -> None:
        parts = [{"type": "text", "text": TAKEOVER_CASE}, {"type": "image_url", "image_url": {"url": "x"}}]
        self.assertEqual(guard.match(parts), ("issue_work_request", "QA-9"))

    def test_case_variations_and_padding(self) -> None:
        # A light fuzz: casing, whitespace and trailing emphasis never change the decision.
        for message in POSITIVES + NEGATIVES:
            if not message or message.lstrip().startswith(("[", ">", "@", "<")) or "`" in message or "---" in message \
                    or ":" in message.split(" ", 1)[0]:
                continue
            variants = ["  " + message + "  ", message.replace(" ", "  ")]
            if not message.rstrip().endswith("?"):
                variants.append(message + "!!")
            if not guard._ISSUE_KEY.search(message):
                variants.append(message.lower())  # issue keys are case-sensitive by design
            for variant in variants:
                with self.subTest(variant=variant):
                    self.assertEqual(guard.match(variant, SELF) is None, guard.match(message, SELF) is None)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class GuardHookTests(unittest.TestCase):
    def setUp(self) -> None:
        diag.reset()
        self.clock = Clock()
        self.reachable = True
        self.platforms: list[str] = []

        def reachable(platform: str):
            self.platforms.append(platform)
            return self.reachable
        self.guard = guard.ChatGuard(clock=self.clock, reachable=reachable)
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

    def result(self, tool: str, args: dict, reply, *, turn: str = "t1", session: str = "s1") -> None:
        self.guard.post_tool_call(tool_name=tool, args=args, result=reply if isinstance(reply, str) else json.dumps(reply),
                                  task_id="task", session_id=session, tool_call_id="c", turn_id=turn, api_request_id="",
                                  duration_ms=1, status="ok", error_type=None, error_message=None, middleware_trace=[])

    def spend(self, **kw) -> None:
        for _ in range(guard.GRACE_TOOL_CALLS):
            self.assertIsNone(self.call("terminal", **kw))

    def never_blocks(self, **kw) -> None:
        for _ in range(guard.GRACE_TOOL_CALLS + 3):
            self.assertIsNone(self.call("terminal", **kw))

    def test_inline_research_is_blocked_after_the_allowance(self) -> None:
        self.turn(RESEARCH_CASE)
        for _ in range(guard.GRACE_TOOL_CALLS):
            self.assertIsNone(self.call("web_search"))
        blocked = self.call("web_extract")
        self.assertEqual(blocked["action"], "block")
        self.assertIn('"action": "work_on"', blocked["message"])
        self.assertIn('"start_work": true', blocked["message"])
        self.assertIn("does not transfer execution", blocked["message"])
        self.assertIn("clarify", blocked["message"])
        self.assertIn("no linear tool", blocked["message"])
        self.assertIsNotNone(self.call("terminal"))
        for tool in ("linear", "linear_work_reference", "kanban_show", "kanban_create", "a2a_send",
                     "message_agent", "clarify", "tool_search", "tool_describe", "tool_call"):
            with self.subTest(tool=tool):
                self.assertIsNone(self.call(tool))
        self.assertEqual(sum("outcome=blocked" in line for line in self._lines()), 1)

    def test_starting_tracked_work_never_spends_or_hits_the_allowance(self) -> None:
        # Live 4 Nov 2026: the lookups needed to start tracked work (project/team, which issue
        # "QA-10" is) and asking the user must stay available; real work tools stay blocked.
        self.turn(RESEARCH_CASE)
        lookups = [("linear", {"action": action}) for action in ("status", "create_issue", "work_on", "track")]
        lookups.append(("clarify", {"question": "Which team owns Fleet QA?"}))
        for _ in range(3):
            for tool, args in lookups:
                self.assertIsNone(self.call(tool, args=args))
        self.spend()  # the whole allowance is still there for inspection tools
        for tool in ("terminal", "execute_code", "web_search", "web_extract", "browser_navigate", "write_file",
                     "patch"):
            with self.subTest(tool=tool):
                self.assertIsNotNone(self.call(tool))
        for tool, args in lookups:
            with self.subTest(tool=tool, args=args):
                self.assertIsNone(self.call(tool, args=args))
        self.clock.now += guard.GRACE_SECONDS * 5  # the time allowance does not close them either
        for tool, args in lookups:
            self.assertIsNone(self.call(tool, args=args))
        self.assertIsNotNone(self.call("terminal"))

    def test_block_message_says_title_prefixes_and_project_only_creates_work(self) -> None:
        message = guard.block_message("QA-10", 6, 72)
        self.assertIn('{"action": "work_on", "issue": "QA-10"}', message)
        self.assertIn("title prefix", message)
        self.assertIn("the project picks its team", message)
        self.assertNotIn('"team": ...', message)

    def test_takeover_releases_on_the_real_work_on_reply(self) -> None:
        self.turn(TAKEOVER_CASE)
        self.spend()
        self.assertIn('"issue": "QA-9"', self.call("terminal")["message"])
        refused = chat.handle(None, {"action": "work_on", "issue": "QA-9"}, None)  # real handler, no service
        self.assertFalse(json.loads(refused)["ok"])
        self.result("linear", {"action": "work_on", "issue": "QA-9"}, refused)
        self.assertIsNotNone(self.call("terminal"))
        self.result("linear", {"action": "work_on", "issue": "QA-9"}, work_on_reply())
        self.assertIsNone(self.call("terminal"))
        lines = self._lines()
        self.assertEqual([l.split("outcome=")[1].split()[0] for l in lines], ["armed", "blocked", "released"])
        self.assertIn("reason=tracked_work_started", lines[-1])
        self.assertIn("issue=QA-9", lines[0])

    def test_existing_work_reply_also_releases(self) -> None:
        self.turn(TAKEOVER_CASE)
        self.result("linear", {"action": "work_on", "issue": "QA-9"}, work_on_reply(created=False))
        self.never_blocks()

    def test_create_issue_start_work_releases_but_record_done_and_refusals_do_not(self) -> None:
        self.turn(RESEARCH_CASE)
        self.result("linear", {"action": "create_issue", "record_done": True, "summary": "x"}, {"ok": True})
        self.result("linear", {"action": "work_on", "issue": "HF-1"}, {"ok": False, "message": "Not started"})
        self.result("linear", {"action": "status", "issue": "HF-1"}, {"ok": True})
        self.result("terminal", {"action": "work_on"}, {"ok": True})
        self.spend()
        self.assertIsNotNone(self.call("terminal"))
        self.result("linear", {"action": "create_issue", "start_work": True, "title": "t", "team": "HF"},
                    work_on_reply())
        self.assertIsNone(self.call("terminal"))

    def test_time_allowance_counts_from_first_tool_call(self) -> None:
        self.turn(TAKEOVER_CASE)
        self.clock.now += 300  # model thinking before any tool call does not spend the allowance
        self.assertIsNone(self.call("terminal"))
        self.clock.now += guard.GRACE_SECONDS
        self.assertIsNotNone(self.call("terminal"))
        self.assertTrue(any("reason=grace_time" in line for line in self._lines()))

    def test_parallel_tool_calls_share_one_allowance(self) -> None:
        self.turn(TAKEOVER_CASE)
        barrier, results = threading.Barrier(16), []
        lock = threading.Lock()

        def worker() -> None:
            barrier.wait()
            outcome = self.call("terminal")
            with lock:
                results.append(outcome)
        threads = [threading.Thread(target=worker) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(r is None for r in results), guard.GRACE_TOOL_CALLS)
        self.assertEqual(sum(r is not None for r in results), 16 - guard.GRACE_TOOL_CALLS)
        self.assertEqual(sum("outcome=blocked" in line for line in self._lines()), 1)

    def test_turn_end_disarms_and_next_turn_is_judged_alone(self) -> None:
        self.turn(TAKEOVER_CASE)
        self.spend()
        self.guard.post_llm_call(session_id="s1", turn_id="t1", user_message=TAKEOVER_CASE, assistant_response="",
                                 conversation_history=[], model="m", platform="slack", task_id="task")
        self.assertIsNone(self.call("terminal"))
        self.assertIn("reason=turn_ended", self._lines()[-1])
        self.turn(TAKEOVER_CASE, turn="t2")
        self.turn("what's the status of QA-9?", turn="t3")  # supersedes t2 in the same session
        self.never_blocks(turn="t3")
        self.assertIsNone(self.call("terminal", turn="t2"))

    def test_status_questions_and_replies_are_never_blocked(self) -> None:
        for n, message in enumerate(("what's the status of HF-12?", REPLY_TO_TRACKED, "Did you continue HF-12?",
                                     "Atlas is working on HF-9, leave it")):
            with self.subTest(message=message):
                self.turn(message, turn=f"q{n}")
                self.never_blocks(turn=f"q{n}")
        self.assertEqual(self._lines(), [])

    def test_never_arms_for_non_chat_platforms_subagents_workers_or_heartbeats(self) -> None:
        for platform in ("linear", "cron", "subagent", "webhook", "a2a", ""):
            with self.subTest(platform=platform):
                self.turn(TAKEOVER_CASE, platform=platform, turn="p-" + platform)
                self.never_blocks(turn="p-" + platform)
        self.turn(TAKEOVER_CASE, turn="child", parent="parent-session")
        self.never_blocks(turn="child")
        self.turn("[Heartbeat — recurring instruction, fires every 30m]\n" + TAKEOVER_CASE, turn="beat")
        self.never_blocks(turn="beat")
        with patch.dict(os.environ, {"HERMES_KANBAN_TASK": "t_1"}):
            self.turn(TAKEOVER_CASE, turn="worker")
            self.never_blocks(turn="worker")
        self.assertEqual(self._lines(), [])

    def test_unreachable_linear_skips_with_one_line_per_turn(self) -> None:
        self.reachable = False
        self.turn(TAKEOVER_CASE)
        self.never_blocks()
        self.assertEqual(self.platforms, ["slack"])
        self.turn("what's the status of QA-9?", turn="t2")  # no request: nothing to skip, nothing logged
        self.turn(TAKEOVER_CASE, turn="t3")
        self.never_blocks(turn="t3")
        skipped = [line for line in self._lines() if "outcome=skipped" in line]
        self.assertEqual(len(skipped), 2)
        self.assertIn("reason=linear_unreachable", skipped[0])
        self.assertFalse(any("outcome=armed" in line for line in self._lines()))

    def test_reachability_is_per_platform_and_cached_briefly(self) -> None:
        self.turn(TAKEOVER_CASE, turn="a")
        self.turn(TAKEOVER_CASE, turn="b")
        self.turn(TAKEOVER_CASE, turn="c", platform="telegram")
        self.assertEqual(self.platforms, ["slack", "telegram"])
        self.clock.now += guard.REACHABILITY_TTL_SECONDS
        self.turn(TAKEOVER_CASE, turn="d")
        self.assertEqual(self.platforms, ["slack", "telegram", "slack"])

    def test_undetermined_reachability_arms_with_the_conditional_message(self) -> None:
        self.reachable = None
        self.turn(TAKEOVER_CASE)
        self.spend()
        self.assertIn("no linear tool", self.call("terminal")["message"])

    def test_reachability_uses_core_platform_toolsets(self) -> None:
        import sys
        import types
        seen = {}
        config = types.ModuleType("hermes_cli.config")
        config.load_config_readonly = lambda: {"platform_toolsets": {"slack": ["linear"]}}
        tools_config = types.ModuleType("hermes_cli.tools_config")

        def platform_tools(cfg, key):
            seen["key"] = key
            return set(cfg["platform_toolsets"].get(key, ["terminal"]))
        tools_config._get_platform_tools = platform_tools
        model_tools = types.ModuleType("model_tools")
        model_tools._select_tool_names = lambda enabled, disabled, quiet: set(enabled)
        registry_module = types.ModuleType("tools.registry")
        entry = types.SimpleNamespace(check_fn=lambda: True)
        registry_module.registry = types.SimpleNamespace(get_entry=lambda name: entry if name == "linear" else None)
        fakes = {"hermes_cli": types.ModuleType("hermes_cli"), "hermes_cli.config": config,
                 "hermes_cli.tools_config": tools_config, "model_tools": model_tools,
                 "tools": types.ModuleType("tools"), "tools.registry": registry_module}
        with patch.dict(sys.modules, fakes), patch.object(chat, "coordinator_inventory", lambda: True):
            self.assertTrue(guard.linear_reachable("slack"))
            self.assertFalse(guard.linear_reachable("telegram"))
            guard.linear_reachable("tui")
            self.assertEqual(seen["key"], "cli")  # Desktop/TUI resolve under core's cli key
            entry.check_fn = lambda: False
            self.assertFalse(guard.linear_reachable("slack"))
            with patch.object(chat, "coordinator_inventory", lambda: False):
                self.assertFalse(guard.linear_reachable("slack"))
        with patch.dict(sys.modules, {"hermes_cli.tools_config": None}), \
                patch.object(chat, "coordinator_inventory", lambda: True):
            self.assertIsNone(guard.linear_reachable("slack"))

    def test_desktop_telegram_and_cli_arm(self) -> None:
        for platform in ("tui", "telegram", "cli"):
            with self.subTest(platform=platform):
                self.turn(TAKEOVER_CASE, platform=platform, turn="d-" + platform, session="s-" + platform)
                self.spend(turn="d-" + platform, session="s-" + platform)
                self.assertIsNotNone(self.call("terminal", turn="d-" + platform, session="s-" + platform))

    def test_missing_turn_id_falls_back_to_the_sessions_armed_turn(self) -> None:
        self.turn(TAKEOVER_CASE)
        self.spend(turn="")
        self.assertIsNotNone(self.call("terminal", turn=""))

    def test_fails_open_and_logs_at_most_hourly(self) -> None:
        self.turn(TAKEOVER_CASE)
        with patch.object(guard, "coordination", side_effect=RuntimeError("boom")), \
                patch.object(diag, "_now", self.clock):
            self.never_blocks()
            self.clock.now += guard.FAILURE_LOG_EVERY_S
            self.never_blocks()
        failed = [line for line in self._lines() if "outcome=failed" in line]
        self.assertEqual(len(failed), 2)
        self.assertIn("reason=exception", failed[0])

    def test_log_lines_are_whitelisted_and_name_odd_tools(self) -> None:
        self.turn(TAKEOVER_CASE)
        self.spend()
        self.call("mcp__github__create_pull_request2")
        blocked = [line for line in self._lines() if "outcome=blocked" in line][0]
        self.assertIn("kind=mcp_github_create_pull", blocked)
        for line in self._lines():
            self.assertNotIn("?", line.split("linear.evt", 1)[1], line)
            self.assertNotIn("Atlas", line)

    def test_bounded_memory(self) -> None:
        for n in range(guard.MAX_TURNS + 50):
            self.turn(TAKEOVER_CASE, turn=f"t{n}", session=f"s{n}")
        self.assertLessEqual(len(self.guard._turns), guard.MAX_TURNS)
        self.clock.now += guard.TURN_TTL_SECONDS + 1
        self.turn(TAKEOVER_CASE, turn="fresh", session="fresh")
        self.assertEqual(list(self.guard._turns), ["fresh"])


class RegistrationTests(unittest.TestCase):
    class Ctx:
        def __init__(self, settings: dict | None = None) -> None:
            self.hooks: list[str] = []
            self.settings = settings or {}

        def get_config(self, key, default=None):
            return self.settings.get(key, default)

        def register_hook(self, name, callback) -> None:
            self.hooks.append(name)

    def setUp(self) -> None:
        env = patch.dict(os.environ, {})
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("HERMES_KANBAN_TASK", None)

    def test_registers_the_four_turn_hooks_by_default(self) -> None:
        ctx = self.Ctx()
        self.assertIsNotNone(guard.register(ctx))
        self.assertEqual(ctx.hooks, ["pre_llm_call", "pre_tool_call", "post_tool_call", "post_llm_call"])

    def test_chat_guard_false_turns_it_off(self) -> None:
        for value in (False, "false", "False", "0", "off", "no", " OFF ", 0):
            with self.subTest(value=value):
                ctx = self.Ctx({"chat_guard": value})
                self.assertIsNone(guard.register(ctx))
                self.assertEqual(ctx.hooks, [])
        for value in (True, "true", "on", "yes", 1, None):
            with self.subTest(value=value):
                self.assertTrue(guard.enabled(value))
        self.assertIn("chat_guard", plugin.SETTINGS)

    def test_native_workers_register_nothing(self) -> None:
        ctx = self.Ctx()
        with patch.dict(os.environ, {"HERMES_KANBAN_TASK": "t_1"}):
            self.assertIsNone(guard.register(ctx))
        self.assertEqual(ctx.hooks, [])

    def test_plugin_registers_the_guard_unless_disabled(self) -> None:
        from test_linear_plugin import FakeContext
        on = FakeContext({"enabled": True})
        plugin.register(on)
        self.assertTrue({"pre_llm_call", "pre_tool_call", "post_tool_call", "post_llm_call"} <= set(on.hooks))
        off = FakeContext({"enabled": True, "chat_guard": False})
        plugin.register(off)
        self.assertFalse({"pre_llm_call", "pre_tool_call", "post_tool_call", "post_llm_call"} & set(off.hooks))


if __name__ == "__main__":
    unittest.main()
