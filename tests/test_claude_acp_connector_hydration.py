#!/usr/bin/env python3
"""ACP session/new must hydrate claude.ai connectors without native Bash/Write."""
from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins" / "model-providers" / "claude-acp"
CALENDAR_ALLOW = "mcp__claude_ai_Google_Calendar__list_*"
WILDCARD = "mcp__claude_ai_*"


def load_client():
    spec = importlib.util.spec_from_file_location("claude_acp_client", PLUGIN / "client.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ConnectorHydrationTests(unittest.TestCase):
    def test_reviewed_claude_code_version_guard_fails_closed(self) -> None:
        client = load_client()
        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp, "package.json")
            executable = Path(tmp, "claude")
            sdk_tools = Path(tmp, "sdk-tools.d.ts")
            executable.write_text("#!/bin/sh\nprintf '2.1.263 (Claude Code)\\n'\n", encoding="utf-8")
            sdk_tools.write_text("export type ToolInputSchemas = BashInput;\n", encoding="utf-8")
            os.chmod(executable, 0o700)
            executable_sha256 = hashlib.sha256(executable.read_bytes()).hexdigest()
            sdk_tools_sha256 = hashlib.sha256(sdk_tools.read_bytes()).hexdigest()
            def verify() -> str:
                return client.assert_reviewed_claude_code_version(
                    package,
                    executable,
                    sdk_tools,
                    executable_sha256=executable_sha256,
                    sdk_tools_sha256=sdk_tools_sha256,
                )
            package.write_text(json.dumps({"version": "2.1.263"}), encoding="utf-8")
            self.assertEqual(verify(), "2.1.263")

            package.write_text(json.dumps({"version": "2.1.264"}), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "review the native tool deny set"):
                verify()

            package.write_text(json.dumps({"version": "2.1.263"}), encoding="utf-8")
            executable.write_text("#!/bin/sh\nprintf '2.1.264 (Claude Code)\\n'\n", encoding="utf-8")
            executable_sha256 = hashlib.sha256(executable.read_bytes()).hexdigest()
            with self.assertRaisesRegex(RuntimeError, "launched Claude Code executable"):
                verify()

            executable.write_text("#!/bin/sh\nprintf '2.1.263 (Claude Code)\\n'\n# drift\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "artifact hash mismatch"):
                verify()

            package.unlink()
            with self.assertRaisesRegex(RuntimeError, "cannot verify"):
                verify()

    def test_connector_allow_tools_reads_specific_rules_not_wildcard(self) -> None:
        client = load_client()
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "settings.json").write_text(json.dumps({
                "permissions": {
                    "allow": [
                        CALENDAR_ALLOW,
                        WILDCARD,
                        "Bash",
                        "mcp__hermes_bridge__probe_tool",
                    ],
                    "deny": [
                        "mcp__claude_ai_Slack__write_*",
                        WILDCARD,
                        "Write",
                    ],
                }
            }), encoding="utf-8")
            rules = client.connector_allow_tools(tmp)
            allow, deny = client.connector_tool_policy(tmp)
        self.assertEqual(rules, [CALENDAR_ALLOW])
        self.assertEqual(allow, [CALENDAR_ALLOW])
        self.assertEqual(deny, ["mcp__claude_ai_Slack__write_*", WILDCARD])
        self.assertNotIn(WILDCARD, rules)

    def test_connector_allow_tools_missing_settings_are_empty(self) -> None:
        client = load_client()
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(client.connector_allow_tools(tmp), [])

    def test_session_options_keep_native_tools_off_and_union_connector_allows(self) -> None:
        client = load_client()
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "settings.json").write_text(json.dumps({
                "permissions": {
                    "allow": [CALENDAR_ALLOW, WILDCARD],
                    "deny": ["mcp__claude_ai_Slack__write_*"],
                }
            }), encoding="utf-8")
            options = client.claude_code_session_options({"probe_tool"}, tmp)
        self.assertEqual(options["tools"], {"type": "preset", "preset": "claude_code"})
        denied = options["disallowedTools"]
        expected_native = {
            "Agent", "Artifact", "AskUserQuestion", "Bash", "ClaudeDesign",
            "CronCreate", "CronDelete", "CronList", "DesignSync", "Edit",
            "EnterPlanMode", "EnterWorktree", "ExitPlanMode", "ExitWorktree",
            "Glob", "Grep", "ListAgents", "ListMcpResources", "Mcp", "Monitor",
            "NotebookEdit", "Projects", "ProposeGoal", "ProposeSkills",
            "PushNotification", "Read", "ReadMcpResource", "ReadMcpResourceDir",
            "ReadNotifications", "RefreshMcpTools", "RemoteTrigger", "REPL",
            "ReportFindings", "ScheduleWakeup", "SendFeedback", "SendMessage",
            "ShareOnboardingGuide", "ShowOnboardingRolePicker", "Skill", "Task",
            "TaskCreate", "TaskGet", "TaskList", "TaskOutput", "TaskStop",
            "TaskUpdate", "TodoWrite", "WebFetch", "WebSearch", "Workflow", "Write",
        }
        self.assertEqual(set(denied[:-1]), expected_native)
        self.assertEqual(denied[-1], "mcp__claude_ai_Slack__write_*")
        self.assertNotIn("ToolSearch", denied)
        allowed = options["allowedTools"]
        self.assertEqual(
            allowed,
            [
                "mcp__hermes_bridge__probe_tool",
                "ToolSearch",
                CALENDAR_ALLOW,
            ],
        )
        self.assertNotIn(WILDCARD, allowed)
        self.assertNotIn("disableClaudeAiConnectors", options["settings"])


if __name__ == "__main__":
    unittest.main()
