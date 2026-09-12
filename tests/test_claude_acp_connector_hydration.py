#!/usr/bin/env python3
"""ACP session/new must hydrate claude.ai connectors without native Bash/Write."""
from __future__ import annotations

import importlib.util
import json
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
                    "deny": [],
                }
            }), encoding="utf-8")
            rules = client.connector_allow_tools(tmp)
        self.assertEqual(rules, [CALENDAR_ALLOW])
        self.assertNotIn(WILDCARD, rules)

    def test_connector_allow_tools_missing_settings_are_empty(self) -> None:
        client = load_client()
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(client.connector_allow_tools(tmp), [])

    def test_session_options_keep_native_tools_off_and_union_connector_allows(self) -> None:
        client = load_client()
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "settings.json").write_text(json.dumps({
                "permissions": {"allow": [CALENDAR_ALLOW, WILDCARD], "deny": []}
            }), encoding="utf-8")
            options = client.claude_code_session_options({"probe_tool"}, tmp)
        self.assertEqual(options["tools"], ["ToolSearch"])
        self.assertNotIn("Bash", options["tools"])
        self.assertNotIn("Write", options["tools"])
        self.assertNotIn("Edit", options["tools"])
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
