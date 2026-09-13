"""Generic plugins belong in public; household maps and Webkite do not."""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LINEAR = ROOT / "plugins" / "linear-agent"
ACP = ROOT / "plugins" / "model-providers" / "claude-acp"
KOKORO = ROOT / "plugins" / "kokoro-voice"
HANDOFF = ROOT / "plugins" / "browser-handoff"
READONLY = ROOT / "plugins" / "readonly-source"
DOCKERFILE = ROOT / "Dockerfile"
CONTRACT = ROOT / "contracts" / "plugins.json"
HOUSEHOLD = re.compile(
    r"switchroom|kenthompson|pixsoul|autograb|browser\.switchroom|"
    + re.escape("klank" + "er")
    + r"|"
    + re.escape("over" + "lord")
    + r"|"
    + re.escape("gym" + "bro")
    + r"|"
    + re.escape("law" + "gpt"),
    re.I,
)
class PublicGenericPluginTests(unittest.TestCase):
    def test_linear_agent_keeps_household_maps_out(self) -> None:
        self.assertFalse((LINEAR / "linear-agents.json").exists())
        self.assertFalse((LINEAR / "linear-publishers.json").exists())
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")
        self.assertIn("test ! -e /opt/hermes/plugins/linear-agent/linear-agents.json", dockerfile)
        self.assertIn(
            "test ! -e /opt/hermes/plugins/linear-agent/linear-publishers.json",
            dockerfile,
        )

    def test_public_policy_reader_stays_fail_closed(self) -> None:
        text = (LINEAR / "linear_policy.py").read_text(encoding="utf-8")
        self.assertIn("O_NOFOLLOW", text)
        self.assertIn("metadata.st_uid != 0", text)
        self.assertIn("os.geteuid() == 0", text)
        self.assertIn("read_immutable_json", text)

    def test_claude_acp_ships_the_profile_local_client(self) -> None:
        text = (ACP / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("ClaudeACPClient", text)
        self.assertNotIn("CopilotACPClient", text)
        self.assertTrue((ACP / "client.py").is_file())
        self.assertTrue((ACP / "tool_bridge_mcp.py").is_file())
        self.assertIn("from .client import ClaudeACPClient", text)
        self.assertIn("/usr/local/bin/hermes-claude-acp-subscription", text)

    def test_browser_handoff_is_env_skinned(self) -> None:
        text = (HANDOFF / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("HERMES_BROWSER_HANDOFF_PUBLIC_HOST", text)
        self.assertIn("HERMES_BROWSER_HANDOFF_TZ", text)
        self.assertNotIn("Australia/", text)
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")
        self.assertIn(
            "COPY plugins/browser-handoff/ /opt/hermes/plugins/browser-handoff/",
            dockerfile,
        )
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        matches = [item for item in contract["components"] if item["id"] == "browser-handoff"]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["target"], "standalone_public_plugin")
        self.assertFalse(matches[0]["default_enabled"])

    def test_readonly_source_is_generic_and_disabled(self) -> None:
        manifest = (READONLY / "plugin.yaml").read_text(encoding="utf-8")
        self.assertIn("author: Hermes Fleet Contributors", manifest)
        self.assertNotIn("Ken Thompson", manifest)
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")
        self.assertIn(
            "COPY plugins/readonly-source/ /opt/hermes/plugins/readonly-source/",
            dockerfile,
        )
        self.assertIn(
            "COPY --chmod=0755 scripts/tooling-policy-hook /opt/hermes/bin/tooling-policy-hook",
            dockerfile,
        )

    def test_kokoro_voice_is_a_generic_disabled_plugin(self) -> None:
        self.assertTrue((KOKORO / "__init__.py").is_file())
        self.assertTrue((KOKORO / "plugin.yaml").is_file())
        manifest = (KOKORO / "plugin.yaml").read_text(encoding="utf-8")
        self.assertIn("author: Hermes Fleet Contributors", manifest)
        self.assertNotIn("Ken Thompson", manifest)
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")
        self.assertIn("COPY plugins/kokoro-voice/ /opt/hermes/plugins/kokoro-voice/", dockerfile)
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        matches = [item for item in contract["components"] if item["id"] == "voice-provider"]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["target"], "standalone_public_plugin")
        self.assertFalse(matches[0]["default_enabled"])
        source = (KOKORO / "__init__.py").read_text(encoding="utf-8")
        self.assertNotIn("hermes-kokoro-sidecar", source)
        self.assertIn('parsed.scheme not in {"http", "https"}', source)
        self.assertIn("not parsed.hostname", source)
        self.assertIn("HERMES_KOKORO_SIDECAR_URL is not set", source)
        workflow = (ROOT / ".github" / "workflows" / "fleet-image.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            workflow.count('pathlib.Path("/opt/hermes/plugins/kokoro-voice")'),
            2,
        )
        self.assertIn("kokoro_module.KokoroProvider()", workflow)
        self.assertIn("assert not kokoro.is_available()", workflow)
        self.assertIn("assert callable(kokoro_module.register)", workflow)

    def test_generic_plugin_trees_have_no_household_literals(self) -> None:
        roots = (LINEAR, ACP, KOKORO, HANDOFF, READONLY, ROOT / "plugins" / "web" / "perplexity")
        for root in roots:
            for path in root.rglob("*"):
                if not path.is_file() or path.suffix not in {".py", ".yaml", ".json", ".md"}:
                    continue
                text = path.read_text(encoding="utf-8")
                self.assertIsNone(HOUSEHOLD.search(text), path.relative_to(ROOT))


if __name__ == "__main__":
    unittest.main()
