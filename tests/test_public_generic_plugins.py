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
        text = (LINEAR / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("O_NOFOLLOW", text)
        self.assertIn("policy is writable by the runtime", text)
        self.assertIn("_read_policy", text)

    def test_claude_acp_keeps_the_public_subscription_shim(self) -> None:
        text = (ACP / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("CopilotACPClient", text)
        self.assertFalse((ACP / "client.py").exists())
        self.assertFalse((ACP / "tool_bridge_mcp.py").exists())

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
        roots = (LINEAR, ACP, KOKORO, ROOT / "plugins" / "web" / "perplexity")
        for root in roots:
            for path in root.rglob("*"):
                if not path.is_file() or path.suffix not in {".py", ".yaml", ".json", ".md"}:
                    continue
                text = path.read_text(encoding="utf-8")
                self.assertIsNone(HOUSEHOLD.search(text), path.relative_to(ROOT))


if __name__ == "__main__":
    unittest.main()
