from __future__ import annotations

import importlib.machinery
import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOOK = ROOT / "scripts" / "tooling-policy-hook"


def load_hook():
    loader = importlib.machinery.SourceFileLoader("hermes_fleet_tooling_policy_hook", str(HOOK))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ToolingPolicyHookTests(unittest.TestCase):
    def setUp(self) -> None:
        self.hook = load_hook()

    def test_blocks_unpinned_package_install(self) -> None:
        self.assertEqual(self.hook.classify_command("pip install requests"), "block")
        self.assertEqual(self.hook.classify_command("apt-get install jq"), "block")

    def test_allows_pinned_project_sync(self) -> None:
        self.assertEqual(self.hook.classify_command("uv sync --frozen"), "project")

    def test_enforce_mode_emits_block_decision(self) -> None:
        result = self.hook.decide(
            {"tool_name": "terminal", "tool_input": {"command": "npm install -g left-pad"}},
            "enforce",
        )
        self.assertEqual(result.get("decision"), "block")


if __name__ == "__main__":
    unittest.main()
