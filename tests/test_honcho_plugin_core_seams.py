"""The baked Honcho plugin must fail the bake, loudly, when Agent core lacks an import it needs at load time."""
from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "verify_honcho_plugin_core_seams", ROOT / "scripts" / "verify-honcho-plugin-core-seams.py")
seams = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(seams)

PLUGIN = (
    "from agent.skill_commands import extract_user_instruction_from_skill_message\n"
    "from agent.memory_provider import MemoryProvider\n"
    "from . import sibling\n"
    "def lazy():\n"
    "    from hermes_cli.plugin_compat import warn_once  # lazy, not a load-time requirement\n"
)


def _tree(root: Path, core_files: dict[str, str]) -> tuple[Path, Path]:
    plugin, core = root / "plugin", root / "core"
    plugin.mkdir()
    (plugin / "__init__.py").write_text(PLUGIN, encoding="utf-8")
    for rel, text in core_files.items():
        target = core / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    return plugin, core


class CoreSeamTests(unittest.TestCase):
    def test_complete_core_passes_and_lazy_imports_are_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin, core = _tree(Path(tmp), {
                "agent/skill_commands.py": "def extract_user_instruction_from_skill_message(c):\n    return c\n",
                "agent/memory_provider.py": "class MemoryProvider:\n    pass\n",
            })
            self.assertEqual(seams.find_missing(plugin, core), [])
            self.assertEqual(seams.main(["x", str(plugin), str(core)]), 0)

    def test_missing_name_is_reported_and_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin, core = _tree(Path(tmp), {
                "agent/skill_commands.py": "def other():\n    pass\n",
                "agent/memory_provider.py": "class MemoryProvider:\n    pass\n",
            })
            missing = seams.find_missing(plugin, core)
            self.assertEqual(len(missing), 1)
            self.assertIn("extract_user_instruction_from_skill_message", missing[0])
            self.assertEqual(seams.main(["x", str(plugin), str(core)]), 1)

    def test_missing_module_is_reported_and_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin, core = _tree(Path(tmp), {"agent/memory_provider.py": "class MemoryProvider:\n    pass\n"})
            missing = seams.find_missing(plugin, core)
            self.assertEqual(len(missing), 1)
            self.assertIn("module agent.skill_commands not found", missing[0])

    def test_missing_directories_fail(self):
        self.assertEqual(seams.main(["x", "/nonexistent-plugin", "/nonexistent-core"]), 2)

    def test_dockerfile_runs_check_after_patch(self):
        text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertLess(text.index("patch-honcho-gateway-turn-filter.py /opt/hermes"),
                        text.index("verify-honcho-plugin-core-seams.py /opt/hermes/plugins/memory/honcho /opt/hermes"))


if __name__ == "__main__":
    unittest.main()
