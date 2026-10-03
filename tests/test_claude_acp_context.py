"""Provider discovery and context resolution against real Agent source.

The required CI lane supplies HERMES_AGENT_SRC and runs this file separately
from unit tests that install module stubs. Import failures fail that lane.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = os.environ.get("HERMES_AGENT_SRC", "")


@unittest.skipUnless(SOURCE, "real Agent source is supplied by the integration lane")
class ClaudeACPContextTests(unittest.TestCase):
    def probe(self, assertions: str) -> None:
        with tempfile.TemporaryDirectory(prefix="claude-acp-context-") as directory:
            home = Path(directory)
            shutil.copytree(
                ROOT / "plugins/model-providers/claude-acp",
                home / "plugins/model-providers/claude-acp",
            )
            (home / "config.yaml").write_text("{}\n")
            code = "\n".join((
                "import sys",
                "sys.path.insert(0, sys.argv[1])",
                "from providers import get_provider_profile",
                "from agent.model_metadata import get_model_context_length",
                "profile = get_provider_profile('claude-acp')",
                "assert profile is not None, 'Claude ACP plugin was not discovered'",
                assertions,
            ))
            result = subprocess.run(
                [sys.executable, "-c", code, str(Path(SOURCE).resolve())],
                cwd=home,
                env={"PATH": os.defpath, "HOME": str(home), "HERMES_HOME": str(home),
                     "HERMES_RUNTIME_DIR": str(home / "runtime"), "TZ": "UTC"},
                text=True, capture_output=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_plugin_declares_only_its_annotated_model_and_alias_resolves(self) -> None:
        self.probe("\n".join((
            "assert profile.get_model_context_length(' OPUS[1M] ') == 1_000_000",
            "assert profile.get_model_context_length('opus') is None",
            "assert profile.get_model_context_length('sonnet') is None",
            "assert get_provider_profile('anthropic').get_model_context_length('opus[1m]') is None",
            "assert get_model_context_length('opus[1m]', provider='claude-acp', base_url='acp://claude') == 1_000_000",
            "assert get_model_context_length('opus[1m]', provider='claude-code-acp', base_url='acp://claude') == 1_000_000",
        )))

    def test_explicit_user_overrides_win_over_plugin_metadata(self) -> None:
        self.probe("\n".join((
            "assert get_model_context_length('opus[1m]', provider='claude-acp', base_url='acp://claude', config_context_length=65536) == 65536",
            "assert get_model_context_length('opus[1m]', provider='claude-code-acp', base_url='acp://claude', config_context_length=32768) == 32768",
        )))


if __name__ == "__main__":
    unittest.main()
