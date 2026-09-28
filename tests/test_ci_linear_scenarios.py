"""Behavioral contract for the dedicated, fail-closed Linear scenario lane."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts/run-linear-scenarios.py"


class LinearScenarioLaneTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.source = self.root / "agent"
        self.source.mkdir()
        subprocess.run(["git", "init", "-q", str(self.source)], check=True)
        (self.source / "fixture.txt").write_text("Agent fixture\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.source), "add", "fixture.txt"], check=True)
        subprocess.run(
            ["git", "-C", str(self.source), "-c", "user.name=Fixture",
             "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture"], check=True,
        )
        self.revision = subprocess.check_output(
            ["git", "-C", str(self.source), "rev-parse", "HEAD"], text=True,
        ).strip()
        self.manifest = self.root / "manifest.json"
        self.write_manifest(self.revision)
        self.tests = self.root / "tests"
        self.tests.mkdir()

    def write_manifest(self, revision):
        digest = "sha256:" + "a" * 64
        self.manifest.write_text(json.dumps({
            "schema_version": 1,
            "repository": "ghcr.io/example/hermes-agent",
            "revision": revision,
            "digest": digest,
            "immutable_ref": "ghcr.io/example/hermes-agent@" + digest,
        }), encoding="utf-8")

    def run_lane(self, env=None):
        return subprocess.run(
            [sys.executable, str(RUNNER), "--manifest", str(self.manifest),
             "--agent-source", str(self.source), "--tests-dir", str(self.tests)],
            cwd=ROOT, capture_output=True, text=True, check=False, env=env,
        )

    def test_dirty_source_fails_before_tests(self):
        (self.tests / "test_linear_kanban_scenarios.py").write_text(
            "import unittest\nclass Scenario(unittest.TestCase):\n"
            "    def test_core(self): pass\n", encoding="utf-8",
        )
        for path in (self.source / "fixture.txt", self.source / "untracked.py"):
            with self.subTest(path=path.name):
                path.write_text("modified source\n", encoding="utf-8")
                try:
                    result = self.run_lane()
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("dirty", result.stderr)
                finally:
                    if path.name == "fixture.txt":
                        path.write_text("Agent fixture\n", encoding="utf-8")
                    else:
                        path.unlink()

    def test_lane_isolates_inherited_profile_and_credentials(self):
        live_home = self.root / "live-home"
        live_home.mkdir()
        (self.tests / "test_linear_kanban_scenarios.py").write_text(
            "import os, pathlib, unittest\n"
            "class Scenario(unittest.TestCase):\n"
            "    def test_core(self):\n"
            "        self.assertNotEqual(os.environ['HOME'], " + repr(str(live_home)) + ")\n"
            "        self.assertNotIn('HERMES_PROFILE', os.environ)\n"
            "        self.assertNotIn('OPENAI_API_KEY', os.environ)\n"
            "        self.assertTrue(pathlib.Path(os.environ['HERMES_HOME']).is_dir())\n"
            "        pathlib.Path(os.environ['HERMES_HOME'], 'scenario-write').write_text('isolated')\n",
            encoding="utf-8",
        )
        env = {**os.environ, "HOME": str(live_home), "HERMES_HOME": str(live_home),
               "HERMES_PROFILE": "synthetic-live", "OPENAI_API_KEY": "synthetic-not-secret"}
        result = self.run_lane(env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(list(live_home.iterdir()), [])

    def test_missing_source_fails(self):
        self.source.rename(self.root / "missing")
        result = self.run_lane()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Agent source", result.stderr)

    def test_wrong_revision_fails_before_tests(self):
        self.write_manifest("b" * 40)
        result = self.run_lane()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("revision", result.stderr)

    def test_empty_suite_fails(self):
        result = self.run_lane()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("zero tests", result.stderr)

    def test_skipped_core_import_fails(self):
        (self.tests / "test_linear_kanban_scenarios.py").write_text(
            "import unittest\n"
            "class Scenario(unittest.TestCase):\n"
            "    @unittest.skip('core import failed')\n"
            "    def test_core(self): pass\n", encoding="utf-8",
        )
        result = self.run_lane()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("skipped=1", result.stderr)

    def test_import_failure_fails(self):
        (self.tests / "test_linear_kanban_scenarios.py").write_text(
            "raise ImportError('broken core import')\n", encoding="utf-8",
        )
        result = self.run_lane()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("broken core import", result.stderr)

    def test_executed_suite_passes(self):
        (self.tests / "test_linear_kanban_scenarios.py").write_text(
            "import unittest\n"
            "class Scenario(unittest.TestCase):\n"
            "    def test_core(self): self.assertTrue(True)\n", encoding="utf-8",
        )
        result = self.run_lane()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Ran 1 test", result.stderr)

    def test_ci_provisions_manifest_revision_and_runs_separately(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        source_job = workflow.split("  source-checks:\n", 1)[1].split("\n  history-root:", 1)[0]
        self.assertIn("scripts/read-agent-image-manifest.py", source_job)
        self.assertIn("ref: ${{ steps.agent.outputs.AGENT_REVISION }}", source_job)
        self.assertIn("scripts/run-linear-scenarios.py", source_job)
        self.assertIn("HERMES_AGENT_SRC", source_job)
        self.assertLess(source_job.index("scripts/verify-public-tree.py"),
                        source_job.index("Read pinned Agent revision"))


if __name__ == "__main__":
    unittest.main()
