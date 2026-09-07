from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / ".git").exists())
SCRIPT = ROOT / "scripts" / "verify-exact-main-ci.py"
SHA = "a" * 40


def run(*, sha=SHA, branch="main", workflow=".github/workflows/ci.yml", status="completed", conclusion="success", created_at="2026-09-07T01:00:00Z"):
    return {"event": "push", "head_sha": sha, "head_branch": branch, "path": workflow, "status": status, "conclusion": conclusion, "created_at": created_at}


class ExactMainCiGateTests(unittest.TestCase):
    job_name = "publish"
    workflow_name = "fleet-image.yml"
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("exact_main_ci", SCRIPT)
        cls.module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(cls.module)

    def test_gate_accepts_latest_success_for_exact_main_ci(self):
        self.assertTrue(self.module.latest_exact_main_ci_is_green([run()], SHA, ".github/workflows/ci.yml"))

    def test_gate_rejects_wrong_sha(self):
        self.assertFalse(self.module.latest_exact_main_ci_is_green([run(sha="b" * 40)], SHA, ".github/workflows/ci.yml"))

    def test_gate_rejects_wrong_branch(self):
        self.assertFalse(self.module.latest_exact_main_ci_is_green([run(branch="release")], SHA, ".github/workflows/ci.yml"))

    def test_gate_rejects_wrong_workflow(self):
        self.assertFalse(self.module.latest_exact_main_ci_is_green([run(workflow=".github/workflows/other.yml")], SHA, ".github/workflows/ci.yml"))

    def test_gate_rejects_latest_pending_after_older_success(self):
        runs = [run(created_at="2026-09-07T01:00:00Z"), run(status="in_progress", conclusion=None, created_at="2026-09-07T02:00:00Z")]
        self.assertFalse(self.module.latest_exact_main_ci_is_green(runs, SHA, ".github/workflows/ci.yml"))

    def test_gate_rejects_latest_failure_after_older_success(self):
        runs = [run(created_at="2026-09-07T01:00:00Z"), run(conclusion="failure", created_at="2026-09-07T02:00:00Z")]
        self.assertFalse(self.module.latest_exact_main_ci_is_green(runs, SHA, ".github/workflows/ci.yml"))

    def test_gate_rejects_empty_and_api_failure(self):
        self.assertFalse(self.module.latest_exact_main_ci_is_green([], SHA, ".github/workflows/ci.yml"))
        with self.assertRaises(RuntimeError):
            self.module.fetch_runs("owner/repo", "ci.yml", lambda *_, **__: (_ for _ in ()).throw(RuntimeError("api failure")))

    def test_publish_job_effectively_has_actions_read(self):
        lines = (ROOT / ".github/workflows/" / self.workflow_name).read_text().splitlines()
        publish = lines.index(f"  {self.job_name}:")
        block = "\n".join(lines[publish:])
        self.assertIn("    permissions:\n      contents: read\n      actions: read", block)
