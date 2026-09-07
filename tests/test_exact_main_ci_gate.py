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
            self.module.fetch_runs("owner/repo", "ci.yml", SHA, lambda *_, **__: (_ for _ in ()).throw(RuntimeError("api failure")))

    def test_fetch_runs_filters_server_side_by_head_sha_without_filtering_status(self):
        requested = []
        pending = {"status": "in_progress", "conclusion": None}
        result = type("Result", (), {"returncode": 0, "stdout": '{"workflow_runs": [{"status": "in_progress", "conclusion": null}]}'})()
        self.assertEqual(self.module.fetch_runs("owner/repo", "ci.yml", SHA, lambda command, **_: requested.append(command) or result), [pending])
        query = requested[0][-1]
        self.assertIn(f"head_sha={SHA}", query)
        self.assertNotIn("status=", query)

    def test_gate_authenticates_github_api(self):
        import yaml
        text = (ROOT / ".github/workflows/" / self.workflow_name).read_text()
        jobs = yaml.safe_load(text)["jobs"]
        gates = [s for j in jobs.values() for s in j.get("steps", [])
                 if "scripts/verify-exact-main-ci.py" in s.get("run", "")]
        self.assertTrue(gates)
        for gate in gates:
            self.assertEqual(gate.get("env", {}).get("GH_TOKEN"), "${{ github.token }}")

    def test_publish_job_effectively_has_actions_read(self):
        lines = (ROOT / ".github/workflows/" / self.workflow_name).read_text().splitlines()
        publish = lines.index(f"  {self.job_name}:")
        block = "\n".join(lines[publish:])
        self.assertIn("    permissions:\n      contents: read\n      actions: read", block)


class FakeClock:
    """Deterministic monotonic clock; sleep advances it and records the requested durations."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class ExactMainCiWaitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ExactMainCiGateTests.setUpClass()
        cls.module = ExactMainCiGateTests.module

    def wait(self, polls, *, timeout=1800, interval=20):
        clock = FakeClock()
        remaining = list(polls)
        seen = []

        def fetch():
            seen.append(clock.now)
            return remaining.pop(0) if len(remaining) > 1 else remaining[0]

        result = self.module.wait_for_exact_main_ci(
            fetch, SHA, ".github/workflows/ci.yml",
            timeout_seconds=timeout, interval_seconds=interval, clock=clock, sleep=clock.sleep, log=lambda _: None,
        )
        return result, clock, seen

    def test_wait_passes_when_in_progress_run_completes_successfully(self):
        pending = [run(status="in_progress", conclusion=None)]
        result, clock, seen = self.wait([pending, pending, [run()]])
        self.assertTrue(result)
        self.assertEqual(clock.sleeps, [20, 20])
        self.assertEqual(len(seen), 3)

    def test_wait_fails_when_in_progress_run_completes_with_failure(self):
        pending = [run(status="in_progress", conclusion=None)]
        result, clock, _ = self.wait([pending, [run(conclusion="failure")]])
        self.assertFalse(result)
        self.assertEqual(clock.sleeps, [20])

    def test_wait_treats_absent_run_as_pending_until_it_appears(self):
        result, clock, _ = self.wait([[], [], [run()]])
        self.assertTrue(result)
        self.assertEqual(clock.sleeps, [20, 20])

    def test_wait_times_out_fail_closed_without_real_sleeping(self):
        pending = [run(status="in_progress", conclusion=None)]
        result, clock, seen = self.wait([pending], timeout=100, interval=30)
        self.assertFalse(result)
        self.assertEqual(clock.sleeps, [30, 30, 30, 10])
        self.assertEqual(len(seen), 5)
        self.assertGreaterEqual(clock.now, 100)

    def test_wait_times_out_fail_closed_when_run_never_appears(self):
        result, clock, _ = self.wait([[]], timeout=60, interval=20)
        self.assertFalse(result)
        self.assertEqual(clock.now, 60)

    def test_wait_lets_a_newer_run_supersede_an_older_one(self):
        old_success = run(created_at="2026-09-07T01:00:00Z")
        new_pending = run(status="in_progress", conclusion=None, created_at="2026-09-07T02:00:00Z")
        new_failed = run(conclusion="failure", created_at="2026-09-07T02:00:00Z")
        result, _, _ = self.wait([[old_success, new_pending], [old_success, new_failed]])
        self.assertFalse(result)

        old_failed = run(conclusion="failure", created_at="2026-09-07T01:00:00Z")
        new_success = run(created_at="2026-09-07T02:00:00Z")
        result, _, _ = self.wait([[old_failed, new_pending], [old_failed, new_success]])
        self.assertTrue(result)

    def test_wait_returns_immediately_for_an_already_completed_run(self):
        for conclusion, expected in (("success", True), ("failure", False), ("cancelled", False)):
            with self.subTest(conclusion=conclusion):
                result, clock, seen = self.wait([[run(conclusion=conclusion)]])
                self.assertEqual(result, expected)
                self.assertEqual(clock.sleeps, [])
                self.assertEqual(len(seen), 1)

    def test_cli_without_wait_checks_once_and_with_wait_polls_using_documented_defaults(self):
        import sys
        from unittest import mock
        argv = ["verify-exact-main-ci.py", "--repository", "o/r", "--workflow", "ci.yml", "--sha", SHA, "--workflow-path", ".github/workflows/ci.yml"]
        pending = [run(status="in_progress", conclusion=None)]
        fetches, sleeps = [], []

        def fake_fetch(*_):
            fetches.append(1)
            return pending if len(fetches) < 3 else [run()]

        with mock.patch.object(self.module, "fetch_runs", fake_fetch), mock.patch.object(sys, "argv", argv):
            with self.assertRaises(SystemExit) as raised:
                self.module.main()
        self.assertNotEqual(raised.exception.code, 0)
        self.assertEqual(len(fetches), 1)

        fetches.clear()
        with mock.patch.object(self.module, "fetch_runs", fake_fetch), mock.patch.object(sys, "argv", argv + ["--wait"]), \
                mock.patch.object(self.module.time, "sleep", sleeps.append), mock.patch.object(self.module.time, "monotonic", lambda: 0.0):
            self.assertEqual(self.module.main(), 0)
        self.assertEqual(len(fetches), 3)
        self.assertEqual(sleeps, [20.0, 20.0])

        fetches.clear()
        with mock.patch.object(self.module, "fetch_runs", lambda *_: pending), mock.patch.object(sys, "argv", argv + ["--wait"]), \
                mock.patch.object(self.module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)), \
                mock.patch.object(self.module.time, "monotonic", lambda: clock[0]):
            clock = [0.0]
            with self.assertRaises(SystemExit) as raised:
                self.module.main()
        self.assertNotEqual(raised.exception.code, 0)
        self.assertEqual(clock[0], 30 * 60)

    def test_publish_gate_waits_and_is_the_last_step_before_the_first_registry_write(self):
        import yaml
        text = (ROOT / ".github/workflows/fleet-image.yml").read_text()
        steps = yaml.safe_load(text)["jobs"]["publish"]["steps"]
        names = [step.get("name", "") for step in steps]
        gate = next(i for i, step in enumerate(steps) if "scripts/verify-exact-main-ci.py" in step.get("run", ""))
        self.assertIn("--wait", steps[gate]["run"])
        push = names.index("Push unique staging candidate and capture pushed digest")
        self.assertEqual(gate + 1, push)
        for step in steps[:gate]:
            self.assertNotIn("docker push", step.get("run", ""))
            self.assertNotIn("imagetools create", step.get("run", ""))
            self.assertFalse(step.get("with", {}).get("push"), step.get("name"))
            self.assertNotIn("attest", step.get("uses", ""))
