"""Linear closeout must keep the acceptance receipt's cause, not only a boolean."""
from __future__ import annotations

import unittest

from linear_fake_api import load_plugin

load_plugin()
from hermes_fleet_linear_plugin import bridge  # noqa: E402


class AcceptanceDetailTests(unittest.TestCase):
    def test_cause_names_the_evidence_source_and_not_the_check_dump(self) -> None:
        cause = bridge.acceptance_cause({
            "ok": False, "classification": "auth", "evidence_source": "checks",
            "detail": "GitHub refused Checks API reads (HTTP 403, checks permission).",
            "checks": [{"name": "build", "token": "must-not-appear"}],
        }, url="https://github.com/acme/repo/pull/7")
        self.assertIn("auth:", cause)
        self.assertIn("checks permission", cause)
        self.assertIn("source checks", cause)
        self.assertIn("acme/repo/pull/7", cause)
        self.assertNotIn("must-not-appear", cause)

    def test_cause_redacts_a_credential_in_the_detail(self) -> None:
        leaked = "ghp_EXAMPLETOKEN"
        cause = bridge.acceptance_cause({
            "classification": "auth", "evidence_source": "checks",
            "detail": f"refused {leaked} and github_pat_EXAMPLETOKEN",
        })
        text = bridge.acceptance_refusal([cause])
        self.assertNotIn(leaked, text)
        self.assertNotIn("github_pat_", text)
        self.assertIn("[redacted]", text)
        self.assertIn("could not be verified", text)

    def test_refused_receipt_is_recorded_and_does_not_count_as_accepted(self) -> None:
        calls = []

        def fake(url, contract=None):
            calls.append((url, contract))
            return {"ok": False, "classification": "failure", "head_sha": "a" * 40,
                    "detail": "required job build failed", "evidence_source": "actions"}

        original = bridge.pr_acceptance
        bridge.pr_acceptance = fake
        try:
            failures: list[str] = []
            heads: dict[str, str] = {}
            url = "https://github.com/acme/repo/pull/7"
            self.assertFalse(bridge.Bridge.accepted_evidence(
                object(), [url], failures=failures, heads=heads))
        finally:
            bridge.pr_acceptance = original
        self.assertEqual(calls, [(url, None)])
        self.assertEqual(heads, {})
        self.assertIn("failure:", failures[0])
        self.assertIn("build failed", failures[0])
        self.assertIn("source actions", failures[0])

    def test_skipped_required_job_without_detail_is_actionable_and_refused(self) -> None:
        receipt = {"ok": False, "classification": "infra", "evidence_source": "actions",
                   "head_sha": "a" * 40, "checks": [
                       {"name": "Full suite", "classification": "success", "conclusion": "success"},
                       {"name": "Targeted tests", "classification": "infra", "conclusion": "skipped",
                        "raw_response": "must-not-appear"}]}
        from unittest.mock import patch
        with patch.object(bridge, "pr_acceptance", return_value=receipt):
            failures, heads = [], {}
            self.assertFalse(bridge.Bridge.accepted_evidence(
                object(), ["https://github.com/acme/repo/pull/7"], failures=failures, heads=heads))
        self.assertEqual(heads, {})
        self.assertIn("Targeted tests", failures[0])
        self.assertIn("skipped", failures[0])
        self.assertNotIn("Full suite", failures[0])
        self.assertNotIn("must-not-appear", failures[0])
        self.assertNotIn("no detail", failures[0])
        self.assertLessEqual(len(failures[0]), 500)

    def test_receipt_checks_are_bounded_and_redacted(self) -> None:
        token = "gh" + "p_" + "x" * 40
        for checks in (None, {}, "not rows", [None, 42, {"name": token,
                "classification": "infra", "conclusion": "skipped"}]):
            with self.subTest(checks_type=type(checks).__name__):
                text = bridge.acceptance_cause({"classification": "infra", "checks": checks})
                self.assertNotIn(token, text)
                self.assertLessEqual(len(text), 500)
        text = bridge.acceptance_cause({"classification": "failure", "checks": [
            {"name": str(n) + "x" * 300, "classification": "failure", "conclusion": "failure"}
            for n in range(100)]})
        self.assertLessEqual(len(text), 500)
        self.assertNotIn("2xxxx", text)

    def test_refusal_text_is_specific_and_still_a_refusal(self) -> None:
        text = bridge.acceptance_refusal(["auth: Checks API denied"])
        self.assertIn("could not be verified", text)
        self.assertIn("Checks API denied", text)
        self.assertIn("leave this issue open", text)
