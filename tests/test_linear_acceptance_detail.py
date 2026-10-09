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

    def test_refusal_text_is_specific_and_still_a_refusal(self) -> None:
        text = bridge.acceptance_refusal(["auth: Checks API denied"])
        self.assertIn("could not be verified", text)
        self.assertIn("Checks API denied", text)
        self.assertIn("leave this issue open", text)
