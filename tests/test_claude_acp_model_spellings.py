#!/usr/bin/env python3
"""Claude ACP must accept the spelling the live adapter offers for one model."""

from __future__ import annotations

import unittest

from tests.test_claude_acp_connector_hydration import load_client


class OfferedSpellingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.select = load_client().select_offered_model

    def test_exact_spelling_wins(self) -> None:
        offered = {"opus[1m]", "claude-fable-5-1[1m]", "sonnet"}
        self.assertEqual(self.select(offered, "opus[1m]"), "opus[1m]")
        self.assertEqual(self.select(offered, "sonnet"), "sonnet")

    def test_opus_context_suffix_flip(self) -> None:
        self.assertEqual(self.select({"opus", "sonnet"}, "opus[1m]"), "opus")
        self.assertEqual(self.select({"opus[1m]", "haiku"}, "opus"), "opus[1m]")

    def test_fable_generation_suffix_flip(self) -> None:
        self.assertEqual(
            self.select({"claude-fable-5[1m]", "sonnet"}, "claude-fable-5-1[1m]"),
            "claude-fable-5[1m]",
        )
        self.assertEqual(
            self.select({"claude-fable-5-1[1m]", "haiku"}, "claude-fable-5[1m]"),
            "claude-fable-5-1[1m]",
        )

    def test_does_not_map_fable_to_opus_or_sonnet(self) -> None:
        offered = {"opus[1m]", "sonnet", "haiku"}
        self.assertIsNone(self.select(offered, "claude-fable-5-1[1m]"))
        self.assertIsNone(self.select({"claude-fable-5-1[1m]"}, "opus[1m]"))
        self.assertIsNone(self.select({"opus"}, "sonnet"))

    def test_two_family_spellings_do_not_guess(self) -> None:
        offered = {"opus", "opus[1m]"}
        self.assertEqual(self.select(offered, "opus"), "opus")
        self.assertIsNone(self.select({"claude-fable-5[1m]", "claude-fable-5-1[1m]"}, "fable"))


if __name__ == "__main__":
    unittest.main()
