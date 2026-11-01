"""Linear text reads like a colleague's update: words, Markdown and line breaks survive."""
from __future__ import annotations

import unittest

from linear_fake_api import load_plugin

plugin = load_plugin()
from hermes_fleet_linear_plugin.bridge import TASK_BODY, _block_message, _project_summary


class ReadableSummaryTests(unittest.TestCase):
    def test_review_example_survives_intact(self):
        text = "CI/CD passed.\n\nNext: read/write split, and/or deploy."
        self.assertEqual(_project_summary(text, 1500), text)

    def test_ordinary_slashes_urls_and_commands_are_not_paths(self):
        text = "Merged https://github.com/acme/app/pull/12 after /status; 24/7 w/o input/output loss."
        self.assertEqual(_project_summary(text), text)

    def test_real_local_paths_are_masked_and_punctuation_kept(self):
        text = ("Logs: /opt/data/runs/42/out.log, ~/repo/notes.md and ./build/a.txt; "
                "see ../shared/x and file:///tmp/report.html. Also C:\\Users\\ops\\x.txt.")
        out = _project_summary(text, 1500)
        for path in ("/opt/data", "~/repo", "./build", "../shared", "file://", "C:\\Users"):
            self.assertNotIn(path, out)
        self.assertEqual(out.count("[local artifact]"), 6)
        self.assertTrue(out.endswith("[local artifact]."))

    def test_secrets_are_still_redacted(self):
        out = _project_summary("Used token=abc123 and ghp_ABCDEF123 to push.")
        self.assertNotIn("abc123", out)
        self.assertNotIn("ghp_", out)
        self.assertIn("[redacted]", out)

    def test_markdown_lists_and_code_keep_their_shape(self):
        text = "**Delivered**\n\n- one\n  - nested   \n\n\n\n```\ncode\n```"
        self.assertEqual(_project_summary(text, 1500), "**Delivered**\n\n- one\n  - nested\n\n```\ncode\n```")

    def test_trim_stops_at_a_sentence_or_word_never_mid_word(self):
        sentences = "The deploy finished cleanly. " * 20
        out = _project_summary(sentences, 100)
        self.assertLessEqual(len(out), 100)
        self.assertTrue(out.endswith("cleanly."))
        words = "alpha beta gamma delta " * 30
        out = _project_summary(words, 50)
        self.assertTrue(out.endswith("\u2026"))
        self.assertTrue(all(word in {"alpha", "beta", "gamma", "delta"} for word in out[:-1].split()))
        paragraphs = "First paragraph is complete.\n\n" + "Second paragraph keeps going " * 10
        self.assertEqual(_project_summary(paragraphs, 60), "First paragraph is complete.")

    def test_one_line_for_project_update_lines(self):
        self.assertEqual(_project_summary("CI/CD passed.\n\nNext: deploy.", one_line=True),
                         "CI/CD passed. Next: deploy.")


class PlainBlockerTests(unittest.TestCase):
    RAW = "pid 1591346 exited with code 130 [owner admission settled interrupted]"

    def test_runtime_blocks_hide_raw_errors(self):
        for kind in ("transient", "capability"):
            for status in ("blocked", "triage"):
                with self.subTest(kind=kind, status=status):
                    _, body = _block_message({"kind": kind, "reason": self.RAW}, status)
                    for leaked in ("pid", "1591346", "exit", "admission", "native", "reconcil"):
                        self.assertNotIn(leaked, body)
                    self.assertTrue(body.startswith("Paused "))

    def test_human_blocks_say_what_is_needed_from_whom(self):
        kind, body = _block_message({"kind": "needs_input", "reason": "Which region?\nUse /opt/data/x"}, "blocked")
        self.assertEqual(kind, "elicitation")
        self.assertEqual(body, "Waiting on you: Which region? Use [local artifact]. Once this is answered, "
                               "ask the agent to resume and it continues the same task.")
        _, body = _block_message({"kind": "dependency", "reason": ""}, "blocked")
        self.assertEqual(body, "Waiting for a dependency: earlier work must finish first. Work continues when it is done.")


class WorkerPromptTests(unittest.TestCase):
    def test_workers_are_told_to_publish_checkpoints_and_a_closing_summary(self):
        self.assertIn("kanban_comment", TASK_BODY)
        self.assertIn("human_checkpoint=true", TASK_BODY)
        self.assertIn("request_id", TASK_BODY)
        for section in ("**Delivered**", "**Evidence**", "**Remaining**"):
            self.assertIn(section, TASK_BODY)
        TASK_BODY.format(ident="ABC-1", url="https://linear.app/x/issue/ABC-1", context="")


if __name__ == "__main__":
    unittest.main()
