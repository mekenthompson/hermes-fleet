"""Companion Core candidate contract for explicit human checkpoints.

Run with HERMES_AGENT_SRC at the exact Core candidate checkout. The manifest
compatibility scenario lane uses its own pinned checkout.
"""
import unittest
from unittest.mock import patch

import test_linear_native_bridge_scenarios as native


class CandidateCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.fixture = native.NativeBridgeScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_exact_checkpoint_only_and_no_ordinary_progress(self):
        fixture = self.fixture
        clock, remote, reporter, task_id = fixture.reporting_fixture("hf578-checkpoint")
        reporter.flush()
        body = "\n## Verified\n\n" + ("- exact Markdown line\n" * 75) + "\n"
        with fixture.board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=clock()):
            native.kb.add_comment(conn, task_id, "worker", "internal comment")
            native.kb.add_comment(conn, task_id, "worker", body,
                           human_checkpoint=True, request_id="hf578-001")
        reporter.pump_kanban()
        clock.now += 10
        reporter.flush()
        bodies = remote.bodies(native.ISSUE)
        self.assertEqual(sum(text == body for text in bodies), 1)
        self.assertFalse(any("internal comment" in text for text in bodies))
        reporter.recover()
        reporter.flush()
        self.assertEqual(sum(text == body for text in remote.bodies(native.ISSUE)), 1)

    def test_checkpoint_is_not_suppressed_by_same_text_in_prior_activity(self):
        fixture = self.fixture
        clock, remote, reporter, task_id = fixture.reporting_fixture("hf578-same-text")
        reporter.flush()
        body = "## Same text, explicit new request\n\nAn earlier activity used this text."
        with fixture.board.conn() as conn:
            native.kb.add_comment(conn, task_id, "worker", body,
                                  human_checkpoint=True, request_id="hf578-same-text-1")
        event = next(e for e in fixture.board.history(task_id, 0) if e.kind == "checkpoint")
        reporter._task_event(reporter.store.get(native.ISSUE), event, "ready", {("thought", body): 1})
        clock.now += 10
        reporter.flush()
        self.assertEqual(sum(text == body for text in remote.bodies(native.ISSUE)), 1)


if __name__ == "__main__":
    unittest.main()
