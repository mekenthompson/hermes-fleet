"""Finished work whose PR GitHub would not let the gateway check (live LIFE-584 and peers).

The gateways' fine-grained GitHub token has no checks:read: core's exact-head
verifier is refused at check-runs (HTTP 403) although pull requests and Actions
read fine. Real pinned Core tasks and verifier, the plugin Store and the HTTP
fake Linear run the current entrypoints; one fake ``gh`` answers both Core and
the plugin, at the subprocess boundary.
"""
import json
import os
import subprocess
import time
import unittest
from unittest.mock import patch

import test_linear_delegate_routing_scenarios as routing
from linear_fake_api import SELF, STATES

IN_REVIEW = {"id": "st-review", "name": "In Review", "type": "started"}
REPO = "acme/app"
PR = f"https://github.com/{REPO}/pull/7"
SHA = "c" * 40
SUMMARY = f"Fixed the receipts drill-down and merged {PR}."
# What plugin f4665ce8 wrote for those issues (only the PR and SHA differ).
LEGACY_BODY = (f"The work finished, but its pull request could not be verified as ready ({PR}: auth: GitHub refused the "
               f"acceptance read (HTTP 403 on repos/{REPO}/commits/{SHA}/check-runs) as the ambient gh login; fix that "
               "profile's GitHub credentials/access to the repository, then retry completion.). The issue stays open "
               "until the PR's required checks pass on its latest commit.")


class FakeGh:
    """``gh api`` for one private repository whose token cannot read the Checks API."""

    def __init__(self):
        self.calls = []
        self.pr_state, self.merged = "closed", True
        self.job = ("completed", "success")
        self.actions_refused = False
        self.real_run = subprocess.run

    def run(self, command, *args, **kwargs):
        if not (isinstance(command, list) and command[:2] == ["gh", "api"]):
            return self.real_run(command, *args, **kwargs)
        endpoint = command[2]
        self.calls.append(endpoint)
        reply = self.reply(endpoint)
        if isinstance(reply, int):
            raise subprocess.CalledProcessError(1, command, output="", stderr=(
                f"gh: Resource not accessible by personal access token (HTTP {reply})"))
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(reply), stderr="")

    def reply(self, endpoint):
        state = "MERGED" if self.merged else ("CLOSED" if self.pr_state == "closed" else "OPEN")
        if endpoint == "graphql":
            return {"data": {"repository": {"pullRequest": {"headRefOid": SHA, "baseRefName": "main", "state": state,
                    "baseRef": {"branchProtectionRule": {"requiredStatusChecks": [
                        {"context": "Backend Pytest", "app": {"databaseId": 15368}}]}}}}}}
        if endpoint.startswith(f"repos/{REPO}/rules/branches/main"):
            return [[]]
        if "/check-runs" in endpoint:
            return 403  # the fine-grained token lacks checks:read
        if endpoint == f"repos/{REPO}/pulls/7":
            return {"state": self.pr_state, "merged": self.merged, "base": {"ref": "main"},
                    "head": {"sha": SHA, "ref": "fix/receipts", "repo": {"full_name": REPO}}}
        if endpoint.startswith(f"repos/{REPO}/actions/runs?head_sha={SHA}"):
            if self.actions_refused:
                return 403
            return [{"total_count": 1, "workflow_runs": [{"id": 41, "head_sha": SHA, "event": "pull_request",
                     "path": ".github/workflows/backend-tests.yml", "run_attempt": 1,
                     "head_repository": {"full_name": REPO}}]}]
        if endpoint.startswith(f"repos/{REPO}/contents/.github/workflows/backend-tests.yml?ref=main"):
            return {"type": "file", "path": ".github/workflows/backend-tests.yml"}
        if endpoint.startswith(f"repos/{REPO}/actions/runs/41/jobs"):
            status, conclusion = self.job
            return [{"total_count": 1, "jobs": [{"id": 9, "name": "Backend Pytest", "status": status,
                     "conclusion": conclusion, "run_attempt": 1, "head_sha": SHA}]}]
        return 404


@unittest.skipUnless(os.environ.get("HERMES_AGENT_SRC"), "requires explicit native source")
class PrAcceptanceFallbackScenarios(unittest.TestCase):
    def setUp(self):
        # The fixture of test_linear_status_follows_work_scenarios: a team with In Review and Blocked.
        self.fixture = routing.DelegateRoutingScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.remote.history.pop("issue-1", None)
        self.clock = self.fixture.fixture.clock
        self.clock.now = time.time() - 5
        self.fixture.remote.set_delegate("issue-1", {"id": SELF}, actor=SELF)
        self.states = [*STATES, IN_REVIEW]
        self.fixture.issue["team"]["states"]["nodes"] = self.states
        self.bridge, self.board, self.remote = self.fixture.bridge, self.fixture.board, self.fixture.remote
        self.gh = FakeGh()
        runner = patch("subprocess.run", self.gh.run)
        runner.start()
        self.addCleanup(runner.stop)

    def task(self, *, claim=False):
        with self.board.conn() as db:
            task = self.board.kb.create_task(db, title="Native work", board=self.board.board,
                assignee=self.board.executor_profile, created_by="operator",
                body="Linear issue issue-1: " + self.fixture.issue["url"])
            run = self.board.kb.claim_task(db, task, claimer="native-worker").current_run_id if claim else None
        self.bridge.track("issue-1", task, completion_scope="outcome")
        return task, run

    def native(self, action, *args, **kwargs):
        with self.board.conn() as db:
            return getattr(self.board.kb, action)(db, *args, **kwargs)

    def set_state(self, name, actor="human-1"):
        before = dict(self.fixture.issue)
        self.fixture.issue["state"] = dict(next(s for s in self.states if s["name"] == name))
        self.fixture.issue["updatedAt"] = self.clock.iso()
        self.remote.record_history("issue-1", actor, before, self.fixture.issue)

    def state(self):
        return self.remote.state("issue-1")

    def mutations(self):
        return self.fixture.mutations()

    def failed(self):
        with self.bridge.store._tx() as db:
            return db.execute("SELECT kind, payload FROM outbox WHERE state='failed'").fetchall()

    def ticks(self, count=3, *, after=2.0):
        self.clock.now = max(self.clock.now, time.time()) + after
        for _ in range(count):
            self.bridge.pump_kanban()
            self.bridge.flush()

    def finish_with_pr(self):
        task, run = self.task(claim=True)
        self.ticks()
        self.assertEqual(self.state(), "In Progress")
        self.assertTrue(self.native("complete_task", task, summary=SUMMARY, expected_run_id=run))
        return task

    def legacy_closeout(self):
        """What plugin f4665ce8 did: no fallback, and an auth refusal closed out as terminal Blocked."""
        from hermes_fleet_linear_plugin import acceptance, bridge
        self.clock.now = max(self.clock.now, time.time()) + 2
        with patch.object(acceptance, "settle", lambda receipt, url, **_: receipt), \
                patch.object(acceptance, "unverified", lambda receipt: False), \
                patch.object(bridge, "acceptance_refusal", lambda failures, where="chat": LEGACY_BODY):
            self.bridge.pump_kanban()  # the old build captures the completion as Blocked ...
            self.bridge.flush()  # ... and sends it
        self.assertEqual(self.state(), "Blocked")
        self.assertIn(LEGACY_BODY, self.said())

    def status_rows(self):
        with self.bridge.store._tx() as db:
            return [json.loads(row[0]) for row in db.execute(
                "SELECT payload FROM outbox WHERE kind='status' ORDER BY rowid")]

    def said(self):
        return self.remote.bodies("issue-1")

    def test_refused_checks_read_is_proved_from_actions_and_is_done(self):
        self.finish_with_pr()
        self.ticks()
        self.assertEqual(self.state(), "Done")
        self.assertTrue(any("/check-runs" in call for call in self.gh.calls))  # core asked and was refused
        self.assertTrue(any("/actions/runs/41/jobs" in call for call in self.gh.calls))
        self.assertNotIn("blocked", [row["state"] for row in self.status_rows()])
        self.assertFalse(any("could not be verified" in body for body in self.said()))
        self.assertEqual(self.failed(), [])

    def test_undecided_fallback_waits_in_review_then_reaches_done(self):
        task = self.finish_with_pr()
        self.gh.actions_refused = True
        self.ticks()
        self.assertEqual(self.state(), "In Review")
        waiting = [body for body in self.said() if "moves to Done by itself" in body]
        self.assertEqual(len(waiting), 1, self.said())
        self.assertIn(PR, waiting[0])
        self.assertFalse(self.bridge.store.terminal_captured(task, issue_id="issue-1"))
        # No polling storm: within the gap, ticks make no GitHub reads and no Linear writes.
        calls, writes = len(self.gh.calls), len(self.mutations())
        self.ticks(10, after=60)
        self.assertEqual((len(self.gh.calls), len(self.mutations())), (calls, writes))
        # Still refused after the gap: one more pass, then a longer wait, and nothing new in Linear.
        self.ticks(1, after=600)
        self.assertGreater(len(self.gh.calls), calls)
        self.assertEqual(self.state(), "In Review")
        self.assertEqual(len(self.said()), len(set(self.said())))
        calls = len(self.gh.calls)
        self.ticks(5, after=30)
        self.assertEqual(len(self.gh.calls), calls)
        # GitHub answers again: the next recheck closes it out as Done.
        self.gh.actions_refused = False
        self.ticks(3, after=4000)
        self.assertEqual(self.state(), "Done")
        self.assertTrue(self.bridge.store.terminal_captured(task, issue_id="issue-1", state="done"))
        calls = len(self.gh.calls)
        self.ticks(3, after=4000)
        self.assertEqual(len(self.gh.calls), calls)  # nothing left to recheck
        self.assertEqual(self.failed(), [])

    def test_running_checks_wait_in_review_and_a_later_failure_is_blocked(self):
        task = self.finish_with_pr()
        self.gh.job = ("in_progress", None)
        self.ticks()
        self.assertEqual(self.state(), "In Review")
        self.assertTrue(any("still running" in body for body in self.said()))
        self.gh.job = ("completed", "failure")
        self.ticks(3, after=4000)
        self.assertEqual(self.state(), "Blocked")
        self.assertTrue(self.bridge.store.terminal_captured(task, issue_id="issue-1", state="blocked"))
        self.assertTrue(any("could not be verified as ready" in body and "Backend Pytest" in body for body in self.said()))
        calls = len(self.gh.calls)
        self.ticks(3, after=4000)
        self.assertEqual(len(self.gh.calls), calls)

    def test_failed_check_stays_blocked_and_is_not_rechecked(self):
        task = self.finish_with_pr()
        self.gh.job = ("completed", "failure")
        self.ticks()
        self.assertEqual(self.state(), "Blocked")
        self.assertTrue(self.bridge.store.terminal_captured(task, issue_id="issue-1", state="blocked"))
        self.assertFalse(any("moves to Done by itself" in body for body in self.said()))
        calls, writes = len(self.gh.calls), len(self.mutations())
        self.gh.job = ("completed", "success")
        self.ticks(3, after=4000)
        self.assertEqual((len(self.gh.calls), len(self.mutations())), (calls, writes))
        self.assertEqual(self.state(), "Blocked")

    def test_pr_closed_without_merge_stays_blocked(self):
        self.finish_with_pr()
        self.gh.pr_state, self.gh.merged = "closed", False
        self.ticks()
        self.assertEqual(self.state(), "Blocked")
        self.assertTrue(any("closed without being merged" in body for body in self.said()))

    def test_existing_auth_blocked_closeout_heals_to_done(self):
        task = self.finish_with_pr()
        self.legacy_closeout()
        self.assertTrue(self.bridge.store.terminal_captured(task, issue_id="issue-1", state="blocked"))
        # Deployed: the next pump rechecks that closeout once and heals it, without a manual Linear write.
        self.ticks()
        self.assertEqual(self.state(), "Done")
        self.assertTrue(self.bridge.store.terminal_captured(task, issue_id="issue-1", state="done"))
        self.assertEqual(sum("merged" in body and "could not be verified" not in body for body in self.said()), 1)
        calls = len(self.gh.calls)
        self.ticks(3, after=4000)
        self.assertEqual(len(self.gh.calls), calls)
        self.assertEqual(self.failed(), [])

    def test_existing_auth_blocked_closeout_with_a_failed_check_is_left_and_settled(self):
        self.finish_with_pr()
        self.legacy_closeout()
        self.gh.job = ("completed", "failure")
        said, writes = list(self.said()), len(self.mutations())
        self.ticks()
        self.assertEqual((self.state(), self.said(), len(self.mutations())), ("Blocked", said, writes))
        calls = len(self.gh.calls)
        self.ticks(3, after=4000)
        self.assertEqual(len(self.gh.calls), calls)  # settled: asked once, not again

    def test_human_move_after_the_legacy_block_wins(self):
        self.finish_with_pr()
        self.legacy_closeout()
        self.clock.now = max(self.clock.now, time.time()) + 5
        self.set_state("Todo")  # Ken reopened it himself
        self.ticks()
        self.assertEqual(self.state(), "Todo")


if __name__ == "__main__":
    unittest.main()
