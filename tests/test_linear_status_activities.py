"""A status read shows what people saw on the issue: comments and Agent Session activities.

HF-620 (3 Nov 2026): Atlas's worker posted "Minute 2 of 10" and the bridge relayed it as an
Agent Session thought. Rowan, taking over, read the issue with the `linear` tool, got comments
only, and told Sam the checkpoint "never reached this thread". These run the real chat handler,
scoped API and fake HTTP Linear: activities arrive in the same IssueProgress query, merged
newest first with comments, labelled by agent, bounded, and the read stays read-only.
"""
from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from linear_fake_api import Clock, FakeLinear, SELF, load_plugin
from test_linear_specialist_authorization import FakeKanban

plugin = load_plugin()
from hermes_fleet_linear_plugin import api as api_module, chat
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.store import Store
from hermes_fleet_linear_plugin.transport import NativeContext

ATLAS = {"id": "app-atlas", "name": "Atlas"}
SAM = "human-1"


class ThreadRemote(FakeLinear):
    def _apply(self, query, variables):
        if "IdentityBinding" in query:
            return 200, {"data": {"viewer": {"id": SELF}, "organization": {"id": "org-1"}}}
        return super()._apply(query, variables)


class StatusActivityTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.home = Path(tmp.name)
        self.clock = Clock()
        self.remote = ThreadRemote(self.clock)
        self.addCleanup(self.remote.close)
        self.remote.add_issue("issue-1", "HF-620")
        self.store = Store(self.home / "state.db")
        self.kanban = FakeKanban()
        # The reader is a different app (Rowan) from the one that posted the activities.
        self.api = plugin.BoundLinearAPI(lambda: "synthetic", endpoint=self.remote.url, clock=self.clock,
            identity={"viewer_id": SELF, "organization_id": "org-1", "teams": ["team-1"], "projects": ["proj-1"]})
        self.queries = []  # full query text; the fake records operation names only
        send = self.api.transport
        self.api.transport = lambda url, body, headers: (self.queries.append(json.loads(body)["query"]),
                                                         send(url, body, headers))[1]
        self.bridge = Bridge(self.store, self.api, self.kanban, profile="synthetic", clock=self.clock)
        self.bridge.profile_home = self.home
        self.context = NativeContext(home=str(self.home), profile="synthetic", platform="telegram", source="telegram",
                                     session_key="chat", session_id="session")

    def comment(self, number, at, body, author=SAM, name="Sam"):
        self.remote.comments.append({"id": f"comment-{number}", "issueId": "issue-1",
            "url": f"https://linear.example/comment/{number}", "body": body,
            "createdAt": self.clock.iso(at), "author_id": author, "author": name})

    def activity(self, number, at, content, *, session="session-k", app=ATLAS, ephemeral=False, issue="issue-1"):
        self.remote.activities.append({"id": f"activity-{session}-{number}", "agentSessionId": session,
            "issueId": issue, "content": content, "createdAt": self.clock.iso(at), "ephemeral": ephemeral, "app": app})

    def hf_620(self):
        """Atlas's session on HF-620 as Linear held it (read live on 3 Nov 2026), plus Sam's comment."""
        self.activity(1, 0, {"type": "thought", "body": "Got it. Checking the issue and any existing work now; updates will appear here."})
        self.activity(2, 26, {"type": "action", "action": "Started", "parameter": "Reading the ticket"})
        self.activity(3, 31, {"type": "response", "body": "The task is under way. It will run for about 10 minutes."})
        self.comment(1, 100, "@Atlas hand this over to Rowan please")
        self.activity(4, 177, {"type": "thought", "body": "**Minute 2 of 10.** The 10-minute window started at 18:16 AEDT."})
        self.activity(5, 182, {"type": "action", "action": "Working", "parameter": "minute 2 checkpoint posted"}, ephemeral=True)
        self.activity(6, 224, {"type": "response", "body": "Handed over to Rowan at Sam's request."})

    def invoke(self):
        return json.loads(chat.handle(self.bridge, {"action": "status", "issue": "HF-620"}, self.context))

    def snapshot(self):
        with closing(sqlite3.connect(self.home / "state.db")) as conn:
            database = list(conn.iterdump())
        return (copy.deepcopy(self.remote.issues), copy.deepcopy(self.remote.comments),
                copy.deepcopy(self.remote.activities), copy.deepcopy(self.kanban.tasks), database)

    def assert_read_only(self, before):
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.store.pending(), [])
        self.assertEqual(self.kanban.creates, [])
        self.assertFalse(any(q.startswith("mutation") for q in self.remote.requests))

    def test_takeover_read_shows_the_minute_2_checkpoint_people_saw(self):
        self.hf_620()
        before = self.snapshot()
        result = self.invoke()
        self.assertTrue(result["ok"], result)
        progress = result["progress"]
        self.assertTrue(progress["agent_activities_included"])
        self.assertEqual(progress["source"], "linear_issue_thread")
        thread = progress["thread"]
        self.assertEqual([(entry["kind"], entry["id"]) for entry in thread], [
            ("agent_activity", "activity-session-k-6"), ("agent_activity", "activity-session-k-4"),
            ("comment", "comment-1"), ("agent_activity", "activity-session-k-3"),
            ("agent_activity", "activity-session-k-2"), ("agent_activity", "activity-session-k-1")])
        minute_2 = thread[1]
        self.assertEqual((minute_2["who"], minute_2["user_id"], minute_2["type"], minute_2["session_id"], minute_2["at"]),
                         ("Atlas", ATLAS["id"], "thought", "session-k", self.clock.iso(177)))
        self.assertTrue(minute_2["body"].startswith("**Minute 2 of 10.**"))
        self.assertNotIn("url", minute_2)
        self.assertEqual((thread[2]["who"], thread[2]["url"]), ("Sam", "https://linear.example/comment/1"))
        self.assertEqual(thread[4]["body"], "Started: Reading the ticket")
        # The superseded ephemeral heartbeat is gone from Linear's thread, so it is not reported.
        self.assertNotIn("minute 2 checkpoint posted", json.dumps(result))
        self.assertTrue(progress["history_complete"])
        self.assertFalse(progress["truncated"])
        self.assert_read_only(before)

    def test_activities_ride_in_the_one_existing_progress_query(self):
        self.hf_620()
        self.invoke()
        progress_queries = [q for q in self.queries if q.startswith("query IssueProgress")]
        self.assertEqual(len(progress_queries), 1)
        self.assertIn(api_module.ACTIVITY_FIELDS, progress_queries[0])
        # No per-session or per-activity follow-up reads.
        self.assertEqual([q for q in self.remote.requests if "agentsession" in q.lower() or "agentactivit" in q.lower()], [])
        query = api_module.ACTIVITY_FIELDS
        self.assertIn("agentSessions(first: %d, orderBy: updatedAt)" % api_module.ACTIVITY_SESSIONS, query)
        self.assertIn("activities(first: %d," % api_module.ACTIVITY_PAGE, query)
        self.assertNotIn('"prompt"', query)

    def test_newest_ephemeral_is_still_shown_and_prompts_and_other_issues_are_not(self):
        self.activity(1, 0, {"type": "prompt", "body": "Sam's prompt is already his comment"})
        self.activity(2, 5, {"type": "thought", "body": "Checking now."})
        self.activity(3, 9, {"type": "action", "action": "Working", "parameter": "minute 0 checkpoint", "result": "posted"},
                      ephemeral=True)
        self.activity(1, 3, {"type": "thought", "body": "Unrelated issue"}, session="session-x", issue="issue-2")
        thread = self.invoke()["progress"]["thread"]
        self.assertEqual([entry["id"] for entry in thread], ["activity-session-k-3", "activity-session-k-2"])
        self.assertEqual(thread[0]["body"], "Working: minute 0 checkpoint\nposted")
        self.assertNotIn("prompt is already", json.dumps(thread))

    def test_a_prompt_that_slips_past_the_filter_is_still_dropped(self):
        original = self.remote._apply
        def unfiltered(query, variables):
            status, payload = original(query, variables)
            if "IssueProgress" in query:
                payload["data"]["issue"]["agentSessions"]["nodes"][0]["activities"]["nodes"].append(
                    {"id": "prompt-1", "createdAt": self.clock.iso(1), "ephemeral": False,
                     "user": {"id": SAM, "name": "Sam"}, "content": {"type": "prompt", "body": "duplicate of a comment"}})
            return status, payload
        self.remote._apply = unfiltered
        self.activity(1, 0, {"type": "error", "body": "Stopped after several failed attempts."})
        thread = self.invoke()["progress"]["thread"]
        self.assertEqual([(entry["id"], entry["type"]) for entry in thread], [("activity-session-k-1", "error")])

    def test_sessions_and_activities_are_bounded_and_truncation_is_explained(self):
        for session in range(api_module.ACTIVITY_SESSIONS + 2):
            for number in range(api_module.ACTIVITY_PAGE + 5):
                self.activity(number, session * 100 + number,
                              {"type": "thought", "body": f"### Checkpoint {session}.{number}\n\n" + "x" * 3000},
                              session=f"session-{session}")
        before = self.snapshot()
        progress = self.invoke()["progress"]
        thread = progress["thread"]
        self.assertLessEqual(len(thread), chat.PROGRESS_WINDOW)
        self.assertLessEqual(sum(len(entry["body"]) for entry in thread), chat.PROGRESS_TEXT_LIMIT)
        self.assertTrue(all(len(entry["body"]) <= chat.PROGRESS_BODY_LIMIT for entry in thread))
        newest = f"activity-session-{api_module.ACTIVITY_SESSIONS + 1}-{api_module.ACTIVITY_PAGE + 4}"
        self.assertEqual(thread[0]["id"], newest)
        self.assertEqual([entry["at"] for entry in thread], sorted((entry["at"] for entry in thread), reverse=True))
        self.assertFalse(progress["history_complete"])
        self.assertTrue(progress["truncated"])
        self.assert_read_only(before)

    def test_older_activities_beyond_the_page_mark_history_incomplete(self):
        for number in range(api_module.ACTIVITY_PAGE + 1):
            self.activity(number, number, {"type": "thought", "body": f"Checkpoint {number}"})
        progress = self.invoke()["progress"]
        self.assertEqual(len(progress["thread"]), api_module.ACTIVITY_PAGE)
        self.assertFalse(progress["history_complete"])

    def test_refused_activity_selection_keeps_comments_and_says_activities_were_not_read(self):
        self.hf_620()
        original = self.remote._apply
        def refuse(query, variables):
            if "agentSessions(" in query:
                return 200, {"errors": [{"message": "Cannot query field \"agentSessions\" on type \"Issue\".",
                                         "extensions": {"code": "GRAPHQL_VALIDATION_FAILED"}}]}
            return original(query, variables)
        self.remote._apply = refuse
        before = self.snapshot()
        progress = self.invoke()["progress"]
        self.assertEqual(progress["status"], "available")
        self.assertFalse(progress["agent_activities_included"])
        self.assertFalse(progress["history_complete"])
        self.assertEqual([(entry["kind"], entry["id"]) for entry in progress["thread"]], [("comment", "comment-1")])
        reads = [q for q in self.queries if q.startswith("query IssueProgress")]
        self.assertEqual(["agentSessions(" in q for q in reads], [True, False])
        # This client stops asking after a definite refusal: the next read is one plain request.
        self.invoke()
        reads = [q for q in self.queries if q.startswith("query IssueProgress")]
        self.assertEqual(["agentSessions(" in q for q in reads], [True, False, False])
        # The pause is bounded: once it ends, the next read asks again.
        self.clock.now += api_module.ACTIVITY_REFUSAL_PAUSE
        self.remote._apply = original
        self.assertTrue(self.invoke()["progress"]["agent_activities_included"])
        self.assert_read_only(before)

    def test_refusal_of_the_whole_read_is_not_blamed_on_activities(self):
        self.hf_620()
        original = self.remote._apply
        self.remote._apply = lambda q, v: (403, {"errors": [{"message": "forbidden"}]}) if "IssueProgress" in q else original(q, v)
        self.assertEqual(self.invoke()["progress"]["code"], "comments_unavailable")
        self.remote._apply = original
        self.assertTrue(self.invoke()["progress"]["agent_activities_included"])

    def test_transient_failure_is_not_mistaken_for_a_refused_field(self):
        self.hf_620()
        original = self.remote._apply
        self.remote._apply = lambda q, v: (503, {"errors": [{"message": "unavailable"}]}) if "IssueProgress" in q else original(q, v)
        self.assertEqual(self.invoke()["progress"]["code"], "comments_unavailable")
        self.remote._apply = original
        self.assertTrue(self.invoke()["progress"]["agent_activities_included"])

    def test_specialist_scope_filters_activity_authors_like_comments(self):
        self.api.specialist_scope = self.api._validate_specialist_scope({
            "allowed_team_ids": ["team-1"], "allowed_project_ids": ["proj-1"], "allowed_requester_ids": [SAM]})
        self.activity(1, 0, {"type": "thought", "body": "Own app checkpoint"}, session="session-own",
                      app={"id": SELF, "name": "This Agent"})
        self.activity(1, 5, {"type": "thought", "body": "Foreign app private thought"}, session="session-k")
        before = self.snapshot()
        result = self.invoke()
        self.assertEqual([entry["id"] for entry in result["progress"]["thread"]], ["activity-session-own-1"])
        self.assertNotIn("Foreign app private", json.dumps(result))
        self.assert_read_only(before)

    def test_ordinary_worker_thread_read_is_unchanged(self):
        self.hf_620()
        thread = self.api.issue_thread("issue-1", limit=20)
        self.assertEqual([c["id"] for c in thread], ["comment-1"])
        threads = [q for q in self.queries if q.startswith("query IssueThread")]
        self.assertEqual(len(threads), 1)
        self.assertNotIn("agentSessions(", threads[0])


if __name__ == "__main__":
    unittest.main()
