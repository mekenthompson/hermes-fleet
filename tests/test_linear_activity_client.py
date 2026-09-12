"""Contract test for Linear Agent Activity GraphQL requests."""
from __future__ import annotations

import json
import sys
import unittest
import urllib.error
from pathlib import Path
from typing import cast

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins" / "linear-agent"))
from linear_activity import LinearActivityClient, waiting_unblock_comment_accepted

class FakeIssueApi:
    def __init__(self) -> None:
        self.updates: list[dict[str, object]] = []
        self.comments: list[dict[str, object]] = []
        self.comment_success = True
        self.state = {"id": "s-todo", "name": "Todo", "type": "unstarted"}

    def __call__(self, _url: str, _headers: dict[str, str], body: bytes) -> bytes:
        request = json.loads(body)
        query = request["query"]
        if "LinearIssueWorkflow" in query:
            return json.dumps({"data": {"issue": {
                "id": request["variables"]["id"],
                "state": self.state,
                "delegate": None,
                "children": {"pageInfo": {"hasNextPage": False}, "nodes": []},
                "team": {"states": {"nodes": [
                    {"id": "s-progress", "name": "In Progress", "type": "started"},
                    {"id": "s-waiting", "name": "Waiting on Principal", "type": "started"},
                    {"id": "s-review", "name": "In Review", "type": "started"},
                    {"id": "s-done", "name": "Done", "type": "completed"},
                ]}},
            }}}).encode()
        if "LinearIssueActor" in query:
            return b'{"data":{"viewer":{"id":"agent-1"}}}'
        if "issueUpdate" in query:
            self.updates.append(request["variables"])
            return b'{"data":{"issueUpdate":{"success":true}}}'
        if "commentCreate" in query:
            self.comments.append(request["variables"])
            success = "true" if self.comment_success else "false"
            return f'{{"data":{{"commentCreate":{{"success":{success}}}}}}}' .encode()
        raise AssertionError(query)


class LinearActivityClientTests(unittest.TestCase):
    def test_readiness_probe_is_read_only_and_authenticated(self) -> None:
        requests = []

        def transport(_url, headers, body):
            requests.append((headers, json.loads(body)))
            return b'{"data":{"viewer":{"id":"viewer-id"}}}'

        LinearActivityClient("test-token", transport=transport).verify_authenticated()
        self.assertEqual(requests[0][0]["Authorization"], "Bearer test-token")
        self.assertIn("viewer", requests[0][1]["query"])
        self.assertNotIn("mutation", requests[0][1]["query"].lower())

    def test_emits_documented_thought_activity_shape(self) -> None:
        seen: dict[str, object] = {}

        def transport(url: str, headers: dict[str, str], body: bytes) -> bytes:
            seen.update(url=url, headers=headers, body=json.loads(body))
            return b'{"data":{"agentActivityCreate":{"success":true}}}'

        LinearActivityClient("test-token", transport=transport).emit("session-1", "thought", "Starting work")
        self.assertEqual(seen.get("url"), "https://api.linear.app/graphql")
        headers = cast(dict[str, str], seen["headers"])
        body = cast(dict[str, object], seen["body"])
        self.assertEqual(headers["Authorization"], "Bearer test-token")
        self.assertEqual(
            cast(dict[str, object], body["variables"])["input"],
            {"agentSessionId": "session-1", "content": {"type": "thought", "body": "Starting work"}, "ephemeral": True},
        )

    def test_dispatch_preserves_agent_activity_operations(self) -> None:
        requests = []

        def transport(_url: str, _headers: dict[str, str], body: bytes) -> bytes:
            requests.append(json.loads(body))
            return b'{"data":{"agentActivityCreate":{"success":true}}}'

        self.assertTrue(LinearActivityClient("test-token", transport=transport).dispatch("session-1", "response", "Finished"))
        self.assertEqual(requests[0]["variables"]["input"], {
            "agentSessionId": "session-1", "content": {"type": "response", "body": "Finished"}
        })

    def test_comment_operation_adds_a_normal_issue_comment(self) -> None:
        requests = []

        def transport(_url: str, _headers: dict[str, str], body: bytes) -> bytes:
            requests.append(json.loads(body))
            return b'{"data":{"commentCreate":{"success":true}}}'

        self.assertTrue(LinearActivityClient("test-token", transport=transport).dispatch(
            "issue-1", "issue_comment", "### Agent session summary\n\nFinished the work."
        ))
        self.assertEqual(requests[0]["variables"]["input"], {
            "issueId": "issue-1", "body": "### Agent session summary\n\nFinished the work."
        })

    def test_project_update_delegates_one_resolved_primary_issue_to_native_publisher(self) -> None:
        calls = []

        def publisher(graphql, session_key, issues, *, configured_workspace):
            calls.append({
                "graphql": graphql,
                "session_key": session_key,
                "issues": issues,
                "configured_workspace": configured_workspace,
            })

        body = json.dumps({
            "session_key": "linear:workspace:session-1:closeout",
            "summary": "### Agent session summary\n\nFinished safely.",
        }, separators=(",", ":"))
        self.assertTrue(LinearActivityClient(
            "test-token",
            transport=lambda *_: self.fail("project updates use the native publisher"),
            project_update_publisher=publisher,
        ).dispatch("issue-1", "project_update", body))
        self.assertEqual(len(calls), 1)
        self.assertTrue(callable(calls[0].pop("graphql")))
        self.assertEqual(calls, [{
            "session_key": "linear:workspace:session-1:closeout",
            "issues": [{
                "issue_id": "issue-1",
                "summary": "### Agent session summary\n\nFinished safely.",
            }],
            "configured_workspace": None,
        }])

    def test_project_update_with_no_project_is_an_ordinary_skip(self) -> None:
        from linear_project_updates import NoProjectUpdate
        def no_project(*_args, **_kwargs):
            raise NoProjectUpdate("source issue TEAM-503 has no project")
        self.assertFalse(LinearActivityClient("test-token", project_update_publisher=no_project).dispatch(
            "TEAM-503", "project_update", json.dumps({"session_key": "closeout", "summary": "Done."})
        ))

    def test_project_update_rejects_malformed_payload_without_native_publish(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid Linear project update payload"):
            LinearActivityClient(
                "test-token", project_update_publisher=object()
            ).dispatch("issue-1", "project_update", "not-json")

    def test_failure_is_local_noop(self) -> None:
        client = LinearActivityClient(
            "test-token", transport=lambda *_: self.fail("failure must not call Linear")
        )
        self.assertTrue(client.dispatch("issue-1", "issue_status", "failure"))

    def test_waiting_comment_contract_rejects_placeholders(self) -> None:
        self.assertTrue(waiting_unblock_comment_accepted("### Blocker\n- Approve the 8G CI VM cap."))
        self.assertFalse(waiting_unblock_comment_accepted("waiting"))
        self.assertFalse(waiting_unblock_comment_accepted("### Blocker\n- none"))
        self.assertFalse(waiting_unblock_comment_accepted("### Blocker\n"))
        self.assertFalse(waiting_unblock_comment_accepted(None))

    def test_handoff_waiting_without_unblock_is_refused(self) -> None:
        api = FakeIssueApi()
        with self.assertRaisesRegex(ValueError, "Waiting on Principal requires an unblock comment"):
            LinearActivityClient("test-token", transport=api).dispatch(
                "issue-1", "issue_handoff", '{"state":"waiting"}'
            )
        self.assertEqual(api.updates, [])
        self.assertEqual(api.comments, [])

    def test_issue_status_waiting_is_refused_without_handoff_comment(self) -> None:
        api = FakeIssueApi()
        with self.assertRaisesRegex(ValueError, "issue_handoff with an unblock comment"):
            LinearActivityClient("test-token", transport=api).dispatch("issue-1", "issue_status", "waiting")
        self.assertEqual(api.updates, [])
        self.assertEqual(api.comments, [])

    def test_waiting_sets_waiting_on_ken_after_unblock_comment(self) -> None:
        api = FakeIssueApi()
        api.state = {"id": "s-progress", "name": "In Progress", "type": "started"}
        body = json.dumps({"state": "waiting", "unblock": "### Blocker\n- Approve the 8G CI VM cap."})
        self.assertTrue(LinearActivityClient("test-token", transport=api).dispatch("issue-1", "issue_handoff", body))
        self.assertEqual(api.comments, [{
            "input": {"issueId": "issue-1", "body": "### Blocker\n- Approve the 8G CI VM cap."},
        }])
        self.assertEqual(api.updates, [{"id": "issue-1", "input": {"stateId": "s-waiting"}}])
        self.assertNotIn("assigneeId", api.updates[0]["input"])
        self.assertNotIn("delegateId", api.updates[0]["input"])

    def test_waiting_without_named_state_still_posts_unblock_comment(self) -> None:
        api = FakeIssueApi()
        api.state = {"id": "s-progress", "name": "In Progress", "type": "started"}

        def transport(_url: str, _headers: dict[str, str], body: bytes) -> bytes:
            request = json.loads(body)
            query = request["query"]
            if "LinearIssueWorkflow" in query:
                return json.dumps({"data": {"issue": {
                    "id": request["variables"]["id"],
                    "state": api.state,
                    "delegate": {"id": "agent-1"},
                    "team": {"states": {"nodes": [
                        {"id": "s-progress", "name": "In Progress", "type": "started"},
                        {"id": "s-done", "name": "Done", "type": "completed"},
                    ]}},
                }}}).encode()
            if "commentCreate" in query:
                api.comments.append(request["variables"])
                return b'{"data":{"commentCreate":{"success":true}}}'
            if "issueUpdate" in query:
                api.updates.append(request["variables"])
                return b'{"data":{"issueUpdate":{"success":true}}}'
            raise AssertionError(query)

        body = json.dumps({"state": "waiting", "unblock": "### Blocker\n- Name the missing Waiting on Principal state."})
        self.assertTrue(LinearActivityClient("test-token", transport=transport).dispatch("issue-1", "issue_handoff", body))
        self.assertEqual(api.comments, [{
            "input": {"issueId": "issue-1", "body": "### Blocker\n- Name the missing Waiting on Principal state."},
        }])
        self.assertEqual(api.updates, [])

    def test_waiting_does_not_change_state_when_comment_is_rejected(self) -> None:
        api = FakeIssueApi()
        api.state = {"id": "s-progress", "name": "In Progress", "type": "started"}
        api.comment_success = False
        body = json.dumps({"state": "waiting", "unblock": "### Blocker\n- Approve the 8G CI VM cap."})
        with self.assertRaisesRegex(RuntimeError, "Linear rejected issue comment"):
            LinearActivityClient("test-token", transport=api).dispatch("issue-1", "issue_handoff", body)
        self.assertEqual(len(api.comments), 1)
        self.assertEqual(api.updates, [])

    def test_already_waiting_does_not_repost_unblock_comment(self) -> None:
        api = FakeIssueApi()
        api.state = {"id": "s-waiting", "name": "Waiting on Principal", "type": "started"}
        body = json.dumps({"state": "waiting", "unblock": "### Blocker\n- Approve the 8G CI VM cap."})
        self.assertTrue(LinearActivityClient("test-token", transport=api).dispatch("issue-1", "issue_handoff", body))
        self.assertEqual(api.comments, [])
        self.assertEqual(api.updates, [])

    def test_active_sets_in_progress_and_self_delegate_never_assignee(self) -> None:
        api = FakeIssueApi()
        self.assertTrue(LinearActivityClient("test-token", transport=api).dispatch("issue-1", "issue_status", "active"))
        self.assertEqual(api.updates, [{"id": "issue-1", "input": {"stateId": "s-progress", "delegateId": "agent-1"}}])
        self.assertNotIn("assigneeId", api.updates[0]["input"])

    def test_done_sets_completed_without_touching_assignee_or_delegate(self) -> None:
        api = FakeIssueApi()
        self.assertTrue(LinearActivityClient("test-token", transport=api).dispatch("issue-1", "issue_status", "done"))
        self.assertEqual(api.updates, [{"id": "issue-1", "input": {"stateId": "s-done"}}])

    def test_handoff_sets_review_and_ignores_assignee_payload(self) -> None:
        api = FakeIssueApi()
        self.assertTrue(LinearActivityClient("test-token", transport=api).dispatch(
            "issue-1",
            "issue_handoff",
            json.dumps({"state": "review", "assigneeId": "human-2", "clearDelegate": True}),
        ))
        self.assertEqual(api.updates, [{"id": "issue-1", "input": {"stateId": "s-review"}}])
        self.assertNotIn("assigneeId", api.updates[0]["input"])
        self.assertNotIn("delegateId", api.updates[0]["input"])

    def test_rejects_unknown_lifecycle_operation_without_network_io(self) -> None:
        with self.assertRaises(ValueError):
            LinearActivityClient("test-token", transport=lambda *_: self.fail("must not call Linear")).dispatch(
                "issue-1", "issue_status", "completed"
            )

    def test_rejects_action_without_action_payload_shape(self) -> None:
        with self.assertRaises(ValueError):
            LinearActivityClient("test-token", transport=lambda *_: b"{}").emit("session-1", "action", "not valid")

    def test_http_401_invalidates_and_retries_exactly_once(self) -> None:
        class Tokens:
            def __init__(self): self.invalidations = 0
            def __call__(self): return "fresh" if self.invalidations else "expired"
            def invalidate(self): self.invalidations += 1
        tokens = Tokens()
        seen = []

        def transport(_url, headers, _body):
            seen.append(headers["Authorization"])
            if len(seen) == 1:
                raise urllib.error.HTTPError("https://api.linear.app/graphql", 401, "unauthorized", {}, None)
            return b'{"data":{"agentActivityCreate":{"success":true}}}'

        LinearActivityClient(tokens, transport=transport).emit("session-1", "thought", "Starting")
        self.assertEqual(seen, ["Bearer expired", "Bearer fresh"])
        self.assertEqual(tokens.invalidations, 1)


if __name__ == "__main__":
    unittest.main()
