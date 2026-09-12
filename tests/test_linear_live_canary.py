from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from threading import Event
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "plugins" / "linear-agent" / "linear_live_canary.py"
PLUGIN = SCRIPT.parent


def load_canary():
    sys.path.insert(0, str(PLUGIN))
    spec = importlib.util.spec_from_file_location("linear_live_canary", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_completed_worker_state(path: Path, session_id: str, *, status_state: str = "sent") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"session_key": "linear:workspace-canary:session-canary:delivery-canary:closeout", "summary": "CANARY_OK"}
    with sqlite3.connect(path) as conn:
        conn.executescript(
            "CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, payload BLOB, state TEXT);"
            "CREATE TABLE outbox (delivery_id TEXT, kind TEXT, linear_session_id TEXT, body TEXT, state TEXT);"
        )
        conn.execute("INSERT INTO deliveries VALUES (?, ?, 'completed')", (
            "delivery-canary", json.dumps({"agentSession": {"id": session_id}}).encode()))
        conn.executemany("INSERT INTO outbox VALUES (?, ?, ?, ?, ?)", [
            ("delivery-canary", "thought", session_id, "thinking", "sent"),
            ("delivery-canary", "issue_status_waiting", "9e3fe8a4-8602-401a-8b34-8d0f1c047068", "waiting", status_state),
            ("delivery-canary", "issue_status_active", "9e3fe8a4-8602-401a-8b34-8d0f1c047068", "active", status_state),
            ("delivery-canary", "response", session_id, "CANARY_OK", "sent"),
            ("delivery-canary", "issue_comment", "9e3fe8a4-8602-401a-8b34-8d0f1c047068", "### Agent session summary\n\nCANARY_OK", "sent"),
            ("delivery-canary", "project_update", "9e3fe8a4-8602-401a-8b34-8d0f1c047068", json.dumps(payload), "sent"),
            ("delivery-canary", "issue_status_done", "9e3fe8a4-8602-401a-8b34-8d0f1c047068", "done", status_state),
        ])


class FakeLinear:
    def __init__(self, *, update: object = "valid", changed_state: bool = False) -> None:
        self.update = update
        self.changed_state = changed_state
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.issue_reads = 0

    def __call__(self, query: str, variables: dict[str, object]) -> dict[str, object]:
        self.calls.append((query, variables))
        if "agentSessionCreateOnIssue" in query:
            return {"data": {"agentSessionCreateOnIssue": {"success": True, "agentSession": {"id": "session-canary"}}}}
        if "ProjectUpdatesViewer" in query:
            return {"data": {"viewer": {"id": "actor-canary", "app": True, "organization": {"id": "workspace-canary"}}}}
        if "ProjectUpdateLookup" in query:
            if self.update == "missing":
                return {"data": {"projectUpdate": None}}
            update_id = variables["id"]
            body = "### [TEAM-999](https://linear.app/issue/TEAM-999)\nCANARY_OK" if self.update != "wrong" else "wrong body"
            return {"data": {"projectUpdate": {"id": update_id, "body": body, "health": "offTrack" if self.update == "wrong_health" else "atRisk", "url": f"https://linear.app/update/{update_id}", "project": {"id": "project-canary"}, "user": {"id": "actor-canary"}}}}
        if "ProjectUpdateIssue" in query:
            return {"data": {"issue": {"id": "9e3fe8a4-8602-401a-8b34-8d0f1c047068", "identifier": "TEAM-999", "url": "https://linear.app/issue/TEAM-999", "project": {"id": "project-canary"}, "team": {"organization": {"id": "workspace-canary"}}}}}
        self.issue_reads += 1
        if self.changed_state and self.issue_reads > 1:
            state = {"id": "canceled-1", "name": "Canceled", "type": "canceled"}
        elif self.issue_reads > 1:
            state = {"id": "done-1", "name": "Done", "type": "completed"}
        else:
            state = {"id": "todo-1", "name": "Todo", "type": "unstarted"}
        return {"data": {"issue": {
            "id": "9e3fe8a4-8602-401a-8b34-8d0f1c047068",
            "state": state,
            "project": {"id": "project-canary", "health": "atRisk"},
            "comments": {"nodes": [{"body": "### Agent session summary\n\nCANARY_OK"}] if self.issue_reads > 1 and self.update != 'missing_comment' else []},
        }}}


class LinearLiveCanaryTests(unittest.TestCase):
    def test_pre_cancelled_canary_performs_no_graphql_mutation(self) -> None:
        canary = load_canary()
        cancelled = Event()
        cancelled.set()
        graphql = mock.Mock()
        with self.assertRaisesRegex(RuntimeError, "cancelled"):
            canary.run_canary(
                issue_id="9e3fe8a4-8602-401a-8b34-8d0f1c047068", database=Path("/unused"),
                timeout=30, graphql=graphql, cancel_event=cancelled,
            )
        graphql.assert_not_called()

    def test_requires_sent_statuses_and_done_issue_with_exact_external_native_update(self) -> None:
        canary = load_canary()
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "state.db"
            write_completed_worker_state(database, "session-canary")
            api = FakeLinear()
            result = canary.run_canary(issue_id="9e3fe8a4-8602-401a-8b34-8d0f1c047068", database=database, timeout=30, graphql=api,
                                       now=iter([0.0, 1.0]).__next__, sleep=lambda _seconds: None)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["project_id"], "project-canary")
        self.assertTrue(result["project_update_url"].startswith("https://linear.app/update/"))
        self.assertIn("project_update_id", result)
        self.assertFalse(any("ProjectUpdateCreate" in query for query, _ in api.calls))

    def test_fails_when_statuses_are_not_sent(self) -> None:
        canary = load_canary()
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "state.db"
            write_completed_worker_state(database, "session-canary", status_state="suppressed")
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                canary.run_canary(issue_id="9e3fe8a4-8602-401a-8b34-8d0f1c047068", database=database, timeout=30, graphql=FakeLinear(),
                                   now=iter([0.0, 11.0, 31.0]).__next__, sleep=lambda _seconds: None)

    def test_missing_or_wrong_external_update_fails_even_when_local_row_is_sent(self) -> None:
        canary = load_canary()
        for update in ("missing", "wrong", "missing_comment", "wrong_health"):
            with self.subTest(update=update), tempfile.TemporaryDirectory() as temp:
                database = Path(temp) / "state.db"
                write_completed_worker_state(database, "session-canary")
                with self.assertRaisesRegex(RuntimeError, "timed out"):
                    canary.run_canary(issue_id="9e3fe8a4-8602-401a-8b34-8d0f1c047068", database=database, timeout=30, graphql=FakeLinear(update=update),
                                       now=iter([0.0, 11.0, 31.0]).__next__, sleep=lambda _seconds: None)

    def test_fails_when_issue_state_changes(self) -> None:
        canary = load_canary()
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "state.db"
            write_completed_worker_state(database, "session-canary")
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                canary.run_canary(issue_id="9e3fe8a4-8602-401a-8b34-8d0f1c047068", database=database, timeout=30, graphql=FakeLinear(changed_state=True),
                                   now=iter([0.0, 11.0, 31.0]).__next__, sleep=lambda _seconds: None)

    def test_make_graphql_preserves_structured_missing_update_error(self) -> None:
        canary = load_canary()
        from linear_activity import LinearGraphQLError
        missing = {"message": "Entity not found: ProjectUpdate", "path": ["projectUpdate"],
                   "extensions": {"code": "INPUT_ERROR", "type": "invalid input"}}
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        response.read.return_value = json.dumps({"errors": [missing]}).encode()
        with mock.patch("linear_activity.urllib.request.urlopen", return_value=response):
            with self.assertRaises(LinearGraphQLError) as raised:
                canary.make_graphql(lambda: "token")("query ProjectUpdateLookup { projectUpdate(id: \"x\") { id } }", {})
        self.assertEqual(raised.exception.errors, [missing])


if __name__ == "__main__":
    unittest.main()
