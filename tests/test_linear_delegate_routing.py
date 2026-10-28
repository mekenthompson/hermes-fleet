"""Own-app delegate routing reuses an existing native task without changing execution."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import uuid

from linear_fake_api import Clock, FakeLinear, OTHER, SELF, load_plugin

load_plugin()
from hermes_fleet_linear_plugin import chat
from hermes_fleet_linear_plugin.api import LinearAPI, LinearError, RateLimited
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.store import Store
from hermes_fleet_linear_plugin.transport import NativeContext


class InspectedBoard:
    executor_profile = "default"
    board = "default"

    def __init__(self):
        self.task = SimpleNamespace(id="existing-remainder", body="", status="blocked", assignee="default")

    def require_task(self, task_id):
        if task_id != self.task.id:
            raise LinearError("Unknown native task", retryable=False)
        return self.task

    def history(self, task_id, after_id):
        self.require_task(task_id)
        return []  # This inspected, unmodified task has no specification events.


class DelegateRoutingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.clock = Clock()
        self.remote = FakeLinear(self.clock)
        self.addCleanup(self.remote.close)
        issue = self.remote.add_issue("issue-1", "ABC-1", project=None)
        issue["assignee"] = {"id": "human-assignee"}
        self.remote.set_delegate("issue-1", OTHER)
        self.api = LinearAPI(lambda: "synthetic", endpoint=self.remote.url)
        self.board = InspectedBoard()
        self.store = Store(self.home / "linear.db")
        self.bridge = Bridge(self.store, self.api, self.board, profile="alpha", clock=self.clock)
        self.bridge.profile_home = self.home
        self.bridge.track("issue-1", self.board.task.id)
        self.context = NativeContext(home=str(self.home), session_key="chat", session_id="session", profile="alpha",
                                     platform="slack", source="slack")
        self.request = {"action": "delegate_self", "issue": "ABC-1", "id": str(uuid.uuid4()),
                        "expected_delegate_id": OTHER["id"], "expected_updated_at": issue["updatedAt"],
                        "expected_binding_revision": self.store.binding_revision("issue-1")}

    def invoke(self, request=None):
        return json.loads(chat.handle(self.bridge, request or self.request, self.context))

    def row(self):
        return self.store.outbox_row(self.request["id"])

    def test_delegate_update_preserves_human_and_native_work(self):
        before = copy.deepcopy(self.remote.issues["issue-1"])
        native = vars(self.board.task).copy()
        self.assertFalse(self.bridge._report_delegate_allowed(self.api.issue("issue-1")))
        queued = self.invoke()
        self.assertTrue(queued["ok"], queued)
        self.assertEqual(self.remote.issues["issue-1"]["delegate"], OTHER)
        self.assertTrue(self.bridge._send(self.row()))
        current = self.remote.issues["issue-1"]
        self.assertEqual(current["delegate"]["id"], SELF)
        for key in ("assignee", "state", "description", "title", "project"):
            self.assertEqual(current[key], before[key], key)
        self.assertEqual(vars(self.board.task), native)
        self.assertEqual(self.store.native_binding("issue-1")["task_id"], native["id"])
        self.assertTrue(self.bridge._report_delegate_allowed(self.api.issue("issue-1")))

    def test_new_remote_delegate_does_not_get_overwritten(self):
        queued = self.invoke()
        self.assertTrue(queued["ok"], queued)
        self.remote.set_delegate("issue-1", {"id": "new-human-selected-agent"})
        with self.assertRaises(LinearError):
            self.bridge._send(self.row())
        self.assertEqual(self.remote.issues["issue-1"]["delegate"]["id"], "new-human-selected-agent")
        self.assertFalse(any(query.startswith("mutation IssueUpdate") for query in self.remote.requests))

    def test_missing_binding_is_a_scoped_refusal(self):
        self.request["issue"] = "ABC-2"
        self.remote.add_issue("issue-2", "ABC-2", project=None)
        result = self.invoke()
        self.assertFalse(result["ok"], result)
        self.assertIsNone(self.row())
        self.assertFalse(any(query.startswith("mutation IssueUpdate") for query in self.remote.requests))

    def test_request_identity_cannot_be_reused_with_different_routing_preimage(self):
        self.assertTrue(self.invoke()["ok"])
        changed = {**self.request, "expected_delegate_id": None}
        self.assertFalse(self.invoke(changed)["ok"])
        self.assertEqual(self.row()["payload"]["expected_delegate_id"], OTHER["id"])

    def test_lost_response_then_new_delegate_holds_new_requests_and_can_be_reconciled(self):
        self.assertTrue(self.invoke()["ok"])
        self.remote.lose_next_response = True
        with self.assertRaises(LinearError):
            self.bridge._send(self.row())
        self.remote.set_delegate("issue-1", {"id": "new-human-selection"})
        with self.assertRaises(LinearError):
            self.bridge._send(self.row())
        self.assertTrue(self.store.issue_reconciliation_blocked("issue-1"))
        new = {**self.request, "id": str(uuid.uuid4()), "expected_delegate_id": "new-human-selection"}
        self.assertFalse(self.invoke(new)["ok"])
        self.assertTrue(self.store.reconcile_terminal(self.request["id"], outcome="applied",
            evidence="Saw the app routing change before later human reassignment", at=self.clock()))
        self.assertFalse(self.store.issue_reconciliation_blocked("issue-1"))
        self.assertEqual(self.remote.issues["issue-1"]["delegate"]["id"], "new-human-selection")

    def test_known_401_refresh_rechecks_routing_and_attempts_once_more(self):
        class Token:
            def __init__(self):
                self.refreshes = 0
            def __call__(self):
                return "synthetic"
            def invalidate(self):
                self.refreshes += 1
        token = Token()
        attempts = []
        def transport(endpoint, body, headers):
            request = json.loads(body)
            if request["query"].startswith("mutation IssueUpdate"):
                attempts.append(request["variables"]["input"])
                if len(attempts) == 1:
                    return 401, {}, b"{}"
            status, response_headers, payload = self.remote.handle(request["query"], request["variables"])
            return status, response_headers, json.dumps(payload).encode()
        self.bridge.api = LinearAPI(token, transport=transport, clock=self.clock)
        self.assertTrue(self.invoke()["ok"])
        self.assertTrue(self.bridge._send(self.row()))
        self.assertEqual(token.refreshes, 1)
        self.assertEqual(attempts, [{"delegateId": SELF}] * 2)
        self.assertTrue(self.row()["payload"]["delegate_receipt"])

    def test_rate_limit_at_mutation_is_known_not_applied_then_can_deliver(self):
        attempts = []
        def transport(endpoint, body, headers):
            request = json.loads(body)
            if request["query"].startswith("mutation IssueUpdate"):
                attempts.append(request)
                if len(attempts) == 1:
                    return 429, {"Retry-After": "1"}, b"{}"
            status, response_headers, payload = self.remote.handle(request["query"], request["variables"])
            return status, response_headers, json.dumps(payload).encode()
        self.bridge.api = LinearAPI(lambda: "synthetic", transport=transport, clock=self.clock)
        self.assertTrue(self.invoke()["ok"])
        with self.assertRaises(RateLimited):
            self.bridge._send(self.row())
        self.assertIs(self.row()["payload"]["write_started"], False)
        self.assertEqual(self.remote.issues["issue-1"]["delegate"], OTHER)
        self.clock.now += 2
        self.assertTrue(self.bridge._send(self.row()))
        self.assertEqual(self.remote.issues["issue-1"]["delegate"]["id"], SELF)
