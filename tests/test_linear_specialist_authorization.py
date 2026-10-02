"""Adversarial coverage for the opt-in specialist authorization boundary."""
from __future__ import annotations

import json
import sqlite3
import threading
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from linear_fake_api import load_plugin
from linear_ingress_fixture import IngressStore, Route

plugin = load_plugin()
from hermes_fleet_linear_plugin.api import LinearError  # noqa: E402
from hermes_fleet_linear_plugin.bridge import Bridge  # noqa: E402
from hermes_fleet_linear_plugin.store import Store  # noqa: E402

VIEWER = "app-allowed"
ORG = "org-allowed"
TEAM = "team-allowed"
PROJECT = "project-allowed"
USER = "user-allowed"
ISSUE = "issue-allowed"
SESSION = "session-allowed"
ACTIVITY = "activity-allowed"
SCOPE = {
    "allowed_team_ids": [TEAM],
    "allowed_project_ids": [PROJECT],
    "allowed_requester_ids": [USER],
}


def issue_record(**changes):
    issue = {
        "id": ISSUE,
        "identifier": "OPS-1",
        "title": "Synthetic issue",
        "description": "Synthetic issue body",
        "url": "https://linear.example/issue/OPS-1",
        "updatedAt": "2026-01-01T00:00:00Z",
        "creator": {"id": USER},
        "delegate": None,
        "state": {"id": "state-todo", "name": "Todo", "type": "unstarted"},
        "project": {"id": PROJECT},
        "team": {"id": TEAM, "key": "OPS", "states": {"nodes": [
            {"id": "state-todo", "name": "Todo", "type": "unstarted"},
            {"id": "state-progress", "name": "In Progress", "type": "started"},
            {"id": "state-blocked", "name": "Blocked", "type": "started"},
            {"id": "state-done", "name": "Done", "type": "completed"},
        ]}},
    }
    issue.update(changes)
    return issue


class Authority:
    """Authoritative API fixture independent from webhook payload fields."""

    def __init__(self):
        self.viewer = VIEWER
        self.organization = ORG
        self.issues = {ISSUE: issue_record()}
        self.sessions = {SESSION: {"id": SESSION, "issue": {"id": ISSUE}, "creator": {"id": USER}}}
        self.activities = {ACTIVITY: {"id": ACTIVITY, "user": {"id": USER},
                                      "agentSession": {"id": SESSION}}}
        self.requests: list[str] = []
        self.mutations: list[str] = []
        self.fail_session = False
        self.fail_issue = False

    def transport(self, _url, body, _headers):
        payload = json.loads(body)
        query, variables = payload["query"], payload.get("variables") or {}
        self.requests.append(query)
        if "IdentityBinding" in query:
            data = {"viewer": {"id": self.viewer}, "organization": {"id": self.organization}}
        elif "query AgentSessionAuthorization" in query:
            if self.fail_session:
                return 503, {}, json.dumps({"errors": [{"message": "fixture unavailable"}]}).encode()
            data = {"agentSession": self.sessions.get(variables["id"])}
        elif "query AgentActivityAuthorization" in query:
            data = {"agentActivity": self.activities.get(variables["id"])}
        elif "query Issue(" in query:
            if self.fail_issue:
                return 503, {}, json.dumps({"errors": [{"message": "fixture unavailable"}]}).encode()
            data = {"issue": self.issues.get(variables["id"])}
        elif query.lstrip().startswith("mutation"):
            self.mutations.append(query)
            field = next((name for name in ("issueUpdate", "commentCreate", "agentActivityCreate",
                                            "projectUpdateCreate") if name in query), None)
            data = {field: {"success": True}} if field else {}
        else:
            data = {"organization": {"id": ORG}}
        return 200, {}, json.dumps({"data": data}).encode()

    def api(self, *, scope=SCOPE, identity=None):
        return plugin.BoundLinearAPI(
            lambda: "synthetic", identity=identity or {"viewer_id": VIEWER, "organization_id": ORG},
            specialist_scope=scope, transport=self.transport)


class FakeKanban:
    def __init__(self):
        self.creates = []
        self.subscriptions = []
        self.tasks = {}
        self.histories = {}
        self.event_calls = []
        self.comments = []

    def create(self, **fields):
        self.creates.append(fields)
        task = SimpleNamespace(id="task-1", status="blocked" if fields.get("initial_status") == "blocked" else "ready", completion_contract=None,
                               result="", title=fields.get("title", "Synthetic task"))
        self.tasks[task.id] = task
        if task.status == "blocked":
            self.histories[task.id] = [SimpleNamespace(id=1, kind="blocked", payload={"reason": "initial_status"})]
        return task

    def get(self, task_id):
        return self.tasks.get(task_id)

    def events(self, task_id, issue_id):
        self.event_calls.append((task_id, issue_id))
        return []

    def history(self, task_id, after_id):
        return [event for event in self.histories.get(task_id, []) if event.id > after_id]

    def evidence_text(self, _task):
        return ""

    def subscribe(self, task_id, issue_id):
        self.subscriptions.append((task_id, issue_id))

    def comment(self, task_id, body):
        self.comments.append((task_id, body))

    def unblock(self, task_id):
        self.tasks[task_id].status = "ready"
        return True

    def archive(self, task_id):
        self.tasks[task_id].status = "archived"

    def block(self, task_id, _reason):
        self.tasks[task_id].status = "blocked"


class LinearSpecialistAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.authority = Authority()
        self.kanban = FakeKanban()
        self.store = Store(Path(self.tmp.name) / "state.db")
        self.api = self.authority.api()
        self.bridge = Bridge(self.store, self.api, self.kanban, profile="synthetic")

    def seed_kanban_work(self, issue_id=ISSUE, *, task_status="ready", events=()):
        task_id = f"task-{issue_id}"
        self.kanban.tasks[task_id] = SimpleNamespace(id=task_id, status=task_status,
                                                     completion_contract=None, result="", title="OPS-1: task")
        self.kanban.histories[task_id] = list(events)
        self.store.put(issue_id, "kanban", SESSION, task_id=task_id, project_id=PROJECT)
        return task_id

    def session_event(self, action="created", *, event_issue=ISSUE, creator=USER,
                      user=USER, event_team=TEAM, event_project=PROJECT, event_user=USER):
        event = {
            "type": "AgentSessionEvent",
            "action": action,
            "createdAt": "2026-01-01T00:00:00Z",
            "agentSession": {
                "id": SESSION,
                "creatorId": creator,
                "issue": {"id": event_issue, "team": {"id": event_team},
                          "project": {"id": event_project}, "creator": {"id": event_user},
                          "title": "Untrusted webhook title"},
            },
            "promptContext": "untrusted webhook context",
        }
        if action == "prompted":
            event["agentActivity"] = {"id": ACTIVITY,
                                      "user": {"id": user},
                                      "content": {"type": "prompt", "body": "untrusted prompt"}}
        return event

    def test_status_send_refuses_second_lookup_redirect(self):
        other = "issue-other"
        self.authority.issues[other] = issue_record(id=other)
        row_id = self.bridge.status(ISSUE, "in_progress", claim=True)
        original = self.api.issue
        calls = 0
        def redirected(ref):
            nonlocal calls
            if ref == ISSUE:
                calls += 1
                if calls == 2:
                    return original(other)
            return original(ref)
        self.api.issue = redirected
        self.assertEqual(self.bridge.flush(), 0)
        self.assertEqual(self.authority.mutations, [])
        self.assertTrue(self.store.scope_fenced(ISSUE))
        self.assertFalse(self.store.scope_fenced(other))
        self.assertEqual(self.store.outbox_row(row_id)["state"], "pending")

    def test_created_scope_denial_fences_authoritative_issue_across_replay(self):
        self.authority.issues[ISSUE] = issue_record(project={"id": "foreign-project"})
        event = self.session_event()
        self.bridge.handle_webhook(event)
        self.assertTrue(self.store.scope_fenced(ISSUE))
        self.authority.issues[ISSUE] = issue_record()
        self.bridge.handle_webhook(event)
        self.assertEqual(self.kanban.creates, [])
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.store.pending(), [])

    def test_project_send_refuses_second_lookup_redirect(self):
        other = "issue-other"
        self.authority.issues[ISSUE] = issue_record(delegate={"id": VIEWER})
        self.authority.issues[other] = issue_record(id=other, delegate={"id": VIEWER})
        self.bridge.project_update(SESSION, PROJECT, "OPS-1", "result", issue_id=ISSUE, quiet=False)
        before = self.store.pending()
        original = self.api.issue
        calls = 0
        def redirected(ref):
            nonlocal calls
            if ref == ISSUE:
                calls += 1
                if calls == 2:
                    return original(other)
            return original(ref)
        self.api.issue = redirected
        self.assertEqual(self.bridge.flush(), 0)
        self.assertEqual(self.authority.mutations, [])
        self.assertTrue(self.store.scope_fenced(ISSUE))
        self.assertFalse(self.store.scope_fenced(other))
        self.assertEqual(self.store.pending(), before)

    def seed_chat_stop(self):
        self.store.put(ISSUE, "chat", "chat-key", run_generation=1)
        self.bridge.handle_webhook(self.session_event())
        intent = self.store.capture_chat_stop(ISSUE, SESSION, ACTIVITY, "synthetic", at=1)
        self.assertIsNotNone(intent)
        return intent

    def test_chat_stop_fenced_before_gateway_calls_preserves_intent(self):
        import asyncio
        self.seed_chat_stop()
        before = self.store.stop_intents()
        self.store.fence_scope(ISSUE, "permanent denial", at=2)
        calls = []
        class Gateway:
            async def get_chat_run_stop_observation(inner, **kwargs):
                calls.append("observe")
                return {"status": "unknown"}
            async def request_chat_run_stop(inner, **kwargs):
                calls.append("stop")
                return {"status": "accepted", "worker_completion": "pending"}
        asyncio.run(plugin.process_chat_stops(self.bridge, SimpleNamespace(
            gateway=Gateway(), profile_home=str(self.tmp.name))))
        self.assertEqual(calls, [])
        self.assertEqual(self.store.stop_intents(), before)

    def test_chat_stop_fenced_during_observation_never_requests_stop(self):
        import asyncio
        self.seed_chat_stop()
        before = self.store.stop_intents()
        calls = []
        class Gateway:
            async def get_chat_run_stop_observation(inner, **kwargs):
                self.store.fence_scope(ISSUE, "permanent denial", at=2)
                return {"status": "unknown"}
            async def request_chat_run_stop(inner, **kwargs):
                calls.append("stop")
                return {"status": "accepted", "worker_completion": "pending"}
        asyncio.run(plugin.process_chat_stops(self.bridge, SimpleNamespace(
            gateway=Gateway(), profile_home=str(self.tmp.name))))
        self.assertEqual(calls, [])
        self.assertEqual(self.store.stop_intents(), before)

    def test_scope_contract_is_complete_and_closed(self):
        for scope in (
            {},
            {"allowed_team_ids": [TEAM], "allowed_project_ids": [PROJECT]},
            {"allowed_team_ids": [TEAM], "allowed_requester_ids": [USER]},
            {"allowed_project_ids": [PROJECT], "allowed_requester_ids": [USER]},
            {**SCOPE, "extra": ["not-accepted"]},
            {**SCOPE, "allowed_team_ids": []},
            {**SCOPE, "allowed_requester_ids": [" "]},
        ):
            with self.subTest(scope=scope), self.assertRaises(ValueError):
                plugin.BoundLinearAPI(lambda: self.fail("credentials must not be read"),
                                      identity={"viewer_id": VIEWER, "organization_id": ORG},
                                      specialist_scope=scope)

    def test_arbitrary_graphql_is_refused_without_network_access_in_specialist_mode(self):
        before = len(self.authority.requests)
        with self.assertRaises(LinearError):
            self.api.graphql("query Exfiltrate { users { nodes { id } } }")
        self.assertEqual(len(self.authority.requests), before)

    def test_general_mode_keeps_existing_arbitrary_graphql_semantics(self):
        general = self.authority.api(scope=None)
        self.assertEqual(general.graphql("query Generic { organization { id } }")["organization"]["id"], ORG)

    def test_issue_resolver_uses_authoritative_team_project_and_requester(self):
        cases = (
            {"team": {"id": "team-foreign", "key": "NO", "states": {"nodes": []}}},
            {"project": {"id": "project-foreign"}},
            {"creator": {"id": "user-foreign"}},
            {"creator": None},
            {"project": None},
        )
        for changes in cases:
            self.authority.issues[ISSUE] = issue_record(**changes)
            with self.subTest(changes=changes), self.assertRaises(LinearError):
                self.api.issue(ISSUE)
        self.authority.issues[ISSUE] = issue_record()
        self.assertEqual(self.api.issue(ISSUE)["id"], ISSUE)

    def test_mutations_refuse_unverified_issue_resource_ids(self):
        operations = (
            lambda: self.api.update_issue(ISSUE, {"stateId": "state-progress"}),
            lambda: self.api.create_comment("comment-1", ISSUE, "body"),
            lambda: self.api.create_activity("activity-1", SESSION, {"type": "response", "body": "body"}, issue_id=ISSUE),
            lambda: self.api.create_project_update("update-1", PROJECT, "body", issue_ids=[ISSUE]),
        )
        for resolved_id in (None, "", " ", "issue-foreign"):
            for index, operation in enumerate(operations):
                self.authority.issues[ISSUE] = issue_record(id=resolved_id)
                with self.subTest(resolved_id=resolved_id, operation=index), self.assertRaises(LinearError):
                    operation()
                self.assertEqual(self.authority.mutations, [])

    def test_session_ingress_denies_resolver_failure_foreign_issue_user_and_actor_before_task(self):
        probes = (
            ("session resolver", lambda: setattr(self.authority, "fail_session", True), False),
            ("issue resolver", lambda: setattr(self.authority, "fail_issue", True), False),
            ("foreign team", lambda: self.authority.issues.__setitem__(ISSUE, issue_record(
                team={"id": "team-foreign", "key": "NO", "states": {"nodes": []}})), False),
            ("foreign project", lambda: self.authority.issues.__setitem__(ISSUE, issue_record(
                project={"id": "project-foreign"})), False),
            ("foreign issue requester", lambda: self.authority.issues.__setitem__(ISSUE, issue_record(
                creator={"id": "user-foreign"})), False),
            ("foreign authoritative session creator", lambda: self.authority.sessions.__setitem__(SESSION,
                {"id": SESSION, "issue": {"id": ISSUE}, "creator": {"id": "user-foreign"}}), False),
            ("foreign payload actor", lambda: None, True),
        )
        for label, change, foreign_payload_actor in probes:
            self.authority.issues[ISSUE] = issue_record()
            self.authority.sessions[SESSION] = {"id": SESSION, "issue": {"id": ISSUE}, "creator": {"id": USER}}
            self.authority.fail_session = self.authority.fail_issue = False
            change()
            event = self.session_event(creator=USER if not foreign_payload_actor else "user-spoofed")
            if foreign_payload_actor:
                self.authority.sessions[SESSION] = {"id": SESSION, "issue": {"id": ISSUE},
                                                    "creator": {"id": "user-foreign"}}
            with self.subTest(reason=label):
                if label.endswith("resolver"):
                    with self.assertRaises(LinearError):
                        self.bridge.handle_webhook(event)
                else:
                    self.bridge.handle_webhook(event)
                self.assertEqual(self.kanban.creates, [])
                self.assertIsNone(self.store.get(ISSUE))
                self.assertEqual(self.authority.mutations, [])

    def test_spoofed_payload_cannot_override_authoritative_issue_or_requester(self):
        self.authority.issues[ISSUE] = issue_record(creator={"id": "user-foreign"})
        event = self.session_event(event_team=TEAM, event_project=PROJECT, event_user=USER)
        self.bridge.handle_webhook(event)
        self.assertEqual(self.kanban.creates, [])
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.authority.mutations, [])

    def test_prompted_activity_actor_and_session_issue_are_resolved_authoritatively(self):
        self.authority.activities[ACTIVITY] = {"id": ACTIVITY, "user": {"id": "user-foreign"},
                                              "agentSession": {"id": SESSION}}
        self.bridge.handle_webhook(self.session_event("prompted", user=USER))
        self.assertEqual(self.kanban.creates, [])
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.authority.mutations, [])

        self.authority.activities[ACTIVITY] = {"id": ACTIVITY, "user": {"id": USER},
                                              "agentSession": {"id": "session-foreign"}}
        self.bridge.handle_webhook(self.session_event("prompted", user=USER))
        self.assertEqual(self.kanban.creates, [])
        self.assertIsNone(self.store.get(ISSUE))

    def test_session_issue_id_must_match_authoritative_session(self):
        self.authority.sessions[SESSION] = {"id": SESSION, "issue": {"id": "issue-foreign"},
                                            "creator": {"id": USER}}
        self.bridge.handle_webhook(self.session_event(event_issue=ISSUE))
        self.assertEqual(self.kanban.creates, [])
        self.assertIsNone(self.store.get(ISSUE))

    def test_foreign_session_requester_fences_only_verified_issue_across_replay(self):
        forged = "issue-forged"
        self.authority.sessions[SESSION] = {"id": SESSION, "issue": {"id": ISSUE},
                                            "creator": {"id": "user-foreign"}}
        event = self.session_event(event_issue=forged)
        self.bridge.handle_webhook(event)
        self.assertTrue(self.store.scope_fenced(ISSUE))
        self.assertFalse(self.store.scope_fenced(forged))
        self.authority.sessions[SESSION]["creator"]["id"] = USER
        self.bridge.handle_webhook(event)
        self.bridge.handle_webhook(self.session_event())
        self.assertEqual(self.kanban.creates, [])
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.store.pending(), [])

    def test_verified_session_denials_fence_only_server_issue(self):
        forged = "issue-forged"
        cases = (
            ("issue mismatch", self.session_event(event_issue=forged), None),
            ("missing payload issue", self.session_event(event_issue=""), None),
            ("creator mismatch", self.session_event(creator="user-forged"), None),
            ("activity actor", self.session_event("prompted"),
             {"id": ACTIVITY, "user": {"id": "user-foreign"}, "agentSession": {"id": SESSION}}),
            ("activity session", self.session_event("prompted"),
             {"id": ACTIVITY, "user": {"id": USER}, "agentSession": {"id": "session-foreign"}}),
            ("payload activity actor", self.session_event("prompted", user="user-forged"),
             {"id": ACTIVITY, "user": {"id": USER}, "agentSession": {"id": SESSION}}),
            ("missing activity", self.session_event("prompted"), None),
        )
        for index, (name, event, activity) in enumerate(cases):
            with self.subTest(name=name):
                self.store = Store(Path(self.tmp.name) / f"denial-{index}.db")
                self.bridge = Bridge(self.store, self.api, self.kanban, profile="synthetic")
                self.authority.activities[ACTIVITY] = activity
                if name == "missing activity":
                    event["agentActivity"]["id"] = ""
                self.bridge.handle_webhook(event)
                self.assertTrue(self.store.scope_fenced(ISSUE))
                self.assertFalse(self.store.scope_fenced(forged))
                self.assertEqual(self.kanban.creates, [])
                self.assertEqual(self.store.pending(), [])

    def test_transient_session_and_activity_failures_do_not_fence(self):
        event = self.session_event("prompted")
        self.authority.fail_session = True
        with self.assertRaises(LinearError): self.bridge.handle_webhook(event)
        self.authority.fail_session = False
        original = self.api.agent_activity
        self.api.agent_activity = lambda *_: (_ for _ in ()).throw(LinearError("temporary activity outage"))
        try:
            with self.assertRaises(LinearError): self.bridge.handle_webhook(event)
        finally:
            self.api.agent_activity = original
        self.assertFalse(self.store.scope_fenced(ISSUE))
        self.assertEqual(self.store.pending(), [])

    def test_unresolved_session_has_no_issue_identity_even_when_error_names_one(self):
        self.authority.sessions[SESSION] = {"id": "session-foreign", "issue": {"id": ISSUE},
                                            "creator": {"id": "user-foreign"}}
        with self.assertRaises(LinearError) as denial:
            self.api.agent_session(SESSION)
        self.assertIsNone(denial.exception.authoritative_issue_id)
        self.bridge.handle_webhook(self.session_event())
        self.assertFalse(self.store.scope_fenced(ISSUE))
        original = self.api.agent_session
        self.api.agent_session = lambda *_: (_ for _ in ()).throw(
            LinearError(f"untrusted error text mentions {ISSUE}", retryable=False))
        try:
            self.bridge.handle_webhook(self.session_event())
        finally:
            self.api.agent_session = original
        self.assertFalse(self.store.scope_fenced(ISSUE))

    def _assert_issue_webhook_redirect_preserves_other(self, second_lookup):
        other = "issue-other"
        self.authority.issues[other] = issue_record(id=other, delegate={"id": "viewer-foreign"})
        self.seed_kanban_work(ISSUE)
        task_id = self.seed_kanban_work(other)
        outbox_id = self.store.enqueue("status", {"issue_id": other, "state": "done"}, at=1)
        work, outbox = self.store.get(other), self.store.outbox_row(outbox_id)
        original = self.api.issue
        calls = 0
        def redirect(ref):
            nonlocal calls
            if ref == ISSUE:
                calls += 1
                if calls == (2 if second_lookup else 1): return original(other)
            return original(ref)
        self.api.issue = redirect
        self.bridge.handle_webhook({"type": "Issue", "updatedFrom": {"delegateId": "other"},
                                    "data": {"id": ISSUE}})
        self.assertEqual(calls, 2 if second_lookup else 1)
        self.assertEqual(self.store.get(other), work)
        self.assertEqual(self.store.outbox_row(outbox_id), outbox)
        self.assertEqual(self.kanban.tasks[task_id].status, "ready")
        self.assertFalse(self.store.scope_fenced(other))
        self.assertEqual(self.authority.mutations, [])

    def test_issue_webhook_first_lookup_redirect_preserves_other(self):
        self._assert_issue_webhook_redirect_preserves_other(False)

    def test_issue_webhook_second_lookup_redirect_preserves_other(self):
        self._assert_issue_webhook_redirect_preserves_other(True)

    def test_issue_webhook_scope_denial_precedes_any_existing_work_effect(self):
        self.store.put(ISSUE, "kanban", "session-1", task_id="task-1")
        self.authority.issues[ISSUE] = issue_record(project={"id": "project-foreign"})
        self.bridge.handle_webhook({"type": "Issue", "updatedFrom": {"delegateId": "other"},
                                    "data": {"id": ISSUE}})
        self.assertEqual(self.kanban.creates, [])
        self.assertEqual(self.store.get(ISSUE)["owner_ref"], "session-1")
        self.assertEqual(self.authority.mutations, [])

    def test_persisted_fence_refuses_created_and_issue_events_even_if_scope_recovers(self):
        self.store.fence_scope(ISSUE, "permanent denial", at=1)
        self.bridge.handle_webhook(self.session_event())
        self.bridge.handle_webhook({"type": "Issue", "updatedFrom": {"delegateId": "other"},
                                    "data": {"id": ISSUE}})
        self.assertEqual(self.kanban.creates, [])
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.store.pending(), [])

        other = "issue-existing"
        self.authority.issues[other] = issue_record(id=other)
        self.store.put(other, "kanban", SESSION, task_id="existing", project_id=PROJECT)
        self.kanban.tasks["existing"] = SimpleNamespace(id="existing", status="ready")
        before = self.store.get(other)
        self.store.fence_scope(other, "permanent denial", at=1)
        self.bridge.handle_webhook({"type": "Issue", "updatedFrom": {"delegateId": "other"},
                                    "data": {"id": other}})
        self.assertEqual(self.store.get(other), before)
        self.assertEqual(self.store.pending(), [])

    def test_authorized_created_event_starts_one_task_and_claim(self):
        self.bridge.handle_webhook(self.session_event())
        self.assertEqual(len(self.kanban.creates), 1)
        self.assertEqual(self.kanban.tasks["task-1"].status, "ready")
        self.assertEqual(self.store.get(ISSUE)["task_id"], "task-1")
        self.assertEqual([row["kind"] for row in self.store.pending(ISSUE)], ["activity", "status"])

    def test_replayed_create_keeps_an_existing_human_block(self):
        task = SimpleNamespace(id="task-existing", status="blocked", completion_contract=None,
                               result="", title="OPS-1: task")
        self.kanban.tasks[task.id] = task
        self.kanban.histories[task.id] = [SimpleNamespace(id=1, kind="blocked", payload={"reason": "waiting"})]
        self.kanban.create = lambda **_fields: task
        self.bridge.handle_webhook(self.session_event())
        self.assertEqual(task.status, "blocked")
        self.assertEqual(self.store.get(ISSUE)["task_id"], task.id)

    def test_fence_during_kanban_create_leaves_no_executable_task_or_admission(self):
        create = self.kanban.create
        def fenced_create(**fields):
            task = create(**fields)
            self.store.fence_scope(ISSUE, "permanent denial", at=1)
            return task
        self.kanban.create = fenced_create
        self.bridge.handle_webhook(self.session_event())
        self.assertEqual(self.kanban.tasks["task-1"].status, "archived")
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.store.pending(), [])
        self.assertEqual(self.kanban.subscriptions, [])

    def test_fence_after_task_authorization_before_atomic_start_parks_task(self):
        put = self.store.put
        def fenced_put(*args, **kwargs):
            self.store.fence_scope(ISSUE, "permanent denial", at=1)
            return put(*args, **kwargs)
        self.store.put = fenced_put
        self.bridge.handle_webhook(self.session_event())
        self.assertEqual(self.kanban.tasks["task-1"].status, "archived")
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.store.pending(), [])

    def test_fence_before_ack_commit_archives_admitted_task_without_false_ack(self):
        enqueue = self.store.enqueue_many
        def fenced_enqueue(*args, **kwargs):
            self.store.fence_scope(ISSUE, "permanent denial", at=1)
            return enqueue(*args, **kwargs)
        self.store.enqueue_many = fenced_enqueue
        self.bridge.handle_webhook(self.session_event())
        self.assertEqual(self.kanban.tasks["task-1"].status, "archived")
        self.assertIsNotNone(self.store.get(ISSUE))
        self.assertEqual(self.store.pending(), [])

    def test_transient_failure_after_task_create_keeps_blocked_task_for_retry(self):
        create = self.kanban.create
        def create_then_fail(**fields):
            task = self.kanban.tasks.get("task-1") or create(**fields)
            self.authority.fail_issue = True
            return task
        self.kanban.create = create_then_fail
        inbox = Path(self.tmp.name) / "ingress.db"
        IngressStore(inbox).enqueue(Route("synthetic", "synthetic", "/linear", inbox, inbox),
                                    "delivery", json.dumps(self.session_event()).encode())
        self.bridge.drain_ingress(inbox)
        with sqlite3.connect(inbox) as db:
            self.assertEqual(db.execute("SELECT status FROM deliveries").fetchone()[0], "pending")
        self.assertFalse(self.store.scope_fenced(ISSUE))
        self.assertEqual(self.kanban.tasks["task-1"].status, "blocked")
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.store.pending(), [])
        self.authority.fail_issue = False
        self.kanban.create = lambda **fields: self.kanban.tasks["task-1"]
        self.bridge.drain_ingress(inbox)
        with sqlite3.connect(inbox) as db:
            self.assertEqual(db.execute("SELECT status FROM deliveries").fetchone()[0], "imported")
        self.assertEqual(self.kanban.tasks["task-1"].status, "ready")
        self.assertIsNotNone(self.store.get(ISSUE))

    def test_resume_refuses_fence_before_kanban_unblock(self):
        task_id = self.seed_kanban_work(task_status="blocked")
        row = self.store.get(ISSUE)
        activate = self.store.activate_task
        def fenced_activate(*args, **kwargs):
            self.store.fence_scope(ISSUE, "permanent denial", at=1)
            return activate(*args, **kwargs)
        self.store.activate_task = fenced_activate
        self.assertFalse(self.bridge._resume(row, "continue"))
        self.assertEqual(self.kanban.tasks[task_id].status, "blocked")
        self.assertEqual(self.kanban.comments, [])
        self.assertEqual(self.store.get(ISSUE), row)

    def test_store_refuses_fenced_admissions_without_changing_prior_state(self):
        self.store.put(ISSUE, "chat", "owner", project_id=PROJECT)
        saved = self.store.get(ISSUE)
        pending_id = self.store.enqueue("comment", {"issue_id": ISSUE, "body": "saved"}, at=1)
        pending = self.store.outbox_row(pending_id)
        self.store.fence_scope(ISSUE, "permanent denial", at=2)
        self.assertFalse(self.store.put(ISSUE, "kanban", SESSION, task_id="task", project_id=PROJECT))
        self.assertFalse(self.store.update(ISSUE, owner_ref="changed", last_event_id=9))
        self.assertFalse(self.store.enqueue("status", {"issue_id": ISSUE, "state": "done"}, at=3))
        self.assertFalse(self.store.enqueue_once("comment", {"issue_id": ISSUE}, "marker", at=3))
        self.assertFalse(self.store.capture_chat_stop(ISSUE, SESSION, ACTIVITY, "synthetic", at=3))
        self.assertEqual(self.store.get(ISSUE), saved)
        self.assertEqual(self.store.outbox_row(pending_id), pending)
        self.assertEqual(len(self.store.pending()), 1)

    def test_batch_ack_refuses_all_rows_if_either_issue_is_fenced(self):
        other = "issue-other"
        self.store.fence_scope(other, "permanent denial", at=1)
        self.assertFalse(self.store.enqueue_many([
            ("activity", {"issue_id": ISSUE, "session_id": SESSION,
                          "content": {"type": "thought", "body": "On it"}}),
            ("status", {"issue_id": other, "state": "in_progress"})], at=2))
        self.assertEqual(self.store.pending(), [])

    def test_work_is_durable_before_task_becomes_executable(self):
        unblock = self.kanban.unblock
        def checked_unblock(task_id):
            with sqlite3.connect(self.store.path) as db:
                self.assertEqual(db.execute("SELECT task_id FROM work WHERE issue_id=?", (ISSUE,)).fetchone()[0],
                                 task_id)
            return unblock(task_id)
        self.kanban.unblock = checked_unblock
        self.bridge.handle_webhook(self.session_event())
        self.assertEqual(self.kanban.tasks["task-1"].status, "ready")

    def test_created_delivery_replays_incomplete_start_without_new_task(self):
        for stage in ("before_activation", "before_ack"):
            with self.subTest(stage=stage):
                self.store = Store(Path(self.tmp.name) / f"{stage}.db")
                self.kanban = FakeKanban()
                self.bridge = Bridge(self.store, self.api, self.kanban, profile="synthetic")
                method = "activate_task" if stage == "before_activation" else "enqueue_many"
                original = getattr(self.store, method)
                def crash(*_args, **_kwargs):
                    raise SystemExit("simulated process death")
                setattr(self.store, method, crash)
                with self.assertRaises(SystemExit):
                    self.bridge.handle_webhook(self.session_event())
                self.assertIsNotNone(self.store.get(ISSUE))
                self.assertEqual(self.store.pending(), [])
                setattr(self.store, method, original)
                self.bridge.handle_webhook(self.session_event())
                self.assertEqual(len(self.kanban.creates), 1)
                self.assertEqual(self.kanban.tasks["task-1"].status, "ready")
                self.assertEqual([row["kind"] for row in self.store.pending(ISSUE)], ["activity", "status"])

    def test_fence_preserves_pending_and_uncertain_outbox_maintenance(self):
        pending_id = self.store.enqueue("comment", {"issue_id": ISSUE, "body": "pending"}, at=1)
        failed_id = self.store.enqueue("status", {"issue_id": ISSUE, "state": "blocked"}, at=1)
        self.store.retry(self.store.outbox_row(failed_id), 2)
        self.store.mark(failed_id, "failed")
        before = {ident: self.store.outbox_row(ident) for ident in (pending_id, failed_id)}
        self.store.fence_scope(ISSUE, "permanent denial", at=3)
        self.store.mark(pending_id, "failed")
        self.store.mark_sent(pending_id, True)
        self.store.defer(pending_id, 20)
        self.store.retry(before[pending_id], 20)
        self.store.report(pending_id)
        self.store.rewrite(pending_id, {"issue_id": ISSUE, "body": "changed"}, 20)
        self.store.drop(pending_id)
        self.assertEqual(self.store.revive_failed(20), 0)
        for ident, row in before.items():
            self.assertEqual(self.store.outbox_row(ident), row)

    def test_bridge_effect_helpers_refuse_fence_between_resolution_and_commit(self):
        for name, invoke, method in (
            ("status", lambda ident: self.bridge.status(ident, "in_progress"), "enqueue"),
            ("comment", lambda ident: self.bridge.comment(ident, "body"), "enqueue"),
            ("activity", lambda ident: self.bridge.activity(ident, SESSION, "response", "body"), "enqueue"),
            ("project", lambda ident: self.bridge.project_update(SESSION, PROJECT, ident, "line", issue_id=ident),
             "queue_project_update"),
        ):
            ident = f"issue-{name}"
            self.authority.issues[ident] = issue_record(id=ident)
            original = getattr(self.store, method)
            def fence_then_write(*args, **kwargs):
                self.store.fence_scope(ident, "permanent denial", at=1)
                return original(*args, **kwargs)
            setattr(self.store, method, fence_then_write)
            try:
                with self.subTest(effect=name):
                    self.assertFalse(invoke(ident))
                    self.assertEqual(self.store.pending(), [])
            finally:
                setattr(self.store, method, original)

    def test_project_update_merge_refuses_fence_on_either_issue(self):
        second = "issue-second"
        first = {"issue_id": "update:session:project", "session_id": SESSION, "project_id": PROJECT,
                 "resolve": ISSUE, "lines": {"OPS-1": "first"}, "line_issues": {"OPS-1": ISSUE}}
        next_line = {**first, "resolve": second, "lines": {"OPS-2": "second"},
                     "line_issues": {"OPS-2": second}}
        for fenced in (ISSUE, second):
            with self.subTest(fenced=fenced):
                path = Path(self.tmp.name) / f"{fenced}.db"
                store = Store(path)
                self.assertTrue(store.queue_project_update(first, due=1, quiet=True))
                before = store.project_update(SESSION, PROJECT)
                store.fence_scope(fenced, "permanent denial", at=2)
                self.assertFalse(store.queue_project_update(next_line, due=3, quiet=True))
                self.assertEqual(store.project_update(SESSION, PROJECT), before)

    def test_nonmergeable_fenced_aggregate_does_not_refuse_allowed_noop(self):
        other = "issue-other"
        payload = {"issue_id": "update:session:project", "session_id": SESSION,
                   "project_id": PROJECT, "resolve": ISSUE, "lines": {"OPS-1": "prior"},
                   "line_issues": {"OPS-1": ISSUE}}
        self.store.queue_project_update(payload, due=1, quiet=True)
        row = self.store.project_update(SESSION, PROJECT)
        self.store.mark_sent(row["id"], True)
        before = self.store.project_update(SESSION, PROJECT)
        self.store.fence_scope(ISSUE, "permanent denial", at=2)
        next_line = {**payload, "resolve": other, "lines": {"OPS-2": "new"},
                     "line_issues": {"OPS-2": other}}
        self.assertTrue(self.store.queue_project_update(next_line, due=3, quiet=True))
        self.assertEqual(self.store.project_update(SESSION, PROJECT), before)

    def test_failed_alert_does_not_repeat_after_fence(self):
        self.seed_kanban_work()
        row_id = self.store.enqueue("comment", {"issue_id": ISSUE, "body": "uncertain", "task_id": "task"}, at=1)
        self.store.mark(row_id, "failed")
        before = self.store.outbox_row(row_id)
        self.store.fence_scope(ISSUE, "permanent denial", at=2)
        self.bridge.flush()
        self.bridge.flush()
        self.assertEqual(self.kanban.comments, [])
        self.assertEqual(self.store.outbox_row(row_id), before)

    def test_outbox_effect_holds_fence_commit_during_remote_mutation(self):
        row_id = self.store.enqueue("comment", {"issue_id": ISSUE, "body": "saved"}, at=1)
        began, finished = threading.Event(), threading.Event()
        worker = None
        transport = self.api.transport
        def send(url, body, headers):
            nonlocal worker
            if json.loads(body)["query"].lstrip().startswith("mutation"):
                def fence():
                    began.set()
                    self.store.fence_scope(ISSUE, "permanent denial", at=2)
                    finished.set()
                worker = threading.Thread(target=fence)
                worker.start()
                self.assertTrue(began.wait(1))
                self.assertFalse(finished.wait(0.05), "fence committed during an authorized send")
            return transport(url, body, headers)
        self.api.transport = send
        self.bridge.flush()
        worker.join(2)
        self.assertTrue(finished.is_set())
        self.assertIn(self.store.outbox_row(row_id)["state"], ("pending", "sent"))
        self.assertTrue(self.store.scope_fenced(ISSUE))

    def test_outbox_preflight_allows_fence_before_remote_mutation(self):
        self.authority.issues[ISSUE]["delegate"] = {"id": VIEWER}
        row_id = self.store.enqueue("comment", {"issue_id": ISSUE, "body": "saved", "task_id": "task"}, at=1)
        original_issue, reads = self.api.issue, 0
        def issue(ident):
            nonlocal reads
            reads += 1
            if reads == 2:
                worker = threading.Thread(target=lambda: self.store.fence_scope(
                    ISSUE, "permanent denial", at=2))
                worker.start()
                worker.join(2)
                self.assertFalse(worker.is_alive(), "preflight held a write transaction")
            return original_issue(ident)
        self.api.issue = issue
        self.assertEqual(self.bridge.flush(), 0)
        self.assertEqual(self.authority.mutations, [])
        self.assertEqual(self.store.outbox_row(row_id)["state"], "pending")

    def test_project_update_resource_rechecks_do_not_delay_fence(self):
        other = "issue-second"
        self.authority.issues[ISSUE]["delegate"] = {"id": VIEWER}
        self.authority.issues[other] = issue_record(id=other, identifier="OPS-2", delegate={"id": VIEWER})
        payload = {"issue_id": "update:session:project", "session_id": SESSION,
                   "project_id": PROJECT, "resolve": ISSUE,
                   "lines": {"OPS-1": "first", "OPS-2": "second"},
                   "line_issues": {"OPS-1": ISSUE, "OPS-2": other}}
        self.assertTrue(self.store.queue_project_update(payload, due=1, quiet=False))
        row = self.store.project_update(SESSION, PROJECT)
        original, completed_during_recheck = self.api._mutation_issue, []
        worker = None
        def recheck(ident):
            nonlocal worker
            if worker is None:
                worker = threading.Thread(target=lambda: self.store.fence_scope(
                    other, "permanent denial", at=2))
                worker.start()
                worker.join(0.1)
                completed_during_recheck.append(not worker.is_alive())
            return original(ident)
        self.api._mutation_issue = recheck
        self.bridge.flush()
        worker.join(2)
        self.assertEqual(completed_during_recheck, [True])
        self.assertTrue(self.store.scope_fenced(other))
        self.assertEqual(self.store.outbox_row(row["id"])["state"], "pending")
        self.assertEqual(self.authority.mutations, [])

    def test_stop_refuses_task_block_when_fence_precedes_work_update(self):
        task_id = self.seed_kanban_work()
        row = self.store.get(ISSUE)
        before = self.store.get(ISSUE)
        update = self.store.update
        def fence_then_update(*args, **kwargs):
            self.store.fence_scope(ISSUE, "permanent denial", at=2)
            return update(*args, **kwargs)
        self.store.update = fence_then_update
        self.bridge._stop(row, SESSION, "requester", 3)
        self.assertEqual(self.kanban.tasks[task_id].status, "ready")
        self.assertEqual(self.store.get(ISSUE), before)
        self.assertEqual(self.store.pending(), [])

    def test_recovery_injection_refuses_preexisting_and_midflight_fence(self):
        messages = []
        self.bridge.inject = lambda key, body: messages.append((key, body)) or True
        self.store.put(ISSUE, "chat", "owner")
        self.store.fence_scope(ISSUE, "permanent denial", at=2)
        self.bridge.recover()
        self.assertEqual(messages, [])

        other = "issue-recovery"
        self.authority.issues[other] = issue_record(id=other, delegate={"id": VIEWER})
        self.store.put(other, "chat", "owner")
        def fence_after_check(_issue_id):
            self.store.fence_scope(other, "permanent denial", at=3)
            return True
        self.bridge.may_execute_existing = fence_after_check
        self.bridge.recover()
        self.assertEqual(messages, [])
        self.assertIsNotNone(self.store.get(other))

    def test_fenced_aggregate_blocks_terminal_merge_and_quiet_shift(self):
        other = "issue-prior"
        self.store.put(ISSUE, "chat", "owner", project_id=PROJECT)
        payload = {"issue_id": "update:session:project", "session_id": SESSION,
                   "project_id": PROJECT, "resolve": other, "lines": {"OPS-2": "prior"},
                   "line_issues": {"OPS-2": other}}
        self.store.queue_project_update(payload, due=2, quiet=True)
        before_work = self.store.get(ISSUE)
        before_update = self.store.project_update(SESSION, PROJECT)
        self.store.fence_scope(other, "permanent denial", at=3)
        self.assertFalse(self.store.finish(ISSUE, [
            ("status", {"issue_id": ISSUE, "state": "done"}),
            ("project_update", {**payload, "resolve": ISSUE, "lines": {"OPS-1": "done"},
                                "line_issues": {"OPS-1": ISSUE}})], at=4))
        self.store.delay_session_updates(SESSION, 50)
        self.assertEqual(self.store.get(ISSUE), before_work)
        self.assertEqual(self.store.project_update(SESSION, PROJECT), before_update)

    def test_fence_between_chat_authorization_and_start_or_blocked_commit(self):
        from hermes_fleet_linear_plugin import chat
        context = SimpleNamespace(session_key="owner", session_id="owner", profile="synthetic")
        put = self.store.put
        def fenced_put(*args, **kwargs):
            self.store.fence_scope(ISSUE, "permanent denial", at=1)
            return put(*args, **kwargs)
        self.store.put = fenced_put
        reply = json.loads(chat.handle(self.bridge, {"action": "start", "issue": ISSUE}, context))
        self.assertFalse(reply["ok"])
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.store.pending(), [])

        other = "issue-blocked"
        self.authority.issues[other] = issue_record(id=other)
        store = Store(Path(self.tmp.name) / "blocked.db")
        store.put(other, "chat", "owner", project_id=PROJECT)
        bridge = Bridge(store, self.api, self.kanban, profile="synthetic")
        enqueue = store.enqueue
        def fenced_enqueue(*args, **kwargs):
            store.fence_scope(other, "permanent denial", at=1)
            return enqueue(*args, **kwargs)
        store.enqueue = fenced_enqueue
        before = store.get(other)
        reply = json.loads(chat.handle(bridge, {"action": "blocked", "issue": other}, context))
        self.assertFalse(reply["ok"])
        self.assertEqual(store.get(other), before)
        self.assertEqual(store.pending(), [])

    def test_chat_action_scope_denial_happens_before_store_or_external_effect(self):
        from hermes_fleet_linear_plugin import chat

        self.authority.issues[ISSUE] = issue_record(creator={"id": "user-foreign"})
        context = SimpleNamespace(session_key="chat-1", session_id="chat-1", profile="synthetic")
        result = json.loads(chat.handle(self.bridge, {"action": "start", "issue": ISSUE}, context))
        self.assertFalse(result["ok"])
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.authority.mutations, [])
        self.assertEqual(self.kanban.creates, [])

    def test_chat_actions_refuse_persisted_specialist_fence_without_changing_history(self):
        from hermes_fleet_linear_plugin import chat
        context = SimpleNamespace(session_key="owner", session_id="owner", profile="synthetic")
        for action in ("start", "done", "blocked", "release"):
            with self.subTest(action=action):
                issue_id = f"{ISSUE}-{action}"
                self.authority.issues[issue_id] = issue_record(id=issue_id)
                if action != "start":
                    self.store.put(issue_id, "chat", "owner", project_id=PROJECT)
                before = self.store.get(issue_id)
                self.store.fence_scope(issue_id, "permanent denial", at=1.0)
                pending = self.store.pending()
                reply = json.loads(chat.handle(self.bridge, {
                    "action": action, "issue": issue_id, "evidence": "https://docs.example/findings/1"}, context))
                self.assertFalse(reply["ok"])
                self.assertEqual(self.store.get(issue_id), before)
                self.assertEqual(self.store.pending(), pending)
                self.assertEqual(self.authority.mutations, [])

    def test_chat_closeout_rechecks_scope_after_evidence_verification(self):
        from hermes_fleet_linear_plugin import chat
        self.store.put(ISSUE, "chat", "owner", project_id=PROJECT)
        before = self.store.get(ISSUE)
        def evidence_checked(_links):
            self.authority.issues[ISSUE] = issue_record(project={"id": "project-foreign"})
            return True
        self.bridge.accepted_evidence = evidence_checked
        context = SimpleNamespace(session_key="owner", session_id="owner", profile="synthetic")
        reply = json.loads(chat.handle(self.bridge, {
            "action": "done", "issue": ISSUE, "evidence": "https://docs.example/findings/1"}, context))
        self.assertFalse(reply["ok"])
        self.assertTrue(self.store.scope_fenced(ISSUE))
        self.assertEqual(self.store.get(ISSUE), before)
        self.assertEqual(self.store.pending(), [])
        self.assertEqual(self.authority.mutations, [])

    def test_kanban_closeout_rechecks_scope_after_evidence_verification(self):
        for accepted in (True, False):
            with self.subTest(evidence_accepted=accepted):
                issue_id = f"{ISSUE}-{accepted}"
                self.authority.issues[issue_id] = issue_record(id=issue_id)
                self.kanban.tasks["task"] = SimpleNamespace(id="task", status="done", completion_contract="https://docs.example/findings/1", result="Done", title="OPS-1: task")
                self.store.put(issue_id, "kanban", SESSION, task_id="task", project_id=PROJECT)
                before = self.store.get(issue_id)
                def evidence_checked(_links, _contract):
                    self.authority.issues[issue_id] = issue_record(id=issue_id, project={"id": "project-foreign"})
                    return accepted
                self.bridge.accepted_evidence = evidence_checked
                self.bridge._finished(before)
                self.assertTrue(self.store.scope_fenced(issue_id))
                self.assertEqual(self.store.get(issue_id), before)
                self.assertEqual(self.store.pending(issue_id), [])
                self.assertEqual(self.authority.mutations, [])

    def test_history_capture_refuses_persisted_fence_atomically(self):
        for forget in (False, True):
            with self.subTest(archived=forget):
                issue_id = f"{ISSUE}-{forget}"
                self.store.put(issue_id, "kanban", SESSION, task_id="task", project_id=PROJECT)
                self.store.enqueue("comment", {"issue_id": issue_id, "body": "preserved"}, at=1)
                before, pending = self.store.get(issue_id), self.store.pending(issue_id)
                self.store.fence_scope(issue_id, "permanent denial", at=1.0)
                self.assertFalse(self.store.capture_event(issue_id, 2, [("status", {"issue_id": issue_id, "state": "done"})], at=2.0, forget=forget))
                self.assertEqual(self.store.get(issue_id), before)
                self.assertEqual(self.store.pending(issue_id), pending)

    def test_terminal_capture_refuses_persisted_fence_atomically(self):
        self.store.put(ISSUE, "chat", "owner", project_id=PROJECT)
        before = self.store.get(ISSUE)
        self.store.fence_scope(ISSUE, "permanent denial", at=1.0)
        self.assertFalse(self.store.finish(ISSUE, [("status", {"issue_id": ISSUE, "state": "done"})], at=2.0))
        self.assertEqual(self.store.get(ISSUE), before)
        self.assertEqual(self.store.pending(), [])

    def test_pump_fences_permanently_revoked_authoritative_scope_before_history_admission(self):
        changes = (
            ("team", {"team": {"id": "team-foreign", "key": "NO", "states": {"nodes": []}}}),
            ("project", {"project": {"id": "project-foreign"}}),
            ("requester", {"creator": {"id": "user-foreign"}}),
        )
        for suffix, (field, change) in enumerate(changes, start=1):
            issue_id, task_id = f"{ISSUE}-{suffix}", f"task-{suffix}"
            self.authority.issues[issue_id] = issue_record(id=issue_id, **change)
            self.kanban.tasks[task_id] = SimpleNamespace(id=task_id, status="blocked",
                                                         completion_contract=None, result="", title="OPS-1: task")
            self.kanban.histories[task_id] = [SimpleNamespace(id=1, kind="blocked",
                                                              payload={"reason": "needs input"})]
            self.store.put(issue_id, "kanban", SESSION, task_id=task_id, project_id=PROJECT)

        self.bridge.pump_kanban()

        for suffix, (field, _change) in enumerate(changes, start=1):
            issue_id = f"{ISSUE}-{suffix}"
            with self.subTest(authoritative_field=field):
                self.assertIsNotNone(self.store.get(issue_id), "revoked work and its history must be preserved")
                self.assertEqual(self.store.get(issue_id)["last_event_id"], 0,
                                 "a denied effect must not advance the Kanban cursor")
                self.assertEqual(self.store.pending(issue_id), [], "denied activity/status must not enter the outbox")

    def test_tick_keeps_pending_and_uncertain_writes_untouched_after_revocation_and_restart(self):
        self.seed_kanban_work(task_status="blocked", events=[SimpleNamespace(
            id=1, kind="blocked", payload={"reason": "needs input"})])
        status_id = self.store.enqueue("status", {"issue_id": ISSUE, "state": "done"}, at=1)
        comment_id = self.store.enqueue("comment", {"issue_id": ISSUE, "body": "saved evidence"}, at=1)
        activity_id = self.store.enqueue("activity", {"issue_id": ISSUE, "session_id": SESSION,
                                                       "content": {"type": "response", "body": "saved"}}, at=1)
        project_id = self.store.enqueue("project_update", {"issue_id": "update:session:project",
                                                            "resolve": ISSUE, "project_id": PROJECT,
                                                            "lines": {"OPS-1": "Done"},
                                                            "line_issues": {"OPS-1": ISSUE}}, at=1)
        frozen_payload = self.store.outbox_row(project_id)["payload"]
        self.assertTrue(self.store.freeze_project_update(project_id, frozen_payload, self.bridge.clock(),
                                                         "Agent update\n\n- OPS-1: Done", PROJECT))
        original = {row_id: self.store.outbox_row(row_id) for row_id in
                    (status_id, comment_id, activity_id, project_id)}
        self.authority.issues[ISSUE] = issue_record(project={"id": "project-foreign"})

        self.bridge.tick()

        self.assertEqual(self.authority.mutations, [], "revocation must prevent every Linear mutation")
        self.assertIsNotNone(self.store.get(ISSUE), "the active task must not be deleted by recheck")
        self.assertEqual(self.store.get(ISSUE)["last_event_id"], 0)
        self.assertEqual(self.kanban.event_calls, [], "scope must be checked before Kanban event admission")
        for row_id, before in original.items():
            after = self.store.outbox_row(row_id)
            self.assertEqual(after, before, "pending and uncertain outbox rows must remain byte-for-byte intact")

        restarted_store = Store(self.store.path)
        restarted = Bridge(restarted_store, self.api, self.kanban, profile="synthetic")
        self.authority.issues[ISSUE] = issue_record(creator={"id": "user-foreign"})
        restarted.tick()
        self.assertEqual(self.authority.mutations, [])
        self.assertIsNotNone(restarted_store.get(ISSUE))
        self.assertEqual(restarted_store.get(ISSUE)["last_event_id"], 0)
        for row_id, before in original.items():
            self.assertEqual(restarted_store.outbox_row(row_id), before)

    def test_transient_scope_resolution_failure_retries_without_permanent_fence(self):
        self.seed_kanban_work(task_status="blocked", events=[SimpleNamespace(id=1, kind="blocked",
                                                       payload={"reason": "needs input"})])
        self.authority.fail_issue = True

        self.bridge.pump_kanban()

        self.assertIsNotNone(self.store.get(ISSUE))
        self.assertEqual(self.store.get(ISSUE)["last_event_id"], 0)
        self.assertEqual(self.store.pending(ISSUE), [])
        self.authority.fail_issue = False
        self.bridge.pump_kanban()
        self.assertEqual(self.store.get(ISSUE)["last_event_id"], 1)
        self.assertEqual([row["kind"] for row in self.store.pending(ISSUE)], ["status", "activity"])

    def test_all_specialist_effects_recheck_the_authoritative_resource_before_mutation(self):
        for name, action in (
            ("status", lambda: self.api.update_issue(ISSUE, {"stateId": "state-done"})),
            ("comment", lambda: self.api.create_comment("client-1", ISSUE, "body")),
            ("activity", lambda: self.api.create_activity("client-1", SESSION,
                                                           {"type": "response"}, issue_id=ISSUE)),
            ("project update", lambda: self.api.create_project_update("client-1", PROJECT, "body",
                                                                       issue_ids=[ISSUE])),
        ):
            self.authority.issues[ISSUE] = issue_record(project={"id": "project-foreign"})
            self.authority.sessions[SESSION] = {"id": SESSION, "issue": {"id": ISSUE}, "creator": {"id": USER}}
            before = len(self.authority.mutations)
            with self.subTest(effect=name), self.assertRaises(LinearError):
                action()
            self.assertEqual(len(self.authority.mutations), before)

    def test_mutations_fail_closed_when_identity_changes(self):
        self.authority.viewer = "viewer-foreign"
        with self.assertRaises(LinearError):
            self.api.create_comment("client-1", ISSUE, "body")
        self.assertEqual(self.authority.mutations, [])
        self.assertEqual(self.store.pending(), [])


if __name__ == "__main__":
    unittest.main()
