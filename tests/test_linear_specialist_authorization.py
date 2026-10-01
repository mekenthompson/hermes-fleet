"""Adversarial coverage for the opt-in specialist authorization boundary."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from linear_fake_api import load_plugin

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

    def create(self, **fields):
        self.creates.append(fields)
        return SimpleNamespace(id="task-1", status="ready")

    def subscribe(self, task_id, issue_id):
        self.subscriptions.append((task_id, issue_id))


class LinearSpecialistAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.authority = Authority()
        self.kanban = FakeKanban()
        self.store = Store(Path(self.tmp.name) / "state.db")
        self.api = self.authority.api()
        self.bridge = Bridge(self.store, self.api, self.kanban, profile="synthetic")

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

    def test_issue_webhook_scope_denial_precedes_any_existing_work_effect(self):
        self.store.put(ISSUE, "kanban", "session-1", task_id="task-1")
        self.authority.issues[ISSUE] = issue_record(project={"id": "project-foreign"})
        self.bridge.handle_webhook({"type": "Issue", "updatedFrom": {"delegateId": "other"},
                                    "data": {"id": ISSUE}})
        self.assertEqual(self.kanban.creates, [])
        self.assertEqual(self.store.get(ISSUE)["owner_ref"], "session-1")
        self.assertEqual(self.authority.mutations, [])

    def test_chat_action_scope_denial_happens_before_store_or_external_effect(self):
        from hermes_fleet_linear_plugin import chat

        self.authority.issues[ISSUE] = issue_record(creator={"id": "user-foreign"})
        context = SimpleNamespace(session_key="chat-1", session_id="chat-1", profile="synthetic")
        result = json.loads(chat.handle(self.bridge, {"action": "start", "issue": ISSUE}, context))
        self.assertFalse(result["ok"])
        self.assertIsNone(self.store.get(ISSUE))
        self.assertEqual(self.authority.mutations, [])
        self.assertEqual(self.kanban.creates, [])

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
