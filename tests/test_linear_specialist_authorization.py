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
        self.tasks = {}
        self.histories = {}
        self.event_calls = []
        self.comments = []

    def create(self, **fields):
        self.creates.append(fields)
        task = SimpleNamespace(id="task-1", status="ready", completion_contract=None,
                               result="", title=fields.get("title", "Synthetic task"))
        self.tasks[task.id] = task
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
