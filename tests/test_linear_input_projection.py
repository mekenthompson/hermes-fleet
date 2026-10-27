"""Provider identity and original text precede native conversation delivery."""
import copy
import json
from pathlib import Path
import sqlite3
import uuid
import tempfile
import unittest

from linear_fake_api import load_plugin
from test_linear_specialist_authorization import (
    ACTIVITY, ISSUE, SESSION, USER, VIEWER, Authority, FakeKanban,
)

load_plugin()
from hermes_fleet_linear_plugin.api import LinearError
from hermes_fleet_linear_plugin import chat
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.native_input import InboxDelivery, project_input, queue_reply
from hermes_fleet_linear_plugin.store import Store
from hermes_fleet_linear_plugin.transport import NativeContext


class InputProjectionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.authority = Authority()
        self.kanban = FakeKanban()
        self.bridge = Bridge(Store(Path(temporary.name) / "state.db"),
                             self.authority.api(), self.kanban, profile="synthetic")
        self.bridge.profile_home = Path(temporary.name)

    def event(self, action="prompted"):
        return {
            "type": "AgentSessionEvent", "action": action,
            "createdAt": "2026-01-01T00:00:00Z",
            "agentSession": {"id": SESSION, "issue": {"id": ISSUE}, "creatorId": USER},
            "agentActivity": {"id": ACTIVITY, "user": {"id": USER},
                              "content": {"body": "  Continue only the remaining work.\n"}},
        }

    def test_prompt_preserves_original_text_and_retry_input_as_issue_changes(self):
        event = self.event()
        first = project_input(self.bridge, event)
        self.authority.issues[ISSUE]["description"] = "Changed issue background"
        second = project_input(self.bridge, copy.deepcopy(event))
        self.assertEqual(first, second)
        self.assertEqual(first.text, "  Continue only the remaining work.\n")
        self.assertEqual((first.actor_id, first.message_id, first.issue_id, first.session_id),
                         (USER, ACTIVITY, ISSUE, SESSION))
        self.assertEqual(self.kanban.creates, [])
        self.assertEqual(self.authority.mutations, [])

    def test_payload_actor_and_session_mismatch_refuse_before_any_effect(self):
        for mutate in (
            lambda event: event["agentActivity"].update(user={"id": "foreign"}),
            lambda event: event["agentSession"].update(creatorId="foreign"),
            lambda event: event["agentSession"].update(issue={"id": "foreign"}),
        ):
            with self.subTest(mutate=mutate):
                event = self.event()
                mutate(event)
                with self.assertRaises(LinearError):
                    project_input(self.bridge, event)
        self.assertEqual(self.kanban.creates, [])
        self.assertEqual(self.authority.mutations, [])

    def test_assignment_uses_session_creator_and_preserves_actorless_dispatch(self):
        event = self.event("created")
        assigned = project_input(self.bridge, event)
        self.assertEqual((assigned.actor_id, assigned.message_id), (USER, SESSION))
        self.authority.sessions[SESSION]["creator"] = None
        event["agentSession"].pop("creatorId")
        self.assertIsNone(project_input(self.bridge, event))
        self.assertEqual(self.kanban.creates, [])

    def test_recipient_app_is_never_substituted_for_a_human_sender(self):
        self.authority.sessions[SESSION]["creator"] = {"id": VIEWER}
        event = self.event("created")
        event["agentSession"]["creatorId"] = VIEWER
        with self.assertRaises(LinearError):
            project_input(self.bridge, event)
        self.authority.activities[ACTIVITY]["user"] = {"id": VIEWER}
        event = self.event()
        event["agentActivity"]["user"] = {"id": VIEWER}
        with self.assertRaises(LinearError):
            project_input(self.bridge, event)

    def test_structured_stop_cannot_become_a_model_prompt(self):
        event = self.event()
        event["agentActivity"]["signal"] = "stop"
        projected = project_input(self.bridge, event)
        self.assertEqual(projected.signal, "stop")
        with self.assertRaisesRegex(LinearError, "native control"):
            projected.message(None)

    def test_missing_source_time_or_malformed_original_text_refuses(self):
        event = self.event()
        event.pop("createdAt")
        with self.assertRaises(LinearError):
            project_input(self.bridge, event)
        event = self.event()
        event["agentActivity"]["content"]["body"] = ["not text"]
        with self.assertRaises(LinearError):
            project_input(self.bridge, event)
        event["agentActivity"]["content"] = ["not an object"]
        with self.assertRaises(LinearError):
            project_input(self.bridge, event)

    def test_reply_queues_once_and_does_not_claim_remote_delivery(self):
        first = queue_reply(self.bridge, ISSUE, SESSION, ACTIVITY, "The work is waiting for input.")
        repeated = queue_reply(self.bridge, ISSUE, SESSION, ACTIVITY, "The work is waiting for input.")
        self.assertEqual(first, repeated)
        row = self.bridge.store.outbox_row(first)
        self.assertEqual(row["state"], "pending")
        self.assertTrue(self.bridge.store.delivery_allowed(first))
        self.assertTrue(self.bridge.store.independent_message("activity", row["payload"]))
        self.assertEqual(self.authority.mutations, [])
        self.assertEqual(self.kanban.creates, [])

    def test_reply_rechecks_own_session_and_current_authorization(self):
        row_id = queue_reply(self.bridge, ISSUE, SESSION, ACTIVITY, "Current status")
        row = self.bridge.store.outbox_row(row_id)
        self.assertTrue(self.bridge._authorize_outbox_effect(row))
        self.authority.sessions[SESSION]["appUser"] = {"id": "another-app"}
        self.assertFalse(self.bridge._authorize_outbox_effect(row))
        with self.assertRaises(LinearError):
            queue_reply(self.bridge, ISSUE, SESSION, ACTIVITY, "Another reply")
        self.assertEqual(self.authority.mutations, [])

    def test_reply_publication_reuses_existing_scoped_outbox_and_remote_identity(self):
        row_id = queue_reply(self.bridge, ISSUE, SESSION, ACTIVITY, "Current status")
        self.assertEqual(self.bridge.flush(only_id=row_id), 1)
        self.assertEqual(self.bridge.store.outbox_row(row_id)["state"], "sent")
        self.assertEqual(queue_reply(self.bridge, ISSUE, SESSION, ACTIVITY, "Current status"), row_id)
        self.assertEqual(self.bridge.flush(only_id=row_id), 0)
        self.assertEqual(len(self.authority.mutations), 1)
        self.assertEqual(self.kanban.creates, [])

    def test_reply_can_publish_while_an_old_effect_stays_uncertain(self):
        retained = {"issue_id": ISSUE, "terminal": True, "write_started": True,
                    "reconcile_required": True, "state": "done"}
        with self.bridge.store._tx() as db:
            db.execute("INSERT INTO outbox(id,kind,payload,state,next_at) VALUES(?,?,?,?,?)",
                       ("old-unknown", "status", json.dumps(retained), "failed", 0))
        row_id = queue_reply(self.bridge, ISSUE, SESSION, ACTIVITY, "That earlier effect needs inspection.")
        self.assertTrue(row_id)
        self.assertEqual(self.bridge.flush(only_id=row_id), 1)
        old = self.bridge.store.outbox_row("old-unknown")
        self.assertEqual(old["state"], "failed")
        self.assertTrue(old["payload"]["reconcile_required"])
        self.assertEqual(len(self.authority.mutations), 1)

    def test_reply_has_no_work_or_execution_authority(self):
        row_id = queue_reply(self.bridge, ISSUE, SESSION, ACTIVITY, "Status without a task")
        payload = self.bridge.store.outbox_row(row_id)["payload"]
        self.bridge.store.fence_scope(ISSUE, "Retained native effects are held", at=1)
        for key in ("task_id", "session_key", "admission_id", "work_owner", "claim", "terminal"):
            with self.subTest(key=key):
                forged = {**payload, key: "unverified"}
                self.assertFalse(self.bridge.store.unlinked_content("activity", forged))
                self.assertFalse(self.bridge.store.conversation_reply("activity", forged))
                self.assertEqual(self.bridge.store.enqueue("activity", {**forged, "id": str(uuid.uuid4())}), "")
        self.assertEqual(self.authority.mutations, [])

    def linear_context(self, **changes):
        values = dict(home=str(self.bridge.profile_home), profile=self.bridge.profile,
                      platform="linear", source="linear", session_key="native-fixture-route",
                      session_id="native-fixture-session", user_id=USER, chat_id=ISSUE,
                      thread_id=SESSION, scope_id=self.authority.organization, message_id=ACTIVITY)
        return NativeContext(**{**values, **changes})

    def use_session_requester_scope(self):
        self.bridge.api.specialist_scope["requester_basis"] = "agent_session"

    def test_native_linear_status_uses_current_provider_session_without_a_task(self):
        self.use_session_requester_scope()
        result = json.loads(chat.handle(self.bridge, {"action": "status", "issue": ISSUE}, self.linear_context()))
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["issue"]["id"], ISSUE)
        self.assertFalse(result["native"]["linked"])
        self.assertEqual(self.kanban.creates, [])

    def test_native_linear_content_persists_verified_session_without_a_task(self):
        self.use_session_requester_scope()
        request_id = str(uuid.uuid4())
        result = json.loads(chat.handle(self.bridge, {
            "action": "add_comment", "issue": ISSUE, "id": request_id, "body": "An authorized update",
        }, self.linear_context()))
        self.assertTrue(result["ok"], result)
        payload = self.bridge.store.outbox_row(request_id)["payload"]
        self.assertEqual(payload["requester_sessions"], {ISSUE: SESSION})
        self.assertEqual(payload["requester_activities"], {ISSUE: ACTIVITY})
        self.assertEqual(self.kanban.creates, [])
        self.assertEqual(self.authority.mutations, [])

    def test_creator_null_later_activity_authorizes_status_content_and_retry_only_while_current(self):
        self.use_session_requester_scope()
        self.authority.sessions[SESSION]["creator"] = None
        event = self.event()
        event["agentSession"].pop("creatorId")
        projected = project_input(self.bridge, event)
        self.assertEqual(projected.actor_id, USER)
        context = self.linear_context()
        status = json.loads(chat.handle(self.bridge, {"action": "status", "issue": ISSUE}, context))
        self.assertTrue(status["ok"], status)
        request_id = str(uuid.uuid4())
        result = json.loads(chat.handle(self.bridge, {"action": "add_comment", "issue": ISSUE,
            "id": request_id, "body": "Preserve this exact authorized update"}, context))
        self.assertTrue(result["ok"], result)
        original = self.bridge.store.outbox_row(request_id)["payload"]
        self.authority.activities[ACTIVITY]["user"] = {"id": "revoked"}
        self.assertEqual(self.bridge.flush(only_id=request_id), 0)
        self.assertEqual(self.authority.mutations, [])
        self.assertNotEqual(self.bridge.store.outbox_row(request_id)["state"], "sent")
        after = self.bridge.store.outbox_row(request_id)
        self.assertEqual(after["payload"]["body"], original["body"])
        self.assertEqual(after["payload"]["requester_activities"], original["requester_activities"])
        self.assertNotEqual(after["state"], "sent")
        self.assertIsNone(self.authority.sessions[SESSION]["creator"])

    def test_native_linear_source_mismatch_refuses_without_effects(self):
        for changes in ({"user_id": "foreign"}, {"chat_id": "foreign"}, {"scope_id": "foreign"}):
            with self.subTest(changes=changes):
                result = json.loads(chat.handle(self.bridge, {"action": "status", "issue": ISSUE},
                                                self.linear_context(**changes)))
                self.assertFalse(result["ok"], result)
        self.assertEqual(self.authority.mutations, [])
        self.assertEqual(self.kanban.creates, [])


class InboxAcknowledgmentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "ingress.db"
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.db.execute("CREATE TABLE deliveries(profile TEXT,logical_agent TEXT,delivery_id TEXT,"
                        "payload TEXT,status TEXT,attempts INTEGER)")
        for profile in ("own", "other"):
            self.db.execute("INSERT INTO deliveries VALUES(?,?,?,?,?,?)",
                            (profile, "agent", "delivery", '{"type":"AgentSessionEvent"}', "pending", 0))
        self.db.commit()
        self.delivery = InboxDelivery(self.path, "own", "agent", "delivery", '{"type":"AgentSessionEvent"}')

    def test_ack_is_idempotent_and_does_not_touch_another_profile(self):
        self.delivery.acknowledge()
        self.delivery.acknowledge()
        self.assertEqual(self.db.execute("SELECT profile,status,attempts FROM deliveries ORDER BY profile").fetchall(),
                         [("other", "pending", 0), ("own", "imported", 1)])

    def test_changed_payload_or_missing_delivery_cannot_be_acknowledged(self):
        self.db.execute("UPDATE deliveries SET payload='{}' WHERE profile='own'")
        self.db.commit()
        with self.assertRaises(LinearError):
            self.delivery.acknowledge()
        self.assertEqual(self.db.execute("SELECT status,attempts FROM deliveries WHERE profile='own'").fetchone(),
                         ("pending", 0))
        self.db.execute("DELETE FROM deliveries WHERE profile='own'")
        self.db.commit()
        with self.assertRaises(LinearError):
            self.delivery.acknowledge()

    def test_a_missing_inbox_is_never_created_by_acknowledgment(self):
        missing = self.path.with_name("missing.db")
        with self.assertRaises(sqlite3.OperationalError):
            InboxDelivery(missing, "own", "agent", "delivery", "{}").acknowledge()
        self.assertFalse(missing.exists())
