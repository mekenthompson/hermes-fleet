"""Completion publication follows the latest admitted turn, including uncertain sends.

Uses the existing real-Core adapter/bridge fixture; only Linear's HTTP boundary is faked.
"""
from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import patch

import test_linear_native_prompt_scenarios as prompts

native = prompts.native


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class FinalDeliveryScenarios(unittest.IsolatedAsyncioTestCase):
    delivery = prompts.NativePromptScenarios.delivery
    deliver = prompts.NativePromptScenarios.deliver
    route = prompts.NativePromptScenarios.route
    start_turn = prompts.NativePromptScenarios.start_turn

    async def asyncSetUp(self):
        await prompts.NativePromptScenarios.asyncSetUp(self)
        self.remote = {}
        self.lost = set()
        self.absent = set()
        original = self.bridge.api.transport

        def transport(url, body, headers):
            request = json.loads(body)
            if "query OutboundAgentActivity" in request["query"]:
                record = self.remote.get(request["variables"]["id"])
                if record is None:
                    return 200, {}, json.dumps({"errors": [{"message": "Entity not found: AgentActivity",
                        "extensions": {"code": "INPUT_ERROR"}}]}).encode()
                return 200, {}, json.dumps({"data": {"agentActivity": record}}).encode()
            result = original(url, body, headers)
            if "agentActivityCreate" in request["query"]:
                fields = request["variables"]["input"]
                identity = fields["id"]
                if identity not in self.absent:
                    self.remote.setdefault(identity, {"id": identity, "user": {"id": native.VIEWER},
                        "agentSession": {"id": fields["agentSessionId"]},
                        "content": {"__typename": "AgentActivity" + fields["content"]["type"].capitalize() + "Content",
                                    "body": fields["content"]["body"]}})
                if identity in self.lost or identity in self.absent:
                    raise TimeoutError("fixture lost response")
            return result

        self.bridge.api.transport = transport

    def queue_final(self, source, body="Finished."):
        cursor = self.adapter._input_cursors.get((native.ISSUE, native.SESSION, source), 0)
        return self.native_input.queue_reply(self.bridge, native.ISSUE, native.SESSION, source, body,
                                             source_cursor=cursor)

    async def flush(self, identity):
        return await asyncio.to_thread(self.bridge.flush, only_id=identity)

    def make_due(self, identity):
        self.store.defer(identity, self.bridge.clock() - 1)

    async def test_late_interim_cannot_close_while_captured_pr_is_unaccepted(self):
        from hermes_cli import kanban_db as kb
        from hermes_fleet_linear_plugin import bridge as bridge_module
        task_id = self.fixture.create()
        with self.fixture.board.conn() as conn:
            self.assertIsNotNone(kb.claim_task(conn, task_id, claimer='probe-worker'))
        await asyncio.to_thread(self.bridge.pump_kanban)
        await asyncio.to_thread(self.bridge.flush)
        await self.start_turn()
        interim = self.queue_interim()
        self.lost.add(interim)
        await self.flush(interim)
        self.assertTrue(self.store.outbox_row(interim)['payload']['write_started'])
        receipt = {'ok': True, 'classification': 'passed', 'head_sha': 'a' * 40, 'checks': []}
        with patch.object(bridge_module, 'pr_acceptance', lambda *args: receipt):
            with self.fixture.board.conn() as conn:
                kb.complete_task(conn, task_id, summary='Implementation and local checks finished.',
                                 result='https://github.com/example/project/pull/12')
            await asyncio.to_thread(self.bridge.pump_kanban)
            status = self.store.latest_status(native.ISSUE)
            self.assertEqual(status['state'], 'done')
            receipt.update(ok=False, classification='pending')
            await self.flush(status['id'])
            self.assertEqual(self.store.outbox_row(status['id'])['state'], 'pending')
            self.assertNotEqual(self.authority.issues[native.ISSUE]['state']['type'], 'completed')
            self.make_due(interim)
            before = len(self.posted)
            await self.flush(interim)
            effects = [p['content'] for p in self.posted[before:]]
            self.assertNotIn('response', [p['type'] for p in effects])
            receipt.update(ok=True, classification="passed")
            self.make_due(status["id"])
            for _ in range(4):
                await asyncio.to_thread(self.bridge.flush)
            finals = [p for p in self.posted if p["content"]["type"] == "response"]
            self.assertEqual(len(finals), 1)
            self.make_due(interim)
            await self.flush(interim)
            self.assertEqual(len([p for p in self.posted if p["content"]["type"] == "response"]), 1)
            self.assertEqual(sum(p["id"] == interim for p in self.posted), 1)

    async def test_response_queued_before_new_input_is_thought_when_delivered(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        self.assertEqual(self.store.outbox_row(first)["payload"]["content"]["type"], "response")
        await self.deliver("newer-input", "Check the next part.")
        self.assertEqual(await self.flush(first), 1)
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")
        self.assertFalse(self.store.conversation_finished(native.ISSUE, native.SESSION, "newer-input"))
        self.assertTrue(self.store.conversation_finished(native.ISSUE, native.SESSION, native.ACTIVITY))

    async def test_older_thought_does_not_reopen_after_newer_final(self):
        await self.start_turn()
        await self.deliver("newer-input", "Check the next part.")
        first = self.queue_final(native.ACTIVITY)
        self.assertEqual(self.store.outbox_row(first)["payload"]["content"]["type"], "thought")
        second = self.queue_final("newer-input", "Both parts finished.")
        await self.flush(second)
        count = len(self.posted)
        self.assertEqual(self.posted[-1]["content"]["type"], "response")
        await self.flush(first)
        self.assertEqual(len(self.posted), count)
        self.assertFalse(self.store.outbox_row(first)["payload"]["applied"])
        # Suppression is about publication, not permission for A's progress to resume.
        self.assertTrue(self.store.conversation_finished(native.ISSUE, native.SESSION, native.ACTIVITY))

    async def test_admission_between_projection_and_mutation_defers_then_reprojects(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        original = self.bridge.store.admit_mutation
        raced = []
        def admit(row_id, **kwargs):
            result = original(row_id, **kwargs)
            if row_id == first and not raced:
                raced.append(True)
                self.store.remember_conversation_input(native.ISSUE, native.SESSION, "newer-input", "admission-b", 2, False)
            return result
        before = len(self.posted)
        with patch.object(self.bridge.store, "admit_mutation", side_effect=admit):
            await self.flush(first)
        self.assertEqual(len(self.posted), before, repr(self.posted))
        self.assertIs(self.store.outbox_row(first)["payload"]["write_started"], False)
        self.make_due(first)
        await self.flush(first)
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")

    async def test_lost_response_is_settled_read_only_after_newer_turn(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        self.lost.add(first)
        await self.flush(first)
        self.assertTrue(self.store.outbox_row(first)["payload"]["write_started"])
        await self.deliver("newer-input", "Check the next part.")
        before = len(self.posted)
        self.make_due(first)
        await self.flush(first)
        self.assertEqual(len(self.posted), before + 1)
        self.assertNotEqual(self.posted[-1]["id"], first)
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")
        self.assertEqual(self.store.outbox_row(first)["state"], "sent")
        self.assertEqual(self.store.outbox_row(first)["payload"]["content"]["type"], "response")

    async def test_lost_thought_is_not_reposted_after_newer_turn_finishes(self):
        await self.start_turn()
        await self.deliver("newer-input", "Check the next part.")
        first = self.queue_final(native.ACTIVITY)
        self.lost.add(first)
        await self.flush(first)
        second = self.queue_final("newer-input", "Both parts finished.")
        await self.flush(second)
        before = len(self.posted)
        self.make_due(first)
        await self.flush(first)
        self.assertEqual(len(self.posted), before + 1)
        self.assertNotEqual(self.posted[-1]["id"], first)
        self.assertEqual(self.store.outbox_row(first)["state"], "sent")
        self.assertEqual(self.store.outbox_row(first)["payload"]["content"]["type"], "thought")
        self.assertEqual(self.posted[-1]["content"]["type"], "response")

    async def test_uncertain_absent_activity_cannot_change_immutable_content(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        self.absent.add(first)
        await self.flush(first)
        await self.deliver("newer-input", "Check the next part.")
        before = len(self.posted)
        self.make_due(first)
        await self.flush(first)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(first)["state"], "pending")
        self.assertEqual(self.store.outbox_row(first)["payload"]["content"]["type"], "response")

    async def test_unscoped_native_done_still_fences_completion(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        self.store.enqueue("activity", {"id": "native-done", "issue_id": native.ISSUE,
            "session_id": native.SESSION, "content": {"type": "response", "body": "Done."}})
        before = len(self.posted)
        await self.flush(first)
        self.assertEqual(len(self.posted), before)
        self.assertFalse(self.store.outbox_row(first)["payload"]["applied"])

    async def test_uncertain_activity_from_another_actor_is_not_settled(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        self.lost.add(first)
        await self.flush(first)
        self.remote[first]["user"]["id"] = native.USER
        self.make_due(first)
        before = len(self.posted)
        await self.flush(first)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(first)["state"], "failed")

    async def test_another_sender_cannot_change_the_already_encoded_body(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        original = self.bridge.store.admit_mutation
        raced = []
        def admit(row_id, **kwargs):
            if row_id == first and not raced:
                raced.append(True)
                self.store.remember_conversation_input(native.ISSUE, native.SESSION, "newer-input", "admission-b", 2, False)
                current = self.store.outbox_row(row_id)["payload"]["content"]
                self.assertTrue(self.store.reproject_conversation_final(row_id, current, {**current, "type": "thought"}))
            return original(row_id, **kwargs)
        before = len(self.posted)
        with patch.object(self.bridge.store, "admit_mutation", side_effect=admit):
            await self.flush(first)
        self.assertEqual(len(self.posted), before)
        self.make_due(first)
        await self.flush(first)
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")

    async def test_rejected_sender_cannot_unfreeze_another_unknown_attempt(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        content = self.store.outbox_row(first)["payload"]["content"]
        self.assertTrue(self.store.admit_mutation(first, expected_content=content, conversation_attempt_id="sender-a"))
        self.assertTrue(self.bridge.store.admit_mutation(first, expected_content=content, conversation_attempt_id="sender-b"))
        self.store.conversation_not_attempted(first, "sender-a")
        self.assertTrue(self.store.outbox_row(first)["payload"]["write_started"])
        await self.deliver("newer-input", "Check the next part.")
        before = len(self.posted)
        await self.flush(first)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(first)["state"], "pending")

    async def test_earlier_summary_can_still_post_while_native_work_is_live(self):
        self.fixture.create()
        await self.start_turn()
        await self.deliver("newer-input", "Check the next part.")
        first = self.queue_final(native.ACTIVITY, "First summary.")
        second = self.queue_final("newer-input", "Second summary.")
        await self.flush(second)
        await self.flush(first)
        self.assertEqual(self.posted[-1]["content"], {"type": "thought", "body": "First summary."})

    async def test_unreadable_tracked_work_holds_final_without_guessing_finished(self):
        from hermes_fleet_linear_plugin.api import LinearError
        self.fixture.create()
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        before = len(self.posted)
        with patch.object(self.bridge, "_native_task", side_effect=LinearError("fixture unavailable")):
            await self.flush(first)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(first)["state"], "pending")
        self.assertIs(self.store.outbox_row(first)["payload"]["write_started"], False)
        self.make_due(first)
        await self.flush(first)
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")

    async def test_fresh_admission_cannot_replace_another_unknown_attempt(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        original = self.bridge.store.admit_mutation
        raced = []
        def admit(row_id, **kwargs):
            if row_id == first and not raced:
                raced.append(True)
                content = self.store.outbox_row(first)["payload"]["content"]
                self.assertTrue(self.store.admit_mutation(first, expected_content=content,
                                                         conversation_attempt_id="other-sender"))
                self.store.remember_conversation_input(native.ISSUE, native.SESSION,
                                                       "newer-input", "admission-b", 2, False)
            return original(row_id, **kwargs)
        before = len(self.posted)
        with patch.object(self.bridge.store, "admit_mutation", side_effect=admit):
            await self.flush(first)
        self.assertEqual(len(self.posted), before)
        payload = self.store.outbox_row(first)["payload"]
        self.assertIs(payload["write_started"], True)
        self.assertEqual(payload["conversation_attempt_id"], "other-sender")
        self.make_due(first)
        await self.flush(first)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(first)["state"], "pending")
        self.assertEqual(self.store.outbox_row(first)["payload"]["content"]["type"], "response")

    async def test_late_repair_keeps_its_identity_and_repairs_current_completion(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        self.lost.add(first)
        await self.flush(first)
        await self.deliver("newer-input", "Check the next part.")
        original = self.bridge.api.transport
        delayed = []
        def transport(url, body, headers):
            request = json.loads(body)
            if "agentActivityCreate" in request["query"]:
                fields = request["variables"]["input"]
                if fields["content"]["type"] == "thought":
                    delayed.append((url, body, headers))
                    raise TimeoutError("fixture holds lifecycle repair in flight")
            return original(url, body, headers)
        self.bridge.api.transport = transport
        self.make_due(first)
        await self.flush(first)
        self.assertEqual(len(delayed), 1)
        fields = json.loads(delayed[0][1])["variables"]["input"]
        self.assertNotEqual(fields["id"], first)
        second = self.queue_final("newer-input", "Both parts finished.")
        await self.flush(second)
        self.assertEqual(self.posted[-1]["content"]["type"], "response")
        original(*delayed[0])  # The repair thought lands after B's response.
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")
        self.make_due(fields["id"])
        await self.flush(fields["id"])
        self.assertEqual(self.posted[-1]["content"]["type"], "response")
        self.assertNotEqual(self.posted[-1]["id"], fields["id"])
        self.assertEqual(self.store.outbox_row(fields["id"])["payload"]["content"], fields["content"])
        self.assertEqual(self.store.outbox_row(fields["id"])["state"], "sent")

    async def test_repair_rechecks_completion_before_publication(self):
        await self.start_turn()
        first = self.queue_final(native.ACTIVITY)
        self.lost.add(first)
        await self.flush(first)
        await self.deliver("newer-input", "Check the next part.")
        original = self.bridge.store.admit_mutation
        repair_ids = []
        def admit(row_id, **kwargs):
            payload = self.store.outbox_row(row_id)["payload"]
            if payload.get("lifecycle_repair_of") and not repair_ids:
                repair_ids.append(row_id)
                self.queue_final("newer-input", "Both parts finished.")
            return original(row_id, **kwargs)
        self.make_due(first)
        before = len(self.posted)
        with patch.object(self.bridge.store, "admit_mutation", side_effect=admit):
            await self.flush(first)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(len(repair_ids), 1)
        self.make_due(repair_ids[0])
        await self.flush(repair_ids[0])
        self.assertEqual(self.posted[-1]["content"]["type"], "response")

    def queue_interim(self, kind="thought"):
        prompt = None
        if kind == "elicitation":
            from tools import clarify_gateway as clarify
            key = self.adapter._core_session_keys(self.messages[0].source)[0]
            clarify.register(clarify_id="probe", session_key=key, question="Which environment?",
                             choices=["staging", "production"])
            self.addCleanup(clarify.clear_session, key)
            prompt = {"kind": "clarify", "clarify_id": "probe", "session_key": key,
                      "question": "Which environment?", "choices": ["staging", "production"], "multi_select": False}
        return self.native_input.queue_reply(self.bridge, native.ISSUE, native.SESSION, native.ACTIVITY,
            "An interim update." if kind == "thought" else "Which environment?", kind=kind,
            nonce="interim-probe", one_try=kind == "thought", native_prompt=prompt,
            signal="select" if kind == "elicitation" else None,
            signal_metadata={"options": [{"value": "staging"}, {"value": "production"}]}
                            if kind == "elicitation" else None)

    async def test_attempted_interim_absent_readback_never_reposts(self):
        await self.start_turn()
        interim = self.queue_interim()
        self.absent.add(interim)
        await self.flush(interim)
        self.assertEqual(self.store.outbox_row(interim)["state"], "pending")
        before = len(self.posted)
        self.make_due(interim)
        await self.flush(interim)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(interim)["state"], "pending")
        self.assertTrue(self.store.outbox_row(interim)["payload"]["write_started"])

    async def test_attempted_interim_settles_read_only_while_turn_is_live(self):
        await self.start_turn()
        interim = self.queue_interim()
        self.lost.add(interim)
        await self.flush(interim)
        self.assertEqual(self.store.outbox_row(interim)["state"], "pending")
        before = len(self.posted)
        self.make_due(interim)
        await self.flush(interim)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(interim)["state"], "sent")
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")

    async def test_legacy_retired_uncertain_interim_is_recovered_for_readback_only(self):
        await self.start_turn()
        interim = self.queue_interim()
        self.lost.add(interim)
        await self.flush(interim)
        # Previous versions retired the ambiguous one-try row without a rejection verdict.
        with self.store._tx() as db:
            db.execute("UPDATE outbox SET state='failed',attempts=0 WHERE id=?", (interim,))
        final = self.queue_final(native.ACTIVITY)
        await self.flush(final)
        before = len(self.posted)
        self.make_due(interim)
        await self.flush(interim)
        self.assertEqual(len(self.posted), before + 1)
        self.assertEqual(self.posted[-1]["content"]["type"], "response")
        self.assertNotEqual(self.posted[-1]["id"], interim)
        self.assertEqual(self.store.outbox_row(interim)["payload"]["content"]["type"], "thought")
        self.assertEqual(self.store.outbox_row(interim)["state"], "sent")

    async def test_live_question_uncertainty_keeps_awaiting_input_and_exact_content(self):
        await self.start_turn()
        question = self.queue_interim("elicitation")
        self.lost.add(question)
        await self.flush(question)
        saved = self.store.outbox_row(question)["payload"]
        before = len(self.posted)
        self.make_due(question)
        await self.flush(question)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.posted[-1]["content"]["type"], "elicitation")
        self.assertEqual(self.store.outbox_row(question)["state"], "sent")
        self.assertEqual(self.store.outbox_row(question)["payload"]["native_prompt"], saved["native_prompt"])
        self.assertEqual(self.store.outbox_row(question)["payload"]["content"], saved["content"])
        self.assertEqual(self.store.outbox_row(question)["payload"]["signal_metadata"], saved["signal_metadata"])
        self.assertEqual(self.posted[-1]["signalMetadata"], saved["signal_metadata"])

    async def test_late_question_after_completion_repairs_without_replaying_question(self):
        await self.start_turn()
        question = self.queue_interim("elicitation")
        self.lost.add(question)
        await self.flush(question)
        final = self.queue_final(native.ACTIVITY)
        await self.flush(final)
        before = len(self.posted)
        self.make_due(question)
        await self.flush(question)
        self.assertEqual(len(self.posted), before + 1)
        self.assertEqual(self.posted[-1]["content"]["type"], "response")
        self.assertNotEqual(self.posted[-1]["id"], question)
        self.assertEqual(self.store.outbox_row(question)["payload"]["content"]["type"], "elicitation")
        self.assertEqual(self.store.outbox_row(question)["state"], "sent")

    async def test_interim_rechecks_completion_at_mutation_admission(self):
        await self.start_turn()
        interim = self.queue_interim()
        original = self.bridge.store.admit_mutation
        def admit(row_id, **kwargs):
            result = original(row_id, **kwargs)
            if row_id == interim:
                self.queue_final(native.ACTIVITY)
            return result
        before = len(self.posted)
        with patch.object(self.bridge.store, "admit_mutation", side_effect=admit):
            await self.flush(interim)
        self.assertEqual(len(self.posted), before)
        self.assertIs(self.store.outbox_row(interim)["payload"]["write_started"], False)
        self.assertEqual(self.store.outbox_row(interim)["state"], "failed")

    async def test_older_final_reply_without_turn_marker_is_not_an_interim(self):
        await self.start_turn()
        for kind in ("response", "error"):
            with self.subTest(kind=kind):
                final = self.queue_final(native.ACTIVITY, "Legacy " + kind + ".")
                payload = self.store.outbox_row(final)["payload"]
                payload.pop("turn_final")
                payload["content"]["type"] = kind
                self.store.rewrite(final, payload, self.bridge.clock())
                await self.flush(final)
                self.assertEqual(self.store.outbox_row(final)["state"], "sent")
                self.assertEqual(self.posted[-1]["content"], payload["content"])

    async def test_rate_limit_does_not_clear_an_earlier_unknown_final_attempt(self):
        await self.start_turn()
        final = self.queue_final(native.ACTIVITY)
        self.absent.add(final)
        await self.flush(final)
        self.assertTrue(self.store.outbox_row(final)["payload"]["write_started"])
        original = self.bridge.api.transport
        def transport(url, body, headers):
            if "agentActivityCreate" in json.loads(body)["query"]:
                return 429, {"Retry-After": "1"}, b"{}"
            return original(url, body, headers)
        self.bridge.api.transport = transport
        self.make_due(final)
        await self.flush(final)
        self.assertTrue(self.store.outbox_row(final)["payload"]["write_started"])
        self.assertEqual(self.store.outbox_row(final)["state"], "pending")

    async def test_partial_rate_limit_response_preserves_question_uncertainty(self):
        await self.start_turn()
        question = self.queue_interim("elicitation")
        original = self.bridge.api.transport
        creates = []
        def transport(url, body, headers):
            request = json.loads(body)
            if "agentActivityCreate" in request["query"]:
                creates.append(request["variables"]["input"])
                return 200, {}, json.dumps({"data": {"agentActivityCreate": {"success": True}},
                    "errors": [{"message": "partial rate limit", "extensions": {"code": "RATELIMITED"}}]}).encode()
            return original(url, body, headers)
        self.bridge.api.transport = transport
        await self.flush(question)
        self.assertTrue(self.store.outbox_row(question)["payload"]["write_started"])
        self.bridge.api.paused_until = 0
        self.make_due(question)
        await self.flush(question)
        self.assertEqual(len(creates), 1)
        self.assertTrue(self.store.outbox_row(question)["payload"]["write_started"])
        self.assertEqual(self.store.outbox_row(question)["state"], "pending")

    async def test_legacy_error_readback_preserves_immutable_error_content(self):
        await self.start_turn()
        final = self.queue_final(native.ACTIVITY, "The earlier request failed.")
        payload = self.store.outbox_row(final)["payload"]
        payload.pop("turn_final")
        payload.pop("write_started")
        payload.pop("source_activity")
        payload.pop("turn_activity")
        payload.pop("source_cursor")
        payload["content"]["type"] = "error"
        self.store.rewrite(final, payload, self.bridge.clock())
        self.remote[final] = {"id": final, "user": {"id": native.VIEWER},
            "agentSession": {"id": native.SESSION},
            "content": {"__typename": "AgentActivityErrorContent", "body": payload["content"]["body"]}}
        before = len(self.posted)
        await self.flush(final)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(final)["state"], "sent")
        self.assertEqual(self.store.outbox_row(final)["payload"]["content"], payload["content"])

    async def test_legacy_final_thought_readback_restores_completed_session(self):
        await self.start_turn()
        await self.deliver("newer-input", "Finish the current part.")
        old = self.queue_final(native.ACTIVITY, "Previous result is ready.")
        original = self.store.outbox_row(old)["payload"]
        legacy = {key: original[key] for key in ("id", "issue_id", "session_id", "content", "content_action",
                  "unlinked_content", "native_reply_version", "requester_sessions", "requester_activities")}
        self.assertEqual(legacy["content"]["type"], "thought")
        self.store.rewrite(old, legacy, self.bridge.clock())
        self.lost.add(old)
        await self.flush(old)
        final = self.queue_final("newer-input", "Everything is complete.")
        await self.flush(final)
        before = len(self.posted)
        self.make_due(old)
        await self.flush(old)
        self.assertEqual(len(self.posted), before + 1)
        self.assertEqual(self.posted[-1]["content"]["type"], "response")
        self.assertNotEqual(self.posted[-1]["id"], old)
        self.assertEqual(self.store.outbox_row(old)["payload"]["content"], legacy["content"])
        self.assertEqual(self.store.outbox_row(old)["state"], "sent")

    async def test_ambiguous_modern_thought_without_attempt_marker_stays_readback_only(self):
        await self.start_turn()
        interim = self.queue_interim()
        payload = self.store.outbox_row(interim)["payload"]
        payload.pop("write_started")
        payload.pop("turn_final", None)
        self.store.rewrite(interim, payload, self.bridge.clock())
        before = len(self.posted)
        for _ in range(2):
            self.make_due(interim)
            await self.flush(interim)
        self.assertEqual(len(self.posted), before)
        self.assertEqual(self.store.outbox_row(interim)["state"], "pending")
        self.assertNotIn("turn_final", self.store.outbox_row(interim)["payload"])
