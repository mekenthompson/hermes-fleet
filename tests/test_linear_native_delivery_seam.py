"""Guarded Core commit/publication boundaries and accepted late Activity repair."""
from __future__ import annotations

import asyncio
import json
import threading
from types import MethodType
import unittest
from unittest.mock import patch

import test_linear_native_input_seam as seam


@unittest.skipUnless(seam.SOURCE and seam.GUARDED_ROOT, "requires scripts/run-linear-input-scenarios.py")
class FinalDeliveryAdmissionScenarios(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = seam.NativeInputSeamScenarios.asyncSetUp
    asyncTearDown = seam.NativeInputSeamScenarios.asyncTearDown
    no_model = seam.NativeInputSeamScenarios.no_model
    until = seam.NativeInputSeamScenarios.until
    event = seam.NativeInputSeamScenarios.event
    deliver = seam.NativeInputSeamScenarios.deliver
    inbox_row = seam.NativeInputSeamScenarios.inbox_row
    rows = seam.NativeInputSeamScenarios.rows
    final_reply_sent = seam.NativeInputSeamScenarios.final_reply_sent

    async def test_committed_input_before_receipt_projection(self):
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        first_started, first_release, second_started, second_release = [asyncio.Event() for _ in range(4)]
        committed, return_commit = threading.Event(), threading.Event()
        self.addCleanup(return_commit.set)
        self.addCleanup(first_release.set)
        self.addCleanup(second_release.set)
        async def model(event):
            started, release = ((first_started, first_release) if event.message_id == 'first'
                                else (second_started, second_release))
            started.set()
            await release.wait()
            body = 'Finished ' + event.message_id
            execution_result.get()['result'] = {'completed': True, 'final_response': body}
            return body
        self.model = model
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        await self.deliver('first', 'Do the first task.')
        await asyncio.wait_for(first_started.wait(), 10)
        await self.until(lambda: self.inbox_row('first')[2] == 'imported')
        original = self.authority._admit_native_write
        def delayed_commit(**kwargs):
            result = original(**kwargs)
            if kwargs['request_id'] == 'second':
                committed.set()
                if not return_commit.wait(15):
                    raise TimeoutError('finite commit return delay')
            return result
        with patch.object(self.authority, '_admit_native_write', side_effect=delayed_commit):
            await self.deliver('second', 'Then do the second task.')
            await self.until(committed.is_set)
            self.assertEqual([row['status'] for row in self.rows()], ['started', 'queued'])
            first_release.set()
            try:
                await self.until(lambda: self.final_reply_sent('first'))
                with self.bridge.store._tx() as db:
                    payload = json.loads(db.execute("SELECT payload FROM outbox WHERE "
                        "json_extract(payload,'$.source_activity')='first' "
                        "AND json_extract(payload,'$.turn_final')=1").fetchone()[0])
                self.assertEqual(payload['content']['type'], 'thought',
                    'A final must not finish the session while B is already committed and unfinished')
                await asyncio.wait_for(second_started.wait(), 10)
                second_release.set()
                await self.until(lambda: self.final_reply_sent('second'))
                with self.bridge.store._tx() as db:
                    second = json.loads(db.execute("SELECT payload FROM outbox WHERE "
                        "json_extract(payload,'$.source_activity')='second' "
                        "AND json_extract(payload,'$.turn_final')=1").fetchone()[0])
                self.assertEqual(second['content']['type'], 'response')
                self.assertTrue(second['applied'],
                    "A's older projected anchor must not suppress B's completed response")
            finally:
                return_commit.set()
                second_release.set()
                await self.until(lambda: self.inbox_row('second')[2] == 'imported')

    async def test_timed_out_response_lands_during_newer_running_input(self):
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        first_started, first_release, second_started, second_release = [asyncio.Event() for _ in range(4)]
        self.addCleanup(first_release.set)
        self.addCleanup(second_release.set)
        async def model(event):
            started, release = ((first_started, first_release) if event.message_id == 'first'
                                else (second_started, second_release))
            started.set()
            await release.wait()
            body = 'Finished ' + event.message_id
            execution_result.get()['result'] = {'completed': True, 'final_response': body}
            return body
        self.model = model
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        original = self.bridge.api.transport
        late_requests = []
        remote_kinds = []
        def transport(url, body, headers):
            request = json.loads(body)
            if 'agentActivityCreate' in request['query']:
                content = request['variables']['input']['content']
                if content['body'] == 'Finished first':
                    late_requests.append((url, body, headers))
                    raise TimeoutError('response timed out while remote create remains in flight')
                remote_kinds.append(content['type'])
            return original(url, body, headers)
        self.bridge.api.transport = transport
        await self.deliver('first', 'Do the first task.')
        await asyncio.wait_for(first_started.wait(), 10)
        await self.until(lambda: self.inbox_row('first')[2] == 'imported')
        first_release.set()
        await self.until(lambda: bool(late_requests))
        await self.deliver('second', 'Then do the second task.')
        await asyncio.wait_for(second_started.wait(), 10)
        await self.until(lambda: self.inbox_row('second')[2] == 'imported')
        request = json.loads(late_requests[0][1])
        fields = request['variables']['input']
        # The timed-out request now lands at Linear, after B has started.
        original(*late_requests[0])
        remote_kinds.append(fields['content']['type'])
        self.bridge.store.defer(fields['id'], self.bridge.clock() - 1)
        await asyncio.to_thread(self.bridge.flush, only_id=fields['id'])
        row = self.bridge.store.outbox_row(fields['id'])
        self.assertEqual(remote_kinds[-1], 'thought',
            'Settling a late remote response must restore working status for the current running input')

    async def test_timed_out_thought_lands_after_newer_input_finishes(self):
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        first_started, first_release, second_started, second_release = [asyncio.Event() for _ in range(4)]
        self.addCleanup(first_release.set)
        self.addCleanup(second_release.set)
        async def model(event):
            started, release = ((first_started, first_release) if event.message_id == "first"
                                else (second_started, second_release))
            started.set()
            await release.wait()
            body = "Finished " + event.message_id
            execution_result.get()["result"] = {"completed": True, "final_response": body}
            return body
        self.model = model
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        original = self.bridge.api.transport
        late_requests, remote_kinds = [], []
        def transport(url, body, headers):
            request = json.loads(body)
            if "agentActivityCreate" in request["query"]:
                fields = request["variables"]["input"]
                if fields["content"]["body"] == "Finished first":
                    late_requests.append((url, body, headers))
                    raise TimeoutError("fixture holds the accepted mutation in flight")
                remote_kinds.append(fields["content"]["type"])
            return original(url, body, headers)
        self.bridge.api.transport = transport
        await self.deliver("first", "Do the first task.")
        await asyncio.wait_for(first_started.wait(), 10)
        await self.until(lambda: self.inbox_row("first")[2] == "imported")
        await self.deliver("second", "Do the second task.")
        await self.until(lambda: self.inbox_row("second")[2] == "imported")
        first_release.set()
        await self.until(lambda: bool(late_requests))
        await asyncio.wait_for(second_started.wait(), 10)
        second_release.set()
        await self.until(lambda: self.final_reply_sent("second"))
        self.assertEqual(remote_kinds[-1], "response")
        fields = json.loads(late_requests[0][1])["variables"]["input"]
        self.assertEqual(fields["content"]["type"], "thought")
        original(*late_requests[0])
        remote_kinds.append(fields["content"]["type"])
        self.bridge.store.defer(fields["id"], self.bridge.clock() - 1)
        await asyncio.to_thread(self.bridge.flush, only_id=fields["id"])
        self.assertEqual(remote_kinds[-1], "response")
        self.assertEqual(self.bridge.store.outbox_row(fields["id"])["state"], "sent")
        self.assertEqual(self.bridge.store.outbox_row(fields["id"])["payload"]["content"], fields["content"])

    async def test_unreadable_committed_input_state_holds_publication(self):
        from gateway.session_authority import SessionAuthority
        from hermes_fleet_linear_plugin.native_input import queue_reply
        started, release = asyncio.Event(), asyncio.Event()
        self.addCleanup(release.set)
        async def model(event):
            started.set()
            await release.wait()
            return None
        self.model = model
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        await self.deliver("first", "Do the first task.")
        await asyncio.wait_for(started.wait(), 10)
        await self.until(lambda: self.inbox_row("first")[2] == "imported")
        final = queue_reply(self.bridge, self.ISSUE, self.SESSION, "first", "Finished first.")
        before = len(self.provider.mutations)
        with patch("hermes_state_runtime.list_session_admissions", side_effect=OSError("fixture state unavailable")):
            await asyncio.to_thread(self.bridge.flush, only_id=final)
        self.assertEqual(len(self.provider.mutations), before)
        self.assertEqual(self.bridge.store.outbox_row(final)["state"], "pending")
        self.assertIs(self.bridge.store.outbox_row(final)["payload"]["write_started"], False)
        self.bridge.store.defer(final, self.bridge.clock() - 1)
        with patch.object(self.bridge, "native_conversation_state", None):
            await asyncio.to_thread(self.bridge.flush, only_id=final)
        self.assertEqual(len(self.provider.mutations), before)
        self.assertEqual(self.bridge.store.outbox_row(final)["state"], "pending")

    async def test_direct_created_session_reply_uses_verified_creator_route(self):
        from hermes_fleet_linear_plugin.native_input import queue_reply
        self.assertNotIn(self.SESSION, self.provider.activities)
        final = queue_reply(self.bridge, self.ISSUE, self.SESSION, self.SESSION, "Here is the requested update.")
        await asyncio.to_thread(self.bridge.flush, only_id=final)
        self.assertEqual(self.bridge.store.outbox_row(final)["state"], "sent")
        self.assertEqual(len(self.provider.mutations), 1)
        self.assertEqual(self.rows(), [])

    async def test_unfinished_and_unresolved_admissions_cannot_publish_completion(self):
        from gateway.session_authority import SessionAuthority
        from hermes_fleet_linear_plugin.native_input import queue_reply
        from hermes_state_runtime import list_session_admissions
        started, release = asyncio.Event(), asyncio.Event()
        self.addCleanup(release.set)
        async def model(event):
            started.set()
            await release.wait()
            return None
        self.model = model
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        await self.deliver("first", "Do the first task.")
        await asyncio.wait_for(started.wait(), 10)
        await self.until(lambda: self.inbox_row("first")[2] == "imported")
        final = queue_reply(self.bridge, self.ISSUE, self.SESSION, "first", "Still processing.")
        await asyncio.to_thread(self.bridge.flush, only_id=final)
        self.assertEqual(self.bridge.store.outbox_row(final)["payload"]["content"]["type"], "thought")
        before = len(self.provider.mutations)
        actual = self.rows()[0]
        for status in ("unknown", "terminal"):
            with self.subTest(status=status):
                held = queue_reply(self.bridge, self.ISSUE, self.SESSION, "first", "A later update " + status)
                unresolved = {**actual, "status": status, "outcome": None}
                with patch("hermes_state_runtime.list_session_admissions", return_value=[unresolved]):
                    await asyncio.to_thread(self.bridge.flush, only_id=held)
                self.assertEqual(len(self.provider.mutations), before)
                self.assertEqual(self.bridge.store.outbox_row(held)["state"], "pending")
                self.assertIs(self.bridge.store.outbox_row(held)["payload"]["write_started"], False)

    async def test_timed_out_interim_lands_after_real_core_completion(self):
        from gateway.session_authority import SessionAuthority
        from gateway.session_results import execution_result
        started, release = asyncio.Event(), asyncio.Event()
        self.addCleanup(release.set)
        delayed, remote_kinds = [], []
        original = self.bridge.api.transport
        def transport(url, body, headers):
            request = json.loads(body)
            if 'agentActivityCreate' in request['query']:
                fields = request['variables']['input']
                if fields['content']['body'] == 'An interim update.':
                    delayed.append((url, body, headers))
                    raise TimeoutError('finite remote create still in flight')
                remote_kinds.append(fields['content']['type'])
            return original(url, body, headers)
        self.bridge.api.transport = transport
        async def model(event):
            started.set()
            await release.wait()
            await self.adapter.send(self.ISSUE, 'An interim update.',
                metadata={'thread_id': self.SESSION, '_interim_send': True})
            execution_result.get()['result'] = {'completed': True, 'final_response': 'All work finished.'}
            return 'All work finished.'
        self.model = model
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        await self.deliver('first', 'Do the bounded work.')
        await asyncio.wait_for(started.wait(), 10)
        await self.until(lambda: self.inbox_row('first')[2] == 'imported')
        release.set()
        await self.until(lambda: self.final_reply_sent('first'))
        self.assertEqual(self.rows()[0]['status'], 'terminal')
        self.assertEqual(remote_kinds[-1], 'response')
        self.assertEqual(len(delayed), 1)
        fields = json.loads(delayed[0][1])['variables']['input']
        original(*delayed[0])
        remote_kinds.append(fields['content']['type'])
        self.bridge.store.defer(fields['id'], self.bridge.clock() - 1)
        await asyncio.to_thread(self.bridge.flush)
        print('LATE_INTERIM_PROOF', json.dumps({'remote_kinds': remote_kinds,
            'outbox_state': self.bridge.store.outbox_row(fields['id'])['state'],
            'core_status': self.rows()[0]['status']}), flush=True)
        self.assertEqual(remote_kinds[-1], 'response', 'Late interim must restore the completed session')

    async def start_live_for_delivery(self):
        from gateway.session_authority import SessionAuthority
        started, release = asyncio.Event(), asyncio.Event()
        self.addCleanup(release.set)
        async def model(event):
            started.set()
            await release.wait()
            return None
        self.model = model
        self.authority._schedule = MethodType(SessionAuthority._schedule, self.authority)
        await self.deliver("live-input", "Keep working until released.")
        await asyncio.wait_for(started.wait(), 10)
        await self.until(lambda: self.inbox_row("live-input")[2] == "imported")
        await self.until(lambda: bool(self.receipts))

    async def test_definitively_rejected_rate_limit_retries_the_exact_question(self):
        from tools import clarify_gateway as clarify
        await self.start_live_for_delivery()
        key = self.adapter._core_session_keys(self.receipts[0][0].source)[0]
        self.addCleanup(clarify.clear_session, key)
        original = self.bridge.api.transport
        for limited in ("http", "graphql"):
            with self.subTest(limited=limited):
                identity = "limited-question-" + limited
                clarify.register(clarify_id=identity, session_key=key,
                                 question="Which environment?", choices=["staging"])
                creates, reads = [], []
                def transport(url, body, headers):
                    request = json.loads(body)
                    if "agentActivityCreate" in request["query"]:
                        creates.append(request["variables"]["input"])
                        if len(creates) == 1:
                            if limited == "http":
                                return 429, {"Retry-After": "1"}, b"{}"
                            return 200, {}, json.dumps({"errors": [{"message": "rate limited",
                                "extensions": {"code": "RATELIMITED"}}]}).encode()
                    if "query OutboundAgentActivity" in request["query"]:
                        reads.append(request["variables"]["id"])
                    return original(url, body, headers)
                self.bridge.api.transport = transport
                result = await self.adapter.send_clarify(self.ISSUE, "Which environment?", ["staging"],
                    identity, key, metadata={"thread_id": self.SESSION})
                question = result.message_id
                self.assertTrue(question)
                started = self.bridge.store.outbox_row(question)["payload"]["write_started"]
                self.bridge.api.paused_until = 0
                self.bridge.store.defer(question, self.bridge.clock() - 1)
                await asyncio.to_thread(self.bridge.flush, only_id=question)
                self.assertIs(started, False)
                self.assertEqual(self.bridge.store.outbox_row(question)["state"], "sent")
                self.assertEqual(len(creates), 2)
                self.assertEqual(creates[0], creates[1])
                self.assertEqual(reads, [])

    def legacy_response(self, identity="c5cf9a31-7480-4667-bfff-81db6ea4703c"):
        # Exact pre-PR queue_reply shape: no source, final, cursor or attempt metadata.
        self.event("older-input", "Previous task.")
        legacy = {"id": identity, "issue_id": self.ISSUE, "session_id": self.SESSION,
            "content": {"type": "response", "body": "Previous task finished."},
            "content_action": True, "unlinked_content": True, "native_reply_version": 1,
            "requester_sessions": {self.ISSUE: self.SESSION},
            "requester_activities": {self.ISSUE: "older-input"}}
        return self.bridge.store.enqueue("activity", legacy, at=self.bridge.clock())

    async def test_pre_upgrade_response_cannot_finish_new_running_turn(self):
        identity = self.legacy_response()
        await self.start_live_for_delivery()
        before = len(self.provider.mutations)
        await asyncio.to_thread(self.bridge.flush, only_id=identity)
        row = self.bridge.store.outbox_row(identity)
        self.assertEqual(len(self.provider.mutations), before)
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["payload"]["content"], {"type": "response", "body": "Previous task finished."})

    async def test_accepted_pre_upgrade_response_is_settled_and_repairs_current_work(self):
        identity = self.legacy_response()
        original = self.bridge.store.outbox_row(identity)
        def land():
            with self.bridge._outbox_requester_scope(original):
                self.bridge.api.create_activity(identity, self.SESSION, original["payload"]["content"],
                                                issue_id=self.ISSUE)
        await asyncio.to_thread(land)
        await self.start_live_for_delivery()
        before = len(self.provider.mutations)
        await asyncio.to_thread(self.bridge.flush, only_id=identity)
        settled = self.bridge.store.outbox_row(identity)
        self.assertEqual(settled["state"], "sent")
        self.assertEqual(settled["payload"]["content"], original["payload"]["content"])
        self.assertEqual(settled["payload"]["requester_activities"], original["payload"]["requester_activities"])
        self.assertEqual(len(self.provider.mutations), before + 1)
        with self.bridge.store._tx() as db:
            repair = json.loads(db.execute("SELECT payload FROM outbox WHERE "
                "json_extract(payload,'$.lifecycle_repair_of')=?", (identity,)).fetchone()[0])
        self.assertEqual(repair["content"]["type"], "thought")
        self.assertNotEqual(repair["id"], identity)
        await asyncio.to_thread(self.bridge.flush, only_id=identity)
        self.assertEqual(len(self.provider.mutations), before + 1)
