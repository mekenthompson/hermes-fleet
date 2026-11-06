"""Lost delegated status replies retain readback and order the single closeout."""
import asyncio
import json
import unittest
from unittest.mock import patch

import test_linear_native_delivery_seam as delivery


@unittest.skipUnless(delivery.seam.SOURCE and delivery.seam.GUARDED_ROOT,
                     'requires scripts/run-linear-input-scenarios.py')
class WorkerDeliveryScenarios(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = delivery.FinalDeliveryAdmissionScenarios.asyncSetUp
    asyncTearDown = delivery.FinalDeliveryAdmissionScenarios.asyncTearDown
    no_model = delivery.FinalDeliveryAdmissionScenarios.no_model
    until = delivery.FinalDeliveryAdmissionScenarios.until
    event = delivery.FinalDeliveryAdmissionScenarios.event
    deliver = delivery.FinalDeliveryAdmissionScenarios.deliver
    inbox_row = delivery.FinalDeliveryAdmissionScenarios.inbox_row
    rows = delivery.FinalDeliveryAdmissionScenarios.rows
    final_reply_sent = delivery.FinalDeliveryAdmissionScenarios.final_reply_sent
    start_live_for_delivery = delivery.FinalDeliveryAdmissionScenarios.start_live_for_delivery

    async def held_worker(self):
        from agent.display import build_status_phrase
        await self.start_live_for_delivery()
        self.adapter.set_authorization_check(lambda actor, *_a, **_k: actor == self.USER)
        issue = self.provider.issues[self.ISSUE]
        task = self.board.create(title='Bounded delegated continuation',
            body=f"Linear issue {self.ISSUE}: {issue['url']}", created_by=self.board.executor_profile)
        with self.board.conn() as conn, patch('hermes_cli.kanban_db.time.time', return_value=self.bridge.clock() - 2):
            task = self.board.kb.claim_task(conn, task.id)
        self.assertTrue(self.bridge.store.bind_native(self.ISSUE, task.id, 'default', self.board.board,
            owner_ref=self.SESSION, completion_scope=self.bridge._completion_scope(self.board, task),
            at=self.bridge.clock()))
        step = {'worker_session_id': 'actual-delegated-owner', 'execution_generation': 1,
                'admission_id': 'same-native-worker-admission', 'run_id': task.current_run_id,
                'phrase': build_status_phrase('terminal', None)}
        return task, self.bridge.store.get(self.ISSUE), step

    async def lose_step(self, binding):
        from hermes_fleet_linear_plugin import worker_progress
        transport, delayed = self.bridge.api.transport, []
        def lose(url, body, headers):
            fields = json.loads(body).get('variables', {}).get('input', {})
            if fields.get('ephemeral') and fields.get('content', {}).get('type') == 'action':
                delayed.append((url, body, headers))
                raise TimeoutError('client timed out; provider still processing')
            return transport(url, body, headers)
        await asyncio.to_thread(worker_progress.queue, self.bridge, binding, self.board)
        with patch.object(self.bridge.api, 'transport', side_effect=lose):
            await asyncio.to_thread(self.bridge.flush)
        self.assertEqual(len(delayed), 1)
        with self.bridge.store._tx() as db:
            identity = db.execute("SELECT id FROM outbox WHERE json_type(payload,'$.worker_step')='object'").fetchone()[0]
        pending = self.bridge.store.outbox_row(identity)
        self.assertEqual(pending['state'], 'pending')
        self.assertIs(pending['payload']['write_started'], True)
        return identity, transport, delayed

    async def test_lost_worker_action_accepted_after_genuine_question_restores_exact_question(self):
        from tools import clarify_gateway as clarify
        from hermes_fleet_linear_plugin import worker_progress
        from hermes_fleet_linear_plugin.store import Store
        _, binding, step = await self.held_worker()
        phase, wall = [step], self.bridge.clock()
        with patch.object(worker_progress, 'current', side_effect=lambda *_a: phase[0]):
            identity, transport, delayed = await self.lose_step(binding)
            key = self.fixture.store._generate_session_key(self.receipts[0][0].source)
            self.addCleanup(clarify.clear_session, key)
            shown = clarify.register('lost-worker-choice', key, 'Choose a color.', ['Blue', 'Green'])
            phase[0] = None
            self.bridge.clock = lambda: wall + 20
            sent = await self.adapter.send_clarify(self.ISSUE, shown.question, shown.choices,
                shown.clarify_id, key, metadata={'thread_id': self.SESSION})
            self.assertTrue(sent.success, sent.error)
            kinds = lambda: [a['content']['type'] for a in self.provider.activities.values() if 'content' in a]
            self.assertEqual(kinds()[-1], 'elicitation')
            await asyncio.to_thread(transport, *delayed[0])
            self.assertEqual(kinds()[-1], 'action')
            self.bridge.store = Store(self.bridge.store.path)  # Reporter replacement retains uncertainty.
            self.bridge.clock = lambda: wall + 120  # Expiry must not discard a possible remote effect.
            await asyncio.to_thread(self.bridge.flush)
            self.assertEqual(kinds()[-1], 'elicitation')
            self.assertEqual(sum(kind == 'action' for kind in kinds()), 1)
            original = self.bridge.store.outbox_row(sent.message_id)['payload']
            repaired = list(self.provider.activities.values())[-1]
            self.assertEqual(repaired['content']['body'], original['content']['body'])
            self.assertEqual(self.bridge.store.outbox_row(identity)['state'], 'sent')
            self.assertIsNone(shown.response)

    async def test_native_completion_stays_done_while_sole_response_waits_for_worker_receipt(self):
        from hermes_fleet_linear_plugin import worker_progress
        task, binding, step = await self.held_worker()
        phase, wall = [step], self.bridge.clock()
        with patch.object(worker_progress, 'current', side_effect=lambda *_a: phase[0]):
            identity, transport, delayed = await self.lose_step(binding)
            summary = 'Completed the bounded check. Evidence: its verification passed. Nothing remains.'
            with self.board.conn() as conn:
                self.assertTrue(self.board.kb.complete_task(conn, task.id, summary=summary,
                    expected_run_id=task.current_run_id))
            phase[0] = None
            self.bridge.clock = lambda: wall + 20
            await asyncio.to_thread(self.bridge.pump_kanban)
            await asyncio.to_thread(self.bridge.flush)
            self.assertEqual(self.board.get(task.id).status, 'done')
            self.assertEqual(self.provider.issues[self.ISSUE]['state']['type'], 'completed')
            self.assertTrue(self.bridge.store.unsettled_worker_action(self.ISSUE, self.SESSION))
            self.assertFalse(any(a.get('content', {}).get('type') == 'response' for a in self.provider.activities.values()))
            await asyncio.to_thread(transport, *delayed[0])
            self.bridge.clock = lambda: wall + 120
            await asyncio.to_thread(self.bridge.flush)
            self.bridge.clock = lambda: wall + 180
            await asyncio.to_thread(self.bridge.flush)
            finals = [a for a in self.provider.activities.values() if a.get('content', {}).get('type') == 'response']
            self.assertEqual(len(finals), 1)
            self.assertIn(summary, finals[0]['content']['body'])
            self.assertEqual(list(self.provider.activities.values())[-1]['content']['type'], 'response')
            self.assertEqual(self.bridge.store.outbox_row(identity)['state'], 'sent')
            self.assertFalse(self.bridge.store.unsettled_worker_action(self.ISSUE, self.SESSION))

    async def test_uncertain_action_is_read_only_after_expiry_and_native_stop(self):
        from hermes_fleet_linear_plugin import worker_progress
        task, binding, step = await self.held_worker()
        phase, wall = [step], self.bridge.clock()
        with patch.object(worker_progress, 'current', side_effect=lambda *_a: phase[0]):
            identity, _, delayed = await self.lose_step(binding)
            with self.board.conn() as conn:
                self.assertTrue(self.board.kb.block_task(conn, task.id, reason='Stopped by requester',
                    kind='needs_input', expected_run_id=task.current_run_id))
            phase[0] = None
            self.bridge.clock = lambda: wall + 120
            with patch.object(self.bridge.api, 'create_activity', wraps=self.bridge.api.create_activity) as creates:
                await asyncio.to_thread(self.bridge.flush, only_id=identity)
            self.assertEqual(creates.call_count, 0, 'An uncertain expired/stopped action is readback-only')
            self.assertEqual(len(delayed), 1)
            row = self.bridge.store.outbox_row(identity)
            self.assertEqual(row['state'], 'pending')
            self.assertIs(row['payload']['write_started'], True)
            self.assertIsNone(row['payload'].get('applied'))

    async def test_definite_rate_limit_rejection_does_not_fence_session_close(self):
        from hermes_fleet_linear_plugin import worker_progress
        _, binding, step = await self.held_worker()
        original = self.bridge.api.transport
        def reject(url, body, headers):
            fields = json.loads(body).get('variables', {}).get('input', {})
            if fields.get('ephemeral'):
                return 429, {'Retry-After': '1'}, json.dumps({'errors': [{'message': 'limited'}]}).encode()
            return original(url, body, headers)
        with patch.object(worker_progress, 'current', return_value=step):
            await asyncio.to_thread(worker_progress.queue, self.bridge, binding, self.board)
            with patch.object(self.bridge.api, 'transport', side_effect=reject):
                await asyncio.to_thread(self.bridge.flush)
        self.assertFalse(self.bridge.store.unsettled_worker_action(self.ISSUE, self.SESSION))
