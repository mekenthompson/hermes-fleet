"""A retained chat claim must never be presented as proof of active execution."""
from __future__ import annotations
import json
import sqlite3
import tempfile
import unittest
import uuid
from unittest.mock import patch
from pathlib import Path
from types import SimpleNamespace
from linear_fake_api import load_plugin, Clock, FakeLinear, SELF
load_plugin()
from hermes_fleet_linear_plugin import chat
from hermes_fleet_linear_plugin.api import LinearAPI
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.store import Store


class ChatOwnershipDetailTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / 'linear.db'
        self.clock = Clock()
        self.remote = FakeLinear(self.clock)
        self.addCleanup(self.remote.close)
        self.remote.add_issue('issue-1', 'ABC-1')
        self.remote.set_delegate('issue-1', {'id': SELF, 'name': 'This Agent'})
        self.remote.set_state('issue-1', 'Blocked')
        self.store = Store(self.path)
        self.store.put('issue-1', 'chat', 'owner-chat', run_generation=3,
                       last_updated_at=self.clock() * 1000)
        self.bridge = Bridge(self.store, LinearAPI(lambda: 'fixture', endpoint=self.remote.url),
                             SimpleNamespace(), profile='alpha', clock=self.clock)
        self.context = SimpleNamespace(session_key='new-chat', session_id='new-session',
                                       profile='alpha', platform='slack', run_generation=4)

    def snapshot(self):
        with sqlite3.connect(self.path) as db:
            work = db.execute('SELECT * FROM work').fetchall()
            outbox = db.execute('SELECT * FROM outbox').fetchall()
        return work, outbox, tuple(q for q in self.remote.requests if q.startswith('mutation'))

    def invoke(self, action):
        return json.loads(chat.handle(self.bridge, {'action': action, 'issue': 'ABC-1'}, self.context))

    def legacy_ack(self, store, state, session='legacy-session'):
        receipt = str(uuid.uuid5(uuid.NAMESPACE_URL, f'linear-session-received:issue-1:{session}'))
        store.enqueue('activity', {'id': receipt, 'issue_id': 'issue-1', 'session_id': session,
            'initial_ack': True, 'requester_sessions': {'issue-1': session},
            'content': {'type': 'thought', 'body': 'Received.'}}, at=self.clock())
        if state == 'failed': store.mark(receipt, 'failed')
        return receipt

    def test_chat_effects_cannot_cross_pending_or_failed_legacy_ack(self):
        actions = {
            'add_comment': {'id': str(uuid.uuid4()), 'body': 'Progress'},
            'update_description': {'id': str(uuid.uuid4()), 'description': 'Updated',
                                   'expected_description': ''},
            'done': {'evidence': 'https://docs.example/accepted'},
            'blocked': {'note': 'Waiting'}, 'release': {'note': 'Waiting'},
        }
        for state in ('pending', 'failed'):
            for action, fields in actions.items():
                with self.subTest(state=state, action=action):
                    store = Store(self.path.parent / f'{state}-{action}.db')
                    store.put('issue-1', 'chat', 'owner-chat')
                    store.update('issue-1', linear_session_id='own-session')
                    receipt = self.legacy_ack(store, state)
                    bridge = Bridge(store, self.bridge.api, SimpleNamespace(), profile='alpha', clock=self.clock)
                    context = SimpleNamespace(session_key='owner-chat', session_id='own-session',
                                              profile='alpha', platform='slack')
                    before_owner, before_receipt = store.get('issue-1'), store.outbox_row(receipt)
                    before_outbox = store.pending('issue-1')
                    before_mutations = tuple(q for q in self.remote.requests if q.startswith('mutation'))
                    reply = json.loads(chat.handle(bridge, {'action': action, 'issue': 'ABC-1', **fields}, context))
                    self.assertFalse(reply['ok'])
                    self.assertEqual(store.get('issue-1'), before_owner)
                    self.assertEqual(store.outbox_row(receipt), before_receipt)
                    self.assertEqual(store.pending('issue-1'), before_outbox)
                    self.assertEqual(bridge.flush(), 0)
                    self.assertEqual(tuple(q for q in self.remote.requests if q.startswith('mutation')), before_mutations)

    def test_persisted_recovery_holds_legacy_issue_and_continues_other_issue(self):
        self.store.put('issue-2', 'chat', 'other-chat')
        self.remote.add_issue('issue-2', 'ABC-2')
        self.remote.set_delegate('issue-2', {'id': SELF, 'name': 'This Agent'})
        receipt = self.legacy_ack(self.store, 'pending')
        before_owner, before_receipt = self.store.get('issue-1'), self.store.outbox_row(receipt)
        injected = []
        bridge = Bridge(Store(self.path), self.bridge.api, SimpleNamespace(), profile='alpha',
                        clock=self.clock, inject=lambda key, body: injected.append((key, body)) or True)
        bridge.recover()
        self.assertEqual([key for key, _ in injected], ['other-chat'])
        self.assertEqual(bridge.store.get('issue-1'), before_owner)
        self.assertEqual(bridge.store.outbox_row(receipt), before_receipt)
        self.assertFalse(bridge.store.recovery_delivered('issue-1', before_owner['ownership_id'], None, None))
        bridge.store.mark_sent(receipt, True)
        bridge.recover()
        self.assertEqual([key for key, _ in injected], ['other-chat', 'owner-chat'])

    def test_failed_legacy_receipt_never_injects_continuation_after_reopen(self):
        receipt = self.legacy_ack(self.store, 'failed')
        before_owner, before_receipt = self.store.get('issue-1'), self.store.outbox_row(receipt)
        injected = []
        bridge = Bridge(Store(self.path), self.bridge.api, SimpleNamespace(), profile='alpha',
                        clock=self.clock, inject=lambda key, body: injected.append((key, body)) or True)
        bridge.recover()
        self.assertEqual(injected, [])
        self.assertEqual(bridge.store.get('issue-1'), before_owner)
        self.assertEqual(bridge.store.outbox_row(receipt), before_receipt)

    def test_coherent_settled_legacy_receipt_allows_distinct_chat_session(self):
        self.context.session_key = 'owner-chat'
        receipt = self.legacy_ack(self.store, 'pending')
        self.store.mark_sent(receipt, True)
        args = {'action': 'add_comment', 'issue': 'ABC-1', 'id': str(uuid.uuid4()), 'body': 'Progress'}
        self.context.session_id = 'legacy-session'
        self.assertFalse(json.loads(chat.handle(self.bridge, args, self.context))['ok'])
        self.context.session_id = 'new-session'
        self.assertTrue(json.loads(chat.handle(self.bridge, args, self.context))['ok'])
        self.assertEqual(len(self.store.pending('issue-1')), 1)
        self.assertEqual(self.store.outbox_row(receipt)['state'], 'sent')

    def test_stop_cancellation_survives_uncertain_ack_while_status_stays_fenced(self):
        self.store.update('issue-1', linear_session_id='legacy-session')
        receipt = self.legacy_ack(self.store, 'pending')
        before = self.store.outbox_row(receipt)
        before_mutations = tuple(q for q in self.remote.requests if q.startswith('mutation'))
        self.bridge._stop(self.store.get('issue-1'), 'legacy-session', 'human',
                          self.clock() * 1000 + 1, 'stop-activity')
        self.assertTrue(self.store.get('issue-1')['stop_requested_at'])
        self.assertEqual(len(self.store.stop_intents()), 1)
        self.assertEqual(self.store.outbox_row(receipt), before)
        self.assertEqual(self.bridge.flush(), 0)
        self.assertEqual(tuple(q for q in self.remote.requests if q.startswith('mutation')), before_mutations)
        injected = []
        Bridge(Store(self.path), self.bridge.api, SimpleNamespace(), profile='alpha', clock=self.clock,
               inject=lambda key, body: injected.append(body) or True).recover()
        self.assertEqual(len(injected), 1)
        self.assertIn('Do not resume', injected[0])
        self.assertEqual(self.remote.state('issue-1'), 'Blocked')

    def test_link_issue_checks_both_exact_targets_before_transport(self):
        self.remote.add_issue('issue-2', 'ABC-2')
        self.legacy_ack(self.store, 'failed')
        with patch.object(self.bridge.api, 'link_issue') as link:
            reply = json.loads(chat.handle(self.bridge, {'action': 'link_issue', 'issue': 'ABC-2',
                'related': 'ABC-1', 'relation': 'related'}, self.context))
        self.assertFalse(reply['ok'])
        link.assert_not_called()

    def test_conflict_names_the_claim_without_asserting_active_work(self):
        before = self.snapshot()
        reply = self.invoke('start')
        self.assertFalse(reply['ok'])
        self.assertEqual(reply.get('code'), 'chat_ownership_conflict')
        self.assertEqual(reply['ownership']['owner_session_key'], 'owner-chat')
        self.assertEqual(reply['ownership']['execution_state'], 'unknown')
        self.assertIn('not proof of active work', reply['message'])
        self.assertIn('owner-chat', reply['message'])
        self.assertIn('Blocked', reply['message'])
        self.assertEqual(self.snapshot(), before)

    def test_stale_queued_status_cannot_change_successor_work(self):
        # Old chat captures a blocker, then closes locally before the transport drains.
        self.context.session_key = 'owner-chat'
        self.assertTrue(self.invoke('blocked')['ok'])
        closeout = json.loads(chat.handle(self.bridge, {'action': 'done', 'issue': 'ABC-1',
            'evidence': 'https://docs.example/accepted', 'note': 'verified'}, self.context))
        self.assertTrue(closeout['ok'])
        self.context.session_key = 'new-chat'
        self.assertTrue(self.invoke('start')['ok'])
        self.remote.set_state('issue-1', 'In Progress')
        queued = next(row for row in self.store.pending('issue-1') if row['kind'] == 'status'
                      and row['payload']['state'] == 'blocked')
        self.assertFalse(self.bridge._send(queued))
        self.assertEqual(self.remote.state('issue-1'), 'In Progress')

    def test_owner_change_during_credentials_blocks_the_status_transport(self):
        from hermes_fleet_linear_plugin.bridge import ProjectUpdateDeferred
        self.context.session_key = 'owner-chat'
        self.assertTrue(self.invoke('blocked')['ok'])
        queued = next(row for row in self.store.pending('issue-1') if row['kind'] == 'status')
        original = self.bridge.api.token
        def token():
            if getattr(self.bridge.api._mutation_guard, 'callback', None):
                self.store.put('issue-1', 'chat', 'new-chat', run_generation=4)
                self.remote.set_state('issue-1', 'In Progress')
            return original()
        self.bridge.api.token = token
        with self.assertRaises(ProjectUpdateDeferred):
            self.bridge._send(queued)
        self.assertEqual(self.remote.state('issue-1'), 'In Progress')
        self.assertFalse(any(q.startswith('mutation') for q in self.remote.requests))

    def test_closeout_conflicts_preserve_the_claim(self):
        for action in ('done', 'blocked', 'release'):
            with self.subTest(action=action):
                before = self.snapshot()
                reply = self.invoke(action)
                self.assertEqual(reply.get('code'), 'chat_ownership_conflict')
                self.assertEqual(reply['ownership']['execution_state'], 'unknown')
                self.assertEqual(self.snapshot(), before)

    def test_pending_release_is_distinct_from_competing_execution(self):
        self.store.update('issue-1', release_pending=1)
        before = self.snapshot()
        reply = self.invoke('start')
        self.assertEqual(reply.get('code'), 'release_pending')
        self.assertTrue(reply['ownership']['release_pending'])
        self.assertEqual(reply['ownership']['execution_state'], 'unknown')
        self.assertEqual(self.snapshot(), before)

    def test_task_binding_does_not_assert_that_the_task_is_running(self):
        self.store.update('issue-1', origin='kanban', owner_ref='linear-session', task_id='task-1')
        before = self.snapshot()
        reply = self.invoke('start')
        self.assertEqual(reply.get('code'), 'task_ownership_conflict')
        self.assertEqual(reply['ownership']['task_id'], 'task-1')
        self.assertIsNone(reply['ownership']['owner_session_key'])
        self.assertNotIn('already running', reply['message'])
        self.assertEqual(self.snapshot(), before)

    def test_stop_fence_is_visible_but_never_released(self):
        self.store.update('issue-1', stop_requested_at=self.clock() * 1000)
        before = self.snapshot()
        reply = self.invoke('start')
        self.assertTrue(reply['ownership']['stop_requested'])
        self.assertEqual(self.snapshot(), before)


if __name__ == '__main__':
    unittest.main()
