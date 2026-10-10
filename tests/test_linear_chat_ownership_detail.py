"""A retained chat claim must never be presented as proof of active execution."""
from __future__ import annotations
import json
import sqlite3
import tempfile
import unittest
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
