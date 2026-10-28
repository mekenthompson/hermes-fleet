"""Cold-review regressions with real pinned Core events and synthetic Linear."""
from contextlib import contextmanager
import copy
import unittest

import test_linear_native_bridge_scenarios as native
from hermes_fleet_linear_plugin.bridge import native_event_payload


@unittest.skipIf(native.CORE is not None, f"native upstream source unavailable ({native.CORE})")
class ReviewCompletionScopesScenarios(unittest.TestCase):
    def setUp(self):
        self.fixture = native.NativeBridgeScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_genuine_unchanged_assignment_outcome_completes_normally(self):
        f = self.fixture
        task_id = f.create()
        with f.board.conn() as conn:
            self.assertTrue(native.kb.complete_task(conn, task_id,
                result='Verified and delivered the complete accepted finite outcome.'))
        original = f.board.get(task_id)
        history = copy.deepcopy(f.board.history(task_id, 0))
        f.bridge.pump_kanban()
        f.bridge.flush()
        self.assertEqual(f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        self.assertEqual(f.board.get(task_id), original)
        self.assertEqual(f.board.history(task_id, 0), history)
        self.assertEqual(len(f.tasks()), 1)

    def test_discovered_completed_preparation_does_not_own_assignment_outcome(self):
        f = self.fixture
        issue = f.authority.issues[native.ISSUE]
        issue['title'] = 'Deliver the final artifact and verify recovery'
        issue['description'] = 'Create and deliver the final artifact, then verify recovery. Preparation is insufficient.'
        with f.board.conn() as conn:
            task_id = native.kb.create_task(conn, title='OPS-1: Read-only preparation', assignee='default',
                body=f"Linear issue OPS-1: {issue['url']}\nOnly inspect preparation; do not deliver the artifact.")
            self.assertTrue(native.kb.complete_task(conn, task_id,
                result='Verified preparation. Artifact delivery and recovery remain outstanding.'))
        original = f.board.get(task_id)
        history = copy.deepcopy(f.board.history(task_id, 0))
        f.bridge.handle_webhook(f.event())
        f.bridge.pump_kanban()
        self.assertEqual(f.store.native_binding(native.ISSUE)['task_id'], task_id)
        f.bridge.flush()
        self.assertNotEqual(issue['state']['type'], 'completed')
        self.assertFalse(any(row['kind'] == 'status' and row['payload'].get('state') == 'done'
                             for row in f.store.pending()))
        self.assertEqual(f.board.get(task_id), original)
        self.assertEqual(f.board.history(task_id, 0), history)
        self.assertEqual(len(f.tasks()), 1)

    def test_native_edited_body_revert_cannot_reconcile_partial_completion(self):
        f = self.fixture
        task_id = f.create()
        original = f.board.get(task_id)
        scope = copy.deepcopy(f.store.native_binding(native.ISSUE)['completion_scope'])
        with f.board.conn() as conn:
            self.assertTrue(native.kb.edit_task(conn, task_id, body='Only prepare; final delivery remains outstanding.'))
            self.assertTrue(native.kb.complete_task(conn, task_id, result='Verified the preparation stage and retained findings.'))
            self.assertTrue(native.kb.edit_task(conn, task_id, body=original.body))
        self.assertEqual(sum(e.kind == 'edited' for e in f.board.history(task_id, 0)), 2)
        self.assertEqual(f.board.get(task_id).body, original.body)
        self.assertEqual(f.store.native_binding(native.ISSUE)['completion_scope'], scope)
        f.bridge.pump_kanban()
        self.assertFalse(any(row['kind'] == 'status' and row['payload'].get('state') == 'done'
                             for row in f.store.pending()))
        f.bridge.flush()
        self.assertNotEqual(f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        self.assertEqual(f.board.get(task_id).status, 'done')
        self.assertEqual(len(f.tasks()), 1)

    def test_queued_done_rechecks_native_edited_version_at_http_boundary(self):
        f = self.fixture
        task_id = f.create()
        original_body = f.board.get(task_id).body
        with f.board.conn() as conn:
            self.assertTrue(native.kb.complete_task(conn, task_id, result='Verified the entire accepted finite outcome.'))
        f.bridge.pump_kanban()
        status = next(row for row in f.store.pending() if row['kind'] == 'status'
                      and row['payload'].get('state') == 'done')
        original_guard = f.bridge.api.guarded_mutation
        reached = []
        @contextmanager
        def edited_at_boundary(guard):
            @contextmanager
            def current_guard():
                reached.append(True)
                with f.board.conn() as conn:
                    self.assertTrue(native.kb.edit_task(conn, task_id, body='Preparation only; delivery remains outstanding.'))
                    self.assertTrue(native.kb.edit_task(conn, task_id, body=original_body))
                with guard():
                    yield
            with original_guard(current_guard):
                yield
        f.bridge.api.guarded_mutation = edited_at_boundary
        before = list(f.authority.mutations)
        f.bridge.flush(only_id=status['id'])
        self.assertTrue(reached)
        self.assertEqual(f.authority.mutations, before)
        self.assertNotEqual(f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        held = f.store.outbox_row(status['id'])
        self.assertIn(held['state'], ('pending', 'failed'))
        self.assertNotEqual(held['payload'].get('applied'), 1)
        self.assertEqual(f.board.get(task_id).body, original_body)
        self.assertEqual(sum(e.kind == 'edited' for e in f.board.history(task_id, 0)), 2)

    def test_native_result_backfill_cannot_hide_concurrent_body_edits(self):
        f = self.fixture
        task_id = f.create()
        original = f.board.get(task_id)
        with f.board.conn() as conn:
            self.assertTrue(native.kb.complete_task(conn, task_id,
                result='Verified the complete accepted outcome.'))
            self.assertTrue(native.kb.edit_task(conn, task_id,
                body='Only preparation was delivered; final delivery remains outstanding.',
                result='Verified preparation and retained its findings.'))
            self.assertTrue(native.kb.edit_task(conn, task_id, body=original.body,
                result='Verified preparation and retained its findings.'))
        edits = [e for e in f.board.history(task_id, 0) if e.kind == 'edited']
        self.assertEqual(len(edits), 2)
        self.assertEqual(f.board.get(task_id).body, original.body)
        # Core records result backfills without listing concurrent body edits.
        self.assertTrue(all('body' not in native_event_payload(e).get('fields', [])
                            for e in edits))
        f.bridge.pump_kanban()
        self.assertFalse(any(row['kind'] == 'status' and row['payload'].get('state') == 'done'
                             for row in f.store.pending()))
        f.bridge.flush()
        self.assertNotEqual(f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        self.assertEqual(f.board.get(task_id).status, 'done')
        self.assertEqual(len(f.tasks()), 1)
