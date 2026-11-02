"""Specialist closeout repair through real Core metadata and the durable outbox.

The native board, requester and Linear authority are isolated test fixtures.
These cases repair reporting only and create no replacement native executor.
"""
import copy
import datetime
import json
import unittest
import uuid

import test_linear_native_bridge_scenarios as native
from test_linear_specialist_authorization import SCOPE


ASSESSMENT = ('Recommendation: adopt the verified bounded change.\n'
              'The native investigation supplied supporting evidence.')


@unittest.skipIf(native.CORE is not None, f"native upstream source unavailable ({native.CORE})")
class AssessmentRepairScenarios(unittest.TestCase):
    def setUp(self):
        self.f = native.NativeBridgeScenarios()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.f.bridge = native.Bridge(self.f.store,
            self.f.authority.api(scope={**SCOPE, 'requester_basis': 'agent_session'}),
            self.f.board, profile='default')
        self.task_id = self.f.create()
        with self.f.board.conn() as conn:
            self.assertTrue(native.kb.complete_task(conn, self.task_id,
                result='Verified the entire accepted specialist outcome.',
                metadata={'issue_comment': ASSESSMENT}))
        self.f.bridge.pump_kanban()
        self.status = next(r for r in self.f.store.pending() if r['kind'] == 'status'
                           and r['payload'].get('state') == 'done')
        self.assessment = next(r for r in self.f.store.pending() if r['payload'].get('assessment'))
        self.created = self.status['payload']['source_ms'] / 1000
        self.native_task = self.f.board.get(self.task_id)
        self.native_history = copy.deepcopy(self.f.board.history(self.task_id, 0))
        self.cursor = self.f.store.get(native.ISSUE)['last_event_id']

    def human_edit(self, seconds):
        self.f.authority.human_edit(native.ISSUE, datetime.datetime.fromtimestamp(
            self.created + seconds, datetime.timezone.utc).isoformat())

    def reject_original_group(self):
        self.human_edit(100)
        self.f.bridge.clock = lambda: self.created + 300
        self.f.bridge.flush()
        for original in (self.status, self.assessment):
            row = self.f.store.outbox_row(original['id'])
            self.assertEqual((row['state'], row['payload'].get('applied')), ('sent', 0))
        self.assertEqual(self.f.authority.comment_records, {})

    def request(self, *, activity=False, request_id=None):
        return dict(requester_session=native.SESSION, replace=True, completion_scope='outcome',
                    reconcile_complete=True, expected_revision=self.f.store.binding_revision(native.ISSUE),
                    request_id=request_id or str(uuid.uuid4()),
                    **({'requester_activity': native.ACTIVITY} if activity else {}))

    def reconcile(self, request):
        with self.f.bridge.api.requester_session(native.SESSION):
            return self.f.bridge.track(native.ISSUE, self.task_id, **request)

    def assert_native_unchanged(self):
        self.assertEqual(self.f.board.get(self.task_id), self.native_task)
        self.assertEqual(self.f.board.history(self.task_id, 0), self.native_history)
        self.assertEqual(self.f.store.get(native.ISSUE)['last_event_id'], self.cursor)
        self.assertEqual(len(self.f.tasks()), 1)

    def assert_one_assessment(self):
        records = list(self.f.authority.comment_records.values())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['body'], ASSESSMENT)
        self.assertEqual(records[0]['issue']['id'], native.ISSUE)

    def repair_rejected_group(self, *, activity):
        self.reject_original_group()
        old = copy.deepcopy(self.f.store.outbox_row(self.assessment['id']))
        request = self.request(activity=activity)
        self.assertTrue(self.reconcile(request)['completion_reporting_reconciled'])
        repaired = [r for r in self.f.store.pending() if r['payload'].get('assessment')
                    and not r.get('hold_reason')]
        self.assertEqual(len(repaired), 1)
        self.assertNotIn(repaired[0]['id'], (self.assessment['id'], request['request_id']))
        self.assertEqual(repaired[0]['payload']['issue_id'], native.ISSUE)
        self.assertEqual(repaired[0]['payload']['task_id'], self.task_id)
        self.assertEqual(repaired[0]['payload']['requester_sessions'], {native.ISSUE: native.SESSION})
        if activity:
            self.assertTrue(self.f.store.outbox_row(self.assessment['id'])['hold_reason'])
            self.assertEqual(repaired[0]['payload']['requester_activities'], {native.ISSUE: native.ACTIVITY})
        rows = {r['id'] for r in self.f.store.pending()}
        self.assertTrue(self.reconcile(request)['completion_reporting_reconciled'])
        self.assertEqual({r['id'] for r in self.f.store.pending()}, rows)
        with self.f.bridge.api.requester_session(native.SESSION):
            self.f.bridge.flush()
        self.assertEqual(self.f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        self.assert_one_assessment()
        self.assertEqual(self.f.store.outbox_row(self.assessment['id'])['payload'], old['payload'])
        self.assert_native_unchanged()

    def test_known_unapplied_group_repairs_required_specialist_assessment(self):
        self.repair_rejected_group(activity=False)

    def test_pending_assessment_with_unapplied_status_cannot_satisfy_fresh_repair(self):
        self.human_edit(100)
        self.f.bridge.clock = lambda: self.created + 300
        self.f.bridge.flush(only_id=self.status['id'])
        original_status = copy.deepcopy(self.f.store.outbox_row(self.status['id']))
        self.assertEqual((original_status['state'], original_status['payload'].get('applied')), ('sent', 0))
        unattempted = self.f.store.outbox_row(self.assessment['id'])
        self.assertEqual((unattempted['state'], unattempted['attempts']), ('pending', 0))
        self.assertIsNot(unattempted['payload'].get('write_started'), True)
        self.assertEqual(unattempted['payload']['requires_status_id'], self.status['id'])
        self.assertEqual(self.f.authority.comment_records, {})
        request = self.request()
        self.assertTrue(self.reconcile(request)['completion_reporting_reconciled'])
        self.reconcile(request)
        self.f.bridge.flush()
        self.assertEqual(self.f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        self.assert_one_assessment()
        self.assertNotIn(self.assessment['id'], self.f.authority.comment_records)
        obsolete = self.f.store.outbox_row(self.assessment['id'])
        self.assertEqual((obsolete['state'], obsolete['payload'].get('applied')), ('sent', 0))
        self.assertEqual(obsolete['payload']['body'], ASSESSMENT)
        self.assertEqual(obsolete['payload']['requires_status_id'], self.status['id'])
        self.assertEqual(self.f.store.outbox_row(self.status['id']), original_status)
        self.assert_native_unchanged()

    def test_changed_requester_activity_repairs_without_old_comment_uuid_collision(self):
        self.repair_rejected_group(activity=True)

    def test_pending_assessment_is_reused_without_duplicate_repair(self):
        request = self.request()
        self.assertTrue(self.reconcile(request)['completion_reporting_reconciled'])
        assessments = [r for r in self.f.store.pending() if r['payload'].get('assessment')]
        self.assertEqual([r['id'] for r in assessments], [self.assessment['id']])
        self.f.bridge.flush()
        self.assert_one_assessment()
        self.assert_native_unchanged()

    def unknown_assessment_is_not_duplicated(self, *, activity):
        self.f.bridge.flush(only_id=self.status['id'])
        transport = self.f.bridge.api.transport
        lost = []
        def lost_reply(url, body, headers):
            result = transport(url, body, headers)
            if 'commentCreate' in json.loads(body)['query'] and not lost:
                lost.append(True)
                return 503, {}, b'{}'  # The remote effect exists; its response was lost.
            return result
        self.f.bridge.api.transport = lost_reply
        self.f.bridge.flush()  # Deliver its preceding activity before the assessment.
        self.f.bridge.api.transport = transport
        self.assertEqual(lost, [True])
        self.assert_one_assessment()
        before = copy.deepcopy(self.f.store.outbox_row(self.assessment['id']))
        self.assertTrue(self.f.store.uncertain(before, before['payload']))
        try:
            self.reconcile(self.request(activity=activity))
        except native.LinearError:
            pass  # Refusing the unresolved request is also safe; no effect is replayable.
        self.f.bridge.flush()
        self.assert_one_assessment()
        self.assertEqual(self.f.store.outbox_row(self.assessment['id'])['payload']['body'], before['payload']['body'])
        self.assertFalse(any(r['payload'].get('assessment') and r['id'] != self.assessment['id']
                             for r in self.f.store.pending()))
        self.assert_native_unchanged()

    def test_unknown_assessment_stays_held_without_replaying_recommendation(self):
        self.unknown_assessment_is_not_duplicated(activity=False)

    def test_changed_activity_does_not_duplicate_unknown_assessment(self):
        self.unknown_assessment_is_not_duplicated(activity=True)

    def applied_assessment_is_not_duplicated(self, *, activity):
        self.f.bridge.flush()
        self.assert_one_assessment()
        original = copy.deepcopy(self.f.store.outbox_row(self.assessment['id']))
        self.assertEqual(original['payload']['applied'], 1)
        request = self.request(activity=activity)
        self.assertTrue(self.reconcile(request)['completion_reporting_reconciled'])
        self.f.bridge.flush()
        self.assert_one_assessment()
        self.assertEqual(self.f.store.outbox_row(self.assessment['id'])['payload'], original['payload'])
        self.assert_native_unchanged()

    def test_applied_assessment_is_not_republished_by_fresh_repair(self):
        self.applied_assessment_is_not_duplicated(activity=False)

    def test_changed_activity_does_not_republish_applied_assessment(self):
        self.applied_assessment_is_not_duplicated(activity=True)

    def test_current_repair_closes_human_reopened_outcome_without_repeating_assessment(self):
        self.f.bridge.flush()
        self.assert_one_assessment()
        self.assertEqual(self.f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        original_comment = copy.deepcopy(self.f.store.outbox_row(self.assessment['id']))
        original_status = copy.deepcopy(self.f.store.outbox_row(self.status['id']))
        self.f.authority.issues[native.ISSUE]['state'] = {
            'id': 'state-todo', 'name': 'Todo', 'type': 'unstarted'}
        self.human_edit(100)
        self.f.bridge.clock = lambda: self.created + 300
        request = self.request()
        self.assertTrue(self.reconcile(request)['completion_reporting_reconciled'])
        self.assertIsNotNone(self.f.store.outbox_row(request['request_id']))
        self.reconcile(request)
        self.f.bridge.flush()
        self.assertEqual(self.f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        self.assert_one_assessment()
        self.assertEqual(self.f.store.outbox_row(self.assessment['id']), original_comment)
        self.assertEqual(self.f.store.outbox_row(self.status['id']), original_status)
        self.assertEqual(sum('issueUpdate' in q for q in self.f.authority.mutations), 2)
        self.assert_native_unchanged()

    def partial_applied_status_repairs_assessment(self, *, activity):
        self.f.bridge.flush(only_id=self.status['id'])
        self.assertEqual(self.f.store.outbox_row(self.status['id'])['payload']['applied'], 1)
        self.f.authority.issues[native.ISSUE]['delegate'] = {'id': 'foreign-human'}
        self.human_edit(100)
        self.f.bridge.clock = lambda: self.created + 300
        self.f.bridge.flush()
        rejected = self.f.store.outbox_row(self.assessment['id'])
        self.assertEqual((rejected['state'], rejected['payload'].get('applied')), ('sent', 0))
        self.assertEqual(self.f.authority.comment_records, {})
        self.f.authority.issues[native.ISSUE]['delegate'] = {'id': native.VIEWER}
        old_status = copy.deepcopy(self.f.store.outbox_row(self.status['id']))
        request = self.request(activity=activity)
        self.assertTrue(self.reconcile(request)['completion_reporting_reconciled'])
        self.assertTrue(any(r['payload'].get('assessment') for r in self.f.store.pending()))
        if activity:
            acknowledged = next(r for r in self.f.store.pending() if r['payload'].get('assessment')
                                and r['payload'].get('completion_reconcile_request_id') == request['request_id'])
            self.assertEqual(acknowledged['payload']['reconciled_status_id'], self.status['id'])
            self.assertNotIn('issue_write_receipt', acknowledged['payload'])
            self.assertTrue(self.f.store.outbox_row(self.status['id'])['hold_reason'])
        self.reconcile(request)
        self.f.bridge.flush()
        self.assert_one_assessment()
        self.assertEqual(self.f.store.outbox_row(self.status['id'])['payload'], old_status['payload'])
        self.assertEqual(sum('issueUpdate' in q for q in self.f.authority.mutations), 1)
        self.assert_native_unchanged()

    def test_applied_status_with_known_unapplied_assessment_repairs_only_missing_report(self):
        self.partial_applied_status_repairs_assessment(activity=False)

    def test_changed_activity_partial_closeout_uses_factual_applied_status_ack(self):
        self.partial_applied_status_repairs_assessment(activity=True)

    def test_newer_human_change_still_holds_repaired_assessment(self):
        self.reject_original_group()
        request = self.request()
        self.assertTrue(self.reconcile(request)['completion_reporting_reconciled'])
        self.human_edit(400)
        self.f.bridge.clock = lambda: self.created + 500
        self.reconcile(request)
        self.f.authority.mutations.clear()
        self.f.bridge.flush()
        self.assertEqual(self.f.authority.comment_records, {})
        self.assertNotEqual(self.f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        self.assertFalse(any('commentCreate' in q or 'issueUpdate' in q for q in self.f.authority.mutations))
        self.assert_native_unchanged()

    def test_repair_request_uuid_cannot_overwrite_existing_assessment_row(self):
        self.reject_original_group()
        original = copy.deepcopy(self.f.store.outbox_row(self.assessment['id']))
        request = self.request(request_id=self.assessment['id'])
        for _ in range(2):
            with self.assertRaisesRegex(ValueError, 'UUID conflicts'):
                self.reconcile(request)
            self.assertEqual(self.f.store.outbox_row(self.assessment['id']), original)
        self.assertEqual(self.f.authority.comment_records, {})
        self.assertNotEqual(self.f.authority.issues[native.ISSUE]['state']['type'], 'completed')
        self.assert_native_unchanged()
