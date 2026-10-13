"""Durable delivery regressions using the real bridge and persisted outbox."""
import tempfile
import uuid
import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from linear_fake_api import load_plugin
load_plugin()
from hermes_fleet_linear_plugin.bridge import Kanban
import unittest
from pathlib import Path

from test_linear_specialist_authorization import (
    Authority, FakeKanban, Bridge, Store, ISSUE, SESSION, USER, VIEWER,
)


class DurableCloseoutTests(unittest.TestCase):
    def test_post_ack_human_edit_prevents_worker_provisioning(self):
        for change in ('reassigned', 'completed', 'canceled'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                authority = Authority()
                authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
                store = Store(Path(directory) / 'state.db')
                kanban = FakeKanban()
                original = authority.transport
                def transport(url, body, headers):
                    result = original(url, body, headers)
                    if 'agentActivityCreate' in json.loads(body)['query']:
                        if change == 'reassigned':
                            authority.issues[ISSUE]['delegate'] = {'id': 'other-agent'}
                        else:
                            authority.issues[ISSUE]['state'] = {'type': change, 'name': change}
                            authority.issues[ISSUE]['updatedAt'] = '2026-01-02T00:00:00Z'
                    return result
                api = authority.api()
                api.transport = transport
                bridge = Bridge(store, api, kanban, profile='synthetic', clock=lambda: 10)
                self.assertTrue(bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start'))
                self.assertEqual(kanban.creates, [])
                self.assertIsNone(store.get(ISSUE))
                self.assertEqual(len(authority.mutations), 1)

    def test_acknowledgement_outcome_never_gates_current_ownership(self):
        """The receipt thought is advisory: ownership admits work, and a lost or rejected
        acknowledgement (including a legacy UUIDv5 row Linear refuses) never blocks the issue."""
        legacy = str(uuid.uuid5(uuid.NAMESPACE_URL, f'linear-session-received:{ISSUE}:{SESSION}'))
        for mode in ('rejected', 'unavailable', 'legacy_pending', 'legacy_failed', 'legacy_other_session'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                authority = Authority()
                authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
                authority.sessions['earlier-session'] = {'id': 'earlier-session', 'issue': {'id': ISSUE},
                                                         'creator': {'id': USER}}
                api = authority.api()
                original = authority.transport
                def transport(url, body, headers):
                    if mode in ('rejected', 'unavailable') and 'agentActivityCreate' in json.loads(body)['query']:
                        if mode == 'unavailable':
                            return 503, {}, b'{}'
                        return 200, {}, json.dumps({'data': None, 'errors': [
                            {'message': 'Argument Validation Error', 'extensions': {'code': 'INPUT_ERROR'}}]}).encode()
                    return original(url, body, headers)
                api.transport = transport
                store = Store(Path(directory) / 'state.db')
                if mode.startswith('legacy'):
                    store.enqueue('activity', {'id': legacy, 'issue_id': ISSUE, 'initial_ack': True,
                        'session_id': 'earlier-session' if mode == 'legacy_other_session' else SESSION,
                        'requester_sessions': {ISSUE: SESSION},
                        'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
                    if mode == 'legacy_failed':
                        store.mark(legacy, 'failed')
                kanban = FakeKanban()
                bridge = Bridge(store, api, kanban, profile='synthetic', clock=lambda: 10)
                self.assertTrue(bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start'))
                self.assertEqual(len(kanban.creates), 1)
                self.assertIsNotNone(store.get(ISSUE))
                bridge.flush()
                self.assertTrue(any('issueUpdate' in query for query in authority.mutations),
                                'the In Progress claim must not wait behind the acknowledgement')
                if mode.startswith('legacy') and mode != 'legacy_failed':
                    self.assertEqual(store.outbox_row(legacy)['state'], 'failed')  # Linear refuses v5 ids
                self.assertEqual(kanban.comments, [])  # a dropped thought is logged, never alerted
                self.assertEqual(bridge.flush(), 0)

    def test_applied_receipt_cannot_revive_stale_closed_issue(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            authority.issues[ISSUE]['state'] = {'type': 'completed', 'name': 'Done'}
            store = Store(Path(directory) / 'state.db')
            ack_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
                f'linear-session-received:{ISSUE}:{SESSION}'))
            store.enqueue('activity', {'id': ack_id, 'issue_id': ISSUE, 'session_id': SESSION,
                'initial_ack': True, 'requester_sessions': {ISSUE: SESSION},
                'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
            store.mark_sent(ack_id, True)
            kanban = FakeKanban()
            bridge = Bridge(store, authority.api(), kanban, profile='synthetic', clock=lambda: 10)
            self.assertTrue(bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start'))
            self.assertEqual(kanban.creates, [])

    def test_initial_ack_rechecks_delegation_at_mutation_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.sessions[SESSION]['appUser'] = {'id': VIEWER}
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            api = authority.api(scope={'all_teams': True, 'all_projects': True,
                'allowed_requester_ids': [USER], 'requester_basis': 'agent_session'})
            store = Store(Path(directory) / 'state.db')
            ack = store.enqueue('activity', {'issue_id': ISSUE, 'session_id': SESSION,
                'initial_ack': True, 'requester_sessions': {ISSUE: SESSION},
                'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
            guarded = api.guarded_mutation
            @contextmanager
            def takeover_before_send(guard):
                authority.issues[ISSUE]['delegate'] = {'id': 'human'}
                with guarded(guard):
                    yield
            api.guarded_mutation = takeover_before_send
            bridge = Bridge(store, api, FakeKanban(), profile='synthetic', clock=lambda: 10)
            self.assertEqual(bridge.flush(), 0)
            self.assertEqual(authority.mutations, [])
            self.assertEqual(store.outbox_row(ack)['attempts'], 0)

    def test_prior_uncertain_terminal_write_fences_prioritized_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.sessions[SESSION]['appUser'] = {'id': VIEWER}
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            api = authority.api(scope={'all_teams': True, 'all_projects': True,
                'allowed_requester_ids': [USER], 'requester_basis': 'agent_session'})
            store = Store(Path(directory) / 'state.db')
            prior = store.enqueue('status', {'issue_id': ISSUE, 'state': 'done', 'terminal': True,
                'requester_sessions': {ISSUE: SESSION}}, at=1)
            ack = store.enqueue('activity', {'issue_id': ISSUE, 'session_id': SESSION,
                'initial_ack': True, 'requester_sessions': {ISSUE: SESSION},
                'content': {'type': 'thought', 'body': 'Received.'}}, at=2)
            previous = store.outbox_row(prior)
            store.rewrite(prior, {**previous['payload'], 'write_started': True}, previous['next_at'])
            self.assertTrue(store.issue_reconciliation_blocked(ISSUE))
            bridge = Bridge(store, api, FakeKanban(), profile='synthetic', clock=lambda: 10)
            self.assertEqual(bridge.flush(), 0)
            self.assertEqual(store.outbox_row(ack)['state'], 'failed')  # one try: never posts after the hold
            self.assertEqual(authority.mutations, [])
            self.assertTrue(store.issue_reconciliation_blocked(ISSUE))

    def test_rate_limited_thought_keeps_its_place_until_the_pause_ends(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            api = authority.api()
            original = authority.transport
            def transport(url, body, headers):
                if 'agentActivityCreate' in json.loads(body)['query']:
                    return 429, {}, b'{}'
                return original(url, body, headers)
            api.transport = transport
            store = Store(Path(directory) / 'state.db')
            thought = store.enqueue('activity', {'issue_id': ISSUE, 'session_id': SESSION,
                'requester_sessions': {ISSUE: SESSION}, 'content': {'type': 'thought', 'body': 'Working'}}, at=1)
            bridge = Bridge(store, api, FakeKanban(), profile='synthetic', clock=lambda: 10)
            bridge.flush()
            self.assertEqual(store.outbox_row(thought)['state'], 'pending')  # nothing is written during the pause

    def test_unbound_authorization_failure_is_durable_and_never_borrows_new_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.sessions[SESSION]['appUser'] = {'id': VIEWER}
            api = authority.api(scope={
                'all_teams': True, 'all_projects': True,
                'allowed_requester_ids': [USER], 'requester_basis': 'agent_session',
            })
            path = Path(directory) / 'state.db'
            store = Store(path)
            old = store.enqueue('comment', {'issue_id': ISSUE, 'body': 'Old retirement', 'takeover': True}, at=1)
            store.put(ISSUE, 'kanban', SESSION, task_id='new-task')
            bridge = Bridge(store, api, FakeKanban(), profile='synthetic', clock=lambda: 10)
            self.assertEqual(bridge.flush(), 0)
            saved = Store(path).outbox_row(old)
            self.assertIn('requester', saved['payload'].get('authorization_error', '').lower())
            self.assertEqual(saved['state'], 'failed')
            self.assertEqual(saved['attempts'], 0)  # auth checks are not remote-send attempts
            self.assertNotIn('requester_sessions', saved['payload'])
            self.assertEqual(authority.mutations, [])

    def test_unbound_historical_row_does_not_block_a_new_authorized_result(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.sessions[SESSION]['appUser'] = {'id': VIEWER}
            api = authority.api(scope={'all_teams': True, 'all_projects': True,
                'allowed_requester_ids': [USER], 'requester_basis': 'agent_session'})
            path = Path(directory) / 'state.db'
            store = Store(path)
            old = store.enqueue('comment', {'issue_id': ISSUE, 'body': 'Unbound historical notice'}, at=1)
            new = store.enqueue('comment', {'issue_id': ISSUE, 'body': 'Saved review result',
                'requester_sessions': {ISSUE: SESSION}}, at=2)
            bridge = Bridge(store, api, FakeKanban(), profile='synthetic', clock=lambda: 10)
            self.assertEqual(bridge.flush(), 1)
            reopened = Store(path)
            self.assertEqual(reopened.outbox_row(old)['state'], 'failed')
            self.assertEqual(reopened.outbox_row(old)['attempts'], 0)
            self.assertEqual(reopened.outbox_row(new)['state'], 'sent')
            self.assertEqual(Bridge(reopened, api, FakeKanban(), profile='synthetic',
                clock=lambda: 11).flush(), 0)
            self.assertEqual(len(authority.mutations), 1)

    def test_acknowledgement_is_sent_before_task_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            store = Store(Path(directory) / 'state.db')
            backlog = store.enqueue('comment', {'issue_id': ISSUE, 'body': 'Older unrelated update'}, at=1)
            kanban = FakeKanban()
            create = kanban.create
            def checked_create(**fields):
                self.assertTrue(any('agentActivityCreate' in query for query in authority.mutations),
                                'no acknowledgement reached transport before worker provisioning')
                self.assertFalse(any('commentCreate' in query for query in authority.mutations))
                return create(**fields)
            kanban.create = checked_create
            bridge = Bridge(store, authority.api(), kanban, profile='synthetic')
            self.assertTrue(bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start'))
            self.assertEqual(store.outbox_row(backlog)['state'], 'pending')

    def test_acknowledgement_does_not_wait_for_an_unbound_predecessor(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.sessions[SESSION]['appUser'] = {'id': VIEWER}
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            api = authority.api(scope={'all_teams': True, 'all_projects': True,
                'allowed_requester_ids': [USER], 'requester_basis': 'agent_session'})
            store = Store(Path(directory) / 'state.db')
            old = store.enqueue('comment', {'issue_id': ISSUE, 'body': 'unbound old notice'}, at=1)
            kanban = FakeKanban()
            create = kanban.create
            def checked_create(**fields):
                self.assertTrue(any('agentActivityCreate' in query for query in authority.mutations))
                self.assertFalse(any('commentCreate' in query for query in authority.mutations))
                return create(**fields)
            kanban.create = checked_create
            bridge = Bridge(store, api, kanban, profile='synthetic')
            with api.requester_session(SESSION):
                self.assertTrue(bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start'))
            self.assertEqual(store.outbox_row(old)['state'], 'pending')

    def test_refused_session_gets_an_answer_instead_of_silence(self):
        for change in ('foreign', 'closed'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                authority = Authority()
                if change == 'foreign':
                    authority.issues[ISSUE]['delegate'] = {'id': 'other-agent', 'name': 'Other Agent'}
                else:
                    authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
                    authority.issues[ISSUE]['state'] = {'type': 'completed', 'name': 'Done'}
                    authority.issues[ISSUE]['completedAt'] = '2026-01-01T00:00:02Z'
                store, kanban = Store(Path(directory) / 'state.db'), FakeKanban()
                bridge = Bridge(store, authority.api(), kanban, profile='synthetic', clock=lambda: 10)
                self.assertTrue(bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start'))
                self.assertEqual(kanban.creates, [])
                [reply] = [r for r in store.pending(ISSUE) if r['kind'] == 'activity']
                self.assertEqual(reply['payload']['content']['type'], 'response')
                self.assertIn('Other Agent' if change == 'foreign' else 'closed', reply['payload']['content']['body'])

    def test_running_work_keeps_its_session_alive_but_never_covers_a_question(self):
        with tempfile.TemporaryDirectory() as directory:
            now = [10_000.0]
            store, kanban = Store(Path(directory) / 'state.db'), FakeKanban()
            bridge = Bridge(store, Authority().api(), kanban, profile='synthetic', clock=lambda: now[0])
            task = kanban.create(initial_status='running')
            kanban.tasks[task.id].status = 'running'
            store.put(ISSUE, 'kanban', SESSION, task_id=task.id)
            row = store.get(ISSUE)
            bridge._keepalive(row)
            [alive] = [r for r in store.pending(ISSUE) if r['kind'] == 'activity']
            self.assertEqual((alive['payload']['content']['action'], alive['payload']['ephemeral']), ('Working', True))
            now[0] += 60
            bridge._keepalive(row)  # recent activity: nothing more
            self.assertEqual(len([r for r in store.pending(ISSUE) if r['kind'] == 'activity']), 1)
            kanban.tasks[task.id].status = 'blocked'
            now[0] += 25 * 60
            bridge._keepalive(row)  # an elicitation is awaiting input: a thought would hide it
            self.assertEqual(len([r for r in store.pending(ISSUE) if r['kind'] == 'activity']), 1)

    def test_started_issue_keeps_its_status_and_a_missing_name_uses_first_started(self):
        from hermes_fleet_linear_plugin.api import state_id
        team = {'states': {'nodes': [{'id': 'review', 'name': 'In Review', 'type': 'started', 'position': 3},
                                     {'id': 'doing', 'name': 'Doing', 'type': 'started', 'position': 1},
                                     {'id': 'todo', 'name': 'Todo', 'type': 'unstarted', 'position': 0}]}}
        self.assertEqual(state_id({'team': team}, 'In Progress', first_started=True), 'doing')
        with self.assertRaises(Exception):
            state_id({'team': team}, 'Blocked')
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            authority.issues[ISSUE]['state'] = {'id': 'review', 'name': 'In Review', 'type': 'started'}
            api, sent = authority.api(), []
            original = api.update_issue
            api.update_issue = lambda issue_id, fields: sent.append(fields) or original(issue_id, fields)
            store = Store(Path(directory) / 'state.db')
            store.enqueue('status', {'issue_id': ISSUE, 'state': 'in_progress', 'claim': True, 'seen': VIEWER,
                                     'requester_sessions': {ISSUE: SESSION}}, at=1)
            Bridge(store, api, FakeKanban(), profile='synthetic', clock=lambda: 10).flush()
            self.assertTrue(sent)
            self.assertNotIn('stateId', sent[0])  # a human's later started state stands

    def test_local_completion_delivers_persisted_run_summary_without_url(self):
        summary = 'Reconciled all rows. The report lacks sale prices.'
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            api = authority.api()
            store = Store(Path(directory) / 'state.db')
            kanban = FakeKanban()
            task = kanban.create(title='OPS-1: review')
            task.status, task.result, task.completion_contract = 'done', None, 'local-only'
            adapter = object.__new__(Kanban)
            @contextmanager
            def connection():
                yield object()
            adapter.conn = connection
            adapter.kb = SimpleNamespace(latest_run=lambda conn, task_id: SimpleNamespace(
                summary=summary, metadata={'artifacts': ['/private/findings.md']}))
            kanban.evidence_text = adapter.evidence_text
            kanban.completion_summary = adapter.completion_summary
            store.put(ISSUE, 'kanban', SESSION, task_id=task.id)
            bridge = Bridge(store, api, kanban, profile='synthetic')
            bridge._finished(store.get(ISSUE))
            responses = [row['payload']['content'] for row in store.pending(ISSUE)
                         if row['kind'] == 'activity']
            self.assertEqual(responses[0]['type'], 'response')
            self.assertIn(summary, responses[0]['body'])
            self.assertNotIn('/private/', responses[0]['body'])

    def test_local_completion_requires_minimum_safe_summary_or_evidence(self):
        cases = (
            ('path only', '/private/report.md', '', 'local-only', False, ''),
            ('secret only', 'token=synthetic-secret', '', 'local-only', False, ''),
            ('placeholders only', '[local artifact] [redacted]', '', 'local-only', False, ''),
            ('uppercase redaction', '[REDACTED]', '', 'local-only', False, ''),
            ('uppercase artifact', '[LOCAL ARTIFACT]', '', 'local-only', False, ''),
            ('mixed-case redaction', 'Done. [ReDaCtEd]', '', 'local-only', False, ''),
            ('empty', '', '', 'local-only', False, ''),
            ('bare done', 'Done', '', 'local-only', False, ''),
            ('bare complete', 'COMPLETE!', '', 'local-only', False, ''),
            ('bare completed', 'Completed.', '', 'local-only', False, ''),
            ('bare ok', 'ok...', '', 'local-only', False, ''),
            ('bare done with path', 'Done. /private/report.md', '', 'local-only', False, ''),
            ('single letter', 'x', '', 'local-only', False, ''),
            ('single digit', '1', '', 'local-only', False, ''),
            ('short finding', 'Fixed typo.', '', 'local-only', True, 'Fixed typo.'),
            ('non-English finding', '原因を特定。', '', 'local-only', True, '原因を特定。'),
            ('mixed', 'Reviewed rows; found two missing prices. /private/report.md', '',
             'local-only', True, 'found two missing prices'),
            ('url', 'Results at https://example.test/findings', '', 'local-only', True,
             'https://example.test/findings'),
            ('url with generic summary', 'Done', 'https://example.test/findings',
             'local-only', True, 'https://example.test/findings'),
            ('strict PR', 'Reviewed rows; found two missing prices.', '',
             'https://github.com/example/repo/pull/1', False, ''),
        )
        for name, result, evidence_text, contract, done, expected in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                authority = Authority()
                store = Store(Path(directory) / 'state.db')
                kanban = FakeKanban()
                task = kanban.create(title='OPS-1: review')
                task.status, task.result, task.completion_contract = 'done', result, contract
                kanban.evidence_text = lambda task: evidence_text or task.result
                store.put(ISSUE, 'kanban', SESSION, task_id=task.id)
                bridge = Bridge(store, authority.api(), kanban, profile='synthetic')
                with patch.object(bridge, 'accepted_evidence', return_value=contract == 'local-only'):
                    bridge._finished(store.get(ISSUE))
                pending = store.pending(ISSUE)
                status = next(row['payload']['state'] for row in pending if row['kind'] == 'status')
                self.assertEqual(status, 'done' if done else 'blocked')
                self.assertEqual(any(row['kind'] == 'project_update' for row in pending), done)
                body = next(row['payload']['content']['body'] for row in pending if row['kind'] == 'activity')
                if expected:
                    self.assertIn(expected, body)
                self.assertNotIn('/private/', body)
                self.assertNotIn('synthetic-secret', body)

    def test_takeover_retains_original_requester_after_work_is_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.sessions[SESSION]['appUser'] = {'id': VIEWER}
            api = authority.api(scope={
                'all_teams': True, 'all_projects': True,
                'allowed_requester_ids': [USER], 'requester_basis': 'agent_session',
            })
            store = Store(Path(directory) / 'state.db')
            kanban = FakeKanban()
            task = kanban.create(title='OPS-1: review')
            store.put(ISSUE, 'kanban', SESSION, task_id=task.id)
            bridge = Bridge(store, api, kanban, profile='synthetic')
            with api.requester_session(SESSION):
                bridge.takeover(store.get(ISSUE), authority.issues[ISSUE])
            self.assertIsNone(store.get(ISSUE))
            pending = Store(Path(directory) / 'state.db').pending(ISSUE)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]['payload'].get('requester_sessions'), {ISSUE: SESSION})
            self.assertTrue(bridge._authorize_outbox_effect(pending[0]))


if __name__ == '__main__':
    unittest.main()
