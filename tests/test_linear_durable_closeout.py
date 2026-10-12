"""Durable delivery regressions using the real bridge and persisted outbox."""
import tempfile
import uuid
import json
import hashlib
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from linear_fake_api import load_plugin
load_plugin()
from hermes_fleet_linear_plugin.bridge import Kanban
import unittest
from pathlib import Path

from test_linear_specialist_authorization import (
    Authority, FakeKanban, Bridge, Store, ISSUE, SESSION, USER, VIEWER, SCOPE,
)


class DurableCloseoutTests(unittest.TestCase):
    def test_legacy_receipt_fences_existing_work_webhook_and_chat_followup(self):
        for origin, action in (('kanban', 'created'), ('kanban', 'prompted'),
                               ('chat', 'prompted')):
            with self.subTest(origin=origin, action=action), tempfile.TemporaryDirectory() as directory:
                authority = Authority()
                authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
                successor = 'successor-session'
                authority.sessions[successor] = {'id': successor, 'issue': {'id': ISSUE},
                    'creator': {'id': USER}, 'appUser': {'id': VIEWER}}
                authority.activities['successor-prompt'] = {'id': 'successor-prompt',
                    'user': {'id': USER}, 'agentSession': {'id': successor}}
                store = Store(Path(directory) / 'state.db')
                kanban = FakeKanban()
                if origin == 'kanban':
                    task = kanban.create(initial_status='blocked')
                    store.put(ISSUE, 'kanban', 'settled-session', task_id=task.id)
                    store.enqueue('status', {'issue_id': ISSUE, 'task_id': task.id,
                        'claim': True, 'state': 'in_progress'}, at=1)
                else:
                    store.put(ISSUE, 'chat', 'owning-chat')
                    store.update(ISSUE, linear_session_id='settled-session')
                legacy = str(uuid.uuid5(uuid.NAMESPACE_URL,
                    f'linear-session-received:{ISSUE}:{SESSION}'))
                store.enqueue('activity', {'id': legacy, 'issue_id': ISSUE,
                    'session_id': SESSION, 'initial_ack': True,
                    'requester_sessions': {ISSUE: SESSION},
                    'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
                before = store.get(ISSUE)
                pending = store.pending(ISSUE)
                bridge = Bridge(store, authority.api(scope={**SCOPE, 'requester_basis': 'agent_session'}),
                                kanban, profile='synthetic', inject=lambda *_: self.fail('injected'))
                event = {'type': 'AgentSessionEvent', 'action': action,
                    'createdAt': '2026-01-02T00:00:00Z', 'agentSession': {'id': successor,
                        'creatorId': USER, 'issue': {'id': ISSUE}}}
                if action == 'prompted':
                    event['agentActivity'] = {'id': 'successor-prompt',
                        'user': {'id': USER}, 'content': {'body': 'continue'}}
                with self.assertRaisesRegex(Exception, 'Legacy initial Linear acknowledgement'):
                    bridge.handle_webhook(event)
                self.assertEqual(store.get(ISSUE), before)
                self.assertEqual(store.pending(ISSUE), pending)
                self.assertEqual(authority.mutations, [])
                self.assertEqual(kanban.comments, [])
                self.assertEqual(len(kanban.creates), 1 if origin == 'kanban' else 0)
                if origin == 'kanban': self.assertEqual(task.status, 'blocked')

    def test_foreign_settled_legacy_binding_fences_third_session(self):
        for mismatch in ('id', 'session', 'requester', 'target', 'outcome', 'truthy_outcome'):
            with self.subTest(mismatch=mismatch), tempfile.TemporaryDirectory() as directory:
                authority = Authority()
                authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
                store = Store(Path(directory) / 'state.db')
                old = SESSION
                legacy = str(uuid.uuid5(uuid.NAMESPACE_URL,
                    f'linear-session-received:{ISSUE}:{old if mismatch != "id" else "foreign"}'))
                payload = {'id': legacy, 'issue_id': ISSUE,
                    'session_id': 'foreign' if mismatch == 'session' else old,
                    'initial_ack': True, 'requester_sessions': {ISSUE: old},
                    'content': {'type': 'thought', 'body': 'Received.'}}
                if mismatch == 'requester': payload['requester_sessions'] = {ISSUE: 'foreign'}
                if mismatch == 'target': payload['owner_issue_id'] = 'foreign-issue'
                store.enqueue('activity', payload, at=1)
                store.mark_sent(legacy, mismatch != 'outcome')
                if mismatch == 'truthy_outcome':
                    row = store.outbox_row(legacy)
                    store.rewrite(legacy, {**row['payload'], 'applied': 2}, 1)
                successor = 'third-session'
                authority.sessions[successor] = {'id': successor, 'issue': {'id': ISSUE},
                    'creator': {'id': USER}, 'appUser': {'id': VIEWER}}
                bridge = Bridge(store, authority.api(scope={**SCOPE, 'requester_basis': 'agent_session'}),
                                FakeKanban(), profile='synthetic')
                event = {'type': 'AgentSessionEvent', 'action': 'created',
                    'createdAt': '2026-01-02T00:00:00Z', 'agentSession': {'id': successor,
                        'creatorId': USER, 'issue': {'id': ISSUE}}}
                with self.assertRaisesRegex(Exception, 'Legacy initial Linear acknowledgement'):
                    bridge.handle_webhook(event)
                self.assertIsNone(store.get(ISSUE))
                self.assertEqual(authority.mutations, [])

    def test_valid_settled_legacy_predecessor_admits_distinct_webhook_session(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            successor = 'successor-session'
            authority.sessions[successor] = {'id': successor, 'issue': {'id': ISSUE},
                'creator': {'id': USER}, 'appUser': {'id': VIEWER}}
            store = Store(Path(directory) / 'state.db')
            legacy = str(uuid.uuid5(uuid.NAMESPACE_URL,
                f'linear-session-received:{ISSUE}:{SESSION}'))
            store.enqueue('activity', {'id': legacy, 'issue_id': ISSUE,
                'session_id': SESSION, 'initial_ack': True,
                'requester_sessions': {ISSUE: SESSION},
                'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
            store.mark_sent(legacy, True)
            kanban = FakeKanban()
            bridge = Bridge(store, authority.api(scope={**SCOPE, 'requester_basis': 'agent_session'}),
                            kanban, profile='synthetic', clock=lambda: 10)
            bridge.handle_webhook({'type': 'AgentSessionEvent', 'action': 'created',
                'createdAt': '2026-01-02T00:00:00Z', 'agentSession': {'id': successor,
                    'creatorId': USER, 'issue': {'id': ISSUE}}})
            self.assertEqual(len(kanban.creates), 1)
            self.assertEqual(store.get(ISSUE)['owner_ref'], successor)
            self.assertEqual(store.outbox_row(legacy)['state'], 'sent')
    @staticmethod
    def ack_id(session_id=SESSION):
        key = json.dumps([ISSUE, session_id], separators=(',', ':'))
        return str(uuid.UUID(bytes=hashlib.sha256(f'linear-session-received-v4:{key}'.encode()).digest()[:16],
                             version=4))

    def test_initial_ack_requires_canonical_uuid4_before_worker_admission(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            accepted = []
            original = authority.transport
            def transport(url, body, headers):
                request = json.loads(body)
                if 'agentActivityCreate' in request['query']:
                    activity_id = request['variables']['input']['id']
                    try:
                        parsed = uuid.UUID(activity_id)
                        canonical_v4 = parsed.version == 4 and str(parsed) == activity_id
                    except (TypeError, ValueError, AttributeError):
                        canonical_v4 = False
                    if not canonical_v4:
                        return 200, {}, json.dumps({
                            'data': None,
                            'errors': [{'message': 'id must be UUIDv4',
                                        'extensions': {'code': 'INVALID_INPUT'}}],
                        }).encode()
                    accepted.append(activity_id)
                return original(url, body, headers)
            api = authority.api()
            api.transport = transport
            store = Store(Path(directory) / 'state.db')
            kanban = FakeKanban()
            create = kanban.create
            def checked_create(**fields):
                self.assertGreaterEqual(len(accepted), len(kanban.creates) + 1,
                                        'worker admitted before accepted acknowledgement')
                return create(**fields)
            kanban.create = checked_create
            bridge = Bridge(store, api, kanban, profile='synthetic', clock=lambda: 10)
            self.assertTrue(bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start'))
            self.assertEqual(len(kanban.creates), 1)
            self.assertEqual(len(accepted), 1)
            receipt = store.outbox_row(accepted[0])
            self.assertEqual(receipt['state'], 'sent')
            self.assertEqual(receipt['payload']['applied'], 1)

            reopened = Bridge(Store(store.path), api, kanban, profile='synthetic', clock=lambda: 11)
            self.assertTrue(reopened._start({}, authority.issues[ISSUE], SESSION, 1,
                                            'synthetic-start', existing=store.get(ISSUE)))
            self.assertEqual(accepted, [self.ack_id()])

            other_session = 'session-other'
            authority.sessions[other_session] = {'id': other_session, 'issue': {'id': ISSUE},
                                                 'creator': {'id': USER}}
            reopened._start({}, authority.issues[ISSUE], other_session, 2, 'synthetic-other')
            self.assertEqual(accepted, [self.ack_id(), self.ack_id(other_session)])
            self.assertEqual(len(set(accepted)), 2)

    def test_initial_ack_reuses_pending_uuid4_after_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            original = authority.transport
            attempted = []
            def transport(url, body, headers):
                request = json.loads(body)
                if 'agentActivityCreate' in request['query']:
                    attempted.append(request['variables']['input']['id'])
                    if len(attempted) == 1:
                        return 503, {}, json.dumps({'errors': [{'message': 'synthetic outage'}]}).encode()
                return original(url, body, headers)
            api = authority.api()
            api.transport = transport
            path = Path(directory) / 'state.db'
            kanban = FakeKanban()
            bridge = Bridge(Store(path), api, kanban, profile='synthetic', clock=lambda: 10)
            with self.assertRaisesRegex(Exception, 'acknowledgement'):
                bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start')
            self.assertEqual(kanban.creates, [])
            self.assertEqual(attempted, [self.ack_id()])
            pending = Store(path).outbox_row(self.ack_id())
            self.assertEqual(pending['state'], 'pending')
            reopened = Bridge(Store(path), api, kanban, profile='synthetic', clock=lambda: 71)
            self.assertTrue(reopened._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start'))
            self.assertEqual(attempted, [self.ack_id(), self.ack_id()])
            self.assertEqual(len(kanban.creates), 1)
            self.assertEqual(reopened.store.outbox_row(self.ack_id())['payload']['applied'], 1)

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

    def test_unapplied_or_foreign_receipt_never_provisions_worker(self):
        for version in ('legacy_v5', 'v4'):
            modes = ("unapplied", "foreign_session", "uncertain")
            for mode in modes + (("applied_legacy", "failed_legacy") if version == 'legacy_v5' else ()):
                with self.subTest(version=version, mode=mode), tempfile.TemporaryDirectory() as directory:
                    authority = Authority()
                    authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
                    api = authority.api()
                    store = Store(Path(directory) / 'state.db')
                    ack_id = (str(uuid.uuid5(uuid.NAMESPACE_URL,
                        f'linear-session-received:{ISSUE}:{SESSION}')) if version == 'legacy_v5'
                        else self.ack_id())
                    store.enqueue('activity', {'id': ack_id, 'issue_id': ISSUE,
                        'session_id': 'other-session' if mode == 'foreign_session' else SESSION,
                        'initial_ack': True, 'requester_sessions': {ISSUE: SESSION},
                        'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
                    if mode != 'uncertain':
                        store.mark_sent(ack_id, mode in ('foreign_session', 'applied_legacy'))
                        if mode == 'failed_legacy':
                            store.mark(ack_id, 'failed')
                    else:
                        row = store.outbox_row(ack_id)
                        store.rewrite(ack_id, {**row['payload'], 'write_started': True}, 9999999999)
                    kanban = FakeKanban()
                    bridge = Bridge(store, api, kanban, profile='synthetic', clock=lambda: 10)
                    before = store.outbox_row(ack_id)
                    with self.assertRaisesRegex(Exception, 'acknowledgement'):
                        bridge._start({}, authority.issues[ISSUE], SESSION, 1, 'synthetic-start')
                    self.assertEqual(kanban.creates, [])
                    self.assertEqual(store.outbox_row(ack_id), before)
                    self.assertEqual(authority.mutations, [])

    def test_reopened_legacy_initial_ack_is_never_flushed_or_ticked(self):
        for state in ('pending', 'failed', 'sent'):
            for started in (False, True):
                with self.subTest(state=state, started=started), tempfile.TemporaryDirectory() as directory:
                    authority = Authority()
                    authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
                    path = Path(directory) / 'state.db'
                    store = Store(path)
                    legacy = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                           f'linear-session-received:{ISSUE}:{SESSION}'))
                    store.enqueue('activity', {'id': legacy, 'issue_id': ISSUE, 'session_id': SESSION,
                        'initial_ack': True, 'requester_sessions': {ISSUE: SESSION},
                        'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
                    if started:
                        row = store.outbox_row(legacy)
                        store.rewrite(legacy, {**row['payload'], 'write_started': True}, 1)
                    if state == 'failed':
                        store.mark(legacy, 'failed')
                    elif state == 'sent':
                        store.mark_sent(legacy, True)
                    reopened = Store(path)
                    before = reopened.outbox_row(legacy)
                    bridge = Bridge(reopened, authority.api(), FakeKanban(), profile='synthetic', clock=lambda: 10)
                    self.assertEqual(bridge.flush(), 0)
                    bridge.tick()
                    self.assertEqual(authority.mutations, [])
                    self.assertEqual(Store(path).outbox_row(legacy), before)

    def test_old_uncertain_legacy_ack_fences_new_session_on_same_issue(self):
        for state in ('pending', 'failed'):
            for started in (False, True):
                with self.subTest(state=state, started=started), tempfile.TemporaryDirectory() as directory:
                    authority = Authority()
                    authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
                    other = 'other-session'
                    authority.sessions[other] = {'id': other, 'issue': {'id': ISSUE},
                                                 'creator': {'id': USER}}
                    path = Path(directory) / 'state.db'
                    store = Store(path)
                    legacy = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                           f'linear-session-received:{ISSUE}:{SESSION}'))
                    store.enqueue('activity', {'id': legacy, 'issue_id': ISSUE, 'session_id': SESSION,
                        'initial_ack': True, 'requester_sessions': {ISSUE: SESSION},
                        'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
                    if started:
                        row = store.outbox_row(legacy)
                        store.rewrite(legacy, {**row['payload'], 'write_started': True}, 9999999999)
                    if state == 'failed':
                        store.mark(legacy, 'failed')
                    before = store.outbox_row(legacy)
                    kanban = FakeKanban()
                    bridge = Bridge(Store(path), authority.api(), kanban, profile='synthetic', clock=lambda: 10)
                    with self.assertRaisesRegex(Exception, 'Legacy initial Linear acknowledgement'):
                        bridge._start({}, authority.issues[ISSUE], other, 2, 'other-start')
                    self.assertEqual(authority.mutations, [])
                    self.assertEqual(kanban.creates, [])
                    self.assertIsNone(Store(path).outbox_row(self.ack_id(other)))
                    self.assertEqual(Store(path).outbox_row(legacy), before)

    def test_settled_legacy_ack_does_not_fence_other_session(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            other = 'other-session'
            authority.sessions[other] = {'id': other, 'issue': {'id': ISSUE}, 'creator': {'id': USER}}
            path = Path(directory) / 'state.db'
            store = Store(path)
            legacy = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                   f'linear-session-received:{ISSUE}:{SESSION}'))
            store.enqueue('activity', {'id': legacy, 'issue_id': ISSUE, 'session_id': SESSION,
                'initial_ack': True, 'requester_sessions': {ISSUE: SESSION},
                'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
            store.mark_sent(legacy, True)
            before = store.outbox_row(legacy)
            kanban = FakeKanban()
            bridge = Bridge(Store(path), authority.api(), kanban, profile='synthetic', clock=lambda: 10)
            self.assertTrue(bridge._start({}, authority.issues[ISSUE], other, 2, 'other-start'))
            self.assertEqual(len(kanban.creates), 1)
            self.assertEqual(Store(path).outbox_row(legacy), before)

    def test_failed_legacy_ack_is_not_revived_by_another_successful_write(self):
        with tempfile.TemporaryDirectory() as directory:
            authority = Authority()
            authority.issues[ISSUE]['delegate'] = {'id': VIEWER}
            path = Path(directory) / 'state.db'
            store = Store(path)
            legacy = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                   f'linear-session-received:{ISSUE}:{SESSION}'))
            store.enqueue('activity', {'id': legacy, 'issue_id': ISSUE, 'session_id': SESSION,
                'initial_ack': True, 'requester_sessions': {ISSUE: SESSION},
                'content': {'type': 'thought', 'body': 'Received.'}}, at=1)
            store.retry(store.outbox_row(legacy), 86402)
            other = store.enqueue('activity', {'issue_id': ISSUE, 'session_id': SESSION,
                'requester_sessions': {ISSUE: SESSION},
                'content': {'type': 'thought', 'body': 'Separate update.'}}, at=1)
            before = store.outbox_row(legacy)
            bridge = Bridge(Store(path), authority.api(), FakeKanban(), profile='synthetic', clock=lambda: 86402)
            self.assertEqual(bridge.flush(), 1)
            self.assertEqual(bridge.store.outbox_row(other)['state'], 'sent')
            self.assertEqual(Store(path).outbox_row(legacy), before)

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
            self.assertEqual(store.outbox_row(ack)['state'], 'pending')
            self.assertEqual(authority.mutations, [])

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
