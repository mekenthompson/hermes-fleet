"""Human-readable content through the real handler, SQLite and fake HTTP."""
from __future__ import annotations
import json
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from linear_fake_api import load_plugin, Clock, FakeLinear, SELF, OTHER
load_plugin()
from hermes_fleet_linear_plugin import chat
from hermes_fleet_linear_plugin.api import LinearAPI, LinearError
from hermes_fleet_linear_plugin.bridge import Bridge, ProjectUpdateDeferred
from hermes_fleet_linear_plugin.store import Store

BODY = '### Current status\n\n- Setup verified.\n\n### Remaining\n\n- [ ] Await replies.\n\n```sh\nprintf "a  b"\n```\n[Evidence](https://example.test/proof)'

class ContentRemote(FakeLinear):
    def _apply(self, query, variables):
        if 'CommentReadback' in query:
            found = next((c for c in self.comments if c['id'] == variables['id']), None)
            return 200, {'data': {'comment': ({'id': found['id'], 'body': found['body'],
                'issue': {'id': found['issueId']}} if found else None)}}
        if 'issueCreate' in query:
            data = variables['input']
            issue = self.add_issue(data['id'], 'ABC-2', project=None)
            issue.update(title=data['title'], description=data.get('description'), assignee=None, parent=None)
            return 200, {'data': {'issueCreate': {'success': True}}}
        if 'issueUpdate' in query and 'description' in variables['input']:
            self.issues[variables['id']]['description'] = variables['input']['description']
        return super()._apply(query, variables)

class ContentActionTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / 'state.db'
        self.clock = Clock()
        self.remote = ContentRemote(self.clock)
        self.addCleanup(self.remote.close)
        self.remote.add_issue('issue-1', 'ABC-1', project=None)
        self.remote.set_delegate('issue-1', {'id': SELF})
        self.store = Store(self.path)
        self.store.put('issue-1', 'chat', 'chat', run_generation=1)
        self.api = LinearAPI(lambda: 'synthetic', endpoint=self.remote.url)
        self.bridge = Bridge(self.store, self.api, SimpleNamespace(), profile='alpha', clock=self.clock)
        self.context = SimpleNamespace(session_key='chat', session_id='session', profile='alpha',
                                       platform='slack', run_generation=1)
        self.request_id = str(uuid.uuid4())

    def invoke(self, action='update_description', *, content=BODY, request_id=None, context=None):
        args = {'action': action, 'issue': 'ABC-1', 'id': request_id or self.request_id,
                'description': content, 'body': content, 'expected_description': 'Synthetic issue body'}
        # Capture durably first so tests can introduce a race before real send.
        with patch.object(self.bridge, 'flush'):
            return json.loads(chat.handle(self.bridge, args, context or self.context))

    def row(self):
        return self.store.outbox_row(self.request_id)

    def mutations(self):
        return [q for q in self.remote.requests if q.startswith('mutation')]

    def test_multiline_description_preserves_payload_and_other_fields(self):
        before = json.loads(json.dumps(self.remote.issues['issue-1']))
        self.assertTrue(self.invoke()['ok'])
        self.assertTrue(self.bridge._send(self.row()))
        self.assertEqual(self.remote.issues['issue-1']['description'], BODY)
        for key in ('title', 'delegate', 'state'):
            self.assertEqual(self.remote.issues['issue-1'][key], before[key])

    def test_multiline_comment_reads_exact_target_and_is_idempotent(self):
        self.assertTrue(self.invoke('add_comment')['ok'])
        self.assertTrue(self.bridge._send(self.row()))
        self.assertTrue(self.invoke('add_comment')['ok'])
        self.assertTrue(self.bridge._send(self.row()))
        self.assertEqual(len(self.remote.comments), 1)
        self.assertEqual(self.remote.comments[0]['body'], BODY)

    def test_create_multiline_description_is_read_back(self):
        created = self.api.create_issue(str(uuid.uuid4()), 'team-1', 'Human outcome', description=BODY)
        self.assertEqual(created['description'], BODY)
        self.assertTrue(any(q == 'query CreatedIssue' for q in self.remote.requests))

    def test_create_wrong_description_is_refused(self):
        original = self.remote._apply
        def changed(query, variables):
            status, payload = original(query, variables)
            if 'CreatedIssue' in query:
                payload['data']['issue']['description'] = 'lost all formatting'
            return status, payload
        self.remote._apply = changed
        with self.assertRaises(LinearError):
            self.api.create_issue(str(uuid.uuid4()), 'team-1', 'Outcome', description=BODY)

    def test_readback_wrong_or_missing_comment_target_and_body_is_refused(self):
        for changed in (None, {'id': self.request_id, 'body': BODY, 'issue': {'id': 'other'}},
                        {'id': 'wrong', 'body': BODY, 'issue': {'id': 'issue-1'}},
                        {'id': self.request_id, 'body': 'changed', 'issue': {'id': 'issue-1'}}):
            with self.subTest(changed=changed):
                with patch.object(self.api, 'graphql', return_value={'comment': changed}):
                    self.assertFalse(self.api.verify_comment(self.request_id, 'issue-1', BODY))

    def test_stop_release_foreign_chat_and_unbound_context_refuse_capture(self):
        for field in ('stop_requested_at', 'release_pending'):
            self.store.update('issue-1', **{field: 1})
            self.assertFalse(self.invoke()['ok'])
            self.store.update('issue-1', **{field: 0})
        self.context.session_key = 'other'
        self.assertFalse(self.invoke()['ok'])
        self.context.session_key = 'chat'
        self.context.profile = 'foreign'
        self.assertFalse(self.invoke()['ok'])
        self.assertEqual(self.mutations(), [])
        self.assertIsNone(self.row())

    def test_scope_denial_has_zero_content_mutations(self):
        with patch.object(self.bridge, 'authorize_specialist_effect', return_value=False):
            self.assertFalse(self.invoke()['ok'])
        self.assertIsNone(self.row())
        self.assertEqual(self.mutations(), [])

    def test_owner_replacement_before_send_suppresses_both_actions(self):
        for action in ('update_description', 'add_comment'):
            with self.subTest(action=action):
                self.request_id = str(uuid.uuid4())
                self.store.put('issue-1', 'chat', 'chat', run_generation=1)
                self.assertTrue(self.invoke(action)['ok'])
                self.store.put('issue-1', 'chat', 'successor', run_generation=2)
                self.assertFalse(self.bridge._send(self.row()))
        self.assertEqual(self.mutations(), [])

    def test_remote_foreign_delegate_or_closed_issue_suppresses_description(self):
        self.assertTrue(self.invoke()['ok'])
        self.remote.set_delegate('issue-1', OTHER)
        self.assertFalse(self.bridge._send(self.row()))
        self.remote.set_delegate('issue-1', {'id': SELF})
        self.remote.set_state('issue-1', 'Done')
        self.assertFalse(self.bridge._send(self.row()))
        self.assertEqual(self.mutations(), [])

    def test_owner_change_during_credentials_suppresses_both_transports(self):
        for action in ('update_description', 'add_comment'):
            with self.subTest(action=action):
                self.request_id = str(uuid.uuid4())
                self.store.put('issue-1', 'chat', 'chat', run_generation=1)
                self.assertTrue(self.invoke(action)['ok'])
                def token():
                    if getattr(self.api._mutation_guard, 'callback', None):
                        self.store.put('issue-1', 'chat', 'successor', run_generation=2)
                    return 'synthetic'
                self.api.token = token
                with self.assertRaises(ProjectUpdateDeferred):
                    self.bridge._send(self.row())
                self.api.token = lambda: 'synthetic'
        self.assertEqual(self.mutations(), [])

    def test_remote_delegate_change_during_credentials_suppresses_description(self):
        self.assertTrue(self.invoke()['ok'])
        def token():
            if getattr(self.api._mutation_guard, 'callback', None):
                self.remote.set_delegate('issue-1', OTHER)
            return 'synthetic'
        self.api.token = token
        with self.assertRaises(ProjectUpdateDeferred):
            self.bridge._send(self.row())
        self.assertEqual(self.mutations(), [])

    def test_description_response_loss_restart_reconciles_without_resend(self):
        self.assertTrue(self.invoke()['ok'])
        self.remote.lose_next_response = True
        with self.assertRaises(LinearError):
            self.bridge._send(self.row())
        self.assertTrue(self.row()['payload']['write_started'])
        self.store = Store(self.path)
        self.bridge.store = self.store
        # A crash before retry accounting leaves attempts zero, but send marker survives.
        self.assertEqual(self.row()['attempts'], 0)
        self.assertTrue(self.bridge._send(self.row()))
        self.assertEqual(len(self.mutations()), 1)

    def test_uncertain_description_never_overwrites_later_human_edit(self):
        self.assertTrue(self.invoke()['ok'])
        self.remote.lose_next_response = True
        with self.assertRaises(LinearError):
            self.bridge._send(self.row())
        self.remote.issues['issue-1']['description'] = 'Human changed this after send'
        self.bridge.store = Store(self.path)
        with self.assertRaises(LinearError):
            self.bridge._send(self.row())
        self.assertEqual(len(self.mutations()), 1)
        self.assertEqual(self.remote.issues['issue-1']['description'], 'Human changed this after send')

    def test_expected_description_mismatch_is_known_unsent(self):
        self.assertTrue(self.invoke()['ok'])
        self.remote.issues['issue-1']['description'] = 'Human edit before send'
        with self.assertRaises(LinearError):
            self.bridge._send(self.row())
        self.assertFalse(self.row()['payload'].get('write_started', False))
        self.assertEqual(self.mutations(), [])

    def test_id_reuse_conflict_is_controlled_and_not_reenqueued(self):
        self.assertTrue(self.invoke('add_comment')['ok'])
        reply = self.invoke('add_comment', content='Different request')
        self.assertFalse(reply['ok'])
        self.assertIn('conflicts', reply['message'])
        self.assertEqual(self.row()['payload']['body'], BODY)
        self.assertFalse(self.invoke(request_id='not-a-uuid')['ok'])
        self.assertEqual(self.mutations(), [])

    def test_flush_restart_after_response_loss_reconciles_exact_request(self):
        self.assertTrue(self.invoke()['ok'])
        self.remote.lose_next_response = True
        self.assertEqual(self.bridge.flush(), 0)
        self.assertEqual(self.row()['attempts'], 1)
        self.assertTrue(self.row()['payload']['write_started'])
        self.bridge.store = self.store = Store(self.path)
        self.clock.now += 3601
        self.assertTrue(self.invoke()['ok'])
        self.assertEqual(self.bridge.flush(), 1)
        self.assertEqual(self.row()['state'], 'sent')
        self.assertEqual(len(self.mutations()), 1)

    def test_unknown_description_fences_new_request_but_allows_same_id_readback(self):
        self.assertTrue(self.invoke()['ok'])
        self.remote.lose_next_response = True
        with self.assertRaises(LinearError):
            self.bridge._send(self.row())
        self.assertFalse(self.invoke(request_id=str(uuid.uuid4()))['ok'])
        self.assertTrue(self.invoke()['ok'])
        self.assertEqual(len(self.mutations()), 1)

    def test_markdown_canonicalization_preserves_code_links_and_checklist_state(self):
        from hermes_fleet_linear_plugin.api import markdown_matches
        canonical = BODY.replace('### Current status\n\n', '### Current status\n').replace('- Setup', '* Setup')
        self.assertTrue(markdown_matches(canonical, BODY))
        self.assertFalse(markdown_matches(BODY.replace('[ ]', '[x]'), BODY))
        self.assertFalse(markdown_matches(BODY.replace('a  b', 'a b'), BODY))
        self.assertFalse(markdown_matches(BODY.replace('/proof', '/wrong'), BODY))
        self.assertFalse(markdown_matches(BODY.replace('### Remaining', 'Remaining'), BODY))
        self.assertFalse(markdown_matches(None, BODY))

    def test_description_wrong_missing_id_and_wrong_body_readback_refused(self):
        for value in ({'id': 'other', 'description': BODY}, {'description': BODY},
                      {'id': 'issue-1', 'description': 'wrong'}):
            with self.subTest(value=value):
                with patch.object(self.api, 'issue', side_effect=[{'id': 'issue-1', 'description': 'old'}, value]), \
                        patch.object(self.api, 'update_issue'):
                    with self.assertRaises(LinearError):
                        self.api.replace_description('issue-1', 'old', BODY)

    def test_create_long_paragraph_warns_without_rewriting(self):
        long = 'Human prose. ' * 60
        args = {'action': 'create_issue', 'id': str(uuid.uuid4()), 'title': 'Outcome',
                'team': 'team-1', 'description': long}
        reply = json.loads(chat.handle(self.bridge, args, self.context))
        self.assertTrue(reply['ok'])
        self.assertIn('Formatting tip', reply['message'])
        created = self.remote.issues[args['id']]
        self.assertEqual(created['description'], long)
        self.assertIsNone(self.store.get(args['id']))

    def test_bound_specialist_allows_content_on_scope_and_refuses_foreign_scope(self):
        from hermes_fleet_linear_plugin import BoundLinearAPI
        original = self.remote._apply
        def identity(query, variables):
            if 'IdentityBinding' in query:
                return 200, {'data': {'viewer': {'id': SELF}, 'organization': {'id': 'org-1'}}}
            return original(query, variables)
        self.remote._apply = identity
        self.remote.issues['issue-1']['creator'] = {'id': 'requester-1'}
        self.remote.issues['issue-1']['project'] = {'id': 'project-1'}
        self.api = BoundLinearAPI(lambda: 'synthetic', endpoint=self.remote.url,
            identity={'viewer_id': SELF, 'organization_id': 'org-1'}, specialist_scope={
                'allowed_team_ids': ['team-1'], 'allowed_project_ids': ['project-1'],
                'allowed_requester_ids': ['requester-1']})
        self.bridge.api = self.api
        self.assertTrue(self.invoke('add_comment')['ok'])
        self.assertTrue(self.bridge._send(self.row()))
        self.request_id = str(uuid.uuid4())
        self.assertTrue(self.invoke()['ok'])
        self.assertTrue(self.bridge._send(self.row()))
        before = len(self.mutations())
        self.remote.issues['issue-1']['creator'] = {'id': 'foreign-requester'}
        self.assertFalse(self.invoke(request_id=str(uuid.uuid4()))['ok'])
        self.assertEqual(len(self.mutations()), before)

    def test_comment_response_loss_restart_does_not_duplicate(self):
        self.assertTrue(self.invoke('add_comment')['ok'])
        self.remote.lose_next_response = True
        self.assertEqual(self.bridge.flush(), 0)
        self.bridge.store = self.store = Store(self.path)
        self.clock.now += 3601
        self.assertEqual(self.bridge.flush(), 1)
        self.assertEqual(self.row()['state'], 'sent')
        self.assertEqual(len(self.remote.comments), 1)
        self.assertTrue(self.api.verify_comment(self.request_id, 'issue-1', BODY))

    def test_takeover_does_not_erase_unknown_description_receipt(self):
        self.assertTrue(self.invoke()['ok'])
        self.remote.lose_next_response = True
        self.assertEqual(self.bridge.flush(), 0)
        self.remote.issues['issue-1']['description'] = 'Human edit'
        self.remote.set_delegate('issue-1', OTHER)
        self.store.put('issue-1', 'chat', 'successor', run_generation=2)
        self.clock.now += 3601
        self.assertEqual(self.bridge.flush(), 0)
        self.assertEqual(self.row()['state'], 'failed')
        self.assertTrue(self.store.issue_reconciliation_blocked('issue-1'))
        self.assertEqual(len(self.mutations()), 1)

    def test_warning_does_not_transform_long_content(self):
        for action in ('update_description', 'add_comment'):
            self.request_id = str(uuid.uuid4())
            long = 'Long prose. ' * 60
            reply = self.invoke(action, content=long)
            self.assertTrue(reply['ok'])
            self.assertIn('Formatting tip', reply['message'])
            self.assertEqual(self.row()['payload']['body'], long)

if __name__ == '__main__':
    unittest.main()
