"""Equivalent remote formatting settles a description; a genuinely uncertain one says how to reconcile it."""
from __future__ import annotations
import json
import re
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from linear_fake_api import load_plugin, Clock, SELF
load_plugin()
from hermes_fleet_linear_plugin import chat, ops
from hermes_fleet_linear_plugin.api import LinearAPI, markdown_matches
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.store import Store
from test_linear_content_actions import ContentRemote

CHECKED = '### Acceptance\n\n- [x] Hourly collection\n- [x] Forecasts\n- [ ] Follow-up'

class CheckboxRemote(ContentRemote):
    """Linear's editor stores a checked task item as `[X]`."""
    def _apply(self, query, variables):
        fields = (variables or {}).get('input') or {}
        if 'issueUpdate' in query and isinstance(fields.get('description'), str):
            stored = re.sub(r'^(\s*[-*+] )\[x\]', r'\1[X]', fields['description'], flags=re.M)
            variables = {**variables, 'input': {**fields, 'description': stored}}
        return super()._apply(query, variables)

class CheckboxCanonicalTests(unittest.TestCase):
    def test_checked_task_case_is_equivalent_but_state_and_code_are_not(self):
        self.assertTrue(markdown_matches('- [X] Done', '- [x] Done'))
        self.assertTrue(markdown_matches('* [X] Done', '- [x] Done'))
        self.assertTrue(markdown_matches('  - [X] Nested', '  - [x] Nested'))
        self.assertTrue(markdown_matches('1. [X] Ordered', '1. [x] Ordered'))
        self.assertFalse(markdown_matches('- [ ] Done', '- [x] Done'))
        self.assertFalse(markdown_matches('- [X] Done', '- [ ] Done'))
        self.assertFalse(markdown_matches('```\n- [X] code\n```', '```\n- [x] code\n```'))
        self.assertFalse(markdown_matches('See [X] here', 'See [x] here'))
        self.assertFalse(markdown_matches('- [X] Done', '- [x] Gone'))
        self.assertFalse(markdown_matches('- [X](https://a.test)', '- [x](https://a.test)'))
        self.assertFalse(markdown_matches('- [X]abc', '- [x]abc'))

class DescriptionSettlementTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / 'state.db'
        self.ingress = Path(tmp.name) / 'ingress.db'
        self.clock = Clock()
        self.remote = CheckboxRemote(self.clock)
        self.addCleanup(self.remote.close)
        self.remote.add_issue('issue-1', 'ABC-1', project=None)
        self.remote.set_delegate('issue-1', {'id': SELF})
        self.store = Store(self.path)
        self.store.put('issue-1', 'chat', 'chat', run_generation=1)
        self.notes: list[tuple[str, str]] = []
        self.api = LinearAPI(lambda: 'synthetic', endpoint=self.remote.url)
        self.bridge = Bridge(self.store, self.api, SimpleNamespace(), profile='alpha', clock=self.clock,
                             inject=lambda key, text: self.notes.append((key, text)) or True)
        self.context = SimpleNamespace(session_key='chat', session_id='session', profile='alpha',
                                       platform='slack', run_generation=1)
        self.request_id = str(uuid.uuid4())

    def invoke(self, action='update_description', *, content=CHECKED, request_id=None, **extra):
        args = {'action': action, 'issue': 'ABC-1', 'id': request_id or self.request_id,
                'description': content, 'body': content, 'expected_description': 'Synthetic issue body', **extra}
        with patch.object(self.bridge, 'flush'):
            return json.loads(chat.handle(self.bridge, args, self.context))

    def row(self, row_id=None):
        return self.store.outbox_row(row_id or self.request_id)

    def mutations(self):
        return [q for q in self.remote.requests if q.startswith('mutation')]

    def wedge(self):
        """A lost response, then a human edit: the remote outcome is genuinely unknown."""
        self.assertTrue(self.invoke()['ok'])
        self.remote.lose_next_response = True
        self.assertEqual(self.bridge.flush(), 0)
        self.remote.issues['issue-1']['description'] = 'Human changed this after send'
        self.clock.now += 3601
        self.bridge.flush()
        self.assertTrue(self.store.issue_reconciliation_blocked('issue-1'))

    def test_canonicalised_checkbox_settles_without_alert_or_wedge(self):
        self.assertTrue(self.invoke()['ok'])
        self.assertEqual(self.bridge.flush(), 1)
        self.assertEqual(self.row()['state'], 'sent')
        self.assertEqual(self.notes, [])
        self.assertFalse(self.store.issue_reconciliation_blocked('issue-1'))
        self.assertTrue(self.invoke('add_comment', request_id=str(uuid.uuid4()))['ok'])

    def test_lost_response_then_canonicalised_readback_settles_once(self):
        self.assertTrue(self.invoke()['ok'])
        self.remote.lose_next_response = True
        self.assertEqual(self.bridge.flush(), 0)
        self.bridge.store = self.store = Store(self.path)
        self.clock.now += 3601
        self.assertEqual(self.bridge.flush(), 1)
        self.assertEqual(self.row()['state'], 'sent')
        self.assertEqual(len(self.mutations()), 1)
        self.assertEqual(self.notes, [])

    def test_uncertain_notice_names_operation_cause_and_resolution(self):
        self.wedge()
        self.assertEqual(len(self.notes), 1)
        text = self.notes[0][1]
        self.assertIn(self.request_id, text)
        self.assertIn('remote description differs', text)
        self.assertIn(f'hermes linear resolve {self.request_id}', text)
        self.assertNotIn('delivery failed', text)

    def test_held_issue_refusals_name_the_write_and_supported_resolution(self):
        self.wedge()
        replies = [self.invoke('add_comment', request_id=str(uuid.uuid4())),
                   json.loads(chat.handle(self.bridge, {'action': 'start', 'issue': 'ABC-1'}, self.context)),
                   json.loads(chat.handle(self.bridge, {'action': 'done', 'issue': 'ABC-1',
                                                         'evidence': 'https://example.test/deploy'}, self.context))]
        for reply in replies:
            with self.subTest(reply=reply['message']):
                self.assertFalse(reply['ok'])
                self.assertIn(self.request_id, reply['message'])
                self.assertIn(f'hermes linear resolve {self.request_id}', reply['message'])
                self.assertNotIn('Specialist authorization', reply['message'])
        self.assertIsNotNone(self.store.get('issue-1'), 'a refused closeout keeps ownership')

    def test_operator_resolution_releases_once_without_duplicate_effects(self):
        self.wedge()
        sent = len(self.mutations())
        args = SimpleNamespace(linear_command='resolve', row_id=self.request_id, outcome='not_applied',
                               evidence='Readback shows the human edit, not the queued body')
        self.assertEqual(ops.run(args, self.path, self.ingress, 'alpha'), 0)
        self.assertEqual(ops.run(args, self.path, self.ingress, 'alpha'), 1)
        self.assertFalse(self.store.issue_reconciliation_blocked('issue-1'))
        self.bridge.flush()
        self.assertEqual(len(self.mutations()), sent)
        self.assertEqual(self.remote.issues['issue-1']['description'], 'Human changed this after send')
        self.assertTrue(self.invoke('add_comment', request_id=str(uuid.uuid4()))['ok'])
        self.assertEqual(self.store.get('issue-1')['owner_ref'], 'chat')

if __name__ == '__main__':
    unittest.main()
