"""A closing receipt proves a regular comment by the configured app, not a mirror."""
import unittest

from test_linear_specialist_authorization import Authority, ISSUE


class CompletionCommentReceiptTests(unittest.TestCase):
    def test_closing_receipt_rejects_nested_foreign_missing_and_conflicting_comment(self):
        authority = Authority()
        api = authority.api(scope=None)
        comment = dict(id='stable-comment', body='Readable result', issue={'id': ISSUE},
                       parent=None, user={'id': authority.viewer})
        authority.comment_records[comment['id']] = comment
        self.assertTrue(api.verify_completion_comment(comment['id'], ISSUE, comment['body']))
        for field, value in (('parent', {'id': 'nested-mirror'}), ('user', {'id': 'other-agent'}),
                             ('body', 'Different result'), ('issue', {'id': 'other-issue'}), ('user', None)):
            with self.subTest(field=field, value=value):
                authority.comment_records[comment['id']] = {**comment, field: value}
                self.assertFalse(api.verify_completion_comment(comment['id'], ISSUE, comment['body']))
        authority.comment_records[comment['id']] = {key: value for key, value in comment.items() if key != 'parent'}
        self.assertFalse(api.verify_completion_comment(comment['id'], ISSUE, comment['body']))

    def test_structured_result_preserves_remaining_and_does_not_repeat_existing_evidence(self):
        from hermes_fleet_linear_plugin.outcome import prepare
        url = 'https://example.test/check/blue'
        summary = ('**Delivered**\n\nChecked the requested color configuration.\n\n'
                   '**Evidence**\n\nBoth color fixtures passed: ' + url + '\n\n'
                   '**Remaining**\n\nMonitor the next scheduled run before widening the rollout.')
        writes = prepare([], issue_id=ISSUE, owner='session', completion={'task_id': 'task'},
                         summary=summary, evidence=[url, url], head_note='')
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][0], 'comment')
        body = writes[0][1]['body']
        self.assertEqual(body, 'Done.\n\n' + summary)
        for heading in ('**Delivered**', '**Evidence**', '**Remaining**'):
            self.assertEqual(body.count(heading), 1)
        self.assertEqual(body.count(url), 1)
        self.assertNotIn('None reported.', body)
