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
