"""Retired transfer records remain read-only reconciliation fences."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from linear_fake_api import load_plugin
load_plugin()
from hermes_fleet_linear_plugin.store import Store

class LegacyTransferPreservationTests(unittest.TestCase):
    def test_prepared_transfer_is_preserved_and_cannot_be_adopted(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / 'state.db')
            store.put('issue', 'chat', 'old', run_generation=2)
            owner = store.get('issue')
            saved = {'issue_id':'issue', 'state':'prepared', 'predecessor':owner,
                     'session_key':'successor', 'successor_owner':'successor-owner'}
            with sqlite3.connect(store.path) as db:
                db.execute('INSERT INTO metadata(key,value) VALUES (?,?)',
                           ('chat-transfer:request',json.dumps(saved)))
            reopened = Store(store.path)
            self.assertEqual(reopened.chat_transfer('request'), saved)
            self.assertFalse(reopened.link_task('issue','native-task',None,3))
            self.assertFalse(reopened.put('issue','chat','successor'))
            self.assertEqual(reopened.get('issue'), owner)
            self.assertTrue(reopened.predecessor_transfer_blocks('issue',{'session_key':'old'}))
