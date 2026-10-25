"""Reporting bindings are atomic; they never replace retained execution records."""
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from linear_fake_api import load_plugin
load_plugin()
from hermes_fleet_linear_plugin.store import Store

class NativeTaskBindingTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.store=Store(Path(tmp.name)/'state.db')

    def test_simultaneous_links_bind_only_one_task(self):
        barrier=threading.Barrier(2)
        def link(task):
            barrier.wait(timeout=5)
            return self.store.link_task('issue',task,None,1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(link,['task-a','task-b']))
        self.assertEqual(sum(results),1)
        self.assertIn(self.store.get('issue')['task_id'],('task-a','task-b'))

    def test_existing_chat_and_stopped_records_are_preserved(self):
        self.store.put('issue','chat','old',run_generation=2)
        self.store.update('issue',stop_requested_at=3)
        before=self.store.get('issue')
        self.assertFalse(self.store.link_task('issue','task',None,4))
        self.assertEqual(self.store.get('issue'),before)

    def test_uncertain_remote_write_fences_new_binding(self):
        with sqlite3.connect(self.store.path) as db:
            db.execute("INSERT INTO outbox VALUES ('unknown','status','{\"issue_id\":\"issue\",\"terminal\":true,\"write_started\":true}',1,0,'pending')")
        self.assertFalse(self.store.link_task('issue','task',None,4))
        self.assertIsNone(self.store.get('issue'))

    def test_same_native_task_is_not_linked_to_competing_outcomes(self):
        self.assertTrue(self.store.link_task('first','task',None,1))
        self.assertFalse(self.store.link_task('second','task',None,1))
