import tempfile
import threading
import unittest
from pathlib import Path

from store import Store


class ChatAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / "store.db")

    def tearDown(self):
        self.tmp.cleanup()

    def prepare(self, req="r1", issue="ISS-1", **kw):
        return self.store.prepare_chat_admission(req, issue, "p", "home", "parent-key", "parent-id", 3,
                                                 kw.get("route"), 12.0)

    def test_prepare_idempotent_and_restart_persistent(self):
        row = self.prepare()
        self.assertEqual(row["state"], "prepared")
        self.assertEqual(self.prepare(), row)
        other = type(self.store)(self.store.path)
        self.assertEqual(other.chat_admission("r1"), row)
        with self.assertRaises(ValueError):
            other.prepare_chat_admission("r1", "ISS-2", "p", "home", "parent-key", "parent-id", 3, None, 12)

    def test_refuses_occupied_scope_retirement_and_reconciliation(self):
        self.assertTrue(self.store.put("ISS-1", "kanban", "owner"))
        self.assertIsNone(self.prepare())
        self.store.delete("ISS-1")
        self.store.retire("ISS-1", "task", {})
        self.assertIsNone(self.prepare())
        self.store.clear_retirement("task", '{}')
        self.store.fence_scope("ISS-1", "denied", at=1)
        self.assertIsNone(self.prepare())

    def test_bind_then_admit_cas_and_terminal_states(self):
        row = self.prepare()
        self.assertTrue(self.store.bind_chat_admission("r1", "task", "proj", 15.0))
        self.assertEqual(self.store.get("ISS-1")["origin"], "kanban_chat")
        self.assertEqual(self.store.get("ISS-1")["owner_ref"], "parent-key")
        self.assertEqual(self.store.chat_admission("r1")["state"], "claiming")
        self.assertTrue(self.store.transition_chat_admission("r1", "claiming", "admitted"))
        self.assertFalse(self.store.transition_chat_admission("r1", "claiming", "prepared"))
        self.assertTrue(self.store.transition_chat_admission("r1", "admitted", "ambiguous", reason_code="lost"))
        self.assertFalse(self.store.transition_chat_admission("r1", "ambiguous", "prepared"))

    def test_conflicting_bind_and_stale_transition_refused(self):
        self.prepare()
        self.assertFalse(self.store.transition_chat_admission("r1", "claiming", "admitted"))
        self.assertTrue(self.store.put("ISS-1", "kanban", "other"))
        self.assertFalse(self.store.bind_chat_admission("r1", "task", None, 1))

    def test_parallel_request_and_issue_uniqueness(self):
        barrier = threading.Barrier(2)
        results = []
        def prepare(req):
            barrier.wait()
            results.append(self.store.prepare_chat_admission(req, "ISS-1", "p", "home", req, req, 1, None, 1))
        threads = [threading.Thread(target=prepare, args=(req,)) for req in ("r1", "r2")]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(sum(result is not None for result in results), 1)
        self.assertEqual(len(self.store.chat_admissions(states=["prepared"])), 1)

    def test_reconciliation_and_retirement_after_prepare_fence_binding(self):
        self.prepare()
        with self.store._tx() as db:
            db.execute("INSERT INTO outbox (id, kind, payload, next_at, attempts) "
                       "VALUES ('u', 'status', '{\"issue_id\":\"ISS-1\",\"terminal\":true,\"write_started\":true}', 0, 1)")
        self.assertIsNone(self.store.prepare_chat_admission("r2", "ISS-1", "p", "home", "k", "s", 1, None, 1))
        self.assertFalse(self.store.bind_chat_admission("r1", "task", None, 1))


if __name__ == "__main__":
    unittest.main()
