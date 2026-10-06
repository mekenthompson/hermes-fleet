"""Chat claims preserve remote event ordering, not local activity time."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from linear_fake_api import load_plugin
load_plugin()
from hermes_fleet_linear_plugin.chat import _start
from hermes_fleet_linear_plugin.bridge import iso_ms
from hermes_fleet_linear_plugin.store import Store


class ChatClaimWatermarkTests(unittest.TestCase):
    def test_new_claim_uses_remote_updated_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "state.db")
            bridge = SimpleNamespace(store=store, status=lambda *a, **k: True,
                                     project_update=lambda *a, **k: True, clock=lambda: 9999999999)
            issue = {"id": "i", "identifier": "ABC-1", "updatedAt": "2026-01-01T00:00:00Z"}
            self.assertTrue(json.loads(_start(bridge, issue, None, "self", "s", "s", 1))["ok"])
            self.assertEqual(store.get("i")["last_updated_at"], iso_ms(issue["updatedAt"]))

    def test_missing_remote_watermark_refuses_new_claim_before_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "state.db")
            effects = []
            bridge = SimpleNamespace(store=store, status=lambda *a, **k: effects.append(a),
                                     project_update=lambda *a, **k: effects.append(a))
            issue = {"id": "i", "identifier": "ABC-1"}
            self.assertFalse(json.loads(_start(bridge, issue, None, "self", "s", "s", 1))["ok"])
            self.assertIsNone(store.get("i"))
            self.assertEqual(effects, [])


if __name__ == "__main__":
    unittest.main()
