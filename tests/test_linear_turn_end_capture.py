"""Turn-end quiet-period capture survives absence of the gateway service socket."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from linear_fake_api import load_plugin
plugin = load_plugin()
from hermes_fleet_linear_plugin import transport
from hermes_fleet_linear_plugin.store import Store


class TurnEndCaptureTests(unittest.TestCase):
    def test_transport_down_captures_original_deadline_durably(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            store = Store(home / "linear" / "state.db")
            row_id = store.enqueue("project_update", {"issue_id": "update:s:p", "session_id": "s",
                "project_id": "p", "lines": {"ABC-1": "work"}}, at=100)
            hooks = {}
            ctx = SimpleNamespace(get_config=lambda k, d=None: {"enabled": True, "quiet_minutes": 1}.get(k, d),
                register_tool=lambda **kw: None, register_profile_service=lambda *a: None,
                register_hook=lambda name, fn: hooks.update({name: fn}))
            plugin.register(ctx)
            with patch.dict("sys.modules", {"hermes_constants": SimpleNamespace(get_hermes_home=lambda: home)}), \
                 patch.object(transport, "request", side_effect=transport.Unavailable("stopped")), \
                 patch("time.time", return_value=200):
                hooks["on_session_end"]("s")
            reopened = Store(store.path)
            self.assertEqual(reopened.outbox_row(row_id)["next_at"], 260)
            reopened.delay_session_updates("s", 260)
            self.assertEqual(reopened.outbox_row(row_id)["next_at"], 260)
            self.assertEqual(reopened.outbox_row(row_id)["payload"]["lines"], {"ABC-1": "work"})

    def test_capture_does_not_create_state_for_untracked_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            hooks = {}
            ctx = SimpleNamespace(get_config=lambda k, d=None: {"enabled": True}.get(k, d),
                register_tool=lambda **kw: None, register_profile_service=lambda *a: None,
                register_hook=lambda name, fn: hooks.update({name: fn}))
            plugin.register(ctx)
            with patch.dict("sys.modules", {"hermes_constants": SimpleNamespace(get_hermes_home=lambda: home)}), \
                 patch.object(transport, "request", side_effect=transport.Unavailable("stopped")):
                hooks["on_session_end"]("s")
            self.assertFalse((home / "linear").exists())
