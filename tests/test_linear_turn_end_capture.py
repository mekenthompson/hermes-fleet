"""Turn-end quiet-period capture survives absence of the gateway service socket."""
import tempfile
import subprocess
import sys
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
                hooks["on_session_end"]("s", turn_id="t")
            reopened = Store(store.path)
            self.assertEqual(reopened.outbox_row(row_id)["next_at"], 260)
            # Same event replayed by a fresh registration after a later restart.
            plugin.register(ctx)
            with patch.dict("sys.modules", {"hermes_constants": SimpleNamespace(get_hermes_home=lambda: home)}), \
                 patch.object(transport, "request", side_effect=transport.Unavailable("stopped")), \
                 patch("time.time", return_value=800):
                hooks["on_session_end"]("s", turn_id="t")
            self.assertEqual(reopened.outbox_row(row_id)["next_at"], 260)
            with patch.dict("sys.modules", {"hermes_constants": SimpleNamespace(get_hermes_home=lambda: home)}), \
                 patch.object(transport, "request", side_effect=transport.Unavailable("stopped")), \
                 patch("time.time", return_value=900):
                hooks["on_session_end"]("s", turn_id="new-turn")
            self.assertEqual(reopened.outbox_row(row_id)["next_at"], 960)
            reopened.delay_session_updates("s", 260)
            self.assertEqual(reopened.outbox_row(row_id)["next_at"], 960)
            self.assertEqual(reopened.outbox_row(row_id)["payload"]["lines"], {"ABC-1": "work"})

    def test_abrupt_exit_after_hook_keeps_capture_and_replay_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            store = Store(home / "linear" / "state.db")
            row_id = store.enqueue("project_update", {"issue_id": "update:s:p", "session_id": "s",
                "lines": {"ABC-1": "work"}}, at=100)
            script = '''
import os, sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from linear_fake_api import load_plugin
plugin = load_plugin()
home = Path(sys.argv[1])
hooks = {}
ctx = SimpleNamespace(get_config=lambda k, d=None: {"enabled": True, "quiet_minutes": 1}.get(k, d),
    register_tool=lambda **kw: None, register_profile_service=lambda *a: None,
    register_hook=lambda n, f: hooks.update({n:f}))
plugin.register(ctx)
with patch.dict("sys.modules", {"hermes_constants": SimpleNamespace(get_hermes_home=lambda: home)}), patch("time.time", return_value=200):
    hooks["on_session_end"]("s", turn_id="crash-turn")
os._exit(0)
'''
            result = subprocess.run([sys.executable, "-c", script, str(home)], capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            reopened = Store(store.path)
            self.assertEqual(reopened.outbox_row(row_id)["next_at"], 260)
            reopened.delay_session_updates("s", 860, turn_id="crash-turn")
            self.assertEqual(reopened.outbox_row(row_id)["next_at"], 260)

    def test_unidentified_shutdown_does_not_extend_turn_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            store = Store(home / "linear" / "state.db")
            row_id = store.enqueue("project_update", {"issue_id": "update:s:p", "session_id": "s"}, at=100)
            hooks = {}
            ctx = SimpleNamespace(get_config=lambda k, d=None: {"enabled": True}.get(k, d),
                register_tool=lambda **kw: None, register_profile_service=lambda *a: None,
                register_hook=lambda n, f: hooks.update({n:f}))
            plugin.register(ctx)
            with patch.dict("sys.modules", {"hermes_constants": SimpleNamespace(get_hermes_home=lambda: home)}):
                hooks["on_session_end"]("s", completed=False, interrupted=True)
            self.assertEqual(store.outbox_row(row_id)["next_at"], 100)

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
                hooks["on_session_end"]("s", turn_id="t")
            self.assertFalse((home / "linear").exists())
