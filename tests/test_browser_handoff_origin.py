from __future__ import annotations

import importlib.util
import os
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins" / "browser-handoff"


def load_plugin():
    spec = importlib.util.spec_from_file_location(
        "hermes_plugins.browser_handoff_public",
        PLUGIN / "__init__.py",
        submodule_search_locations=[str(PLUGIN)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class BrowserHandoffOriginTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_plugin()
        self.session = str(uuid.uuid4())
        self.invocation = SimpleNamespace(
            profile="sample",
            platform="telegram",
            user_id="1",
            chat_id="1",
            chat_type="dm",
            browser_workspace_id="default",
        )

    def test_missing_host_rejects_every_url(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HERMES_BROWSER_HANDOFF_PUBLIC_HOST", None)
            self.assertFalse(
                self.module._valid_handoff_url(
                    f"https://handoff.example/{self.invocation.profile}/{self.session}",
                    self.invocation,
                )
            )

    def test_configured_host_accepts_matching_https_path(self) -> None:
        env = {
            "HERMES_BROWSER_HANDOFF_PUBLIC_HOST": "handoff.example",
            "HERMES_BROWSER_HANDOFF_TZ": "UTC",
        }
        url = f"https://handoff.example/{self.invocation.profile}/{self.session}"
        with mock.patch.dict(os.environ, env, clear=False):
            self.assertTrue(self.module._valid_handoff_url(url, self.invocation))
            self.assertFalse(
                self.module._valid_handoff_url(
                    f"https://other.example/{self.invocation.profile}/{self.session}",
                    self.invocation,
                )
            )

    def test_start_without_public_host_does_not_mint(self) -> None:
        import json

        broker = mock.Mock()
        broker.start.return_value = {
            "ok": True,
            "url": f"https://handoff.example/{self.invocation.profile}/{self.session}",
            "session_id": self.session,
        }
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HERMES_BROWSER_HANDOFF_PUBLIC_HOST", None)
            payload = json.loads(
                self.module.start_handoff({}, invocation_context=self.invocation, broker=broker)
            )
        self.assertEqual(payload["error"], "missing_public_host")
        broker.start.assert_not_called()

    def test_start_rejects_bad_url_as_broker_invalid_url(self) -> None:
        import json

        broker = mock.Mock()
        broker.start.return_value = {
            "ok": True,
            "url": f"https://other.example/{self.invocation.profile}/{self.session}",
            "session_id": self.session,
        }
        env = {"HERMES_BROWSER_HANDOFF_PUBLIC_HOST": "handoff.example"}
        with mock.patch.dict(os.environ, env, clear=False):
            payload = json.loads(
                self.module.start_handoff({}, invocation_context=self.invocation, broker=broker)
            )
        self.assertEqual(payload["error"], "broker_invalid_url")
        broker.start.assert_called_once()

    def test_status_none_clears_hold_after_broker_reset(self) -> None:
        env = {"HERMES_BROWSER_HANDOFF_PUBLIC_HOST": "handoff.example"}
        transport = self.module.BrokerHttpTransport("http://127.0.0.1:9", "token")
        owner = {"platform": "telegram", "user_id": "1", "chat_id": "1", "chat_type": "dm"}
        with mock.patch.dict(os.environ, env, clear=False):
            transport._hold(owner, {"session_id": self.session, "ok": True})
            self.assertIsNotNone(transport.held_handoff())
            transport._settle(owner, dict(self.module.NO_ACTIVE_HANDOFF))
            self.assertIsNone(transport.held_handoff())


if __name__ == "__main__":
    unittest.main()
