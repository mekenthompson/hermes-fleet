from __future__ import annotations

import importlib.util
import json
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins" / "browser-handoff"
SIDECAR = ROOT / "sidecars" / "browser-broker"


def load_plugin():
    spec = importlib.util.spec_from_file_location(
        "hermes_plugins.browser_handoff_chat_observe",
        PLUGIN / "__init__.py",
        submodule_search_locations=[str(PLUGIN)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_session_policy():
    if str(SIDECAR) not in sys.path:
        sys.path.insert(0, str(SIDECAR))
    import session_policy as policy

    return policy


class FakeBroker:
    def __init__(self) -> None:
        self.ended = 0
        self.observed = 0
        self._status: dict = {
            "ok": True,
            "state": "active",
            "session_id": str(uuid.uuid4()),
            "mode": "takeover",
            "automation_blocked": True,
        }

    def status(self, invocation):  # noqa: ARG002
        return dict(self._status)

    def end(self, invocation):  # noqa: ARG002
        self.ended += 1
        self._status = {
            "ok": True,
            "state": "ended",
            "session_id": self._status["session_id"],
        }
        return dict(self._status)

    def observe(self, invocation):  # noqa: ARG002
        self.observed += 1
        self._status = {
            "ok": True,
            "state": "active",
            "session_id": self._status["session_id"],
            "mode": "observe",
            "automation_blocked": False,
        }
        return dict(self._status)


class ChatReplyObservePluginTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_plugin()
        self.invocation = SimpleNamespace(
            profile="sample",
            platform="telegram",
            user_id="1",
            chat_id="1",
            chat_type="dm",
            thread_id="",
            scope_id="",
            browser_workspace_id="default",
        )

    def test_status_after_chat_reply_observes_and_does_not_end(self) -> None:
        broker = FakeBroker()
        payload = json.loads(
            self.module.status_handoff(invocation_context=self.invocation, broker=broker)
        )
        self.assertEqual(broker.observed, 1)
        self.assertEqual(broker.ended, 0)
        self.assertEqual(payload["state"], "active")
        self.assertEqual(payload["mode"], "observe")
        self.assertFalse(payload["automation_blocked"])

    def test_status_does_not_fall_back_to_end_when_observe_is_unavailable(self) -> None:
        class EndOnly:
            def __init__(self) -> None:
                self.ended = 0
                self._status = {
                    "ok": True,
                    "state": "active",
                    "session_id": "sess-1",
                    "mode": "takeover",
                    "automation_blocked": True,
                }

            def status(self, invocation):
                return dict(self._status)

            def end(self, invocation):
                self.ended += 1
                self._status = {"ok": True, "state": "ended", "session_id": "sess-1"}
                return dict(self._status)

        broker = EndOnly()
        payload = json.loads(
            self.module.status_handoff(invocation_context=self.invocation, broker=broker)
        )
        self.assertEqual(broker.ended, 0)
        self.assertEqual(payload.get("ok"), False)
        self.assertEqual(payload.get("error"), "observe_unavailable")
        self.assertEqual(payload.get("state"), "active")

    def test_already_observing_status_is_idempotent(self) -> None:
        broker = FakeBroker()
        broker._status = {
            "ok": True,
            "state": "active",
            "session_id": broker._status["session_id"],
            "mode": "observe",
            "automation_blocked": False,
        }
        payload = json.loads(
            self.module.status_handoff(invocation_context=self.invocation, broker=broker)
        )
        self.assertEqual(broker.observed, 0)
        self.assertEqual(broker.ended, 0)
        self.assertEqual(payload["mode"], "observe")

    def test_hold_settles_when_mode_is_observe(self) -> None:
        transport = self.module.BrokerHttpTransport("http://127.0.0.1:9", "token")
        transport._hold(self.invocation, {"ok": True, "session_id": "sid-1"})
        self.assertIsNotNone(transport.held_handoff())
        transport._settle(
            self.invocation,
            {"ok": True, "state": "active", "mode": "observe", "automation_blocked": False},
        )
        self.assertIsNone(transport.held_handoff())

    def test_inspect_only_status_does_not_observe(self) -> None:
        transport = self.module.BrokerHttpTransport("http://127.0.0.1:9", "token")
        with mock.patch.object(transport, "_post", return_value={"ok": True, "state": "active", "mode": "takeover"}) as posted:
            transport.status(self.invocation)
        posted.assert_called_once()
        self.assertEqual(posted.call_args.args[0], "/status")

    def test_next_step_does_not_tell_the_model_to_end_after_a_reply(self) -> None:
        self.assertNotIn("If the handoff is still held after that check, call browser_handoff_end", self.module.NEXT_STEP)
        self.assertIn("Do not call browser_handoff_end unless they asked to close the session", self.module.NEXT_STEP)
        self.assertIn("observe", self.module.NEXT_STEP)

    def test_human_copy_says_the_tab_stays_open_to_watch(self) -> None:
        text = self.module.handoff_message("https://handoff.example/sample/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        self.assertIn("watch", text.lower())
        self.assertNotIn("then reply here so", text)


class AgentObserveCurrentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_session_policy()
        self.env = {
            "PUBLIC_ORIGIN": "https://handoff.example",
            "PUBLIC_BASE": "https://handoff.example",
        }
        principal = self.policy.AccessPrincipal(
            principal_id="p1",
            access_email="user@example.com",
            routes=(("telegram", "1", None),),
            agents=("sample",),
            browser_workspaces=("default",),
        )
        self.invocation = self.policy.Invocation(
            profile="sample",
            platform="telegram",
            user_id="1",
            chat_id="1",
            thread_id="",
            chat_type="dm",
        )
        self.observed = []

        def on_observe(agent, workspace, session_id):
            self.observed.append((agent, workspace, session_id))
            return "checkpointed"

        with mock.patch.dict("os.environ", self.env, clear=False):
            self.broker = self.policy.HandoffBroker(
                [principal],
                clock=lambda: 1_000.0,
                ttl_seconds=900,
                configured_agent="sample",
                on_observe=on_observe,
            )

    def test_observe_current_keeps_session_active_and_unblocks_automation(self) -> None:
        with mock.patch.dict("os.environ", self.env, clear=False):
            minted = self.broker.mint(self.invocation)
            status = self.broker.observe_current(self.invocation)
        self.assertEqual(status["session_id"], minted.session_id)
        self.assertNotIn(status["state"], {"ended", "expired", "revoked", "none"})
        self.assertEqual(status["mode"], "observe")
        self.assertFalse(status["automation_blocked"])
        self.assertEqual(self.observed, [("sample", "default", minted.session_id)])
        with mock.patch.dict("os.environ", self.env, clear=False):
            still = self.broker.status(self.invocation)
        self.assertNotIn(still["state"], {"ended", "expired", "revoked", "none"})
        self.assertEqual(still["mode"], "observe")

    def test_observe_current_does_not_end_the_session(self) -> None:
        with mock.patch.dict("os.environ", self.env, clear=False):
            self.broker.mint(self.invocation)
            self.broker.observe_current(self.invocation)
            self.assertNotEqual(self.broker.status(self.invocation)["state"], "ended")


class ViewerForcedObserveCopyTests(unittest.TestCase):
    def test_viewer_polls_server_mode(self) -> None:
        sys.path.insert(0, str(SIDECAR))
        import ux

        self.assertIn('"/mode"', ux.VIEWER_JS)
        self.assertIn("applyServerMode", ux.VIEWER_JS)
        self.assertIn("watch", ux.viewer_page("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", agent_id="sample").lower())

    def test_default_workspace_mode_route_uses_bare_action_name(self) -> None:
        """UUID_RE nests the slash; group(4) is 'mode', not '/mode'.

        GET /<agent>/<uuid>/mode must match that existing default-resource ABI
        or the viewer poll proxies into Camofox instead of forcing Observe.
        """
        import re

        source = (SIDECAR / "origin.py").read_text(encoding="utf-8")
        uuid_re = re.search(r"^UUID_RE = re\.compile\(\s*r\"([^\"]+)\"", source, re.M)
        workspace_re = re.search(r"^WORKSPACE_UUID_RE = re\.compile\(\s*r\"([^\"]+)\"", source, re.M)
        self.assertIsNotNone(uuid_re)
        self.assertIsNotNone(workspace_re)
        assert uuid_re is not None and workspace_re is not None
        uuid_pat = re.compile(uuid_re.group(1))
        workspace_pat = re.compile(workspace_re.group(1))
        sid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        match = uuid_pat.match(f"/sample/{sid}/mode")
        self.assertIsNotNone(match)
        self.assertEqual(match.group(3), "/mode")
        self.assertEqual(match.group(4), "mode")
        workspace = workspace_pat.match(f"/sample/default/{sid}/mode")
        self.assertIsNotNone(workspace)
        self.assertEqual(workspace.group(4), "/mode")
        self.assertEqual(workspace.group(5), "mode")


if __name__ == "__main__":
    unittest.main()
