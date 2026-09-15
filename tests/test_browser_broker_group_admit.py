"""Group Slack admission follows the matched person route, not a channel list."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_policy():
    path = ROOT / "sidecars" / "browser-broker" / "session_policy.py"
    spec = importlib.util.spec_from_file_location("session_policy_group_admit", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class GroupThreadAdmitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_policy()
        principal = self.policy.AccessPrincipal(
            principal_id="person",
            access_email="user@example.test",
            routes=(("slack", "U123", "TTEAM"),),
            agents=("example",),
        )
        self.broker = self.policy.HandoffBroker(
            [principal],
            clock=lambda: 0.0,
            configured_agent="example",
        )

    def _inv(self, **overrides):
        values = dict(
            profile="example",
            platform="slack",
            user_id="U123",
            chat_id="CCHANNEL",
            thread_id="111.222",
            chat_type="group",
            scope_id="TTEAM",
        )
        values.update(overrides)
        return self.policy.Invocation(**values)

    def test_group_with_thread_is_admitted_without_channel_list(self) -> None:
        principal = self.broker._authorized_principal(self._inv())
        self.assertEqual(principal.principal_id, "person")

    def test_group_without_thread_is_denied(self) -> None:
        with self.assertRaises(self.policy.BrokerError) as raised:
            self.broker._authorized_principal(self._inv(thread_id=""))
        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(str(raised.exception), "route denied")

    def test_wrong_team_is_unknown_principal(self) -> None:
        with self.assertRaises(self.policy.BrokerError) as raised:
            self.broker._authorized_principal(self._inv(scope_id="TOTHER"))
        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(str(raised.exception), "unknown principal")

    def test_dm_still_admitted(self) -> None:
        principal = self.broker._authorized_principal(
            self._inv(chat_type="dm", chat_id="DDM", thread_id="")
        )
        self.assertEqual(principal.principal_id, "person")


if __name__ == "__main__":
    unittest.main()
