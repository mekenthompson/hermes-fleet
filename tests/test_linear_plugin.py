"""Unit checks for plugins/linear that need no Agent source (these run in public CI).

The end-to-end rules are proven in tests/test_linear_kanban_scenarios.py against a real Kanban board.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import tempfile
import time
import unittest
from pathlib import Path

from linear_fake_api import ROOT, Clock, load_plugin

plugin = load_plugin()
from hermes_fleet_linear_plugin import api as linear_api  # noqa: E402
from hermes_fleet_linear_plugin.store import Store  # noqa: E402

PLUGIN = ROOT / "plugins" / "linear"
BUDGET = 1800


class FakeContext:
    def __init__(self, settings: dict) -> None:
        self.settings, self.tools, self.hooks, self.services = settings, [], [], []

    def get_config(self, key, default=None):
        return self.settings.get(key, default)

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)

    def register_hook(self, name, callback):
        self.hooks.append(name)

    def register_profile_service(self, name, factory):
        self.services.append(name)


class LinearPluginUnitTests(unittest.TestCase):
    def test_off_by_default_and_one_block_enables_it(self) -> None:
        idle = FakeContext({})
        plugin.register(idle)
        self.assertEqual((idle.tools, idle.hooks, idle.services), ([], [], []))
        on = FakeContext({"enabled": True})
        plugin.register(on)
        self.assertEqual([t["name"] for t in on.tools], ["linear"])
        self.assertTrue(on.tools[0]["inject_invocation_context"])
        self.assertEqual((on.hooks, on.services), (["on_session_end"], ["linear"]))
        contract = json.loads((ROOT / "contracts" / "plugins.json").read_text(encoding="utf-8"))
        self.assertFalse(next(c for c in contract["components"] if c["id"] == "linear")["default_enabled"])

    def test_production_size_budget(self) -> None:
        lines = sum(len(p.read_text(encoding="utf-8").splitlines())
                    for p in PLUGIN.iterdir() if p.suffix in {".py", ".yaml"})
        self.assertLessEqual(lines, BUDGET)
        self.assertEqual(sorted(p.name for p in PLUGIN.iterdir() if p.is_file()),
                         ["README.md", "__init__.py", "api.py", "bridge.py", "chat.py", "oauth.py", "plugin.yaml",
                          "store.py"])

    def test_duplicate_create_matches_only_the_live_conflict_shape(self) -> None:
        cid = "0b8f5a7e-3c1d-4e2f-9a6b-7c8d9e0f1a2b"
        conflict = [{"message": "conflict on insert of Comment", "extensions": {
            "code": "INPUT_ERROR", "userPresentableMessage": f"Entity Comment with id {cid} already exists."}}]
        self.assertTrue(linear_api.is_duplicate_create_error(conflict, cid))
        self.assertFalse(linear_api.is_duplicate_create_error(conflict, "another-id"))
        invalid = [{"message": "Argument Validation Error", "extensions": {"code": "INVALID_INPUT"}}]
        self.assertFalse(linear_api.is_duplicate_create_error(invalid, cid))
        self.assertFalse(linear_api.is_duplicate_create_error(None, cid))

    def test_webhook_signature_and_freshness(self) -> None:
        secret, now = b"synthetic-secret", time.time() * 1000
        body = json.dumps({"type": "AgentSessionEvent", "webhookTimestamp": now}).encode()
        good = hmac.new(secret, body, hashlib.sha256).hexdigest()
        self.assertTrue(linear_api.verify_webhook(secret, body, good, now))
        self.assertFalse(linear_api.verify_webhook(b"wrong", body, good, now))
        self.assertFalse(linear_api.verify_webhook(secret, body, good, now + 61_000))
        self.assertFalse(linear_api.verify_webhook(secret, body, "not-hex", now))

    def test_rate_limit_pauses_until_the_reset_header(self) -> None:
        clock, calls = Clock(), []

        def transport(url, body, headers):
            calls.append(json.loads(body))
            reset = str(int((clock() + 90) * 1000))
            return 400, {"X-RateLimit-Requests-Reset": reset}, json.dumps(
                {"errors": [{"message": "limited", "extensions": {"code": "RATELIMITED"}}]}).encode()

        client = linear_api.LinearAPI(lambda: "t", transport=transport, clock=clock)
        with self.assertRaises(linear_api.RateLimited) as caught:
            client.viewer_id()
        self.assertAlmostEqual(caught.exception.until, clock() + 90, delta=1)
        with self.assertRaises(linear_api.RateLimited):
            client.viewer_id()
        self.assertEqual(len(calls), 1)

    def test_unauthorized_refreshes_the_token_once(self) -> None:
        class Token:
            refreshed = 0

            def __call__(self):
                return "fresh" if self.refreshed else "stale"

            def invalidate(self):
                self.refreshed += 1

        def transport(url, body, headers):
            if headers["Authorization"] == "Bearer stale":
                return 401, {}, b"{}"
            return 200, {}, json.dumps({"data": {"viewer": {"id": "app-1"}}}).encode()

        token = Token()
        self.assertEqual(linear_api.LinearAPI(token, transport=transport).viewer_id(), "app-1")
        self.assertEqual(token.refreshed, 1)

    def test_outbox_backoff_give_up_and_revival(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store, now = Store(Path(tmp) / "state.db"), 1_000_000.0
            store.enqueue("status", {"issue_id": "i", "state": "in_progress"}, at=now)
            delays = []
            for _ in range(8):
                [row] = store.due(1e12)
                store.retry(row, now)
                delays.append(store.pending()[0]["next_at"] - now)
            self.assertEqual(delays, [60, 120, 240, 480, 960, 1920, 3600, 3600])
            [row] = store.due(1e12)
            self.assertTrue(store.retry(row, now + 86_400))
            self.assertEqual(store.pending(), [])
            store.enqueue("status", {"issue_id": "i", "state": "done"}, at=now)
            store.enqueue("comment", {"issue_id": "i", "body": "x"}, at=now)
            self.assertEqual([r["kind"] for r in store.due(now)], ["status"])  # one issue, in order
            store.revive_failed(now)  # the older status write is superseded: it must not land late
            self.assertEqual([r["payload"]["state"] for r in store.pending() if r["kind"] == "status"], ["done"])


if __name__ == "__main__":
    unittest.main()
