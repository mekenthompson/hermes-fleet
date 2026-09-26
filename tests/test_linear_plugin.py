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
from hermes_fleet_linear_plugin import oauth  # noqa: E402
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



class FakeConnect:
    """A 1Password Connect item: whole-field PATCH semantics, as the live service needs."""

    def __init__(self) -> None:
        self.fields = [{"id": "f1", "label": "client_id", "value": "cid"},
                       {"id": "f2", "label": "client_secret", "value": "secret"},
                       {"id": "f3", "label": "refresh_token", "value": "r0"}]
        self.fail_patch = False
        self.patches = 0

    def __call__(self, method, url, headers, body):
        if method == "PATCH":
            if self.fail_patch:
                raise OSError("Connect down")
            self.patches += 1
            [op] = json.loads(body)
            index = next(i for i, f in enumerate(self.fields) if op["path"] == f"/fields/{f['id']}")
            self.fields[index] = op["value"]
        return {"id": "item-x", "vault": {"id": "vault-x"}, "fields": [dict(f) for f in self.fields]}


class ConnectOAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.cache = Path(tmp.name) / "secrets" / "linear-oauth.json"
        self.connect, self.clock, self.posts = FakeConnect(), Clock(), []
        item = oauth.ConnectItem("https://connect.example", "t", "vault-x", "item-x", transport=self.connect)
        self.provider = oauth.ConnectOAuth(item, self.cache, clock=self.clock, post=self.post)

    def post(self, form):
        self.posts.append(form["refresh_token"])
        n = len(self.posts)
        return {"access_token": f"a{n}", "refresh_token": f"r{n}", "expires_in": 3600}

    def refresh_token(self) -> str:
        return next(f["value"] for f in self.connect.fields if f["label"] == "refresh_token")

    def test_refresh_rotates_into_connect_and_caches_the_access_token(self) -> None:
        self.assertEqual(self.provider(), "a1")
        self.assertEqual(self.posts, ["r0"])
        self.assertEqual(self.refresh_token(), "r1")  # rotation stored, whole-field PATCH
        self.assertEqual(self.provider(), "a1")  # cached until near expiry
        self.clock.now += 3600
        self.assertEqual(self.provider(), "a2")
        self.assertEqual(self.posts, ["r0", "r1"])
        self.assertEqual(oct(self.cache.stat().st_mode & 0o777), "0o600")

    def test_connect_outage_keeps_the_rotated_token_and_retries(self) -> None:
        self.connect.fail_patch = True
        self.assertEqual(self.provider(), "a1")
        self.assertEqual(self.refresh_token(), "r0")
        self.clock.now += 3600  # expired: the next refresh must use the rotated token, not the stale one
        self.assertEqual(self.provider(), "a2")
        self.assertEqual(self.posts, ["r0", "r1"])
        self.connect.fail_patch = False
        self.clock.now += 400
        self.provider()
        self.assertEqual(self.refresh_token(), "r2")

    def test_deleted_cache_rebuilds_from_connect(self) -> None:
        self.provider()
        self.cache.unlink()
        self.assertEqual(self.provider(), "a2")
        self.assertEqual(self.posts, ["r0", "r1"])

    def test_rejected_refresh_token_asks_for_reauthorization(self) -> None:
        import io
        import urllib.error
        from unittest import mock

        error = urllib.error.HTTPError(oauth.TOKEN_ENDPOINT, 400, "bad", {}, io.BytesIO(b"{}"))
        with mock.patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaises(oauth.ReauthorizationRequired):
                oauth.ConnectOAuth._post_refresh({"grant_type": "refresh_token"})

    def test_token_failure_is_a_retryable_linear_error(self) -> None:
        def broken():
            raise oauth.ReauthorizationRequired("reauthorize")

        with self.assertRaises(linear_api.LinearError) as caught:
            linear_api.LinearAPI(broken, transport=lambda *a: (200, {}, b"{}")).viewer_id()
        self.assertTrue(caught.exception.retryable)

if __name__ == "__main__":
    unittest.main()
