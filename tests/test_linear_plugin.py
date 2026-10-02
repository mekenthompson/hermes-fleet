"""Unit checks for plugins/linear that need no Agent source (these run in public CI).

The end-to-end rules are proven in tests/test_linear_kanban_scenarios.py against a real Kanban board.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
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
# Durable chat Stop adds a profile/generation-fenced receipt path to this plugin.
# Keep a bounded production surface without compressing safety-critical branches.
# Actor/workspace binding and mutation-refusal regressions require explicit safety branches.
# Durable recovery and exact credential binding retain explicit, readable safety branches.
BUDGET = 2700


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


class LinearOutboxOrderTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = Store(Path(tmp.name) / "state.db")
        self.store.put("issue-1", "chat", "chat-key")
        self.store.finish("issue-1", [("status", {"issue_id": "issue-1", "state": "done"}),
                                      ("comment", {"issue_id": "issue-1", "body": "evidence"})], at=100)
        self.status = next(row for row in self.store.pending() if row["kind"] == "status")

    def test_retryable_failed_status_holds_evidence_until_recovery_or_supersession(self) -> None:
        self.assertTrue(self.store.retry(self.status, 100 + 86_401))
        self.assertEqual(self.store.due(100 + 86_401), [])
        newer = self.store.enqueue("status", {"issue_id": "issue-1", "state": "in_progress"}, at=100 + 86_402)
        self.assertEqual([row["id"] for row in self.store.due(100 + 86_402)], [newer])
        self.store.mark_sent(newer, True)
        self.assertEqual([row["kind"] for row in self.store.due(100 + 86_402)], ["comment"])
        self.assertFalse(self.store.terminal_status_applied(self.status["id"]))
        self.assertEqual(self.store.revive_failed(100 + 86_402), 0)

    def test_nonretryable_failed_status_does_not_block_later_issue_writes(self) -> None:
        self.store.mark(self.status["id"], "failed")
        [dependent] = self.store.due(100)
        self.assertEqual(dependent["kind"], "comment")
        self.store.mark_sent(dependent["id"], False)
        newer = self.store.enqueue("status", {"issue_id": "issue-1", "state": "in_progress"}, at=101)
        self.assertEqual([row["id"] for row in self.store.due(101)], [newer])

    def test_legacy_attempted_terminal_without_send_marker_requires_explicit_resolution(self) -> None:
        self.store.rewrite(self.status["id"], {**self.status["payload"], "terminal": True}, 100)
        attempted = self.store.outbox_row(self.status["id"])
        self.store.retry(attempted, 100)
        self.assertTrue(self.store.issue_reconciliation_blocked("issue-1"))
        self.assertFalse(self.store.put("issue-1", "chat", "fresh"))
        self.assertEqual(self.store.enqueue("status", {"issue_id": "issue-1", "state": "in_progress"}, at=101), "")
        with self.assertRaises(ValueError):
            self.store.reconcile_terminal(self.status["id"], outcome="applied", evidence="", at=102)
        self.assertTrue(self.store.reconcile_terminal(
            self.status["id"], outcome="not_applied", evidence="https://docs.example/verified", at=102))
        self.assertFalse(self.store.issue_reconciliation_blocked("issue-1"))
        self.assertTrue(self.store.put("issue-1", "chat", "fresh"))
        receipt = self.store.outbox_row(self.status["id"])
        self.assertEqual(receipt["state"], "failed")
        self.assertEqual(receipt["attempts"], 1)
        self.assertEqual(receipt["payload"]["reconciliation"]["outcome"], "not_applied")


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

    def test_malformed_viewer_identity_is_a_linear_error(self) -> None:
        client = linear_api.LinearAPI(
            lambda: "t",
            transport=lambda *args: (200, {}, json.dumps({"data": {"viewer": {}}}).encode()),
        )
        with self.assertRaises(linear_api.LinearError):
            client.viewer_id()
        self.assertIsNone(client._viewer)

    def test_named_mutations_require_literal_success_true(self) -> None:
        mutations = (
            ("issueUpdate", lambda api: api.update_issue("issue-1", {"stateId": "done"})),
            ("commentCreate", lambda api: api.create_comment("client-1", "issue-1", "body")),
            ("agentActivityCreate", lambda api: api.create_activity("client-1", "session-1", {"type": "response"})),
            ("projectUpdateCreate", lambda api: api.create_project_update("client-1", "project-1", "body")),
        )
        malformed = (False, None, 1, "true", {}, "missing")
        for mutation, invoke in mutations:
            for success in malformed:
                with self.subTest(mutation=mutation, success=success):
                    def transport(url, body, headers):
                        query = json.loads(body)["query"]
                        self.assertIn(mutation, query)
                        field = {} if success == "missing" else {"success": success}
                        return 200, {}, json.dumps({"data": {mutation: field}}).encode()

                    api = linear_api.LinearAPI(lambda: "t", transport=transport)
                    with self.assertRaises(linear_api.LinearError) as caught:
                        invoke(api)
                    if success is False:
                        self.assertFalse(caught.exception.retryable)

    def test_named_mutations_accept_literal_success_true(self) -> None:
        mutations = (
            ("issueUpdate", lambda api: api.update_issue("issue-1", {"stateId": "done"})),
            ("commentCreate", lambda api: api.create_comment("client-1", "issue-1", "body")),
            ("agentActivityCreate", lambda api: api.create_activity("client-1", "session-1", {"type": "response"})),
            ("projectUpdateCreate", lambda api: api.create_project_update("client-1", "project-1", "body")),
        )
        for mutation, invoke in mutations:
            with self.subTest(mutation=mutation):
                def transport(url, body, headers):
                    query = json.loads(body)["query"]
                    self.assertIn(mutation, query)
                    return 200, {}, json.dumps({"data": {mutation: {"success": True}}}).encode()

                invoke(linear_api.LinearAPI(lambda: "t", transport=transport))

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

    def test_terminal_outbox_capture_and_work_removal_are_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "state.db")
            store.put("i", "kanban", "session", task_id="task")
            with self.assertRaises(TypeError):
                store.finish("i", [("status", {"issue_id": "i", "state": "done"}),
                                   ("activity", {"issue_id": "i", "body": object()})], at=1000)
            self.assertIsNotNone(store.get("i"))
            self.assertEqual(store.pending(), [])
            self.assertTrue(store.finish("i", [("status", {"issue_id": "i", "state": "done"})], at=1000))
            self.assertIsNone(store.get("i"))
            self.assertEqual(len(store.pending()), 1)

    def test_existing_work_table_adds_chat_panel_and_stop_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.db"
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE work (issue_id TEXT PRIMARY KEY, origin TEXT NOT NULL, "
                           "owner_ref TEXT NOT NULL, task_id TEXT, project_id TEXT, "
                           "last_updated_at REAL NOT NULL DEFAULT 0)")
                db.execute("INSERT INTO work VALUES ('i', 'chat', 'chat-key', NULL, NULL, 0)")
            store = Store(path)
            self.assertEqual(store.get("i")["stop_requested_at"], 0)
            store.update("i", panel_note="Need input", stop_requested_at=123)
            self.assertEqual(Store(path).get("i")["panel_note"], "Need input")

    def test_legacy_chat_generation_stays_null_and_kanban_cannot_be_targeted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.db"
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE work (issue_id TEXT PRIMARY KEY, origin TEXT NOT NULL, "
                           "owner_ref TEXT NOT NULL, task_id TEXT, project_id TEXT, "
                           "last_updated_at REAL NOT NULL DEFAULT 0)")
                db.execute("INSERT INTO work VALUES ('old', 'chat', 'chat-key', NULL, NULL, 0)")
            store = Store(path)
            self.assertIsNone(store.get("old")["run_generation"])
            self.assertIsNone(store.capture_chat_stop("old", "linear-s", "activity-1", "alpha", at=100))
            store.put("board", "kanban", "linear-s", task_id="task")
            self.assertIsNone(store.capture_chat_stop("board", "linear-s", "activity-2", "alpha", at=100))

    def test_stop_intent_is_durable_idempotent_and_keeps_original_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.db"
            store = Store(path)
            store.put("i", "chat", "chat-key", run_generation=7)
            store.update("i", linear_session_id="linear-s")
            first = store.capture_chat_stop("i", "linear-s", "activity-1", "alpha", at=100)
            self.assertEqual(first["run_generation"], 7)
            self.assertEqual(first["session_key"], "chat-key")
            store.update("i", run_generation=8)
            self.assertEqual(store.capture_chat_stop("i", "linear-s", "activity-1", "alpha", at=101)["id"], first["id"])
            self.assertEqual(Store(path).stop_intents()[0]["run_generation"], 7)
            self.assertEqual(len(store.stop_intents()), 1)

    def test_stop_guard_rolls_back_if_intent_insert_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.db"
            store = Store(path)
            store.put("i", "chat", "chat-key", run_generation=7)
            store.update("i", linear_session_id="linear-s")
            with sqlite3.connect(path) as db:
                db.execute("CREATE TRIGGER fail_stop BEFORE INSERT ON chat_stop BEGIN "
                           "SELECT RAISE(ABORT, 'synthetic crash'); END")
            with self.assertRaises(sqlite3.IntegrityError):
                store.capture_chat_stop("i", "linear-s", "activity-1", "alpha", at=100, stamp=100_000)
            self.assertEqual(store.get("i")["stop_requested_at"], 0)
            self.assertEqual(store.stop_intents(), [])
            self.assertEqual(store.pending(), [])



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
