"""Rate-limit admission uses Linear's wire headers and survives a service restart."""
from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from email.utils import formatdate
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from linear_fake_api import Clock, load_plugin

plugin = load_plugin()
from hermes_fleet_linear_plugin.api import LinearAPI, LinearError, RateLimited  # noqa: E402

LIMIT = json.dumps({"errors": [{"extensions": {"code": "RATELIMITED"}}]}).encode()
OK = b'{"data":{"viewer":{"id":"app-a"}}}'


class RateLimitTests(unittest.TestCase):
    def test_exhausted_budget_wins_over_unexhausted_reset(self):
        for budget in ("Requests", "Complexity", "Endpoint-Requests"):
            with self.subTest(budget=budget):
                clock = Clock()
                headers = {"X-RateLimit-Requests-Remaining": "100", "X-RateLimit-Requests-Reset": str((clock() + 3600) * 1000),
                           f"X-RateLimit-{budget}-Remaining": "0", f"X-RateLimit-{budget}-Reset": str((clock() + 90) * 1000)}
                api = LinearAPI(lambda: "t", transport=lambda *a: (400, headers, LIMIT), clock=clock)
                with self.assertRaises(RateLimited) as caught: api.graphql("query { viewer { id } }")
                self.assertEqual(caught.exception.until, clock() + 90)

    def test_successful_last_request_is_returned_and_next_request_is_paused(self):
        for budget in ("requests", "complexity", "endpoint-requests"):
            with self.subTest(budget=budget):
                clock, calls = Clock(), []
                def send(*args):
                    calls.append(args)
                    return 200, {f"x-ratelimit-{budget}-remaining": "0", f"x-ratelimit-{budget}-reset": str((clock() + 120) * 1000)}, OK
                api = LinearAPI(lambda: "t", transport=send, clock=clock)
                self.assertEqual(api.graphql("query { viewer { id } }")["viewer"]["id"], "app-a")
                with self.assertRaises(RateLimited): api.graphql("query { viewer { id } }")
                self.assertEqual(len(calls), 1)

    def test_retry_after_seconds_and_http_date_and_fallback(self):
        clock = Clock()
        for value, delay in (("90", 90), (formatdate(clock() + 120, usegmt=True), 120),
                             ("nan", 60), ("inf", 60), ("-1", 60), ("invalid", 60)):
            with self.subTest(value=value):
                api = LinearAPI(lambda: "t", transport=lambda *a: (429, {"retry-after": value}, b"{}"), clock=clock)
                with self.assertRaises(RateLimited) as caught: api.graphql("query { viewer { id } }")
                self.assertEqual(caught.exception.until, clock() + delay)

    def test_retry_after_below_fallback_with_exhausted_budget(self):
        clock = Clock()
        api = LinearAPI(lambda: "t", transport=lambda *a: (429, {"Retry-After": "15", "X-RateLimit-Requests-Remaining": "0"}, LIMIT), clock=clock)
        with self.assertRaises(RateLimited) as caught: api.graphql("query { viewer { id } }")
        self.assertEqual(caught.exception.until, clock() + 15)

    def test_cooldown_survives_new_client_without_accessing_credentials(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quota.json"
            api = LinearAPI(lambda: "t", transport=lambda *a: (400, {"Retry-After": "90"}, LIMIT), clock=clock, rate_limit_path=path)
            with self.assertRaises(RateLimited): api.graphql("query { viewer { id } }")
            restored = LinearAPI(lambda: self.fail("credentials accessed during pause"), clock=clock, rate_limit_path=path)
            with self.assertRaises(RateLimited): restored.graphql("query { viewer { id } }")
            clock.now += 91
            restored.token, restored.transport = lambda: "t", lambda *a: (200, {}, OK)
            self.assertEqual(restored.viewer_id(), "app-a")
            self.assertEqual(set(json.loads(path.read_text())), {"paused_until"})

    def test_corrupt_state_is_healed_and_unwritable_state_refuses_network(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quota.json"
            path.write_text("broken")
            api = LinearAPI(lambda: self.fail("credentials accessed"), clock=clock, rate_limit_path=path)
            with self.assertRaises(RateLimited): api.graphql("query { viewer { id } }")
            self.assertEqual(json.loads(path.read_text())["paused_until"], clock() + 60)
            path.unlink()
            path.mkdir()
            with self.assertRaises(LinearError): api.graphql("query { viewer { id } }")

    def test_concurrent_call_waits_for_rate_limit_observation(self):
        clock, calls = Clock(), []
        entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
        def send(*args):
            calls.append(args)
            entered.set()
            self.assertTrue(release.wait(5))
            return 400, {"Retry-After": "90"}, LIMIT
        api = LinearAPI(lambda: "t", transport=send, clock=clock)
        def request(second=False):
            if second: waiting.set()
            with self.assertRaises(RateLimited): api.graphql("query { viewer { id } }")
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(request)
            self.assertTrue(entered.wait(5))
            second = pool.submit(request, True)
            self.assertTrue(waiting.wait(5))
            release.set()
            first.result(5)
            second.result(5)
        self.assertEqual(len(calls), 1)

    def test_startup_cooldown_wait_can_be_stopped_without_admitting_work(self):
        services = {}
        class Context:
            def get_config(self, key, default=None):
                return {"enabled": True, "identity": {"viewer_id": "app-a", "organization_id": "org-a"}}.get(key, default)
            def register_tool(self, **kwargs): pass
            def register_hook(self, *args): pass
            def register_profile_service(self, name, factory): services[name] = factory
        plugin.register(Context())
        async def run(directory):
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            def limited(*args):
                loop.call_soon_threadsafe(stop.set)
                raise RateLimited(time.time() + 120)
            with patch.object(plugin.BoundLinearAPI, "viewer_id", side_effect=limited), patch.object(plugin, "Store") as store, \
                    patch.object(plugin, "token_provider", return_value=lambda: "t"):
                await asyncio.wait_for(services["linear"](SimpleNamespace(profile_home=Path(directory), stop_event=stop)), 5)
                store.assert_not_called()
        with tempfile.TemporaryDirectory() as directory: asyncio.run(run(directory))

    def test_401_exhaustion_stops_refresh_retry_before_another_request(self):
        clock, calls = Clock(), []
        class Token:
            def __call__(self): return "t"
            def invalidate(self): pass
        def send(*args):
            calls.append(args)
            return 401, {"X-RateLimit-Requests-Remaining": "0", "X-RateLimit-Requests-Reset": str((clock() + 90) * 1000)}, b"{}"
        api = LinearAPI(Token(), transport=send, clock=clock)
        with self.assertRaises(RateLimited): api.graphql("query { viewer { id } }")
        self.assertEqual(len(calls), 1)

    def test_bound_identity_checks_and_requests_share_reentrant_admission_lock(self):
        clock, calls = Clock(), []
        def send(url, body, headers):
            calls.append(json.loads(body)["query"])
            return 200, {}, b'{"data":{"viewer":{"id":"app-a"},"organization":{"id":"org-a"}}}'
        api = plugin.BoundLinearAPI(lambda: "t", identity={"viewer_id": "app-a", "organization_id": "org-a"}, transport=send, clock=clock)
        with ThreadPoolExecutor(max_workers=1) as pool:
            self.assertEqual(pool.submit(api.graphql, "query { viewer { id } }").result(5)["viewer"]["id"], "app-a")
        self.assertEqual(len(calls), 2)

    def test_startup_retries_identity_after_cooldown_before_recovery(self):
        services, attempts = {}, []
        class Context:
            def get_config(self, key, default=None):
                return {"enabled": True, "identity": {"viewer_id": "app-a", "organization_id": "org-a"}}.get(key, default)
            def register_tool(self, **kwargs): pass
            def register_hook(self, *args): pass
            def register_profile_service(self, name, factory): services[name] = factory
        plugin.register(Context())
        async def run(directory):
            stop, bridge = asyncio.Event(), Mock()
            def identity(api):
                attempts.append(time.time())
                if len(attempts) == 1: api.paused_until = time.time() + 0.01
            def create_bridge(*args, **kwargs):
                self.assertEqual(len(attempts), 2)
                stop.set()
                return bridge
            with patch.object(plugin.BoundLinearAPI, "verify_identity", identity), \
                    patch.object(plugin, "Bridge", side_effect=create_bridge), patch.object(plugin, "Kanban"), \
                    patch.object(plugin, "token_provider", return_value=lambda: "t"):
                await asyncio.wait_for(services["linear"](SimpleNamespace(profile_home=Path(directory), profile_name="alpha", stop_event=stop)), 5)
                bridge.recover.assert_called_once()
            self.assertGreaterEqual(attempts[1] - attempts[0], 0.01)
        with tempfile.TemporaryDirectory() as directory: asyncio.run(run(directory))
