"""Inbox addressing is independent of the local chat/executor namespace."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from contextlib import closing

from linear_fake_api import load_plugin
from linear_ingress_fixture import IngressStore, Route

load_plugin()
from hermes_fleet_linear_plugin.bridge import Bridge
from hermes_fleet_linear_plugin.store import Store


class IngressBindingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.inbox = self.home / "ingress.db"
        self.producer = IngressStore(self.inbox)
        self.store = Store(self.home / "state.db")

    def enqueue(self, profile, delivery, event=None):
        route = Route(profile, profile, "/webhook/" + profile, self.home / "unused", self.inbox)
        self.producer.enqueue(route, delivery, json.dumps(event or {"type": "unused"}).encode())

    def statuses(self):
        with closing(sqlite3.connect(self.inbox)) as db:
            return dict(db.execute("SELECT delivery_id, status FROM deliveries"))

    def bridge(self, settings=None):
        return Bridge(self.store, SimpleNamespace(), SimpleNamespace(executor_profile="default"),
                      profile="default", settings=settings)

    def test_explicit_inbox_binding_imports_only_target_without_relabeling_executor(self):
        self.enqueue("worker-a", "target")
        self.enqueue("default", "local-name")
        self.enqueue("worker-b", "foreign")
        bridge = self.bridge({"ingress_profile": "worker-a"})
        bridge.drain_ingress(self.inbox)
        self.assertEqual(self.statuses(), {"target": "imported", "local-name": "pending", "foreign": "pending"})
        self.assertEqual(bridge.profile, "default")
        self.assertTrue(bridge.chat_profile_matches("default"))
        self.assertFalse(bridge.chat_profile_matches("worker-a"))
        self.assertFalse(self.store.active())
        self.assertFalse(self.store.pending())

    def test_target_stop_uses_same_binding_and_precedes_bounded_normal_work(self):
        from unittest.mock import patch
        self.store.put("i", "chat", "owner")
        self.enqueue("worker-a", "a-work", {"type": "unused", "tag": "work"})
        stop = {"agentActivity": {"signal": "stop", "createdAt": "2026-10-06T00:00:00Z"},
                "agentSession": {"issue": {"id": "i"}}, "tag": "target-stop"}
        self.enqueue("worker-a", "z-stop", stop)
        self.enqueue("default", "foreign-stop", {**stop, "agentSession": {"issue": {"id": "other"}}})
        bridge = self.bridge({"ingress_profile": "worker-a", "ingress_database": str(self.inbox)})
        self.assertTrue(bridge._queued_stop("i", 0))
        self.assertFalse(bridge._queued_stop("other", 0))
        seen = []
        with patch.object(bridge, "handle_webhook", side_effect=lambda event: seen.append(event["tag"])):
            bridge.drain_ingress(self.inbox, limit=1)
        self.assertEqual(seen, ["target-stop", "work"])
        self.assertEqual(self.statuses(), {"a-work": "imported", "z-stop": "imported", "foreign-stop": "pending"})

    def test_absent_binding_preserves_legacy_profile_selection(self):
        self.enqueue("default", "local")
        self.enqueue("worker-a", "foreign")
        self.bridge().drain_ingress(self.inbox)
        self.assertEqual(self.statuses(), {"local": "imported", "foreign": "pending"})

    def test_reopened_consumer_keeps_owner_and_does_not_repeat_imports(self):
        from unittest.mock import patch
        self.store.put("i", "chat", "owner", last_updated_at=1000)
        before = self.store.get("i")
        self.enqueue("worker-a", "target")
        self.bridge({"ingress_profile": "worker-a"}).drain_ingress(self.inbox)
        reopened = Bridge(Store(self.store.path), SimpleNamespace(), SimpleNamespace(executor_profile="default"),
                          profile="default", settings={"ingress_profile": "worker-a"})
        with patch.object(reopened, "handle_webhook") as handle:
            reopened.drain_ingress(self.inbox)
            handle.assert_not_called()
        self.assertEqual(reopened.store.get("i"), before)
        with closing(sqlite3.connect(self.inbox)) as db:
            self.assertEqual(db.execute("SELECT status, attempts FROM deliveries").fetchone(), ("imported", 1))

    def test_registered_service_forwards_explicit_binding_to_bridge(self):
        import asyncio
        from unittest.mock import patch
        plugin = load_plugin()
        services, captured = {}, {}
        values = {"enabled": True, "identity": {"viewer_id": "app-a", "organization_id": "org-a"},
                  "ingress_profile": "worker-a"}
        context = SimpleNamespace(get_config=lambda key, default=None: values.get(key, default),
                                  register_tool=lambda **kwargs: None, register_hook=lambda *args: None,
                                  register_profile_service=lambda name, factory: services.update({name: factory}))
        plugin.register(context)
        class Captured(Exception): pass
        def capture(*args, **kwargs):
            captured.update(kwargs)
            raise Captured()
        async def run():
            runtime = SimpleNamespace(profile_home=self.home, profile_name="default", stop_event=asyncio.Event())
            with patch.object(plugin.BoundLinearAPI, "viewer_id", return_value="app-a"), \
                    patch.object(plugin, "token_provider", return_value=lambda: "synthetic"), \
                    patch.object(plugin, "Store"), patch.object(plugin, "Kanban"), \
                    patch.object(plugin, "Bridge", side_effect=capture):
                with self.assertRaises(Captured):
                    await asyncio.wait_for(services["linear"](runtime), 5)
        asyncio.run(run())
        self.assertEqual(captured["settings"].get("ingress_profile"), "worker-a")
        self.assertEqual(captured["profile"], "default")

    def test_invalid_binding_refuses_service_before_credentials_or_state_admission(self):
        import asyncio
        from unittest.mock import Mock, patch
        from hermes_fleet_linear_plugin import transport
        plugin = load_plugin()
        for invalid in ("", " worker-a", "../worker-a", [], False, 123):
            with self.subTest(value=invalid):
                services = {}
                values = {"enabled": True, "identity": {"viewer_id": "app-a", "organization_id": "org-a"},
                          "ingress_profile": invalid}
                context = SimpleNamespace(get_config=lambda key, default=None: values.get(key, default),
                                          register_tool=lambda **kwargs: None, register_hook=lambda *args: None,
                                          register_profile_service=lambda name, factory: services.update({name: factory}))
                plugin.register(context)
                async def run():
                    stop = asyncio.Event()
                    runtime = SimpleNamespace(profile_home=self.home, profile_name="default", stop_event=stop)
                    api = SimpleNamespace(viewer_id=lambda: "app-a", clock=lambda: 0, paused_until=0)
                    def admit(*args, **kwargs):
                        stop.set()
                        return Mock()
                    with patch.object(plugin, "BoundLinearAPI", return_value=api) as api_factory, \
                            patch.object(plugin, "token_provider") as credentials, \
                            patch.object(plugin, "Store") as store, patch.object(plugin, "Kanban"), \
                            patch.object(plugin, "Bridge", side_effect=admit), patch.object(transport, "Server"):
                        with self.assertRaisesRegex(ValueError, "ingress_profile"):
                            await asyncio.wait_for(services["linear"](runtime), 5)
                        api_factory.assert_not_called()
                        credentials.assert_not_called()
                        store.assert_not_called()
                asyncio.run(run())
