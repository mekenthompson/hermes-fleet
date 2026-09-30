"""Deployment identity and optional issue-scope binding without real credentials."""
import json
import unittest

from linear_fake_api import load_plugin

plugin = load_plugin()
from hermes_fleet_linear_plugin import BoundLinearAPI
from hermes_fleet_linear_plugin.api import LinearError


class IdentityBindingTests(unittest.TestCase):
    def client(self, identity, *, viewer="app-a", organization="org-a", issue=None):
        calls = []

        def transport(url, body, headers):
            query = json.loads(body)["query"]
            calls.append(query)
            data = ({"viewer": {"id": viewer}, "organization": {"id": organization}}
                    if "IdentityBinding" in query else
                    {"issue": issue} if "query Issue(" in query else
                    {"issueUpdate": {"success": True}})
            return 200, {}, json.dumps({"data": data}).encode()

        return BoundLinearAPI(lambda: "synthetic", identity=identity, transport=transport), calls

    def test_wrong_viewer_or_workspace_refuses_before_mutation(self):
        for viewer, org in (("foreign", "org-a"), ("app-a", "foreign"), (None, "org-a")):
            with self.subTest(viewer=viewer, organization=org):
                client, calls = self.client({"viewer_id": "app-a", "organization_id": "org-a"},
                                            viewer=viewer, organization=org)
                with self.assertRaises(LinearError):
                    client.update_issue("i", {"stateId": "started"})
                self.assertFalse(any(q.startswith("mutation") for q in calls))

    def test_rechecks_identity_after_cached_viewer_and_before_mutation(self):
        client, calls = self.client({"viewer_id": "app-a", "organization_id": "org-a"})
        self.assertEqual(client.viewer_id(), "app-a")
        client.update_issue("i", {"stateId": "started"})
        self.assertEqual(sum("IdentityBinding" in q for q in calls), 2)

    def test_identity_changes_after_viewer_lookup_refuse_next_mutation(self):
        client, calls = self.client({"viewer_id": "app-a", "organization_id": "org-a"})
        self.assertEqual(client.viewer_id(), "app-a")
        def changed_transport(url, body, headers):
            query = json.loads(body)["query"]
            calls.append(query)
            return 200, {}, json.dumps({"data": {"viewer": {"id": "foreign"},
                                                 "organization": {"id": "org-a"}}}).encode()
        client.transport = changed_transport
        with self.assertRaises(LinearError):
            client.update_issue("i", {"stateId": "started"})
        self.assertFalse(any(q.startswith("mutation") for q in calls))

    def test_malformed_identity_is_a_controlled_refusal(self):
        for value in (None, [], "app-a", {}, {"id": None}):
            client, _ = self.client({"viewer_id": "app-a", "organization_id": "org-a"})
            client.transport = lambda *args: (200, {}, json.dumps({"data": {
                "viewer": value, "organization": {"id": "org-a"}}}).encode())
            with self.subTest(value=value), self.assertRaises(LinearError):
                client.viewer_id()

    def test_service_refuses_invalid_binding_before_credentials_or_store(self):
        import asyncio
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import patch
        services = {}
        class Context:
            def get_config(self, key, default=None):
                return {"enabled": True, "identity": {}}.get(key, default)
            def register_tool(self, **kwargs):
                pass
            def register_hook(self, *args):
                pass
            def register_profile_service(self, name, factory):
                services[name] = factory
        plugin.register(Context())
        with tempfile.TemporaryDirectory() as directory, patch.object(plugin, "token_provider") as tokens, \
                patch.object(plugin, "Store") as store:
            with self.assertRaises(ValueError):
                asyncio.run(services["linear"](SimpleNamespace(profile_home=Path(directory))))
            tokens.assert_not_called()
            store.assert_not_called()

    def test_service_refuses_wrong_actor_before_store(self):
        import asyncio
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import patch
        services = {}
        class Context:
            def get_config(self, key, default=None):
                return {"enabled": True, "identity": {"viewer_id": "app-a", "organization_id": "org-a"}}.get(key, default)
            def register_tool(self, **kwargs):
                pass
            def register_hook(self, *args):
                pass
            def register_profile_service(self, name, factory):
                services[name] = factory
        plugin.register(Context())
        with tempfile.TemporaryDirectory() as directory, patch.object(plugin, "token_provider", return_value=lambda: "synthetic"), \
                patch.object(BoundLinearAPI, "verify_identity", side_effect=LinearError("wrong actor")), \
                patch.object(plugin, "Store") as store:
            with self.assertRaises(LinearError):
                asyncio.run(services["linear"](SimpleNamespace(profile_home=Path(directory))))
            store.assert_not_called()

    def test_invalid_binding_refused_without_credential_access(self):
        for identity in ({}, {"viewer_id": "app-a"}, {"viewer_id": 1, "organization_id": "org-a"},
                         {"viewer_id": "app-a", "organization_id": "org-a", "teams": "OPS"}):
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                BoundLinearAPI(lambda: self.fail("credentials accessed"), identity=identity)

    def test_optional_scope_denies_foreign_team_or_project(self):
        identity = {"viewer_id": "app-a", "organization_id": "org-a", "teams": ["OPS"], "projects": ["p"]}
        for team, project in (("OTHER", "p"), ("OPS", "foreign"), ("OPS", None)):
            client, _ = self.client(identity, issue={"id": "i", "team": {"key": team},
                                                     "project": {"id": project}})
            with self.subTest(team=team, project=project), self.assertRaises(LinearError):
                client.issue("i")
        client, _ = self.client(identity, issue={"id": "i", "team": {"key": "OPS"}, "project": {"id": "p"}})
        self.assertEqual(client.issue("i")["id"], "i")
