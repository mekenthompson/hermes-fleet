"""Tests for target-profile Linear agent provisioning."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins" / "linear-agent"))

import linear_provision


class LinearProvisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.publisher_binding = patch.object(
            linear_provision,
            "_publisher_binding",
            return_value={"viewer_id": "11111111-1111-4111-8111-111111111111", "organization_id": "22222222-2222-4222-8222-222222222222"},
        )
        self.publisher_binding.start()
        self.addCleanup(self.publisher_binding.stop)

    def test_provision_verifies_connect_binding_by_resolving_three_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            (home / ".op.env").write_text(
                "OP_CONNECT_HOST=http://onepassword-connect:8080\nOP_CONNECT_TOKEN=connect-token\n",
                encoding="utf-8",
            )
            (home / ".op.env").chmod(0o600)
            vault_id, item_id = "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb"
            policy = home / "linear-agents.json"
            policy.write_text(json.dumps({"agents": [{
                "logical_agent": "alpha", "profile": "alpha", "workspace": "demo-space",
                "rollout_scope": ["alpha"],
                "oauth": {"mode": "managed_oauth_v1", "vault_id": vault_id, "item_id": item_id,
                          "local_state": "/opt/data/secrets/linear-oauth.json", "connect_env_file": "/opt/data/.op.env"},
            }]}), encoding="utf-8")
            policy.chmod(0o444)
            item = Mock()
            item.credentials.return_value = {"client_id": "client", "client_secret": "secret", "refresh_token": "refresh"}
            verified = []
            with (
                patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}),
                patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"),
                patch.object(linear_provision, "_POLICY_PATH", policy),
                patch.object(linear_provision, "ConnectItem", return_value=item) as factory,
            ):
                linear_provision.provision(profile_home=home, profile="alpha", workspace="demo-space", vault_id=vault_id, item_id=item_id, auth_verify=verified.append, config_set=lambda *_: None, config_unset=lambda *_: None, plugin_enable=lambda *_: None)
            factory.assert_called_once_with("http://onepassword-connect:8080", "connect-token", vault_id, item_id)
            item.credentials.assert_called_once_with()
            self.assertEqual(verified[0].cache_path, home / "secrets" / "linear-oauth.json")
            self.assertIs(verified[0].connect_item, item)

    def test_provision_rejects_publisher_binding_before_connect_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            vault_id, item_id = "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb"
            policy = home / "linear-agents.json"
            policy.write_text(json.dumps({"agents": [{
                "logical_agent": "alpha", "profile": "alpha", "workspace": "demo-space",
                "rollout_scope": ["alpha"],
                "oauth": {"mode": "managed_oauth_v1", "vault_id": vault_id, "item_id": item_id,
                          "local_state": "/opt/data/secrets/linear-oauth.json", "connect_env_file": "/opt/data/.op.env"},
            }]}), encoding="utf-8")
            policy.chmod(0o444)
            with (
                patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}),
                patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"),
                patch.object(linear_provision, "_POLICY_PATH", policy),
                patch.object(linear_provision, "_publisher_binding", side_effect=RuntimeError("binding refused")),
                patch.object(linear_provision, "load_connect_env", side_effect=AssertionError("credentials read before binding")),
            ):
                with self.assertRaisesRegex(RuntimeError, "binding refused"):
                    linear_provision.provision(profile_home=home, profile="alpha", workspace="demo-space", vault_id=vault_id, item_id=item_id, config_set=lambda *_: None, config_unset=lambda *_: None, plugin_enable=lambda *_: None)

    def test_direct_provision_requires_container_identity_binding(self) -> None:
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}), patch.object(socket, "gethostname", return_value="hermes-beta"), self.assertRaisesRegex(RuntimeError, "container identity"):
            linear_provision.provision(profile_home=Path(temp), profile="alpha", workspace="demo-space", vault_id="aaaaaaaaaaaaaaaaaaaaaaaaaa", item_id="bbbbbbbbbbbbbbbbbbbbbbbbbb", config_set=lambda *_: None, config_unset=lambda _key: None, plugin_enable=lambda *_: None)

    def test_cli_requires_profile_wrapper_binding(self) -> None:
        script = ROOT / "plugins" / "linear-agent" / "linear_provision.py"
        env = dict(os.environ)
        env.pop("HERMES_PROFILE", None)
        result = subprocess.run([sys.executable, str(script), "alpha", "--workspace", "demo-space", "--vault-id", "vault-alpha", "--item-id", "item-linear"], env=env, text=True, capture_output=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires HERMES_PROFILE", result.stderr)

    def test_cli_rejects_manifest_override(self) -> None:
        script = ROOT / "plugins" / "linear-agent" / "linear_provision.py"
        result = subprocess.run([sys.executable, str(script), "alpha", "--workspace", "demo-space", "--vault-id", "vault-alpha", "--item-id", "item-linear", "--manifest", "/tmp/evil.json"], env=dict(os.environ, HERMES_PROFILE="alpha"), text=True, capture_output=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unrecognized arguments", result.stderr)

    def test_cli_rejects_target_different_from_profile_wrapper(self) -> None:
        script = ROOT / "plugins" / "linear-agent" / "linear_provision.py"
        env = dict(os.environ, HERMES_PROFILE="beta")
        result = subprocess.run([sys.executable, str(script), "alpha", "--workspace", "demo-space", "--vault-id", "vault-alpha", "--item-id", "item-linear"], env=env, text=True, capture_output=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match runtime profile", result.stderr)

    def test_provisions_private_state_and_complete_managed_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            env = home / ".op.env"
            env.write_text("OP_CONNECT_HOST=http://onepassword-connect:8080\nOP_CONNECT_TOKEN=connect-token\n", encoding="utf-8")
            env.chmod(0o600)
            marker = home / ".linear-provisioning-not-ready"
            marker.write_text("pending\n", encoding="utf-8")
            marker.chmod(0o600)
            vault_id = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
            item_id = "bbbbbbbbbbbbbbbbbbbbbbbbbb"
            fake_item = Mock()
            fake_item.credentials.return_value = {"client_id": "client", "client_secret": "secret", "refresh_token": "refresh"}
            connect_patch = patch.object(linear_provision, "ConnectItem", return_value=fake_item)
            connect_patch.start()
            self.addCleanup(connect_patch.stop)
            allowed_user_id = "11111111-1111-4111-8111-111111111111"
            policy = home / "linear-agents.json"
            policy.write_text(json.dumps({"agents": [{
                "logical_agent": "alpha",
                "profile": "alpha",
                "workspace": "demo-space",
                "rollout_scope": ["alpha"],
                "allowed_linear_user_ids": [allowed_user_id],
                "terminal_issue_status": "review",
                "reassign_to_requester": True,
                "heartbeat_seconds": 600,
                "oauth": {
                    "mode": "managed_oauth_v1",
                    "vault_id": vault_id,
                    "item_id": item_id,
                    "local_state": "/opt/data/secrets/linear-oauth.json",
                    "connect_env_file": "/opt/data/.op.env",
                },
            }]}), encoding="utf-8")
            policy.chmod(0o444)
            settings = []
            enabled = []
            verified = []
            with patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}), patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"), patch.object(linear_provision, "_POLICY_PATH", policy):
                linear_provision.provision(profile_home=home, profile="alpha", workspace="demo-space", vault_id=vault_id, item_id=item_id, auth_verify=lambda oauth: verified.append(oauth.cache_path), config_set=lambda key, value: settings.append((key, value)), config_unset=lambda _key: None, plugin_enable=lambda name: enabled.append(name))
            self.assertFalse((home / "secrets" / "linear-oauth.json").exists(), "cache is created lazily on first token()")
            prefix = "plugins.entries.linear-agent.settings"
            self.assertIn((f"{prefix}.credential_mode", "managed_oauth_v1"), settings)
            self.assertIn((f"{prefix}.connect_env_file", str(home / ".op.env")), settings)
            self.assertIn((f"{prefix}.oauth_vault_id", vault_id), settings)
            self.assertIn((f"{prefix}.oauth_item_id", item_id), settings)
            self.assertIn((
                f"{prefix}.allowed_linear_user_ids",
                json.dumps([allowed_user_id], separators=(",", ":")),
            ), settings)
            self.assertIn((f"{prefix}.enabled", "true"), settings)
            self.assertIn((f"{prefix}.terminal_issue_status", "review"), settings)
            self.assertIn((f"{prefix}.reassign_to_requester", "true"), settings)
            self.assertIn((f"{prefix}.heartbeat_seconds", "600"), settings)
            self.assertEqual(enabled, ["linear-agent"])
            self.assertEqual(verified, [home / "secrets" / "linear-oauth.json"])
            self.assertFalse(marker.exists())
            settings.clear()
            with patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}), patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"), patch.object(linear_provision, "_POLICY_PATH", policy):
                linear_provision.provision(profile_home=home, profile="alpha", workspace="demo-space", vault_id=vault_id, item_id=item_id, auth_verify=lambda oauth: verified.append(oauth.cache_path), config_load=lambda: {"plugins": {"enabled": ["linear-agent"], "disabled": [], "entries": {"linear-agent": {"settings": {"terminal_issue_status": "done", "reassign_to_requester": False, "heartbeat_seconds": 300}}}}}, config_set=lambda key, value: settings.append((key, value)), config_unset=lambda _key: None, plugin_enable=lambda name: enabled.append(name))
            self.assertIn((f"{prefix}.enabled", "true"), settings)
            self.assertIn((f"{prefix}.terminal_issue_status", "review"), settings)
            self.assertIn((f"{prefix}.reassign_to_requester", "true"), settings)
            self.assertIn((f"{prefix}.heartbeat_seconds", "600"), settings)

            policy_data = json.loads(policy.read_text(encoding="utf-8"))
            del policy_data["agents"][0]["allowed_linear_user_ids"]
            del policy_data["agents"][0]["terminal_issue_status"]
            del policy_data["agents"][0]["reassign_to_requester"]
            del policy_data["agents"][0]["heartbeat_seconds"]
            policy.chmod(0o644)
            policy.write_text(json.dumps(policy_data), encoding="utf-8")
            policy.chmod(0o444)
            unset_keys: list[str] = []
            with patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}), patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"), patch.object(linear_provision, "_POLICY_PATH", policy):
                linear_provision.provision(profile_home=home, profile="alpha", workspace="demo-space", vault_id=vault_id, item_id=item_id, auth_verify=lambda oauth: verified.append(oauth.cache_path), config_set=lambda key, value: settings.append((key, value)), config_unset=unset_keys.append, plugin_enable=lambda name: enabled.append(name))
            self.assertEqual(
                unset_keys,
                [f"{prefix}.allowed_linear_user_ids"],
            )

            with self.assertRaisesRegex(RuntimeError, "manifest"), patch.dict(os.environ, {"HERMES_PROFILE": "beta"}), patch.object(linear_provision.socket, "gethostname", return_value="hermes-beta"), patch.object(linear_provision, "_POLICY_PATH", policy):
                linear_provision.provision(profile_home=home, profile="beta", workspace="demo-space", vault_id=vault_id, item_id=item_id, config_set=lambda _key, _value: None, config_unset=lambda _key: None, plugin_enable=lambda _name: None)

    def test_preconfigured_immutable_config_skips_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            (home / ".op.env").write_text(
                "OP_CONNECT_HOST=http://onepassword-connect:8080\n"
                "OP_CONNECT_TOKEN=connect-token\n",
                encoding="utf-8",
            )
            (home / ".op.env").chmod(0o600)
            marker = home / ".linear-provisioning-not-ready"
            marker.write_text("pending\n", encoding="utf-8")
            marker.chmod(0o600)
            vault_id = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
            item_id = "bbbbbbbbbbbbbbbbbbbbbbbbbb"
            fake_item = Mock()
            fake_item.credentials.return_value = {"client_id": "client", "client_secret": "secret", "refresh_token": "refresh"}
            connect_patch = patch.object(linear_provision, "ConnectItem", return_value=fake_item)
            connect_patch.start()
            self.addCleanup(connect_patch.stop)
            allowed_user_id = "11111111-1111-4111-8111-111111111111"
            policy = home / "linear-agents.json"
            policy.write_text(json.dumps({"agents": [{
                "logical_agent": "alpha",
                "profile": "alpha",
                "workspace": "demo-space",
                "rollout_scope": ["alpha"],
                "allowed_linear_user_ids": [allowed_user_id],
                "terminal_issue_status": "review",
                "reassign_to_requester": True,
                "heartbeat_seconds": 600,
                "oauth": {
                    "mode": "managed_oauth_v1",
                    "vault_id": vault_id,
                    "item_id": item_id,
                    "local_state": "/opt/data/secrets/linear-oauth.json",
                    "connect_env_file": "/opt/data/.op.env",
                },
            }]}), encoding="utf-8")
            policy.chmod(0o444)
            expected_settings = {
                "profile": "alpha",
                "workspace": "demo-space",
                "ingress_database": str(home / "workspace" / "linear-agent" / "ingress.db"),
                "state_database": str(home / "linear-agent" / "state.db"),
                "shared_authority_database": "/opt/hermes-fleet/shared/linear-authority/shared-issue-authority.db",
                "credential_mode": "managed_oauth_v1",
                "oauth_file": str(home / "secrets" / "linear-oauth.json"),
                "connect_env_file": str(home / ".op.env"),
                "oauth_vault_id": vault_id,
                "oauth_item_id": item_id,
                "allowed_linear_user_ids": [allowed_user_id],
                "terminal_issue_status": "review",
                "reassign_to_requester": True,
                "heartbeat_seconds": 600,
                "dry_run": False,
                "enabled": True,
            }
            runtime_config = {
                "plugins": {
                    "enabled": ["linear-agent", "web/perplexity"],
                    "disabled": [],
                    "entries": {"linear-agent": {"settings": expected_settings}},
                }
            }
            mutations: list[object] = []

            with patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}), patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"), patch.object(linear_provision, "_POLICY_PATH", policy):
                linear_provision.provision(
                    profile_home=home,
                    profile="alpha",
                    workspace="demo-space",
                    vault_id=vault_id,
                    item_id=item_id,
                    auth_verify=lambda _oauth: None,
                    config_load=lambda: runtime_config,
                    config_set=lambda *args: mutations.append(("set", args)),
                    config_unset=lambda *args: mutations.append(("unset", args)),
                    plugin_enable=lambda *args: mutations.append(("enable", args)),
                )

            self.assertEqual(mutations, [])
            self.assertFalse(marker.exists())

            expected_settings["reassign_to_requester"] = 1
            with patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}), patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"), patch.object(linear_provision, "_POLICY_PATH", policy):
                linear_provision.provision(
                    profile_home=home, profile="alpha", workspace="demo-space",
                    vault_id=vault_id, item_id=item_id,
                    auth_verify=lambda *_: None, config_load=lambda: runtime_config,
                    config_set=lambda *args: mutations.append(("set", args)),
                    config_unset=lambda *_: None, plugin_enable=lambda *_: None,
                )
            self.assertIn(("set", ("plugins.entries.linear-agent.settings.reassign_to_requester", "true")), mutations)
            expected_settings["reassign_to_requester"] = True
            mutations.clear()

            marker.write_text("pending\n", encoding="utf-8")
            marker.chmod(0o600)
            runtime_config["plugins"]["disabled"] = ["linear-agent"]

            def reject_immutable_write(*_args):
                raise PermissionError("config is read-only")

            with self.assertRaisesRegex(PermissionError, "read-only"), patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}), patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"), patch.object(linear_provision, "_POLICY_PATH", policy):
                linear_provision.provision(
                    profile_home=home,
                    profile="alpha",
                    workspace="demo-space",
                    vault_id=vault_id,
                    item_id=item_id,
                    auth_verify=lambda _oauth: None,
                    config_load=lambda: runtime_config,
                    config_set=reject_immutable_write,
                    config_unset=lambda *args: mutations.append(("unset", args)),
                    plugin_enable=lambda *args: mutations.append(("enable", args)),
                )

            self.assertTrue(marker.exists())


    def test_provision_rejects_invalid_registry_lifecycle_settings(self) -> None:
        vault_id = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
        item_id = "bbbbbbbbbbbbbbbbbbbbbbbbbb"
        invalid_settings = (
            ("terminal_issue_status", "closed", "terminal_issue_status must be done or review"),
            ("terminal_issue_status", True, "terminal_issue_status must be done or review"),
            ("reassign_to_requester", 1, "reassign_to_requester must be a boolean"),
            ("heartbeat_seconds", 0, "heartbeat_seconds must be a positive finite number"),
            ("heartbeat_seconds", True, "heartbeat_seconds must be a positive finite number"),
            ("heartbeat_seconds", float("nan"), "heartbeat_seconds must be a positive finite number"),
            ("heartbeat_seconds", float("inf"), "heartbeat_seconds must be a positive finite number"),
            ("heartbeat_seconds", "600", "heartbeat_seconds must be a positive finite number"),
        )
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            policy = home / "linear-agents.json"
            for key, value, message in invalid_settings:
                with self.subTest(key=key, value=value):
                    entry = {
                        "logical_agent": "alpha",
                        "profile": "alpha",
                        "workspace": "demo-space",
                        "rollout_scope": ["alpha"],
                        "oauth": {
                            "mode": "managed_oauth_v1",
                            "vault_id": vault_id,
                            "item_id": item_id,
                            "local_state": "/opt/data/secrets/linear-oauth.json",
                            "connect_env_file": "/opt/data/.op.env",
                        },
                        key: value,
                    }
                    if policy.exists():
                        policy.chmod(0o644)
                    policy.write_text(json.dumps({"agents": [entry]}), encoding="utf-8")
                    policy.chmod(0o444)
                    with patch.dict(os.environ, {"HERMES_PROFILE": "alpha"}), patch.object(linear_provision.socket, "gethostname", return_value="hermes-alpha"), patch.object(linear_provision, "_POLICY_PATH", policy), self.assertRaisesRegex(RuntimeError, message):
                        linear_provision.provision(
                            profile_home=home,
                            profile="alpha",
                            workspace="demo-space",
                            vault_id=vault_id,
                            item_id=item_id,
                            auth_verify=lambda *_args: None,
                            config_set=lambda *_args: None,
                            config_unset=lambda *_args: None,
                            plugin_enable=lambda *_args: None,
                        )


if __name__ == "__main__":
    unittest.main()
