from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Coroutine, cast

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins" / "linear-agent"
SOURCE_FILES = {
    "__init__.py",
    "linear_activity.py",
    "linear_agent.py",
    "linear_attachments.py",
    "linear_budgets.py",
    "linear_chat_closeout.py",
    "linear_completion.py",
    "linear_connect.py",
    "linear_cos_dispatch.py",
    "linear_guard_health.py",
    "linear_handoff.py",
    "linear_limits.py",
    "linear_live_canary.py",
    "linear_oauth.py",
    "linear_ownership.py",
    "linear_parent_continuation.py",
    "linear_parent_followup.py",
    "linear_policy.py",
    "linear_project_updates.py",
    "linear_provision.py",
    "linear_quota.py",
    "linear_readiness.py",
    "linear_reconcile.py",
    "linear_resume.py",
    "linear_runtime.py",
    "linear_stop.py",
    "linear_tracking.py",
    "plugin.yaml",
}


def load_plugin():
    spec = importlib.util.spec_from_file_location(
        "hermes_plugins.linear_agent_public_contract",
        PLUGIN / "__init__.py",
        submodule_search_locations=[str(PLUGIN)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class LinearPluginPublicContractTests(unittest.TestCase):
    def test_generic_plugin_source_is_complete(self) -> None:
        self.assertEqual(
            {path.name for path in PLUGIN.iterdir() if path.is_file()},
            SOURCE_FILES,
        )

    def test_house_policy_and_operations_are_not_embedded(self) -> None:
        self.assertFalse((PLUGIN / "linear-agents.json").exists())
        self.assertFalse((PLUGIN / "linear-publishers.json").exists())
        self.assertTrue((PLUGIN / "linear_ownership.py").is_file())
        self.assertTrue((PLUGIN / "linear_stop.py").is_file())
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn(
            "COPY plugins/linear-agent/ /opt/hermes/plugins/linear-agent/",
            dockerfile,
        )
        self.assertIn(
            "test ! -e /opt/hermes/plugins/linear-agent/linear-agents.json",
            dockerfile,
        )

    def test_plugin_is_declared_public_and_disabled_by_default(self) -> None:
        manifest = (PLUGIN / "plugin.yaml").read_text(encoding="utf-8")
        self.assertIn("author: Hermes Fleet Contributors", manifest)
        contract = json.loads((ROOT / "contracts" / "plugins.json").read_text())
        matches = [item for item in contract["components"] if item["id"] == "linear-agent"]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["target"], "standalone_public_plugin")
        self.assertFalse(matches[0]["default_enabled"])
        self.assertIn("external_deployment_policy", matches[0]["conditions"])
        self.assertIn("runtime_read_only_policy", matches[0]["conditions"])

    def test_missing_house_policy_fails_closed(self) -> None:
        module = load_plugin()
        with tempfile.TemporaryDirectory() as temp:
            module._POLICY_PATH = Path(temp) / "missing.json"
            with self.assertRaisesRegex(RuntimeError, "policy is unavailable"):
                module._require_policy(
                    profile="sample",
                    workspace="example-workspace",
                    vault_id="vault-sample",
                    item_id="item-linear",
                    allowed_linear_user_ids=None,
                )

    def test_missing_house_policy_stops_registered_service_before_secrets(self) -> None:
        module = load_plugin()
        captured: dict[str, object] = {}

        class Context:
            def __init__(self, settings: dict[str, object]) -> None:
                self.settings = settings

            def get_config(self, key: str, default: object = None) -> object:
                return self.settings.get(key, default)

            def register_profile_service(self, name: str, factory) -> None:
                captured[name] = factory

        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            secrets = home / "secrets"
            secrets.mkdir(mode=0o700)
            setattr(module, "_POLICY_PATH", home / "missing-policy.json")
            setattr(
                module,
                "load_connect_environment",
                lambda _path: (_ for _ in ()).throw(
                    AssertionError("Connect must not be read before policy validation")
                ),
            )
            base_settings = {
                "enabled": True,
                "workspace": "example-workspace",
                "profile": "sample",
                "ingress_database": str(home / "workspace" / "linear-agent" / "ingress.db"),
                "state_database": str(home / "linear-agent" / "state.db"),
                "credential_mode": "managed_oauth_v1",
                "oauth_file": str(secrets / "linear-oauth.json"),
                "connect_env_file": str(home / ".op.env"),
                "oauth_vault_id": "example-vault-binding",
                "oauth_item_id": "example-item-binding",
            }
            for dry_run in (False, True):
                with self.subTest(dry_run=dry_run):
                    captured.clear()
                    settings = {**base_settings, "dry_run": dry_run}
                    module.register(Context(settings))
                    service = cast(
                        Callable[[object], Coroutine[Any, Any, None]],
                        captured["linear-agent"],
                    )
                    runtime = SimpleNamespace(
                        profile_name="sample",
                        profile_home=home,
                        stop_event=asyncio.Event(),
                    )
                    with self.assertRaisesRegex(RuntimeError, "policy is unavailable"):
                        asyncio.run(asyncio.wait_for(service(runtime), timeout=0.1))

            policy = home / "linear-agents.json"
            policy.write_text(json.dumps({
                "agents": [{
                    "profile": "sample",
                    "logical_agent": "sample",
                    "workspace": "example-workspace",
                    "rollout_scope": ["sample"],
                    "allowed_linear_user_ids": None,
                    "oauth": {
                        "mode": "managed_oauth_v1",
                        "vault_id": "example-vault-binding",
                        "item_id": "example-item-binding",
                        "local_state": "/opt/data/secrets/linear-oauth.json",
                        "connect_env_file": "/opt/data/.op.env",
                    },
                }]
            }), encoding="utf-8")
            policy.chmod(0o444)
            setattr(module, "_POLICY_PATH", policy)
            setattr(module, "read_agent_policy", lambda _path: json.loads(policy.read_text(encoding="utf-8")))
            captured.clear()
            module.register(Context({**base_settings, "dry_run": True}))
            service = cast(
                Callable[[object], Coroutine[Any, Any, None]],
                captured["linear-agent"],
            )
            stop_event = asyncio.Event()
            stop_event.set()
            runtime = SimpleNamespace(
                profile_name="sample",
                profile_home=home,
                stop_event=stop_event,
            )
            asyncio.run(asyncio.wait_for(service(runtime), timeout=0.1))

    def test_all_policy_consumers_use_shared_immutable_reader(self) -> None:
        for name in (
            "__init__.py",
            "linear_live_canary.py",
            "linear_project_updates.py",
            "linear_provision.py",
            "linear_tracking.py",
        ):
            with self.subTest(name=name):
                source = (PLUGIN / name).read_text(encoding="utf-8")
                self.assertIn("linear_policy", source)
                self.assertNotIn('with_name("linear-agents.json")', source)
                self.assertNotIn('with_name("linear-publishers.json")', source)

    def test_writable_house_policy_is_rejected(self) -> None:
        module = load_plugin()
        with tempfile.TemporaryDirectory() as temp:
            policy = Path(temp) / "linear-agents.json"
            policy.write_text('{"agents": []}\n', encoding="utf-8")
            policy.chmod(0o644)
            setattr(module, "_POLICY_PATH", policy)
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                module._require_policy(
                    profile="sample",
                    workspace="example-workspace",
                    vault_id="vault-sample",
                    item_id="item-linear",
                    allowed_linear_user_ids=None,
                )

    def test_symlinked_house_policy_is_rejected(self) -> None:
        module = load_plugin()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / "policy-target.json"
            target.write_text('{"agents": []}\n', encoding="utf-8")
            target.chmod(0o444)
            policy = root / "linear-agents.json"
            policy.symlink_to(target)
            setattr(module, "_POLICY_PATH", policy)
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                module._require_policy(
                    profile="sample",
                    workspace="example-workspace",
                    vault_id="vault-sample",
                    item_id="item-linear",
                    allowed_linear_user_ids=None,
                )

    def test_runtime_owned_synthetic_external_policy_is_rejected(self) -> None:
        module = load_plugin()
        user_id = "11111111-1111-4111-8111-111111111111"
        with tempfile.TemporaryDirectory() as temp:
            policy = Path(temp) / "linear-agents.json"
            policy.write_text(json.dumps({"agents": [{
                "logical_agent": "sample",
                "profile": "sample",
                "workspace": "example-workspace",
                "rollout_scope": ["sample"],
                "allowed_linear_user_ids": [user_id],
                "oauth": {
                    "mode": "managed_oauth_v1",
                    "vault_id": "vault-sample",
                    "item_id": "item-linear",
                    "local_state": "/opt/data/secrets/linear-oauth.json",
                    "connect_env_file": "/opt/data/.op.env",
                },
            }]}), encoding="utf-8")
            policy.chmod(0o444)
            module._POLICY_PATH = policy
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                module._require_policy(
                    profile="sample",
                    workspace="example-workspace",
                    vault_id="vault-sample",
                    item_id="item-linear",
                    allowed_linear_user_ids=[user_id],
                )


if __name__ == "__main__":
    unittest.main()
