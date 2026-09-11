"""Public sidecar package contract: new GHCR names, no private bake coupling."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "sidecars/packages.json"
IMAGE_REF = ROOT / "scripts/sidecar_image_ref.py"
SCOPE = ROOT / "scripts/fleet-image-change-scope.py"
FLEET_WORKFLOW = ROOT / ".github/workflows/fleet-image.yml"

PUBLIC_PACKAGES = {
    "rest-lock-proxy": "ghcr.io/mekenthompson/hermes-fleet-rest-lock-proxy",
    "tcp-proxy": "ghcr.io/mekenthompson/hermes-fleet-tcp-proxy",
    "browser-broker": "ghcr.io/mekenthompson/hermes-fleet-browser-broker",
    "kokoro": "ghcr.io/mekenthompson/hermes-fleet-kokoro",
    "camofox": "ghcr.io/mekenthompson/hermes-fleet-camofox",
}

RETIRED_PRIVATE_PACKAGES = (
    "ghcr.io/mekenthompson/hermes-rest-lock-proxy",
    "ghcr.io/mekenthompson/hermes-tcp-proxy",
    "ghcr.io/mekenthompson/hermes-browser-broker",
    "ghcr.io/mekenthompson/hermes-kokoro",
    "ghcr.io/mekenthompson/hermes-camofox",
)

DIGEST = "0123456789abcdef" * 4


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class SidecarContractTests(unittest.TestCase):
    def test_sidecar_contract_declares_prefixed_public_packages(self) -> None:
        payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(
            payload["source_repository"],
            "https://github.com/mekenthompson/hermes-fleet",
        )
        packages = payload["packages"]
        for key, image in PUBLIC_PACKAGES.items():
            with self.subTest(key=key):
                self.assertEqual(packages[key]["image"], image)
                self.assertEqual(packages[key]["visibility"], "public")
                self.assertTrue(packages[key]["require_digest_pin"])
                self.assertEqual(packages[key]["context"], f"sidecars/{key}")
        self.assertEqual(packages["rest-lock-proxy"]["status"], "publishing")
        self.assertEqual(packages["tcp-proxy"]["status"], "publishing")
        self.assertEqual(packages["browser-broker"]["status"], "publishing")
        self.assertEqual(packages["kokoro"]["status"], "planned")
        self.assertEqual(packages["camofox"]["status"], "planned")
        self.assertFalse((ROOT / "services").exists())

    def test_sidecar_contract_does_not_publish_unprefixed_private_names(self) -> None:
        text = CONTRACT.read_text(encoding="utf-8")
        for name in RETIRED_PRIVATE_PACKAGES:
            with self.subTest(name=name):
                self.assertNotIn(name, text)
        self.assertNotIn("hermes-fleet-private", text)
        self.assertNotIn("private_house", text.lower())

    def test_publishing_sidecars_have_dockerfiles_and_public_source_label(self) -> None:
        for name in ("rest-lock-proxy", "tcp-proxy", "browser-broker"):
            dockerfile = ROOT / "sidecars" / name / "Dockerfile"
            with self.subTest(name=name):
                text = dockerfile.read_text(encoding="utf-8")
                self.assertIn("org.opencontainers.image.source=\"https://github.com/mekenthompson/hermes-fleet\"", text)
                self.assertIn(f"org.opencontainers.image.title=\"hermes-fleet-{name}\"", text)
                self.assertNotIn("hermes-fleet-private", text)
                self.assertNotIn("KEN-", text)


class BrowserBrokerPublicTests(unittest.TestCase):
    def test_browser_broker_is_publishing_with_public_dockerfile(self) -> None:
        payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
        self.assertEqual(payload["packages"]["browser-broker"]["status"], "publishing")
        dockerfile = ROOT / "sidecars/browser-broker/Dockerfile"
        self.assertTrue(dockerfile.is_file(), dockerfile)
        text = dockerfile.read_text(encoding="utf-8")
        self.assertIn(
            'org.opencontainers.image.source="https://github.com/mekenthompson/hermes-fleet"',
            text,
        )
        self.assertIn('org.opencontainers.image.title="hermes-fleet-browser-broker"', text)
        self.assertNotIn("hermes-fleet-private", text)
        self.assertNotIn("KEN-", text)

    def test_browser_broker_source_has_no_house_topology(self) -> None:
        root = ROOT / "sidecars/browser-broker"
        self.assertTrue(root.is_dir(), root)
        prohibited = (
            "switchroom",
            "car" + "rie",
            "over" + "lord",
            "klank" + "er",
            "gr" + "unt",
            "cl" + "erk",
            "gym" + "bro",
            "mar" + "ko",
            "law" + "gpt",
            "ag" + "gie",
            "KEN-",
        )
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            lowered = text.lower()
            for ident in prohibited:
                with self.subTest(path=str(path.relative_to(ROOT)), ident=ident):
                    self.assertNotIn(ident.lower() if ident != "KEN-" else ident, lowered if ident != "KEN-" else text)

    def test_browser_broker_public_base_comes_from_env(self) -> None:
        previous = os.environ.get("PUBLIC_BASE")
        os.environ["PUBLIC_BASE"] = "https://browser.example.test"
        try:
            policy = load(
                ROOT / "sidecars/browser-broker/session_policy.py",
                "session_policy_public_base",
            )
            self.assertEqual(policy.PUBLIC_BASE, "https://browser.example.test")
            self.assertNotIn("switchroom", policy.PUBLIC_BASE.lower())
        finally:
            if previous is None:
                os.environ.pop("PUBLIC_BASE", None)
            else:
                os.environ["PUBLIC_BASE"] = previous

    def test_browser_broker_workflow_targets_new_public_package(self) -> None:
        path = ROOT / ".github/workflows/sidecar-browser-broker.yml"
        self.assertTrue(path.is_file(), path)
        text = path.read_text(encoding="utf-8")
        self.assertIn("ghcr.io/mekenthompson/hermes-fleet-browser-broker", text)
        self.assertIn("github.repository == 'mekenthompson/hermes-fleet'", text)
        self.assertNotIn("hermes-fleet-private", text)
        self.assertNotIn("ghcr.io/mekenthompson/hermes-browser-broker", text)
        self.assertIn("sidecars/browser-broker/**", text)
        self.assertNotIn("services/browser-broker", text)
        self.assertNotIn("secrets.", text)
        self.assertIn("github.token", text)


class SidecarImageRefTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ref = load(IMAGE_REF, "sidecar_image_ref")

    def test_accepts_digest_pin_for_allowed_public_package(self) -> None:
        value = f"ghcr.io/mekenthompson/hermes-fleet-rest-lock-proxy@sha256:{DIGEST}"
        self.assertEqual(self.ref.require_sidecar_image(value), value)

    def test_rejects_mutable_tag(self) -> None:
        with self.assertRaises(ValueError):
            self.ref.require_sidecar_image(
                "ghcr.io/mekenthompson/hermes-fleet-rest-lock-proxy:latest"
            )

    def test_rejects_unprefixed_private_package_even_with_digest(self) -> None:
        with self.assertRaises(ValueError):
            self.ref.require_sidecar_image(
                f"ghcr.io/mekenthompson/hermes-rest-lock-proxy@sha256:{DIGEST}"
            )

    def test_rejects_agent_child_package(self) -> None:
        with self.assertRaises(ValueError):
            self.ref.require_sidecar_image(
                f"ghcr.io/mekenthompson/hermes-fleet-public@sha256:{DIGEST}"
            )


class SidecarScopeIsolationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = load(SCOPE, "fleet_image_change_scope")

    def test_sidecar_paths_do_not_trigger_fleet_child_bake(self) -> None:
        for path in (
            "sidecars/rest-lock-proxy/Dockerfile",
            "sidecars/tcp-proxy/entrypoint.sh",
            "sidecars/browser-broker/Dockerfile",
            ".github/workflows/sidecar-rest-lock-proxy.yml",
            ".github/workflows/sidecar-tcp-proxy.yml",
            ".github/workflows/sidecar-browser-broker.yml",
            "sidecars/packages.json",
            "scripts/sidecar_image_ref.py",
            "docs/sidecars.md",
        ):
            with self.subTest(path=path):
                run, _, matched = self.scope.decide(
                    "push", "a" * 40, "b" * 40, lambda *_: [path]
                )
                self.assertFalse(run)
                self.assertEqual(matched, [])

    def test_sidecar_workflows_target_new_public_packages_and_this_repo(self) -> None:
        mapping = {
            "rest-lock-proxy": ROOT / ".github/workflows/sidecar-rest-lock-proxy.yml",
            "tcp-proxy": ROOT / ".github/workflows/sidecar-tcp-proxy.yml",
            "browser-broker": ROOT / ".github/workflows/sidecar-browser-broker.yml",
        }
        for name, path in mapping.items():
            text = path.read_text(encoding="utf-8")
            with self.subTest(name=name):
                self.assertIn(f"ghcr.io/mekenthompson/hermes-fleet-{name}", text)
                self.assertIn("github.repository == 'mekenthompson/hermes-fleet'", text)
                self.assertNotIn("hermes-fleet-private", text)
                self.assertNotIn(f"ghcr.io/mekenthompson/hermes-{name}", text)
                self.assertIn(f"sidecars/{name}/**", text)
                self.assertNotIn(f"services/{name}", text)
                self.assertNotIn("secrets.", text)
                self.assertIn("github.token", text)
                self.assertNotIn("Dockerfile\n", text.split("paths:")[1].split("jobs:")[0] if "paths:" in text else "")


class RestLockBehaviorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.lock = load(ROOT / "sidecars/rest-lock-proxy/lock_proxy.py", "lock_proxy")

    def test_missing_lock_file_is_unlocked(self) -> None:
        missing = Path(tempfile.mkdtemp()) / "lock"
        self.assertFalse(self.lock.lock_is_active(missing))

    def test_existing_lock_file_is_locked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "lock"
            lock_file.write_text("held\n", encoding="utf-8")
            self.assertTrue(self.lock.lock_is_active(lock_file))


class TcpProxyEntrypointTests(unittest.TestCase):
    def test_entrypoint_requires_listen_port_and_target(self) -> None:
        script = ROOT / "sidecars/tcp-proxy/entrypoint.sh"
        self.assertTrue(script.is_file(), script)
        env = {**os.environ}
        env.pop("LISTEN_PORT", None)
        env.pop("TARGET", None)
        result = subprocess.run(
            ["sh", str(script)],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
