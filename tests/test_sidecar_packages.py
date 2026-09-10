"""Public sidecar package contract: new GHCR names, no private bake coupling."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
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
        self.assertEqual(packages["rest-lock-proxy"]["status"], "publishing")
        self.assertEqual(packages["tcp-proxy"]["status"], "publishing")
        self.assertEqual(packages["browser-broker"]["status"], "planned")
        self.assertEqual(packages["kokoro"]["status"], "planned")
        self.assertEqual(packages["camofox"]["status"], "planned")
        self.assertEqual(packages["rest-lock-proxy"]["context"], "services/rest-lock-proxy")
        self.assertEqual(packages["tcp-proxy"]["context"], "services/tcp-proxy")

    def test_sidecar_contract_does_not_publish_unprefixed_private_names(self) -> None:
        text = CONTRACT.read_text(encoding="utf-8")
        for name in RETIRED_PRIVATE_PACKAGES:
            with self.subTest(name=name):
                self.assertNotIn(name, text)
        self.assertNotIn("hermes-fleet-private", text)
        self.assertNotIn("private_house", text.lower())

    def test_publishing_sidecars_have_dockerfiles_and_public_source_label(self) -> None:
        for name in ("rest-lock-proxy", "tcp-proxy"):
            dockerfile = ROOT / "services" / name / "Dockerfile"
            with self.subTest(name=name):
                text = dockerfile.read_text(encoding="utf-8")
                self.assertIn("org.opencontainers.image.source=\"https://github.com/mekenthompson/hermes-fleet\"", text)
                self.assertIn(f"org.opencontainers.image.title=\"hermes-fleet-{name}\"", text)
                self.assertNotIn("hermes-fleet-private", text)
                self.assertNotIn("KEN-", text)


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
            "services/rest-lock-proxy/Dockerfile",
            "services/tcp-proxy/entrypoint.sh",
            ".github/workflows/sidecar-rest-lock-proxy.yml",
            ".github/workflows/sidecar-tcp-proxy.yml",
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
        }
        for name, path in mapping.items():
            text = path.read_text(encoding="utf-8")
            with self.subTest(name=name):
                self.assertIn(f"ghcr.io/mekenthompson/hermes-fleet-{name}", text)
                self.assertIn("github.repository == 'mekenthompson/hermes-fleet'", text)
                self.assertNotIn("hermes-fleet-private", text)
                self.assertNotIn(f"ghcr.io/mekenthompson/hermes-{name}", text)
                self.assertIn(f"services/{name}/**", text)
                self.assertNotIn("secrets.", text)
                self.assertIn("github.token", text)
                self.assertNotIn("Dockerfile\n", text.split("paths:")[1].split("jobs:")[0] if "paths:" in text else "")


class RestLockBehaviorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.lock = load(ROOT / "services/rest-lock-proxy/lock_proxy.py", "lock_proxy")

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
        script = ROOT / "services/tcp-proxy/entrypoint.sh"
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
