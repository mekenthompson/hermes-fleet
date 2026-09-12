from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
PROVIDER = ROOT / "plugins" / "web" / "webkite" / "provider.py"


def load_provider():
    agent = types.ModuleType("agent")
    web_search_provider = types.ModuleType("agent.web_search_provider")
    setattr(web_search_provider, "WebSearchProvider", type("WebSearchProvider", (), {}))
    setattr(agent, "web_search_provider", web_search_provider)
    spec = importlib.util.spec_from_file_location("public_webkite_provider", PROVIDER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "agent": agent,
            "agent.web_search_provider": web_search_provider,
            spec.name: module,
        },
    ):
        spec.loader.exec_module(module)
    return module


class WebkiteProviderTests(unittest.TestCase):
    def test_public_image_contract_bundles_provider_disabled_by_default(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        metadata = (PROVIDER.parent / "plugin.yaml").read_text(encoding="utf-8")
        contract = json.loads((ROOT / "contracts" / "plugins.json").read_text())
        matches = [
            item
            for item in contract["components"]
            if item["id"] == "webkite-web-provider"
        ]

        self.assertIn(
            "COPY plugins/web/webkite/ /opt/hermes/plugins/web/webkite/",
            dockerfile,
        )
        self.assertIn(
            "hermes plugins doctor /opt/hermes/plugins/web/webkite --ci",
            dockerfile,
        )
        self.assertEqual(
            matches,
            [
                {
                    "id": "webkite-web-provider",
                    "target": "standalone_public_plugin",
                    "default_enabled": False,
                    "conditions": [
                        "generic_configuration",
                        "local_cli",
                        "independent_tests",
                        "license_review",
                        "no_deployment_identity",
                    ],
                }
            ],
        )
        self.assertIn("author: Hermes Fleet Contributors", metadata)

    def test_search_and_extract_are_generic(self):
        provider = load_provider().WebkiteWebSearchProvider()
        self.assertEqual(provider.name, "webkite")
        self.assertTrue(provider.supports_search())
        self.assertTrue(provider.supports_extract())

    def test_search_maps_json_results(self):
        module = load_provider()
        completed = subprocess.CompletedProcess(
            args=["webkite"],
            returncode=0,
            stdout=json.dumps(
                [{"title": "A", "url": "https://example.com", "snippet": "B"}]
            ),
            stderr="",
        )
        with patch.object(module.WebkiteWebSearchProvider, "_run", return_value=completed):
            result = module.WebkiteWebSearchProvider().search("  query  ", 9)
        self.assertTrue(result["success"])
        self.assertEqual(
            result["data"]["web"],
            [
                {
                    "title": "A",
                    "url": "https://example.com",
                    "description": "B",
                    "position": 1,
                }
            ],
        )
