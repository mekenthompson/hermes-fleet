from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/fleet-image.yml"
AGENT_MANIFEST = ROOT / "release/agent-image-manifest.json"
READ_MANIFEST = ROOT / "scripts/read-agent-image-manifest.py"
EMIT_MANIFEST = ROOT / "scripts/emit-fleet-image-manifest.py"
COMPACT_SBOM = ROOT / "scripts/compact-spdx-sbom.py"
EXTRACT_PUSH_DIGEST = ROOT / "scripts/extract-pushed-image-digest.py"
AGENT_REPOSITORY = "ghcr.io/mekenthompson/hermes-agent"
AGENT_REVISION = "b2b676ca3ae04ded170e32cd8872173f30348d3d"
AGENT_DIGEST = "sha256:bc955c2ac9765ede2a9e4b5fda17ee38bf8c82a058ecd4247267d0acd23447d8"
AGENT_REF = f"{AGENT_REPOSITORY}@{AGENT_DIGEST}"
ONEPASSWORD_CLI_IMAGE = (
    "docker.io/1password/op@"
    "sha256:d7d12b409ec699c9fa139d3bdfc80671f744380d39db8c539d9dc6e7e553d3c1"
)
CLAUDE_CODE_VERSION = "2.1.261"
CLAUDE_CODE_INTEGRITY = "sha512-j6+AkfCl6/UJBcx66nlZUmWc4XGK3TscvW19Tiat+oDwkz3WqQfKzjvHO5FhR+shXTtktqs6vqSBrJmeSWpU3Q=="
CLAUDE_CODE_LINUX_X64_INTEGRITY = "sha512-t7yPrjZH7/xOPR0HYg8NDBr9dGqIi2y3dCDLKmx2zHedSHjAGxoX7HnHeXr1tdk3iw2a6sM/LsV9O7/+Ar86FA=="
CLAUDE_AGENT_ACP_VERSION = "0.75.1"
CLAUDE_AGENT_ACP_INTEGRITY = "sha512-Un6I4BRkhpCFS3I7kr5C/lkAm8Nc3VuGZU2YQ3xIpJAIxV94iWO0Q2CH2QABxMERpONRu4Le6XC9V+5PImQZ2A=="
CLAUDE_ACP_PLUGIN_SOURCE = "https://github.com/mvdbastos/hermes-acp-agents"
CLAUDE_ACP_PLUGIN_REVISION = "0526610a3945cc376ac517b63ca358a5b838a2fc"


class FleetImageReleaseTests(unittest.TestCase):
    def test_release_files_exist(self) -> None:
        for path in (
            WORKFLOW,
            AGENT_MANIFEST,
            READ_MANIFEST,
            EMIT_MANIFEST,
            COMPACT_SBOM,
            ROOT / "scripts/validate-spdx-schema.py",
            ROOT / "release/spdx-2.3-schema.json",
            ROOT / "release/spdx-validation-requirements.txt",
            ROOT / "docs/image-release.md",
            ROOT / "scripts/claude-acp-subscription",
        ):
            with self.subTest(path=path):
                self.assertTrue(path.is_file())

    def test_agent_handoff_is_exact_and_validator_is_fail_closed(self) -> None:
        manifest = json.loads(AGENT_MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(
            manifest,
            {
                "schema_version": 1,
                "repository": AGENT_REPOSITORY,
                "revision": AGENT_REVISION,
                "digest": AGENT_DIGEST,
                "immutable_ref": AGENT_REF,
            },
        )
        result = subprocess.run(
            ["python3", str(READ_MANIFEST), "--manifest", str(AGENT_MANIFEST)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["AGENT_IMAGE"], AGENT_REF)

        bad_values = (
            {**manifest, "immutable_ref": f"{AGENT_REPOSITORY}:latest"},
            {**manifest, "digest": "sha256:1234"},
            {**manifest, "revision": "main"},
            {**manifest, "extra": "not-allowed"},
        )
        for bad in bad_values:
            with self.subTest(bad=bad), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "manifest.json"
                path.write_text(json.dumps(bad), encoding="utf-8")
                rejected = subprocess.run(
                    ["python3", str(READ_MANIFEST), "--manifest", str(path)],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(rejected.returncode, 0)

    def test_dockerfile_bakes_public_provenance_without_changing_runtime_contract(self) -> None:
        text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertEqual(
            re.findall(r"(?m)^FROM\s+(.+)$", text),
            ["${ONEPASSWORD_CLI_IMAGE} AS onepassword_cli", "${AGENT_IMAGE}"],
        )
        for token in (
            f"ARG ONEPASSWORD_CLI_IMAGE={ONEPASSWORD_CLI_IMAGE}",
            "COPY --from=onepassword_cli --chmod=0755 /usr/local/bin/op /usr/local/bin/op",
            'test "$(/usr/local/bin/op --version)" = "2.39.0"',
            "ARG FLEET_GIT_SHA",
            "ARG FLEET_IMAGE_IDENTITY",
            "HERMES_FLEET_GIT_SHA=${FLEET_GIT_SHA}",
            "HERMES_FLEET_IMAGE_IDENTITY=${FLEET_IMAGE_IDENTITY}",
            "HERMES_FLEET_AGENT_IMAGE=${AGENT_IMAGE}",
            "HERMES_FLEET_ONEPASSWORD_CLI_IMAGE=${ONEPASSWORD_CLI_IMAGE}",
            "HERMES_FLEET_CLAUDE_CODE_VERSION=${CLAUDE_CODE_VERSION}",
            "COPY package.json package-lock.json /opt/coding-clis/",
            "npm ci --omit=dev --prefix /opt/coding-clis",
            "--ignore-scripts",
            "node /opt/coding-clis/node_modules/@anthropic-ai/claude-code/install.cjs",
            "DISABLE_AUTOUPDATER=1",
            'test "$(/usr/local/bin/claude --version)" = "${CLAUDE_CODE_VERSION} (Claude Code)"',
            "org.opencontainers.image.revision=\"${FLEET_GIT_SHA}\"",
            "onepassword_cli_image",
            "claude_code_version",
            "claude_agent_acp_version",
            "claude_acp_plugin_source",
            "claude_acp_plugin_revision",
            "COPY plugins/model-providers/claude-acp/ /opt/hermes/plugins/model-providers/claude-acp/",
            "COPY --chmod=0755 scripts/claude-acp-subscription /usr/local/bin/hermes-claude-acp-subscription",
            "ln -s /opt/coding-clis/node_modules/.bin/claude-agent-acp /usr/local/bin/claude-agent-acp",
            "/etc/hermes-fleet/image-provenance.json",
        ):
            self.assertIn(token, text)
        self.assertNotRegex(text, r"(?m)^\s*(?:ENTRYPOINT|CMD|USER)\b")

    def test_tooling_versions_match_build_and_workflow_inputs(self) -> None:
        package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        workflow = WORKFLOW.read_text(encoding="utf-8")
        for name, dependency in (
            ("CLAUDE_CODE_VERSION", "@anthropic-ai/claude-code"),
            ("CLAUDE_AGENT_ACP_VERSION", "@agentclientprotocol/claude-agent-acp"),
        ):
            version = package["dependencies"][dependency]
            self.assertIn(f"ARG {name}={version}\n", dockerfile)
            self.assertIn(f'{name}: "{version}"', workflow)

    def test_claude_code_dependency_is_exact_and_integrity_locked(self) -> None:
        package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
        lock = json.loads((ROOT / "package-lock.json").read_text(encoding="utf-8"))
        self.assertEqual(
            package["dependencies"],
            {
                "@agentclientprotocol/claude-agent-acp": CLAUDE_AGENT_ACP_VERSION,
                "@anthropic-ai/claude-code": CLAUDE_CODE_VERSION,
            },
        )
        self.assertTrue(package["private"])
        self.assertEqual(lock["packages"][""]["dependencies"], package["dependencies"])
        entry = lock["packages"]["node_modules/@anthropic-ai/claude-code"]
        self.assertEqual(entry["version"], CLAUDE_CODE_VERSION)
        self.assertEqual(entry["integrity"], CLAUDE_CODE_INTEGRITY)
        self.assertEqual(
            entry["resolved"],
            f"https://registry.npmjs.org/@anthropic-ai/claude-code/-/claude-code-{CLAUDE_CODE_VERSION}.tgz",
        )
        native = lock["packages"]["node_modules/@anthropic-ai/claude-code-linux-x64"]
        self.assertEqual(native["version"], CLAUDE_CODE_VERSION)
        self.assertEqual(native["integrity"], CLAUDE_CODE_LINUX_X64_INTEGRITY)
        self.assertEqual(native["os"], ["linux"])
        self.assertEqual(native["cpu"], ["x64"])
        self.assertEqual(
            native["resolved"],
            f"https://registry.npmjs.org/@anthropic-ai/claude-code-linux-x64/-/claude-code-linux-x64-{CLAUDE_CODE_VERSION}.tgz",
        )

    def test_claude_acp_adapter_and_plugin_are_exact_and_subscription_only(self) -> None:
        package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
        lock = json.loads((ROOT / "package-lock.json").read_text(encoding="utf-8"))
        self.assertEqual(
            package["dependencies"]["@agentclientprotocol/claude-agent-acp"],
            CLAUDE_AGENT_ACP_VERSION,
        )
        entry = lock["packages"]["node_modules/@agentclientprotocol/claude-agent-acp"]
        self.assertEqual(entry["version"], CLAUDE_AGENT_ACP_VERSION)
        self.assertEqual(entry["integrity"], CLAUDE_AGENT_ACP_INTEGRITY)
        self.assertEqual(
            entry["resolved"],
            "https://registry.npmjs.org/@agentclientprotocol/claude-agent-acp/-/"
            f"claude-agent-acp-{CLAUDE_AGENT_ACP_VERSION}.tgz",
        )

        plugin = (ROOT / "plugins/model-providers/claude-acp/__init__.py").read_text(encoding="utf-8")
        metadata = (ROOT / "plugins/model-providers/claude-acp/plugin.yaml").read_text(encoding="utf-8")
        provenance = json.loads(
            (ROOT / "plugins/model-providers/claude-acp/upstream.json").read_text(encoding="utf-8")
        )
        upstream_license_path = ROOT / "plugins/model-providers/claude-acp/UPSTREAM_LICENSE"
        upstream_license = upstream_license_path.read_bytes()
        upstream_license_sha256 = hashlib.sha256(upstream_license).hexdigest()
        self.assertEqual(
            upstream_license_sha256,
            "3522fd8996b0a9759df1daf7846dd7b3d4c4aa934e776089d2897dd049f4c689",
        )
        self.assertIn(b"Copyright (c) 2026 hermes-acp-agents contributors", upstream_license)
        self.assertEqual(
            provenance,
            {
                "license": "MIT",
                "license_file": "UPSTREAM_LICENSE",
                "license_sha256": "3522fd8996b0a9759df1daf7846dd7b3d4c4aa934e776089d2897dd049f4c689",
                "source_repository": CLAUDE_ACP_PLUGIN_SOURCE,
                "source_revision": CLAUDE_ACP_PLUGIN_REVISION,
                "upstream_init_sha256": "ff1c5505711c4562c755106f61c4074c6cd6d470ea573a60a937a7e73b9db56e",
                "upstream_plugin_yaml_sha256": "e1dd416295af66cefc1ba67812d359f02f9d1132f4a5f89a28d019797c6da00f",
            },
        )
        self.assertNotIn("agent.acp_agent_registry", plugin)
        self.assertIn('name="claude-acp"', plugin)
        self.assertIn('base_url="acp://claude"', plugin)
        tree = ast.parse(plugin)
        profile_call = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "ClaudeACPProfile"
        )
        profile_keywords = {keyword.arg: keyword.value for keyword in profile_call.keywords}
        self.assertEqual(ast.literal_eval(profile_keywords["env_vars"]), ("CLAUDE_CODE_OAUTH_TOKEN",))
        self.assertEqual(ast.literal_eval(profile_keywords["auth_type"]), "external_process")
        self.assertEqual(ast.literal_eval(profile_keywords["base_url"]), "acp://claude")
        self.assertEqual(
            ast.literal_eval(profile_keywords["process_command"]),
            "/usr/local/bin/hermes-claude-acp-subscription",
        )
        self.assertEqual(ast.literal_eval(profile_keywords["process_args"]), ())
        self.assertEqual(
            ast.literal_eval(profile_keywords["fallback_models"]),
            ("default", "opus[1m]", "sonnet", "haiku"),
        )
        launcher = (ROOT / "scripts/claude-acp-subscription").read_text(encoding="utf-8")
        for variable in (
            "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL",
            "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY",
            "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
        ):
            self.assertIn(variable, launcher)
        self.assertIn('exec /usr/local/bin/claude-agent-acp "$@"', launcher)
        self.assertIn(CLAUDE_ACP_PLUGIN_REVISION, plugin)
        self.assertIn("kind: model-provider", metadata)

    def test_release_workflow_is_manual_publish_and_least_privilege(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("pull_request:", text)
        self.assertIn("workflow_dispatch:", text)
        self.assertNotRegex(text, r"(?m)^\s*push:\s*$")
        self.assertIn("github.repository == 'mekenthompson/hermes-fleet-public'", text)
        self.assertIn("github.repository == 'mekenthompson/hermes-fleet'", text)
        self.assertIn("ghcr.io/mekenthompson/hermes-fleet-public", text)
        self.assertIn("github.ref == 'refs/heads/main'", text)
        self.assertIn("environment: fleet-image-publish", text)
        self.assertIn("platforms: linux/amd64", text)
        self.assertIn("packages: read", text.split("\n  publish:\n", 1)[0])
        self.assertNotIn("packages: write", text.split("\n  publish:\n", 1)[0])
        self.assertRegex(
            text,
            r"(?s)permissions:\n  contents: read.*?publish:.*?permissions:\n      contents: read\n      actions: read\n      packages: write\n      id-token: write\n      attestations: write",
        )
        self.assertNotIn("secrets.", text)
        for uses in re.findall(r"(?m)^\s*-?\s*uses:\s*([^\s#]+)", text):
            self.assertRegex(uses, r"^[^@]+@[0-9a-f]{40}$")

    def test_publish_requires_a_successful_ci_run_for_this_exact_main_sha(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("Verify exact main CI gate", text)
        self.assertIn("actions: read", text)
        self.assertIn("scripts/verify-exact-main-ci.py", text)
        self.assertIn('--workflow "ci.yml"', text)
        self.assertIn('--workflow-path ".github/workflows/ci.yml"', text)
        self.assertNotIn("needs: preflight", text)
        publish = text.split("\n  publish:\n", 1)[1]
        self.assertLess(publish.index("Verify exact main CI gate"), publish.index("Build and scan exact publish candidate"))

    def test_publish_checks_out_the_gate_script_before_running_it(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        publish = text.split("\n  publish:\n", 1)[1]
        self.assertLess(publish.index("Checkout exact pushed commit"), publish.index("Verify exact main CI gate"))
        self.assertIn("persist-credentials: false", publish.split("Verify exact main CI gate", 1)[0])
        self.assertLess(publish.index("Verify exact main CI gate"), publish.index("Log in to GHCR"))

    def test_preflight_consumes_handoff_and_verifies_runtime_isolation(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        for token in (
            "scripts/read-agent-image-manifest.py",
            "release/agent-image-manifest.json",
            "AGENT_IMAGE=${{ steps.agent.outputs.AGENT_IMAGE }}",
            "FLEET_GIT_SHA=${{ github.sha }}",
            "FLEET_IMAGE_IDENTITY=${{ env.IMAGE_REPOSITORY }}",
            "EXPECTED_AGENT_REVISION",
            "/etc/hermes/image-provenance.json",
            "/etc/hermes-fleet/image-provenance.json",
            "metadata.version(\"hermes-agent\")",
            "HERMES_FLEET_GIT_SHA",
            "HERMES_FLEET_IMAGE_IDENTITY",
            "HERMES_FLEET_AGENT_IMAGE",
            "HERMES_FLEET_ONEPASSWORD_CLI_IMAGE",
            "HERMES_FLEET_CLAUDE_CODE_VERSION",
            "HERMES_FLEET_CLAUDE_AGENT_ACP_VERSION",
            "HERMES_FLEET_CLAUDE_ACP_PLUGIN_REVISION",
            "CLAUDE_CODE_VERSION",
            "CLAUDE_AGENT_ACP_VERSION",
            "ONEPASSWORD_CLI_IMAGE",
            'subprocess.check_output(["/usr/local/bin/op", "--version"]',
            'subprocess.check_output(["/usr/local/bin/claude", "--version"]',
            'pathlib.Path("/opt/coding-clis/node_modules/@agentclientprotocol/claude-agent-acp/package.json")',
            "/opt/hermes/plugins/model-providers/claude-acp/__init__.py",
            'get_provider_profile("claude-acp")',
            "resolve_external_process_provider_credentials",
            "hermes-claude-acp-subscription",
            "docker.sock",
            "scripts/verify-inherited-runtime-config.py",
            "os.getuid() != 0",
            "docker/login-action@",
        ):
            self.assertIn(token, text)
        runtime = (ROOT / "scripts/verify-inherited-runtime-config.py").read_text(encoding="utf-8")
        self.assertIn("Config.User", runtime)
        self.assertIn("Config.Entrypoint", runtime)
        self.assertIn("Config.Cmd", runtime)
        self.assertEqual(
            text.count('subprocess.check_output(["/usr/local/bin/op", "--version"]'),
            2,
        )
        self.assertEqual(
            text.count('subprocess.check_output(["/usr/local/bin/claude", "--version"]'),
            2,
        )

    def test_exact_candidate_scan_and_publication_contract(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        publish = text.split("\n  publish:\n", 1)[1]
        self.assertNotIn("docker save", text)
        self.assertNotIn("docker load", text)
        self.assertNotIn("fleet-image.tar.gz", text)
        self.assertNotIn("actions/download-artifact@", publish)
        self.assertIn("Build and scan exact publish candidate", publish)
        self.assertIn("docker push \"$STAGING_IMAGE\"", publish)
        self.assertIn("docker buildx imagetools inspect --raw \"$IMMUTABLE_REF\"", publish)
        self.assertIn("docker buildx imagetools create", publish)
        self.assertIn("fleet-stage-${{ github.run_id }}-${{ github.run_attempt }}", text)
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertLess(dockerfile.index("ARG FLEET_GIT_SHA=development"), dockerfile.index("RUN python3 -c"))
        self.assertLess(dockerfile.index("RUN npm ci"), dockerfile.index("ARG FLEET_GIT_SHA=development"))
        self.assertLess(publish.index("Verify exact main CI gate"), publish.index("docker push \"$STAGING_IMAGE\""))

    def test_push_digest_extractor_accepts_real_docker_push_summary(self) -> None:
        digest = "sha256:" + "a" * 64
        push_log = (
            "The push refers to repository [ghcr.io/mekenthompson/hermes-fleet-public]\n"
            "a0b1c2d3e4f5: Pushed\n"
            "fleet-stage-123-1: digest: " + digest + " size: 742\n"
        )
        result = subprocess.run(
            ["python3", str(EXTRACT_PUSH_DIGEST)],
            cwd=ROOT,
            input=push_log,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, digest + "\n")

    def test_publish_false_dispatch_runs_read_only_bake_and_promotion_is_verified(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        preflight = text.split("\n  preflight:\n", 1)[1].split("\n  publish:\n", 1)[0]
        publish = text.split("\n  publish:\n", 1)[1]
        self.assertIn("github.event_name == 'workflow_dispatch'", preflight)
        self.assertIn("push: false", preflight)
        self.assertNotIn("packages: write", preflight)
        self.assertIn("scripts/extract-pushed-image-digest.py", publish)
        self.assertIn("--prefer-index=false", publish)
        self.assertIn("Verify promoted SHA tag resolves to staged digest", publish)
        self.assertIn("promoted SHA tag does not resolve to staged digest", publish)

    def test_publish_repeats_preflight_runtime_and_supply_chain_gates(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        publish = text.split("\n  publish:\n", 1)[1]
        for token in (
            'docker run --rm --entrypoint hermes "$TEST_IMAGE" --version',
            "Set up Python for SPDX schema validation",
            "scripts/verify-inherited-runtime-config.py",
            "scripts/validate-spdx-schema.py",
            "scripts/verify-trivy-vex.py",
        ):
            with self.subTest(token=token):
                self.assertIn(token, publish)

    def test_publication_fails_closed_on_remote_manifest_config_mismatch(self) -> None:
        publish = WORKFLOW.read_text(encoding="utf-8").split("\n  publish:\n", 1)[1]
        self.assertIn("Verify remote immutable manifest identity", publish)
        self.assertIn("remote-config-digest", publish)
        self.assertIn("local-config-digest", publish)
        self.assertIn("remote manifest config does not match scanned candidate", publish)
        self.assertIn("test -n \"$digest\"", publish)
        self.assertIn("test -n \"$remote_config_digest\"", publish)

    def test_critical_scan_remains_mandatory_and_vex_is_revalidated(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        scan = text.index("- name: Scan critical image vulnerabilities")
        gate = text.index("- name: Enforce critical findings and scoped VEX policy")
        publish_scan = text.rindex("- name: Scan critical image vulnerabilities")
        publish_verify = text.index("- name: Revalidate scoped VEX decision before publication")
        push = text.index("- name: Push unique staging candidate and capture pushed digest")
        self.assertLess(scan, gate)
        self.assertLess(publish_scan, publish_verify)
        self.assertLess(publish_verify, push)
        self.assertIn("aquasecurity/trivy-action@", text)
        self.assertIn("severity: CRITICAL", text)
        self.assertIn("scripts/verify-trivy-vex.py", text)
        self.assertIn("release/vex-exceptions.json", text)
        self.assertGreaterEqual(text.count("vex-evaluation.json"), 2)

    def test_release_preserves_full_evidence_and_attests_bounded_sbom(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        for token in (
            "fleet-image.spdx.json",
            "fleet-image.attestation.spdx.json",
            "scripts/compact-spdx-sbom.py",
            "--max-bytes 16777216",
            "trivy-image.json",
            "severity: CRITICAL",
            "actions/attest-build-provenance@",
            "actions/attest-sbom@",
            "sbom-path: fleet-image.attestation.spdx.json",
            "fleet-image-handoff-${{ github.sha }}",
        ):
            self.assertIn(token, text)

    def test_fleet_handoff_generator_binds_agent_and_fleet_identities(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "handoff.json"
            digest = "sha256:" + "a" * 64
            result = subprocess.run(
                [
                    "python3",
                    str(EMIT_MANIFEST),
                    "--repository",
                    "ghcr.io/mekenthompson/hermes-fleet-public",
                    "--revision",
                    "b" * 40,
                    "--digest",
                    digest,
                    "--agent-manifest",
                    str(AGENT_MANIFEST),
                    "--output",
                    str(output),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            data = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(data["schema_version"], 1)
            self.assertEqual(data["fleet"]["immutable_ref"], f"ghcr.io/mekenthompson/hermes-fleet-public@{digest}")
            self.assertEqual(data["fleet"]["source_repository"], "https://github.com/mekenthompson/hermes-fleet")
            self.assertEqual(data["agent"]["immutable_ref"], AGENT_REF)
            self.assertEqual(output.read_text(encoding="utf-8"), json.dumps(data, indent=2, sort_keys=True) + "\n")

    def test_compact_sbom_removes_file_surfaces_and_is_deterministic(self) -> None:
        document = {
            "spdxVersion": "SPDX-2.3",
            "dataLicense": "CC0-1.0",
            "SPDXID": "SPDXRef-DOCUMENT",
            "name": "valid fixture",
            "documentNamespace": "https://example.invalid/spdx/valid-fixture",
            "creationInfo": {
                "created": "2026-08-31T00:00:00Z",
                "creators": ["Tool: hermes-fleet-test"],
            },
            "documentDescribes": ["SPDXRef-Package-a", "SPDXRef-File-a"],
            "packages": [{
                "name": "a",
                "SPDXID": "SPDXRef-Package-a",
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": True,
                "hasFiles": ["SPDXRef-File-a"],
                "licenseInfoFromFiles": ["MIT"],
                "packageVerificationCode": {"packageVerificationCodeValue": "abc"},
            }],
            "files": [{
                "SPDXID": "SPDXRef-File-a",
                "fileName": "/a",
                "checksums": [{"algorithm": "SHA256", "checksumValue": "0" * 64}],
            }],
            "snippets": [{
                "SPDXID": "SPDXRef-Snippet-a",
                "name": "snippet a",
                "snippetFromFile": "SPDXRef-File-a",
                "ranges": [{
                    "startPointer": {"reference": "SPDXRef-File-a", "offset": 0},
                    "endPointer": {"reference": "SPDXRef-File-a", "offset": 1},
                }],
            }],
            "relationships": [
                {"spdxElementId": "SPDXRef-DOCUMENT", "relationshipType": "DESCRIBES", "relatedSpdxElement": "SPDXRef-Package-a"},
                {"spdxElementId": "SPDXRef-Package-a", "relationshipType": "CONTAINS", "relatedSpdxElement": "SPDXRef-File-a"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "full.json"
            first = Path(directory) / "first.json"
            second = Path(directory) / "second.json"
            source.write_text(json.dumps(document), encoding="utf-8")
            for output in (first, second):
                result = subprocess.run(
                    ["python3", str(COMPACT_SBOM), "--input", str(source), "--output", str(output)],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            compact = json.loads(first.read_text(encoding="utf-8"))
            self.assertEqual(compact["files"], [])
            self.assertEqual(compact["snippets"], [])
            self.assertEqual(compact["documentDescribes"], ["SPDXRef-Package-a"])
            self.assertEqual(len(compact["relationships"]), 1)
            package = compact["packages"][0]
            self.assertFalse(package["filesAnalyzed"])
            for field in ("hasFiles", "licenseInfoFromFiles", "packageVerificationCode"):
                self.assertNotIn(field, package)

    def test_compact_sbom_rejects_malformed_ids_and_relationships(self) -> None:
        cases = (
            {"SPDXID": "bad", "packages": [{"SPDXID": "SPDXRef-Package-a"}], "files": [], "relationships": []},
            {"SPDXID": "SPDXRef-DOCUMENT", "packages": [{"SPDXID": 123}], "files": [], "relationships": []},
            {"SPDXID": "SPDXRef-DOCUMENT", "packages": [{"SPDXID": "SPDXRef-Package-a"}], "files": [], "relationships": ["bad"]},
        )
        for document in cases:
            with self.subTest(document=document), tempfile.TemporaryDirectory() as directory:
                source = Path(directory) / "full.json"
                output = Path(directory) / "compact.json"
                source.write_text(json.dumps(document), encoding="utf-8")
                result = subprocess.run(
                    ["python3", str(COMPACT_SBOM), "--input", str(source), "--output", str(output)],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())
                self.assertNotIn("Traceback", result.stderr)

    def test_compact_sbom_rejects_required_field_and_enum_violations(self) -> None:
        valid = {
            "spdxVersion": "SPDX-2.3",
            "dataLicense": "CC0-1.0",
            "SPDXID": "SPDXRef-DOCUMENT",
            "name": "valid fixture",
            "documentNamespace": "https://example.invalid/spdx/semantic-fixture",
            "creationInfo": {
                "created": "2026-08-31T00:00:00Z",
                "creators": ["Tool: hermes-fleet-test"],
            },
            "packages": [{
                "SPDXID": "SPDXRef-Package-a",
                "name": "a",
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
            }],
            "files": [],
            "snippets": [],
            "relationships": [{
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relationshipType": "DESCRIBES",
                "relatedSpdxElement": "SPDXRef-Package-a",
            }],
        }
        cases = []
        missing_name = json.loads(json.dumps(valid))
        missing_name.pop("name")
        cases.append(missing_name)
        missing_creators = json.loads(json.dumps(valid))
        missing_creators["creationInfo"].pop("creators")
        cases.append(missing_creators)
        missing_download = json.loads(json.dumps(valid))
        missing_download["packages"][0].pop("downloadLocation")
        cases.append(missing_download)
        bad_relationship = json.loads(json.dumps(valid))
        bad_relationship["relationships"][0]["relationshipType"] = "NOT_A_SPDX_REL"
        cases.append(bad_relationship)
        wrong_version = json.loads(json.dumps(valid))
        wrong_version["spdxVersion"] = "SPDX-2.2"
        cases.append(wrong_version)

        for document in cases:
            with self.subTest(document=document), tempfile.TemporaryDirectory() as directory:
                source = Path(directory) / "full.json"
                output = Path(directory) / "compact.json"
                source.write_text(json.dumps(document), encoding="utf-8")
                result = subprocess.run(
                    ["python3", str(COMPACT_SBOM), "--input", str(source), "--output", str(output)],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())
                self.assertNotIn("Traceback", result.stderr)

    def test_workflow_validates_full_and_compact_documents_against_official_schema(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        for token in (
            "release/spdx-2.3-schema.json",
            "release/spdx-validation-requirements.txt",
            "--require-hashes",
            "scripts/validate-spdx-schema.py",
            "fleet-image.spdx.json",
            "fleet-image.attestation.spdx.json",
        ):
            self.assertIn(token, text)
        self.assertLess(text.index("Validate full and compact SPDX 2.3 documents"), text.index("Push unique staging candidate and capture pushed digest"))

    def test_release_documentation_states_boundaries(self) -> None:
        text = (ROOT / "docs/image-release.md").read_text(encoding="utf-8").lower()
        for token in (
            "exact agent digest",
            "without rebuilding",
            "full spdx",
            "package-level",
            "no production deployment",
            "fleet-image-publish",
            "rollback",
            "1password cli 2.39.0",
            "official image digest",
            "contains no credentials",
        ):
            self.assertIn(token, text)


if __name__ == "__main__":
    unittest.main()
