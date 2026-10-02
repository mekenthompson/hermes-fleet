from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GATE = ROOT / "scripts/verify-trivy-vex.py"
IMAGE = "docker.io/1password/op@sha256:515a00a06792c8a0aaa46a442b2cfc106e2dcef6ab25f6c367cd575f6172952a"


class TrivyVexGateTests(unittest.TestCase):
    def fixture(self, directory: Path) -> tuple[Path, Path, Path]:
        binary = directory / "op"
        binary.write_bytes(b"ELF fixture without affected ssh server symbols")
        digest = hashlib.sha256(binary.read_bytes()).hexdigest()
        policy = {
            "schema_version": 1,
            "exceptions": [{
                "id": "KEN-275-GO-2026-6303",
                "status": "not_affected",
                "justification": "vulnerable_code_not_present",
                "vulnerability_id": "CVE-2026-56854",
                "go_advisory_id": "GO-2026-6303",
                "package": "golang.org/x/crypto",
                "installed_version": "v0.52.0",
                "fixed_version": "0.55.0",
                "target": "usr/local/bin/op",
                "target_class": "lang-pkgs",
                "target_type": "gobinary",
                "component_image": IMAGE,
                "binary_sha256": digest,
                "affected_symbols": [
                    "golang.org/x/crypto/ssh.NewServerConn",
                    "golang.org/x/crypto/ssh.(*connection).serverAuthenticate",
                ],
                "approved_by": "Ken Thompson (CPO)",
                "approved_at": "2026-09-01T13:31:49Z",
                "expires_at": "2026-10-01T00:00:00Z",
                "evidence": "Exact binary lacks both affected symbols; Fleet uses op read and does not run an SSH server.",
            }],
        }
        report = {
            "SchemaVersion": 2,
            "Results": [{
                "Target": "usr/local/bin/op",
                "Class": "lang-pkgs",
                "Type": "gobinary",
                "Vulnerabilities": [{
                    "VulnerabilityID": "CVE-2026-56854",
                    "PkgName": "golang.org/x/crypto",
                    "InstalledVersion": "v0.52.0",
                    "FixedVersion": "0.55.0",
                    "Severity": "CRITICAL",
                    "Status": "fixed",
                }],
            }],
        }
        policy_path = directory / "policy.json"
        report_path = directory / "trivy.json"
        output_path = directory / "evaluation.json"
        policy_path.write_text(json.dumps(policy), encoding="utf-8")
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return policy_path, report_path, output_path

    def run_gate(self, policy: Path, report: Path, binary: Path, output: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3", str(GATE),
                "--trivy", str(report),
                "--policy", str(policy),
                "--binary", str(binary),
                "--component-image", IMAGE,
                "--output", str(output),
                "--now", "2026-09-02T00:00:00Z",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_exact_reviewed_non_reachable_finding_passes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            policy, report, output = self.fixture(directory)
            result = self.run_gate(policy, report, directory / "op", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            evaluation = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(evaluation["decision"], "pass")
            self.assertEqual(evaluation["excepted_vulnerabilities"], ["CVE-2026-56854"])

    def test_empty_report_still_binds_candidate_binary_and_component(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            policy, report, output = self.fixture(directory)
            report.write_text(json.dumps({"SchemaVersion": 2, "Results": []}), encoding="utf-8")
            binary = directory / "op"
            binary.write_bytes(b"tampered executable")
            result = self.run_gate(policy, report, binary, output)
            self.assertNotEqual(result.returncode, 0)

    def test_duplicate_matching_finding_fails(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            policy, report, output = self.fixture(directory)
            data = json.loads(report.read_text(encoding="utf-8"))
            finding = dict(data["Results"][0]["Vulnerabilities"][0])
            data["Results"][0]["Vulnerabilities"].append(finding)
            report.write_text(json.dumps(data), encoding="utf-8")
            result = self.run_gate(policy, report, directory / "op", output)
            self.assertNotEqual(result.returncode, 0)

    def test_every_bound_value_and_expiry_fail_closed(self) -> None:
        mutations = {
            "vulnerability_id": "CVE-2026-00000",
            "package": "example.invalid/crypto",
            "installed_version": "v0.51.0",
            "fixed_version": "0.54.0",
            "target": "usr/bin/op",
            "target_class": "os-pkgs",
            "target_type": "library",
            "component_image": "docker.io/1password/op@sha256:" + "0" * 64,
            "binary_sha256": "0" * 64,
            "expires_at": "2026-09-01T00:00:00Z",
            "status": "affected",
            "justification": "component_not_present",
        }
        for key, value in mutations.items():
            with self.subTest(key=key), tempfile.TemporaryDirectory() as raw:
                directory = Path(raw)
                policy, report, output = self.fixture(directory)
                data = json.loads(policy.read_text(encoding="utf-8"))
                data["exceptions"][0][key] = value
                policy.write_text(json.dumps(data), encoding="utf-8")
                result = self.run_gate(policy, report, directory / "op", output)
                self.assertNotEqual(result.returncode, 0)

    def test_affected_symbol_presence_fails(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            policy, report, output = self.fixture(directory)
            binary = directory / "op"
            binary.write_bytes(binary.read_bytes() + b" golang.org/x/crypto/ssh.NewServerConn")
            data = json.loads(policy.read_text(encoding="utf-8"))
            data["exceptions"][0]["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
            policy.write_text(json.dumps(data), encoding="utf-8")
            result = self.run_gate(policy, report, binary, output)
            self.assertNotEqual(result.returncode, 0)

    def test_any_additional_critical_finding_fails(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            policy, report, output = self.fixture(directory)
            data = json.loads(report.read_text(encoding="utf-8"))
            extra = dict(data["Results"][0]["Vulnerabilities"][0])
            extra["VulnerabilityID"] = "CVE-2026-99999"
            data["Results"][0]["Vulnerabilities"].append(extra)
            report.write_text(json.dumps(data), encoding="utf-8")
            result = self.run_gate(policy, report, directory / "op", output)
            self.assertNotEqual(result.returncode, 0)

    def test_empty_policy_passes_only_without_critical_findings(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            policy, report, output = self.fixture(directory)
            data = json.loads(policy.read_text(encoding="utf-8"))
            data["exceptions"] = []
            policy.write_text(json.dumps(data), encoding="utf-8")
            report.write_text(json.dumps({"SchemaVersion": 2, "Results": []}), encoding="utf-8")
            result = self.run_gate(policy, report, directory / "op", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            evaluation = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(evaluation["critical_findings"], 0)
            self.assertEqual(evaluation["excepted_vulnerabilities"], [])

        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            policy, report, output = self.fixture(directory)
            data = json.loads(policy.read_text(encoding="utf-8"))
            data["exceptions"] = []
            policy.write_text(json.dumps(data), encoding="utf-8")
            result = self.run_gate(policy, report, directory / "op", output)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unexcepted critical vulnerability", result.stderr)

    def test_unknown_policy_fields_and_duplicate_exceptions_fail(self) -> None:
        for mutation in ("unknown", "duplicate"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as raw:
                directory = Path(raw)
                policy, report, output = self.fixture(directory)
                data = json.loads(policy.read_text(encoding="utf-8"))
                if mutation == "unknown":
                    data["exceptions"][0]["unexpected"] = True
                else:
                    data["exceptions"].append(dict(data["exceptions"][0]))
                policy.write_text(json.dumps(data), encoding="utf-8")
                result = self.run_gate(policy, report, directory / "op", output)
                self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
