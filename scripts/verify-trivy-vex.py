#!/usr/bin/env python3
"""Fail-closed critical-vulnerability gate with exact VEX evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

IMAGE_RE = re.compile(r"^[a-z0-9./_-]+@sha256:[0-9a-f]{64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
EXPECTED_ADVISORY = {
    "vulnerability_id": "CVE-2026-56854",
    "go_advisory_id": "GO-2026-6303",
    "package": "golang.org/x/crypto",
    "installed_version": "v0.52.0",
    "fixed_version": "0.55.0",
    "target": "usr/local/bin/op",
    "target_class": "lang-pkgs",
    "target_type": "gobinary",
    "affected_symbols": [
        "golang.org/x/crypto/ssh.NewServerConn",
        "golang.org/x/crypto/ssh.(*connection).serverAuthenticate",
    ],
}
POLICY_KEYS = {"schema_version", "exceptions"}
EXCEPTION_KEYS = {
    "id",
    "status",
    "justification",
    "vulnerability_id",
    "go_advisory_id",
    "package",
    "installed_version",
    "fixed_version",
    "target",
    "target_class",
    "target_type",
    "component_image",
    "binary_sha256",
    "affected_symbols",
    "approved_by",
    "approved_at",
    "expires_at",
    "evidence",
}


def fail(message: str) -> None:
    raise SystemExit(message)


def load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"invalid {label}: {exc}")
    if not isinstance(value, dict):
        fail(f"{label} must be a JSON object")
    return value


def parse_time(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        fail(f"{label} must be an RFC3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        fail(f"invalid {label}: {exc}")
    if parsed.tzinfo != timezone.utc:
        fail(f"{label} must use UTC")
    return parsed


def validate_policy(policy: dict[str, Any], now: datetime) -> list[dict[str, Any]]:
    if set(policy) != POLICY_KEYS or policy.get("schema_version") != 1:
        fail("VEX policy has unknown fields or unsupported schema")
    exceptions = policy.get("exceptions")
    if not isinstance(exceptions, list):
        fail("VEX policy exceptions must be a list")
    if not exceptions:
        return []
    ids: set[str] = set()
    vulnerabilities: set[str] = set()
    validated: list[dict[str, Any]] = []
    for item in exceptions:
        if not isinstance(item, dict) or set(item) != EXCEPTION_KEYS:
            fail("VEX exception has missing or unknown fields")
        exception_id = item.get("id")
        if not isinstance(exception_id, str) or not re.fullmatch(r"[A-Z0-9-]+", exception_id):
            fail("VEX exception id is invalid")
        if exception_id in ids or item.get("vulnerability_id") in vulnerabilities:
            fail("duplicate VEX exception")
        ids.add(exception_id)
        vulnerabilities.add(item["vulnerability_id"])
        for key, expected in EXPECTED_ADVISORY.items():
            if item.get(key) != expected:
                fail(f"VEX {key} does not match the reviewed advisory")
        if item.get("status") != "not_affected":
            fail("VEX status must be not_affected")
        if item.get("justification") != "vulnerable_code_not_present":
            fail("VEX justification must be vulnerable_code_not_present")
        image = item.get("component_image")
        if not isinstance(image, str) or not IMAGE_RE.fullmatch(image):
            fail("VEX component image must be immutable")
        digest = item.get("binary_sha256")
        if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
            fail("VEX binary SHA-256 is invalid")
        if not isinstance(item.get("approved_by"), str) or not item["approved_by"].strip():
            fail("VEX approval identity is required")
        if not isinstance(item.get("evidence"), str) or len(item["evidence"].strip()) < 40:
            fail("VEX evidence is insufficient")
        approved = parse_time(item.get("approved_at"), "approved_at")
        expires = parse_time(item.get("expires_at"), "expires_at")
        if approved > now:
            fail("VEX approval is in the future")
        if expires <= now:
            fail("VEX exception is expired")
        if expires <= approved:
            fail("VEX expiry must follow approval")
        validated.append(item)
    return validated


def critical_findings(report: dict[str, Any]) -> list[dict[str, Any]]:
    if report.get("SchemaVersion") != 2 or not isinstance(report.get("Results"), list):
        fail("unsupported or malformed Trivy report")
    findings: list[dict[str, Any]] = []
    for result in report["Results"]:
        if not isinstance(result, dict):
            fail("malformed Trivy result")
        vulnerabilities = result.get("Vulnerabilities") or []
        if not isinstance(vulnerabilities, list):
            fail("malformed Trivy vulnerability list")
        for vulnerability in vulnerabilities:
            if not isinstance(vulnerability, dict):
                fail("malformed Trivy vulnerability")
            if vulnerability.get("Severity") != "CRITICAL":
                continue
            findings.append({
                "vulnerability_id": vulnerability.get("VulnerabilityID"),
                "package": vulnerability.get("PkgName"),
                "installed_version": vulnerability.get("InstalledVersion"),
                "fixed_version": vulnerability.get("FixedVersion"),
                "severity": vulnerability.get("Severity"),
                "status": vulnerability.get("Status"),
                "target": result.get("Target"),
                "target_class": result.get("Class"),
                "target_type": result.get("Type"),
            })
    return findings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trivy", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--component-image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--now", help="RFC3339 UTC time; tests and deterministic replay only")
    args = parser.parse_args()

    now = parse_time(args.now, "now") if args.now else datetime.now(timezone.utc)
    policy_bytes = args.policy.read_bytes()
    report_bytes = args.trivy.read_bytes()
    binary = args.binary.read_bytes()
    policy = load_object(args.policy, "VEX policy")
    report = load_object(args.trivy, "Trivy report")
    exceptions = validate_policy(policy, now)
    binary_sha256 = hashlib.sha256(binary).hexdigest()
    for item in exceptions:
        if args.component_image != item["component_image"]:
            fail("1Password component image does not match VEX")
        if binary_sha256 != item["binary_sha256"]:
            fail("op binary SHA-256 does not match VEX")
        present = [
            symbol
            for symbol in item["affected_symbols"]
            if symbol.encode("ascii") in binary
        ]
        if present:
            fail(f"affected symbol present in op binary: {present[0]}")

    by_vulnerability = {item["vulnerability_id"]: item for item in exceptions}
    findings = critical_findings(report)
    excepted: list[str] = []
    seen_vulnerabilities: set[str] = set()

    for finding in findings:
        vulnerability_id = finding["vulnerability_id"]
        if vulnerability_id in seen_vulnerabilities:
            fail(f"duplicate critical vulnerability finding: {vulnerability_id}")
        seen_vulnerabilities.add(vulnerability_id)
        item = by_vulnerability.get(vulnerability_id)
        if item is None:
            fail(f"unexcepted critical vulnerability: {vulnerability_id}")
        expected_finding = {
            "vulnerability_id": item["vulnerability_id"],
            "package": item["package"],
            "installed_version": item["installed_version"],
            "fixed_version": item["fixed_version"],
            "severity": "CRITICAL",
            "status": "fixed",
            "target": item["target"],
            "target_class": item["target_class"],
            "target_type": item["target_type"],
        }
        if finding != expected_finding:
            fail(f"critical finding does not exactly match VEX: {vulnerability_id}")
        excepted.append(vulnerability_id)

    evaluation = {
        "schema_version": 1,
        "decision": "pass",
        "evaluated_at": now.isoformat().replace("+00:00", "Z"),
        "component_image": args.component_image,
        "binary_sha256": hashlib.sha256(binary).hexdigest(),
        "policy_sha256": hashlib.sha256(policy_bytes).hexdigest(),
        "trivy_report_sha256": hashlib.sha256(report_bytes).hexdigest(),
        "critical_findings": len(findings),
        "excepted_vulnerabilities": sorted(excepted),
    }
    args.output.write_text(json.dumps(evaluation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(evaluation, sort_keys=True))


if __name__ == "__main__":
    main()
