#!/usr/bin/env python3
"""Run the real Agent-backed Linear scenarios, failing on absent or skipped proof."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from importlib import util


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts/read-agent-image-manifest.py"


def expected_revision(manifest: Path) -> str:
    spec = util.spec_from_file_location("agent_image_manifest", VALIDATOR)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load Agent manifest validator")
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.load_manifest(manifest)["revision"]


def run(agent_source: Path, manifest: Path, tests_dir: Path) -> int:
    if not agent_source.is_dir():
        print(f"Agent source missing: {agent_source}", file=sys.stderr)
        return 1
    try:
        actual = subprocess.run(
            ["git", "-C", str(agent_source), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
        expected = expected_revision(manifest)
        dirty = subprocess.run(
            ["git", "-C", str(agent_source), "status", "--porcelain", "--untracked-files=normal"],
            check=True, capture_output=True, text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"Agent source revision unavailable: {exc}", file=sys.stderr)
        return 1
    if actual != expected:
        print(f"Agent source revision mismatch: expected {expected}, got {actual}", file=sys.stderr)
        return 1

    if dirty.strip():
        print("Agent source is dirty; use a clean manifest-pinned checkout", file=sys.stderr)
        return 1

    inherited = dict(os.environ)
    with tempfile.TemporaryDirectory(prefix="linear-scenario-run-") as home:
        isolated = {
            "PATH": os.defpath, "HOME": home, "HERMES_HOME": home,
            "XDG_CONFIG_HOME": home, "XDG_CACHE_HOME": home,
            "TMPDIR": home, "TZ": "UTC", "LANG": "C.UTF-8",
            "HERMES_AGENT_SRC": str(agent_source.resolve()),
        }
        try:
            os.environ.clear()
            os.environ.update(isolated)
            return run_suite(tests_dir)
        finally:
            os.environ.clear()
            os.environ.update(inherited)


def run_suite(tests_dir: Path) -> int:
    suite = unittest.defaultTestLoader.discover(
        start_dir=str(tests_dir), pattern="test_linear_kanban_scenarios.py",
    )
    if suite.countTestCases() == 0:
        print("Linear scenario suite has zero tests", file=sys.stderr)
        return 1
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if result.skipped:
        print(f"Linear scenario suite skipped={len(result.skipped)}; required lane failed", file=sys.stderr)
        return 1
    return 0 if result.wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-source", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=ROOT / "release/agent-image-manifest.json")
    parser.add_argument("--tests-dir", type=Path, default=ROOT / "tests")
    args = parser.parse_args()
    return run(args.agent_source, args.manifest, args.tests_dir)


if __name__ == "__main__":
    raise SystemExit(main())
