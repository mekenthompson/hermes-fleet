#!/usr/bin/env python3
"""Run the mounted origin proof with isolation and I/O denial installed before native imports."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-source", type=Path, required=True)
    args = parser.parse_args()
    source = args.agent_source.resolve(strict=True)
    manifest = json.loads((ROOT / "release/agent-image-manifest.json").read_text())
    actual = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(source), "status", "--porcelain"], check=True, capture_output=True, text=True).stdout.strip()
    if actual != manifest["revision"] or dirty:
        parser.error("Use the clean manifest-pinned native source")
    real_home = Path.home()
    with tempfile.TemporaryDirectory(prefix="linear-origin-proof-") as temporary:
        home = Path(temporary)
        os.environ.clear()
        os.environ.update(PATH=os.defpath, HOME=str(home), USERPROFILE=str(home), HERMES_HOME=str(home),
            XDG_CONFIG_HOME=str(home / "xdg-config"), XDG_CACHE_HOME=str(home / "xdg-cache"),
            CLAUDE_CONFIG_DIR=str(home / ".claude"), GH_CONFIG_DIR=str(home / ".config/gh"),
            TMPDIR=str(home), TZ="UTC", LANG="C.UTF-8", PYTHONDONTWRITEBYTECODE="1",
            HERMES_AGENT_SRC=str(source), HERMES_LINEAR_ORIGIN_TEST_ROOT=str(home))
        sys.dont_write_bytecode = True
        tempfile.tempdir = str(home)
        sys.path.insert(0, str(ROOT / "tests"))
        from linear_origin_guard import Guard
        guard = Guard(home, real_home)
        guard.install()
        # Packaged Hermes carries build provenance; a source checkout would probe git while
        # importing provider declarations. Use the already verified immutable fixture identity.
        sys.path.insert(0, str(source))
        from hermes_cli import version_info
        version_info._cached_version_info = version_info.VersionInfo("fixture", "fixture", 0, actual, None, "ci")
        suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_linear_origin_native.py")
        result = unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
        # A caught guard refusal is still a failed proof: it must never become green by fallback.
        if guard.violations:
            print("Forbidden operations attempted: " + json.dumps(guard.violations), file=sys.stderr)
        return 0 if result.wasSuccessful() and not result.skipped and not guard.violations else 1


if __name__ == "__main__":
    raise SystemExit(main())
