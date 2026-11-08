"""The scanner download helper uses an asset only when its bytes match the pin.

Runs scripts/fetch-pinned-release-asset.sh with a stub
``curl`` on PATH, so no network is touched.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/fetch-pinned-release-asset.sh"
PAYLOAD = b"pinned scanner archive\n"
API = "https://api.github.com/repos/anchore/syft/releases/assets/532610151"
FALLBACK = "https://github.com/anchore/syft/releases/download/v1.51.1/syft_1.51.1_linux_amd64.tar.gz"

# Stub curl: records argv, then runs BODY with $url and $out parsed.
CURL = """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$CALLS"
out=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in
    --output) out=$2; shift 2 ;;
    --header|--retry|--retry-delay|--retry-max-time|--connect-timeout|--max-time) shift 2 ;;
    -*) shift ;;
    *) url=$1; shift ;;
  esac
done
"""


class FetchPinnedReleaseAssetTests(unittest.TestCase):
    def run_helper(self, body: str, *, sha256: str | None = None, size: int | None = None,
                   fallback: str = FALLBACK, token: str | None = "workflow-token",
                   failing_mv: bool = False):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            bindir = tmpdir / "bin"
            bindir.mkdir()
            src = tmpdir / "payload"
            src.write_bytes(PAYLOAD)
            curl = bindir / "curl"
            curl.write_text(CURL + body, encoding="utf-8")
            curl.chmod(0o755)
            if failing_mv:
                mv = bindir / "mv"
                mv.write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
                mv.chmod(0o755)
            calls = tmpdir / "calls"
            out = tmpdir / "asset.tar.gz"
            env = {k: v for k, v in os.environ.items() if k not in {"GH_TOKEN", "GITHUB_API_URL"}}
            env.update(PATH=f"{bindir}{os.pathsep}{env['PATH']}", CALLS=str(calls), SRC=str(src))
            if token is not None:
                env["GH_TOKEN"] = token
            proc = subprocess.run(
                ["bash", str(SCRIPT), "anchore/syft", "532610151",
                 str(len(PAYLOAD) if size is None else size),
                 sha256 or hashlib.sha256(PAYLOAD).hexdigest(),
                 fallback, str(out)],
                env=env, capture_output=True, text=True, check=False,
            )
            lines = calls.read_text(encoding="utf-8").splitlines() if calls.exists() else []
            return proc, out.exists() and out.read_bytes() == PAYLOAD, out.exists(), \
                Path(f"{out}.partial").exists(), lines

    def test_rest_asset_api_is_the_primary_route(self) -> None:
        proc, ok, _, _, calls = self.run_helper('cp "$SRC" "$out"\n')
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(ok)
        self.assertEqual(len(calls), 1)
        self.assertIn(API, calls[0])
        self.assertIn("Accept: application/octet-stream", calls[0])
        self.assertIn("Authorization: Bearer workflow-token", calls[0])
        self.assertIn("--retry-all-errors", calls[0])

    def test_unauthenticated_api_request_when_no_token(self) -> None:
        proc, ok, _, _, calls = self.run_helper('cp "$SRC" "$out"\n', token=None)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(ok)
        self.assertNotIn("Authorization", calls[0])

    def test_falls_back_to_release_download_when_api_fails(self) -> None:
        body = 'case "$url" in https://api.github.com/*) exit 22 ;; esac\ncp "$SRC" "$out"\n'
        proc, ok, _, _, calls = self.run_helper(body)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(ok)
        self.assertEqual(len(calls), 2)
        self.assertIn(FALLBACK, calls[1])

    def test_falls_back_when_api_bytes_do_not_match(self) -> None:
        body = ('case "$url" in https://api.github.com/*) printf tampered > "$out"; exit 0 ;; esac\n'
                'cp "$SRC" "$out"\n')
        proc, ok, _, _, calls = self.run_helper(body)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(ok)
        self.assertEqual(len(calls), 2)

    def test_digest_mismatch_on_every_route_fails_closed(self) -> None:
        proc, _, exists, partial, calls = self.run_helper('cp "$SRC" "$out"\n', sha256="0" * 64)
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(exists)
        self.assertFalse(partial)
        self.assertEqual(len(calls), 2)

    def test_size_mismatch_fails_closed(self) -> None:
        proc, _, exists, partial, _ = self.run_helper('cp "$SRC" "$out"\n', size=len(PAYLOAD) + 1)
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(exists)
        self.assertFalse(partial)

    def test_both_routes_down_fails_closed(self) -> None:
        proc, _, exists, _, calls = self.run_helper("exit 22\n")
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(exists)
        self.assertEqual(len(calls), 2)
        self.assertIn("either route", proc.stderr)

    def test_failed_move_of_a_verified_download_is_a_hard_failure(self) -> None:
        proc, _, exists, partial, calls = self.run_helper('cp "$SRC" "$out"\n', failing_mv=True)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertFalse(exists)
        self.assertFalse(partial)
        self.assertNotIn("Fetched", proc.stdout + proc.stderr)
        self.assertIn("Could not move", proc.stderr)
        # A local filesystem fault is not retried through the fallback route.
        self.assertEqual(len(calls), 1)

    def test_rejects_fallback_outside_the_pinned_repository(self) -> None:
        proc, _, exists, _, calls = self.run_helper(
            'cp "$SRC" "$out"\n', fallback="https://example.com/syft.tar.gz")
        self.assertEqual(proc.returncode, 2)
        self.assertFalse(exists)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
