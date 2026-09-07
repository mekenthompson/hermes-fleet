#!/usr/bin/env python3
"""Fail closed unless the newest matching main CI run for this SHA succeeded."""
from __future__ import annotations
import argparse, json, subprocess
from collections.abc import Callable, Sequence
from typing import Any

def latest_exact_main_ci_is_green(runs: Sequence[dict[str, Any]], sha: str, workflow_path: str) -> bool:
    matching = [run for run in runs if run.get("event") == "push" and run.get("head_sha") == sha and run.get("head_branch") == "main" and run.get("path") == workflow_path]
    if not matching:
        return False
    latest = max(matching, key=lambda run: (str(run.get("created_at", "")), int(run.get("id", 0))))
    return latest.get("status") == "completed" and latest.get("conclusion") == "success"

def fetch_runs(repository: str, workflow: str, sha: str, call: Callable[..., Any] = subprocess.run) -> list[dict[str, Any]]:
    result = call(["gh", "api", f"repos/{repository}/actions/workflows/{workflow}/runs?event=push&head_sha={sha}&per_page=100"], text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "GitHub Actions API request failed")
    payload = json.loads(result.stdout)
    runs = payload.get("workflow_runs")
    if not isinstance(runs, list) or not all(isinstance(run, dict) for run in runs):
        raise RuntimeError("GitHub Actions API response lacks workflow_runs")
    return runs

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True); parser.add_argument("--workflow", required=True)
    parser.add_argument("--sha", required=True); parser.add_argument("--workflow-path", required=True)
    args = parser.parse_args()
    if not latest_exact_main_ci_is_green(fetch_runs(args.repository, args.workflow, args.sha), args.sha, args.workflow_path):
        raise SystemExit("latest exact-SHA main CI run is absent, incomplete, or not successful")
    return 0
if __name__ == "__main__": raise SystemExit(main())
