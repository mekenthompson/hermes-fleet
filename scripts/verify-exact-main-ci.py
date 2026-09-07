#!/usr/bin/env python3
"""Fail closed unless the newest matching main CI run for this SHA succeeded.

With ``--wait`` the script polls until that newest run has completed (or the
timeout elapses) before applying the same rule, so a publication triggered by
the push itself can overlap its build with the CI run it depends on.
"""
from __future__ import annotations
import argparse, json, subprocess, sys, time
from collections.abc import Callable, Sequence
from typing import Any

def latest_exact_main_ci_run(runs: Sequence[dict[str, Any]], sha: str, workflow_path: str) -> dict[str, Any] | None:
    matching = [run for run in runs if run.get("event") == "push" and run.get("head_sha") == sha and run.get("head_branch") == "main" and run.get("path") == workflow_path]
    if not matching:
        return None
    return max(matching, key=lambda run: (str(run.get("created_at", "")), int(run.get("id", 0))))

def run_is_green(run: dict[str, Any] | None) -> bool:
    return run is not None and run.get("status") == "completed" and run.get("conclusion") == "success"

def latest_exact_main_ci_is_green(runs: Sequence[dict[str, Any]], sha: str, workflow_path: str) -> bool:
    return run_is_green(latest_exact_main_ci_run(runs, sha, workflow_path))

def wait_for_exact_main_ci(
    fetch: Callable[[], Sequence[dict[str, Any]]], sha: str, workflow_path: str, *,
    timeout_seconds: float, interval_seconds: float,
    clock: Callable[[], float] | None = None, sleep: Callable[[float], None] | None = None,
    log: Callable[[str], None] = lambda message: print(message, file=sys.stderr),
) -> bool:
    """Poll until the newest matching run has completed; fail closed on timeout.

    Each poll re-selects the newest run, so a run created after polling began
    supersedes any older run for the same SHA. ``clock`` and ``sleep`` default
    to ``time.monotonic`` / ``time.sleep`` and are injectable for tests.
    """
    clock = clock or time.monotonic
    sleep = sleep or time.sleep
    deadline = clock() + timeout_seconds
    while True:
        latest = latest_exact_main_ci_run(fetch(), sha, workflow_path)
        if latest is not None and latest.get("status") == "completed":
            return run_is_green(latest)
        state = "absent" if latest is None else str(latest.get("status"))
        remaining = deadline - clock()
        if remaining <= 0:
            log(f"exact-SHA main CI run still {state} after timeout")
            return False
        log(f"exact-SHA main CI run is {state}; retrying in {interval_seconds:g}s ({remaining:.0f}s left)")
        sleep(min(interval_seconds, remaining))

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
    parser.add_argument("--wait", action="store_true", help="poll until the newest exact-SHA run has completed")
    parser.add_argument("--timeout-minutes", type=float, default=30.0)
    parser.add_argument("--interval-seconds", type=float, default=20.0)
    args = parser.parse_args()
    fetch = lambda: fetch_runs(args.repository, args.workflow, args.sha)
    if args.wait:
        green = wait_for_exact_main_ci(fetch, args.sha, args.workflow_path, timeout_seconds=args.timeout_minutes * 60, interval_seconds=args.interval_seconds)
    else:
        green = latest_exact_main_ci_is_green(fetch(), args.sha, args.workflow_path)
    if not green:
        raise SystemExit("latest exact-SHA main CI run is absent, incomplete, or not successful")
    return 0
if __name__ == "__main__": raise SystemExit(main())
