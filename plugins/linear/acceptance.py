"""GitHub PR acceptance for Linear closeouts, around core's exact-head verifier.

Core's ``collect_acceptance`` reads the Checks API. A fine-grained token without
``checks:read`` is refused there (HTTP 403) although it can read pull requests,
commit statuses and Actions runs. Then, and only then, the required checks are
proved from GitHub Actions jobs instead, with the safeguards of hermes-agent
#133 / a3faa265: every requirement pinned to the GitHub Actions app, the exact
head SHA, the latest attempt of trusted runs (pull_request, push or merge_group)
whose head repository is the PR's own repository and whose workflow file exists
on the base branch, complete pagination, and a final PR re-read that still shows
the same head and base (merged is accepted, closed without merging is not).
Anything this path cannot decide stays unverified; it is never accepted.

``unverified`` separates "GitHub could not be asked" (refused, unreachable,
still running) from "GitHub answered no" (a check failed, the PR was closed, no
evidence). Only the first is worth asking again later.

Network work only; nothing here touches the store. gh's stderr is never kept.
"""
from __future__ import annotations

import json
import re
import subprocess
from typing import Any, Callable
from urllib.parse import quote

ACTIONS_APP_ID = 15368  # GitHub's own Actions app. Unpinned and third-party requirements are not this.
TRUSTED_EVENTS = frozenset({"merge_group", "pull_request", "push"})
# Receipt classes that mean GitHub could not be asked, not that it said no.
UNVERIFIED = frozenset({"auth", "infra", "pending"})
# Per-check classes that are a definite answer about a check, not a failed read.
DEFINITE_CHECK = frozenset({"failure", "infra"})
_PR = re.compile(r"https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/([1-9][0-9]*)\Z")
_SHA = re.compile(r"[0-9a-f]{40}\Z")
# Core's auth receipt names the refused endpoint; only a refused check-runs read opens the fallback.
_CHECKS_DENIED = re.compile(
    r"\(HTTP 40[13] on repos/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/commits/([0-9a-f]{40})/check-runs\)")


class Denied(Exception):
    """GitHub refused the read (401/403/404) or gh has no login. ``status`` is the HTTP code, if any."""

    def __init__(self, message: str, status: str | None = None) -> None:
        super().__init__(message)
        self.status = status


class Unavailable(Exception):
    """GitHub did not answer usefully: network, rate limit, timeout or a malformed reply."""


def gh_api(endpoint: str, *, paginate: bool = False) -> Any:
    """One ``gh api`` read as the ambient login (the same login core's verifier used)."""
    path = endpoint.split("?")[0]
    command = ["gh", "api", endpoint, "--hostname", "github.com"]
    if paginate:
        command += ["--paginate", "--slurp"]
    try:
        result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                                encoding="utf-8", errors="replace", timeout=30, check=True)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr or ""
        if "rate limit" in stderr.lower():
            raise Unavailable(f"GitHub rate limit on {path}") from None
        denied = re.search(r"HTTP (40[134])", stderr)
        if denied:
            raise Denied(f"HTTP {denied[1]} on {path}", denied[1]) from None
        if exc.returncode == 4:  # gh's authentication-required exit
            raise Denied(f"gh has no login for {path}") from None
        raise Unavailable(f"gh api failed on {path}") from None
    except (OSError, subprocess.SubprocessError):
        raise Unavailable(f"gh api unavailable for {path}") from None
    try:
        return json.loads(result.stdout)
    except ValueError:
        raise Unavailable(f"GitHub returned unreadable JSON for {path}") from None


def checks_read_denied(receipt: Any, url: str) -> bool:
    """Core refused only at the check-runs read for this PR's recorded head."""
    if not isinstance(receipt, dict) or receipt.get("ok") is True or receipt.get("classification") != "auth":
        return False
    denied, pr = _CHECKS_DENIED.search(str(receipt.get("detail") or "")), _PR.fullmatch(url or "")
    required = receipt.get("required")
    return bool(denied and pr and denied[1] == pr[1] and denied[2] == receipt.get("head_sha")
                and isinstance(required, list) and required)


def unverified(receipt: Any) -> bool:
    """GitHub could not be asked (refused, unreachable, still running), as opposed to answering no."""
    if not isinstance(receipt, dict) or receipt.get("ok") is True or receipt.get("classification") not in UNVERIFIED:
        return False
    checks = receipt.get("checks") if isinstance(receipt.get("checks"), list) else []
    return not any(isinstance(check, dict) and check.get("classification") in DEFINITE_CHECK for check in checks)


def settle(receipt: Any, url: str, *, gh: Callable[..., Any] | None = None) -> Any:
    """Core's receipt, completed from Actions when only the Checks read was refused.

    An unverified receipt is also told apart from a PR closed without merging,
    which is a definite answer (core reports it as unavailable evidence).
    """
    gh = gh or gh_api
    if checks_read_denied(receipt, url):
        return actions_fallback(receipt, url, gh=gh)
    if unverified(receipt):
        return _closed_unmerged(receipt, url, gh)
    return receipt


def _closed_unmerged(receipt: dict, url: str, gh: Callable[..., Any]) -> dict:
    match = _PR.fullmatch(url or "")
    if not match:
        return receipt
    try:
        pr = gh(f"repos/{match[1]}/pulls/{int(match[2])}")
        closed = pr["state"] == "closed" and not pr.get("merged")
    except (Denied, Unavailable, KeyError, TypeError, ValueError):
        return receipt
    if not closed:
        return receipt
    return {**receipt, "ok": False, "classification": "closed",
            "detail": "the pull request was closed without being merged"}


def _rows(pages: Any, key: str) -> list:
    if not isinstance(pages, list) or not pages or not isinstance(pages[0], dict):
        raise ValueError(f"incomplete {key} pagination")
    total = pages[0].get("total_count")
    rows = [row for page in pages for row in page[key]]
    if not isinstance(total, int) or len({row["id"] for row in rows}) != total:
        raise ValueError(f"incomplete {key} pagination")
    return rows


def _trusted_workflow_path(path: Any) -> bool:
    return (isinstance(path, str) and path.startswith(".github/workflows/")
            and ".." not in path.split("/") and (path.endswith(".yml") or path.endswith(".yaml")))


def _trusted_run(run: dict, repo: str) -> None:
    """Fail closed unless this run is a workflow of the protected repository itself."""
    if not _trusted_workflow_path(run.get("path")):
        raise ValueError("an Actions run on the PR head has no trusted workflow provenance")
    head_repo = run.get("head_repository")
    if (head_repo.get("full_name") if isinstance(head_repo, dict) else None) != repo:
        raise ValueError("an Actions run on the PR head comes from another repository")
    if not isinstance(run.get("run_attempt"), int) or run["run_attempt"] < 1:
        raise ValueError("an Actions run attempt is missing")


def _classify(job: dict, sha: str) -> str:
    if job.get("head_sha") != sha:
        return "stale"
    if job.get("status") != "completed":
        return "pending"
    return {"success": "success", "failure": "failure"}.get(job.get("conclusion"), "infra")


def _current_pr(gh: Callable[..., Any], repo: str, number: int, sha: str, branch: str | None) -> tuple[str | None, str, dict]:
    """(refusal class or None, detail, pr). Merged keeps its head; closed unmerged or a moved head refuses."""
    pr = gh(f"repos/{repo}/pulls/{number}")
    if pr["state"] == "closed" and not pr.get("merged"):
        return "closed", "the pull request was closed without being merged", pr
    if pr["head"]["sha"] != sha or (branch is not None and pr["base"]["ref"] != branch):
        return "stale", "the pull request's head or base changed while its checks were read; retry", pr
    return None, "", pr


def actions_fallback(receipt: dict, url: str, *, gh: Callable[..., Any] | None = None) -> dict:
    """Prove Actions-pinned requirements from exact-head workflow jobs. Never accepts what it cannot decide."""
    gh = gh or gh_api
    match = _PR.fullmatch(url)
    repo, number, sha = match[1], int(match[2]), receipt["head_sha"]
    out = {**receipt, "ok": False, "checks": [], "evidence_source": "actions"}

    def undecided(detail: str, classification: str = "auth") -> dict:
        return {**out, "classification": classification,
                "detail": f"GitHub refused the Checks read and Actions evidence {detail}"}

    if not isinstance(sha, str) or not _SHA.fullmatch(sha):
        return undecided("has no exact head to check")
    try:
        required = sorted({(str(row["context"]), row.get("app_id")) for row in receipt["required"]}, key=str)
    except (KeyError, TypeError):
        return undecided("cannot read the required checks", "infra")
    foreign = [context for context, app_id in required if app_id != ACTIONS_APP_ID]
    if foreign:
        return undecided("cannot prove checks that are not pinned to GitHub Actions: " + ", ".join(foreign)[:200])
    try:
        refusal, detail, pr = _current_pr(gh, repo, number, sha, None)
        if refusal:
            return {**out, "classification": refusal, "detail": detail}
        if (pr["head"].get("repo") or {}).get("full_name") != repo:
            return undecided("cannot vouch for a pull request from another repository")
        branch = pr["base"]["ref"]
        runs = _rows(gh(f"repos/{repo}/actions/runs?head_sha={sha}&per_page=100", paginate=True), "workflow_runs")
        trusted, untrusted = [], False
        for run in runs:
            if run.get("head_sha") != sha:
                continue
            if run.get("event") not in TRUSTED_EVENTS:
                untrusted = True  # workflow_dispatch, pull_request_target and the like are not evidence
                continue
            _trusted_run(run, repo)
            trusted.append(run)
        if not trusted and untrusted:
            return undecided("has only untrusted runs on the PR head", "infra")
        for path in sorted({run["path"] for run in trusted}):
            try:
                body = gh(f"repos/{repo}/contents/{quote(path, safe='/')}?ref={quote(branch, safe='')}")
            except Denied as exc:
                if exc.status == "404":
                    return undecided("names a workflow that is not on the base branch", "infra")
                raise
            if not isinstance(body, dict) or body.get("type") != "file" or body.get("path") != path:
                return undecided("names a workflow that is not a file on the base branch", "infra")
        jobs = []
        for run in trusted:
            for job in _rows(gh(f"repos/{repo}/actions/runs/{int(run['id'])}/jobs?filter=latest&per_page=100",
                                paginate=True), "jobs"):
                if job.get("run_attempt") == run["run_attempt"]:
                    jobs.append((run, job))
        by_name: dict[Any, list] = {}
        for run, job in jobs:
            by_name.setdefault(job.get("name"), []).append((run, job))
        outcomes, checks = [], []
        for context, _app_id in required:
            selected = by_name.get(context) or []
            if selected:
                newest = max(int(run["id"]) for run, _job in selected)
                selected = [(run, job) for run, job in selected if int(run["id"]) == newest]
            if not selected:
                outcomes.append("missing")
                checks.append({"name": context, "classification": "missing", "head_sha": sha,
                               "evidence_source": "actions"})
            for run, job in selected:
                classification = _classify(job, sha)
                outcomes.append(classification)
                checks.append({"name": context, "id": job.get("id"), "url": job.get("html_url"),
                               "head_sha": job.get("head_sha"), "classification": classification,
                               "conclusion": job.get("conclusion"), "evidence_source": "actions",
                               "workflow": run.get("path"), "run_attempt": run.get("run_attempt")})
        # Re-read after every page: old-head successes never transfer.
        refusal, detail, _pr = _current_pr(gh, repo, number, sha, branch)
        if refusal:
            return {**out, "checks": checks, "classification": refusal, "detail": detail}
    except Denied as exc:
        return undecided(f"was refused too ({exc})")
    except Unavailable as exc:
        return undecided(f"is unavailable ({exc})", "infra")
    except (ValueError, KeyError, TypeError, IndexError) as exc:
        return undecided(f"is incomplete ({exc if isinstance(exc, ValueError) else 'unexpected reply'})", "infra")
    classification = next((x for x in outcomes if x != "success"), "success" if outcomes else "missing")
    result = {**out, "checks": checks, "classification": classification, "ok": classification == "success"}
    result.pop("detail", None)  # core's refusal detail no longer describes this receipt
    return result
