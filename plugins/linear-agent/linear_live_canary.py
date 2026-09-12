"""Run one post-start Linear agent lifecycle canary on a disposable issue."""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from threading import Event

try:
    from .linear_activity import LinearActivityClient
    from .linear_oauth import LinearOAuth
    from .linear_project_updates import LinearProjectUpdatePublisher, ProjectUpdateError
except ImportError:  # Direct script/test import.
    from linear_activity import LinearActivityClient
    from linear_oauth import LinearOAuth
    from linear_project_updates import LinearProjectUpdatePublisher, ProjectUpdateError
_ISSUE = """query LinearAgentCanaryIssue($id: String!) {
  issue(id: $id) { id state { id name } project { id health } comments(last: 50) { nodes { body } } }
}"""
_CREATE_SESSION = """mutation LinearAgentCanarySession($input: AgentSessionCreateOnIssue!) {
  agentSessionCreateOnIssue(input: $input) { success agentSession { id } }
}"""
_SAFE_ID = re.compile(r"[A-Za-z0-9-]{3,64}$")
_SENT = {"response", "issue_comment", "project_update"}


def _issue_snapshot(issue_id: str, graphql: Callable[[str, dict[str, object]], dict[str, object]]) -> dict[str, object]:
    result = graphql(_ISSUE, {"id": issue_id})
    issue = result.get("data", {}).get("issue")  # type: ignore[union-attr]
    state = issue.get("state") if isinstance(issue, dict) else None
    project = issue.get("project") if isinstance(issue, dict) else None
    if (not isinstance(issue, dict) or not isinstance(issue.get("id"), str)
            or not isinstance(state, dict) or not isinstance(state.get("id"), str)
            or not isinstance(project, dict) or not isinstance(project.get("id"), str)):
        raise RuntimeError("Linear canary issue metadata is unavailable")
    return issue


def _create_session(issue_id: str, graphql: Callable[[str, dict[str, object]], dict[str, object]]) -> str:
    result = graphql(_CREATE_SESSION, {"input": {"issueId": issue_id}})
    payload = result.get("data", {}).get("agentSessionCreateOnIssue")  # type: ignore[union-attr]
    session = payload.get("agentSession") if isinstance(payload, dict) else None
    if not isinstance(payload, dict) or payload.get("success") is not True or not isinstance(session, dict) or not session.get("id"):
        raise RuntimeError("Linear canary session creation failed")
    return str(session["id"])


def inspect_worker(database: Path, session_id: str) -> dict[str, str] | None:
    """Read only durable state; project-update body and target are verification inputs."""
    if not database.is_file() or database.is_symlink():
        return None
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as conn:
            rows = conn.execute("SELECT delivery_id, payload, state FROM deliveries ORDER BY rowid DESC").fetchall()
            for delivery_id, raw, state in rows:
                try:
                    event = json.loads(bytes(raw))
                except (TypeError, ValueError):
                    continue
                session = event.get("agentSession") if isinstance(event, dict) else None
                if not isinstance(session, dict) or str(session.get("id")) != session_id:
                    continue
                outbox = conn.execute("SELECT kind, linear_session_id, body, state FROM outbox WHERE delivery_id = ?", (delivery_id,)).fetchall()
                entries = {str(kind): (str(target), str(body), str(item_state)) for kind, target, body, item_state in outbox}
                if str(state) != "completed" or any(entries.get(kind, ("", "", ""))[2] != "sent" for kind in _SENT):
                    return None
                status_entries = [entry for kind, entry in entries.items() if kind.startswith("issue_status_")]
                if not status_entries or any(entry[2] != "sent" for entry in status_entries):
                    return None
                target, body, _ = entries["project_update"]
                try:
                    payload = json.loads(body)
                except (TypeError, ValueError):
                    return None
                if not isinstance(payload, dict) or not isinstance(payload.get("session_key"), str) or not isinstance(payload.get("summary"), str):
                    return None
                return {"delivery_id": str(delivery_id), "project_target": target, "project_body": body,
                        "session_key": payload["session_key"], "summary": payload["summary"],
                        "comment_body": entries['issue_comment'][1]}
    except sqlite3.Error:
        return None
    return None


def _comment_count(issue: dict[str, object], expected: str) -> int:
    comments = issue.get('comments')
    nodes = comments.get('nodes') if isinstance(comments, dict) else None
    if not isinstance(nodes, list):
        raise RuntimeError('Linear canary comment readback is unavailable')
    return sum(1 for node in nodes if isinstance(node, dict) and LinearProjectUpdatePublisher.markdown_equivalent(expected, node.get('body')))


def _external_update(*, issue_id: str, project_id: str, worker: dict[str, str], graphql: Callable[[str, dict[str, object]], dict[str, object]]) -> dict[str, str] | None:
    if worker["project_target"] != issue_id:
        return None
    publisher = LinearProjectUpdatePublisher(graphql)
    try:
        actor, organization = publisher._viewer()
        groups = publisher.resolve_projects([issue_id], workspace=organization)
        if set(groups) != {project_id}:
            return None
        body = f"### [{publisher._issue_identifiers[issue_id]}]({publisher._issue_urls[issue_id]})\n{worker['summary']}"
        update_id = publisher.deterministic_id(actor, organization, project_id, worker["session_key"])
        # Readback only: never call publish() from a verifier.
        return publisher._checked_update(update_id, project_id=project_id, body=body, actor=actor)
    except ProjectUpdateError:
        return None


def run_canary(*, issue_id: str, database: Path, timeout: int,
               graphql: Callable[[str, dict[str, object]], dict[str, object]],
               cancel_event: Event | None = None, now: Callable[[], float] = time.monotonic,
               sleep: Callable[[float], None] = time.sleep) -> dict[str, str]:
    """Create and verify one canary, cooperatively stopping before later writes."""
    cancel_event = cancel_event or Event()
    def cancelled() -> None:
        if cancel_event.is_set():
            raise RuntimeError("Linear agent lifecycle canary cancelled")
    if _SAFE_ID.fullmatch(issue_id) is None:
        raise ValueError("Linear canary issue identifier is invalid")
    if type(timeout) is not int or not 30 <= timeout <= 900:
        raise ValueError("Linear canary timeout must be 30..900 seconds")
    cancelled()
    initial = _issue_snapshot(issue_id, graphql)
    if initial.get("id") != issue_id:
        raise RuntimeError("Linear canary issue canonical ID does not match registration")
    initial_state = initial["state"]
    initial_project = initial["project"]
    assert isinstance(initial_state, dict) and isinstance(initial_project, dict)
    # This is the sole mutation. Check cancellation immediately before it.
    cancelled()
    session_id = _create_session(issue_id, graphql)
    deadline = now() + timeout
    while True:
        cancelled()
        worker = inspect_worker(database, session_id)
        issue = _issue_snapshot(issue_id, graphql)
        if issue.get("id") != issue_id:
            raise RuntimeError("Linear canary issue canonical ID changed")
        state, project = issue["state"], issue["project"]
        assert isinstance(state, dict) and isinstance(project, dict)
        completed = state.get("name") == "Done" or state.get("type") == "completed"
        same_project = project.get("id") == initial_project.get("id")
        update = _external_update(issue_id=str(issue["id"]), project_id=str(project["id"]), worker=worker, graphql=graphql) if worker is not None and completed and same_project else None
        if (update is not None and worker is not None
                and update['health'] == project.get('health') == initial_project.get('health')
                and _comment_count(issue, worker['comment_body']) == _comment_count(initial, worker['comment_body']) + 1):
            return {"issue_id": str(issue["id"]), "linear_session_id": session_id, "delivery_id": worker["delivery_id"],
                    "project_id": update["project_id"], "project_update_id": update["update_id"],
                    "project_update_url": update["url"], "status": "passed"}
        if now() >= deadline:
            raise RuntimeError("Linear agent lifecycle canary timed out")
        # An injected sleep remains useful in deterministic tests; production
        # waits on the Event so cancellation cannot be hidden for one second.
        if sleep is time.sleep:
            cancel_event.wait(min(1.0, max(0.0, deadline - now())))
        else:
            sleep(1)


def make_graphql(token: LinearOAuth) -> Callable[[str, dict[str, object]], dict[str, object]]:
    return LinearActivityClient(token).graphql


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", required=True); parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if not 30 <= args.timeout <= 900: parser.error("--timeout must be 30..900 seconds")
    from linear_tracking import _tracking_config
    from linear_project_updates import _configured_publisher
    _home, _entry, profile, _workspace, database = _tracking_config()
    manifest = json.loads(Path(__file__).with_name("linear-agents.json").read_text())
    matches = [item for item in manifest.get("agents", []) if isinstance(item, dict) and item.get("profile") == profile]
    if len(matches) != 1: parser.error("Linear canary execution profile is not registered")
    publisher = _configured_publisher()
    publisher._viewer()  # Pin the app and organization before creating a session.
    print(json.dumps(run_canary(issue_id=args.issue, database=database, timeout=args.timeout, graphql=publisher.graphql), sort_keys=True))
    return 0

if __name__ == "__main__": raise SystemExit(main())
