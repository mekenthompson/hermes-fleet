"""Chat-origin work: `linear start|done|blocked|release`. The chat session stays the executor."""
from __future__ import annotations

import json
from typing import Any

from .api import LinearError
from .bridge import Bridge, evidence_links

SCHEMA = {
    "name": "linear",
    "description": (
        "Track work you are doing in this chat on a Linear issue. start: claim the issue (refused if another "
        "agent is working it). done: finish it; needs an evidence link (PR, merge, deploy check or findings). "
        "blocked: you need a human; say what. release: stop tracking it unfinished."),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["start", "done", "blocked", "release"]},
            "issue": {"type": "string", "description": "Issue identifier, for example ABC-123"},
            "evidence": {"type": "string", "description": "done: link(s) proving the result reached its destination"},
            "note": {"type": "string", "description": "done: outcome summary; blocked/release: what is needed or why"},
        },
        "required": ["action", "issue"],
    },
}


def _reply(ok: bool, message: str) -> str:
    return json.dumps({"ok": ok, "message": message})


def handle(bridge: Bridge | None, args: dict[str, Any], invocation_context: Any = None) -> str:
    if bridge is None:
        return _reply(False, "The Linear service is not running on this profile.")
    session_key = str(getattr(invocation_context, "session_key", "") or "")
    session_id = str(getattr(invocation_context, "session_id", "") or session_key)
    if not session_key:
        return _reply(False, "linear needs a chat session; it cannot run from this context.")
    action, ref = str(args.get("action", "")), str(args.get("issue", "")).strip()
    note = str(args.get("note") or "").strip()
    try:
        issue = bridge.api.issue(ref)
        me = bridge.api.viewer_id()
    except LinearError as exc:
        return _reply(False, f"Linear is unavailable ({exc}); try again shortly.")
    issue_id, ident, project = issue["id"], issue.get("identifier") or ref, (issue.get("project") or {}).get("id")
    with bridge.lock(issue_id):
        row = bridge.store.get(issue_id)
        if action == "start":
            return _start(bridge, issue, row, me, session_key, session_id)
        if not row or row["origin"] != "chat":
            return _reply(False, f"{ident} is not tracked from chat here; run `linear start {ident}` first.")
        if action == "done":
            links = evidence_links(str(args.get("evidence") or ""))
            if not links:
                return _reply(False, "done needs an evidence link: the PR, merged commit, deploy check or findings. "
                                     "Without one, use `linear blocked` or `linear release`.")
            bridge.store.delete(issue_id)
            bridge.status(issue_id, "done")
            bridge.comment(issue_id, f"Done. {note}\n\nEvidence: {' '.join(links)}".replace(". \n", ".\n"))
            bridge.project_update(session_id, project, ident, f"Done: {' '.join(links)}", session_key=session_key)
            return _reply(True, f"{ident} marked Done in Linear.")
        if action == "blocked":
            bridge.status(issue_id, "blocked")
            bridge.comment(issue_id, f"Blocked: {note or 'needs input'}.")
            bridge.project_update(session_id, project, ident, f"Blocked: {note}", session_key=session_key)
            return _reply(True, f"{ident} marked Blocked; it stays yours.")
        if action == "release":
            bridge.store.delete(issue_id)
            bridge.status(issue_id, "blocked")
            bridge.comment(issue_id, f"Released unfinished from chat: {note or 'no reason given'}.")
            bridge.project_update(session_id, project, ident, "Released unfinished", session_key=session_key)
            return _reply(True, f"Stopped tracking {ident}; it is Blocked in Linear.")
    return _reply(False, f"Unknown action {action!r}.")


def _start(bridge: Bridge, issue: dict[str, Any], row: dict | None, me: str, session_key: str, session_id: str) -> str:
    ident, url = issue.get("identifier"), issue.get("url", "")
    if row and row["origin"] == "kanban":
        return _reply(False, f"{ident} is already running here as Kanban task {row['task_id']}; "
                             f"reply in its Linear session to steer it: {url}")
    delegate = issue.get("delegate") or {}
    if delegate.get("id") not in (None, me) and (issue.get("state") or {}).get("type") == "started":
        return _reply(False, f"{ident} is taken by {delegate.get('name') or 'another agent'}: {url}")
    project = (issue.get("project") or {}).get("id")
    bridge.store.put(issue["id"], "chat", session_key, project_id=project)
    bridge.status(issue["id"], "in_progress", claim=True)
    bridge.project_update(session_id, project, ident, "In progress", session_key=session_key)
    return _reply(True, f"Tracking {ident} from this chat: {url}")


def on_turn_end(bridge: Bridge | None, session_id: str) -> None:
    """Every finished turn restarts the quiet period for that session's project updates."""
    if bridge is not None and session_id:
        bridge.store.delay_session_updates(session_id, bridge.clock() + bridge.quiet)
