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
    profile = str(getattr(invocation_context, "profile", "") or "")
    generation = getattr(invocation_context, "run_generation", None)
    generation = generation if profile == bridge.profile and type(generation) is int and generation > 0 else None
    if profile != bridge.profile:
        return _reply(False, "linear needs a tool turn bound to this profile.")
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
            return _start(bridge, issue, row, me, session_key, session_id, generation)
        if not row or row["origin"] != "chat":
            return _reply(False, f"{ident} is not tracked from chat here; run `linear start {ident}` first.")
        if action in {"done", "blocked", "release"} and row.get("owner_ref") != session_key:
            return _reply(False, f"{ident} is not owned by this chat session; no change was made.")
        if action == "done":
            if row.get("stop_requested_at"):
                return _reply(False, "Stop was requested in Linear; this issue cannot be marked Done until a newer "
                                     "instruction explicitly resumes it.")
            links = evidence_links(str(args.get("evidence") or ""))
            if not links:
                return _reply(False, "done needs an evidence link: the PR, merged commit, deploy check or findings. "
                                     "Without one, use `linear blocked` or `linear release`.")
            heads: dict[str, str] = {}
            if not bridge.accepted_evidence(links, heads=heads):
                return _reply(False, "PR acceptance on the exact head and required checks could not be verified; "
                                     "leave this issue open and reconcile the PR.")
            _finish(bridge, row, session_key, session_id, project, ident, "done",
                    f"Done. {note}\n\nEvidence: {' '.join(links)}".replace(". \n", ".\n"),
                    f"Done: {' '.join(links)}", links, heads)
            return _reply(True, f"{ident} closeout queued durably; Linear delivery is not yet confirmed.")
        if action == "blocked":
            message = f"Blocked: {note or 'needs input'}."
            bridge.status(issue_id, "blocked")
            if row.get("linear_session_id"):
                bridge.activity(issue_id, row["linear_session_id"], "elicitation", message, row=row)
                bridge.store.update(issue_id, panel_note=None)
            else:
                bridge.store.update(issue_id, panel_note=message)
                bridge.comment(issue_id, message)
            bridge.project_update(session_id, project, ident, f"Blocked: {note}", session_key=session_key,
                                  issue_id=issue_id)
            return _reply(True, f"{ident} Blocked update queued; it stays yours. Delivery is not yet confirmed.")
        if action == "release":
            _finish(bridge, row, session_key, session_id, project, ident, "blocked",
                    f"Released unfinished from chat: {note or 'no reason given'}.", "Released unfinished")
            return _reply(True, f"Stopped chat tracking {ident}; Blocked closeout queued, not yet confirmed in Linear.")
    return _reply(False, f"Unknown action {action!r}.")


def _finish(bridge: Bridge, row: dict, session_key: str, session_id: str, project: str | None,
            ident: str, state: str, message: str, update: str, evidence: list[str] | None = None,
            heads: dict[str, str] | None = None) -> None:
    issue_id = row["issue_id"]
    route = {"session_key": session_key, "terminal": True, "owner_issue_id": issue_id}
    bridge.store.finish(issue_id, [
        ("status", {"issue_id": issue_id, "state": state, "evidence": evidence or [],
                    "evidence_contract": "local-only", "pr_heads": heads or {}, **route}),
        ("comment", {"issue_id": issue_id, "body": message, **route}),
        ("project_update", {"issue_id": f"update:{session_id}:{project or issue_id}",
                            "session_id": session_id, "project_id": project, "resolve": issue_id,
                            "lines": {ident: update}, "line_issues": {ident: issue_id},
                            "quiet": bridge.quiet, **route}),
    ], at=bridge.clock())


def _start(bridge: Bridge, issue: dict[str, Any], row: dict | None, me: str, session_key: str,
           session_id: str, generation: int | None) -> str:
    ident, url = issue.get("identifier"), issue.get("url", "")
    if row and row["origin"] == "kanban":
        return _reply(False, f"{ident} is already running here as Kanban task {row['task_id']}; "
                             f"reply in its Linear session to steer it: {url}")
    if row and row["origin"] == "chat" and row["owner_ref"] != session_key:
        return _reply(False, f"{ident} is already tracked in another chat session: {url}")
    delegate = issue.get("delegate") or {}
    if delegate.get("id") not in (None, me) and (issue.get("state") or {}).get("type") == "started":
        return _reply(False, f"{ident} is taken by {delegate.get('name') or 'another agent'}: {url}")
    project = (issue.get("project") or {}).get("id")
    if row:
        if row.get("stop_requested_at") and (row.get("run_generation") is None or generation is None or
                                             generation <= row["run_generation"]):
            return _reply(False, "Stop is still fenced; a newer bound chat turn must explicitly resume this issue.")
        if generation is not None and generation > (row.get("run_generation") or 0):
            bridge.store.update(issue["id"], run_generation=generation)
        if row.get("stop_requested_at"):
            fence = max(bridge.clock() * 1000, row["last_updated_at"] + 1)
            bridge.store.update(issue["id"], stop_requested_at=0,
                                last_updated_at=fence, resume_fence_at=fence)
    else:
        bridge.store.put(issue["id"], "chat", session_key, project_id=project, run_generation=generation)
    bridge.status(issue["id"], "in_progress", claim=True, seen=delegate.get("id"))
    bridge.project_update(session_id, project, ident, "In progress", session_key=session_key, issue_id=issue["id"])
    return _reply(True, f"Tracking {ident} from this chat: {url}")


def on_turn_end(bridge: Bridge | None, session_id: str) -> None:
    """Every finished turn restarts the quiet period for that session's project updates."""
    if bridge is not None and session_id:
        bridge.store.delay_session_updates(session_id, bridge.clock() + bridge.quiet)
