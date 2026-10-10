"""Chat-origin work: `linear start|done|blocked|release`. The chat session stays the executor."""
from __future__ import annotations
import json
from typing import Any
from .api import LinearError
from .bridge import Bridge, acceptance_refusal, evidence_links, iso_ms
SCHEMA = {
    "name": "linear",
    "description": (
        "Track work on a Linear issue, or create issues and projects as this profile's own app. "
        "start: claim an existing issue for this chat (refused if another agent is working it). "
        "delegate: session-free gateway-chat handoff of a fresh unowned issue to one durable Kanban task; requires id. "
        "done: finish it; needs an evidence link. blocked: you need a human. "
        "release: stop tracking unfinished work. "
        "create_issue: create an undelegated issue; requires id, title, and team. "
        "update_description: replace an issue description with an expected_description preflight; readable headings and bullets/checklists required. "
        "add_comment: add a concise progress note with headings; requires a caller UUID id. "
        "create_project: create a project; requires id, name, and team. "
        "link_issue: relate two existing issues. Creates never assign, delegate, or start tracking."),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["start", "delegate", "done", "blocked", "release", "create_issue", "create_project", "link_issue", "update_description", "add_comment"]},
            "issue": {"type": "string", "description": "Existing issue identifier, for example ABC-123. Required for tracking and link_issue."},
            "id": {"type": "string", "description": "Caller UUID for create_issue or create_project. Reuse it to reconcile a lost response."},
            "title": {"type": "string", "description": "create_issue title."},
            "name": {"type": "string", "description": "create_project name."},
            "team": {"type": "string", "description": "Team id or key. Required for create_issue and create_project."},
            "description": {"type": "string", "description": "Markdown description. Use readable headings and bullets/checklists, not one long paragraph. Short prose is valid."},
            "expected_description": {"type": "string", "description": "Required current description for update_description's optimistic preflight. This is not server-side compare-and-swap; concurrent edits can race."},
            "project": {"type": "string", "description": "Optional project id for create_issue."},
            "parent": {"type": "string", "description": "Optional parent issue id for create_issue."},
            "related": {"type": "string", "description": "Other issue id for link_issue."},
            "relation": {"type": "string", "enum": ["blocks", "blocked_by", "related"], "description": "link_issue relation."},
            "evidence": {"type": "string", "description": "done: link(s) proving the result reached its destination"},
            "note": {"type": "string", "description": "done: outcome summary; blocked/release: what is needed or why"},
            "body": {"type": "string", "description": "add_comment: concise progress note; use short headings and bullets/checklists rather than one long paragraph. Short prose is valid."},
        },
        "required": ["action"],
    },
}
def _reply(ok: bool, message: str, **details: Any) -> str:
    return json.dumps({"ok": ok, "message": message, **details})

def _ownership_refusal(issue: dict[str, Any], row: dict, code: str) -> str:
    """Durable ownership is not a liveness observation. Do not invent one."""
    owner = row.get("owner_ref") or ""
    state = (issue.get("state") or {}).get("name") or "unknown"
    ownership = {"origin": row["origin"], "owner_session_key": owner if row["origin"] == "chat" else None,
                 "task_id": row.get("task_id"), "execution_state": "unknown",
                 "release_pending": bool(row.get("release_pending")),
                 "stop_requested": bool(row.get("stop_requested_at")), "linear_state": state}
    if code == "release_pending":
        message = "Release is pending verified relinquishment; reconcile it before further work."
    elif row["origin"] == "chat":
        message = (f"{issue.get('identifier')} has a retained ownership claim from chat {owner!r}. "
                   "This is not proof of active work; execution state is unknown. "
                   f"Linear state is {state!r}, not a liveness signal. "
                   "Continue in the owning chat to reconcile current evidence and use done or release. "
                   "If that chat cannot be resumed, request guarded ownership recovery; "
                   "do not clear the claim based on inactivity or an old blocker comment.")
    else:
        message = (f"{issue.get('identifier')} is bound to Kanban task {row.get('task_id')!r}; "
                   "the binding alone does not prove the task is running. "
                   "Continue its existing task/Linear session rather than creating another executor.")
    return _reply(False, f"{message} {issue.get('url', '')}", code=code, ownership=ownership)

def _plan(bridge: Bridge, args: dict[str, Any], action: str) -> str:
    """Create or link as this app. Do not assign, delegate, or start tracking."""
    client_id = str(args.get("id") or "").strip()
    try:
        if action == "create_issue":
            title, team = str(args.get("title") or "").strip(), str(args.get("team") or "").strip()
            if not client_id or not title or not team:
                return _reply(False, "create_issue needs id, title, and team.")
            created = bridge.api.create_issue(
                client_id, team, title, description=args.get("description"),
                project_id=args.get("project") or None, parent_id=args.get("parent") or None)
            return _reply(True, f"Created {created.get('identifier')} {created.get('url')} undelegated." +
                          (" Formatting tip: split this long single paragraph with headings and bullets/checklists." if
                           isinstance(args.get("description"), str) and len(args["description"]) > 500 and "\n" not in args["description"] else ""))
        if action == "create_project":
            name, team = str(args.get("name") or "").strip(), str(args.get("team") or "").strip()
            if not client_id or not name or not team:
                return _reply(False, "create_project needs id, name, and team.")
            created = bridge.api.create_project(client_id, name, [team], description=args.get("description"))
            return _reply(True, f"Created project {created.get('name')} {created.get('url')}.")
        if action == "link_issue":
            issue = str(args.get("issue") or "").strip()
            related, relation = str(args.get("related") or "").strip(), str(args.get("relation") or "").strip()
            if not issue or not related or not relation:
                return _reply(False, "link_issue needs issue, related, and relation.")
            linked = bridge.api.link_issue(issue, related, relation)
            return _reply(True, f"Linked {linked.get('type')} {issue} -> {related}.")
    except LinearError as exc:
        return _reply(False, str(exc))
    return _reply(False, f"Unknown planning action {action!r}.")

def handle(bridge: Bridge | None, args: dict[str, Any], invocation_context: Any = None) -> str:
    if bridge is None:
        return _reply(False, "The Linear service is not running on this profile.")
    session_key = str(getattr(invocation_context, "session_key", "") or "")
    session_id = str(getattr(invocation_context, "session_id", "") or session_key)
    profile = str(getattr(invocation_context, "profile", "") or "")
    platform = str(getattr(invocation_context, "platform", "") or "")
    bound = bridge.chat_profile_matches(profile, platform=platform)
    generation = getattr(invocation_context, "run_generation", None)
    generation = generation if bound and type(generation) is int and generation > 0 else None
    if not bound:
        return _reply(False, "linear needs a tool turn bound to this profile.")
    if not session_key:
        return _reply(False, "linear needs a chat session; it cannot run from this context.")
    action = str(args.get("action", ""))
    if action == "delegate":
        from . import admission
        return admission.handle(bridge, args, invocation_context)
    if action in {"update_description", "add_comment"}:
        ref = str(args.get("issue") or "").strip()
        client_id = str(args.get("id") or "").strip()
        body = args.get("description") if action == "update_description" else args.get("body")
        if not ref or not client_id or not isinstance(body, str) or not body.strip():
            return _reply(False, f"{action} needs issue, caller UUID id, and nonempty content.")
        try:
            issue = bridge.api.issue(ref)
            issue_id = issue.get("id")
            if ref not in (issue_id, issue.get("identifier")):
                return _reply(False, "Linear issue resolution did not match the requested issue.")
            if not bridge.authorize_specialist_effect(issue_id):
                return _reply(False, "Specialist authorization is fenced or unavailable; no change was made.")
            with bridge.lock(issue_id):
                owner = bridge.store.get(issue_id)
                if not owner or owner.get("origin") != "chat" or owner.get("owner_ref") != session_key:
                    return _reply(False, "Content changes require the owning chat's active Linear claim.")
                if owner.get("release_pending") or owner.get("stop_requested_at"):
                    return _reply(False, "Content changes are blocked during Stop or release reconciliation.")
                payload = {"issue_id": issue_id, "body": body, "session_key": session_key,
                           "work_owner": owner.get("ownership_id")}
                if action == "update_description":
                    expected = args.get("expected_description")
                    if not isinstance(expected, str):
                        return _reply(False, "update_description requires expected_description (the exact current text).")
                    payload.update({"expected_description": expected, "body": body})
                    kind = "description"
                if action == "add_comment":
                    try:
                        import uuid
                        if str(uuid.UUID(client_id)) != client_id.lower(): raise ValueError
                    except (ValueError, AttributeError):
                        return _reply(False, "add_comment id must be a UUID caller id.")
                    payload["id"] = client_id
                    payload["content_action"] = True
                    kind = "comment"
                row_id = bridge.store.enqueue(kind, payload, at=bridge.clock())
                if not row_id:
                    return _reply(False, "Content change was not admitted; no change was made.")
                warning = (" Formatting tip: split this long single paragraph with headings and bullets/checklists."
                           if len(body) > 500 and "\n" not in body else "")
                bridge.flush()
                return _reply(True, f"{action} queued durably for {issue.get('identifier')}; delivery is not yet confirmed.{warning}", outbox_id=row_id)
        except LinearError as exc:
            if getattr(bridge.api, "specialist_scope", None) is not None and not exc.retryable:
                bridge._fence_scope_denial(getattr(exc, "authoritative_issue_id", "") or "", exc)
                return _reply(False, "This issue is outside the configured Linear specialist scope.")
            return _reply(False, f"Linear is unavailable ({exc}); no content change was confirmed.")
    if action in {"create_issue", "create_project", "link_issue"}:
        return _plan(bridge, args, action)
    ref = str(args.get("issue", "")).strip()
    if not ref:
        return _reply(False, "tracking needs an issue identifier.")
    action, ref = action, ref
    note = str(args.get("note") or "").strip()
    try:
        issue = bridge.api.issue(ref)
        if ref not in (issue.get("id"), issue.get("identifier")):
            raise LinearError("Linear issue resolution does not match the requested ref")
        me = bridge.api.viewer_id()
    except LinearError as exc:
        if getattr(bridge.api, "specialist_scope", None) is not None and not exc.retryable:
            bridge._fence_scope_denial(exc.authoritative_issue_id or "", exc)
            return _reply(False, "This issue is outside the configured Linear specialist scope.")
        return _reply(False, f"Linear is unavailable ({exc}); try again shortly.")
    issue_id, ident, project = issue["id"], issue.get("identifier") or ref, (issue.get("project") or {}).get("id")
    with bridge.lock(issue_id):
        if not bridge.authorize_specialist_effect(issue_id):
            return _reply(False, "Specialist authorization is fenced or unavailable; no change was made.")
        row = bridge.store.get(issue_id)
        if row and row.get("release_pending"):
            return _ownership_refusal(issue, row, "release_pending")
        if action == "start":
            try: bridge.await_retired(issue_id)
            except LinearError as exc: return _reply(False, str(exc))
            if bridge.store.issue_reconciliation_blocked(issue_id):
                return _reply(False, "An earlier terminal write needs reconciliation before fresh work.")
            return _start(bridge, issue, row, me, session_key, session_id, generation)
        if not row or row["origin"] != "chat":
            return _reply(False, f"{ident} is not tracked from chat here; run `linear start {ident}` first.")
        if action in {"done", "blocked", "release"} and row.get("owner_ref") != session_key:
            return _ownership_refusal(issue, row, "chat_ownership_conflict")
        if action == "done":
            if row.get("stop_requested_at"):
                return _reply(False, "Stop was requested in Linear; this issue cannot be marked Done until a newer "
                                     "instruction explicitly resumes it.")
            links = evidence_links(str(args.get("evidence") or ""))
            if not links:
                return _reply(False, "done needs an evidence link: the PR, merged commit, deploy check or findings. "
                                     "Without one, use `linear blocked` or `linear release`.")
            heads: dict[str, str] = {}
            failures: list[str] = []
            if not bridge.accepted_evidence(links, heads=heads, failures=failures):
                return _reply(False, acceptance_refusal(failures))
            if not _finish(bridge, row, session_key, session_id, project, ident, "done",
                           f"Done. {note}\n\nEvidence: {' '.join(links)}".replace(". \n", ".\n"),
                           f"Done: {' '.join(links)}", links, heads):
                return _reply(False, "Specialist authorization is fenced or unavailable; closeout was not captured.")
            return _reply(True, f"{ident} closeout queued durably; Linear delivery is not yet confirmed.")
        if action == "blocked":
            message = f"Blocked: {note or 'needs input'}."
            if not bridge.status(issue_id, "blocked"):
                return _reply(False, "Specialist authorization is fenced or unavailable; Blocked was not captured.")
            if row.get("linear_session_id"):
                captured = bridge.activity(issue_id, row["linear_session_id"], "elicitation", message, row=row)
                updated = bridge.store.update(issue_id, panel_note=None)
            else:
                updated = bridge.store.update(issue_id, panel_note=message)
                captured = bridge.comment(issue_id, message) if updated else ""
            if not (captured and updated and bridge.project_update(
                    session_id, project, ident, f"Blocked: {note}", session_key=session_key, issue_id=issue_id)):
                return _reply(False, "Specialist authorization is fenced or unavailable; Blocked was not fully captured.")
            return _reply(True, f"{ident} Blocked update queued; it stays yours. Delivery is not yet confirmed.")
        if action == "release":
            if not _finish(bridge, row, session_key, session_id, project, ident, "blocked",
                           f"Released unfinished from chat: {note or 'no reason given'}.", "Released unfinished",
                           release=True):
                return _reply(False, "Specialist authorization is fenced or unavailable; closeout was not captured.")
            return _reply(True, f"{ident} unfinished release queued durably; relinquishment is not yet verified.")
    return _reply(False, f"Unknown action {action!r}.")
def _finish(bridge: Bridge, row: dict, session_key: str, session_id: str, project: str | None,
            ident: str, state: str, message: str, update: str, evidence: list[str] | None = None,
            heads: dict[str, str] | None = None, *, release: bool = False) -> bool:
    issue_id = row["issue_id"]
    if not bridge.authorize_specialist_effect(issue_id):
        return False
    route = {"session_key": session_key, "terminal": True, "owner_issue_id": issue_id}
    return bridge.store.finish(issue_id, [
        ("status", {"issue_id": issue_id, "state": state, "evidence": evidence or [],
                    "evidence_contract": "local-only", "pr_heads": heads or {}, "release": release, **route}),
        ("comment", {"issue_id": issue_id, "body": message, **route}),
        *([("activity", {"issue_id": issue_id, "session_id": row["linear_session_id"],
                         "content": {"type": "response", "body": message}, **route})]
          if release and row.get("linear_session_id") else []),
        ("project_update", {"issue_id": f"update:{session_id}:{project or issue_id}",
                            "session_id": session_id, "project_id": project, "resolve": issue_id,
                            "lines": {ident: update}, "line_issues": {ident: issue_id},
                            "quiet": bridge.quiet, **route}),
    ], at=bridge.clock(), release=release)
def _start(bridge: Bridge, issue: dict[str, Any], row: dict | None, me: str, session_key: str,
           session_id: str, generation: int | None) -> str:
    ident, url = issue.get("identifier"), issue.get("url", "")
    if row and row["origin"] == "kanban":
        return _ownership_refusal(issue, row, "task_ownership_conflict")
    if row and row["origin"] == "chat" and row["owner_ref"] != session_key:
        return _ownership_refusal(issue, row, "chat_ownership_conflict")
    delegate = issue.get("delegate") or {}
    if delegate.get("id") not in (None, me) and (issue.get("state") or {}).get("type") == "started":
        return _reply(False, f"{ident} is taken by {delegate.get('name') or 'another agent'}: {url}")
    project = (issue.get("project") or {}).get("id")
    if row:
        if row.get("stop_requested_at") and (row.get("run_generation") is None or generation is None or
                                             generation <= row["run_generation"]):
            return _reply(False, "Stop is still fenced; a newer bound chat turn must explicitly resume this issue.")
        if generation is not None and generation > (row.get("run_generation") or 0):
            if not bridge.store.update(issue["id"], run_generation=generation):
                return _reply(False, "Specialist authorization is fenced or unavailable; no change was made.")
        if row.get("stop_requested_at"):
            fence = max(bridge.clock() * 1000, row["last_updated_at"] + 1)
            if not bridge.store.update(issue["id"], stop_requested_at=0,
                                       last_updated_at=fence, resume_fence_at=fence):
                return _reply(False, "Specialist authorization is fenced or unavailable; no change was made.")
    else:
        watermark = iso_ms(issue.get("updatedAt"))
        if not watermark or watermark <= 0:
            return _reply(False, "Linear issue ordering timestamp is unavailable; no claim was captured.")
        if not bridge.store.put(issue["id"], "chat", session_key, project_id=project,
                                run_generation=generation, last_updated_at=watermark):
            return _reply(False, "Specialist authorization is fenced or unavailable; no change was made.")
    if not bridge.status(issue["id"], "in_progress", claim=True, seen=delegate.get("id")) or not bridge.project_update(
            session_id, project, ident, "In progress", session_key=session_key, issue_id=issue["id"]):
        return _reply(False, "Specialist authorization is fenced or unavailable; tracking was not fully captured.")
    message = f"Tracking {ident} from this chat: {url}"
    if generation is None:
        message += " Linear Stop cannot interrupt this chat adapter; use the chat's own Stop control."
    return _reply(True, message)
def on_turn_end(bridge: Bridge | None, session_id: str) -> None:
    """Every finished turn restarts the quiet period for that session's project updates."""
    if bridge is not None and session_id:
        bridge.store.delay_session_updates(session_id, bridge.clock() + bridge.quiet)
