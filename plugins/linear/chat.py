"""Linear records and native task links; execution stays in Hermes Kanban."""
from __future__ import annotations
import json
import uuid
from typing import Any
from .api import LinearError
from .ops import held
from . import transport

SCHEMA = {
    "name": "linear",
    "description": (
        "Read Linear status, link an existing native task for reporting, or edit issues and projects. "
        "track requires issue and task_id and never starts, claims, resumes or transfers work. "
        "After inspecting and coordinating native work, replace=true can replace only the reporting link; "
        "requires id and the exact binding_revision from status, and preserves prior history. "
        "status reports the existing task and retained reconciliation records. "
        "followup adds an operator comment to active native work; requires issue, body and UUID id. "
        "Use native Kanban controls for an explicit resume, cancellation or new task. "
        "origin_reference accepts only action and returns the current authenticated native input reference; "
        "it grants no control permission and must be independently inspected by the owner. "
        "create_issue, create_project, link_issue, update_description and add_comment work without "
        "a task link or chat claim. Description replacement requires the exact expected_description. "
        "Reuse the same UUID id and content after a lost mutation response."),
    "parameters": {"type": "object", "properties": {
        "action": {"type": "string", "enum": ["track", "status", "followup", "create_issue", "create_project", "link_issue", "update_description", "add_comment", "origin_reference"]},
        "issue": {"type": "string", "description": "Existing Linear identifier or exact issue id."},
        "task_id": {"type": "string", "description": "track: existing native task assigned to this profile."},
        "board": {"type": "string", "description": "track: existing native board; defaults to the caller's current native board. A target selector, never identity or permission."},
        "replace": {"type": "boolean", "description": "track: explicitly replace a prior reporting link after inspecting actual work. Does not transfer execution or prove the old worker stopped."},
        "expected_binding_revision": {"type": "string", "description": "track with replace: exact native.binding_revision from a fresh status read."},
        "id": {"type": "string", "description": "Caller UUIDv4 for creates, update_description, add_comment and followup; reuse with identical content after an uncertain response."},
        "title": {"type": "string"}, "name": {"type": "string"}, "team": {"type": "string"},
        "description": {"type": "string", "description": "Readable Linear Markdown."},
        "expected_description": {"type": "string", "description": "Exact current description for replacement preflight; this is not remote compare-and-swap."},
        "project": {"type": "string"}, "parent": {"type": "string"}, "related": {"type": "string"},
        "relation": {"type": "string", "enum": ["blocks", "blocked_by", "related"]},
        "body": {"type": "string", "description": "Comment or instruction; never an execution grant."},
    }, "required": ["action"], "additionalProperties": False},
}

def _reply(ok: bool, message: str, **details: Any) -> str:
    return json.dumps({"ok": ok, "message": message, **details})

def _uuid(value: Any) -> str:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("id must be a canonical caller UUIDv4.") from None
    if parsed.version != 4 or str(parsed) != value:
        raise ValueError("id must be a canonical caller UUIDv4.")
    return value

def _text(args: dict, key: str) -> str:
    value = args.get(key)
    return value.strip() if isinstance(value, str) else ""


def _check_native_route(bridge: Any, action: str, issue_id: str, context: Any) -> None:
    """Keep host-native board resolution and worker fences across the service hop."""
    if action == "track":
        if not context.native_board or not context.native_db_path:
            raise LinearError("track requires a native board inspected by the caller's host.")
        _, path = transport.native_board_target(context.native_board)
        if path != context.native_db_path:
            raise LinearError("The service cannot verify the caller's native board; inspect the target.")
    elif context.pinned_db_path:
        binding = bridge.store.native_binding(issue_id)
        if not binding:
            return
        if binding["board"] != context.pinned_board:
            raise LinearError("The linked task is outside the caller's native board pin.")
        _, path = transport.native_board_target(binding["board"])
    else:
        return
    if context.pinned_db_path and path != context.pinned_db_path:
        raise LinearError("The linked task is outside the caller's native board pin.")


def _content_requester(bridge: Any, issue_id: str, prior: dict | None, session: str | None = None,
                       activity: str | None = None) -> dict:
    """Persist the exact requester used by current specialist authorization, never a chat id."""
    if (getattr(bridge.api, "specialist_scope", None) or {}).get("requester_basis") != "agent_session":
        return {}
    if prior is not None:
        session = (prior["payload"].get("requester_sessions") or {}).get(issue_id)
        activity = (prior["payload"].get("requester_activities") or {}).get(issue_id)
    elif session is None:
        resolver = getattr(bridge.api, "requester_session_for_issue", None)
        session = resolver(issue_id) if callable(resolver) else None
        activity_resolver = getattr(bridge.api, "requester_activity_for_issue", None)
        activity = activity_resolver(issue_id) if callable(activity_resolver) else None
    if not isinstance(session, str) or not session:
        raise LinearError("Specialist content needs a verified requester session for this issue; nothing was queued.")
    sessions = {issue_id: session}
    activities = {issue_id: activity} if activity else {}
    # BoundLinearAPI checks the exact session's app, requester allowlist and issue again.
    # A retry retains its original requester even if the work binding later changes.
    with bridge.api.requester_session(sessions, activity_id=activities or None):
        issue = bridge.api.issue(issue_id)
        if issue.get("id") != issue_id:
            raise LinearError("The verified requester does not authorize this content target.")
    return {"requester_sessions": sessions, **({"requester_activities": activities} if activities else {})}


def _plan(bridge: Any, args: dict, action: str) -> str:
    if action == "create_issue":
        client_id, title, team = _uuid(args.get("id")), _text(args, "title"), _text(args, "team")
        if not title or not team:
            raise ValueError("create_issue needs title and team.")
        created = bridge.api.create_issue(client_id, team, title, description=args.get("description"),
                                          project_id=args.get("project") or None, parent_id=args.get("parent") or None)
        warning = (" Formatting tip: split this long paragraph into headings and bullets." if
                   isinstance(args.get("description"), str) and len(args["description"]) > 500 and "\n" not in args["description"] else "")
        return _reply(True, "Created an undelegated issue." + warning, issue=created)
    if action == "create_project":
        client_id, name, team = _uuid(args.get("id")), _text(args, "name"), _text(args, "team")
        if not name or not team:
            raise ValueError("create_project needs name and team.")
        created = bridge.api.create_project(client_id, name, [team], description=args.get("description"))
        return _reply(True, "Created a project.", project=created)
    issue, related, relation = (_text(args, key) for key in ("issue", "related", "relation"))
    if not issue or not related or relation not in {"blocks", "blocked_by", "related"}:
        raise ValueError("link_issue needs issue, related, and a supported relation.")
    return _reply(True, "Linked issues.", relation=bridge.api.link_issue(issue, related, relation))

def handle(bridge: Any, args: dict[str, Any], context: Any) -> str:
    if (getattr(context, "platform", None) != "linear" or bridge is None
            or context.profile != bridge.profile or str(context.home) != str(bridge.profile_home)):
        return _handle(bridge, args, context)
    from .native_input import requester_scope
    try:
        with requester_scope(bridge, context):
            return _handle(bridge, args, context)
    except (LinearError, transport.Unavailable) as exc:
        return _reply(False, str(exc), code="request_refused")


def _handle(bridge: Any, args: dict[str, Any], context: Any) -> str:
    if bridge is None:
        return _reply(False, "The Linear service is not running on this profile.", code="service_unavailable")
    # The registered handler captures host context. Model args never supply it.
    if context is None or context.profile != bridge.profile or str(context.home) != str(bridge.profile_home):
        return _reply(False, "Linear needs trusted native session context for this profile.", code="context_unavailable")
    action = _text(args, "action")
    if action not in SCHEMA["parameters"]["properties"]["action"]["enum"]:
        return _reply(False, "Use track/status for native reporting or a supported content action.", code="unsupported_action")
    try:
        if action in {"create_issue", "create_project", "link_issue"}:
            return _plan(bridge, args, action)
        ref = _text(args, "issue")
        if not ref:
            raise ValueError(f"{action} needs an issue identifier.")
        issue = bridge.api.issue(ref)
        if not isinstance(issue, dict) or ref not in (issue.get("id"), issue.get("identifier")):
            raise LinearError("Linear issue resolution did not match the requested issue.")
        issue_id = issue["id"]
        if action == "status":
            try:
                _check_native_route(bridge, action, issue_id, context)
                native = bridge.native_status(issue_id)
            except (LinearError, transport.Unavailable) as exc:
                # Issue authorization above does not grant access to pinned or
                # unavailable native evidence. Keep that refusal scoped here.
                native = {"issue_id": issue_id, "linked": False, "status": "unavailable", "message": str(exc)}
            if bridge.store.scope_fenced(issue_id):
                native = {**native, "effects_held": True,
                          "message": "Prior specialist effects remain held for reconciliation."}
            return _reply(True, "Current Linear and native reporting state.", issue=issue,
                          native=native)
        if not bridge.authorize_specialist_effect(issue_id):
            return _reply(False, "Specialist authorization is fenced or unavailable.", code="authorization_unavailable")
        if action in {"track", "followup"}:
            _check_native_route(bridge, action, issue_id, context)
        if action == "track":
            task_id = _text(args, "task_id")
            if not task_id:
                raise ValueError("track needs an existing task_id.")
            replace = args.get("replace", False)
            if type(replace) is not bool:
                raise ValueError("track replace must be a boolean.")
            return _reply(True, "Existing native task linked for reporting; execution state was not changed.",
                          native=bridge.track(issue_id, task_id, board=context.native_board, replace=replace,
                              requester_session=context.thread_id if context.platform == "linear" else None,
                              requester_activity=(context.message_id if context.platform == "linear" and
                                  context.message_id != context.thread_id else None),
                              expected_revision=args.get("expected_binding_revision"),
                              request_id=_uuid(args.get("id")) if replace else None))
        if action == "followup":
            if context.delegated_child:
                return _reply(False, "Native delegated children cannot mutate Kanban; return findings to the parent agent.",
                              code="native_scope_refused")
            body = _text(args, "body")
            if not body:
                raise ValueError("followup needs a nonempty body.")
            result = bridge.followup(issue_id, body, _uuid(args.get("id")))
            return _reply(True, "Native follow-up recorded; native task state controls execution.", native=result)
        client_id = _uuid(args.get("id"))
        body = args.get("description") if action == "update_description" else args.get("body")
        if not isinstance(body, str) or not body.strip():
            raise ValueError(f"{action} needs nonempty content.")
        writes = bridge.store.issue_reconciliation_blocked(issue_id, except_id=client_id) if action == "update_description" else []
        if writes:
            return _reply(False, held(writes[0]), code="reconciliation_required", held_writes=[w["id"] for w in writes])
        prior = bridge.store.outbox_row(client_id)
        payload = {"issue_id": issue_id, "body": body, "id": client_id, "content_action": True,
                   "unlinked_content": True, "write_started": False, "caller_context": context.to_wire(),
                   **_content_requester(bridge, issue_id, prior,
                                        context.thread_id if getattr(context, "platform", None) == "linear" else None,
                                        context.message_id if getattr(context, "platform", None) == "linear" and
                                        context.message_id != context.thread_id else None)}
        kind = "comment"
        if action == "update_description":
            expected = args.get("expected_description")
            if not isinstance(expected, str):
                raise ValueError("update_description requires expected_description (the exact current text).")
            if prior is None and (issue.get("description") or "") != expected:
                return _reply(False, "Description changed; inspect it before replacing it.", code="description_conflict")
            payload["expected_description"] = expected
            kind = "description"
        with bridge.lock(issue_id):
            row_id = bridge.store.enqueue(kind, payload, at=bridge.clock())
        if not row_id:
            return _reply(False, "An uncertain issue write or authorization fence needs reconciliation.", code="reconciliation_required")
        warning = (" Formatting tip: split this long paragraph into headings and bullets." if
                   len(body) > 500 and "\n" not in body else "")
        return _reply(True, "Content queued durably; Linear delivery is not yet confirmed." + warning, outbox_id=row_id)
    except (ValueError, LinearError, transport.Unavailable) as exc:
        return _reply(False, str(exc), code="request_refused")
