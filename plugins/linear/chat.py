"""Linear records and native task links; execution stays in Hermes Kanban."""
from __future__ import annotations
import json
import uuid
from typing import Any
from .api import LinearError
from .ops import held

SCHEMA = {
    "name": "linear",
    "description": (
        "Read Linear status, link an existing native task for reporting, or edit issues and projects. "
        "track requires issue and task_id and never starts, claims, resumes or transfers work. "
        "status reports the existing task and retained reconciliation records. "
        "followup adds an operator comment to active native work; requires issue, body and UUID id. "
        "Use native Kanban controls for an explicit resume, cancellation or new task. "
        "create_issue, create_project, link_issue, update_description and add_comment work without "
        "a task link or chat claim. Description replacement requires the exact expected_description. "
        "Reuse the same UUID id and content after a lost mutation response."),
    "parameters": {"type": "object", "properties": {
        "action": {"type": "string", "enum": ["track", "status", "followup", "create_issue", "create_project", "link_issue", "update_description", "add_comment"]},
        "issue": {"type": "string", "description": "Existing Linear identifier or exact issue id."},
        "task_id": {"type": "string", "description": "track: existing native task on this profile's configured board."},
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
        if not bridge.authorize_specialist_effect(issue_id):
            return _reply(False, "Specialist authorization is fenced or unavailable.", code="authorization_unavailable")
        if action == "status":
            return _reply(True, "Current Linear and native reporting state.", issue=issue,
                          native=bridge.native_status(issue_id))
        if action == "track":
            task_id = _text(args, "task_id")
            if not task_id:
                raise ValueError("track needs an existing task_id.")
            return _reply(True, "Existing native task linked for reporting.", native=bridge.track(issue_id, task_id))
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
        writes = bridge.store.issue_reconciliation_blocked(issue_id, except_id=client_id)
        if writes:
            return _reply(False, held(writes[0]), code="reconciliation_required", held_writes=[w["id"] for w in writes])
        prior = bridge.store.outbox_row(client_id)
        payload = {"issue_id": issue_id, "body": body, "id": client_id, "content_action": True,
                   "unlinked_content": True, "write_started": False, "caller_context": context.to_wire()}
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
    except (ValueError, LinearError) as exc:
        return _reply(False, str(exc), code="request_refused")
