"""User-authorized control handoff with a separate execution admission decision."""
from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

from .api import LinearError
from .bridge import CLOSED


def _reply(ok, message, **details):
    return json.dumps({"ok": ok, "message": message, **details})


def _uuid4(value):
    try:
        parsed = uuid.UUID(value)
        return isinstance(value, str) and str(parsed) == value and parsed.version == 4
    except (ValueError, AttributeError, TypeError):
        return False


def _host_instruction(bridge, context):
    """Serialized Desktop context never acquires the in-process live gateway grant."""
    if getattr(context, "_serialized_transport", False):
        platform = getattr(context, "platform", "")
        desktop = (platform == "desktop" and getattr(context, "ui_session_id", "") and
                   getattr(context, "browser_control_provider", "") and
                   getattr(context, "browser_control_subject", ""))
        api = (platform == "api_server" and getattr(context, "api_control_principal", "") and
               getattr(context, "api_control_transport_family", "") and
               getattr(context, "api_conversation_key", ""))
        if not (desktop or api):
            return ""
        return getattr(context, "user_message", "") or ""
    if getattr(context, "platform", "") == "api_server":
        try:
            from tools import registry
            if (isinstance(context, registry.ToolInvocationContext) and
                    context == registry._current_tool_invocation_context() and
                    context.api_control_principal and context.api_control_transport_family and
                    context.api_conversation_key):
                return context.user_message
        except ImportError:
            pass
        return ""
    validate = getattr(bridge, "validate_chat_admission", None)
    source = getattr(bridge, "transfer_user_message", None)
    return source(context) if callable(validate) and validate(context) and callable(source) else ""


def _explicit(text, identifier, *, resume=False):
    if not isinstance(text, str) or not text or identifier.casefold() not in text.casefold():
        return False
    words = text.casefold()
    if resume:
        return bool(re.search(r"\b(reconcil\w*|reviewed effects)\b", words) and
                    re.search(r"\b(resume|continue|proceed)\b", words))
    return bool(re.search(r"\b(transfer|move|handoff|hand over|take over)\b", words) and
                re.search(r"\b(this|here|current|new)\b", words) and
                re.search(r"\b(chat|thread|conversation|here)\b", words))


def _identity(bridge, context):
    home = getattr(bridge, "profile_home", None) or getattr(bridge.kanban, "profile_home", None)
    if home is None:
        return None
    home = str(Path(home).resolve(strict=True))
    platform = getattr(context, "platform", "")
    ui = getattr(context, "ui_session_id", "") if platform == "desktop" else ""
    api = [getattr(context, "api_control_principal", ""),
           getattr(context, "api_control_transport_family", "")] if platform == "api_server" else ["", ""]
    destination = (ui if platform == "desktop" else
                   getattr(context, "api_conversation_key", "") if platform == "api_server" else
                   getattr(context, "session_key", ""))
    return {"profile": bridge.profile, "home": home,
            "session_key": getattr(context, "session_key", ""),
            "session_id": getattr(context, "session_id", ""),
            "platform": platform, "destination_id": destination, "ui_session_id": ui,
            "api_identity": api,
            "browser_identity": [getattr(context, "browser_control_provider", ""),
                                 getattr(context, "browser_control_subject", "")]}


def _ownership_receipt(saved):
    return _reply(True, "Ownership moved to this chat. Reconcile predecessor effects, then explicitly "
                  "resume this transfer before any issue work; no work was replayed.",
                  code="ownership_transferred", transfer_id=saved["id"],
                  owner_id=saved["successor_owner"], execution_admitted=False,
                  predecessor_effects=saved["retirement"]["effects"])


def handle(bridge, args, context):
    request_id = args.get("id")
    if not _uuid4(request_id):
        return _reply(False, "Transfer needs a canonical UUIDv4 id; reuse it on retry.")
    ref = args.get("issue")
    if not isinstance(ref, str) or not ref or ref != ref.strip():
        return _reply(False, "Transfer needs an exact issue identifier.")
    retire = getattr(bridge, "retire_chat", None)
    if not callable(retire):
        return _reply(False, "This Agent runtime lacks durable chat retirement; no transfer was prepared.",
                      code="capability_unavailable")
    try:
        instruction = _host_instruction(bridge, context)
        identity = _identity(bridge, context)
        if not identity or not identity["session_key"] or not identity["session_id"] or not identity["destination_id"]:
            return _reply(False, "Transfer needs a profile-bound authenticated chat turn.")
        issue = bridge.api.issue(ref)
        if ref not in (issue.get("id"), issue.get("identifier")):
            return _reply(False, "Issue resolution did not match the requested issue.")
        issue_id, ident = issue["id"], issue.get("identifier") or ref
        resume = args.get("action") == "resume_transfer"
        if not _explicit(instruction, ident, resume=resume):
            return _reply(False, "The current authenticated user turn must explicitly request this issue's "
                          + ("reconciliation and resume." if resume else "transfer to this chat."))
        with bridge.lock(issue_id):
            if not bridge.authorize_specialist_effect(issue_id):
                return _reply(False, "Issue authorization is fenced or unavailable.")
            fresh = bridge.api.issue(issue_id)
            if (fresh.get("state") or {}).get("type") in CLOSED:
                return _reply(False, "Closed issues cannot be transferred or resumed.")
            if (fresh.get("delegate") or {}).get("id") != bridge.api.viewer_id():
                return _reply(False, "This agent no longer owns the Linear delegation.")
            if resume:
                evidence = args.get("evidence")
                if (not isinstance(evidence, str) or not evidence.startswith("https://") or
                        evidence not in instruction):
                    return _reply(False, "Resume needs a reconciliation evidence link supplied in the "
                                  "authenticated user instruction.")
                saved = bridge.store.rebind_chat_transfer(request_id, identity, issue_id)
                if not saved:
                    return _reply(False, "Transfer receipt does not belong to this issue and chat.")
                result = bridge.store.resume_chat_transfer(request_id, evidence=evidence, at=bridge.clock())
                if not result:
                    return _reply(False, "Reconciliation, Stop, release, or ownership still blocks execution.",
                                  code="reconciliation_required")
                return _reply(True, "Successor execution admitted after explicit reconciliation; no work replayed.",
                              transfer_id=request_id, owner_id=result["successor_owner"],
                              predecessor_effects=result["retirement"]["effects"])
            expected = args.get("expected_owner")
            if not isinstance(expected, str) or not expected:
                return _reply(False, "Transfer needs the exact expected_owner ownership id.")
            request = {"id": request_id, "issue_id": issue_id, "expected_owner": expected, **identity}
            saved = bridge.store.prepare_chat_transfer(request, at=bridge.clock())
            if saved is None:
                return _reply(False, "Ownership changed or execution/reconciliation is fenced; no transfer was prepared.")
            if saved["state"] in ("committed", "resumed"):
                current = bridge.store.get(issue_id)
                if (not current or current["owner_ref"] != saved["session_key"] or
                        current["ownership_id"] != saved["successor_owner"] or
                        current["stop_requested_at"] or current["release_pending"]):
                    return _reply(False, "Transferred ownership changed; the old receipt grants no authority.")
                return _ownership_receipt(saved)
            old = saved["predecessor"]
            retirement = retire(old["owner_ref"], old["run_generation"], request_id)
            if retirement.get("status") != "retired":
                return _reply(False, "Transfer intent is durable; physical predecessor work remains unverified.",
                              code="retirement_pending", transfer_id=request_id, predecessor_effects="unknown")
            if not bridge.authorize_specialist_effect(issue_id):
                return _reply(False, "Issue authorization changed after retirement; transfer remains fenced.")
            fresh = bridge.api.issue(issue_id)
            if ((fresh.get("state") or {}).get("type") in CLOSED or
                    (fresh.get("delegate") or {}).get("id") != bridge.api.viewer_id()):
                return _reply(False, "Linear ownership changed after retirement; transfer remains fenced.")
            result = bridge.store.commit_chat_transfer(request_id, retirement, at=bridge.clock())
            if not result:
                return _reply(False, "Stop, release, scope, or ownership changed; transfer remains fenced.")
            return _ownership_receipt(result)
    except (LinearError, ValueError, OSError) as exc:
        return _reply(False, f"Transfer cannot be confirmed: {exc}. Reuse the same request id.")
