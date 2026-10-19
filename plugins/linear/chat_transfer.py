"""User-authorized control handoff with a separate execution admission decision."""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime
from pathlib import Path

from .api import LinearError
from .bridge import CLOSED
from .store import SharedChatOwner


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
    if not isinstance(text, str) or not text:
        return False
    issue = re.escape(identifier.casefold())
    if resume:
        pattern = (rf"i(?:'ve| have)?\s+(?:reconciled|reviewed(?:\s+effects(?:\s+(?:for|of))?)?)\s+"
                   rf"{issue}\s+(?:at|against)\s+https://[^\s;]+\s+(?:and|;)\s*"
                   r"(?:please\s+)?(?:resume|continue|proceed)(?:\s+(?:here|now|work))?[.!]?")
    else:
        pattern = (r"(?:please\s+|i\s+(?:want|would like|authorize|request|ask)\s+"
                   r"(?:you\s+)?to\s+)?(?:transfer|move|handoff|hand\s+over|take\s+over)\s+"
                   rf"(?:issue\s+)?{issue}\s+(?:(?:to|into)\s+)?"
                   r"(?:(?:this|current|new)\s+(?:chat|thread|conversation)|here)[.!]?")
    return re.fullmatch(pattern, text.casefold().strip()) is not None


def _identity(bridge, context):
    home = getattr(bridge, "profile_home", None) or getattr(bridge.kanban, "profile_home", None)
    if home is None:
        return None
    home = str(Path(home).resolve(strict=True))
    platform = getattr(context, "platform", "")
    ui = getattr(context, "ui_session_id", "") if platform == "desktop" else ""
    api = [getattr(context, "api_control_principal", ""),
           getattr(context, "api_control_transport_family", "")] if platform == "api_server" else ["", ""]
    native = None
    if platform not in ("desktop", "api_server"):
        native = [getattr(context, field, "") for field in
                  ("user_id", "chat_id", "chat_type", "thread_id", "scope_id")]
        if not all(isinstance(value, str) for value in native) or not native[0] or not native[1]:
            return None
    destination = (ui if platform == "desktop" else
                   getattr(context, "api_conversation_key", "") if platform == "api_server" else
                   getattr(context, "session_key", ""))
    return {"profile": bridge.profile, "home": home,
            "session_key": getattr(context, "session_key", ""),
            "session_id": getattr(context, "session_id", ""),
            "platform": platform, "destination_id": destination, "ui_session_id": ui,
            "api_identity": api,
            "browser_identity": [getattr(context, "browser_control_provider", ""),
                                 getattr(context, "browser_control_subject", "")],
            **({"native_identity": native} if native is not None else {})}


def _ownership_receipt(saved):
    return _reply(True, "Ownership moved to this chat. Reconcile predecessor effects, then explicitly "
                  "resume this transfer before any issue work; no work was replayed.",
                  code="ownership_transferred", transfer_id=saved["id"],
                  owner_id=saved["successor_owner"], execution_admitted=False,
                  retirement_sha256=_retirement_sha256(saved["retirement"]),
                  predecessor_effects=saved["retirement"]["effects"])


def _retirement_sha256(receipt):
    return hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _reconciliation_decision(bridge, issue, saved, evidence):
    """Read a later, issue-bound human decision from Linear, not caller prose."""
    creator = (issue.get("creator") or {}).get("id")
    assignee = (issue.get("assignee") or {}).get("id")
    allowed = set((getattr(bridge.api, "specialist_scope", None) or {}).get("allowed_requester_ids", ()))
    if creator:
        allowed.add(creator)
    if assignee:
        allowed.add(assignee)
    allowed.discard(bridge.api.viewer_id())
    if not allowed:
        return None
    expected = {"kind": "chat-transfer-reconciliation/v1", "issue_id": saved["issue_id"],
                "transfer_id": saved["id"], "predecessor_owner": saved["predecessor"]["ownership_id"],
                "retirement_sha256": _retirement_sha256(saved["retirement"]),
                "decision": "resume_after_review", "evidence": evidence}
    for comment in reversed(bridge.api.issue_thread(saved["issue_id"])):
        if comment.get("user_id") not in allowed:
            continue
        try:
            created = datetime.fromisoformat(comment["at"].replace("Z", "+00:00"))
            body = json.loads(comment["body"])
        except (KeyError, ValueError, TypeError, AttributeError):
            continue
        if (created.tzinfo is not None and created.timestamp() > saved["committed_at"]
                and body == expected):
            return {"comment_at": comment["at"], "author_id": comment["user_id"], **expected}
    return None


def handle(bridge, args, context):
    request_id = args.get("id")
    if not _uuid4(request_id):
        return _reply(False, "Transfer needs a canonical UUIDv4 id; reuse it on retry.")
    ref = args.get("issue")
    if not isinstance(ref, str) or not ref or ref != ref.strip():
        return _reply(False, "Transfer needs an exact issue identifier.")
    retire = getattr(bridge, "retire_chat", None)
    lineage = getattr(bridge, "chat_lineage", None)
    if not callable(retire) or not callable(lineage):
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
            if not resume:
                predecessor = bridge.store.get(issue_id)
                if predecessor and predecessor["origin"] == "chat":
                    keys = tuple(lineage(predecessor["owner_ref"]))
                    if not keys or keys[0] != predecessor["owner_ref"]:
                        return _reply(False, "Predecessor physical chat lineage could not be verified.",
                                      code="lineage_unverified")
                    if len(keys) != 1:
                        return _reply(False, "A compressed predecessor may share another issue. "
                                      "Transfer it only after the chat lineage is isolated.",
                                      code="compressed_predecessor")
            if resume:
                evidence = args.get("evidence")
                if (not isinstance(evidence, str) or not evidence.startswith("https://") or
                        evidence not in instruction):
                    return _reply(False, "Resume needs a reconciliation evidence link supplied in the "
                                  "authenticated user instruction.")
                saved = bridge.store.rebind_chat_transfer(request_id, identity, issue_id)
                if not saved:
                    return _reply(False, "Transfer receipt does not belong to this issue and chat.")
                previous = saved.get("reconciliation") if saved["state"] == "resumed" else None
                decision = (previous if isinstance(previous, dict) and
                            previous.get("evidence") == evidence else
                            _reconciliation_decision(bridge, fresh, saved, evidence))
                if not decision:
                    return _reply(False, "A later issue-bound human reconciliation decision is required.",
                                  code="reconciliation_required")
                if not bridge.authorize_specialist_effect(issue_id):
                    return _reply(False, "Issue authorization changed during reconciliation.",
                                  code="reconciliation_required")
                current = bridge.api.issue(issue_id)
                if (current.get("id") != issue_id or
                        (current.get("state") or {}).get("type") in CLOSED or
                        (current.get("delegate") or {}).get("id") != bridge.api.viewer_id()):
                    return _reply(False, "Issue authority changed during reconciliation.",
                                  code="reconciliation_required")
                result = bridge.store.resume_chat_transfer(request_id, evidence=decision, at=bridge.clock())
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
    except SharedChatOwner:
        return _reply(False, "This predecessor chat owns another issue. Finish or release that issue "
                      "before retrying; no transfer was prepared.", code="shared_predecessor")
    except (LinearError, ValueError, OSError) as exc:
        return _reply(False, f"Transfer cannot be confirmed: {exc}. Reuse the same request id.")
