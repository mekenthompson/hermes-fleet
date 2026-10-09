"""Durable session-free delegation from a live gateway chat turn."""
from __future__ import annotations
import json
import uuid
from pathlib import Path
from .api import LinearError
import sqlite3
from contextlib import closing
from .bridge import CLOSED, TASK_BODY


def bound_to_live_turn(runtime, context):
    """Verify the immutable host snapshot against the current gateway turn.

    This is an in-process boundary, not a sandbox against arbitrary same-UID
    code. Serialized chat RPC requests deliberately cannot acquire this grant.
    """
    from tools import registry
    if not isinstance(context, registry.ToolInvocationContext):
        return False
    if context != registry._current_tool_invocation_context():
        return False
    if (not context.profile or not context.session_key or not context.session_id or
            not context.platform or not context.chat_id or
            type(context.run_generation) is not int or context.run_generation < 1):
        return False
    try:
        gateway = runtime.gateway
        state = gateway._peek_session_state(context.session_key)
        if (state is None or state.turn.agent is None or context.profile != runtime.profile_name or
                state.persistent.run_generation != context.run_generation or
                not gateway._chat_stop_profile_matches(context.session_key, runtime.profile_home) or
                not gateway._is_user_authorized_for_source(state.turn.event.source)):
            return False
        turn = state.turn.ctx
        if (turn.session_key != context.session_key or turn.session_id != context.session_id or
                turn.run_generation != context.run_generation or not callable(turn._run_still_current) or
                not turn._run_still_current()):
            return False
        source = state.turn.event.source
        if source.platform.value != context.platform or any(
                (getattr(source, key, None) or "") != (getattr(context, key, None) or "")
                for key in ("chat_id", "thread_id", "chat_type", "user_id", "scope_id")):
            return False
        home = Path(runtime.profile_home).resolve(strict=True)
        with closing(sqlite3.connect(f"file:{home / 'state.db'}?mode=ro", uri=True, timeout=5)) as db:
            db.execute("PRAGMA query_only=ON")
            row = db.execute("SELECT ended_at FROM sessions WHERE id=?", (context.session_id,)).fetchone()
        return row is not None and row[0] is None
    except (AttributeError, OSError, ValueError, sqlite3.Error):
        return False


def handle(bridge, args, context):
    def reply(ok, text):
        return json.dumps({"ok": ok, "message": text})
    validate = getattr(bridge, "validate_chat_admission", None)
    if not callable(validate) or not validate(context):
        return reply(False, "delegate requires a live gateway-owned chat turn; no change was made.")
    request = args.get("id")
    try:
        if not isinstance(request, str) or str(uuid.UUID(request)) != request:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        return reply(False, "delegate needs a canonical request UUID in id; reuse it on retry.")
    generation = getattr(context, "run_generation", None)
    if type(generation) is not int or generation < 1:
        return reply(False, "delegate requires a current run generation.")
    home = str(Path(bridge.kanban.profile_home).resolve(strict=True))
    route = {key: getattr(context, key, "") for key in
             ("platform", "chat_id", "thread_id", "chat_type", "user_id", "scope_id")}
    ref = args.get("issue")
    if not isinstance(ref, str) or not ref or ref != ref.strip():
        return reply(False, "delegate needs an exact issue identifier.")
    try:
        issue = bridge.api.issue(ref)
        if ref not in (issue.get("id"), issue.get("identifier")):
            raise LinearError("Issue resolution mismatch", retryable=False)
        issue_id = issue["id"]
        with bridge.lock(issue_id):
            prior = bridge.store.chat_admission(request)
            if prior:
                if any(prior[key] != value for key, value in (
                        ("issue_id", issue_id), ("profile", bridge.profile), ("home", home),
                        ("parent_session_key", context.session_key), ("parent_session_id", context.session_id),
                        ("run_generation", generation), ("route", route))):
                    return reply(False, "Request identity conflicts; no change was made.")
                if prior["state"] in {"rejected", "ambiguous"}:
                    return reply(False, "Admission is fenced or belongs to an expired run; reconcile it, do not replay.")
            bridge.await_retired(issue_id)
            if not bridge.authorize_specialist_effect(issue_id):
                return reply(False, "Issue authorization is fenced or unavailable.")
            fresh = bridge.api.issue(issue_id)
            if (fresh.get("state") or {}).get("type") in CLOSED:
                return reply(False, "Closed issues cannot be delegated.")
            if prior and prior["state"] == "admitted":
                work = bridge.store.get(issue_id)
                if (not work or work["origin"] != "kanban_chat" or
                        work.get("task_id") != prior["task_id"] or work.get("ownership_id") != prior["ownership_id"] or
                        not bridge.may_execute_existing(issue_id)):
                    return reply(False, "Existing admission authority changed; reconcile without replay.")
                return reply(True, f"Already admitted as task {prior['task_id']}; no new executor created.")
            if not prior and ((fresh.get("delegate") or {}).get("id") or bridge.store.get(issue_id)):
                return reply(False, "delegate requires a fresh unowned issue; existing work is not transferred.")
            admission = prior or bridge.store.prepare_chat_admission(
                request, issue_id, bridge.profile, home, context.session_key, context.session_id,
                generation, route, bridge.clock())
            if not admission:
                return reply(False, "Admission conflicts with existing work or retirement.")
            task = bridge.kanban.get(admission["task_id"]) if admission.get("task_id") else None
            if task is None:
                task = bridge.kanban.create(
                    title=f"{fresh.get('identifier') or issue_id}: {fresh.get('title') or 'Linear issue'}",
                    assignee=bridge.profile, created_by="linear", initial_status="blocked", max_retries=0,
                    idempotency_key=f"linear-chat-admission:{request}",
                    body=TASK_BODY.format(ident=fresh.get("identifier") or issue_id,
                                         url=fresh.get("url", ""), context=fresh.get("description") or ""),
                    completion_contract=bridge.contracts.get((fresh.get("project") or {}).get("id")))
                if not bridge.store.bind_chat_admission(request, task.id,
                        (fresh.get("project") or {}).get("id"), bridge.clock() * 1000):
                    bridge.store.transition_chat_admission(request, admission["state"], "ambiguous",
                        task_id=task.id, reason_code="binding_refused")
                    return reply(False, "Admission binding refused; the blocked task is retained for reconciliation.")
            bridge.kanban.subscribe(task.id, issue_id)
            bridge.kanban.subscribe_parent(task.id, bridge.profile, route)
            claim_id = bridge.store.enqueue_once("status", {
                "issue_id": issue_id, "owner_issue_id": issue_id, "task_id": task.id,
                "state": "in_progress", "claim": True, "terminal": True,
                "work_owner": admission["ownership_id"], "admission_id": request}, f"chat-admission:{request}:claim", at=bridge.clock())
        active = bridge.__dict__.setdefault("_active_admission_claims", {})
        active[request] = lambda: validate(context)
        try:
            bridge.flush()
        finally:
            active.pop(request, None)
        with bridge.lock(issue_id):
            claim = bridge.store.outbox_row(claim_id)
            if not bridge.store.terminal_status_applied(claim_id):
                if claim and claim["payload"].get("write_started"):
                    bridge.store.transition_chat_admission(request, "claiming", "ambiguous", reason_code="claim_uncertain")
                return reply(False, f"Task {task.id} remains blocked until claim reconciliation; reuse this request.")
            if not validate(context) or not bridge.may_execute_existing(issue_id):
                return reply(False, "Authority changed; the same task stays blocked for reconciliation.")
            if task.status == "blocked" and not bridge.kanban.unblock(task.id):
                return reply(False, "Claim captured but task activation refused; reconcile the same task.")
            if not bridge.store.transition_chat_admission(request, "claiming", "admitted", task_id=task.id):
                return reply(False, "Admission state changed; reconcile the same task.")
            bridge.store.enqueue_once("comment", {"issue_id": issue_id, "task_id": task.id,
                "body": f"Chat delegated work to Kanban task {task.id}; the original chat remains the subscriber."},
                f"chat-admission:{request}:ack", at=bridge.clock())
            return reply(True, f"Delegated as Kanban task {task.id}; the worker executes and this chat receives updates.")
    except (LinearError, ValueError, OSError):
        return reply(False, "Admission could not be confirmed; reconcile the same request before retrying.")
