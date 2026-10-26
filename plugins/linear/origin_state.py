"""Read-only projection of one native request; imports never activate a runtime or credentials."""
from __future__ import annotations
import json
from pathlib import Path
from .transport import NativeContext, Unavailable

INTERACTIVE_SOURCES = frozenset({"cli", "tui", "gui", "acp"})


def stop_evidence(home: Path, session_id: str) -> dict:
    """Read the existing core cleanup checkpoint when supported; absence is never false."""
    from gateway import session_ancillary
    getter = getattr(session_ancillary, "process_stop_pending", None)
    if getter is None:
        return {"state": "unknown", "source": "native_stop_checkpoint_unavailable", "quiescence_proven": False}
    from hermes_state import SessionDB
    with SessionDB(home / "state.db", read_only=True) as db:
        pending = getter(db, session_id)
    if pending is not None:
        _refuse("The native owner has a pending Stop checkpoint; current origin evidence is refused.")
    return {"state": "clear", "source": "native_process_stop_checkpoint", "quiescence_proven": False}


def _refuse(message="The originating native request is not current and independently verifiable."):
    raise Unavailable(message)


def resolve_reference(home: Path, ref: dict) -> str:
    """Resolve private logical coordinates only from this owner's exact native admission."""
    from hermes_state import SessionDB
    from hermes_state_runtime import get_session_admission
    with SessionDB(home / "state.db", read_only=True) as db:
        row = get_session_admission(db, admission_id=ref["admission_id"])
        if (row is None or row["status"] != "started" or row["outcome"] is not None
                or row["owner_epoch"] != ref["authority_epoch"]
                or row["generation"] != ref["execution_generation"]):
            _refuse()
        sid = row["target_session_id"]
        if not isinstance(sid, str) or not sid or len(sid) > 4096:
            _refuse()
        return sid


def _policy(db, session_id: str, platform: str) -> dict:
    if platform != "local":
        return {"source": platform}
    from hermes_state_local import local_receipt
    from gateway.session_policy import restore_policy
    policy = restore_policy(local_receipt(db, session_id)["policy"])
    if policy.source not in INTERACTIVE_SOURCES:
        _refuse("Producer and peer sessions cannot supply a human origin reference.")
    return {"source": policy.source, "cwd": policy.cwd, "toolsets": list(policy.toolsets),
            "safe_mode": policy.safe_mode}


def _evidence(home: Path, ref: dict, current: dict, declared_scope: dict,
              context: NativeContext | None) -> dict:
    """Read exactly one native admission, using the owner's native read-only state API."""
    from hermes_state import SessionDB
    from hermes_state_runtime import get_session_admission
    from gateway.session_operator import is_viewer_input
    from agent.turn_author import parse_turn_author
    with SessionDB(home / "state.db", read_only=True) as db:
        sid = current["session_id"]
        row = get_session_admission(db, admission_id=ref["admission_id"])
        if (row is None or row["target_session_id"] != sid
                or row["request_id"] != current["input_id"] or row["status"] != "started"
                or row["outcome"] is not None or row["owner_epoch"] != ref["authority_epoch"]
                or row["generation"] != ref["execution_generation"]):
            _refuse()
        payload = row["payload"]
        text = payload.get("text")
        if not isinstance(text, str) or not text.strip() or len(text) > 16384 or text != current.get("text"):
            _refuse("The exact original native text cannot be inspected within this bounded request.")
        for key in ("local_automation_v1", "cron_v1", "kanban_card_v1"):
            if key in payload:
                _refuse("Automation cannot supply a human origin reference.")
        for key in ("cli_turn_v1", "api_turn_v1"):
            author = parse_turn_author((payload.get(key) or {}).get("turn_author"))
            if author and author.get("is_bot"):
                _refuse("Bot-authored input cannot supply a human origin reference.")
        requester = {"principal_id": row["principal_id"]}
        native = payload.get("native_text_v1")
        if isinstance(native, dict):
            source, event = native["source"], native["event"]
            platform = source.get("platform")
            identity = [source.get(k) for k in ("profile", "platform", "chat_id", "thread_id", "user_id")]
            if (platform not in {"telegram", "slack"} or source.get("is_bot") is not False
                    or not source.get("user_id") or event.get("allow_gateway_control") is not True
                    or "automation" in native or event.get("inherit_channel_pin") is True
                    or row["principal_id"] != "messaging:" + json.dumps(identity, separators=(",", ":"))):
                _refuse("The admission is not ordinary authenticated human messaging input.")
            requester.update(kind="messaging", platform=platform, user_id=source["user_id"],
                             chat_id=source.get("chat_id"), thread_id=source.get("thread_id"),
                             scope_id=source.get("scope_id"))
            if context and (context.platform != platform or context.session_key != native.get("route")
                            or any(getattr(context, k) != str(source.get(k) or "")
                                   for k in ("user_id", "chat_id", "thread_id", "scope_id"))):
                _refuse("The captured caller does not match the admitted native message.")
        elif "api_turn_v1" in payload:
            from gateway.session_api_turn import _valid_transfer_auth
            data = payload["api_turn_v1"]
            auth = data.get("transfer_auth")
            if (row["principal_id"] != "api" or not _valid_transfer_auth(auth)
                    or (data.get("settings") or {}).get("room_dispatch") is not None
                    or (data.get("settings") or {}).get("room_execution_policy") is not None):
                _refuse("The API admission has no independently attributable ordinary credential principal.")
            platform = "api_server"
            requester.update(kind="api_credential", **auth)
            if context and context.platform != platform:
                _refuse()
        elif is_viewer_input(row):
            from hermes_state_local import local_receipt
            platform = "local"
            stamp = payload.get("local_operator_v1")
            owner = local_receipt(db, sid)
            if (stamp is None and row["principal_id"] != owner.get("principal_id")
                    or stamp is not None and stamp != {"profile_id": str(home), "session_id": sid,
                                                       "principal_id": row["principal_id"]}):
                _refuse()
            requester.update(kind="local_principal", platform=platform)
            if context and context.platform != platform:
                _refuse()
        else:
            _refuse("The admission is not ordinary native requester input.")
        stored = db.get_session(sid)
        if not stored or stored.get("source") not in ({platform} if platform != "local" else INTERACTIVE_SOURCES | {"local"}):
            _refuse("The stored session is not an eligible requester surface.")
        policy = _policy(db, sid, platform)
        policy["profile"] = ref["profile"]
        if platform != "local" and stored.get("cwd"):
            policy["cwd"] = stored["cwd"]
    return {"reference": ref, "requester": requester, "original_message": text,
            "policy": policy, "scope": declared_scope, "permission_granted": False}


def read_current(home: Path, session_id: str) -> tuple[dict, dict]:
    """Only this session's lineage and admission state; no transcript, config or tool inventory."""
    from hermes_state import SessionDB
    from hermes_state_runtime import list_session_admissions
    with SessionDB(home / "state.db", read_only=True) as db:
        lineage = db.get_compression_lineage(session_id)
        canonical = lineage[0] if lineage else session_id
        session = db.get_session(canonical)
        if session is None:
            _refuse()
        # Native steering transfers are terminal rows held against the active generation.
        # The native list API includes them only with pending_only=False. Never return them.
        rows = list_session_admissions(db, session_id=canonical, pending_only=False)
        generation = session.get("runtime_generation")
        started = [r for r in rows if r["status"] == "started" and r.get("generation") == generation]
        if type(generation) is not int or generation < 1 or len(started) != 1:
            _refuse("The originating turn is not current, or has ambiguous steering.")
        row = started[0]
        # Authenticate this owner/generation only. Historical uncertainty remains the native
        # owner's responsibility; it is not a blanket refusal of a newly admitted request.
        same_claim = [r for r in rows if r.get("generation") == generation and r.get("owner_epoch") == row["owner_epoch"]]
        if (row["outcome"] is not None or any(r["status"] == "unknown"
                or r.get("outcome") in {"steering", "steered"} for r in same_claim)):
            _refuse()
        snapshot = {"session_id": canonical, "authority_epoch": row["owner_epoch"], "execution_generation": generation}
        current = {"admission_id": row["admission_id"], "input_id": row["request_id"],
                   "authority_epoch": row["owner_epoch"], "execution_generation": row["generation"],
                   "text": row["payload"].get("text")}
        return snapshot, current
