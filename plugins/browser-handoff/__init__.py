"""Identity-bound browser handoff plugin and its broker HTTP transport."""
from __future__ import annotations

from datetime import datetime
import ipaddress
import json
import os
import threading
import time
import uuid
from dataclasses import asdict, is_dataclass
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo
import urllib.error
import urllib.request

try:
    from .policy import IssueRequest, issuance_allowed
except ImportError:
    from policy import IssueRequest, issuance_allowed

_IDENTITY_FIELDS = frozenset({
    "email", "profile", "agent", "user_id", "chat_id", "chat_type", "platform",
    "thread_id", "scope_id", "session_id", "session_key", "message_id", "ttl",
    "callback_url", "access_email", "browser_workspace_id",
})
_EXCLUDED_DESKTOP_IDENTITIES = frozenset({
    ("server-internal", "server-internal"),
})


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Do not send the bearer capability to a redirect target."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


# States in which the broker no longer reserves the browser for a human.
RELEASED_STATES = frozenset({"none", "ended", "expired"})
# States in which a human reply should checkpoint and release via agent-side end.
HELD_STATES = frozenset({"pending", "active", "ending", "recovery_required"})
# After blocking browser tools for this long, refresh the hold once via status
# using the remembered owner (core passes no invocation to pre_tool_call).
HOLD_REFRESH_AFTER = 60.0
HANDOFF_PROTOCOL_VERSION = 1
PLUGIN_PROTOCOL_HEADER = "X-Handoff-Plugin-Protocol-Version"


class BrokerHttpTransport:
    """Fail-closed client for the broker's internal mint endpoint.

    Besides the wire calls it remembers every handoff this process minted and
    has not yet seen terminate, so the pre_tool_call hook can keep browser_*
    tools away from a browser a human is using without asking the broker.
    """

    def __init__(self, base_url: str, capability_token: str, timeout: float = 5) -> None:
        self.base_url = _validated_broker_url(base_url)
        self.capability_token = capability_token
        self.timeout = timeout
        self._handles = {}
        self._held = {}
        self._handles_lock = threading.Lock()
        self._opener = urllib.request.build_opener(_NoRedirects())

    def start(self, invocation: Any, reason: str | None = None) -> dict[str, Any]:
        del reason
        payload: dict[str, Any] = {"invocation": _invocation_payload(invocation)}
        result = self._post("", payload, require_url=True)
        self._remember(invocation, result)
        self._hold(invocation, result)
        return result

    def status(self, invocation: Any) -> dict[str, Any]:
        result = self._post("/status", {"invocation": _invocation_payload(invocation)})
        self._remember(invocation, result)
        self._settle(invocation, result)
        return result

    def end(self, invocation: Any) -> dict[str, Any]:
        owner = self._owner_key(invocation)
        with self._handles_lock:
            sid = self._handles.get(owner)
        if sid is None:
            result = self.status(invocation)
            sid = result.get('session_id')
            if not sid:
                return result
        result = self._post('/end', {'invocation': _invocation_payload(invocation), 'session_id': sid})
        self._settle(invocation, result)
        return result

    def held_handoff(self) -> dict[str, Any] | None:
        """The handoff this process minted and has not seen released, if any.

        Marks when blocking began. After HOLD_REFRESH_AFTER seconds of blocking
        it refreshes once through status using the remembered owner, so a
        handoff ended elsewhere (or a restarted broker) releases the hold
        without the model having to call status itself.
        """
        now = time.monotonic()
        with self._handles_lock:
            for owner, entry in self._held.items():
                if entry.get("blocked_since") is None:
                    entry["blocked_since"] = now
                held = dict(entry, owner=owner)
                break
            else:
                return None
        if not held.get("refreshed") and now - held["blocked_since"] >= HOLD_REFRESH_AFTER:
            with self._handles_lock:
                entry = self._held.get(held["owner"])
                if entry is not None:
                    entry["refreshed"] = True
            self.status(dict(held["owner"]))
            with self._handles_lock:
                entry = self._held.get(held["owner"])
                return dict(entry, owner=held["owner"]) if entry is not None else None
        return held

    @staticmethod
    def _owner_key(invocation):
        return tuple(_invocation_payload(invocation).items())

    def _remember(self, invocation, result):
        sid = result.get('session_id') if isinstance(result, dict) else None
        if isinstance(sid, str):
            with self._handles_lock:
                self._handles[self._owner_key(invocation)] = sid

    def _hold(self, invocation, result):
        if not isinstance(result, dict) or result.get("ok") is False:
            return
        sid = result.get("session_id")
        if isinstance(sid, str) and sid:
            with self._handles_lock:
                self._held[self._owner_key(invocation)] = {"session_id": sid, "since": time.monotonic(), "blocked_since": None, "refreshed": False}

    def _settle(self, invocation, result):
        """Release the hold only on explicit evidence; errors keep it."""
        if not isinstance(result, dict) or result.get("ok") is False:
            return
        if result.get("state") in RELEASED_STATES:
            with self._handles_lock:
                self._held.pop(self._owner_key(invocation), None)

    def _post(self, suffix: str, payload: dict[str, Any], *, require_url: bool = False) -> dict[str, Any]:
        if not self.base_url or not self.capability_token:
            return {"ok": False, "error": "missing_broker"}
        request = urllib.request.Request(
            f"{self.base_url}/v1/handoffs{suffix}",
            data=json.dumps(payload, separators=(",", ":")).encode(),
            headers={"Authorization": f"Bearer {self.capability_token}", "Content-Type": "application/json", "Accept": "application/json",
                     PLUGIN_PROTOCOL_HEADER: str(HANDOFF_PROTOCOL_VERSION)},
            method="POST",
        )
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            # Only the authenticated status endpoint's exact idle response is
            # evidence of no handoff. A proxy or missing route can also be 404.
            if suffix == "/status" and exc.code == 404:
                try:
                    idle = exc.read(128) == b"no active handoff\n"
                except (OSError, TimeoutError):
                    idle = False
                finally:
                    exc.close()
                if idle:
                    return dict(NO_ACTIVE_HANDOFF)
            return {"ok": False, "error": "broker_redirect" if 300 <= exc.code < 400 else f"broker_http_{exc.code}"}
        except (urllib.error.URLError, OSError, TimeoutError):
            return {"ok": False, "error": "broker_unavailable"}
        try:
            result = json.loads(body.decode())
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {"ok": False, "error": "broker_invalid_response"}
        if not isinstance(result, dict) or (require_url and not isinstance(result.get("url"), str)):
            return {"ok": False, "error": "broker_invalid_response"}
        return result


def _validated_broker_url(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("missing broker URL")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("broker URL must be a bare internal http(s) origin")
    if parsed.path not in {"", "/"}:
        raise ValueError("broker URL must not contain a path")
    host = parsed.hostname.rstrip(".").lower()
    try:
        internal = not ipaddress.ip_address(host).is_global
    except ValueError:
        internal = host == "localhost" or host.endswith(".localhost") or host.endswith(".internal") or "." not in host
    if not internal:
        raise ValueError("broker URL must be internal")
    return parsed.geturl().rstrip("/")


def _invocation_payload(invocation: Any) -> dict[str, Any]:
    # scope_id is injected from the task-local gateway ContextVar, never from
    # model tool arguments. Slack uses it as its authenticated team binding.
    # browser_workspace_id is the same class of host identity: gateway/invocation
    # only. Missing or blank is the established default resource.
    fields = ("profile", "platform", "user_id", "chat_id", "chat_type", "thread_id", "scope_id")
    desktop_fields = ("browser_control_provider", "browser_control_subject")
    if isinstance(invocation, dict):
        raw = invocation
    elif is_dataclass(invocation):
        raw = asdict(invocation)
    else:
        raw = {field: getattr(invocation, field, "") for field in (*fields, *desktop_fields, "browser_workspace_id")}
    payload = {field: str(raw.get(field) or "") for field in fields}
    payload["browser_workspace_id"] = str(raw.get("browser_workspace_id") or "default")
    if payload["platform"] == "desktop":
        payload.update({field: str(raw.get(field) or "") for field in desktop_fields})
    return payload


NO_ACTIVE_HANDOFF: dict[str, Any] = {
    "ok": True,
    "state": "none",
    "active": False,
    "message": "No active browser handoff; browser tools may be used.",
}
END_BUTTON = "End takeover"
def _handoff_tz_name() -> str:
    value = (os.environ.get("HERMES_BROWSER_HANDOFF_TZ") or "UTC").strip()
    return value or "UTC"


def _public_handoff_host() -> str | None:
    host = (os.environ.get("HERMES_BROWSER_HANDOFF_PUBLIC_HOST") or "").strip().lower().rstrip(".")
    return host or None



def _local_time(expires_at: Any) -> str:
    try:
        return datetime.fromtimestamp(float(expires_at), ZoneInfo(_handoff_tz_name())).strftime("%-I:%M %p")
    except (TypeError, OverflowError, OSError, ValueError):
        return ""


def handoff_message(url: str, expires_at: Any = None, *, agent: str | None = None) -> str:
    """Ready-to-send copy for the human. Carries the scoped link; never a fragment."""
    link = (url or "").split("#", 1)[0]
    who = agent or "the agent"
    when = _local_time(expires_at)
    lines = [
        "*Browser sign-in needed*",
        f"{who} needs you to finish a sign-in in its browser. Open this on a laptop, in Chrome or Safari, not Slack's preview:",
        link,
        "1. Choose the Google account this link was sent to",
        "2. Sign in on the page you see. Paste works: copy from your password manager and press Ctrl+V (Cmd+V on a Mac)",
        "3. If the site offers a passkey or push approval, pick a code or SMS instead",
        f"4. Reply here when you're done so {who} can carry on. You can also press *{END_BUTTON}* in the viewer",
    ]
    if when:
        lines.append(f"The link expires at {when}. Reply here if you need a new one.")
    lines.append("Never send passwords or codes in this chat.")
    return "\n".join(lines)


NEXT_STEP = (
    "Call the clarify tool with questions=[{\"question\": user_message}] (no choices) so this turn blocks until the human replies; "
    "if clarify is unavailable on this platform, send user_message and end the turn instead. "
    "When the reply arrives, call browser_handoff_status once. A chat reply is the unlock signal on every platform. "
    "browser_handoff_status checkpoints and releases if the handoff is still held. "
    "If state is \"none\" or \"ended\", continue with browser tools. "
    "If the handoff is still held after that check, call browser_handoff_end, then status once more. "
    "Do not ask the human to press End takeover. "
    "Other errors do not confirm release. Never ask for passwords or codes in chat."
)


def _with_user_message(result: dict[str, Any], invocation_context: Any) -> dict[str, Any]:
    url = result.get("url") if isinstance(result, dict) else None
    if not isinstance(url, str) or not url:
        return result
    agent = _invocation_payload(invocation_context).get("profile") or None
    enriched = dict(result)
    enriched["user_message"] = handoff_message(url, result.get("expires_at"), agent=agent)
    enriched["next_step"] = NEXT_STEP
    return enriched


def _result(value: dict[str, Any]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _identity_args(args: dict[str, Any]) -> bool:
    return any(key in _IDENTITY_FIELDS for key in args)


def _allowed(
    args: dict[str, Any], invocation_context: Any,
    desktop_authorized_subjects: tuple[tuple[str, str, str], ...] = (),
) -> tuple[bool, str]:
    if _identity_args(args):
        return False, "model_supplied_identity"
    return issuance_allowed(IssueRequest(invocation_context, None, None, desktop_authorized_subjects))


def _valid_handoff_url(value: Any, invocation: Any) -> bool:
    """Accept only the fleet's canonical public URL for this trusted agent."""
    if not isinstance(value, str):
        return False
    agent = _invocation_payload(invocation)["profile"]
    host = _public_handoff_host()
    if host is None:
        return False
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    if (
        parsed.scheme != "https"
        or parsed.hostname != host
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return False
    segments = parsed.path.split("/")
    workspace = _invocation_payload(invocation)["browser_workspace_id"]
    if workspace == "default":
        if len(segments) != 3 or segments[0] or segments[1] != agent:
            return False
        session_id = segments[2]
    else:
        if len(segments) != 4 or segments[0] or segments[1] != agent or segments[2] != workspace:
            return False
        session_id = segments[3]
    try:
        return str(uuid.UUID(session_id)) == session_id
    except (ValueError, AttributeError):
        return False


def start_handoff(args: dict[str, Any] | None = None, *, invocation_context: Any = None, broker: Any | None = None, desktop_authorized_subjects: tuple[tuple[str, str, str], ...] = (), **_: Any) -> str:
    args = args or {}
    ok, reason = _allowed(args, invocation_context, desktop_authorized_subjects)
    if not ok:
        return _result({"ok": False, "error": reason})
    if broker is None:
        return _result({"ok": False, "error": "missing_broker"})
    result = broker.start(invocation_context)
    if not isinstance(result, dict):
        return _result({"ok": False, "error": "broker_invalid_response"})
    if result.get("ok") is not False and not _valid_handoff_url(result.get("url"), invocation_context):
        return _result({"ok": False, "error": "broker_invalid_response"})
    return _result(_with_user_message(result, invocation_context))


def _status_then_release(broker: Any, invocation_context: Any) -> dict[str, Any]:
    """Inspect, then checkpoint-and-end if a human reply found the session still held.

    BrokerHttpTransport.status stays inspect-only: the hold-refresh path uses it
    without unlocking while the human is still in the viewer.
    """
    result = broker.status(invocation_context)
    if not isinstance(result, dict):
        return {"ok": False, "error": "broker_invalid_response"}
    if result.get("ok") is False:
        return result
    if result.get("state") in RELEASED_STATES:
        return result
    if result.get("state") in HELD_STATES and hasattr(broker, "end"):
        ended = broker.end(invocation_context)
        if not isinstance(ended, dict):
            return {"ok": False, "error": "broker_invalid_response"}
        return ended
    return result


def status_handoff(args: dict[str, Any] | None = None, *, invocation_context: Any = None, broker: Any | None = None, desktop_authorized_subjects: tuple[tuple[str, str, str], ...] = (), **_: Any) -> str:
    args = args or {}
    ok, reason = _allowed(args, invocation_context, desktop_authorized_subjects)
    if not ok:
        return _result({"ok": False, "error": reason})
    if broker is None or not hasattr(broker, "status"):
        return _result({"ok": False, "error": "status_unavailable"})
    return _result(_status_then_release(broker, invocation_context))


def end_handoff(args: dict[str, Any] | None = None, *, invocation_context: Any = None, broker: Any | None = None, desktop_authorized_subjects: tuple[tuple[str, str, str], ...] = (), **_: Any) -> str:
    args = args or {}
    ok, reason = _allowed(args, invocation_context, desktop_authorized_subjects)
    if not ok:
        return _result({"ok": False, "error": reason})
    if broker is None or not hasattr(broker, "end"):
        return _result({"ok": False, "error": "end_unavailable"})
    return _result(broker.end(invocation_context))


def guard_browser_tools(tool_name: str = "", args: Any = None, *, broker: Any | None = None, **kwargs: Any) -> dict[str, str] | None:
    """pre_tool_call: keep browser_* tools off a browser a human is using.

    Blocks while this process holds a handoff it minted and has not seen
    released. Never calls the broker per browser call; fails open without a
    transport (the REST lock proxy is defence in depth).
    """
    if not isinstance(tool_name, str) or not tool_name.startswith("browser_") or tool_name.startswith("browser_handoff_"):
        return None
    if broker is None or not hasattr(broker, "held_handoff"):
        return None
    held = broker.held_handoff()
    if held is None:
        return None
    return {
        "action": "block",
        "message": (
            f"A human currently has this browser (handoff {held['session_id']}). "
            "Call browser_handoff_status after the user replies; do not use browser tools until it reports state none."
        ),
    }


def _config(ctx: Any, key: str, default: Any = None) -> Any:
    getter = getattr(ctx, "get_config", None)
    return getter(key, default) if callable(getter) else default


def _desktop_authorized_subjects(ctx: Any) -> tuple[tuple[str, str, str], ...]:
    """Read the renderer-generated Desktop projection; absent or malformed is deny-all.

    This deliberately does not accept a config list: that would create a second,
    drifting authorization source beside the broker principal artifact.
    """
    path = _config(ctx, "desktop_authorized_subjects_file", "")
    if not isinstance(path, str) or not path:
        return ()
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return ()
    if not isinstance(raw, list):
        return ()
    parsed: list[tuple[str, str, str]] = []
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"profile", "provider", "subject"}:
            return ()
        profile, provider, subject = (item[key] for key in ("profile", "provider", "subject"))
        if (
            not all(isinstance(value, str) and value.strip() == value and value != "*" and not any(char.isspace() or ord(char) < 32 for char in value) and len(value) <= 256 for value in (profile, provider, subject))
            or (provider, subject) in _EXCLUDED_DESKTOP_IDENTITIES
            or (profile, provider, subject) in parsed
        ):
            return ()
        parsed.append((profile, provider, subject))
    return tuple(parsed)


def _read_capability_file(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        token = Path(value).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None
    return token if token and not any(char.isspace() for char in token) else None


def _broker_from_context(ctx: Any) -> BrokerHttpTransport | None:
    # This is an explicit rollout acknowledgement, not a default. The broker
    # separately verifies this running plugin emits its protocol header, so a
    # rendered config value alone cannot activate scoped handoff semantics.
    if _config(ctx, "handoff_protocol_version") != HANDOFF_PROTOCOL_VERSION:
        return None
    base_url = os.environ.get("BROWSER_HANDOFF_URL") or _config(ctx, "broker_url", "")
    cap_file = os.environ.get("BROWSER_HANDOFF_CAP_FILE") or _config(ctx, "capability_file", "")
    token = _read_capability_file(cap_file)
    if not isinstance(base_url, str) or not token:
        return None
    timeout = _config(ctx, "broker_timeout", 5)
    try:
        return BrokerHttpTransport(base_url, token, float(timeout))
    except (TypeError, ValueError):
        return None


def _schema(name: str, description: str, properties: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, "description": description, "parameters": {"type": "object", "properties": properties, "additionalProperties": False}}


def register(ctx: Any) -> None:
    broker = _broker_from_context(ctx)
    desktop_authorized_subjects = _desktop_authorized_subjects(ctx)
    definitions = (
        (
            "browser_handoff_start",
            "Start an identity-bound browser handoff so the human can sign in or finish a step in this agent's browser. "
            "Use it when a page needs a login, MFA, CAPTCHA or a decision only the human can make. "
            "Pass the returned user_message verbatim as the question of a clarify call (questions=[{\"question\": user_message}]) "
            "so the turn blocks until the human replies; if clarify is unavailable, send user_message and end the turn. "
            "When they reply, call browser_handoff_status once before any browser action; "
            "a chat reply unlocks the browser. browser_* tools are blocked while the handoff is outstanding. "
            "Never ask the user for a password or code in chat.",
            {"reason": {"type": "string", "description": "Short note for logs about why the handoff is needed. Not shown to the human."}},
            start_handoff,
        ),
        (
            "browser_handoff_status",
            "Check once, after the human answers the clarify prompt or replies in this thread. "
            "A chat reply is the unlock signal on every platform; do not ask them to press End takeover. "
            "If the handoff is still held, this tool checkpoints and releases it. "
            "state \"none\" or \"ended\" (ok: true) means the browser is free and you can continue. "
            "If it is still pending, active, ending, or recovery_required after that, do not use browser_* tools; call browser_handoff_end. "
            "Other errors do not confirm release.",
            {},
            status_handoff,
        ),
        (
            "browser_handoff_end",
            "End the current browser handoff from the agent side (checkpoint then release). "
            "Use after a human reply if status did not already release, or if they ask you to end it. "
            "The viewer End takeover button is optional.",
            {},
            end_handoff,
        ),
    )
    for name, description, properties, handler in definitions:
        ctx.register_tool(name=name, toolset="browser_handoff", schema=_schema(name, description, properties), handler=partial(handler, broker=broker, desktop_authorized_subjects=desktop_authorized_subjects), inject_invocation_context=True)
    ctx.register_hook("pre_tool_call", partial(guard_browser_tools, broker=broker))
