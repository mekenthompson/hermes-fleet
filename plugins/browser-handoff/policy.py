"""Issuance policy. The model cannot choose identity."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IssueRequest:
    invocation: Any | None
    requested_email: str | None
    requested_profile: str | None
    # Explicit fleet policy tuples: (agent profile, verified provider, subject).
    # No display name or model argument is an identity. A dashboard session
    # token is admitted only when the rendered policy maps it onto a person.
    desktop_authorized_subjects: tuple[tuple[str, str, str], ...] = ()


REQUIRED = ("profile", "platform", "user_id", "chat_id", "chat_type")
_EXCLUDED_DESKTOP_IDENTITIES = frozenset({
    ("server-internal", "server-internal"),
})


def _value(invocation: Any, key: str) -> Any:
    if isinstance(invocation, dict):
        return invocation.get(key)
    return getattr(invocation, key, None)


def issuance_allowed(req: IssueRequest) -> tuple[bool, str]:
    if req.requested_email or req.requested_profile:
        return False, "model_supplied_identity"
    inv = req.invocation
    if inv is None:
        return False, "missing_invocation_context"
    if _value(inv, "platform") == "desktop":
        profile = str(_value(inv, "profile") or "")
        provider = str(_value(inv, "browser_control_provider") or "")
        subject = str(_value(inv, "browser_control_subject") or "")
        session_id = str(_value(inv, "session_id") or "")
        if not all((profile.strip(), provider.strip(), subject.strip(), session_id.strip())):
            return False, "missing_invocation_context"
        if (provider, subject) in _EXCLUDED_DESKTOP_IDENTITIES:
            return False, "unauthorized_desktop_subject"
        if (profile, provider, subject) not in req.desktop_authorized_subjects:
            return False, "unauthorized_desktop_subject"
        return True, "ok"
    if any(not str(_value(inv, key) or "").strip() for key in REQUIRED):
        return False, "missing_invocation_context"
    return True, "ok"
