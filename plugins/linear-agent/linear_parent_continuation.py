"""Fail-closed continuation into the owning parent conversation.

This POC primitive does not create Linear Agent Sessions, mutate issues, or
forward untrusted text into an arbitrary chat. It only decides whether work
may continue on an already-owned parent session.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ParentOwner:
    issue_id: str
    hermes_session_id: str
    linear_session_id: str
    generation: str
    mode: str


@dataclass(frozen=True)
class ParentWake:
    action: str
    hermes_session_id: str = ""
    linear_session_id: str = ""
    child_issue_id: str = ""
    reason: str = ""


@dataclass(frozen=True)
class FollowupDecision:
    action: str
    redirect_linear_session_id: str = ""
    reason: str = ""


def _active_owner(owner: ParentOwner | None) -> ParentOwner | None:
    if owner is None:
        return None
    if owner.mode != "active":
        return None
    if not owner.issue_id or not owner.hermes_session_id or not owner.linear_session_id:
        return None
    if not owner.generation:
        return None
    return owner


def decide_parent_wake(
    owner: ParentOwner | None,
    *,
    child_issue_id: str,
    stop_active: bool,
) -> ParentWake:
    """Wake the owning parent session after a child changes. Never start a second executor."""
    if stop_active:
        return ParentWake(action="deny", reason="stop_active")
    if not isinstance(child_issue_id, str) or not child_issue_id:
        return ParentWake(action="deny", reason="child_identity_unavailable")
    active = _active_owner(owner)
    if active is None:
        return ParentWake(action="deny", reason="parent_owner_unavailable")
    return ParentWake(
        action="continue",
        hermes_session_id=active.hermes_session_id,
        linear_session_id=active.linear_session_id,
        child_issue_id=child_issue_id,
        reason="child_completed",
    )


def decide_prompted_followup(
    owner: ParentOwner | None,
    *,
    incoming_linear_session_id: str,
) -> FollowupDecision:
    """Same-session follow-ups continue. A foreign Linear session is suppressed."""
    if not isinstance(incoming_linear_session_id, str) or not incoming_linear_session_id:
        return FollowupDecision(action="suppress", reason="session_identity_unavailable")
    active = _active_owner(owner)
    if active is None:
        return FollowupDecision(action="suppress", reason="parent_owner_unavailable")
    if incoming_linear_session_id == active.linear_session_id:
        return FollowupDecision(action="continue", reason="owning_session")
    return FollowupDecision(
        action="suppress",
        redirect_linear_session_id=active.linear_session_id,
        reason="foreign_session",
    )
