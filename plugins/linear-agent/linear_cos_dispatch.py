"""Read-only chief-of-staff inspect and dispatch preflight.

This module does not mutate Linear. Inspecting a parent must not launch every
ready child. Dispatch is an explicit later decision against one child.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChildView:
    issue_id: str
    identifier: str
    state_type: str
    blocked: bool
    running: bool
    already_owned: bool


@dataclass(frozen=True)
class InspectResult:
    parent_issue_id: str
    ready: tuple[ChildView, ...]
    blocked: tuple[ChildView, ...]
    done: tuple[ChildView, ...]
    dispatch: None = None


@dataclass(frozen=True)
class DispatchDecision:
    action: str
    reason: str = ""
    issue_id: str = ""


STALE_AFTER_SECONDS = 3600
STALE_SUPPRESS_SECONDS = 14400
_TERMINAL = {"completed", "canceled", "cancelled", "duplicate"}


@dataclass(frozen=True)
class CheckNowResult:
    action: str
    reason: str = ""
    fingerprint: str = ""
    same_child_followup_id: str = ""
    create_new_session: bool = False


def _fingerprint(children: tuple[ChildView, ...] | list[ChildView]) -> str:
    parts = sorted(
        f"{child.issue_id}:{child.state_type}:{int(child.blocked)}:{int(child.running)}:{int(child.already_owned)}"
        for child in children
    )
    return "|".join(parts)


def check_now(
    *,
    previous: tuple[ChildView, ...] | list[ChildView],
    current: tuple[ChildView, ...] | list[ChildView],
    now: int,
    last_change_at: int,
    last_stale_wakeup_at: int,
    last_stale_fingerprint: str,
    quota_cooled: bool,
    evidence_missing_ids: tuple[str, ...] = (),
) -> CheckNowResult:
    """Bounded parent reconciliation. Does not mutate Linear or start sessions."""
    fingerprint = _fingerprint(current)
    if quota_cooled:
        return CheckNowResult(action="deferred", reason="quota_cooldown", fingerprint=fingerprint)
    missing = tuple(item for item in evidence_missing_ids if isinstance(item, str) and item)
    if missing:
        return CheckNowResult(
            action="wakeup",
            reason="missing_evidence",
            fingerprint=fingerprint,
            same_child_followup_id=missing[0],
            create_new_session=False,
        )
    if _fingerprint(previous) != fingerprint:
        return CheckNowResult(action="wakeup", reason="material_transition", fingerprint=fingerprint)
    if now - last_change_at >= STALE_AFTER_SECONDS:
        if last_stale_fingerprint == fingerprint and now - last_stale_wakeup_at < STALE_SUPPRESS_SECONDS:
            return CheckNowResult(action="noop", reason="stale_suppressed", fingerprint=fingerprint)
        return CheckNowResult(action="wakeup", reason="stale", fingerprint=fingerprint)
    return CheckNowResult(action="noop", reason="unchanged", fingerprint=fingerprint)


def inspect_parent(parent_issue_id: str, children: list[ChildView]) -> InspectResult:
    if not isinstance(parent_issue_id, str) or not parent_issue_id:
        return InspectResult(parent_issue_id="", ready=(), blocked=(), done=())
    ready: list[ChildView] = []
    blocked: list[ChildView] = []
    done: list[ChildView] = []
    for child in children:
        if child.state_type in _TERMINAL:
            done.append(child)
            continue
        if child.blocked:
            blocked.append(child)
            continue
        ready.append(child)
    return InspectResult(
        parent_issue_id=parent_issue_id,
        ready=tuple(ready),
        blocked=tuple(blocked),
        done=tuple(done),
        dispatch=None,
    )


def decide_dispatch(
    child: ChildView,
    *,
    parent_owner_present: bool,
    budget_ok: bool,
    slots_available: bool,
) -> DispatchDecision:
    if child.state_type in _TERMINAL:
        return DispatchDecision(action="skip", reason="terminal", issue_id=child.issue_id)
    if child.blocked:
        return DispatchDecision(action="skip", reason="blocked", issue_id=child.issue_id)
    if child.running or child.already_owned:
        return DispatchDecision(action="skip", reason="already_executing", issue_id=child.issue_id)
    if not parent_owner_present:
        return DispatchDecision(action="skip", reason="parent_owner_unavailable", issue_id=child.issue_id)
    if not budget_ok:
        return DispatchDecision(action="skip", reason="budget", issue_id=child.issue_id)
    if not slots_available:
        return DispatchDecision(action="skip", reason="no_slot", issue_id=child.issue_id)
    if child.state_type != "unstarted":
        return DispatchDecision(action="skip", reason="not_ready", issue_id=child.issue_id)
    return DispatchDecision(action="dispatch", reason="ready", issue_id=child.issue_id)
