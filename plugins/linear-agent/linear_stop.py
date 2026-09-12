"""Exact, fail-closed evidence handling for Linear stop controls.

A Stop acknowledgement means the request reached the gateway, not that gateway
execution, tools, child agents, processes, or remote requests have stopped.
Only a matching terminal lifecycle receipt can prove physical completion.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

_LIFETIMES = ("tools", "children", "processes", "remote")
_REQUIRED_RECEIPT_KEYS = frozenset({
    "lifecycle_version", "session_key", "execution_id", "generation", "state",
    "occupancy", *_LIFETIMES,
})
_STOP_STATUSES = frozenset({"accepted", "not_delivered", "not_running", "stale"})
_CONTROL_CAUSES = frozenset({"stop", "reassigned", "permission_lost"})


@dataclass(frozen=True)
class LifetimeAcknowledgement:
    """Receipt truth without conflating delivery with physical completion."""

    acknowledged: bool
    physically_stopped: bool


def validate_control_cause(cause: str) -> str:
    if cause not in _CONTROL_CAUSES:
        raise ValueError("unsupported Linear cancellation cause")
    return cause


def acknowledge_stop_delivery(
    receipt: object, session_key: str, execution_id: str
) -> LifetimeAcknowledgement:
    """Validate an exact Stop acknowledgement; never infer physical completion."""
    valid = (
        isinstance(receipt, Mapping)
        and receipt.get("session_key") == session_key
        and receipt.get("execution_id") == execution_id
        and receipt.get("status") in _STOP_STATUSES
    )
    return LifetimeAcknowledgement(acknowledged=valid, physically_stopped=False)


def acknowledge_lifetime(
    receipt: object, session_key: str, execution_id: str
) -> LifetimeAcknowledgement:
    """Accept physical stop only with v2 proof for every owned lifetime."""
    if not isinstance(receipt, Mapping):
        return LifetimeAcknowledgement(False, False)
    if set(receipt) != _REQUIRED_RECEIPT_KEYS:
        return LifetimeAcknowledgement(False, False)
    acknowledged = (
        receipt.get("lifecycle_version") == "execution-lifecycle/v2"
        and receipt.get("session_key") == session_key
        and receipt.get("execution_id") == execution_id
        and type(receipt.get("generation")) is int
        and receipt["generation"] >= 0
    )
    physically_stopped = acknowledged and (
        receipt.get("state") == "completed"
        and receipt.get("occupancy") == "released"
        and all(receipt.get(lifetime) == "none" for lifetime in _LIFETIMES)
    )
    return LifetimeAcknowledgement(acknowledged, physically_stopped)


def acknowledge_handoff_receipt(
    receipt: object, session_key: str, execution_id: str
) -> bool:
    """Accept an acknowledged Stop delivery or a physically stopped v2 lifetime."""
    if acknowledge_stop_delivery(receipt, session_key, execution_id).acknowledged:
        return True
    return acknowledge_lifetime(receipt, session_key, execution_id).physically_stopped
