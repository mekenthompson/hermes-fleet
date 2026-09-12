"""Authoritative evidence required before Linear terminal Done."""
from __future__ import annotations

from collections.abc import Mapping, Sequence

_REQUIRED_RECEIPT_KEYS = {
    "lifecycle_version",
    "session_key",
    "execution_id",
    "generation",
    "state",
    "occupancy",
    "tools",
    "children",
    "processes",
    "remote",
}


def accepted_completion_evidence(
    receipt: object,
    *,
    session_key: str,
    execution_id: str,
) -> bool:
    """Accept only an exact v2 receipt that this execution is released."""
    if not isinstance(receipt, dict) or not session_key or not execution_id:
        return False
    if set(receipt) != _REQUIRED_RECEIPT_KEYS:
        return False
    if receipt.get("lifecycle_version") != "execution-lifecycle/v2":
        return False
    if receipt.get("session_key") != session_key or receipt.get("execution_id") != execution_id:
        return False
    if type(receipt.get("generation")) is not int or receipt["generation"] < 0:
        return False
    return (
        receipt.get("state") == "completed"
        and receipt.get("occupancy") == "released"
        and all(receipt.get(name) == "none" for name in ("tools", "children", "processes", "remote"))
    )


def mandatory_children_accepted(children: object) -> bool:
    """Parent Done requires an explicit ledger; absence is not proof."""
    if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
        return False
    for child in children:
        if not isinstance(child, Mapping):
            return False
        if child.get("mandatory") is not True:
            return False
        if child.get("accepted") is not True:
            return False
    return True


def linear_children_ledger(nodes: object) -> list[dict[str, bool]] | None:
    """Map Linear children to the mandatory ledger. Missing nodes are not proof."""
    if not isinstance(nodes, list):
        return None
    ledger: list[dict[str, bool]] = []
    for child in nodes:
        if not isinstance(child, Mapping):
            return None
        raw_state = child.get("state")
        state = raw_state if isinstance(raw_state, Mapping) else {}
        ledger.append({
            "mandatory": True,
            "accepted": state.get("type") == "completed",
        })
    return ledger


def linear_children_connection_accepted(children: object) -> bool:
    """Refuse parent Done unless the full Linear children page is proven complete."""
    if not isinstance(children, Mapping):
        return False
    page_info = children.get("pageInfo")
    if not isinstance(page_info, Mapping) or page_info.get("hasNextPage") is not False:
        return False
    ledger = linear_children_ledger(children.get("nodes"))
    if ledger is None:
        return False
    return mandatory_children_accepted(ledger)
