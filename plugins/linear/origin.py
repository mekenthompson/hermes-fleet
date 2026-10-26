"""Exact, read-only native request evidence. References confer no control permission."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from .transport import NativeContext, Unavailable

VERSION = 1
REFERENCE_FIELDS = frozenset({"version", "profile", "session_id", "admission_id", "input_id",
                              "instance_id", "authority_epoch", "execution_generation", "scope_digest"})


def _refuse(message: str = "The originating native request is not current and independently verifiable."):
    raise Unavailable(message)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def scope(settings: dict[str, Any]) -> dict[str, Any]:
    """Only declared non-secret limits; neither identity nor limits grant permission."""
    from . import BoundLinearAPI
    identity = settings.get("identity") or {}
    if not isinstance(identity, dict):
        _refuse("The configured Linear identity cannot be inspected.")
    selected = {key: identity[key] for key in ("organization_id", "teams", "projects") if key in identity}
    try:
        projected = {"linear": selected,
                     "specialist_scope": BoundLinearAPI._validate_specialist_scope(settings.get("specialist_scope"))}
        return json.loads(json.dumps(projected))
    except (ValueError, TypeError) as exc:
        raise Unavailable("The configured requester scope cannot be independently inspected.") from exc


def reference(value: Any) -> dict:
    if (not isinstance(value, dict) or set(value) != REFERENCE_FIELDS
            or type(value.get("version")) is not int or value["version"] != VERSION):
        _refuse("Use the complete reference returned by the current native tool invocation.")
    if (any(not isinstance(value[k], str) or not value[k] or len(value[k]) > 4096
            for k in REFERENCE_FIELDS - {"version", "authority_epoch", "execution_generation"})
            or any(type(value[k]) is not int or value[k] < 1
                   for k in ("authority_epoch", "execution_generation"))):
        _refuse("The native origin reference is malformed.")
    return dict(value)


@asynccontextmanager
async def _client(home: Path):
    """Discover an existing owner only: never launch, recover, or replace a gateway."""
    from hermes_cli.gateway_client import connect_gateway
    from hermes_cli.gateway_runtime import discover_gateway_endpoint, gateway_profile_home
    ready = await asyncio.to_thread(discover_gateway_endpoint, home)
    endpoint = ready.endpoint
    if (ready.state != "ready" or endpoint is None
            or Path(endpoint.profile_id).resolve() != gateway_profile_home(home)):
        _refuse("The existing native owner is unavailable; no runtime was started.")
    async with connect_gateway(endpoint=endpoint) as client:
        yield client, endpoint


async def _snapshot(client, home: Path, session_id: str) -> tuple[dict, dict]:
    from .origin_state import read_current
    snapshot, row = await asyncio.to_thread(read_current, home, session_id)
    receipt = await client.rpc("prompt.receipt", session_id=snapshot["session_id"], admission_id=row["admission_id"])
    if (receipt.get("status") != "started" or receipt.get("outcome") is not None
            or any(receipt.get(k) != row.get(k) for k in
                   ("admission_id", "authority_epoch", "execution_generation"))
            or receipt.get("ref", {}).get("session_id") != snapshot["session_id"]
            or receipt.get("ref", {}).get("profile_id") != str(home)):
        _refuse()
    return snapshot, row


async def _inspect(home: Path, profile: str, declared_scope: dict, ref: dict | None,
                   context: NativeContext | None = None) -> dict:
    async with _client(home) as (client, endpoint):
        snapshot, current = await _snapshot(client, home, context.session_id if context else ref["session_id"])
        observed = {"version": VERSION, "profile": profile, "session_id": snapshot["session_id"],
                    "admission_id": current["admission_id"], "input_id": current["input_id"],
                    "instance_id": endpoint.instance_id, "authority_epoch": endpoint.authority_epoch,
                    "execution_generation": snapshot["execution_generation"], "scope_digest": _digest(declared_scope)}
        if (observed["authority_epoch"] != snapshot.get("authority_epoch")
                or observed["authority_epoch"] != current.get("authority_epoch")
                or ref is not None and observed != ref):
            _refuse()
        ref = reference(observed)
        from .origin_state import _evidence
        evidence = await asyncio.to_thread(_evidence, home, ref, current, declared_scope, context)
        # Recheck after the state read: Stop, settlement, restart or a new turn invalidates the result.
        after, active = await _snapshot(client, home, ref["session_id"])
        if (after.get("authority_epoch") != ref["authority_epoch"]
                or after.get("execution_generation") != ref["execution_generation"]
                or active.get("admission_id") != ref["admission_id"]):
            _refuse()
        return evidence


def _run(home: Path, profile: str, declared_scope: dict, ref: dict | None,
         context: NativeContext | None = None) -> dict:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        _refuse("Native origin inspection needs the synchronous tool or owner CLI boundary.")
    try:
        return asyncio.run(_inspect(home, profile, declared_scope, ref, context))
    except Unavailable:
        raise
    except (RuntimeError, OSError, ValueError, ImportError, KeyError, TypeError, sqlite3.DatabaseError) as exc:
        raise Unavailable("Native origin evidence is unavailable; inspect the owning runtime before continuing.") from exc


def current(context: NativeContext, settings: dict) -> dict:
    if context.delegated_child:
        _refuse("A delegated child cannot supply the originating user's request.")
    if context.platform not in {"local", "api_server", "telegram", "slack"}:
        _refuse("This surface cannot supply an ordinary native origin reference.")
    evidence = _run(Path(context.home), context.profile, scope(settings), None, context)
    return {"reference": evidence["reference"], "permission_granted": False}


def inspect(value: Any, settings: dict) -> dict:
    """Owner CLI only. Caller supplies a reference, never another home or database path."""
    from hermes_constants import get_hermes_home
    from hermes_cli.profiles import get_active_profile_name
    ref = reference(value)
    home = Path(get_hermes_home()).resolve(strict=True)
    if ref["profile"] != get_active_profile_name():
        _refuse("The reference belongs to another native profile.")
    return _run(home, ref["profile"], scope(settings), ref)
