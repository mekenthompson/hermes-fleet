"""Owner side of an agent-to-agent takeover: stop, prove, then hand over or put things back.

``hermes linear hand-off`` runs inside the profile that owns the work (the old delegate),
started by the requester's ``take_over`` through the operator-configured owner command.
In one call, in code, it:

1. checks everything that can be checked before touching the work (the issue's reporting
   link, the one native task, its issue scope, guarded controls, the native Stop getter and
   any tasks it started) and refuses with nothing changed if any of them fails;
2. pauses the task with one guarded native Stop whose reason names the handover;
3. waits for the existing cleanup proof (``cleanup.inspect``) to show the stopped work has
   fully ended;
4. on proof, reports ``handed_off`` with the evidence; the requester then continues the work
   as its one tracked task. Without proof it resumes the task it paused (guarded on the
   event it last saw), so the work never ends up stopped with nobody running it.

A task that is already paused for input (for example by an earlier, uncoordinated stop) is
not stopped again: only the proof runs, and a failed proof leaves it exactly as it was.
Repeating the call with the same request id is safe: the Stop carries that id, so a retry
after a lost reply proves the same Stop instead of stopping twice. ``cancel`` resumes the
task only when the latest Stop is still this request's own, for a requester that could not
start its side. Nothing here grants permission; the requester's chat request is the
authority, exactly as for ``work_on``.
"""
from __future__ import annotations

import hashlib
import inspect as _inspect
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from . import cleanup, diag
from .cleanup import Unknown

HANDOFF_REASON = "Paused for a handover to {to} [handoff:{request_id}]"
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,39}")
DEFAULT_WAIT_SECONDS = 90.0
MAX_WAIT_SECONDS = 300.0
POLL_SECONDS = 2.0
STOPPABLE = ("running", "ready")
PAUSED = ("blocked", "triage")
# Cleanup holds that clear by themselves while a stopped worker winds down. Every other hold
# is a fact about the work that waiting will not change, so the proof stops there.
TRANSIENT = frozenset({
    "run_not_settled", "old_worker_alive", "native_admission_not_settled", "native_worker_or_spawner_unsettled",
    "native_child_spawner_unsettled", "native_session_has_unsettled_work", "native_stop_checkpoint_pending",
    "native_executor_settlement_missing", "cleanup_evidence_changed", "cleanup_evidence_unavailable",
    "worker_identity_unreadable", "native_receipt_changed", "scoped_process_cleanup_unknown",
    "native_owner_changed",
})

# Plain sentences for the requester to pass on; never internal codes.
REFUSALS = {
    "not_bound_here": "{owner} has no tracked work linked to this issue, so there was nothing here to hand over.",
    "task_finished_here": "{owner}'s work on this issue has already finished, so there was nothing to hand over.",
    "task_not_handable": "{owner}'s work on this issue is waiting on something else (a dependency, a schedule or "
                         "a review), so it was left as it is.",
    "started_other_tasks": "{owner}'s work on this issue started other tasks that are still open, so it was left "
                           "running; those need to finish or be stopped first.",
    "controls_unsupported": "{owner}'s version can't stop and resume work safely for a handover, so nothing was "
                            "changed.",
    "changed_during_handoff": "{owner}'s work changed while the handover was being set up, so nothing was changed. "
                              "Try again.",
}


def handoff_reason(to: str, request_id: str) -> str:
    return HANDOFF_REASON.format(to=to, request_id=request_id)


def resume_request_id(request_id: str) -> str:
    return "linear:" + hashlib.sha256(f"handoff-resume:{request_id}".encode()).hexdigest()


def _refuse(reason: str, owner: str, **extra: Any) -> dict:
    diag.evt("handoff.decision", "refused", reason)
    return {"handed_off": False, "state": "unchanged", "reason": reason,
            "message": REFUSALS[reason].format(owner=owner), "permission_granted": False, **extra}


# Plain facts for the holds people meet most often in a handover; cleanup.HOLD_FACTS has the rest.
HOLD_FACTS = {
    "proof_timeout": "its run did not finish winding down in time.",
    "native_task_issue_scope_conflict": "the task on that agent names a different issue.",
    "native_history_match_missing_or_ambiguous": "that agent has no single task for this issue.",
    "native_history_match_foreign_or_archived": "the task for this issue belongs to another agent or was archived.",
    "issue_scope_fenced": "earlier changes on this issue are held for a check.",
    "native_stop_checkpoint_unsupported": "that agent's version can't confirm a stop has finished.",
    "invalid_cleanup_selector": "the request did not name the issue completely.",
}


def _hold_message(reason: str, owner: str) -> str:
    fact = HOLD_FACTS.get(reason) or cleanup.HOLD_FACTS.get(reason)
    if not fact:
        return "The handover couldn't be confirmed safe."
    return "The handover couldn't be confirmed safe: " + fact[0].lower() + fact[1:]


def _stops(events: list) -> list:
    return [event for event in events if event.kind in ("blocked", "block_loop_detected")]


def _own_stop(events: list, reason: str):
    """This request's Stop, when it is the latest Stop and nothing resumed or finished the task after it."""
    stops = _stops(events)
    if not stops or (stops[-1].payload or {}).get("reason") != reason:
        return None
    later = [event for event in events if event.id > stops[-1].id and event.kind in ("unblocked", "completed", "archived")]
    return None if later else stops[-1]


def _started_open(adapter, task_id: str) -> bool:
    """Does this task have started tasks that are still open? Their fate is not part of the proof."""
    with adapter.read_conn() as conn:
        children = []
        for row in conn.execute("SELECT task_id, payload FROM task_events WHERE kind='created'"):
            try:
                payload = json.loads(row["payload"] or "null")
            except (TypeError, ValueError):
                continue
            if isinstance(payload, dict) and payload.get("creator_task_id") == task_id:
                children.append(row["task_id"])
    return any((child := adapter.get(task_id_)) is not None and child.status not in ("done", "archived")
               for task_id_ in children)


def _block(adapter, task, reason: str, expected_event_id: int) -> bool:
    """One guarded native Stop. A pause for a handover is not a recurring blocker: it must not
    push the task into the recurrence fence, which cannot be resumed."""
    options = {}
    if "count_recurrence" in _inspect.signature(adapter.kb.block_task).parameters:
        options["count_recurrence"] = not (task.block_kind == "needs_input" and (task.block_recurrences or 0) >= 1)
    with adapter.conn() as conn:
        return adapter.kb.block_task(conn, task.id, reason=reason, kind="needs_input",
                                     expected_run_id=task.current_run_id if task.status == "running" else None,
                                     expected_event_id=expected_event_id, **options)


def _resume(adapter, task_id: str, request_id: str, reason: str) -> str:
    """Resume the task this request paused: "resumed", "not_ours" when anyone acted on it since
    (their decision stands) or "failed". Guarded on the latest event; repeated reads absorb late
    worker events (the exit result arrives after the Stop)."""
    label = resume_request_id(request_id)
    for _ in range(4):
        events = adapter.history(task_id, 0)
        if any(event.kind == "unblocked" and (event.payload or {}).get("request_id") == label for event in events):
            return "resumed"
        if _own_stop(events, reason) is None:
            return "not_ours"
        if adapter.require_task(task_id).status != "blocked":
            return "not_ours"
        with adapter.conn() as conn:
            if adapter.kb.unblock_task(conn, task_id, expected_event_id=events[-1].id, request_id=label):
                return "resumed"
    return "failed"


def _context(state: Path, issue: str, metadata: Any, request_id: Any, to: Any):
    from hermes_constants import get_hermes_home
    from hermes_cli.profiles import get_active_profile_name
    from agent.delegation_context import is_delegated_child_process_context
    from agent.runtime_session_store import is_worker_process
    if is_delegated_child_process_context() or is_worker_process():
        raise Unknown("owner_cli_required")
    try:
        valid_id = isinstance(request_id, str) and str(uuid.UUID(request_id)) == request_id
    except ValueError:
        valid_id = False
    if not (valid_id and isinstance(to, str) and _NAME.fullmatch(to) and isinstance(issue, str) and issue
            and isinstance(metadata, dict) and set(metadata) == {"id", "identifier", "url"}
            and all(isinstance(v, str) and 0 < len(v) <= 4096 for v in metadata.values())
            and issue in (metadata["id"], metadata["identifier"])):
        raise Unknown("invalid_cleanup_selector")
    home = Path(get_hermes_home()).resolve(strict=True)
    return home, get_active_profile_name()


def _adapter(home: Path, board: str, native_profile: str):
    from .bridge import Kanban
    return Kanban(board, profile=native_profile, profile_home=home)


def hand_off(state: Path, issue: str, binding_profile: str, settings: dict, metadata: dict, *, to: str, request_id: str,
             owner_name: str | None = None, ingress: Path | None = None, wait_seconds: float = DEFAULT_WAIT_SECONDS,
             clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
             inspect_cleanup: Callable[..., dict] = cleanup.inspect) -> dict:
    """Hand this profile's work on one issue to ``to``, or change nothing and say why.

    ``binding_profile`` is the deployment profile the cleanup proof scopes to; ``owner_name``
    only words the reply."""
    from .api import LinearError
    owner = owner_name or binding_profile
    from .bridge import require_task_issue_scope
    try:
        home, native_profile = _context(state, issue, metadata, request_id, to)
        issue_id = metadata["id"]
        bound = cleanup._binding(state, issue_id, native_profile)
        if bound is None:
            return _refuse("not_bound_here", owner)
        # Same discovery as the cleanup proof: one native task, this executor, not archived.
        bound, _ = cleanup._discover(home, issue_id, native_profile, native_profile, metadata, bound)
        adapter = _adapter(home, bound["board"], native_profile)
        if not getattr(adapter, "guarded_controls", False):
            return _refuse("controls_unsupported", owner)
        from gateway import session_ancillary
        if getattr(session_ancillary, "process_stop_pending", None) is None:
            return _refuse("controls_unsupported", owner)
        task = adapter.require_task(bound["task_id"])
        try:
            require_task_issue_scope(task, metadata)
        except LinearError:
            raise Unknown("native_task_issue_scope_conflict")
        reason = handoff_reason(to, request_id)
        events = adapter.history(task.id, 0)
        mine = _own_stop(events, reason)
        stopped_here = mine is not None
        if mine is not None:
            stop_id = mine.id  # A retry after a lost reply: prove the same Stop, never stop twice.
        elif task.status in ("done", "archived"):
            return _refuse("task_finished_here", owner)
        elif task.status in PAUSED:
            stops = _stops(events)
            if (task.block_kind != "needs_input" or task.current_run_id is not None or not stops
                    or (stops[-1].payload or {}).get("kind") != "needs_input"):
                return _refuse("task_not_handable", owner)
            stop_id = stops[-1].id  # Already paused: prove that Stop, change nothing.
        elif task.status not in STOPPABLE:
            return _refuse("task_not_handable", owner)
        else:
            if _started_open(adapter, task.id):
                return _refuse("started_other_tasks", owner)
            # Preflight the parts of the proof a Stop cannot change, before stopping anything.
            preflight = inspect_cleanup(state, issue_id, binding_profile, events[-1].id, settings, metadata, ingress=ingress)
            if preflight.get("reason") not in ("task_not_currently_stopped", "stop_event_changed"):
                raise Unknown(preflight.get("reason") or "cleanup_evidence_unavailable")
            try:
                _block(adapter, task, reason, events[-1].id)
            except (RuntimeError, OSError) as exc:
                # Native block commits before it delivers Stop to the running worker; a delivery
                # error leaves the Stop in place. History, not the exception, says what happened.
                diag.evt("handoff.decision", "held", "unverified", exc=type(exc).__name__)
            mine = _own_stop(adapter.history(task.id, 0), reason)
            if mine is None:
                return _refuse("changed_during_handoff", owner)
            stop_id, stopped_here = mine.id, True
            diag.evt("handoff.decision", "stopped", "-", task=task.id, evt=stop_id)
    except Unknown as exc:
        diag.evt("handoff.decision", "refused", str(exc))
        return {"handed_off": False, "state": "unchanged", "reason": str(exc),
                "message": _hold_message(str(exc), owner) + " Nothing was changed.", "permission_granted": False}
    except (LinearError, OSError, RuntimeError, ValueError, KeyError, TypeError, ImportError) as exc:
        diag.evt("handoff.decision", "refused", "cleanup_evidence_unavailable", exc=type(exc).__name__)
        return {"handed_off": False, "state": "unchanged", "reason": "cleanup_evidence_unavailable",
                "message": f"{owner}'s work couldn't be checked, so nothing was changed.", "permission_granted": False}

    # Prove the stopped work has ended. Only holds that clear while a worker winds down are waited on.
    deadline = clock() + max(0.0, min(float(wait_seconds), MAX_WAIT_SECONDS))
    while True:
        proof = inspect_cleanup(state, issue_id, binding_profile, stop_id, settings, metadata, ingress=ingress)
        if proof.get("quiescence_proven") is True:
            diag.evt("handoff.decision", "applied", "handed_off", task=bound["task_id"], evt=stop_id)
            return {"handed_off": True, "state": "handed_off", "issue_id": issue_id, "task_id": bound["task_id"],
                    "board": bound["board"], "stop_event_id": stop_id, "stopped_here": stopped_here,
                    "evidence_digest": proof.get("evidence_digest"), "to": to, "permission_granted": False,
                    "message": f"{owner}'s work stopped and is confirmed finished; {to} can continue it."}
        held = proof.get("reason") or "cleanup_evidence_unavailable"
        if held not in TRANSIENT or clock() >= deadline:
            break
        sleep(POLL_SECONDS)
    held = "proof_timeout" if held in TRANSIENT else held
    message = _hold_message(held, owner)
    if not stopped_here:
        diag.evt("handoff.decision", "refused", "handoff_unchanged", task=bound["task_id"], evt=stop_id)
        return {"handed_off": False, "state": "unchanged", "reason": held, "permission_granted": False,
                "message": message + f" {owner}'s work was left as it was, and nothing was handed over."}
    try:
        resumed = _resume(adapter, bound["task_id"], request_id, reason)
    except (LinearError, OSError, RuntimeError, ValueError) as exc:
        diag.evt("handoff.decision", "failed", "handoff_stuck", exc=type(exc).__name__)
        resumed = "failed"
    if resumed == "not_ours":
        diag.evt("handoff.decision", "skipped", "changed_during_handoff", task=bound["task_id"], evt=stop_id)
        return {"handed_off": False, "state": "unchanged", "reason": "changed_during_handoff", "permission_granted": False,
                "message": message + f" Someone resumed or stopped {owner}'s work during the handover, so it was "
                           "left as they set it, and nothing was handed over."}
    if resumed == "resumed":
        diag.evt("handoff.decision", "applied", "handoff_resumed", task=bound["task_id"], evt=stop_id)
        return {"handed_off": False, "state": "resumed", "reason": held, "permission_granted": False,
                "message": message + f" {owner}'s work was resumed and nothing was handed over."}
    diag.evt("handoff.decision", "failed", "handoff_stuck", task=bound["task_id"], evt=stop_id)
    return {"handed_off": False, "state": "stopped", "reason": held, "permission_granted": False,
            "message": message + f" {owner}'s work is paused and could not be resumed automatically: an operator "
                       f"needs to resume it on {owner}'s board."}


def cancel(state: Path, issue: str, binding_profile: str, metadata: dict, *, to: str, request_id: str,
           owner_name: str | None = None) -> dict:
    """Resume the task only if its latest Stop is still this request's own handover pause."""
    from .api import LinearError
    owner = owner_name or binding_profile
    try:
        home, native_profile = _context(state, issue, metadata, request_id, to)
        bound = cleanup._binding(state, metadata["id"], native_profile)
        if bound is None:
            return _refuse("not_bound_here", owner)
        adapter = _adapter(home, bound["board"], native_profile)
        if _own_stop(adapter.history(bound["task_id"], 0), handoff_reason(to, request_id)) is None:
            diag.evt("handoff.decision", "skipped", "handoff_unchanged", task=bound["task_id"])
            return {"handed_off": False, "state": "unchanged", "reason": "handoff_unchanged", "permission_granted": False,
                    "message": f"{owner}'s work is not paused by this handover, so it was left as it is."}
        if _resume(adapter, bound["task_id"], request_id, handoff_reason(to, request_id)) == "resumed":
            diag.evt("handoff.decision", "applied", "handoff_cancelled", task=bound["task_id"])
            return {"handed_off": False, "state": "resumed", "reason": "handoff_cancelled", "permission_granted": False,
                    "message": f"The handover was called off and {owner}'s work was resumed."}
    except Unknown as exc:
        return {"handed_off": False, "state": "unchanged", "reason": str(exc), "permission_granted": False,
                "message": _hold_message(str(exc), owner) + " Nothing was changed."}
    except (LinearError, OSError, RuntimeError, ValueError, KeyError, TypeError, ImportError) as exc:
        diag.evt("handoff.decision", "failed", "handoff_stuck", exc=type(exc).__name__)
    return {"handed_off": False, "state": "stopped", "reason": "handoff_stuck", "permission_granted": False,
            "message": f"{owner}'s work is paused and could not be resumed automatically: an operator needs to "
                       f"resume it on {owner}'s board."}


# -- requester side -------------------------------------------------------------------------
# The requester reaches the owner only through an operator-configured command that runs the
# owner's own ``hermes ... linear hand-off`` (for example a host bridge into its container).
# The plugin never invents that route: an agent with no configured command for the current
# delegate refuses before anything changes.
_STATES = frozenset({"handed_off", "unchanged", "resumed", "stopped"})


def takeover_settings(raw: Any, *, default_name: str) -> dict:
    """``takeover: {name, agents: [{name, delegate_id, command: [argv...], wait_seconds}]}``.

    Invalid entries are dropped (and logged once) rather than failing the service: an agent
    without a valid entry simply cannot take over from that delegate."""
    if not isinstance(raw, dict):
        return {"name": default_name, "agents": {}}
    name = raw.get("name") if isinstance(raw.get("name"), str) and _NAME.fullmatch(raw["name"]) else default_name
    agents = {}
    for entry in raw.get("agents") or []:
        command = entry.get("command") if isinstance(entry, dict) else None
        wait = entry.get("wait_seconds", DEFAULT_WAIT_SECONDS) if isinstance(entry, dict) else None
        if not (isinstance(entry, dict) and isinstance(entry.get("name"), str) and _NAME.fullmatch(entry["name"])
                and isinstance(entry.get("delegate_id"), str) and 0 < len(entry["delegate_id"]) <= 128
                and isinstance(command, list) and command and len(command) <= 32
                and all(isinstance(part, str) and part and "\0" not in part and len(part) <= 512 for part in command)
                and isinstance(wait, (int, float)) and not isinstance(wait, bool) and 0 < wait <= MAX_WAIT_SECONDS):
            diag.evt("diag.config", "skipped", "takeover_unavailable")
            continue
        agents[entry["delegate_id"]] = {"name": entry["name"], "command": list(command), "wait_seconds": float(wait)}
    return {"name": name, "agents": agents}


def request(peer: dict, issue: dict, *, to: str, request_id: str, cancel: bool = False,
            run: Callable[..., Any] | None = None) -> dict | None:
    """Run the owner's hand-off and return its reply, or None when the outcome is unknown."""
    import subprocess
    run = run or subprocess.run
    metadata = {key: issue[key] for key in ("id", "identifier", "url")}
    argv = [*peer["command"], "linear", "hand-off", issue["id"], "--issue-json", json.dumps(metadata),
            "--to", to, "--from-name", peer["name"], "--request-id", request_id,
            "--wait-seconds", str(int(peer["wait_seconds"]))] + (["--cancel"] if cancel else [])
    try:
        done = run(argv, capture_output=True, text=True, stdin=subprocess.DEVNULL, check=False,
                   timeout=peer["wait_seconds"] + 90)
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    # Bridges can print their own lines (secret loaders, banners) before the reply.
    for line in reversed(str(getattr(done, "stdout", "") or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            reply = json.loads(line)
        except ValueError:
            continue
        if (isinstance(reply, dict) and isinstance(reply.get("handed_off"), bool) and reply.get("state") in _STATES
                and isinstance(reply.get("message"), str) and reply.get("permission_granted") is False):
            if reply["handed_off"] and (reply.get("issue_id") != issue["id"] or reply["state"] != "handed_off"):
                return None
            return reply
    return None
