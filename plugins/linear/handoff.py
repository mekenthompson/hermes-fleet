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
after a lost reply proves the same Stop instead of stopping twice. ``cancel`` calls a request
off, for a requester that could not start its side or never heard back: it records the call-off
first (a hand-off still running for that request then undoes its own Stop instead of reporting
a handover nobody continues) and resumes the task only while the latest Stop is still this
request's own. One hand-off or cancel runs per issue at a time (a file lock beside the state
database). A task already paused for a handover to a different agent is never proven for
another one. Nothing here grants permission; the requester's chat request is the authority,
exactly as for ``work_on``.
"""
from __future__ import annotations

import fcntl
import hashlib
import inspect as _inspect
import json
import os
import re
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

from . import cleanup, diag
from .cleanup import Unknown

HANDOFF_REASON = "Paused for a handover to {to} [handoff:{request_id}]"
_NAME_PATTERN = r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,39}"
_NAME = re.compile(_NAME_PATTERN)
HANDOFF_PATTERN = re.compile(r"Paused for a handover to (?P<to>" + _NAME_PATTERN + r") "
                             r"\[handoff:(?P<request_id>[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\]")
DEFAULT_WAIT_SECONDS = 90.0
MAX_WAIT_SECONDS = 300.0  # What the owner CLI accepts; a takeover entry is capped lower, below.
POLL_SECONDS = 2.0
LOCK_POLL_SECONDS = 0.2
LOCK_SLACK_SECONDS = 30.0  # How much longer than one proof wait a step waits for the issue lock.
# Timeouts for one take_over, worked out from the cap so the chat client always outlasts the
# server's own worst case: at most two owner calls (hand-off, then one --cancel, each the wait
# plus this overhead, which covers the cancel's wait for the issue lock) and one start here.
TAKEOVER_MAX_WAIT_SECONDS = 120.0
OWNER_CALL_OVERHEAD_SECONDS = LOCK_SLACK_SECONDS + 30.0
ENTRUST_BUDGET_SECONDS = 40.0
CLIENT_TIMEOUT_SECONDS = 2 * (TAKEOVER_MAX_WAIT_SECONDS + OWNER_CALL_OVERHEAD_SECONDS) + ENTRUST_BUDGET_SECONDS + 20.0
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
    "handover_in_progress": "{owner}'s work is already being handed over to another agent, so it was left as it is. "
                            "Try again once that handover has finished or been called off.",
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


def _would_fence(adapter, task) -> bool:
    """Would the handover Stop push this task into the recurrence fence (TRIAGE), which cannot be
    resumed? Only on a native whose block cannot skip counting the recurrence."""
    if "count_recurrence" in _inspect.signature(adapter.kb.block_task).parameters:
        return False
    previous = (task.block_recurrences or 0) if task.block_kind == "needs_input" else 0
    return previous + 1 >= int(getattr(adapter.kb, "BLOCK_RECURRENCE_LIMIT", 1))


def _resume(adapter, task_id: str, request_id: str, reason: str) -> str:
    """Resume the task this request paused: "resumed"; "absent" when this request never paused
    it; "not_ours" when anyone acted on it since (their decision stands); "failed" when it is
    still this request's pause but cannot be resumed here (for example our own Stop left it in
    TRIAGE, which only an operator lifts). Guarded on the latest event; repeated reads absorb
    late worker events (the exit result arrives after the Stop)."""
    label = resume_request_id(request_id)
    for _ in range(4):
        events = adapter.history(task_id, 0)
        if any(event.kind == "unblocked" and (event.payload or {}).get("request_id") == label for event in events):
            return "resumed"
        if not any((stop.payload or {}).get("reason") == reason for stop in _stops(events)):
            return "absent"
        if _own_stop(events, reason) is None:
            return "not_ours"
        status = adapter.require_task(task_id).status
        if status == "triage":
            return "failed"
        if status != "blocked":
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


def _handoff_dir(state: Path) -> Path:
    return Path(state).parent / "handoff"


def _digest(kind: str, value: str) -> str:
    return hashlib.sha256(f"{kind}:{value}".encode()).hexdigest()[:32]


@contextmanager
def issue_lock(directory: Path, issue_id: str, timeout: float):
    """One handover step per issue at a time, across processes and threads (flock on a private
    file). Yields False, holding nothing, when the lock is not free within ``timeout`` seconds."""
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(directory / (_digest("issue", issue_id) + ".lock"), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    yield False
                    return
                time.sleep(LOCK_POLL_SECONDS)
        try:
            yield True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _called_off_path(state: Path, request_id: str) -> Path:
    return _handoff_dir(state) / (_digest("called-off", request_id) + ".cancelled")


def _call_off(state: Path, request_id: str) -> None:
    """Record that this request was called off. Written before cancel waits for the issue lock,
    so a hand-off still running for the same request undoes its own Stop instead of reporting a
    handover nobody will continue, and a later hand-off with this id never stops anything."""
    path = _called_off_path(state, request_id)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))


def _is_called_off(state: Path, request_id: str) -> bool:
    return _called_off_path(state, request_id).exists()


def _unchanged(reason: str, message: str) -> dict:
    return {"handed_off": False, "state": "unchanged", "reason": reason, "message": message, "permission_granted": False}


def _stuck(owner: str, reason: str, prefix: str = "") -> dict:
    return {"handed_off": False, "state": "stopped", "reason": reason, "permission_granted": False,
            "message": prefix + f"{owner}'s work is paused and could not be resumed automatically: an operator "
                                f"needs to resume it on {owner}'s board."}


def _undo(adapter, task_id: str, request_id: str, reason: str, held: str, owner: str, prefix: str) -> dict:
    """Put back the Stop this request made. Whatever goes wrong, never report "unchanged" for
    work this request may have paused: "resumed", "unchanged" only when this request's Stop is
    provably absent or someone acted on it since, otherwise "stopped" (an operator resumes it)."""
    try:
        resumed = _resume(adapter, task_id, request_id, reason)
    except Exception as exc:  # noqa: BLE001 - every failure here leaves the pause in place
        diag.evt("handoff.decision", "failed", "handoff_stuck", exc=type(exc).__name__)
        resumed = "failed"
    if resumed == "absent":
        diag.evt("handoff.decision", "refused", "changed_during_handoff", task=task_id)
        return _unchanged("changed_during_handoff", prefix + f"{owner}'s work was left as it was, and nothing was "
                                                             "handed over.")
    if resumed == "not_ours":
        diag.evt("handoff.decision", "skipped", "changed_during_handoff", task=task_id)
        return _unchanged("changed_during_handoff", prefix + f"Someone resumed or stopped {owner}'s work during the "
                                                             "handover, so it was left as they set it, and nothing "
                                                             "was handed over.")
    if resumed == "resumed":
        diag.evt("handoff.decision", "applied", "handoff_resumed", task=task_id)
        return {"handed_off": False, "state": "resumed", "reason": held, "permission_granted": False,
                "message": prefix + f"{owner}'s work was resumed and nothing was handed over."}
    diag.evt("handoff.decision", "failed", "handoff_stuck", task=task_id)
    return _stuck(owner, held, prefix)


def hand_off(state: Path, issue: str, binding_profile: str, settings: dict, metadata: dict, *, to: str, request_id: str,
             owner_name: str | None = None, ingress: Path | None = None, wait_seconds: float = DEFAULT_WAIT_SECONDS,
             clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
             inspect_cleanup: Callable[..., dict] = cleanup.inspect) -> dict:
    """Hand this profile's work on one issue to ``to``, or change nothing and say why.

    ``binding_profile`` is the deployment profile the cleanup proof scopes to; ``owner_name``
    only words the reply. One hand-off or cancel runs per issue at a time."""
    owner = owner_name or binding_profile
    try:
        context = _context(state, issue, metadata, request_id, to)
        wait = max(0.0, min(float(wait_seconds), MAX_WAIT_SECONDS))
    except Unknown as exc:
        diag.evt("handoff.decision", "refused", str(exc))
        return _unchanged(str(exc), _hold_message(str(exc), owner) + " Nothing was changed.")
    except Exception as exc:  # noqa: BLE001 - nothing has been read or changed yet
        diag.evt("handoff.decision", "refused", "cleanup_evidence_unavailable", exc=type(exc).__name__)
        return _unchanged("cleanup_evidence_unavailable", f"{owner}'s work couldn't be checked, so nothing was changed.")
    with issue_lock(_handoff_dir(state), metadata["id"], wait + LOCK_SLACK_SECONDS) as held:
        if not held:
            return _refuse("handover_in_progress", owner)
        if _is_called_off(state, request_id):
            # A retry of a request that was called off finishes calling it off; it never stops anything.
            return _cancel_locked(state, binding_profile, metadata, context, to=to, request_id=request_id, owner=owner,
                                  prefix="This handover was called off earlier, so it was not started again. ")
        return _hand_off_locked(state, binding_profile, settings, metadata, context, to=to, request_id=request_id,
                                owner=owner, ingress=ingress, wait=wait, clock=clock, sleep=sleep,
                                inspect_cleanup=inspect_cleanup)


def _hand_off_locked(state: Path, binding_profile: str, settings: dict, metadata: dict, context, *, to: str,
                     request_id: str, owner: str, ingress, wait: float, clock, sleep, inspect_cleanup) -> dict:
    from .api import LinearError
    from .bridge import require_task_issue_scope
    home, native_profile = context
    issue_id = metadata["id"]
    reason = handoff_reason(to, request_id)
    stop_now = False
    try:
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
            other = HANDOFF_PATTERN.fullmatch(str((stops[-1].payload or {}).get("reason") or ""))
            if other and other.group("to") != to:
                # Paused for another agent's handover: proving that pause here would let two agents
                # continue the same work. That handover finishes or is called off first.
                return _refuse("handover_in_progress", owner)
            stop_id = stops[-1].id  # Already paused: prove that Stop, change nothing.
        elif task.status not in STOPPABLE:
            return _refuse("task_not_handable", owner)
        else:
            if _started_open(adapter, task.id):
                return _refuse("started_other_tasks", owner)
            if _would_fence(adapter, task):
                return _refuse("controls_unsupported", owner)
            # Preflight the parts of the proof a Stop cannot change, before stopping anything.
            preflight = inspect_cleanup(state, issue_id, binding_profile, events[-1].id, settings, metadata, ingress=ingress)
            if preflight.get("reason") not in ("task_not_currently_stopped", "stop_event_changed"):
                raise Unknown(preflight.get("reason") or "cleanup_evidence_unavailable")
            stop_now = True
    except Unknown as exc:
        diag.evt("handoff.decision", "refused", str(exc))
        return _unchanged(str(exc), _hold_message(str(exc), owner) + " Nothing was changed.")
    except Exception as exc:  # noqa: BLE001 - nothing has been stopped yet
        diag.evt("handoff.decision", "refused", "cleanup_evidence_unavailable", exc=type(exc).__name__)
        return _unchanged("cleanup_evidence_unavailable", f"{owner}'s work couldn't be checked, so nothing was changed.")

    task_id = bound["task_id"]
    if stop_now:
        # From here on the work may be paused: every failure rechecks and resumes, never "unchanged" blindly.
        try:
            _block(adapter, task, reason, events[-1].id)
        except Exception as exc:  # noqa: BLE001
            # Native block commits before it delivers Stop to the running worker; a delivery
            # error leaves the Stop in place. History, not the exception, says what happened.
            diag.evt("handoff.decision", "held", "unverified", exc=type(exc).__name__)
        try:
            after = adapter.history(task_id, 0)
        except Exception as exc:  # noqa: BLE001
            diag.evt("handoff.decision", "held", "stop_unverified", exc=type(exc).__name__)
            return _undo(adapter, task_id, request_id, reason, "cleanup_evidence_unavailable", owner,
                         f"{owner}'s work couldn't be checked after it was asked to stop. ")
        mine = _own_stop(after, reason)
        if mine is None:
            if not any((stop.payload or {}).get("reason") == reason for stop in _stops(after)):
                return _refuse("changed_during_handoff", owner)  # The guarded Stop never landed.
            return _undo(adapter, task_id, request_id, reason, "changed_during_handoff", owner, "")
        stop_id, stopped_here = mine.id, True
        diag.evt("handoff.decision", "stopped", "-", task=task_id, evt=stop_id)

    # Prove the stopped work has ended. Only holds that clear while a worker winds down are waited on.
    deadline = clock() + wait
    while True:
        try:
            proof = inspect_cleanup(state, issue_id, binding_profile, stop_id, settings, metadata, ingress=ingress)
        except Exception as exc:  # noqa: BLE001 - an unreadable proof is not a proof
            diag.evt("handoff.decision", "held", "cleanup_evidence_unavailable", exc=type(exc).__name__)
            proof = {"quiescence_proven": False, "reason": "cleanup_evidence_unavailable"}
        if proof.get("quiescence_proven") is True:
            if _is_called_off(state, request_id):
                # Called off while this proof ran (the requester gave up waiting): nobody will continue it.
                return _cancel_locked(state, binding_profile, metadata, context, to=to, request_id=request_id,
                                      owner=owner, prefix="The handover was called off before it finished. ")
            diag.evt("handoff.decision", "applied", "handed_off", task=task_id, evt=stop_id)
            return {"handed_off": True, "state": "handed_off", "issue_id": issue_id, "task_id": task_id,
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
        diag.evt("handoff.decision", "refused", "handoff_unchanged", task=task_id, evt=stop_id)
        return _unchanged(held, message + f" {owner}'s work was left as it was, and nothing was handed over.")
    return _undo(adapter, task_id, request_id, reason, held, owner, message + " ")


def cancel(state: Path, issue: str, binding_profile: str, metadata: dict, *, to: str, request_id: str,
           owner_name: str | None = None, wait_seconds: float = DEFAULT_WAIT_SECONDS) -> dict:
    """Call this request off: resume the task only if its latest Stop is still this request's own.

    The request is recorded as called off first, so a hand-off for it that is still running (the
    requester gave up waiting) undoes its own Stop, and repeating it never stops anything."""
    owner = owner_name or binding_profile
    try:
        context = _context(state, issue, metadata, request_id, to)
        wait = max(0.0, min(float(wait_seconds), MAX_WAIT_SECONDS))
        _call_off(state, request_id)
    except Unknown as exc:
        return _unchanged(str(exc), _hold_message(str(exc), owner) + " Nothing was changed.")
    except Exception as exc:  # noqa: BLE001
        diag.evt("handoff.decision", "failed", "handoff_stuck", exc=type(exc).__name__)
        return _stuck(owner, "handoff_stuck")
    with issue_lock(_handoff_dir(state), metadata["id"], wait + LOCK_SLACK_SECONDS) as held:
        if not held:
            # A hand-off is still running; it sees the call-off and undoes its own Stop. A retry confirms.
            diag.evt("handoff.decision", "held", "handover_in_progress")
            return _stuck(owner, "handover_in_progress", "The handover was called off while it was still running. ")
        return _cancel_locked(state, binding_profile, metadata, context, to=to, request_id=request_id, owner=owner)


def _cancel_locked(state: Path, binding_profile: str, metadata: dict, context, *, to: str, request_id: str,
                   owner: str, prefix: str = "") -> dict:
    home, native_profile = context
    reason = handoff_reason(to, request_id)
    try:
        bound = cleanup._binding(state, metadata["id"], native_profile)
        if bound is None:
            return _refuse("not_bound_here", owner)
        adapter = _adapter(home, bound["board"], native_profile)
        if _own_stop(adapter.history(bound["task_id"], 0), reason) is None:
            diag.evt("handoff.decision", "skipped", "handoff_unchanged", task=bound["task_id"])
            return _unchanged("handoff_unchanged", prefix + f"{owner}'s work is not paused by this handover, so it "
                                                            "was left as it is.")
    except Exception as exc:  # noqa: BLE001
        diag.evt("handoff.decision", "failed", "handoff_stuck", exc=type(exc).__name__)
        return _stuck(owner, "handoff_stuck", prefix)
    result = _undo(adapter, bound["task_id"], request_id, reason, "handoff_cancelled", owner, prefix)
    if result["state"] == "resumed":
        diag.evt("handoff.decision", "applied", "handoff_cancelled", task=bound["task_id"])
        result["message"] = prefix + f"The handover was called off and {owner}'s work was resumed."
    return result


# -- requester side -------------------------------------------------------------------------
# The requester reaches the owner only through an operator-configured command that runs the
# owner's own ``hermes ... linear hand-off`` (for example a host bridge into its container).
# The plugin never invents that route: an agent with no configured command for the current
# delegate refuses before anything changes.
_STATES = frozenset({"handed_off", "unchanged", "resumed", "stopped"})


def takeover_settings(raw: Any, *, default_name: str) -> dict:
    """``takeover: {name, agents: [{name, delegate_id, command: [argv...], wait_seconds}]}``.

    Invalid entries are dropped (and logged once) rather than failing the service: an agent
    without a valid entry simply cannot take over from that delegate. ``wait_seconds`` is capped
    at TAKEOVER_MAX_WAIT_SECONDS so one take_over always ends inside the chat client's timeout."""
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
        if wait > TAKEOVER_MAX_WAIT_SECONDS:
            diag.evt("diag.config", "skipped", "takeover_wait_capped")
        agents[entry["delegate_id"]] = {"name": entry["name"], "command": list(command),
                                        "wait_seconds": min(float(wait), TAKEOVER_MAX_WAIT_SECONDS)}
    return {"name": name, "agents": agents}


def request(peer: dict, issue: dict, *, to: str, request_id: str, cancel: bool = False,
            run: Callable[..., Any] | None = None) -> dict | None:
    """Run the owner's hand-off (or its --cancel) and return its reply, or None when the outcome is
    unknown. Never raises: an unknown outcome is for the caller to call off."""
    import subprocess
    run = run or subprocess.run
    metadata = {key: issue[key] for key in ("id", "identifier", "url")}
    argv = [*peer["command"], "linear", "hand-off", issue["id"], "--issue-json", json.dumps(metadata),
            "--to", to, "--from-name", peer["name"], "--request-id", request_id,
            "--wait-seconds", str(int(peer["wait_seconds"]))] + (["--cancel"] if cancel else [])
    try:
        done = run(argv, capture_output=True, text=True, stdin=subprocess.DEVNULL, check=False,
                   timeout=peer["wait_seconds"] + OWNER_CALL_OVERHEAD_SECONDS)
    except Exception:  # noqa: BLE001 - timeouts, a missing bridge, anything: the outcome is unknown
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
