"""Owner-CLI-only, read-only cleanup evidence for an existing Linear/native binding."""
from __future__ import annotations

import asyncio
from contextlib import closing, contextmanager
import json
import os
import stat
from pathlib import Path
import sqlite3

from . import origin
from .transport import Unavailable


# task_runs' documented schema plus ended-run values written by native
# kanban_db (_end_run/_synthesize_ended_run), kanban_db_controls and
# kanban_db_dispatch (_DeadWorker/_record_task_failure). This is evidence
# validation only: unfamiliar future native states require native reconciliation.
_SETTLED_RUN_STATUSES = frozenset({
    "done", "blocked", "crashed", "timed_out", "failed", "released",
    "reclaimed", "stale", "rate_limited", "spawn_failed", "gave_up",
    "review", "ready", "todo", "scheduled", "completed", "review_requested",
})


class Unknown(RuntimeError):
    pass


def _require(value, reason):
    if not value:
        raise Unknown(reason)


@contextmanager
def _read(path):
    from hermes_cli.sqlite_safe_read import connect_tracked
    with closing(connect_tracked(Path(path).resolve(strict=True).as_uri() + "?mode=ro", uri=True, isolation_level=None)) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        yield db


def _binding(state, issue, profile):
    if not Path(state).is_file():
        return None
    with _read(state) as db:
        row = db.execute("SELECT value FROM metadata WHERE key=?", ("native-binding:" + issue,)).fetchone()
        work = db.execute("SELECT origin,task_id FROM work WHERE issue_id=?", (issue,)).fetchone()
        fenced = db.execute("SELECT 1 FROM specialist_scope_fence WHERE issue_id=?", (issue,)).fetchone()
        _require(not fenced, "issue_scope_fenced")
        if not row or not work or work["origin"] != "native":
            return None
        bound = json.loads(row[0])
        if not (bound.get("issue_id") == issue and bound.get("profile") == profile
                and bound.get("task_id") == work["task_id"] and isinstance(bound.get("board"), str)):
            return None  # A stale reporting link does not replace inspection of actual local work.
        return bound


def _discover(home, issue, profile, native_profile, metadata, bound=None):
    from .bridge import Kanban
    from hermes_cli import kanban_db as kb
    class ReadOnlyDiscovery(Kanban):
        def __init__(self):
            # Bypass the ordinary adapter's create/migrate constructor. Its existing discovery
            # algorithm and inherited strict tracked read-only history reader remain unchanged.
            self.kb, self.board = kb, kb.get_current_board()
            self.executor_profile, self.profile_home = native_profile, home
    reader = ReadOnlyDiscovery()
    found = reader.discover(metadata or {"id": issue})
    if bound is not None:
        # A reporting link can include marker-free manual work, but cannot
        # hide any other retained executor discovered through native history.
        _require(len(found) <= 1 and all(adapter.board == bound["board"] and task.id == bound["task_id"]
                                        for adapter, task in found), "native_history_match_missing_or_ambiguous")
    else:
        _require(len(found) == 1, "native_history_match_missing_or_ambiguous")
    if found:
        adapter, task = found[0]
        _require(task.assignee == native_profile and not getattr(adapter, "history_only", False),
                 "native_history_match_foreign_or_archived")
    if bound is None:
        bound = {"issue_id": issue, "task_id": task.id, "board": adapter.board, "profile": profile}
    return bound, reader.for_board(bound["board"])


def _pid_gone(pid, fingerprint, owner=None):
    """Compose native epoch and birth validation; never infer identity from junk."""
    from hermes_cli.kanban_db_dispatch import _process_fingerprint
    from hermes_cli.kanban_db import _pid_alive
    from gateway.drain_control import current_instantiation_epoch
    from gateway.status import _start_times_agree, start_time_fingerprints_match
    _require(type(pid) is int and pid > 0, "worker_identity_missing")
    epoch = current_instantiation_epoch()
    _require(isinstance(epoch, str) and epoch, "worker_identity_unreadable")
    def birth(witness, reason):
        _require(isinstance(witness, str) and witness.count("|") == 1, reason)
        recorded_epoch, _, start = witness.partition("|")
        _require(recorded_epoch == epoch, reason)
        try:
            valid = _start_times_agree(start, start)
        except (TypeError, ValueError, OverflowError):
            valid = False
        _require(valid, reason)
        return start
    recorded = birth(fingerprint, "worker_identity_missing")
    if not _pid_alive(pid):
        return "gone"
    current = birth(_process_fingerprint(pid), "worker_identity_unreadable")
    # Linux's native fingerprint may fall back from boot ticks to epoch
    # centiseconds on a transient /proc read failure. Neither persisted witness
    # identifies which source produced it: compare both native representations
    # rather than interpreting a source change as PID reuse.
    import psutil
    from hermes_cli.process_identity import process_birth, worker_birth
    try:
        process = psutil.Process(pid)
        epoch_birth = round(process_birth(process) * 100)
        readings = [epoch_birth]
        monotonic_birth = worker_birth(process)
        if psutil.LINUX:
            readings.append(monotonic_birth * os.sysconf("SC_CLK_TCK"))
    except psutil.Error as exc:
        raise Unknown("worker_identity_unreadable") from exc
    _require(all(_start_times_agree(value, value) for value in readings), "worker_identity_unreadable")
    _require(any(start_time_fingerprints_match(float(current), value) for value in readings),
             "worker_identity_unreadable")
    _require(not any(start_time_fingerprints_match(float(recorded), value) for value in readings),
             "old_worker_alive")
    # Legacy fingerprint numbers do not identify their historical source.
    # A clock step can move an epoch fallback while its Linux boot birth stays
    # unchanged. Only this exact attempt's fully matched native admission
    # receipt supplies the original native worker_birth, never a PID-only join.
    _require(isinstance(owner, dict) and type(owner.get("pid")) is int and owner["pid"] == pid
             and type(owner.get("birth")) in (int, float), "worker_identity_unproven")
    original_birth = owner["birth"]
    _require(_start_times_agree(original_birth, original_birth), "worker_identity_unproven")
    _require(not _start_times_agree(monotonic_birth, original_birth), "old_worker_alive")
    # Recheck the native identity after the independent reads; a changing
    # incarnation or source that neither reading explains stays uncertain.
    repeated = birth(_process_fingerprint(pid), "worker_identity_unreadable")
    _require(any(start_time_fingerprints_match(float(repeated), value) for value in readings),
             "worker_identity_unreadable")
    try:
        repeated_birth = worker_birth(process)
    except psutil.Error as exc:
        raise Unknown("worker_identity_unreadable") from exc
    _require(_start_times_agree(monotonic_birth, repeated_birth), "worker_identity_unreadable")
    return "recycled"


def _checkpoint(home, owners):
    """Read the canonical scoped checkpoint only; never recover or load a process manager."""
    path = home / "processes.json"
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return {"scoped_records": 0, "recycled_records": 0}
    # A dangling symlink is a retained obligation, not genuine absence. Never
    # follow a checkpoint link or treat inaccessible metadata as an empty file.
    _require(stat.S_ISREG(metadata.st_mode) and metadata.st_size <= 1024 * 1024,
             "checkpoint_unreadable")
    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            _require(key not in value, "checkpoint_duplicate_key")
            value[key] = item
        return value
    entries = json.loads(path.read_text(encoding="utf-8-sig"), object_pairs_hook=unique_object)
    _require(isinstance(entries, list), "checkpoint_unreadable")
    for entry in entries:
        _require(isinstance(entry, dict), "checkpoint_ownership_ambiguous")
        route = entry.get("session_key")
        attributed = [entry[key] for key in ("parent_session_id", "owner_task_id", "task_id")
                      if entry.get(key) is not None]
        complete = bool(attributed) and all(isinstance(value, str) and value
                                           and value == value.strip() for value in (route, *attributed))
        if (complete and all(route not in keys for keys, _ in owners)
                and all(owner not in ids for owner in attributed for _, ids in owners)):
            continue  # Only positively foreign records can be excluded.
        _require(complete and any(route in keys and all(owner in ids for owner in attributed)
                                  for keys, ids in owners), "checkpoint_ownership_ambiguous")
        # Native Stop retains both legacy and managed checkpoints until its
        # scope, reader and durable ownership/output obligations are reconciled.
        # Neither leader fate nor an exit file lets this reader discharge them.
        raise Unknown("scoped_process_cleanup_unknown")
    return {"scoped_records": 0, "recycled_records": 0}


def _snapshot(home, state, issue, binding_profile, native_profile, stop_id, declared, metadata):
    from hermes_cli import kanban_db as kb
    from hermes_state import SessionDB
    from hermes_state_local import local_receipt
    from hermes_state_keys import local_identity
    from hermes_state_runtime import find_session_admission, list_session_admissions
    from hermes_state_runtime_workers import runtime_lineage, worker_states, descendant_range
    from gateway.session_policy import restore_policy
    from gateway import session_ancillary
    getter = getattr(session_ancillary, "process_stop_pending", None)
    _require(getter is not None, "native_stop_checkpoint_unsupported")
    # The ingress/deployment routing label can differ from the executor's native profile
    # (isolated fleet homes use default). It must never select a home or reject its valid link.
    bound = _binding(state, issue, native_profile)
    source = "saved_native_binding" if bound else "native_history_discovery"
    bound, reader = _discover(home, issue, native_profile, native_profile, metadata, bound)
    path = kb.kanban_db_path(board=bound["board"]).resolve(strict=True)
    receipts, owners, worker_evidence = [], [], []
    with reader.history_conn(path, snapshot=True) as board, SessionDB(home / "state.db", read_only=True) as db:
        from .bridge import task_issue_match, native_text_row
        discovered = reader.tasks(board)
        _require(all(task.id == bound["task_id"] for task in discovered
                     if task_issue_match(task, metadata or {"id": issue})), "native_history_match_missing_or_ambiguous")
        selected = [task for task in discovered if task.id == bound["task_id"]]
        _require(len(selected) == 1, "native_task_missing_or_ambiguous")
        reader.task_keys(board, "task_events")
        reader.task_keys(board, "task_runs")
        task = board.execute("SELECT * FROM tasks WHERE id=?", (bound["task_id"],)).fetchone()
        _require(task and task["status"] == "blocked" and task["block_kind"] == "needs_input"
                 and task["current_run_id"] is None and task["claim_lock"] is None
                 and task["assignee"] == native_profile, "task_not_currently_stopped")
        for key in board.execute("SELECT run_id FROM task_events"):
            native_text_row(key, "run_id")
        _require(board.execute("SELECT 1 FROM task_events e JOIN task_runs r ON e.run_id=r.id "
                               "WHERE r.task_id=? AND e.task_id IS NOT ? LIMIT 1",
                               (task["id"], task["id"])).fetchone() is None, "attempt_event_task_mismatch")
        events = [{"id": event.id, "run_id": event.run_id, "kind": event.kind,
                   "payload": json.dumps(event.payload)} for event in reader.history_rows(board, task["id"])]
        stops = [e for e in events if e["kind"] == "blocked" and (json.loads(e["payload"] or "{}" ).get("kind") == "needs_input")]
        _require(stops and stops[-1]["id"] == stop_id, "stop_event_changed")
        runs = board.execute("SELECT * FROM task_runs WHERE task_id=? ORDER BY id", (task["id"],)).fetchall()
        by_run = {run["id"]: run for run in runs}
        bindings = [e for e in events if e["kind"] == "owner_admitted"]
        workers = [e for e in events if e["kind"] in {"spawned", "worker_registered", "worker_bound"}]
        _require(bindings and all(e["id"] < stop_id for e in bindings), "admission_after_stop_or_missing")
        _require(all(e["run_id"] in by_run for e in bindings + workers), "attempt_event_run_unknown")
        # A submitter can spawn before native admission. Inspect every retained run
        # and worker event before requiring admission evidence; absence is never safety.
        for run in runs:
            native_run = kb.Run.from_row(native_text_row(run, "id", "task_id", "status", "profile", "ended_at"))
            _require(native_run.status in _SETTLED_RUN_STATUSES
                     and native_run.ended_at is not None and native_run.profile == native_profile, "run_not_settled")
        for worker in workers:
            _require(worker["id"] < stop_id, "late_worker_binding_after_stop")
        for run in runs:
            admitted = [e for e in bindings if e["run_id"] == run["id"]]
            _require(len(admitted) == 1, "attempt_native_admission_missing_or_ambiguous")
            event = admitted[0]
            run_id, marker = run["id"], json.loads(event["payload"])
            sid = marker["session_id"]
            _require(Path(marker["db"]).resolve() == home / "state.db", "foreign_native_owner")
            saved = local_receipt(db, sid)
            policy = restore_policy(saved["policy"])
            frozen = json.loads(policy.kanban_json or "{}")
            request = "kanban:" + json.dumps([bound["board"], task["id"], run_id], separators=(",", ":"))
            _require(saved["profile_id"] == str(home) and policy.source == "kanban"
                     and frozen.get("board") == bound["board"] and frozen.get("task_id") == task["id"]
                     and frozen.get("run_id") == run_id and frozen.get("profile") == native_profile
                     and frozen.get("claim_lock") == run["claim_lock"]
                     and Path(frozen.get("db", "")).resolve() == path and policy.cwd == frozen.get("workspace")
                     and saved["request_id"] == request and marker["request_id"] == request
                     and sid == local_identity(str(home), saved["principal_id"], request), "native_attempt_binding_mismatch")
            admission = find_session_admission(db, principal_id=saved["principal_id"], session_id=sid, request_id=request)
            _require(admission and admission["status"] == "terminal" and admission["outcome"] in {"completed", "interrupted", "failed", "rejected"}
                     and admission["payload"].get("kanban_card_v1") == {"board": bound["board"], "task_id": task["id"], "run_id": run_id},
                     "native_admission_not_settled")
            if run["worker_pid"] is not None or run["worker_started_at"] is not None:
                _pid_gone(run["worker_pid"], run["worker_started_at"], marker)
            _require(all(r["status"] == "terminal" and r.get("outcome") not in {"steering"}
                         for r in list_session_admissions(db, session_id=sid, pending_only=False)), "native_session_has_unsettled_work")
            with db._read_ctx() as native:
                lineage = runtime_lineage(native, sid)
                _require(not worker_states(native, sid), "native_worker_or_spawner_unsettled")
                executor = native.execute("SELECT status,owner_epoch,generation FROM worker_executions WHERE execution_id=?",
                                          ("admission-worker:" + admission["admission_id"],)).fetchone()
                _require(executor and tuple(executor) == ("terminal", admission["owner_epoch"], admission["generation"]),
                         "native_executor_settlement_missing")
                children = native.execute("SELECT session_id,status FROM worker_executions WHERE execution_id>=? AND execution_id<?",
                                          descendant_range("admission-worker:" + admission["admission_id"])).fetchall()
                _require(all(child["status"] == "terminal" for child in children), "native_child_spawner_unsettled")
                epoch = native.execute("SELECT epoch,instance_id FROM runtime_epoch WHERE singleton=1").fetchone()
                _require(epoch is not None, "native_owner_unknown")
            _require(getter(db, sid) is None, "native_stop_checkpoint_pending")
            relevant = [e for e in events if e["run_id"] == run_id and e["kind"] in {"spawned", "worker_registered", "worker_bound"}]
            _require(any(e["kind"] == "worker_bound" for e in relevant)
                     and any(e["kind"] in {"spawned", "worker_registered"} for e in relevant), "worker_binding_evidence_missing")
            observed = []
            submitters = [json.loads(e["payload"]) for e in relevant if e["kind"] in {"spawned", "worker_registered"}]
            _require(any(p.get("pid") == run["worker_pid"] and p.get("started_at") == run["worker_started_at"] for p in submitters),
                     "submitter_run_identity_missing")
            for worker in relevant:
                data = json.loads(worker["payload"])
                _require(worker["id"] < stop_id, "late_worker_binding_after_stop")
                if worker["kind"] == "worker_bound":
                    _require(data.get("claim_lock") == frozen.get("claim_lock"), "worker_claim_mismatch")
                observed.append({"kind": worker["kind"], "fate": _pid_gone(data.get("pid"), data.get("started_at"), marker)})
            worker_evidence.append({"run_id": run_id, "workers": observed,
                "scope": {key: frozen.get(key) for key in ("profile", "workspace", "branch", "tenant", "skills")}})
            owners.append(({saved["route"], *lineage}, {*lineage, *(child["session_id"] for child in children)}))
            receipts.append({"session_id": sid, "admission_id": admission["admission_id"],
                             "status": admission["status"], "outcome": admission["outcome"],
                             "authority_epoch": admission["owner_epoch"], "execution_generation": admission["generation"]})
        checkpoint = _checkpoint(home, owners)
        projection = {"issue_id": issue, "board": bound["board"], "task_id": bound["task_id"],
                      "stop_event_id": stop_id, "latest_event_id": events[-1]["id"],
                      "attempts": worker_evidence, "scoped_checkpoint": checkpoint,
                      "stop_checkpoint": "clear", "scope": declared, "discovery_source": source,
                      "owner_epoch": epoch[0], "owner_instance_id": epoch[1]}
        digest = origin._digest({"binding": bound, "task": dict(task), "events": [dict(e) for e in events], "runs": [dict(run) for run in runs],
                                 "receipts": receipts, "projection": projection})
        return projection, receipts, digest


async def _inspect(home, state, issue, binding_profile, native_profile, stop_id, declared, metadata):
    async with origin._client(home) as (client, endpoint):
        before, rows, digest = await asyncio.to_thread(_snapshot, home, state, issue, binding_profile, native_profile, stop_id, declared, metadata)
        _require(before["owner_epoch"] == endpoint.authority_epoch and before["owner_instance_id"] == endpoint.instance_id,
                 "native_owner_changed")
        for row in rows:
            receipt = await client.rpc("prompt.receipt", session_id=row["session_id"], admission_id=row["admission_id"])
            _require(all(receipt.get(k) == row[k] for k in ("admission_id", "status", "outcome", "authority_epoch", "execution_generation"))
                     and receipt.get("ref") == {"profile_id": str(home), "session_id": row["session_id"]}, "native_receipt_changed")
        after, _, repeated = await asyncio.to_thread(_snapshot, home, state, issue, binding_profile, native_profile, stop_id, declared, metadata)
        _require(digest == repeated and before == after, "cleanup_evidence_changed")
        return {**after, "owner_instance_id": endpoint.instance_id, "owner_epoch": endpoint.authority_epoch,
                "evidence_digest": digest, "quiescence_proven": True, "permission_granted": False}


def inspect(state: Path, issue: str, binding_profile: str, stop_id: int, settings: dict, metadata: dict | None = None) -> dict:
    """Explicit owner CLI boundary; selects only a current saved Linear/native binding."""
    try:
        from hermes_constants import get_hermes_home
        from hermes_cli.profiles import get_active_profile_name
        from agent.delegation_context import is_delegated_child_process_context
        from agent.runtime_session_store import is_worker_process
        _require(not is_delegated_child_process_context() and not is_worker_process(), "owner_cli_required")
        _require(isinstance(issue, str) and 0 < len(issue) <= 1024 and type(stop_id) is int and stop_id > 0, "invalid_cleanup_selector")
        if metadata is not None:
            _require(isinstance(metadata, dict) and metadata.get("id") == issue and set(metadata) <= {"id", "identifier", "url"}
                     and all(isinstance(v, str) and 0 < len(v) <= 4096 for v in metadata.values()), "invalid_issue_discovery_metadata")
        home = Path(get_hermes_home()).resolve(strict=True)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise Unknown("synchronous_owner_cli_required")
        return asyncio.run(_inspect(home, state, issue, binding_profile, get_active_profile_name(), stop_id, origin.scope(settings), metadata))
    except (Unknown, Unavailable, OSError, RuntimeError, ValueError, KeyError, IndexError, TypeError, ImportError, sqlite3.DatabaseError) as exc:
        reason = str(exc) if isinstance(exc, Unknown) else "cleanup_evidence_unavailable"
        return {"issue_id": issue, "stop_event_id": stop_id, "quiescence_proven": False,
                "state": "unknown", "reason": reason, "permission_granted": False}
