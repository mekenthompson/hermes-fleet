"""Async runtime adapter for one profile-local Linear worker."""
from __future__ import annotations

import asyncio
import contextvars
import inspect
import math
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path

from .linear_agent import (
    CLAIM_THOUGHT,
    HEARTBEAT_THOUGHT,
    LinearWorker,
    QueuedLinearJob,
)
from .linear_completion import accepted_completion_evidence
from .linear_guard_health import WorkerGuardHealth
from .linear_handoff import HandoffDenied
from .linear_resume import ResumeDenied
from .linear_stop import acknowledge_lifetime, acknowledge_stop_delivery

_MISSING = object()

LINEAR_WORKTREE: contextvars.ContextVar[Path | None] = contextvars.ContextVar(
    "linear_issue_worktree", default=None
)


def pin_issue_worktree_cwd(worktree: Path | None) -> None:
    """Pin the current session cwd to the bound issue worktree."""
    if worktree is None:
        return
    try:
        from agent.runtime_cwd import set_session_cwd
    except ImportError:
        return
    set_session_cwd(str(worktree))


def install_worktree_cwd_pin(gateway: object) -> None:
    """Re-pin issue worktree cwd after gateway session env overwrites it."""
    original = getattr(gateway, "_set_session_env", None)
    if not callable(original) or getattr(gateway, "_linear_worktree_cwd_wrapped", False):
        return

    def wrapped(context: object) -> object:
        tokens = original(context)
        pin_issue_worktree_cwd(LINEAR_WORKTREE.get())
        return tokens

    setattr(gateway, "_set_session_env", wrapped)
    setattr(gateway, "_linear_worktree_cwd_wrapped", True)


class _EmissionAdmission:
    """Serialize local emission admission with terminal close.

    Admission, not remote completion, is fenced: an admitted callback may still
    reach its remote endpoint after close starts and is therefore uncertain.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._closed = False
        self._inflight = 0

    def call(self, callback: Callable[[], object]) -> tuple[bool, object | None]:
        with self._condition:
            if self._closed:
                return False, None
            self._inflight += 1
        try:
            return True, callback()
        finally:
            with self._condition:
                self._inflight -= 1
                if self._inflight == 0:
                    self._condition.notify_all()

    def close_and_wait(self, timeout_seconds: float) -> bool:
        """Close admission and boundedly wait for local callback return only."""
        deadline = time.monotonic() + timeout_seconds
        with self._condition:
            self._closed = True
            while self._inflight:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True


@dataclass
class _ActiveSlot:
    job: QueuedLinearJob
    execution_id: str
    task: asyncio.Task[bool]
    quarantined: bool = False


class ProfileLinearRuntime:
    """Admit sessions immediately while a bounded executor pool runs issue turns."""

    def __init__(
        self,
        worker: LinearWorker,
        ingress_database: Path,
        execute: Callable[..., Awaitable[str]],
        emit: Callable[[str, str, str], object],
        *,
        prepare: Callable[..., Awaitable[object]] | None = None,
        request_stop: Callable[[str, str], Awaitable[dict[str, object]]] | None = None,
        lifecycle: Callable[[str, str], Awaitable[dict[str, object]]] | None = None,
        heartbeat_seconds: float = 600.0,
        guard_health: WorkerGuardHealth | None = None,
        emission_drain_timeout_seconds: float = 1.0,
        dependency_readiness: Callable[[str | None], bool] | None = None,
        budget_ledger: object | None = None,
    ) -> None:
        self._worker = worker
        self._ingress_database = ingress_database
        self._execute = self._adapt_execute(execute) if execute is not None else self._no_execute
        self._request_stop = request_stop or self._no_request_stop
        self._lifecycle = lifecycle or self._no_lifecycle
        self._emit = emit
        self._prepare = self._adapt_prepare(prepare) if prepare is not None else self._no_prepare
        self._heartbeat_seconds = float(heartbeat_seconds)
        self._guard_health = guard_health
        self._dependency_readiness = dependency_readiness
        self._budget_ledger = budget_ledger
        drain_timeout_seconds = float(emission_drain_timeout_seconds)
        if (
            not math.isfinite(drain_timeout_seconds)
            or drain_timeout_seconds > threading.TIMEOUT_MAX
        ):
            raise ValueError(
                "emission_drain_timeout_seconds must be finite and no greater "
                "than threading.TIMEOUT_MAX"
            )
        self._emission_drain_timeout_seconds = max(0.0, drain_timeout_seconds)
        self._emission_admission = _EmissionAdmission()
        # One physical best-effort send at a time, across job boundaries. Never
        # cancel this future: cancellation cannot stop its executor thread and
        # would make a second send look safe while the first is still blocked.
        self._thought_future: asyncio.Future[None] | None = None
        self._slots: dict[str, _ActiveSlot] = {}
        self._pending_terminal_release: tuple[QueuedLinearJob, object, str] | None = None
        self._active_job_override: object = _MISSING
        self._closing = False

    def _bind_slot(
        self,
        job: QueuedLinearJob,
        execution_id: str,
        task: asyncio.Task[bool],
        *,
        quarantined: bool = False,
    ) -> _ActiveSlot:
        slot = _ActiveSlot(
            job=job,
            execution_id=execution_id,
            task=task,
            quarantined=quarantined,
        )
        self._slots[execution_id] = slot
        return slot

    def active_job_count(self) -> int:
        return len(self._slots)

    def slot_for_session(self, linear_session_id: str) -> _ActiveSlot | None:
        for slot in self._slots.values():
            if slot.job.linear_session_id == linear_session_id:
                return slot
        return None

    def _sole_slot(self) -> _ActiveSlot | None:
        if len(self._slots) != 1:
            return None
        return next(iter(self._slots.values()))

    @property
    def _active_job(self) -> QueuedLinearJob | None:
        if self._active_job_override is not _MISSING:
            job = self._active_job_override
            return job if job is None or isinstance(job, QueuedLinearJob) else None
        slot = self._sole_slot()
        return None if slot is None else slot.job

    @_active_job.setter
    def _active_job(self, job: QueuedLinearJob | None) -> None:
        self._active_job_override = job

    @property
    def _active_task(self) -> asyncio.Task[bool] | None:
        slot = self._sole_slot()
        return None if slot is None else slot.task

    @property
    def _active_execution_id(self) -> str | None:
        slot = self._sole_slot()
        return None if slot is None else slot.execution_id

    @property
    def _active_quarantined(self) -> bool:
        slot = self._sole_slot()
        return False if slot is None else slot.quarantined

    @staticmethod
    async def _no_prepare(_session_key: str, _job: QueuedLinearJob) -> None:
        return None

    @staticmethod
    def _adapt_prepare(prepare: Callable[..., Awaitable[object]]) -> Callable[[str, QueuedLinearJob], Awaitable[object]]:
        try:
            inspect.signature(prepare).bind("session", object())
        except (TypeError, ValueError):
            try:
                inspect.signature(prepare).bind("session")
            except (TypeError, ValueError) as exc:
                raise TypeError("prepare must accept (session_key[, job])") from exc
            return lambda session_key, _job: prepare(session_key)
        return lambda session_key, job: prepare(session_key, job)

    @staticmethod
    async def _no_request_stop(_session_key: str, _execution_id: str) -> dict[str, object]:
        raise RuntimeError("exact core Stop delivery is unavailable")

    @staticmethod
    async def _no_lifecycle(_session_key: str, _execution_id: str) -> dict[str, object]:
        raise RuntimeError("exact core lifecycle receipt is unavailable")

    @staticmethod
    def _safe_terminal_receipt(receipt: object, session_key: str, execution_id: str) -> bool:
        """Accept only the core v2 proof that this exact execution is released."""
        return accepted_completion_evidence(
            receipt, session_key=session_key, execution_id=execution_id
        )

    @staticmethod
    async def _no_execute(_session_key: str, _prompt: str, _execution_id: str, _job: QueuedLinearJob) -> str:
        raise RuntimeError("Linear execution callback is unavailable")

    @staticmethod
    def _adapt_execute(execute: Callable[..., Awaitable[str]]) -> Callable[[str, str, str, QueuedLinearJob], Awaitable[str]]:
        """Pass the canonical job when supported; retain older callback ABIs."""
        try:
            signature = inspect.signature(execute)
            signature.bind("session", "prompt", "execution", object())
        except (TypeError, ValueError):
            try:
                signature = inspect.signature(execute)
                signature.bind("session", "prompt", "execution")
            except (TypeError, ValueError) as exc:
                try:
                    signature = inspect.signature(execute)
                    signature.bind("session", "prompt")
                except (TypeError, ValueError) as nested_exc:
                    raise TypeError("execute must accept (session_key, prompt[, execution_id[, job]])") from nested_exc
                return lambda session_key, prompt, _execution_id, _job: execute(session_key, prompt)
            return lambda session_key, prompt, execution_id, _job: execute(session_key, prompt, execution_id)
        return lambda session_key, prompt, execution_id, job: execute(session_key, prompt, execution_id, job)

    def _job_for_thought(
        self, session_id: str, job: QueuedLinearJob | None
    ) -> QueuedLinearJob | None:
        if job is not None:
            return job
        slot = self.slot_for_session(session_id)
        if slot is not None:
            return slot.job
        return self._active_job

    async def _drain_live_thoughts(self) -> None:
        future = self._thought_future
        if future is None or future.done():
            return
        with suppress(Exception):
            await future

    async def _release_shared_if_terminal(
        self,
        job: QueuedLinearJob,
        receipt: object,
        execution_id: str,
        released: bool,
    ) -> None:
        if not released:
            return
        await self._drain_live_thoughts()
        await asyncio.to_thread(
            self._worker.queue_terminal_closeout, job, receipt, execution_id
        )
        self._pending_terminal_release = (job, receipt, execution_id)

    async def _finalize_terminal_release(self) -> bool:
        pending = self._pending_terminal_release
        if pending is None:
            pending = await asyncio.to_thread(self._worker.pending_terminal_closeout)
        if pending is None:
            return False
        self._pending_terminal_release = pending
        job, receipt, execution_id = pending
        await self._drain_live_thoughts()
        await self._flush_all()
        settled = await asyncio.to_thread(
            self._worker.terminal_outbox_settled, job.delivery_id
        )
        if not settled:
            return False
        try:
            await asyncio.to_thread(
                self._worker.release_shared_after_stop,
                job,
                receipt,
                session_key=job.hermes_session_key,
                execution_id=execution_id,
            )
        except HandoffDenied as exc:
            if "in flight" not in str(exc):
                raise
            return False
        await asyncio.to_thread(self._worker.clear_terminal_closeout, job.delivery_id)
        self._pending_terminal_release = None
        return True

    def _emit_thought(
        self, session_id: str, body: str, job: QueuedLinearJob | None = None
    ) -> None:
        if job is None:
            job = self._job_for_thought(session_id, None)
        if job is not None:
            ticket_ctx = None
            try:
                ticket_ctx = self._worker.begin_live_activity(job, "thought")
            except Exception:  # noqa: BLE001 - live thoughts are best-effort
                return
            try:
                def emit() -> object:
                    self._worker.authorize_live_activity(job, "thought")
                    return self._emit(session_id, "thought", body)

                self._emission_admission.call(emit)
            except Exception:  # noqa: BLE001 - live thoughts are best-effort
                return
            finally:
                self._worker.finish_live_activity(ticket_ctx)
            return
        if getattr(self._worker, "shared_authority", None) is not None:
            return
        # No claimed job and no shared store: keep the best-effort admission
        # path so progress resume and shutdown drain still work.
        try:
            self._emission_admission.call(
                lambda: self._emit(session_id, "thought", body),
            )
        except Exception:  # noqa: BLE001 - live thoughts are best-effort
            return

    def _queue_thought(
        self, session_id: str, body: str, job: QueuedLinearJob | None = None
    ) -> None:
        """Drop redundant progress while the previous send is physically busy."""
        if self._closing or (
            self._thought_future is not None and not self._thought_future.done()
        ):
            return
        captured = self._job_for_thought(session_id, job)
        # Always set the future, including the no-job best-effort path.
        self._thought_future = asyncio.get_running_loop().run_in_executor(
            None,
            contextvars.copy_context().run,
            self._emit_thought,
            session_id,
            body,
            captured,
        )

    async def _flush_one(self) -> bool:
        def emit_if_admitted(session_id: str, activity_type: str, body: str) -> object:
            admitted, outcome = self._emission_admission.call(
                lambda: self._emit(session_id, activity_type, body),
            )
            if not admitted:
                return False
            return outcome

        try:
            return await asyncio.to_thread(
                self._worker.dispatch_outbox,
                emit_if_admitted,
            )
        except Exception:  # noqa: BLE001 - external-send ambiguity is quarantined
            return False

    async def _flush_all(self) -> bool:
        flushed = False
        while not self._closing:
            if not await self._flush_one():
                break
            flushed = True
        return flushed

    async def _execute_job(self, job: QueuedLinearJob, execution_id: str | None = None) -> bool:
        if not execution_id:
            raise RuntimeError("active Linear executor has no canonical execution ID")
        stop = asyncio.Event()

        async def heartbeat() -> None:
            if self._heartbeat_seconds <= 0:
                return
            while True:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._heartbeat_seconds)
                    return
                except TimeoutError:
                    self._queue_thought(job.linear_session_id, HEARTBEAT_THOUGHT, job)

        hb = asyncio.create_task(heartbeat())
        try:
            self._queue_thought(job.linear_session_id, CLAIM_THOUGHT, job)
            try:
                worktree = await asyncio.to_thread(self._worker.bind_issue_worktree, job)
            except HandoffDenied:
                await asyncio.to_thread(self._worker.fail_job, job)
                return True
            if worktree is not None:
                job = replace(job, execution_worktree=worktree)
            token = LINEAR_WORKTREE.set(worktree)
            try:
                pin_issue_worktree_cwd(worktree)
                try:
                    response = str(
                        await self._execute(job.hermes_session_key, job.prompt, execution_id, job)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - emit only a redacted terminal response
                    await asyncio.to_thread(self._worker.fail_job, job)
                else:
                    try:
                        receipt = await self._lifecycle(job.hermes_session_key, execution_id)
                    except Exception:  # noqa: BLE001 - missing receipt is not terminal proof
                        receipt = {}
                    released = self._safe_terminal_receipt(
                        receipt, job.hermes_session_key, execution_id
                    )
                    await asyncio.to_thread(
                        self._worker.record_lifecycle_receipt,
                        job,
                        execution_id,
                        receipt,
                        released=released,
                    )
                    await asyncio.to_thread(
                        self._worker.complete_job,
                        job,
                        response,
                        closeout_receipt=receipt if released else None,
                        closeout_execution_id=execution_id if released else None,
                    )
                    stop.set()
                    hb.cancel()
                    with suppress(asyncio.CancelledError):
                        await hb
                    await self._release_shared_if_terminal(job, receipt, execution_id, released)
            finally:
                LINEAR_WORKTREE.reset(token)
            return True
        finally:
            stop.set()
            hb.cancel()
            with suppress(asyncio.CancelledError):
                await hb

    async def _collect_finished_task(self) -> bool:
        finished = False
        errors: list[BaseException] = []
        for slot in list(self._slots.values()):
            if slot.task is None or not slot.task.done():
                continue
            try:
                finished = await self._collect_slot(slot) or finished
            except Exception as exc:  # noqa: BLE001 - persist remaining slots first
                errors.append(exc)
        if errors:
            raise errors[0]
        return finished

    async def _collect_slot(self, slot: _ActiveSlot) -> bool:
        task = slot.task
        job = slot.job
        execution_id = slot.execution_id
        if task.cancelled():
            # A cancelled wrapper may have detached from a to_thread worker. Recheck
            # only this execution's core receipt: it can release the slot only with
            # v2's physical-completion/no-effects proof.
            try:
                receipt = await self._lifecycle(job.hermes_session_key, execution_id)
            except Exception:  # noqa: BLE001 - lookup failure preserves ambiguity
                receipt = {}
            released = self._safe_terminal_receipt(receipt, job.hermes_session_key, execution_id)
            await asyncio.to_thread(
                self._worker.record_lifecycle_receipt, job, execution_id, receipt, released=released
            )
            if released:
                await self._release_shared_if_terminal(job, receipt, execution_id, released)
                self._slots.pop(execution_id, None)
                return True
            # asyncio cancellation is not evidence that a worker, tool, child,
            # process, or remote effect stopped. Commit the durable fence once
            # and retain the exact identity and occupancy.
            if not slot.quarantined:
                await asyncio.to_thread(self._worker.quarantine_interrupted, job)
                slot.quarantined = True
            return True
        if await asyncio.to_thread(self._worker.stop_requested, job):
            # An asyncio wrapper returning is not proof that its thread, tools,
            # children, processes, or remote effects are terminal.
            try:
                receipt = await self._lifecycle(job.hermes_session_key, execution_id)
            except Exception:  # noqa: BLE001 - lifecycle lookup failure is durable ambiguity
                receipt = {}
            released = self._safe_terminal_receipt(receipt, job.hermes_session_key, execution_id)
            await asyncio.to_thread(
                self._worker.record_lifecycle_receipt, job, execution_id, receipt, released=released
            )
            if released:
                await self._release_shared_if_terminal(job, receipt, execution_id, released)
            if not released:
                if not slot.quarantined:
                    await asyncio.to_thread(self._worker.quarantine_interrupted, job)
                    slot.quarantined = True
                return True
        self._slots.pop(execution_id, None)
        return await task

    async def _deliver_active_stop(self) -> bool:
        delivered = False
        for slot in list(self._slots.values()):
            delivered = await self._deliver_slot_stop(slot) or delivered
        return delivered

    async def _deliver_slot_stop(self, slot: _ActiveSlot) -> bool:
        job, execution_id = slot.job, slot.execution_id
        if not await asyncio.to_thread(self._worker.stop_requested, job):
            return False
        reserved = await asyncio.to_thread(
            self._worker.begin_stop_delivery, job, execution_id
        )
        if not reserved:
            return False
        try:
            receipt = await self._request_stop(job.hermes_session_key, execution_id)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - any external Stop failure is ambiguous; retain pending receipt
            return False
        acknowledgement = acknowledge_stop_delivery(
            receipt, job.hermes_session_key, execution_id
        )
        if not acknowledgement.acknowledged:
            return False
        return await asyncio.to_thread(
            self._worker.record_stop_delivery, job, execution_id, receipt["status"]
        )

    async def revoke_session(self, linear_session_id: str, *, cause: str) -> bool:
        """Propagate reassignment/permission loss into queued and running work.

        The gateway receives the same exact-execution Stop request as a native
        stop event. Cancelling the local task is only a fence: lifecycle proof
        remains mandatory before its slot can ever be released.
        """
        revoked = await asyncio.to_thread(
            self._worker.revoke_session, linear_session_id, cause=cause
        )
        slot = self.slot_for_session(linear_session_id)
        if slot is None:
            return revoked
        if not revoked:
            controlled = await asyncio.to_thread(self._worker.stop_requested, slot.job)
            if not controlled:
                return False
        await self._deliver_slot_stop(slot)
        if not slot.task.done():
            slot.task.cancel()
        return True

    async def handle_human_reassignment(self, linear_session_id: str) -> bool:
        return await self.revoke_session(linear_session_id, cause="reassigned")

    async def handle_permission_loss(self, linear_session_id: str) -> bool:
        return await self.revoke_session(linear_session_id, cause="permission_lost")

    async def run_once(self) -> bool:
        if self._closing:
            return False
        # Only the live runtime publishes readiness; helpers never construct it.
        if self._guard_health is not None:
            await asyncio.to_thread(self._guard_health.publish)
        if self._closing:
            return False
        completed = await self._collect_finished_task()
        imported = await asyncio.to_thread(
            self._worker.import_from_ingress_once,
            self._ingress_database,
        )
        admitted, _admitted_job = await asyncio.to_thread(self._worker.admit_once)
        for slot in list(self._slots.values()):
            cause = await asyncio.to_thread(
                self._worker.control_cause, slot.job.linear_session_id
            )
            if cause == "reassigned":
                await self.handle_human_reassignment(slot.job.linear_session_id)
            elif cause == "permission_lost":
                await self.handle_permission_loss(slot.job.linear_session_id)
        stop_delivered = await self._deliver_active_stop()
        rejected = await asyncio.to_thread(self._worker.reauthorize_recoverable)
        # Re-authorize durable recovery work before any pending admission
        # side effects can be flushed.
        flushed = await self._flush_all()
        flushed = await self._finalize_terminal_release() or flushed
        # Always prepare the oldest durable queue entry. A newly imported event
        # must not overtake work admitted before a restart.
        job = await asyncio.to_thread(self._worker.next_unprepared)
        if job is not None:
            try:
                await self._prepare(job.hermes_session_key, job)
                if self._closing:
                    return imported or admitted or rejected or completed or flushed
                await asyncio.to_thread(self._worker.mark_prepared, job)
            except Exception:  # noqa: BLE001 - preparation failures are redacted
                if self._closing:
                    return imported or admitted or rejected or completed or flushed
                # Preserve Linear's required acknowledgement ordering even when
                # Hermes session preparation fails.
                flushed = await self._flush_all() or flushed
                await asyncio.to_thread(self._worker.fail_job, job)
                job = None

        flushed = await self._flush_all() or flushed
        if self._closing:
            return imported or admitted or rejected or completed or flushed
        started = False
        skipped: set[str] = set()
        while len(self._slots) < self._worker.limits.max_concurrent_jobs:
            queued = await asyncio.to_thread(
                self._worker.claim_prepared, tuple(skipped)
            )
            if queued is None:
                break
            if self._closing:
                # A claim can finish after shutdown has observed no active
                # task. Fence it durably before this loop can publish its
                # active status or create an executor wrapper.
                await asyncio.to_thread(self._worker.quarantine_interrupted, queued)
                break
            flushed = await self._flush_all() or flushed
            if self._closing:
                await asyncio.to_thread(self._worker.quarantine_interrupted, queued)
                break
            try:
                await asyncio.to_thread(self._worker.resume_if_reconciled, queued)
            except ResumeDenied:
                await asyncio.to_thread(self._worker.release_claim, queued)
                skipped.add(queued.delivery_id)
                continue
            if self._dependency_readiness is not None:
                try:
                    dependencies_ready = await asyncio.to_thread(
                        self._dependency_readiness, queued.issue_id
                    )
                except Exception:  # noqa: BLE001 - readiness authority is fail-closed
                    dependencies_ready = False
                if not dependencies_ready:
                    await asyncio.to_thread(self._worker.release_claim, queued)
                    skipped.add(queued.delivery_id)
                    continue
            ready = await asyncio.to_thread(self._worker.execution_ready, queued)
            if self._closing:
                await asyncio.to_thread(self._worker.quarantine_interrupted, queued)
                break
            if not ready:
                suppressed = await asyncio.to_thread(
                    self._worker.execution_suppressed,
                    queued,
                )
                if suppressed:
                    await asyncio.to_thread(self._worker.cancel_job, queued)
                else:
                    await asyncio.to_thread(self._worker.release_claim, queued)
                skipped.add(queued.delivery_id)
                continue
            if self._budget_ledger is not None:
                try:
                    budget_admission = await asyncio.to_thread(
                        self._budget_ledger.admit, queued.issue_id
                    )
                    budget_ready = bool(getattr(budget_admission, "allowed", False))
                except Exception:  # noqa: BLE001 - budget persistence uncertainty is fail-closed
                    budget_ready = False
                if not budget_ready:
                    await asyncio.to_thread(self._worker.release_claim, queued)
                    skipped.add(queued.delivery_id)
                    continue
            execution_id = uuid.uuid4().hex
            task = asyncio.create_task(self._execute_job(queued, execution_id))
            self._bind_slot(queued, execution_id, task)
            started = True
            await asyncio.sleep(0)
            completed = await self._collect_finished_task() or completed
            if completed:
                flushed = await self._flush_all() or flushed
                flushed = await self._finalize_terminal_release() or flushed
        return imported or admitted or rejected or stop_delivered or started or completed or flushed

    async def shutdown(self) -> None:
        """Close local admission; remote completion of pre-admitted sends is unproven."""
        self._closing = True
        # A synchronous outbox thread can be inside an admitted callback. Wait
        # off the event loop and only boundedly: timeout preserves uncertainty,
        # it does not claim the remote operation stopped.
        await asyncio.to_thread(
            self._emission_admission.close_and_wait,
            self._emission_drain_timeout_seconds,
        )
        slots = list(self._slots.values())
        if self._guard_health is not None:
            await asyncio.to_thread(self._guard_health.close)
        if not slots:
            return
        errors: list[BaseException] = []
        unfinished: list[asyncio.Task[bool]] = []
        for slot in slots:
            if slot.task.done():
                continue
            try:
                # Durable fencing must precede cancellation. On DB failure keep
                # the executor reference rather than freeing it.
                await asyncio.to_thread(self._worker.quarantine_interrupted, slot.job)
                slot.quarantined = True
            except Exception as exc:  # noqa: BLE001 - fence remaining slots first
                errors.append(exc)
                continue
            slot.task.cancel()
            unfinished.append(slot.task)
        if unfinished:
            await asyncio.gather(*unfinished, return_exceptions=True)
        try:
            # A cancelled wrapper remains occupied after durable quarantine;
            # shutdown must not turn wrapper completion into a slot release.
            await self._collect_finished_task()
        except Exception as exc:  # noqa: BLE001 - preserve the first fence error
            errors.append(exc)
        if errors:
            raise errors[0]
        # Do not clear identity merely because cancellation ended the wrapper:
        # executor-backed work can still be alive. A closed runtime admits no
        # replacement; a later explicit operator reconciliation releases it.
