"""Profile-local runtime integration tests, no real gateway or Linear calls."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# CI supplies the pinned source fixture. Local source-only verification must
# explicitly provide the candidate core checkout; no workstation fallback is a
# valid substitute for the source ABI under test.
# Pinned-core consumer checks are a marked integration suite. CI exports
# HERMES_CORE_SOURCE; local collection and unittest discovery must not fail
# when that checkout is absent.
CORE_SOURCE = os.environ.get("HERMES_CORE_SOURCE")
ActualGatewayExecutionLifecycleMixin = None


def _load_core_lifecycle():
    global ActualGatewayExecutionLifecycleMixin
    if ActualGatewayExecutionLifecycleMixin is not None:
        return ActualGatewayExecutionLifecycleMixin
    if not CORE_SOURCE:
        raise unittest.SkipTest(
            "HERMES_CORE_SOURCE must name the pinned core lifecycle source checkout"
        )
    core_spec = importlib.util.spec_from_file_location(
        "actual_core_execution_lifecycle",
        Path(CORE_SOURCE) / "gateway" / "execution_lifecycle.py",
    )
    assert core_spec is not None and core_spec.loader is not None
    core_lifecycle = importlib.util.module_from_spec(core_spec)
    sys.modules[core_spec.name] = core_lifecycle
    core_spec.loader.exec_module(core_lifecycle)
    ActualGatewayExecutionLifecycleMixin = core_lifecycle.GatewayExecutionLifecycleMixin
    return ActualGatewayExecutionLifecycleMixin

sys.path.insert(0, str(ROOT))
agent = types.ModuleType("agent")
secret_scope = types.ModuleType("agent.secret_scope")
secret_scope.get_secret = lambda _name, default="": default
agent.secret_scope = secret_scope
sys.modules.setdefault("agent", agent)
sys.modules.setdefault("agent.secret_scope", secret_scope)
PLUGIN_DIR = ROOT / "plugins" / "linear-agent"
SPEC = importlib.util.spec_from_file_location(
    "hermes_plugins.linear_agent_runtime_test",
    PLUGIN_DIR / "__init__.py",
    submodule_search_locations=[str(PLUGIN_DIR)],
)
assert SPEC is not None and SPEC.loader is not None
PACKAGE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PACKAGE
SPEC.loader.exec_module(PACKAGE)
from hermes_plugins.linear_agent_runtime_test.linear_agent import LinearWorker
from hermes_plugins.linear_agent_runtime_test.linear_budgets import (
    BudgetAdmission,
    IssueBudget,
    IssueBudgetLedger,
)
from hermes_plugins.linear_agent_runtime_test.linear_guard_health import (
    WorkerGuardHealth,
)
from hermes_plugins.linear_agent_runtime_test.linear_handoff import (
    HandoffDenied,
    SharedIssueAuthority,
)
from hermes_plugins.linear_agent_runtime_test.linear_runtime import (
    LINEAR_WORKTREE,
    ProfileLinearRuntime,
)

from scripts.linear_ingress import IngressStore, Route

ALLOWED_USER_ID = "11111111-1111-4111-8111-111111111111"
DENIED_USER_ID = "22222222-2222-4222-8222-222222222222"


@unittest.skipUnless(
    bool(CORE_SOURCE),
    "pinned-core integration requires HERMES_CORE_SOURCE",
)
class ActualCoreLifecycleIntegrationTests(unittest.TestCase):
    """Marked integration suite: receipts from the pinned core, never hand-written v2 dicts."""

    def _runner(self):
        mixin = _load_core_lifecycle()

        class State:
            def __init__(self):
                self.persistent = types.SimpleNamespace(run_generation=1)
                self.turn = types.SimpleNamespace(agent=None)

        class Base:
            def _session_state(self, key):
                return self.__dict__.setdefault("_states", {}).setdefault(key, State())
            def _peek_session_state(self, key):
                return self.__dict__.get("_states", {}).get(key)

        class Runner(mixin, Base):
            pass
        return Runner()

    def _event(self, execution_id):
        return types.SimpleNamespace(
            _internal_plugin_execution_id=execution_id,
            source=types.SimpleNamespace(),
        )

    def test_actual_core_normal_worker_and_unknown_effect_receipts(self) -> None:
        async def scenario():
            runner, session = self._runner(), "linear:w:s"
            runner._session_state(session)
            # Normal no-tool completion releases only after its real worker Event.
            runner._register_internal_plugin_execution(self._event("normal"), session)
            runner._session_state(session).persistent.run_generation = 2
            done = threading.Event()
            runner._track_internal_plugin_execution_worker("normal", done)
            before = await runner.get_execution_lifecycle(session_key=session, execution_id="normal")
            self.assertFalse(ProfileLinearRuntime._safe_terminal_receipt(before, session, "normal"))
            done.set()
            normal = await runner.get_execution_lifecycle(session_key=session, execution_id="normal")
            self.assertTrue(ProfileLinearRuntime._safe_terminal_receipt(normal, session, "normal"))

            # An observed tool permanently leaves effects unresolved, even when the
            # registered worker finishes; consumer must retain durable quarantine.
            runner._register_internal_plugin_execution(self._event("tool-stop"), session)
            runner._session_state(session).persistent.run_generation = 3
            tool_done = threading.Event()
            runner._track_internal_plugin_execution_worker("tool-stop", tool_done)
            runner._observe_internal_plugin_tool_event("tool-stop", "tool.started")
            tool_done.set()
            unknown = await runner.get_execution_lifecycle(session_key=session, execution_id="tool-stop")
            self.assertFalse(ProfileLinearRuntime._safe_terminal_receipt(unknown, session, "tool-stop"))
            self.assertEqual(unknown["tools"], "unknown")
        asyncio.run(scenario())

    def test_actual_core_stale_identity_cannot_release_next_execution(self) -> None:
        async def scenario():
            runner, session = self._runner(), "linear:w:s"
            runner._session_state(session)
            runner._register_internal_plugin_execution(self._event("old"), session)
            runner._session_state(session).persistent.run_generation = 2
            runner._complete_internal_plugin_execution("old", wrapper_completed=True)
            runner._register_internal_plugin_execution(self._event("next"), session)
            runner._session_state(session).persistent.run_generation = 3
            stale = await runner.get_execution_lifecycle(session_key=session, execution_id="old")
            live = await runner.get_execution_lifecycle(session_key=session, execution_id="next")
            self.assertTrue(ProfileLinearRuntime._safe_terminal_receipt(stale, session, "old"))
            self.assertFalse(ProfileLinearRuntime._safe_terminal_receipt(live, session, "next"))
        asyncio.run(scenario())

    def test_unknown_core_receipt_remains_durably_occupied_after_runtime_restart(self) -> None:
        """SQLite retains the exact v2 unknown receipt; a fresh runtime cannot replace it."""
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                session = "linear:demo-space:session-1"
                runner = self._runner()
                runner._session_state(session)
                runner._register_internal_plugin_execution(self._event("unknown-tool"), session)
                runner._session_state(session).persistent.run_generation = 2
                done = threading.Event()
                runner._track_internal_plugin_execution_worker("unknown-tool", done)
                runner._observe_internal_plugin_tool_event("unknown-tool", "tool.started")
                done.set()
                receipt = await runner.get_execution_lifecycle(
                    session_key=session, execution_id="unknown-tool"
                )
                self.assertFalse(ProfileLinearRuntime._safe_terminal_receipt(
                    receipt, session, "unknown-tool"
                ))

                worker = LinearWorker(
                    root / "worker.db", profile="alpha", workspace="demo-space"
                )
                worker.add_delivery("work", json.dumps({
                    "type": "AgentSessionEvent", "action": "created",
                    "agentSession": {"id": "session-1", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode())
                self.assertTrue(worker.admit_once()[0])
                job = worker.next_unprepared()
                self.assertIsNotNone(job)
                worker.mark_prepared(job)
                job = worker.claim_prepared()
                self.assertIsNotNone(job)
                worker.record_lifecycle_receipt(
                    job, "unknown-tool", receipt, released=False
                )
                worker.quarantine_interrupted(job)

                restarted = LinearWorker(
                    root / "worker.db", profile="alpha", workspace="demo-space"
                )
                self.assertEqual(restarted.execution_occupancy("session-1"), "occupied")
                self.assertEqual(restarted.delivery_state("work"), "ambiguous")
                self.assertEqual(
                    restarted.ownership.claim("issue-1", "replacement").status,
                    "worker_active",
                )
        asyncio.run(scenario())

    def test_actual_core_stop_fences_prepare_and_live_worker_without_wrapper_proof(self) -> None:
        async def scenario():
            # request_stop imports gateway.run at call-time. Provide the minimal
            # interrupt bridge so this integration remains against the imported
            # core mixin rather than a fabricated receipt.
            package, run = types.ModuleType("gateway"), types.ModuleType("gateway.run")
            sentinel, interrupted = object(), []
            run._AGENT_PENDING_SENTINEL = sentinel
            run.request_hard_interrupt = lambda agent, reason: (interrupted.append((agent, reason)), True)[1]
            old_package, old_run = sys.modules.get("gateway"), sys.modules.get("gateway.run")
            sys.modules["gateway"], sys.modules["gateway.run"] = package, run
            try:
                runner, session = self._runner(), "linear:w:s"
                runner._session_state(session)
                runner._register_internal_plugin_execution(self._event("prepare-stop"), session)
                runner._session_state(session).persistent.run_generation = 2
                accepted = await runner.request_stop(session_key=session, expected_execution_id="prepare-stop")
                self.assertEqual(accepted["status"], "accepted")
                self.assertFalse(runner._promote_running_agent(session_key=session, run_generation=2, agent=object(), internal_plugin_execution_id="prepare-stop"))

                runner._complete_internal_plugin_execution("prepare-stop", wrapper_completed=True)
                runner._register_internal_plugin_execution(self._event("live-stop"), session)
                runner._session_state(session).persistent.run_generation = 3
                agent, done = object(), threading.Event()
                self.assertTrue(runner._promote_running_agent(session_key=session, run_generation=3, agent=agent, internal_plugin_execution_id="live-stop"))
                runner._track_internal_plugin_execution_worker("live-stop", done)
                self.assertEqual((await runner.request_stop(session_key=session, expected_execution_id="live-stop"))["status"], "accepted")
                self.assertEqual(interrupted, [(agent, "Internal plugin stop requested")])
                # A cancelled wrapper still has no authority over this live thread.
                live = await runner.get_execution_lifecycle(session_key=session, execution_id="live-stop")
                self.assertFalse(ProfileLinearRuntime._safe_terminal_receipt(live, session, "live-stop"))
                done.set()
                finished = await runner.get_execution_lifecycle(session_key=session, execution_id="live-stop")
                self.assertTrue(ProfileLinearRuntime._safe_terminal_receipt(finished, session, "live-stop"))
            finally:
                if old_package is None: sys.modules.pop("gateway", None)
                else: sys.modules["gateway"] = old_package
                if old_run is None: sys.modules.pop("gateway.run", None)
                else: sys.modules["gateway.run"] = old_run
        asyncio.run(scenario())


class LinearRuntimeTests(unittest.TestCase):
    def test_lifecycle_receipt_requires_exact_identity_and_all_lifetimes_terminal(self) -> None:
        receipt = {
            "lifecycle_version": "execution-lifecycle/v2", "session_key": "linear:w:s",
            "execution_id": "exact", "generation": 7, "state": "completed",
            "occupancy": "released", "tools": "none", "children": "none",
            "processes": "none", "remote": "none",
        }
        self.assertTrue(ProfileLinearRuntime._safe_terminal_receipt(receipt, "linear:w:s", "exact"))
        for key, value in (("execution_id", "stale"), ("tools", "unknown"), ("occupancy", "occupied"), ("generation", "7")):
            candidate = dict(receipt); candidate[key] = value
            self.assertFalse(ProfileLinearRuntime._safe_terminal_receipt(candidate, "linear:w:s", "exact"), key)

    def test_execute_receives_bound_issue_worktree(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "session-1", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode())
                seen: list[object] = []
                seen_cwd: list[object] = []
                trees = root / "trees"

                async def execute(_session_key: str, _prompt: str, _execution_id: str, job):
                    seen.append(job)
                    seen_cwd.append(LINEAR_WORKTREE.get())
                    return "ok"

                runtime = ProfileLinearRuntime(
                    LinearWorker(
                        root / "worker.db",
                        profile="alpha",
                        workspace="demo-space",
                        shared_authority_database=root / "shared.db",
                        worktree_root=trees,
                    ),
                    inbox / "ingress.db",
                    execute,
                    lambda *_: None,
                )
                self.assertTrue(await runtime.run_once())
                for _ in range(20):
                    if seen:
                        break
                    await runtime.run_once()
                    await asyncio.sleep(0)
                self.assertEqual(len(seen), 1)
                expected = trees / "demo-space" / "issue-1"
                self.assertEqual(getattr(seen[0], "execution_worktree"), expected)
                self.assertTrue(expected.is_dir())
                self.assertEqual(seen_cwd, [expected])
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_live_thought_does_not_emit_after_transfer(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shared = root / "shared.db"
            worker = LinearWorker(
                root / "worker.db",
                profile="coordinator",
                workspace="demo-space",
                shared_authority_database=shared,
            )
            worker.add_delivery(
                "d1",
                json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "old-session", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode(),
            )
            job = worker.admit_once()[1]
            self.assertIsNotNone(job)
            lease = SharedIssueAuthority(shared).get(workspace="demo-space", issue_id="issue-1")
            assert lease is not None
            SharedIssueAuthority(shared).transfer(
                workspace="demo-space",
                issue_id="issue-1",
                from_owner="coordinator:old-session",
                to_owner="operator:new-session",
                generation=lease.generation,
                stop_receipt={"session_key": "session-1", "execution_id": "exec-1", "status": "accepted"},
                session_key="session-1",
                execution_id="exec-1",
            )
            emitted: list[tuple[str, str, str]] = []

            async def execute(*_args):
                return "ok"

            runtime = ProfileLinearRuntime(
                worker,
                root / "ingress.db",
                execute,
                lambda *args: emitted.append(args),
                heartbeat_seconds=0,
            )
            runtime._active_job = job
            runtime._emit_thought("old-session", "post-transfer thought")
            self.assertEqual(emitted, [])

    def test_live_thought_without_issue_does_not_emit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shared = root / "shared.db"
            worker = LinearWorker(
                root / "worker.db",
                profile="coordinator",
                workspace="demo-space",
                shared_authority_database=shared,
            )
            from hermes_plugins.linear_agent_runtime_test.linear_agent import QueuedLinearJob

            job = QueuedLinearJob(
                delivery_id="d1",
                linear_session_id="old-session",
                hermes_session_key="session-1",
                prompt="work",
                issue_id=None,
            )
            emitted: list[tuple[str, str, str]] = []

            async def execute(*_args):
                return "ok"

            runtime = ProfileLinearRuntime(
                worker,
                root / "ingress.db",
                execute,
                lambda *args: emitted.append(args),
                heartbeat_seconds=0,
            )
            runtime._active_job = job
            runtime._emit_thought("old-session", "issue-less thought")
            self.assertEqual(emitted, [])

    def test_live_thought_without_active_job_does_not_emit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shared = root / "shared.db"
            worker = LinearWorker(
                root / "worker.db",
                profile="coordinator",
                workspace="demo-space",
                shared_authority_database=shared,
            )
            emitted: list[tuple[str, str, str]] = []

            async def execute(*_args):
                return "ok"

            runtime = ProfileLinearRuntime(
                worker,
                root / "ingress.db",
                execute,
                lambda *args: emitted.append(args),
                heartbeat_seconds=0,
            )
            runtime._emit_thought("old-session", "orphaned thought")
            self.assertEqual(emitted, [])

    def test_queued_thought_after_job_cleared_does_not_emit_unfenced(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                shared = root / "shared.db"
                worker = LinearWorker(
                    root / "worker.db",
                    profile="coordinator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )
                worker.add_delivery(
                    "d1",
                    json.dumps({
                        "type": "AgentSessionEvent",
                        "action": "created",
                        "agentSession": {"id": "old-session", "issue": {"id": "issue-1"}},
                        "promptContext": "work",
                    }).encode(),
                )
                job = worker.admit_once()[1]
                self.assertIsNotNone(job)
                lease = SharedIssueAuthority(shared).get(workspace="demo-space", issue_id="issue-1")
                assert lease is not None
                emitted: list[tuple[str, str, str]] = []
                gate = threading.Event()

                def emit(session_id: str, kind: str, body: str) -> None:
                    gate.wait(timeout=2)
                    emitted.append((session_id, kind, body))

                async def execute(*_args):
                    return "ok"

                runtime = ProfileLinearRuntime(
                    worker,
                    root / "ingress.db",
                    execute,
                    emit,
                    heartbeat_seconds=0,
                )
                runtime._active_job = job
                runtime._queue_thought("old-session", "delayed thought")
                runtime._active_job = None
                SharedIssueAuthority(shared).transfer(
                    workspace="demo-space",
                    issue_id="issue-1",
                    from_owner="coordinator:old-session",
                    to_owner="operator:new-session",
                    generation=lease.generation,
                    stop_receipt={
                        "session_key": "session-1",
                        "execution_id": "exec-1",
                        "status": "accepted",
                    },
                    session_key="session-1",
                    execution_id="exec-1",
                )
                gate.set()
                future = runtime._thought_future
                self.assertIsNotNone(future)
                await asyncio.wrap_future(future)
                self.assertEqual(emitted, [])

        asyncio.run(scenario())

    def test_live_thought_holds_ticket_across_emit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shared = root / "shared.db"
            worker = LinearWorker(
                root / "worker.db",
                profile="coordinator",
                workspace="demo-space",
                shared_authority_database=shared,
            )
            worker.add_delivery(
                "d1",
                json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "old-session", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode(),
            )
            job = worker.admit_once()[1]
            self.assertIsNotNone(job)
            lease = SharedIssueAuthority(shared).get(workspace="demo-space", issue_id="issue-1")
            assert lease is not None
            emitted: list[tuple[str, str, str]] = []

            def emit(session_id: str, kind: str, body: str) -> None:
                with self.assertRaises(HandoffDenied):
                    SharedIssueAuthority(shared).transfer(
                        workspace="demo-space",
                        issue_id="issue-1",
                        from_owner="coordinator:old-session",
                        to_owner="operator:new-session",
                        generation=lease.generation,
                        stop_receipt={
                            "session_key": "session-1",
                            "execution_id": "exec-1",
                            "status": "accepted",
                        },
                        session_key="session-1",
                        execution_id="exec-1",
                    )
                emitted.append((session_id, kind, body))

            async def execute(*_args):
                return "ok"

            runtime = ProfileLinearRuntime(
                worker,
                root / "ingress.db",
                execute,
                emit,
                heartbeat_seconds=0,
            )
            runtime._active_job = job
            runtime._emit_thought("old-session", "post-transfer thought")
            self.assertEqual(emitted, [("old-session", "thought", "post-transfer thought")])
            current = SharedIssueAuthority(shared).get(workspace="demo-space", issue_id="issue-1")
            assert current is not None
            self.assertEqual(current.owner_id, "coordinator:old-session")
            self.assertEqual(current.generation, lease.generation)

    def test_waiting_successor_runs_after_owner_terminal_receipt(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                shared = root / "shared.db"
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "coordinator",
                    "coordinator",
                    "/webhook/coordinator/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "d1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "linear-coordinator", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode())
                owner = LinearWorker(
                    root / "coordinator.db",
                    profile="coordinator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )
                successor = LinearWorker(
                    root / "operator.db",
                    profile="operator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )
                started = asyncio.Event()
                finish = asyncio.Event()

                async def execute(*_args):
                    started.set()
                    await finish.wait()
                    return "ok"

                async def lifecycle(session_key: str, execution_id: str):
                    return {
                        "lifecycle_version": "execution-lifecycle/v2",
                        "session_key": session_key,
                        "execution_id": execution_id,
                        "generation": 1,
                        "state": "completed",
                        "occupancy": "released",
                        "tools": "none",
                        "children": "none",
                        "processes": "none",
                        "remote": "none",
                    }

                runtime = ProfileLinearRuntime(
                    owner,
                    inbox / "ingress.db",
                    execute,
                    lambda *_: None,
                    lifecycle=lifecycle,
                    heartbeat_seconds=0,
                )
                for _ in range(40):
                    await runtime.run_once()
                    if started.is_set():
                        break
                    await asyncio.sleep(0)
                self.assertTrue(started.is_set())
                successor.add_delivery(
                    "d2",
                    json.dumps({
                        "type": "AgentSessionEvent",
                        "action": "created",
                        "agentSession": {"id": "linear-operator", "issue": {"id": "issue-1"}},
                        "promptContext": "take over",
                    }).encode(),
                )
                admitted, taken = successor.admit_once()
                self.assertTrue(admitted)
                self.assertIsNone(taken)
                self.assertEqual(successor.delivery_state("d2"), "waiting_handoff")
                finish.set()
                for _ in range(40):
                    await runtime.run_once()
                    await asyncio.sleep(0)
                    lease = SharedIssueAuthority(shared).get(
                        workspace="demo-space", issue_id="issue-1"
                    )
                    if lease is not None and lease.owner_id == "operator:linear-operator":
                        break
                admitted, taken = successor.admit_once()
                self.assertTrue(admitted)
                self.assertIsNotNone(taken)
                self.assertNotEqual(successor.delivery_state("d2"), "waiting_handoff")

        asyncio.run(scenario())

    def test_terminal_response_is_emitted_before_lease_release(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                shared = root / "shared.db"
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "coordinator",
                    "coordinator",
                    "/webhook/coordinator/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "d1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "linear-coordinator", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode())
                owner = LinearWorker(
                    root / "coordinator.db",
                    profile="coordinator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )
                emitted: list[tuple[str, str, str]] = []

                async def execute(*_args):
                    return "done"

                async def lifecycle(session_key: str, execution_id: str):
                    return {
                        "lifecycle_version": "execution-lifecycle/v2",
                        "session_key": session_key,
                        "execution_id": execution_id,
                        "generation": 1,
                        "state": "completed",
                        "occupancy": "released",
                        "tools": "none",
                        "children": "none",
                        "processes": "none",
                        "remote": "none",
                    }

                runtime = ProfileLinearRuntime(
                    owner,
                    inbox / "ingress.db",
                    execute,
                    lambda *args: emitted.append(args),
                    lifecycle=lifecycle,
                    heartbeat_seconds=0,
                )
                for _ in range(40):
                    await runtime.run_once()
                    if any(kind == "response" for _session, kind, _body in emitted):
                        break
                    await asyncio.sleep(0)
                self.assertIn("response", [kind for _session, kind, _body in emitted])
                self.assertNotIn("dead_letter", owner.outbox_state())

        asyncio.run(scenario())

    def test_inflight_thought_ticket_does_not_abandon_terminal_handoff(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                shared = root / "shared.db"
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "coordinator",
                    "coordinator",
                    "/webhook/coordinator/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "d1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "linear-coordinator", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode())
                owner = LinearWorker(
                    root / "coordinator.db",
                    profile="coordinator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )
                successor = LinearWorker(
                    root / "operator.db",
                    profile="operator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )
                thought_started = threading.Event()
                release_thought = threading.Event()
                finish_execute = asyncio.Event()

                def emit(session_id: str, kind: str, body: str) -> None:
                    if kind == "thought":
                        thought_started.set()
                        release_thought.wait(timeout=2)

                async def execute(*_args):
                    await asyncio.to_thread(thought_started.wait, 2)
                    await finish_execute.wait()
                    return "ok"

                async def lifecycle(session_key: str, execution_id: str):
                    return {
                        "lifecycle_version": "execution-lifecycle/v2",
                        "session_key": session_key,
                        "execution_id": execution_id,
                        "generation": 1,
                        "state": "completed",
                        "occupancy": "released",
                        "tools": "none",
                        "children": "none",
                        "processes": "none",
                        "remote": "none",
                    }

                runtime = ProfileLinearRuntime(
                    owner,
                    inbox / "ingress.db",
                    execute,
                    emit,
                    lifecycle=lifecycle,
                    heartbeat_seconds=0,
                )
                started = False
                for _ in range(40):
                    await runtime.run_once()
                    if thought_started.is_set():
                        started = True
                        break
                    await asyncio.sleep(0)
                self.assertTrue(started)
                successor.add_delivery(
                    "d2",
                    json.dumps({
                        "type": "AgentSessionEvent",
                        "action": "created",
                        "agentSession": {"id": "linear-operator", "issue": {"id": "issue-1"}},
                        "promptContext": "take over",
                    }).encode(),
                )
                successor.admit_once()
                self.assertEqual(successor.delivery_state("d2"), "waiting_handoff")
                finish_execute.set()
                await asyncio.sleep(0.05)
                release_thought.set()
                for _ in range(40):
                    await runtime.run_once()
                    await asyncio.sleep(0)
                    lease = SharedIssueAuthority(shared).get(
                        workspace="demo-space", issue_id="issue-1"
                    )
                    if lease is not None and lease.owner_id == "operator:linear-operator":
                        break
                lease = SharedIssueAuthority(shared).get(
                    workspace="demo-space", issue_id="issue-1"
                )
                self.assertIsNotNone(lease)
                self.assertEqual(lease.owner_id, "operator:linear-operator")
                admitted, taken = successor.admit_once()
                self.assertTrue(admitted)
                self.assertIsNotNone(taken)

        asyncio.run(scenario())

    def test_failed_terminal_response_does_not_release_lease(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                shared = root / "shared.db"
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "coordinator",
                    "coordinator",
                    "/webhook/coordinator/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "d1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "linear-coordinator", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode())
                owner = LinearWorker(
                    root / "coordinator.db",
                    profile="coordinator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )

                def emit(session_id: str, kind: str, body: str) -> None:
                    if kind == "response":
                        raise RuntimeError("linear response publish failed")

                async def execute(*_args):
                    return "done"

                async def lifecycle(session_key: str, execution_id: str):
                    return {
                        "lifecycle_version": "execution-lifecycle/v2",
                        "session_key": session_key,
                        "execution_id": execution_id,
                        "generation": 1,
                        "state": "completed",
                        "occupancy": "released",
                        "tools": "none",
                        "children": "none",
                        "processes": "none",
                        "remote": "none",
                    }

                runtime = ProfileLinearRuntime(
                    owner,
                    inbox / "ingress.db",
                    execute,
                    emit,
                    lifecycle=lifecycle,
                    heartbeat_seconds=0,
                )
                for _ in range(40):
                    await runtime.run_once()
                    await asyncio.sleep(0)
                lease = SharedIssueAuthority(shared).get(
                    workspace="demo-space", issue_id="issue-1"
                )
                self.assertIsNotNone(lease)
                self.assertEqual(lease.mode, "active")
                self.assertEqual(lease.owner_id, "coordinator:linear-coordinator")
                self.assertIn("ambiguous", owner.outbox_state())

        asyncio.run(scenario())

    def test_terminal_closeout_survives_runtime_restart(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                shared = root / "shared.db"
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "coordinator",
                    "coordinator",
                    "/webhook/coordinator/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "d1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "linear-coordinator", "issue": {"id": "issue-1"}},
                    "promptContext": "work",
                }).encode())
                owner = LinearWorker(
                    root / "coordinator.db",
                    profile="coordinator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )
                successor = LinearWorker(
                    root / "operator.db",
                    profile="operator",
                    workspace="demo-space",
                    shared_authority_database=shared,
                )
                started = asyncio.Event()
                finish = asyncio.Event()
                emitted: list[tuple[str, str, str]] = []

                async def execute(*_args):
                    started.set()
                    await finish.wait()
                    return "done"

                async def lifecycle(session_key: str, execution_id: str):
                    return {
                        "lifecycle_version": "execution-lifecycle/v2",
                        "session_key": session_key,
                        "execution_id": execution_id,
                        "generation": 1,
                        "state": "completed",
                        "occupancy": "released",
                        "tools": "none",
                        "children": "none",
                        "processes": "none",
                        "remote": "none",
                    }

                runtime = ProfileLinearRuntime(
                    owner,
                    inbox / "ingress.db",
                    execute,
                    lambda *args: emitted.append(args),
                    lifecycle=lifecycle,
                    heartbeat_seconds=0,
                )
                for _ in range(40):
                    await runtime.run_once()
                    if started.is_set():
                        break
                    await asyncio.sleep(0)
                self.assertTrue(started.is_set())
                successor.add_delivery(
                    "d2",
                    json.dumps({
                        "type": "AgentSessionEvent",
                        "action": "created",
                        "agentSession": {"id": "linear-operator", "issue": {"id": "issue-1"}},
                        "promptContext": "take over",
                    }).encode(),
                )
                successor.admit_once()
                self.assertEqual(successor.delivery_state("d2"), "waiting_handoff")
                finish.set()
                pending = None
                for _ in range(40):
                    pending = owner.pending_terminal_closeout()
                    if pending is not None:
                        break
                    await asyncio.sleep(0.05)
                self.assertIsNotNone(pending)
                lease = SharedIssueAuthority(shared).get(
                    workspace="demo-space", issue_id="issue-1"
                )
                self.assertIsNotNone(lease)
                self.assertEqual(lease.owner_id, "coordinator:linear-coordinator")
                recovered = ProfileLinearRuntime(
                    owner,
                    inbox / "ingress.db",
                    execute,
                    lambda *args: emitted.append(args),
                    lifecycle=lifecycle,
                    heartbeat_seconds=0,
                )
                for _ in range(40):
                    await recovered.run_once()
                    await asyncio.sleep(0)
                    lease = SharedIssueAuthority(shared).get(
                        workspace="demo-space", issue_id="issue-1"
                    )
                    if lease is not None and lease.owner_id == "operator:linear-operator":
                        break
                lease = SharedIssueAuthority(shared).get(
                    workspace="demo-space", issue_id="issue-1"
                )
                self.assertIsNotNone(lease)
                self.assertEqual(lease.owner_id, "operator:linear-operator")
                self.assertIn("response", [kind for _session, kind, _body in emitted])

        asyncio.run(scenario())

    def test_core_stop_capability_requires_exact_keyword_abis(self) -> None:
        class CurrentCore:
            async def dispatch_internal_plugin_event(self, event, *, execution_id=None):
                return None

            async def request_stop(self, *, session_key, expected_execution_id, reason="stop"):
                return {}

            async def get_execution_lifecycle(self, session_key, execution_id):
                return {}

        class OldCore:
            async def dispatch_internal_plugin_event(self, event):
                return None

        PACKAGE._require_gateway_stop_capability(CurrentCore())
        with self.assertRaisesRegex(RuntimeError, "execution_id"):
            PACKAGE._require_gateway_stop_capability(OldCore())

    def test_unauthorized_event_never_prepares_or_executes_session(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "creatorId": DENIED_USER_ID,
                    },
                    "promptContext": "must not execute",
                }).encode())
                prepared: list[str] = []
                executed: list[str] = []
                activities: list[tuple[str, str, str]] = []
                worker = LinearWorker(
                    root / "worker.db",
                    profile="alpha",
                    workspace="demo-space",
                    allowed_linear_user_ids=[ALLOWED_USER_ID],
                )

                async def prepare(session_key: str):
                    prepared.append(session_key)

                async def execute(_session_key: str, prompt: str):
                    executed.append(prompt)
                    return "must not execute"

                runtime = ProfileLinearRuntime(
                    worker,
                    inbox / "ingress.db",
                    execute,
                    lambda session_id, activity_type, body: activities.append(
                        (session_id, activity_type, body)
                    ),
                    prepare=prepare,
                )

                self.assertTrue(await runtime.run_once())

                self.assertEqual(prepared, [])
                self.assertEqual(executed, [])
                self.assertEqual(worker.delivery_state("delivery-1"), "rejected")
                self.assertEqual(activities, [(
                    "session-1",
                    "response",
                    "This agent is restricted to approved workspace users.",
                )])

        asyncio.run(scenario())

    def test_policy_migration_rejects_recovery_before_flush(self) -> None:
        async def scenario(state):
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                db, ingress = root / "worker.db", root / "ingress.db"
                IngressStore(ingress)
                payload = {
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "creatorId": DENIED_USER_ID,
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }
                old = LinearWorker(db, profile="alpha", workspace="demo-space")
                old.add_delivery("delivery-1", json.dumps(payload).encode())
                self.assertTrue(old.admit_once()[0])
                if state in ("prepared", "ambiguous"):
                    job = old.next_unprepared()
                    self.assertIsNotNone(job)
                    old.mark_prepared(job)
                if state == "ambiguous":
                    claimed = old.claim_prepared()
                    self.assertIsNotNone(claimed)
                prepared, executed, activities = [], [], []

                async def prepare(key):
                    prepared.append(key)

                async def execute(_key, prompt):
                    executed.append(prompt)
                    return "must not execute"

                restricted = LinearWorker(
                    db,
                    profile="alpha",
                    workspace="demo-space",
                    allowed_linear_user_ids=[ALLOWED_USER_ID],
                )
                runtime = ProfileLinearRuntime(
                    restricted,
                    ingress,
                    execute,
                    lambda *activity: activities.append(activity),
                    prepare=prepare,
                )
                self.assertTrue(await runtime.run_once())
                self.assertEqual((prepared, executed), ([], []))
                self.assertEqual(restricted.delivery_state("delivery-1"), "rejected")
                self.assertEqual(
                    activities,
                    [(
                        "session-1",
                        "response",
                        "This agent is restricted to approved workspace users.",
                    )],
                )
                outbox_states = restricted.outbox_state()
                self.assertEqual(outbox_states.count("sent"), 1)
                self.assertEqual(
                    set(outbox_states),
                    {"sent", "suppressed"},
                )

        for state in ("queued", "prepared", "ambiguous"):
            asyncio.run(scenario(state))

    def test_new_sessions_are_prepared_immediately_but_execute_fifo(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                for number in (1, 2):
                    ingress.enqueue(route, f"delivery-{number}", json.dumps({
                        "type": "AgentSessionEvent",
                        "action": "created",
                        "agentSession": {"id": f"session-{number}"},
                        "promptContext": f"work-{number}",
                    }).encode())

                first_started = asyncio.Event()
                release_first = asyncio.Event()
                prepared, executed, activities = [], [], []
                active = 0
                max_active = 0

                async def prepare(session_key):
                    prepared.append(session_key)

                async def execute(session_key, prompt):
                    nonlocal active, max_active
                    active += 1
                    max_active = max(max_active, active)
                    executed.append((session_key, prompt))
                    if prompt == "work-1":
                        first_started.set()
                        await release_first.wait()
                    active -= 1
                    return f"finished-{prompt}"

                runtime = ProfileLinearRuntime(
                    LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space"),
                    inbox / "ingress.db",
                    execute,
                    lambda session_id, activity_type, body: activities.append((session_id, activity_type, body)),
                    prepare=prepare,
                )

                self.assertTrue(await runtime.run_once())
                await asyncio.wait_for(first_started.wait(), timeout=1)
                self.assertTrue(await runtime.run_once())

                self.assertEqual(prepared, [
                    "linear:demo-space:session-1",
                    "linear:demo-space:session-2",
                ])
                self.assertIn(("linear:demo-space:session-1", "work-1"), executed)
                self.assertEqual(
                    activities[0],
                    ("session-1", "thought", "Inspecting the issue."),
                )
                self.assertIn(
                    ("session-1", "thought", "Reading source snapshots."),
                    activities,
                )

                release_first.set()
                for _ in range(20):
                    await runtime.run_once()
                    if (
                        len(executed) == 2
                        and ("session-1", "response", "finished-work-1") in activities
                        and ("session-2", "response", "finished-work-2") in activities
                    ):
                        break
                    await asyncio.sleep(0)

                self.assertEqual(executed, [
                    ("linear:demo-space:session-1", "work-1"),
                    ("linear:demo-space:session-2", "work-2"),
                ])
                self.assertEqual(max_active, 2)
                self.assertIn(("session-1", "response", "finished-work-1"), activities)
                self.assertIn(("session-2", "response", "finished-work-2"), activities)
                self.assertIn(("session-2", "thought", "Inspecting the issue."), activities)

        asyncio.run(scenario())

    def test_restart_executes_older_queued_job_before_new_delivery(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                worker = LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space")
                worker.add_delivery("older", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "session-older"},
                    "promptContext": "older",
                }).encode())
                admitted, old_job = worker.admit_once()
                self.assertTrue(admitted)
                self.assertIsNotNone(old_job)

                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                ingress.enqueue(route, "newer", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "session-newer"},
                    "promptContext": "newer",
                }).encode())
                prepared, executed = [], []

                async def prepare(session_key):
                    prepared.append(session_key)

                async def execute(_session_key, prompt):
                    executed.append(prompt)
                    return "done"

                runtime = ProfileLinearRuntime(
                    LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space"),
                    inbox / "ingress.db",
                    execute,
                    lambda *_args: None,
                    prepare=prepare,
                )
                self.assertTrue(await runtime.run_once())
                for _ in range(8):
                    await asyncio.sleep(0)
                    await runtime.run_once()
                    if executed:
                        break

                self.assertEqual(prepared[0], "linear:demo-space:session-older")
                self.assertEqual(executed[0], "older")

        asyncio.run(scenario())

    def test_restart_retries_session_preparation_before_execution(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                IngressStore(inbox / "ingress.db")
                worker = LinearWorker(
                    root / "worker.db",
                    profile="alpha",
                    workspace="demo-space",
                )
                worker.add_delivery("delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "session-1"},
                    "promptContext": "work",
                }).encode())
                admitted, job = worker.admit_once()
                self.assertTrue(admitted)
                self.assertIsNotNone(job)
                self.assertEqual(worker.delivery_state("delivery-1"), "queued")

                order = []

                async def prepare(_session_key):
                    order.append("prepare")

                async def execute(_session_key, _prompt):
                    order.append("execute")
                    return "done"

                runtime = ProfileLinearRuntime(
                    LinearWorker(
                        root / "worker.db",
                        profile="alpha",
                        workspace="demo-space",
                    ),
                    inbox / "ingress.db",
                    execute,
                    lambda *_args: None,
                    prepare=prepare,
                )
                for _ in range(10):
                    await runtime.run_once()
                    if order == ["prepare", "execute"]:
                        break
                    await asyncio.sleep(0)

                self.assertEqual(order, ["prepare", "execute"])
                for _ in range(10):
                    await runtime.run_once()
                    if worker.delivery_state("delivery-1") == "completed":
                        break
                    await asyncio.sleep(0)
                self.assertEqual(worker.delivery_state("delivery-1"), "completed")

        asyncio.run(scenario())

    def test_queued_acknowledgement_is_sent_before_session_preparation_finishes(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }).encode())
                activities: list[tuple[str, str, str]] = []
                prepare_started = asyncio.Event()
                release_prepare = asyncio.Event()

                async def prepare(_session_key):
                    prepare_started.set()
                    await release_prepare.wait()

                runtime = ProfileLinearRuntime(
                    LinearWorker(
                        root / "worker.db",
                        profile="alpha",
                        workspace="demo-space",
                    ),
                    inbox / "ingress.db",
                    lambda _key, _prompt: asyncio.sleep(0, result="Finished"),
                    lambda target, operation, body: activities.append(
                        (target, operation, body)
                    ),
                    prepare=prepare,
                )
                running = asyncio.create_task(runtime.run_once())
                # Assert acknowledgement ordering, not database/thread startup speed.
                await asyncio.wait_for(prepare_started.wait(), timeout=5)
                self.assertEqual(
                    activities[0],
                    ("session-1", "thought", "Inspecting the issue."),
                )
                release_prepare.set()
                await running
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_execution_does_not_start_when_active_status_is_not_confirmed(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }).encode())
                executions: list[str] = []

                async def execute(_key, _prompt):
                    executions.append("started")
                    return "Finished"

                def emit(_target, operation, body):
                    if operation == "issue_status" and body == "active":
                        raise TimeoutError("uncertain status update")

                runtime = ProfileLinearRuntime(
                    LinearWorker(
                        root / "worker.db",
                        profile="alpha",
                        workspace="demo-space",
                    ),
                    inbox / "ingress.db",
                    execute,
                    emit,
                    prepare=lambda _key: asyncio.sleep(0),
                )
                self.assertTrue(await runtime.run_once())
                await asyncio.sleep(0)
                self.assertEqual(executions, [])
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_unconfirmed_status_does_not_consume_issue_budget(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }).encode())
                executions: list[str] = []

                async def execute(_key, _prompt):
                    executions.append("started")
                    return "Finished"

                def emit(_target, operation, body):
                    if operation == "issue_status" and body == "active":
                        raise TimeoutError("uncertain status update")

                ledger = IssueBudgetLedger(
                    root / "worker.db",
                    IssueBudget(max_attempts=1, max_seconds=10, max_cost=10.0, cost_per_attempt=0.5),
                )
                runtime = ProfileLinearRuntime(
                    LinearWorker(
                        root / "worker.db",
                        profile="alpha",
                        workspace="demo-space",
                    ),
                    inbox / "ingress.db",
                    execute,
                    emit,
                    prepare=lambda _key: asyncio.sleep(0),
                    budget_ledger=ledger,
                )
                self.assertTrue(await runtime.run_once())
                await asyncio.sleep(0)
                self.assertEqual(executions, [])
                self.assertIsNone(ledger.snapshot("issue-1"))
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_unready_dependency_does_not_consume_issue_budget(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }).encode())
                executions: list[str] = []

                async def execute(_key, _prompt):
                    executions.append("started")
                    return "Finished"

                ledger = IssueBudgetLedger(
                    root / "worker.db",
                    IssueBudget(max_attempts=1, max_seconds=10, max_cost=10.0, cost_per_attempt=0.5),
                )
                runtime = ProfileLinearRuntime(
                    LinearWorker(
                        root / "worker.db",
                        profile="alpha",
                        workspace="demo-space",
                    ),
                    inbox / "ingress.db",
                    execute,
                    lambda *_args: True,
                    prepare=lambda _key: asyncio.sleep(0),
                    dependency_readiness=lambda _issue_id: False,
                    budget_ledger=ledger,
                )
                self.assertTrue(await runtime.run_once())
                await asyncio.sleep(0)
                self.assertEqual(executions, [])
                self.assertIsNone(ledger.snapshot("issue-1"))
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_prompted_followup_after_three_issue_turns_still_executes(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                executions: list[str] = []

                async def execute(_key, prompt):
                    executions.append(str(prompt))
                    return "Finished"

                ledger = IssueBudgetLedger(
                    root / "worker.db",
                    IssueBudget(max_attempts=3, max_seconds=86_400, max_cost=10.0, cost_per_attempt=1.0),
                )
                worker = LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space")
                runtime = ProfileLinearRuntime(
                    worker,
                    inbox / "ingress.db",
                    execute,
                    lambda *_args: True,
                    prepare=lambda _key: asyncio.sleep(0),
                    budget_ledger=ledger,
                )
                events = [
                    ("delivery-1", "created", None, "work-1"),
                    ("delivery-2", "prompted", "activity-2", "work-2"),
                    ("delivery-3", "prompted", "activity-3", "work-3"),
                    ("delivery-4", "prompted", "activity-4", "work-4"),
                ]
                for delivery_id, action, activity_id, prompt in events:
                    payload = {
                        "type": "AgentSessionEvent",
                        "action": action,
                        "agentSession": {"id": "session-1", "issue": {"id": "issue-1"}},
                    }
                    if action == "created":
                        payload["promptContext"] = prompt
                    else:
                        payload["agentActivity"] = {"id": activity_id, "body": prompt, "signal": "continue"}
                    ingress.enqueue(route, delivery_id, json.dumps(payload).encode())
                    for _ in range(40):
                        await runtime.run_once()
                        await asyncio.sleep(0)
                        if len(executions) >= events.index((delivery_id, action, activity_id, prompt)) + 1:
                            break
                    else:
                        self.fail(f"{delivery_id} did not execute")
                self.assertEqual(executions, ["work-1", "work-2", "work-3", "work-4"])
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_budget_refuse_fails_job_instead_of_prepared_spin(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "prompted",
                    "agentSession": {"id": "session-1", "issue": {"id": "issue-1"}},
                    "agentActivity": {"id": "activity-1", "body": "follow up", "signal": "continue"},
                }).encode())
                executions: list[str] = []
                activities: list[tuple[str, str, str]] = []

                async def execute(_key, _prompt):
                    executions.append("started")
                    return "Finished"

                class RefusingLedger:
                    def admit(self, issue_id, generation_id=None, **_kwargs):
                        del issue_id, generation_id
                        return BudgetAdmission(False, "issue_attempt_budget_exhausted")

                worker = LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space")
                runtime = ProfileLinearRuntime(
                    worker,
                    inbox / "ingress.db",
                    execute,
                    lambda target, operation, body: activities.append((str(target), str(operation), str(body))),
                    prepare=lambda _key: asyncio.sleep(0),
                    budget_ledger=RefusingLedger(),
                )
                for _ in range(8):
                    await runtime.run_once()
                    await asyncio.sleep(0)
                with sqlite3.connect(worker.database) as conn:
                    states = [str(row[0]) for row in conn.execute("SELECT state FROM deliveries")]
                active = [item for item in activities if item[1] == "issue_status" and item[2] == "active"]
                errors = [item for item in activities if item[1] == "error"]
                self.assertEqual(executions, [])
                self.assertEqual(states, ["completed"])
                self.assertLessEqual(len(active), 1)
                self.assertTrue(errors)
                self.assertIn("issue_attempt_budget_exhausted", errors[0][2])
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_terminal_issue_status_suppression_cancels_queued_execution(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }).encode())
                executions: list[str] = []

                async def execute(_key, _prompt):
                    executions.append("started")
                    return "Finished"

                def emit(_target, operation, _body):
                    return operation != "issue_status"

                worker = LinearWorker(
                    root / "worker.db",
                    profile="alpha",
                    workspace="demo-space",
                )
                runtime = ProfileLinearRuntime(
                    worker,
                    inbox / "ingress.db",
                    execute,
                    emit,
                    prepare=lambda _key: asyncio.sleep(0),
                )
                self.assertTrue(await runtime.run_once())
                await asyncio.sleep(0)
                self.assertEqual(executions, [])
                self.assertEqual(worker.delivery_state("delivery-1"), "canceled")
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_graceful_shutdown_preserves_uncertainty_without_terminal_output(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }).encode())
                execution_started = asyncio.Event()
                activities: list[tuple[str, str, str]] = []

                async def execute(_key, _prompt):
                    execution_started.set()
                    await asyncio.Event().wait()
                    return "unreachable"

                worker = LinearWorker(
                    root / "worker.db",
                    profile="alpha",
                    workspace="demo-space",
                )
                runtime = ProfileLinearRuntime(
                    worker,
                    inbox / "ingress.db",
                    execute,
                    lambda target, operation, body: activities.append(
                        (target, operation, body)
                    ),
                    prepare=lambda _key: asyncio.sleep(0),
                )
                await runtime.run_once()
                await asyncio.wait_for(execution_started.wait(), timeout=1)
                await runtime.shutdown()

                self.assertEqual(worker.delivery_state("delivery-1"), "ambiguous")
                self.assertNotIn(("issue-1", "issue_status", "failure"), activities)
                self.assertFalse(any(operation in {"error", "response", "issue_comment", "project_update"}
                                     for _, operation, _ in activities))

        asyncio.run(scenario())

    def test_created_delivery_emits_thought_runs_profile_session_and_emits_response(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1", "teamId": "team-1"},
                    },
                    "promptContext": "untrusted context",
                }).encode())
                calls, activities = [], []

                async def execute(session_key, prompt):
                    calls.append((session_key, prompt))
                    return "Finished"

                def emit(session_id, activity_type, body):
                    activities.append((session_id, activity_type, body))

                runtime = ProfileLinearRuntime(
                    LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space"),
                    inbox / "ingress.db", execute, emit,
                )
                self.assertTrue(await runtime.run_once())
                for _ in range(10):
                    if len(activities) >= 8:
                        break
                    await runtime.run_once()
                    await asyncio.sleep(0)
                self.assertEqual(calls, [("linear:demo-space:session-1", "untrusted context")])
                self.assertEqual(activities, [
                    ("session-1", "thought", "Inspecting the issue."),
                    ("issue-1", "issue_status", "active"),
                    ("session-1", "thought", "Reading source snapshots."),
                    ("session-1", "response", "Finished"),
                    (
                        "session-1",
                        "session_update",
                        json.dumps({"summary": "Finished"}, separators=(",", ":")),
                    ),
                    (
                        "issue-1",
                        "issue_comment",
                        "### Agent session summary\n\nFinished",
                    ),
                    (
                        "issue-1",
                        "project_update",
                        json.dumps({"session_key": "linear:demo-space:session-1:delivery-1:closeout", "summary": "### Agent session summary\n\nWork session finished. Detailed findings and remaining actions are recorded on the issue. Issue completion and project health are not inferred from session completion."}, separators=(",", ":")),
                    ),
                ])
                while await runtime.run_once():
                    await asyncio.sleep(0)

        asyncio.run(scenario())

    def test_unrelated_comment_webhook_does_not_block_agent_session(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                ingress.enqueue(route, "comment-delivery", json.dumps({
                    "type": "Comment",
                    "action": "create",
                    "data": {"id": "comment-1", "body": "@alpha hello"},
                }).encode())
                ingress.enqueue(route, "session-delivery", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "session-1"},
                    "promptContext": "hello",
                }).encode())
                calls, activities = [], []

                async def execute(session_key, prompt):
                    calls.append((session_key, prompt))
                    return "Hi"

                runtime = ProfileLinearRuntime(
                    LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space"),
                    inbox / "ingress.db", execute,
                    lambda session_id, activity_type, body: activities.append((session_id, activity_type, body)),
                )

                self.assertTrue(await runtime.run_once())
                self.assertEqual(calls, [])
                self.assertEqual(activities, [])
                for _ in range(40):
                    await runtime.run_once()
                    await asyncio.sleep(0)
                    if any(activity_type == "response" for _, activity_type, _ in activities):
                        break
                self.assertEqual(calls, [("linear:demo-space:session-1", "hello")])
                self.assertEqual(activities, [
                    ("session-1", "thought", "Inspecting the issue."),
                    ("session-1", "thought", "Reading source snapshots."),
                    ("session-1", "response", "Hi"),
                    ("session-1", "session_update", json.dumps({"summary": "Hi"}, separators=(",", ":"))),
                ])
                while await runtime.run_once():
                    await asyncio.sleep(0)

        asyncio.run(scenario())

    def test_prompted_delivery_emits_thought_before_execution(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "prompted",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "agentActivity": {
                        "id": "activity-1",
                        "body": "follow up",
                    },
                }).encode())
                activities, execution_snapshots = [], []

                async def execute(_session_key, _prompt):
                    # Durable acknowledgement/status ordering precedes execution;
                    # best-effort live progress may finish asynchronously.
                    execution_snapshots.append([
                        activity for activity in activities
                        if activity[2] != "Reading source snapshots."
                    ])
                    return "Finished"

                worker = LinearWorker(
                    root / "worker.db",
                    profile="alpha",
                    workspace="demo-space",
                )
                runtime = ProfileLinearRuntime(
                    worker,
                    inbox / "ingress.db",
                    execute,
                    lambda session_id, activity_type, body: activities.append((session_id, activity_type, body)),
                    prepare=lambda _key: asyncio.sleep(0),
                )
                for _ in range(40):
                    await runtime.run_once()
                    await asyncio.sleep(0)
                    if any(operation == "issue_comment" for _, operation, _ in activities):
                        break

                self.assertEqual(execution_snapshots, [[
                    ("session-1", "thought", "Inspecting the issue."),
                    ("issue-1", "issue_status", "active"),
                ]])
                self.assertEqual(
                    [
                        body for _, operation, body in activities
                        if operation == "issue_comment"
                    ],
                    ["### Agent session summary\n\nFinished"],
                )

        asyncio.run(scenario())

    def test_session_preparation_failure_emits_ordered_redacted_error_without_execution(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }).encode())
                activities, executions = [], []

                async def prepare(_session_key):
                    raise RuntimeError("sensitive preparation failure")

                async def execute(*_args):
                    executions.append(True)
                    return "must not run"

                runtime = ProfileLinearRuntime(
                    LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space"),
                    inbox / "ingress.db",
                    execute,
                    lambda session_id, activity_type, body: activities.append((session_id, activity_type, body)),
                    prepare=prepare,
                )
                self.assertTrue(await runtime.run_once())

                self.assertEqual(executions, [])
                self.assertEqual(activities, [
                    ("session-1", "thought", "Inspecting the issue."),
                    (
                        "session-1",
                        "error",
                        "Unable to complete this request. Please retry.",
                    ),
                    (
                        "issue-1",
                        "issue_comment",
                        (
                            "### Agent session summary\n\n"
                            "Unable to complete this request. Please retry."
                        ),
                    ),
                    ("issue-1", "issue_status", "failure"),
                ])

        asyncio.run(scenario())

    def test_execution_failure_emits_redacted_error_and_does_not_crash_runtime(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "context",
                }).encode())
                activities = []

                async def execute(_key, _prompt):
                    raise RuntimeError("sensitive failure")

                runtime = ProfileLinearRuntime(
                    LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space"),
                    inbox / "ingress.db", execute,
                    lambda session_id, activity_type, body: activities.append((session_id, activity_type, body)),
                )
                terminal = ("issue-1", "issue_status", "failure")
                for _ in range(10):
                    await runtime.run_once()
                    if activities and activities[-1] == terminal:
                        break
                else:
                    self.fail("runtime did not emit a terminal redacted failure")
                self.assertEqual(activities, [
                    ("session-1", "thought", "Inspecting the issue."),
                    ("issue-1", "issue_status", "active"),
                    ("session-1", "thought", "Reading source snapshots."),
                    (
                        "session-1",
                        "error",
                        "Unable to complete this request. Please retry.",
                    ),
                    (
                        "issue-1",
                        "issue_comment",
                        (
                            "### Agent session summary\n\n"
                            "Unable to complete this request. Please retry."
                        ),
                    ),
                    ("issue-1", "issue_status", "failure"),
                ])

        asyncio.run(scenario())


    def test_outbound_activity_failure_is_quarantined_without_crashing_runtime(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route("alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
                ingress.enqueue(route, "delivery-1", json.dumps({"type": "AgentSessionEvent", "action": "created", "agentSession": {"id": "session-1"}, "promptContext": "work"}).encode())
                runtime = ProfileLinearRuntime(
                    LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space"),
                    inbox / "ingress.db", lambda _key, _prompt: asyncio.sleep(0, result="Finished"),
                    lambda *_: (_ for _ in ()).throw(RuntimeError("network unavailable")),
                )
                self.assertTrue(await runtime.run_once())
                self.assertFalse(await runtime.run_once())

        asyncio.run(scenario())

    def test_heartbeat_emits_thought_during_long_execute(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inbox = root / "inbox"
                ingress = IngressStore(inbox / "ingress.db")
                route = Route(
                    "alpha",
                    "alpha",
                    "/webhook/alpha/linear",
                    root / "unused.env",
                    inbox,
                )
                ingress.enqueue(route, "delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {"id": "session-1"},
                    "promptContext": "work",
                }).encode())
                activities: list[tuple[str, str, str]] = []

                async def execute(_key, _prompt):
                    await asyncio.sleep(0.2)
                    return "Finished"

                runtime = ProfileLinearRuntime(
                    LinearWorker(
                        root / "worker.db",
                        profile="alpha",
                        workspace="demo-space",
                    ),
                    inbox / "ingress.db",
                    execute,
                    lambda session_id, activity_type, body: activities.append(
                        (session_id, activity_type, body)
                    ),
                    heartbeat_seconds=0.05,
                )
                self.assertTrue(await runtime.run_once())
                for _ in range(40):
                    if any(
                        body == "Still inspecting." for _, _, body in activities
                    ) and any(
                        operation == "response" for _, operation, _ in activities
                    ):
                        break
                    await runtime.run_once()
                    await asyncio.sleep(0.02)
                self.assertIn(("session-1", "thought", "Still inspecting."), activities)
                self.assertIn(("session-1", "response", "Finished"), activities)

        asyncio.run(scenario())

    def test_runtime_publishes_and_invalidates_guard_heartbeat(self) -> None:
        async def scenario():
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                ingress = root / "ingress.db"
                IngressStore(ingress)
                worker = LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space")
                health = WorkerGuardHealth(root / "worker.db", profile="alpha", workspace="demo-space")
                runtime = ProfileLinearRuntime(worker, ingress, lambda *_: asyncio.sleep(0, result="ok"), lambda *_: None, guard_health=health)
                self.assertFalse(health.is_ready())
                await runtime.run_once()
                self.assertTrue(health.is_ready())
                await runtime.shutdown()
                self.assertFalse(health.is_ready())
        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
