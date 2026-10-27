"""Retained native physical sessions remain part of the stopped work's evidence."""
from functools import partial
import os
import unittest
from unittest.mock import patch

import test_linear_origin_native as base


class CleanupLineage(base.OriginScenarios):
    def compressed(self, parent, suffix):
        tip = parent + suffix
        self.authority.db.publish_compression_child(parent_session_id=parent, child_session_id=tip,
            source="local", messages=[{"role": "user", "content": "Finite native checkpoint"}],
            require_compression_lease=False)
        return tip

    def admit(self, target, request):
        from hermes_state_runtime import admit_session_input
        return admit_session_input(self.authority.db, epoch=self.authority.epoch,
            principal_id=self.conn.actor.subject, session_id=target, request_id=request,
            payload={"text": "Finite retained native instruction"})

    def retired_extra_root(self, target, name, *, child=True):
        import psutil
        from hermes_cli.process_identity import worker_birth
        from gateway.session_worker_child import reserve_child_execution
        from hermes_state_runtime import (register_worker_execution, adopt_worker_execution,
            apply_worker_receipt, mutate_worker_execution)
        generation = self.authority.db._read_all("SELECT runtime_generation FROM sessions WHERE id=?", (target,))[0]["runtime_generation"]
        owned = dict(epoch=self.authority.epoch, execution_id=name, session_id=target, generation=generation)
        register_worker_execution(self.authority.db, **owned, kind="compute", adoption_secret="finite-extra-root")
        adopt_worker_execution(self.authority.db, **owned, adoption_secret="finite-extra-root")
        reserved = None
        if child:
            identity = {"profile_id": str(self.home), "pid": os.getpid(), "birth": worker_birth(psutil.Process())}
            reserve = partial(reserve_child_execution, epoch=self.authority.epoch, execution_id=name,
                identity=identity, principal=self.conn.actor.subject, limits=(1, 1), refused=None)
            reserved = apply_worker_receipt(self.authority.db, **owned, sequence=1,
                operation="child.reserve", payload={"secret": "finite-extra-child-secret"},
                handlers={"child.reserve": reserve})["child"]
        mutate_worker_execution(self.authority.db, **owned, sequence=2 if child else 1,
                                operation="execution.finish", payload={})
        return reserved

    async def test_lineage_admissions_remain_owned_through_native_settlement(self):
        from gateway.session_settlement_recovery import _mark_unknown
        from hermes_state_runtime import claim_session_input, resolve_unknown_session_input
        from hermes_state_runtime_workers import runtime_lineage
        await self.stopped_issue()
        tip = self.compressed(self.compressed(self.sid, "-compressed"), "-next")
        with self.authority.db._read_ctx() as conn:
            self.assertIn(tip, runtime_lineage(conn, self.sid))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        row = self.admit(tip, "retained-physical-work")
        before = self.authority.db._read_all("SELECT * FROM session_admissions")
        held = await self.cleanup_inspected()
        self.assertFalse(held["quiescence_proven"])
        self.assertEqual(held["reason"], "native_session_has_unsettled_work")
        self.assertEqual(before, self.authority.db._read_all("SELECT * FROM session_admissions"))
        claimed = claim_session_input(self.authority.db, epoch=self.authority.epoch, session_id=tip)
        self.assertEqual(claimed["admission_id"], row["admission_id"])
        self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        self.assertEqual(_mark_unknown(self.authority, claimed), "unknown")
        self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        resolved = resolve_unknown_session_input(self.authority.db, epoch=self.authority.epoch,
            admission_id=row["admission_id"], generation=claimed["generation"])
        self.assertEqual((resolved["status"], resolved["outcome"]), ("terminal", "interrupted"))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        self.authority.db.create_session("foreign-physical-session", source="local")
        self.admit("foreign-physical-session", "unrelated-queued-work")
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])

    async def test_lineage_stop_marker_requires_native_clear(self):
        from gateway import session_ancillary
        from gateway.session_contract import SessionRef
        getter = getattr(session_ancillary, "process_stop_pending", None)
        await self.stopped_issue()
        tip = self.compressed(self.sid, "-compressed")
        if getter is not None:
            self.enterContext(patch.object(session_ancillary, "process_stop_pending", getter))
            ref = SessionRef(profile_id=str(self.home), session_id=tip)
            token = session_ancillary.record_process_stop(self.authority, ref, 0)
        else:
            # The release-pinned Core lacks this capability. Preserve the
            # existing synthetic adapter; candidate Core uses its real writers.
            self.authority.db.set_meta("fixture.pending.stop." + tip, "pending")
        evidence = await self.cleanup_inspected()
        self.assertFalse(evidence["quiescence_proven"])
        self.assertEqual(evidence["reason"], "native_stop_checkpoint_pending")
        if getter is not None:
            self.assertEqual(getter(self.authority.db, tip), token)
            session_ancillary.clear_process_stop(self.authority, ref, token)
        else:
            self.authority.db._execute_write(lambda conn: conn.execute(
                "DELETE FROM state_meta WHERE key=?", ("fixture.pending.stop." + tip,)))
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])

    async def test_lineage_arrivals_during_owner_read_are_rechecked(self):
        from hermes_state_runtime import cancel_session_input
        await self.stopped_issue()
        tip = self.compressed(self.sid, "-compressed")
        for kind in ("queued", "settled", "lineage"):
            with self.subTest(kind=kind):
                arrived = []
                def arriving(_method, _params):
                    self.before_rpc = None
                    if kind == "lineage":
                        self.compressed(tip, "-arrived")
                    else:
                        row = self.admit(tip, "arrived-" + kind)
                        arrived.append(row)
                        if kind == "settled":
                            cancel_session_input(self.authority.db, epoch=self.authority.epoch,
                                                 admission_id=row["admission_id"])
                self.before_rpc = arriving
                evidence = await self.cleanup_inspected()
                self.assertFalse(evidence["quiescence_proven"])
                self.assertEqual(evidence["reason"], "native_session_has_unsettled_work" if kind == "queued"
                                 else "cleanup_evidence_changed")
                for row in arrived:
                    cancel_session_input(self.authority.db, epoch=self.authority.epoch,
                                         admission_id=row["admission_id"])
                self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])

    async def test_lineage_of_actual_retired_descendant_keeps_its_admissions(self):
        import psutil
        from hermes_cli.process_identity import worker_birth
        from gateway.session_worker_child import reserve_child_execution
        from hermes_state_runtime import apply_worker_receipt, cancel_session_input
        import hermes_state_runtime as runtime
        original = runtime.mutate_worker_execution
        children = []
        def reserve_before_finish(db, **args):
            identity = {"profile_id": str(self.home), "pid": os.getpid(), "birth": worker_birth(psutil.Process())}
            reserve = partial(reserve_child_execution, epoch=args["epoch"], execution_id=args["execution_id"],
                identity=identity, principal=self.conn.actor.subject, limits=(1, 1), refused=None)
            child = apply_worker_receipt(db, **{k: args[k] for k in
                ("epoch", "execution_id", "session_id", "generation", "sequence")},
                operation="child.reserve", payload={"secret": "finite-native-child-secret"},
                handlers={"child.reserve": reserve})["child"]
            children.append(child)
            return original(db, **{**args, "sequence": args["sequence"] + 1})
        with patch.object(runtime, "mutate_worker_execution", reserve_before_finish):
            await self.stopped_issue()
        self.assertEqual(len(children), 1)
        child = children[0]
        saved = self.authority.db._read_all("SELECT status FROM worker_executions WHERE execution_id=?",
                                           (child["execution_id"],))
        self.assertEqual(saved[0]["status"], "terminal")
        tip = self.compressed(child["session_id"], "-compressed")
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        row = self.admit(tip, "retained-descendant-work")
        self.assertFalse((await self.cleanup_inspected())["quiescence_proven"])
        cancel_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=row["admission_id"])
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])

    async def test_lineage_all_retained_roots_and_descendant_compressions_are_closed(self):
        from hermes_state_runtime import cancel_session_input
        await self.stopped_issue()
        target = self.sid
        for index in range(3):
            # Independent compute roots deliberately do not share the primary
            # admission-worker prefix. The session binding establishes scope.
            target = self.compressed(target, "-continued")
            child = self.retired_extra_root(target, "independent-retained-root-" + str(index))
            target = child["session_id"]
            self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        target = self.compressed(target, "-last")
        row = self.admit(target, "deep-retained-native-work")
        before = self.authority.db._read_all("SELECT * FROM worker_executions")
        held = await self.cleanup_inspected()
        self.assertFalse(held["quiescence_proven"])
        self.assertEqual(held["reason"], "native_session_has_unsettled_work")
        self.assertEqual(before, self.authority.db._read_all("SELECT * FROM worker_executions"))
        cancel_session_input(self.authority.db, epoch=self.authority.epoch, admission_id=row["admission_id"])
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
        self.authority.db.set_meta("fixture.pending.stop." + target, "pending")
        self.assertEqual((await self.cleanup_inspected())["reason"], "native_stop_checkpoint_pending")
        self.authority.db._execute_write(lambda conn: conn.execute(
            "DELETE FROM state_meta WHERE key=?", ("fixture.pending.stop." + target,)))
        self.authority.db.create_session("foreign-extra-root-session", source="local")
        foreign = self.retired_extra_root("foreign-extra-root-session", "independent-retained-root-foreign")
        self.admit(foreign["session_id"], "foreign-descendant-queued-work")
        self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])

    async def test_lineage_extra_retained_roots_arriving_during_owner_read_change_digest(self):
        from hermes_state_runtime import cancel_session_input
        await self.stopped_issue()
        tip = self.compressed(self.sid, "-continued")
        for kind in ("terminal-root", "terminal-child", "settled-child", "queued-child"):
            with self.subTest(kind=kind):
                admitted = []
                def arriving(_method, _params):
                    self.before_rpc = None
                    child = self.retired_extra_root(tip, "arrived-extra-" + kind, child=kind != "terminal-root")
                    if kind in ("settled-child", "queued-child"):
                        row = self.admit(child["session_id"], "arrived-extra-work-" + kind)
                        admitted.append(row)
                        if kind == "settled-child":
                            cancel_session_input(self.authority.db, epoch=self.authority.epoch,
                                                 admission_id=row["admission_id"])
                self.before_rpc = arriving
                evidence = await self.cleanup_inspected()
                self.assertFalse(evidence["quiescence_proven"])
                self.assertEqual(evidence["reason"], "native_session_has_unsettled_work" if kind == "queued-child"
                                 else "cleanup_evidence_changed")
                for row in admitted:
                    cancel_session_input(self.authority.db, epoch=self.authority.epoch,
                                         admission_id=row["admission_id"])
                self.assertTrue((await self.cleanup_inspected())["quiescence_proven"])
def load_tests(loader, tests, pattern):
    return unittest.TestSuite(CleanupLineage(name) for name in loader.getTestCaseNames(CleanupLineage)
                              if name.startswith("test_lineage_"))
