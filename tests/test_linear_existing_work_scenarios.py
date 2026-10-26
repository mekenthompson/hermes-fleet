"""Native issue discovery before assignment admission; no client-UAT claim."""
import hashlib
import json
import os
from contextlib import closing
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_linear_native_bridge_scenarios as native


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class ExistingWorkScenarios(unittest.TestCase):
    def setUp(self):
        self.fixture = native.NativeBridgeScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.board, self.bridge, self.store = self.fixture.board, self.fixture.bridge, self.fixture.store
        self.issue = self.fixture.authority.issues[native.ISSUE]

    def existing(self, *, board=None, key=None, assignee="default", body=None, contract=None):
        adapter = self.board if board is None else native.Kanban(board, profile="default", profile_home=self.fixture.home)
        with adapter.conn() as conn:
            task_id = native.kb.create_task(conn, board=adapter.board, assignee=assignee,
                title=f"{self.issue['identifier']}: existing work", created_by="operator",
                body=body if body is not None else f"Linear issue {native.ISSUE}: {self.issue['url']}",
                idempotency_key=key or f"chat-work:{native.ISSUE}", completion_contract=contract)
        return adapter, task_id

    def assign(self):
        self.bridge.handle_webhook(self.fixture.event())

    def test_running_unlinked_chat_task_is_reused_and_followup_keeps_its_run(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            original = native.kb.claim_task(conn, task_id)
        self.assign()
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
        self.bridge.followup(native.ISSUE, "Current authorized steering", "existing-followup")
        current = adapter.get(task_id)
        self.assertEqual((current.current_run_id, current.claim_lock), (original.current_run_id, original.claim_lock))
        self.assertEqual(self.fixture.comments(task_id)[-1].body, "Current authorized steering")
        self.assign()
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_exact_native_key_reuses_task_without_requiring_identical_creation_prose(self):
        _, task_id = self.existing(key=f"linear:{native.ISSUE}",
            body=f"Linear issue {native.ISSUE}: {self.issue['url']}\nOriginal approved objective")
        self.assign()
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_key_only_collision_on_an_alternate_board_is_not_adopted_or_duplicated(self):
        adapter, task_id = self.existing(board="project-work", key=f"linear:{native.ISSUE}",
                                        body="Manual work with a colliding retry key")
        with self.assertRaises(native.LinearError):
            self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
        self.assertEqual(self.fixture.tasks(), [])
        self.assertEqual(adapter.get(task_id).status, "ready")

    def test_alternate_existing_board_remains_the_execution_and_followup_board(self):
        adapter, task_id = self.existing(board="project-work")
        self.assign()
        self.assertEqual(self.store.native_binding(native.ISSUE)["board"], "project-work")
        self.assertEqual(self.fixture.tasks(), [])
        self.bridge.followup(native.ISSUE, "Steer existing project work", "project-followup")
        with adapter.conn() as conn:
            self.assertEqual(len(native.kb.list_tasks(conn)), 1)
            self.assertEqual(native.kb.list_comments_after(conn, task_id)[-1].body, "Steer existing project work")

    def test_stopped_work_is_linked_without_activation_even_after_restart(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            native.kb.block_task(conn, task_id, reason="operator Stop", kind="needs_input")
        history = [(event.id, event.kind) for event in adapter.history(task_id, 0)]
        self.assign()
        self.fixture.make_bridge().recover()
        self.assertEqual(adapter.get(task_id).status, "blocked")
        self.assertEqual([(e.id, e.kind) for e in adapter.history(task_id, 0)], history)
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_new_stop_during_discovery_is_not_undone(self):
        adapter, task_id = self.existing()
        discover = self.board.discover
        def stop_after_inspection(issue):
            found = discover(issue)
            with adapter.conn() as conn:
                native.kb.block_task(conn, task_id, reason="new Stop", kind="needs_input")
            return found
        with patch.object(self.board, "discover", side_effect=stop_after_inspection):
            self.assign()
        self.assertEqual(adapter.get(task_id).status, "blocked")
        self.assertFalse(any(e.kind == "unblocked" for e in adapter.history(task_id, 0)))

    def test_foreign_executor_does_not_trigger_replacement_or_implicit_transfer(self):
        adapter, task_id = self.existing(assignee="different-local-profile")
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(len(self.fixture.tasks()), 1)
        self.assertEqual(adapter.get(task_id).assignee, "different-local-profile")

    def test_ambiguous_existing_work_is_not_selected_or_duplicated(self):
        self.existing()
        self.existing(board="other-project")
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))

    def test_incidental_issue_mentions_are_not_implicit_task_links(self):
        _, old_id = self.existing(body=f"Research notes refer to {self.issue['identifier']} and {self.issue['url']}")
        self.assign()
        self.assertEqual(len(self.fixture.tasks()), 2)
        self.assertNotEqual(self.store.get(native.ISSUE)["task_id"], old_id)

    def test_completed_existing_work_stays_history_without_new_execution(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            native.kb.complete_task(conn, task_id, result="Verified original approved outcome")
        self.assign()
        self.assertEqual(adapter.get(task_id).status, "done")
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_identifier_marker_is_accepted_without_a_machine_creation_key(self):
        _, task_id = self.existing(body=f"Linear issue {self.issue['identifier']}: {self.issue['url']}")
        self.assign()
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)

    def test_reassignment_during_discovery_prevents_binding_or_creation(self):
        self.existing()
        discover = self.board.discover
        def changed(issue):
            found = discover(issue)
            self.issue["delegate"] = {"id": "another-app"}
            return found
        with patch.object(self.board, "discover", side_effect=changed):
            self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_unreadable_existing_board_is_not_treated_as_empty(self):
        self.existing(board="project-work")
        original = self.board.for_board
        def denied(board):
            if board == "project-work":
                raise native.LinearError("Existing native target unavailable", retryable=False)
            return original(board)
        with patch.object(self.board, "for_board", side_effect=denied):
            self.assign()
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
        self.assertIsNone(self.store.get(native.ISSUE))

    def test_configured_project_acceptance_is_not_downgraded_by_reuse(self):
        adapter, task_id = self.existing()
        self.bridge.contracts[self.issue["project"]["id"]] = "example/repository"
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(adapter.get(task_id).status, "ready")
        self.assertEqual(len(self.fixture.tasks()), 1)

    def test_matching_project_acceptance_reuses_existing_task(self):
        _, task_id = self.existing(contract="example/repository")
        self.bridge.contracts[self.issue["project"]["id"]] = "example/repository"
        self.assign()
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        self.assertEqual(len(self.fixture.tasks()), 1)

    def archive_board(self, *, assignee="default", body=None):
        adapter, task_id = self.existing(board="project-work", assignee=assignee, body=body)
        with adapter.conn() as conn:
            native.kb.complete_task(conn, task_id, result="Original completed outcome")
        result = native.kb.remove_board("project-work", archive=True)
        return Path(result["new_path"]), task_id

    def archive_bytes(self, path):
        return {str(item.relative_to(path)): hashlib.sha256(item.read_bytes()).hexdigest()
                for item in path.rglob("*") if item.is_file()}

    def test_completed_archived_task_is_history_without_replacement_or_false_unfinished_report(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            native.kb.complete_task(conn, task_id, result="Original completed outcome")
            native.kb.archive_task(conn, task_id)
        before = [(event.id, event.kind) for event in adapter.history(task_id, 0)]
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
        self.assertEqual(adapter.get(task_id).status, "archived")
        self.assertEqual([(event.id, event.kind) for event in adapter.history(task_id, 0)], before)
        self.assertTrue(any("earlier completed outcome" in row["payload"]["content"]["body"]
                            for row in self.store.pending(native.ISSUE)))

    def test_archived_board_history_is_read_without_reopening_linking_or_file_changes(self):
        path, _ = self.archive_board()
        before = self.archive_bytes(path)
        connect = self.board.kc.connect
        def no_archive_connect(*args, **kwargs):
            self.assertNotEqual(kwargs.get("board"), "project-work")
            return connect(*args, **kwargs)
        with patch.object(self.board.kc, "connect", side_effect=no_archive_connect):
            self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
        self.assertEqual(self.fixture.tasks(), [])
        self.assertEqual(self.archive_bytes(path), before)
        self.assertTrue(native.kb.read_board_metadata("project-work")["archived"])
        self.assertFalse(Path(native.kb.kanban_db_path(board="project-work")).exists())

    def test_archive_snapshot_reads_committed_wal_without_mutating_archive_files(self):
        adapter, task_id = self.existing(board="project-work")
        database = Path(native.kb.kanban_db_path(board="project-work"))
        # Native runtime safety may deliberately choose DELETE on older SQLite.
        # Craft WAL only in this disposable single-writer fixture, with every
        # native connection closed; no runtime safety function is patched.
        with closing(sqlite3.connect(database, isolation_level=None)) as conn:
            conn.row_factory = sqlite3.Row
            self.assertEqual(conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0], "delete")
            self.assertEqual(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            conn.execute("PRAGMA wal_autocheckpoint=0")
            native.kb.complete_task(conn, task_id, result="Completed result committed in WAL")
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro&immutable=1", uri=True)) as main_only:
                main_only.row_factory = sqlite3.Row
                self.assertEqual(native.kb.get_task(main_only, task_id).status, "ready")
            path = Path(native.kb.remove_board("project-work", archive=True)["new_path"])
            self.assertGreater((path / "kanban.db-wal").stat().st_size, 0)
            before = self.archive_bytes(path)
            self.assign()
            self.assertIsNone(self.store.get(native.ISSUE))
            self.assertEqual(self.fixture.tasks(), [])
            self.assertEqual(self.archive_bytes(path), before)
            self.assertTrue(any("earlier completed outcome" in row["payload"]["content"]["body"]
                                for row in self.store.pending(native.ISSUE)))

    def test_archive_reader_enforces_query_only(self):
        path, _ = self.archive_board()
        before = self.archive_bytes(path)
        [(adapter, _)] = self.board.discover(self.issue)
        with adapter.conn() as conn:
            self.assertEqual(conn.execute("PRAGMA query_only").fetchone()[0], 1)
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("DELETE FROM tasks")
        self.assertEqual(self.archive_bytes(path), before)

    def test_archive_changing_during_snapshot_is_not_treated_as_absent_work(self):
        path, _ = self.archive_board()
        from hermes_fleet_linear_plugin import bridge as module
        copyfile = module.shutil.copyfile
        def changed(source, destination):
            result = copyfile(source, destination)
            if Path(source) == path / "kanban.db":
                stat = Path(source).stat()
                os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
            return result
        with patch.object(module.shutil, "copyfile", side_effect=changed):
            self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(self.fixture.tasks(), [])

    def test_unrelated_intact_archived_board_does_not_block_new_authorized_assignment(self):
        path, _ = self.archive_board(body="Linear issue OTHER-1: https://linear.example/issue/OTHER-1")
        before = self.archive_bytes(path)
        self.assign()
        self.assertEqual(len(self.fixture.tasks()), 1)
        self.assertIsNotNone(self.store.get(native.ISSUE))
        self.assertEqual(self.archive_bytes(path), before)

    def test_archive_metadata_wrong_slug_is_not_trusted_or_treated_as_empty(self):
        path, _ = self.archive_board()
        metadata = path / "board.json"
        raw = json.loads(metadata.read_text())
        raw["slug"] = "another-board"
        metadata.write_text(json.dumps(raw))
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(self.fixture.tasks(), [])

    def test_archive_database_symlink_is_not_followed(self):
        path, _ = self.archive_board()
        database = path / "kanban.db"
        saved = database.with_name("saved.db")
        database.rename(saved)
        database.symlink_to(saved)
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(self.fixture.tasks(), [])

    def test_foreign_executor_archive_is_read_as_history_without_implicit_transfer(self):
        path, _ = self.archive_board(assignee="different-local-profile")
        before = self.archive_bytes(path)
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(self.fixture.tasks(), [])
        self.assertEqual(self.archive_bytes(path), before)

    def test_unavailable_registered_database_holds_only_replacement_and_is_not_recreated(self):
        adapter, task_id = self.existing(board="project-work")
        with adapter.conn() as conn:
            original = native.kb.claim_task(conn, task_id)
        database = Path(native.kb.kanban_db_path(board="project-work"))
        saved = database.with_name("temporarily-unavailable.db")
        database.rename(saved)
        try:
            self.assign()
            self.assertFalse(database.exists())
            self.assertIsNone(self.store.get(native.ISSUE))
            self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
            self.assertEqual(self.bridge.native_status(native.ISSUE)["status"], "unlinked")
            self.assertEqual(self.fixture.tasks(), [])
            # Inspection failure does not install a global native execution gate.
            self.board.create(title="Unrelated approved work")
        finally:
            saved.rename(database)
        current = adapter.get(task_id)
        self.assertEqual((current.status, current.current_run_id, current.claim_lock),
                         (original.status, original.current_run_id, original.claim_lock))

    def test_unavailable_archived_database_is_not_treated_as_empty(self):
        path, _ = self.archive_board()
        (path / "kanban.db").rename(path / "temporarily-unavailable.db")
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(self.fixture.tasks(), [])

    def test_discovery_does_not_truncate_older_archived_linked_work(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            native.kb.complete_task(conn, task_id, result="Original completed outcome")
            native.kb.archive_task(conn, task_id)
            for index in range(205):
                native.kb.create_task(conn, title=f"Unrelated {index}", priority=10, assignee="default")
        self.assign()
        self.assertIsNone(self.store.get(native.ISSUE))
        self.assertEqual(len(self.fixture.tasks()), 206)

    def test_synthesized_absent_default_in_fresh_named_board_home_allows_assignment(self):
        home = self.fixture.home / "fresh-home"
        home.mkdir()
        with patch.dict(os.environ, {"HERMES_HOME": str(home), "HERMES_KANBAN_HOME": str(home), "HERMES_PROFILE": "default"}):
            board = native.Kanban("linear-inbox", profile="default", profile_home=home)
            store = native.Store(home / "linear.db")
            bridge = native.Bridge(store, self.fixture.authority.api(scope=None), board, profile="default")
            default = Path(native.kb.kanban_db_path(board="default"))
            self.assertFalse(default.exists())
            bridge.handle_webhook(self.fixture.event())
            self.assertIsNotNone(store.get(native.ISSUE))
            self.assertFalse(default.exists())

    def test_missing_previously_configured_default_is_not_fresh_setup(self):
        database = Path(native.kb.kanban_db_path(board="default"))
        saved = database.with_name("temporarily-unavailable.db")
        database.rename(saved)
        try:
            self.assign()
            self.assertFalse(database.exists())
            self.assertIsNone(self.store.get(native.ISSUE))
        finally:
            saved.rename(database)

    def test_plugin_restart_does_not_recreate_missing_default_and_issue_reads_still_work(self):
        adapter, task_id = self.existing()
        with adapter.conn() as conn:
            original = native.kb.claim_task(conn, task_id)
        database = Path(native.kb.kanban_db_path(board="default"))
        self.assertFalse(native.kb.board_metadata_path("default").exists())  # Metadata is synthesized.
        saved = database.with_name("missing-saved.db")
        database.rename(saved)
        try:
            restarted = native.Kanban(profile="default", profile_home=self.fixture.home)
            self.assertFalse(database.exists())
            bridge = native.Bridge(self.store, self.fixture.authority.api(scope=None), restarted, profile="default")
            bridge.profile_home = self.fixture.home
            from hermes_fleet_linear_plugin import chat
            # Isolated handler fixture; transport identity producer paths have
            # separate real-native frontdoor coverage, not a client-UAT claim.
            context = SimpleNamespace(profile="default", home=self.fixture.home, pinned_db_path="")
            result = json.loads(chat.handle(bridge, {"action": "status", "issue": native.ISSUE}, context))
            self.assertTrue(result["ok"])
            self.assertEqual(result["issue"]["id"], native.ISSUE)
            bridge.handle_webhook(self.fixture.event())
            self.assertFalse(database.exists())
            self.assertIsNone(self.store.get(native.ISSUE))
            self.assertIsNone(self.store.native_intent(f"create:{native.ISSUE}"))
        finally:
            saved.rename(database)
        # Restoring native data is sufficient; no plugin marker/reset is needed.
        bridge.handle_webhook(self.fixture.event())
        self.assertEqual(self.store.get(native.ISSUE)["task_id"], task_id)
        current = restarted.get(task_id)
        self.assertEqual((current.current_run_id, current.claim_lock), (original.current_run_id, original.claim_lock))

    def test_plugin_restart_does_not_recreate_missing_registered_named_database(self):
        adapter, task_id = self.existing(board="project-work")
        database = Path(native.kb.kanban_db_path(board="project-work"))
        saved = database.with_name("missing-saved.db")
        database.rename(saved)
        try:
            restarted = native.Kanban("project-work", profile="default", profile_home=self.fixture.home)
            self.assertFalse(database.exists())
            bridge = native.Bridge(self.store, self.fixture.authority.api(scope=None), restarted, profile="default")
            bridge.handle_webhook(self.fixture.event())
            self.assertFalse(database.exists())
            self.assertIsNone(self.store.get(native.ISSUE))
        finally:
            saved.rename(database)
        self.assertEqual(adapter.get(task_id).status, "ready")

    def test_missing_default_with_native_workspace_evidence_is_not_initialized_even_without_lock(self):
        adapter, task_id = self.existing()
        from hermes_cli.kanban_db_workspace import resolve_workspace
        workspace = Path(resolve_workspace(adapter.get(task_id), board="default"))
        self.assertTrue(workspace.is_dir())
        database = Path(native.kb.kanban_db_path(board="default"))
        saved = database.with_name("missing-saved.db")
        database.rename(saved)
        Path(str(database) + ".init.lock").unlink()
        try:
            native.Kanban(profile="default", profile_home=self.fixture.home)
            self.assertFalse(database.exists())
            self.assign()
            self.assertFalse(database.exists())
            self.assertIsNone(self.store.get(native.ISSUE))
        finally:
            saved.rename(database)

    def test_named_service_does_not_ignore_missing_default_with_prior_initialization_evidence(self):
        self.existing()
        creation = native.Kanban("linear-inbox", profile="default", profile_home=self.fixture.home)
        database = Path(native.kb.kanban_db_path(board="default"))
        saved = database.with_name("missing-saved.db")
        database.rename(saved)
        try:
            bridge = native.Bridge(self.store, self.fixture.authority.api(scope=None), creation, profile="default")
            bridge.handle_webhook(self.fixture.event())
            self.assertFalse(database.exists())
            self.assertIsNone(self.store.get(native.ISSUE))
            with creation.conn() as conn:
                self.assertEqual(native.kb.list_tasks(conn), [])
        finally:
            saved.rename(database)


if __name__ == "__main__":
    unittest.main()
