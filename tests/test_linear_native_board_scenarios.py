"""Existing board routing and board-local task ID collisions against real native SQLite."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from linear_fake_api import Clock, FakeLinear, SELF, load_plugin

SOURCE = os.environ.get("HERMES_AGENT_SRC")
if SOURCE:
    sys.path.insert(0, SOURCE)
load_plugin()
from hermes_fleet_linear_plugin.api import LinearAPI, LinearError
from hermes_fleet_linear_plugin.bridge import Bridge, Kanban
from hermes_fleet_linear_plugin.store import Store


@unittest.skipUnless(SOURCE, "requires explicit official HERMES_AGENT_SRC")
class NativeBoardScenarios(unittest.TestCase):
    def setUp(self):
        from hermes_cli import kanban_db as kb
        self.kb = kb
        self.assertTrue(Path(kb.__file__).resolve().is_relative_to(Path(SOURCE).resolve()))
        temp = tempfile.TemporaryDirectory(prefix="linear-native-boards-")
        self.addCleanup(temp.cleanup)
        self.home = Path(temp.name)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith("HERMES_KANBAN_") and key != "HERMES_DELEGATED_CHILD_CONTEXT"}
        env.update(HERMES_HOME=str(self.home), HERMES_KANBAN_HOME=str(self.home / "boards"), HERMES_PROFILE="default")
        scope = patch.dict(os.environ, env, clear=True)
        scope.start()
        self.addCleanup(scope.stop)
        self.clock = Clock()
        self.remote = FakeLinear(self.clock)
        self.addCleanup(self.remote.close)
        self.creation = Kanban("sample-linear-v1", profile="default", profile_home=self.home)
        self.store = Store(self.home / "linear.db")
        self.bridge = self.restart()

    def restart(self):
        return Bridge(Store(self.home / "linear.db"),
                      LinearAPI(lambda: "synthetic", endpoint=self.remote.url, clock=self.clock),
                      self.creation, profile="default", clock=self.clock)

    def existing(self, slug, issue_id, *, collide=False, assignee="default"):
        board = Kanban(slug, profile="default", profile_home=self.home)
        self.remote.add_issue(issue_id, issue_id.upper())
        with board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=self.clock()):
            if collide:
                with patch("hermes_cli.kanban_db.secrets.token_hex", return_value="abc12345"):
                    task_id = self.kb.create_task(conn, title=issue_id.upper() + ": Existing work", assignee=assignee,
                                                  board=slug, initial_status="blocked")
            else:
                task_id = self.kb.create_task(conn, title=issue_id.upper() + ": Existing work", assignee=assignee,
                                              board=slug, initial_status="blocked")
        return board, task_id

    def operator(self, board, fn, task_id, **kwargs):
        self.clock.now += 2
        with board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=self.clock()):
            return fn(conn, task_id, **kwargs)

    def test_existing_board_followup_and_stop_keep_one_run_and_creation_board_empty(self):
        board, task_id = self.existing("project-board", "issue-project")
        before = board.get(task_id)
        self.bridge.track("issue-project", task_id, board="project-board")
        linked = board.get(task_id)
        self.assertEqual((linked.status, linked.current_run_id, linked.claim_lock, linked.assignee),
                         (before.status, before.current_run_id, before.claim_lock, before.assignee))
        self.operator(board, self.kb.unblock_task, task_id)
        worker = self.operator(board, self.kb.claim_task, task_id, claimer="native-fixture")
        self.bridge.followup("issue-project", "Inspect only the remaining endpoint", "request-one")
        self.assertEqual(board.get(task_id).current_run_id, worker.current_run_id)
        with board.conn() as conn:
            self.assertEqual([comment.body for comment in self.kb.list_comments(conn, task_id)],
                             ["Inspect only the remaining endpoint"])
        self.remote.set_delegate("issue-project", {"id": SELF})
        self.bridge._stop(self.store.get("issue-project"), "", "authorized native operator",
                          (self.clock() + 2) * 1000, "stop-one")
        self.assertEqual(board.get(task_id).status, "blocked")
        with self.creation.conn() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM tasks").fetchone()[0], 0)

    def test_foreign_executor_and_missing_or_archived_board_refuse_without_creation(self):
        board, foreign = self.existing("project-board", "issue-foreign", assignee="other-profile")
        with self.assertRaises(LinearError):
            self.bridge.track("issue-foreign", foreign, board="project-board")
        self.assertIsNone(self.store.native_binding("issue-foreign"))
        self.remote.add_issue("issue-missing", "MISSING-1")
        for slug in ("missing-board", "archived-board"):
            if slug == "archived-board":
                Kanban(slug, profile="default", profile_home=self.home)
                self.kb.remove_board(slug, archive=True)
            with self.assertRaises(LinearError):
                self.bridge.track("issue-missing", "t_missing", board=slug)
            self.assertFalse(self.kb.kanban_db_path(board=slug).exists())

    def test_archived_creation_board_stays_archived_while_content_and_other_boards_work(self):
        import json
        import uuid
        from test_linear_content_actions import ContentRemote
        from hermes_fleet_linear_plugin import chat
        from hermes_fleet_linear_plugin.transport import NativeContext
        self.remote = ContentRemote(self.clock)
        self.addCleanup(self.remote.close)
        board, task_id = self.existing("default", "issue-existing")
        self.kb.remove_board(self.creation.board, archive=True)
        archived = self.kb.read_board_metadata(self.creation.board)
        self.assertTrue(archived["archived"])
        self.assertFalse(Path(archived["db_path"]).exists())
        self.creation = Kanban(self.creation.board, profile="default", profile_home=self.home)
        self.bridge = self.restart()
        self.bridge.profile_home = self.home
        self.remote.add_issue("ordinary-issue", "ORDINARY-1")
        context = NativeContext(home=str(self.home), profile="default", platform="api_server", source="api_server",
                                session_key="authorized-chat", session_id="native-session")
        result = json.loads(chat.handle(self.bridge, {"action": "add_comment", "issue": "ORDINARY-1",
            "id": str(uuid.uuid4()), "body": "Ordinary authorized content after restart"}, context))
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.bridge.flush(), 1)
        self.assertEqual(self.remote.comments[0]["body"], "Ordinary authorized content after restart")
        self.bridge.track("issue-existing", task_id, board="default")
        self.assertEqual(self.bridge.native_status("issue-existing")["board"], "default")
        with self.assertRaises(LinearError):
            self.creation.create(title="Must not resurrect archived work")
        self.assertEqual(self.kb.read_board_metadata(self.creation.board), archived)
        self.assertFalse(Path(archived["db_path"]).exists())
        self.assertEqual(board.get(task_id).status, "blocked")
        self.assertIsNone(board.get(task_id).current_run_id)
        with board.conn() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM tasks").fetchone()[0], 1)

    def test_other_board_native_comment_stays_internal_to_that_board(self):
        board, task_id = self.existing("project-board", "issue-comment")
        self.bridge.track("issue-comment", task_id, board="project-board")
        body = "Native operator confirmed the remaining project endpoint."
        self.clock.now += 2
        with board.conn() as conn, patch("hermes_cli.kanban_db.time.time", return_value=self.clock()):
            self.kb.add_comment(conn, task_id, "dashboard", body)
        self.bridge.pump_kanban()
        self.clock.now += 10
        self.bridge.flush()
        self.assertNotIn(body, self.remote.bodies("issue-comment"))
        with self.creation.conn() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM task_comments").fetchone()[0], 0)

    def test_other_board_specialist_assessment_posts_exact_issue_comment_and_review_state(self):
        from linear_fake_api import load_plugin
        from test_linear_content_actions import ContentRemote
        self.remote = ContentRemote(self.clock)
        self.addCleanup(self.remote.close)
        board, task_id = self.existing("project-board", "issue-specialist")
        self.remote.set_delegate("issue-specialist", {"id": SELF})
        self.remote.issues["issue-specialist"]["team"]["states"]["nodes"].append(
            {"id": "st-review", "name": "In Review", "type": "started"})
        self.remote.sessions["linear-request"] = "issue-specialist"
        original = self.remote._apply
        # Synthetic authoritative HTTP responses; production BoundLinearAPI performs
        # the exact app/workspace/requester/issue checks, without authorization stubs.
        def authority(query, variables):
            if "IdentityBinding" in query:
                return 200, {"data": {"viewer": {"id": SELF}, "organization": {"id": "org-1"}}}
            if "AgentSessionAuthorization" in query:
                return 200, {"data": {"agentSession": {"id": "linear-request", "creator": {"id": "human-1"},
                    "appUser": {"id": SELF}, "issue": {"id": "issue-specialist"}}}}
            return original(query, variables)
        self.remote._apply = authority
        api = load_plugin().BoundLinearAPI(lambda: "synthetic", endpoint=self.remote.url, clock=self.clock,
            identity={"viewer_id": SELF, "organization_id": "org-1"},
            specialist_scope={"allowed_team_ids": ["team-1"], "allowed_project_ids": ["proj-1"],
                              "allowed_requester_ids": ["human-1"], "requester_basis": "agent_session"})
        self.bridge = Bridge(self.store, api, self.creation, profile="default", clock=self.clock,
                             settings={"states": {"done": "In Review"}})
        with api.requester_session({"issue-specialist": "linear-request"}):
            self.bridge.track("issue-specialist", task_id, board="project-board", requester_session="linear-request")
        assessment = "Recommendation: use the verified project endpoint.\nThe native assessment found the bounded change and its supporting evidence."
        self.operator(board, self.kb.complete_task, task_id, result="Verified the project endpoint assessment",
                      metadata={"issue_comment": assessment})
        self.bridge.pump_kanban()
        self.clock.now += 10
        self.bridge.flush()
        self.assertEqual(self.remote.state("issue-specialist"), "In Review")
        self.assertEqual([row["body"] for row in self.remote.comments if row["issueId"] == "issue-specialist"], [assessment])
        self.assertEqual(board.get(task_id).status, "done")
        self.assertEqual(self.store.native_status_receipt("issue-specialist", task_id)["board"], "project-board")

    def test_restart_reports_each_binding_board_with_scoped_atomic_receipts(self):
        for slug in ("default", "project-board"):
            issue_id = "issue-" + slug
            board, task_id = self.existing(slug, issue_id)
            self.bridge.track(issue_id, task_id, board=slug)
            self.operator(board, self.kb.unblock_task, task_id)
            self.bridge.pump_kanban()
            self.operator(board, self.kb.complete_task, task_id,
                          result="Verified the bounded endpoint change with clear supporting findings")
            self.clock.now += 10
            self.assertEqual(self.bridge.flush(), 1)
            self.bridge = self.restart()
            self.bridge.recover()
            self.bridge.flush()
            self.assertEqual(self.remote.state(issue_id), "Done")
            receipt = self.store.native_status_receipt(issue_id, task_id)
            self.assertEqual((receipt["profile"], receipt["board"], receipt["task_id"]), ("default", slug, task_id))
            count = len(self.remote.comments)
            self.bridge = self.restart()
            self.bridge.recover()
            self.bridge.flush()
            self.assertEqual(len(self.remote.comments), count)
        self.assertEqual(self.creation.board, "sample-linear-v1")

    def test_identical_task_ids_do_not_share_terminal_receipts_or_project_batches(self):
        tasks = [self.existing(slug, issue_id, collide=True)
                 for slug, issue_id in (("default", "issue-default"), ("project-board", "issue-project"))]
        self.assertEqual(tasks[0][1], tasks[1][1])
        for (board, task_id), issue_id in zip(tasks, ("issue-default", "issue-project")):
            self.bridge.track(issue_id, task_id, board=board.board)
            self.operator(board, self.kb.complete_task, task_id,
                          result="Verified independent findings for " + issue_id)
            self.bridge.pump_kanban()
            self.clock.now += 10
            self.bridge.flush()
            self.assertEqual(self.remote.state(issue_id), "Done")
            receipt = self.store.native_status_receipt(issue_id, task_id)
            self.assertEqual(receipt["board"], board.board)
        self.assertEqual(len(self.remote.project_updates), 2)
        bodies = [row["body"] for row in self.remote.project_updates]
        self.assertTrue(any("issue-default" in body for body in bodies))
        self.assertTrue(any("issue-project" in body for body in bodies))
        # Another board's prior terminal must not suppress an unfinished archive.
        other, task_id = self.existing("archive-board", "issue-archive", collide=True)
        self.bridge.track("issue-archive", task_id, board="archive-board")
        self.assertFalse(self.store.terminal_captured(task_id, issue_id="issue-archive"))
        self.operator(other, self.kb.archive_task, task_id)
        self.bridge.pump_kanban()
        self.clock.now += 10
        self.bridge.flush()
        self.assertEqual(self.remote.state("issue-archive"), "Blocked")
        self.assertTrue(any("unfinished" in body for body in self.remote.bodies("issue-archive")))
