"""Linear plugin diagnostics: metadata-only decision lines, bounded volume, honest levels.

Job spec "Diagnosable failures": a failed handoff, closure or takeover can be
explained from these lines alone, and they never carry content or credentials.
"""
from __future__ import annotations

import ast
import asyncio
import contextlib
import json
import logging
import os
import re
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from linear_fake_api import ROOT, load_plugin

plugin = load_plugin()
from hermes_fleet_linear_plugin import api as linear_api  # noqa: E402
from hermes_fleet_linear_plugin import chat, diag  # noqa: E402
from hermes_fleet_linear_plugin.api import LinearError  # noqa: E402
from hermes_fleet_linear_plugin.bridge import Bridge  # noqa: E402
from hermes_fleet_linear_plugin.store import Store  # noqa: E402

LOGGER = "hermes_plugins.linear"
ISSUE = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"
TASK = "t_1234abcd"
SECRET_TOKEN = "lin_api_SyntheticSecretToken0123456789"
EMAIL = "someone@example.com"
BODY = f"Please ship it. Bearer {SECRET_TOKEN}\nContact {EMAIL} https://example.com/x?token=abc"
FORBIDDEN = (SECRET_TOKEN, EMAIL, "Bearer", "lin_", "@", "Please ship it", "https://", "\n")
LINE = re.compile(r"linear\.evt action=[a-z_.?]+ outcome=[a-z_?-]+ reason=[a-z0-9_?-]+( [a-z_]+=[A-Za-z0-9_.+?-]+)*")


class Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def lines(self, level: int | None = None) -> list[str]:
        return [r.getMessage() for r in self.records if level is None or r.levelno == level]


class DiagCase(unittest.TestCase):
    def setUp(self) -> None:
        diag.reset()
        self.now = [1000.0]
        self.wall = [1_900_000_000.0]
        for name, source in (("_now", self.now), ("_wall", self.wall)):
            patcher = patch.object(diag, name, lambda source=source: source[0])
            patcher.start()
            self.addCleanup(patcher.stop)
        self.capture = Capture()
        logger = logging.getLogger(LOGGER)
        previous = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self.capture)
        self.addCleanup(logger.removeHandler, self.capture)
        self.addCleanup(logger.setLevel, previous)
        self.addCleanup(diag.reset)

    def assertNoContent(self, records=None) -> None:
        for record in records if records is not None else self.capture.records:
            text = record.getMessage()
            if record.exc_info:
                text += logging.Formatter().formatException(record.exc_info)
            for forbidden in FORBIDDEN:
                self.assertNotIn(forbidden, text)


class FieldSafetyTests(DiagCase):
    def test_every_field_rejects_content_and_credentials(self) -> None:
        hostile = [BODY, SECRET_TOKEN, EMAIL, "Bearer abc", "https://example.com/?q=1", "a\nb", "x" * 500,
                   "ABC-1 extra", "t_1234abcd\n", {"body": BODY}, [BODY], b"bytes", 3.5e300, True]
        for value in hostile:
            fields = {key: value for key in diag.FIELDS}
            diag.evt("status.write", "refused", value if isinstance(value, str) else "-", **fields)
            diag.evt(value if isinstance(value, str) else "status.write", value if isinstance(value, str) else "-")
        self.assertTrue(self.capture.records)
        self.assertNoContent()
        for line in self.capture.lines():
            self.assertRegex(line, LINE)

    def test_unknown_keys_are_dropped_and_known_ids_are_shortened(self) -> None:
        diag.remember({"id": ISSUE, "identifier": "ABC-584", "title": BODY})
        line = diag.render("status.write", "refused", "updated_after_source", iss=ISSUE, task=TASK,
                           sess="9f8e7d6c-1111-4222-8333-444455556666", row="93ff1e7f-aaaa-4bbb-8ccc-dddd",
                           attempt=0, ms=12.7, body=BODY, title=BODY, state="blocked")
        self.assertEqual(line, "linear.evt action=status.write outcome=refused reason=updated_after_source "
                               "issue=ABC-584 iss=0a1b2c3d task=t_1234abcd sess=9f8e7d6c row=93ff1e7f "
                               "attempt=0 ms=12 state=blocked")

    def test_fixed_field_set_and_agent_context(self) -> None:
        diag.configure(agent="alpha", profile="default")
        diag.evt("native.event", "queued", "native_gave_up", iss=ISSUE, task=TASK, evt=8558, kind="gave_up")
        self.assertEqual(self.capture.lines(), [
            "linear.evt action=native.event outcome=queued reason=native_gave_up agent=alpha profile=default "
            "iss=0a1b2c3d task=t_1234abcd evt=8558 kind=gave_up"])

    def test_reason_vocabulary_is_closed(self) -> None:
        diag.evt("status.write", "refused", "made_up_reason")
        diag.evt("status.write", "refused", "native_crashed")
        diag.evt("status.write", "invented_outcome", "-")
        self.assertIn("reason=?", self.capture.lines()[0])
        self.assertIn("reason=native_crashed", self.capture.lines()[1])
        self.assertIn("outcome=?", self.capture.lines()[2])


class VolumeTests(DiagCase):
    def test_warning_once_per_fifteen_minutes_then_suppressed_count(self) -> None:
        # One binding whose read raises on every 2 s tick for an hour.
        for _ in range(1800):
            diag.warn("binding.held", "held", "retryable_http", iss=ISSUE, task=TASK, exc="LinearError")
            self.now[0] += 2
        warnings = self.capture.lines(logging.WARNING)
        self.assertLessEqual(len(warnings), 4)
        self.assertNotIn("suppressed=", warnings[0])
        self.assertIn("suppressed=", warnings[-1])

    def test_distinct_issues_and_reasons_are_not_merged(self) -> None:
        diag.warn("binding.held", "held", "retryable_http", iss=ISSUE)
        diag.warn("binding.held", "held", "non_retryable", iss=ISSUE)
        diag.warn("binding.held", "held", "retryable_http", iss="ffffffff-0000-4000-8000-000000000000")
        self.assertEqual(len(self.capture.lines(logging.WARNING)), 3)

    def test_repeated_refusal_is_hourly_and_applied_is_never_deduped(self) -> None:
        for _ in range(30):
            diag.evt("status.write", "refused", "updated_after_source", iss=ISSUE, task=TASK)
            diag.evt("status.write", "applied", "-", iss=ISSUE, task=TASK)
            self.now[0] += 60
        lines = self.capture.lines(logging.INFO)
        self.assertEqual(sum("outcome=refused" in line for line in lines), 1)
        self.assertEqual(sum("outcome=applied" in line for line in lines), 30)

    def test_transition_logs_not_running_once(self) -> None:
        for _ in range(1800):
            diag.transition("delegate.write", "skipped", "not_running", key=ISSUE, iss=ISSUE, task=TASK)
        self.assertEqual(len(self.capture.lines()), 1)
        diag.transition("delegate.write", "queued", "binding_recovery", key=ISSUE, iss=ISSUE)
        self.assertEqual(len(self.capture.lines()), 2)

    def test_hard_cap_of_two_hundred_info_lines_per_hour(self) -> None:
        for index in range(2000):
            diag.evt("status.write", "applied", "-", iss=f"{index:08x}")
            diag.count("sent")
            diag.tick_done(self.now[0])
            self.now[0] += 1.7  # one hour
        info = self.capture.lines(logging.INFO)
        self.assertLessEqual(len(info), diag.HOURLY_CAP)
        self.assertTrue(any("reconcile.summary" in line for line in info), "summaries keep flowing")
        throttled = [line for line in self.capture.lines(logging.WARNING) if "diag.throttled" in line]
        self.assertEqual(len(throttled), 1)

    def test_idle_budget_zero_lines_and_hourly_health_only_with_linked_work(self) -> None:
        gauges = {"n_bindings": 0, "n_pending": 0}
        for _ in range(1800 * 3):  # three idle hours at 2 s ticks
            diag.tick_done(self.now[0], lambda: gauges)
            self.now[0] += 2
        self.assertEqual(self.capture.lines(), [])
        gauges = {"n_bindings": 18, "n_pending": 0, "n_held": 0, "n_failed_unreported": 0}
        for _ in range(1800 * 3):
            diag.tick_done(self.now[0], lambda: gauges)
            self.now[0] += 2
        lines = self.capture.lines()
        self.assertEqual(len(lines), 3)
        self.assertTrue(all("action=service.health" in line and "n_bindings=18" in line for line in lines))

    def test_routine_background_work_never_triggers_a_summary(self) -> None:
        for _ in range(1800):  # one hour of heartbeats and keepalive comments on running work
            diag.native_event("heartbeat", iss=ISSUE, task=TASK, evt=1)
            diag.count("content")
            diag.tick_done(self.now[0])
            self.now[0] += 2
        self.assertEqual(self.capture.lines(), [])

    def test_summary_once_per_minute_only_when_something_happened(self) -> None:
        for second in range(0, 600, 2):
            if second < 120:
                diag.count("events")
            diag.tick_done(self.now[0])
            self.now[0] += 2
        summaries = [line for line in self.capture.lines() if "reconcile.summary" in line]
        self.assertEqual(len(summaries), 2)
        self.assertIn("n_events=", summaries[0])


class DetailSettingTests(DiagCase):
    def test_detail_requires_until_and_expires(self) -> None:
        diag.configure({"detail": True, "issues": ["ABC-584"]}, agent="a", profile="p")
        self.assertIn("reason=invalid_diagnostics", self.capture.lines(logging.WARNING)[0])
        self.assertFalse(diag.detail_active())
        diag.configure({"detail": True, "issues": ["ABC-584"], "until": "2031-01-01T00:00:00Z"})
        diag.remember({"id": ISSUE, "identifier": "ABC-584"})
        self.assertTrue(diag.detail("status.write", "queued", iss=ISSUE, state="blocked"))
        self.assertFalse(diag.detail("status.write", "queued", iss="ffffffff-0000-4000-8000-000000000000"))
        self.assertIn("lvl=detail", self.capture.lines(logging.INFO)[-1])
        self.wall[0] = 1_900_000_000.0 + 86_400 * 365
        self.assertFalse(diag.detail("status.write", "queued", iss=ISSUE))

    def test_detail_off_by_default(self) -> None:
        diag.configure(None, agent="a", profile="p")
        self.assertFalse(diag.detail("tool.result", "applied"))
        self.assertEqual(self.capture.records, [])

    def test_diagnostics_is_an_accepted_plugin_setting(self) -> None:
        self.assertIn("diagnostics", plugin.SETTINGS)


class Kanban:
    executor_profile, board = "default", None

    def __init__(self, status: str = "blocked") -> None:
        self.status = status

    def require_task(self, task_id):
        return SimpleNamespace(id=task_id, body="", assignee="default", status=self.status, current_run_id=None)

    get = require_task


class Api:
    specialist_scope = None

    def __init__(self, *, state="started", fail_comment=False) -> None:
        self.state, self.fail_comment, self.comments = state, fail_comment, []

    @contextlib.contextmanager
    def guarded_mutation(self, guard):
        yield

    def issue(self, ref):
        return {"id": ref, "identifier": "ABC-566", "title": BODY, "description": BODY,
                "state": {"type": self.state, "id": "s"}, "delegate": None, "updatedAt": "2026-01-01T00:00:00Z",
                "team": {"key": "HF", "states": {"nodes": []}}}

    def viewer_id(self):
        return "app-user"

    def create_comment(self, client_id, issue_id, body):
        if self.fail_comment:
            raise LinearError("Linear rejected: " + body, retryable=False)
        self.comments.append(body)

    def verify_comment(self, comment_id, issue_id, body):
        return True


class BridgePathTests(DiagCase):
    def make(self, api: Api, kanban: Kanban | None = None) -> tuple[Bridge, Store]:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = Store(Path(tmp.name) / "state.db")
        store.bind_native(ISSUE, TASK, "default", "default", at=0)
        bridge = Bridge(store, api, kanban or Kanban(), profile="default",
                        settings={"ingress_profile": "alpha"})
        return bridge, store

    def test_failed_comment_logs_metadata_only_and_keeps_reason_on_row(self) -> None:
        bridge, store = self.make(Api(fail_comment=True))
        row_id = store.enqueue("comment", {"issue_id": ISSUE, "body": BODY, "content_action": True,
                                           "unlinked_content": True})
        bridge.flush()
        warnings = self.capture.lines(logging.WARNING)
        self.assertEqual(len(warnings), 1, warnings)
        self.assertIn("action=outbox.retry outcome=failed reason=non_retryable", warnings[0])
        self.assertIn("exc=LinearError", warnings[0])
        self.assertNoContent()
        self.assertEqual(store.outbox_row(row_id)["payload"]["failure_reason"], "non_retryable")

    def test_applied_comment_is_detail_only_and_never_logs_its_body(self) -> None:
        bridge, store = self.make(Api())
        store.enqueue("comment", {"issue_id": ISSUE, "body": BODY, "content_action": True, "unlinked_content": True})
        bridge.flush()
        self.assertEqual(self.capture.lines(logging.INFO), [])
        diag.configure({"detail": True, "until": "2031-01-01T00:00:00Z"})
        store.enqueue("comment", {"issue_id": ISSUE, "body": BODY + " again", "content_action": True,
                                  "unlinked_content": True})
        bridge.flush()
        self.assertTrue(any("action=content.write outcome=applied" in line for line in self.capture.lines()))
        self.assertNoContent()

    def test_refused_status_names_the_fence_and_persists_it(self) -> None:
        # ABC-566: a terminal-looking status write refused with no trace. Now the line and the row say why.
        bridge, store = self.make(Api(state="completed"))
        row_id = store.enqueue("status", {"issue_id": ISSUE, "task_id": TASK, "state": "blocked"})
        self.assertTrue(row_id)
        bridge.flush()
        refused = [line for line in self.capture.lines(logging.INFO) if "action=status.write" in line]
        self.assertEqual(len(refused), 1, self.capture.lines())
        self.assertIn("outcome=refused reason=issue_closed", refused[0])
        self.assertIn("issue=ABC-566", refused[0])
        self.assertIn(f"row={row_id[:8]}", refused[0])
        self.assertEqual(store.outbox_row(row_id)["payload"]["outcome_reason"], "issue_closed")
        self.assertNoContent()

    def test_bound_issue_webhook_is_one_line_without_payload_text(self) -> None:
        bridge, store = self.make(Api())
        event = {"type": "Issue", "action": "update", "data": {"id": ISSUE, "title": BODY, "description": BODY}}
        unbound = {"type": "Issue", "action": "update", "data": {"id": "ffffffff-0000-4000-8000-000000000000"}}
        comment = {"type": "Comment", "action": "create", "data": {"body": BODY}}
        with tempfile.TemporaryDirectory() as directory:
            inbox = Path(directory) / "ingress.db"
            import sqlite3
            with contextlib.closing(sqlite3.connect(inbox)) as db, db:
                db.execute("CREATE TABLE deliveries (logical_agent TEXT, delivery_id TEXT, profile TEXT, payload TEXT, "
                           "received_at REAL, attempts INTEGER DEFAULT 0, status TEXT DEFAULT 'pending')")
                for index, payload in enumerate((event, unbound, comment)):
                    db.execute("INSERT INTO deliveries (logical_agent, delivery_id, profile, payload, received_at) "
                               "VALUES ('alpha', ?, 'alpha', ?, 0)", (f"dlv-{index}", json.dumps(payload)))
            bridge.drain_ingress(inbox, retry_now=True)
        deliveries = [line for line in self.capture.lines() if "action=webhook.delivery" in line]
        self.assertEqual(len(deliveries), 1, self.capture.lines())
        self.assertIn("outcome=routed reason=bound_issue_recheck", deliveries[0])
        self.assertIn("kind=Issue.update", deliveries[0])
        self.assertNoContent()

    def test_delegate_recovery_names_its_skip_once(self) -> None:
        bridge, store = self.make(Api())
        row = store.get(ISSUE)
        adapter = Kanban("running")  # Running, but its claim is not readable yet.
        with patch.object(Bridge, "_current_claim", staticmethod(lambda adapter, task: None)):
            for _ in range(50):
                bridge._recover_binding_delegate(row, adapter)
        lines = self.capture.lines()
        self.assertEqual(lines, [
            "linear.evt action=delegate.write outcome=skipped reason=claim_unconsumed agent=alpha profile=default "
            f"iss=0a1b2c3d task={TASK}"])


class DemotedErrorTests(DiagCase):
    def test_duplicate_create_replay_is_info_not_error(self) -> None:
        client_id = "0b8f5a7e-3c1d-4e2f-9a6b-7c8d9e0f1a2b"
        payload = {"data": None, "errors": [{"message": "conflict on insert of Comment",
                                              "extensions": {"code": "INPUT_ERROR",
                                                             "userPresentableMessage": f"id {client_id} exists"}}]}
        api = linear_api.LinearAPI(lambda: SECRET_TOKEN, transport=lambda url, body, headers: (
            200, {}, json.dumps(payload).encode()))
        api.create_comment(client_id, ISSUE, BODY)  # treated as success
        self.assertEqual([r for r in self.capture.records if r.levelno >= logging.ERROR], [])
        self.assertEqual(self.capture.lines(logging.INFO), [
            "linear.evt action=api.result outcome=deduped reason=duplicate_create code=INPUT_ERROR"])
        self.assertNoContent()

    def test_real_graphql_error_stays_error(self) -> None:
        payload = {"errors": [{"message": "Argument Validation Error", "extensions": {"code": "INVALID_INPUT"}}]}
        api = linear_api.LinearAPI(lambda: SECRET_TOKEN, transport=lambda url, body, headers: (
            200, {}, json.dumps(payload).encode()))
        with self.assertRaises(LinearError):
            api.graphql("query Q { viewer { id } }")
        self.assertEqual(len([r for r in self.capture.records if r.levelno == logging.ERROR]), 1)

    def _serve(self, error: BaseException) -> None:
        class Context:
            def __init__(self) -> None:
                self.unloads = []

            def get_config(self, key, default=None):
                return {"enabled": True}.get(key, default)

            def register_tool(self, **kwargs):
                pass

            def register_hook(self, name, callback):
                pass

            def on_unload(self, callback):
                self.unloads.append(callback)

        async def failing(*args, **kwargs):
            raise error

        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        constants = types.ModuleType("hermes_constants")
        constants.get_hermes_home = lambda: Path(home.name)
        context = Context()
        with patch.dict(sys.modules, {"hermes_constants": constants}), \
                patch.dict(os.environ, {"_HERMES_GATEWAY": "1"}), \
                patch.object(plugin, "_reporting_loop", failing):
            os.environ.pop("HERMES_KANBAN_TASK", None)
            plugin.register(context)
            for _ in range(500):  # let the service fail on its own before unload
                if self.capture.records:
                    break
                threading.Event().wait(0.01)
            for close in context.unloads:
                close()

    def test_shutdown_race_is_info_without_traceback(self) -> None:
        self._serve(RuntimeError("cannot schedule new futures after shutdown"))
        self.assertEqual([r for r in self.capture.records if r.levelno >= logging.WARNING or r.exc_info], [])
        self.assertEqual(self.capture.lines(logging.INFO), [
            "linear.evt action=service.stop outcome=stopped reason=shutdown exc=RuntimeError"])

    def test_genuine_crash_keeps_error_and_traceback(self) -> None:
        self._serve(ValueError("synthetic crash"))
        errors = [r for r in self.capture.records if r.levelno == logging.ERROR]
        self.assertEqual(len(errors), 1)
        self.assertIn("reason=crash", errors[0].getMessage())
        self.assertIsNotNone(errors[0].exc_info)


class StartupTests(DiagCase):
    def test_version_line_names_the_running_build(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = Store(Path(tmp.name) / "state.db")
        store.bind_native(ISSUE, TASK, "default", "default", at=0)
        diag.configure(agent="alpha", profile="default")
        diag.service_start(store, 2.0, 300.0)
        line = self.capture.lines(logging.INFO)[0]
        version = (ROOT / "plugins/linear/plugin.yaml").read_text().split("version:", 1)[1].split()[0]
        self.assertIn("action=service.start outcome=started", line)
        self.assertIn(f"ver={version}", line)
        self.assertRegex(line, r" sha=[0-9a-f]{8}")
        self.assertIn("n_bindings=1", line)
        self.assertIn("detail=off", line)

    def test_logger_is_under_the_core_component_prefix(self) -> None:
        # Core routes hermes_plugins.* to gateway.log (about six weeks of history).
        self.assertTrue(diag.LOGGER_NAME.startswith("hermes_plugins."))
        source = "\n".join(path.read_text() for path in (ROOT / "plugins/linear").glob("*.py"))
        self.assertNotRegex(source, r'getLogger\("linear')


class ReasonCoverageTests(unittest.TestCase):
    """Every refusal branch in the decision functions names a code from the closed set."""

    SOURCE = (ROOT / "plugins/linear/bridge.py").read_text()
    DECISIONS = ("_send", "may_write", "_stop", "_guarded_stop", "_send_delegate", "_recover_binding_delegate",
                 "_handle_webhook")
    # The two remaining literal returns: may_write already recorded the reason; an unknown kind is "not_applied".
    ALLOWED_BARE_FALSE = 2

    def test_reason_literals_are_in_the_vocabulary(self) -> None:
        codes = re.findall(r'diag\.(?:because|skip|warn|evt|transition|detail)\(\s*"([a-z0-9_]+)"', self.SOURCE)
        codes += re.findall(r'diag\.(?:warn|evt|detail|transition)\("[a-z._]+", "[a-z_-]+", "([a-z0-9_]+)"', self.SOURCE)
        codes += re.findall(r'_log_delegate\(row, "([a-z_]+)"', self.SOURCE)
        reasons = {code for code in codes if code not in diag.ACTIONS}
        self.assertTrue(reasons)
        self.assertEqual(sorted(code for code in reasons if code not in diag.REASONS and "." not in code), [])

    def test_decision_functions_have_no_silent_refusals(self) -> None:
        tree = ast.parse(self.SOURCE)
        silent = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in self.DECISIONS:
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Return) and (sub.value is None or (
                            isinstance(sub.value, ast.Constant) and sub.value.value is False)):
                        silent.append((node.name, sub.lineno))
        self.assertEqual(len(silent), self.ALLOWED_BARE_FALSE, silent)
        self.assertTrue(all(name == "_send" for name, _ in silent), silent)


if __name__ == "__main__":
    unittest.main()
