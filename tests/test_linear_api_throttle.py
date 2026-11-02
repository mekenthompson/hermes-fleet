"""Linear API budget: one shared budget, no per-call identity tax, no tight retry loops.

Linear gives each OAuth app user 5,000 requests and 2,000,000 complexity points
per hour, shared by every client of that app user. These tests drive the real
client, store and bridge with synthetic transports; nothing reaches Linear.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from linear_fake_api import Clock, load_plugin

plugin = load_plugin()
from hermes_fleet_linear_plugin import BoundLinearAPI  # noqa: E402
from hermes_fleet_linear_plugin.api import (  # noqa: E402
    LinearAPI, LinearError, RateLimited, Throttled, backoff_delay)
from hermes_fleet_linear_plugin.oauth import ConnectOAuth, ReauthorizationRequired  # noqa: E402
from hermes_fleet_linear_plugin.store import Store  # noqa: E402
from test_linear_specialist_authorization import (  # noqa: E402
    PROJECT, Authority, Bridge, FakeKanban, ISSUE, SESSION, VIEWER, issue_record)

IDENTITY = {"viewer_id": "app-a", "organization_id": "org-a"}
ISSUE_DATA = {"id": "issue-1", "identifier": "OPS-1", "team": {"id": "team-1", "key": "OPS"}, "project": None}


def budget_headers(clock, remaining, limit=5000, complexity=(2_000_000, 2_000_000)):
    reset = str((clock() + 1800) * 1000)
    return {"X-RateLimit-Requests-Remaining": str(remaining), "X-RateLimit-Requests-Limit": str(limit),
            "X-RateLimit-Requests-Reset": reset, "X-RateLimit-Complexity-Remaining": str(complexity[0]),
            "X-RateLimit-Complexity-Limit": str(complexity[1]), "X-RateLimit-Complexity-Reset": reset,
            "X-Complexity": "12"}


class Wire:
    """Counts requests by kind; replies like Linear for identity, issue reads and mutations."""
    def __init__(self, clock, headers=None):
        self.clock, self.headers, self.calls = clock, headers or (lambda: {}), []

    def __call__(self, url, body, headers):
        query = json.loads(body)["query"]
        kind = ("identity" if "IdentityBinding" in query else "mutation" if query.lstrip().startswith("mutation")
                else "read")
        self.calls.append((kind, headers["Authorization"]))
        if kind == "identity":
            data = {"viewer": {"id": "app-a"}, "organization": {"id": "org-a"}}
        elif kind == "mutation":
            data = {"issueUpdate": {"success": True, "issue": ISSUE_DATA}}
        else:
            data = {"issue": ISSUE_DATA, "viewer": {"id": "app-a"}}
        return 200, self.headers(), json.dumps({"data": data}).encode()

    def kinds(self):
        return [kind for kind, _ in self.calls]


class IdentityProofCacheTests(unittest.TestCase):
    def test_n_reads_cost_n_plus_one_requests_not_2n(self):
        clock = Clock()
        wire = Wire(clock)
        api = BoundLinearAPI(lambda: "token-a", identity=IDENTITY, transport=wire, clock=clock)
        for _ in range(10):
            api.issue("issue-1")
        api.viewer_id()
        self.assertEqual(wire.kinds(), ["identity"] + ["read"] * 10)

    def test_new_token_ttl_and_401_each_force_a_fresh_identity_check(self):
        clock, token = Clock(), ["token-a"]
        wire = Wire(clock)
        api = BoundLinearAPI(lambda: token[0], identity=IDENTITY, transport=wire, clock=clock)
        api.issue("issue-1")
        token[0] = "token-b"
        api.issue("issue-1")
        clock.now += 601
        api.issue("issue-1")
        self.assertEqual(wire.kinds(), ["identity", "read"] * 3)
        api._credential_rejected()  # what a 401 does
        api.issue("issue-1")
        self.assertEqual(wire.kinds()[-2:], ["identity", "read"])

    def test_mutations_always_reprove_identity(self):
        clock = Clock()
        wire = Wire(clock)
        api = BoundLinearAPI(lambda: "token-a", identity=IDENTITY, transport=wire, clock=clock)
        api.issue("issue-1")
        api.update_issue("issue-1", {"stateId": "s"})
        self.assertEqual(wire.kinds(), ["identity", "read", "identity", "mutation"])

    def test_unauthorized_read_drops_the_cached_proof(self):
        clock, statuses = Clock(), iter([200, 200, 401, 200, 200])
        wire = Wire(clock)
        def transport(url, body, headers):
            status, reply_headers, raw = wire(url, body, headers)
            return next(statuses), reply_headers, raw
        api = BoundLinearAPI(lambda: "token-a", identity=IDENTITY, transport=transport, clock=clock)
        api.issue("issue-1")
        with self.assertRaises(LinearError):
            api.issue("issue-1")  # 401, provider cannot refresh
        api.issue("issue-1")
        self.assertEqual(wire.kinds(), ["identity", "read", "read", "identity", "read"])


class SharedBudgetTests(unittest.TestCase):
    def test_ratelimited_with_headers_pauses_every_client_of_the_profile(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.db.rate-limit.json"
            limited = json.dumps({"errors": [{"extensions": {"code": "RATELIMITED"}}]}).encode()
            headers = {**budget_headers(clock, 0), "X-RateLimit-Requests-Reset": str((clock() + 900) * 1000)}
            bridge_client = LinearAPI(lambda: "t", transport=lambda *a: (400, headers, limited), clock=clock,
                                      rate_limit_path=path)
            with self.assertRaises(RateLimited) as caught:
                bridge_client.graphql("query { viewer { id } }")
            self.assertEqual(caught.exception.until, clock() + 900)
            sent = []
            skill_client = LinearAPI(lambda: "t", transport=lambda *a: sent.append(a), clock=clock, rate_limit_path=path)
            with self.assertRaises(RateLimited):
                skill_client.graphql("query { viewer { id } }")
            self.assertEqual(sent, [])

    def test_low_budget_defers_optional_then_normal_reads_but_keeps_writes_and_essential_work(self):
        clock, remaining = Clock(), [5000]
        wire = Wire(clock, lambda: budget_headers(clock, remaining[0]))
        api = LinearAPI(lambda: "t", transport=wire, clock=clock)
        with api.priority("optional"):
            api.issue("issue-1")
        remaining[0] = 900  # 18%: optional work yields
        api.issue("issue-1")
        with api.priority("optional"), self.assertRaises(Throttled) as caught:
            api.issue("issue-1")
        self.assertEqual(caught.exception.priority, "optional")
        self.assertEqual(caught.exception.until, clock() + 1800)
        self.assertIsInstance(caught.exception, RateLimited)  # callers that already wait for quota still do
        api.issue("issue-1")  # normal still allowed
        remaining[0] = 200  # 4%: only writes and essential work
        api.issue("issue-1")
        with self.assertRaises(Throttled):
            api.issue("issue-1")
        api.update_issue("issue-1", {"stateId": "s"})
        with api.priority("essential"):
            api.issue("issue-1")
            with api.priority("optional"):  # nested work keeps the caller's priority
                api.issue("issue-1")
        # The 4% observation arrives with the fourth read; the next normal read is the one deferred.
        self.assertEqual(wire.kinds(), ["read"] * 4 + ["mutation", "read", "read"])
        clock.now += 1801  # window reset: the old observation no longer applies
        with api.priority("optional"):
            api.issue("issue-1")

    def test_complexity_budget_counts_too_and_crossings_are_logged_once(self):
        clock = Clock()
        wire = Wire(clock, lambda: budget_headers(clock, 4000, complexity=(300_000, 2_000_000)))
        api = LinearAPI(lambda: "t", transport=wire, clock=clock)
        with self.assertLogs("linear", "WARNING") as logs:
            api.issue("issue-1")
            api.issue("issue-1")  # 15% of complexity left: normal reads continue
        self.assertEqual(len([line for line in logs.output if "shared request budget" in line]), 1)
        with api.priority("optional"), self.assertRaises(Throttled):
            api.issue("issue-1")

    def test_budget_observed_by_one_client_is_used_by_another(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.db.rate-limit.json"
            first = LinearAPI(lambda: "synthetic-secret-token", transport=Wire(clock, lambda: budget_headers(clock, 100)), clock=clock,
                              rate_limit_path=path)
            first.issue("issue-1")
            wire = Wire(clock)
            second = LinearAPI(lambda: "t", transport=wire, clock=clock, rate_limit_path=path)
            with second.priority("optional"), self.assertRaises(Throttled):
                second.issue("issue-1")
            self.assertEqual(wire.calls, [])
            saved = json.loads(path.read_text())
            self.assertEqual(saved["budget"]["requests"]["remaining"], 100)
            self.assertNotIn("synthetic-secret-token", path.read_text())

    def test_profile_rate_limit_path_is_one_shared_helper(self):
        home = Path("/synthetic/home")
        self.assertEqual(plugin.rate_limit_path(None, home), home / "linear" / "state.db.rate-limit.json")
        self.assertEqual(plugin.rate_limit_path("/x/state.db", home), Path("/x/state.db.rate-limit.json"))


class BackoffTests(unittest.TestCase):
    def test_delay_doubles_caps_and_jitters(self):
        delays = [backoff_delay(n, base=15, cap=3600, rand=lambda: 0.5) for n in range(1, 12)]
        self.assertEqual(delays[:4], [15, 30, 60, 120])
        self.assertEqual(max(delays), 3600)
        low, high = backoff_delay(3, base=15, cap=3600, rand=lambda: 0.0), backoff_delay(3, base=15, cap=3600, rand=lambda: 1.0)
        self.assertEqual((low, high), (48.0, 72.0))  # restarted callers spread out instead of retrying in step

    def test_store_backoff_survives_restart_and_clears(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            self.assertTrue(store.backoff_ready("delegate:issue-1", 1000))
            first = store.backoff_fail("delegate:issue-1", 1000)
            second = store.backoff_fail("delegate:issue-1", 1000)
            self.assertTrue(1000 + 12 <= first <= 1000 + 18 and 1000 + 24 <= second <= 1000 + 36)
            reopened = Store(Path(directory) / "state.db")
            self.assertFalse(reopened.backoff_ready("delegate:issue-1", 1001))
            self.assertTrue(reopened.backoff_ready("delegate:issue-2", 1001))
            reopened.backoff_clear("delegate:issue-1")
            self.assertTrue(reopened.backoff_ready("delegate:issue-1", 1001))

    def test_guard_deferred_row_backs_off_after_two_quick_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            row = store.enqueue("comment", {"issue_id": "issue-1", "body": "b", "content_action": True, "unlinked_content": True}, at=0)
            self.assertTrue(row)
            now, waits = 100.0, []
            for _ in range(12):
                until = store.defer_guarded(row, now)
                waits.append(max(0.0, until - now))
                now = max(now, until)
            self.assertEqual(waits[:2], [0, 0])  # two quick retries leave the row untouched
            self.assertTrue(24 <= waits[2] <= 36 and waits[3] > waits[2])
            self.assertLessEqual(max(waits), 1800 * 1.2)
            self.assertIsNone(store.defer_guarded(row, now - 1))  # a future next_at (quiet period) is left alone

    def test_rejected_rows_are_never_revived_and_timeouts_at_most_three_times_hourly(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            rejected = store.enqueue("comment", {"issue_id": "issue-1", "body": "r", "content_action": True, "unlinked_content": True}, at=0)
            timeout = store.enqueue("comment", {"issue_id": "issue-2", "body": "t", "content_action": True, "unlinked_content": True}, at=0)
            store.retry(store.outbox_row(rejected), 10)  # an earlier retry: attempts > 0
            store.mark(rejected, "failed", "Linear GraphQL error: invalid input")
            store.retry(store.outbox_row(timeout), 90_000)  # 24 h old: given up as a timeout
            self.assertEqual(store.outbox_row(timeout)["state"], "failed")
            revivals, now = 0, 100_000.0
            for _ in range(20):
                revivals += store.revive_failed(now)
                for row_id in (rejected, timeout):
                    if store.outbox_row(row_id)["state"] == "pending":
                        store.mark(row_id, "failed") if row_id == rejected else store.retry(store.outbox_row(row_id), now)
                now += 600
            self.assertEqual(store.outbox_row(rejected)["state"], "failed")
            self.assertEqual(store.outbox_row(rejected)["payload"]["failure_kind"], "rejected")
            self.assertEqual(revivals, 3)
            self.assertEqual(store.outbox_row(timeout)["payload"]["revivals"], 3)


class RefreshCooldownTests(unittest.TestCase):
    def test_failed_refresh_waits_instead_of_posting_every_call(self):
        for failure in (RuntimeError("Linear token endpoint HTTP 503; will retry"), ReauthorizationRequired("revoked")):
            with self.subTest(failure=type(failure).__name__), tempfile.TemporaryDirectory() as directory:
                clock, posts = Clock(), []
                item = SimpleNamespace(credentials=lambda: {"refresh_token": "r", "client_id": "c", "client_secret": "s"})
                def post(form):
                    posts.append(form)
                    raise failure
                oauth = ConnectOAuth(item, Path(directory) / "oauth.json", clock=clock, post=post)
                for _ in range(30):
                    with self.assertRaises(type(failure)):
                        oauth()
                self.assertEqual(len(posts), 1)
                clock.now += 61
                with self.assertRaises(type(failure)):
                    oauth()
                self.assertEqual(len(posts), 2)
                clock.now += 61  # second cooldown is 120 s
                with self.assertRaises(type(failure)):
                    oauth()
                self.assertEqual(len(posts), 2)


class BridgeLoopTests(unittest.TestCase):
    def bridge(self, directory, clock):
        authority = Authority()
        authority.issues[ISSUE]["delegate"] = {"id": VIEWER}
        store, kanban = Store(Path(directory) / "state.db"), FakeKanban()
        bridge = Bridge(store, authority.api(scope=None), kanban, profile="synthetic", clock=clock)
        return bridge, authority, store, kanban

    def test_issue_webhooks_skip_our_echoes_and_coalesce_to_one_issue(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, _, store, _ = self.bridge(directory, clock)
            for issue_id in ("issue-a", "issue-b", "issue-c"):
                store.put(issue_id, "native", "", task_id="task-" + issue_id)
            checked = []
            bridge._recheck_row = lambda row: checked.append(row["issue_id"])
            bridge._last_recheck = clock()
            bridge.handle_webhook({"type": "Issue", "action": "update", "data": {"id": "issue-a"}, "actor": {"id": VIEWER}})
            bridge.recheck()
            self.assertEqual(checked, [])  # our own write echoed back
            for _ in range(27):  # a burst of human edits on one issue
                bridge.handle_webhook({"type": "Issue", "action": "update", "data": {"id": "issue-a"}, "actor": {"id": "person"}})
            bridge.recheck()
            self.assertEqual(checked, ["issue-a"])  # one issue, once, not every tracked issue
            bridge.handle_webhook({"type": "Issue", "action": "update", "data": {"id": "issue-a"}, "actor": {"id": "person"}})
            for _ in range(10):
                clock.now += 2
                bridge.recheck()
            self.assertEqual(checked, ["issue-a"])
            clock.now += 15
            bridge.recheck()
            self.assertEqual(checked, ["issue-a", "issue-a"])  # the later edit is not lost, only coalesced

    def test_finished_rows_do_not_poll_linear(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, authority, store, _ = self.bridge(directory, clock)
            status = {"value": "done"}
            bridge._task_gone = lambda row: False
            bridge._native_task = lambda issue_id: (store.get(issue_id), SimpleNamespace(status=status["value"]))
            bridge._last_unbound_sweep = clock()  # the hourly unbound sweep is its own bounded pass, not per row
            counts = []
            for rows in (5, 50, 200):
                for index in range(rows):
                    store.put(f"issue-{index}", "native", "", task_id=f"task-{index}")
                before = len(authority.requests)
                bridge.recheck(force=True)
                counts.append(len(authority.requests) - before)
            self.assertEqual(counts, [0, 0, 0])
            status["value"] = "running"
            authority.issues.update({f"issue-{i}": {**authority.issues[ISSUE], "id": f"issue-{i}"} for i in range(3)})
            bridge.recheck(force=True)
            self.assertGreater(len(authority.requests), 0)

    def test_failing_pump_step_backs_off_instead_of_reading_every_tick(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, _, store, kanban = self.bridge(directory, clock)
            task = kanban.create(title="OPS-1: native task")
            store.bind_native(ISSUE, task.id, "synthetic", "default", owner_ref=SESSION, at=0)
            attempts = []
            def refuse(row, adapter):
                attempts.append(clock())
                raise LinearError("Linear changed; read current status before routing work", retryable=False)
            bridge._reconcile_status = refuse
            bridge._recover_binding_delegate = lambda row, adapter: None
            start = clock()
            for _ in range(1800):  # one hour of 2 s ticks
                bridge.pump_kanban()
                clock.now += 2
            self.assertLess(len(attempts), 12)  # was 1,800
            self.assertLess(attempts[1] - attempts[0], 30)  # a transient refusal still retries soon
            self.assertGreater(clock() - start, 3000)

    def blocked_task_with_uncertain_closeout(self, directory, clock):
        bridge, authority, store, kanban = self.bridge(directory, clock)
        issue = authority.issues[ISSUE]
        # Every Linear-created task carries the issue marker, so its effects need a Linear read.
        task = kanban.create(title="OPS-1", body=f"Linear issue {issue.get('identifier') or ISSUE}: {issue.get('url')}\n")
        store.bind_native(ISSUE, task.id, "synthetic", "default", owner_ref=SESSION, at=0)
        task.status = "blocked"
        kanban.histories[task.id] = [SimpleNamespace(id=1, kind="blocked", created_at=0, payload={"reason": "x"})]
        with store._tx() as db:
            db.execute("UPDATE work SET last_event_id=1 WHERE issue_id=?", (ISSUE,))
            db.execute("INSERT INTO outbox (id, kind, payload, next_at, state, attempts) VALUES ('t1', 'status', ?, ?, "
                       "'failed', 1)", (json.dumps({"issue_id": ISSUE, "task_id": task.id, "state": "done",
                                                    "terminal": True, "write_started": True, "enqueued_at": 0}), 1e12))
        return bridge, authority, store, task

    def test_held_status_reconcile_makes_no_linear_reads(self):
        """An uncertain closeout held for an operator must not cost a read every 2 s tick (was ~1,800/h)."""
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, authority, store, _ = self.blocked_task_with_uncertain_closeout(directory, clock)
            self.assertFalse(store.effect_admitted(ISSUE))
            before = len(authority.requests)
            for _ in range(1800):  # one hour of 2 s ticks
                bridge.pump_kanban()
                clock.now += 2
            self.assertEqual(len(authority.requests) - before, 0)

    def test_refused_status_reconcile_backs_off_after_its_read(self):
        """A store refusal after the issue read raises, so the persisted pump backoff applies."""
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, authority, store, _ = self.blocked_task_with_uncertain_closeout(directory, clock)
            with store._tx() as db:
                db.execute("DELETE FROM outbox WHERE id='t1'")  # admitted now; the work row still binds
            self.assertTrue(store.effect_admitted(ISSUE))
            store.reconcile_status = lambda issue_id, payload, at: False  # e.g. the work row moved underneath us
            before = len(authority.requests)
            for _ in range(1800):
                bridge.pump_kanban()
                clock.now += 2
            self.assertGreater(len(authority.requests) - before, 0)
            self.assertLess(len(authority.requests) - before, 12)  # 15 s doubling to 1 h, not every tick

    def test_spent_budget_defers_reads_but_queued_writes_still_go_out(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, authority, store, _ = self.bridge(directory, clock)
            bridge.api.clock = clock
            bridge.api.budget = {"requests": {"remaining": 100, "limit": 5000, "reset": clock() + 600, "at": clock()}}
            row = store.enqueue("comment", {"issue_id": ISSUE, "body": "Done here.", "content_action": True,
                                            "unlinked_content": True}, at=clock())
            reads = []
            def pump():
                reads.append(1)
                bridge.api.issue(ISSUE)  # a normal read at 2% left
            bridge.pump_kanban = pump
            bridge.tick()
            self.assertEqual(reads, [1])
            self.assertEqual(store.outbox_row(row)["state"], "sent")
            self.assertTrue(any("commentCreate" in query for query in authority.mutations))

    def test_unconfirmed_stop_reply_posts_once_per_activity(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, _, store, kanban = self.bridge(directory, clock)
            task = kanban.create(title="OPS-1: native task")
            store.bind_native(ISSUE, task.id, "synthetic", "default", owner_ref=SESSION, at=0)
            for _ in range(50):
                bridge._decline(ISSUE, SESSION, "Stop is not confirmed yet.", activity_id="activity-1")
            bridge._decline(ISSUE, SESSION, "Stop is not confirmed yet.", activity_id="activity-2")
            replies = [row for row in store.pending(ISSUE) if row["kind"] == "activity"]
            self.assertEqual(len(replies), 2)


class SpecialistPumpTests(unittest.TestCase):
    """A specialist-scope profile proves scope per row, not per row per 2 s tick."""

    def bridge(self, directory, clock, rows=3):
        authority = Authority()
        store, kanban = Store(Path(directory) / "state.db"), FakeKanban()
        bridge = Bridge(store, authority.api(), kanban, profile="synthetic", clock=clock)
        issues = [ISSUE] + [f"issue-{index}" for index in range(1, rows)]
        for issue_id in issues:
            if issue_id != ISSUE:
                authority.issues[issue_id] = issue_record(id=issue_id)
            authority.issues[issue_id]["delegate"] = {"id": VIEWER}
            task_id = f"task-{issue_id}"
            kanban.tasks[task_id] = SimpleNamespace(id=task_id, status="ready", completion_contract=None, result="",
                                                    title="OPS-1: task", current_run_id=None, assignee="synthetic")
            kanban.histories[task_id] = []
            store.bind_native(issue_id, task_id, "synthetic", "default", owner_ref=SESSION, project_id=PROJECT, at=0)
        return bridge, authority, issues

    @staticmethod
    def issue_reads(authority):
        return sum(1 for query in authority.requests if "query Issue(" in query)

    def test_idle_rows_cost_one_read_each_not_one_per_tick(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, authority, issues = self.bridge(directory, clock)
            for _ in range(140):  # 280 s of 2 s ticks, inside one proof lifetime
                bridge.pump_kanban()
                clock.now += 2
            self.assertEqual(self.issue_reads(authority), len(issues))  # was 3 x 140 = 420
            for _ in range(1800 - 140):  # the rest of the hour
                bridge.pump_kanban()
                clock.now += 2
            # One re-proof per row every 5 minutes: about 12/h per row instead of 1,800/h.
            self.assertLessEqual(self.issue_reads(authority), len(issues) * 13)

    def test_issue_webhook_drops_the_cached_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, authority, issues = self.bridge(directory, clock, rows=1)
            bridge.pump_kanban()
            clock.now += 2
            bridge._mark_dirty(ISSUE)  # what a tracked Issue webhook from a person does
            before = self.issue_reads(authority)
            bridge.pump_kanban()
            self.assertEqual(self.issue_reads(authority) - before, 1)

    def test_refused_scope_backs_off_instead_of_reading_every_tick(self):
        with tempfile.TemporaryDirectory() as directory:
            clock = Clock()
            bridge, authority, issues = self.bridge(directory, clock, rows=1)
            authority.fail_issue = True  # Linear keeps answering 503 for the issue
            for _ in range(1800):  # one hour of 2 s ticks
                bridge.pump_kanban()
                clock.now += 2
            self.assertLess(self.issue_reads(authority), 12)  # was 1,800
            authority.fail_issue = False
            clock.now += 3600 * 1.2 + 1  # past the longest jittered wait (1 h cap, +20%)
            bridge.pump_kanban()
            self.assertFalse(bridge.store.scope_fenced(ISSUE))  # a transient refusal never fences
            self.assertIn(ISSUE, bridge._pump_authorized)  # and it is admitted again once Linear answers


if __name__ == "__main__":
    unittest.main()
