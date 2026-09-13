"""Safe Linear work-tracking unit tests; transport is entirely fake."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins" / "linear-agent"))

sys.modules.pop("linear_tracking", None)
import linear_tracking
from linear_guard_health import WorkerGuardHealth
from linear_tracking import (
    LinearTracking,
    TrackingError,
    _active_config,
    _configured_tracker,
    _local_tracker,
    _tracking_config,
    main,
)


_UNSET = object()


class FakeLinear:
    def __init__(self, *, delegate_id=None, state_type="started", update_success=True, readback_delegate=None, payload_delegate=_UNSET, readback_description=None, archived_at=None, issue_id="123e4567-e89b-42d3-a456-426614174001", viewer_app=True, viewer_org="org-1", issue_org="org-1", project_team_ids=("team-1",), project_response=None):
        self.delegate_id, self.state_type = delegate_id, state_type
        self.update_success, self.readback_delegate = update_success, readback_delegate
        self.payload_delegate = payload_delegate
        self.readback_description = readback_description
        self.archived_at, self.issue_id = archived_at, issue_id
        self.viewer_app, self.viewer_org, self.issue_org = viewer_app, viewer_org, issue_org
        self.project_team_ids = project_team_ids
        self.project_response = project_response
        self.calls, self.applied = [], {}

    def __call__(self, query, variables):
        self.calls.append((query, variables))
        if "Viewer" in query:
            return {"data": {"viewer": {"id": "app-1", "app": self.viewer_app, "organization": {"id": self.viewer_org}}}}
        if "IssueLookup" in query:
            lookup_count = sum("IssueLookup" in prior for prior, _ in self.calls)
            delegate = self.delegate_id if lookup_count == 1 else self.readback_delegate
            created = next((value["input"] for prior, value in self.calls if "IssueCreate" in prior), None)
            if created is not None:
                delegate_id = self.applied.get("delegateId")
                state_id = self.applied.get("stateId")
                state = {"id": state_id, "name": "In Progress", "type": "started"} if state_id else {"id": "state-todo", "name": "Todo", "type": "unstarted"}
                return {"data": {"issue": {"id": variables["id"], "title": created["title"], "description": self.readback_description if self.readback_description is not None else created["description"], "team": {"id": created["teamId"], "organization": {"id": self.issue_org}, "states": {"nodes": [{"id": "state-todo", "name": "Todo", "type": "unstarted"}, {"id": "state-progress", "name": "In Progress", "type": "started"}, {"id": "state-done", "name": "Done", "type": "completed"}, {"id": "state-canceled", "name": "Canceled", "type": "canceled"}]}}, "parent": ({"id": created["parentId"]} if "parentId" in created else None), "project": ({"id": created["projectId"]} if "projectId" in created else None), "assignee": None, "delegate": ({"id": delegate_id} if delegate_id else None), "archivedAt": None, "state": state}}}
            state = None if self.state_type is None else {"id": {"unstarted": "state-todo", "started": "state-progress", "completed": "state-done", "canceled": "state-canceled"}.get(self.state_type, "state-other"), "name": {"unstarted": "Todo", "started": "In Progress", "completed": "Done", "canceled": "Canceled"}.get(self.state_type, "Other"), "type": self.state_type}
            if lookup_count > 1 and "stateId" in self.applied:
                applied = self.applied["stateId"]
                state = {"id": applied, "name": "In Progress", "type": "started"}
            return {"data": {"issue": {"id": self.issue_id, "archivedAt": self.archived_at, "team": {"id": "team-1", "organization": {"id": self.issue_org}, "states": {"nodes": [{"id": "state-todo", "name": "Todo", "type": "unstarted"}, {"id": "state-progress", "name": "In Progress", "type": "started"}, {"id": "state-done", "name": "Done", "type": "completed"}, {"id": "state-canceled", "name": "Canceled", "type": "canceled"}]}}, "assignee": {"id": "human-1"}, "delegate": ({"id": delegate} if delegate else None), "state": state}}}
        if "ProjectLookup" in query:
            if self.project_response is not None:
                return self.project_response
            return {"data": {"project": {"id": variables["id"], "archivedAt": None, "teams": {"nodes": [{"id": team_id} for team_id in self.project_team_ids], "pageInfo": {"hasNextPage": False}}}}}
        if "IssueUpdate" in query:
            if self.update_success:
                self.applied.update(variables["input"])
            delegate = self.applied.get("delegateId") if self.payload_delegate is _UNSET else self.payload_delegate
            state_id = self.applied.get("stateId", "state-progress")
            assignee = None if any("IssueCreate" in prior for prior, _ in self.calls) else {"id": "human-1"}
            return {"data": {"issueUpdate": {"success": self.update_success, "issue": {
                "id": self.issue_id,
                "assignee": assignee,
                "delegate": ({"id": delegate} if delegate else None),
                "state": {"id": state_id, "name": "In Progress", "type": "started"},
            }}}}
        if "IssueCreate" in query:
            return {"data": {"issueCreate": {"success": True, "issue": {"id": variables["input"]["id"]}}}}
        raise AssertionError(query)


class LinearTrackingTests(unittest.TestCase):
    def _tracking(self, root, api, *, ready=True):
        db = root / "state.db"
        health = WorkerGuardHealth(db, profile="alpha", workspace="demo", ttl_seconds=60)
        if ready:
            health.publish(now=1000)
        return LinearTracking(db, profile="alpha", workspace="demo", owner_session_id="chat-1", graphql=api, health=health, clock=lambda: 1001)

    def test_create_requires_caller_uuid_and_reads_back_exact_fields(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear()
            issue_id = "123e4567-e89b-42d3-a456-426614174000"
            with self.assertRaisesRegex(TrackingError, "--issue-id"):
                self._tracking(Path(temp), api).create(title="Title", description="Body", team_id="team-1")
            result = self._tracking(Path(temp), api).create(title="Title", description="Body", team_id="team-1", parent_id="parent-1", issue_id=issue_id, team_only_maintenance=True)
            self.assertEqual(result["issue_id"], issue_id)
            payload = next(variables for query, variables in api.calls if "IssueCreate" in query)
            self.assertEqual(payload["input"]["id"], issue_id)
            self.assertEqual(payload["input"]["parentId"], "parent-1")
            self.assertEqual(payload["input"]["assigneeId"], None)
            self.assertEqual(payload["input"]["delegateId"], None)
            self.assertTrue(any("IssueLookup" in query for query, _ in api.calls))

    def test_create_with_ready_worker_claims_after_undelegated_create(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear(readback_delegate="app-1")
            issue_id = "123e4567-e89b-42d3-a456-426614174000"
            result = self._tracking(Path(temp), api).create(
                title="Title", description="Body", team_id="team-1",
                issue_id=issue_id, team_only_maintenance=True,
            )
            self.assertEqual(result["status"], "claimed")
            create = next(variables["input"] for query, variables in api.calls if "IssueCreate" in query)
            self.assertIsNone(create["assigneeId"])
            self.assertIsNone(create["delegateId"])
            update = next(variables["input"] for query, variables in api.calls if "IssueUpdate" in query)
            self.assertEqual(update["delegateId"], "app-1")
            self.assertNotIn("assigneeId", update)
            create_at = next(i for i, call in enumerate(api.calls) if "IssueCreate" in call[0])
            update_at = next(i for i, call in enumerate(api.calls) if "IssueUpdate" in call[0])
            self.assertGreater(update_at, create_at)

    def test_create_without_ready_worker_stays_undelegated(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear()
            result = self._tracking(Path(temp), api, ready=False).create(
                title="Title", description="Body", team_id="team-1",
                issue_id="123e4567-e89b-42d3-a456-426614174000",
                team_only_maintenance=True,
            )
            self.assertEqual(result["status"], "created")
            self.assertFalse(any("IssueUpdate" in query for query, _ in api.calls))

    def test_create_child_stays_undelegated(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear()
            result = self._tracking(Path(temp), api).create(
                title="Title", description="Body", team_id="team-1",
                parent_id="123e4567-e89b-42d3-a456-426614174099",
                issue_id="123e4567-e89b-42d3-a456-426614174000",
                team_only_maintenance=True,
            )
            self.assertEqual(result["status"], "created")
            self.assertFalse(any("IssueUpdate" in query for query, _ in api.calls))

    def test_create_requires_explicit_project_or_team_only_maintenance_intent(self):
        with tempfile.TemporaryDirectory() as temp:
            tracking = self._tracking(Path(temp), FakeLinear())
            with self.assertRaisesRegex(TrackingError, "project|maintenance"):
                tracking.create(title="Title", description="Body", team_id="team-1", issue_id="123e4567-e89b-42d3-a456-426614174000")
            result = tracking.create(title="Title", description="Body", team_id="team-1", issue_id="123e4567-e89b-42d3-a456-426614174000", team_only_maintenance=True)
            self.assertEqual(result["status"], "claimed")

    def test_create_refuses_project_outside_selected_team_before_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear(project_team_ids=("other-team",))
            with self.assertRaisesRegex(TrackingError, "project.*team"):
                self._tracking(Path(temp), api).create(title="Title", description="Body", team_id="team-1", project_id="project-1", issue_id="123e4567-e89b-42d3-a456-426614174000")
            self.assertFalse(any("IssueCreate" in query for query, _ in api.calls))

    def test_create_refuses_mixed_valid_and_malformed_project_team_nodes_before_mutation(self):
        malformed_nodes = (
            None,
            "team-2",
            {},
            {"id": None},
            {"id": ""},
            {"id": "  "},
            {"id": ["unhashable"]},
        )
        for malformed in malformed_nodes:
            with self.subTest(malformed=malformed), tempfile.TemporaryDirectory() as temp:
                response = {
                    "data": {
                        "project": {
                            "id": "project-1",
                            "archivedAt": None,
                            "teams": {
                                "nodes": [{"id": "team-1"}, malformed],
                                "pageInfo": {"hasNextPage": False},
                            },
                        },
                    },
                }
                api = FakeLinear(project_response=response)
                with self.assertRaises(TrackingError):
                    self._tracking(Path(temp), api).create(
                        title="Title", description="Body", team_id="team-1",
                        project_id="project-1", issue_id="123e4567-e89b-42d3-a456-426614174000",
                    )
                self.assertFalse(any("IssueCreate" in query for query, _ in api.calls))

    def test_create_refuses_archived_or_partial_project_response_before_mutation(self):
        invalid_responses = (
            {"data": {"project": {"id": "project-1", "archivedAt": "2026-09-08T00:00:00.000Z", "teams": {"nodes": [{"id": "team-1"}], "pageInfo": {"hasNextPage": False}}}}},
            {"data": {"project": {"id": "project-1", "archivedAt": None, "teams": {"nodes": [{"id": "team-1"}]}}}},
            {"data": {"project": {"id": "project-1", "archivedAt": None, "teams": {"nodes": [{"id": "team-1"}], "pageInfo": {"hasNextPage": True}}}}},
        )
        for response in invalid_responses:
            with self.subTest(response=response), tempfile.TemporaryDirectory() as temp:
                api = FakeLinear(project_response=response)
                with self.assertRaisesRegex(TrackingError, "project.*team"):
                    self._tracking(Path(temp), api).create(title="Title", description="Body", team_id="team-1", project_id="project-1", issue_id="123e4567-e89b-42d3-a456-426614174000")
                self.assertFalse(any("IssueCreate" in query for query, _ in api.calls))

    def test_create_validates_selected_project_before_creating(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear()
            result = self._tracking(Path(temp), api).create(title="Title", description="Body", team_id="team-1", project_id="project-1", issue_id="123e4567-e89b-42d3-a456-426614174000")
            self.assertEqual(result["status"], "claimed")
            self.assertLess(
                next(index for index, call in enumerate(api.calls) if "ProjectLookup" in call[0]),
                next(index for index, call in enumerate(api.calls) if "IssueCreate" in call[0]),
            )

    def test_create_rejects_mutually_exclusive_project_and_team_only_intent_before_lookup(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear()
            with self.assertRaisesRegex(TrackingError, "cannot be combined"):
                self._tracking(Path(temp), api).create(title="Title", description="Body", team_id="team-1", project_id="project-1", issue_id="123e4567-e89b-42d3-a456-426614174000", team_only_maintenance=True)
            self.assertFalse(any("ProjectLookup" in query or "IssueCreate" in query for query, _ in api.calls))

    def test_create_accepts_linear_heading_spacing_and_bullet_canonicalization(self):
        with tempfile.TemporaryDirectory() as temp:
            description = "## Outcome\nTitle\n\n## Acceptance\n- Retain this acceptance criterion\n- Preserve this prose"
            canonical = "## Outcome\n\nTitle\n\n## Acceptance\n\n* Retain this acceptance criterion\n* Preserve this prose"
            api = FakeLinear(readback_description=canonical)
            result = self._tracking(Path(temp), api).create(
                title="Title", description=description, team_id="team-1",
                issue_id="123e4567-e89b-42d3-a456-426614174000", team_only_maintenance=True,
            )
            self.assertEqual(result["status"], "claimed")

    def test_create_rejects_changed_acceptance_content_after_markdown_canonicalization(self):
        with tempfile.TemporaryDirectory() as temp:
            description = "## Acceptance\n- Retain this acceptance criterion"
            altered = "## Acceptance\n\n* Replace this acceptance criterion"
            api = FakeLinear(readback_description=altered)
            with self.assertRaisesRegex(TrackingError, "readback mismatch"):
                self._tracking(Path(temp), api).create(
                    title="Title", description=description, team_id="team-1",
                    issue_id="123e4567-e89b-42d3-a456-426614174000", team_only_maintenance=True,
                )

    def test_create_rejects_bullet_rewrite_inside_fenced_code(self):
        with tempfile.TemporaryDirectory() as temp:
            description = "## Evidence\n```text\n* literal example\n```"
            altered = "## Evidence\n\n```text\n- literal example\n```"
            api = FakeLinear(readback_description=altered)
            with self.assertRaisesRegex(TrackingError, "readback mismatch"):
                self._tracking(Path(temp), api).create(
                    title="Title", description=description, team_id="team-1",
                    issue_id="123e4567-e89b-42d3-a456-426614174000", team_only_maintenance=True,
                )

    def test_markdown_readback_rejects_changed_literal_in_longer_fence(self):
        expected = "````text\n```\n* literal\n````\n"
        actual = "````text\n```\n- literal\n````\n"
        self.assertFalse(LinearTracking._markdown_readback_equivalent(actual, expected))

    def test_claim_persists_before_delegate_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear(readback_delegate="app-1")
            result = self._tracking(Path(temp), api).claim("ENG-1")
            self.assertEqual(result["status"], "claimed")
            update_at = next(i for i, call in enumerate(api.calls) if "IssueUpdate" in call[0])
            self.assertIsNotNone(self._tracking(Path(temp), api).ownership.get("123e4567-e89b-42d3-a456-426614174001"))
            self.assertGreater(update_at, 1)
            update = api.calls[update_at][1]["input"]
            self.assertEqual(update, {"delegateId": "app-1"})
            self.assertNotIn("assigneeId", update)

    def test_claim_sets_in_progress_without_touching_assignee(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear(state_type="unstarted", readback_delegate="app-1")
            result = self._tracking(Path(temp), api).claim("ENG-1")
            self.assertEqual(result["status"], "claimed")
            update = next(variables["input"] for query, variables in api.calls if "mutation IssueUpdate" in query)
            self.assertEqual(update["delegateId"], "app-1")
            self.assertEqual(update["stateId"], "state-progress")
            self.assertNotIn("assigneeId", update)
            lookups = [variables for query, variables in api.calls if "query IssueLookup" in query]
            self.assertEqual(len(lookups), 1)
            update_query = next(query for query, _ in api.calls if "mutation IssueUpdate" in query)
            self.assertIn("issue { id assignee { id } delegate { id } state { id name type } }", update_query)

    def test_takeover_replaces_foreign_delegate_without_assignee(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = self._tracking(root, FakeLinear(readback_delegate="app-1"))
            claimed = first.claim("ENG-1")
            api = FakeLinear(delegate_id="other-app", state_type="unstarted", readback_delegate="app-1")
            second = LinearTracking(
                root / "state.db",
                profile="alpha",
                workspace="demo",
                owner_session_id="chat-2",
                graphql=api,
                health=first.health,
                clock=lambda: 1001,
            )
            taken = second.takeover("ENG-1")
            self.assertEqual(taken["status"], "taken_over")
            self.assertNotEqual(taken["generation"], claimed["generation"])
            update = next(variables["input"] for query, variables in api.calls if "mutation IssueUpdate" in query)
            self.assertEqual(update["delegateId"], "app-1")
            self.assertEqual(update["stateId"], "state-progress")
            self.assertNotIn("assigneeId", update)
            self.assertEqual(second.ownership.get(claimed["issue_id"]).owner_session_id, "chat-2")

    def test_foreign_delegate_conflict_never_mutates(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear(delegate_id="other-app")
            with self.assertRaisesRegex(TrackingError, "foreign"):
                self._tracking(Path(temp), api).claim("ENG-1")
            self.assertFalse(any("IssueUpdate" in query for query, _ in api.calls))

    def test_claim_rejects_noncanonical_archived_or_stateless_issue_before_mutation(self):
        for api in (
            FakeLinear(issue_id="ENG-1"),
            FakeLinear(archived_at="2026-01-01T00:00:00.000Z"),
            FakeLinear(state_type=None),  # type: ignore[arg-type]
        ):
            with self.subTest(api=api), tempfile.TemporaryDirectory() as temp:
                with self.assertRaisesRegex(TrackingError, "canonical|archived|state"):
                    self._tracking(Path(temp), api).claim("ENG-1")
                self.assertFalse(any("IssueUpdate" in query for query, _ in api.calls))

    def test_claim_requires_app_viewer_in_same_organization(self):
        for api in (FakeLinear(viewer_app=False), FakeLinear(issue_org="other-org")):
            with self.subTest(api=api), tempfile.TemporaryDirectory() as temp:
                with self.assertRaisesRegex(TrackingError, "app|organization"):
                    self._tracking(Path(temp), api).claim("ENG-1")
                self.assertFalse(any("IssueUpdate" in query for query, _ in api.calls))

        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear()
            with self.assertRaisesRegex(TrackingError, "ready"):
                self._tracking(Path(temp), api, ready=False).claim("ENG-1")
            self.assertEqual(api.calls, [])

    def test_claim_uses_schema_valid_app_and_team_organization_query_shape(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear(readback_delegate="app-1")
            self._tracking(Path(temp), api).claim("ENG-1")
            viewer_query = next(query for query, _ in api.calls if "Viewer" in query)
            issue_query = next(query for query, _ in api.calls if "IssueLookup" in query)
            self.assertIn("viewer { id app organization { id } }", viewer_query)
            self.assertIn("team { id organization { id } states { nodes { id name type } } }", issue_query)
            self.assertIn("state { id name type }", issue_query)

    def test_readback_mismatch_releases_claim_without_fencing(self):
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear(payload_delegate=None)
            tracking = self._tracking(Path(temp), api)
            with self.assertRaisesRegex(TrackingError, r"readback mismatch: expected 'app-1', got None"):
                tracking.claim("ENG-1")
            record = tracking.ownership.get("123e4567-e89b-42d3-a456-426614174001")
            self.assertIsNotNone(record)
            self.assertEqual(record.mode, "released")

    def test_claim_accepts_delegate_from_issue_update_payload_when_lookup_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            api = FakeLinear(readback_delegate=None)
            tracking = self._tracking(Path(temp), api)
            result = tracking.claim("ENG-1")
            self.assertEqual(result["status"], "claimed")
            record = tracking.ownership.get("123e4567-e89b-42d3-a456-426614174001")
            self.assertIsNotNone(record)
            self.assertEqual(record.mode, "active")
            self.assertEqual(sum("query IssueLookup" in query for query, _ in api.calls), 1)

    def test_update_timeout_keeps_claim_for_reconciliation(self):
        class TimeoutLinear(FakeLinear):
            def __call__(self, query, variables):
                if "IssueUpdate" in query:
                    raise TimeoutError("ambiguous")
                return super().__call__(query, variables)
        with tempfile.TemporaryDirectory() as temp:
            tracking = self._tracking(Path(temp), TimeoutLinear())
            with self.assertRaises(TimeoutError):
                tracking.claim("ENG-1")
            self.assertEqual(tracking.ownership.get("123e4567-e89b-42d3-a456-426614174001").mode, "reconcile")


class LinearTrackingCliTests(unittest.TestCase):
    @staticmethod
    def _write_policy(home: Path, *, vault_id: str, item_id: str) -> None:
        policy = home / "linear-agents.json"
        policy.write_text(
            json.dumps(
                {
                    "agents": [
                        {
                            "profile": "alpha",
                            "workspace": "demo-space",
                            "oauth": {"vault_id": vault_id, "item_id": item_id},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        policy.chmod(0o444)

    def test_configured_tracker_builds_oauth_from_profile_home_and_binding(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            (home / "secrets").mkdir(mode=0o700)
            state = home / "linear-agent" / "state.db"
            state.parent.mkdir(mode=0o700)
            vault_id, item_id = "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb"
            self._write_policy(home, vault_id=vault_id, item_id=item_id)
            entry = {"credential_mode": "managed_oauth_v1", "oauth_file": str(home / "secrets" / "linear-oauth.json"), "connect_env_file": str(home / ".op.env"), "oauth_vault_id": vault_id, "oauth_item_id": item_id}
            oauth = object()
            client = mock.Mock(_graphql=mock.Mock())
            resolve_binding = mock.Mock(return_value={"viewer_id": "v", "organization_id": "o"})
            factory = mock.Mock(return_value=oauth)
            client_factory = mock.Mock(return_value=client)
            with (
                mock.patch.dict(
                    _configured_tracker.__globals__,
                    {
                        "_tracking_config": mock.Mock(return_value=(home, entry, "alpha", "demo-space", state)),
                        "_publisher_binding": resolve_binding,
                        "make_oauth": factory,
                        "LinearActivityClient": client_factory,
                    },
                ),
                mock.patch("linear_policy.AGENT_POLICY_PATH", home / "linear-agents.json"),
                mock.patch(
                    "linear_policy.read_agent_policy",
                    return_value=json.loads((home / "linear-agents.json").read_text(encoding="utf-8")),
                ),
                mock.patch.dict(os.environ, {"HERMES_SESSION_ID": "chat-1"}, clear=False),
            ):
                _configured_tracker()
            resolve_binding.assert_called_once_with("alpha", "demo-space", vault_id, item_id)
            factory.assert_called_once_with("alpha", home, vault_id, item_id)
            client_factory.assert_called_once_with(oauth)
            client.verify_authenticated.assert_called_once_with()

    def test_configured_tracker_rejects_publisher_binding_before_connect_credentials(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            (home / "secrets").mkdir(mode=0o700)
            state = home / "linear-agent" / "state.db"
            state.parent.mkdir(mode=0o700)
            vault_id, item_id = "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb"
            self._write_policy(home, vault_id=vault_id, item_id=item_id)
            entry = {"credential_mode": "managed_oauth_v1", "oauth_file": str(home / "secrets" / "linear-oauth.json"), "connect_env_file": str(home / ".op.env"), "oauth_vault_id": vault_id, "oauth_item_id": item_id}
            with (
                mock.patch.dict(
                    _configured_tracker.__globals__,
                    {
                        "_tracking_config": mock.Mock(return_value=(home, entry, "alpha", "demo-space", state)),
                        "_publisher_binding": mock.Mock(side_effect=TrackingError("binding refused")),
                        "make_oauth": mock.Mock(side_effect=AssertionError("credentials read before binding")),
                    },
                ),
                mock.patch("linear_policy.AGENT_POLICY_PATH", home / "linear-agents.json"),
                mock.patch(
                    "linear_policy.read_agent_policy",
                    return_value=json.loads((home / "linear-agents.json").read_text(encoding="utf-8")),
                ),
                mock.patch.dict(os.environ, {"HERMES_SESSION_ID": "chat-1"}, clear=False),
            ):
                with self.assertRaisesRegex(TrackingError, "binding refused"):
                    _configured_tracker()

    def test_active_config_reads_enabled_nested_settings(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            (home / "config.yaml").write_text("""plugins:
  enabled: [linear-agent]
  entries:
    linear-agent:
      settings:
        enabled: true
        dry_run: false
        profile: alpha
""", encoding="utf-8")
            with mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "HERMES_SESSION_ID": "chat-1", "HERMES_SESSION_PROFILE": "alpha"}, clear=False):
                active_home, settings = _active_config()
            self.assertEqual(active_home, home)
            self.assertEqual(settings["profile"], "alpha")

    def test_active_config_rejects_default_or_mismatched_runtime_profile(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            (home / "config.yaml").write_text("""plugins:
  enabled: [linear-agent]
  entries:
    linear-agent:
      settings:
        enabled: true
        dry_run: false
        profile: alpha
""", encoding="utf-8")
            for runtime_profile in ("default", "other", ""):
                with (
                    self.subTest(runtime_profile=runtime_profile),
                    mock.patch.dict(os.environ, {"HERMES_HOME": str(home), "HERMES_SESSION_ID": "chat-1", "HERMES_SESSION_PROFILE": runtime_profile}, clear=False),
                    self.assertRaisesRegex(TrackingError, "profile"),
                ):
                    _active_config()

    def test_symlinked_home_and_missing_state_parent_are_tolerated(self):
        """Directory-shape drift warns and self-heals; only the token file and the
        database path keep hard refusals (see the test below and the OAuth suite).

        A symlinked HERMES_HOME or an absent private state directory used to refuse
        construction outright, which turned an ordinary chown/relocation into an
        operator incident. The tracker now creates the state directory on use and
        surfaces real problems through the worker guard instead.
        """
        for use_symlink in (True, False):
            with self.subTest(symlinked_home=use_symlink), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                real_home = root / "real-home"
                real_home.mkdir(mode=0o700)
                home = root / "symlink-home"
                if use_symlink:
                    home.symlink_to(real_home, target_is_directory=True)
                else:
                    home = real_home
                settings = {"profile": "alpha", "workspace": "demo", "state_database": str(home / "linear-agent" / "state.db")}
                self.assertFalse((real_home / "linear-agent").exists())
                with mock.patch.dict(
                    _tracking_config.__globals__, {"_active_config": mock.Mock(return_value=(home, settings))}
                ), mock.patch.dict(os.environ, {"HERMES_SESSION_ID": "chat-1"}, clear=False):
                    tracker = _local_tracker()
                self.assertEqual(tracker.status("123e4567-e89b-42d3-a456-426614174001"), {"local": None})
                self.assertTrue((real_home / "linear-agent").is_dir())

    def test_local_and_remote_tracker_reject_database_symlink(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            state_dir = home / "linear-agent"
            state_dir.mkdir(mode=0o700)
            target = home / "other.db"
            target.touch(mode=0o600)
            (state_dir / "state.db").symlink_to(target)
            settings = {"profile": "alpha", "workspace": "demo", "state_database": str(state_dir / "state.db")}
            with mock.patch.dict(
                _tracking_config.__globals__, {"_active_config": mock.Mock(return_value=(home, settings))}
            ):
                with self.assertRaisesRegex(TrackingError, "private state paths"):
                    _tracking_config()

    def test_status_is_local_and_does_not_construct_oauth_tracker(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = LinearTracking(Path(temp) / "state.db", profile="alpha", workspace="demo", owner_session_id="chat-1", graphql=FakeLinear(), health=mock.Mock())
            local_factory = mock.Mock(return_value=tracker)
            oauth_factory = mock.Mock(side_effect=AssertionError("OAuth must not be used"))
            with mock.patch.dict(main.__globals__, {
                "_local_tracker": local_factory,
                "_configured_tracker": oauth_factory,
            }):
                self.assertEqual(main(["status", "--issue", "123e4567-e89b-42d3-a456-426614174001"]), 0)
            local_factory.assert_called_once_with()
            oauth_factory.assert_not_called()
