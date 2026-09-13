"""Native project-update publisher tests. All Linear traffic is fake."""
from __future__ import annotations

import sys
import io
import json
import tempfile
from unittest import mock
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins" / "linear-agent"))

import linear_project_updates
from linear_project_updates import LinearProjectUpdatePublisher, NoProjectUpdate, ProjectUpdateError, _publisher_binding, main, publish_session_updates


class FakeLinear:
    def __init__(self, *, timeout_create: bool = False, altered_body: str | None = None):
        self.timeout_create = timeout_create
        self.altered_body = altered_body
        self.health = 'atRisk'
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.updates: dict[str, dict[str, object]] = {}
        self.issues = {
            "TEAM-501": {"id": "f7a2e3f4-7ea8-4de5-a45f-03a1f499f371", "identifier": "TEAM-501", "url": "https://linear.app/acme/issue/TEAM-501", "project": {"id": "project-a"}, "team": {"organization": {"id": "workspace-a"}}},
            "TEAM-502": {"id": "59ec210b-4b1e-4767-a84b-2d8c7d13383b", "identifier": "TEAM-502", "url": "https://linear.app/acme/issue/TEAM-502", "project": {"id": "project-b"}, "team": {"organization": {"id": "workspace-a"}}},
        }

    def __call__(self, query: str, variables: dict[str, object]) -> dict[str, object]:
        self.calls.append((query, variables))
        if "ProjectUpdatesViewer" in query:
            return {"data": {"viewer": {"id": "actor-a", "app": True, "organization": {"id": "workspace-a"}}}}
        if "ProjectUpdateIssue" in query:
            return {"data": {"issue": self.issues.get(variables["id"])}}
        if "ProjectUpdateLookup" in query:
            return {"data": {"projectUpdate": self.updates.get(variables["id"])}}
        if "ProjectUpdateHealth" in query:
            return {"data": {"project": {"id": variables['id'], "health": self.health}}}
        if "ProjectUpdateCreate" in query:
            input_ = variables["input"]
            assert isinstance(input_, dict)
            created = {"id": input_["id"], "body": self.altered_body if self.altered_body is not None else input_["body"], "project": {"id": input_["projectId"]}, "user": {"id": "actor-a"}, "url": f"https://linear.app/update/{input_['id']}"}
            self.updates[input_["id"]] = created
            created['health'] = input_.get('health', 'onTrack')
            if self.timeout_create:
                raise TimeoutError("ambiguous")
            return {"data": {"projectUpdateCreate": {"success": True, "projectUpdate": created}}}
        raise AssertionError(query)


class ProjectUpdatePublisherTests(unittest.TestCase):
    def publisher(self, api: FakeLinear) -> LinearProjectUpdatePublisher:
        return LinearProjectUpdatePublisher(api, configured_workspace="workspace-a")

    def test_publishes_one_native_update_per_resolved_project_with_deterministic_ids(self) -> None:
        api = FakeLinear()
        result = self.publisher(api).publish(
            session_key="chat:run-1", issue_ids=["TEAM-501", "TEAM-502"],
            project_summaries={"project-a": "Finished only A.", "project-b": "Finished only B."},
        )
        self.assertEqual([item["project_id"] for item in result], ["project-a", "project-b"])
        creates = [variables["input"] for query, variables in api.calls if "ProjectUpdateCreate" in query]
        self.assertEqual(len(creates), 2)
        self.assertTrue(all(item.get('health') == 'atRisk' for item in creates))
        self.assertEqual(creates[0]["body"], "Finished only A.")
        self.assertEqual(creates[1]["body"], "Finished only B.")
        self.assertTrue(all("commentCreate" not in query for query, _ in api.calls))
        self.assertEqual(result[0]["authored_by"], "actor-a")
        self.assertTrue(result[0]["url"].startswith("https://linear.app/"))

    def test_uuid_source_is_preserved_and_uses_live_identifier(self) -> None:
        api = FakeLinear()
        source = '9e3fe8a4-8602-401a-8b34-8d0f1c047068'
        api.issues[source] = {**api.issues['TEAM-501'], 'id': source}
        result = publish_session_updates(api, 'uuid-test', [{'issue_id': source, 'summary': 'Work.'}])
        self.assertEqual(len(result), 1)
        body = next(iter(api.updates.values()))['body']
        self.assertIn('[TEAM-501]', body)

    def test_missing_health_and_wrong_actor_fail_before_create(self) -> None:
        api = FakeLinear()
        api.health = None
        with self.assertRaises(ProjectUpdateError):
            self.publisher(api).publish(session_key='health', issue_ids=['TEAM-501'], project_summaries={'project-a': 'Work.'})
        self.assertFalse(api.updates)
        with self.assertRaises(ProjectUpdateError):
            LinearProjectUpdatePublisher(FakeLinear(), configured_actor='wrong').publish(session_key='actor', issue_ids=['TEAM-501'], project_summaries={'project-a': 'Work.'})

    def test_unprojected_source_does_not_suppress_other_project_updates(self) -> None:
        api = FakeLinear()
        api.issues['TEAM-502']['project'] = None
        results = publish_session_updates(api, 'mixed', [
            {'issue_id': 'TEAM-501', 'summary': 'Valid project work.'},
            {'issue_id': 'TEAM-502', 'summary': 'No project.'}])
        self.assertEqual(len(api.updates), 1)
        self.assertTrue(any(row.get('status') == 'skipped_no_project' and row.get('issue_id') == 'TEAM-502' for row in results))

    def test_native_runtime_context_excludes_only_its_primary_project(self) -> None:
        api = FakeLinear()
        native_context = {
            "HERMES_SESSION_PLATFORM": "local",
            "HERMES_SESSION_CHAT_ID": "linear:demo-space:linear-session-1",
            "HERMES_SESSION_THREAD_ID": "linear-primary:f7a2e3f4-7ea8-4de5-a45f-03a1f499f371",
        }
        with mock.patch.dict("os.environ", native_context, clear=False):
            result = self.publisher(api).publish(
                session_key="arbitrary-chat-closeout-key",
                issue_ids=["TEAM-501", "TEAM-502"],
                project_summaries={"project-a": "Primary work.", "project-b": "Additional work."},
            )
        self.assertEqual([item["project_id"] for item in result], ["project-a", "project-b"])
        self.assertEqual(result[0]["status"], "skipped_native_primary_project")
        self.assertEqual(result[1]["status"], "created")
        self.assertEqual(len(api.updates), 1)
        self.assertEqual(next(iter(api.updates.values()))["project"]["id"], "project-b")

    def test_later_or_non_native_session_does_not_inherit_primary_exclusion(self) -> None:
        api = FakeLinear()
        stale_or_chat_context = {
            "HERMES_SESSION_PLATFORM": "telegram",
            "HERMES_SESSION_CHAT_ID": "linear:demo-space:linear-session-1",
            "HERMES_SESSION_THREAD_ID": "linear-primary:f7a2e3f4-7ea8-4de5-a45f-03a1f499f371",
        }
        with mock.patch.dict("os.environ", stale_or_chat_context, clear=False):
            result = self.publisher(api).publish(
                session_key="later-session-key",
                issue_ids=["TEAM-501"],
                project_summaries={"project-a": "Later independent work."},
            )
        self.assertEqual(result[0]["status"], "created")
        self.assertEqual(len(api.updates), 1)

    def test_retry_reads_deterministic_update_before_create(self) -> None:
        api = FakeLinear()
        publisher = self.publisher(api)
        first = publisher.publish(session_key="run-1", issue_ids=["TEAM-501"], project_summaries={"project-a": "Done."})
        call_count = len([1 for query, _ in api.calls if "ProjectUpdateCreate" in query])
        second = publisher.publish(session_key="run-1", issue_ids=["TEAM-501"], project_summaries={"project-a": "Done."})
        self.assertEqual(first[0]["update_id"], second[0]["update_id"])
        self.assertEqual(second[0]["status"], "existing")
        self.assertEqual(len([1 for query, _ in api.calls if "ProjectUpdateCreate" in query]), call_count)

    def test_timeout_reconciles_by_exact_readback_without_recreating(self) -> None:
        api = FakeLinear(timeout_create=True)
        result = self.publisher(api).publish(session_key="run-1", issue_ids=["TEAM-501"], project_summaries={"project-a": "Done."})
        self.assertEqual(result[0]["project_id"], "project-a")
        self.assertEqual(len([1 for query, _ in api.calls if "ProjectUpdateCreate" in query]), 1)

    def test_rejects_same_key_with_different_payload(self) -> None:
        api = FakeLinear()
        publisher = self.publisher(api)
        publisher.publish(session_key="run-1", issue_ids=["TEAM-501"], project_summaries={"project-a": "Done."})
        with self.assertRaisesRegex(ProjectUpdateError, "different payload"):
            publisher.publish(session_key="run-1", issue_ids=["TEAM-501"], project_summaries={"project-a": "Changed."})

    def test_rejects_unprojected_or_foreign_issue_and_invalid_source_without_create(self) -> None:
        for issue_ids, setup in (([" TEAM-501"], None), (["TEAM-501"], "unprojected"), (["TEAM-501"], "foreign")):
            with self.subTest(issue_ids=issue_ids, setup=setup):
                api = FakeLinear()
                if setup == "unprojected": api.issues["TEAM-501"]["project"] = None
                if setup == "foreign": api.issues["TEAM-501"]["team"]["organization"]["id"] = "other"
                error = NoProjectUpdate if setup == "unprojected" else ProjectUpdateError
                with self.assertRaises(error):
                    self.publisher(api).publish(session_key="run-1", issue_ids=issue_ids, project_summaries={"project-a": "Done."})
                self.assertFalse(any("ProjectUpdateCreate" in query for query, _ in api.calls))

    def test_embedded_api_groups_only_issue_specific_summaries(self) -> None:
        api = FakeLinear()
        result = publish_session_updates(api, "run-1", [
            {"issue_id": "TEAM-501", "summary": "A-only evidence."},
            {"issue_id": "TEAM-502", "summary": "B-only evidence."},
        ], configured_workspace="workspace-a")
        self.assertEqual(len(result), 2)
        bodies = [variables["input"]["body"] for query, variables in api.calls if "ProjectUpdateCreate" in query]
        self.assertEqual(bodies, ["### [TEAM-501](https://linear.app/acme/issue/TEAM-501)\nA-only evidence.", "### [TEAM-502](https://linear.app/acme/issue/TEAM-502)\nB-only evidence."])

    def test_publisher_policy_is_separate_from_execution_roster_and_pins_oauth(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            policy_dir = Path(temp)
            policy = {
                "publishers": [
                    {
                        "profile": "operator",
                        "workspace": "demo-space",
                        "viewer_id": "11111111-1111-4111-8111-111111111111",
                        "organization_id": "22222222-2222-4222-8222-222222222222",
                        "oauth": {
                            "mode": "managed_oauth_v1",
                            "vault_id": "cccccccccccccccccccccccccc",
                            "item_id": "dddddddddddddddddddddddddd",
                        },
                    }
                ]
            }
            policy_path = policy_dir / "linear-publishers.json"
            policy_path.write_text(json.dumps(policy), encoding="utf-8")
            policy_path.chmod(0o444)
            with (
                mock.patch.object(linear_project_updates, "_PUBLISHER_POLICY_PATH", policy_path),
                mock.patch.object(linear_project_updates, "read_publisher_policy", return_value=policy),
            ):
                binding = _publisher_binding(
                    "operator", "demo-space", "cccccccccccccccccccccccccc", "dddddddddddddddddddddddddd"
                )
                self.assertEqual(
                    binding,
                    {
                        "viewer_id": "11111111-1111-4111-8111-111111111111",
                        "organization_id": "22222222-2222-4222-8222-222222222222",
                    },
                )
                with self.assertRaises(ProjectUpdateError):
                    _publisher_binding("operator", "demo-space", "cccccccccccccccccccccccccc", "wrong-item")

    def test_cli_reports_no_project_as_explicit_skip(self) -> None:
        api = FakeLinear()
        api.issues["TEAM-501"]["project"] = None
        publisher = LinearProjectUpdatePublisher(api, configured_workspace="workspace-a", configured_actor="actor-a")
        stdout = io.StringIO()
        with mock.patch("linear_project_updates._configured_publisher", return_value=publisher), mock.patch("sys.stdout", stdout):
            result = main(["--session-key", "closeout", "--issues-json", '[{"issue_id":"TEAM-501","summary":"Done."}]'])
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(stdout.getvalue())["status"], "skipped_no_project")
        self.assertFalse(any("ProjectUpdateCreate" in query for query, _ in api.calls))

    def test_real_absent_lookup_error_is_distinct_from_denied_or_broken_read(self) -> None:
        from linear_activity import LinearActivityClient
        absent = {'message': 'Entity not found: ProjectUpdate',
                  'path': ['projectUpdate'],
                  'extensions': {'code': 'INPUT_ERROR', 'type': 'invalid input'}}
        for error, expected_create in ((absent, True),
                                       ({**absent, 'extensions': {'code': 'FORBIDDEN'}}, False),
                                       ({**absent, 'path': ['issue']}, False)):
            with self.subTest(error=error):
                api = FakeLinear()
                def transport(url, headers, body):
                    payload = json.loads(body)
                    query, variables = payload['query'], payload['variables']
                    if 'ProjectUpdateLookup' in query and variables['id'] not in api.updates:
                        return json.dumps({'errors': [error], 'data': None}).encode()
                    return json.dumps(api(query, variables)).encode()
                client = LinearActivityClient('test-token', transport=transport)
                publisher = LinearProjectUpdatePublisher(client._graphql)
                if expected_create:
                    self.assertEqual(publisher.publish(session_key='absent-test', issue_ids=['TEAM-501'], project_summaries={'project-a': 'Work.'})[0]['status'], 'created')
                else:
                    with self.assertRaises(ProjectUpdateError):
                        publisher.publish(session_key='absent-test', issue_ids=['TEAM-501'], project_summaries={'project-a': 'Work.'})
                    self.assertFalse(api.updates)

    def test_invalid_identifier_is_rejected_before_any_network_request(self) -> None:
        for source in ('team-501', 'TEAM-0501', 'TEAM-0', 'TEAM-501 ', 'not-an-id', ' https://linear.app/x'):
            api = FakeLinear()
            with self.subTest(source=source), self.assertRaises(ProjectUpdateError):
                publish_session_updates(api, 'test', [{'issue_id': source, 'summary': 'Work.'}])
            self.assertFalse(api.calls)

    def test_publishing_settings_do_not_require_worker_enablement(self) -> None:
        from linear_project_updates import _publisher_settings
        disabled = {'plugins': {'enabled': [], 'entries': {'linear-agent': {'settings': {
            'profile': 'operator', 'workspace': 'demo-space',
            'enabled': False, 'dry_run': True, 'credential_mode': 'managed_oauth_v1',
        }}}}}
        self.assertEqual(_publisher_settings(disabled, 'operator')['profile'], 'operator')
        for profile in (None, 'coordinator', 'default'):
            with self.assertRaises(ProjectUpdateError):
                _publisher_settings(disabled, profile)
        with self.assertRaises(ProjectUpdateError):
            _publisher_settings({}, 'operator')

    def test_readback_only_accepts_benign_markdown_canonicalization(self) -> None:
        api = FakeLinear(altered_body="## Outcome\n\n* Done\n")
        result = self.publisher(api).publish(session_key="run-1", issue_ids=["TEAM-501"], project_summaries={"project-a": "## Outcome\n- Done\n"})
        self.assertEqual(result[0]["project_id"], "project-a")
        self.assertTrue(LinearProjectUpdatePublisher.markdown_equivalent("## Outcome\n- Done\n", "## Outcome\n\n* Done\n"))
        self.assertFalse(LinearProjectUpdatePublisher.markdown_equivalent("`literal`", "`changed`"))
        self.assertFalse(LinearProjectUpdatePublisher.markdown_equivalent("```\na\n\nb\n```", "```\na\nb\n```"))
        self.assertFalse(LinearProjectUpdatePublisher.markdown_equivalent("```\n# comment\n\n* literal\n```", "```\n# comment\n- literal\n```"))
        self.assertFalse(LinearProjectUpdatePublisher.markdown_equivalent("a  \nb", "a\nb"))
        self.assertFalse(LinearProjectUpdatePublisher.markdown_equivalent("a\n\nb", "a\nb"))


if __name__ == "__main__":
    unittest.main()
