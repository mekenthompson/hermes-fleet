"""Durable worker tests for the profile-local Linear adapter."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "plugins" / "linear-agent"))
from linear_agent import LinearWorker, unauthorized_response_body_from_entry


ALLOWED_USER_ID = "11111111-1111-4111-8111-111111111111"
DENIED_USER_ID = "22222222-2222-4222-8222-222222222222"
UPPERCASE_USER_ID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"


class LinearAgentWorkerTests(unittest.TestCase):
    def test_project_summary_is_explicit_scoped_and_redacted(self) -> None:
        from linear_agent import _project_summary
        response = ('Private detail from another project.\n\n### Project status update\n'
                    'Landed the approved fix. api_key=secretvalue\n'
                    'Next: verify rollout.\n\n### Other work\nPrivate unrelated detail.')
        summary = _project_summary(response)
        self.assertIn('Landed the approved fix.', summary)
        self.assertIn('Next: verify rollout.', summary)
        self.assertNotIn('secretvalue', summary)
        self.assertNotIn('Private', summary)
        self.assertNotIn('Other work', summary)

    def test_missing_duplicate_or_fenced_project_summary_uses_safe_pointer(self) -> None:
        from linear_agent import _project_summary
        for response in ('Private detail.',
                         '```markdown\n### Project status update\nPrivate detail.\n```',
                         '### Project status update\nPrivate one.\n### Project status update\nPrivate two.'):
            with self.subTest(response=response):
                summary = _project_summary(response)
                self.assertNotIn('Private', summary)
                self.assertIn('recorded on the issue', summary)

    def test_second_linear_session_for_an_admitted_issue_is_rejected_before_session_creation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(Path(temp) / "worker.db", profile="alpha", workspace="demo-space")
            def payload(session_id: str, prompt: str) -> bytes:
                return json.dumps({
                    "type": "AgentSessionEvent", "action": "created",
                    "agentSession": {"id": session_id, "issue": {"id": "issue-1"}},
                    "promptContext": prompt,
                }).encode()

            worker.add_delivery("delivery-1", payload("linear-session-1", "first"))
            self.assertTrue(worker.admit_once()[0])
            worker.add_delivery("delivery-2", payload("linear-session-2", "must not execute"))

            admitted, job = worker.admit_once()

            self.assertTrue(admitted)
            self.assertIsNone(job)
            self.assertEqual(worker.delivery_state("delivery-2"), "rejected")
            self.assertEqual(worker.delivery_rejection("delivery-2")[1], "issue_active_in_linear_session")
            self.assertEqual(
                [row for row in worker.outbox() if row[1] == "linear-session-2"],
                [("response", "linear-session-2", "This issue is already active in another Linear session.")],
            )

    def test_chat_owned_issue_rejects_created_and_prompted_without_session_mapping(self) -> None:
        from linear_ownership import IssueOwnership
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "worker.db"
            worker = LinearWorker(database, profile="alpha", workspace="demo-space")
            IssueOwnership(database, profile="alpha", workspace="demo-space").claim("issue-1", "chat-1")
            for delivery, action, session, extra in (
                ("created", "created", "linear-1", {"promptContext": "work"}),
                ("prompted", "prompted", "linear-2", {"agentActivity": {"id": "activity-1", "body": "follow up"}}),
            ):
                worker.add_delivery(delivery, json.dumps({
                    "type": "AgentSessionEvent", "action": action,
                    "agentSession": {"id": session, "issue": {"id": "issue-1"}}, **extra,
                }).encode())
                self.assertTrue(worker.admit_once()[0])
                self.assertEqual(worker.delivery_rejection(delivery)[1], "issue_owned_by_chat")
                self.assertIn(
                    ("response", session, "already in progress from Hermes chat, updates will land on the ticket"),
                    worker.outbox(),
                )
            with worker._connect() as conn:
                mappings = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            self.assertEqual(mappings, 0)

    def test_unauthorized_response_body_requires_allowlist_and_exact_text(self) -> None:
        with self.assertRaisesRegex(ValueError, "allowed_linear_user_ids"):
            unauthorized_response_body_from_entry({
                "unauthorized_response_body": "nope",
            })
        with self.assertRaisesRegex(ValueError, "unauthorized_response_body"):
            unauthorized_response_body_from_entry({
                "allowed_linear_user_ids": [ALLOWED_USER_ID],
                "unauthorized_response_body": " padded ",
            })
        self.assertIsNone(unauthorized_response_body_from_entry({
            "allowed_linear_user_ids": [ALLOWED_USER_ID],
        }))
        self.assertEqual(
            unauthorized_response_body_from_entry({
                "allowed_linear_user_ids": [ALLOWED_USER_ID],
                "unauthorized_response_body": "Yo yo",
            }),
            "Yo yo",
        )

    def test_allowlist_configuration_must_be_non_empty_unique_uuids(self) -> None:
        invalid_policies = (
            [],
            ["not-a-linear-user-id"],
            [UPPERCASE_USER_ID],
            [ALLOWED_USER_ID, ALLOWED_USER_ID],
        )
        for policy in invalid_policies:
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as temp:
                with self.assertRaisesRegex(ValueError, "allowed_linear_user_ids"):
                    LinearWorker(
                        Path(temp) / "worker.db",
                        profile="alpha",
                        workspace="demo-space",
                        allowed_linear_user_ids=policy,
                    )

    def test_distinct_prompted_activities_in_one_session_both_execute(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(Path(temp) / "worker.db", profile="alpha", workspace="demo-space")
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent", "action": "created",
                "agentSession": {"id": "linear-session-1", "issue": {"id": "issue-1"}},
                "promptContext": "first follow-up",
            }).encode())
            worker.add_delivery("delivery-2", json.dumps({
                "type": "AgentSessionEvent", "action": "prompted",
                "agentSession": {"id": "linear-session-1", "issue": {"id": "issue-1"}},
                "agentActivity": {
                    "id": "activity-2",
                    "content": {"type": "prompt", "body": "second follow-up"},
                },
            }).encode())
            self.assertTrue(worker.admit_once()[0])
            first = worker.next_unprepared()
            assert first is not None
            worker.mark_prepared(first)
            first = worker.claim_prepared()
            assert first is not None
            worker.complete_job(first, "first complete")
            self.assertTrue(worker.admit_once()[0])
            second = worker.next_unprepared()
            assert second is not None
            worker.mark_prepared(second)
            second = worker.claim_prepared()
            assert second is not None
            worker.complete_job(second, "second complete")
            project_update_keys = [
                json.loads(body)["session_key"]
                for kind, _target, body in worker.outbox()
                if kind == "project_update"
            ]
            self.assertEqual(project_update_keys, [
                "linear:demo-space:linear-session-1:delivery-1:closeout",
                "linear:demo-space:linear-session-1:delivery-2:closeout",
            ])

    def test_allowlist_rejects_queued_and_prepared_jobs_during_recovery(self) -> None:
        for initial_state in ("queued", "prepared"):
            with self.subTest(initial_state=initial_state), tempfile.TemporaryDirectory() as temp:
                database = Path(temp) / "worker.db"
                unrestricted = LinearWorker(
                    database,
                    profile="alpha",
                    workspace="demo-space",
                )
                unrestricted.add_delivery("delivery-1", json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "linear-session-1",
                        "creatorId": DENIED_USER_ID,
                    },
                    "promptContext": "must not execute",
                }).encode())
                admitted, _job = unrestricted.admit_once()
                self.assertTrue(admitted)
                if initial_state == "prepared":
                    queued = unrestricted.next_unprepared()
                    assert queued is not None
                    unrestricted.mark_prepared(queued)

                restricted = LinearWorker(
                    database,
                    profile="alpha",
                    workspace="demo-space",
                    allowed_linear_user_ids=[ALLOWED_USER_ID],
                )
                recovered = (
                    restricted.next_unprepared()
                    if initial_state == "queued"
                    else restricted.claim_prepared()
                )

                self.assertIsNone(recovered)
                self.assertEqual(restricted.delivery_state("delivery-1"), "rejected")
                self.assertEqual(
                    restricted.delivery_rejection("delivery-1"),
                    (DENIED_USER_ID, "linear_user_not_allowed"),
                )
                denials = [
                    item for item in restricted.outbox()
                    if item[2] == "This agent is restricted to approved workspace users."
                ]
                self.assertEqual(len(denials), 1)

    def test_restart_retries_status_update_left_sending(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "worker.db"
            worker = LinearWorker(
                database,
                profile="alpha",
                workspace="demo-space",
            )
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode())
            worker.admit_once()
            job = worker.next_unprepared()
            assert job is not None
            worker.mark_prepared(job)
            running = worker.claim_prepared()
            assert running is not None
            receipt = {
                "lifecycle_version": "execution-lifecycle/v2",
                "session_key": running.hermes_session_key,
                "execution_id": "exec-1",
                "generation": 1,
                "state": "completed",
                "occupancy": "released",
                "tools": "none",
                "children": "none",
                "processes": "none",
                "remote": "none",
            }
            worker.record_lifecycle_receipt(
                running, "exec-1", receipt, released=True
            )
            worker.complete_job(running, "Finished")
            with worker._connect() as conn:
                conn.execute("UPDATE outbox SET state = 'sent'")
                conn.execute(
                    "UPDATE outbox SET state = 'sending' "
                    "WHERE kind = 'issue_status_done'"
                )

            recovered = LinearWorker(
                database,
                profile="alpha",
                workspace="demo-space",
            )
            emitted: list[tuple[str, str, str]] = []
            self.assertTrue(recovered.dispatch_outbox(
                lambda target, operation, body: emitted.append(
                    (target, operation, body)
                )
            ))
            self.assertEqual(
                emitted,
                [("issue-1", "issue_status", "done")],
            )

    def test_restart_preserves_admitted_job_for_fifo_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "worker.db"
            payload = json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {"id": "linear-session-1"},
                "promptContext": "work",
            }).encode()
            worker = LinearWorker(database, profile="alpha", workspace="demo-space")
            worker.add_delivery("delivery-1", payload)

            admitted, job = worker.admit_once()
            self.assertTrue(admitted)
            self.assertIsNotNone(job)
            self.assertEqual(worker.delivery_state("delivery-1"), "queued")

            recovered = LinearWorker(database, profile="alpha", workspace="demo-space")
            queued = recovered.next_unprepared()
            self.assertIsNotNone(queued)
            self.assertEqual(queued.prompt, "work")
            recovered.mark_prepared(queued)
            queued = recovered.claim_prepared()
            self.assertIsNotNone(queued)
            recovered.complete_job(queued, "Done")
            self.assertEqual(recovered.delivery_state("delivery-1"), "completed")

    def test_import_marks_selected_logical_agent_when_name_differs_from_profile(self) -> None:
        from tests.linear_ingress_fixture import IngressStore, Route
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inbox = root / "inbox"
            ingress = IngressStore(inbox / "ingress.db")
            route = Route("team-alpha", "alpha", "/webhook/alpha/linear", root / "unused.env", inbox)
            ingress.enqueue(route, "delivery-1", json.dumps({"data": {"agentSession": {"id": "linear-session-1"}, "prompt": "work"}}).encode())
            worker = LinearWorker(root / "worker.db", profile="alpha", workspace="demo-space")
            self.assertTrue(worker.import_from_ingress_once(inbox / "ingress.db"))
            self.assertFalse(worker.import_from_ingress_once(inbox / "ingress.db"))


    def test_queued_follow_up_does_not_move_an_actively_running_issue_back_to_todo(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="demo-space",
            )
            first = json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "first turn",
            }).encode()
            follow_up = json.dumps({
                "type": "AgentSessionEvent",
                "action": "prompted",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "agentActivity": {
                    "id": "activity-2",
                    "body": "second turn",
                },
            }).encode()

            worker.add_delivery("delivery-1", first)
            self.assertTrue(worker.admit_once()[0])
            first_job = worker.next_unprepared()
            assert first_job is not None
            worker.mark_prepared(first_job)
            self.assertIsNotNone(worker.claim_prepared())

            worker.add_delivery("delivery-2", follow_up)
            self.assertTrue(worker.admit_once()[0])
            emitted: list[tuple[str, str, str]] = []
            while worker.dispatch_outbox(
                lambda target, operation, body: emitted.append(
                    (target, operation, body)
                )
            ):
                pass

            self.assertEqual(
                [body for _, operation, body in emitted if operation == "issue_status"],
                ["active"],
            )
    def test_finishing_a_turn_leaves_issue_in_todo_when_a_follow_up_is_queued(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="demo-space",
            )
            first = json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "first turn",
            }).encode()
            follow_up = json.dumps({
                "type": "AgentSessionEvent",
                "action": "prompted",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "agentActivity": {
                    "id": "activity-2",
                    "body": "second turn",
                },
            }).encode()

            worker.add_delivery("delivery-1", first)
            self.assertTrue(worker.admit_once()[0])
            first_job = worker.next_unprepared()
            assert first_job is not None
            worker.mark_prepared(first_job)
            running = worker.claim_prepared()
            assert running is not None
            worker.add_delivery("delivery-2", follow_up)
            self.assertTrue(worker.admit_once()[0])

            worker.complete_job(running, "first complete")
            emitted: list[tuple[str, str, str]] = []
            while worker.dispatch_outbox(
                lambda target, operation, body: emitted.append(
                    (target, operation, body)
                )
            ):
                pass

            self.assertEqual(
                [body for _, operation, body in emitted if operation == "issue_status"],
                ["active"],
            )
    def test_restart_marks_interrupted_turn_failed_without_duplicate_comment(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "worker.db"
            worker = LinearWorker(
                database,
                profile="alpha",
                workspace="demo-space",
            )
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode())
            worker.admit_once()
            job = worker.next_unprepared()
            assert job is not None
            worker.mark_prepared(job)
            self.assertIsNotNone(worker.claim_prepared())

            recovered = LinearWorker(
                database,
                profile="alpha",
                workspace="demo-space",
            )
            self.assertEqual(recovered.delivery_state("delivery-1"), "ambiguous")
            emitted: list[tuple[str, str, str]] = []
            while recovered.dispatch_outbox(
                lambda target, operation, body: emitted.append(
                    (target, operation, body)
                )
            ):
                pass

            self.assertIn(
                (
                    "linear-session-1",
                    "error",
                    (
                        "The agent was interrupted before its status could be "
                        "confirmed. Please retry this request."
                    ),
                ),
                emitted,
            )
            self.assertFalse(
                any(operation == "issue_comment" for _, operation, _ in emitted)
            )
            self.assertEqual(
                [body for _, operation, body in emitted if operation == "issue_status"],
                ["active", "failure"],
            )
    def test_summary_comment_is_redacted_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="demo-space",
            )
            job = worker._standard_job_from_payload(
                "delivery-1",
                json.dumps({
                    "type": "AgentSessionEvent",
                    "action": "created",
                    "agentSession": {
                        "id": "linear-session-1",
                        "issue": {"id": "issue-1"},
                    },
                    "promptContext": "work",
                }).encode(),
            )[0]
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode())
            progressed, job = worker.admit_once()
            self.assertTrue(progressed)
            self.assertIsNotNone(job)
            worker.mark_prepared(job)
            job = worker.claim_prepared()
            self.assertIsNotNone(job)
            self.assertEqual(worker.delivery_state("delivery-1"), "running")
            private_key_marker = "PRIVATE " + "KEY"
            fake_private_key = "\n".join((
                f"-----BEGIN {private_key_marker}-----",
                "private-material",
                f"-----END {private_key_marker}-----",
            ))
            sensitive_lines = (
                "access_token=super-secret",
                "Authorization: Basic abc123",
                "https://user:pass@example.test/x?api_key=url-secret",
                "Cookie: sessionid=cookie-secret",
                '{"apiKey": "json secret with spaces"}',
                "apiKey:",
                "  - super-secret-list-value",
                "clientSecret: yaml-secret",
                fake_private_key,
            )
            sensitive = "\n".join(sensitive_lines)
            worker.complete_job(
                job,
                sensitive + " " + ("x" * 9000),
            )
            emitted: list[tuple[str, str, str]] = []
            while worker.dispatch_outbox(
                lambda target, operation, body: emitted.append(
                    (target, operation, body)
                )
            ):
                pass

            comment = next(
                body for _, operation, body in emitted
                if operation == "issue_comment"
            )
            for secret in (
                "super-secret",
                "abc123",
                "user:pass",
                "url-secret",
                "cookie-secret",
                "json secret with spaces",
                "super-secret-list-value",
                "yaml-secret",
                "private-material",
            ):
                self.assertNotIn(secret, comment)
            self.assertIn("[REDACTED]", comment)
            self.assertLessEqual(len(comment), 8000)
            self.assertTrue(comment.endswith("…"))
    def test_duplicate_replay_does_not_repeat_issue_statuses_or_summary_comment(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="demo-space",
            )
            payload = json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode()
            worker.add_delivery("delivery-1", payload)
            worker.admit_once()
            job = worker.next_unprepared()
            assert job is not None
            worker.mark_prepared(job)
            running = worker.claim_prepared()
            assert running is not None
            receipt = {
                "lifecycle_version": "execution-lifecycle/v2",
                "session_key": running.hermes_session_key,
                "execution_id": "exec-replay",
                "generation": 1,
                "state": "completed",
                "occupancy": "released",
                "tools": "none",
                "children": "none",
                "processes": "none",
                "remote": "none",
            }
            worker.record_lifecycle_receipt(running, "exec-replay", receipt, released=True)
            worker.complete_job(running, "Finished")

            worker.add_delivery("delivery-2", payload)
            worker.admit_once()
            emitted: list[tuple[str, str, str]] = []
            while worker.dispatch_outbox(
                lambda target, operation, body: emitted.append(
                    (target, operation, body)
                )
            ):
                pass

            self.assertEqual(
                sum(operation == "issue_comment" for _, operation, _ in emitted),
                1,
            )
            project_updates = [
                (target, json.loads(body))
                for target, operation, body in emitted
                if operation == "project_update"
            ]
            self.assertEqual(project_updates, [
                ("issue-1", {
                    "session_key": "linear:demo-space:linear-session-1:delivery-1:closeout",
                    "summary": (
                        "### Agent session summary\n\n"
                        "Work session finished. Detailed findings and remaining actions are recorded on the issue. "
                        "Issue completion and project health are not inferred from session completion."
                    ),
                })
            ])
            self.assertEqual(
                [body for _, operation, body in emitted if operation == "issue_status"],
                ["active", "done"],
            )
    def test_existing_worker_database_is_migrated_for_issue_lifecycle(self) -> None:
        import sqlite3

        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "worker.db"
            with sqlite3.connect(database) as conn:
                conn.executescript("""
                    CREATE TABLE deliveries (
                        delivery_id TEXT PRIMARY KEY,
                        payload BLOB NOT NULL,
                        payload_sha256 TEXT NOT NULL,
                        state TEXT NOT NULL DEFAULT 'pending',
                        created_at INTEGER NOT NULL
                    );
                    CREATE TABLE outbox (
                        delivery_id TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        linear_session_id TEXT NOT NULL,
                        body TEXT NOT NULL,
                        state TEXT NOT NULL DEFAULT 'pending',
                        PRIMARY KEY (delivery_id, kind)
                    );
                """)

            worker = LinearWorker(
                database,
                profile="alpha",
                workspace="demo-space",
            )
            with worker._connect() as conn:
                delivery_columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA table_info(deliveries)"
                    )
                }
                outbox_columns = {
                    str(row[1]) for row in conn.execute("PRAGMA table_info(outbox)")
                }
                tables = {
                    str(row[0]) for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
            self.assertIn("issue_id", delivery_columns)
            self.assertIn("requester_user_id", delivery_columns)
            self.assertIn("rejection_reason", delivery_columns)
            self.assertIn("sequence", outbox_columns)
            self.assertIn("outbox_sequence", tables)
    def test_status_update_with_uncertain_result_is_retried_safely(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="demo-space",
            )
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode())
            worker.admit_once()
            job = worker.next_unprepared()
            assert job is not None
            worker.mark_prepared(job)
            self.assertIsNotNone(worker.claim_prepared())
            active_attempts = 0

            def emit(_target, operation, body):
                nonlocal active_attempts
                if operation == "issue_status" and body == "active":
                    active_attempts += 1
                    if active_attempts == 1:
                        raise TimeoutError("uncertain status update")

            with self.assertRaises(TimeoutError):
                while worker.dispatch_outbox(emit):
                    pass
            self.assertTrue(worker.dispatch_outbox(emit))
            self.assertEqual(active_attempts, 2)


    def test_review_handoff_reassigns_requester_and_does_not_mark_done(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="examplecom",
                allowed_linear_user_ids=[ALLOWED_USER_ID],
                terminal_issue_status="review",
                reassign_to_requester=True,
            )
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "creatorId": ALLOWED_USER_ID,
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode())
            admitted, job = worker.admit_once()
            self.assertTrue(admitted)
            assert job is not None
            self.assertEqual(job.requester_user_id, ALLOWED_USER_ID)
            prepared = worker.next_unprepared()
            assert prepared is not None
            worker.mark_prepared(prepared)
            running = worker.claim_prepared()
            assert running is not None
            worker.complete_job(running, "Recommend a small fix.")
            kinds = [kind for kind, _, _ in worker.outbox()]
            self.assertIn("issue_handoff", kinds)
            self.assertNotIn("issue_status_done", kinds)
            handoff = [
                json.loads(body)
                for kind, _, body in worker.outbox()
                if kind == "issue_handoff"
            ]
            self.assertEqual(
                handoff,
                [{
                    "state": "review",
                    "assigneeId": ALLOWED_USER_ID,
                    "clearDelegate": True,
                }],
            )

    def test_complete_job_does_not_mark_done_from_model_text_alone(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="demo-space",
            )
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode())
            worker.admit_once()
            prepared = worker.next_unprepared()
            assert prepared is not None
            worker.mark_prepared(prepared)
            running = worker.claim_prepared()
            assert running is not None
            worker.complete_job(running, "Finished")
            kinds = [kind for kind, _, _ in worker.outbox()]
            self.assertIn("response", kinds)
            self.assertNotIn("issue_status_done", kinds)

    def test_complete_job_marks_done_only_with_accepted_lifecycle_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="demo-space",
            )
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode())
            worker.admit_once()
            prepared = worker.next_unprepared()
            assert prepared is not None
            worker.mark_prepared(prepared)
            running = worker.claim_prepared()
            assert running is not None
            receipt = {
                "lifecycle_version": "execution-lifecycle/v2",
                "session_key": running.hermes_session_key,
                "execution_id": "exec-1",
                "generation": 1,
                "state": "completed",
                "occupancy": "released",
                "tools": "none",
                "children": "none",
                "processes": "none",
                "remote": "none",
            }
            worker.record_lifecycle_receipt(
                running, "exec-1", receipt, released=True
            )
            worker.complete_job(running, "Finished")
            kinds = [kind for kind, _, _ in worker.outbox()]
            self.assertIn("issue_status_done", kinds)

    def test_fail_job_emits_error_activity_not_done(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            worker = LinearWorker(
                Path(temp) / "worker.db",
                profile="alpha",
                workspace="examplecom",
            )
            worker.add_delivery("delivery-1", json.dumps({
                "type": "AgentSessionEvent",
                "action": "created",
                "agentSession": {
                    "id": "linear-session-1",
                    "issue": {"id": "issue-1"},
                },
                "promptContext": "work",
            }).encode())
            worker.admit_once()
            prepared = worker.next_unprepared()
            assert prepared is not None
            worker.mark_prepared(prepared)
            running = worker.claim_prepared()
            assert running is not None
            worker.fail_job(running)
            kinds = [kind for kind, _, _ in worker.outbox()]
            self.assertIn("error", kinds)
            self.assertNotIn("issue_status_done", kinds)
            self.assertIn("issue_status_failure", kinds)


if __name__ == "__main__":
    unittest.main()
