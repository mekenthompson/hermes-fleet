"""GraphQL rejection detail must reach the linear log and the chat reply.

A rate-limit body stays a pause. It must not be reported as a generic GraphQL error.
"""
from __future__ import annotations

import json
import logging
import unittest

from linear_fake_api import load_plugin

load_plugin()
from hermes_fleet_linear_plugin import api as linear_api  # noqa: E402
from hermes_fleet_linear_plugin import chat  # noqa: E402

MESSAGE = "Variable $id of type String! was provided invalid value"
CODE = "GRAPHQL_VALIDATION_FAILED"
TOKEN = "synthetic-token-not-for-logs"
CLIENT = "0b8f5a7e-3c1d-4e2f-9a6b-7c8d9e0f1a2b"


class _Context:
    session_key = session_id = "chat"
    profile = "alpha"
    platform = "slack"
    run_generation = 1


class _Bridge:
    def __init__(self, api) -> None:
        self.api = api

    def chat_profile_matches(self, profile, platform=""):
        return True


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class GraphQLErrorDetailTests(unittest.TestCase):
    def _client(self, payload, *, status=200, headers=None):
        seen = []

        def transport(url, body, request_headers):
            seen.append((body, request_headers))
            return status, headers or {}, json.dumps(payload).encode()

        return linear_api.LinearAPI(lambda: TOKEN, transport=transport), seen

    def _records(self):
        handler = _Capture()
        logger = logging.getLogger("linear")
        previous = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        return logger, handler, handler.records, previous

    def _release(self, logger, handler, previous) -> None:
        logger.removeHandler(handler)
        logger.setLevel(previous)

    def test_graphql_error_is_logged_and_shown_for_lookup_and_create(self) -> None:
        payload = {"errors": [{"message": MESSAGE, "extensions": {"code": CODE}}]}
        api, seen = self._client(payload)
        logger, handler, records, previous = self._records()
        try:
            with self.assertRaises(linear_api.LinearError) as caught:
                api.graphql("query Marked { viewer { id } }")
            lookup = json.loads(chat.handle(
                _Bridge(api), {"action": "start", "issue": "ABC-1"}, _Context()))
            created = json.loads(chat.handle(
                _Bridge(api),
                {"action": "create_issue", "id": CLIENT, "title": "Reconcile", "team": "HF"},
                _Context()))
        finally:
            self._release(logger, handler, previous)

        self.assertFalse(caught.exception.retryable)
        self.assertEqual(caught.exception.errors, payload["errors"])
        self.assertTrue(str(caught.exception).startswith("Linear GraphQL error"))
        self.assertIn(MESSAGE, str(caught.exception))
        self.assertIn(CODE, str(caught.exception))

        for reply in (lookup, created):
            self.assertFalse(reply["ok"])
            text = reply["message"]
            self.assertIn("Linear GraphQL error", text)
            self.assertLess(text.index("Linear GraphQL error"), text.index(MESSAGE))
            self.assertIn(CODE, text)
            self.assertNotIn(TOKEN, text)
        self.assertTrue(created["message"].startswith("Linear GraphQL error"))

        self.assertGreaterEqual(len(records), 1)
        self.assertTrue(any("Authorization" in headers for _, headers in seen))
        for record in records:
            text = record.getMessage()
            self.assertIn(MESSAGE, text)
            self.assertIn(CODE, text)
            self.assertNotIn(TOKEN, text)
            self.assertNotIn("Authorization", text)
            self.assertNotIn("Bearer", text)
            self.assertNotIn("Marked", text)
            self.assertNotIn("IssueCreate", text)

    def test_rate_limit_payload_is_not_a_graphql_error(self) -> None:
        payload = {"errors": [{"message": "quota exhausted", "extensions": {"code": "RATELIMITED"}}]}
        api, _seen = self._client(payload, status=400, headers={"Retry-After": "30"})
        logger, handler, records, previous = self._records()
        try:
            reply = json.loads(chat.handle(
                _Bridge(api), {"action": "start", "issue": "ABC-1"}, _Context()))
        finally:
            self._release(logger, handler, previous)

        self.assertGreater(api.paused_until, 0)
        self.assertNotIn("Linear GraphQL error", reply["message"])
        self.assertNotIn("quota exhausted", reply["message"])
        self.assertIn("rate limit", reply["message"].lower())
        self.assertFalse(any("GraphQL error" in record.getMessage() for record in records))
        self.assertFalse(any("quota exhausted" in record.getMessage() for record in records))
        self.assertFalse(any(TOKEN in record.getMessage() for record in records))


if __name__ == "__main__":
    unittest.main()
