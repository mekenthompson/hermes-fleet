"""Credential-free wire regressions; authenticated inference is a separate gate."""
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from tests.test_claude_acp_connector_hydration import load_client

PEER = Path(__file__).parent / "fixtures/claude_acp_wire_peer.py"
TOOLS = [{"type": "function", "function": {"name": "probe", "parameters": {
    "type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"],
}}}]


class ClaudeWireTests(unittest.TestCase):
    def setUp(self):
        self.module = load_client()
        self.home = tempfile.TemporaryDirectory()
        self.addCleanup(self.home.cleanup)
        # Only the deterministic wire fixture bypasses artifact identity; real
        # candidate artifact verification runs separately and fails closed.
        guard = patch.object(self.module, "assert_reviewed_claude_code_version")
        guard.start()
        self.addCleanup(guard.stop)
        env = patch.dict("os.environ", {"CLAUDE_CONFIG_DIR": self.home.name})
        env.start()
        self.addCleanup(env.stop)

    def client(self, mode):
        value = self.module.ClaudeACPClient(command=sys.executable,
            args=[str(PEER), mode], acp_cwd=self.home.name)
        self.addCleanup(value.close)
        return value

    def test_stream_and_legacy_compaction_do_not_create_tool_calls(self):
        client = self.client("text")
        stream = client.chat.completions.create(messages=[{"role": "user", "content": "test"}], stream=True, timeout=10)
        chunks = list(stream)
        text = "".join(chunk.choices[0].delta.content or "" for chunk in chunks if chunk.choices)
        self.assertEqual(text, "hello world")
        self.assertFalse(any(getattr(chunk.choices[0].delta, "tool_calls", None) for chunk in chunks if chunk.choices))
        self.assertTrue(client.is_closed)

    def test_session_new_advertises_hermes_bridge_on_the_acp_mcp_servers_list(self):
        client = self.client("bridge")
        response = client.chat.completions.create(messages=[{"role": "user", "content": "test"}], tools=TOOLS, timeout=10)
        self.assertEqual(response.choices[0].message.tool_calls[0].function.name, "probe")
        self.assertTrue(client.is_closed)

    def test_native_bash_permission_is_rejected_not_cancelled(self):
        client = self.client("permission_bash")
        response = client.chat.completions.create(messages=[{"role": "user", "content": "test"}], tools=TOOLS, timeout=10)
        self.assertEqual(response.choices[0].message.content.strip(), "rejected-native-bash")
        self.assertIsNone(response.choices[0].message.tool_calls)
        self.assertTrue(client.is_closed)

    def test_new_tool_name_and_metadata_only_update_require_real_bridge_capture(self):
        client = self.client("bridge")
        response = client.chat.completions.create(messages=[{"role": "user", "content": "test"}], tools=TOOLS, timeout=10)
        choice = response.choices[0]
        self.assertEqual(choice.finish_reason, "tool_calls")
        self.assertEqual(choice.message.tool_calls[0].function.name, "probe")
        self.assertEqual(json.loads(choice.message.tool_calls[0].function.arguments), {"value": "checked"})
        self.assertTrue(client.is_closed)

    def test_forged_bridge_event_without_capture_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "not proven by the Hermes bridge capture"):
            self.client("forged").chat.completions.create(messages=[{"role": "user", "content": "test"}], tools=TOOLS, timeout=10)

    def test_stream_close_reaps_adapter(self):
        client = self.client("hang")
        stream = client.chat.completions.create(messages=[{"role": "user", "content": "test"}], stream=True, timeout=10)
        # Consume until the fixture has actually entered its long-running prompt.
        for chunk in stream:
            if chunk.choices[0].delta.content == "ready":
                break
        process = client._process
        self.assertIsNotNone(process)
        start = time.monotonic()
        stream.close()
        self.assertLess(time.monotonic() - start, 4)
        self.assertIsNotNone(process.poll())

    def test_prompt_deadline_reaps_adapter(self):
        client = self.client("hang")
        with self.assertRaises(TimeoutError):
            client.chat.completions.create(messages=[{"role": "user", "content": "test"}], timeout=0.5)
        self.assertTrue(client.is_closed)
        self.assertIsNone(client._process)

    def test_model_ack_that_does_not_match_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "did not apply the requested model"):
            self.client("wrong_model").chat.completions.create(
                model="claude-b", messages=[{"role": "user", "content": "test"}], timeout=10)

    def test_model_ack_that_matches_succeeds(self):
        client = self.client("model_ok")
        response = client.chat.completions.create(
            model="claude-b", messages=[{"role": "user", "content": "test"}], timeout=10)
        self.assertEqual(response.choices[0].message.content, "hello world")
        self.assertTrue(client.is_closed)

    def test_model_ack_missing_model_option_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "did not acknowledge the requested model"):
            self.client("missing_model").chat.completions.create(
                model="claude-b", messages=[{"role": "user", "content": "test"}], timeout=10)

    def test_result_frame_without_result_or_error_fails_fast(self):
        start = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "no result"):
            self.client("malformed_result").chat.completions.create(
                messages=[{"role": "user", "content": "test"}], timeout=10)
        self.assertLess(time.monotonic() - start, 2)

    def test_closed_stdout_is_a_transport_failure_not_a_timeout(self):
        client = self.client("close_stdout")
        start = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            client.chat.completions.create(messages=[{"role": "user", "content": "test"}], timeout=10)
        self.assertLess(time.monotonic() - start, 3)
        self.assertTrue(client.is_closed)
        self.assertIsNone(client._process)

    def test_preflight_version_check_is_bounded_by_request_deadline(self):
        client = self.client("hang")
        with self.assertRaises(TimeoutError):
            client.chat.completions.create(messages=[{"role": "user", "content": "test"}], timeout=0.5)
        guard = self.module.assert_reviewed_claude_code_version
        guard.assert_called_once()
        budget = guard.call_args.kwargs.get("timeout")
        self.assertIsNotNone(budget)
        self.assertGreater(budget, 0)
        self.assertLessEqual(budget, 0.5)


if __name__ == "__main__":
    unittest.main()
