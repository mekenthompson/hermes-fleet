"""Credential-free wire regressions; authenticated inference is a separate gate."""
import asyncio
import base64
import inspect
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from tests.test_claude_acp_connector_hydration import load_client

PEER = Path(__file__).parent / "fixtures/claude_acp_wire_peer.py"
TOOLS = [{"type": "function", "function": {"name": "probe", "parameters": {
    "type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"],
}}}]
RED_PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"
GREEN_PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGNg+M8AAAICAQB7CYF4AAAAAElFTkSuQmCC"


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

    def test_async_completion_is_awaitable_and_does_not_block_the_event_loop(self):
        client = self.client("text")
        release = threading.Event()

        def slow_completion(*_args, **_kwargs):
            release.wait(0.2)
            return "async-result"

        with patch.object(client, "_complete", side_effect=slow_completion):
            async def invoke():
                heartbeat = asyncio.Event()
                asyncio.get_running_loop().call_soon(heartbeat.set)
                pending = client.chat.completions.create(
                    messages=[{"role": "user", "content": "test"}], timeout=1
                )
                self.assertTrue(inspect.isawaitable(pending))
                result = await pending
                self.assertTrue(heartbeat.is_set())
                return result

            self.assertEqual(asyncio.run(invoke()), "async-result")

    def test_async_text_completion_remains_compatible(self):
        client = self.client("text")

        async def invoke():
            return await client.chat.completions.create(
                messages=[{"role": "user", "content": "test"}], timeout=10
            )

        response = asyncio.run(invoke())
        self.assertEqual(response.choices[0].message.content, "hello world")
        self.assertTrue(client.is_closed)

    def test_async_stream_is_awaitable_and_iterable(self):
        client = self.client("text")

        async def invoke():
            pending = client.chat.completions.create(
                messages=[{"role": "user", "content": "test"}], stream=True, timeout=10
            )
            if not inspect.isawaitable(pending):
                pending.close()
                self.fail("streaming completion must be awaitable in an event loop")
            stream = await pending
            chunks = []
            async for chunk in stream:
                chunks.append(chunk)
            return "".join(chunk.choices[0].delta.content or "" for chunk in chunks if chunk.choices)

        self.assertEqual(asyncio.run(invoke()), "hello world")
        self.assertTrue(client.is_closed)

    def test_async_cancellation_reaps_adapter_process(self):
        client = self.client("hang")

        async def invoke():
            loop = asyncio.get_running_loop()
            task = asyncio.create_task(client.chat.completions.create(
                messages=[{"role": "user", "content": "test"}], timeout=2
            ))
            deadline = loop.time() + 1.5
            while client._process is None and loop.time() < deadline:
                await asyncio.sleep(0.01)
            process = client._process
            self.assertIsNotNone(process, "async worker never started the ACP adapter")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            return process

        process = asyncio.run(invoke())
        self.assertIsNotNone(process.poll(), "cancelled async request left the ACP process alive")
        self.assertIsNone(client._process)

    def test_async_image_prompt_preserves_multiple_ordered_image_blocks_on_the_wire(self):
        client = self.client("images")
        messages = [{"role": "user", "content": [
            {"type": "text", "text": "Inspect these in order: "},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{RED_PNG}"}},
            {"type": "text", "text": " then compare with "},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{GREEN_PNG}"}},
            {"type": "text", "text": "."},
        ]}]

        async def invoke():
            return await client.chat.completions.create(messages=messages, timeout=10)

        response = asyncio.run(invoke())
        # The fixture asserts the exact ACP request and returns no model text.
        self.assertEqual(response.choices[0].message.content, "")
        self.assertTrue(client.is_closed)

    def test_image_prompt_requires_negotiated_agent_capability(self):
        client = self.client("no_image_capability")
        messages = [{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{RED_PNG}"}},
        ]}]

        async def invoke():
            return await client.chat.completions.create(messages=messages, timeout=10)

        with self.assertRaisesRegex(RuntimeError, "does not advertise image prompt support"):
            asyncio.run(invoke())
        self.assertTrue(client.is_closed)

    def test_image_payloads_reject_remote_urls_malformed_base64_and_unsupported_mime(self):
        cases = (
            ("https://example.invalid/image.png", "only base64 data:image URLs are supported"),
            ("data:image/png;base64,not base64!", "malformed base64 image payload"),
            ("data:image/tiff;base64,AA==", "unsupported image MIME type"),
        )
        for url, error in cases:
            with self.subTest(url=url):
                client = self.client("text")
                messages = [{"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": url}},
                ]}]
                with self.assertRaisesRegex(ValueError, error):
                    client.chat.completions.create(messages=messages, timeout=10)
                self.assertIsNone(client._process, "invalid image started an ACP subprocess")

    def test_image_payload_size_uses_the_existing_four_mib_core_ceiling(self):
        client = self.client("text")
        oversized = base64.b64encode(b"x" * (4 * 1024 * 1024 + 1)).decode("ascii")
        messages = [{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{oversized}"}},
        ]}]
        with self.assertRaisesRegex(ValueError, "exceeds the 4 MiB image limit"):
            client.chat.completions.create(messages=messages, timeout=10)
        self.assertIsNone(client._process)


if __name__ == "__main__":
    unittest.main()
