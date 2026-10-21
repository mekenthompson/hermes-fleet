"""Transport-level coverage of the internal seam, not connector acceptance."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from tests.test_claude_acp_connector_hydration import load_client

PEER = Path(__file__).parent / "fixtures/claude_acp_controller_peer.py"
EVENT = {"sessionUpdate": "tool_call", "toolCallId": "native-1",
         "_meta": {"claudeCode": {"toolName": "mcp__claude_ai_Example__search"}},
         "rawInput": {"query": "fixture"}, "status": "pending"}

class Controller:
    def __init__(self, decision=None, fail=False):
        self.events = []
        self.decision = decision or {"optionId": "yes", "kind": "allow_once"}
        self.fail = fail

    def on_permission(self, session_id, params):
        if self.fail:
            raise RuntimeError("test callback failure")
        # A mutation must not change the offered options used by the host.
        params["options"].append({"optionId": "forged", "kind": "allow_once"})
        return self.decision

    def on_tool_event(self, session_id, event):
        self.events.append(event)
        event["rawInput"] = {"mutated": True}

class NativeControllerTests(unittest.TestCase):
    def setUp(self):
        self.module = load_client()
        self.home = tempfile.TemporaryDirectory()
        self.addCleanup(self.home.cleanup)
        for context in [patch.object(self.module, "assert_reviewed_claude_code_version"),
                        patch.dict("os.environ", {"CLAUDE_CONFIG_DIR": self.home.name})]:
            context.start()
            self.addCleanup(context.stop)

    def client(self, scenario, factory=None):
        base = self.module.ClaudeACPClient
        class Trusted(base):
            def _new_native_controller(self):
                return factory() if factory else None
        client = Trusted(command=sys.executable, args=[str(PEER), json.dumps(scenario)], acp_cwd=self.home.name)
        self.addCleanup(client.close)
        return client

    def complete(self, client, model=None):
        return client.chat.completions.create(model=model, messages=[{"role": "user", "content": "fixture"}], timeout=3)

    def test_default_rejects_connector(self):
        result = self.complete(self.client({}))
        self.assertEqual(json.loads(result.choices[0].message.content)["optionId"], "no")

    def test_valid_once_and_native_rejection(self):
        for tool, expected in [("mcp__claude_ai_Example__search", "yes"), ("Bash", "no"), ("mcp__hermes_bridge__probe", "no")]:
            with self.subTest(tool=tool):
                result = self.complete(self.client({"tool": tool}, Controller))
                self.assertEqual(json.loads(result.choices[0].message.content)["optionId"], expected)

    def test_always_unoffered_and_mutated_option_rejected(self):
        for decision in [{"optionId": "always", "kind": "allow_always"},
                         {"optionId": "missing", "kind": "allow_once"},
                         {"optionId": "forged", "kind": "allow_once"}]:
            with self.subTest(decision=decision):
                client = self.client({}, lambda: Controller(decision))
                with self.assertRaisesRegex(RuntimeError, "unsafe permission"):
                    self.complete(client)
                self.assertTrue(client.is_closed)

    def test_callback_error_sends_cancel_before_cleanup(self):
        client = self.client({}, lambda: Controller(fail=True))
        sent = []
        original = client._send
        def record(proc, value):
            sent.append(value)
            return original(proc, value)
        client._send = record
        with self.assertRaisesRegex(RuntimeError, "test callback failure"):
            self.complete(client)
        responses = [v for v in sent if v.get("id") == "permission"]
        self.assertEqual(responses, [{"jsonrpc": "2.0", "id": "permission", "result": {"outcome": {"outcome": "cancelled"}}}])
        self.assertTrue(client.is_closed)

    def test_wrong_session_and_early_permission(self):
        for scenario, model, error in [({"session": "wrong"}, None, "active session"), ({"early": True}, "fixture", "active prompt")]:
            for factory in [None, Controller]:
                with self.subTest(scenario=scenario, factory=factory):
                    client = self.client(scenario, factory)
                    with self.assertRaisesRegex(RuntimeError, error):
                        self.complete(client, model)
                    self.assertTrue(client.is_closed)

    def test_malformed_event_fields(self):
        for changed, error in [({"toolCallId": ""}, "toolCallId"), ({"_meta": {}}, "tool name"), ({"rawInput": []}, "rawInput")]:
            with self.subTest(changed=changed):
                client = self.client({"events": [dict(EVENT, **changed)], "permission": False}, Controller)
                with self.assertRaisesRegex(RuntimeError, error):
                    self.complete(client)
                self.assertTrue(client.is_closed)

    def test_cumulative_event_bound(self):
        size = len(json.dumps(EVENT, ensure_ascii=True, separators=(",", ":")).encode())
        controller = Controller()
        client = self.client({"events": [EVENT, EVENT], "permission": False}, lambda: controller)
        with patch.object(self.module, "_MAX_RESPONSE_CHARS", size + 1):
            with self.assertRaisesRegex(RuntimeError, "size limit"):
                self.complete(client)
        self.assertEqual(len(controller.events), 1)

    def test_event_callback_failure_reaps_process(self):
        class Broken(Controller):
            def on_tool_event(self, session_id, event):
                raise ValueError("private callback detail")
        client = self.client({"events": [EVENT], "permission": False}, Broken)
        with self.assertRaisesRegex(RuntimeError, "native tool event controller failed"):
            self.complete(client)
        self.assertTrue(client.is_closed)

    def test_fresh_controller_for_each_run_and_mutation_isolated(self):
        created = []
        def factory():
            controller = Controller()
            created.append(controller)
            return controller
        client = self.client({"events": [EVENT], "permission": False}, factory)
        for _ in range(2):
            self.complete(client)
        self.assertEqual(len(created), 2)
        self.assertIsNot(created[0], created[1])
        self.assertEqual([len(c.events) for c in created], [1, 1])
        self.assertEqual(EVENT["rawInput"], {"query": "fixture"})
