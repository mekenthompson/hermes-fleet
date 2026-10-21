"""Optional plugin integration, without credentials or inference."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / 'plugins/claude-account-connectors/plugin.py'

def load():
    spec = importlib.util.spec_from_file_location('connector_under_test', PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

class ConnectorTests(unittest.TestCase):
    def test_native_backend_is_present_and_registration_is_default_off(self):
        module = load()
        self.assertTrue(hasattr(module, 'NativeBackend'))
        class Context:
            def get_config(self, key, default=None): return default
            def register_tool(self, **kw): raise AssertionError('default must not register')
        module.register(Context())

    def test_gate_enforces_once_arguments_provenance_and_actual_results(self):
        module = load()
        name = 'mcp__claude_ai_Example__read'
        gate = module.NativeGate(name, {'count': 1}, lambda n: n == name)
        event = {'sessionUpdate': 'tool_call', 'toolCallId': 'one', 'rawInput': {'count': 1},
                 '_meta': {'claudeCode': {'toolName': name}}, 'status': 'pending'}
        gate.on_tool_event('session', event)
        params = {'sessionId': 'session', 'toolCall': dict(event),
                  'options': [{'optionId': 'yes', 'kind': 'allow_once'}, {'optionId': 'no', 'kind': 'reject_once'}]}
        params['toolCall']['_meta'] = {'claudeCode': {'toolName': name, 'mcpServer': {'source': 'claudeai'}}}
        params['toolCall']['rawInput'] = {'count': True}
        self.assertEqual(gate.on_permission('session', params)['optionId'], 'no')
        params['toolCall']['rawInput'] = {'count': 1}
        self.assertEqual(gate.on_permission('session', params)['optionId'], 'yes')
        self.assertEqual(gate.on_permission('session', params)['optionId'], 'no')
        gate.on_tool_event('session', dict(event, sessionUpdate='tool_call_update', status='completed', rawOutput={'answer': 2}))
        self.assertEqual(gate.result()['rawOutput'], {'answer': 2})

    def test_no_prose_success_and_unknown_after_approval(self):
        module = load()
        gate = module.NativeGate(None, None, lambda n: True)
        self.assertEqual(gate.result()['status'], 'no_execution')

    def test_registered_handlers_use_real_acp_transport(self):
        import tempfile
        from unittest.mock import patch
        module = load()
        acp = module._load_acp()
        original = acp.ClaudeACPClient
        name = 'mcp__claude_ai_Example__read'
        peer = ROOT / 'tests/fixtures/claude_connector_peer.py'
        for mode in ['normal', 'repeat', 'no_execution']:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as home:
                Path(home, 'settings.json').write_text(json.dumps({'permissions': {'allow': [name]}}))
                class FixtureClient(original):
                    def __init__(self, **kwargs):
                        super().__init__(command=sys.executable, args=[str(peer), mode], acp_cwd=home)
                with patch.object(acp, 'ClaudeACPClient', FixtureClient), patch.object(acp, 'assert_reviewed_claude_code_version'), patch.dict('os.environ', {'CLAUDE_CONFIG_DIR': home}), patch.object(module, '_load_acp', return_value=acp), patch.object(module, '_capture_cancel_event', side_effect=__import__('threading').Event):
                    class Context:
                        def __init__(self): self.tools = {}
                        def get_config(self, key, default=None): return True if key == 'enabled' else default
                        def register_tool(self, **kw): self.tools[kw['name']] = kw['handler']
                    context = Context()
                    module.register(context)
                    search = json.loads(context.tools['claude_connector_search']({'query': 'fixture'}))
                    call = json.loads(context.tools['claude_connector_call']({'tool': name, 'arguments': {'count': 1}}))
                    if mode == 'no_execution':
                        self.assertEqual(search['status'], 'no_execution')
                        self.assertEqual(call['status'], 'no_execution')
                    else:
                        self.assertEqual(search['references'], [name])
                        self.assertFalse(search['schemas_exported'])
                        self.assertEqual(call['status'], 'completed', call)
                        self.assertEqual(call['rawOutput'], {'value': 42})
                    denied = json.loads(context.tools['claude_connector_call']({'tool': 'mcp__claude_ai_Other__read', 'arguments': {}}))
                    self.assertEqual(denied['status'], 'denied')

    def test_transport_cancellation_before_and_after_approval(self):
        import tempfile
        import threading
        from unittest.mock import patch
        module = load()
        acp = module._load_acp()
        base = acp.ClaudeACPClient
        name = 'mcp__claude_ai_Example__read'
        peer = ROOT / 'tests/fixtures/claude_connector_peer.py'
        for phase in ['before', 'after']:
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as home:
                Path(home, 'settings.json').write_text(json.dumps({'permissions': {'allow': [name]}}))
                cancelled = threading.Event()
                if phase == 'before': cancelled.set()
                class FixtureClient(base):
                    def __init__(self, **kwargs):
                        super().__init__(command=sys.executable, args=[str(peer), 'normal'], acp_cwd=home)
                original = module.NativeGate.on_permission
                def permission(gate, session, params):
                    decision = original(gate, session, params)
                    if decision['kind'] == 'allow_once': cancelled.set()
                    return decision
                with patch.object(acp, 'ClaudeACPClient', FixtureClient), patch.object(acp, 'assert_reviewed_claude_code_version'), patch.dict('os.environ', {'CLAUDE_CONFIG_DIR': home}), patch.object(module, '_capture_cancel_event', return_value=cancelled), patch.object(module.NativeGate, 'on_permission', permission):
                    result = module.NativeBackend(acp=acp).run(tool=name, arguments={'count': 1})
                self.assertEqual(result['status'], 'no_execution' if phase == 'before' else 'unknown_outcome')
                self.assertFalse(result['retry'])

    def test_cancel_event_captures_trusted_thread_and_context(self):
        import contextvars
        import threading
        import types
        from unittest.mock import patch
        module = load()
        interrupted = set()
        owner = contextvars.ContextVar('test_owner', default=None)
        api = types.ModuleType('tools.interrupt')
        api.acting_for_tid = owner
        api.is_thread_interrupted = lambda tid: tid in interrupted
        with patch.dict(sys.modules, {'tools.interrupt': api}):
            token = owner.set(123)
            try: event = module._capture_cancel_event()
            finally: owner.reset(token)
            interrupted.add(456)
            self.assertFalse(event.is_set())
            interrupted.add(123)
            self.assertTrue(event.is_set())
            interrupted.clear()
            interrupted.add(threading.get_ident())
            self.assertTrue(event.is_set())
            interrupted.clear()
            event.set()
            self.assertTrue(event.is_set())

    def test_reference_shape_is_native_not_model_schema(self):
        module = load()
        self.assertEqual(module._refs({'rawOutput': [{'type': 'tool_reference', 'tool_name': 'mcp__claude_ai_Example__read'}]}), ['mcp__claude_ai_Example__read'])

if __name__ == '__main__': unittest.main()
