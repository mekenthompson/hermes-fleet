import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from controller import OneUseConnectorController

class ControllerTests(unittest.TestCase):
    def test_exact_name_args_and_session_are_one_use(self):
        c = OneUseConnectorController('session-1', {'mcp__claude_ai_drive__search': {'query': 'cats'}})
        params = {'sessionId':'session-1','toolCall':{'toolCallId':'call-1','_meta':{'claudeCode':{'toolName':'mcp__claude_ai_drive__search'}},'rawInput':{'query':'cats'}},'options':[{'optionId':'yes','kind':'allow_once'}]}
        self.assertEqual(c.on_permission('session-1', params), {'optionId':'yes','kind':'allow_once'})
        with self.assertRaises(ValueError): c.on_permission('session-1', params)
    def test_wrong_args_and_malformed_native_refused(self):
        c = OneUseConnectorController('s', {'mcp__claude_ai_drive__search': {'query': 1}})
        for name, raw in [('mcp__claude_ai_drive__search', {'query': True}), ('Bash', {})]:
            params={'sessionId':'s','toolCall':{'toolCallId':'c','_meta':{'claudeCode':{'toolName':name}},'rawInput':raw},'options':[{'optionId':'y','kind':'allow_once'}]}
            with self.assertRaises(ValueError): c.on_permission('s',params)
    def test_registration_is_explicitly_opt_in(self):
        from plugin import register, ConnectorTools
        tools=ConnectorTools(search=lambda q: {}, invoke=lambda t,a: {'text':'fabricated'})
        self.assertEqual(tools.claude_connector_call('mcp__claude_ai_drive__search', {}), {'status':'no_execution','retry':False,'reason':'no_correlated_native_output'})
        tools=ConnectorTools(search=lambda q: {}, invoke=lambda t,a: {'rawOutput':{'items':[]}})
        self.assertEqual(tools.claude_connector_call('mcp__claude_ai_drive__search', {}), {'status':'completed','rawOutput':{'items':[]}})
        class C:
            def __init__(self): self.tools=[]
            def register_tool(self, **kw): self.tools.append(kw)
        c=C(); register(c, {})
        self.assertEqual(c.tools, [])
        register(c, {'enabled':True, '_backend': lambda *args: {'rawOutput': []}})
        self.assertEqual({x['name'] for x in c.tools}, {'claude_connector_search','claude_connector_call'})

if __name__ == '__main__': unittest.main()
