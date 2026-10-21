"""No credentials or business operations: deterministic connector ACP peer."""
import json
import sys

NAME = 'mcp__claude_ai_Example__read'
sid = 'fixture'
prompt_id = None
repeat = sys.argv[1] == 'repeat'
call_id = 'native-1'

def send(data):
    print(json.dumps(data), flush=True)

def event(name, identity, kind, **data):
    update = {'sessionUpdate': kind, 'toolCallId': identity,
              '_meta': {'claudeCode': {'toolName': name}}, **data}
    send({'method': 'session/update', 'params': {'sessionId': sid, 'update': update}})

def permission(identity):
    event(NAME, identity, 'tool_call', status='pending', rawInput={'count': 1})
    send({'id': identity, 'method': 'session/request_permission', 'params': {
        'sessionId': sid, 'toolCall': {'toolCallId': identity, 'rawInput': {'count': 1},
        '_meta': {'claudeCode': {'toolName': NAME, 'mcpServer': {'source': 'claudeai', 'name': 'claude_ai_Example'}}}},
        'options': [{'optionId': 'yes', 'kind': 'allow_once'}, {'optionId': 'no', 'kind': 'reject_once'}]}})

for line in sys.stdin:
    m = json.loads(line)
    method = m.get('method')
    result = {}
    if m.get('id') in {'native-1', 'native-2'} and method is None:
        allowed = m['result']['outcome'].get('optionId') == 'yes'
        expected = m['id'] == 'native-1'
        assert allowed == expected
        event(NAME, m['id'], 'tool_call_update', status='completed' if allowed else 'failed', rawOutput={'value': 42} if allowed else {'denied': True})
        if repeat and allowed:
            permission('native-2')
            continue
        send({'id': prompt_id, 'result': {'stopReason': 'end_turn'}})
        continue
    if method == 'initialize':
        result = {'protocolVersion': 1}
    elif method == 'session/new':
        options = m['params']['_meta']['claudeCode']['options']
        assert options['settings']['permissions']['ask'] == ['mcp__*']
        assert options['allowedTools'] == ['ToolSearch']
        assert 'Bash' in options['disallowedTools']
        assert options['settings']['disableAllHooks'] is True
        assert options['env']['MCP_CONNECTION_NONBLOCKING'] == '0'
        result = {'sessionId': sid, 'configOptions': [{'id': 'model', 'category': 'model', 'currentValue': 'sonnet', 'options': [{'value': 'sonnet'}]}]}
    elif method == 'session/set_config_option':
        result = {'configOptions': [{'id': 'model', 'currentValue': 'sonnet'}]}
    elif method == 'session/prompt':
        prompt_id = m['id']
        if sys.argv[1] == 'no_execution':
            send({'method': 'session/update', 'params': {'sessionId': sid, 'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'Tool succeeded, I promise.'}}}})
        else:
            event('ToolSearch', 'search-1', 'tool_call', rawInput={'query': 'fixture'}, status='in_progress')
            event('ToolSearch', 'search-1', 'tool_call_update', status='completed', rawOutput=[{'type': 'tool_reference', 'tool_name': NAME}])
            text = ''.join(b.get('text', '') for b in m['params']['prompt'])
            if '"tool":' in text:
                permission('native-1')
                continue
        result = {'stopReason': 'end_turn'}
    elif method == 'session/cancel':
        continue
    send({'id': m['id'], 'result': result})
