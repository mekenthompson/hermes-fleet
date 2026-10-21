"""A tool-less one-shot pays no never-read message cache write.

Hermes' auxiliary calls (compression summaries, titles) send one user turn and no tools. Native marks
that turn for its 1-hour cache, but no later request replays it, so the mark only bought a 2x input
write. These fixtures use native's real 2.1.280 opening layout.
"""
import json

from admission import drop_message_breakpoints

MARKER = {'type': 'ephemeral', 'ttl': '1h'}
POLICY = [{'type': 'text', 'text': 'You are Claude Code.', 'cache_control': MARKER},
          {'type': 'text', 'text': 'Summarize the transcript in the next user message.', 'cache_control': MARKER}]
TOOLS = [{'name': 'mcp__hermes__probe', 'input_schema': {'type': 'object'}, 'cache_control': MARKER}]
REMINDER = {'type': 'text', 'text': '<system-reminder>\n# userEmail\na@example.invalid\n</system-reminder>\n'}
FRAME = {'type': 'text', 'text': '<transcript>' + ' earlier turn' * 200 + '</transcript>'}


def opening(**extra):
    return {'model': 'm', 'system': POLICY, **extra, 'messages': [
        {'role': 'user', 'content': [REMINDER, FRAME]},
        {'role': 'system', 'content': [{'type': 'text', 'text': "Today's date is 2026-10-16.", 'cache_control': MARKER}]},
    ]}


def marks(body, key):
    return [b for m in body[key] for b in (m['content'] if key == 'messages' else [m]) if 'cache_control' in b]


def test_message_marks_go_and_system_and_tools_marks_stay():
    sent = opening(tools=TOOLS)
    out = json.loads(drop_message_breakpoints(json.dumps(sent).encode()))
    assert marks(out, 'messages') == []
    assert out['system'] == POLICY and out['tools'] == TOOLS
    strip = lambda body: [[{k: v for k, v in b.items() if k != 'cache_control'} for b in m['content']] for m in body['messages']]
    assert strip(out) == strip(sent)


def test_a_payload_without_message_marks_forwards_byte_identical():
    raw = json.dumps({'model': 'm', 'system': POLICY, 'messages': [{'role': 'user', 'content': [FRAME]}]}).encode()
    assert drop_message_breakpoints(raw) is raw


def test_a_payload_that_does_not_parse_forwards_as_is():
    for raw in (b'not json', json.dumps({'model': 'm'}).encode()):
        assert drop_message_breakpoints(raw) is raw


def test_only_hermes_auxiliary_calls_are_marked(profile):
    # Hermes' call_llm passes no conversation context; its chat path always passes session_id (even None).
    aux = profile.build_api_kwargs_extras(reasoning_config={'effort': 'medium'}, supports_reasoning=True,
                                          model='claude-opus-5-5[1m]', base_url='process://claude')
    chat = profile.build_api_kwargs_extras(reasoning_config=None, supports_reasoning=False, model='claude-opus-5-5[1m]',
                                           base_url='process://claude', session_id=None, cache_scope_id=None)
    assert aux == ({'reasoning': {'effort': 'medium'}}, {'hermes_auxiliary': True})
    assert chat == ({}, {})
