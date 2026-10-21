"""Narrow one-use permission controller for Claude.ai connector references."""
from __future__ import annotations
import json
import re

_NAME = re.compile(r"^mcp__claude_ai_[A-Za-z0-9_-]{1,64}__[A-Za-z0-9_-]{1,64}$")

class OneUseConnectorController:
    def __init__(self, session_id: str, expected: dict[str, object]):
        if not isinstance(session_id, str) or not session_id:
            raise ValueError('active session required')
        self.session_id = session_id
        self.expected = {k: _canonical(v) for k, v in expected.items() if isinstance(k, str) and _NAME.fullmatch(k)}
        self.consumed: set[tuple[str, str]] = set()

    def on_permission(self, session_id: str, params: dict) -> dict:
        if session_id != self.session_id or params.get('sessionId') != self.session_id:
            raise ValueError('permission is outside the active session')
        tool = params.get('toolCall')
        if not isinstance(tool, dict) or 'rawInput' not in tool or not isinstance(tool['rawInput'], dict):
            raise ValueError('raw native input required')
        meta = tool.get('_meta') or {}
        claude = meta.get('claudeCode') if isinstance(meta, dict) else None
        name = claude.get('toolName') if isinstance(claude, dict) else None
        call_id = tool.get('toolCallId')
        if not isinstance(name, str) or not _NAME.fullmatch(name) or name not in self.expected:
            raise ValueError('connector reference is not authorized')
        if _canonical(tool['rawInput']) != self.expected[name]:
            raise ValueError('native arguments differ from approved arguments')
        if not isinstance(call_id, str) or not call_id:
            raise ValueError('native call id required')
        key = (name, call_id)
        if key in self.consumed or any(n == name for n, _ in self.consumed):
            raise ValueError('permission budget already consumed')
        options = params.get('options')
        offered = next((o for o in options if isinstance(o, dict) and o.get('kind') == 'allow_once' and (o.get('optionId') or o.get('id'))), None) if isinstance(options, list) else None
        if not offered:
            raise ValueError('adapter did not offer allow_once')
        self.consumed.add(key)
        return {'optionId': offered.get('optionId') or offered.get('id'), 'kind': 'allow_once'}

def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)
