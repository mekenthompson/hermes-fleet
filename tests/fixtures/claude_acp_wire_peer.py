"""Deterministic ACP wire peer, not a Claude/model substitute.

Exercises Fleet's subprocess and trusted MCP bridge with ACP 0.78 event shapes.
"""
import json
import socket
import sys
import time

mode = sys.argv[1]
session_id = "fixture-session"
options = {}


def send(value):
    print(json.dumps(value), flush=True)


def update(value):
    send({"jsonrpc": "2.0", "method": "session/update", "params": {
        "sessionId": session_id, "update": value,
    }})


for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    result = {}
    if method == "initialize":
        assert message["params"]["clientCapabilities"] == {}
        result = {"protocolVersion": 1}
    elif method == "session/new":
        options = message["params"]["_meta"]["claudeCode"]["options"]
        assert options["allowDangerouslySkipPermissions"] is False
        result = {"sessionId": session_id}
    elif method == "session/prompt":
        if mode == "hang":
            update({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "ready"}})
            time.sleep(60)
        elif mode in {"bridge", "forged"}:
            if mode == "bridge":
                address = options["mcpServers"]["hermes_bridge"]["args"][-1]
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as peer:
                    peer.connect(address)
                    peer.sendall((json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
                        "name": "probe", "arguments": {"value": "checked"},
                    }}) + "\n").encode())
                    with peer.makefile("rb") as reader:
                        assert "result" in json.loads(reader.readline())
            update({"sessionUpdate": "tool_call", "toolCallId": "call-1", "name": "mcp__hermes_bridge__probe",
                    "_meta": {"claudeCode": {"toolName": "mcp__hermes_bridge__probe"}},
                    "rawInput": {"value": "checked"}, "status": "in_progress"})
            # Upstream update frames need not repeat the new standard name field.
            update({"sessionUpdate": "tool_call_update", "toolCallId": "call-1",
                    "_meta": {"claudeCode": {"toolName": "mcp__hermes_bridge__probe"}}, "status": "completed"})
        else:
            # With no compaction capability, ACP preserves synthetic tool events.
            update({"sessionUpdate": "tool_call", "toolCallId": "compact", "title": "Compacting context",
                    "status": "in_progress", "kind": "other"})
            update({"sessionUpdate": "tool_call_update", "toolCallId": "compact", "status": "completed"})
            for text in ("hello", " world"):
                update({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": text}})
        result = {"stopReason": "end_turn"}
    elif method == "session/cancel":
        continue
    send({"jsonrpc": "2.0", "id": message["id"], "result": result})
