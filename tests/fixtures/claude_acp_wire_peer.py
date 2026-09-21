"""Deterministic ACP wire peer, not a Claude/model substitute.

Exercises Fleet's subprocess and trusted MCP bridge with ACP 0.78 event shapes.
"""
import json
import os
import socket
import sys
import time

mode = sys.argv[1]
session_id = "fixture-session"
options = {}
prompt_id = None
pending_permission_id = None
MODEL_OPTION = {"id": "model", "category": "model", "currentValue": "claude-a",
                "options": [{"value": "claude-a"}, {"value": "claude-b"}]}
BASH_PERMISSION_OPTIONS = [
    {"optionId": "allow-once", "name": "Yes", "kind": "allow_once"},
    {"optionId": "reject", "name": "No", "kind": "reject_once"},
]


def send(value):
    print(json.dumps(value), flush=True)


def update(value):
    send({"jsonrpc": "2.0", "method": "session/update", "params": {
        "sessionId": session_id, "update": value,
    }})


for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if pending_permission_id is not None and message.get("id") == pending_permission_id:
        outcome = (message.get("result") or {}).get("outcome") or {}
        assert outcome.get("outcome") == "selected", message
        assert outcome.get("optionId") == "reject", message
        pending_permission_id = None
        update({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "rejected-native-bash"}})
        send({"jsonrpc": "2.0", "id": prompt_id, "result": {"stopReason": "end_turn"}})
        continue
    result = {}
    if method == "initialize":
        assert message["params"]["clientCapabilities"] == {}
        result = {"protocolVersion": 1}
    elif method == "session/new":
        options = message["params"]["_meta"]["claudeCode"]["options"]
        assert options["allowDangerouslySkipPermissions"] is False
        if mode in {"bridge", "forged", "permission_bash"}:
            servers = message["params"].get("mcpServers")
            assert isinstance(servers, list) and servers, "top-level mcpServers must advertise hermes_bridge"
            server = servers[0]
            assert server.get("name") == "hermes_bridge"
            assert "type" not in server
            assert server.get("command")
            assert server.get("args")
        result = {"sessionId": session_id}
        if mode in {"wrong_model", "model_ok", "missing_model"}:
            result["configOptions"] = [dict(MODEL_OPTION)]
    elif method == "session/set_config_option":
        requested = message["params"]["value"]
        if mode == "missing_model":
            result = {"configOptions": [{"id": "permissionMode", "currentValue": "default"}]}
        else:
            applied = "claude-a" if mode == "wrong_model" else requested
            result = {"configOptions": [dict(MODEL_OPTION, currentValue=applied)]}
    elif method == "session/prompt":
        prompt_id = message["id"]
        if mode == "permission_bash":
            pending_permission_id = "perm-1"
            send({"jsonrpc": "2.0", "id": pending_permission_id, "method": "session/request_permission",
                  "params": {
                      "sessionId": session_id,
                      "toolCall": {"toolCallId": "bash-1", "title": "Bash", "kind": "execute",
                                   "rawInput": {"command": "gh pr view 1"}},
                      "options": BASH_PERMISSION_OPTIONS,
                  }})
            continue
        if mode == "malformed_result":
            # Right id, but neither "result" nor "error": not a valid JSON-RPC reply.
            send({"jsonrpc": "2.0", "id": message["id"]})
            continue
        elif mode == "hang":
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
    if mode == "close_stdout" and method == "session/new":
        # Adapter drops its output pipe but stays alive.
        sys.stdout.close()
        os.close(1)
        time.sleep(60)
