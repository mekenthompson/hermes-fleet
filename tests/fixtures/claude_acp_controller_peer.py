"""Credential-free protocol fixture. Never executes a native tool."""
import json
import sys

scenario = json.loads(sys.argv[1])
session = "native-fixture"
prompt_id = None

def send(value):
    print(json.dumps(value), flush=True)

def permission():
    send({"jsonrpc": "2.0", "id": "permission", "method": "session/request_permission",
          "params": {"sessionId": scenario.get("session", session),
                     "toolCall": {"toolCallId": "native-1", "rawInput": {"query": "fixture"},
                                  "_meta": {"claudeCode": {"toolName": scenario.get("tool", "mcp__claude_ai_Example__search")}}},
                     "options": [{"optionId": "yes", "kind": "allow_once"},
                                 {"optionId": "no", "kind": "reject_once"},
                                 {"optionId": "always", "kind": "allow_always"}]}})

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if message.get("id") == "permission":
        outcome = message["result"]["outcome"]
        if scenario.get("receipt"):
            with open(scenario["receipt"], "w") as stream:
                json.dump(outcome, stream)
        if prompt_id is not None:
            send({"method": "session/update", "params": {"sessionId": session, "update": {
                "sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": json.dumps(outcome)}}}})
            send({"id": prompt_id, "result": {"stopReason": "end_turn"}})
        continue
    if method == "initialize":
        result = {"protocolVersion": 1}
    elif method == "session/new":
        result = {"sessionId": session, "configOptions": [{"id": "model", "category": "model", "currentValue": "fixture",
                  "options": [{"value": "fixture"}]}]}
    elif method == "session/set_config_option":
        if scenario.get("early"):
            permission()
            continue
        result = {"configOptions": [{"id": "model", "currentValue": "fixture"}]}
    elif method == "session/prompt":
        prompt_id = message["id"]
        for event in scenario.get("events", []):
            send({"method": "session/update", "params": {"sessionId": scenario.get("session", session), "update": event}})
        if scenario.get("permission", True):
            permission()
            continue
        result = {"stopReason": "end_turn"}
    elif method == "session/cancel":
        continue
    else:
        result = {}
    send({"id": message["id"], "result": result})
