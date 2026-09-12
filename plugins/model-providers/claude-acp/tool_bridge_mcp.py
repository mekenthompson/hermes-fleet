#!/usr/bin/env python3
"""Non-executing MCP server exposing Hermes tool schemas to Claude.

The trusted server is launched directly by the Hermes-side ACP client. Claude's
adapter launches only a stdio proxy that connects to the server's private Unix
socket. Valid MCP calls are reported on the server's private stdout; neither the
adapter nor the proxy receives that capture channel, and this process never
imports or invokes Hermes tools.
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import socket
import sys
import threading
from pathlib import Path
from typing import Any, BinaryIO

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

_MAX_FRAME_BYTES = 1024 * 1024
_MAX_SCHEMA_BYTES = 1024 * 1024
_MAX_TOOLS = 128
_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_SUPPORTED_PROTOCOL_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")


def _load_tools(path: str) -> list[dict[str, Any]]:
    source = Path(path)
    if source.stat().st_size > _MAX_SCHEMA_BYTES:
        raise ValueError("tool schema file is too large")
    entries = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or len(entries) > _MAX_TOOLS:
        raise ValueError("tool schema file must contain a bounded list")
    tools: list[dict[str, Any]] = []
    for entry in entries:
        function = entry.get("function") if isinstance(entry, dict) else None
        if not isinstance(function, dict):
            raise TypeError("tool entry must contain a function mapping")
        name = function.get("name")
        parameters = function.get("parameters") or {"type": "object", "properties": {}}
        if not isinstance(name, str) or not _TOOL_NAME_RE.fullmatch(name):
            raise ValueError("tool name is invalid")
        if not isinstance(parameters, dict):
            raise TypeError("tool input schema must be a mapping")
        Draft202012Validator.check_schema(parameters)
        tools.append({
            "name": name,
            "description": str(function.get("description") or "Hermes tool")[:4096],
            "inputSchema": parameters,
        })
    return tools


def _result(message_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def _error(message_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": message}}


def _serve_message(
    message: Any,
    tools: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not isinstance(message, dict):
        return _error(None, -32600, "Invalid Request"), None
    method = message.get("method")
    message_id = message.get("id")
    params = message.get("params") or {}
    if not isinstance(params, dict):
        return _error(message_id, -32602, "Invalid params"), None
    names = {tool["name"] for tool in tools}
    if method == "initialize":
        requested = params.get("protocolVersion")
        protocol = requested if requested in _SUPPORTED_PROTOCOL_VERSIONS else _SUPPORTED_PROTOCOL_VERSIONS[0]
        return _result(message_id, {
            "protocolVersion": protocol,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "hermes-tool-bridge", "version": "1.0.0"},
        }), None
    if method == "tools/list":
        return _result(message_id, {"tools": tools}), None
    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments")
        if not isinstance(name, str) or name not in names or not isinstance(arguments, dict):
            return _error(message_id, -32602, "Unknown tool or invalid arguments"), None
        schema = next(tool["inputSchema"] for tool in tools if tool["name"] == name)
        try:
            Draft202012Validator(schema).validate(arguments)
        except (SchemaError, ValidationError):
            return _error(message_id, -32602, "Arguments do not match the tool schema"), None
        capture = {"name": name, "arguments": arguments}
        encoded = json.dumps(capture, ensure_ascii=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > _MAX_FRAME_BYTES:
            return _error(message_id, -32602, "Tool call is too large"), None
        return _result(message_id, {
            "content": [{
                "type": "text",
                "text": "Tool request captured for execution by the host application.",
            }],
        }), capture
    if method == "ping":
        return _result(message_id, {}), None
    if message_id is None:
        return None, None
    return _error(message_id, -32601, "Method not found"), None


def _serve_stream(stream: BinaryIO, output: BinaryIO, tools: list[dict[str, Any]]) -> int:
    while True:
        raw = stream.readline(_MAX_FRAME_BYTES + 1)
        if not raw:
            return 0
        if len(raw) > _MAX_FRAME_BYTES:
            response = _error(None, -32600, "Request frame is too large")
            output.write(json.dumps(response, separators=(",", ":")).encode("utf-8") + b"\n")
            output.flush()
            return 2
        try:
            message = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            response, capture = _error(None, -32700, "Parse error"), None
        else:
            response, capture = _serve_message(message, tools)
        if capture is not None:
            sys.stdout.write(json.dumps(capture, ensure_ascii=True, separators=(",", ":")) + "\n")
            sys.stdout.flush()
        if response is not None:
            output.write(json.dumps(response, separators=(",", ":")).encode("utf-8") + b"\n")
            output.flush()


def _server(schema_path: str, socket_path: str) -> int:
    tools = _load_tools(schema_path)
    path = Path(socket_path)
    with contextlib.suppress(FileNotFoundError):
        path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(socket_path)
        os.chmod(socket_path, 0o600)
        server.listen(1)
        connection, _ = server.accept()
        with connection:
            stream = connection.makefile("rb")
            output = connection.makefile("wb")
            try:
                return _serve_stream(stream, output, tools)
            finally:
                stream.close()
                output.close()
    finally:
        server.close()
        with contextlib.suppress(FileNotFoundError):
            path.unlink()


def _proxy(socket_path: str) -> int:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.connect(socket_path)

    def forward_input() -> None:
        try:
            while True:
                data = os.read(sys.stdin.fileno(), 64 * 1024)
                if not data:
                    break
                connection.sendall(data)
        finally:
            with contextlib.suppress(OSError):
                connection.shutdown(socket.SHUT_WR)

    input_thread = threading.Thread(target=forward_input, daemon=True)
    input_thread.start()
    try:
        while True:
            data = connection.recv(64 * 1024)
            if not data:
                return 0
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
    finally:
        connection.close()


def main() -> int:
    try:
        if len(sys.argv) == 4 and sys.argv[1] == "--server":
            return _server(sys.argv[2], sys.argv[3])
        if len(sys.argv) == 3 and sys.argv[1] == "--proxy":
            return _proxy(sys.argv[2])
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
