"""Profile-private chat RPC to the single gateway-owned Linear bridge.

API/desktop tool handlers live in another process. They submit the host-owned
invocation context to this bridge; they never start another scheduler or worker.
"""
from __future__ import annotations
import contextvars
import json
import os
import socket
import socketserver
import stat
import struct
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

MAX_BYTES = 65536
CONTEXT_FIELDS = ('profile', 'platform', 'session_key', 'session_id', 'run_generation')

class Unavailable(RuntimeError):
    pass

class Uncertain(RuntimeError):
    pass

def endpoint(home: Path) -> Path:
    return Path(home) / 'linear-transport' / 'chat.sock'

def _private_parent(home: Path, *, create: bool = False) -> Path:
    home = Path(home).resolve(strict=True)
    path = endpoint(home).parent
    if create:
        path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise Unavailable('Linear transport directory is not private to this profile')
    return path

def request(home: Path, payload: dict[str, Any], *, timeout: float = 20.0) -> Any:
    sent = False
    try:
        path = _private_parent(home) / 'chat.sock'
        info = path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise Unavailable('Linear transport socket is not private to this profile')
        body = json.dumps(payload).encode() + b'\n'
        if len(body) > MAX_BYTES:
            raise Unavailable('Linear request is too large')
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout)
            client.connect(str(path))
            # Linux peer identity binds the listener to the profile's UID.
            if struct.unpack('3i', client.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[1] != os.geteuid():
                raise Unavailable('Linear service belongs to another user')
            sent = True
            client.sendall(body)
            with client.makefile('rb') as stream:
                response = stream.readline(MAX_BYTES + 1)
            if not response.endswith(b'\n') or len(response) > MAX_BYTES:
                raise ValueError('incomplete Linear response')
            result = json.loads(response)
            if not isinstance(result, dict) or set(result) != {'result'}:
                raise ValueError('invalid Linear response')
            return result['result']
    except Unavailable:
        raise
    except (OSError, ValueError) as exc:
        if sent:
            raise Uncertain('Linear request may have been captured; reconcile ownership before retrying') from exc
        raise Unavailable('The Linear gateway service is not available on this profile') from exc

class Server:
    def __init__(self, home: Path, profile: str, chat: Callable, turn_end: Callable) -> None:
        self.home = Path(home).resolve(strict=True)
        parent = _private_parent(self.home, create=True)
        self.path = parent / 'chat.sock'
        # A live listener is never displaced by another gateway/discovery.
        if self.path.exists():
            info = self.path.lstat()
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid():
                raise Unavailable('Existing Linear transport is unsafe')
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                probe.settimeout(1.0)
                try:
                    probe.connect(str(self.path))
                except ConnectionRefusedError:
                    self.path.unlink()
                else:
                    raise Unavailable('A Linear gateway service already owns this profile')
        scope = contextvars.copy_context()
        slots = threading.BoundedSemaphore(8)
        def handle(payload):
            op = payload.get('op')
            if op == 'status':
                return {'ok': True, 'profile': profile, 'pid': os.getpid(), 'ready': True}
            if payload.get('home') != str(self.home):
                raise ValueError('Linear request belongs to another profile home')
            if op == 'chat':
                context = payload.get('context'); args = payload.get('args')
                if not isinstance(context, dict) or set(context) != set(CONTEXT_FIELDS) or not isinstance(args, dict):
                    raise ValueError('Linear request has no bound invocation context')
                return chat(args, SimpleNamespace(**context))
            if op == 'turn_end' and isinstance(payload.get('session_id'), str):
                turn_end(payload['session_id'])
                return {'ok': True}
            raise ValueError('Unknown Linear transport operation')
        class Handler(socketserver.StreamRequestHandler):
            def handle(inner):
                if not slots.acquire(blocking=False):
                    return
                try:
                    inner.request.settimeout(20)
                    uid = struct.unpack('3i', inner.request.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[1]
                    if uid != os.geteuid():
                        return
                    raw = inner.rfile.readline(MAX_BYTES + 1)
                    if not raw.endswith(b'\n') or len(raw) > MAX_BYTES:
                        return
                    payload = json.loads(raw)
                    if not isinstance(payload, dict):
                        return
                    result = scope.copy().run(handle, payload)
                    body = json.dumps({'result': result}).encode() + b'\n'
                    if len(body) <= MAX_BYTES:
                        inner.wfile.write(body)
                except Exception:  # a failed capture has an uncertain outcome for the caller
                    # No exception or request body is reflected into another process.
                    return
                finally:
                    slots.release()
        class Listener(socketserver.ThreadingUnixStreamServer):
            daemon_threads = False
            block_on_close = True
        self.listener = Listener(str(self.path), Handler)
        os.chmod(self.path, 0o600)
        self.inode = self.path.lstat().st_ino
        self.thread = threading.Thread(target=self.listener.serve_forever, daemon=True)
        self.thread.start()
    def close(self) -> None:
        self.listener.shutdown()
        self.listener.server_close()
        self.thread.join(timeout=2)
        try:
            if self.path.lstat().st_ino == self.inode:
                self.path.unlink()
        except FileNotFoundError:
            pass

def chat_request(home: Path, args: dict[str, Any], context: Any) -> str:
    # Identity is copied from the immutable host argument, never from model args.
    snapshot = {field: getattr(context, field, None) for field in CONTEXT_FIELDS}
    return request(home, {'op': 'chat', 'home': str(Path(home).resolve(strict=True)),
                          'args': args, 'context': snapshot})
