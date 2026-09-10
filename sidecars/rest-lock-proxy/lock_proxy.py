"""HTTP 423 while a human takeover lock file exists. Per-connection."""
from __future__ import annotations

import fcntl
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Tuple
import urllib.error
import urllib.request

# The agent's browser tools stringify the HTTP error, so the reason phrase is
# what the model reads. Make it an instruction, not a bare word. 423 has one
# meaning only: a human currently has this browser.
LOCKED_REASON = (
    "Locked: a human currently has this browser. Do not retry browser actions; "
    "wait for the user with the clarify tool, then check browser_handoff_status once before resuming"
)
LOCKED_BODY = (LOCKED_REASON + "\n").encode()

# Camofox's own handler timeout is 30s; the proxy must never be the first to
# give up on an admitted request.
UPSTREAM_TIMEOUT = 45


def lock_is_active(lock_file: Path) -> bool:
    """Fail closed: an inaccessible lock cannot be treated as an unlock."""
    try:
        lock_file.stat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


class RestLockProxy:
    def __init__(
        self,
        listen: Tuple[str, int],
        target: str,
        lock_file: Path,
        *, upstream_timeout: float = UPSTREAM_TIMEOUT,
    ) -> None:
        self.lock_file = Path(lock_file)
        self.target = target
        self.upstream_timeout = upstream_timeout
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args) -> None:
                return

            def do_GET(self) -> None:  # noqa: N802
                self._handle()

            def do_POST(self) -> None:  # noqa: N802
                self._handle()

            def do_DELETE(self) -> None:  # noqa: N802
                self._handle()

            def _locked(self) -> None:
                self.send_response(423, LOCKED_REASON)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(LOCKED_BODY)))
                self.end_headers()
                self.wfile.write(LOCKED_BODY)

            def _handle(self) -> None:
                if lock_is_active(outer.lock_file):
                    self._locked()
                    return
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    self.send_error(400, "invalid Content-Length")
                    return
                if length < 0 or self.headers.get("Transfer-Encoding"):
                    self.send_error(400, "unsupported request framing")
                    return
                if length > 16 * 1024 * 1024:
                    self.send_error(413, "request body too large")
                    return
                self.connection.settimeout(10)
                try:
                    data = self.rfile.read(length) if length else None
                except TimeoutError:
                    self.send_error(408, "request body timed out")
                    return
                if length and len(data) != length:
                    self.send_error(400, "incomplete request body")
                    return
                # Shared with the broker's exclusive takeover drain: a takeover
                # waits for every admitted request to finish. Recheck the marker
                # only after taking the gate; never forward a body buffered
                # pre-lock.
                try:
                    descriptor = os.open(outer.lock_file.parent, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_SH)
                        if lock_is_active(outer.lock_file):
                            self._locked()
                            return
                        self._forward(data)
                    finally:
                        os.close(descriptor)
                except OSError:
                    self.send_error(503, "browser admission unavailable")

            def _forward(self, data: bytes | None) -> None:
                url = f"http://{outer.target}{self.path}"
                headers = {
                    name: self.headers[name]
                    for name in ("Content-Type", "Authorization")
                    if name in self.headers
                }
                req = urllib.request.Request(url, data=data, headers=headers, method=self.command)
                try:
                    with urllib.request.urlopen(req, timeout=outer.upstream_timeout) as resp:
                        payload = resp.read()
                        self.send_response(resp.status)
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                except urllib.error.HTTPError as exc:
                    payload = exc.read()
                    self.send_response(exc.code)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (urllib.error.URLError, TimeoutError, OSError):
                    body = b"bad gateway\n"
                    self.send_response(502)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

        self._httpd = ThreadingHTTPServer(listen, Handler)

    def serve_forever(self) -> None:
        self._httpd.serve_forever()

    def shutdown(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
