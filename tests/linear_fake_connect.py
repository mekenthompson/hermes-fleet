"""In-process fake 1Password Connect and Linear token endpoint for OAuth tests."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs


def login_item(vault_id: str, item_id: str, *, client_id="client", client_secret="secret", refresh_token="refresh-0", title="Linear Client") -> dict[str, Any]:
    """Legacy login layout: generic username/credential fields plus a refresh_token field."""
    return {
        "id": item_id, "title": title, "vault": {"id": vault_id},
        "fields": [
            {"id": "username", "label": "username", "value": client_id},
            {"id": "credential", "label": "credential", "value": client_secret},
            {"id": "abc123", "label": "refresh_token", "value": refresh_token},
            {"id": "notesPlain", "label": "notesPlain", "value": "unrelated"},
        ],
    }


def canonical_item(vault_id: str, item_id: str, *, client_id="client", client_secret="secret", refresh_token="refresh-0", title="Linear OAuth") -> dict[str, Any]:
    """Profile-style layout: opaque ids with canonical labels alongside unrelated generic fields."""
    return {
        "id": item_id, "title": title, "vault": {"id": vault_id},
        "fields": [
            {"id": "username", "label": "username", "value": "not-the-client-id"},
            {"id": "password", "label": "password", "value": "not-the-secret"},
            {"id": "f1", "label": "Client_ID", "value": client_id},
            {"id": "f2", "label": "client_secret", "value": client_secret},
            {"id": "f3", "label": "refresh_token", "value": refresh_token},
        ],
    }


class FakeConnect:
    """Serves GET/PATCH /v1/vaults/{v}/items/{i} and POST /oauth/token on 127.0.0.1."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.token = "connect-token"
        self.requests: list[tuple[str, str]] = []
        self.fail_patch = False
        self.token_responses: list[tuple[int, dict[str, Any]]] = []
        self.token_requests: list[dict[str, str]] = []
        self.token_handler: Callable[[dict[str, str]], tuple[int, dict[str, Any]]] | None = None
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):  # noqa: D401 - silence
                return

            def _send(self, status: int, payload: Any) -> None:
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _item(self):
                parts = self.path.split("/")
                if len(parts) == 6 and parts[1:3] == ["v1", "vaults"] and parts[4] == "items":
                    return fake.items.get((parts[3], parts[5]))
                return None

            def do_GET(self):
                fake.requests.append(("GET", self.path))
                if self.headers.get("Authorization") != "Bearer " + fake.token:
                    return self._send(401, {"message": "bad token"})
                item = self._item()
                if item is None:
                    return self._send(404, {"message": "not found"})
                return self._send(200, item)

            def do_PATCH(self):
                fake.requests.append(("PATCH", self.path))
                if fake.fail_patch:
                    return self._send(500, {"message": "connect down"})
                item = self._item()
                if item is None:
                    return self._send(404, {"message": "not found"})
                ops = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
                for op in ops:
                    field_id = op["path"].split("/")[2]
                    for field in item["fields"]:
                        if field["id"] == field_id:
                            field["value"] = op["value"]
                return self._send(200, item)

            def do_POST(self):
                fake.requests.append(("POST", self.path))
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()
                form = {key: values[0] for key, values in parse_qs(raw).items()}
                fake.token_requests.append(form)
                if fake.token_handler is not None:
                    status, payload = fake.token_handler(form)
                elif fake.token_responses:
                    status, payload = fake.token_responses.pop(0)
                else:
                    status, payload = 200, {"access_token": "access-" + form.get("refresh_token", ""), "refresh_token": "rotated-" + form.get("refresh_token", ""), "expires_in": 3600}
                return self._send(status, payload)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def host(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    @property
    def token_endpoint(self) -> str:
        return self.host + "/oauth/token"

    def __enter__(self) -> "FakeConnect":
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self._server.shutdown()
        self._server.server_close()

    def field_value(self, vault_id: str, item_id: str, label: str) -> str:
        for field in self.items[(vault_id, item_id)]["fields"]:
            if field["label"] == label:
                return str(field["value"])
        raise KeyError(label)
