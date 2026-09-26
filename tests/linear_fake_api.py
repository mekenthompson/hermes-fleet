"""A small fake of Linear's GraphQL API over real HTTP, for plugins/linear scenario tests.

It models only what the plugin uses, with the live behaviours recorded in plan phase 0:
client ``id`` on creates (duplicate -> HTTP 200 INPUT_ERROR "conflict on insert of ..."),
RATELIMITED as HTTP 400 with epoch-millisecond reset headers, and the delegate field.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SELF = "app-self"
OTHER = {"id": "app-other", "name": "Other Agent"}
STATES = [
    {"id": "st-todo", "name": "Todo", "type": "unstarted"},
    {"id": "st-progress", "name": "In Progress", "type": "started"},
    {"id": "st-blocked", "name": "Blocked", "type": "started"},
    {"id": "st-done", "name": "Done", "type": "completed"},
    {"id": "st-canceled", "name": "Canceled", "type": "canceled"},
]


def load_plugin():
    name = "hermes_fleet_linear_plugin"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "plugins" / "linear" / "__init__.py", submodule_search_locations=[str(ROOT / "plugins" / "linear")])
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class Clock:
    def __init__(self, start: float = 1_900_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def iso(self, offset: float = 0.0) -> str:
        return datetime.fromtimestamp(self.now + offset, timezone.utc).isoformat().replace("+00:00", "Z")


class FakeLinear:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.issues: dict[str, dict] = {}
        self.comments: list[dict] = []
        self.activities: list[dict] = []
        self.project_updates: list[dict] = []
        self.ids: set[str] = set()
        self.sessions: dict[str, str] = {}
        self.requests: list[str] = []
        self.down = False
        self.lose_next_response = False
        self.rate_limited_until = 0.0
        self.lock = threading.Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args) -> None:
                pass

            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                status, headers, payload = fake.handle(body["query"], body.get("variables") or {})
                raw = json.dumps(payload).encode()
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/graphql"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def add_issue(self, issue_id: str, identifier: str, *, project: str | None = "proj-1") -> dict:
        issue = {"id": issue_id, "identifier": identifier, "title": f"Title of {identifier}",
                 "description": "Synthetic issue body", "url": f"https://linear.example/issue/{identifier}",
                 "updatedAt": self.clock.iso(), "delegate": None, "state": dict(STATES[0]),
                 "project": {"id": project} if project else None,
                 "team": {"id": "team-1", "key": "ABC", "states": {"nodes": STATES}}}
        self.issues[issue_id] = issue
        return issue

    def set_delegate(self, issue_id: str, delegate: dict | None) -> None:
        self.issues[issue_id]["delegate"] = delegate
        self.issues[issue_id]["updatedAt"] = self.clock.iso()

    def set_state(self, issue_id: str, name: str) -> None:
        self.issues[issue_id]["state"] = dict(next(s for s in STATES if s["name"] == name))
        self.issues[issue_id]["updatedAt"] = self.clock.iso()

    def state(self, issue_id: str) -> str:
        return self.issues[issue_id]["state"]["name"]

    def bodies(self, issue_id: str) -> list[str]:
        return [c["body"] for c in self.comments if c["issueId"] == issue_id] + \
               [a["content"]["body"] for a in self.activities if a.get("issueId") == issue_id]

    def handle(self, query: str, variables: dict) -> tuple[int, dict, dict]:
        with self.lock:
            self.requests.append(query.split("(")[0].split("{")[0].strip())
            if self.down:
                return 503, {}, {"errors": [{"message": "service unavailable"}]}
            if self.clock() < self.rate_limited_until:
                reset = str(int(self.rate_limited_until * 1000))
                return 400, {"X-RateLimit-Requests-Remaining": "4999", "X-RateLimit-Requests-Reset": reset}, {
                    "errors": [{"message": "Rate limit exceeded", "extensions": {"code": "RATELIMITED"}}]}
            status, payload = self._apply(query, variables)
            if self.lose_next_response and "mutation" in query:
                self.lose_next_response = False
                return 502, {}, {"errors": [{"message": "bad gateway"}]}
            return status, {"X-RateLimit-Requests-Remaining": "4999"}, payload

    def _apply(self, query: str, variables: dict) -> tuple[int, dict]:
        if "viewer" in query:
            return 200, {"data": {"viewer": {"id": SELF}}}
        if "issueUpdate" in query:
            issue = self.issues[variables["id"]]
            fields = variables["input"]
            if "stateId" in fields:
                issue["state"] = dict(next(s for s in issue["team"]["states"]["nodes"] if s["id"] == fields["stateId"]))
            if "delegateId" in fields:
                issue["delegate"] = {"id": SELF, "name": "This Agent"} if fields["delegateId"] == SELF else OTHER
            issue["updatedAt"] = self.clock.iso()
            return 200, {"data": {"issueUpdate": {"success": True}}}
        for mutation, store, entity in (("commentCreate", self.comments, "Comment"),
                                        ("agentActivityCreate", self.activities, "AgentActivity"),
                                        ("projectUpdateCreate", self.project_updates, "ProjectUpdate")):
            if mutation in query:
                fields = dict(variables["input"])
                if fields["id"] in self.ids:
                    return 200, {"data": None, "errors": [{
                        "message": f"conflict on insert of {entity}",
                        "extensions": {"type": "invalid input", "code": "INPUT_ERROR", "statusCode": 400,
                                       "userPresentableMessage": f"Entity {entity} with id {fields['id']} already exists."}}]}
                self.ids.add(fields["id"])
                if "agentSessionId" in fields:
                    fields["issueId"] = self.sessions.get(fields["agentSessionId"])
                store.append(fields)
                return 200, {"data": {mutation: {"success": True}}}
        if "issue(" in query:
            issue = self.issues.get(variables["id"]) or next(
                (i for i in self.issues.values() if i["identifier"] == variables["id"]), None)
            return 200, {"data": {"issue": json.loads(json.dumps(issue)) if issue else None}}
        return 400, {"errors": [{"message": "unsupported", "extensions": {"code": "INVALID_INPUT"}}]}

    def session_event(self, action: str, issue_id: str, session_id: str, *, body: str = "", signal: str | None = None,
                      creator: str = "human-1", activity_id: str = "act-1", at: float = 0.0) -> dict:
        """An AgentSessionEvent shaped like the phase 0 captures (issue has no updatedAt)."""
        self.sessions[session_id] = issue_id
        issue = self.issues[issue_id]
        event = {"type": "AgentSessionEvent", "action": action, "createdAt": self.clock.iso(at),
                 "webhookTimestamp": int((self.clock() + at) * 1000),
                 "agentSession": {"id": session_id, "creatorId": creator, "createdAt": self.clock.iso(at),
                                  "issue": {k: issue[k] for k in ("id", "identifier", "title", "description", "url")}},
                 "promptContext": f"<issue identifier=\"{issue['identifier']}\">{issue['title']}</issue>"}
        if action == "prompted":
            event["agentActivity"] = {"id": activity_id, "createdAt": self.clock.iso(at),
                                      "content": {"type": "prompt", "body": body or "stop"},
                                      "user": {"id": "human-1", "name": "Pat Example"}}
            if signal:
                event["agentActivity"]["signal"] = signal
        return event
