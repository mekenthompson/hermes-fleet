"""Small Linear GraphQL client: rate-limit aware, token-provider driven, client-id creates."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager, nullcontext
from typing import Any, Callable
from .oauth import ReauthorizationRequired

ENDPOINT = "https://api.linear.app/graphql"
DEFAULT_RATE_LIMIT_PAUSE = 60.0
ISSUE_FIELDS = """id identifier title description url updatedAt completedAt canceledAt creator { id }
  delegate { id name } state { id name type } project { id }
  team { id key states { nodes { id name type } } }"""

class LinearError(RuntimeError):
    """A Linear request failed. ``retryable`` is False for a definite GraphQL rejection."""
    def __init__(self, message: str, *, errors: Any = None, retryable: bool = True,
                 authoritative_issue_id: str | None = None, authoritative_session_id: str | None = None) -> None:
        super().__init__(message)
        self.errors = errors
        self.retryable = retryable
        self.authoritative_issue_id = authoritative_issue_id
        self.authoritative_session_id = authoritative_session_id

class RateLimited(LinearError):
    def __init__(self, until: float) -> None:
        super().__init__(f"Linear rate limit; paused until {until:.0f}")
        self.until = until

def _require_mutation_success(data: dict[str, Any], mutation: str) -> None:
    result = data.get(mutation)
    success = result.get("success") if isinstance(result, dict) else None
    if success is True:
        return
    if success is False:
        raise LinearError(f"Linear {mutation} rejected the mutation", retryable=False)
    raise LinearError(f"Linear {mutation} response did not confirm success")

def is_duplicate_create_error(errors: Any, client_id: str) -> bool:
    """True when Linear rejected a create because an entity with our client ``id`` exists.

    Every create sends the UUID v4 stored with its outbox row, so this means an earlier
    attempt (whose response we lost) already landed. Verified live (plan phase 0) for
    commentCreate and agentActivityCreate: HTTP 200, ``data: null``, code ``INPUT_ERROR``,
    message "conflict on insert of <Entity>", userPresentableMessage naming our id.
    ``INVALID_INPUT`` is a real input error and never matches.
    """
    for error in errors if isinstance(errors, list) else []:
        ext = (error.get("extensions") or {}) if isinstance(error, dict) else {}
        if (ext.get("code") == "INPUT_ERROR" and str(error.get("message", "")).startswith("conflict on insert of")
                and client_id in str(ext.get("userPresentableMessage", ""))):
            return True
    return False

def verify_webhook(secret: bytes, body: bytes, signature: str, now_ms: float, max_skew_ms: int = 60_000) -> bool:
    """HMAC-SHA256 ``linear-signature`` check plus Linear's one-minute ``webhookTimestamp`` window."""
    try:
        supplied = bytes.fromhex(signature)
        stamp = json.loads(body).get("webhookTimestamp")
    except (ValueError, TypeError, AttributeError, UnicodeDecodeError):
        return False
    expected = hmac.new(secret, body, hashlib.sha256).digest()
    if not hmac.compare_digest(expected, supplied):
        return False
    if isinstance(stamp, bool) or not isinstance(stamp, (int, float)) or not math.isfinite(stamp):
        return False
    return abs(now_ms - stamp) <= max_skew_ms

def _http(url: str, body: bytes, headers: dict[str, str]) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:  # nosec B310: configured https endpoint
            return response.status, dict(response.headers.items()), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers.items()) if exc.headers else {}, exc.read() or b""

class LinearAPI:
    """``token`` is the provider interface: a callable returning an access token, with an
    optional ``invalidate()`` that forces a refresh after a 401."""

    def __init__(self, token: Callable[[], str], *, endpoint: str = ENDPOINT,
                 transport: Callable[..., tuple[int, dict[str, str], bytes]] = _http,
                 clock: Callable[[], float] = time.time) -> None:
        self.token = token
        self.endpoint = endpoint
        self.transport = transport
        self.clock = clock
        self.paused_until = 0.0
        self._viewer: str | None = None
        self._mutation_guard = threading.local()
    @contextmanager
    def guarded_mutation(self, guard):
        previous = getattr(self._mutation_guard, "callback", None)
        self._mutation_guard.callback = guard
        try: yield
        finally: self._mutation_guard.callback = previous
    def _pause(self, headers: dict[str, str]) -> float:
        """Linear signals a limit with RATELIMITED; its reset headers are epoch milliseconds."""
        resets = []
        for name, value in headers.items():
            if name.lower().startswith("x-ratelimit-") and name.lower().endswith("-reset"):
                try:
                    resets.append(float(value) / 1000.0)
                except ValueError:
                    continue
        future = [r for r in resets if r > self.clock()]
        self.paused_until = max(future) if future else self.clock() + DEFAULT_RATE_LIMIT_PAUSE
        return self.paused_until
    def graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._graphql(query, variables)
    def _graphql(self, query: str, variables: dict[str, Any] | None = None, *,
                 token_provider: Callable[[], str] | None = None,
                 verify_credential: Callable[[Callable[[], str]], tuple[Callable[[], str], str]] | None = None) -> dict[str, Any]:
        if self.clock() < self.paused_until:
            raise RateLimited(self.paused_until)
        body = json.dumps({"query": query, "variables": variables or {}}).encode()
        guard = getattr(self._mutation_guard, "callback", None) if query.lstrip().startswith("mutation") else None
        for attempt in (1, 2):
            provider = token_provider if token_provider is not None else self.token
            credential = None
            if verify_credential is not None:
                provider, credential = verify_credential(provider)
            if self.clock() < self.paused_until:
                raise RateLimited(self.paused_until)
            try:
                headers = {"Content-Type": "application/json",
                           "Authorization": "Bearer " + (credential if credential is not None else provider())}
            except Exception as exc:  # noqa: BLE001 - credential providers may include secrets in errors
                logging.getLogger("linear").error("linear: credentials unavailable")
                if isinstance(exc, ReauthorizationRequired):
                    raise LinearError("Linear credentials unavailable") from exc
                raise LinearError("Linear credentials unavailable") from None
            try:
                with guard() if guard else nullcontext():
                    status, response_headers, raw = self.transport(self.endpoint, body, headers)
            except (OSError, TimeoutError) as exc:
                raise LinearError(f"Linear unreachable: {exc}") from exc
            refresh_provider = provider if token_provider is not None or verify_credential is not None else self.token
            if status == 401 and attempt == 1 and callable(getattr(refresh_provider, "invalidate", None)):
                refresh_provider.invalidate()
                continue
            break
        try:
            payload = json.loads(raw or b"{}")
        except ValueError:
            payload = {}
        errors = payload.get("errors") if isinstance(payload, dict) else None
        codes = {((e.get("extensions") or {}).get("code")) for e in errors or [] if isinstance(e, dict)}
        if status == 429 or "RATELIMITED" in codes:
            raise RateLimited(self._pause(response_headers))
        if status >= 500 or status in (401, 408):
            raise LinearError(f"Linear HTTP {status}")
        if status == 403:
            raise LinearError("Linear refused this app (HTTP 403); check its scopes", retryable=False)
        if errors:
            raise LinearError("Linear GraphQL error", errors=errors, retryable=False)
        if status >= 400 or not isinstance(payload.get("data"), dict):
            raise LinearError(f"Linear HTTP {status}", retryable=status < 400 or status >= 500)
        return payload["data"]
    def viewer_id(self) -> str:
        if self._viewer is not None:
            return self._viewer
        viewer = self.graphql("query Viewer { viewer { id } }").get("viewer")
        candidate = viewer.get("id") if isinstance(viewer, dict) else None
        if not isinstance(candidate, str) or not candidate:
            raise LinearError("Linear viewer identity response is missing an id")
        self._viewer = candidate
        return candidate
    def issue(self, ref: str) -> dict[str, Any]:
        data = self.graphql(f"query Issue($id: String!) {{ issue(id: $id) {{ {ISSUE_FIELDS} }} }}", {"id": ref})
        if not data.get("issue"):
            raise LinearError(f"Linear issue {ref} not found", retryable=False)
        return data["issue"]
    def update_issue(self, issue_id: str, fields: dict[str, Any]) -> None:
        data = self.graphql("mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) "
                            "{ issueUpdate(id: $id, input: $input) { success } }",
                            {"id": issue_id, "input": fields})
        _require_mutation_success(data, "issueUpdate")
    def _create(self, mutation: str, input_type: str, fields: dict[str, Any]) -> None:
        try:
            data = self.graphql(f"mutation Create($input: {input_type}!) {{ {mutation}(input: $input) {{ success }} }}",
                                {"input": fields})
            _require_mutation_success(data, mutation)
        except LinearError as exc:
            if not is_duplicate_create_error(exc.errors, fields["id"]):
                raise
    def agent_session(self, session_id: str) -> dict[str, Any]:
        raise LinearError("Agent Session authorization requires a bound specialist client", retryable=False)
    def agent_activity(self, activity_id: str, session_id: str) -> dict[str, Any]:
        raise LinearError("Agent Activity authorization requires a bound specialist client", retryable=False)
    def create_comment(self, client_id: str, issue_id: str, body: str) -> None:
        self._create("commentCreate", "CommentCreateInput", {"id": client_id, "issueId": issue_id, "body": body})
    def create_activity(self, client_id: str, session_id: str, content: dict[str, Any], *,
                        issue_id: str | None = None) -> None:
        self._create("agentActivityCreate", "AgentActivityCreateInput",
                     {"id": client_id, "agentSessionId": session_id, "content": content})
    def create_project_update(self, client_id: str, project_id: str, body: str, *,
                              issue_ids: list[str] | None = None) -> None:
        self._create("projectUpdateCreate", "ProjectUpdateCreateInput",
                     {"id": client_id, "projectId": project_id, "body": body})
    def create_issue(self, client_id: str, team_id: str, title: str, *, description: str | None = None,
                     project_id: str | None = None, parent_id: str | None = None) -> dict[str, Any]:
        """Create an issue as the authenticated app. Never assigns or delegates."""
        fields = {"id": client_id, "teamId": team_id, "title": title, "assigneeId": None, "delegateId": None}
        if description is not None:
            fields["description"] = description
        if project_id is not None:
            fields["projectId"] = project_id
        if parent_id is not None:
            fields["parentId"] = parent_id
        try:
            data = self.graphql("mutation IssueCreate($input: IssueCreateInput!) "
                                "{ issueCreate(input: $input) { success } }", {"input": fields})
            _require_mutation_success(data, "issueCreate")
        except LinearError as exc:
            if not is_duplicate_create_error(exc.errors, client_id):
                raise
        issue = self.graphql(
            "query CreatedIssue($id: String!) { issue(id: $id) { id identifier title url "
            "team { id key } project { id } parent { id } assignee { id } delegate { id } } }",
            {"id": client_id}).get("issue")
        team = (issue or {}).get("team") or {}
        project = (issue or {}).get("project") or {}
        parent = (issue or {}).get("parent") or {}
        if (not isinstance(issue, dict) or issue.get("id") != client_id or issue.get("title") != title
                or team_id not in {team.get("id"), team.get("key")}
                or issue.get("assignee") is not None or issue.get("delegate") is not None
                or (project_id is not None and project.get("id") != project_id)
                or (parent_id is not None and parent.get("id") != parent_id)):
            raise LinearError("Linear issue readback does not match the create", retryable=False)
        return issue
    def create_project(self, client_id: str, name: str, team_ids: list[str], *,
                       description: str | None = None) -> dict[str, Any]:
        """Create a project as the authenticated app. Never sets a lead."""
        fields: dict[str, Any] = {"id": client_id, "name": name, "teamIds": list(team_ids), "leadId": None}
        if description is not None:
            fields["description"] = description
        try:
            data = self.graphql("mutation ProjectCreate($input: ProjectCreateInput!) "
                                "{ projectCreate(input: $input) { success } }", {"input": fields})
            _require_mutation_success(data, "projectCreate")
        except LinearError as exc:
            if not is_duplicate_create_error(exc.errors, client_id):
                raise
        project = self.graphql(
            "query CreatedProject($id: String!) { project(id: $id) { id name url lead { id } teams { nodes { id key } } } }",
            {"id": client_id}).get("project")
        teams = {node.get("id") for node in ((project or {}).get("teams") or {}).get("nodes") or []}
        if (not isinstance(project, dict) or project.get("id") != client_id or project.get("name") != name
                or project.get("lead") is not None or not set(team_ids) <= teams):
            raise LinearError("Linear project readback does not match the create", retryable=False)
        return project
    def link_issue(self, issue_id: str, related_issue_id: str, relation: str) -> dict[str, Any]:
        """Link two issues. ``blocked_by`` records the related issue as the blocker."""
        if relation not in {"blocks", "blocked_by", "related"}:
            raise LinearError("issue link relation must be blocks, blocked_by, or related", retryable=False)
        if relation == "blocked_by":
            issue_id, related_issue_id, kind = related_issue_id, issue_id, "blocks"
        else:
            kind = relation
        data = self.graphql(
            "mutation IssueRelationCreate($input: IssueRelationCreateInput!) "
            "{ issueRelationCreate(input: $input) { success issueRelation "
            "{ id type issue { id } relatedIssue { id } } } }",
            {"input": {"issueId": issue_id, "relatedIssueId": related_issue_id, "type": kind}})
        _require_mutation_success(data, "issueRelationCreate")
        relation_row = (data.get("issueRelationCreate") or {}).get("issueRelation") or {}
        if (relation_row.get("type") != kind or (relation_row.get("issue") or {}).get("id") != issue_id
                or (relation_row.get("relatedIssue") or {}).get("id") != related_issue_id):
            raise LinearError("Linear issue link readback does not match the create", retryable=False)
        return relation_row

def state_id(issue: dict[str, Any], name: str) -> str:
    """Resolve a workflow state by name on the issue's team (case-insensitive)."""
    for node in ((issue.get("team") or {}).get("states") or {}).get("nodes") or []:
        if str(node.get("name", "")).casefold() == name.casefold():
            return str(node["id"])
    raise LinearError(f"team has no '{name}' workflow state; add it or fix the states config", retryable=False)
