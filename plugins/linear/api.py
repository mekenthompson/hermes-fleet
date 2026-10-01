"""Small Linear GraphQL client: rate-limit aware, token-provider driven, client-id creates."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import time
import urllib.error
import urllib.request
from typing import Any, Callable

ENDPOINT = "https://api.linear.app/graphql"
DEFAULT_RATE_LIMIT_PAUSE = 60.0

ISSUE_FIELDS = """id identifier title description url updatedAt creator { id }
  delegate { id name } state { id name type } project { id }
  team { id key states { nodes { id name type } } }"""


class LinearError(RuntimeError):
    """A Linear request failed. ``retryable`` is False for a definite GraphQL rejection."""

    def __init__(self, message: str, *, errors: Any = None, retryable: bool = True) -> None:
        super().__init__(message)
        self.errors = errors
        self.retryable = retryable


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
        if self.clock() < self.paused_until:
            raise RateLimited(self.paused_until)
        body = json.dumps({"query": query, "variables": variables or {}}).encode()
        for attempt in (1, 2):
            try:
                headers = {"Content-Type": "application/json", "Authorization": "Bearer " + self.token()}
            except Exception as exc:  # noqa: BLE001 - Connect outage or refresh failure: retry later, loudly
                logging.getLogger("linear").error("linear: credentials unavailable: %s", exc)
                raise LinearError(f"Linear credentials unavailable: {exc}") from exc
            try:
                status, response_headers, raw = self.transport(self.endpoint, body, headers)
            except (OSError, TimeoutError) as exc:
                raise LinearError(f"Linear unreachable: {exc}") from exc
            if status == 401 and attempt == 1 and callable(getattr(self.token, "invalidate", None)):
                self.token.invalidate()
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


def state_id(issue: dict[str, Any], name: str) -> str:
    """Resolve a workflow state by name on the issue's team (case-insensitive)."""
    for node in ((issue.get("team") or {}).get("states") or {}).get("nodes") or []:
        if str(node.get("name", "")).casefold() == name.casefold():
            return str(node["id"])
    raise LinearError(f"team has no '{name}' workflow state; add it or fix the states config", retryable=False)
