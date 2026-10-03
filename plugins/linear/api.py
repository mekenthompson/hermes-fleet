"""Small Linear GraphQL client: rate-limit aware, token-provider driven, client-id creates."""
from __future__ import annotations

import hashlib
import fcntl
import hmac
import json
import logging
import math
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager, nullcontext
from email.utils import parsedate_to_datetime
from pathlib import Path
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
                 clock: Callable[[], float] = time.time, rate_limit_path: Path | None = None) -> None:
        self.token = token
        self.endpoint = endpoint
        self.transport = transport
        self.clock = clock
        self.paused_until = 0.0
        self.rate_limit_path = rate_limit_path
        self._request_lock = threading.RLock()
        self._viewer: str | None = None
        self._mutation_guard = threading.local()
    @contextmanager
    def guarded_mutation(self, guard):
        previous = getattr(self._mutation_guard, "callback", None)
        self._mutation_guard.callback = guard
        try: yield
        finally: self._mutation_guard.callback = previous
    def _pause_state(self, until: float = 0.0) -> float:
        """Retain the longest cooldown in this profile across client/service restarts."""
        self.paused_until = max(self.paused_until, until)
        if self.rate_limit_path is None: return self.paused_until
        try:
            self.rate_limit_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with self.rate_limit_path.open("a+", encoding="utf-8") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                handle.seek(0)
                raw = handle.read()
                try:
                    saved = float(json.loads(raw)["paused_until"]) if raw else 0.0
                    if not math.isfinite(saved): raise ValueError("invalid cooldown")
                except (ValueError, TypeError, KeyError):
                    saved = self.clock() + DEFAULT_RATE_LIMIT_PAUSE
                self.paused_until = max(self.paused_until, saved)
                handle.seek(0)
                handle.truncate()
                json.dump({"paused_until": self.paused_until}, handle)
        except OSError:
            raise LinearError("Linear rate-limit state unavailable") from None
        return self.paused_until
    def _pause(self, headers: dict[str, str], *, limited: bool = True) -> float:
        """Linear signals a limit with RATELIMITED; its reset headers are epoch milliseconds."""
        now, resets, exhausted = self.clock(), [], []
        headers = {name.lower(): value for name, value in headers.items()}
        for prefix in ("x-ratelimit-requests", "x-ratelimit-complexity", "x-ratelimit-endpoint-requests"):
            try: empty = float(headers.get(prefix + "-remaining", "nan")) <= 0
            except (ValueError, TypeError): empty = False
            if empty: exhausted.append(prefix)
        if not limited and not exhausted: return self.paused_until
        for prefix in exhausted or ("x-ratelimit-requests", "x-ratelimit-complexity", "x-ratelimit-endpoint-requests"):
            value = headers.get(prefix + "-reset")
            if value is not None:
                try:
                    reset = float(value) / 1000.0
                    if math.isfinite(reset) and reset > now: resets.append(reset)
                except (ValueError, TypeError):
                    continue
        if limited and "retry-after" in headers:
            try:
                value = headers["retry-after"]
                try: retry = now + float(value)
                except ValueError: retry = parsedate_to_datetime(value).timestamp()
                if math.isfinite(retry) and retry > now: resets.append(retry)
            except (ValueError, TypeError, OverflowError): pass
        return self._pause_state(max(resets) if resets else now + DEFAULT_RATE_LIMIT_PAUSE)
    def graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._graphql(query, variables)
    def _graphql(self, query: str, variables: dict[str, Any] | None = None, *,
                 token_provider: Callable[[], str] | None = None,
                 verify_credential: Callable[[Callable[[], str]], tuple[Callable[[], str], str]] | None = None) -> dict[str, Any]:
        with self._request_lock:
            return self._request(query, variables, token_provider=token_provider, verify_credential=verify_credential)
    def _request(self, query, variables, *, token_provider, verify_credential):
        if self.clock() < self._pause_state():
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
            try:
                payload = json.loads(raw or b"{}")
            except ValueError:
                payload = {}
            errors = payload.get("errors") if isinstance(payload, dict) else None
            codes = [e["extensions"].get("code") for e in errors or []
                     if isinstance(e, dict) and isinstance(e.get("extensions"), dict)]
            if status == 429 or "RATELIMITED" in codes:
                raise RateLimited(self._pause(response_headers))
            self._pause(response_headers, limited=False)
            refresh_provider = provider if token_provider is not None or verify_credential is not None else self.token
            if status == 401 and attempt == 1 and callable(getattr(refresh_provider, "invalidate", None)):
                refresh_provider.invalidate()
                continue
            break
        if status >= 500 or status in (401, 408):
            raise LinearError(f"Linear HTTP {status}")
        if status == 403:
            raise LinearError("Linear refused this app (HTTP 403); check its scopes", retryable=False)
        if errors:
            raise LinearError("Linear GraphQL error", errors=errors, retryable=False)
        if status >= 400 or not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
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
