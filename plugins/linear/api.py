"""Small Linear GraphQL client: rate-limit aware, token-provider driven, client-id creates."""
from __future__ import annotations

import hashlib
import fcntl
import hmac
import json
import logging
import math
import re
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
ISSUE_FIELDS = """id identifier title description url updatedAt completedAt canceledAt creator { id } assignee { id }
  delegate { id name } state { id name type } project { id }
  team { id key states { nodes { id name type position } } }"""

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

def _graphql_rejection(errors: Any, request_kind: str = "graphql") -> str:
    """Keep bounded, redacted GraphQL detail beside the stable error prefix."""
    parts: list[str] = []
    log = logging.getLogger("linear")
    entries = errors if isinstance(errors, list) else []
    for error in entries[:3]:
        if not isinstance(error, dict):
            continue
        message = error.get("message")
        raw_extensions = error.get("extensions")
        extensions = raw_extensions if isinstance(raw_extensions, dict) else {}
        code = extensions.get("code")
        text = _safe_diagnostic_text(message, 360)
        safe_code = code if isinstance(code, str) and re.fullmatch(r"[A-Z0-9_]{1,80}", code) else ""
        log.error("linear: GraphQL error request_kind=%s message=%s code=%s", request_kind, text, safe_code)
        if text and safe_code:
            parts.append(f"{text} ({safe_code})")
        elif text or safe_code:
            parts.append(text or safe_code)
    if len(entries) > 3:
        parts.append("additional errors omitted")
    return "Linear GraphQL error" + (f": {'; '.join(parts)}" if parts else "")

def _safe_diagnostic_text(value: Any, limit: int) -> str:
    if not isinstance(value, str):
        return ""
    text = re.sub(r"[\r\n\x00-\x1f\x7f]+", " ", value).strip()
    text = re.sub(r"(?i)\bBearer\s+\S+", "Bearer [redacted]", text)
    text = re.sub(r"(?i)\b(?:api[_-]?key|token|secret)\s*[:=]\s*\S+", "[redacted]", text)
    text = re.sub(
        r'''(?i)(["']?(?:access[_-]?token|refresh[_-]?token|token|password|secret|api[_-]?key|authorization)["']?\s*[:=]\s*["']?)[^"'&\s,}]+''',
        r"\1[redacted]", text)
    text = re.sub(r"(?i)(https?://)[^/@\s]+:[^/@\s]+@", r"\1[redacted]@", text)
    text = text[:limit]
    return re.sub(r"\s+", " ", text)

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

def _bare_host_line_matches(actual: str, expected: str) -> bool:
    """Permit only an observed HTTP autolink added to an expected bare host.

    Be conservative around Markdown syntax: code, escapes, HTML and authored
    links are not candidates. Do not erase links on both sides, which would
    hide a removed authored link or a changed destination.
    """
    prose = re.sub(r'^(?:[ \t]*(?:[-*+]|[0-9]+[.)]) )\[[xX ]\] ', '', expected)
    if expected.startswith(('    ', '\t')) or any(char in prose for char in '`\\[]<>'):
        return False
    hosts = re.finditer(
        r'(?<![\w@./:+%?&=#~-])'
        r'(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}'
        r'(?![\w@/:+%?&=#~-]|\.[\w-])', expected)
    parts, offset = [], 0
    for match in hosts:
        host = re.escape(match.group())
        parts.extend((re.escape(expected[offset:match.start()]),
                      rf'(?:{host}|\[{host}\]\(http://{host}\))'))
        offset = match.end()
    parts.append(re.escape(expected[offset:]))
    return re.fullmatch(''.join(parts), actual) is not None


def markdown_matches(actual: object, expected: str) -> bool:
    """Accept narrow editor rewrites, including bare-host autolinks; preserve code/text."""
    if not isinstance(actual, str):
        return False
    def canonical(text: str) -> list[tuple[str, bool]]:
        lines, result, fence = text.splitlines(), [], None
        after_heading = False
        inline_run, delimiters = None, 0
        for line in lines:
            marker = re.match(r'^\s*(`{3,}|~{3,})', line)
            if marker:
                run = marker.group(1)
                if fence is None: fence = (run[0], len(run))
                elif run[0] == fence[0] and len(run) >= fence[1]: fence = None
                result.append((line, False))
                after_heading = False
                continue
            if fence is not None:
                result.append((line, False))
                continue
            if not line and after_heading:
                continue
            after_heading = bool(re.match(r'^#{1,6} +', line))
            # A bare-looking host on a later line may still be inside inline
            # code, a link label/destination, or an HTML tag opened earlier.
            prose = inline_run is None and delimiters == 0
            for token in re.findall(r'\\.|`+|[\[\]()<>]', line):
                if token.startswith('\\'):
                    continue
                if token.startswith('`'):
                    if inline_run is None: inline_run = len(token)
                    elif len(token) == inline_run: inline_run = None
                elif inline_run is None:
                    delimiters = max(0, delimiters + (1 if token in '[(<' else -1))
            prose = prose and inline_run is None and delimiters == 0
            if not prose or '`' in line or line.expandtabs(4).startswith('    '):
                result.append((line, False))
                continue
            line = re.sub(r'^([ \t]*(?:[-*+]|[0-9]+[.)]) )\[X\](?=[ \t]|$)', r'\1[x]', line)
            result.append((re.sub(r'\]\(<((?:[^\s<>()]|\([^\s<>()]*\))+)>\)', r'](\1)', re.sub(r'^([ \t]*)[*+] ', r'\1- ', line)), prose))
        return result
    received, authored = canonical(actual), canonical(expected)
    return len(received) == len(authored) and all(
        line == wanted or ('<' not in expected and prose and expected_prose and _bare_host_line_matches(line, wanted))
        for (line, prose), (wanted, expected_prose) in zip(received, authored))

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
                category = "network error" if isinstance(exc, TimeoutError) else "OS error"
                raise LinearError(f"Linear unreachable ({category})") from None
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
            rejected = getattr(guard, "rejected", None) if guard is not None else None
            if status == 401 and callable(rejected):
                # False means this project-update create was already started or is a
                # prepared legacy create. Other guarded mutations do not install this
                # callback, so their 401 still refreshes credentials and retries once.
                if rejected() is False:
                    break
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
            request_kind = "mutation" if query.lstrip().startswith("mutation") else "query"
            duplicate_check = request_kind == "mutation" and "mutation Create(" in query
            raise LinearError(_graphql_rejection(errors, request_kind),
                              errors=errors if duplicate_check else None, retryable=False)
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
    def issue_with_thread(self, issue_id: str, limit: int = 50, *, progress: bool = False) -> tuple[dict[str, Any], list[dict[str, str]]]:
        """Read one bounded page; ordinary threads retain creation-time chronology."""
        operation = "IssueProgress" if progress else "IssueThread"
        order = "updatedAt" if progress else "createdAt"
        updated = " updatedAt" if progress else ""
        page = " pageInfo { hasNextPage }" if progress else ""
        data = self.graphql("query %s($id: String!) { issue(id: $id) { %s comments(first: %d, orderBy: %s) "
                            "{ nodes { id url body createdAt%s user { id name } botActor { name } }%s } } }"
                            % (operation, ISSUE_FIELDS, limit, order, updated, page), {"id": issue_id})
        if not data.get("issue"):
            raise LinearError(f"Linear issue {issue_id} not found", retryable=False)
        nodes = (data["issue"].get("comments") or {}).get("nodes") or []
        comments = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            created = str(node.get("createdAt") or "")
            comment = {"id": str(node.get("id") or ""), "url": str(node.get("url") or ""),
                       "at": created, "body": str(node.get("body") or ""),
                       "user_id": (node.get("user") or {}).get("id"),
                       "who": str((node.get("user") or node.get("botActor") or {}).get("name") or "unknown")}
            if progress:
                comment.update(created_at=created, updated_at=str(node.get("updatedAt") or ""))
                comment["at"] = comment["updated_at"] or created
            comments.append(comment)
        # Linear's first page is newest first for the selected order. Only
        # ordinary worker threads need their existing oldest-first presentation.
        return data["issue"], comments if progress else sorted(comments, key=lambda comment: comment["at"])
    def routing_history(self, issue_id: str, limit: int = 50, after: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        """One newest-first IssueHistory page: who changed state, assignee or delegate, and when.

        Returns the page and the cursor of the next (older) page, or None at the end."""
        return self._routing_history(issue_id, limit, after)[1:]
    def _routing_history(self, issue_id: str, limit: int, after: str | None = None) -> tuple[dict[str, Any], list[dict[str, Any]], str | None]:
        data = self.graphql("query IssueRoutingHistory($id: String!, $after: String) { issue(id: $id) { %s "
                            "history(first: %d, after: $after) "
                            "{ nodes { createdAt actorId fromStateId toStateId fromAssigneeId toAssigneeId "
                            "fromDelegate { id } toDelegate { id } } pageInfo { hasNextPage endCursor } } } }"
                            % (ISSUE_FIELDS, limit), {"id": issue_id, "after": after})
        issue = data.get("issue")
        history = (issue or {}).get("history")
        if not isinstance(history, dict) or not isinstance(history.get("nodes"), list):
            raise LinearError(f"Linear issue {issue_id} history unavailable")
        nodes = [node for node in history["nodes"] if isinstance(node, dict)]
        page = history.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            return issue, nodes, None
        cursor = page.get("endCursor")
        if not isinstance(cursor, str) or not cursor:
            raise LinearError(f"Linear issue {issue_id} history page has no cursor")
        return issue, nodes, cursor
    def issue_thread(self, issue_id: str, limit: int = 50) -> list[dict[str, str]]:
        return self.issue_with_thread(issue_id, limit)[1]
    def update_issue(self, issue_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        data = self.graphql("mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) "
                            "{ issueUpdate(id: $id, input: $input) { success issue { " + ISSUE_FIELDS + " } } }",
                            {"id": issue_id, "input": fields})
        _require_mutation_success(data, "issueUpdate")
        issue = data["issueUpdate"].get("issue")
        return issue if isinstance(issue, dict) else None
    def replace_description(self, issue_id: str, expected: str, body: str) -> None:
        current = self.issue(issue_id)
        if current.get("id") != issue_id:
            raise LinearError("Description target could not be verified", retryable=False)
        if markdown_matches(current.get("description"), body):
            return  # same-id readback reconciliation; never overwrite a concurrent change
        if current.get("description") != expected:
            raise LinearError("Description changed or could not be verified; no replacement made", retryable=False)
        # This is an optimistic preflight, not a server-side CAS: a concurrent
        # edit between this read and mutation can still be overwritten.
        self.update_issue(issue_id, {"description": body})
        verified = self.issue(issue_id)
        if verified.get("id") != issue_id or not markdown_matches(verified.get("description"), body):
            raise LinearError("Description replacement could not be verified; reconcile before retrying")
    def verify_comment(self, comment_id: str, issue_id: str, body: str) -> bool:
        data = self.graphql("query CommentReadback($id: String!) { comment(id: $id) { id body issue { id } } }",
                            {"id": comment_id})
        comment = data.get("comment")
        return bool(isinstance(comment, dict) and comment.get("id") == comment_id and
                    markdown_matches(comment.get("body"), body) and isinstance(comment.get("issue"), dict) and
                    comment["issue"].get("id") == issue_id)
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
                        issue_id: str | None = None, ephemeral: bool = False) -> None:
        fields = {"id": client_id, "agentSessionId": session_id, "content": content}
        if ephemeral:
            if content.get("type") not in ("thought", "action"):
                raise LinearError("only a thought or action activity can be ephemeral", retryable=False)
            fields["ephemeral"] = True
        self._create("agentActivityCreate", "AgentActivityCreateInput", fields)
    def create_project_update(self, client_id: str, project_id: str, body: str, *,
                              issue_ids: list[str] | None = None) -> None:
        self._create("projectUpdateCreate", "ProjectUpdateCreateInput",
                     {"id": client_id, "projectId": project_id, "body": body})
    def create_issue(self, client_id: str, team_id: str, title: str, *, description: str | None = None,
                     project_id: str | None = None, parent_id: str | None = None) -> dict[str, Any]:
        """Create an issue as the authenticated app. Never assigns or delegates."""
        fields = {"id": client_id, "teamId": team_id, "title": title, "assigneeId": None, "delegateId": None}
        fields.update({key: value for key, value in (
            ("description", description), ("projectId", project_id), ("parentId", parent_id)) if value is not None})
        self._create("issueCreate", "IssueCreateInput", fields)
        issue = self.graphql(
            "query CreatedIssue($id: String!) { issue(id: $id) { id identifier title description url "
            "team { id key } project { id } parent { id } assignee { id } delegate { id } } }",
            {"id": client_id}).get("issue")
        team = (issue or {}).get("team") or {}
        project = (issue or {}).get("project") or {}
        parent = (issue or {}).get("parent") or {}
        if (not isinstance(issue, dict) or issue.get("id") != client_id or issue.get("title") != title
                or team_id not in {team.get("id"), team.get("key")}
                or (description is not None and not markdown_matches(issue.get("description"), description))
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
        self._create("projectCreate", "ProjectCreateInput", fields)
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
