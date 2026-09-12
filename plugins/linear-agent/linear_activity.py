"""Minimal Linear Agent Activity GraphQL client."""
from __future__ import annotations

import base64
import binascii
import json
import urllib.error
import urllib.request
from collections.abc import Callable

try:
    from .linear_attachments import LinearMediaClient, MediaRejected
    from .linear_completion import linear_children_connection_accepted
    from .linear_quota import LinearQuotaExceeded, LinearQuotaGate, LinearReadCache
except ImportError:  # Direct script/test import.
    from linear_attachments import LinearMediaClient, MediaRejected
    from linear_completion import linear_children_connection_accepted
    from linear_quota import LinearQuotaExceeded, LinearQuotaGate, LinearReadCache

_ENDPOINT = "https://api.linear.app/graphql"
_MUTATION = """mutation AgentActivityCreate($input: AgentActivityCreateInput!) {
  agentActivityCreate(input: $input) { success }
}"""
_READINESS_QUERY = "query LinearAgentReadiness { viewer { id } }"
_COMMENT_CREATE_MUTATION = """mutation LinearIssueComment($input: CommentCreateInput!) {
  commentCreate(input: $input) { success }
}"""
_ACTIVITY_TYPES = {"thought", "response", "elicitation", "error"}
_EPHEMERAL_TYPES = {"thought", "action"}
_AUTONOMOUS_STATUSES = frozenset({"waiting", "failure", "active", "review", "done"})
_BOARD_STATUSES = {
    "active": ("In Progress", "started"),
    "waiting": ("Waiting on Ken", None),
    "review": ("In Review", "started"),
    "done": ("Done", "completed"),
}
_ISSUE_WORKFLOW = """query LinearIssueWorkflow($id: String!) {
  issue(id: $id) {
    id
    state { id name type }
    delegate { id }
    children { pageInfo { hasNextPage } nodes { id state { type } } }
    team { states { nodes { id name type } } }
  }
}"""
_ISSUE_UPDATE = """mutation LinearIssueUpdate($id: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $id, input: $input) { success }
}"""
_ACTOR_QUERY = """query LinearIssueActor { viewer { id } }"""


class LinearGraphQLError(RuntimeError):
    """Retain structured API errors without exposing their contents in logs."""

    def __init__(self, errors: object) -> None:
        super().__init__("Linear GraphQL operation failed")
        self.errors = errors


class LinearActivityClient:
    def __init__(
        self,
        access_token: str | Callable[[], str],
        *,
        transport: Callable[[str, dict[str, str], bytes], bytes] | None = None,
        put: Callable[[str, dict[str, str], bytes], None] | None = None,
        project_update_publisher: Callable[..., object] | None = None,
        project_update_policy: dict[str, str] | None = None,
        quota: LinearQuotaGate | None = None,
        read_cache: LinearReadCache | None = None,
    ) -> None:
        if not access_token:
            raise ValueError("Linear access token is required")
        self._access_token = access_token
        self._transport = transport or self._http_transport
        self._put = put or self._http_put
        self._project_update_publisher = project_update_publisher
        self._project_update_policy = project_update_policy
        self._quota = quota if quota is not None else LinearQuotaGate.shared()
        self._read_cache = read_cache if read_cache is not None else LinearReadCache.shared()
        self._viewer_id: str | None = None

    def _project_updates(self) -> Callable[..., object]:
        if self._project_update_publisher is None:
            try:
                from .linear_project_updates import publish_session_updates
            except ImportError:  # Direct script/test import.
                from linear_project_updates import publish_session_updates
            self._project_update_publisher = publish_session_updates
        return self._project_update_publisher

    def _token(self) -> str:
        access_token = self._access_token() if callable(self._access_token) else self._access_token
        if not access_token:
            raise RuntimeError("Linear access token provider returned no token")
        return access_token

    def graphql(self, query: str, variables: dict[str, object]) -> dict[str, object]:
        """Run a read-only GraphQL query for admission authorities."""
        return self._graphql(query, variables)

    def verify_authenticated(self) -> None:
        encoded = json.dumps({"query": _READINESS_QUERY}, separators=(",", ":")).encode("utf-8")
        raw = self._send_authenticated(encoded)
        result = json.loads(raw)
        if result.get("errors") or not result.get("data", {}).get("viewer", {}).get("id"):
            raise RuntimeError("Linear authentication readiness check failed")

    @staticmethod
    def _http_transport(url: str, headers: dict[str, str], body: bytes) -> bytes:
        request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(request, timeout=15) as response:  # nosec B310: fixed HTTPS API endpoint
            return response.read()

    @staticmethod
    def _http_put(url: str, headers: dict[str, str], payload: bytes) -> None:
        if not isinstance(url, str) or not url.startswith("https://"):
            raise RuntimeError("Linear file upload was not accepted")
        request = urllib.request.Request(url, data=payload, headers=headers, method="PUT")
        with urllib.request.urlopen(request, timeout=60) as response:  # nosec B310: HTTPS upload URL from Linear
            response.read()

    def attach_media(
        self,
        issue_id: str,
        filename: str,
        content_type: str,
        payload: bytes,
        *,
        title: str | None = None,
        comment_body: str | None = None,
        size: int | None = None,
    ) -> bool:
        if not issue_id:
            raise ValueError("Linear operation requires target and body")
        if not isinstance(payload, (bytes, bytearray)):
            raise MediaRejected("media payload is required")
        data = bytes(payload)
        if size is not None and size != len(data):
            raise MediaRejected("media size is not allowed")
        media = LinearMediaClient(self._graphql, self._put)
        asset = media.upload(filename, content_type, data)
        if comment_body is not None:
            media.comment_with_media(issue_id, comment_body, asset, alt=title or asset.filename)
        else:
            media.attach_issue(issue_id, asset, title=title or asset.filename)
        return True

    def _dispatch_issue_media(self, issue_id: str, body: str) -> bool:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError("invalid Linear media payload") from exc
        allowed = {"filename", "content_type", "data_b64", "title", "comment", "size"}
        if not isinstance(payload, dict) or not payload or set(payload) - allowed:
            raise ValueError("invalid Linear media payload")
        if not {"filename", "content_type", "data_b64"} <= set(payload):
            raise ValueError("invalid Linear media payload")
        raw = payload.get("data_b64")
        if not isinstance(raw, str) or not raw:
            raise MediaRejected("media payload is required")
        try:
            data = base64.b64decode(raw, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise MediaRejected("media payload is required") from exc
        title = payload.get("title")
        comment = payload["comment"] if "comment" in payload else None
        size = payload.get("size")
        if title is not None and not isinstance(title, str):
            raise ValueError("invalid Linear media payload")
        if comment is not None and not isinstance(comment, str):
            raise ValueError("invalid Linear media payload")
        if size is not None and type(size) is not int:
            raise MediaRejected("media size is not allowed")
        return self.attach_media(
            issue_id,
            payload["filename"],
            payload["content_type"],
            data,
            title=title,
            comment_body=comment,
            size=size,
        )

    def emit(
        self,
        agent_session_id: str,
        activity_type: str,
        body: str,
        *,
        ephemeral: bool | None = None,
    ) -> None:
        if activity_type == "action":
            raise ValueError("action activity requires action payload shape")
        if activity_type not in _ACTIVITY_TYPES:
            raise ValueError("unsupported Agent Activity type")
        if not agent_session_id or not body:
            raise ValueError("agent session and activity body are required")
        if ephemeral is None:
            ephemeral = activity_type == "thought"
        if ephemeral and activity_type not in _EPHEMERAL_TYPES:
            raise ValueError("only thought and action activities may be ephemeral")
        content: dict[str, object] = {"type": activity_type, "body": body}
        if ephemeral:
            content["ephemeral"] = True
        payload = {
            "query": _MUTATION,
            "variables": {"input": {"agentSessionId": agent_session_id, "content": content}},
        }
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        raw = self._send_authenticated(encoded)
        result = json.loads(raw)
        if result.get("errors") or not result.get("data", {}).get("agentActivityCreate", {}).get("success"):
            raise RuntimeError("Linear rejected Agent Activity")

    def dispatch(self, target_id: str, operation: str, body: str) -> bool:
        if not target_id or not body:
            raise ValueError("Linear operation requires target and body")
        if operation in _ACTIVITY_TYPES:
            self.emit(target_id, operation, body)
            return True
        if operation == "issue_comment":
            result = self._graphql(
                _COMMENT_CREATE_MUTATION,
                {"input": {"issueId": target_id, "body": body}},
            )
            if not result.get("data", {}).get("commentCreate", {}).get("success"):
                raise RuntimeError("Linear rejected issue comment")
            return True
        if operation == "issue_media":
            return self._dispatch_issue_media(target_id, body)
        if operation == "project_update":
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                raise ValueError("invalid Linear project update payload") from exc
            if (
                not isinstance(payload, dict)
                or set(payload) != {"session_key", "summary"}
                or not isinstance(payload["session_key"], str)
                or not payload["session_key"]
                or not isinstance(payload["summary"], str)
                or not payload["summary"]
            ):
                raise ValueError("invalid Linear project update payload")
            try:
                policy = self._project_update_policy or {}
                kwargs: dict[str, object] = {"configured_workspace": policy.get("organization_id")}
                # Keep custom publisher callables source-compatible. The installed
                # native binding always supplies this pinned actor.
                if policy.get("viewer_id"):
                    kwargs["configured_actor"] = policy["viewer_id"]
                self._project_updates()(
                    self._graphql, payload["session_key"],
                    [{"issue_id": target_id, "summary": payload["summary"]}], **kwargs,
                )
            except Exception as exc:
                try:
                    from .linear_project_updates import NoProjectUpdate
                except ImportError:
                    from linear_project_updates import NoProjectUpdate
                if isinstance(exc, NoProjectUpdate):
                    return False
                raise
            return True
        if operation == "issue_handoff":
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                raise ValueError("unsupported Linear issue operation") from exc
            if not isinstance(payload, dict) or payload.get("state") not in _AUTONOMOUS_STATUSES:
                raise ValueError("unsupported Linear issue operation")
            return self._apply_issue_lifecycle(target_id, str(payload["state"]))
        if operation != "issue_status" or body not in _AUTONOMOUS_STATUSES:
            raise ValueError("unsupported Linear issue operation")
        return self._apply_issue_lifecycle(target_id, body)

    def _actor_id(self) -> str:
        if self._viewer_id is None:
            result = self._graphql(_ACTOR_QUERY, {})
            viewer = result.get("data", {}).get("viewer") if isinstance(result.get("data"), dict) else None
            actor = viewer.get("id") if isinstance(viewer, dict) else None
            if not isinstance(actor, str) or not actor:
                raise RuntimeError("Linear viewer id is unavailable")
            self._viewer_id = actor
        return self._viewer_id

    def _apply_issue_lifecycle(self, issue_id: str, status: str) -> bool:
        """Write native status. Self-delegate only while active. Never set assigneeId."""
        if status == "failure":
            return True
        preferred, type_name = _BOARD_STATUSES[status]
        snapshot = self._graphql(_ISSUE_WORKFLOW, {"id": issue_id}).get("data", {}).get("issue")
        if not isinstance(snapshot, dict):
            raise RuntimeError("Linear issue workflow is unavailable")
        current = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
        if current.get("type") == "canceled":
            return False
        if current.get("type") == "completed" and status != "done":
            return False
        if status == "done":
            if not linear_children_connection_accepted(snapshot.get("children")):
                return False
        team = snapshot.get("team") if isinstance(snapshot.get("team"), dict) else {}
        nodes = team.get("states", {}).get("nodes") if isinstance(team.get("states"), dict) else None
        try:
            state_id = _select_state_id(nodes, preferred, type_name)
        except RuntimeError:
            if status == "waiting":
                return True
            raise
        update: dict[str, object] = {}
        if current.get("id") != state_id:
            update["stateId"] = state_id
        if status == "active":
            actor = self._actor_id()
            delegate = snapshot.get("delegate") if isinstance(snapshot.get("delegate"), dict) else {}
            if delegate.get("id") != actor:
                update["delegateId"] = actor
        if not update:
            return True
        result = self._graphql(_ISSUE_UPDATE, {"id": issue_id, "input": update})
        payload = result.get("data", {}).get("issueUpdate") if isinstance(result.get("data"), dict) else None
        if not isinstance(payload, dict) or payload.get("success") is not True:
            raise RuntimeError("Linear issue update failed")
        return True

    def _graphql(self, query: str, variables: dict[str, object]) -> dict[str, object]:
        cached = self._read_cache.get(query, variables)
        if cached is not None:
            return cached
        encoded = json.dumps(
            {"query": query, "variables": variables}, separators=(",", ":")
        ).encode("utf-8")
        raw = self._send_authenticated(encoded)
        result = json.loads(raw)
        self._quota.observe_payload(result, raw)
        if not isinstance(result, dict):
            raise LinearGraphQLError(result)
        if result.get("errors"):
            raise LinearGraphQLError(result["errors"])
        self._read_cache.put(query, variables, result)
        return result

    def _send_authenticated(self, encoded: bytes) -> bytes:
        self._quota.raise_if_cooling_down()
        def send() -> bytes:
            return self._transport(
                _ENDPOINT,
                {"Authorization": f"Bearer {self._token()}", "Content-Type": "application/json"},
                encoded,
            )
        try:
            raw = send()
        except urllib.error.HTTPError as exc:
            if exc.code == 401 and callable(self._access_token) and hasattr(self._access_token, "invalidate"):
                self._access_token.invalidate()
                try:
                    raw = send()
                except urllib.error.HTTPError as retry_exc:
                    self._quota.observe_http_error(retry_exc)
                    raise
            else:
                self._quota.observe_http_error(exc)
                raise
        self._quota.observe_payload(None, raw)
        return raw


def _select_state_id(nodes: object, preferred: str, type_name: str | None) -> str:
    if not isinstance(nodes, list):
        raise RuntimeError("Linear team states are unavailable")
    typed = None
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        if not isinstance(node_id, str) or not node_id:
            continue
        if node.get("name") == preferred:
            return node_id
        if type_name is not None and typed is None and node.get("type") == type_name:
            typed = node_id
    if typed is None:
        raise RuntimeError("Linear team has no matching workflow state")
    return typed
