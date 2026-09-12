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
_SESSION_UPDATE = """mutation LinearAgentSessionUpdate($id: String!, $input: AgentSessionUpdateInput!) {
  agentSessionUpdate(id: $id, input: $input) { success }
}"""
_SKILL_CREATE = """mutation LinearAgentSkillCreate($input: AgentSkillCreateInput!) {
  agentSkillCreate(input: $input) { success agentSkill { id } }
}"""
_REPO_SUGGESTIONS = """query LinearIssueRepositorySuggestions(
  $issueId: String!, $candidateRepositories: [CandidateRepository!]!, $agentSessionId: String
) {
  issueRepositorySuggestions(
    issueId: $issueId, candidateRepositories: $candidateRepositories, agentSessionId: $agentSessionId
  ) { suggestions { repositoryFullName confidence } }
}"""
_READINESS_QUERY = """query LinearAgentReadiness { viewer { id } }"""
_COMMENT_CREATE_MUTATION = """mutation LinearIssueComment($input: CommentCreateInput!) {
  commentCreate(input: $input) { success }
}"""
_ACTIVITY_TYPES = {"thought", "response", "elicitation", "error", "action"}
_EPHEMERAL_TYPES = {"thought", "action"}
_ELICITATION_SIGNALS = frozenset({"auth", "select"})
_AUTONOMOUS_STATUSES = frozenset({"waiting", "failure", "active", "review", "done"})
DEFAULT_WAITING_STATE_NAME = "Waiting on Principal"


def board_statuses(waiting_state_name: str = DEFAULT_WAITING_STATE_NAME) -> dict[str, tuple[str, str | None]]:
    if not isinstance(waiting_state_name, str) or not waiting_state_name.strip():
        raise ValueError("waiting state name must be a non-empty string")
    return {
        "active": ("In Progress", "started"),
        "waiting": (waiting_state_name.strip(), None),
        "review": ("In Review", "started"),
        "done": ("Done", "completed"),
    }


_BOARD_STATUSES = board_statuses()
_GENERIC_WAITING_BLOCKERS = frozenset({"none", "n/a", "na", "tbd", "unknown"})


def waiting_unblock_comment_accepted(body: object) -> bool:
    """True when a waiting-state comment states the exact principal request."""
    if not isinstance(body, str):
        return False
    text = body.strip()
    heading = "### Blocker"
    if heading not in text:
        return False
    remainder = text.split(heading, 1)[1]
    lines = [line.strip().lstrip("-*").strip() for line in remainder.splitlines()[1:]]
    lines = [line for line in lines if line]
    if not lines:
        return False
    return " ".join(lines).casefold() not in _GENERIC_WAITING_BLOCKERS
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
        waiting_state_name: str = DEFAULT_WAITING_STATE_NAME,
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
        self._board_statuses = board_statuses(waiting_state_name)
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
        signal: str | None = None,
        signal_metadata: object | None = None,
        reason_code: str | None = None,
    ) -> None:
        if activity_type == "action":
            raise ValueError("action activity requires action payload shape")
        if activity_type == "elicitation":
            raise ValueError("elicitation activity requires auth or select signal")
        if activity_type not in _ACTIVITY_TYPES:
            raise ValueError("unsupported Agent Activity type")
        if not agent_session_id or not body:
            raise ValueError("agent session and activity body are required")
        if ephemeral is None:
            ephemeral = activity_type == "thought"
        if ephemeral and activity_type not in _EPHEMERAL_TYPES:
            raise ValueError("only thought and action activities may be ephemeral")
        content: dict[str, object] = {"type": activity_type, "body": body}
        if activity_type == "error" and reason_code:
            content["reasonCode"] = reason_code
        self._create_activity(
            agent_session_id,
            content,
            ephemeral=ephemeral,
            signal=signal,
            signal_metadata=signal_metadata,
        )

    def emit_action(
        self,
        agent_session_id: str,
        action: str,
        parameter: str,
        *,
        result: str | None = None,
        ephemeral: bool = True,
    ) -> None:
        if not agent_session_id or not action or not parameter:
            raise ValueError("action activity requires action payload shape")
        content: dict[str, object] = {"type": "action", "action": action, "parameter": parameter}
        if result is not None:
            content["result"] = result
        self._create_activity(agent_session_id, content, ephemeral=ephemeral)

    def emit_elicitation(
        self,
        agent_session_id: str,
        body: str,
        signal: str,
        *,
        signal_metadata: object | None = None,
    ) -> None:
        if not agent_session_id or not body:
            raise ValueError("agent session and activity body are required")
        if signal not in _ELICITATION_SIGNALS:
            raise ValueError("elicitation activity requires auth or select signal")
        self._create_activity(
            agent_session_id,
            {"type": "elicitation", "body": body},
            signal=signal,
            signal_metadata=signal_metadata,
        )

    def update_session(
        self,
        agent_session_id: str,
        *,
        summary: str | None = None,
        plan: object | None = None,
        external_urls: list[dict[str, str]] | None = None,
    ) -> None:
        if not agent_session_id:
            raise ValueError("agent session is required")
        update: dict[str, object] = {}
        if summary is not None:
            if not isinstance(summary, str) or not summary.strip() or "\n" in summary or "\0" in summary:
                raise ValueError("session summary must be 1 to 255 characters without line breaks")
            if len(summary) > 255:
                raise ValueError("session summary must be 1 to 255 characters without line breaks")
            update["summary"] = summary
        if plan is not None:
            if not isinstance(plan, dict):
                raise ValueError("session plan must be an object")
            update["plan"] = plan
        if external_urls is not None:
            if not isinstance(external_urls, list) or any(
                not isinstance(item, dict) or not item.get("url") for item in external_urls
            ):
                raise ValueError("session externalUrls require url")
            update["externalUrls"] = external_urls
        if not update:
            raise ValueError("session update requires summary, plan, or externalUrls")
        result = self._graphql(_SESSION_UPDATE, {"id": agent_session_id, "input": update})
        payload = result.get("data", {}).get("agentSessionUpdate") if isinstance(result.get("data"), dict) else None
        if not isinstance(payload, dict) or payload.get("success") is not True:
            raise RuntimeError("Linear rejected agent session update")

    def create_agent_skill(self, title: str, body: str, *, team_id: str | None = None) -> dict[str, object]:
        if not title or not body:
            raise ValueError("agent skill requires title and body")
        variables: dict[str, object] = {"input": {"title": title, "body": body}}
        if team_id:
            cast_input = variables["input"]
            if isinstance(cast_input, dict):
                cast_input["teamId"] = team_id
        result = self._graphql(_SKILL_CREATE, variables)
        payload = result.get("data", {}).get("agentSkillCreate") if isinstance(result.get("data"), dict) else None
        skill = payload.get("agentSkill") if isinstance(payload, dict) else None
        if not isinstance(payload, dict) or payload.get("success") is not True or not isinstance(skill, dict):
            raise RuntimeError("Linear rejected agent skill create")
        return skill

    def issue_repository_suggestions(
        self,
        issue_id: str,
        candidate_repositories: list[dict[str, str]],
        *,
        agent_session_id: str | None = None,
    ) -> list[dict[str, object]]:
        if not issue_id or not candidate_repositories:
            raise ValueError("repository suggestions require issueId and candidateRepositories")
        result = self._graphql(
            _REPO_SUGGESTIONS,
            {
                "issueId": issue_id,
                "candidateRepositories": candidate_repositories,
                "agentSessionId": agent_session_id,
            },
        )
        payload = result.get("data", {}).get("issueRepositorySuggestions") if isinstance(result.get("data"), dict) else None
        suggestions = payload.get("suggestions") if isinstance(payload, dict) else None
        if not isinstance(suggestions, list):
            raise RuntimeError("Linear repository suggestions failed")
        return [item for item in suggestions if isinstance(item, dict)]

    def _create_activity(
        self,
        agent_session_id: str,
        content: dict[str, object],
        *,
        ephemeral: bool = False,
        signal: str | None = None,
        signal_metadata: object | None = None,
    ) -> None:
        activity_input: dict[str, object] = {"agentSessionId": agent_session_id, "content": content}
        if ephemeral:
            activity_input["ephemeral"] = True
        if signal is not None:
            activity_input["signal"] = signal
        if signal_metadata is not None:
            activity_input["signalMetadata"] = signal_metadata
        payload = {"query": _MUTATION, "variables": {"input": activity_input}}
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        raw = self._send_authenticated(encoded)
        result = json.loads(raw)
        if result.get("errors") or not result.get("data", {}).get("agentActivityCreate", {}).get("success"):
            raise RuntimeError("Linear rejected Agent Activity")

    def dispatch(self, target_id: str, operation: str, body: str) -> bool:
        if not target_id or not body:
            raise ValueError("Linear operation requires target and body")
        if operation == "action":
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("action activity requires action payload shape")
            self.emit_action(
                target_id,
                str(payload.get("action") or ""),
                str(payload.get("parameter") or ""),
                result=str(payload["result"]) if payload.get("result") is not None else None,
            )
            return True
        if operation == "elicitation":
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("elicitation activity requires auth or select signal")
            self.emit_elicitation(
                target_id,
                str(payload.get("body") or ""),
                str(payload.get("signal") or ""),
                signal_metadata=payload.get("signalMetadata"),
            )
            return True
        if operation == "session_update":
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("session update requires summary, plan, or externalUrls")
            self.update_session(
                target_id,
                summary=str(payload["summary"]) if payload.get("summary") is not None else None,
                plan=payload.get("plan"),
                external_urls=payload.get("externalUrls") if isinstance(payload.get("externalUrls"), list) else None,
            )
            return True
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
            unblock = payload.get("unblock")
            if unblock is None:
                unblock = payload.get("blocker")
            return self._apply_issue_lifecycle(
                target_id,
                str(payload["state"]),
                unblock_comment=unblock if isinstance(unblock, str) else None,
            )
        if operation != "issue_status" or body not in _AUTONOMOUS_STATUSES:
            raise ValueError("unsupported Linear issue operation")
        if body == "waiting":
            raise ValueError(
                f"{self._board_statuses['waiting'][0]} requires issue_handoff with an unblock comment"
            )
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

    def _apply_issue_lifecycle(
        self,
        issue_id: str,
        status: str,
        *,
        unblock_comment: str | None = None,
    ) -> bool:
        """Write native status. Self-delegate only while active. Never set assigneeId."""
        if status == "failure":
            return True
        if status == "waiting" and not waiting_unblock_comment_accepted(unblock_comment):
            raise ValueError(
                f"{self._board_statuses['waiting'][0]} requires an unblock comment with ### Blocker "
                "and the exact principal decision or artifact needed"
            )
        preferred, type_name = self._board_statuses[status]
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
        current_name = current.get("name")
        if status == "waiting" and current_name == preferred:
            return True
        if status == "waiting":
            result = self._graphql(
                _COMMENT_CREATE_MUTATION,
                {"input": {"issueId": issue_id, "body": str(unblock_comment).strip()}},
            )
            if not result.get("data", {}).get("commentCreate", {}).get("success"):
                raise RuntimeError("Linear rejected issue comment")
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
