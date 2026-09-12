"""Fail-closed Linear issue tracking and native delegate ownership CLI."""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

try:
    from .linear_activity import LinearActivityClient, _BOARD_STATUSES, _select_state_id
    from .linear_chat_closeout import ChatCloseoutError, ChatCloseoutRegistry
    from .linear_guard_health import WorkerGuardHealth
    from .linear_oauth import make_oauth, validate_private_directory
    from .linear_ownership import IssueOwnership
    from .linear_project_updates import _publisher_binding
except ImportError:  # direct script invocation
    from linear_activity import LinearActivityClient, _BOARD_STATUSES, _select_state_id
    from linear_chat_closeout import ChatCloseoutError, ChatCloseoutRegistry
    from linear_guard_health import WorkerGuardHealth
    from linear_oauth import make_oauth, validate_private_directory
    from linear_ownership import IssueOwnership
    from linear_project_updates import _publisher_binding


class TrackingError(RuntimeError):
    pass


_VIEWER = "query LinearTrackingViewer { viewer { id app organization { id } } }"
_ISSUE = """query IssueLookup($id: String!) { issue(id: $id) { id archivedAt title description team { id organization { id } states { nodes { id name type } } } parent { id } project { id } assignee { id } delegate { id } state { id name type } } }"""
_UPDATE = """mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) { issueUpdate(id: $id, input: $input) { success } }"""
_CREATE = """mutation IssueCreate($input: IssueCreateInput!) { issueCreate(input: $input) { success issue { id } } }"""
_PROJECT = """query ProjectLookup($id: String!) { project(id: $id) { id archivedAt teams(first: 50) { nodes { id } pageInfo { hasNextPage } } } }"""
_TERMINAL = {"completed", "canceled", "cancelled", "duplicate"}


class LinearTracking:
    """Tracking operations. Inject GraphQL for testability; no credential fallback exists here."""
    def __init__(self, database: Path, *, profile: str, workspace: str, owner_session_id: str, graphql: Callable[[str, dict[str, object]], dict[str, object]], health: WorkerGuardHealth, clock: Callable[[], float] | None = None, publisher_identity: tuple[str, str] | None = None) -> None:
        if not owner_session_id:
            raise ValueError("current Hermes session is required")
        self.database = Path(database).absolute()
        self.owner_session_id = owner_session_id
        self.graphql = graphql
        self.health = health
        self.ownership = IssueOwnership(self.database, profile=profile, workspace=workspace)
        self.closeout = ChatCloseoutRegistry(self.database, profile=profile, workspace=workspace, clock=clock)
        self.clock = clock
        self.publisher_identity = publisher_identity

    def _query(self, query: str, variables: dict[str, object]) -> dict[str, object]:
        result = self.graphql(query, variables)
        if not isinstance(result, dict) or result.get("errors"):
            raise TrackingError("Linear GraphQL operation failed")
        return result

    def _issue(self, identifier: str) -> dict[str, Any]:
        issue = self._query(_ISSUE, {"id": identifier}).get("data", {}).get("issue")
        if not isinstance(issue, dict) or not isinstance(issue.get("id"), str) or not issue["id"]:
            raise TrackingError("Linear issue was not found")
        return issue

    @staticmethod
    def _delegate_id(issue: dict[str, Any]) -> str | None:
        delegate = issue.get("delegate")
        return delegate.get("id") if isinstance(delegate, dict) and isinstance(delegate.get("id"), str) else None

    @staticmethod
    def _assignee_id(issue: dict[str, Any]) -> str | None:
        assignee = issue.get("assignee")
        return assignee.get("id") if isinstance(assignee, dict) and isinstance(assignee.get("id"), str) else None

    def _progress_update(self, issue: dict[str, Any], app_id: str) -> dict[str, object]:
        team = issue.get("team") if isinstance(issue.get("team"), dict) else {}
        nodes = team.get("states", {}).get("nodes") if isinstance(team.get("states"), dict) else None
        preferred, type_name = _BOARD_STATUSES["active"]
        state_id = _select_state_id(nodes, preferred, type_name)
        if not state_id:
            raise TrackingError("Linear team states are unavailable")
        update: dict[str, object] = {}
        current = issue.get("state") if isinstance(issue.get("state"), dict) else {}
        if current.get("id") != state_id:
            update["stateId"] = state_id
        if self._delegate_id(issue) != app_id:
            update["delegateId"] = app_id
        return update

    def _apply_claim_update(self, canonical_id: str, issue: dict[str, Any], app_id: str, record) -> None:
        original_assignee = self._assignee_id(issue)
        update = self._progress_update(issue, app_id)
        if not update:
            return
        try:
            updated = self._query(_UPDATE, {"id": canonical_id, "input": update})
            if not updated.get("data", {}).get("issueUpdate", {}).get("success"):
                raise TrackingError("Linear rejected claim update")
            readback = self._issue(canonical_id)
            if self._assignee_id(readback) != original_assignee:
                raise TrackingError("Linear assignee readback mismatch")
            if "delegateId" in update and self._delegate_id(readback) != app_id:
                raise TrackingError("Linear delegate readback mismatch")
            if "stateId" in update:
                state = readback.get("state") if isinstance(readback.get("state"), dict) else {}
                if state.get("id") != update["stateId"] and state.get("type") != "started":
                    raise TrackingError("Linear state readback mismatch")
        except TrackingError:
            # Known Linear outcome. Reconcile would block native Agent Sessions
            # with issue_requires_reconciliation after Linear already accepted
            # the mutation (LIFE-49 / HF-288).
            self.ownership.release(canonical_id, self.owner_session_id, record.generation)
            raise
        except Exception:
            self.ownership.reconcile(canonical_id, self.owner_session_id, record.generation)
            raise

    @staticmethod
    def _organization_id(value: object) -> str | None:
        return value.get("id") if isinstance(value, dict) and isinstance(value.get("id"), str) and value["id"] else None

    @staticmethod
    def _canonical_uuid(value: str, *, option: str) -> str:
        try:
            parsed = uuid.UUID(value)
        except (AttributeError, ValueError, TypeError) as exc:
            raise TrackingError(f"{option} must be a canonical UUIDv4") from exc
        if parsed.version != 4 or str(parsed) != value:
            raise TrackingError(f"{option} must be a canonical UUIDv4")
        return value

    @staticmethod
    def _related_id(issue: dict[str, Any], field: str) -> str | None:
        related = issue.get(field)
        return related.get("id") if isinstance(related, dict) and isinstance(related.get("id"), str) else None

    @staticmethod
    def _markdown_readback_equivalent(actual: object, expected: str) -> bool:
        """Accept only observed prose rewrites; code and URL autolinks must match exactly."""
        if not isinstance(actual, str):
            return False
        if actual == expected:
            return True

        def contains_code(markdown: str) -> bool:
            return any(
                line.startswith(("    ", "\t")) or line.lstrip().startswith(("```", "~~~"))
                for line in markdown.splitlines()
            )

        # Do not parse Markdown: reject normalizing any code-containing description.
        if contains_code(actual) or contains_code(expected):
            return False

        def normalize(markdown: str) -> str:
            normalized: list[str] = []
            for line in markdown.splitlines(keepends=True):
                content = line.rstrip("\r\n")
                if content == "" and normalized:
                    previous = normalized[-1].rstrip("\r\n")
                    hashes = len(previous) - len(previous.lstrip("#"))
                    if 1 <= hashes <= 6 and (len(previous) == hashes or previous[hashes] in " \t"):
                        continue
                if content.startswith("* "):
                    line = "- " + line[2:]
                normalized.append(line)
            return "".join(normalized)

        return normalize(actual) == normalize(expected)

    def create(self, *, title: str, description: str, team_id: str, parent_id: str | None = None, project_id: str | None = None, issue_id: str | None = None, team_only_maintenance: bool = False) -> dict[str, str]:
        if not all(isinstance(value, str) and value.strip() == value and value for value in (title, description, team_id)):
            raise TrackingError("title, description, and team are required")
        if issue_id is None:
            raise TrackingError("--issue-id is required and must be a canonical UUIDv4")
        issue_id = self._canonical_uuid(issue_id, option="--issue-id")
        if project_id is not None and (not isinstance(project_id, str) or project_id.strip() != project_id or not project_id):
            raise TrackingError("--project must be a non-empty explicit Linear project ID")
        if not isinstance(team_only_maintenance, bool):
            raise TrackingError("--team-only-maintenance must be an explicit boolean intent")
        if project_id is None and not team_only_maintenance:
            raise TrackingError("an explicit --project or --team-only-maintenance intent is required")
        if project_id is not None and team_only_maintenance:
            raise TrackingError("--project and --team-only-maintenance cannot be combined")
        if project_id is not None:
            response = self._query(_PROJECT, {"id": project_id})
            data = response.get("data")
            project = data.get("project") if isinstance(data, dict) else None
            connection = project.get("teams") if isinstance(project, dict) else None
            teams = connection.get("nodes") if isinstance(connection, dict) else None
            page_info = connection.get("pageInfo") if isinstance(connection, dict) else None
            if (
                not isinstance(project, dict)
                or project.get("id") != project_id
                or "archivedAt" not in project
                or project["archivedAt"] is not None
                or not isinstance(teams, list)
                or not isinstance(page_info, dict)
                or page_info.get("hasNextPage") is not False
                or any(not isinstance(team, dict) or not isinstance(team.get("id"), str) or not team["id"].strip() for team in teams)
                or team_id not in {team["id"] for team in teams}
            ):
                raise TrackingError("selected project does not belong to selected team")
        input_: dict[str, object] = {"id": issue_id, "title": title, "description": description, "teamId": team_id, "assigneeId": None, "delegateId": None}
        if parent_id:
            input_["parentId"] = parent_id
        if project_id:
            input_["projectId"] = project_id
        try:
            result = self._query(_CREATE, {"input": input_})
        except Exception as exc:
            raise TrackingError(f"Linear create outcome is unknown; inspect issue {issue_id} before retrying") from exc
        created = result.get("data", {}).get("issueCreate", {})
        created_id = created.get("issue", {}).get("id") if isinstance(created, dict) else None
        if not created.get("success") or created_id != issue_id:
            raise TrackingError(f"Linear create response was rejected or mismatched for known issue {issue_id}")
        try:
            readback = self._issue(issue_id)
        except Exception as exc:
            raise TrackingError(f"Linear create outcome is unknown; inspect issue {issue_id} before retrying") from exc
        actual = {
            "title": readback.get("title"), "description": readback.get("description"),
            "team": self._related_id(readback, "team"), "parent": self._related_id(readback, "parent"),
            "project": self._related_id(readback, "project"), "assignee": self._related_id(readback, "assignee"),
            "delegate": self._related_id(readback, "delegate"),
        }
        if (
            actual["title"] != title
            or not self._markdown_readback_equivalent(actual["description"], description)
            or actual["team"] != team_id
            or actual["parent"] != parent_id
            or actual["project"] != project_id
            or actual["assignee"] is not None
            or actual["delegate"] is not None
        ):
            raise TrackingError(f"Linear create readback mismatch for known issue {issue_id}")
        created = {"status": "created", "issue_id": issue_id}
        if parent_id or not self.health.is_ready(now=self.clock() if self.clock else None):
            return created
        return self.claim(issue_id)

    def claim(self, identifier: str) -> dict[str, str]:
        # This gate occurs before viewer/issue reads, avoiding any activity from a
        # disabled, old, foreign, or stale runtime configuration.
        if not self.health.is_ready(now=self.clock() if self.clock else None):
            raise TrackingError("current Linear worker guard is not ready")
        viewer = self._query(_VIEWER, {}).get("data", {}).get("viewer", {})
        app_id = viewer.get("id") if isinstance(viewer, dict) else None
        if not isinstance(app_id, str) or not app_id or viewer.get("app") is not True:
            raise TrackingError("authenticated Linear viewer must be an app")
        viewer_organization = self._organization_id(viewer.get("organization"))
        if viewer_organization is None:
            raise TrackingError("authenticated Linear viewer organization is unavailable")
        issue = self._issue(identifier)
        team = issue.get("team")
        issue_organization = self._organization_id(team.get("organization")) if isinstance(team, dict) else None
        if issue_organization is None or issue_organization != viewer_organization:
            raise TrackingError("Linear issue team organization does not match authenticated app")
        canonical_id = self._canonical_uuid(issue["id"], option="fetched Linear issue ID")
        if issue.get("archivedAt") is not None:
            raise TrackingError("archived Linear issue cannot be claimed")
        state = issue.get("state")
        if not isinstance(state, dict) or not isinstance(state.get("type"), str) or not state["type"]:
            raise TrackingError("Linear issue state is unavailable")
        if state["type"] in _TERMINAL:
            raise TrackingError("terminal Linear issue cannot be claimed")
        existing_delegate = self._delegate_id(issue)
        if existing_delegate is not None and existing_delegate != app_id:
            raise TrackingError("foreign Linear delegate owns this issue")
        canonical_id = issue["id"]
        claimed = self.ownership.claim(canonical_id, self.owner_session_id)
        if claimed.status != "claimed" or claimed.record is None:
            raise TrackingError(f"local issue ownership is {claimed.status}")
        record = claimed.record
        # The pre-read check can expire during local ownership admission. Keep
        # the durable fence if this check fails; never mutate Linear unready.
        if not self.health.is_ready(now=self.clock() if self.clock else None):
            raise TrackingError("current Linear worker guard is not ready after local claim")
        self._apply_claim_update(canonical_id, issue, app_id, record)
        self.closeout.track(canonical_id, self.owner_session_id, record.generation)
        return {"status": "claimed", "issue_id": canonical_id, "generation": record.generation}

    def takeover(self, identifier: str) -> dict[str, object]:
        """Explicit cross-agent steal: replace local owner and Linear delegate. Never assignee."""
        if not self.health.is_ready(now=self.clock() if self.clock else None):
            raise TrackingError("current Linear worker guard is not ready")
        viewer = self._query(_VIEWER, {}).get("data", {}).get("viewer", {})
        app_id = viewer.get("id") if isinstance(viewer, dict) else None
        if not isinstance(app_id, str) or not app_id or viewer.get("app") is not True:
            raise TrackingError("authenticated Linear viewer must be an app")
        viewer_organization = self._organization_id(viewer.get("organization"))
        if viewer_organization is None:
            raise TrackingError("authenticated Linear viewer organization is unavailable")
        issue = self._issue(identifier)
        team = issue.get("team")
        issue_organization = self._organization_id(team.get("organization")) if isinstance(team, dict) else None
        if issue_organization is None or issue_organization != viewer_organization:
            raise TrackingError("Linear issue team organization does not match authenticated app")
        canonical_id = self._canonical_uuid(issue["id"], option="fetched Linear issue ID")
        if issue.get("archivedAt") is not None:
            raise TrackingError("archived Linear issue cannot be taken over")
        state = issue.get("state")
        if not isinstance(state, dict) or not isinstance(state.get("type"), str) or not state["type"]:
            raise TrackingError("Linear issue state is unavailable")
        if state["type"] in _TERMINAL:
            raise TrackingError("terminal Linear issue cannot be taken over")
        claimed = self.ownership.takeover(canonical_id, self.owner_session_id)
        if claimed.status != "claimed" or claimed.record is None:
            raise TrackingError(f"local issue ownership is {claimed.status}")
        record = claimed.record
        if not self.health.is_ready(now=self.clock() if self.clock else None):
            raise TrackingError("current Linear worker guard is not ready after local takeover")
        self._apply_claim_update(canonical_id, issue, app_id, record)
        self.closeout.track(canonical_id, self.owner_session_id, record.generation)
        return {"status": "taken_over", "issue_id": canonical_id, "generation": record.generation}

    def track_chat(self, identifier: str) -> dict[str, object]:
        """Admit a publisher-authorized chat closeout without delegation or worker use."""
        viewer = self._query(_VIEWER, {}).get("data", {}).get("viewer", {})
        app_id = viewer.get("id") if isinstance(viewer, dict) else None
        organization = self._organization_id(viewer.get("organization")) if isinstance(viewer, dict) else None
        if not isinstance(app_id, str) or viewer.get("app") is not True or not organization:
            raise TrackingError("authenticated Linear viewer must be an app with an organization")
        if self.publisher_identity != (app_id, organization):
            raise TrackingError("authenticated app does not match immutable publishing policy")
        issue = self._issue(identifier)
        team = issue.get("team")
        issue_organization = self._organization_id(team.get("organization")) if isinstance(team, dict) else None
        if issue_organization != organization:
            raise TrackingError("Linear issue team organization does not match authenticated app")
        canonical_id = self._canonical_uuid(issue["id"], option="fetched Linear issue ID")
        state = issue.get("state")
        if issue.get("archivedAt") is not None or not isinstance(state, dict) or state.get("type") in _TERMINAL:
            raise TrackingError("archived or terminal Linear issue cannot be chat tracked")
        # Native delivery stays authoritative: a delegated issue may not acquire
        # this nondelegating route, even when delegated to this same app.
        if self._delegate_id(issue) is not None:
            raise TrackingError("delegated Linear issue remains native-worker owned")
        claimed = self.ownership.claim(canonical_id, self.owner_session_id)
        if claimed.status != "claimed" or claimed.record is None:
            raise TrackingError(f"local issue ownership is {claimed.status}")
        self.closeout.track(canonical_id, self.owner_session_id, claimed.record.generation)
        return {"status": "chat_tracked", "issue_id": canonical_id, "generation": claimed.record.generation}

    def status(self, issue_id: str) -> dict[str, object]:
        record = self.ownership.get(issue_id)
        return {"local": None if record is None else {"owner_session_id": record.owner_session_id, "generation": record.generation, "mode": record.mode}}

    def release(self, issue_id: str, generation: str) -> dict[str, bool]:
        return {"released": self.ownership.release(issue_id, self.owner_session_id, generation)}

    def complete(self, issue_id: str, generation: str, summary: str) -> dict[str, str]:
        """Explicitly mark tracked work complete and atomically admit closeout."""
        try:
            key = self.closeout.complete(issue_id, self.owner_session_id, generation, summary)
        except ChatCloseoutError as exc:
            raise TrackingError(str(exc)) from exc
        return {"status": "closeout_admitted", "issue_id": issue_id, "closeout_key": key}


def _active_config() -> tuple[Path, dict[str, object]]:
    home_value = os.environ.get("HERMES_HOME")
    session_id = os.environ.get("HERMES_SESSION_ID")
    if not home_value or not session_id:
        raise TrackingError("HERMES_HOME and current HERMES_SESSION_ID are required")
    home = Path(home_value).absolute()
    try:
        import yaml
        config = yaml.safe_load((home / "config.yaml").read_text(encoding="utf-8"))
    except Exception as exc:
        raise TrackingError("active Hermes configuration is unavailable") from exc
    plugins = config.get("plugins") if isinstance(config, dict) else None
    entries = plugins.get("entries") if isinstance(plugins, dict) else None
    enabled = plugins.get("enabled") if isinstance(plugins, dict) else None
    entry = entries.get("linear-agent") if isinstance(entries, dict) else None
    settings = entry.get("settings") if isinstance(entry, dict) else None
    if not isinstance(enabled, list) or "linear-agent" not in enabled:
        raise TrackingError("active linear-agent is not enabled")
    if not isinstance(settings, dict) or settings.get("enabled") is not True or settings.get("dry_run") is not False:
        raise TrackingError("active linear-agent is disabled or dry-run")
    session_profile = os.environ.get("HERMES_SESSION_PROFILE")
    configured_profile = settings.get("profile")
    # Plugin services receive runtime.profile_name; the CLI bridge must receive
    # the same explicit profile identity and never treat default as an alias.
    if not isinstance(configured_profile, str) or not configured_profile or session_profile != configured_profile:
        raise TrackingError("current Hermes session profile does not match linear-agent policy")
    return home, settings


def _tracking_config() -> tuple[Path, dict[str, object], str, str, Path]:
    home, entry = _active_config()
    profile = str(entry.get("profile", ""))
    workspace = str(entry.get("workspace", ""))
    state = Path(str(entry.get("state_database", "")))
    if not profile or not workspace or state != home / "linear-agent" / "state.db":
        raise TrackingError("active linear-agent state configuration is invalid")
    try:
        validate_private_directory(home)
        validate_private_directory(state.parent)
        if state.is_symlink() or (state.exists() and not state.is_file()):
            raise RuntimeError("state database must be a regular non-symlink file")
    except RuntimeError as exc:
        raise TrackingError("active linear-agent private state paths are invalid") from exc
    return home, entry, profile, workspace, state


def _local_tracker() -> LinearTracking:
    _home, _entry, profile, workspace, state = _tracking_config()
    return LinearTracking(state, profile=profile, workspace=workspace, owner_session_id=os.environ["HERMES_SESSION_ID"], graphql=lambda _query, _variables: (_ for _ in ()).throw(TrackingError("local action does not use Linear API")), health=WorkerGuardHealth(state, profile=profile, workspace=workspace))


def _configured_tracker() -> LinearTracking:
    home, entry, profile, workspace, state = _tracking_config()
    if str(entry.get("credential_mode", "")) != "managed_oauth_v1":
        raise TrackingError("managed Linear OAuth is required")
    oauth_file = home / "secrets" / "linear-oauth.json"
    connect_file = home / ".op.env"
    if Path(str(entry.get("oauth_file", ""))) != oauth_file or Path(str(entry.get("connect_env_file", ""))) != connect_file:
        raise TrackingError("active managed OAuth paths are invalid")
    vault_id, item_id = str(entry.get("oauth_vault_id", "")), str(entry.get("oauth_item_id", ""))
    if not vault_id or not item_id:
        raise TrackingError("managed OAuth policy identifiers are unavailable")
    try:
        policy = json.loads(Path(__file__).with_name("linear-agents.json").read_text(encoding="utf-8"))
        matches = [item for item in policy.get("agents", []) if isinstance(item, dict) and item.get("profile") == profile]
    except (OSError, ValueError, AttributeError) as exc:
        raise TrackingError("managed OAuth policy is unavailable") from exc
    if len(matches) != 1 or matches[0].get("workspace") != workspace or matches[0].get("oauth", {}).get("vault_id") != vault_id or matches[0].get("oauth", {}).get("item_id") != item_id:
        raise TrackingError("active Linear OAuth does not match managed policy")
    validate_private_directory(oauth_file.parent)
    _publisher_binding(profile, workspace, vault_id, item_id)
    client = LinearActivityClient(make_oauth(profile, home, vault_id, item_id))
    # Verify exact managed identity before constructing the mutation client.
    client.verify_authenticated()
    return LinearTracking(state, profile=profile, workspace=workspace, owner_session_id=os.environ["HERMES_SESSION_ID"], graphql=client._graphql, health=WorkerGuardHealth(state, profile=profile, workspace=workspace))


def _configured_chat_tracker() -> LinearTracking:
    """Publisher-only authorization path; deliberately does not read worker roster."""
    home, entry, profile, workspace, state = _tracking_config()
    if str(entry.get("credential_mode", "")) != "managed_oauth_v1":
        raise TrackingError("managed Linear OAuth is required")
    oauth_file, connect_file = home / "secrets" / "linear-oauth.json", home / ".op.env"
    if Path(str(entry.get("oauth_file", ""))) != oauth_file or Path(str(entry.get("connect_env_file", ""))) != connect_file:
        raise TrackingError("active managed OAuth paths are invalid")
    vault_id, item_id = str(entry.get("oauth_vault_id", "")), str(entry.get("oauth_item_id", ""))
    if not vault_id or not item_id:
        raise TrackingError("managed OAuth policy identifiers are unavailable")
    validate_private_directory(oauth_file.parent)
    binding = _publisher_binding(profile, workspace, vault_id, item_id)
    client = LinearActivityClient(make_oauth(profile, home, vault_id, item_id))
    client.verify_authenticated()
    return LinearTracking(state, profile=profile, workspace=workspace, owner_session_id=os.environ["HERMES_SESSION_ID"], graphql=client._graphql, health=WorkerGuardHealth(state, profile=profile, workspace=workspace), publisher_identity=(binding["viewer_id"], binding["organization_id"]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="safe Linear tracking")
    parser.add_argument("action", choices=("create", "claim", "takeover", "chat-track", "status", "release", "complete"))
    parser.add_argument("--issue")
    parser.add_argument("--issue-id")
    parser.add_argument("--title")
    parser.add_argument("--description")
    parser.add_argument("--team")
    parser.add_argument("--parent")
    parser.add_argument("--project")
    parser.add_argument("--team-only-maintenance", action="store_true", help="explicitly create team-only maintenance work without a project")
    parser.add_argument("--generation")
    parser.add_argument("--summary")
    args = parser.parse_args(argv)
    if args.action in {"status", "release", "complete"}:
        tracking = _local_tracker()
        if not args.issue:
            raise TrackingError("--issue is required and must be the canonical Linear issue UUID for local status/release")
        args.issue = tracking._canonical_uuid(args.issue, option="--issue")
    elif args.action == "chat-track":
        tracking = _configured_chat_tracker()
    else:
        tracking = _configured_tracker()
    if args.action == "create":
        result = tracking.create(title=args.title or "", description=args.description or "", team_id=args.team or "", parent_id=args.parent, project_id=args.project, issue_id=args.issue_id, team_only_maintenance=args.team_only_maintenance)
    elif args.action == "claim":
        if not args.issue:
            raise TrackingError("--issue is required")
        result = tracking.claim(args.issue)
    elif args.action == "takeover":
        if not args.issue:
            raise TrackingError("--issue is required")
        result = tracking.takeover(args.issue)
    elif args.action == "chat-track":
        if not args.issue:
            raise TrackingError("--issue is required")
        result = tracking.track_chat(args.issue)
    elif args.action == "status":
        result = tracking.status(args.issue)
    elif args.action == "release":
        if not args.generation:
            raise TrackingError("--generation is required")
        result = tracking.release(args.issue, args.generation)
    else:
        if not args.generation or args.summary is None:
            raise TrackingError("--generation and --summary are required")
        result = tracking.complete(args.issue, args.generation, args.summary)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TrackingError as exc:
        print(f"linear tracking refused: {exc}", file=sys.stderr)
        raise SystemExit(2)
