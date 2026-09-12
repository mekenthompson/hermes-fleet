"""Publish native Linear project updates without creating issue comments.

Reusable API: ``publish_session_updates(graphql, session_key, issues)`` accepts a
raw authenticated GraphQL callable (for ``LinearActivityClient._graphql``) and
explicit ``[{"issue_id": "TEAM-1", "summary": "..."}]`` source records. It
reads each live issue, groups only those records by its resolved project, and
publishes one update per project. A summary can never be copied to another
project. ``LinearProjectUpdatePublisher.publish`` is the lower-level CLI API
when callers already hold an explicit project-to-summary mapping.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TypedDict

try:
    from .linear_activity import LinearActivityClient, LinearGraphQLError
    from .linear_oauth import make_oauth, validate_private_directory
except ImportError:  # direct script invocation
    from linear_activity import LinearActivityClient, LinearGraphQLError
    from linear_oauth import make_oauth, validate_private_directory


class ProjectUpdateError(RuntimeError):
    """A project update could not be safely authored or reconciled."""


class NoProjectUpdate(ProjectUpdateError):
    """The source issue has no project, so there is deliberately nothing to publish."""


class PublisherBinding(TypedDict):
    viewer_id: str
    organization_id: str


_VIEWER = "query ProjectUpdatesViewer { viewer { id app organization { id } } }"
_ISSUE = """query ProjectUpdateIssue($id: String!) { issue(id: $id) { id identifier url project { id } team { organization { id } } } }"""
_PROJECT_HEALTH = "query ProjectUpdateHealth($id: String!) { project(id: $id) { id health } }"
_UPDATE = """query ProjectUpdateLookup($id: String!) { projectUpdate(id: $id) { id body url health project { id } user { id } } }"""
_CREATE = """mutation ProjectUpdateCreate($input: ProjectUpdateCreateInput!) { projectUpdateCreate(input: $input) { success projectUpdate { id body url project { id } user { id } } } }"""
_NAMESPACE = uuid.UUID("6c164a0d-7d2c-5f05-87e7-2cdb96fd3c0c")


class LinearProjectUpdatePublisher:
    def __init__(self, graphql: Callable[[str, dict[str, object]], dict[str, object]], *, configured_workspace: str | None = None, configured_actor: str | None = None) -> None:
        self.graphql = graphql
        self.configured_workspace = configured_workspace
        self.configured_actor = configured_actor
        self._issue_urls: dict[str, str] = {}
        self._issue_identifiers: dict[str, str] = {}
        self._issue_native_ids: dict[str, str] = {}
        self._unprojected: list[str] = []

    def _query(self, query: str, variables: dict[str, object]) -> dict[str, object]:
        try:
            result = self.graphql(query, variables)
        except LinearGraphQLError as exc:
            result = {"errors": exc.errors}
        except Exception as exc:
            raise ProjectUpdateError("Linear operation outcome is unknown; reconcile by deterministic update ID") from exc
        if isinstance(result, dict) and query == _UPDATE and self._absent_update(result.get("errors")):
            return {"data": {"projectUpdate": None}}
        if not isinstance(result, dict) or result.get("errors") or not isinstance(result.get("data"), dict):
            raise ProjectUpdateError("Linear GraphQL operation failed")
        if query == _UPDATE and "projectUpdate" not in result["data"]:
            raise ProjectUpdateError("Linear project-update read is incomplete")
        return result

    @staticmethod
    def _absent_update(errors: object) -> bool:
        # Verified against the live API: absence is a typed GraphQL error,
        # not data.projectUpdate=null. Never classify auth/transport errors as absence.
        if not isinstance(errors, list) or len(errors) != 1 or not isinstance(errors[0], dict):
            return False
        error = errors[0]
        extensions = error.get("extensions")
        return (error.get("path") == ["projectUpdate"]
                and error.get("message") == "Entity not found: ProjectUpdate"
                and isinstance(extensions, dict)
                and extensions.get("code") == "INPUT_ERROR"
                and extensions.get("type") == "invalid input")

    @staticmethod
    def markdown_equivalent(expected: str, actual: object) -> bool:
        """Allow only Linear's harmless heading/list whitespace rewrites."""
        if not isinstance(actual, str):
            return False
        if actual == expected:
            return True
        # Code spans/blocks and links are literal evidence: do not normalize them.
        if any(token in expected or token in actual for token in ("`", "[", "]", "<", ">")):
            return False
        def normalize(markdown: str) -> str:
            lines = markdown.replace("\r\n", "\n").split("\n")
            output: list[str] = []
            for line in lines:
                # Trailing spaces can encode hard breaks and must not be erased.
                if line.startswith("* "):
                    line = "- " + line[2:]
                if not line and output and output[-1].startswith("#"):
                    continue
                output.append(line)
            while output and not output[-1]:
                output.pop()
            return "\n".join(output)
        return normalize(expected) == normalize(actual)

    @staticmethod
    def _require_source(value: object, label: str) -> str:
        if not isinstance(value, str) or not value or value.strip() != value:
            raise ProjectUpdateError(f"invalid {label}")
        return value

    @classmethod
    def _issue_source(cls, value: object) -> str:
        original = cls._require_source(value, "source issue ID")
        if re.fullmatch(r"[A-Z][A-Z0-9]*-[1-9][0-9]*", original):
            return original
        try:
            canonical = str(uuid.UUID(original))
        except ValueError as exc:
            raise ProjectUpdateError("source issue ID must be an exact identifier or canonical UUID") from exc
        if canonical != original:
            raise ProjectUpdateError("source issue ID must be an exact identifier or canonical UUID")
        return original

    def _viewer(self) -> tuple[str, str]:
        viewer = self._query(_VIEWER, {}).get("data", {}).get("viewer")
        actor = viewer.get("id") if isinstance(viewer, dict) else None
        organization = viewer.get("organization") if isinstance(viewer, dict) else None
        workspace = organization.get("id") if isinstance(organization, dict) else None
        if not isinstance(actor, str) or not actor or viewer.get("app") is not True:
            raise ProjectUpdateError("authenticated Linear viewer must be an app")
        if not isinstance(workspace, str) or not workspace:
            raise ProjectUpdateError("authenticated Linear workspace is unavailable")
        if self.configured_workspace is not None and self.configured_workspace != workspace:
            raise ProjectUpdateError("authenticated workspace does not match configured profile workspace")
        if self.configured_actor is not None and self.configured_actor != actor:
            raise ProjectUpdateError("authenticated app viewer does not match configured publishing policy")
        return actor, workspace

    def resolve_projects(self, issue_ids: Sequence[str], *, workspace: str) -> dict[str, list[str]]:
        if not issue_ids or len(set(issue_ids)) != len(issue_ids):
            raise ProjectUpdateError("issue IDs must be non-empty and unique")
        groups: dict[str, list[str]] = {}
        self._unprojected = []
        self._issue_native_ids = {}
        for original_id in issue_ids:
            self._issue_source(original_id)
            issue = self._query(_ISSUE, {"id": original_id}).get("data", {}).get("issue")
            project = issue.get("project") if isinstance(issue, dict) else None
            team = issue.get("team") if isinstance(issue, dict) else None
            organization = team.get("organization") if isinstance(team, dict) else None
            project_id = project.get("id") if isinstance(project, dict) else None
            issue_workspace = organization.get("id") if isinstance(organization, dict) else None
            if issue_workspace != workspace:
                raise ProjectUpdateError(f"source issue {original_id} is outside authenticated workspace")
            if not isinstance(project_id, str) or not project_id:
                self._unprojected.append(original_id)
                continue
            identifier, url = issue.get("identifier") if isinstance(issue, dict) else None, issue.get("url") if isinstance(issue, dict) else None
            if (not isinstance(identifier, str) or original_id not in {identifier, issue.get('id')}
                    or re.fullmatch(r'[A-Z][A-Z0-9]*-[1-9][0-9]*', identifier) is None
                    or not isinstance(url, str) or not url.startswith('https://linear.app/')):
                raise ProjectUpdateError(f"source issue {original_id} has incomplete identity")
            self._issue_urls[original_id] = url
            self._issue_identifiers[original_id] = identifier
            native_issue_id = issue.get("id")
            if not isinstance(native_issue_id, str) or not native_issue_id:
                raise ProjectUpdateError(f"source issue {original_id} has incomplete identity")
            self._issue_native_ids[original_id] = native_issue_id
            groups.setdefault(project_id, []).append(original_id)
        if not groups:
            raise NoProjectUpdate('source issues have no project: ' + ', '.join(self._unprojected))
        return groups

    @staticmethod
    def _native_primary_issue_id() -> str | None:
        """Return the worker-injected primary issue, never a model-provided flag.

        Hermes bridges the active ``SessionSource`` fields to every tool
        subprocess.  The Linear worker owns this local source and stamps its
        canonical issue UUID in ``thread_id``; ordinary chat sessions cannot
        accidentally acquire this context.
        """
        if os.environ.get("HERMES_SESSION_PLATFORM") != "local":
            return None
        if not os.environ.get("HERMES_SESSION_CHAT_ID", "").startswith("linear:"):
            return None
        marker = os.environ.get("HERMES_SESSION_THREAD_ID", "")
        prefix = "linear-primary:"
        candidate = marker.removeprefix(prefix)
        if candidate == marker:
            return None
        try:
            canonical = str(uuid.UUID(candidate))
        except ValueError:
            return None
        return canonical if canonical == candidate else None

    @staticmethod
    def deterministic_id(actor: str, workspace: str, project_id: str, session_key: str) -> str:
        return str(uuid.uuid5(_NAMESPACE, "\x1f".join((actor, workspace, project_id, session_key))))

    def _checked_update(self, update_id: str, *, project_id: str, body: str, actor: str) -> dict[str, str] | None:
        update = self._query(_UPDATE, {"id": update_id}).get("data", {}).get("projectUpdate")
        if update is None:
            return None
        project = update.get("project") if isinstance(update, dict) else None
        user = update.get("user") if isinstance(update, dict) else None
        url = update.get("url") if isinstance(update, dict) else None
        if not isinstance(update, dict) or update.get("id") != update_id or not self.markdown_equivalent(body, update.get("body")) or not isinstance(project, dict) or project.get("id") != project_id or not isinstance(user, dict) or user.get("id") != actor or not isinstance(url, str) or not url:
            raise ProjectUpdateError("existing deterministic project update has different payload or author")
        health = update.get('health')
        if health not in {'onTrack', 'atRisk', 'offTrack'}:
            raise ProjectUpdateError('project update health readback is unavailable')
        return {"status": "existing", "project_id": project_id, "update_id": update_id, "url": url, "authored_by": actor, "health": health}

    def publish(self, *, session_key: str, issue_ids: Sequence[str], project_summaries: Mapping[str, str]) -> list[dict[str, str]]:
        session_key = self._require_source(session_key, "session key")
        for issue_id in issue_ids:
            self._issue_source(issue_id)
        actor, workspace = self._viewer()
        groups = self.resolve_projects(issue_ids, workspace=workspace)
        return self._publish_resolved(session_key, actor, workspace, groups, project_summaries)

    def _publish_resolved(self, session_key: str, actor: str, workspace: str,
                          groups: Mapping[str, Sequence[str]], project_summaries: Mapping[str, str]) -> list[dict[str, str]]:
        self._require_source(session_key, "session key")
        if set(project_summaries) != set(groups):
            raise ProjectUpdateError("project summaries must exactly match resolved projects")
        native_primary_issue_id = self._native_primary_issue_id()
        results: list[dict[str, str]] = []
        for project_id in sorted(groups):
            body = project_summaries[project_id]
            if not isinstance(body, str) or not body.strip():
                raise ProjectUpdateError(f"invalid summary for project {project_id}")
            if native_primary_issue_id is not None and any(
                self._issue_native_ids.get(issue_id) == native_primary_issue_id
                for issue_id in groups[project_id]
            ):
                results.append({"status": "skipped_native_primary_project", "project_id": project_id})
                continue
            update_id = self.deterministic_id(actor, workspace, project_id, session_key)
            existing = self._checked_update(update_id, project_id=project_id, body=body, actor=actor)
            if existing is not None:
                results.append(existing)
                continue
            project = self._query(_PROJECT_HEALTH, {'id': project_id})['data'].get('project')
            health = project.get('health') if isinstance(project, dict) else None
            if not isinstance(project, dict) or project.get('id') != project_id or health not in {'onTrack', 'atRisk', 'offTrack'}:
                raise ProjectUpdateError('current project health is unavailable; refusing an inferred health')
            input_ = {"id": update_id, "body": body, "projectId": project_id, "health": health}
            try:
                created = self._query(_CREATE, {"input": input_})
                payload = created.get("data", {}).get("projectUpdateCreate", {})
                if not isinstance(payload, dict) or payload.get("success") is not True:
                    raise ProjectUpdateError("Linear rejected project update create")
            except ProjectUpdateError:
                # A transport timeout may have committed. Re-read exact ID once;
                # never retry a create that can duplicate externally.
                reconciled = self._checked_update(update_id, project_id=project_id, body=body, actor=actor)
                if reconciled is None:
                    raise
                if reconciled['health'] != health:
                    raise ProjectUpdateError('project health readback differs from the carried-forward value')
                results.append(reconciled)
                continue
            readback = self._checked_update(update_id, project_id=project_id, body=body, actor=actor)
            if readback is None:
                raise ProjectUpdateError("project update create readback is absent")
            if readback['health'] != health:
                raise ProjectUpdateError('project health readback differs from the carried-forward value')
            readback["status"] = "created"
            results.append(readback)
        results.extend({'status': 'skipped_no_project', 'issue_id': issue_id} for issue_id in self._unprojected)
        return results


def publish_session_updates(graphql: Callable[[str, dict[str, object]], dict[str, object]], session_key: str, issues: Sequence[Mapping[str, object]], *, configured_workspace: str | None = None, configured_actor: str | None = None) -> list[dict[str, str]]:
    """Chat/lifecycle entry point with summaries constrained to each source issue."""
    issue_ids: list[str] = []
    by_issue: dict[str, str] = {}
    for source in issues:
        if not isinstance(source, Mapping):
            raise ProjectUpdateError("invalid issue summary source")
        issue_id = LinearProjectUpdatePublisher._issue_source(source.get("issue_id"))
        summary = LinearProjectUpdatePublisher._require_source(source.get("summary"), f"summary for source issue {issue_id}")
        if issue_id in by_issue:
            raise ProjectUpdateError("issue IDs must be unique")
        issue_ids.append(issue_id)
        by_issue[issue_id] = summary
    publisher = LinearProjectUpdatePublisher(graphql, configured_workspace=configured_workspace, configured_actor=configured_actor)
    actor, workspace = publisher._viewer()
    groups = publisher.resolve_projects(issue_ids, workspace=workspace)
    # The body is composed only from sources inside that project group.
    summaries = {project_id: "\n\n".join(f"### [{publisher._issue_identifiers[issue_id]}]({publisher._issue_urls[issue_id]})\n{by_issue[issue_id]}" for issue_id in ids) for project_id, ids in groups.items()}
    return publisher._publish_resolved(session_key, actor, workspace, groups, summaries)


def _publisher_settings(config: object, session_profile: str | None) -> dict[str, object]:
    """Publishing permission is independent from worker dispatch enablement."""
    plugins = config.get("plugins") if isinstance(config, dict) else None
    entries = plugins.get("entries") if isinstance(plugins, dict) else None
    entry = entries.get("linear-agent") if isinstance(entries, dict) else None
    settings = entry.get("settings") if isinstance(entry, dict) else None
    if not isinstance(settings, dict):
        raise ProjectUpdateError("active Linear publishing settings are unavailable")
    profile, workspace = settings.get("profile"), settings.get("workspace")
    if not isinstance(profile, str) or not profile or profile != session_profile:
        raise ProjectUpdateError("current Hermes session profile does not match publishing policy")
    if not isinstance(workspace, str) or not workspace:
        raise ProjectUpdateError("Linear publishing workspace is unavailable")
    return settings


def _publisher_binding(profile: str, workspace: str, vault_id: str, item_id: str) -> PublisherBinding:
    """Resolve a publisher-only roster; it never changes worker dispatch scope."""
    try:
        policy = json.loads(Path(__file__).with_name("linear-publishers.json").read_text(encoding="utf-8"))
        matches = [entry for entry in policy.get("publishers", []) if isinstance(entry, dict) and entry.get("profile") == profile]
    except (OSError, ValueError, AttributeError) as exc:
        raise ProjectUpdateError("immutable Linear publishing policy is unavailable") from exc
    if len(matches) != 1:
        raise ProjectUpdateError("current profile is not authorized to publish Linear project updates")
    entry = matches[0]
    oauth = entry.get("oauth")
    if (not isinstance(oauth, dict) or entry.get("workspace") != workspace
            or oauth.get("mode") != "managed_oauth_v1" or oauth.get("vault_id") != vault_id
            or oauth.get("item_id") != item_id):
        raise ProjectUpdateError("active Linear OAuth does not match immutable publishing policy")
    actor, organization = entry.get("viewer_id"), entry.get("organization_id")
    if not isinstance(actor, str) or not actor or not isinstance(organization, str) or not organization:
        raise ProjectUpdateError("immutable publishing identity policy is incomplete")
    return {"viewer_id": actor, "organization_id": organization}


def _configured_publisher() -> LinearProjectUpdatePublisher:
    import os
    home_value = os.environ.get("HERMES_HOME")
    if not home_value or not os.environ.get("HERMES_SESSION_ID"):
        raise ProjectUpdateError("HERMES_HOME and current HERMES_SESSION_ID are required")
    home = Path(home_value).absolute()
    validate_private_directory(home)
    try:
        import yaml
        config = yaml.safe_load((home / "config.yaml").read_text(encoding="utf-8"))
    except Exception as exc:
        raise ProjectUpdateError("active Hermes configuration is unavailable") from exc
    entry = _publisher_settings(config, os.environ.get("HERMES_SESSION_PROFILE"))
    profile, workspace = str(entry["profile"]), str(entry["workspace"])
    if entry.get("credential_mode") != "managed_oauth_v1":
        raise ProjectUpdateError("managed Linear OAuth is required")
    oauth_file = home / "secrets" / "linear-oauth.json"
    connect_file = home / ".op.env"
    if Path(str(entry.get("oauth_file", ""))) != oauth_file or Path(str(entry.get("connect_env_file", ""))) != connect_file:
        raise ProjectUpdateError("active managed OAuth paths are invalid")
    vault_id, item_id = str(entry.get("oauth_vault_id", "")), str(entry.get("oauth_item_id", ""))
    if not vault_id or not item_id:
        raise ProjectUpdateError("managed OAuth policy identifiers are unavailable")
    binding = _publisher_binding(profile, workspace, vault_id, item_id)
    validate_private_directory(oauth_file.parent)
    client = LinearActivityClient(make_oauth(profile, home, vault_id, item_id))
    client.verify_authenticated()
    # The policy workspace may be a profile label rather than Linear's organization UUID;
    # its binding was checked above. Live viewer organization is the mutation fence.
    return LinearProjectUpdatePublisher(client._graphql, configured_workspace=binding["organization_id"], configured_actor=binding["viewer_id"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="publish one native Linear project update per affected project")
    parser.add_argument("--session-key", required=True)
    parser.add_argument("--issues-json", required=True, help='JSON list: [{"issue_id":"TEAM-1","summary":"..."}]')
    args = parser.parse_args(argv)
    try:
        sources = json.loads(args.issues_json)
    except json.JSONDecodeError as exc:
        raise ProjectUpdateError("--issues-json must be JSON") from exc
    if not isinstance(sources, list):
        raise ProjectUpdateError("--issues-json must be a list")
    publisher = _configured_publisher()
    try:
        result = publish_session_updates(publisher.graphql, args.session_key, sources, configured_workspace=publisher.configured_workspace, configured_actor=publisher.configured_actor)
    except NoProjectUpdate as exc:
        print(json.dumps({"status": "skipped_no_project", "reason": str(exc)}, sort_keys=True))
        return 0
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProjectUpdateError as exc:
        print(f"linear project update refused: {exc}", file=sys.stderr)
        raise SystemExit(2)
