"""Default-disabled Linear integration: Linear records work; profile Kanban executes it. Settings are in README.md."""
from __future__ import annotations

import asyncio
import logging
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from . import chat
from .api import ENDPOINT, LinearAPI, LinearError
from .bridge import Bridge, Kanban, validate_activation_cutoff_ms
from .oauth import token_provider
from .store import Store

log = logging.getLogger("linear")
SETTINGS = ("identity", "credentials", "states", "team_states", "completion_contracts", "quiet_minutes", "recheck_minutes",
            "api_url", "board", "ingress_database", "state_database", "tick_seconds", "activation_cutoff_ms",
            "specialist_scope")
class BoundLinearAPI(LinearAPI):
    """Bind credentials to an actor/workspace and, optionally, a bounded specialist scope."""

    SCOPE_KEYS = {"allowed_team_ids", "allowed_project_ids", "allowed_requester_ids"}
    ALL_SCOPE_KEYS = {"all_teams", "all_projects", "allowed_requester_ids"}
    def __init__(self, token, *, identity, specialist_scope=None, **kwargs):
        if not isinstance(identity, dict) or any(
                not isinstance(identity.get(k), str) or not identity[k].strip()
                for k in ("viewer_id", "organization_id")):
            raise ValueError("linear: identity needs viewer_id and organization_id")
        for key in ("teams", "projects"):
            if key in identity and (not isinstance(identity[key], list) or not identity[key] or
                                    any(not isinstance(v, str) or not v.strip() for v in identity[key])):
                raise ValueError(f"linear: identity.{key} must be a nonempty list of IDs or keys")
        self.identity = {k: list(v) if isinstance(v, list) else v for k, v in identity.items()}
        self.specialist_scope = self._validate_specialist_scope(specialist_scope)
        self._graphql_permit = threading.local()
        super().__init__(token, **kwargs)
    @classmethod
    def _validate_specialist_scope(cls, scope):
        if scope is None: return None
        if not isinstance(scope, dict) or set(scope) not in (cls.SCOPE_KEYS, cls.ALL_SCOPE_KEYS):
            raise ValueError("linear: specialist_scope requires exact team/project/requester lists or explicit all_teams/all_projects with a requester list")
        result = {}
        if set(scope) == cls.ALL_SCOPE_KEYS:
            if scope["all_teams"] is not True or scope["all_projects"] is not True:
                raise ValueError("linear: all_teams and all_projects must both be explicitly true")
            result = {"all_teams": True, "all_projects": True}
        for key in sorted(cls.SCOPE_KEYS):
            if key not in scope: continue
            values = scope.get(key)
            if (not isinstance(values, list) or not values or
                    any(not isinstance(value, str) or not value or value != value.strip() for value in values) or
                    len(set(values)) != len(values)):
                raise ValueError(f"linear: specialist_scope.{key} must be a nonempty list of unique exact IDs")
            result[key] = list(values)
        return result
    @contextmanager
    def _permit_specialist_operation(self):
        depth = getattr(self._graphql_permit, "depth", 0)
        self._graphql_permit.depth = depth + 1
        try:
            yield
        finally:
            self._graphql_permit.depth = depth
    def verify_identity(self):
        self._verified_credential(self.token)
    def _verified_credential(self, provider):
        credential = None
        def capture():
            nonlocal credential
            credential = provider()
            return credential
        def invalidate():
            nonlocal provider
            provider.invalidate()
            provider = self.token
        capture.invalidate = invalidate if callable(getattr(provider, "invalidate", None)) else None
        identity = super()._graphql("query IdentityBinding { viewer { id } organization { id } }",
                                    token_provider=capture)
        if any(not isinstance(identity.get(field), dict) or
               identity[field].get("id") != self.identity[expected]
               for field, expected in (("viewer", "viewer_id"), ("organization", "organization_id"))):
            raise LinearError("Linear actor/workspace does not match configured identity", retryable=False)
        return provider, credential
    def graphql(self, query, variables=None):
        if self.specialist_scope is not None and not getattr(self._graphql_permit, "depth", 0):
            raise LinearError("Arbitrary Linear GraphQL is disabled by specialist_scope", retryable=False)
        return super()._graphql(query, variables, verify_credential=self._verified_credential)
    def viewer_id(self) -> str:
        self.verify_identity()
        return str(self.identity["viewer_id"])
    def _check_issue(self, issue):
        if self.specialist_scope is None:
            return
        if (not isinstance(issue, dict) or not isinstance(issue.get("id"), str) or not issue["id"].strip()):
            raise LinearError("Linear issue authorization response is malformed", retryable=False)
        team, project, creator = issue.get("team"), issue.get("project"), issue.get("creator")
        if (not isinstance(team, dict) or not isinstance(team.get("id"), str) or not team["id"] or
                (not self.specialist_scope.get("all_teams") and team["id"] not in self.specialist_scope["allowed_team_ids"]) or
                (not self.specialist_scope.get("all_projects") and
                 (not isinstance(project, dict) or project.get("id") not in self.specialist_scope["allowed_project_ids"])) or
                (self.specialist_scope.get("all_projects") and project is not None and
                 (not isinstance(project, dict) or not isinstance(project.get("id"), str) or not project["id"])) or
                not isinstance(creator, dict) or creator.get("id") not in self.specialist_scope["allowed_requester_ids"]):
            raise LinearError("Linear issue is outside configured specialist scope", retryable=False, authoritative_issue_id=issue.get("id"))
    def issue(self, ref):
        with self._permit_specialist_operation():
            issue = super().issue(ref)
        if ref not in (issue.get("id"), issue.get("identifier")):
            raise LinearError("Linear issue resolution does not match the requested ref")
        team = issue.get("team") or {}
        if self.identity.get("teams") and not {team.get("id"), team.get("key")} & set(self.identity["teams"]):
            raise LinearError("Linear issue team is outside configured scope", retryable=False, authoritative_issue_id=issue.get("id"))
        if self.identity.get("projects") and (issue.get("project") or {}).get("id") not in self.identity["projects"]:
            raise LinearError("Linear issue project is outside configured scope", retryable=False, authoritative_issue_id=issue.get("id"))
        self._check_issue(issue)
        return issue
    def _mutation_issue(self, issue_id):
        issue = self.issue(issue_id)
        if not isinstance(issue_id, str) or not issue_id.strip() or issue.get("id") != issue_id:
            raise LinearError("Linear issue authorization does not match mutation target", retryable=False)
        return issue
    def agent_session(self, session_id):
        if self.specialist_scope is None:
            raise LinearError("Agent Session resolution requires specialist_scope", retryable=False)
        query = ("query AgentSessionAuthorization($id: String!) { agentSession(id: $id) "
                 "{ id creator { id } appUser { id } issue { id } } }")
        with self._permit_specialist_operation():
            session = self.graphql(query, {"id": session_id}).get("agentSession")
        if not isinstance(session, dict) or session.get("id") != session_id:
            raise LinearError("Linear Agent Session could not be authoritatively resolved", retryable=False)
        issue = session.get("issue")
        creator = session.get("creator")
        creator_id = creator.get("id") if isinstance(creator, dict) else None
        if not isinstance(issue, dict) or not isinstance(issue.get("id"), str) or not issue["id"].strip():
            raise LinearError("Linear Agent Session requester or issue is outside specialist scope", retryable=False)
        if creator is None and isinstance(session.get("appUser"), dict) and session["appUser"].get("id") == self.identity["viewer_id"]:
            # Automatic delegation sessions have no creator. Admit only this
            # app's authoritative session on an authorized creator's issue.
            self.issue(issue["id"])
            return session
        if not isinstance(creator_id, str) or creator_id not in (
                self.identity["viewer_id"], *self.specialist_scope["allowed_requester_ids"]):
            raise LinearError("Linear Agent Session requester is outside specialist scope", retryable=False,
                              authoritative_issue_id=issue["id"])
        return session
    def agent_activity(self, activity_id, session_id):
        if self.specialist_scope is None:
            raise LinearError("Agent Activity resolution requires specialist_scope", retryable=False)
        query = ("query AgentActivityAuthorization($id: String!) { agentActivity(id: $id) "
                 "{ id user { id } agentSession { id } } }")
        with self._permit_specialist_operation():
            activity = self.graphql(query, {"id": activity_id}).get("agentActivity")
        user = activity.get("user") if isinstance(activity, dict) else None
        session = activity.get("agentSession") if isinstance(activity, dict) else None
        user_id = user.get("id") if isinstance(user, dict) else None
        if (not isinstance(activity, dict) or activity.get("id") != activity_id or
                not isinstance(session, dict) or session.get("id") != session_id):
            raise LinearError("Linear Agent Activity could not be authoritatively resolved", retryable=False)
        if not isinstance(user_id, str) or user_id not in self.specialist_scope["allowed_requester_ids"]:
            raise LinearError("Linear Agent Activity actor is outside specialist scope", retryable=False, authoritative_session_id=session_id)
        return activity
    def update_issue(self, issue_id, fields):
        if self.specialist_scope is not None:
            issue = self._mutation_issue(issue_id)
            if (not isinstance(fields, dict) or set(fields) - {"stateId", "delegateId"} or
                    ("delegateId" in fields and fields["delegateId"] != self.identity["viewer_id"]) or
                    ("stateId" in fields and fields["stateId"] not in {
                        item.get("id") for item in ((issue.get("team") or {}).get("states") or {}).get("nodes") or []
                    })):
                raise LinearError("Linear issue update exceeds specialist scope", retryable=False)
        elif self.identity.get("teams") or self.identity.get("projects"):
            self._mutation_issue(issue_id)
        with self._permit_specialist_operation():
            return super().update_issue(issue_id, fields)
    def create_comment(self, client_id, issue_id, body):
        if self.specialist_scope is not None or self.identity.get("teams") or self.identity.get("projects"):
            self._mutation_issue(issue_id)
        with self._permit_specialist_operation():
            return super().create_comment(client_id, issue_id, body)
    def create_activity(self, client_id, session_id, content, *, issue_id=None):
        if self.specialist_scope is not None:
            if not isinstance(issue_id, str) or not issue_id:
                raise LinearError("Specialist Agent Activity requires its owning issue id", retryable=False)
            session = self.agent_session(session_id)
            if session["issue"]["id"] != issue_id:
                raise LinearError("Agent Activity session does not belong to the authorized issue", retryable=False)
            self._mutation_issue(issue_id)
        with self._permit_specialist_operation():
            return super().create_activity(client_id, session_id, content)

    def create_project_update(self, client_id, project_id, body, *, issue_ids=None):
        if self.specialist_scope is not None:
            if not isinstance(issue_ids, list) or not issue_ids:
                raise LinearError("Specialist project update requires its owning issue ids", retryable=False)
            if not self.specialist_scope.get("all_projects") and project_id not in self.specialist_scope["allowed_project_ids"]:
                raise LinearError("Linear project is outside configured specialist scope", retryable=False)
            for issue_id in issue_ids:
                if (self._mutation_issue(issue_id).get("project") or {}).get("id") != project_id:
                    raise LinearError("Project update issue is outside configured specialist scope", retryable=False)
        elif self.identity.get("projects") and project_id not in self.identity["projects"]:
            raise LinearError("Linear project is outside configured scope", retryable=False)
        with self._permit_specialist_operation():
            return super().create_project_update(client_id, project_id, body)

async def process_chat_stops(bridge: Bridge, runtime: Any) -> None:
    """Run core's ordinary-chat API on the profile service's gateway loop."""
    gateway = runtime.gateway
    for intent in bridge.store.stop_intents():
        if intent["profile"] != bridge.profile or intent["status"] not in ("requested", "accepted"):
            continue
        if intent["status"] == "accepted" and intent["worker_completion"] == "completed": continue
        if not bridge.authorize_specialist_effect(intent["issue_id"]): continue
        if not callable(getattr(gateway, "request_chat_run_stop", None)) or not callable(
                getattr(gateway, "get_chat_run_stop_observation", None)):
            bridge.store.stop_result(intent["id"], "unsupported", "unknown", at=bridge.clock())
            continue
        target = {"session_key": intent["session_key"], "profile_home": runtime.profile_home}
        try:
            if intent["status"] == "accepted":
                observed = await gateway.get_chat_run_stop_observation(
                    **target, run_generation=intent["run_generation"])
                if not bridge.authorize_specialist_effect(intent["issue_id"]): continue
                completion = observed.get("worker_completion", "unknown") if observed.get("status") == "observed" else "unknown"
                if completion != intent["worker_completion"]:
                    bridge.store.stop_result(intent["id"], "accepted", completion, at=bridge.clock())
                continue
            observed = await gateway.get_chat_run_stop_observation(
                **target, run_generation=intent["run_generation"])
            if not bridge.authorize_specialist_effect(intent["issue_id"]): continue
            if observed.get("status") == "observed":
                status = observed.get("stop_status", "unknown")
                completion = observed.get("worker_completion", "unknown")
            else:
                if not bridge.authorize_specialist_effect(intent["issue_id"]): continue
                receipt = await gateway.request_chat_run_stop(
                    **target, expected_run_generation=intent["run_generation"])
                if not bridge.authorize_specialist_effect(intent["issue_id"]): continue
                status = receipt.get("status", "unknown")
                completion = receipt.get("worker_completion", "unknown")
            if status not in ("accepted", "stale", "not_running", "unsupported"): status = "unknown"
            if completion not in ("pending", "completed", "unknown"):
                completion = "unknown"
            bridge.store.stop_result(intent["id"], status, completion, at=bridge.clock())
        except Exception:  # noqa: BLE001 - a crash after core accepted stays ambiguous
            log.exception("linear: chat Stop observation/request uncertain for issue %s", intent["issue_id"])

def register(ctx: Any) -> None:
    if ctx.get_config("enabled", False) is not True:
        return
    running: dict[str, Bridge] = {}

    def tool(args: dict[str, Any] | None = None, invocation_context: Any = None, **_: Any) -> str:
        return chat.handle(running.get("bridge"), args or {}, invocation_context)

    def on_session_end(session_id: str = "", **_: Any) -> None:
        chat.on_turn_end(running.get("bridge"), session_id)

    ctx.register_tool(name="linear", toolset="linear", schema=chat.SCHEMA, handler=tool,
                      description=chat.SCHEMA["description"], inject_invocation_context=True)
    ctx.register_hook("on_session_end", on_session_end)

    async def service(runtime: Any) -> None:
        settings = {key: ctx.get_config(key) for key in SETTINGS if ctx.get_config(key) is not None}
        validate_activation_cutoff_ms(settings.get("activation_cutoff_ms"))
        home = Path(runtime.profile_home)
        api = BoundLinearAPI(lambda: "", identity=settings.get("identity"),
                             specialist_scope=settings.get("specialist_scope"),
                             endpoint=settings.get("api_url") or ENDPOINT)
        api.token = token_provider(settings, home)  # identity settings validated before credentials
        await asyncio.to_thread(api.viewer_id)  # refuse before state, recovery, or service admission
        bridge = Bridge(Store(settings.get("state_database") or home / "linear" / "state.db"), api,
                        await asyncio.to_thread(Kanban, settings.get("board"), profile=runtime.profile_name,
                                                profile_home=home), profile=runtime.profile_name,
                        settings=settings, inject=lambda key, text: bool(ctx.inject_message(text, session_key=key)))
        ingress = Path(settings.get("ingress_database") or home / "workspace" / "linear" / "ingress.db")
        running["bridge"] = bridge
        try:
            await asyncio.to_thread(bridge.recover, ingress)
            while not runtime.stop_event.is_set():
                try:
                    await asyncio.to_thread(bridge.tick, ingress)
                    await process_chat_stops(bridge, runtime)
                    await asyncio.to_thread(bridge.flush)
                except Exception:  # noqa: BLE001 - one bad tick must not stop the service
                    log.exception("linear: tick failed")
                try:
                    await asyncio.wait_for(runtime.stop_event.wait(), timeout=float(settings.get("tick_seconds", 2)))
                except asyncio.TimeoutError:
                    pass
        finally:
            running.pop("bridge", None)

    ctx.register_profile_service("linear", service)
