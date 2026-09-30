"""Linear integration: Linear is the human record, this profile's Kanban board runs the work.

Off by default. Enable per profile with ``plugins.entries.linear.settings.enabled: true``;
the settings schema is in README.md.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from . import chat
from .api import ENDPOINT, LinearAPI, LinearError
from .bridge import Bridge, Kanban, validate_activation_cutoff_ms
from .oauth import token_provider
from .store import Store

log = logging.getLogger("linear")
SETTINGS = ("identity", "credentials", "states", "team_states", "completion_contracts", "quiet_minutes", "recheck_minutes",
            "api_url", "board", "ingress_database", "state_database", "tick_seconds", "activation_cutoff_ms")


class BoundLinearAPI(LinearAPI):
    """Bind configured credentials to an immutable deployment actor and workspace."""

    def __init__(self, token, *, identity, **kwargs):
        if not isinstance(identity, dict) or any(
                not isinstance(identity.get(k), str) or not identity[k].strip()
                for k in ("viewer_id", "organization_id")):
            raise ValueError("linear: identity needs viewer_id and organization_id")
        for key in ("teams", "projects"):
            if key in identity and (not isinstance(identity[key], list) or not identity[key] or
                                    any(not isinstance(v, str) or not v.strip() for v in identity[key])):
                raise ValueError(f"linear: identity.{key} must be a nonempty list of IDs or keys")
        self.identity = {k: list(v) if isinstance(v, list) else v for k, v in identity.items()}
        super().__init__(token, **kwargs)

    def verify_identity(self):
        identity = super().graphql("query IdentityBinding { viewer { id } organization { id } }")
        if any(not isinstance(identity.get(field), dict) or
               identity[field].get("id") != self.identity[expected]
               for field, expected in (("viewer", "viewer_id"), ("organization", "organization_id"))):
            raise LinearError("Linear actor/workspace does not match configured identity", retryable=False)

    def graphql(self, query, variables=None):
        self.verify_identity()
        return super().graphql(query, variables)

    def viewer_id(self) -> str:
        self.verify_identity()
        return str(self.identity["viewer_id"])

    def issue(self, ref):
        issue = super().issue(ref)
        team = issue.get("team") or {}
        if self.identity.get("teams") and not {team.get("id"), team.get("key")} & set(self.identity["teams"]):
            raise LinearError("Linear issue team is outside configured scope", retryable=False)
        if self.identity.get("projects") and (issue.get("project") or {}).get("id") not in self.identity["projects"]:
            raise LinearError("Linear issue project is outside configured scope", retryable=False)
        return issue

    def update_issue(self, issue_id, fields):
        if self.identity.get("teams") or self.identity.get("projects"):
            self.issue(issue_id)
        return super().update_issue(issue_id, fields)

    def create_comment(self, client_id, issue_id, body):
        if self.identity.get("teams") or self.identity.get("projects"):
            self.issue(issue_id)
        return super().create_comment(client_id, issue_id, body)

    def create_project_update(self, client_id, project_id, body):
        if self.identity.get("projects") and project_id not in self.identity["projects"]:
            raise LinearError("Linear project is outside configured scope", retryable=False)
        return super().create_project_update(client_id, project_id, body)


async def process_chat_stops(bridge: Bridge, runtime: Any) -> None:
    """Run core's ordinary-chat API on the profile service's gateway loop."""
    gateway = runtime.gateway
    for intent in bridge.store.stop_intents():
        if intent["profile"] != bridge.profile or intent["status"] not in ("requested", "accepted"):
            continue
        if intent["status"] == "accepted" and intent["worker_completion"] == "completed":
            continue
        if not callable(getattr(gateway, "request_chat_run_stop", None)) or not callable(
                getattr(gateway, "get_chat_run_stop_observation", None)):
            bridge.store.stop_result(intent["id"], "unsupported", "unknown", at=bridge.clock())
            continue
        target = {"session_key": intent["session_key"], "profile_home": runtime.profile_home}
        try:
            if intent["status"] == "accepted":
                observed = await gateway.get_chat_run_stop_observation(
                    **target, run_generation=intent["run_generation"])
                completion = observed.get("worker_completion", "unknown") if observed.get("status") == "observed" else "unknown"
                if completion != intent["worker_completion"]:
                    bridge.store.stop_result(intent["id"], "accepted", completion, at=bridge.clock())
                continue
            observed = await gateway.get_chat_run_stop_observation(
                **target, run_generation=intent["run_generation"])
            if observed.get("status") == "observed":
                status = observed.get("stop_status", "unknown")
                completion = observed.get("worker_completion", "unknown")
            else:
                receipt = await gateway.request_chat_run_stop(
                    **target, expected_run_generation=intent["run_generation"])
                status = receipt.get("status", "unknown")
                completion = receipt.get("worker_completion", "unknown")
            if status not in ("accepted", "stale", "not_running", "unsupported"):
                status = "unknown"
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
                             endpoint=settings.get("api_url") or ENDPOINT)
        api.token = token_provider(settings, home)  # identity settings validated before credentials
        await asyncio.to_thread(api.viewer_id)  # refuse before state, recovery, or service admission
        bridge = Bridge(Store(settings.get("state_database") or home / "linear" / "state.db"), api,
                        await asyncio.to_thread(Kanban, settings.get("board")), profile=runtime.profile_name,
                        settings=settings, inject=lambda key, text: bool(ctx.inject_message(text, session_key=key)))
        ingress = Path(settings.get("ingress_database") or home / "workspace" / "linear" / "ingress.db")
        running["bridge"] = bridge
        try:
            await asyncio.to_thread(bridge.recover)
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
