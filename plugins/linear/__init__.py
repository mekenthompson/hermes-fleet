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
from .api import ENDPOINT, LinearAPI
from .bridge import Bridge, Kanban
from .oauth import token_provider
from .store import Store

log = logging.getLogger("linear")
SETTINGS = ("credentials", "states", "team_states", "completion_contracts", "quiet_minutes", "recheck_minutes",
            "api_url", "board", "ingress_database", "state_database", "tick_seconds")


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
        home = Path(runtime.profile_home)
        api = LinearAPI(token_provider(settings, home), endpoint=settings.get("api_url") or ENDPOINT)
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
