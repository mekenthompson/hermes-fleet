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
                except Exception:  # noqa: BLE001 - one bad tick must not stop the service
                    log.exception("linear: tick failed")
                try:
                    await asyncio.wait_for(runtime.stop_event.wait(), timeout=float(settings.get("tick_seconds", 2)))
                except asyncio.TimeoutError:
                    pass
        finally:
            running.pop("bridge", None)

    ctx.register_profile_service("linear", service)
