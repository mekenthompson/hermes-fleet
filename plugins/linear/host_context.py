"""Validate immutable host context against native execution; no plugin admission."""
from __future__ import annotations
from pathlib import Path
import sqlite3
from contextlib import closing


def bound_to_live_turn(runtime, context):
    """Verify the immutable host snapshot against the current gateway turn.

    This is an in-process boundary, not a sandbox against arbitrary same-UID
    code. Serialized chat RPC requests deliberately cannot acquire this grant.
    """
    from tools import registry
    if not isinstance(context, registry.ToolInvocationContext):
        return False
    if context != registry._current_tool_invocation_context():
        return False
    if (not context.profile or not context.session_key or not context.session_id or
            not context.platform or not context.chat_id or
            type(context.run_generation) is not int or context.run_generation < 1):
        return False
    try:
        gateway = runtime.gateway
        state = gateway._peek_session_state(context.session_key)
        if (state is None or state.turn.agent is None or context.profile != runtime.profile_name or
                state.persistent.run_generation != context.run_generation or
                not gateway._chat_stop_profile_matches(context.session_key, runtime.profile_home) or
                not gateway._is_user_authorized_for_source(state.turn.event.source)):
            return False
        turn = state.turn.ctx
        if (turn.session_key != context.session_key or turn.session_id != context.session_id or
                turn.run_generation != context.run_generation or not callable(turn._run_still_current) or
                not turn._run_still_current()):
            return False
        source = state.turn.event.source
        if source.platform.value != context.platform or any(
                (getattr(source, key, None) or "") != (getattr(context, key, None) or "")
                for key in ("chat_id", "thread_id", "chat_type", "user_id", "scope_id")):
            return False
        home = Path(runtime.profile_home).resolve(strict=True)
        with closing(sqlite3.connect(f"file:{home / 'state.db'}?mode=ro", uri=True, timeout=5)) as db:
            db.execute("PRAGMA query_only=ON")
            row = db.execute("SELECT ended_at FROM sessions WHERE id=?", (context.session_id,)).fetchone()
        return row is not None and row[0] is None
    except (AttributeError, OSError, ValueError, sqlite3.Error):
        return False

