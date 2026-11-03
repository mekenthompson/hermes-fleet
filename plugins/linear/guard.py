"""Chat guard: Linear-tracked work asked for in chat runs as native work, not inline.

When a chat turn's inbound message asks for work tied to Linear, the agent gets a short
inspection allowance. After it, only coordination tools run until the turn starts or binds
tracked work through the ``linear`` tool (``work_on``, ``create_issue`` with
``start_work``, or ``track``). A native worker then does the job, Linear shows it In
Progress, and Stop, restart, takeover and steering have something to follow.

Turn context comes from core hooks, not model arguments:

* ``pre_llm_call`` (once per turn): ``turn_id``, ``session_id``, ``platform``,
  ``user_message``, ``parent_session_id``. The guard arms here.
* ``pre_tool_call`` (every tool): ``tool_name``, ``session_id``, ``turn_id``. It counts or blocks.
* ``post_tool_call``: the ``linear`` result. A successful start or bind releases the turn.
* ``post_llm_call`` (turn end): forgets the turn.

The guard never runs in Linear-platform turns, cron, subagents or native Kanban workers, and
fails open: an exception is logged once per process and the tool call proceeds.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from typing import Any, Callable

from . import diag

# Inspection allowance once armed. Both count only non-coordination tools, and the clock
# starts at the first one, so model thinking time before any tool use does not spend it.
GRACE_TOOL_CALLS = 6
GRACE_SECONDS = 60.0
# Bounded memory for turns that never reach post_llm_call (interrupted or crashed turns).
MAX_TURNS = 256
TURN_TTL_SECONDS = 6 * 3600.0

# Turns from these platforms are never chat requests from a person.
NEVER_ARM_PLATFORMS = frozenset({"", "linear", "cron", "subagent", "webhook", "msgraph_webhook"})

# Coordination and read tools that stay available after the allowance: starting or steering
# tracked work, native Kanban, agent-to-agent messaging, asking the user, and the Tool Search
# catalog (the linear tool may be deferred behind it; tool_call is unwrapped by core).
COORDINATION_TOOLS = frozenset({
    "linear", "linear_work_reference", "message_agent", "clarify", "tool_search", "tool_describe",
})
COORDINATION_PREFIXES = ("kanban_", "a2a_")

# A successful result of one of these releases the turn: tracked work exists.
_START_ACTIONS = frozenset({"work_on", "track"})

# -- matcher ------------------------------------------------------------------
# Kept deliberately small; tests/test_linear_chat_guard.py carries the corpus.
# Code blocks, inline code and quoted lines are pasted material, not the request.
_CODE = re.compile(r"```.*?(?:```|\Z)|`[^`\n]*`|^\s*>.*$", re.S | re.M)
# A person's explicit choice to keep the work in this chat wins.
_OPT_OUT = re.compile(r"\b(?:don'?t|do\s+not|no\s+need\s+to)\s+track\b|\bno\s+tracking\b"
                      r"|\b(?:do|handle)\s+(?:it|this)\s+(?:here|inline)\b", re.I)
# Rule 1: an explicit tracking request ("track it", "track the work ... in Linear").
_TRACK = re.compile(r"\btrack\s+(?:it|this|that|the\s+work|this\s+work|(?:the\s+)?progress)\b"
                    r"(?![^.?!\n]{0,30}\bdown\b)"
                    r"|\btrack(?:ed)?\b[^.?!\n]{0,40}\b(?:in|on)\s+linear\b", re.I)
# Rule 2: Linear named as the tracker (not "linear regression", "linear time", "the linear plugin")
# together with a work verb.
_LINEAR_REF = re.compile(
    r"\blinear\.app/"
    r"|\blinear\s+(?:issue|ticket|card|project|board|backlog|cycle)s?\b"
    r"|\b(?:in|on|to|into|onto|via|through|under)\s+linear\b"
    r"(?=\s*$|\s*[.,;:!?()\[\]]|\s+[-–—]\s|\s+(?:and|then|so|too|as|please|pls|under|for|with|now|"
    r"first|project|team|board|thanks)\b)", re.I)
_WORK_VERB = re.compile(
    r"\b(?:research|investigate|write|draft|build|implement|fix|refactor|migrate|deploy|set\s+up|design|"
    r"analy[sz]e|audit|benchmark|compare|prepare|document|handle|tackle|finish|take\s+over|work\s+on|"
    r"pick\s+up|continue|resume|start\s+on|carry\s+on|ship)\b", re.I)
# Rule 3: an issue key together with a takeover/continuation verb ("take over QA-9", "finish HF-12").
# Case-sensitive keys keep "utf-8"/"covid-19" out; common non-issue prefixes are excluded.
_ISSUE_KEY = re.compile(r"(?<![A-Za-z0-9_-])(?!(?:SHA|UTF|ISO|RFC|CVE|GPT|COVID|HTTP|TLS|SSL|AES|RSA|PEP|IPV|"
                        r"WCAG|TCP|UDP|USB|ARM|MP)-)([A-Z]{2,6}-[0-9]{1,7})(?![A-Za-z0-9_]|-[0-9])")
_ISSUE_VERB = re.compile(r"\b(?:take|taking|took)\s+over\b|\bfinish(?:ing)?\b|\bwork(?:ing)?\s+on\b"
                         r"|\bcontinu(?:e|ing)\b|\bpick(?:ing)?\s+up\b|\bcarry\s+on\b|\bresume\b"
                         r"|\bdo(?:ing)?\s+(?:it|this|that|the\s+(?:rest|work|task|issue|ticket)|[A-Z]{2,6}-[0-9]+)\b",
                         re.I)
# Native task-status wakes (core gateway.kanban.wake.message) are notifications, not requests.
_WAKE = re.compile(r"^\s*\[kanban\]")


def _text(message: Any) -> str:
    """Plain text of a user message (string or multimodal content parts)."""
    if isinstance(message, str):
        return message
    if isinstance(message, list):
        return "\n".join(part.get("text", "") if isinstance(part, dict) else str(part)
                         for part in message if isinstance(part, (dict, str)))
    if isinstance(message, dict):
        return _text(message.get("content"))
    return ""


def match(message: Any) -> tuple[str, str] | None:
    """``(reason, issue_key)`` when the message asks for Linear-tracked work, else None."""
    raw = _text(message)
    if not raw or _WAKE.match(raw):
        return None
    text = _CODE.sub(" ", raw)
    if _OPT_OUT.search(text):
        return None
    keys = _ISSUE_KEY.findall(text)
    key = keys[0] if keys else ""
    if _TRACK.search(text):
        return "track_request", key
    if _LINEAR_REF.search(text) and _WORK_VERB.search(text):
        return "linear_work_request", key
    if key and _ISSUE_VERB.search(text):
        return "issue_work_request", key
    return None


def coordination(tool_name: str) -> bool:
    return tool_name in COORDINATION_TOOLS or tool_name.startswith(COORDINATION_PREFIXES)


def _started(tool_name: str, args: Any, result: Any) -> bool:
    """True for an ok ``linear`` reply that started or bound tracked work."""
    if tool_name != "linear" or not isinstance(args, dict):
        return False
    action = args.get("action")
    if action not in _START_ACTIONS and not (action == "create_issue" and args.get("start_work") is True):
        return False
    if args.get("record_done") is True:
        return False  # recording finished inline work binds nothing that runs
    try:
        reply = json.loads(result) if isinstance(result, str) else result
    except ValueError:
        return False
    return isinstance(reply, dict) and reply.get("ok") is True


def block_message(key: str, used: int, elapsed: float) -> str:
    issue = key or "<issue key>"
    return (
        "Blocked: this chat asked for Linear-tracked work, and this turn has used its inspection allowance "
        f"({used} tool calls, {int(elapsed)}s). Do not do the work in this chat turn. Start tracked work first "
        "with the linear tool: for an existing issue call "
        f'{{"action": "work_on", "issue": "{issue}"}}; for new work call '
        '{"action": "create_issue", "title": ..., "team": ..., "project": ..., "start_work": true, '
        '"id": "<new UUIDv4>"}. One native worker then does the job, Linear shows it In Progress, and '
        "progress and the result come back to this chat. linear (status, followup for steering), kanban_* "
        "and clarify stay available. If the start is refused, tell the user why instead of doing the work here.")


def _worker_process() -> bool:
    return bool(os.environ.get("HERMES_KANBAN_TASK"))


def _sess(session_id: Any) -> str:
    return hashlib.sha256(str(session_id).encode()).hexdigest()[:8] if session_id else ""


class _Turn:
    __slots__ = ("session", "key", "reason", "armed_at", "first_at", "used", "blocked")

    def __init__(self, session: str, key: str, reason: str, now: float) -> None:
        self.session, self.key, self.reason, self.armed_at = session, key, reason, now
        self.first_at: float | None = None
        self.used = 0
        self.blocked = False


class ChatGuard:
    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._turns: dict[str, _Turn] = {}
        self._latest: dict[str, str] = {}  # session id -> armed turn id (hooks without a turn id)
        self._failed = False

    # -- hook entry points: each is fail-open ---------------------------------
    def pre_llm_call(self, **kw: Any) -> None:
        self._safe(self._arm, kw)
        return None

    def pre_tool_call(self, **kw: Any) -> dict[str, str] | None:
        return self._safe(self._check, kw)

    def post_tool_call(self, **kw: Any) -> None:
        self._safe(self._observe, kw)

    def post_llm_call(self, **kw: Any) -> None:
        self._safe(self._end, kw)

    def _safe(self, fn: Callable[[dict], Any], kw: dict) -> Any:
        try:
            return fn(kw)
        except Exception as exc:  # fail open: the guard must never break a turn
            if not self._failed:
                self._failed = True
                diag.evt("chat.guard", "failed", "exception", level=logging.WARNING, exc=type(exc).__name__)
            return None

    # -- state ------------------------------------------------------------------
    def _arm(self, kw: dict) -> None:
        session, turn_id = str(kw.get("session_id") or ""), str(kw.get("turn_id") or "")
        with self._lock:
            # A new turn in a session supersedes whatever the previous one left behind.
            prior = self._latest.pop(session, None)
            if prior is not None:
                self._turns.pop(prior, None)
        if not turn_id or _worker_process() or kw.get("parent_session_id"):
            return
        if str(kw.get("platform") or "").strip().lower() in NEVER_ARM_PLATFORMS:
            return
        found = match(kw.get("user_message"))
        if found is None:
            return
        reason, key = found
        now = self._clock()
        with self._lock:
            self._prune(now)
            self._turns[turn_id] = _Turn(session, key, reason, now)
            if session:
                self._latest[session] = turn_id
        diag.evt("chat.guard", "armed", reason, issue=key or None, sess=_sess(session))

    def _turn(self, kw: dict) -> tuple[str, _Turn] | None:
        turn_id = str(kw.get("turn_id") or "") or self._latest.get(str(kw.get("session_id") or ""), "")
        turn = self._turns.get(turn_id) if turn_id else None
        return (turn_id, turn) if turn is not None else None

    def _check(self, kw: dict) -> dict[str, str] | None:
        if not self._turns:
            return None
        tool = str(kw.get("tool_name") or "")
        if coordination(tool):
            return None
        with self._lock:
            found = self._turn(kw)
            if found is None:
                return None
            _, turn = found
            now = self._clock()
            if turn.first_at is None:
                turn.first_at = now
            elapsed = now - turn.first_at
            if turn.used < GRACE_TOOL_CALLS and elapsed < GRACE_SECONDS:
                turn.used += 1
                return None
            first_block = not turn.blocked
            turn.blocked = True
            used, key, session = turn.used, turn.key, turn.session
        if first_block:
            diag.evt("chat.guard", "blocked", "grace_calls" if used >= GRACE_TOOL_CALLS else "grace_time",
                     issue=key or None, sess=_sess(session), kind=tool, attempt=used, ms=int(elapsed * 1000))
        return {"action": "block", "message": block_message(key, used, elapsed)}

    def _observe(self, kw: dict) -> None:
        if not self._turns or not _started(str(kw.get("tool_name") or ""), kw.get("args"), kw.get("result")):
            return
        with self._lock:
            found = self._turn(kw)
            if found is None:
                return
            turn_id, turn = found
            self._forget(turn_id, turn)
        diag.evt("chat.guard", "released", "tracked_work_started", issue=turn.key or None, sess=_sess(turn.session))

    def _end(self, kw: dict) -> None:
        if not self._turns:
            return
        with self._lock:
            found = self._turn(kw)
            if found is None:
                return
            turn_id, turn = found
            self._forget(turn_id, turn)
        diag.evt("chat.guard", "released", "turn_ended", issue=turn.key or None, sess=_sess(turn.session))

    def _forget(self, turn_id: str, turn: _Turn) -> None:
        self._turns.pop(turn_id, None)
        if self._latest.get(turn.session) == turn_id:
            self._latest.pop(turn.session, None)

    def _prune(self, now: float) -> None:
        for turn_id, turn in list(self._turns.items()):
            if now - turn.armed_at > TURN_TTL_SECONDS:
                self._forget(turn_id, turn)
        while len(self._turns) >= MAX_TURNS:
            turn_id = next(iter(self._turns))
            self._forget(turn_id, self._turns[turn_id])


def register(ctx: Any) -> ChatGuard | None:
    """Register the guard's hooks; native Kanban workers get none."""
    if _worker_process():
        return None
    guard = ChatGuard()
    for event in ("pre_llm_call", "pre_tool_call", "post_tool_call", "post_llm_call"):
        ctx.register_hook(event, getattr(guard, event))
    return guard
