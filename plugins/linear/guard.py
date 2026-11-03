"""Chat guard: Linear-tracked work asked for in chat runs as native work, not inline.

When a person's chat message asks the agent to do work tied to Linear, the turn gets a short
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

The guard never arms in Linear, cron, agent-to-agent, subagent or webhook turns, for
heartbeat or other machine-authored prompts, in native Kanban workers, or when this session
cannot reach the ``linear`` tool. It fails open: an exception is logged (at most hourly) and
the tool call proceeds. Settings ``chat_guard: false`` turns it off.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
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
FAILURE_LOG_EVERY_S = 3600.0

# Turns from these platforms are never a person's chat request.
NEVER_ARM_PLATFORMS = frozenset({"", "linear", "cron", "subagent", "webhook", "msgraph_webhook", "a2a"})

# Coordination and read tools that stay available after the allowance: starting or steering
# tracked work, native Kanban, agent-to-agent messaging, asking the user, and the Tool Search
# catalog (core unwraps tool_call, so the real tool name is what this guard sees).
COORDINATION_TOOLS = frozenset({
    "linear", "linear_work_reference", "message_agent", "clarify", "tool_search", "tool_describe",
})
COORDINATION_PREFIXES = ("kanban_", "a2a_")

# A successful result of one of these releases the turn: tracked work exists.
_START_ACTIONS = frozenset({"work_on", "track"})

# -- what the person typed ----------------------------------------------------
# Core wraps the typed text: a Discord triggering note and the gateway reply pointer
# (gateway/run_inbound.py _prepend_inbound_reply_context; same shape as core's
# agent/context_compressor.py _GATEWAY_REPLY_POINTER_RE), attached-document notes and inlined
# text documents (gateway/run.py _build_document_context_note, Slack "[Content of NAME]:"),
# and @-reference footers (agent/context_references.py). None of that is the request.
_REPLY_POINTER = re.compile(r'\A(?:\[Triggering message id: [^\n]*\]\n\n)?'
                            r'\[Replying to(?: your previous message)?: ".*?"\]\n\n', re.S)
_CONTEXT_FOOTER = re.compile(r"\n*--- (?:Context Warnings|Attached Context) ---\n.*", re.S)
_INLINED_DOCUMENT = re.compile(r"^\[Content of [^\n]*\]:\n.*\n\n", re.S | re.M)  # greedy: the typed text is last
_NOTE_LINE = re.compile(r"^\s*\[[^\]\n]*\]\s*$", re.M)  # document notes, app-context lines, sender tags alone
_SENDER_TAG = re.compile(r"^\s*\[[^\]\n]{1,80}\]\s+(?=\S)")
# Machine-authored turns: native task wakes, heartbeats (hermes_cli/heartbeat.py) and core notes.
_MACHINE = re.compile(r"\A\s*\[(?:kanban\]|Heartbeat\b|System note:|Runtime note:|SYSTEM\]|System:|"
                      r"CONTEXT COMPACTION)")
# Pasted material: code blocks, inline code and quoted lines.
_CODE = re.compile(r"```.*?(?:```|\Z)|`[^`\n]*`|^\s*>.*$", re.S | re.M)

# -- matcher ------------------------------------------------------------------
# Kept deliberately small; tests/test_linear_chat_guard.py carries the corpus. Every rule needs
# an imperative request to this agent: a clause that starts with the verb, optionally after
# "please", "now", "can you", "I need you to" and similar. A question (other than "can you
# ...?"), a negation ("don't do it until"), or another subject ("Atlas is working on",
# "I'll take over") is not one.
_OPT_OUT = re.compile(r"\b(?:don'?t|do\s+not|no\s+need\s+to)\s+track\b|\bno\s+tracking\b"
                      r"|\b(?:do|handle)\s+(?:it|this)\s+(?:here|inline)\b", re.I)
_SENTENCE = re.compile(r"[^.!?\n]+[.!?]*")
_CLAUSE_SPLIT = re.compile(r"[,;:()]|\s[-–—]\s|\s(?:and|then|but|so|also)\s", re.I)
_PREFACE = (r"^\s*(?:(?:ok(?:ay)?|now|then|also|pls|please|hey|right|great|thanks)\s+)*"
            r"(?:(?:can|could|would|will)\s+you\s+(?:please\s+)?|(?:i(?:\s+(?:need|want|would\s+like)|'d\s+like)"
            r"\s+you\s+to|you\s+should|go\s+ahead\s+and|time\s+to)\s+)?(?:please\s+)?")
_DO = r"do\s+(?:it|this|that|the\s+(?:rest|work|task|issue|ticket|job)|[A-Z]{2,6}-[0-9]+)\b"
_NOT = r"(?!\s+not\b)"
_TRACK = re.compile(_PREFACE + r"track\s+(?:it|this|that|the\s+work|this\s+work|(?:the\s+)?progress)\b"
                    r"(?!\s+down\b)(?![^.?!\n]{0,30}\bdown\b)", re.I)
_WORK = re.compile(_PREFACE + r"(?:research|investigate|write|draft|build|implement|fix|refactor|migrate|deploy|"
                   r"set\s+up|design|analy[sz]e|audit|benchmark|compare|prepare|document|handle|tackle|finish|"
                   r"take\s+over|work\s+on|pick\s+up|continue|resume|start\s+on|carry\s+on|ship)\b" + _NOT + "|" + _PREFACE + _DO,
                   re.I)
_ISSUE_WORK = re.compile(_PREFACE + r"(?:take\s+over|finish|work\s+on|continue|pick\s+up|carry\s+on|resume)\b"
                         + _NOT + "|" + _PREFACE + _DO, re.I)
# Linear named as the tracker, not "linear regression", "linear time" or "the linear plugin".
_LINEAR_REF = re.compile(
    r"\blinear\.app/"
    r"|\blinear\s+(?:issue|ticket|card|project|board|backlog|cycle)s?\b"
    r"|\b(?:in|on|to|into|onto|via|through|under)\s+linear\b"
    r"(?=\s*$|\s*[.,;:!?()\[\]]|\s+[-–—]\s|\s+(?:and|then|so|too|as|please|pls|under|for|with|now|"
    r"first|project|team|board|thanks)\b)", re.I)
# Case-sensitive keys keep "utf-8"/"covid-19" out; common non-issue prefixes are excluded.
_ISSUE_KEY = re.compile(r"(?<![A-Za-z0-9_-])(?!(?:SHA|UTF|ISO|RFC|CVE|GPT|COVID|HTTP|TLS|SSL|AES|RSA|PEP|IPV|"
                        r"WCAG|TCP|UDP|USB|ARM|MP)-)([A-Z]{2,6}-[0-9]{1,7})(?![A-Za-z0-9_]|-[0-9])")
_POLITE_QUESTION = re.compile(r"^\s*(?:(?:ok(?:ay)?|now|hey|please)\s+)*(?:can|could|would|will)\s+you\b", re.I)


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


def request_text(message: Any) -> str | None:
    """What the person typed this turn, or None for a machine-authored turn."""
    text = _REPLY_POINTER.sub("", _text(message), count=1)
    if _MACHINE.match(text):
        return None
    text = _CONTEXT_FOOTER.sub("", text)
    text = _INLINED_DOCUMENT.sub("", text)
    text = _NOTE_LINE.sub("", text)
    text = _SENDER_TAG.sub("", text)
    return _CODE.sub(" ", text).strip()


def _requests(text: str, pattern: re.Pattern) -> list[str]:
    """Sentences of ``text`` with a clause that is an imperative request matching ``pattern``."""
    found = []
    for sentence in _SENTENCE.findall(text):
        stripped = sentence.strip()
        if stripped.endswith("?") and not _POLITE_QUESTION.match(stripped):
            continue
        if any(pattern.match(clause) for clause in _CLAUSE_SPLIT.split(stripped)):
            found.append(stripped)
    return found


def match(message: Any) -> tuple[str, str] | None:
    """``(reason, issue_key)`` when the message asks this agent for Linear-tracked work, else None."""
    text = request_text(message)
    if not text or _OPT_OUT.search(text):
        return None
    keys = _ISSUE_KEY.findall(text)
    named = bool(keys) or bool(_LINEAR_REF.search(text))

    def key_in(sentences: list[str]) -> str:
        for sentence in sentences:
            found = _ISSUE_KEY.search(sentence)
            if found:
                return found.group(1)
        return keys[0] if keys else ""

    tracking = _requests(text, _TRACK)
    if tracking and named:
        return "track_request", key_in(tracking)
    if _LINEAR_REF.search(text):
        work = _requests(text, _WORK)
        if work:
            return "linear_work_request", key_in(work)
    issue_work = [s for s in _requests(text, _ISSUE_WORK) if _ISSUE_KEY.search(s)]
    if issue_work:
        return "issue_work_request", key_in(issue_work)
    return None


def coordination(tool_name: str) -> bool:
    return tool_name in COORDINATION_TOOLS or tool_name.startswith(COORDINATION_PREFIXES)


def linear_reachable() -> bool:
    """True only when this session can start tracked work with the linear tool.

    Uses core's resolved tool selection for the current agent (model_tools
    ``_last_resolved_tool_names``, set before Tool Search collapses deferred tools) and the
    plugin's own coordinator check. Unknown means unreachable, so the guard stays off.
    """
    from . import chat
    try:
        from agent.delegation_context import is_delegated_child_process_context
        if is_delegated_child_process_context():
            return False  # delegated children cannot start work
    except ImportError:
        pass
    if not chat.coordinator_inventory():
        return False
    names = getattr(sys.modules.get("model_tools"), "_last_resolved_tool_names", None)
    return isinstance(names, list) and "linear" in names


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
        f"({used} tool calls, {int(elapsed)}s). Do not do the work in this chat turn. If the user wants the work "
        "done now, start tracked work first with the linear tool: for an existing issue call "
        f'{{"action": "work_on", "issue": "{issue}"}}; for new work call '
        '{"action": "create_issue", "title": ..., "team": ..., "project": ..., "start_work": true, '
        '"id": "<new UUIDv4>"}. One native worker then does the job, Linear shows it In Progress, and '
        "progress and the result come back to this chat. If the start is refused, tell the user why. If the user "
        "did not ask for the work to be done now (a status question or a discussion), answer with what you "
        "already have, or ask with clarify: inspecting and discussing work does not transfer execution. "
        "linear (status, followup for steering), kanban_* and clarify stay available.")


def _worker_process() -> bool:
    return bool(os.environ.get("HERMES_KANBAN_TASK"))


def _sess(session_id: Any) -> str:
    return hashlib.sha256(str(session_id).encode()).hexdigest()[:8] if session_id else ""


def _kind(tool: str) -> str | None:
    """A tool name in the diag ``kind`` shape: ``mcp__github__create_pr`` -> ``mcp_github_create_pr``."""
    parts = [part[:24] for part in re.split(r"[^a-z]+", tool.lower()) if part][:4]
    return "_".join(parts) or None


class _Turn:
    __slots__ = ("session", "key", "reason", "armed_at", "first_at", "used", "blocked")

    def __init__(self, session: str, key: str, reason: str, now: float) -> None:
        self.session, self.key, self.reason, self.armed_at = session, key, reason, now
        self.first_at: float | None = None
        self.used = 0
        self.blocked = False


class ChatGuard:
    def __init__(self, clock: Callable[[], float] = time.monotonic,
                 reachable: Callable[[], bool] = linear_reachable) -> None:
        self._clock = clock
        self._reachable = reachable
        self._lock = threading.Lock()
        self._turns: dict[str, _Turn] = {}
        self._latest: dict[str, str] = {}  # session id -> armed turn id (hooks without a turn id)
        self._failed_at: float | None = None

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
            now = self._clock()
            if self._failed_at is None or now - self._failed_at >= FAILURE_LOG_EVERY_S:
                self._failed_at = now
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
        if found is None or not self._reachable():
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
                     issue=key or None, sess=_sess(session), kind=_kind(tool), attempt=used, ms=int(elapsed * 1000))
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
    """Register the guard's hooks unless ``chat_guard: false``; native Kanban workers get none."""
    if _worker_process() or ctx.get_config("chat_guard", True) is False:
        return None
    guard = ChatGuard()
    for event in ("pre_llm_call", "pre_tool_call", "post_tool_call", "post_llm_call"):
        ctx.register_hook(event, getattr(guard, event))
    return guard
