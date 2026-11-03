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
heartbeat or other machine-authored prompts, in native Kanban workers, or when core's
toolset resolution for the turn's platform does not include the ``linear`` tool (logged
once for that turn as ``outcome=skipped reason=linear_unreachable``). It fails open: an exception is logged (at most hourly) and
the tool call proceeds. Settings ``chat_guard: false`` (or "off", "0", "no") turns it off.
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
# bridge (core normally unwraps tool_call so the real tool name is checked; a bare tool_call
# that reaches this hook is the bridge itself).
COORDINATION_TOOLS = frozenset({
    "linear", "linear_work_reference", "message_agent", "clarify", "tool_search", "tool_describe", "tool_call",
})
COORDINATION_PREFIXES = ("kanban_", "a2a_")

# A successful result of one of these releases the turn: tracked work exists.
_START_ACTIONS = frozenset({"work_on", "track"})

# -- what the person typed ----------------------------------------------------
# Core wraps the typed text (pinned Agent b81aa0b0):
# * outermost, the reply pointer and Discord triggering note (gateway/run_inbound.py
#   _prepend_inbound_reply_context). Stripped greedily to the last '"]' + blank line, so a quote
#   that itself contains '"]' never leaks its tail into the request;
# * channel history before ``[New message]`` (gateway/run_inbound.py _prefix_inbound_sender_context):
#   Slack thread context, Discord recent channel messages. Only the text after the LAST marker is
#   the request; leftover history blocks without the marker make the turn unreadable (no arm);
# * the sender tag ``[Name | Slack user <@U..>]``, attached-document notes and inlined documents
#   (gateway/run.py _build_document_context_note, Slack "[Content of NAME]:"), and @-reference
#   footers (agent/context_references.py).
_REPLY_POINTER = re.compile(r'\A(?:\[Triggering message id: [^\n]*\]\n\n)?'
                            r'\[Replying to(?: your previous message)?: ".*"\]\n\n', re.S)
_NEW_MESSAGE = "[New message]\n"
_HISTORY = re.compile(r"\[(?:Thread context —|Recent channel messages\]|Context around the replied-to message\])")
_CONTEXT_FOOTER = re.compile(r"\n*--- (?:Context Warnings|Attached Context) ---\n.*", re.S)
_INLINED_DOCUMENT = re.compile(r"^\[Content of [^\n]*\]:\n.*\n\n", re.S | re.M)  # greedy: the typed text is last
_NOTE_LINE = re.compile(r"^\s*\[[^\]\n]*\]\s*$", re.M)  # document notes, app-context lines, sender tags alone
_SENDER_TAG = re.compile(r"^\s*\[[^\]\n]{1,120}\]\s+(?=\S)")
# Machine-authored turns: native task wakes, heartbeats (hermes_cli/heartbeat.py) and core notes.
_MACHINE = re.compile(r"\A\s*\[(?:kanban\]|Heartbeat\b|System note:|Runtime note:|SYSTEM\]|System:|"
                      r"CONTEXT COMPACTION)")
# Pasted material: code blocks, inline code and quoted lines.
_CODE = re.compile(r"```.*?(?:```|\Z)|`[^`\n]*`|^\s*>.*$", re.S | re.M)

# -- matcher ------------------------------------------------------------------
# Kept deliberately small; tests/test_linear_chat_guard.py carries the corpus. Every rule needs
# an imperative request to this agent: a clause that starts with the verb, optionally after
# "please", "now", "hey <name>", "can you", "I need you to" and similar. Not requests: a
# question (other than "can you ...?"), a negation ("don't do it until"), another subject
# ("Atlas is working on", "I'll take over"), a conditional sentence ("If Atlas fails, take
# over HF-9"), a message addressed to someone else ("Atlas: ...", "@atlas ..."), and a
# message that retracts itself ("... Actually no, wait").
_OPT_OUT = re.compile(r"\b(?:don'?t|do\s+not|no\s+need\s+to)\s+track\b|\bno\s+tracking\b"
                      r"|\b(?:do|handle)\s+(?:it|this)\s+(?:here|inline)\b", re.I)
_RETRACTED = re.compile(r"\bactually,?\s+(?:no|don'?t|wait)\b|\bno,?\s+wait\b|\bnever\s*mind\b|\bscratch\s+that\b"
                        r"|\bforget\s+(?:it|that)\b|\bignore\s+(?:that|this)\b|\bcancel\s+that\b|\bhold\s+off\b", re.I)
_ADDRESSED = re.compile(r"\A\s*(?:@([\w.-]{1,40})|<@[A-Z0-9]+>|([A-Z][\w-]{1,30}):\s)")
_NOT_ADDRESSEES = frozenset({"note", "update", "todo", "fyi", "reminder", "re", "ps", "context", "task", "request",
                             "urgent", "edit", "status", "q", "question", "summary", "plan", "next", "goal", "ask"})
_CONDITIONAL = re.compile(r"^\s*(?:(?:ok(?:ay)?|and|but|so|then)\s+)*(?:if|unless|in\s+case|should|once|when|"
                          r"whenever|after|before|until)\b", re.I)
_SENTENCE = re.compile(r"[^.!?\n]+[.!?]*")
_CLAUSE_SPLIT = re.compile(r"[,;:()]|\s[-–—]\s|\s(?:and|then|but|so|also)\s", re.I)
_PREFACE = (r"^\s*(?:(?:ok(?:ay)?|now|then|also|pls|please|right|great|thanks)\s+)*"
            r"(?:(?:hey|hi|yo)\s+(?:@?[\w.-]{1,40}\s+)?)?"
            r"(?:(?:can|could|would|will)\s+you\s+(?:please\s+)?|(?:i(?:\s+(?:need|want|would\s+like)|'d\s+like)"
            r"\s+you\s+to|you\s+should|go\s+ahead\s+and|time\s+to)\s+)?(?:please\s+)?")
_KEY = r"[A-Z]{2,6}-[0-9]{1,7}"
_DO = r"do\s+(?:it|this|that|the\s+(?:rest|work|task|issue|ticket|job)|" + _KEY + r")\b"
_GET_DONE = r"get\s+(?:it|this|" + _KEY + r")\s+(?:done|finished|over\s+the\s+line)\b"
_NOT = r"(?!\s+not\b)"
_TRACK = re.compile(_PREFACE + r"track\s+(?:it|this|that|the\s+work|this\s+work|(?:the\s+)?progress)\b"
                    r"(?!\s+down\b)(?![^.?!\n]{0,30}\bdown\b)", re.I)
_WORK = re.compile(_PREFACE + r"(?:research|investigate|write|draft|build|implement|fix|refactor|migrate|deploy|"
                   r"set\s+up|design|analy[sz]e|audit|benchmark|compare|prepare|document|handle|tackle|finish|"
                   r"complete|take\s+over|work\s+on|pick\s+up|continue|resume|start\s+on|carry\s+on|ship)\b" + _NOT
                   + "|" + _PREFACE + "(?:" + _DO + "|" + _GET_DONE + ")", re.I)
_ISSUE_WORK = re.compile(_PREFACE + r"(?:take\s+over|finish|complete|work\s+on|continue|pick\s+up|carry\s+on|resume)\b"
                         + _NOT + "|" + _PREFACE + "(?:" + _DO + "|" + _GET_DONE + ")", re.I)
# Linear as the tracking destination, not the thing being worked on: "in/on/into Linear" at a
# clause end, "track/log/file/record/put ... in Linear", "create/open/file a Linear issue ...",
# "as a Linear issue/project". "fix the linear issue sync bug", "the Linear project picker",
# "linear.app/ links", "linear regression" and "the linear plugin" are not.
_CLAUSE_END = (r"(?=\s*$|\s*[.,;:!?()\[\]]|\s+[-–—]\s|\s+(?:and|then|so|too|as|please|pls|under|for|with|"
               r"now|first|project|team|board|thanks)\b)")
_LINEAR_REF = re.compile(
    r"\b(?:in|on|into|onto|to)\s+linear\b" + _CLAUSE_END +
    r"|\b(?:track|log|file|record|put|add|capture)\b[^.?!\n]{0,40}\b(?:in|on|into|onto|to)\s+linear\b" +
    r"|\b(?:create|open|file|raise|make|add|log|start)\s+(?:a|an|the)?\s*(?:new\s+)?linear\s+(?:issue|ticket|card|project)s?\b"
    r"|\bas\s+(?:a|an)\s+linear\s+(?:issue|ticket|card|project)\b", re.I)
# Case-sensitive keys keep "utf-8"/"covid-19" out; common non-issue prefixes are excluded.
_ISSUE_KEY = re.compile(r"(?<![A-Za-z0-9_-])(?!(?:SHA|UTF|ISO|RFC|CVE|GPT|COVID|HTTP|TLS|SSL|AES|RSA|PEP|IPV|"
                        r"WCAG|TCP|UDP|USB|ARM|MP)-)(" + _KEY + r")(?![A-Za-z0-9_]|-[0-9])")
_POLITE_QUESTION = re.compile(r"^\s*(?:(?:ok(?:ay)?|now|please)\s+)*(?:(?:hey|hi|yo)\s+(?:@?[\w.-]{1,40}\s+)?)?"
                              r"(?:can|could|would|will)\s+you\b", re.I)


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
    """What the person typed this turn, or None for a machine-authored or unreadable turn."""
    text = _REPLY_POINTER.sub("", _text(message), count=1)
    if _NEW_MESSAGE in text:
        text = text.rsplit(_NEW_MESSAGE, 1)[1]
    elif _HISTORY.search(text):
        return None  # channel history without its marker: the request cannot be told apart
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
        if _CONDITIONAL.match(stripped):
            continue
        if any(pattern.match(clause) for clause in _CLAUSE_SPLIT.split(stripped)):
            found.append(stripped)
    return found


def _addressed_elsewhere(text: str, self_names: frozenset[str]) -> bool:
    found = _ADDRESSED.match(text)
    if not found:
        return False
    name = (found.group(1) or found.group(2) or "").lower()
    return name not in self_names and name not in _NOT_ADDRESSEES


def match(message: Any, self_names: frozenset[str] = frozenset()) -> tuple[str, str] | None:
    """``(reason, issue_key)`` when the message asks this agent for Linear-tracked work, else None.

    ``self_names`` (lower case) are names this agent answers to in a leading ``Name:`` / ``@name``.
    """
    text = request_text(message)
    if not text or _OPT_OUT.search(text) or _RETRACTED.search(text) or _addressed_elsewhere(text, self_names):
        return None
    keys = _ISSUE_KEY.findall(text)
    destination = bool(_LINEAR_REF.search(text))

    def key_in(sentences: list[str]) -> str:
        for sentence in sentences:
            found = _ISSUE_KEY.search(sentence)
            if found:
                return found.group(1)
        return keys[0] if keys else ""

    tracking = _requests(text, _TRACK)
    if tracking and (keys or destination):
        return "track_request", key_in(tracking)
    if destination:
        work = _requests(text, _WORK)
        if work:
            return "linear_work_request", key_in(work)
    issue_work = [s for s in _requests(text, _ISSUE_WORK) if _ISSUE_KEY.search(s)]
    if issue_work:
        return "issue_work_request", key_in(issue_work)
    return None


def coordination(tool_name: str) -> bool:
    return tool_name in COORDINATION_TOOLS or tool_name.startswith(COORDINATION_PREFIXES)


# Core resolves toolsets per platform; Desktop/TUI and local turns use the "cli" key
# (tui_gateway/agent_factory.py), messaging platforms their own name (gateway/run_turn.py).
_PLATFORM_TOOLSET_KEY = {"tui": "cli", "local": "cli", "desktop": "cli"}
REACHABILITY_TTL_SECONDS = 60.0


def linear_reachable(platform: str) -> bool | None:
    """Can a turn on ``platform`` start tracked work with the linear tool?

    True or False when core's per-platform toolset resolution (hermes_cli.tools_config
    ``_get_platform_tools`` over the active profile's config, expanded to tool names by
    model_tools ``_select_tool_names``), the registry entry and its availability check
    (the plugin's coordinator check; a delegated child cannot start work) say so; None when
    that cannot be determined, in which case the block message itself is conditional.
    """
    from . import chat
    try:
        from agent.delegation_context import is_delegated_child_process_context
        if is_delegated_child_process_context():
            return False
    except ImportError:
        pass
    if not chat.coordinator_inventory():
        return False
    try:
        from hermes_cli.config import load_config_readonly
        from hermes_cli.tools_config import _get_platform_tools
        from model_tools import _select_tool_names
        from tools.registry import registry
    except ImportError:
        return None
    key = _PLATFORM_TOOLSET_KEY.get(platform, platform)
    toolsets = sorted(_get_platform_tools(load_config_readonly(), key))
    if "linear" not in _select_tool_names(toolsets, None, True):
        return False
    entry = registry.get_entry("linear")
    if entry is None:
        return False
    check = getattr(entry, "check_fn", None)
    return bool(check()) if callable(check) else True


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
        "If this session has no linear tool, answer with what you have and say that tracked work isn't "
        "available here. linear (status, followup for steering), kanban_* and clarify stay available.")


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
                 reachable: Callable[[str], bool | None] = linear_reachable,
                 self_names: Callable[[], frozenset[str]] = frozenset) -> None:
        self._clock = clock
        self._reachable = reachable
        self._self_names = self_names
        self._reach_cache: dict[str, tuple[float, bool | None]] = {}
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
        platform = str(kw.get("platform") or "").strip().lower()
        if platform in NEVER_ARM_PLATFORMS:
            return
        found = match(kw.get("user_message"), self._names())
        if found is None:
            return
        reason, key = found
        now = self._clock()
        if self._reachable_on(platform, now) is False:
            # Never point the model at a tool this turn cannot reach; say so once for this turn.
            diag.evt("chat.guard", "skipped", "linear_unreachable", issue=key or None, sess=_sess(session))
            return
        with self._lock:
            self._prune(now)
            self._turns[turn_id] = _Turn(session, key, reason, now)
            if session:
                self._latest[session] = turn_id
        diag.evt("chat.guard", "armed", reason, issue=key or None, sess=_sess(session))

    def _names(self) -> frozenset[str]:
        try:
            return frozenset(name.lower() for name in self._self_names() if isinstance(name, str) and name)
        except Exception:
            return frozenset()

    def _reachable_on(self, platform: str, now: float) -> bool | None:
        cached = self._reach_cache.get(platform)
        if cached is not None and now - cached[0] < REACHABILITY_TTL_SECONDS:
            return cached[1]
        try:
            value = self._reachable(platform)
        except Exception:
            value = None  # undetermined: arm; the block message is conditional
        self._reach_cache[platform] = (now, value)
        return value

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


_OFF = frozenset({"false", "0", "off", "no", "disabled"})


def enabled(value: Any) -> bool:
    """``chat_guard`` setting: on unless false, 0 or an off-like string."""
    if value is False or (isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0):
        return False
    return not (isinstance(value, str) and value.strip().lower() in _OFF)


def register(ctx: Any) -> ChatGuard | None:
    """Register the guard's hooks unless ``chat_guard`` is off; native Kanban workers get none."""
    if _worker_process() or not enabled(ctx.get_config("chat_guard", True)):
        return None

    def self_names() -> frozenset[str]:
        names = {ctx.get_config("ingress_profile")}
        try:
            from hermes_cli.profiles import get_active_profile_name
            names.add(get_active_profile_name())
        except Exception:
            pass
        return frozenset(name for name in names if isinstance(name, str) and name and name != "default")

    guard = ChatGuard(self_names=self_names)
    for event in ("pre_llm_call", "pre_tool_call", "post_tool_call", "post_llm_call"):
        ctx.register_hook(event, getattr(guard, event))
    return guard
