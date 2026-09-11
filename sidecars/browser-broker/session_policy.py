"""Identity-bound, short-lived browser handoff sessions.

UUID is a route handle. Cloudflare Access email plus pre-bound principal
is authentication. The model cannot supply email, profile, or agent.
"""
from __future__ import annotations

import hashlib
import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from public_config import canonical_public_url


DEFAULT_TTL = 900
MAX_TTL = 1800
TERMINAL_STATES = frozenset({"expired", "revoked", "ended"})
ACTIVE_STATES = frozenset({"pending", "active"})
# After a failed terminal checkpoint the background reaper waits this long
# before retrying the same session; the 0.25s reaper otherwise hammers the
# checkpoint service several times a second.
# An explicit End retries immediately; status itself stays responsive.
REAPER_RETRY_SECONDS = 15.0


class BrokerError(Exception):
    def __init__(self, status: int, message: str = "") -> None:
        super().__init__(message or str(status))
        self.status = status


@dataclass(frozen=True)
class Invocation:
    profile: str
    platform: str
    user_id: str
    chat_id: str
    thread_id: str
    chat_type: str
    scope_id: str = ""
    # Desktop is a verified provider/subject claim, never a Slack-shaped route.
    browser_control_provider: str = ""
    browser_control_subject: str = ""
    # Resource selection is independent of messaging scope and human identity.
    # Empty retains the deployed /<agent>/<uuid> default-resource ABI.
    browser_workspace_id: str = ""


@dataclass(frozen=True)
class AccessPrincipal:
    principal_id: str
    access_email: str
    routes: tuple[tuple[str, str, str | None], ...]
    agents: tuple[str, ...]
    group_routes: tuple["ChannelThreadGrant", ...] = ()
    # Desktop is a verified provider/subject claim, never a Slack-shaped route.
    desktop_routes: tuple[tuple[str, str], ...] = ()
    # Old policy grants only the existing resource. Fresh resources are never
    # implicitly granted to a trusted principal.
    browser_workspaces: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChannelThreadGrant:
    """One deliberate non-DM admission route for an already-bound principal."""

    agent: str
    platform: str
    scope_id: str
    channel_id: str
    chat_type: str
    require_thread: bool

    def matches(self, invocation: Invocation) -> bool:
        return (
            self.agent == invocation.profile
            and self.platform == invocation.platform
            and self.scope_id == invocation.scope_id
            and self.channel_id == invocation.chat_id
            and self.chat_type == invocation.chat_type
            and self.require_thread
            and bool(invocation.thread_id)
        )


@dataclass(frozen=True)
class Minted:
    session_id: str
    url: str
    expires_at: float


@dataclass(frozen=True)
class Page:
    status: int
    kind: str = "viewer"


@dataclass
class _Session:
    session_id: str
    agent_id: str
    browser_workspace_id: str
    principal_id: str
    access_email: str
    # Retain the admission route, not just a person-shaped identifier. Policy
    # reloads must validate this precise principal/provider/subject/route.
    invocation: Invocation
    state: str
    created_at: float
    expires_at: float
    access_subject_sha256: str | None = None
    sockets: list[Callable[[], None]] = field(default_factory=list)
    # Socket-local RFB parsers are only inspected while action_lock is held.
    # This makes an Observe preflight a boundary check, not a best-effort read
    # racing a splice thread that is appending a fragmented record.
    socket_preflights: dict[int, Callable[[], bool]] = field(default_factory=dict, repr=False)
    expiry_timer: threading.Timer | None = field(default=None, repr=False)
    reaper_retry_at: float = 0.0
    owner: tuple[str, ...] = ()
    terminal_target: str = "ended"
    checkpoint_outcome: str | None = None
    # Only the authenticated route that actually acquired the human-control
    # lock may authorize release of a marker inherited across a broker restart.
    takeover_confirmed: bool = False
    # Observe keeps the authenticated viewer alive but is transport-read-only.
    # It is set only after the lifecycle-owned checkpoint has released the
    # automation fence successfully.
    mode: str = "takeover"
    action_lock: threading.RLock = field(default_factory=threading.RLock, repr=False)


class _ResourceSessionIndex(dict):
    """Tuple keys isolate resources while preserving the legacy default lookup."""

    @staticmethod
    def _key(key):
        return (key, "default") if isinstance(key, str) else key

    def __getitem__(self, key):
        return super().__getitem__(self._key(key))

    def get(self, key, default=None):
        return super().get(self._key(key), default)

    def pop(self, key, *args):
        return super().pop(self._key(key), *args)


class HandoffBroker:
    """One owner per agent; external cleanup never holds the state mutex."""

    def __init__(self, principals, *, clock, ttl_seconds=DEFAULT_TTL,
                 configured_agent=None, on_transition=None, on_terminal=None,
                 on_hold_status=None, on_observe=None, on_policy_revoked=None,
                 terminal_limit=1024, terminal_ttl=MAX_TTL,
                 configured_workspaces=("default",)):
        if type(terminal_limit) is not int or terminal_limit < 1:
            raise ValueError('terminal_limit must be a positive integer')
        import math
        if isinstance(terminal_ttl, bool) or not isinstance(terminal_ttl, (int, float)) or not math.isfinite(terminal_ttl) or terminal_ttl <= 0:
            raise ValueError('terminal_ttl must be positive and finite')
        self.principals = tuple(principals)
        self._clock = clock
        self._ttl = min(max(int(ttl_seconds), 1), MAX_TTL)
        self.configured_agent = configured_agent
        self.configured_workspaces = tuple(configured_workspaces)
        if not self.configured_workspaces or any(not isinstance(item, str) or not item for item in self.configured_workspaces):
            raise ValueError('configured_workspaces must be nonempty resource names')
        self._on_transition = on_transition
        self._on_terminal = on_terminal
        self._on_hold_status = on_hold_status
        self._on_observe = on_observe
        self._on_policy_revoked = on_policy_revoked
        self._sessions = {}
        self._active_by_agent = _ResourceSessionIndex()
        self._last_by_owner = {}
        self._terminal = OrderedDict()
        self._terminal_limit = terminal_limit
        self._terminal_ttl = terminal_ttl
        self._lifecycle_lock = threading.RLock()

    def _prune_terminal_locked(self):
        cutoff = self._clock() - self._terminal_ttl
        while self._terminal:
            sid, ended_at = next(iter(self._terminal.items()))
            if len(self._terminal) <= self._terminal_limit and ended_at > cutoff:
                break
            self._terminal.popitem(last=False)
            sess = self._sessions.get(sid)
            if sess is not None and sess.state in TERMINAL_STATES:
                self._sessions.pop(sid)
                owner_key = (sess.owner, sess.browser_workspace_id)
                if self._last_by_owner.get(owner_key) == sid:
                    self._last_by_owner.pop(owner_key)

    @staticmethod
    def _owner(invocation):
        return (invocation.profile, invocation.platform, invocation.scope_id, invocation.user_id,
                invocation.chat_id, invocation.thread_id, invocation.chat_type)

    def _require_configured_agent(self, agent):
        if self.configured_agent and agent != self.configured_agent:
            raise BrokerError(403, 'agent denied')

    def _authorized_principal(self, invocation):
        # A renderer can revoke a principal without restarting the broker.  Take
        # a consistent snapshot while the caller replaces that projection.
        with self._lifecycle_lock:
            principals = self.principals
        return self._authorized_principal_from(principals, invocation)

    def _authorized_principal_from(self, principals, invocation):
        self._require_configured_agent(invocation.profile)
        for principal in principals:
            if invocation.platform == "desktop":
                if (invocation.browser_control_provider, invocation.browser_control_subject) in principal.desktop_routes:
                    if invocation.profile not in principal.agents:
                        raise BrokerError(403, 'agent denied')
                    workspace = invocation.browser_workspace_id or "default"
                    if workspace not in self.configured_workspaces or workspace not in (principal.browser_workspaces or ("default",)):
                        raise BrokerError(403, 'browser workspace denied')
                    return principal
                continue
            if (invocation.platform, invocation.user_id, invocation.scope_id or None) in principal.routes:
                if invocation.profile not in principal.agents:
                    raise BrokerError(403, 'agent denied')
                workspace = invocation.browser_workspace_id or "default"
                if workspace not in self.configured_workspaces or workspace not in (principal.browser_workspaces or ("default",)):
                    raise BrokerError(403, 'browser workspace denied')
                if invocation.chat_type in {'dm', 'private'}:
                    return principal
                if any(grant.matches(invocation) for grant in principal.group_routes):
                    return principal
                raise BrokerError(403, 'route denied')
        raise BrokerError(403, 'unknown principal')

    def replace_principals(self, principals):
        """Replace policy and terminally revoke sessions it no longer admits.

        Policy publication first marks every affected session non-forwardable
        under the lifecycle mutex. Socket cleanup then takes each action lock
        without holding that mutex, preserving the established action->lifecycle
        ordering and avoiding a splice/reload deadlock. This call returns only
        after closers have run, so no input can be forwarded after reload.
        Revocation deliberately does not invoke terminal checkpoint/unlock: a
        withdrawn grant is never authority to release the automation fence.
        """
        replacement = tuple(principals)
        with self._lifecycle_lock:
            self.principals = replacement
            affected = []
            for sess in self._sessions.values():
                if sess.state not in ACTIVE_STATES and sess.state != 'recovery_required':
                    continue
                try:
                    principal = self._authorized_principal_from(replacement, sess.invocation)
                    # A principal id alone is not a grant: bind all of the
                    # policy identity that admitted this handoff as well as
                    # the invocation's provider/subject/route above.
                    allowed = (
                        principal.principal_id == sess.principal_id
                        and principal.access_email.lower() == sess.access_email
                    )
                except BrokerError:
                    allowed = False
                if not allowed:
                    # This blocks all new forwarding before we wait to close a
                    # possibly blocked socket action.
                    sess.state = 'revoking'
                    if sess.expiry_timer is not None:
                        sess.expiry_timer.cancel()
                        sess.expiry_timer = None
                    affected.append(sess)
        for sess in affected:
            self._revoke_after_policy_change(sess)

    def _revoke_after_policy_change(self, sess):
        """Close a policy-revoked viewer without releasing its takeover fence."""
        with sess.action_lock:
            with self._lifecycle_lock:
                closers = tuple(sess.sockets)
            # Keep any failed closer recorded; it is not a quiescence receipt.
            # State remains non-forwardable in either case and no cleanup path
            # may checkpoint/unlock this revoked grant.
            for closer in closers:
                try:
                    closer()
                except Exception:
                    continue
                with self._lifecycle_lock:
                    if closer in sess.sockets:
                        sess.sockets.remove(closer)
                        sess.socket_preflights.pop(id(closer), None)
            with self._lifecycle_lock:
                sess.state = 'revoked'
                sess.checkpoint_outcome = 'policy_revoked_fence_retained'
                if self._active_by_agent.get(sess.agent_id) == sess.session_id:
                    self._active_by_agent.pop(sess.agent_id)
                self._terminal[sess.session_id] = self._clock()
                self._prune_terminal_locked()
            # This only records that a future release needs a fresh takeover;
            # it must never checkpoint or unlock on behalf of the revoked grant.
            if self._on_policy_revoked:
                self._on_policy_revoked(sess.agent_id)

    def mint(self, invocation):
        principal = self._authorized_principal(invocation)
        owner = self._owner(invocation)
        with self._lifecycle_lock:
            self._prune_terminal_locked()
            workspace = invocation.browser_workspace_id or "default"
            key = (invocation.profile, workspace)
            old = self._sessions.get(self._active_by_agent.get(key))
            if old is not None:
                if old.owner == owner and old.state in ACTIVE_STATES and self._clock() < old.expires_at:
                    return self._minted(old)
                raise BrokerError(409, 'browser handoff already owned; end or recover it first')
            now = self._clock()
            sid = str(uuid.uuid4())
            sess = _Session(sid, invocation.profile, workspace, principal.principal_id,
                            principal.access_email.lower(), invocation, 'pending', now, now + self._ttl,
                            owner=owner)
            self._sessions[sid] = sess
            self._active_by_agent[key] = sid
            self._last_by_owner[(owner, workspace)] = sid
            return self._minted(sess)

    @staticmethod
    def _minted(sess):
        path = f'{sess.agent_id}/{sess.session_id}' if sess.browser_workspace_id == 'default' else f'{sess.agent_id}/{sess.browser_workspace_id}/{sess.session_id}'
        return Minted(sess.session_id, f'{canonical_public_url()}/{path}', sess.expires_at)

    def _current(self, invocation, *, include_ended=False):
        principal = self._authorized_principal(invocation)
        owner = self._owner(invocation)
        with self._lifecycle_lock:
            workspace = invocation.browser_workspace_id or "default"
            sid = self._active_by_agent.get((invocation.profile, workspace))
            if sid is None and include_ended:
                sid = self._last_by_owner.get((owner, workspace))
            sess = self._sessions.get(sid)
            if sess is None:
                raise BrokerError(404, 'no active handoff')
            if sess.principal_id != principal.principal_id or sess.owner not in ((), owner):
                raise BrokerError(403, 'wrong handoff owner')
            return sess

    def status(self, invocation):
        principal = self._authorized_principal(invocation)
        owner = self._owner(invocation)
        with self._lifecycle_lock:
            sess = self._sessions.get(self._active_by_agent.get((invocation.profile, invocation.browser_workspace_id or "default")))
            if sess is not None and (sess.principal_id != principal.principal_id or sess.owner not in ((), owner)):
                raise BrokerError(403, 'wrong handoff owner')
        if sess is None:
            held = self._held_status(invocation.profile)
            if held is not None:
                return held
            raise BrokerError(404, 'no active handoff')
        self._expire_session(sess.session_id, background=True)
        with self._lifecycle_lock:
            if sess.state in TERMINAL_STATES:
                held = self._held_status(invocation.profile)
                if held is not None:
                    return held
                raise BrokerError(404, 'no active handoff')
            return self._session_status(sess)

    def end_current(self, invocation, expected_session_id=None):
        if expected_session_id is None:
            sess = self._current(invocation, include_ended=True)
        else:
            principal = self._authorized_principal(invocation)
            with self._lifecycle_lock:
                sess = self._sessions.get(expected_session_id)
                if sess is None:
                    raise BrokerError(404, 'unknown handoff')
                if sess.principal_id != principal.principal_id or sess.owner != self._owner(invocation):
                    raise BrokerError(403, 'wrong handoff owner')
        self._terminate(sess, 'ended', explicit_end=True)
        return sess.session_id

    def status_for_agent(self, agent):
        self._require_configured_agent(agent)
        with self._lifecycle_lock:
            sess = self._sessions.get(self._active_by_agent.get(agent))
            if sess is None:
                held = self._held_status(agent)
                if held is not None:
                    return held
                raise BrokerError(404, 'no active handoff')
        self._expire_session(sess.session_id, background=True)
        with self._lifecycle_lock:
            if sess.state in TERMINAL_STATES:
                held = self._held_status(agent)
                if held is not None:
                    return held
                raise BrokerError(404, 'no active handoff')
            return self._session_status(sess)

    def _held_status(self, agent):
        status = self._on_hold_status(agent) if self._on_hold_status else None
        return dict(status) if isinstance(status, dict) else None

    def end_for_agent(self, agent):
        self._require_configured_agent(agent)
        with self._lifecycle_lock:
            sess = self._sessions.get(self._active_by_agent.get(agent))
            if sess is None:
                raise BrokerError(404, 'no active handoff')
        self._terminate(sess, 'ended', explicit_end=True)
        return self._session_status(sess)

    def _public_session(self, sid, email):
        if not (email or '').strip():
            raise BrokerError(401, 'missing access')
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
            if sess is None:
                raise BrokerError(401, 'unknown')
            if email.strip().lower() != sess.access_email:
                raise BrokerError(403, 'wrong principal')
        return sess

    def _human_may_connect(self, sess):
        # recovery_required still holds the takeover lock: the human must be
        # able to come back in, fix the browser, and End again.
        if sess.state == 'recovery_required':
            return True
        return sess.state in ACTIVE_STATES and self._clock() < sess.expires_at

    def public_page(self, sid, *, method, access_email):
        sess = self._public_session(sid, access_email)
        self._expire_session(sid, background=True)
        with self._lifecycle_lock:
            if not self._human_may_connect(sess):
                raise BrokerError(410, sess.state)
            if method.upper() == 'HEAD':
                return Page(200, 'head')
            if sess.state == 'pending':
                sess.state = 'active'
            elif sess.state == 'recovery_required':
                # A returning human gets one fresh TTL window to fix and End.
                sess.expires_at = max(sess.expires_at, self._clock() + self._ttl)
            sess.access_subject_sha256 = hashlib.sha256(sess.access_email.encode()).hexdigest()
            return Page(200, 'viewer')

    def extend(self, sid, *, access_email):
        sess = self._public_session(sid, access_email)
        self._expire_session(sid, background=True)
        with self._lifecycle_lock:
            if sess.state not in ACTIVE_STATES or self._clock() >= sess.expires_at:
                raise BrokerError(410, sess.state)
            sess.expires_at = min(sess.created_at + MAX_TTL, max(sess.expires_at, self._clock() + self._ttl))
            if sess.expiry_timer is not None:
                self._schedule_expiry_locked(sess)
            return sess.expires_at

    def end(self, sid, *, access_email):
        self._terminate(self._public_session(sid, access_email), 'ended', explicit_end=True)

    def confirm_takeover(self, sid, *, access_email):
        """Record a successful authenticated human-control acquisition."""
        sess = self._public_session(sid, access_email)
        with self._lifecycle_lock:
            if not self._human_may_connect(sess):
                raise BrokerError(410, sess.state)
            sess.takeover_confirmed = True

    def acquire_takeover(self, sid, *, access_email, acquire):
        """Acquire the fence before allowing a viewer to send input again."""
        sess = self._public_session(sid, access_email)
        with sess.action_lock:
            if not self.is_active(sid):
                raise BrokerError(410, 'session ended')
            acquire()
            with self._lifecycle_lock:
                sess.takeover_confirmed = True
                sess.mode = 'takeover'

    def acquire_takeover_if_current(self, sid, *, access_email, acquire):
        """Acquire only when this connection began in takeover mode.

        Reconnect is not a mode transition: an Observe reconnect must not
        recreate the automation fence after Observe released it.
        """
        sess = self._public_session(sid, access_email)
        with sess.action_lock:
            if not self.is_active(sid):
                raise BrokerError(410, 'session ended')
            with self._lifecycle_lock:
                takeover = sess.mode == 'takeover'
            if takeover:
                acquire()
                with self._lifecycle_lock:
                    sess.takeover_confirmed = True
            return takeover

    def release_to_observe(self, sid, *, access_email):
        """Checkpoint and release the automation fence while retaining a viewer.

        A failed checkpoint leaves both the takeover mode and lock untouched.
        The callback is serialized with accepted viewer actions, so its success
        is the only transition that can make automation available again.
        """
        sess = self._public_session(sid, access_email)
        with sess.action_lock:
            if not self.is_active(sid):
                raise BrokerError(410, 'session ended')
            if sess.mode == 'observe':
                return sess.checkpoint_outcome
            if self._on_observe is None:
                raise BrokerError(503, 'observe release lifecycle is unavailable')
            # Do this before the checkpoint callback (which releases the
            # physical automation fence). A false or failed parser check means
            # a live takeover record is incomplete; retain takeover unchanged
            # so the human can finish the record and explicitly retry.
            for preflight in tuple(sess.socket_preflights.values()):
                try:
                    safe = preflight()
                except Exception:
                    safe = False
                if not safe:
                    raise BrokerError(409, 'viewer RFB record is incomplete; takeover remains locked')
            try:
                try:
                    outcome = self._on_observe(sess.agent_id, sess.browser_workspace_id, sess.session_id)
                except TypeError:
                    # Legacy callback ABI is retained for default-only brokers.
                    outcome = self._on_observe(sess.agent_id, sess.session_id)
            except BrokerError:
                raise
            except Exception as exc:
                raise BrokerError(502, 'checkpoint failed; takeover remains locked') from exc
            with self._lifecycle_lock:
                sess.mode = 'observe'
                sess.checkpoint_outcome = outcome if isinstance(outcome, str) else None
            return sess.checkpoint_outcome

    def viewer_may_send_input(self, sid):
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
            return bool(sess and self._human_may_connect(sess) and sess.mode == 'takeover')

    def attach_socket(self, sid, closer, *, transition_safe=None):
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
            if sess is None:
                raise BrokerError(401, 'unknown')
        with sess.action_lock:
            with self._lifecycle_lock:
                allowed = self._human_may_connect(sess)
                if allowed:
                    sess.sockets.append(closer)
                    if transition_safe is not None:
                        sess.socket_preflights[id(closer)] = transition_safe
                    if sess.expiry_timer is None and sess.state in ACTIVE_STATES:
                        self._schedule_expiry_locked(sess)
        if not allowed:
            closer()
            raise BrokerError(410, 'viewer revoked')

    def detach_socket(self, sid, closer):
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
        if sess is None:
            return
        with sess.action_lock:
            with self._lifecycle_lock:
                # Ending owns the close/drain receipt until cleanup succeeds.
                if sess.state != 'ending' and closer in sess.sockets:
                    sess.sockets.remove(closer)
                    sess.socket_preflights.pop(id(closer), None)

    def is_active(self, sid):
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
            return bool(sess and self._human_may_connect(sess))

    def run_while_active(self, sid, action):
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
            if sess is None:
                raise BrokerError(404, 'unknown session')
        with sess.action_lock:
            if not self.is_active(sid):
                raise BrokerError(410, 'session ended')
            action()
            if not self.is_active(sid):
                raise BrokerError(410, 'session ended')

    def forward_viewer_rfb(self, sid, *, observe, takeover):
        """Serialize the current mode decision with its forwarding action."""
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
            if sess is None:
                raise BrokerError(404, 'unknown session')
        with sess.action_lock:
            with self._lifecycle_lock:
                if not self._human_may_connect(sess):
                    raise BrokerError(410, 'session ended')
                action = takeover if sess.mode == 'takeover' else observe
            action()

    def send_viewer_input(self, sid, action):
        """Authorize and forward one viewer payload as one fenced operation.

        release_to_observe holds this same lock while checkpointing and changing
        mode. Keeping the mode check here prevents a payload buffered before a
        successful release from being sent after automation is unlocked.
        """
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
            if sess is None:
                raise BrokerError(404, 'unknown session')
        with sess.action_lock:
            with self._lifecycle_lock:
                allowed = self._human_may_connect(sess) and sess.mode == 'takeover'
            if not allowed:
                raise BrokerError(410, 'viewer is observe-only')
            action()
            with self._lifecycle_lock:
                if not self._human_may_connect(sess):
                    raise BrokerError(410, 'session ended')

    def _schedule_expiry_locked(self, sess):
        if sess.expiry_timer is not None:
            sess.expiry_timer.cancel()
        timer = threading.Timer(max(0, sess.expires_at - self._clock()), self._expire_session, (sess.session_id,))
        timer.daemon = True
        sess.expiry_timer = timer
        timer.start()

    def _begin_terminal(self, sess, target, *, explicit_end=False):
        with self._lifecycle_lock:
            if sess.state in TERMINAL_STATES:
                return False
            if sess.state == 'ending':
                raise BrokerError(409, 'handoff ending')
            if sess.state == 'recovery_required':
                # A failed End remains eligible only for another explicit End.
                # A reaper retry is an expiry, never evidence of human intent.
                target = 'ended' if explicit_end else 'expired'
            sess.state = 'ending'
            sess.terminal_target = target
            if sess.expiry_timer is not None:
                sess.expiry_timer.cancel()
                sess.expiry_timer = None
            return True

    def _finish_terminal(self, sess):
        try:
            # Ending already rejects new actions. Interrupt blocked socket I/O
            # before waiting for actions that need that shutdown to unwind.
            with self._lifecycle_lock:
                closers = tuple(sess.sockets)
            for closer in closers:
                closer()  # Failed close is not evidence of quiescence.
                with self._lifecycle_lock:
                    sess.sockets.remove(closer)
                    sess.socket_preflights.pop(id(closer), None)
            # Still drain admitted actions before checkpoint or ownership release.
            with sess.action_lock:
                if self._on_transition:
                    try:
                        self._on_transition(sess.agent_id, sess.browser_workspace_id, sess.session_id, sess.terminal_target)
                    except TypeError:
                        self._on_transition(sess.agent_id, sess.session_id, sess.terminal_target)
                if self._on_terminal:
                    try:
                        outcome = self._on_terminal(sess.agent_id, sess.browser_workspace_id, sess.terminal_target)
                    except TypeError:
                        outcome = self._on_terminal(sess.agent_id, sess.terminal_target)
                else:
                    outcome = None
            with self._lifecycle_lock:
                sess.checkpoint_outcome = outcome if isinstance(outcome, str) else None
                sess.state = sess.terminal_target
                if self._active_by_agent.get((sess.agent_id, sess.browser_workspace_id)) == sess.session_id:
                    self._active_by_agent.pop((sess.agent_id, sess.browser_workspace_id))
                self._terminal[sess.session_id] = self._clock()
                self._prune_terminal_locked()
        except Exception as exc:
            with self._lifecycle_lock:
                sess.state = 'recovery_required'
                sess.checkpoint_outcome = 'save_failed'
                sess.reaper_retry_at = self._clock() + REAPER_RETRY_SECONDS
            raise BrokerError(502, 'viewer revoked; browser recovery required') from exc

    def _terminate(self, sess, target, *, explicit_end=False):
        if self._begin_terminal(sess, target, explicit_end=explicit_end):
            self._finish_terminal(sess)

    def _expire_session(self, sid, background=False):
        with self._lifecycle_lock:
            sess = self._sessions.get(sid)
            if sess is None or sess.state in TERMINAL_STATES or sess.state == 'ending':
                return
            if sess.state == 'recovery_required':
                # A connected human owns the retry (End) until the TTL; the
                # reaper backs off but each return is bounded by one TTL window.
                if self._clock() < sess.reaper_retry_at:
                    return
                if sess.sockets and self._clock() < sess.expires_at:
                    return
            elif self._clock() < sess.expires_at:
                return
            if not self._begin_terminal(sess, 'expired'):
                return
        def finish():
            try:
                self._finish_terminal(sess)
            except BrokerError:
                pass  # Recovery state and bounded retry remain visible.
        if background:
            threading.Thread(target=finish, daemon=True).start()
        else:
            finish()

    def expire_sessions(self):
        with self._lifecycle_lock:
            self._prune_terminal_locked()
            ids = tuple(self._active_by_agent.values())
        for sid in ids:
            self._expire_session(sid)

    def debug(self, sid):
        self._expire_session(sid, background=True)
        with self._lifecycle_lock:
            return self._session_status(self._sessions[sid])

    @staticmethod
    def _session_status(sess):
        return {'session_id': sess.session_id, 'state': sess.state,
                'access_subject_sha256': sess.access_subject_sha256,
                'agent_id': sess.agent_id, 'browser_workspace_id': sess.browser_workspace_id, 'principal_id': sess.principal_id,
                'expires_at': sess.expires_at, 'max_expires_at': sess.created_at + MAX_TTL,
                'url': HandoffBroker._minted(sess).url,
                'automation_blocked': sess.state not in TERMINAL_STATES and sess.mode != 'observe',
                'checkpoint_outcome': sess.checkpoint_outcome,
                'takeover_confirmed': sess.takeover_confirmed,
                'mode': sess.mode}
