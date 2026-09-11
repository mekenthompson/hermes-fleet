#!/usr/bin/env python3
"""JWT-gated viewer. Public 9131. Admin mint 9134. VNC only on /s/<uuid>.

Live VNC upstream, REST lock, and End checkpoint follow the redeemed
session agent. Do not share another agent's rest-proxy.
"""
from __future__ import annotations

import json
import fcntl
import os
import re
import select
import socket
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys

import jwt
from jwt import PyJWKClient

from session_policy import (
    AccessPrincipal,
    BrokerError,
    ChannelThreadGrant,
    HandoffBroker,
    Invocation,
)
from ux import shell_page, status_page
from rollout_contract import RolloutContractError, RolloutVersions, preflight, verify_running_plugin_protocol

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rfb_filter import ObserveRfb, ProtocolError, WebSocketClientFrames, client_binary_frame
from origin_routing import (  # noqa: E402
    cap_may_mint,
    configured_bind,
    route_for_agent,
    take_automation_lock,
)
from public_config import canonical_public_url, PublicUrlError

STATE = Path(os.environ.get("ACCESS_STATE", "/state/browser-access-state.json"))
PORT = int(os.environ.get("BROKER_PORT", "9131"))
ADMIN_PORT = int(os.environ.get("ADMIN_PORT", "9134"))
AGENT_RE = re.compile(r"[a-z][a-z0-9-]{0,63}\Z")
DESKTOP_TOKEN_RE = re.compile(r"[^\s\x00-\x1f*]{1,256}\Z")
SYNTHETIC_DESKTOP_ROUTES = frozenset({
    ("server-internal", "server-internal"),
})
UUID_RE = re.compile(
    r"^/([a-z][a-z0-9-]{0,63})/([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(/(end|observe|takeover|extend))?$"
)
WORKSPACE_UUID_RE = re.compile(
    r"^/([a-z][a-z0-9-]{0,63})/([a-z][a-z0-9-]{0,63})/([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(/(end|observe|takeover|extend))?$"
)
SCOPED_RE = re.compile(
    r"^/([a-z][a-z0-9-]{0,63})/([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(?:/(.*))?$"
)
WORKSPACE_SCOPED_RE = re.compile(
    r"^/([a-z][a-z0-9-]{0,63})/([a-z][a-z0-9-]{0,63})/([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(?:/(.*))?$"
)
FORBIDDEN_MINT = frozenset(
    {"email", "profile", "agent", "ttl", "callback_url", "access_email"}
)
def load_state() -> dict:
    return json.loads(STATE.read_text())


def load_principals(configured_agent: str) -> tuple[AccessPrincipal, ...]:
    raw_path = (os.environ.get("HANDOFF_PRINCIPALS_FILE") or "").strip()
    if not raw_path:
        raise SystemExit("missing HANDOFF_PRINCIPALS_FILE")
    try:
        rows = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit("invalid HANDOFF_PRINCIPALS_FILE") from exc
    if not isinstance(rows, list) or not rows:
        raise SystemExit("principals must be a nonempty list")
    principals: list[AccessPrincipal] = []
    seen_ids: set[str] = set()
    seen_routes: set[tuple[str, str, str | None]] = set()
    seen_desktop_mappings: set[tuple[str, str]] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise SystemExit("invalid principal")
        pid = str(row.get("principal_id") or "").strip()
        email = str(row.get("access_email") or "").strip().lower()
        routes = row.get("routes")
        agents = row.get("agents")
        workspaces = row.get("browser_workspaces", ["default"])
        if not pid or not email or pid in seen_ids or not isinstance(routes, list) or agents != [configured_agent]:
            raise SystemExit("principal must have explicit one-agent identity")
        if not isinstance(workspaces, list) or not workspaces or any(not isinstance(item, str) or not AGENT_RE.fullmatch(item) for item in workspaces) or len(workspaces) != len(set(workspaces)):
            raise SystemExit("principal browser_workspaces must be an explicit nonempty resource list")
        parsed_routes: list[tuple[str, str, str | None]] = []
        for route in routes:
            if not isinstance(route, (list, tuple)) or len(route) != 3:
                raise SystemExit("invalid principal route")
            platform, user_id, scope_id = route
            if not isinstance(platform, str) or not isinstance(user_id, str):
                raise SystemExit("invalid principal route")
            platform, user_id = platform.strip(), user_id.strip()
            if platform == "slack":
                if not isinstance(scope_id, str) or not scope_id.strip():
                    raise SystemExit("Slack principal route requires team scope")
                item = (platform, user_id, scope_id.strip())
            elif scope_id is None:
                item = (platform, user_id, None)
            else:
                raise SystemExit("non-Slack principal route must not declare scope")
            if not platform or not user_id or item in seen_routes:
                raise SystemExit("duplicate or invalid principal route")
            seen_routes.add(item)
            parsed_routes.append(item)
        if not parsed_routes:
            raise SystemExit("principal requires explicit routes")
        raw_desktop_routes = row.get("desktop_routes", [])
        if not isinstance(raw_desktop_routes, list):
            raise SystemExit("desktop_routes must be a list")
        parsed_desktop_routes: list[tuple[str, str]] = []
        seen_desktop_routes: set[tuple[str, str]] = set()
        for route in raw_desktop_routes:
            if not isinstance(route, (list, tuple)) or len(route) != 2:
                raise SystemExit("invalid desktop route")
            provider, subject = route
            if (
                not isinstance(provider, str)
                or not isinstance(subject, str)
                or not DESKTOP_TOKEN_RE.fullmatch(provider)
                or not DESKTOP_TOKEN_RE.fullmatch(subject)
                or (provider, subject) in SYNTHETIC_DESKTOP_ROUTES
                or (provider, subject) in seen_desktop_routes
                or (provider, subject) in seen_desktop_mappings
            ):
                raise SystemExit("duplicate or invalid desktop route")
            seen_desktop_routes.add((provider, subject))
            seen_desktop_mappings.add((provider, subject))
            parsed_desktop_routes.append((provider, subject))
        raw_group_routes = row.get("group_routes")
        if raw_group_routes is None and "group_routes" in row:
            raise SystemExit("group_routes must be a nonempty list when configured")
        if raw_group_routes is not None and (not isinstance(raw_group_routes, list) or not raw_group_routes):
            raise SystemExit("group_routes must be a nonempty list when configured")
        parsed_group_routes: list[ChannelThreadGrant] = []
        seen_group_routes: set[tuple[str, str, str, str, str]] = set()
        for route in raw_group_routes or ():
            required = {"agent", "platform", "scope_id", "channel_id", "chat_type", "require_thread"}
            if not isinstance(route, dict) or set(route) != required:
                raise SystemExit("invalid group route")
            agent = route["agent"]
            platform = route["platform"]
            scope_id = route["scope_id"]
            channel_id = route["channel_id"]
            chat_type = route["chat_type"]
            require_thread = route["require_thread"]
            if (
                not all(isinstance(value, str) and value.strip() for value in (agent, platform, scope_id, channel_id, chat_type))
                or agent != configured_agent
                or platform != "slack"
                or chat_type in {"dm", "private"}
                or require_thread is not True
            ):
                raise SystemExit("invalid group route")
            item = (agent, platform, scope_id, channel_id, chat_type)
            if item in seen_group_routes:
                raise SystemExit("duplicate group route")
            seen_group_routes.add(item)
            parsed_group_routes.append(ChannelThreadGrant(*item, require_thread=True))
        seen_ids.add(pid)
        principals.append(AccessPrincipal(
            pid, email, tuple(parsed_routes), (configured_agent,), tuple(parsed_group_routes),
            desktop_routes=tuple(parsed_desktop_routes), browser_workspaces=tuple(workspaces),
        ))
    return tuple(principals)


def load_capabilities(configured_agent: str) -> dict[str, str]:
    cap_path = (os.environ.get("HANDOFF_CAPABILITY_FILE") or "").strip()
    cap_agent = (os.environ.get("HANDOFF_CAPABILITY_AGENT") or "").strip()
    if not cap_path or cap_agent != configured_agent:
        raise SystemExit("capability must be explicitly bound to HANDOFF_AGENT")
    try:
        token = Path(cap_path).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SystemExit("missing capability file") from exc
    if not token or any(c.isspace() for c in token):
        raise SystemExit("invalid capability")
    return {token: configured_agent}


def split_host(value: str) -> tuple[str, int]:
    host, port = value.rsplit(":", 1)
    return host, int(port)


# Test injection only; empty in production and never defaults.
UPSTREAMS: dict[str, str] = {}
LOCK_FILES: dict[str, Path] = {}
CHECKPOINT_URLS: dict[str, str] = {}


def _route_key(agent: str, workspace: str = "default") -> str:
    return agent if workspace == "default" else f"{agent}/{workspace}"


def route_for(agent: str, workspace: str = "default") -> tuple[str, Path, str]:
    """Resolve an explicit physical resource; fresh resources never fall back."""
    key = _route_key(agent, workspace)
    if key in UPSTREAMS and key in LOCK_FILES and key in CHECKPOINT_URLS:
        return UPSTREAMS[key], LOCK_FILES[key], CHECKPOINT_URLS[key]
    if workspace != "default":
        raise BrokerError(503, "browser workspace route unavailable")
    if agent in UPSTREAMS and agent in LOCK_FILES and agent in CHECKPOINT_URLS:
        return UPSTREAMS[agent], LOCK_FILES[agent], CHECKPOINT_URLS[agent]
    try:
        return route_for_agent(agent)
    except ValueError as exc:
        raise BrokerError(503, "agent route unavailable") from exc


def lock_for(agent: str, workspace: str = "default") -> Path:
    return route_for(agent, workspace)[1]


# The checkpoint sidecar answers 409 only after the backend reported that no
# live session exists for the agent (backend 404 behind authenticated REST).
CHECKPOINT_NO_LIVE_SESSION = 409


def checkpoint_before_end(agent: str, workspace: str = "default") -> str:
    """Return the persistence outcome; unconfirmed failures retain ownership."""
    _, lock, url = route_for(agent, workspace)
    if not lock.exists():
        return 'not_needed'
    try:
        request = urllib.request.Request(url, method="POST", data=b"")
        with urllib.request.urlopen(request, timeout=15) as response:
            if not 200 <= response.status < 300:
                raise OSError("checkpoint failed")
    except urllib.error.HTTPError as exc:
        if exc.code == CHECKPOINT_NO_LIVE_SESSION:
            # Corroborated absence permits release, but is not a save receipt.
            print(f"origin checkpoint {agent} no live session; releasing takeover lock", flush=True)
            return 'no_live_session'
        raise BrokerError(502, "checkpoint failed; takeover remains locked") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise BrokerError(502, "checkpoint failed; takeover remains locked") from exc
    return 'saved'


def checkpoint_then_release(agent: str, workspace: str = "default", _state: str = "ended") -> str:
    """Checkpoint then unlock while the broker still owns the lifecycle lock.

    The two-argument legacy ABI was ``(agent, state)``; retain it only for
    terminal state tokens while fresh workspace IDs use the three-argument ABI.
    """
    if workspace in {"ended", "expired", "observe"} and _state == "ended":
        _state, workspace = workspace, "default"
    outcome = checkpoint_before_end(agent) if workspace == "default" else checkpoint_before_end(agent, workspace)
    (lock_for(agent) if workspace == "default" else lock_for(agent, workspace)).unlink(missing_ok=True)
    return outcome


class InheritedLockLifecycle:
    """Keep an ambiguous or revoked marker fenced until a fresh human End.

    The broker reports each terminal transition before the checkpoint callback.
    This records eligibility from the authenticated route's successful lock
    acquisition; idle expiry and a reaper retry cannot manufacture it.
    """

    def __init__(self, agent: str, *, inherited: bool, workspace: str = "default") -> None:
        self.agent = agent
        self.workspace = workspace
        self.inherited = inherited
        self._eligible_session: str | None = None
        self._lock = threading.RLock()

    def transition(self, agent: str, session_id: str, state: str, takeover_confirmed: bool) -> None:
        with self._lock:
            if agent == self.agent and state == 'ended' and takeover_confirmed:
                self._eligible_session = session_id

    def retain_after_policy_revocation(self, agent: str) -> None:
        """Require a newly authenticated takeover before any later release.

        Policy revocation cannot donate the old session's prior takeover to a
        replacement session. This records the fence only; it neither checkpoints
        nor unlinks while the revoked grant is in scope.
        """
        with self._lock:
            if agent == self.agent:
                self.inherited = True
                self._eligible_session = None

    def finish(self, agent: str, state: str) -> str:
        with self._lock:
            if self.inherited:
                if state != 'ended' or self._eligible_session is None:
                    return 'inherited_lock_retained'
                self._eligible_session = None
            outcome = checkpoint_then_release(agent, self.workspace, state)
            self.inherited = False
            return outcome

    def release_to_observe(self, agent: str, session_id: str, takeover_confirmed: bool) -> str:
        """Release only a freshly acquired inherited lock after checkpointing."""
        with self._lock:
            if self.inherited and (agent != self.agent or not takeover_confirmed):
                raise BrokerError(409, 'inherited takeover lock requires fresh takeover before release')
            outcome = checkpoint_then_release(agent, self.workspace, 'observe')
            self.inherited = False
            return outcome

    def status(self, agent: str) -> dict | None:
        with self._lock:
            if agent != self.agent or not self.inherited:
                return None
            return {"state": "recovery_required", "automation_blocked": True,
                    "checkpoint_outcome": "inherited_lock_retained"}


def load_workspace_routes(configured_agent: str) -> tuple[str, ...]:
    """Load reviewed physical resources; absent config is the legacy default."""
    path = (os.environ.get("HANDOFF_RESOURCES_FILE") or "").strip()
    if not path:
        return ("default",)
    try:
        rows = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit("invalid HANDOFF_RESOURCES_FILE") from exc
    if not isinstance(rows, list) or not rows:
        raise SystemExit("resources must be a nonempty list")
    names: list[str] = []
    upstreams: set[str] = set()
    locks: set[Path] = set()
    checkpoints: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"id", "novnc_upstream", "lock_file", "checkpoint_url"}:
            raise SystemExit("invalid browser resource")
        workspace = row["id"]
        if not isinstance(workspace, str) or not AGENT_RE.fullmatch(workspace) or workspace in names:
            raise SystemExit("invalid browser resource id")
        if not all(isinstance(row[key], str) and row[key] for key in ("novnc_upstream", "lock_file", "checkpoint_url")):
            raise SystemExit("browser resource requires explicit physical routes")
        upstream = row["novnc_upstream"]
        lock = Path(row["lock_file"])
        checkpoint = row["checkpoint_url"]
        try:
            host, port = split_host(upstream)
            parsed_checkpoint = urlsplit(checkpoint)
        except (TypeError, ValueError):
            raise SystemExit("invalid browser resource endpoint") from None
        if (not host or not 1 <= port <= 65535 or any(char.isspace() for char in upstream)
                or not lock.is_absolute() or any(char.isspace() for char in str(lock))
                or parsed_checkpoint.scheme not in {"http", "https"} or not parsed_checkpoint.netloc
                or parsed_checkpoint.username is not None or parsed_checkpoint.password is not None
                or parsed_checkpoint.query or parsed_checkpoint.fragment):
            raise SystemExit("invalid browser resource endpoint")
        resolved_lock = lock.resolve(strict=False)
        if upstream in upstreams or resolved_lock in locks or checkpoint in checkpoints:
            raise SystemExit("browser resource physical endpoint collision")
        key = _route_key(configured_agent, workspace)
        UPSTREAMS[key], LOCK_FILES[key], CHECKPOINT_URLS[key] = upstream, resolved_lock, checkpoint
        names.append(workspace)
        upstreams.add(upstream)
        locks.add(resolved_lock)
        checkpoints.add(checkpoint)
    if "default" not in names:
        raise SystemExit("resources must explicitly retain default")
    return tuple(names)


def recover_startup_lock(agent: str, workspace: str = "default") -> bool:
    """Retain an unowned lock left by a previous process.

    Broker handles are deliberately in-memory, so a restart cannot establish
    which disconnected human owned a surviving lock.  Only a fresh handoff's
    explicit terminal lifecycle may checkpoint and release that marker.
    """
    lock = lock_for(agent, workspace)
    if not lock.exists():
        return True
    print(
        f"startup {agent} ambiguous takeover lock retained; automation stays blocked "
        "until a fresh handoff ends successfully",
        flush=True,
    )
    return True


def acquire_human_control(lock):
    try:
        take_automation_lock(lock)
    except OSError as exc:
        raise BrokerError(503, 'browser draining') from exc


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        code = args[1] if len(args) > 1 else ""
        print(f"origin {self.command} {self.path.split('?', 1)[0]} {code}", flush=True)

    def _email(self) -> str:
        assertion = self.headers.get("Cf-Access-Jwt-Assertion") or ""
        if not assertion:
            raise PermissionError("missing")
        state = self.server.state  # type: ignore[attr-defined]
        jwks = self.server.jwks  # type: ignore[attr-defined]
        key = jwks.get_signing_key_from_jwt(assertion)
        claims = jwt.decode(
            assertion,
            key.key,
            algorithms=["RS256"],
            audience=state["aud"],
            issuer=state["issuer"],
        )
        email = (claims.get("email") or "").strip().lower()
        if not email:
            ident = claims.get("identity") or {}
            if isinstance(ident, dict):
                email = (ident.get("email") or "").strip().lower()
        return email

    def _deny(self, status: int, body: bytes) -> None:
        self.close_connection = True
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _authorized(self) -> str | None:
        try:
            email = self._email()
        except Exception:
            self._deny(401, b"unauthorized\n")
            return None
        allowed = {p.access_email for p in self.server.principals}  # type: ignore[attr-defined]
        if email not in allowed:
            self._deny(403, b"forbidden\n")
            return None
        return (email or "").strip().lower()

    def _session_agent(self, route_agent: str, session_id: str, workspace: str = "default") -> str:
        broker: HandoffBroker = self.server.broker  # type: ignore[attr-defined]
        try:
            agent = str(broker.debug(session_id).get("agent_id") or "")
        except Exception:
            raise BrokerError(401, "unknown")
        if route_agent != agent or str(broker.debug(session_id).get("browser_workspace_id") or "default") != workspace or (
            getattr(self.server, "configured_agent", None) not in (None, agent)
        ):
            raise BrokerError(404, "agent route mismatch")
        route_for(agent, workspace)
        return agent

    def _proxy(self, agent: str, session_id: str, upstream_path: str, *, access_email: str | None = None, workspace: str = "default") -> None:
        connection_tokens = {
            token.strip().lower()
            for value in self.headers.get_all("Connection", [])
            for token in value.split(",")
            if token.strip()
        }
        requested_upgrade = bool(self.headers.get("Upgrade")) or "upgrade" in connection_tokens
        upgrade = (self.headers.get("Upgrade") or "").lower() == "websocket"
        if requested_upgrade and (
            not upgrade
            or self.command != "GET"
            or self.headers.get("Content-Length", "0") != "0"
            or self.headers.get("Transfer-Encoding")
            or "upgrade" not in connection_tokens
            or not self.headers.get("Sec-WebSocket-Key")
            or self.headers.get("Sec-WebSocket-Version") != "13"
        ):
            self._deny(400, b"bad websocket upgrade\n")
            return
        if upgrade:
            # A takeover connection acquires the fence before it can send input.
            # An observer reconnect stays read-only and must not re-lock the agent.
            lock = route_for(agent, workspace)[1]
            lock.parent.mkdir(parents=True, exist_ok=True)
            try:
                if access_email is None:
                    raise BrokerError(401, "missing access")
                self.server.broker.acquire_takeover_if_current(
                    session_id, access_email=access_email,
                    acquire=lambda: acquire_human_control(lock),
                )
            except BrokerError as exc:
                self._deny(exc.status, str(exc).encode())
                return
        spec = route_for(agent, workspace)[0]
        up_host, up_port = split_host(spec)
        upstream = socket.create_connection((up_host, up_port), timeout=10)
        headers = [f"{self.command} {upstream_path} HTTP/1.1", f"Host: {up_host}:{up_port}"]
        if upgrade:
            headers.extend(("Upgrade: websocket", "Connection: Upgrade"))
        else:
            headers.append("Connection: close")
        skip = {
            "connection",
            "host",
            "cf-access-jwt-assertion",
            "cookie",
            "keep-alive",
            "proxy-connection",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
        }
        for key, value in self.headers.items():
            if key.lower() in skip:
                continue
            headers.append(f"{key}: {value}")
        raw = ("\r\n".join(headers) + "\r\n\r\n").encode()
        length = int(self.headers.get("Content-Length") or 0)
        if length and self.command != "HEAD":
            raw += self.rfile.read(length)
        upstream.sendall(raw)
        if upgrade:
            self.close_connection = True
            self._splice(upstream, session_id)
            return
        data = b""
        while True:
            chunk = upstream.recv(65536)
            if not chunk:
                break
            data += chunk
        upstream.close()
        self.connection.sendall(data)

    def _splice(self, upstream: socket.socket, session_id: str) -> None:
        client = self.connection
        sockets = [client, upstream]
        broker: HandoffBroker = self.server.broker  # type: ignore[attr-defined]
        frames = WebSocketClientFrames()
        server_frames = WebSocketClientFrames(masked=False)
        observe_rfb = ObserveRfb()
        handshake_trackable = True
        upstream_handshake = bytearray()
        upstream_ready = False

        def track_server(payload: bytes) -> None:
            """Keep legacy takeover byte-transparent but fence Observe fail-closed."""
            nonlocal handshake_trackable
            if not handshake_trackable:
                if broker.debug(session_id)["mode"] != "takeover":
                    raise ProtocolError('untracked takeover socket cannot enter Observe')
                return
            try:
                observe_rfb.server_bytes(payload)
            except ProtocolError:
                # RFB 3.3 and vendor streams are valid transparent takeover
                # traffic.  They have no safe Observe parser, so only mark them
                # ineligible until a mode switch is attempted.
                if broker.debug(session_id)["mode"] != "takeover":
                    raise
                handshake_trackable = False

        def close_splice() -> None:
            for sock in (client, upstream):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass

        # The release preflight reads these parser states under the same broker
        # action lock used by both splice directions below. A fragmented WS
        # frame or RFB record therefore denies Observe before checkpoint can
        # unlock automation, rather than closing after a successful release.
        broker.attach_socket(
            session_id, close_splice,
            transition_safe=lambda: (handshake_trackable and frames.transition_safe
                                     and server_frames.transition_safe
                                     and observe_rfb.takeover_transition_safe),
        )
        try:
            while True:
                try:
                    readable, _, _ = select.select(sockets, [], [], 60)
                except (OSError, ValueError):
                    return
                if not readable:
                    # An idle WebSocket is still a live human connection.
                    continue
                for sock in readable:
                    other = upstream if sock is client else client
                    try:
                        data = sock.recv(65536)
                    except OSError:
                        return
                    if not data:
                        return
                    try:
                        if sock is client:
                            # Observe remains a real RFB client: its handshake,
                            # display configuration and framebuffer requests pass
                            # after parsing; keyboard/pointer/clipboard and every
                            # unrecognised extension terminate fail-closed.
                            def readonly():
                                if not handshake_trackable:
                                    raise ProtocolError('untracked takeover socket cannot enter Observe')
                                for kind, payload in frames.feed(data):
                                    if kind == 'control':
                                        other.sendall(payload)
                                        continue
                                    allowed = observe_rfb.client_message(payload)
                                    if not observe_rfb.transition_safe:
                                        raise ProtocolError('partial takeover RFB record contaminates Observe socket')
                                    if allowed:
                                        other.sendall(client_binary_frame(allowed))

                            def takeover():
                                nonlocal handshake_trackable
                                # Track both sides of the initial RFB handshake while
                                # forwarding takeover traffic unchanged. An invalid
                                # legacy/raw stream is retained for takeover but is
                                # explicitly ineligible for a later Observe switch.
                                if handshake_trackable:
                                    try:
                                        for kind, payload in frames.feed(data):
                                            if kind == 'binary':
                                                observe_rfb.track_client_bytes(payload)
                                    except ProtocolError:
                                        handshake_trackable = False
                                other.sendall(data)

                            broker.forward_viewer_rfb(
                                session_id, observe=readonly, takeover=takeover,
                            )
                        else:
                            def upstream_to_viewer():
                                nonlocal upstream_ready
                                if not upstream_ready:
                                    upstream_handshake.extend(data)
                                    if len(upstream_handshake) > 16384:
                                        raise ProtocolError('oversized upstream WebSocket handshake')
                                    marker = upstream_handshake.find(b'\r\n\r\n')
                                    if marker < 0:
                                        # The raw response is still forwarded, but no
                                        # RFB bytes can be interpreted before 101 ends.
                                        other.sendall(data)
                                        return
                                    if not upstream_handshake.startswith(b'HTTP/1.1 101'):
                                        raise ProtocolError('upstream refused WebSocket upgrade')
                                    rfb = bytes(upstream_handshake[marker + 4:])
                                    upstream_handshake.clear()
                                    upstream_ready = True
                                    if rfb:
                                        for kind, payload in server_frames.feed(rfb):
                                            if kind == 'binary':
                                                track_server(payload)
                                else:
                                    for kind, payload in server_frames.feed(data):
                                        if kind == 'binary':
                                            track_server(payload)
                                other.sendall(data)
                            broker.run_while_active(session_id, upstream_to_viewer)
                    except (OSError, BrokerError, ProtocolError):
                        return
        finally:
            broker.detach_socket(session_id, close_splice)
            try:
                upstream.close()
            except OSError:
                pass

    def _html(self, body: str, *, outcome: str | None = None) -> None:
        raw = body.encode()
        self.send_response(200)
        if outcome is not None:
            self.send_header("X-Checkpoint-Outcome", outcome)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(raw)

    def _shell(self) -> None:
        path = self.path.split("?", 1)[0]
        if path in ("/ended", "/ended.html"):
            self._html(status_page("ended"))
            return
        if path in ("/", "/index.html"):
            self._html(status_page("no_session"))
            return
        self._deny(404, b"scoped session required\n")

    def _scoped_proxy(self, route_agent: str, session_id: str, suffix: str, workspace: str = "default") -> None:
        # Authenticate every noVNC asset and WebSocket against its URL session.
        try:
            email = (self._email() or "").strip().lower()
        except Exception:
            self._deny(401, b"unauthorized\n")
            return
        broker: HandoffBroker = self.server.broker  # type: ignore[attr-defined]
        try:
            broker.public_page(session_id, method="HEAD", access_email=email)
            agent = self._session_agent(route_agent, session_id, workspace)
        except BrokerError as exc:
            self._deny(exc.status, f"{exc}\n".encode())
            return
        query = self.path.partition("?")[2]
        upstream_path = "/" + suffix
        if query:
            upstream_path += "?" + query
        self._proxy(agent, session_id, upstream_path, access_email=email, workspace=workspace)

    def _uuid_get(self, route_agent: str, session_id: str, workspace: str = "default") -> None:
        try:
            self._session_agent(route_agent, session_id, workspace)
        except BrokerError as exc:
            self._deny(exc.status, f"{exc}\n".encode())
            return
        ua = self.headers.get("User-Agent") or ""
        if "slack" in ua.lower():
            self._html(shell_page(ua, session_id=session_id, agent_id=route_agent))
            return
        if self.command == "HEAD":
            self.send_response(200)
            self.send_header("X-OG-Title", "Secure browser sign-in")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        try:
            email = (self._email() or "").strip().lower()
        except Exception:
            self._deny(401, b"unauthorized\n")
            return
        broker: HandoffBroker = self.server.broker  # type: ignore[attr-defined]
        try:
            self._session_agent(route_agent, session_id, workspace)
            page_out = broker.public_page(session_id, method="GET", access_email=email)
        except BrokerError as exc:
            self._deny(exc.status, f"{exc}\n".encode())
            return
        if page_out.status != 200:
            self._deny(page_out.status, b"denied\n")
            return
        try:
            agent = self._session_agent(route_agent, session_id, workspace)
            rec = broker.debug(session_id)
        except BrokerError as exc:
            self._deny(exc.status, f"{exc}\n".encode())
            return
        now = time.time()
        remaining = max(0.0, float(rec.get("expires_at") or 0) - now)
        max_remaining = max(remaining, float(rec.get("max_expires_at") or 0) - now)
        self._html(shell_page(ua, session_id=session_id, agent_id=route_agent, remaining=remaining, max_remaining=max_remaining, agent=agent))

    def _uuid_mode(self, route_agent: str, session_id: str, takeover: bool, workspace: str = "default") -> None:
        if (self.headers.get("Origin") or "") != canonical_public_url():
            self._deny(403, b"csrf\n")
            return
        try:
            email = (self._email() or "").strip().lower()
        except Exception:
            self._deny(401, b"unauthorized\n")
            return
        broker: HandoffBroker = self.server.broker  # type: ignore[attr-defined]
        try:
            self._session_agent(route_agent, session_id, workspace)
            page_out = broker.public_page(session_id, method="GET", access_email=email)
        except BrokerError as exc:
            self._deny(exc.status, f"{exc}\n".encode())
            return
        if page_out.status != 200:
            self._deny(page_out.status, b"denied\n")
            return
        try:
            agent = self._session_agent(route_agent, session_id, workspace)
        except BrokerError as exc:
            self._deny(exc.status, f"{exc}\n".encode())
            return
        lock = lock_for(agent, workspace)
        lock.parent.mkdir(parents=True, exist_ok=True)
        try:
            if takeover:
                self.server.broker.acquire_takeover(
                    session_id, access_email=email,
                    acquire=lambda: acquire_human_control(lock),
                )
            else:
                self.server.broker.release_to_observe(session_id, access_email=email)
        except BrokerError as exc:
            self._deny(exc.status, str(exc).encode())
            return
        body = b"takeover\n" if takeover else b"observe\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _uuid_extend(self, route_agent: str, session_id: str, workspace: str = "default") -> None:
        if (self.headers.get("Origin") or "") != canonical_public_url():
            self._deny(403, b"csrf\n")
            return
        try:
            email = (self._email() or "").strip().lower()
            self._session_agent(route_agent, session_id, workspace)
            broker: HandoffBroker = self.server.broker  # type: ignore[attr-defined]
            broker.public_page(session_id, method="HEAD", access_email=email)
            expires_at = broker.extend(session_id, access_email=email)
            rec = broker.debug(session_id)
        except BrokerError as exc:
            self._deny(exc.status, f"{exc}\n".encode())
            return
        now = time.time()
        body = json.dumps({"remaining": max(0.0, expires_at - now), "max_remaining": max(0.0, float(rec.get("max_expires_at") or expires_at) - now)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _uuid_end(self, route_agent: str, session_id: str, workspace: str = "default") -> None:
        if (self.headers.get("Origin") or "") != canonical_public_url():
            self._deny(403, b"csrf\n")
            return
        try:
            email = (self._email() or "").strip().lower()
        except Exception:
            self._deny(401, b"unauthorized\n")
            return
        broker: HandoffBroker = self.server.broker  # type: ignore[attr-defined]
        try:
            # public_page authenticates before a lazy expiry can run terminal
            # cleanup; the broker owns checkpoint/unlock during end().
            self._session_agent(route_agent, session_id, workspace)
            broker.end(session_id, access_email=email)
        except BrokerError as exc:
            self._deny(exc.status, f"{exc}\n".encode())
            return
        self._html("Ended", outcome=broker.debug(session_id).get("checkpoint_outcome"))

    def do_HEAD(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        workspace = WORKSPACE_UUID_RE.match(path)
        if workspace and not workspace.group(4):
            self._uuid_get(workspace.group(1), workspace.group(3), workspace.group(2))
            return
        scoped_workspace = WORKSPACE_SCOPED_RE.match(path)
        if scoped_workspace and scoped_workspace.group(4):
            self._scoped_proxy(scoped_workspace.group(1), scoped_workspace.group(3), scoped_workspace.group(4) or "", scoped_workspace.group(2))
            return
        match = SCOPED_RE.match(path)
        if match and not match.group(3):
            self._uuid_get(match.group(1), match.group(2))
            return
        if match and match.group(3):
            self._scoped_proxy(match.group(1), match.group(2), match.group(3) or "")
            return
        self._deny(404, b"scoped session required\n")

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        # These are informational only: they must never be a fallback route to a session.
        if path in ("/", "/index.html"):
            self._html(status_page("no_session"))
            return
        if path in ("/ended", "/ended.html"):
            self._html(status_page("ended"))
            return
        workspace = WORKSPACE_UUID_RE.match(path)
        if workspace and not workspace.group(4):
            self._uuid_get(workspace.group(1), workspace.group(3), workspace.group(2))
            return
        scoped_workspace = WORKSPACE_SCOPED_RE.match(path)
        if scoped_workspace and scoped_workspace.group(4):
            self._scoped_proxy(scoped_workspace.group(1), scoped_workspace.group(3), scoped_workspace.group(4) or "", scoped_workspace.group(2))
            return
        match = SCOPED_RE.match(path)
        if match and not match.group(3):
            self._uuid_get(match.group(1), match.group(2))
            return
        if match and match.group(3):
            self._scoped_proxy(match.group(1), match.group(2), match.group(3) or "")
            return
        self._deny(404, b"scoped session required\n")

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        workspace = WORKSPACE_UUID_RE.match(path)
        if workspace:
            agent, resource, session_id, action, _action_name = workspace.groups()
            if action == "/end":
                self._uuid_end(agent, session_id, resource); return
            if action in ("/observe", "/takeover"):
                self._uuid_mode(agent, session_id, action == "/takeover", resource); return
            if action == "/extend":
                self._uuid_extend(agent, session_id, resource); return
        scoped_workspace = WORKSPACE_SCOPED_RE.match(path)
        if scoped_workspace and scoped_workspace.group(4):
            self._scoped_proxy(scoped_workspace.group(1), scoped_workspace.group(3), scoped_workspace.group(4) or "", scoped_workspace.group(2))
            return
        match = UUID_RE.match(path)
        if match and match.group(4) == "end":
            self._uuid_end(match.group(1), match.group(2))
            return
        if match and match.group(4) in ("observe", "takeover"):
            self._uuid_mode(match.group(1), match.group(2), takeover=(match.group(4) == "takeover"))
            return
        if match and match.group(4) == "extend":
            self._uuid_extend(match.group(1), match.group(2))
            return
        scoped = SCOPED_RE.match(path)
        if scoped and scoped.group(2):
            self._scoped_proxy(scoped.group(1), scoped.group(2), scoped.group(3) or "")
            return
        self._deny(404, b"scoped session required\n")


class AdminHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        code = args[1] if len(args) > 1 else ""
        print(f"admin {self.command} {self.path.split('?', 1)[0]} {code}", flush=True)

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _deny(self, status: int, message: str) -> None:
        body = (message + "\n").encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _invocation(self) -> Invocation | None:
        auth = self.headers.get("Authorization") or ""
        token = auth[7:].strip() if auth.startswith("Bearer ") else ""
        capabilities = self.server.capabilities  # type: ignore[attr-defined]
        cap_agent = capabilities.get(token)
        if not token or not cap_agent:
            self._deny(401, "unauthorized")
            return None
        try:
            verify_running_plugin_protocol(self.headers.get("X-Handoff-Plugin-Protocol-Version"))
        except RolloutContractError:
            self._deny(409, "running plugin protocol mismatch")
            return None
        try:
            length = int(self.headers.get('Content-Length') or 0)
            if length < 0 or length > 65536 or self.headers.get('Transfer-Encoding'):
                raise ValueError()
        except ValueError:
            self.close_connection = True
            self._deny(400, 'invalid request framing')
            return None
        self.connection.settimeout(5)
        try:
            raw = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, TimeoutError, UnicodeError):
            self.close_connection = True
            self._deny(400, "bad json")
            return None
        if not isinstance(raw, dict) or set(raw) - {"invocation", "session_id"} or FORBIDDEN_MINT.intersection(raw):
            self._deny(400, "model identity rejected")
            return None
        self.requested_session_id = raw.get("session_id")
        if self.requested_session_id is not None and (not isinstance(self.requested_session_id, str) or len(self.requested_session_id) != 36):
            self._deny(400, "invalid session handle")
            return None
        inv_raw = raw.get("invocation")
        invocation_fields = {"profile", "platform", "user_id", "chat_id", "thread_id", "chat_type", "scope_id", "browser_control_provider", "browser_control_subject", "browser_workspace_id"}
        if not isinstance(inv_raw, dict) or set(inv_raw) - invocation_fields:
            self._deny(400, "missing invocation")
            return None
        provider = inv_raw.get("browser_control_provider")
        subject = inv_raw.get("browser_control_subject")
        if any(value is not None and not isinstance(value, str) for value in (provider, subject)):
            self._deny(400, "invalid desktop identity")
            return None
        inv = Invocation(
            profile=str(inv_raw.get("profile") or ""),
            platform=str(inv_raw.get("platform") or ""),
            user_id=str(inv_raw.get("user_id") or ""),
            chat_id=str(inv_raw.get("chat_id") or ""),
            thread_id=str(inv_raw.get("thread_id") or ""),
            chat_type=str(inv_raw.get("chat_type") or ""),
            scope_id=str(inv_raw.get("scope_id") or ""),
            browser_control_provider=provider or "",
            browser_control_subject=subject or "",
            browser_workspace_id=str(inv_raw.get("browser_workspace_id") or "default"),
        )
        if not cap_may_mint(cap_agent, inv.profile):
            self._deny(403, "agent denied")
            return None
        return inv

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path not in {"/v1/handoffs", "/v1/handoffs/status", "/v1/handoffs/end"}:
            self._deny(404, "not found")
            return
        inv = self._invocation()
        if inv is None:
            return
        try:
            self.server.reload_principals()  # type: ignore[attr-defined]
        except SystemExit:
            # Invalid or unavailable revocation data must fail closed, never
            # leave a previously loaded grant usable indefinitely.
            self._deny(503, "principal policy unavailable")
            return
        broker: HandoffBroker = self.server.broker  # type: ignore[attr-defined]
        try:
            broker._authorized_principal(inv)
            if path == "/v1/handoffs":
                minted = broker.mint(inv)
                payload = {
                    "session_id": minted.session_id,
                    "url": minted.url,
                    "expires_at": minted.expires_at,
                }
            elif path == "/v1/handoffs/status":
                # A recovery_required session is still live here and reports
                # itself; a missing handle is 404 'no active handoff'.
                payload = broker.status(inv)
            else:
                # end_current authenticates before the broker performs its
                # lifecycle-owned checkpoint and unlock.
                session_id = broker.end_current(inv, expected_session_id=getattr(self, 'requested_session_id', None))
                try:
                    payload = broker.status(inv)
                except BrokerError as status_error:
                    if status_error.status != 404:
                        raise
                    payload = {"session_id": session_id, "state": "ended",
                               "checkpoint_outcome": broker.debug(session_id).get("checkpoint_outcome")}
        except BrokerError as exc:
            print(
                f"admin handoff denied {exc.status} {exc} chat_type={inv.chat_type!r} "
                f"profile={inv.profile!r}",
                flush=True,
            )
            self._deny(exc.status, str(exc))
            return
        self._send_json(200, payload)

class Server(ThreadingHTTPServer):
    def __init__(self, addr, handler, state, jwks, broker, capabilities, principals=(), principals_loader=None):
        super().__init__(addr, handler)
        self.state = state
        self.jwks = jwks
        self.broker = broker
        self.capabilities = capabilities
        self.principals = tuple(principals) or tuple(broker.principals)
        self.principals_loader = principals_loader

    def reload_principals(self) -> None:
        if self.principals_loader is None:
            return
        try:
            principals = tuple(self.principals_loader())
        except (OSError, ValueError, SystemExit):
            # A malformed or missing render is a revocation event, not a reason
            # to keep the last successful grant usable. Publish an empty policy
            # and synchronously revoke its active viewers before surfacing the
            # admin failure.
            self.broker.replace_principals(())
            self.principals = ()
            raise SystemExit("principal policy unavailable")
        self.broker.replace_principals(principals)
        self.principals = principals


def rollout_versions_from_environment() -> RolloutVersions:
    """Read the atomically rendered rollout generation; missing is non-ready."""
    values: dict[str, int] = {}
    for field, variable in (
        ("plugin", "HANDOFF_PLUGIN_PROTOCOL_VERSION"),
        ("broker", "HANDOFF_BROKER_PROTOCOL_VERSION"),
        ("policy", "HANDOFF_POLICY_VERSION"),
    ):
        raw = os.environ.get(variable)
        if not isinstance(raw, str) or not raw.isdecimal():
            raise RolloutContractError(f"{variable} is required")
        values[field] = int(raw)
    return RolloutVersions(**values)


def main() -> int:
    # A handle is an in-memory, single-process capability. Reject persisted
    # handle configuration before consulting any unrelated runtime settings.
    if os.environ.get("HANDOFF_STATE_FILE") or os.environ.get("HANDOFF_SESSION_STORE"):
        raise SystemExit("persisted handoff handles are not supported by this release")
    # Refuse mixed or undeclared plugin/broker/policy generations before
    # loading state or inspecting an inherited physical lock.
    try:
        preflight(rollout_versions_from_environment())
    except RolloutContractError as exc:
        raise SystemExit(f"rollout preflight failed: {exc}") from exc
    state = load_state()
    if not state.get("aud") or not state.get("issuer"):
        raise SystemExit("missing aud/issuer")
    configured_agent = (os.environ.get("HANDOFF_AGENT") or "").strip()
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,63}", configured_agent):
        raise SystemExit("missing or invalid HANDOFF_AGENT")
    try:
        canonical_public_url()
    except PublicUrlError as exc:
        raise SystemExit(str(exc)) from exc
    principals = load_principals(configured_agent)
    try:
        public_bind = configured_bind("PUBLIC_BIND_HOST")
        admin_bind = configured_bind("ADMIN_BIND_HOST")
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if public_bind == admin_bind:
        raise SystemExit("public and admin bind IPs must differ")
    capabilities = load_capabilities(configured_agent)
    if not capabilities:
        raise SystemExit("missing capabilities")
    jwks = PyJWKClient(f"{state['issuer'].rstrip('/')}/cdn-cgi/access/certs")
    ttl = int(os.environ.get("HANDOFF_TTL_SECONDS") or "1800")
    if set(capabilities.values()) != {configured_agent}:
        raise SystemExit("capabilities must belong to HANDOFF_AGENT")
    workspace_ids = load_workspace_routes(configured_agent)
    upstream, lock_file, _checkpoint = route_for(configured_agent)
    lifecycles = {
        workspace: InheritedLockLifecycle(configured_agent, workspace=workspace, inherited=lock_for(configured_agent, workspace).exists())
        for workspace in workspace_ids
    }
    broker: HandoffBroker

    def terminal_transition(agent: str, workspace: str, session_id: str, state: str) -> None:
        lifecycles[workspace].transition(agent, session_id, state, broker.debug(session_id)["takeover_confirmed"])

    def observe_transition(agent: str, workspace: str, session_id: str) -> str:
        return lifecycles[workspace].release_to_observe(
            agent, session_id, broker.debug(session_id)["takeover_confirmed"]
        )

    def terminal_finish(agent: str, workspace: str, state: str) -> str:
        return lifecycles[workspace].finish(agent, state)

    def held_status(agent: str) -> dict | None:
        # Status is legacy default-only; fresh resource status comes via its explicit invocation.
        return lifecycles["default"].status(agent)

    def policy_revoked(agent: str) -> None:
        # A revoked principal may have sessions on multiple physical resources.
        # Fence every resource; no callback can release a revoked lease.
        for lifecycle in lifecycles.values():
            lifecycle.retain_after_policy_revocation(agent)

    broker = HandoffBroker(
        principals,
        clock=time.time,
        ttl_seconds=ttl,
        configured_agent=configured_agent,
        configured_workspaces=workspace_ids,
        on_transition=terminal_transition,
        on_terminal=terminal_finish,
        on_hold_status=held_status,
        on_observe=observe_transition,
        on_policy_revoked=policy_revoked,
    )
    # Handles are in-memory: a restart invalidates them all. Browser
    # cookies/state are separate and remain in the existing browser.
    httpd = Server((public_bind, PORT), Handler, state, jwks, broker, capabilities, principals)
    httpd.principals_loader = lambda: load_principals(configured_agent)
    httpd.configured_agent = configured_agent
    admin = Server((admin_bind, ADMIN_PORT), AdminHandler, state, jwks, broker, capabilities, principals)
    admin.principals_loader = lambda: load_principals(configured_agent)
    admin.configured_agent = configured_agent
    # Leases are per physical resource: sibling workspaces may run together,
    # but two brokers can never own/checkpoint the same lock directory.
    broker_leases = []
    for workspace in workspace_ids:
        resource_lock = lock_for(configured_agent, workspace)
        resource_lock.parent.mkdir(parents=True, exist_ok=True)
        lease = os.open(resource_lock.parent / ('.broker-lease-' + resource_lock.name), os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        broker_leases.append(lease)
    # Handles do not survive a restart, so every surviving marker remains
    # fenced independently until a fresh handoff ends successfully.
    for workspace in workspace_ids:
        recover_startup_lock(configured_agent, workspace)
    threading.Thread(target=admin.serve_forever, daemon=True).start()
    print(
        f"listening {public_bind}:{PORT} {configured_agent}={upstream}; admin {admin_bind}:{ADMIN_PORT}",
        flush=True,
    )

    def expire_idle() -> None:
        while True:
            time.sleep(0.25)
            broker.expire_sessions()
    threading.Thread(target=expire_idle, daemon=True).start()
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
