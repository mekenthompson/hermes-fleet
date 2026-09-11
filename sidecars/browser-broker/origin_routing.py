"""Strict one-agent broker configuration helpers."""
from __future__ import annotations

import fcntl
import os
import re
import socket
from pathlib import Path
from typing import Mapping

AGENT_RE = re.compile(r"[a-z][a-z0-9-]{0,63}\Z")


def agent_env_suffix(agent: str) -> str:
    if not AGENT_RE.fullmatch(agent):
        raise ValueError("invalid agent")
    return agent.upper().replace("-", "_")


def env_key(prefix: str, agent: str) -> str:
    return f"{prefix}_{agent_env_suffix(agent)}"


def required_agent_value(prefix: str, agent: str, environ: Mapping[str, str] | None = None) -> str:
    """Read an agent's explicit setting; never select a default value."""
    env = os.environ if environ is None else environ
    value = (env.get(env_key(prefix, agent)) or env.get(prefix) or "").strip()
    if not value:
        raise ValueError(f"missing {prefix} for {agent}")
    return value


def route_for_agent(agent: str, environ: Mapping[str, str] | None = None) -> tuple[str, Path, str]:
    env = os.environ if environ is None else environ
    upstream = required_agent_value("NOVNC_UPSTREAM", agent, env)
    lock = Path(required_agent_value("HANDOFF_LOCK_FILE", agent, env))
    checkpoint = required_agent_value("CHECKPOINT_URL", agent, env)
    return upstream, lock, checkpoint


def resolve_single_bind(value: str) -> str:
    """Resolve a bind name to exactly one concrete IP, rejecting wildcards."""
    candidate = value.strip()
    if not candidate or candidate in {"0.0.0.0", "::", "*"}:
        raise ValueError("wildcard or empty bind")
    try:
        infos = socket.getaddrinfo(candidate, 0, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError(f"unresolvable bind: {candidate}") from exc
    addresses = {info[4][0] for info in infos}
    if len(addresses) != 1:
        raise ValueError(f"bind must resolve exactly one IP: {candidate}")
    address = str(next(iter(addresses)))
    if address in {"0.0.0.0", "::"}:
        raise ValueError("wildcard bind")
    return address


def configured_bind(kind: str, environ: Mapping[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    # HANDOFF_ names are canonical; old non-prefixed names remain an explicit alias.
    value = env.get(f"HANDOFF_{kind}") or env.get(kind) or ""
    return resolve_single_bind(value)


def cap_may_mint(cap_agent: str, profile: str) -> bool:
    """Capabilities are a one-to-one binding, never a wildcard/admin grant."""
    return bool(AGENT_RE.fullmatch(cap_agent or "") and cap_agent == profile)


def take_automation_lock(lock: Path) -> None:
    """Stop admission, then drain in-flight REST calls before acknowledging human control.

    The REST lock proxy holds a shared flock on the same mounted directory
    inode for every request it forwards; taking the exclusive flock returns
    only once all admitted requests have finished.
    """
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.touch(mode=0o600, exist_ok=True)
    descriptor = os.open(lock.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
    finally:
        os.close(descriptor)
