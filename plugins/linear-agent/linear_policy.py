"""Bounded, no-follow readers for externally mounted Linear policy maps."""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path

POLICY_LIMIT = 1_048_576
AGENT_POLICY_PATH = Path(
    os.environ.get("HERMES_LINEAR_AGENT_POLICY_PATH", "/opt/hermes-fleet/policy/linear-agents.json")
)
PUBLISHER_POLICY_PATH = Path(
    os.environ.get("HERMES_LINEAR_PUBLISHER_POLICY_PATH", "/opt/hermes-fleet/policy/linear-publishers.json")
)


class ImmutablePolicyError(RuntimeError):
    """An external policy map is unavailable or not safe to trust."""


def read_immutable_json(path: Path, *, description: str) -> object:
    """Read one small, regular, non-writable policy map without following links.

    Policy maps are mounted by the deployment overlay. They may be owned by root
    and readable by the runtime, but no actor may retain a writable mode bit.
    """
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ImmutablePolicyError(f"{description} is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ImmutablePolicyError(f"{description} must be a regular file")
        if metadata.st_uid not in {0, os.geteuid()}:
            raise ImmutablePolicyError(f"{description} owner is invalid")
        if stat.S_IMODE(metadata.st_mode) & 0o222:
            raise ImmutablePolicyError(f"{description} policy is writable by the runtime")
        if metadata.st_size > POLICY_LIMIT:
            raise ImmutablePolicyError(f"{description} is too large")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65_536, POLICY_LIMIT + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > POLICY_LIMIT:
                raise ImmutablePolicyError(f"{description} is too large")
    finally:
        os.close(descriptor)
    try:
        return json.loads(b"".join(chunks).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ImmutablePolicyError(f"{description} is unavailable") from exc


def read_agent_policy(path: Path = AGENT_POLICY_PATH) -> object:
    return read_immutable_json(path, description="linear-agent immutable managed OAuth policy")


def read_publisher_policy(path: Path = PUBLISHER_POLICY_PATH) -> object:
    return read_immutable_json(path, description="immutable Linear publishing policy")
