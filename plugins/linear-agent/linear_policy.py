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


def _require_trusted_root_owned(metadata: os.stat_result, *, description: str) -> None:
    if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise ImmutablePolicyError(f"{description} is unavailable")


def _open_trusted_policy(path: Path, *, description: str) -> int:
    """Open a root-owned regular file through root-owned, non-writable directories."""
    if os.geteuid() == 0:
        raise ImmutablePolicyError(f"{description} reader must not run as root")
    if not path.is_absolute():
        raise ImmutablePolicyError(f"{description} path must be absolute")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    directory: int | None = None
    descriptor: int | None = None
    try:
        directory = os.open("/", directory_flags)
        _require_trusted_root_owned(os.fstat(directory), description=description)
        for component in path.parts[1:-1]:
            child = os.open(component, directory_flags, dir_fd=directory)
            os.close(directory)
            directory = child
            metadata = os.fstat(directory)
            if not stat.S_ISDIR(metadata.st_mode):
                raise ImmutablePolicyError(f"{description} ancestor is not a directory")
            _require_trusted_root_owned(metadata, description=description)
        descriptor = os.open(path.name, file_flags, dir_fd=directory)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ImmutablePolicyError(f"{description} must be a regular file")
        _require_trusted_root_owned(metadata, description=description)
        return descriptor
    except (ImmutablePolicyError, OSError) as exc:
        if descriptor is not None:
            os.close(descriptor)
        if isinstance(exc, ImmutablePolicyError):
            raise
        raise ImmutablePolicyError(f"{description} is unavailable") from exc
    finally:
        if directory is not None:
            os.close(directory)


def read_immutable_json(path: Path, *, description: str) -> object:
    """Read a bounded policy map from the deployment's immutable trust boundary.

    A runtime-owned read-only file is not a policy boundary: that runtime can
    chmod, rewrite, and chmod it back. The file and every path ancestor must
    instead be root-owned and non-writable by group or other users.
    """
    descriptor = _open_trusted_policy(path, description=description)
    try:
        metadata = os.fstat(descriptor)
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
