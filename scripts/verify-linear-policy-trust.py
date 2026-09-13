#!/usr/bin/env python3
"""Exercise the installed Linear policy boundary as the image runtime UID."""
from __future__ import annotations

import json
import os
import pwd
import sys
from pathlib import Path

sys.path.insert(0, "/opt/hermes/plugins/linear-agent")
from linear_policy import ImmutablePolicyError, POLICY_LIMIT, read_immutable_json


BASE = Path("/opt/hermes-fleet/policy-acceptance")
RUNTIME = pwd.getpwnam("hermes")


def write_policy(path: Path) -> None:
    path.write_text(json.dumps({"agents": []}), encoding="utf-8")
    path.chmod(0o444)


def rejected(path: Path, expected: str) -> None:
    try:
        read_immutable_json(path, description="acceptance policy")
    except ImmutablePolicyError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"unsafe policy accepted: {path}")


def main() -> None:
    assert os.geteuid() == 0, "harness must create fixtures before dropping privileges"
    assert RUNTIME.pw_uid == 1000, "acceptance must exercise the production runtime UID"
    BASE.mkdir(mode=0o755, parents=True, exist_ok=False)
    safe = BASE / "safe.json"
    root_writable = BASE / "root-writable.json"
    runtime_owned = BASE / "runtime-owned.json"
    unsafe_parent = BASE / "runtime-owned-parent"
    unsafe_leaf = unsafe_parent / "policy.json"
    writable_parent = BASE / "group-writable-parent"
    fifo = BASE / "policy.fifo"
    for path in (safe, root_writable, runtime_owned):
        write_policy(path)
    root_writable.chmod(0o644)
    os.chown(runtime_owned, RUNTIME.pw_uid, RUNTIME.pw_gid)
    unsafe_parent.mkdir(mode=0o755)
    os.chown(unsafe_parent, RUNTIME.pw_uid, RUNTIME.pw_gid)
    write_policy(unsafe_leaf)
    writable_parent.mkdir(mode=0o755)
    write_policy(writable_parent / "policy.json")
    writable_parent.chmod(0o775)
    group_writable = BASE / "group-writable.json"
    write_policy(group_writable)
    group_writable.chmod(0o664)
    (BASE / "leaf-link.json").symlink_to(safe)
    (BASE / "parent-link").symlink_to(BASE, target_is_directory=True)
    oversized = BASE / "oversized.json"
    oversized.write_bytes(b" " * (POLICY_LIMIT + 1))
    oversized.chmod(0o444)
    invalid = BASE / "invalid.json"
    invalid.write_text("not json", encoding="utf-8")
    invalid.chmod(0o444)
    os.mkfifo(fifo, 0o444)

    rejected(safe, "run as root")
    os.setgroups([])
    os.setgid(RUNTIME.pw_gid)
    os.setuid(RUNTIME.pw_uid)
    assert os.geteuid() == RUNTIME.pw_uid
    for path in (safe, root_writable):
        assert read_immutable_json(path, description="acceptance policy") == {"agents": []}
        try:
            path.chmod(0o644)
        except PermissionError:
            pass
        else:
            raise AssertionError("runtime can change trusted policy permissions")
    rejected(runtime_owned, "unavailable")
    runtime_owned.chmod(0o644)
    runtime_owned.write_text('{"agents": ["changed"]}', encoding="utf-8")
    runtime_owned.chmod(0o444)
    rejected(runtime_owned, "unavailable")
    rejected(unsafe_leaf, "unavailable")
    unsafe_leaf.unlink()
    write_policy(unsafe_leaf)
    unsafe_parent.chmod(0o555)
    rejected(unsafe_leaf, "unavailable")
    for path in (group_writable, writable_parent / "policy.json", BASE / "leaf-link.json", BASE / "parent-link" / "safe.json"):
        rejected(path, "unavailable")
    rejected(fifo, "regular file")
    rejected(oversized, "too large")
    rejected(invalid, "unavailable")
    descriptors_before = len(list(Path("/proc/self/fd").iterdir()))
    for _ in range(32):
        rejected(runtime_owned, "unavailable")
        rejected(fifo, "regular file")
    assert len(list(Path("/proc/self/fd").iterdir())) == descriptors_before
    print("Linear policy boundary: trusted reads and runtime mutation/path/special-file refusals passed (uid=1000)")


if __name__ == "__main__":
    main()
