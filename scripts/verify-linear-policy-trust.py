#!/usr/bin/env python3
"""Exercise the installed Linear policy boundary as the image runtime UID."""
from __future__ import annotations

import json
import os
import pwd
import sys
from pathlib import Path

sys.path.insert(0, "/opt/hermes/plugins/linear-agent")
from linear_policy import ImmutablePolicyError, read_immutable_json


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
    BASE.mkdir(mode=0o755, parents=True, exist_ok=True)
    BASE.chmod(0o755)
    safe = BASE / "safe.json"
    runtime_owned = BASE / "runtime-owned.json"
    unsafe_parent = BASE / "runtime-owned-parent"
    unsafe_leaf = unsafe_parent / "policy.json"
    fifo = BASE / "policy.fifo"
    write_policy(safe)
    write_policy(runtime_owned)
    os.chown(runtime_owned, RUNTIME.pw_uid, RUNTIME.pw_gid)
    unsafe_parent.mkdir(mode=0o755, exist_ok=True)
    unsafe_parent.chmod(0o755)
    os.chown(unsafe_parent, RUNTIME.pw_uid, RUNTIME.pw_gid)
    write_policy(unsafe_leaf)
    os.mkfifo(fifo, 0o444)

    rejected(safe, "run as root")
    os.setgroups([])
    os.setgid(RUNTIME.pw_gid)
    os.setuid(RUNTIME.pw_uid)
    assert os.geteuid() == RUNTIME.pw_uid
    assert read_immutable_json(safe, description="acceptance policy") == {"agents": []}
    rejected(runtime_owned, "unavailable")
    rejected(unsafe_leaf, "unavailable")
    rejected(fifo, "regular file")


if __name__ == "__main__":
    main()
