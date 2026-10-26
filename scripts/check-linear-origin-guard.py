#!/usr/bin/env python3
"""Pure-stdlib subprocess proof for the origin guard; never imports native Hermes."""
from __future__ import annotations
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def child(base):
    # Both descriptors are created before the guard, allowing the test to challenge dir_fd handling.
    proof, outside, would_be_home = base / "proof", base / "outside", base / "would-be-real-home"
    proof_fd = os.open(proof, os.O_RDONLY | os.O_DIRECTORY)
    outside_fd = os.open(outside, os.O_RDONLY | os.O_DIRECTORY)
    # -I ignores PYTHONDONTWRITEBYTECODE, so set the interpreter flag before loading the guard.
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(ROOT / "tests"))
    from linear_origin_guard import Guard
    guard = Guard(proof, would_be_home)
    guard.install()
    checked = []
    def denied(label, operation):
        count = len(guard.violations)
        try:
            operation()
        except PermissionError:
            assert len(guard.violations) == count + 1, label
        else:
            raise AssertionError("Guard admitted " + label)
        checked.append(label)
    denied("credential/config read", lambda: open(would_be_home / ".config/credentials-never-created"))
    denied("external relative-FD write", lambda: os.open("never-created", os.O_WRONLY | os.O_CREAT, dir_fd=outside_fd))
    denied("external relative-FD mkdir", lambda: os.mkdir("never-created", dir_fd=outside_fd))
    denied("external relative-FD chmod", lambda: os.chmod(".", 0o700, dir_fd=outside_fd))
    denied("external relative-FD utime", lambda: os.utime(".", dir_fd=outside_fd))
    denied("external relative-FD rename", lambda: os.rename("absent", "absent", src_dir_fd=proof_fd, dst_dir_fd=outside_fd))
    denied("external relative-FD remove", lambda: os.unlink("absent", dir_fd=outside_fd))
    denied("nonlocal network", lambda: socket.socket().connect(("203.0.113.1", 9)))
    denied("external process", lambda: subprocess.Popen([sys.executable, "-c", "raise AssertionError('must not execute')"]))
    os.mkdir("child", dir_fd=proof_fd)
    child_fd = os.open("child", os.O_RDONLY | os.O_DIRECTORY, dir_fd=proof_fd)
    fd = os.open("sentinel", os.O_WRONLY | os.O_CREAT, dir_fd=child_fd)
    os.write(fd, b"fixture")
    os.close(fd)
    os.rename("sentinel", "renamed", src_dir_fd=child_fd, dst_dir_fd=child_fd)
    os.utime("renamed", dir_fd=child_fd)
    os.unlink("renamed", dir_fd=child_fd)
    os.close(child_fd)
    shutil.rmtree("child", dir_fd=proof_fd)
    os.close(proof_fd)
    os.close(outside_fd)
    assert not (outside / "never-created").exists()
    assert not (proof / "child").exists()
    print(json.dumps({"denied": checked, "proof_relative_fd_cleanup": "passed", "native_imports": False}))


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--child":
        child(Path(sys.argv[2]))
        return 0
    with tempfile.TemporaryDirectory(prefix="linear-origin-guard-check-") as temporary:
        base = Path(temporary)
        for name in ("proof", "outside", "would-be-real-home"):
            (base / name).mkdir()
        env = {"PATH": os.defpath, "HOME": str(base / "proof"), "USERPROFILE": str(base / "proof"),
               "HERMES_HOME": str(base / "proof"), "XDG_CONFIG_HOME": str(base / "proof/.config"),
               "XDG_CACHE_HOME": str(base / "proof/.cache"), "PYTHONDONTWRITEBYTECODE": "1"}
        return subprocess.run([sys.executable, "-I", "-S", str(Path(__file__).resolve()), "--child", str(base)], env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
