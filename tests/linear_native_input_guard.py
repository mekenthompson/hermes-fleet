"""Deny credential/model work, external I/O and non-fixture writes before native imports."""
from __future__ import annotations
import ipaddress
import builtins
import os
from pathlib import Path
import sys
import threading
from urllib.parse import unquote, urlsplit


class Guard:
    def __init__(self, root: Path, real_home: Path):
        self.root = root.resolve(strict=True)
        self.real_home = real_home.resolve()
        self.violations = []
        self.patching = False
        self.open_paths = threading.local()

    def deny(self, operation):
        self.violations.append(operation)
        frames, frame = [], sys._getframe(1)
        while frame is not None and len(frames) < 12:
            frames.append(str(frame.f_globals.get("__name__") or "unknown") + "." + frame.f_code.co_name + ":" + str(frame.f_lineno))
            frame = frame.f_back
        os.write(2, ("ORIGIN_GUARD_DENIED: " + operation + " <- " + " <- ".join(frames) + "\n").encode())
        raise PermissionError("Origin proof forbids " + operation)

    def inside(self, path):
        return Path(path).resolve().is_relative_to(self.root)

    def path(self, value, dir_fd=None, *, follow_leaf=True):
        if isinstance(value, int):
            return Path(os.readlink("/proc/self/fd/" + str(value))).resolve()
        path = Path(os.fsdecode(value))
        if not path.is_absolute():
            base = Path.cwd() if dir_fd in (None, -1) else Path(os.readlink("/proc/self/fd/" + str(dir_fd)))
            path = base / path
        return path.resolve() if follow_leaf else path.parent.resolve() / path.name

    def mutation(self, value, dir_fd=None, *, follow_leaf=True):
        if not self.inside(self.path(value, dir_fd, follow_leaf=follow_leaf)):
            self.deny("filesystem mutation outside isolated proof root")

    def audit(self, event, args):
        if event == "open" and not isinstance(args[0], int):
            stack = getattr(self.open_paths, "stack", [])
            path = stack[-1][1] if stack and stack[-1][0] == os.fspath(args[0]) else self.path(args[0])
            if any(path.is_relative_to(self.real_home / name) for name in
                   (".hermes", ".codex", ".claude", ".qwen", ".gemini", ".aws", ".azure", ".config", ".cache")):
                self.deny("real user configuration/credential access")
            mode, flags = args[1], args[2]
            writes = isinstance(mode, str) and any(c in mode for c in "wax+")
            writes |= isinstance(flags, int) and bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
            if writes and not self.inside(path):
                self.deny("write outside isolated proof root")
        elif event == "sqlite3.connect":
            path = os.fsdecode(args[0])
            if path != ":memory:":
                if path.startswith("file:"):
                    path = unquote(urlsplit(path).path)
                if not self.inside(path):
                    self.deny("database access outside isolated proof root")
        elif event in {"os.remove", "os.rmdir"}:
            self.mutation(args[0], args[1], follow_leaf=False)
        elif event in {"os.mkdir", "os.chmod"}:
            self.mutation(args[0], args[2], follow_leaf=event == "os.chmod")
        elif event == "os.chown":
            self.mutation(args[0], args[3])
        elif event == "os.utime":
            self.mutation(args[0], args[3])
        elif event == "os.truncate":
            self.mutation(args[0])
        elif event in {"os.rename", "os.link", "os.symlink"}:
            if event == "os.symlink":
                self.mutation(args[1], args[2], follow_leaf=False)
            else:
                self.mutation(args[0], args[2], follow_leaf=False)
                self.mutation(args[1], args[3], follow_leaf=False)
        elif event == "socket.connect":
            address = args[1]
            if isinstance(address, (str, bytes)):
                if not self.inside(os.fsdecode(address)):
                    self.deny("socket outside isolated proof root")
            else:
                try:
                    local = ipaddress.ip_address(address[0]).is_loopback
                except ValueError:
                    local = address[0] == "localhost"
                if not local:
                    self.deny("nonlocal network")
        elif event == "socket.getaddrinfo" and args[0] not in {None, "localhost", "127.0.0.1", "::1"}:
            self.deny("nonlocal DNS")
        elif event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.exec"}:
            self.deny("external process")

    def profile(self, frame, event, arg):
        if event != "call":
            return
        module, name = frame.f_globals.get("__name__") or "", frame.f_code.co_name
        if (module.startswith("hermes_cli.auth") and name in {
                "_load_auth_store", "_load_global_auth_store", "_save_auth_store", "_auth_store_lock",
                "_save_provider_state", "_persist_provider_state_to_store"}
                or module == "agent.credential_pool" and name in {
                    "load_pool", "select", "_refresh_entry", "_refresh_entry_impl", "_persist"}
                or module == "tools.xai_http" and name == "resolve_xai_http_credentials"
                or module == "gateway.session_local_inventory" and name in {"snapshot_info", "session_inventory"}
                or module == "hermes_cli.banner" and name == "compute_toolset_availability"
                or module == "run_agent" and name in {"run_conversation", "__init__"}
                or module.startswith("openai.resources.") and name in {"create", "stream"}
                or module.startswith("anthropic.resources.") and name in {"create", "stream"}):
            self.deny("credential, inventory or model activation: " + module + "." + name)

    def patch_loaded(self):
        """Persistent call guards also survive code that catches a profiling-hook refusal."""
        if self.patching:
            return
        self.patching = True
        try:
            targets = {
                "hermes_cli.auth": ("_load_auth_store", "_load_global_auth_store", "_save_auth_store", "_auth_store_lock"),
                "agent.credential_pool": ("load_pool", "persist_pool_entries"),
                "tools.xai_http": ("resolve_xai_http_credentials",),
                "gateway.session_local_inventory": ("snapshot_info", "session_inventory"),
                "hermes_cli.banner": ("compute_toolset_availability",),
            }
            objects = [(sys.modules.get(module), name, module + "." + name)
                       for module, names in targets.items() for name in names]
            for module, cls, names in (
                    ("agent.credential_pool", "CredentialPool", ("select", "_refresh_entry", "_persist")),
                    ("run_agent", "AIAgent", ("__init__", "run_conversation"))):
                owner = getattr(sys.modules.get(module), cls, None)
                objects.extend((owner, name, module + "." + cls + "." + name) for name in names)
            for owner, name, label in objects:
                if owner is None:
                    continue
                value = getattr(owner, name, None)
                if callable(value) and not getattr(value, "_origin_guard", False):
                    def blocked(*args, _label=label, **kwargs):
                        self.deny("credential, inventory or model activation: " + _label)
                    blocked._origin_guard = True
                    setattr(owner, name, blocked)
        finally:
            self.patching = False

    def install(self):
        sys.addaudithook(self.audit)
        original_open = os.open
        def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
            # CPython's open audit event omits dir_fd; carry the resolved actual target into it.
            resolved = self.path(path, dir_fd)
            stack = getattr(self.open_paths, "stack", None)
            if stack is None:
                stack = self.open_paths.stack = []
            stack.append((os.fspath(path), resolved))
            try:
                return original_open(path, flags, mode, dir_fd=dir_fd)
            finally:
                stack.pop()
        os.open = guarded_open
        if original_open in os.supports_dir_fd:
            os.supports_dir_fd.add(guarded_open)
        original = builtins.__import__
        def guarded_import(*args, **kwargs):
            result = original(*args, **kwargs)
            self.patch_loaded()
            return result
        builtins.__import__ = guarded_import
        sys.setprofile(self.profile)
        threading.setprofile(self.profile)
