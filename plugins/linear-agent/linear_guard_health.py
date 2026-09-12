"""Fail-closed readiness heartbeat written only by a running Linear runtime."""
from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

# Capability handshake, not an assertion of full gateway/child cancellation.
# Version 5 requires startup validation of the exact core Stop ABI.
SCHEMA_VERSION = 5


class WorkerGuardHealth:
    def __init__(self, database: Path, *, profile: str, workspace: str, ttl_seconds: float = 30.0) -> None:
        if not profile or not workspace or ttl_seconds <= 0:
            raise ValueError("profile, workspace, and positive ttl_seconds are required")
        self.database = Path(database).absolute()
        self.profile = profile
        self.workspace = workspace
        self.ttl_seconds = float(ttl_seconds)
        self._instance_id = uuid.uuid4().hex
        self._lifecycle_lock = threading.Lock()
        self._closed = False
        self.database.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("""CREATE TABLE IF NOT EXISTS linear_worker_guard_heartbeat (
                profile TEXT NOT NULL, workspace TEXT NOT NULL, database_path TEXT NOT NULL,
                schema_version INTEGER NOT NULL, pid INTEGER NOT NULL, process_starttime TEXT NOT NULL,
                wallclock REAL NOT NULL, active INTEGER NOT NULL CHECK(active IN (0,1)),
                instance_id TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(profile, workspace)
            )""")
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(linear_worker_guard_heartbeat)")}
            if "instance_id" not in columns:
                try:
                    conn.execute("ALTER TABLE linear_worker_guard_heartbeat ADD COLUMN instance_id TEXT NOT NULL DEFAULT ''")
                except sqlite3.OperationalError:
                    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(linear_worker_guard_heartbeat)")}
                    if "instance_id" not in columns:
                        raise

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.database, isolation_level=None, timeout=5.0)
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def process_starttime(pid: int) -> str:
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            # Field 22; the executable name may contain spaces and parentheses.
            tail = stat[stat.rfind(")") + 2:].split()
            return tail[19]
        except (OSError, IndexError, ValueError) as exc:
            raise RuntimeError("cannot attest worker process start time") from exc

    def publish(self, *, pid: int | None = None, starttime: str | None = None, now: float | None = None) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            pid = os.getpid() if pid is None else int(pid)
            starttime = self.process_starttime(pid) if starttime is None else starttime
            if not starttime:
                raise ValueError("process starttime is required")
            wallclock = time.time() if now is None else float(now)
            with self._connect() as conn:
                conn.execute("""INSERT INTO linear_worker_guard_heartbeat
                (profile, workspace, database_path, schema_version, pid, process_starttime, wallclock, active, instance_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(profile, workspace) DO UPDATE SET database_path=excluded.database_path,
                schema_version=excluded.schema_version, pid=excluded.pid, process_starttime=excluded.process_starttime,
                wallclock=excluded.wallclock, active=1, instance_id=excluded.instance_id""", (self.profile, self.workspace, str(self.database), SCHEMA_VERSION, pid, starttime, wallclock, self._instance_id))

    def is_ready(self, *, now: float | None = None) -> bool:
        try:
            wallclock = time.time() if now is None else float(now)
        except (TypeError, ValueError):
            return False
        with self._connect() as conn:
            row = conn.execute("SELECT database_path, schema_version, pid, process_starttime, wallclock, active FROM linear_worker_guard_heartbeat WHERE profile=? AND workspace=?", (self.profile, self.workspace)).fetchone()
        if row is None:
            return False
        path, version, pid, starttime, stamped, active = row
        try:
            age = wallclock - float(stamped)
            version_matches = type(version) is int and version == SCHEMA_VERSION
            ready_pid = int(pid)
        except (TypeError, ValueError, OverflowError):
            return False
        if not active or path != str(self.database) or not version_matches or not (0 <= age < self.ttl_seconds):
            return False
        try:
            return self.process_starttime(ready_pid) == str(starttime)
        except (RuntimeError, TypeError, ValueError, OverflowError):
            return False

    def close(self) -> None:
        with self._lifecycle_lock:
            self._closed = True
            with self._connect() as conn:
                conn.execute("UPDATE linear_worker_guard_heartbeat SET active=0 WHERE profile=? AND workspace=? AND instance_id=?", (self.profile, self.workspace, self._instance_id))
