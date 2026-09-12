"""Read-only operator inspection; never constructs or starts a Linear worker.

This command deliberately cannot resume work, clear Stop, or replay effects.
A snapshot is evidence for reconciliation, not execution authorization.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import stat
import time
from pathlib import Path

try:
    from .linear_guard_health import SCHEMA_VERSION, WorkerGuardHealth
except ImportError:  # direct script invocation
    from linear_guard_health import SCHEMA_VERSION, WorkerGuardHealth


def _guard_snapshot(conn, tables, database, profile, workspace):
    """Observe an existing heartbeat without constructing/migrating its writer."""
    report = {"ready": False, "reason": "missing_heartbeat", "expected_capability": SCHEMA_VERSION}
    if "linear_worker_guard_heartbeat" not in tables:
        return report
    row = conn.execute(
        "SELECT database_path, schema_version, pid, process_starttime, wallclock, active "
        "FROM linear_worker_guard_heartbeat WHERE profile=? AND workspace=?",
        (profile, workspace),
    ).fetchone()
    if row is None:
        return report
    path, version, pid, starttime, stamped, active = row
    if type(version) is not int or version != SCHEMA_VERSION:
        report["reason"] = "capability_mismatch"
    elif path != str(database):
        report["reason"] = "database_mismatch"
    elif active != 1:
        report["reason"] = "inactive_worker"
    elif not isinstance(stamped, (int, float)) or not 0 <= time.time() - stamped < 30.0:
        report["reason"] = "stale_heartbeat"
    elif type(pid) is not int or pid <= 0 or not isinstance(starttime, str):
        report["reason"] = "invalid_process_identity"
    else:
        try:
            matches = WorkerGuardHealth.process_starttime(pid) == starttime
        except RuntimeError:
            matches = False
        report["ready"] = matches
        report["reason"] = "ready" if matches else "process_mismatch"
    return report


class InspectionError(RuntimeError):
    """State cannot safely be inspected."""


def inspect_state(database: Path, *, profile: str, workspace: str, limit: int = 100) -> dict:
    if not profile or not workspace or type(limit) is not int or not 1 <= limit <= 1000:
        raise InspectionError("profile, workspace and a limit between 1 and 1000 are required")
    database = Path(database).absolute()
    try:
        if database.resolve(strict=True) != database:
            raise InspectionError("state path must not contain symlinks")
        parent_info = database.parent.lstat()
        database_info = database.lstat()
        if (
            not stat.S_ISDIR(parent_info.st_mode)
            or parent_info.st_uid != os.getuid()
            or stat.S_IMODE(parent_info.st_mode) != 0o700
        ):
            raise InspectionError("state directory must be a user-owned private 0700 directory")
        if not stat.S_ISREG(database_info.st_mode):
            raise InspectionError("state database must be a regular file")
        if database_info.st_uid != os.getuid() or stat.S_IMODE(database_info.st_mode) not in (0o600, 0o644):
            raise InspectionError("state database must be a user-owned 0600 or contained 0644 regular file")
    except OSError as exc:
        raise InspectionError("existing private state database is required") from exc

    # mode=ro honors WAL; immutable=1 would silently omit live WAL changes.
    try:
        conn = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=1)
        try:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            required = {"issue_ownership_meta", "issue_ownership", "deliveries", "outbox", "session_controls"}
            if not required.issubset(tables):
                raise InspectionError("incomplete worker state; no migration or worker startup was attempted")
            version = conn.execute("SELECT schema_version FROM issue_ownership_meta WHERE singleton=1").fetchone()
            if version is None or type(version[0]) is not int or version[0] != 1:
                raise InspectionError("unsupported ownership schema")
            for table in ("issue_ownership", "sessions", "session_controls", "linear_worker_guard_heartbeat"):
                if table in tables and conn.execute(
                    f"SELECT 1 FROM {table} WHERE profile != ? OR workspace != ? LIMIT 1", (profile, workspace)
                ).fetchone():
                    raise InspectionError("database contains a different profile or workspace")
            def counts(table: str) -> dict[str, int]:
                return dict(conn.execute(f"SELECT state, COUNT(*) FROM {table} GROUP BY state ORDER BY state"))
            rows = conn.execute(
                "SELECT issue_id, mode, updated_at FROM issue_ownership "
                "WHERE profile=? AND workspace=? ORDER BY issue_id LIMIT ?", (profile, workspace, limit + 1)
            ).fetchall()
            interrupted = [
                dict(zip(("delivery_id", "issue_id", "state"), row))
                for row in conn.execute(
                    "SELECT delivery_id, issue_id, state FROM deliveries "
                    "WHERE issue_id IS NOT NULL AND state NOT IN ('completed','failed','canceled','rejected','ignored') "
                    "ORDER BY delivery_id LIMIT ?", (limit,)
                )
            ]
            ambiguous_effects = [
                dict(zip(("delivery_id", "kind"), row))
                for row in conn.execute(
                    "SELECT delivery_id, kind FROM outbox WHERE state='ambiguous' "
                    "ORDER BY delivery_id, kind LIMIT ?", (limit,)
                )
            ]
            return {
                "profile": profile, "workspace": workspace, "scope": "profile-local database",
                "read_only": True, "can_resume": False,
                "guard": _guard_snapshot(conn, tables, database, profile, workspace),
                "deliveries": counts("deliveries"), "outbox": counts("outbox"),
                "stop_controls": conn.execute("SELECT COUNT(*) FROM session_controls").fetchone()[0],
                "ownership": [dict(zip(("issue_id", "mode", "updated_at"), row)) for row in rows[:limit]],
                "ownership_has_more": len(rows) > limit,
                "interrupted": interrupted,
                "ambiguous_effects": ambiguous_effects,
                "next_action": "Reconcile exact external effects and obtain executor-stop evidence; never clear fences based on age.",
            }
        finally:
            conn.close()
    except sqlite3.Error as exc:
        raise InspectionError("state unavailable or incompatible; no repair attempted") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--check", action="store_true", help="Exit 1 unless the local guarded worker heartbeat is ready; not fleet or execution acceptance")
    args = parser.parse_args(argv)
    try:
        report = inspect_state(args.database, profile=args.profile, workspace=args.workspace, limit=args.limit)
    except InspectionError as exc:
        parser.exit(2, f"linear inspection refused: {exc}\n")
    print(json.dumps(report, sort_keys=True))
    return 1 if args.check and not report["guard"]["ready"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
