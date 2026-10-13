"""Operator view of held Linear work: what is stuck, why, and the one supported next step."""
from __future__ import annotations
import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any
from .store import ADVISORY, Store

RESOLVE = "Check the issue in Linear, then: hermes linear resolve {id} applied|not_applied --evidence \"<what you saw>\""

def _read(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    return db

def stuck(state: Path, ingress: Path, profile: str, now: float | None = None) -> list[dict[str, Any]]:
    """Every item holding an issue or a delivery, oldest first, each with its next supported action."""
    now, items = time.time() if now is None else now, []
    def add(issue: str, ref: str, since: float, why: str, action: str) -> None:
        items.append({"issue": issue or "-", "ref": ref, "age_s": max(0, int(now - since)), "why": why, "action": action})
    if state.is_file():
        with closing(_read(state)) as db:
            rows = db.execute("SELECT * FROM outbox WHERE state != 'sent' AND NOT " + ADVISORY + " ORDER BY rowid").fetchall()
            for row in rows:
                payload, since = json.loads(row["payload"]), float(json.loads(row["payload"]).get("enqueued_at") or row["next_at"])
                if Store.uncertain(row, payload):
                    held = sum(1 for r in rows if json.loads(r["payload"]).get("requires_status_id") == row["id"])
                    add(payload.get("issue_id", ""), row["id"], since, f"{row['kind']} write may or may not have "
                        f"reached Linear; it holds this issue" + (f" and {held} dependent write(s)" if held else ""),
                        RESOLVE.format(id=row["id"]))
                elif row["state"] == "failed" and not payload.get("reconciliation") and now - since < 7 * 86_400:
                    add(payload.get("issue_id", ""), row["id"], since, f"{row['kind']} write was not delivered"
                        + (f": {payload['authorization_error']}" if payload.get("authorization_error") else ""),
                        "Retried after the next successful write." if row["attempts"] else "Not retried: redo the step.")
            for row in db.execute("SELECT * FROM specialist_scope_fence"):
                add(row["issue_id"], "fence", row["fenced_at"], f"outside specialist scope: {row['reason']}",
                    "Permanent by policy; recover only through an approved procedure.")
            for row in db.execute("SELECT * FROM chat_admissions WHERE state IN ('claiming', 'ambiguous')"):
                add(row["issue_id"], row["request_id"], now, f"chat-to-worker admission is {row['state']}"
                    + (f" for task {row['task_id']}" if row["task_id"] else ""),
                    "Confirm the task and delegate in Linear and Kanban before new work on this issue.")
            for row in db.execute("SELECT issue_id, last_updated_at FROM work WHERE release_pending = 1"):
                add(row["issue_id"], "release", row["last_updated_at"] / 1000,
                    "chat release is waiting for Linear to read back no delegate", "Retried each tick; check Linear.")
            for row in db.execute("SELECT task_id, issue_id FROM retirement"):
                add(row["issue_id"], row["task_id"], now, f"waiting for Kanban task {row['task_id']} to exit",
                    "Stop the worker in Kanban if it is still running.")
    if ingress.is_file():
        with closing(_read(ingress)) as db:
            for row in db.execute("SELECT delivery_id, payload, received_at, status, attempts FROM deliveries "
                                  "WHERE profile = ? AND (status = 'failed' OR (status = 'pending' AND attempts > 0)) "
                                  "ORDER BY received_at", (profile,)):
                try:
                    event = json.loads(row["payload"])
                    issue = ((event.get("agentSession") or {}).get("issue") or {}).get("id") or (event.get("data") or {}).get("id")
                except (ValueError, TypeError, AttributeError):
                    issue = ""
                parked = row["status"] == "failed"
                add(issue or "", row["delivery_id"], row["received_at"],
                    f"Linear delivery {'parked' if parked else 'retrying'} after {row['attempts']} failed attempt(s)",
                    f"Fix the logged cause, then: hermes linear replay {row['delivery_id']}" if parked
                    else "Backing off; a restart retries it at once.")
    return sorted(items, key=lambda item: -item["age_s"])

def report(items: list[dict[str, Any]]) -> str:
    if not items:
        return "Linear: nothing is held."
    lines = [f"Linear: {len(items)} item(s) held."]
    for item in items:
        lines += [f"- {item['issue']} [{item['ref']}] {item['age_s'] / 3600:.1f}h: {item['why']}", f"  {item['action']}"]
    return "\n".join(lines)

def replay(ingress: Path, profile: str, delivery_id: str) -> bool:
    """Give a parked delivery one more try; if it fails again it parks again, without a second alert."""
    if not ingress.is_file():
        return False
    with closing(sqlite3.connect(ingress, timeout=10, isolation_level=None)) as db:
        return bool(db.execute("UPDATE deliveries SET status = 'pending', attempts = 0 WHERE profile = ? AND "
                               "delivery_id = ? AND status = 'failed'", (profile, delivery_id)).rowcount)

def setup(parser: Any) -> None:
    sub = parser.add_subparsers(dest="linear_command", required=True)
    sub.add_parser("status", help="List held Linear work and the next step").add_argument("--json", action="store_true")
    resolve = sub.add_parser("resolve", help="Record the verified outcome of an uncertain status or description write")
    resolve.add_argument("row_id")
    resolve.add_argument("outcome", choices=("applied", "not_applied"))
    resolve.add_argument("--evidence", required=True, help="What you checked in Linear")
    sub.add_parser("replay", help="Retry one parked Linear delivery").add_argument("delivery_id")

def run(args: Any, state: Path, ingress: Path, profile: str) -> int:
    if args.linear_command == "status":
        items = stuck(state, ingress, profile)
        print(json.dumps(items, indent=2) if args.json else report(items))
        return 1 if items else 0
    if args.linear_command == "resolve":
        done = state.is_file() and Store(state).reconcile_terminal(
            args.row_id, outcome=args.outcome, evidence=args.evidence, at=time.time())
        print(f"Recorded {args.outcome} for {args.row_id}; the issue is released." if done else
              f"{args.row_id} is not an uncertain status or description write; see: hermes linear status")
        return 0 if done else 1
    done = replay(ingress, profile, args.delivery_id)
    print(f"Delivery {args.delivery_id} will be retried on the next tick." if done else
          f"{args.delivery_id} is not a parked delivery for profile {profile}; see: hermes linear status")
    return 0 if done else 1
