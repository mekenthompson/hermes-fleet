"""Durable, fail-closed execution budgets for autonomous retries."""
from __future__ import annotations

import math
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class IssueBudget:
    """One autonomous generation may consume at most these local execution resources."""

    max_attempts: int = 3
    max_seconds: float = 3_600.0
    max_cost: float = 1.0
    cost_per_attempt: float = 0.1

    def __post_init__(self) -> None:
        for name, value in (("max_attempts", self.max_attempts),):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name, value in (
            ("max_seconds", self.max_seconds),
            ("max_cost", self.max_cost),
            ("cost_per_attempt", self.cost_per_attempt),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a positive number")


@dataclass(frozen=True)
class BudgetAdmission:
    allowed: bool
    reason: str | None = None


class IssueBudgetLedger:
    """SQLite ledger; limits are local safety caps rather than billing truth."""

    def __init__(self, database: Path, budget: IssueBudget = IssueBudget()) -> None:
        self.database = database
        self.budget = budget
        conn = self._connect()
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS linear_issue_budgets (
                    issue_id TEXT PRIMARY KEY,
                    attempts INTEGER NOT NULL,
                    first_attempt_at REAL NOT NULL,
                    spent REAL NOT NULL
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS linear_generation_budgets (
                    issue_id TEXT NOT NULL,
                    generation_id TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    first_attempt_at REAL NOT NULL,
                    spent REAL NOT NULL,
                    PRIMARY KEY (issue_id, generation_id)
                )"""
            )
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database, isolation_level=None, timeout=5.0)
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def admit(
        self,
        issue_id: str | None,
        generation_id: str | None = None,
        *,
        now: float | None = None,
    ) -> BudgetAdmission:
        """Atomically reserve one attempt; unknown identity is refused."""
        if not isinstance(issue_id, str) or not issue_id:
            return BudgetAdmission(False, "issue_budget_identity_unavailable")
        if generation_id is not None and (not isinstance(generation_id, str) or not generation_id):
            return BudgetAdmission(False, "issue_budget_identity_unavailable")
        now = time.time() if now is None else now
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            return BudgetAdmission(False, "issue_time_budget_clock_regressed")
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            with closing(conn.cursor()) as cursor:
                if generation_id is None:
                    row = cursor.execute(
                        "SELECT attempts, first_attempt_at, spent FROM linear_issue_budgets WHERE issue_id = ?",
                        (issue_id,),
                    ).fetchone()
                else:
                    row = cursor.execute(
                        "SELECT attempts, first_attempt_at, spent FROM linear_generation_budgets "
                        "WHERE issue_id = ? AND generation_id = ?",
                        (issue_id, generation_id),
                    ).fetchone()
                attempts, first_attempt_at, spent = row if row else (0, now, 0.0)
                if now < first_attempt_at:
                    conn.execute("ROLLBACK")
                    return BudgetAdmission(False, "issue_time_budget_clock_regressed")
                if now - first_attempt_at >= self.budget.max_seconds:
                    conn.execute("ROLLBACK")
                    return BudgetAdmission(False, "issue_time_budget_exhausted")
                if attempts >= self.budget.max_attempts:
                    conn.execute("ROLLBACK")
                    return BudgetAdmission(False, "issue_attempt_budget_exhausted")
                if spent + self.budget.cost_per_attempt > self.budget.max_cost:
                    conn.execute("ROLLBACK")
                    return BudgetAdmission(False, "issue_monetary_budget_exhausted")
                if generation_id is None:
                    cursor.execute(
                        """INSERT INTO linear_issue_budgets(issue_id, attempts, first_attempt_at, spent)
                           VALUES (?, ?, ?, ?)
                           ON CONFLICT(issue_id) DO UPDATE SET attempts=excluded.attempts, spent=excluded.spent""",
                        (issue_id, attempts + 1, first_attempt_at, spent + self.budget.cost_per_attempt),
                    )
                else:
                    cursor.execute(
                        """INSERT INTO linear_generation_budgets(
                               issue_id, generation_id, attempts, first_attempt_at, spent)
                           VALUES (?, ?, ?, ?, ?)
                           ON CONFLICT(issue_id, generation_id) DO UPDATE SET
                               attempts=excluded.attempts, spent=excluded.spent""",
                        (
                            issue_id,
                            generation_id,
                            attempts + 1,
                            first_attempt_at,
                            spent + self.budget.cost_per_attempt,
                        ),
                    )
            conn.execute("COMMIT")
            return BudgetAdmission(True)
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            return BudgetAdmission(False, "issue_budget_unavailable")
        finally:
            conn.close()

    def snapshot(
        self, issue_id: str, generation_id: str | None = None
    ) -> tuple[int, float, float] | None:
        conn = self._connect()
        try:
            if generation_id is None:
                row = conn.execute(
                    "SELECT attempts, first_attempt_at, spent FROM linear_issue_budgets WHERE issue_id = ?",
                    (issue_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT attempts, first_attempt_at, spent FROM linear_generation_budgets "
                    "WHERE issue_id = ? AND generation_id = ?",
                    (issue_id, generation_id),
                ).fetchone()
        finally:
            conn.close()
        return tuple(row) if row else None
