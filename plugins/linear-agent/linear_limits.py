"""Validated local bounds for one profile-local Linear worker."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LinearLimits:
    """Local admission and retry bounds; SQLite is not a fleet-wide fence."""

    max_agent_queue: int = 100
    max_session_queue: int = 10
    max_issue_queue: int = 10
    max_outbox_attempts: int = 3
    max_concurrent_jobs: int = 5

    def __post_init__(self) -> None:
        for name, value in (
            ("max_agent_queue", self.max_agent_queue),
            ("max_session_queue", self.max_session_queue),
            ("max_issue_queue", self.max_issue_queue),
            ("max_outbox_attempts", self.max_outbox_attempts),
            ("max_concurrent_jobs", self.max_concurrent_jobs),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
