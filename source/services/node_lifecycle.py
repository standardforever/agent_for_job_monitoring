from __future__ import annotations

from dataclasses import dataclass
from typing import Final


NOT_STARTED: Final = "not_started"
QUEUED: Final = "queued"
RUNNING: Final = "running"
COMPLETED: Final = "completed"
FAILED: Final = "failed"
PARTIAL_COMPLETED: Final = "partial_completed"
BLOCKED: Final = "blocked"

TERMINAL_STATUSES: Final = {COMPLETED, FAILED, PARTIAL_COMPLETED, BLOCKED}
ACTIVE_STATUSES: Final = {QUEUED, RUNNING}


@dataclass(frozen=True, slots=True)
class NodeRetryPolicy:
    max_attempts: int
    retry_countdown_seconds: int = 10


NODE_RETRY_POLICIES: Final = {
    "search": NodeRetryPolicy(max_attempts=2),
    "career_category": NodeRetryPolicy(max_attempts=2),
    "job_pattern": NodeRetryPolicy(max_attempts=1),
    "job_pagination": NodeRetryPolicy(max_attempts=2),
    "job_extraction": NodeRetryPolicy(max_attempts=3),
}


def retry_policy(node: str, default_max_attempts: int) -> NodeRetryPolicy:
    policy = NODE_RETRY_POLICIES.get(node)
    if policy:
        return policy
    return NodeRetryPolicy(max_attempts=default_max_attempts)


def terminal_status(*, completed: int, failed: int, blocked: int = 0) -> str:
    if completed and (failed or blocked):
        return PARTIAL_COMPLETED
    if completed:
        return COMPLETED
    if blocked and not failed:
        return BLOCKED
    return FAILED


def status_from_totals(totals: dict) -> str:
    if int(totals.get("running") or 0) > 0 or int(totals.get("queued") or 0) > 0:
        return RUNNING
    return terminal_status(
        completed=int(totals.get("completed") or 0),
        failed=int(totals.get("failed") or 0),
        blocked=int(totals.get("blocked") or 0),
    )
