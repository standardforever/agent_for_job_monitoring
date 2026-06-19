from __future__ import annotations

from typing import Any


CONFIRMED_END_REASONS = {
    "next_control_disabled",
    "next_control_hidden",
    "next_control_missing_after_success",
    "not_found_after_success",
    "repeated_url_after_success",
    "no_new_jobs",
}

RETRYABLE_PAGINATION_REASONS = {
    "navigation_failed",
    "pagination_end_unconfirmed",
    "click_fallback_missing",
}


def should_probe_infinite_after_pagination(result: dict[str, Any]) -> bool:
    stop_reason = str(result.get("stop_reason") or "")
    if stop_reason in CONFIRMED_END_REASONS:
        return False
    if int(result.get("new_pages_extracted") or 0) <= 0:
        return stop_reason in RETRYABLE_PAGINATION_REASONS
    return stop_reason in RETRYABLE_PAGINATION_REASONS


def classify_action_outcome(stop_reason: str, *, new_jobs: int = 0, url_changed: bool = False) -> str:
    if stop_reason in {"next_control_disabled", "next_control_hidden"}:
        return "confirmed_end"
    if stop_reason in {"navigation_failed", "pagination_end_unconfirmed", "click_fallback_missing"}:
        return "retryable_failure"
    if new_jobs > 0:
        return "advanced_with_new_jobs"
    if url_changed:
        return "advanced_without_new_jobs"
    if stop_reason == "no_new_jobs":
        return "same_jobs_stop"
    return stop_reason or "unknown"
