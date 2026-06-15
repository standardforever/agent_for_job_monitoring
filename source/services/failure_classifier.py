from __future__ import annotations

from typing import Final


PAGE_LOAD_FAILED: Final = "page_load_failed"
LLM_FAILED: Final = "llm_failed"
PATTERN_FAILED: Final = "pattern_failed"
SELENIUM_FAILED: Final = "selenium_failed"
TIMEOUT: Final = "timeout"
NO_JOBS_FOUND: Final = "no_jobs_found"
NOT_JOB_RELATED: Final = "not_job_related"
CONFIG_FAILED: Final = "config_failed"
QUEUE_FAILED: Final = "queue_failed"
UNKNOWN_FAILED: Final = "unknown_failed"


def classify_failure(error: str | None) -> str:
    normalized = str(error or "").lower()
    if not normalized:
        return UNKNOWN_FAILED
    if "no selenium session slot" in normalized or "selenium" in normalized or "playwright" in normalized:
        return SELENIUM_FAILED
    if "timeout" in normalized or "timed out" in normalized or "navigation_timeout" in normalized:
        return TIMEOUT
    if "navigation" in normalized or "page load" in normalized or "net::" in normalized:
        return PAGE_LOAD_FAILED
    if "openai" in normalized or "api_key" in normalized or "model" in normalized or "llm" in normalized:
        return LLM_FAILED
    if "pattern" in normalized:
        return PATTERN_FAILED
    if "no job" in normalized or "no active job listing" in normalized or "no current vacanc" in normalized:
        return NO_JOBS_FOUND
    if "not job related" in normalized:
        return NOT_JOB_RELATED
    if "client" in normalized or "config" in normalized:
        return CONFIG_FAILED
    if "maximum attempts" in normalized or "queue" in normalized:
        return QUEUE_FAILED
    return UNKNOWN_FAILED
