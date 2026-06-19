from __future__ import annotations

from typing import Any


def summarize_job_validation(jobs: list[dict[str, Any]], page_reports: list[dict[str, Any]]) -> dict[str, Any]:
    pages_with_no_new_jobs = sum(1 for report in page_reports if int(report.get("new_jobs") or 0) <= 0)
    return {
        "total_jobs": len(jobs),
        "pages_seen": len(page_reports),
        "pages_with_no_new_jobs": pages_with_no_new_jobs,
        "has_jobs": bool(jobs),
    }
