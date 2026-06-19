from __future__ import annotations

import copy
import re
from typing import Any

from ..pagination.job_bridge import JobPatternBridge, _job_key, _unique_jobs
from ..pagination.browser import wait_for_job_content
from ..pipeline.context import PipelineContext


class JobExtractionContext:
    def __init__(self, pattern: dict[str, Any] | None = None) -> None:
        self.pattern: dict[str, Any] = copy.deepcopy(pattern or {})
        self.bridge = JobPatternBridge(self.pattern or None)
        self.seed_result: dict[str, Any] = {}

    async def discover(self, page, context: PipelineContext) -> dict[str, Any]:
        if self.pattern:
            return await self._extract_with_existing_pattern(page, context)
        self.seed_result = await self.bridge.build_pattern(page, page.url)
        self.pattern = self.bridge.pattern or {}
        jobs = _unique_jobs(self.seed_result.get("jobs") or [])
        new_count = self.merge_jobs(context, jobs)
        return {
            "pattern": self.pattern,
            "jobs": jobs,
            "new_jobs": new_count,
            "validation": self.seed_result.get("validation"),
            "diagnostics": self.seed_result.get("diagnostics"),
        }

    async def _extract_with_existing_pattern(self, page, context: PipelineContext) -> dict[str, Any]:
        result = await self.bridge.extract_current_page(page, page.url, pattern_override=self._extraction_pattern())
        jobs = _unique_jobs(result.get("jobs") or [])
        new_count = self.merge_jobs(context, jobs)
        return {
            "pattern": self.pattern,
            "jobs": jobs,
            "new_jobs": new_count,
            "validation": result.get("validation"),
            "diagnostics": result.get("diagnostics"),
            "html_length": result.get("html_length"),
        }

    @property
    def job_container_selector(self) -> str | None:
        selector = self.pattern.get("job_container_selector")
        return _generalize_paginated_page_selector(str(selector).strip()) if selector else None

    async def extract_current_page(self, page, context: PipelineContext, source: str) -> dict[str, Any]:
        extraction_pattern = self._extraction_pattern()
        readiness = await wait_for_job_content(page, self.job_container_selector)
        result = await self.bridge.extract_current_page(page, page.url, pattern_override=extraction_pattern)
        jobs = _unique_jobs(result.get("jobs") or [])
        if not jobs and int(readiness.get("container_count") or 0) <= 0:
            readiness = await wait_for_job_content(page, self.job_container_selector, timeout_ms=18_000, settle_seconds=2.0)
            result = await self.bridge.extract_current_page(page, page.url, pattern_override=extraction_pattern)
            jobs = _unique_jobs(result.get("jobs") or [])
        new_count = self.merge_jobs(context, jobs)
        result["jobs"] = jobs
        result["new_jobs"] = new_count
        result["readiness"] = readiness
        context.page_reports.append(
            {
                "page_index": len(context.page_reports) + 1,
                "url": page.url,
                "new_jobs": new_count,
                "total_jobs": len(context.jobs),
                "source": source,
                "validation": result.get("validation"),
                "readiness": readiness,
            }
        )
        return result

    def _extraction_pattern(self) -> dict[str, Any]:
        pattern = copy.deepcopy(self.pattern)
        selector = str(pattern.get("job_container_selector") or "").strip()
        generalized = _generalize_paginated_page_selector(selector)
        if generalized and generalized != selector:
            pattern["job_container_selector"] = generalized
        return pattern

    def merge_jobs(self, context: PipelineContext, jobs: list[dict[str, Any]]) -> int:
        new_count = 0
        for job in jobs:
            key = _job_key(job)
            if key and key not in context.seen_job_keys:
                context.seen_job_keys.add(key)
                context.jobs.append(job)
                new_count += 1
        return new_count


def _generalize_paginated_page_selector(selector: str) -> str:
    if not selector:
        return selector
    return re.sub(
        r"\[aria-label=(['\"])Page\s+\d+\1\]",
        r"[aria-label^=\1Page \1]",
        selector,
        flags=re.IGNORECASE,
    )
