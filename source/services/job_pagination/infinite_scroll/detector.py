from __future__ import annotations

from typing import Any

from ..extraction.extractor import JobExtractionContext
from ..pagination.browser import gradual_scroll_probe_for_more_content
from ..pagination.llm import analyse_url_pattern_from_click
from ..pagination.state import url_plan_after_observed_move
from ..pipeline.context import PipelineContext
from utils.logging import get_logger, log_event


logger = get_logger("job_pagination_infinite_scroll_detector")


async def detect_infinite_scroll(
    page,
    extractor: JobExtractionContext,
    context: PipelineContext,
    attempts: int = 3,
) -> dict[str, Any]:
    probes: list[dict[str, Any]] = []
    height_growth_count = 0
    job_growth_count = 0
    url_pattern: dict[str, Any] | None = None

    log_event(logger, "info", "infinite_scroll_detection_started", domain="job_pagination", page_url=page.url, attempts=attempts)
    for attempt in range(1, attempts + 1):
        before_url = page.url
        before_total_jobs = len(context.jobs)
        probe = await gradual_scroll_probe_for_more_content(page)
        extraction = await extractor.extract_current_page(page, context, f"infinite_detection_{attempt}")
        new_jobs = int(extraction.get("new_jobs") or 0)
        height_increased = bool(probe.get("height_increased"))
        url_changed = page.url != before_url

        if height_increased:
            height_growth_count += 1
        if len(context.jobs) > before_total_jobs or new_jobs > 0:
            job_growth_count += 1
        if url_changed and url_pattern is None:
            pattern = analyse_url_pattern_from_click(before_url, page.url)
            if pattern.get("can_use_url") and pattern.get("url"):
                pattern["url"] = url_plan_after_observed_move(pattern["url"])
                url_pattern = pattern

        report = {
            "attempt": attempt,
            "before_height": probe.get("before_height"),
            "after_height": probe.get("after_height"),
            "height_increased": height_increased,
            "url_before": before_url,
            "url_after": page.url,
            "url_changed": url_changed,
            "new_jobs": new_jobs,
            "total_jobs": len(context.jobs),
        }
        probes.append(report)
        log_event(
            logger,
            "info",
            "infinite_scroll_probe_completed",
            domain="job_pagination",
            page_url=page.url,
            attempt=attempt,
            before_height=report["before_height"],
            after_height=report["after_height"],
            height_increased=height_increased,
            new_jobs=new_jobs,
            total_jobs=len(context.jobs),
        )

    is_infinite = height_growth_count >= 2 and job_growth_count >= 1
    result = {
        "is_infinite": is_infinite,
        "height_growth_count": height_growth_count,
        "job_growth_count": job_growth_count,
        "url_pattern": url_pattern,
        "probes": probes,
    }
    log_event(
        logger,
        "info",
        "infinite_scroll_detection_completed",
        domain="job_pagination",
        page_url=page.url,
        is_infinite=is_infinite,
        height_growth_count=height_growth_count,
        job_growth_count=job_growth_count,
    )
    return result
