from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any

from services.job_pattern.utils.extraction import extract_jobs_with_diagnostics, validate_jobs
from services.job_pattern.utils.html_extraction import extract_clean_html
from services.job_pattern.utils.patterns import (
    correct_pattern_with_llm,
    ensure_pattern_defaults,
    final_review_pattern_with_llm,
    generate_pattern_with_llm,
    prepare_html_for_llm,
)
from utils.logging import get_logger, log_event


logger = get_logger("job_pattern_main")


def _normalize_example_jobs(example_jobs: list[dict[str, Any]] | None) -> list[dict[str, str | None]]:
    normalized: list[dict[str, str | None]] = []
    for item in example_jobs or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("job_title") or "").strip()
        job_url = str(item.get("job_url") or "").strip() or None
        if title or job_url:
            normalized.append({"title": title, "job_url": job_url})
    return normalized[:20]


async def main(
    page: Any,
    max_html_chars: int = 0,
    url: str = "",
    example_jobs: list[dict[str, Any]] | None = None,
    seed_failed_pattern: dict[str, Any] | None = None,
    seed_extracted_jobs: list[dict[str, Any]] | None = None,
    seed_validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "job_pattern_generation_started url=%s",
        url,
        domain=url or "job_pattern",
        page_url=url,
        max_html_chars=max_html_chars,
        example_job_count=len(example_jobs or []),
    )

    try:
        html = await extract_clean_html(page)
        original_html_length = len(html)
        if max_html_chars and max_html_chars > 0:
            html = html[:max_html_chars]

        llm_html = prepare_html_for_llm(html)
        normalized_example_jobs = _normalize_example_jobs(example_jobs)
        attempts: list[dict[str, Any]] = []

        log_event(
            logger,
            "info",
            "job_pattern_html_prepared url=%s html_length=%s prepared_html_length=%s",
            url,
            len(html),
            len(llm_html),
            domain=url or "job_pattern",
            page_url=url,
            original_html_length=original_html_length,
            html_length=len(html),
            prepared_html_length=len(llm_html),
            example_job_count=len(normalized_example_jobs),
        )

        if seed_failed_pattern:
            pattern = ensure_pattern_defaults(
                await asyncio.to_thread(
                    correct_pattern_with_llm,
                    html=html,
                    page_url=url,
                    failed_pattern=seed_failed_pattern,
                    extracted_jobs=seed_extracted_jobs or [],
                    validation=seed_validation or {"valid": False, "problems": ["Seed pattern failed during rerun."]},
                    prepared_html=llm_html,
                    example_jobs=normalized_example_jobs,
                )
            )
            initial_stage = "seed_correction"
        else:
            pattern = ensure_pattern_defaults(
                await asyncio.to_thread(
                    generate_pattern_with_llm,
                    html,
                    page_url=url,
                    prepared_html=llm_html,
                    example_jobs=normalized_example_jobs,
                )
            )
            initial_stage = "discovery"

        extraction_result = extract_jobs_with_diagnostics(html, pattern, base_url=url)
        jobs = extraction_result["jobs"]
        diagnostics = extraction_result["diagnostics"]
        validation = validate_jobs(jobs, pattern, diagnostics=diagnostics)

        attempts.append(
            {
                "attempt": 1,
                "stage": initial_stage,
                "pattern": pattern,
                "jobs": jobs,
                "validation": validation,
            }
        )

        log_event(
            logger,
            "info",
            "job_pattern_attempt_completed url=%s attempt=%s stage=%s valid=%s job_count=%s",
            url,
            1,
            initial_stage,
            validation["valid"],
            validation["job_count"],
            domain=url or "job_pattern",
            page_url=url,
            attempt=1,
            stage=initial_stage,
            valid=validation["valid"],
            job_count=validation["job_count"],
            containers_seen=validation.get("containers_seen"),
            rejected_candidate_count=validation.get("rejected_candidate_count"),
            problems=validation.get("problems"),
        )

        if not validation["valid"]:
            log_event(
                logger,
                "info",
                "job_pattern_correction_started url=%s problems=%s",
                url,
                validation.get("problems"),
                domain=url or "job_pattern",
                page_url=url,
                attempt=2,
                previous_problems=validation.get("problems"),
            )
            pattern = ensure_pattern_defaults(
                await asyncio.to_thread(
                    correct_pattern_with_llm,
                    html=html,
                    page_url=url,
                    failed_pattern=pattern,
                    extracted_jobs=jobs,
                    validation=validation,
                    prepared_html=llm_html,
                    example_jobs=normalized_example_jobs,
                )
            )
            extraction_result = extract_jobs_with_diagnostics(html, pattern, base_url=url)
            jobs = extraction_result["jobs"]
            diagnostics = extraction_result["diagnostics"]
            validation = validate_jobs(jobs, pattern, diagnostics=diagnostics)

            attempts.append(
                {
                    "attempt": 2,
                    "stage": "correction",
                    "pattern": pattern,
                    "jobs": jobs,
                    "validation": validation,
                }
            )

            log_event(
                logger,
                "info",
                "job_pattern_attempt_completed url=%s attempt=%s stage=%s valid=%s job_count=%s",
                url,
                2,
                "correction",
                validation["valid"],
                validation["job_count"],
                domain=url or "job_pattern",
                page_url=url,
                attempt=2,
                stage="correction",
                valid=validation["valid"],
                job_count=validation["job_count"],
                containers_seen=validation.get("containers_seen"),
                rejected_candidate_count=validation.get("rejected_candidate_count"),
                problems=validation.get("problems"),
            )

        if not validation["valid"]:
            log_event(
                logger,
                "info",
                "job_pattern_final_review_started url=%s problems=%s",
                url,
                validation.get("problems"),
                domain=url or "job_pattern",
                page_url=url,
                attempt=3,
                previous_problems=validation.get("problems"),
            )
            pattern = ensure_pattern_defaults(
                await asyncio.to_thread(
                    final_review_pattern_with_llm,
                    html=html,
                    page_url=url,
                    failed_patterns=[attempt["pattern"] for attempt in attempts],
                    extracted_jobs=jobs,
                    validation=validation,
                    prepared_html=llm_html,
                    example_jobs=normalized_example_jobs,
                )
            )
            extraction_result = extract_jobs_with_diagnostics(html, pattern, base_url=url)
            jobs = extraction_result["jobs"]
            diagnostics = extraction_result["diagnostics"]
            validation = validate_jobs(jobs, pattern, diagnostics=diagnostics)

            attempts.append(
                {
                    "attempt": 3,
                    "stage": "final_review",
                    "pattern": pattern,
                    "jobs": jobs,
                    "validation": validation,
                }
            )

            log_event(
                logger,
                "info",
                "job_pattern_attempt_completed url=%s attempt=%s stage=%s valid=%s job_count=%s",
                url,
                3,
                "final_review",
                validation["valid"],
                validation["job_count"],
                domain=url or "job_pattern",
                page_url=url,
                attempt=3,
                stage="final_review",
                valid=validation["valid"],
                job_count=validation["job_count"],
                containers_seen=validation.get("containers_seen"),
                rejected_candidate_count=validation.get("rejected_candidate_count"),
                problems=validation.get("problems"),
            )

        status = "pattern_ready" if validation["valid"] else "pattern_validation_failed"
        log_event(
            logger,
            "info",
            "job_pattern_generation_completed url=%s status=%s attempts=%s job_count=%s",
            url,
            status,
            len(attempts),
            len(jobs),
            domain=url or "job_pattern",
            page_url=url,
            status=status,
            attempts=len(attempts),
            job_count=len(jobs),
            valid=validation["valid"],
            problems=validation.get("problems"),
        )

        return {
            "status": status,
            "page_url": url,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pattern": pattern,
            "jobs": jobs,
            "validation": validation,
            "diagnostics": diagnostics,
            "attempts": attempts,
            "html_length": len(html),
            "prepared_html_length": len(llm_html),
            "page_fingerprint": hashlib.sha256(llm_html.encode("utf-8")).hexdigest(),
            "example_jobs": normalized_example_jobs,
        }
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "job_pattern_generation_failed url=%s error=%s",
            url,
            str(exc),
            domain=url or "job_pattern",
            page_url=url,
            error=str(exc),
            exc_info=True,
        )
        raise
