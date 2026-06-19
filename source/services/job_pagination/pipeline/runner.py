from __future__ import annotations

from typing import Any

from playwright.async_api import async_playwright
from utils.logging import get_logger, log_event

from ..extraction.extractor import JobExtractionContext
from ..pipeline.context import PipelineContext
from ..pipeline.policy import should_probe_infinite_after_pagination
from ..pipeline.persistence import ArtifactStore
from ..pipeline.services import (
    BottomContinuationService,
    InfiniteScrollService,
    JobExtractionService,
    PageLoadService,
    PaginationDiscoveryService,
    PaginationExecutionService,
    ValidationService,
)


logger = get_logger("job_pagination_pipeline")


class PipelineOrchestrator:
    def __init__(
        self,
        *,
        page_loader: PageLoadService | None = None,
        extraction: JobExtractionService | None = None,
        pagination_discovery: PaginationDiscoveryService | None = None,
        pagination_execution: PaginationExecutionService | None = None,
        infinite_scroll: InfiniteScrollService | None = None,
        bottom_continuation: BottomContinuationService | None = None,
        validation: ValidationService | None = None,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self.page_loader = page_loader or PageLoadService()
        self.extraction = extraction or JobExtractionService()
        self.pagination_discovery = pagination_discovery or PaginationDiscoveryService()
        self.pagination_execution = pagination_execution or PaginationExecutionService()
        self.infinite_scroll = infinite_scroll or InfiniteScrollService()
        self.bottom_continuation = bottom_continuation or BottomContinuationService()
        self.validation = validation or ValidationService()
        self.artifact_store = artifact_store or ArtifactStore()

    @property
    def extractor(self):
        return self.extraction.extractor

    async def run(
        self,
        cdp_url: str,
        page_url: str,
        *,
        job_pattern: dict[str, Any] | None = None,
        pagination_plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = PipelineContext(source_url=page_url)
        extraction = JobExtractionService(JobExtractionContext(job_pattern)) if job_pattern else self.extraction
        pagination_result: dict[str, Any] = {}
        run_dir = self.artifact_store.create_run_dir(page_url)
        context.artifacts["run_dir"] = str(run_dir)
        validation: dict[str, Any] = {}

        async with async_playwright() as pw:
            browser = None
            try:
                browser = await pw.chromium.connect_over_cdp(cdp_url)
                browser_context = browser.contexts[0] if browser.contexts else await browser.new_context()
                page = await browser_context.new_page()
                await self.page_loader.load(page, page_url, context)
                await extraction.discover(page, context)

                existing_execution = await self._try_existing_pagination_plan(
                    page,
                    page_url,
                    extraction.extractor,
                    context,
                    pagination_plan or {},
                )
                if not self._existing_plan_finished(existing_execution):
                    pagination_result = await self._discover_and_execute_pagination(page, extraction.extractor, context)

                validation = self.validation.validate(context)
                context.stop_reason = context.stop_reason if context.stop_reason != "not_started" else "completed"
            except Exception as exc:
                context.stop_reason = "pipeline_error"
                context.add_error("pipeline", str(exc))
                context.add_decision("pipeline", "failed", "unhandled_exception", error=str(exc))
                log_event(
                    logger,
                    "exception",
                    "job_pagination_pipeline_failed url=%s error=%s",
                    page_url,
                    str(exc),
                    domain="job_pagination",
                    page_url=page_url,
                    error=str(exc),
                    exc_info=True,
                )
                raise
            finally:
                self._persist_artifacts(run_dir, context, pagination_result, validation)
                if browser is not None:
                    await browser.close()

        return _pipeline_result(context, pagination_result, validation)

    def _persist_artifacts(
        self,
        run_dir,
        context: PipelineContext,
        pagination_result: dict[str, Any],
        validation: dict[str, Any],
    ) -> None:
        context.artifacts["job_pattern_path"] = str(
            self.artifact_store.write_json(run_dir, "patterns/job_pattern.json", context.artifacts.get("job_pattern") or {})
        )
        context.artifacts["pagination_patterns_path"] = str(
            self.artifact_store.write_json(
                run_dir,
                "patterns/pagination_patterns.json",
                context.artifacts.get("pagination_patterns") or context.pagination_discoveries,
            )
        )
        context.artifacts["page_states_path"] = str(
            self.artifact_store.write_json(run_dir, "evidence/page_states.json", context.page_states)
        )
        context.artifacts["decisions_path"] = str(
            self.artifact_store.write_json(run_dir, "evidence/decisions.json", context.decisions)
        )
        self.artifact_store.write_json(
            run_dir,
            "run_summary.json",
            _pipeline_result(context, pagination_result, validation or {}),
        )

    async def _run_infinite_scroll_path(self, page, extractor, context: PipelineContext, stage_prefix: str = "") -> None:
        detection_stage = f"{stage_prefix}_infinite_scroll_detection" if stage_prefix else "infinite_scroll_detection"
        execution_stage = f"{stage_prefix}_infinite_scroll_execution" if stage_prefix else "infinite_scroll_execution"
        post_pagination_stage = (
            f"{stage_prefix}_post_infinite_pagination_discovery"
            if stage_prefix
            else "post_infinite_pagination_discovery"
        )

        detection = await self.infinite_scroll.detect(page, extractor, context, stage_name=detection_stage)
        if await self._try_url_pattern_pagination(page, extractor, context, detection):
            return

        if not detection.get("is_infinite"):
            if _scroll_added_jobs(detection):
                if await self._try_bottom_continuation(page, extractor, context, stage_prefix):
                    return
            if context.stop_reason in ("not_started", "navigation_failed", "end_of_pagination"):
                context.stop_reason = "no_pagination_or_infinite_scroll"
            return

        infinite_result = await self.infinite_scroll.execute(page, extractor, context, stage_name=execution_stage)
        if await self._try_url_pattern_pagination(page, extractor, context, infinite_result):
            return

        if _scroll_added_jobs(infinite_result):
            if await self._try_bottom_continuation(page, extractor, context, stage_prefix):
                return

        pagination_result = await self.pagination_discovery.discover(page, context, stage_name=post_pagination_stage)
        if self.pagination_discovery.has_executable_plan(pagination_result):
            await self.pagination_execution.execute(page, pagination_result, extractor, context)

    async def _try_bottom_continuation(self, page, extractor, context: PipelineContext, stage_prefix: str) -> bool:
        bottom_result = await self.bottom_continuation.discover(page, extractor, context, stage_prefix)
        if not self.pagination_discovery.has_executable_plan(bottom_result):
            return False
        bottom_execution = await self.pagination_execution.execute(page, bottom_result, extractor, context)
        return int(bottom_execution.get("new_pages_extracted") or 0) > 0

    async def _try_url_pattern_pagination(self, page, extractor, context: PipelineContext, result: dict[str, Any]) -> bool:
        if not result.get("url_pattern"):
            return False
        url_pagination_result = _pagination_result_from_url_pattern(result["url_pattern"])
        url_execution = await self.pagination_execution.execute(page, url_pagination_result, extractor, context)
        return int(url_execution.get("new_pages_extracted") or 0) > 0


    async def _try_existing_pagination_plan(
        self,
        page,
        page_url: str,
        extractor,
        context: PipelineContext,
        pagination_plan: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not pagination_plan or not self.pagination_discovery.has_executable_plan(pagination_plan):
            return None
        context.pagination_discoveries.append({**pagination_plan, "source": "existing_saved_plan"})
        context.add_decision(
            "pagination",
            "use_existing_plan",
            "saved_pagination_plan_found",
            method=(pagination_plan.get("navigation") or {}).get("method"),
            pagination_type=pagination_plan.get("pagination_type"),
        )
        execution = await self.pagination_execution.execute(
            page,
            pagination_plan,
            extractor,
            context,
            stage_name="existing_pagination_execution",
        )
        if self._existing_plan_finished(execution):
            return execution
        context.add_decision(
            "pagination",
            "rediscover_after_existing_plan",
            execution.get("stop_reason") or "existing_plan_failed",
            execution=execution,
        )
        await self.page_loader.load(page, page_url, context)
        return execution

    def _existing_plan_finished(self, execution: dict[str, Any] | None) -> bool:
        if not execution:
            return False
        stop_reason = str(execution.get("stop_reason") or "")
        if stop_reason == "navigation_failed":
            return False
        if int(execution.get("successful_moves") or 0) > 0:
            return True
        if int(execution.get("new_pages_extracted") or 0) > 0:
            return True
        return stop_reason in {
            "end_of_pagination",
            "no_new_jobs",
            "completed_or_limit",
            "no_pagination_or_infinite_scroll",
        }

    async def _discover_and_execute_pagination(self, page, extractor, context: PipelineContext) -> dict[str, Any]:
        pagination_result = await self.pagination_discovery.discover(page, context)
        if self.pagination_discovery.has_executable_plan(pagination_result):
            pagination_execution = await self.pagination_execution.execute(
                page,
                pagination_result,
                extractor,
                context,
            )
            if should_probe_infinite_after_pagination(pagination_execution):
                await self._run_infinite_scroll_path(page, extractor, context, stage_prefix="post_pagination")
        else:
            await self._run_infinite_scroll_path(page, extractor, context)
        return pagination_result


async def run_pipeline(
    cdp_url: str,
    page_url: str,
    *,
    job_pattern: dict[str, Any] | None = None,
    pagination_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return await PipelineOrchestrator().run(
        cdp_url,
        page_url,
        job_pattern=job_pattern,
        pagination_plan=pagination_plan,
    )


def _scroll_added_jobs(result: dict[str, Any]) -> bool:
    if int(result.get("job_growth_count") or 0) > 0:
        return True
    if int(result.get("job_growth_rounds") or 0) > 0:
        return True
    records = result.get("probes") or result.get("rounds") or []
    return any(int(record.get("new_jobs") or 0) > 0 for record in records)


def _pagination_result_from_url_pattern(pattern: dict[str, Any]) -> dict[str, Any]:
    return {
        "has_pagination": True,
        "pagination_type": "url",
        "confidence": pattern.get("confidence", 0.8),
        "summary": "URL pagination inferred from URL changes during infinite-scroll probing.",
        "navigation": {
            "method": "url",
            "max_pages": 3,
            "url": pattern.get("url") or {},
            "click": {
                "next_selector": None,
                "disabled_selector": None,
                "wait_after_click_ms": 1500,
                "selector_candidates": [],
                "container_selector": None,
                "item_selector": None,
                "active_selector": None,
                "aria_label_template": None,
                "text_template": None,
                "current_page": None,
                "next_page": None,
                "increment": None,
            },
            "stop_when": [],
        },
        "notes": pattern.get("notes", ""),
    }


def _pipeline_result(context: PipelineContext, pagination_result: dict[str, Any], validation: dict[str, Any]) -> dict[str, Any]:
    return {
        "jobs": context.jobs,
        "pagination": context.pagination_discoveries[-1] if context.pagination_discoveries else pagination_result,
        "pagination_discoveries": context.pagination_discoveries,
        "pagination_runs": context.pagination_runs,
        "page_reports": context.page_reports,
        "navigation_errors": context.navigation_errors,
        "infinite_scroll_runs": context.infinite_scroll_runs,
        "page_states": context.page_states,
        "decisions": context.decisions,
        "artifacts": context.artifacts,
        "stages": [_stage_to_dict(stage) for stage in context.stages],
        "validation": validation,
        "stop_reason": context.stop_reason,
    }


def _stage_to_dict(stage) -> dict[str, Any]:
    return {
        "stage": stage.stage,
        "success": stage.success,
        "data": stage.data,
        "errors": stage.errors,
        "metrics": stage.metrics,
    }
