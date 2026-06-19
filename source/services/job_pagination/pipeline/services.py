from __future__ import annotations

import asyncio
from typing import Any

from ..extraction.extractor import JobExtractionContext
from ..infinite_scroll.detector import detect_infinite_scroll
from ..infinite_scroll.executor import execute_infinite_scroll
from ..pagination.browser import light_lazy_scroll
from ..pagination.discovery import discover_bottom_continuation, discover_pagination, has_executable_pagination
from ..pagination.executor import execute_pagination
from ..pipeline.context import PipelineContext, StageTimer
from ..pipeline.state import add_signature_hash, capture_page_state
from ..validation.jobs import summarize_job_validation
from services.navigation import navigate_to_url
from utils.logging import get_logger, log_event


logger = get_logger("job_pagination_pipeline_services")


async def _safe_page_state(page, *, stage: str, job_selector: str | None = None) -> dict[str, Any]:
    try:
        return add_signature_hash(await capture_page_state(page, stage=stage, job_selector=job_selector))
    except Exception as exc:
        return {
            "stage": stage,
            "url": getattr(page, "url", ""),
            "job_selector": job_selector,
            "snapshot_error": str(exc),
        }


class PageLoadService:
    async def load(self, page, page_url: str, context: PipelineContext) -> None:
        timer = StageTimer("initial_page_load")
        log_event(logger, "info", "job_pagination_page_load_started", domain="job_pagination", page_url=page_url)
        navigation = await navigate_to_url(
            page,
            agent_index=0,
            tab_handle=None,
            url=page_url,
            post_navigation_delay_ms=1500,
        )
        if navigation.get("status") != "navigated":
            context.stop_reason = "navigation_failed"
            context.add_error("initial_page_load", navigation.get("error") or navigation.get("status"), url=page_url)
            context.add_decision("initial_page_load", "failed", "navigation_failed", navigation=navigation)
            raise RuntimeError(navigation.get("error") or navigation.get("status") or "Pagination page navigation failed")
        await asyncio.sleep(2)
        await light_lazy_scroll(page, steps=2)
        state = await _safe_page_state(page, stage="initial_page_load")
        context.add_page_state(state)
        context.add_stage(timer.result(data={"url": page.url, "page_state": state}))
        log_event(logger, "info", "job_pagination_page_load_completed", domain="job_pagination", page_url=page_url, final_url=page.url)


class JobExtractionService:
    def __init__(self, extractor: JobExtractionContext | None = None) -> None:
        self.extractor = extractor or JobExtractionContext()

    async def discover(self, page, context: PipelineContext) -> dict[str, Any]:
        timer = StageTimer("job_pattern_discovery")
        log_event(logger, "info", "job_pagination_job_pattern_discovery_started", domain="job_pagination", page_url=page.url)
        result = await self.extractor.discover(page, context)
        state = await _safe_page_state(page, stage="job_pattern_discovery", job_selector=self.extractor.job_container_selector)
        context.add_page_state(state)
        context.artifacts["job_pattern"] = result.get("pattern") or {}
        context.page_reports.append(
            {
                "page_index": 1,
                "url": page.url,
                "new_jobs": result.get("new_jobs", 0),
                "total_jobs": len(context.jobs),
                "source": "job_pattern_discovery",
                "validation": result.get("validation"),
            }
        )
        log_event(
            logger,
            "info",
            "job_pagination_initial_jobs_extracted",
            domain="job_pagination",
            page_url=page.url,
            new_jobs=result.get("new_jobs", 0),
            total_jobs=len(context.jobs),
        )
        context.add_stage(
            timer.result(
                data={
                    "new_jobs": result.get("new_jobs", 0),
                    "total_jobs": len(context.jobs),
                    "validation": result.get("validation"),
                    "page_state": state,
                }
            )
        )
        return result


class PaginationDiscoveryService:
    async def discover(self, page, context: PipelineContext, stage_name: str = "pagination_discovery") -> dict[str, Any]:
        timer = StageTimer(stage_name)
        pagination_result = await discover_pagination(page)
        state = await _safe_page_state(page, stage=stage_name)
        context.add_page_state(state)
        context.pagination_discoveries.append(pagination_result)
        context.artifacts.setdefault("pagination_patterns", []).append(
            {"stage": stage_name, "pattern": pagination_result}
        )
        context.add_stage(
            timer.result(
                success=not bool(pagination_result.get("error")),
                data={
                    "has_pagination": pagination_result.get("has_pagination"),
                    "method": (pagination_result.get("navigation") or {}).get("method"),
                    "confidence": pagination_result.get("confidence"),
                    "pagination": pagination_result,
                    "page_state": state,
                },
                errors=[{"detail": pagination_result["error"]}] if pagination_result.get("error") else [],
            )
        )
        return pagination_result

    def has_executable_plan(self, pagination_result: dict[str, Any]) -> bool:
        return has_executable_pagination(pagination_result)


class PaginationExecutionService:
    async def execute(
        self,
        page,
        pagination_result: dict[str, Any],
        extractor: JobExtractionContext,
        context: PipelineContext,
        stage_name: str = "pagination_execution",
    ) -> dict[str, Any]:
        timer = StageTimer(stage_name)
        before_state = await _safe_page_state(page, stage=f"{stage_name}_before", job_selector=extractor.job_container_selector)
        context.add_page_state(before_state)
        result = await execute_pagination(page, pagination_result, extractor, context)
        self._promote_repaired_navigation(context, result)
        after_state = await _safe_page_state(page, stage=f"{stage_name}_after", job_selector=extractor.job_container_selector)
        context.add_page_state(after_state)
        context.stop_reason = result.get("stop_reason") or "pagination_completed"
        context.add_stage(
            timer.result(
                data={**result, "before_state": before_state, "after_state": after_state},
                success=result.get("stop_reason") != "navigation_failed",
            )
        )
        return result

    def _promote_repaired_navigation(self, context: PipelineContext, result: dict[str, Any]) -> None:
        if not result.get("repaired_navigation"):
            return
        final_navigation = result.get("final_navigation")
        if not isinstance(final_navigation, dict) or not context.pagination_discoveries:
            return
        current = context.pagination_discoveries[-1]
        current["navigation"] = final_navigation
        current["repaired_navigation"] = True
        current["repair_history"] = result.get("repairs") or []


class InfiniteScrollService:
    async def detect(
        self,
        page,
        extractor: JobExtractionContext,
        context: PipelineContext,
        stage_name: str = "infinite_scroll_detection",
    ) -> dict[str, Any]:
        timer = StageTimer(stage_name)
        before_state = await _safe_page_state(page, stage=f"{stage_name}_before", job_selector=extractor.job_container_selector)
        context.add_page_state(before_state)
        result = await detect_infinite_scroll(page, extractor, context)
        after_state = await _safe_page_state(page, stage=f"{stage_name}_after", job_selector=extractor.job_container_selector)
        context.add_page_state(after_state)
        context.add_stage(timer.result(data={**result, "before_state": before_state, "after_state": after_state}, success=True))
        return result

    async def execute(
        self,
        page,
        extractor: JobExtractionContext,
        context: PipelineContext,
        stage_name: str = "infinite_scroll_execution",
    ) -> dict[str, Any]:
        timer = StageTimer(stage_name)
        before_state = await _safe_page_state(page, stage=f"{stage_name}_before", job_selector=extractor.job_container_selector)
        context.add_page_state(before_state)
        result = await execute_infinite_scroll(page, extractor, context)
        after_state = await _safe_page_state(page, stage=f"{stage_name}_after", job_selector=extractor.job_container_selector)
        context.add_page_state(after_state)
        context.add_stage(timer.result(data={**result, "before_state": before_state, "after_state": after_state}, success=True))
        context.stop_reason = result.get("stop_reason") or "infinite_scroll_completed"
        return result


class BottomContinuationService:
    async def discover(
        self,
        page,
        extractor: JobExtractionContext,
        context: PipelineContext,
        stage_prefix: str = "",
    ) -> dict[str, Any]:
        stage_name = f"{stage_prefix}_bottom_continuation_discovery" if stage_prefix else "bottom_continuation_discovery"
        timer = StageTimer(stage_name)
        result = await discover_bottom_continuation(page, extractor.job_container_selector)
        state = await _safe_page_state(page, stage=stage_name, job_selector=extractor.job_container_selector)
        context.add_page_state(state)
        context.pagination_discoveries.append(result)
        context.artifacts.setdefault("pagination_patterns", []).append(
            {"stage": stage_name, "pattern": result}
        )
        context.add_stage(
            timer.result(
                success=not bool(result.get("error")),
                data={
                    "has_pagination": result.get("has_pagination"),
                    "method": (result.get("navigation") or {}).get("method"),
                    "confidence": result.get("confidence"),
                    "bottom_context": result.get("bottom_context"),
                    "pagination": result,
                    "page_state": state,
                },
                errors=[{"detail": result["error"]}] if result.get("error") else [],
            )
        )
        return result


class ValidationService:
    def validate(self, context: PipelineContext) -> dict[str, Any]:
        timer = StageTimer("validation")
        validation = summarize_job_validation(context.jobs, context.page_reports)
        context.add_stage(timer.result(data=validation, success=validation["has_jobs"]))
        return validation
