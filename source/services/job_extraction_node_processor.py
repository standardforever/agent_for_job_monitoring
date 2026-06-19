from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from core.config import Settings, get_settings
from services.domain_job_service import get_domain_job_service
from services.grid_session import (
    attach_playwright_to_cdp,
    close_browser_attachment,
    close_session_via_http_async,
    create_session_async,
)
from services.job_listing_pattern_store import next_pattern_version, pattern_signature
from services.job_pattern.job_main import main as repair_job_listing_pattern
from services.job_pattern.utils.extraction import extract_jobs_with_diagnostics, validate_jobs
from services.job_pattern.utils.html_extraction import extract_clean_html
from services.navigation import navigate_to_url
from services.openai_service import reset_openai_runtime_config, set_openai_runtime_config
from services.selenium_session_heartbeat import SeleniumSessionHeartbeat
from services.selenium_session_slot_service import get_selenium_session_slot_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("job_extraction_node_processor")
NO_JOBS_TERMS = (
    "no current vacancies",
    "no vacancies",
    "no open positions",
    "no jobs available",
    "no roles available",
    "currently no opportunities",
    "currently no vacancies",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


class NoJobExtractionSessionSlotAvailable(RuntimeError):
    pass


class JobExtractionNodeProcessor:
    def __init__(self, mongodb: SyncMongoDBService | None = None, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._mongodb = mongodb or get_sync_mongodb_service()
        self._clients = self._mongodb.collection(self._settings.mongodb_clients_collection)
        self._processes = self._mongodb.collection(self._settings.mongodb_process_uploads_collection)

    def process(
        self,
        task: dict[str, Any],
        *,
        process_id: str,
        worker_name: str,
        celery_task_id: str,
    ) -> dict[str, Any]:
        slot = self._claim_slot(task, process_id, worker_name, celery_task_id)
        tokens = self._set_client_openai_config(process_id)
        try:
            return asyncio.run(self._process_async(task, slot))
        except Exception as exc:
            self._log_processing_exception(task, slot, exc)
            self._mark_slot_stale(slot, str(exc))
            raise
        finally:
            reset_openai_runtime_config(tokens)
            self._release_slot(slot)

    def _claim_slot(
        self,
        task: dict[str, Any],
        process_id: str,
        worker_name: str,
        celery_task_id: str,
    ) -> dict[str, Any]:
        slot = get_selenium_session_slot_service().claim_slot(
            worker_name,
            celery_task_id,
            process_id=process_id,
            registered_domain=task["registered_domain"],
        )
        if not slot:
            raise NoJobExtractionSessionSlotAvailable("No Selenium session slot is currently available")
        return slot

    def _set_client_openai_config(self, process_id: str | None):
        client = self._load_client_config(process_id)
        return set_openai_runtime_config(api_key=client.get("api_key"), model=client.get("model"))

    def _load_client_config(self, process_id: str | None) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id}, {"client.client_name": 1})
        client_name = str((process or {}).get("client", {}).get("client_name") or "").strip()
        client = self._clients.find_one({"client_name": client_name})
        if not client:
            raise ValueError(f"Client '{client_name}' was not found")
        return client

    async def _process_async(self, task: dict[str, Any], slot: dict[str, Any]) -> dict[str, Any]:
        started = time.monotonic()
        session = None
        browser_session = None
        with SeleniumSessionHeartbeat(slot["slot_id"]):
            try:
                self._log_session_create_started(task, slot)
                session = await self._create_selenium_session(slot)
                self._heartbeat(slot)
                browser_session = await self._attach_browser(session.cdp_url)
                self._heartbeat(slot)
                results = await self._run_patterns(task, browser_session, slot)
                return self._result(task, results, time.monotonic() - started)
            finally:
                await close_browser_attachment(browser_session)
                await self._close_selenium_session(slot, session)

    async def _create_selenium_session(self, slot: dict[str, Any]):
        session = await create_session_async(grid_url=slot["grid_url"], reuse_existing=False)
        if not session or not session.cdp_url:
            raise NoJobExtractionSessionSlotAvailable("Could not create Selenium session for job extraction node")
        return session

    async def _attach_browser(self, cdp_url: str):
        browser_session = await attach_playwright_to_cdp(cdp_url, raise_on_failure=True)
        if browser_session is None:
            raise RuntimeError("Could not attach Playwright to Selenium session")
        return browser_session

    async def _run_patterns(self, task: dict[str, Any], browser_session: Any, slot: dict[str, Any]) -> list[dict[str, Any]]:
        patterns = list(task.get("input", {}).get("job_listing_patterns") or [])
        results = []
        for pattern in patterns:
            results.append(await self._run_pattern(task, pattern, browser_session, slot))
        return results

    async def _run_pattern(
        self,
        task: dict[str, Any],
        pattern_entry: dict[str, Any],
        browser_session: Any,
        slot: dict[str, Any],
    ) -> dict[str, Any]:
        page_url = str(pattern_entry.get("page_url") or "").strip()
        page = browser_session.page
        self._log_started(task, page_url)
        navigation = await navigate_to_url(
            page,
            agent_index=int(slot.get("session_index") or 0),
            tab_handle=None,
            url=page_url,
            post_navigation_delay_ms=self._settings.post_navigation_delay_ms,
        )
        self._heartbeat(slot)
        if navigation.get("status") != "navigated":
            return self._failed_pattern(pattern_entry, navigation.get("error") or navigation.get("status"))
        html = await extract_clean_html(page)
        self._heartbeat(slot)
        extraction = extract_jobs_with_diagnostics(html, pattern_entry.get("pattern") or {}, base_url=page_url)
        validation = validate_jobs(extraction["jobs"], pattern_entry.get("pattern") or {}, extraction["diagnostics"])
        self._heartbeat(slot)
        if validation["valid"]:
            return self._completed_pattern(pattern_entry, extraction["jobs"], validation, extraction["diagnostics"])
        if await self._looks_like_no_jobs_page(page):
            return self._no_jobs_pattern(pattern_entry, validation, extraction["diagnostics"])
        repaired = await repair_job_listing_pattern(
            page,
            url=page_url,
            example_jobs=pattern_entry.get("example_jobs") or [],
            seed_failed_pattern=pattern_entry.get("pattern") or {},
            seed_extracted_jobs=extraction["jobs"],
            seed_validation=validation,
        )
        self._heartbeat(slot)
        if repaired.get("status") == "pattern_ready":
            return self._repaired_pattern(pattern_entry, repaired)
        return self._failed_pattern(pattern_entry, "Pattern repair failed", repaired)

    async def _looks_like_no_jobs_page(self, page: Any) -> bool:
        try:
            text = await page.locator("body").inner_text(timeout=2_000)
        except Exception:
            return False
        normalized = text.lower()
        return any(term in normalized for term in NO_JOBS_TERMS)

    def _completed_pattern(
        self,
        pattern_entry: dict[str, Any],
        jobs: list[dict[str, Any]],
        validation: dict[str, Any],
        diagnostics: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            **pattern_entry,
            "status": "extraction_completed",
            "jobs": self._jobs_with_source(pattern_entry, jobs),
            "job_count": len(jobs),
            "example_jobs": self._sample_jobs(jobs),
            "validation": validation,
            "diagnostics": diagnostics,
            "job_extraction_mode": pattern_entry.get("job_extraction_mode"),
            "last_extracted_at": _now_iso(),
        }

    def _repaired_pattern(self, pattern_entry: dict[str, Any], repaired: dict[str, Any]) -> dict[str, Any]:
        jobs = list(repaired.get("jobs") or [])
        pattern = repaired.get("pattern")
        return {
            **pattern_entry,
            "status": "pattern_repaired",
            "pattern": pattern,
            "jobs": self._jobs_with_source(pattern_entry, jobs),
            "job_count": len(jobs),
            "example_jobs": self._sample_jobs(jobs),
            "validation": repaired.get("validation"),
            "diagnostics": repaired.get("diagnostics"),
            "pattern_version": next_pattern_version(pattern_entry),
            "pattern_signature": pattern_signature(pattern),
            "page_fingerprint": repaired.get("page_fingerprint"),
            "last_validated_at": _now_iso(),
            "repair": {
                "status": "completed",
                "repaired_at": _now_iso(),
                "previous_pattern_signature": pattern_entry.get("pattern_signature"),
            },
            "job_extraction_mode": pattern_entry.get("job_extraction_mode"),
            "last_extracted_at": _now_iso(),
        }

    def _no_jobs_pattern(
        self,
        pattern_entry: dict[str, Any],
        validation: dict[str, Any],
        diagnostics: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            **pattern_entry,
            "status": "no_jobs_listed",
            "jobs": [],
            "job_count": 0,
            "validation": validation,
            "diagnostics": diagnostics,
            "inactive_reason": "Loaded page appears to have no current vacancies.",
            "job_extraction_mode": pattern_entry.get("job_extraction_mode"),
            "last_extracted_at": _now_iso(),
        }

    def _failed_pattern(
        self,
        pattern_entry: dict[str, Any],
        error: str,
        repair_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            **pattern_entry,
            "status": "extraction_failed",
            "jobs": [],
            "job_count": 0,
            "last_error": error,
            "repair": repair_result,
            "job_extraction_mode": pattern_entry.get("job_extraction_mode"),
            "last_extracted_at": _now_iso(),
        }

    def _jobs_with_source(self, pattern_entry: dict[str, Any], jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        source_url = pattern_entry.get("page_url")
        return [{**job, "source_url": source_url} for job in jobs if isinstance(job, dict)]

    def _sample_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{"title": job.get("job_title") or job.get("title"), "job_url": job.get("job_url")} for job in jobs[:2]]

    def _result(self, task: dict[str, Any], patterns: list[dict[str, Any]], duration_seconds: float) -> dict[str, Any]:
        all_jobs = [job for pattern in patterns for job in pattern.get("jobs") or []]
        job_storage = get_domain_job_service().upsert_jobs(task["registered_domain"], all_jobs, _now())
        status = self._overall_status(patterns)
        return {
            "node": "job_extraction",
            "processor": "job_extraction_node",
            "domain": task.get("domain"),
            "registered_domain": task["registered_domain"],
            "status": status,
            "job_listing_patterns": self._clean_patterns(patterns),
            "job_storage": job_storage,
            "duration_seconds": round(duration_seconds, 3),
            "processed_at": _now_iso(),
        }

    def _overall_status(self, patterns: list[dict[str, Any]]) -> str:
        if not patterns:
            return "failed"
        successful_statuses = {"extraction_completed", "pattern_repaired", "no_jobs_listed"}
        return "completed" if any(pattern.get("status") in successful_statuses for pattern in patterns) else "failed"

    def _clean_patterns(self, patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        cleaned = []
        for pattern in patterns:
            cleaned.append(
                {
                    "page_url": pattern.get("page_url"),
                    "status": pattern.get("status"),
                    "pattern": pattern.get("pattern"),
                    "job_count": pattern.get("job_count"),
                    "example_jobs": list(pattern.get("example_jobs") or [])[:2],
                    "validation": pattern.get("validation"),
                    "diagnostics": pattern.get("diagnostics"),
                    "repair": pattern.get("repair"),
                    "inactive_reason": pattern.get("inactive_reason"),
                    "last_error": pattern.get("last_error"),
                    "job_extraction_mode": pattern.get("job_extraction_mode"),
                    "generated_at": pattern.get("generated_at"),
                    "last_extracted_at": pattern.get("last_extracted_at"),
                }
            )
        return cleaned

    async def _close_selenium_session(self, slot: dict[str, Any], session: Any) -> None:
        if session is not None:
            await close_session_via_http_async(slot["grid_url"], session.session_id)

    def _mark_slot_stale(self, slot: dict[str, Any], error: str) -> None:
        get_selenium_session_slot_service().mark_slot_stale(slot["slot_id"], error)

    def _release_slot(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().release_slot(slot["slot_id"])

    def _heartbeat(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().heartbeat_slot(slot["slot_id"])

    def _log_started(self, task: dict[str, Any], page_url: str) -> None:
        log_event(
            logger,
            "info",
            "job_extraction_pattern_started",
            domain="job_extraction",
            registered_domain=task["registered_domain"],
            page_url=page_url,
        )

    def _log_session_create_started(self, task: dict[str, Any], slot: dict[str, Any]) -> None:
        log_event(
            logger,
            "info",
            "job_extraction_session_create_started",
            domain="job_extraction",
            registered_domain=task["registered_domain"],
            selenium_node_id=slot["selenium_node_id"],
            slot_id=slot["slot_id"],
        )

    def _log_processing_exception(self, task: dict[str, Any], slot: dict[str, Any], exc: Exception) -> None:
        log_event(
            logger,
            "exception",
            "job_extraction_processing_exception",
            domain="job_extraction",
            registered_domain=task["registered_domain"],
            selenium_node_id=slot.get("selenium_node_id"),
            slot_id=slot.get("slot_id"),
            worker_name=task.get("worker_name"),
            celery_task_id=task.get("celery_task_id"),
            error=str(exc),
            exc_info=True,
        )
