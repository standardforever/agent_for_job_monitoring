from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from core.config import Settings, get_settings
from services.grid_session import (
    attach_playwright_to_cdp,
    close_browser_attachment,
    close_session_via_http_async,
    create_session_async,
)
from services.job_listing_pattern_store import next_pattern_version, pattern_signature
from services.job_pattern.job_main import main as generate_job_listing_pattern
from services.navigation import navigate_to_url
from services.openai_service import reset_openai_runtime_config, set_openai_runtime_config
from services.selenium_session_heartbeat import SeleniumSessionHeartbeat
from services.selenium_session_slot_service import get_selenium_session_slot_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("job_pattern_node_processor")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class NoJobPatternSessionSlotAvailable(RuntimeError):
    pass


class JobPatternNodeProcessor:
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

    def _claim_slot(self, task: dict[str, Any], process_id: str, worker_name: str, celery_task_id: str) -> dict[str, Any]:
        slot = get_selenium_session_slot_service().claim_slot(
            worker_name,
            celery_task_id,
            process_id=process_id,
            registered_domain=task["registered_domain"],
        )
        if not slot:
            raise NoJobPatternSessionSlotAvailable("No Selenium session slot is currently available")
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
                session = await self._create_selenium_session(slot)
                self._attach_session(slot, session.session_id)
                self._heartbeat(slot)
                browser_session = await self._attach_browser(session.cdp_url)
                self._heartbeat(slot)
                patterns = await self._generate_patterns(task, browser_session, slot)
                return self._result(task, patterns, time.monotonic() - started)
            finally:
                await close_browser_attachment(browser_session)
                await self._close_selenium_session(slot, session)

    async def _create_selenium_session(self, slot: dict[str, Any]):
        session = await create_session_async(grid_url=slot["grid_url"], reuse_existing=False)
        if not session or not session.cdp_url:
            raise NoJobPatternSessionSlotAvailable("Could not create Selenium session for job pattern node")
        return session

    def _attach_session(self, slot: dict[str, Any], session_id: str) -> None:
        get_selenium_session_slot_service().attach_session(slot["slot_id"], session_id)

    async def _attach_browser(self, cdp_url: str):
        browser_session = await attach_playwright_to_cdp(cdp_url, raise_on_failure=True)
        if browser_session is None:
            raise RuntimeError("Could not attach Playwright to Selenium session")
        return browser_session

    async def _generate_patterns(self, task: dict[str, Any], browser_session: Any, slot: dict[str, Any]) -> list[dict[str, Any]]:
        results = []
        for candidate in task.get("input", {}).get("job_listing_patterns") or []:
            results.append(await self._generate_pattern(candidate, browser_session, slot))
        return results

    async def _generate_pattern(self, candidate: dict[str, Any], browser_session: Any, slot: dict[str, Any]) -> dict[str, Any]:
        page_url = str(candidate.get("page_url") or "").strip()
        self._log_started(page_url)
        navigation = await navigate_to_url(
            browser_session.page,
            agent_index=int(slot.get("session_index") or 0),
            tab_handle=None,
            url=page_url,
            post_navigation_delay_ms=self._settings.post_navigation_delay_ms,
        )
        self._heartbeat(slot)
        if navigation.get("status") != "navigated":
            return {**candidate, "status": "pattern_generation_failed", "last_error": navigation.get("error") or navigation.get("status"), "generated_at": _now_iso()}
        pattern_result = await generate_job_listing_pattern(
            browser_session.page,
            url=page_url,
            example_jobs=candidate.get("example_jobs") or [],
            seed_failed_pattern=self._seed_failed_pattern(candidate),
            seed_extracted_jobs=candidate.get("jobs") or candidate.get("example_jobs") or [],
            seed_validation=candidate.get("validation") or None,
        )
        self._heartbeat(slot)
        status = pattern_result.get("status")
        validation = pattern_result.get("validation") or {}
        pattern = pattern_result.get("pattern")
        return {
            **candidate,
            "status": status,
            "pattern": pattern,
            "job_count": len(pattern_result.get("jobs") or []),
            "example_jobs": list(pattern_result.get("example_jobs") or candidate.get("example_jobs") or [])[:2],
            "validation": validation,
            "diagnostics": pattern_result.get("diagnostics"),
            "generated_at": pattern_result.get("generated_at") or _now_iso(),
            "last_validated_at": _now_iso(),
            "pattern_version": next_pattern_version(candidate),
            "pattern_signature": pattern_signature(pattern),
            "page_fingerprint": pattern_result.get("page_fingerprint"),
            "generation_attempts": len(pattern_result.get("attempts") or []),
            "regeneration_mode": candidate.get("regeneration_mode"),
            "repair_seed_used": bool(self._seed_failed_pattern(candidate)),
            "last_error": None if status == "pattern_ready" else self._validation_error(validation),
        }

    def _result(self, task: dict[str, Any], patterns: list[dict[str, Any]], duration_seconds: float) -> dict[str, Any]:
        status = "completed" if any(item.get("status") == "pattern_ready" for item in patterns) else "failed"
        return {
            "node": "job_pattern",
            "processor": "job_pattern_node",
            "domain": task.get("domain"),
            "registered_domain": task["registered_domain"],
            "status": status,
            "job_listing_patterns": patterns,
            "duration_seconds": round(duration_seconds, 3),
            "processed_at": _now_iso(),
        }

    def _validation_error(self, validation: dict[str, Any]) -> str:
        problems = [str(problem) for problem in validation.get("problems") or [] if str(problem).strip()]
        return " | ".join(problems) or "Pattern validation failed"

    def _seed_failed_pattern(self, candidate: dict[str, Any]) -> dict[str, Any] | None:
        pattern = candidate.get("pattern")
        if not isinstance(pattern, dict):
            return None
        if candidate.get("regeneration_mode") == "force":
            return None
        if candidate.get("status") in {"pattern_ready", "pagination_completed", "extraction_completed"}:
            return None
        return pattern

    async def _close_selenium_session(self, slot: dict[str, Any], session: Any) -> None:
        if session is not None:
            await close_session_via_http_async(slot["grid_url"], session.session_id)

    def _mark_slot_stale(self, slot: dict[str, Any], error: str) -> None:
        get_selenium_session_slot_service().mark_slot_stale(slot["slot_id"], error)

    def _release_slot(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().release_slot(slot["slot_id"])

    def _heartbeat(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().heartbeat_slot(slot["slot_id"])

    def _log_started(self, page_url: str) -> None:
        log_event(logger, "info", "job_pattern_generation_started", domain="job_pattern", page_url=page_url)

    def _log_processing_exception(self, task: dict[str, Any], slot: dict[str, Any], exc: Exception) -> None:
        log_event(
            logger,
            "exception",
            "job_pattern_processing_exception",
            domain="job_pattern",
            registered_domain=task["registered_domain"],
            selenium_node_id=slot.get("selenium_node_id"),
            slot_id=slot.get("slot_id"),
            worker_name=task.get("worker_name"),
            celery_task_id=task.get("celery_task_id"),
            error=str(exc),
            exc_info=True,
        )
