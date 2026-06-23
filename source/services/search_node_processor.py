from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from core.config import get_settings
from nodes.url_extraction import career_url_extraction_node
from services.grid_session import (
    attach_playwright_to_cdp,
    close_browser_attachment,
    close_session_via_http_async,
    create_session_async,
)
from services.selenium_session_heartbeat import SeleniumSessionHeartbeat
from services.selenium_session_slot_service import get_selenium_session_slot_service
from services.process_runtime_service import get_process_runtime_service
from utils.logging import get_logger, log_event


logger = get_logger("search_node_processor")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class NoSearchNodeSessionSlotAvailable(RuntimeError):
    pass


class SearchNodeProcessor:
    def process(
        self,
        process_id: str,
        domain_ref: dict[str, Any],
        worker_name: str,
        task_id: str,
    ) -> dict[str, Any]:
        domain_ref = {**domain_ref, "dispatch_process_id": process_id}
        if self._has_supplied_career_url(domain_ref):
            return self._supplied_career_url_result(domain_ref)
        slot = self._claim_slot(process_id, domain_ref, worker_name, task_id)
        try:
            return asyncio.run(self._process_async(domain_ref, slot))
        except Exception as exc:
            self._log_processing_exception(domain_ref, slot, exc)
            self._mark_slot_stale(slot, str(exc))
            raise
        finally:
            self._release_slot(slot)

    def _claim_slot(
        self,
        process_id: str,
        domain_ref: dict[str, Any],
        worker_name: str,
        task_id: str,
    ) -> dict[str, Any]:
        slot = get_selenium_session_slot_service().claim_slot(
            worker_name,
            task_id,
            process_id=process_id,
            registered_domain=domain_ref["registered_domain"],
        )
        if not slot:
            raise NoSearchNodeSessionSlotAvailable("No Selenium session slot is currently available")
        return slot

    async def _process_async(self, domain_ref: dict[str, Any], slot: dict[str, Any]) -> dict[str, Any]:
        started = time.monotonic()
        session = None
        browser_session = None
        with SeleniumSessionHeartbeat(slot["slot_id"]):
            try:
                self._log_session_create_started(domain_ref, slot)
                self._progress(domain_ref, "creating_selenium_session")
                session = await self._create_selenium_session(slot)
                self._attach_session(slot, session.session_id)
                self._log_session_created(domain_ref, slot, session.session_id)
                self._heartbeat(slot)
                self._progress(domain_ref, "attaching_browser")
                browser_session = await self._attach_browser(session.cdp_url)
                self._log_browser_attached(domain_ref, session.session_id)
                self._heartbeat(slot)
                self._progress(domain_ref, "running_url_extraction", domain_ref.get("domain"))
                node_result = await self._run_search_node(domain_ref, browser_session, slot)
                self._heartbeat(slot)
                self._progress(domain_ref, "search_completed", domain_ref.get("domain"))
                return self._result(domain_ref, slot, node_result, time.monotonic() - started)
            finally:
                await close_browser_attachment(browser_session)
                await self._close_selenium_session(slot, session)

    async def _create_selenium_session(self, slot: dict[str, Any]):
        session = await create_session_async(grid_url=slot["grid_url"], reuse_existing=False)
        if not session or not session.cdp_url:
            raise NoSearchNodeSessionSlotAvailable("Could not create Selenium session for search node")
        return session

    def _attach_session(self, slot: dict[str, Any], session_id: str) -> None:
        get_selenium_session_slot_service().attach_session(slot["slot_id"], session_id)

    async def _attach_browser(self, cdp_url: str):
        browser_session = await attach_playwright_to_cdp(cdp_url, raise_on_failure=True)
        if browser_session is None:
            raise RuntimeError("Could not attach Playwright to Selenium session")
        return browser_session

    async def _run_search_node(self, domain_ref: dict[str, Any], browser_session: Any, slot: dict[str, Any]) -> dict[str, Any]:
        target = str(domain_ref.get("domain") or domain_ref["registered_domain"])
        self._log_started(domain_ref, target)
        return await career_url_extraction_node(
            target,
            browser_session,
            registered_domain=domain_ref["registered_domain"],
            heartbeat=lambda: self._heartbeat(slot),
            progress=lambda step, current_url=None: self._progress(domain_ref, step, current_url),
            step_timeout_seconds=get_settings().node_step_timeout_seconds,
        )

    async def _close_selenium_session(self, slot: dict[str, Any], session: Any) -> None:
        if session is None:
            return
        await close_session_via_http_async(slot["grid_url"], session.session_id)

    def _result(
        self,
        domain_ref: dict[str, Any],
        slot: dict[str, Any],
        node_result: dict[str, Any],
        duration_seconds: float,
    ) -> dict[str, Any]:
        career_urls = list(node_result.get("career_urls", []) or [])
        success = node_result.get("status") == "career_urls_found"
        return {
            "node": "search_engine",
            "processor": "search_node",
            "domain": domain_ref.get("domain"),
            "registered_domain": domain_ref["registered_domain"],
            "career_url": career_urls[0] if career_urls else None,
            "career_urls": career_urls,
            "non_domain_career_urls": node_result.get("non_domain_career_urls", []),
            "all_urls": node_result.get("all_urls", []),
            "diagnostics": node_result.get("diagnostics", {}),
            "source_type": "search_engine",
            "cache_scope": "shared_domain",
            "status": node_result.get("status"),
            "success": success,
            "error": node_result.get("error_message"),
            "jobs_found": 0,
            "duration_seconds": round(duration_seconds, 3),
            "selenium_node_id": slot["selenium_node_id"],
            "selenium_session_slot_id": slot["slot_id"],
            "session_index": slot["session_index"],
            "processed_at": _now_iso(),
        }

    def _log_started(self, domain_ref: dict[str, Any], target: str) -> None:
        log_event(
            logger,
            "info",
            "search_node_started",
            domain="search_node",
            registered_domain=domain_ref["registered_domain"],
            target=target,
        )

    def _log_session_create_started(self, domain_ref: dict[str, Any], slot: dict[str, Any]) -> None:
        log_event(
            logger,
            "info",
            "search_node_session_create_started",
            domain="search_node",
            registered_domain=domain_ref["registered_domain"],
            selenium_node_id=slot["selenium_node_id"],
            slot_id=slot["slot_id"],
        )

    def _log_session_created(self, domain_ref: dict[str, Any], slot: dict[str, Any], session_id: str) -> None:
        log_event(
            logger,
            "info",
            "search_node_session_created",
            domain="search_node",
            registered_domain=domain_ref["registered_domain"],
            selenium_node_id=slot["selenium_node_id"],
            slot_id=slot["slot_id"],
            selenium_session_id=session_id,
        )

    def _log_browser_attached(self, domain_ref: dict[str, Any], session_id: str) -> None:
        log_event(
            logger,
            "info",
            "search_node_browser_attached",
            domain="search_node",
            registered_domain=domain_ref["registered_domain"],
            selenium_session_id=session_id,
        )

    def _mark_slot_stale(self, slot: dict[str, Any], error: str) -> None:
        get_selenium_session_slot_service().mark_slot_stale(slot["slot_id"], error)

    def _release_slot(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().release_slot(slot["slot_id"])

    def _heartbeat(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().heartbeat_slot(slot["slot_id"])

    def _progress(self, domain_ref: dict[str, Any], step: str, current_url: str | None = None) -> None:
        process_id = str(domain_ref.get("dispatch_process_id") or "")
        if not process_id:
            return
        get_process_runtime_service().update_domain_progress(
            process_id,
            domain_ref["registered_domain"],
            step=step,
            current_url=current_url,
        )

    def _has_supplied_career_url(self, domain_ref: dict[str, Any]) -> bool:
        return bool(str(domain_ref.get("career_url") or "").strip())

    def _supplied_career_url_result(self, domain_ref: dict[str, Any]) -> dict[str, Any]:
        career_url = str(domain_ref.get("career_url") or "").strip()
        log_event(
            logger,
            "info",
            "search_node_career_url_supplied",
            domain="search_node",
            registered_domain=domain_ref["registered_domain"],
            career_url=career_url,
        )
        return {
            "node": "search_engine",
            "processor": "search_node",
            "domain": domain_ref.get("domain"),
            "registered_domain": domain_ref["registered_domain"],
            "career_url": career_url,
            "career_urls": [career_url],
            "non_domain_career_urls": [],
            "all_urls": [career_url],
            "diagnostics": {"source": "user_supplied_career_url"},
            "source_type": "process_supplied_career_url",
            "cache_scope": "process_only",
            "uses_process_supplied_career_url": True,
            "status": "career_url_supplied",
            "success": True,
            "error": None,
            "jobs_found": 0,
            "duration_seconds": 0,
            "selenium_node_id": None,
            "selenium_session_slot_id": None,
            "session_index": None,
            "processed_at": _now_iso(),
        }

    def _log_processing_exception(self, domain_ref: dict[str, Any], slot: dict[str, Any], exc: Exception) -> None:
        log_event(
            logger,
            "exception",
            "search_node_processing_exception",
            domain="search_node",
            registered_domain=domain_ref["registered_domain"],
            selenium_node_id=slot.get("selenium_node_id"),
            slot_id=slot.get("slot_id"),
            worker_name=domain_ref.get("worker_name"),
            celery_task_id=domain_ref.get("celery_task_id"),
            error=str(exc),
            exc_info=True,
        )
