from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from core.config import Settings, get_settings
from nodes.career_page_category import career_page_category_node
from services.grid_session import (
    attach_playwright_to_cdp,
    close_browser_attachment,
    close_session_via_http_async,
    create_session_async,
)
from services.openai_service import reset_openai_runtime_config, set_openai_runtime_config
from services.selenium_session_heartbeat import SeleniumSessionHeartbeat
from services.selenium_session_slot_service import get_selenium_session_slot_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("career_category_node_processor")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class NoCareerCategorySessionSlotAvailable(RuntimeError):
    pass


class CareerCategoryNodeProcessor:
    def __init__(self, mongodb: SyncMongoDBService | None = None, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._mongodb = mongodb or get_sync_mongodb_service()
        self._clients = self._mongodb.collection(self._settings.mongodb_clients_collection)
        self._processes = self._mongodb.collection(self._settings.mongodb_process_uploads_collection)

    def process(
        self,
        task: dict[str, Any],
        *,
        process_id: str | None,
        worker_name: str,
        celery_task_id: str,
    ) -> dict[str, Any]:
        slot = self._claim_slot(task, process_id, worker_name, celery_task_id)
        tokens = self._set_client_openai_config(process_id)
        try:
            return asyncio.run(self._process_async(task, slot))
        except Exception as exc:
            self._mark_slot_stale(slot, str(exc))
            raise
        finally:
            reset_openai_runtime_config(tokens)
            self._release_slot(slot)

    def _claim_slot(
        self,
        task: dict[str, Any],
        process_id: str | None,
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
            raise NoCareerCategorySessionSlotAvailable("No Selenium session slot is currently available")
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
                self._log_session_created(task, slot, session.session_id)
                self._heartbeat(slot)
                browser_session = await self._attach_browser(session.cdp_url)
                self._log_browser_attached(task, session.session_id)
                self._heartbeat(slot)
                node_result = await self._run_category_node(task, browser_session, slot)
                self._heartbeat(slot)
                return self._result(task, slot, node_result, time.monotonic() - started)
            finally:
                await close_browser_attachment(browser_session)
                await self._close_selenium_session(slot, session)

    async def _create_selenium_session(self, slot: dict[str, Any]):
        session = await create_session_async(grid_url=slot["grid_url"], reuse_existing=False)
        if not session or not session.cdp_url:
            raise NoCareerCategorySessionSlotAvailable("Could not create Selenium session for category node")
        return session

    async def _attach_browser(self, cdp_url: str):
        browser_session = await attach_playwright_to_cdp(cdp_url, raise_on_failure=True)
        if browser_session is None:
            raise RuntimeError("Could not attach Playwright to Selenium session")
        return browser_session

    async def _run_category_node(self, task: dict[str, Any], browser_session: Any, slot: dict[str, Any]) -> dict[str, Any]:
        career_urls = list(task.get("input", {}).get("career_urls") or [])
        self._log_started(task, career_urls)
        return await career_page_category_node(
            career_urls,
            browser_session,
            agent_index=int(slot.get("session_index") or 0),
            agent_tab={"handle": None},
            heartbeat=lambda: self._heartbeat(slot),
        )

    async def _close_selenium_session(self, slot: dict[str, Any], session: Any) -> None:
        if session is None:
            return
        await close_session_via_http_async(slot["grid_url"], session.session_id)

    def _result(
        self,
        task: dict[str, Any],
        slot: dict[str, Any],
        node_result: dict[str, Any],
        duration_seconds: float,
    ) -> dict[str, Any]:
        overview = node_result.get("overview") or {}
        return {
            "node": "career_page_category",
            "processor": "career_category_node",
            "domain": task.get("domain"),
            "registered_domain": task["registered_domain"],
            "career_urls": list(task.get("input", {}).get("career_urls") or []),
            "overview": overview,
            "career_pages_analysis": node_result.get("career_pages_analysis") or [],
            "job_listing_patterns": node_result.get("job_listing_patterns") or [],
            "outcome": overview.get("outcome"),
            "jobs_found": bool(overview.get("jobs_found")),
            "total_jobs_found": int(overview.get("total_jobs_found") or 0),
            "duration_seconds": round(duration_seconds, 3),
            "selenium_node_id": slot["selenium_node_id"],
            "selenium_session_slot_id": slot["slot_id"],
            "session_index": slot["session_index"],
            "processed_at": _now_iso(),
        }

    def _log_started(self, task: dict[str, Any], career_urls: list[str]) -> None:
        log_event(
            logger,
            "info",
            "career_category_node_started",
            domain="career_category",
            registered_domain=task["registered_domain"],
            career_url_count=len(career_urls),
        )

    def _log_session_create_started(self, task: dict[str, Any], slot: dict[str, Any]) -> None:
        log_event(
            logger,
            "info",
            "career_category_session_create_started",
            domain="career_category",
            registered_domain=task["registered_domain"],
            selenium_node_id=slot["selenium_node_id"],
            slot_id=slot["slot_id"],
        )

    def _log_session_created(self, task: dict[str, Any], slot: dict[str, Any], session_id: str) -> None:
        log_event(
            logger,
            "info",
            "career_category_session_created",
            domain="career_category",
            registered_domain=task["registered_domain"],
            selenium_node_id=slot["selenium_node_id"],
            slot_id=slot["slot_id"],
            selenium_session_id=session_id,
        )

    def _log_browser_attached(self, task: dict[str, Any], session_id: str) -> None:
        log_event(
            logger,
            "info",
            "career_category_browser_attached",
            domain="career_category",
            registered_domain=task["registered_domain"],
            selenium_session_id=session_id,
        )

    def _mark_slot_stale(self, slot: dict[str, Any], error: str) -> None:
        get_selenium_session_slot_service().mark_slot_stale(slot["slot_id"], error)

    def _release_slot(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().release_slot(slot["slot_id"])

    def _heartbeat(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().heartbeat_slot(slot["slot_id"])
