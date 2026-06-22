from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from core.config import Settings, get_settings
from services.domain_job_service import get_domain_job_service
from services.grid_session import close_session_via_http_async, create_session_async
from services.job_pagination.pipeline.runner import run_pipeline
from services.openai_service import reset_openai_runtime_config, set_openai_runtime_config
from services.selenium_session_heartbeat import SeleniumSessionHeartbeat
from services.selenium_session_slot_service import get_selenium_session_slot_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("job_pagination_node_processor")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


class NoJobPaginationSessionSlotAvailable(RuntimeError):
    pass


class JobPaginationNodeProcessor:
    def __init__(self, mongodb: SyncMongoDBService | None = None, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._mongodb = mongodb or get_sync_mongodb_service()
        self._clients = self._mongodb.collection(self._settings.mongodb_clients_collection)
        self._processes = self._mongodb.collection(self._settings.mongodb_process_uploads_collection)
        self._pagination_runs = self._mongodb.collection(self._settings.mongodb_job_pagination_runs_collection)

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
            log_event(
                logger,
                "exception",
                "job_pagination_processing_failed registered_domain=%s error=%s",
                task.get("registered_domain"),
                str(exc),
                domain="job_pagination",
                process_id=process_id,
                registered_domain=task.get("registered_domain"),
                worker_name=worker_name,
                celery_task_id=celery_task_id,
                slot_id=slot.get("slot_id"),
                error=str(exc),
                exc_info=True,
            )
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
            raise NoJobPaginationSessionSlotAvailable("No Selenium session slot is currently available")
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
        with SeleniumSessionHeartbeat(slot["slot_id"]):
            try:
                self._log_session_create_started(task, slot)
                self._progress(task, "pagination_creating_selenium_session")
                session = await self._create_selenium_session(slot)
                self._attach_session(slot, session.session_id)
                self._heartbeat(slot)
                self._progress(task, "pagination_running_patterns")
                patterns = await self._run_patterns(task, session.cdp_url, slot)
                self._progress(task, "pagination_completed")
                return self._result(task, patterns, time.monotonic() - started)
            finally:
                await self._close_selenium_session(slot, session)

    async def _create_selenium_session(self, slot: dict[str, Any]):
        session = await create_session_async(grid_url=slot["grid_url"], reuse_existing=False)
        if not session or not session.cdp_url:
            raise NoJobPaginationSessionSlotAvailable("Could not create Selenium session for job pagination node")
        return session

    def _attach_session(self, slot: dict[str, Any], session_id: str) -> None:
        get_selenium_session_slot_service().attach_session(slot["slot_id"], session_id)

    async def _run_patterns(self, task: dict[str, Any], cdp_url: str, slot: dict[str, Any]) -> list[dict[str, Any]]:
        results = []
        for pattern in task.get("input", {}).get("job_listing_patterns") or []:
            results.append(await self._run_pattern(task, pattern, cdp_url, slot))
        return results

    async def _run_pattern(
        self,
        task: dict[str, Any],
        pattern_entry: dict[str, Any],
        cdp_url: str,
        slot: dict[str, Any],
    ) -> dict[str, Any]:
        page_url = str(pattern_entry.get("page_url") or "").strip()
        self._log_started(task, page_url)
        self._progress(task, "pagination_pattern_started", page_url, 1)
        result = await run_pipeline(
            cdp_url,
            page_url,
            job_pattern=pattern_entry.get("pattern") or {},
            pagination_plan=pattern_entry.get("pagination") or {},
            progress=lambda step, current_url=None, page_index=None: self._progress(
                task,
                step,
                current_url,
                page_index,
            ),
        )
        self._heartbeat(slot)
        return {
            **pattern_entry,
            "status": "pagination_completed" if (result.get("validation") or {}).get("has_jobs") else "pagination_no_jobs",
            "jobs": self._jobs_with_source(pattern_entry, result.get("jobs") or []),
            "job_count": len(result.get("jobs") or []),
            "example_jobs": self._sample_jobs(result.get("jobs") or []),
            "pagination": result.get("pagination"),
            "pagination_discoveries": result.get("pagination_discoveries"),
            "pagination_runs": result.get("pagination_runs"),
            "infinite_scroll_runs": result.get("infinite_scroll_runs"),
            "page_reports": result.get("page_reports"),
            "navigation_errors": result.get("navigation_errors"),
            "validation": result.get("validation"),
            "stop_reason": result.get("stop_reason"),
            "pagination_mode": pattern_entry.get("pagination_mode"),
            "last_paginated_at": _now_iso(),
        }

    def _result(self, task: dict[str, Any], patterns: list[dict[str, Any]], duration_seconds: float) -> dict[str, Any]:
        all_jobs = [job for pattern in patterns for job in pattern.get("jobs") or []]
        job_storage = get_domain_job_service().upsert_jobs(task["registered_domain"], all_jobs, _now())
        status = "completed" if patterns else "failed"
        return {
            "node": "job_pagination",
            "processor": "job_pagination_node",
            "domain": task.get("domain"),
            "registered_domain": task["registered_domain"],
            "status": status,
            "job_listing_patterns": self._clean_patterns(patterns),
            "job_storage": job_storage,
            "duration_seconds": round(duration_seconds, 3),
            "processed_at": _now_iso(),
        }

    def _clean_patterns(self, patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "page_url": item.get("page_url"),
                "status": item.get("status"),
                "pattern": item.get("pattern"),
                "job_count": item.get("job_count"),
                "example_jobs": list(item.get("example_jobs") or [])[:2],
                "validation": item.get("validation"),
                "pagination": item.get("pagination"),
                "pagination_runs": item.get("pagination_runs"),
                "pagination_discoveries": item.get("pagination_discoveries"),
                "infinite_scroll_runs": item.get("infinite_scroll_runs"),
                "page_reports": item.get("page_reports"),
                "navigation_errors": item.get("navigation_errors"),
                "stop_reason": item.get("stop_reason"),
                "pagination_mode": item.get("pagination_mode"),
                "last_paginated_at": item.get("last_paginated_at"),
            }
            for item in patterns
        ]

    def _jobs_with_source(self, pattern_entry: dict[str, Any], jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        source_url = pattern_entry.get("page_url")
        return [{**job, "source_url": source_url} for job in jobs if isinstance(job, dict)]

    def _sample_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{"title": job.get("job_title") or job.get("title"), "job_url": job.get("job_url")} for job in jobs[:2]]

    async def _close_selenium_session(self, slot: dict[str, Any], session: Any) -> None:
        if session is not None:
            await close_session_via_http_async(slot["grid_url"], session.session_id)

    def _mark_slot_stale(self, slot: dict[str, Any], error: str) -> None:
        get_selenium_session_slot_service().mark_slot_stale(slot["slot_id"], error)

    def _release_slot(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().release_slot(slot["slot_id"])

    def _heartbeat(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().heartbeat_slot(slot["slot_id"])

    def _progress(
        self,
        task: dict[str, Any],
        step: str,
        current_url: str | None = None,
        page_index: int | None = None,
    ) -> None:
        timestamp = _now()
        fields: dict[str, Any] = {
            "current_step": step,
            "last_step_at": timestamp,
            "updated_at": timestamp,
        }
        if current_url:
            fields["current_url"] = current_url
        if page_index is not None:
            fields["current_page_index"] = page_index
        self._pagination_runs.update_one(
            {"job_pagination_run_key": self._pagination_run_key(task["registered_domain"]), "status": "running"},
            {"$set": fields},
        )

    def _log_started(self, task: dict[str, Any], page_url: str) -> None:
        log_event(logger, "info", "job_pagination_pattern_started", domain="job_pagination", registered_domain=task["registered_domain"], page_url=page_url)

    def _log_session_create_started(self, task: dict[str, Any], slot: dict[str, Any]) -> None:
        log_event(
            logger,
            "info",
            "job_pagination_session_create_started",
            domain="job_pagination",
            registered_domain=task["registered_domain"],
            selenium_node_id=slot["selenium_node_id"],
            slot_id=slot["slot_id"],
        )

    def _pagination_run_key(self, registered_domain: str) -> str:
        return f"shared:{registered_domain}"
