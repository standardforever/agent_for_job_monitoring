from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, ReturnDocument

from core.config import Settings, get_settings
from services.domain_job_service import get_domain_job_service
from services.failure_classifier import classify_failure
from services.job_listing_pattern_store import dedupe_job_listing_patterns
from services.node_lifecycle import terminal_status, status_from_totals, retry_policy
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("career_process_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class CareerProcessService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._domain_tasks.create_index([("career_process_status", ASCENDING), ("career_process_updated_at", ASCENDING)])
        get_domain_job_service().ensure_indexes()
        self._indexes_ready = True

    def create_category_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        self.ensure_indexes()
        created_tasks: list[dict[str, Any]] = []
        failed = 0
        blocked = 0
        for task in tasks:
            if task["status"] == "failed":
                self._mark_missing_candidates(task)
                failed += 1
                continue
            if self._queue_task(task):
                created_tasks.append(task)
                continue
            blocked += 1
        return {"created": len(created_tasks), "failed": failed, "blocked": blocked, "created_tasks": created_tasks}

    def start_process_run(self, process_id: str, summary: dict[str, Any]) -> None:
        queued = int(summary.get("created") or 0)
        failed = int(summary.get("failed") or 0)
        blocked = int(summary.get("blocked") or 0)
        status = "running" if queued else self._terminal_process_status(completed=0, failed=failed, blocked=blocked)
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "career_status": status,
                    "career_totals": {
                        "domains": queued + failed + blocked,
                        "queued": queued,
                        "running": 0,
                        "completed": 0,
                        "failed": failed,
                        "blocked": blocked,
                    },
                    "career_started_at": _now(),
                    "career_completed_at": _now() if status != "running" else None,
                    "updated_at": _now(),
                },
                "$unset": {"career_last_error": ""},
            },
        )

    def mark_process_task_running(self, process_id: str) -> None:
        self._processes.update_one(
            {"process_id": process_id, "career_totals.queued": {"$gt": 0}},
            {
                "$inc": {"career_totals.queued": -1, "career_totals.running": 1},
                "$set": {"career_status": "running", "updated_at": _now()},
            },
        )

    def _queue_task(self, task: dict[str, Any]) -> bool:
        timestamp = _now()
        result = self._domain_tasks.update_one(
            {
                "registered_domain": task["registered_domain"],
                "$or": [
                    {"career_process_status": {"$exists": False}},
                    {"career_process_status": {"$in": ["failed", "completed"]}},
                ],
            },
            {
                "$set": {
                    "career_process_status": "queued",
                    "career_process_input": task["input"],
                    "career_process_attempts": 0,
                    "career_process_last_started_at": None,
                    "career_process_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {
                    "career_process": "",
                    "career_process_last_completed_at": "",
                    "career_process_last_error": "",
                },
            },
        )
        return bool(result.modified_count)

    def _mark_missing_candidates(self, task: dict[str, Any]) -> None:
        timestamp = _now()
        self._domain_tasks.update_one(
            {"registered_domain": task["registered_domain"]},
            {
                "$set": {
                    "career_process_status": "failed",
                    "career_process_input": task["input"],
                    "career_process_last_error": task["last_error"],
                    "career_process_last_failure_type": classify_failure(task["last_error"]),
                    "career_process_updated_at": timestamp,
                    "updated_at": timestamp,
                },
            },
        )

    def claim_category_task(self, registered_domain: str, worker_name: str, task_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        timestamp = _now()
        stale_threshold = timestamp - timedelta(seconds=self._settings.stale_task_seconds)
        task = self._domain_tasks.find_one_and_update(
            {
                "registered_domain": registered_domain,
                "$or": [
                    {"career_process_status": "queued"},
                    {"career_process_status": "running", "career_process_last_started_at": {"$lt": stale_threshold}},
                ],
            },
            {
                "$set": {
                    "career_process_status": "running",
                    "career_process_worker_name": worker_name,
                    "career_process_celery_task_id": task_id,
                    "career_process_last_started_at": timestamp,
                    "career_process_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$inc": {"career_process_attempts": 1},
                "$unset": {"career_process_last_error": ""},
            },
            return_document=ReturnDocument.AFTER,
        )
        if not task:
            return {"status": "not_available"}
        if int(task.get("career_process_attempts") or 0) > retry_policy("career_category", self._settings.task_max_attempts).max_attempts:
            self.fail_task(registered_domain, "Maximum attempts exceeded")
            return {"status": "max_attempts_exceeded"}
        return {"status": "claimed", "task": self._task_from_domain(task)}

    def _task_from_domain(self, domain_task: dict[str, Any]) -> dict[str, Any]:
        return {
            "registered_domain": domain_task["registered_domain"],
            "domain": domain_task.get("domain"),
            "input": domain_task.get("career_process_input") or {},
            "worker_name": domain_task.get("career_process_worker_name"),
            "celery_task_id": domain_task.get("career_process_celery_task_id"),
            "career_process_attempts": domain_task.get("career_process_attempts"),
        }

    def complete_task(self, registered_domain: str, result: dict[str, Any], process_id: str | None = None) -> None:
        timestamp = _now()
        clean_result = self._clean_category_result(result, timestamp)
        job_summary = self._upsert_jobs(registered_domain, result, timestamp)
        clean_result["job_storage"] = job_summary
        ignored_candidates = self._ignored_candidates(clean_result)
        self._domain_tasks.update_one(
            self._task_filter(registered_domain, "running"),
            {
                "$set": {
                    "career_process_status": "completed",
                    "career_process": clean_result,
                    "career_process_last_completed_at": timestamp,
                    "career_process_updated_at": timestamp,
                    "career_candidate_ignore_urls": ignored_candidates,
                    "updated_at": timestamp,
                },
                "$unset": {
                    "career_process_worker_name": "",
                    "career_process_celery_task_id": "",
                    "career_process_last_error": "",
                },
            },
        )
        if process_id:
            self.mark_process_task_completed(process_id)

    def mark_process_task_completed(self, process_id: str) -> None:
        self._move_process_career_counter(process_id, "completed")

    def mark_process_task_failed(self, process_id: str, error: str | None = None) -> None:
        update: dict[str, Any] = {}
        if error:
            update["career_last_error"] = error
        self._move_process_career_counter(process_id, "failed", extra_set=update)

    def mark_process_queued_task_failed(self, process_id: str, error: str | None = None) -> None:
        update: dict[str, Any] = {"updated_at": _now()}
        if error:
            update["career_last_error"] = error
        self._processes.update_one(
            {"process_id": process_id, "career_totals.queued": {"$gt": 0}},
            {
                "$inc": {"career_totals.queued": -1, "career_totals.failed": 1},
                "$set": update,
            },
        )
        self._refresh_process_career_status(process_id)

    def _move_process_career_counter(
        self,
        process_id: str,
        target: str,
        *,
        extra_set: dict[str, Any] | None = None,
    ) -> None:
        self._processes.update_one(
            {"process_id": process_id, "career_totals.running": {"$gt": 0}},
            {
                "$inc": {"career_totals.running": -1, f"career_totals.{target}": 1},
                "$set": {"updated_at": _now(), **(extra_set or {})},
            },
        )
        self._refresh_process_career_status(process_id)

    def mark_process_task_requeued(self, process_id: str, error: str | None = None) -> None:
        update: dict[str, Any] = {"updated_at": _now()}
        if error:
            update["career_last_error"] = error
        self._processes.update_one(
            {"process_id": process_id, "career_totals.running": {"$gt": 0}},
            {
                "$inc": {"career_totals.running": -1, "career_totals.queued": 1},
                "$set": update,
            },
        )

    def _refresh_process_career_status(self, process_id: str) -> None:
        process = self._processes.find_one({"process_id": process_id}, {"career_totals": 1})
        totals = (process or {}).get("career_totals") or {}
        status = self._career_status_from_totals(totals)
        update = {"career_status": status, "updated_at": _now()}
        if status in {"completed", "partial_completed", "failed"}:
            update["career_completed_at"] = _now()
        self._processes.update_one({"process_id": process_id}, {"$set": update})

    def _career_status_from_totals(self, totals: dict[str, Any]) -> str:
        if int(totals.get("running") or 0) > 0 or int(totals.get("queued") or 0) > 0:
            return "running"
        return status_from_totals(totals)

    def _terminal_process_status(self, *, completed: int, failed: int, blocked: int) -> str:
        return terminal_status(completed=completed, failed=failed, blocked=blocked)

    def fail_task(self, registered_domain: str, error: str, process_id: str | None = None) -> None:
        timestamp = _now()
        self._domain_tasks.update_one(
            {"registered_domain": registered_domain},
            {
                "$set": {
                    "career_process_status": "failed",
                    "career_process_last_error": error,
                    "career_process_last_failure_type": classify_failure(error),
                    "career_process_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {
                    "career_process_worker_name": "",
                    "career_process_celery_task_id": "",
                },
            },
        )
        if process_id:
            self.mark_process_task_failed(process_id, error)

    def requeue_task(self, registered_domain: str, error: str, *, decrement_attempt: bool = False) -> None:
        update: dict[str, Any] = {
            "$set": {
                "career_process_status": "queued",
                "career_process_last_error": error,
                "career_process_last_failure_type": classify_failure(error),
                "career_process_updated_at": _now(),
                "updated_at": _now(),
            },
            "$unset": {
                "career_process_worker_name": "",
                "career_process_celery_task_id": "",
            },
        }
        if decrement_attempt:
            update["$inc"] = {"career_process_attempts": -1}
        self._domain_tasks.update_one(self._task_filter(registered_domain, "running"), update)

    def active_process_run(self, process: dict[str, Any]) -> bool:
        return process.get("career_status") in {"queued", "running"}

    def requeue_stale_category_tasks(self) -> int:
        self.ensure_indexes()
        threshold = _now() - timedelta(seconds=self._settings.stale_task_seconds)
        result = self._domain_tasks.update_many(
            {
                "career_process_status": "running",
                "career_process_last_started_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "career_process_status": "queued",
                    "career_process_last_error": "Requeued stale category task",
                    "career_process_updated_at": _now(),
                    "updated_at": _now(),
                },
                "$unset": {
                    "career_process_worker_name": "",
                    "career_process_celery_task_id": "",
                },
            },
        )
        if result.modified_count:
            log_event(logger, "warning", "stale_category_tasks_requeued", domain="watchdog", count=result.modified_count)
        return int(result.modified_count)

    def queued_category_tasks_for_watchdog(self) -> list[dict[str, Any]]:
        self.ensure_indexes()
        return list(self._domain_tasks.find({"career_process_status": "queued"}))

    def _task_filter(self, registered_domain: str, status: str) -> dict[str, Any]:
        return {"registered_domain": registered_domain, "career_process_status": status}

    def _clean_category_result(self, result: dict[str, Any], timestamp: datetime) -> dict[str, Any]:
        overview = dict(result.get("overview") or {})
        return {
            "status": "completed",
            "career_urls": list(result.get("career_urls") or []),
            "career_urls_checked": self._career_urls_checked(result),
            "outcome": result.get("outcome") or overview.get("outcome"),
            "outcome_reason": overview.get("outcome_reason"),
            "jobs_found": bool(result.get("jobs_found") or overview.get("jobs_found")),
            "total_jobs_found": int(result.get("total_jobs_found") or overview.get("total_jobs_found") or 0),
            "job_sample_urls": list(overview.get("job_urls") or [])[:2],
            "job_found_on_urls": list(overview.get("job_found_on_urls") or []),
            "career_page_confirmed": bool(overview.get("career_page_confirmed")),
            "no_vacancy_urls": list(overview.get("no_vacancy_urls") or []),
            "general_job_info_urls": list(overview.get("general_job_info_urls") or []),
            "listing_ui": overview.get("listing_ui"),
            "job_alert": bool(overview.get("job_alert")),
            "job_alert_urls": list(overview.get("job_alert_urls") or []),
            "access_issue_urls": list(overview.get("access_issue_urls") or []),
            "not_job_related_urls": list(overview.get("not_job_related_urls") or []),
            "navigation_issues": list(overview.get("navigation_issues") or []),
            "job_listing_patterns": self._clean_job_patterns(result.get("job_listing_patterns") or []),
            "duration_seconds": result.get("duration_seconds"),
            "processed_at": result.get("processed_at"),
            "updated_at": timestamp,
        }

    def _career_urls_checked(self, result: dict[str, Any]) -> list[dict[str, Any]]:
        checked = []
        for item in result.get("career_pages_analysis") or []:
            if not isinstance(item, dict):
                continue
            url = item.get("classified_job_listing_url") or item.get("extracted_url") or item.get("current_url") or item.get("url")
            checked.append(
                {
                    "url": url,
                    "status": item.get("status"),
                    "jobs_found": bool(item.get("jobs_listed_on_page")),
                    "job_sample_urls": list(item.get("jobs_listed_on_page") or [])[:2],
                    "job_alert": bool(item.get("job_alert")),
                    "page_access_status": item.get("page_access_status"),
                    "last_checked_at": item.get("extracted_at") or result.get("processed_at"),
                }
            )
        return checked

    def _clean_job_patterns(self, patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        cleaned = []
        for pattern in dedupe_job_listing_patterns(patterns):
            if not isinstance(pattern, dict):
                continue
            cleaned.append(
                {
                    "page_url": pattern.get("page_url"),
                    "status": pattern.get("status"),
                    "pattern": pattern.get("pattern"),
                    "job_count": pattern.get("job_count"),
                    "example_jobs": list(pattern.get("example_jobs") or [])[:2],
                    "listing_ui": self._clean_listing_ui(pattern.get("listing_ui") or {}),
                    "generated_at": pattern.get("generated_at"),
                }
            )
        return cleaned

    def _clean_listing_ui(self, listing_ui: dict[str, Any]) -> dict[str, Any]:
        return {
            "ui_category": listing_ui.get("ui_category"),
            "filter_present": bool(listing_ui.get("filter_present", False)),
            "filter_types": [str(item) for item in (listing_ui.get("filter_types") or []) if item],
            "sort_present": bool(listing_ui.get("sort_present", False)),
            "sort_types": [str(item) for item in (listing_ui.get("sort_types") or []) if item],
            "pagination_present": bool(listing_ui.get("pagination_present", False)),
            "pagination_type": listing_ui.get("pagination_type"),
            "pagination_category": listing_ui.get("pagination_category"),
            "pagination_navigation_method": listing_ui.get("pagination_navigation_method"),
            "next_page_url": listing_ui.get("next_page_url"),
        }

    def _ignored_candidates(self, clean_result: dict[str, Any]) -> list[dict[str, Any]]:
        ignored = []
        blocked_urls = set(clean_result.get("not_job_related_urls") or [])
        for item in clean_result.get("career_urls_checked") or []:
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            if item.get("status") != "not_job_related" and url not in blocked_urls:
                continue
            ignored.append(
                {
                    "url": url,
                    "reason": "not_job_related",
                    "last_checked_at": item.get("last_checked_at") or clean_result.get("processed_at"),
                }
            )
        return ignored

    def _upsert_jobs(self, registered_domain: str, result: dict[str, Any], timestamp: datetime) -> dict[str, Any]:
        return get_domain_job_service().upsert_jobs(registered_domain, self._jobs_from_patterns(result), timestamp)

    def _jobs_from_patterns(self, result: dict[str, Any]) -> list[dict[str, Any]]:
        jobs = []
        for pattern in result.get("job_listing_patterns") or []:
            source_url = pattern.get("page_url")
            for job in pattern.get("jobs") or []:
                if isinstance(job, dict):
                    jobs.append({**job, "source_url": source_url})
        return jobs


@lru_cache(maxsize=1)
def get_career_process_service() -> CareerProcessService:
    return CareerProcessService(get_sync_mongodb_service(), get_settings())
