from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, ReturnDocument

from core.config import Settings, get_settings
from services.failure_classifier import classify_failure
from services.job_listing_pattern_store import dedupe_job_listing_patterns, merge_job_listing_patterns
from services.node_lifecycle import retry_policy, status_from_totals, terminal_status
from services.node_preflight_service import get_node_preflight_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("job_extraction_node_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class JobExtractionNodeService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._domain_tasks.create_index([("job_extraction_status", ASCENDING), ("job_extraction_updated_at", ASCENDING)])
        self._indexes_ready = True

    def start_process(self, process_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        get_node_preflight_service().require_client_openai_config(process_id)
        process = self._load_ready_process(process_id)
        tasks = self._build_tasks(process)
        summary = self._queue_tasks(tasks)
        self._start_process_run(process_id, summary)
        self._dispatch_tasks(process_id, summary["created_tasks"])
        return {
            "process_id": process_id,
            "created": summary["created"],
            "failed_without_patterns": summary["failed"],
            "blocked": summary["blocked"],
            "enqueued": len(summary["created_tasks"]),
        }

    def _load_ready_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        if process.get("job_extraction_status") in {"queued", "running"}:
            raise RuntimeError("Job extraction node is already running for this process")
        if process.get("job_pattern_status") not in {"completed", "partial_completed"}:
            raise RuntimeError("Job pattern node must complete before job extraction starts")
        return process

    def _build_tasks(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        tasks = []
        for ref in self._completed_refs(process):
            task = self._task_from_ref(ref)
            if task:
                tasks.append(task)
        return tasks

    def _completed_refs(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        return list(process.get("domains", {}).get("completed", []) or [])

    def _task_from_ref(self, ref: dict[str, Any]) -> dict[str, Any] | None:
        domain_task = self._domain_tasks.find_one(
            {"registered_domain": ref["registered_domain"], "career_process_status": "completed"},
            {"domain": 1, "registered_domain": 1, "career_process.job_listing_patterns": 1},
        )
        if not domain_task:
            return None
        patterns = self._active_patterns(domain_task.get("career_process", {}).get("job_listing_patterns") or [])
        if not patterns:
            return {
                "registered_domain": ref["registered_domain"],
                "domain": ref.get("domain"),
                "status": "failed",
                "last_error": "No active job listing patterns are available",
                "input": {"job_listing_patterns": []},
            }
        return {
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "status": "queued",
            "last_error": None,
            "input": {"job_listing_patterns": patterns},
        }

    def _active_patterns(self, patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        active = []
        for pattern in dedupe_job_listing_patterns(patterns):
            if not isinstance(pattern, dict):
                continue
            page_url = str(pattern.get("page_url") or "").strip()
            if not page_url:
                continue
            if not isinstance(pattern.get("pattern"), dict):
                continue
            if pattern.get("status") not in {"pattern_ready", "extraction_completed", "pattern_repaired"}:
                continue
            if not bool((pattern.get("validation") or {}).get("valid")):
                continue
            active.append(pattern)
        return active

    def _queue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        created_tasks = []
        failed = 0
        blocked = 0
        for task in tasks:
            if task["status"] == "failed":
                failed += 1
                continue
            if self._queue_task(task):
                created_tasks.append(task)
                continue
            blocked += 1
        return {"created": len(created_tasks), "failed": failed, "blocked": blocked, "created_tasks": created_tasks}

    def _queue_task(self, task: dict[str, Any]) -> bool:
        timestamp = _now()
        result = self._domain_tasks.update_one(
            {
                "registered_domain": task["registered_domain"],
                "$or": [
                    {"job_extraction_status": {"$exists": False}},
                    {"job_extraction_status": {"$in": ["failed", "completed"]}},
                ],
            },
            {
                "$set": {
                    "job_extraction_status": "queued",
                    "job_extraction_input": task["input"],
                    "job_extraction_attempts": 0,
                    "job_extraction_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {
                    "job_extraction_last_error": "",
                    "job_extraction_last_completed_at": "",
                    "job_extraction_result": "",
                },
            },
        )
        return bool(result.modified_count)

    def _start_process_run(self, process_id: str, summary: dict[str, Any]) -> None:
        queued = int(summary.get("created") or 0)
        failed = int(summary.get("failed") or 0)
        blocked = int(summary.get("blocked") or 0)
        status = "running" if queued else self._terminal_status(completed=0, failed=failed, blocked=blocked)
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "job_extraction_status": status,
                    "job_extraction_totals": {
                        "domains": queued + failed + blocked,
                        "queued": queued,
                        "running": 0,
                        "completed": 0,
                        "failed": failed,
                        "blocked": blocked,
                    },
                    "job_extraction_started_at": _now(),
                    "job_extraction_completed_at": _now() if status != "running" else None,
                    "updated_at": _now(),
                },
                "$unset": {"job_extraction_last_error": ""},
            },
        )

    def claim_task(self, registered_domain: str, worker_name: str, task_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        timestamp = _now()
        stale_threshold = timestamp - timedelta(seconds=self._settings.stale_task_seconds)
        task = self._domain_tasks.find_one_and_update(
            {
                "registered_domain": registered_domain,
                "$or": [
                    {"job_extraction_status": "queued"},
                    {"job_extraction_status": "running", "job_extraction_last_started_at": {"$lt": stale_threshold}},
                ],
            },
            {
                "$set": {
                    "job_extraction_status": "running",
                    "job_extraction_worker_name": worker_name,
                    "job_extraction_celery_task_id": task_id,
                    "job_extraction_last_started_at": timestamp,
                    "job_extraction_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$inc": {"job_extraction_attempts": 1},
                "$unset": {"job_extraction_last_error": ""},
            },
            return_document=ReturnDocument.AFTER,
        )
        if not task:
            return {"status": "not_available"}
        if int(task.get("job_extraction_attempts") or 0) > retry_policy("job_extraction", self._settings.task_max_attempts).max_attempts:
            self.fail_task(registered_domain, "Maximum attempts exceeded")
            return {"status": "max_attempts_exceeded"}
        return {"status": "claimed", "task": self._task_from_domain_task(task)}

    def _task_from_domain_task(self, task: dict[str, Any]) -> dict[str, Any]:
        return {
            "registered_domain": task["registered_domain"],
            "domain": task.get("domain"),
            "input": task.get("job_extraction_input") or {},
            "worker_name": task.get("job_extraction_worker_name"),
            "celery_task_id": task.get("job_extraction_celery_task_id"),
            "job_extraction_attempts": task.get("job_extraction_attempts"),
        }

    def mark_process_task_running(self, process_id: str) -> None:
        self._processes.update_one(
            {"process_id": process_id, "job_extraction_totals.queued": {"$gt": 0}},
            {
                "$inc": {"job_extraction_totals.queued": -1, "job_extraction_totals.running": 1},
                "$set": {"job_extraction_status": "running", "updated_at": _now()},
            },
        )

    def complete_task(self, registered_domain: str, result: dict[str, Any], process_id: str | None = None) -> None:
        timestamp = _now()
        status = "failed" if result.get("status") == "failed" else "completed"
        merged_patterns = self._merged_patterns(registered_domain, result.get("job_listing_patterns") or [])
        self._domain_tasks.update_one(
            {"registered_domain": registered_domain, "job_extraction_status": "running"},
            {
                "$set": {
                    "job_extraction_status": status,
                    "job_extraction_result": result,
                    "job_extraction_last_completed_at": timestamp,
                    "job_extraction_updated_at": timestamp,
                    "career_process.job_listing_patterns": merged_patterns,
                    "career_process.job_storage": result.get("job_storage") or {},
                    "updated_at": timestamp,
                },
                "$unset": {
                    "job_extraction_worker_name": "",
                    "job_extraction_celery_task_id": "",
                    "job_extraction_last_error": "",
                },
            },
        )
        if process_id:
            self._move_process_counter(process_id, status)

    def _merged_patterns(self, registered_domain: str, incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
        task = self._domain_tasks.find_one(
            {"registered_domain": registered_domain},
            {"career_process.job_listing_patterns": 1},
        )
        existing = ((task or {}).get("career_process") or {}).get("job_listing_patterns") or []
        return merge_job_listing_patterns(existing, incoming)

    def fail_task(self, registered_domain: str, error: str, process_id: str | None = None) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": registered_domain},
            {
                "$set": {
                    "job_extraction_status": "failed",
                    "job_extraction_last_error": error,
                    "job_extraction_last_failure_type": classify_failure(error),
                    "job_extraction_updated_at": _now(),
                    "updated_at": _now(),
                },
                "$unset": {
                    "job_extraction_worker_name": "",
                    "job_extraction_celery_task_id": "",
                },
            },
        )
        if process_id:
            self._move_process_counter(process_id, "failed", error=error)

    def requeue_task(self, registered_domain: str, error: str, *, decrement_attempt: bool = False) -> None:
        update: dict[str, Any] = {
            "$set": {
                "job_extraction_status": "queued",
                "job_extraction_last_error": error,
                "job_extraction_last_failure_type": classify_failure(error),
                "job_extraction_updated_at": _now(),
                "updated_at": _now(),
            },
            "$unset": {
                "job_extraction_worker_name": "",
                "job_extraction_celery_task_id": "",
            },
        }
        if decrement_attempt:
            update["$inc"] = {"job_extraction_attempts": -1}
        self._domain_tasks.update_one({"registered_domain": registered_domain, "job_extraction_status": "running"}, update)

    def mark_process_task_requeued(self, process_id: str, error: str | None = None) -> None:
        update: dict[str, Any] = {"updated_at": _now()}
        if error:
            update["job_extraction_last_error"] = error
        self._processes.update_one(
            {"process_id": process_id, "job_extraction_totals.running": {"$gt": 0}},
            {
                "$inc": {"job_extraction_totals.running": -1, "job_extraction_totals.queued": 1},
                "$set": update,
            },
        )

    def mark_process_queued_task_failed(self, process_id: str, error: str | None = None) -> None:
        update: dict[str, Any] = {"updated_at": _now()}
        if error:
            update["job_extraction_last_error"] = error
        self._processes.update_one(
            {"process_id": process_id, "job_extraction_totals.queued": {"$gt": 0}},
            {
                "$inc": {"job_extraction_totals.queued": -1, "job_extraction_totals.failed": 1},
                "$set": update,
            },
        )
        self._refresh_process_status(process_id)

    def requeue_stale_tasks(self) -> int:
        self.ensure_indexes()
        threshold = _now() - timedelta(seconds=self._settings.stale_task_seconds)
        result = self._domain_tasks.update_many(
            {
                "job_extraction_status": "running",
                "job_extraction_last_started_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "job_extraction_status": "queued",
                    "job_extraction_last_error": "Requeued stale job extraction task",
                    "job_extraction_updated_at": _now(),
                    "updated_at": _now(),
                },
                "$unset": {
                    "job_extraction_worker_name": "",
                    "job_extraction_celery_task_id": "",
                },
            },
        )
        if result.modified_count:
            log_event(logger, "warning", "stale_job_extraction_tasks_requeued", domain="watchdog", count=result.modified_count)
        return int(result.modified_count)

    def queued_tasks_for_watchdog(self) -> list[dict[str, Any]]:
        self.ensure_indexes()
        return list(self._domain_tasks.find({"job_extraction_status": "queued"}))

    def _move_process_counter(self, process_id: str, target: str, error: str | None = None) -> None:
        update: dict[str, Any] = {"updated_at": _now()}
        if error:
            update["job_extraction_last_error"] = error
        self._processes.update_one(
            {"process_id": process_id, "job_extraction_totals.running": {"$gt": 0}},
            {
                "$inc": {"job_extraction_totals.running": -1, f"job_extraction_totals.{target}": 1},
                "$set": update,
            },
        )
        self._refresh_process_status(process_id)

    def _refresh_process_status(self, process_id: str) -> None:
        process = self._processes.find_one({"process_id": process_id}, {"job_extraction_totals": 1})
        totals = (process or {}).get("job_extraction_totals") or {}
        status = self._status_from_totals(totals)
        update = {"job_extraction_status": status, "updated_at": _now()}
        if status in {"completed", "partial_completed", "failed"}:
            update["job_extraction_completed_at"] = _now()
        self._processes.update_one({"process_id": process_id}, {"$set": update})

    def _status_from_totals(self, totals: dict[str, Any]) -> str:
        return status_from_totals(totals)

    def _terminal_status(self, *, completed: int, failed: int, blocked: int) -> str:
        return terminal_status(completed=completed, failed=failed, blocked=blocked)

    def _dispatch_tasks(self, process_id: str, tasks: list[dict[str, Any]]) -> None:
        for task in tasks:
            self._dispatch_task(process_id, task)

    def _dispatch_task(self, process_id: str, task: dict[str, Any]) -> None:
        from infrastructure.tasks import run_job_extraction_node

        log_event(
            logger,
            "info",
            "job_extraction_domain_dispatched",
            domain="job_extraction",
            process_id=process_id,
            registered_domain=task["registered_domain"],
        )
        run_job_extraction_node.apply_async(args=[process_id, task["registered_domain"]], queue="processes")


@lru_cache(maxsize=1)
def get_job_extraction_node_service() -> JobExtractionNodeService:
    return JobExtractionNodeService(get_sync_mongodb_service(), get_settings())
