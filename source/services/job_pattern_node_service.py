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


logger = get_logger("job_pattern_node_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class JobPatternNodeService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._domain_tasks.create_index([("job_pattern_status", ASCENDING), ("job_pattern_updated_at", ASCENDING)])
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
            "reused": summary["completed"],
            "failed": summary["failed"],
            "blocked": summary["blocked"],
            "enqueued": len(summary["created_tasks"]),
        }

    def _load_ready_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        if process.get("job_pattern_status") in {"queued", "running"}:
            raise RuntimeError("Job pattern node is already running for this process")
        if process.get("career_status") not in {"completed", "partial_completed"}:
            raise RuntimeError("Career category node must complete before job pattern starts")
        return process

    def _build_tasks(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        tasks = []
        for ref in process.get("domains", {}).get("completed", []) or []:
            task = self._task_from_ref(ref)
            if task:
                tasks.append(task)
        return tasks

    def _task_from_ref(self, ref: dict[str, Any]) -> dict[str, Any] | None:
        domain_task = self._domain_tasks.find_one(
            {"registered_domain": ref["registered_domain"], "career_process_status": "completed"},
            {"domain": 1, "registered_domain": 1, "career_process.job_listing_patterns": 1},
        )
        if not domain_task:
            return self._blocked_task(ref, "Career category result is unavailable")
        patterns = domain_task.get("career_process", {}).get("job_listing_patterns") or []
        ready_patterns = self._ready_patterns(patterns)
        if ready_patterns:
            return {
                "registered_domain": ref["registered_domain"],
                "domain": ref.get("domain"),
                "status": "completed",
                "last_error": None,
                "input": {"job_listing_patterns": ready_patterns},
            }
        candidates = self._pending_candidates(patterns)
        if not candidates:
            return self._blocked_task(ref, "No job listing page was identified by career category")
        return {
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "status": "queued",
            "last_error": None,
            "input": {"job_listing_patterns": candidates},
        }

    def _blocked_task(self, ref: dict[str, Any], reason: str) -> dict[str, Any]:
        return {
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "status": "blocked",
            "last_error": reason,
            "input": {"job_listing_patterns": []},
        }

    def _ready_patterns(self, patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            item
            for item in dedupe_job_listing_patterns(patterns)
            if isinstance(item, dict)
            and item.get("status") in {"pattern_ready", "extraction_completed", "pattern_repaired"}
            and isinstance(item.get("pattern"), dict)
            and bool((item.get("validation") or {}).get("valid"))
        ]

    def _pending_candidates(self, patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        candidates = []
        for item in dedupe_job_listing_patterns(patterns):
            if not isinstance(item, dict):
                continue
            if not str(item.get("page_url") or "").strip():
                continue
            if item.get("status") in {"pattern_ready", "extraction_completed", "pattern_repaired"} and bool(
                (item.get("validation") or {}).get("valid")
            ):
                continue
            if item.get("status") in {"no_jobs_listed", "inactive_no_jobs", "not_job_listing_page"}:
                continue
            candidates.append(item)
        return candidates

    def _queue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        created_tasks = []
        completed = 0
        failed = 0
        blocked = 0
        for task in tasks:
            if task["status"] == "completed":
                completed += 1
                self._mark_reused_task(task)
                continue
            if task["status"] == "blocked":
                blocked += 1
                self._mark_blocked_task(task)
                continue
            if task["status"] == "failed":
                failed += 1
                continue
            if self._queue_task(task):
                created_tasks.append(task)
                continue
            blocked += 1
        return {
            "created": len(created_tasks),
            "completed": completed,
            "failed": failed,
            "blocked": blocked,
            "created_tasks": created_tasks,
        }

    def _mark_reused_task(self, task: dict[str, Any]) -> None:
        timestamp = _now()
        self._domain_tasks.update_one(
            {"registered_domain": task["registered_domain"]},
            {
                "$set": {
                    "job_pattern_status": "completed",
                    "job_pattern_reused": True,
                    "job_pattern_last_completed_at": timestamp,
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {"job_pattern_last_error": "", "job_pattern_last_failure_type": ""},
            },
        )

    def _mark_blocked_task(self, task: dict[str, Any]) -> None:
        timestamp = _now()
        self._domain_tasks.update_one(
            {"registered_domain": task["registered_domain"]},
            {
                "$set": {
                    "job_pattern_status": "blocked_no_listing_page",
                    "job_pattern_last_error": task["last_error"],
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {"job_pattern_last_failure_type": ""},
            },
        )

    def _queue_task(self, task: dict[str, Any]) -> bool:
        timestamp = _now()
        result = self._domain_tasks.update_one(
            {
                "registered_domain": task["registered_domain"],
                "$or": [
                    {"job_pattern_status": {"$exists": False}},
                    {"job_pattern_status": {"$in": ["failed", "completed", "blocked_no_listing_page"]}},
                ],
            },
            {
                "$set": {
                    "job_pattern_status": "queued",
                    "job_pattern_input": task["input"],
                    "job_pattern_attempts": 0,
                    "job_pattern_reused": False,
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {
                    "job_pattern_last_error": "",
                    "job_pattern_last_completed_at": "",
                    "job_pattern_result": "",
                },
            },
        )
        return bool(result.modified_count)

    def _start_process_run(self, process_id: str, summary: dict[str, Any]) -> None:
        queued = int(summary.get("created") or 0)
        completed = int(summary.get("completed") or 0)
        failed = int(summary.get("failed") or 0)
        blocked = int(summary.get("blocked") or 0)
        status = "running" if queued else self._terminal_status(completed=completed, failed=failed, blocked=blocked)
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "job_pattern_status": status,
                    "job_pattern_totals": {
                        "domains": queued + completed + failed + blocked,
                        "queued": queued,
                        "running": 0,
                        "completed": completed,
                        "failed": failed,
                        "blocked": blocked,
                    },
                    "job_pattern_started_at": _now(),
                    "job_pattern_completed_at": _now() if status != "running" else None,
                    "updated_at": _now(),
                },
                "$unset": {"job_pattern_last_error": ""},
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
                    {"job_pattern_status": "queued"},
                    {"job_pattern_status": "running", "job_pattern_last_started_at": {"$lt": stale_threshold}},
                ],
            },
            {
                "$set": {
                    "job_pattern_status": "running",
                    "job_pattern_worker_name": worker_name,
                    "job_pattern_celery_task_id": task_id,
                    "job_pattern_last_started_at": timestamp,
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$inc": {"job_pattern_attempts": 1},
                "$unset": {"job_pattern_last_error": ""},
            },
            return_document=ReturnDocument.AFTER,
        )
        if not task:
            return {"status": "not_available"}
        if int(task.get("job_pattern_attempts") or 0) > retry_policy("job_pattern", self._settings.task_max_attempts).max_attempts:
            self.fail_task(registered_domain, "Maximum attempts exceeded")
            return {"status": "max_attempts_exceeded"}
        return {"status": "claimed", "task": self._task_from_domain_task(task)}

    def _task_from_domain_task(self, task: dict[str, Any]) -> dict[str, Any]:
        return {
            "registered_domain": task["registered_domain"],
            "domain": task.get("domain"),
            "input": task.get("job_pattern_input") or {},
            "worker_name": task.get("job_pattern_worker_name"),
            "celery_task_id": task.get("job_pattern_celery_task_id"),
            "job_pattern_attempts": task.get("job_pattern_attempts"),
        }

    def mark_process_task_running(self, process_id: str) -> None:
        self._processes.update_one(
            {"process_id": process_id, "job_pattern_totals.queued": {"$gt": 0}},
            {"$inc": {"job_pattern_totals.queued": -1, "job_pattern_totals.running": 1}, "$set": {"job_pattern_status": "running", "updated_at": _now()}},
        )

    def complete_task(self, registered_domain: str, result: dict[str, Any], process_id: str | None = None) -> None:
        status = "failed" if result.get("status") == "failed" else "completed"
        merged_patterns = self._merged_patterns(registered_domain, result.get("job_listing_patterns") or [])
        failed_patterns = [item for item in result.get("job_listing_patterns") or [] if item.get("status") != "pattern_ready"]
        last_error = " | ".join(
            str(item.get("last_error") or item.get("status") or "Pattern generation failed")
            for item in failed_patterns
        )
        fields: dict[str, Any] = {
            "job_pattern_status": status,
            "job_pattern_result": result,
            "job_pattern_last_completed_at": _now(),
            "job_pattern_updated_at": _now(),
            "career_process.job_listing_patterns": merged_patterns,
            "updated_at": _now(),
        }
        if status == "failed":
            fields["job_pattern_last_error"] = last_error or "Pattern generation failed"
            fields["job_pattern_last_failure_type"] = classify_failure(fields["job_pattern_last_error"])
        self._domain_tasks.update_one(
            {"registered_domain": registered_domain, "job_pattern_status": "running"},
            {
                "$set": fields,
                "$unset": self._completion_unset_fields(status),
            },
        )
        if process_id:
            self._move_process_counter(process_id, status)

    def _completion_unset_fields(self, status: str) -> dict[str, str]:
        fields = {"job_pattern_worker_name": "", "job_pattern_celery_task_id": ""}
        if status == "completed":
            fields["job_pattern_last_error"] = ""
            fields["job_pattern_last_failure_type"] = ""
        return fields

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
                    "job_pattern_status": "failed",
                    "job_pattern_last_error": error,
                    "job_pattern_last_failure_type": classify_failure(error),
                    "job_pattern_updated_at": _now(),
                    "updated_at": _now(),
                },
                "$unset": {"job_pattern_worker_name": "", "job_pattern_celery_task_id": ""},
            },
        )
        if process_id:
            self._move_process_counter(process_id, "failed", error=error)

    def requeue_task(self, registered_domain: str, error: str, *, decrement_attempt: bool = False) -> None:
        update: dict[str, Any] = {
            "$set": {
                "job_pattern_status": "queued",
                "job_pattern_last_error": error,
                "job_pattern_last_failure_type": classify_failure(error),
                "job_pattern_updated_at": _now(),
                "updated_at": _now(),
            },
            "$unset": {"job_pattern_worker_name": "", "job_pattern_celery_task_id": ""},
        }
        if decrement_attempt:
            update["$inc"] = {"job_pattern_attempts": -1}
        self._domain_tasks.update_one({"registered_domain": registered_domain, "job_pattern_status": "running"}, update)

    def mark_process_task_requeued(self, process_id: str, error: str | None = None) -> None:
        update = {"updated_at": _now()}
        if error:
            update["job_pattern_last_error"] = error
        self._processes.update_one(
            {"process_id": process_id, "job_pattern_totals.running": {"$gt": 0}},
            {"$inc": {"job_pattern_totals.running": -1, "job_pattern_totals.queued": 1}, "$set": update},
        )

    def mark_process_queued_task_failed(self, process_id: str, error: str | None = None) -> None:
        update = {"updated_at": _now()}
        if error:
            update["job_pattern_last_error"] = error
        self._processes.update_one(
            {"process_id": process_id, "job_pattern_totals.queued": {"$gt": 0}},
            {"$inc": {"job_pattern_totals.queued": -1, "job_pattern_totals.failed": 1}, "$set": update},
        )
        self._refresh_process_status(process_id)

    def requeue_stale_tasks(self) -> int:
        self.ensure_indexes()
        threshold = _now() - timedelta(seconds=self._settings.stale_task_seconds)
        result = self._domain_tasks.update_many(
            {"job_pattern_status": "running", "job_pattern_last_started_at": {"$lt": threshold}},
            {
                "$set": {"job_pattern_status": "queued", "job_pattern_last_error": "Requeued stale job pattern task", "job_pattern_updated_at": _now(), "updated_at": _now()},
                "$unset": {"job_pattern_worker_name": "", "job_pattern_celery_task_id": ""},
            },
        )
        if result.modified_count:
            log_event(logger, "warning", "stale_job_pattern_tasks_requeued", domain="watchdog", count=result.modified_count)
        return int(result.modified_count)

    def queued_tasks_for_watchdog(self) -> list[dict[str, Any]]:
        self.ensure_indexes()
        return list(self._domain_tasks.find({"job_pattern_status": "queued"}))

    def _move_process_counter(self, process_id: str, target: str, error: str | None = None) -> None:
        update = {"updated_at": _now()}
        if error:
            update["job_pattern_last_error"] = error
        self._processes.update_one(
            {"process_id": process_id, "job_pattern_totals.running": {"$gt": 0}},
            {"$inc": {"job_pattern_totals.running": -1, f"job_pattern_totals.{target}": 1}, "$set": update},
        )
        self._refresh_process_status(process_id)

    def _refresh_process_status(self, process_id: str) -> None:
        process = self._processes.find_one({"process_id": process_id}, {"job_pattern_totals": 1})
        totals = (process or {}).get("job_pattern_totals") or {}
        status = self._status_from_totals(totals)
        update = {"job_pattern_status": status, "updated_at": _now()}
        if status in {"completed", "partial_completed", "failed"}:
            update["job_pattern_completed_at"] = _now()
        self._processes.update_one({"process_id": process_id}, {"$set": update})

    def _status_from_totals(self, totals: dict[str, Any]) -> str:
        return status_from_totals(totals)

    def _terminal_status(self, *, completed: int, failed: int, blocked: int) -> str:
        return terminal_status(completed=completed, failed=failed, blocked=blocked)

    def _dispatch_tasks(self, process_id: str, tasks: list[dict[str, Any]]) -> None:
        for task in tasks:
            self._dispatch_task(process_id, task)

    def _dispatch_task(self, process_id: str, task: dict[str, Any]) -> None:
        from infrastructure.tasks import run_job_pattern_node

        log_event(logger, "info", "job_pattern_domain_dispatched", domain="job_pattern", process_id=process_id, registered_domain=task["registered_domain"])
        run_job_pattern_node.apply_async(args=[process_id, task["registered_domain"]], queue="processes")


@lru_cache(maxsize=1)
def get_job_pattern_node_service() -> JobPatternNodeService:
    return JobPatternNodeService(get_sync_mongodb_service(), get_settings())
