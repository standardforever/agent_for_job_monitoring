from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from core.config import Settings, get_settings
from services.failure_classifier import classify_failure
from services.job_listing_pattern_store import dedupe_job_listing_patterns, merge_job_listing_patterns
from services.node_lifecycle import retry_policy, status_from_totals, terminal_status
from services.node_preflight_service import get_node_preflight_service
from services.process_control_service import get_process_control_service
from services.process_domain_ref_service import get_process_domain_ref_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("job_pattern_node_service")
PATTERN_STALE_AFTER_DAYS = 7


def _now() -> datetime:
    return datetime.now(timezone.utc)


class JobPatternNodeService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._category_runs = mongodb.collection(settings.mongodb_career_category_runs_collection)
        self._pattern_runs = mongodb.collection(settings.mongodb_job_pattern_runs_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._domain_tasks.create_index([("job_pattern_status", ASCENDING), ("job_pattern_updated_at", ASCENDING)])
        self._pattern_runs.create_index([("job_pattern_run_key", ASCENDING)], unique=True)
        self._pattern_runs.create_index([("registered_domain", ASCENDING), ("status", ASCENDING)])
        self._pattern_runs.create_index([("status", ASCENDING), ("updated_at", ASCENDING)])
        self._pattern_runs.create_index([("last_started_at", ASCENDING)])
        self._indexes_ready = True

    def start_process(self, process_id: str, *, mode: str = "start") -> dict[str, Any]:
        self.ensure_indexes()
        get_node_preflight_service().require_client_openai_config(process_id)
        process = self._load_ready_process(process_id)
        normalized_mode = self._normalize_mode(mode)
        tasks = self._build_tasks(process, normalized_mode)
        summary = self._queue_tasks(tasks)
        self._start_process_run(process_id, summary, mode=normalized_mode)
        self._dispatch_tasks(process_id, summary["created_tasks"])
        return {
            "process_id": process_id,
            "mode": normalized_mode,
            "created": summary["created"],
            "reused": summary["completed"],
            "failed": summary["failed"],
            "blocked": summary["blocked"],
            "enqueued": len(summary["created_tasks"]),
        }

    def _normalize_mode(self, mode: str) -> str:
        normalized = str(mode or "start").strip().lower()
        if normalized in {"start", "rerun", "force"}:
            return normalized
        raise RuntimeError(f"Unsupported job pattern mode: {mode}")

    def _load_ready_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        if process.get("job_pattern_status") in {"queued", "running"}:
            raise RuntimeError("Job pattern node is already running for this process")
        if process.get("career_status") not in {"completed", "partial_completed"}:
            raise RuntimeError("Career category node must complete before job pattern starts")
        return process

    def _build_tasks(self, process: dict[str, Any], mode: str) -> list[dict[str, Any]]:
        tasks = []
        refs = get_process_control_service().filter_refs(
            process["process_id"],
            get_process_domain_ref_service().refs_for_process(process["process_id"], statuses=["completed"]),
            "job_pattern",
        )
        for ref in refs:
            task = self._task_from_ref(process, ref, mode)
            if task:
                tasks.append(task)
        return tasks

    def _task_from_ref(self, process: dict[str, Any], ref: dict[str, Any], mode: str) -> dict[str, Any] | None:
        category_result = self._category_result(ref["registered_domain"])
        if not category_result:
            return self._blocked_task(process, ref, "Career category result is unavailable")
        if not self._category_allows_pattern(category_result):
            return self._blocked_task(process, ref, self._category_block_reason(category_result))
        patterns = category_result.get("job_listing_patterns") or []
        ready_patterns = self._ready_patterns(patterns, mode)
        if ready_patterns:
            return {
                "process_id": process["process_id"],
                "registered_domain": ref["registered_domain"],
                "domain": ref.get("domain"),
                "status": "completed",
                "last_error": None,
                "input": {"job_listing_patterns": ready_patterns},
            }
        candidates = self._pending_candidates(patterns, mode)
        if not candidates:
            return self._blocked_task(process, ref, "No job listing page was identified by career category")
        return {
            "process_id": process["process_id"],
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "status": "queued",
            "last_error": None,
            "input": {"job_listing_patterns": candidates, "mode": mode},
        }

    def _blocked_task(self, process: dict[str, Any], ref: dict[str, Any], reason: str) -> dict[str, Any]:
        return {
            "process_id": process["process_id"],
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "status": "blocked",
            "last_error": reason,
            "input": {"job_listing_patterns": []},
        }

    def _category_result(self, registered_domain: str) -> dict[str, Any]:
        category_run = self._category_runs.find_one(
            {"category_run_key": self._category_run_key(registered_domain), "status": "completed"},
            {"result": 1},
        )
        result = (category_run or {}).get("result")
        if isinstance(result, dict) and result:
            return result
        domain_task = self._domain_tasks.find_one(
            {"registered_domain": registered_domain, "career_process_status": "completed"},
            {
                "career_process.outcome": 1,
                "career_process.outcome_category": 1,
                "career_process.change_judgement": 1,
                "career_process.job_listing_patterns": 1,
            },
        )
        result = (domain_task or {}).get("career_process")
        return result if isinstance(result, dict) else {}

    def _category_allows_pattern(self, career_process: dict[str, Any]) -> bool:
        if not career_process:
            return False
        if career_process.get("jobs_found"):
            return True
        return str(career_process.get("outcome_category") or "") == "jobs_available"

    def _category_block_reason(self, career_process: dict[str, Any]) -> str:
        judgement = career_process.get("change_judgement") or {}
        outcome = career_process.get("outcome") or "unknown"
        return str(
            judgement.get("judgement")
            or f"Career category latest outcome is not jobs_available: {outcome}"
        )

    def _ready_patterns(self, patterns: list[dict[str, Any]], mode: str) -> list[dict[str, Any]]:
        return [
            item
            for item in dedupe_job_listing_patterns(patterns)
            if self._is_reusable_pattern(item, mode)
        ]

    def _pending_candidates(self, patterns: list[dict[str, Any]], mode: str) -> list[dict[str, Any]]:
        candidates = []
        for item in dedupe_job_listing_patterns(patterns):
            if not isinstance(item, dict):
                continue
            if not str(item.get("page_url") or "").strip():
                continue
            if self._is_reusable_pattern(item, mode):
                continue
            if item.get("status") in {"no_jobs_listed", "inactive_no_jobs", "not_job_listing_page"}:
                continue
            candidates.append({**item, "regeneration_mode": mode})
        return candidates

    def _is_reusable_pattern(self, item: Any, mode: str) -> bool:
        if mode == "force":
            return False
        if not isinstance(item, dict):
            return False
        if item.get("status") not in {"pattern_ready", "pagination_completed", "extraction_completed", "pattern_repaired"}:
            return False
        if not isinstance(item.get("pattern"), dict):
            return False
        if not bool((item.get("validation") or {}).get("valid")):
            return False
        if mode == "rerun" and self._pattern_is_stale(item):
            return False
        return True

    def _pattern_is_stale(self, pattern: dict[str, Any]) -> bool:
        last_validated_at = self._parse_datetime(pattern.get("last_validated_at") or pattern.get("generated_at"))
        if not last_validated_at:
            return True
        return last_validated_at < _now() - timedelta(days=PATTERN_STALE_AFTER_DAYS)

    def _parse_datetime(self, value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

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
        result = {"job_listing_patterns": task["input"].get("job_listing_patterns") or [], "source": "reused"}
        self._pattern_runs.update_one(
            {"job_pattern_run_key": self._pattern_run_key(task["registered_domain"])},
            {
                "$set": {
                    "job_pattern_run_key": self._pattern_run_key(task["registered_domain"]),
                    "node": "job_pattern",
                    "registered_domain": task["registered_domain"],
                    "domain": task.get("domain"),
                    "last_process_id": task.get("process_id"),
                    "status": "completed",
                    "reused": True,
                    "mode": "reuse",
                    "input": task["input"],
                    "result": result,
                    "last_completed_at": timestamp,
                    "updated_at": timestamp,
                },
                "$setOnInsert": {"created_at": timestamp, "attempts": 0, "run_count": 0},
                "$unset": {"last_error": "", "last_failure_type": "", "last_error_details": ""},
            },
            upsert=True,
        )
        self._mirror_reused_task(task, result, timestamp)

    def _mark_blocked_task(self, task: dict[str, Any]) -> None:
        timestamp = _now()
        error_details = self._error_details(task["last_error"])
        self._pattern_runs.update_one(
            {"job_pattern_run_key": self._pattern_run_key(task["registered_domain"])},
            {
                "$set": {
                    "job_pattern_run_key": self._pattern_run_key(task["registered_domain"]),
                    "node": "job_pattern",
                    "registered_domain": task["registered_domain"],
                    "domain": task.get("domain"),
                    "last_process_id": task.get("process_id"),
                    "status": "blocked_no_listing_page",
                    "input": task["input"],
                    "last_error": task["last_error"],
                    "last_error_details": error_details,
                    "updated_at": timestamp,
                },
                "$setOnInsert": {"created_at": timestamp, "attempts": 0, "run_count": 0},
                "$unset": {"last_failure_type": ""},
            },
            upsert=True,
        )
        self._mirror_blocked_task(task, error_details, timestamp)

    def _queue_task(self, task: dict[str, Any]) -> bool:
        timestamp = _now()
        try:
            result = self._pattern_runs.update_one(
                {
                    "job_pattern_run_key": self._pattern_run_key(task["registered_domain"]),
                    "$or": [
                        {"status": {"$exists": False}},
                        {"status": {"$in": ["failed", "completed", "blocked_no_listing_page"]}},
                    ],
                },
                {
                    "$set": {
                        "job_pattern_run_key": self._pattern_run_key(task["registered_domain"]),
                        "node": "job_pattern",
                        "registered_domain": task["registered_domain"],
                        "domain": task.get("domain"),
                        "last_process_id": task.get("process_id"),
                        "status": "queued",
                        "input": task["input"],
                        "attempts": 0,
                        "reused": False,
                        "mode": task["input"].get("mode") or "start",
                        "queued_at": timestamp,
                        "updated_at": timestamp,
                    },
                    "$setOnInsert": {"created_at": timestamp, "first_started_at": None, "run_count": 0},
                    "$unset": {
                        "last_error": "",
                        "last_completed_at": "",
                        "result": "",
                        "dispatched_at": "",
                        "last_error_details": "",
                        "worker_name": "",
                        "celery_task_id": "",
                        "heartbeat_at": "",
                        "lease_expires_at": "",
                    },
                },
                upsert=True,
            )
        except DuplicateKeyError:
            return False
        queued = bool(result.modified_count or result.upserted_id)
        if queued:
            self._mirror_queued_task(task, timestamp)
        return queued

    def _start_process_run(self, process_id: str, summary: dict[str, Any], *, mode: str) -> None:
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
                    "job_pattern_mode": mode,
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

    def mark_task_dispatched(self, registered_domain: str) -> bool:
        threshold = _now() - timedelta(seconds=max(30, self._settings.watchdog_interval_seconds * 2))
        timestamp = _now()
        result = self._pattern_runs.update_one(
            {
                "job_pattern_run_key": self._pattern_run_key(registered_domain),
                "status": "queued",
                "$or": [
                    {"dispatched_at": {"$exists": False}},
                    {"dispatched_at": {"$lt": threshold}},
                ],
            },
            {"$set": {"dispatched_at": timestamp, "updated_at": timestamp}},
        )
        if result.modified_count:
            self._mirror_dispatched_task(registered_domain, timestamp)
        return bool(result.modified_count)

    def claim_task(self, registered_domain: str, worker_name: str, task_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        timestamp = _now()
        stale_threshold = timestamp - timedelta(seconds=self._settings.stale_task_seconds)
        task = self._pattern_runs.find_one_and_update(
            {
                "job_pattern_run_key": self._pattern_run_key(registered_domain),
                "$or": [
                    {"status": "queued"},
                    {"status": "running", "lease_expires_at": {"$lt": timestamp}},
                    {
                        "status": "running",
                        "lease_expires_at": {"$exists": False},
                        "last_started_at": {"$lt": stale_threshold},
                    },
                ],
            },
            {
                "$set": {
                    "status": "running",
                    "worker_name": worker_name,
                    "celery_task_id": task_id,
                    "last_started_at": timestamp,
                    "heartbeat_at": timestamp,
                    "lease_expires_at": timestamp + timedelta(seconds=self._settings.stale_task_seconds),
                    "updated_at": timestamp,
                },
                "$inc": {"attempts": 1, "run_count": 1},
                "$unset": {"last_error": "", "dispatched_at": ""},
            },
            return_document=ReturnDocument.AFTER,
        )
        if not task:
            return {"status": "not_available"}
        if not task.get("first_started_at"):
            self._pattern_runs.update_one(
                {"job_pattern_run_key": self._pattern_run_key(registered_domain)},
                {"$set": {"first_started_at": timestamp}},
            )
            task["first_started_at"] = timestamp
        self._mirror_running_task(task, timestamp)
        if int(task.get("attempts") or 0) > retry_policy("job_pattern", self._settings.task_max_attempts).max_attempts:
            self.fail_task(registered_domain, "Maximum attempts exceeded")
            return {"status": "max_attempts_exceeded"}
        return {"status": "claimed", "task": self._task_from_domain_task(task)}

    def _task_from_domain_task(self, task: dict[str, Any]) -> dict[str, Any]:
        return {
            "registered_domain": task["registered_domain"],
            "domain": task.get("domain"),
            "input": task.get("input") or task.get("job_pattern_input") or {},
            "job_pattern_mode": task.get("mode") or task.get("job_pattern_mode") or (task.get("input") or task.get("job_pattern_input") or {}).get("mode"),
            "worker_name": task.get("worker_name") or task.get("job_pattern_worker_name"),
            "celery_task_id": task.get("celery_task_id") or task.get("job_pattern_celery_task_id"),
            "job_pattern_attempts": task.get("attempts") or task.get("job_pattern_attempts"),
            "job_pattern_run_key": task.get("job_pattern_run_key"),
        }

    def heartbeat_task(self, registered_domain: str) -> None:
        timestamp = _now()
        expires_at = timestamp + timedelta(seconds=self._settings.stale_task_seconds)
        self._pattern_runs.update_one(
            {"job_pattern_run_key": self._pattern_run_key(registered_domain), "status": "running"},
            {"$set": {"heartbeat_at": timestamp, "lease_expires_at": expires_at, "updated_at": timestamp}},
        )
        self._domain_tasks.update_one(
            {"registered_domain": registered_domain, "job_pattern_status": "running"},
            {
                "$set": {
                    "job_pattern_heartbeat_at": timestamp,
                    "job_pattern_lease_expires_at": expires_at,
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                }
            },
        )

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
        timestamp = _now()
        fields: dict[str, Any] = {
            "status": status,
            "result": result,
            "last_completed_at": timestamp,
            "merged_job_listing_patterns": merged_patterns,
            "updated_at": timestamp,
        }
        if status == "failed":
            fields["last_error"] = last_error or "Pattern generation failed"
            fields["last_failure_type"] = classify_failure(fields["last_error"])
            fields["last_error_details"] = self._error_details(fields["last_error"], result)
        self._pattern_runs.update_one(
            {"job_pattern_run_key": self._pattern_run_key(registered_domain), "status": "running"},
            {
                "$set": fields,
                "$unset": self._completion_unset_fields(status, mirror=False),
            },
        )
        self._mirror_completed_task(registered_domain, status, result, merged_patterns, timestamp, last_error=fields.get("last_error"))
        if process_id:
            self._move_process_counter(process_id, status)

    def _completion_unset_fields(self, status: str, *, mirror: bool = True) -> dict[str, str]:
        if not mirror:
            fields = {"worker_name": "", "celery_task_id": "", "heartbeat_at": "", "lease_expires_at": ""}
            if status == "completed":
                fields["last_error"] = ""
                fields["last_failure_type"] = ""
                fields["last_error_details"] = ""
            return fields
        fields = {
            "job_pattern_worker_name": "",
            "job_pattern_celery_task_id": "",
            "job_pattern_heartbeat_at": "",
            "job_pattern_lease_expires_at": "",
        }
        if status == "completed":
            fields["job_pattern_last_error"] = ""
            fields["job_pattern_last_failure_type"] = ""
            fields["job_pattern_last_error_details"] = ""
        return fields

    def _merged_patterns(self, registered_domain: str, incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
        existing = self._category_result(registered_domain).get("job_listing_patterns") or []
        return merge_job_listing_patterns(existing, incoming)

    def fail_task(self, registered_domain: str, error: str, process_id: str | None = None) -> None:
        timestamp = _now()
        error_details = self._error_details(error)
        self._pattern_runs.update_one(
            {"job_pattern_run_key": self._pattern_run_key(registered_domain)},
            {
                "$set": {
                    "job_pattern_run_key": self._pattern_run_key(registered_domain),
                    "node": "job_pattern",
                    "registered_domain": registered_domain,
                    "status": "failed",
                    "last_error": error,
                    "last_failure_type": classify_failure(error),
                    "last_error_details": error_details,
                    "updated_at": timestamp,
                },
                "$setOnInsert": {"created_at": timestamp, "attempts": 0, "run_count": 0},
                "$unset": {"worker_name": "", "celery_task_id": ""},
            },
            upsert=True,
        )
        self._mirror_failed_task(registered_domain, error, error_details, timestamp)
        if process_id:
            self._move_process_counter(process_id, "failed", error=error)

    def requeue_task(self, registered_domain: str, error: str, *, decrement_attempt: bool = False) -> None:
        timestamp = _now()
        error_details = self._error_details(error)
        update: dict[str, Any] = {
            "$set": {
                "status": "queued",
                "last_error": error,
                "last_failure_type": classify_failure(error),
                "last_error_details": error_details,
                "updated_at": timestamp,
            },
            "$unset": {"worker_name": "", "celery_task_id": "", "heartbeat_at": "", "lease_expires_at": "", "dispatched_at": ""},
        }
        if decrement_attempt:
            update["$inc"] = {"attempts": -1}
        self._pattern_runs.update_one({"job_pattern_run_key": self._pattern_run_key(registered_domain), "status": "running"}, update)
        self._mirror_requeued_task(registered_domain, error, error_details, timestamp, decrement_attempt=decrement_attempt)

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
        timestamp = _now()
        stale_runs = list(
            self._pattern_runs.find(
                {
                    "status": "running",
                    "$or": [
                        {"lease_expires_at": {"$lt": timestamp}},
                        {"lease_expires_at": {"$exists": False}, "last_started_at": {"$lt": threshold}},
                    ],
                },
                {"registered_domain": 1},
            )
        )
        error = "Requeued stale job pattern task"
        error_details = self._error_details(error)
        result = self._pattern_runs.update_many(
            {
                "status": "running",
                "$or": [
                    {"lease_expires_at": {"$lt": timestamp}},
                    {"lease_expires_at": {"$exists": False}, "last_started_at": {"$lt": threshold}},
                ],
            },
            {
                "$set": {
                    "status": "queued",
                    "last_error": error,
                    "last_error_details": error_details,
                    "updated_at": timestamp,
                },
                "$unset": {"worker_name": "", "celery_task_id": "", "heartbeat_at": "", "lease_expires_at": "", "dispatched_at": ""},
            },
        )
        if result.modified_count:
            for run in stale_runs:
                self._mirror_requeued_task(str(run.get("registered_domain") or ""), error, error_details, timestamp)
            log_event(logger, "warning", "stale_job_pattern_tasks_requeued", domain="watchdog", count=result.modified_count)
        return int(result.modified_count)

    def queued_tasks_for_watchdog(self) -> list[dict[str, Any]]:
        self.ensure_indexes()
        return list(self._pattern_runs.find({"status": "queued"}))

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

        if not self.mark_task_dispatched(task["registered_domain"]):
            log_event(
                logger,
                "info",
                "job_pattern_dispatch_skipped",
                domain="job_pattern",
                process_id=process_id,
                registered_domain=task["registered_domain"],
                reason="recently_dispatched",
            )
            return
        log_event(logger, "info", "job_pattern_domain_dispatched", domain="job_pattern", process_id=process_id, registered_domain=task["registered_domain"])
        run_job_pattern_node.apply_async(args=[process_id, task["registered_domain"]], queue="processes")

    def _pattern_run_key(self, registered_domain: str) -> str:
        return f"shared:{registered_domain}"

    def _category_run_key(self, registered_domain: str) -> str:
        return f"shared:{registered_domain}"

    def _mirror_reused_task(self, task: dict[str, Any], result: dict[str, Any], timestamp: datetime) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": task["registered_domain"]},
            {
                "$set": {
                    "job_pattern_status": "completed",
                    "job_pattern_result": result,
                    "job_pattern_reused": True,
                    "job_pattern_mode": "reuse",
                    "job_pattern_last_completed_at": timestamp,
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {"job_pattern_last_error": "", "job_pattern_last_failure_type": "", "job_pattern_last_error_details": ""},
            },
            upsert=True,
        )

    def _mirror_blocked_task(self, task: dict[str, Any], error_details: dict[str, Any], timestamp: datetime) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": task["registered_domain"]},
            {
                "$set": {
                    "registered_domain": task["registered_domain"],
                    "domain": task.get("domain"),
                    "job_pattern_status": "blocked_no_listing_page",
                    "job_pattern_last_error": task["last_error"],
                    "job_pattern_last_error_details": error_details,
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {"job_pattern_last_failure_type": ""},
            },
            upsert=True,
        )

    def _mirror_queued_task(self, task: dict[str, Any], timestamp: datetime) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": task["registered_domain"]},
            {
                "$set": {
                    "registered_domain": task["registered_domain"],
                    "domain": task.get("domain"),
                    "job_pattern_status": "queued",
                    "job_pattern_input": task["input"],
                    "job_pattern_attempts": 0,
                    "job_pattern_reused": False,
                    "job_pattern_mode": task["input"].get("mode") or "start",
                    "job_pattern_queued_at": timestamp,
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {
                    "job_pattern_last_error": "",
                    "job_pattern_last_completed_at": "",
                    "job_pattern_result": "",
                    "job_pattern_dispatched_at": "",
                    "job_pattern_last_error_details": "",
                },
            },
            upsert=True,
        )

    def _mirror_dispatched_task(self, registered_domain: str, timestamp: datetime) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": registered_domain},
            {"$set": {"job_pattern_dispatched_at": timestamp, "job_pattern_updated_at": timestamp, "updated_at": timestamp}},
        )

    def _mirror_running_task(self, task: dict[str, Any], timestamp: datetime) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": task["registered_domain"]},
            {
                "$set": {
                    "job_pattern_status": "running",
                    "job_pattern_worker_name": task.get("worker_name"),
                    "job_pattern_celery_task_id": task.get("celery_task_id"),
                    "job_pattern_last_started_at": timestamp,
                    "job_pattern_heartbeat_at": timestamp,
                    "job_pattern_lease_expires_at": timestamp + timedelta(seconds=self._settings.stale_task_seconds),
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$inc": {"job_pattern_attempts": 1},
                "$unset": {"job_pattern_last_error": "", "job_pattern_dispatched_at": ""},
            },
            upsert=True,
        )

    def _mirror_completed_task(
        self,
        registered_domain: str,
        status: str,
        result: dict[str, Any],
        merged_patterns: list[dict[str, Any]],
        timestamp: datetime,
        *,
        last_error: str | None = None,
    ) -> None:
        fields: dict[str, Any] = {
            "job_pattern_status": status,
            "job_pattern_result": result,
            "job_pattern_last_completed_at": timestamp,
            "job_pattern_updated_at": timestamp,
            "career_process.job_listing_patterns": merged_patterns,
            "updated_at": timestamp,
        }
        if status == "failed":
            fields["job_pattern_last_error"] = last_error or "Pattern generation failed"
            fields["job_pattern_last_failure_type"] = classify_failure(fields["job_pattern_last_error"])
            fields["job_pattern_last_error_details"] = self._error_details(fields["job_pattern_last_error"], result)
        self._domain_tasks.update_one(
            {"registered_domain": registered_domain},
            {"$set": fields, "$unset": self._completion_unset_fields(status)},
            upsert=True,
        )

    def _mirror_failed_task(
        self,
        registered_domain: str,
        error: str,
        error_details: dict[str, Any],
        timestamp: datetime,
    ) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": registered_domain},
            {
                "$set": {
                    "registered_domain": registered_domain,
                    "job_pattern_status": "failed",
                    "job_pattern_last_error": error,
                    "job_pattern_last_failure_type": classify_failure(error),
                    "job_pattern_last_error_details": error_details,
                    "job_pattern_updated_at": timestamp,
                    "updated_at": timestamp,
                },
                "$unset": {"job_pattern_worker_name": "", "job_pattern_celery_task_id": ""},
            },
            upsert=True,
        )

    def _mirror_requeued_task(
        self,
        registered_domain: str,
        error: str,
        error_details: dict[str, Any],
        timestamp: datetime,
        *,
        decrement_attempt: bool = False,
    ) -> None:
        if not registered_domain:
            return
        update: dict[str, Any] = {
            "$set": {
                "job_pattern_status": "queued",
                "job_pattern_last_error": error,
                "job_pattern_last_failure_type": classify_failure(error),
                "job_pattern_last_error_details": error_details,
                "job_pattern_updated_at": timestamp,
                "updated_at": timestamp,
            },
            "$unset": {"job_pattern_worker_name": "", "job_pattern_celery_task_id": "", "job_pattern_dispatched_at": ""},
        }
        if decrement_attempt:
            update["$inc"] = {"job_pattern_attempts": -1}
        self._domain_tasks.update_one({"registered_domain": registered_domain}, update)

    def _error_details(self, error: str, result: dict[str, Any] | None = None) -> dict[str, Any]:
        failed_patterns = [item for item in (result or {}).get("job_listing_patterns") or [] if item.get("status") != "pattern_ready"]
        return {
            key: value
            for key, value in {
                "error": error,
                "failure_type": classify_failure(error),
                "failed_at": _now(),
                "failed_pattern_count": len(failed_patterns),
                "failed_pages": [
                    {
                        "page_url": item.get("page_url"),
                        "status": item.get("status"),
                        "last_error": item.get("last_error"),
                        "validation": item.get("validation"),
                        "diagnostics": item.get("diagnostics"),
                    }
                    for item in failed_patterns[:10]
                ],
            }.items()
            if value not in (None, "", [])
        }


@lru_cache(maxsize=1)
def get_job_pattern_node_service() -> JobPatternNodeService:
    return JobPatternNodeService(get_sync_mongodb_service(), get_settings())
