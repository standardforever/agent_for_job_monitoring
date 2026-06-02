from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, ReturnDocument

from core.config import Settings, get_settings
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("process_node_task_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ProcessNodeTaskService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._tasks = mongodb.collection(settings.mongodb_process_node_tasks_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._tasks.create_index(
            [("process_id", ASCENDING), ("node", ASCENDING), ("registered_domain", ASCENDING)],
            unique=True,
        )
        self._tasks.create_index([("node", ASCENDING), ("status", ASCENDING), ("updated_at", ASCENDING)])
        self._indexes_ready = True

    def upsert_category_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, int]:
        self.ensure_indexes()
        created = 0
        failed = 0
        for task in tasks:
            if self._upsert_task(task):
                created += 1
            if task["status"] == "failed":
                failed += 1
        return {"created": created, "failed": failed}

    def _upsert_task(self, task: dict[str, Any]) -> bool:
        timestamp = _now()
        document = {
            **task,
            "attempts": 0,
            "created_at": timestamp,
            "updated_at": timestamp,
        }
        result = self._tasks.update_one(
            self._task_key(task),
            {"$setOnInsert": document},
            upsert=True,
        )
        return bool(result.upserted_id)

    def _task_key(self, task: dict[str, Any]) -> dict[str, Any]:
        return {
            "process_id": task["process_id"],
            "node": task["node"],
            "registered_domain": task["registered_domain"],
        }

    def queued_category_tasks(self, process_id: str) -> list[dict[str, Any]]:
        self.ensure_indexes()
        return list(
            self._tasks.find(
                {
                    "process_id": process_id,
                    "node": "career_page_category",
                    "status": "queued",
                }
            )
        )

    def claim_category_task(self, process_id: str, registered_domain: str, worker_name: str, task_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        timestamp = _now()
        stale_threshold = timestamp - timedelta(seconds=self._settings.stale_task_seconds)
        task = self._tasks.find_one_and_update(
            {
                "process_id": process_id,
                "node": "career_page_category",
                "registered_domain": registered_domain,
                "$or": [
                    {"status": "queued"},
                    {"status": "running", "last_started_at": {"$lt": stale_threshold}},
                ],
            },
            {
                "$set": {
                    "status": "running",
                    "worker_name": worker_name,
                    "celery_task_id": task_id,
                    "last_started_at": timestamp,
                    "updated_at": timestamp,
                    "last_error": None,
                },
                "$inc": {"attempts": 1},
            },
            return_document=ReturnDocument.AFTER,
        )
        if not task:
            return {"status": "not_available"}
        if int(task.get("attempts") or 0) > self._settings.task_max_attempts:
            self.fail_task(process_id, registered_domain, "Maximum attempts exceeded")
            return {"status": "max_attempts_exceeded"}
        return {"status": "claimed", "task": task}

    def complete_task(self, process_id: str, registered_domain: str, result: dict[str, Any]) -> None:
        self._tasks.update_one(
            self._task_filter(process_id, registered_domain, "running"),
            {
                "$set": {
                    "status": "completed",
                    "result": result,
                    "last_completed_at": _now(),
                    "updated_at": _now(),
                    "last_error": None,
                },
                "$unset": {"worker_name": "", "celery_task_id": ""},
            },
        )

    def fail_task(self, process_id: str, registered_domain: str, error: str) -> None:
        self._tasks.update_one(
            {
                "process_id": process_id,
                "node": "career_page_category",
                "registered_domain": registered_domain,
            },
            {
                "$set": {
                    "status": "failed",
                    "last_error": error,
                    "failed_at": _now(),
                    "updated_at": _now(),
                },
                "$unset": {"worker_name": "", "celery_task_id": ""},
            },
        )

    def requeue_task(
        self,
        process_id: str,
        registered_domain: str,
        error: str,
        *,
        decrement_attempt: bool = False,
    ) -> None:
        update: dict[str, Any] = {
            "$set": {
                "status": "queued",
                "last_error": error,
                "updated_at": _now(),
            },
            "$unset": {"worker_name": "", "celery_task_id": ""},
        }
        if decrement_attempt:
            update["$inc"] = {"attempts": -1}
        self._tasks.update_one(
            self._task_filter(process_id, registered_domain, "running"),
            update,
        )

    def requeue_stale_category_tasks(self) -> int:
        self.ensure_indexes()
        threshold = _now() - timedelta(seconds=self._settings.stale_task_seconds)
        result = self._tasks.update_many(
            {
                "node": "career_page_category",
                "status": "running",
                "last_started_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "status": "queued",
                    "last_error": "Requeued stale category task",
                    "updated_at": _now(),
                },
                "$unset": {"worker_name": "", "celery_task_id": ""},
            },
        )
        if result.modified_count:
            log_event(
                logger,
                "warning",
                "stale_category_tasks_requeued",
                domain="watchdog",
                count=result.modified_count,
            )
        return int(result.modified_count)

    def queued_category_processes(self) -> list[dict[str, Any]]:
        self.ensure_indexes()
        return list(
            self._tasks.aggregate(
                [
                    {"$match": {"node": "career_page_category", "status": "queued"}},
                    {"$group": {"_id": "$process_id", "domains": {"$push": "$registered_domain"}}},
                    {"$project": {"_id": 0, "process_id": "$_id", "domains": 1}},
                ]
            )
        )

    def _task_filter(self, process_id: str, registered_domain: str, status: str) -> dict[str, Any]:
        return {
            "process_id": process_id,
            "node": "career_page_category",
            "registered_domain": registered_domain,
            "status": status,
        }


@lru_cache(maxsize=1)
def get_process_node_task_service() -> ProcessNodeTaskService:
    return ProcessNodeTaskService(get_sync_mongodb_service(), get_settings())
