from __future__ import annotations

from functools import lru_cache
from typing import Any

import redis

from core.config import Settings, get_settings
from infrastructure.celery_app import celery_app
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("pipeline_observability_service")


class PipelineObservabilityService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._slots = mongodb.collection(settings.mongodb_selenium_session_slots_collection)

    def snapshot(self) -> dict[str, Any]:
        return {
            "processes": self._process_counts(),
            "domains": self._domain_counts(),
            "selenium_slots": self._slot_counts(),
            "queue": self._queue_stats(),
            "workers": self._worker_stats(),
        }

    def _process_counts(self) -> dict[str, int]:
        return self._count_by_field(self._processes, "status")

    def _domain_counts(self) -> dict[str, int]:
        return self._count_by_field(self._domain_tasks, "status")

    def _slot_counts(self) -> dict[str, int]:
        return self._count_by_field(self._slots, "status")

    def _count_by_field(self, collection: Any, field: str) -> dict[str, int]:
        rows = collection.aggregate([{"$group": {"_id": f"${field}", "count": {"$sum": 1}}}])
        return {str(row["_id"] or "unknown"): int(row["count"]) for row in rows}

    def _queue_stats(self) -> dict[str, int | str]:
        try:
            client = redis.Redis.from_url(self._settings.celery_broker_url)
            return {"name": "processes", "pending": int(client.llen("processes"))}
        except Exception as exc:
            log_event(logger, "warning", "queue_stats_failed", domain="observability", error=str(exc))
            return {"name": "processes", "pending": -1}

    def _worker_stats(self) -> dict[str, Any]:
        try:
            active = celery_app.control.inspect(timeout=1).active() or {}
            return {"online": len(active), "active_tasks": self._active_task_count(active)}
        except Exception as exc:
            log_event(logger, "warning", "worker_stats_failed", domain="observability", error=str(exc))
            return {"online": 0, "active_tasks": 0}

    def _active_task_count(self, active: dict[str, list[dict[str, Any]]]) -> int:
        return sum(len(tasks) for tasks in active.values())


@lru_cache(maxsize=1)
def get_pipeline_observability_service() -> PipelineObservabilityService:
    return PipelineObservabilityService(get_sync_mongodb_service(), get_settings())
