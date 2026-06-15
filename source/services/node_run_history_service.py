from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING

from core.config import Settings, get_settings
from services.failure_classifier import classify_failure
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("node_run_history_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class NodeRunHistoryService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._runs = mongodb.collection(settings.mongodb_node_runs_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._runs.create_index([("run_id", ASCENDING)], unique=True)
        self._runs.create_index([("process_id", ASCENDING), ("node", ASCENDING), ("started_at", DESCENDING)])
        self._runs.create_index([("registered_domain", ASCENDING), ("node", ASCENDING), ("started_at", DESCENDING)])
        self._runs.create_index([("status", ASCENDING), ("updated_at", DESCENDING)])
        self._indexes_ready = True

    def start_run(
        self,
        *,
        node: str,
        process_id: str | None,
        registered_domain: str | None,
        worker_name: str | None = None,
        celery_task_id: str | None = None,
        attempt: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        self.ensure_indexes()
        run_id = uuid4().hex
        timestamp = _now()
        self._runs.insert_one(
            {
                "run_id": run_id,
                "node": node,
                "process_id": process_id,
                "registered_domain": registered_domain,
                "worker_name": worker_name,
                "celery_task_id": celery_task_id,
                "attempt": attempt,
                "status": "running",
                "failure_type": None,
                "metadata": metadata or {},
                "started_at": timestamp,
                "updated_at": timestamp,
            }
        )
        log_event(logger, "info", "node_run_started", domain="node_run", node=node, run_id=run_id, process_id=process_id, registered_domain=registered_domain)
        return run_id

    def complete_run(self, run_id: str, result: dict[str, Any] | None = None) -> None:
        self._finish_run(run_id, "completed", result=result)

    def fail_run(self, run_id: str, error: str, result: dict[str, Any] | None = None) -> None:
        self._finish_run(run_id, "failed", error=error, result=result)

    def requeue_run(self, run_id: str, error: str) -> None:
        self._finish_run(run_id, "requeued", error=error)

    def _finish_run(
        self,
        run_id: str,
        status: str,
        *,
        error: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        timestamp = _now()
        update: dict[str, Any] = {
            "status": status,
            "completed_at": timestamp,
            "updated_at": timestamp,
        }
        if error:
            update["error"] = error
            update["failure_type"] = classify_failure(error)
        if result:
            update["duration_seconds"] = result.get("duration_seconds")
            update["result_summary"] = self._result_summary(result)
        self._runs.update_one({"run_id": run_id}, {"$set": update})

    def recent_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self.ensure_indexes()
        cursor = self._runs.find({}, {"_id": 0}).sort("started_at", DESCENDING).limit(limit)
        return list(cursor)

    def counts_by_node_status(self) -> list[dict[str, Any]]:
        self.ensure_indexes()
        rows = self._runs.aggregate(
            [
                {"$group": {"_id": {"node": "$node", "status": "$status"}, "count": {"$sum": 1}}},
                {"$sort": {"_id.node": 1, "_id.status": 1}},
            ]
        )
        return [{"node": row["_id"]["node"], "status": row["_id"]["status"], "count": int(row["count"])} for row in rows]

    def _result_summary(self, result: dict[str, Any]) -> dict[str, Any]:
        summary = {
            "status": result.get("status"),
            "success": result.get("success"),
            "career_url": result.get("career_url"),
            "job_count": result.get("job_count"),
            "job_storage": result.get("job_storage"),
            "pattern_count": len(result.get("job_listing_patterns") or []),
            "jobs_found": result.get("jobs_found"),
            "outcome": result.get("outcome"),
            "total_jobs_found": result.get("total_jobs_found"),
        }
        return {key: value for key, value in summary.items() if value is not None}


@lru_cache(maxsize=1)
def get_node_run_history_service() -> NodeRunHistoryService:
    return NodeRunHistoryService(get_sync_mongodb_service(), get_settings())
