from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, ReturnDocument, UpdateOne

from core.config import Settings, get_settings
from services.failure_classifier import classify_failure
from services.node_lifecycle import retry_policy
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service


def _now() -> datetime:
    return datetime.now(timezone.utc)


class SearchRunService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._runs = mongodb.collection(settings.mongodb_search_runs_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._runs.create_index([("search_run_key", ASCENDING)], unique=True)
        self._runs.create_index([("registered_domain", ASCENDING), ("scope", ASCENDING)])
        self._runs.create_index([("status", ASCENDING), ("updated_at", ASCENDING)])
        self._runs.create_index([("last_completed_at", ASCENDING)])
        self._indexes_ready = True

    def initialize_domains(self, refs: list[dict[str, Any]]) -> None:
        self.ensure_indexes()
        operations = [self._initial_upsert(ref) for ref in refs if ref.get("registered_domain")]
        if operations:
            self._runs.bulk_write(operations, ordered=False)

    def _initial_upsert(self, ref: dict[str, Any]) -> UpdateOne:
        timestamp = _now()
        return UpdateOne(
            {"search_run_key": self.shared_key(ref["registered_domain"])},
            {
                "$setOnInsert": {
                    "search_run_key": self.shared_key(ref["registered_domain"]),
                    "registered_domain": ref["registered_domain"],
                    "scope": "shared_domain",
                    "status": "queued",
                    "attempts": 0,
                    "run_count": 0,
                    "first_started_at": None,
                    "last_started_at": None,
                    "last_completed_at": None,
                    "last_error": None,
                    "created_at": timestamp,
                },
                "$set": {"domain": ref.get("domain"), "updated_at": timestamp},
            },
            upsert=True,
        )

    def shared_key(self, registered_domain: str) -> str:
        return f"shared:{registered_domain}"

    def supplied_key(self, process_id: str, registered_domain: str) -> str:
        return f"process:{process_id}:{registered_domain}:supplied"

    def fresh_completed(self, registered_domain: str) -> dict[str, Any] | None:
        self.ensure_indexes()
        threshold = _now() - timedelta(hours=24)
        return self._runs.find_one(
            {
                "search_run_key": self.shared_key(registered_domain),
                "status": "completed",
                "last_completed_at": {"$gte": threshold},
            },
        )

    def completed_result(self, registered_domain: str) -> dict[str, Any]:
        self.ensure_indexes()
        run = self._runs.find_one(
            {
                "search_run_key": self.shared_key(registered_domain),
                "status": "completed",
            },
            {"result": 1, "career_url": 1, "career_urls": 1},
        )
        if not run:
            return {}
        result = run.get("result")
        if isinstance(result, dict):
            return result
        return {
            key: value
            for key, value in {
                "career_url": run.get("career_url"),
                "career_urls": run.get("career_urls"),
            }.items()
            if value not in (None, "", [])
        }

    def attempts(self, registered_domain: str) -> int:
        self.ensure_indexes()
        run = self._runs.find_one({"search_run_key": self.shared_key(registered_domain)}, {"attempts": 1})
        return int((run or {}).get("attempts") or 0)

    def claim_shared(self, ref: dict[str, Any], worker_name: str, task_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        completed = self.fresh_completed(ref["registered_domain"])
        if completed:
            return {"status": "fresh_completed", "run": completed}
        self.initialize_domains([ref])
        if self._attempts_exhausted(ref["registered_domain"]) and not self._last_attempt_failed(ref["registered_domain"]):
            return {"status": "max_attempts_exceeded"}
        claimed = self._claim_runnable(ref, worker_name, task_id)
        if not claimed:
            return {"status": "busy"}
        self._ensure_first_started(ref["registered_domain"], claimed.get("last_started_at") or _now())
        if int(claimed.get("attempts") or 0) > retry_policy("search", self._settings.task_max_attempts).max_attempts:
            self.mark_failed(ref, "Maximum attempts exceeded")
            return {"status": "max_attempts_exceeded"}
        return {"status": "claimed", "run": claimed}

    def _ensure_first_started(self, registered_domain: str, timestamp: datetime) -> None:
        self._runs.update_one(
            {
                "search_run_key": self.shared_key(registered_domain),
                "$or": [{"first_started_at": None}, {"first_started_at": {"$exists": False}}],
            },
            {"$set": {"first_started_at": timestamp}},
        )

    def _claim_runnable(self, ref: dict[str, Any], worker_name: str, task_id: str) -> dict[str, Any] | None:
        timestamp = _now()
        max_attempts = retry_policy("search", self._settings.task_max_attempts).max_attempts
        return self._runs.find_one_and_update(
            {
                "search_run_key": self.shared_key(ref["registered_domain"]),
                "$or": [
                    {"status": {"$in": ["queued", "failed"]}, "attempts": {"$lt": max_attempts}},
                    {"status": "running", "lease_expires_at": {"$lt": timestamp}},
                    {
                        "status": "running",
                        "lease_expires_at": {"$exists": False},
                        "last_started_at": {"$lt": timestamp - timedelta(seconds=self._settings.stale_task_seconds)},
                    },
                    {"status": "completed", "last_completed_at": {"$lt": timestamp - timedelta(hours=24)}},
                ],
            },
            {
                "$set": {
                    "domain": ref.get("domain"),
                    "registered_domain": ref["registered_domain"],
                    "scope": "shared_domain",
                    "status": "running",
                    "worker_name": worker_name,
                    "celery_task_id": task_id,
                    "last_started_at": timestamp,
                    "heartbeat_at": timestamp,
                    "lease_expires_at": timestamp + timedelta(seconds=self._settings.stale_task_seconds),
                    "updated_at": timestamp,
                    "last_error": None,
                },
                "$setOnInsert": {
                    "search_run_key": self.shared_key(ref["registered_domain"]),
                    "created_at": timestamp,
                    "first_started_at": timestamp,
                },
                "$inc": {"attempts": 1, "run_count": 1},
            },
            return_document=ReturnDocument.AFTER,
        )

    def _attempts_exhausted(self, registered_domain: str) -> bool:
        run = self._runs.find_one({"search_run_key": self.shared_key(registered_domain)}, {"attempts": 1})
        attempts = int((run or {}).get("attempts") or 0)
        return attempts >= retry_policy("search", self._settings.task_max_attempts).max_attempts

    def _last_attempt_failed(self, registered_domain: str) -> bool:
        run = self._runs.find_one({"search_run_key": self.shared_key(registered_domain)}, {"status": 1})
        return (run or {}).get("status") == "failed"

    def mark_completed(self, ref: dict[str, Any], result: dict[str, Any], *, process_id: str | None = None) -> None:
        self.ensure_indexes()
        timestamp = _now()
        key = self._key_for_ref(ref, process_id)
        fields = {
            "search_run_key": key,
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "career_url": result.get("career_url"),
            "career_urls": result.get("career_urls", []),
            "result": result,
            "source_type": result.get("source_type") or "search_engine",
            "scope": result.get("cache_scope") or ("process_only" if ref.get("uses_process_supplied_career_url") else "shared_domain"),
            "status": "completed",
            "last_completed_at": timestamp,
            "updated_at": timestamp,
            "last_error": None,
            "last_failure_type": None,
        }
        self._runs.update_one(
            {"search_run_key": key},
            {
                "$set": fields,
                "$setOnInsert": {
                    "created_at": timestamp,
                    "first_started_at": ref.get("started_at") or timestamp,
                    "attempts": 1,
                    "run_count": 1,
                },
                "$unset": {"worker_name": "", "celery_task_id": "", "heartbeat_at": "", "lease_expires_at": ""},
            },
            upsert=True,
        )

    def mark_failed(self, ref: dict[str, Any], error: str, result: dict[str, Any] | None = None, *, process_id: str | None = None) -> None:
        self.ensure_indexes()
        timestamp = _now()
        key = self._key_for_ref(ref, process_id)
        fields: dict[str, Any] = {
            "search_run_key": key,
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "scope": "process_only" if ref.get("uses_process_supplied_career_url") else "shared_domain",
            "status": "failed",
            "last_error": error,
            "last_failure_type": classify_failure(error),
            "last_error_details": self._error_details(error, result),
            "updated_at": timestamp,
        }
        if result:
            fields["last_result"] = result
        self._runs.update_one(
            {"search_run_key": key},
            {
                "$set": fields,
                "$setOnInsert": {
                    "created_at": timestamp,
                    "first_started_at": ref.get("started_at") or timestamp,
                    "attempts": 1,
                    "run_count": 1,
                },
                "$unset": {"worker_name": "", "celery_task_id": "", "heartbeat_at": "", "lease_expires_at": ""},
            },
            upsert=True,
        )

    def heartbeat(self, registered_domain: str) -> None:
        timestamp = _now()
        self._runs.update_one(
            {"search_run_key": self.shared_key(registered_domain), "status": "running"},
            {
                "$set": {
                    "heartbeat_at": timestamp,
                    "lease_expires_at": timestamp + timedelta(seconds=self._settings.stale_task_seconds),
                    "updated_at": timestamp,
                }
            },
        )

    def requeue(self, registered_domain: str, *, decrement_attempt: bool = False) -> None:
        update: dict[str, Any] = {
            "$set": {"status": "queued", "updated_at": _now()},
            "$unset": {"worker_name": "", "celery_task_id": "", "heartbeat_at": "", "lease_expires_at": ""},
        }
        if decrement_attempt:
            update["$inc"] = {"attempts": -1}
        self._runs.update_one({"search_run_key": self.shared_key(registered_domain), "status": "running"}, update)

    def _key_for_ref(self, ref: dict[str, Any], process_id: str | None) -> str:
        if ref.get("uses_process_supplied_career_url"):
            return self.supplied_key(str(process_id or ""), ref["registered_domain"])
        return self.shared_key(ref["registered_domain"])

    def _error_details(self, error: str, result: dict[str, Any] | None) -> dict[str, Any]:
        details = {
            "error": error,
            "failure_type": classify_failure(error),
            "failed_at": _now(),
        }
        if result:
            details.update(
                {
                    key: value
                    for key, value in {
                        "status": result.get("status"),
                        "diagnostics": result.get("diagnostics"),
                        "career_urls": result.get("career_urls"),
                        "non_domain_career_urls": result.get("non_domain_career_urls"),
                        "all_urls_count": len(result.get("all_urls", []) or []),
                        "duration_seconds": result.get("duration_seconds"),
                        "selenium_node_id": result.get("selenium_node_id"),
                        "selenium_session_slot_id": result.get("selenium_session_slot_id"),
                    }.items()
                    if value not in (None, "", [])
                }
            )
        return details


@lru_cache(maxsize=1)
def get_search_run_service() -> SearchRunService:
    return SearchRunService(get_sync_mongodb_service(), get_settings())
