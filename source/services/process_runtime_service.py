from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, DESCENDING, ReturnDocument

from core.config import Settings, get_settings
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("process_runtime_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ProcessRuntimeService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)

    def start_process(self, process_id: str) -> list[dict[str, Any]]:
        self._repair_legacy_process_shape(process_id)
        self.requeue_stale_processing(process_id)
        process = self._load_process(process_id)
        if not self._queued_refs(process):
            self._refresh_process_status(process_id)
            return []
        self._mark_process_running(process_id)
        return self.dispatchable_refs(process_id)

    def dispatchable_refs(self, process_id: str) -> list[dict[str, Any]]:
        process = self._load_process(process_id)
        if process.get("status") != "running":
            return []
        capacity = self._available_process_capacity(process)
        if capacity <= 0:
            return []
        return self._queued_refs(process)[:capacity]

    def _load_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        return process

    def _repair_legacy_process_shape(self, process_id: str) -> None:
        process = self._load_process(process_id)
        if self._has_domain_state(process):
            return
        legacy_tasks = self._legacy_domain_tasks(process_id)
        if not legacy_tasks:
            return
        self._save_repaired_process(process_id, legacy_tasks)
        log_event(logger, "info", "legacy_process_shape_repaired", domain="process", process_id=process_id)

    def _has_domain_state(self, process: dict[str, Any]) -> bool:
        return isinstance(process.get("domains"), dict)

    def _legacy_domain_tasks(self, process_id: str) -> list[dict[str, Any]]:
        return list(self._domain_tasks.find({"process_id": process_id}))

    def _save_repaired_process(self, process_id: str, tasks: list[dict[str, Any]]) -> None:
        domains = self._domain_state_from_tasks(tasks)
        self._processes.update_one(
            {"process_id": process_id},
            {"$set": {"domains": domains, "totals": self._totals_from_domains(domains), "updated_at": _now()}},
        )

    def _domain_state_from_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        domains = self._empty_domain_state()
        for task in tasks:
            domains[self._process_bucket(task)].append(self._domain_ref_from_task(task))
        return domains

    def _empty_domain_state(self) -> dict[str, list[dict[str, Any]]]:
        return {"queued": [], "processing": [], "completed": [], "failed": []}

    def _process_bucket(self, task: dict[str, Any]) -> str:
        status = str(task.get("status") or "queued")
        return status if status in {"queued", "processing", "completed", "failed"} else "queued"

    def _domain_ref_from_task(self, task: dict[str, Any]) -> dict[str, Any]:
        ref = {
            "domain": task.get("domain"),
            "registered_domain": task.get("registered_domain"),
            "career_url": task.get("career_url"),
        }
        return {key: value for key, value in ref.items() if value is not None}

    def _totals_from_domains(self, domains: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        return {
            "domains": sum(len(items) for items in domains.values()),
            "queued": len(domains["queued"]),
            "processing": len(domains["processing"]),
            "completed": len(domains["completed"]),
            "failed": len(domains["failed"]),
            "supplied_career_urls": self._career_url_count(domains),
        }

    def _career_url_count(self, domains: dict[str, list[dict[str, Any]]]) -> int:
        return sum(1 for items in domains.values() for item in items if item.get("career_url"))

    def _queued_refs(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        return list(process.get("domains", {}).get("queued", []) or [])

    def _available_process_capacity(self, process: dict[str, Any]) -> int:
        agent_count = max(1, int(process.get("agent_count") or 1))
        processing = int(process.get("totals", {}).get("processing") or 0)
        return max(0, agent_count - processing)

    def _mark_process_running(self, process_id: str) -> None:
        self._processes.update_one(
            {"process_id": process_id},
            {"$set": {"status": "running", "updated_at": _now()}},
        )

    def claim_domain_for_process(
        self,
        *,
        process_id: str,
        registered_domain: str,
        worker_name: str,
        task_id: str,
    ) -> dict[str, Any]:
        ref = self._get_queued_ref(process_id, registered_domain)
        if not ref:
            return {"status": "not_in_queue"}
        if not self._process_has_capacity(process_id):
            return {"status": "process_at_capacity"}
        global_claim = self._claim_global_domain(ref, worker_name, task_id)
        if global_claim["status"] != "claimed":
            return global_claim
        moved = self._move_to_processing(process_id, ref, worker_name, task_id)
        if not moved:
            self._release_global_domain(ref["registered_domain"])
            return {"status": "not_in_queue"}
        return {"status": "claimed", "domain_ref": moved}

    def _process_has_capacity(self, process_id: str) -> bool:
        process = self._load_process(process_id)
        return self._available_process_capacity(process) > 0

    def _get_queued_ref(self, process_id: str, registered_domain: str) -> dict[str, Any] | None:
        process = self._processes.find_one(
            {"process_id": process_id, "domains.queued.registered_domain": registered_domain},
            {"domains.queued.$": 1},
        )
        if not process:
            return None
        queued = process.get("domains", {}).get("queued", [])
        return queued[0] if queued else None

    def _claim_global_domain(self, ref: dict[str, Any], worker_name: str, task_id: str) -> dict[str, Any]:
        completed = self._fresh_completed_domain(ref["registered_domain"])
        if completed:
            return {"status": "fresh_completed", "result": completed.get("result")}
        if self._attempts_exhausted(ref["registered_domain"]):
            return {"status": "max_attempts_exceeded"}
        claimed = self._claim_runnable_domain(ref, worker_name, task_id)
        if claimed:
            return {"status": "claimed"}
        return {"status": "busy"}

    def _attempts_exhausted(self, registered_domain: str) -> bool:
        task = self._domain_tasks.find_one({"registered_domain": registered_domain}, {"attempts": 1})
        attempts = int((task or {}).get("attempts") or 0)
        return attempts >= self._settings.task_max_attempts

    def _fresh_completed_domain(self, registered_domain: str) -> dict[str, Any] | None:
        threshold = _now() - timedelta(hours=24)
        return self._domain_tasks.find_one(
            {
                "registered_domain": registered_domain,
                "status": "completed",
                "last_completed_at": {"$gte": threshold},
            },
        )

    def _claim_runnable_domain(self, ref: dict[str, Any], worker_name: str, task_id: str) -> dict[str, Any] | None:
        timestamp = _now()
        stale_threshold = timestamp - timedelta(seconds=self._settings.stale_task_seconds)
        return self._domain_tasks.find_one_and_update(
            {
                "registered_domain": ref["registered_domain"],
                "$or": [
                    {"status": {"$in": ["queued", "failed"]}, "attempts": {"$lt": self._settings.task_max_attempts}},
                    {"status": "running", "last_started_at": {"$lt": stale_threshold}},
                    {"status": "completed", "last_completed_at": {"$lt": timestamp - timedelta(hours=24)}},
                ],
            },
            {
                "$set": self._running_domain_fields(ref, worker_name, task_id, timestamp),
                "$inc": {"attempts": 1},
            },
            return_document=ReturnDocument.AFTER,
        )

    def _running_domain_fields(
        self,
        ref: dict[str, Any],
        worker_name: str,
        task_id: str,
        timestamp: datetime,
    ) -> dict[str, Any]:
        return {
            "domain": ref["domain"],
            "registered_domain": ref["registered_domain"],
            "career_url": ref.get("career_url"),
            "status": "running",
            "worker_name": worker_name,
            "celery_task_id": task_id,
            "last_started_at": timestamp,
            "updated_at": timestamp,
            "last_error": None,
        }

    def _move_to_processing(
        self,
        process_id: str,
        ref: dict[str, Any],
        worker_name: str,
        task_id: str,
    ) -> dict[str, Any] | None:
        processing_ref = self._processing_ref(ref, worker_name, task_id)
        process = self._processes.find_one_and_update(
            {"process_id": process_id, "domains.queued.registered_domain": ref["registered_domain"]},
            {
                "$pull": {"domains.queued": {"registered_domain": ref["registered_domain"]}},
                "$push": {"domains.processing": processing_ref},
                "$inc": {"totals.queued": -1, "totals.processing": 1},
                "$set": {"status": "running", "updated_at": _now()},
            },
            return_document=ReturnDocument.AFTER,
        )
        return processing_ref if process else None

    def _processing_ref(self, ref: dict[str, Any], worker_name: str, task_id: str) -> dict[str, Any]:
        updated = dict(ref)
        updated["worker_name"] = worker_name
        updated["celery_task_id"] = task_id
        updated["started_at"] = _now()
        return updated

    def _release_global_domain(self, registered_domain: str, *, decrement_attempt: bool = False) -> None:
        update: dict[str, Any] = {"$set": {"status": "queued", "updated_at": _now()}}
        if decrement_attempt:
            update["$inc"] = {"attempts": -1}
        self._domain_tasks.update_one({"registered_domain": registered_domain, "status": "running"}, update)

    def complete_with_reused_result(self, process_id: str, registered_domain: str, result: dict[str, Any] | None) -> None:
        ref = self._get_queued_ref(process_id, registered_domain)
        if not ref:
            return
        completed_ref = self._completed_ref(ref, result or {}, reused=True)
        self._move_queue_to_completed(process_id, ref, completed_ref)

    def complete_domain(self, process_id: str, domain_ref: dict[str, Any], result: dict[str, Any]) -> None:
        completed_ref = self._completed_ref(domain_ref, result, reused=False)
        self._move_processing_to_completed(process_id, domain_ref, completed_ref)
        self._mark_global_completed(domain_ref, result)

    def _completed_ref(self, ref: dict[str, Any], result: dict[str, Any], reused: bool) -> dict[str, Any]:
        completed = self._clean_runtime_ref(ref)
        completed["result"] = result
        completed["reused"] = reused
        completed["completed_at"] = _now()
        return completed

    def _clean_runtime_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(ref)
        cleaned.pop("worker_name", None)
        cleaned.pop("celery_task_id", None)
        return cleaned

    def _move_queue_to_completed(
        self,
        process_id: str,
        ref: dict[str, Any],
        completed_ref: dict[str, Any],
    ) -> None:
        self._processes.update_one(
            {"process_id": process_id, "domains.queued.registered_domain": ref["registered_domain"]},
            {
                "$pull": {"domains.queued": {"registered_domain": ref["registered_domain"]}},
                "$push": {"domains.completed": completed_ref},
                "$inc": {"totals.queued": -1, "totals.completed": 1},
                "$set": {"updated_at": _now()},
            },
        )
        self._refresh_process_status(process_id)

    def _move_processing_to_completed(
        self,
        process_id: str,
        ref: dict[str, Any],
        completed_ref: dict[str, Any],
    ) -> None:
        self._processes.update_one(
            {"process_id": process_id, "domains.processing.registered_domain": ref["registered_domain"]},
            {
                "$pull": {"domains.processing": {"registered_domain": ref["registered_domain"]}},
                "$push": {"domains.completed": completed_ref},
                "$inc": {"totals.processing": -1, "totals.completed": 1},
                "$set": {"updated_at": _now()},
            },
        )
        self._refresh_process_status(process_id)

    def _mark_global_completed(self, ref: dict[str, Any], result: dict[str, Any]) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": ref["registered_domain"]},
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

    def fail_domain(self, process_id: str, domain_ref: dict[str, Any], error: str) -> None:
        failed_ref = self._failed_ref(domain_ref, error)
        self._move_processing_to_failed(process_id, domain_ref, failed_ref)
        self._mark_global_failed(domain_ref, error)

    def fail_queued_domain(self, process_id: str, registered_domain: str, error: str) -> None:
        ref = self._get_queued_ref(process_id, registered_domain)
        if not ref:
            return
        failed_ref = self._failed_ref(ref, error)
        self._move_queue_to_failed(process_id, ref, failed_ref)
        self._mark_global_failed(ref, error)

    def requeue_domain(
        self,
        process_id: str,
        domain_ref: dict[str, Any],
        reason: str,
        *,
        decrement_attempt: bool = False,
    ) -> None:
        queued_ref = self._clean_runtime_ref(domain_ref)
        queued_ref.pop("started_at", None)
        queued_ref["last_requeue_reason"] = reason
        self._move_processing_to_queue(process_id, domain_ref, queued_ref)
        self._release_global_domain(domain_ref["registered_domain"], decrement_attempt=decrement_attempt)

    def _move_processing_to_queue(
        self,
        process_id: str,
        ref: dict[str, Any],
        queued_ref: dict[str, Any],
    ) -> None:
        self._processes.update_one(
            {"process_id": process_id, "domains.processing.registered_domain": ref["registered_domain"]},
            {
                "$pull": {"domains.processing": {"registered_domain": ref["registered_domain"]}},
                "$push": {"domains.queued": queued_ref},
                "$inc": {"totals.processing": -1, "totals.queued": 1},
                "$set": {"status": "running", "updated_at": _now()},
            },
        )

    def _failed_ref(self, ref: dict[str, Any], error: str) -> dict[str, Any]:
        failed = self._clean_runtime_ref(ref)
        failed["error"] = error
        failed["failed_at"] = _now()
        return failed

    def _move_queue_to_failed(self, process_id: str, ref: dict[str, Any], failed_ref: dict[str, Any]) -> None:
        self._processes.update_one(
            {"process_id": process_id, "domains.queued.registered_domain": ref["registered_domain"]},
            {
                "$pull": {"domains.queued": {"registered_domain": ref["registered_domain"]}},
                "$push": {"domains.failed": failed_ref},
                "$inc": {"totals.queued": -1, "totals.failed": 1},
                "$set": {"updated_at": _now()},
            },
        )
        self._refresh_process_status(process_id)

    def _move_processing_to_failed(self, process_id: str, ref: dict[str, Any], failed_ref: dict[str, Any]) -> None:
        self._processes.update_one(
            {"process_id": process_id, "domains.processing.registered_domain": ref["registered_domain"]},
            {
                "$pull": {"domains.processing": {"registered_domain": ref["registered_domain"]}},
                "$push": {"domains.failed": failed_ref},
                "$inc": {"totals.processing": -1, "totals.failed": 1},
                "$set": {"updated_at": _now()},
            },
        )
        self._refresh_process_status(process_id)

    def _mark_global_failed(self, ref: dict[str, Any], error: str) -> None:
        self._domain_tasks.update_one(
            {"registered_domain": ref["registered_domain"]},
            {
                "$set": {"status": "failed", "last_error": error, "updated_at": _now()},
                "$unset": {"worker_name": "", "celery_task_id": ""},
            },
        )

    def requeue_stale_processing(self, process_id: str) -> int:
        process = self._load_process(process_id)
        stale_refs = self._stale_refs(process)
        for ref in stale_refs:
            self._requeue_process_ref(process_id, ref)
        return len(stale_refs)

    def _stale_refs(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        threshold = _now() - timedelta(seconds=self._settings.stale_task_seconds)
        refs = process.get("domains", {}).get("processing", []) or []
        return [ref for ref in refs if self._is_stale_ref(ref, threshold)]

    def _is_stale_ref(self, ref: dict[str, Any], threshold: datetime) -> bool:
        started_at = ref.get("started_at")
        if not isinstance(started_at, datetime):
            return False
        return self._aware_datetime(started_at) < threshold

    def _aware_datetime(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def _requeue_process_ref(self, process_id: str, ref: dict[str, Any]) -> None:
        queued_ref = self._clean_runtime_ref(ref)
        queued_ref.pop("started_at", None)
        self._processes.update_one(
            {"process_id": process_id, "domains.processing.registered_domain": ref["registered_domain"]},
            {
                "$pull": {"domains.processing": {"registered_domain": ref["registered_domain"]}},
                "$push": {"domains.queued": queued_ref},
                "$inc": {"totals.processing": -1, "totals.queued": 1},
                "$set": {"status": "running", "updated_at": _now()},
            },
        )
        self._release_global_domain(ref["registered_domain"])

    def _refresh_process_status(self, process_id: str) -> None:
        process = self._load_process(process_id)
        status = self._next_status(process)
        self._processes.update_one(
            {"process_id": process_id},
            {"$set": {"status": status, "updated_at": _now()}},
        )

    def _next_status(self, process: dict[str, Any]) -> str:
        totals = process.get("totals", {})
        if totals.get("processing", 0) > 0:
            return "running"
        if totals.get("queued", 0) > 0:
            if process.get("status") == "running":
                return "running"
            return "queued"
        if totals.get("failed", 0) > 0:
            return "partial_completed"
        return "completed"


@lru_cache(maxsize=1)
def get_process_runtime_service() -> ProcessRuntimeService:
    return ProcessRuntimeService(get_sync_mongodb_service(), get_settings())
