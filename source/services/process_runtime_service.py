from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, ReturnDocument

from core.config import Settings, get_settings
from services.failure_classifier import classify_failure
from services.node_lifecycle import terminal_status
from services.process_control_service import get_process_control_service
from services.search_run_service import get_search_run_service
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

    def start_or_restart_search_process(self, process_id: str) -> dict[str, Any]:
        self._repair_legacy_process_shape(process_id)
        self.requeue_stale_processing(process_id)
        process = self._load_process(process_id)
        mode = self._search_start_mode(process)
        if mode == "blocked":
            raise RuntimeError("Process is already queued or running")
        if mode == "rerun":
            self._reset_terminal_process(process_id, process)
        refs = self.start_process(process_id)
        return {"mode": mode, "refs": refs}

    def dispatchable_refs(self, process_id: str) -> list[dict[str, Any]]:
        process = self._load_process(process_id)
        if process.get("status") != "running":
            return []
        capacity = self._available_process_capacity(process)
        if capacity <= 0:
            return []
        return self._queued_refs(process)[:capacity]

    def mark_domain_dispatched(self, process_id: str, registered_domain: str) -> bool:
        threshold = _now() - timedelta(seconds=max(30, self._settings.watchdog_interval_seconds * 2))
        result = self._processes.update_one(
            {
                "process_id": process_id,
                "domains.queued": {
                    "$elemMatch": {
                        "registered_domain": registered_domain,
                        "$or": [
                            {"dispatched_at": {"$exists": False}},
                            {"dispatched_at": {"$lt": threshold}},
                        ],
                    }
                },
            },
            {"$set": {"domains.queued.$.dispatched_at": _now(), "updated_at": _now()}},
        )
        return bool(result.modified_count)

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
        refs = list(process.get("domains", {}).get("queued", []) or [])
        return get_process_control_service().filter_refs(process["process_id"], refs, "search")

    def _search_start_mode(self, process: dict[str, Any]) -> str:
        totals = process.get("totals", {})
        if self._has_active_process(process, totals):
            return "blocked"
        if self._has_terminal_domains(totals):
            return "rerun"
        return "start"

    def _has_active_process(self, process: dict[str, Any], totals: dict[str, Any]) -> bool:
        if int(totals.get("processing") or 0) > 0:
            return True
        if process.get("status") == "running" and int(totals.get("queued") or 0) > 0:
            return True
        return int(totals.get("queued") or 0) > 0 and self._has_terminal_domains(totals)

    def _has_terminal_domains(self, totals: dict[str, Any]) -> bool:
        return int(totals.get("completed") or 0) > 0 or int(totals.get("failed") or 0) > 0

    def _reset_terminal_process(self, process_id: str, process: dict[str, Any]) -> None:
        queued = self._terminal_refs(process)
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "status": "queued",
                    "domains": {"queued": queued, "processing": [], "completed": [], "failed": []},
                    "totals": self._totals_from_domains(
                        {"queued": queued, "processing": [], "completed": [], "failed": []}
                    ),
                    "updated_at": _now(),
                }
            },
        )

    def _terminal_refs(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        domains = process.get("domains", {})
        refs = list(domains.get("completed", []) or []) + list(domains.get("failed", []) or [])
        return [self._clean_terminal_ref(ref) for ref in refs]

    def _clean_terminal_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        cleaned = self._clean_runtime_ref(ref)
        for key in ("result", "reused", "completed_at", "error", "failed_at", "last_requeue_reason"):
            cleaned.pop(key, None)
        return cleaned

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
        if self._has_supplied_career_url(ref):
            moved = self._move_to_processing(process_id, ref, worker_name, task_id)
            if not moved:
                return {"status": "not_in_queue"}
            moved["uses_process_supplied_career_url"] = True
            return {"status": "claimed", "domain_ref": moved}
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
        claim = get_search_run_service().claim_shared(ref, worker_name, task_id)
        if claim["status"] == "fresh_completed":
            return {"status": "fresh_completed"}
        if claim["status"] == "claimed":
            return {"status": "claimed"}
        return {"status": claim["status"]}

    def _has_supplied_career_url(self, ref: dict[str, Any]) -> bool:
        return bool(str(ref.get("career_url") or "").strip())

    def _fresh_completed_domain(self, registered_domain: str) -> dict[str, Any] | None:
        return get_search_run_service().fresh_completed(registered_domain)

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
        updated["attempts"] = self._domain_attempts(ref["registered_domain"])
        updated["started_at"] = _now()
        updated["heartbeat_at"] = _now()
        updated["lease_expires_at"] = _now() + timedelta(seconds=self._settings.stale_task_seconds)
        return updated

    def heartbeat_domain(self, process_id: str, registered_domain: str) -> None:
        timestamp = _now()
        expires_at = timestamp + timedelta(seconds=self._settings.stale_task_seconds)
        self._processes.update_one(
            {"process_id": process_id, "domains.processing.registered_domain": registered_domain},
            {
                "$set": {
                    "domains.processing.$.heartbeat_at": timestamp,
                    "domains.processing.$.lease_expires_at": expires_at,
                    "updated_at": timestamp,
                }
            },
        )
        get_search_run_service().heartbeat(registered_domain)

    def _domain_attempts(self, registered_domain: str) -> int:
        return get_search_run_service().attempts(registered_domain)

    def _release_global_domain(self, registered_domain: str, *, decrement_attempt: bool = False) -> None:
        get_search_run_service().requeue(registered_domain, decrement_attempt=decrement_attempt)

    def complete_with_reused_result(self, process_id: str, registered_domain: str) -> None:
        ref = self._get_queued_ref(process_id, registered_domain)
        if not ref:
            return
        completed = self._fresh_completed_domain(registered_domain) or {}
        completed_ref = self._completed_ref(ref, reused=True, result=completed.get("result") or completed)
        self._move_queue_to_completed(process_id, ref, completed_ref)

    def complete_domain(self, process_id: str, domain_ref: dict[str, Any], result: dict[str, Any]) -> None:
        completed_ref = self._completed_ref(domain_ref, reused=False, result=result)
        self._move_processing_to_completed(process_id, domain_ref, completed_ref)
        get_search_run_service().mark_completed(domain_ref, result, process_id=process_id)
        if not domain_ref.get("uses_process_supplied_career_url"):
            self._mark_global_completed(domain_ref, result)

    def _completed_ref(self, ref: dict[str, Any], reused: bool, result: dict[str, Any] | None = None) -> dict[str, Any]:
        completed = self._clean_runtime_ref(ref)
        if completed.get("career_url") and not completed.get("supplied_career_url"):
            completed["supplied_career_url"] = completed["career_url"]
        completed["reused"] = reused
        completed["completed_at"] = _now()
        if result:
            completed.update(self._search_summary_from_result(result, reused=reused))
        return completed

    def _clean_runtime_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(ref)
        cleaned.pop("worker_name", None)
        cleaned.pop("celery_task_id", None)
        cleaned.pop("heartbeat_at", None)
        cleaned.pop("lease_expires_at", None)
        cleaned.pop("uses_process_supplied_career_url", None)
        cleaned.pop("dispatched_at", None)
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
                    "career_url": result.get("career_url"),
                    "career_urls": result.get("career_urls", []),
                    "source_type": result.get("source_type") or "search_engine",
                    "cache_scope": result.get("cache_scope") or "shared_domain",
                    "last_completed_at": _now(),
                    "updated_at": _now(),
                    "last_error": None,
                    "last_failure_type": None,
                },
                "$unset": {"worker_name": "", "celery_task_id": ""},
            },
        )

    def fail_domain(
        self,
        process_id: str,
        domain_ref: dict[str, Any],
        error: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        failed_ref = self._failed_ref(domain_ref, error)
        self._move_processing_to_failed(process_id, domain_ref, failed_ref)
        get_search_run_service().mark_failed(domain_ref, error, result, process_id=process_id)
        self._mark_global_failed(domain_ref, error, result)

    def fail_queued_domain(
        self,
        process_id: str,
        registered_domain: str,
        error: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        ref = self._get_queued_ref(process_id, registered_domain)
        if not ref:
            return
        failed_ref = self._failed_ref(ref, error)
        self._move_queue_to_failed(process_id, ref, failed_ref)
        get_search_run_service().mark_failed(ref, error, result, process_id=process_id)
        self._mark_global_failed(ref, error, result)

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
        failed["failure_type"] = classify_failure(error)
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

    def _mark_global_failed(self, ref: dict[str, Any], error: str, result: dict[str, Any] | None = None) -> None:
        fields: dict[str, Any] = {
            "status": "failed",
            "last_error": error,
            "last_failure_type": classify_failure(error),
            "updated_at": _now(),
        }
        if result:
            fields["last_result"] = result
            fields["last_error_details"] = self._error_details_from_result(result)
        else:
            fields["last_error_details"] = {
                "error": error,
                "failure_type": classify_failure(error),
                "failed_at": _now(),
            }
        self._domain_tasks.update_one(
            {"registered_domain": ref["registered_domain"]},
            {
                "$set": fields,
                "$unset": {"worker_name": "", "celery_task_id": ""},
            },
        )

    def _error_details_from_result(self, result: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "status": result.get("status"),
                "error": result.get("error"),
                "failure_type": classify_failure(str(result.get("error") or result.get("status") or "")),
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

    def _search_summary_from_result(self, result: dict[str, Any], *, reused: bool) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "career_url": result.get("career_url"),
                "career_urls": result.get("career_urls"),
                "search_status": result.get("status"),
                "source_type": result.get("source_type") or ("reused_fresh_domain_result" if reused else "search_engine"),
                "cache_scope": result.get("cache_scope"),
                "reused_from_shared_domain": reused,
            }.items()
            if value not in (None, "", [])
        }

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
        lease_expires_at = ref.get("lease_expires_at")
        if isinstance(lease_expires_at, datetime):
            return self._aware_datetime(lease_expires_at) < _now()
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
        return terminal_status(
            completed=int(totals.get("completed") or 0),
            failed=int(totals.get("failed") or 0),
        )


@lru_cache(maxsize=1)
def get_process_runtime_service() -> ProcessRuntimeService:
    return ProcessRuntimeService(get_sync_mongodb_service(), get_settings())
