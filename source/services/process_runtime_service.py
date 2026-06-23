from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, ReturnDocument

from core.config import Settings, get_settings
from services.failure_classifier import classify_failure
from services.node_lifecycle import terminal_status
from services.process_control_service import get_process_control_service
from services.process_domain_ref_service import get_process_domain_ref_service
from services.search_run_service import get_search_run_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger


logger = get_logger("process_runtime_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ProcessRuntimeService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._process_refs = mongodb.collection(settings.mongodb_process_domain_refs_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._ref_service = get_process_domain_ref_service()

    def start_process(self, process_id: str) -> list[dict[str, Any]]:
        self._ensure_ref_state(process_id)
        self.requeue_stale_processing(process_id)
        process = self._load_process(process_id)
        if not self._queued_refs(process_id):
            self._refresh_process_totals(process_id)
            self._refresh_process_status(process_id)
            return []
        self._mark_process_running(process_id)
        return self.dispatchable_refs(process_id)

    def start_or_restart_search_process(self, process_id: str) -> dict[str, Any]:
        self._ensure_ref_state(process_id)
        self.requeue_stale_processing(process_id)
        process = self._load_process(process_id)
        mode = self._search_start_mode(process)
        if mode == "blocked":
            raise RuntimeError("Process is already queued or running")
        if mode == "rerun":
            self._reset_terminal_process(process_id)
        refs = self.start_process(process_id)
        return {"mode": mode, "refs": refs}

    def dispatchable_refs(self, process_id: str) -> list[dict[str, Any]]:
        self.repair_stale_dispatched_queued(process_id)
        process = self._load_process(process_id)
        if process.get("status") != "running":
            return []
        capacity = self._available_process_capacity(process)
        if capacity <= 0:
            return []
        refs = self._queued_refs(process_id)
        if not refs:
            self._refresh_process_status(process_id)
            return []
        return refs[:capacity]

    def mark_domain_dispatched(self, process_id: str, registered_domain: str) -> bool:
        threshold = self._dispatch_threshold()
        result = self._process_refs.update_one(
            {
                "process_id": process_id,
                "registered_domain": registered_domain,
                "status": "queued",
                "$or": [
                    {"dispatched_at": {"$exists": False}},
                    {"dispatched_at": {"$lt": threshold}},
                ],
            },
            {"$set": {"dispatched_at": _now(), "updated_at": _now()}},
        )
        return bool(result.modified_count)

    def clear_domain_dispatch(self, process_id: str, registered_domain: str, reason: str) -> None:
        self._process_refs.update_one(
            {"process_id": process_id, "registered_domain": registered_domain, "status": "queued"},
            {
                "$set": {"updated_at": _now(), "last_dispatch_clear_reason": reason},
                "$unset": {"dispatched_at": ""},
            },
        )

    def repair_stale_dispatched_queued(self, process_id: str) -> int:
        result = self._process_refs.update_many(
            {
                "process_id": process_id,
                "status": "queued",
                "dispatched_at": {"$lt": self._dispatch_threshold()},
            },
            {
                "$set": {
                    "updated_at": _now(),
                    "last_dispatch_clear_reason": "stale_dispatched_queued",
                },
                "$unset": {"dispatched_at": ""},
            },
        )
        if result.modified_count:
            self._refresh_process_totals(process_id)
            self._refresh_process_status(process_id)
        return int(result.modified_count)

    def _dispatch_threshold(self) -> datetime:
        timeout_seconds = max(60, self._settings.watchdog_interval_seconds * 3)
        return _now() - timedelta(seconds=timeout_seconds)

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

    def heartbeat_domain(self, process_id: str, registered_domain: str) -> None:
        timestamp = _now()
        expires_at = timestamp + timedelta(seconds=self._settings.stale_task_seconds)
        self._process_refs.update_one(
            {"process_id": process_id, "registered_domain": registered_domain, "status": "processing"},
            {"$set": {"heartbeat_at": timestamp, "lease_expires_at": expires_at, "updated_at": timestamp}},
        )
        get_search_run_service().heartbeat(registered_domain)

    def update_domain_progress(
        self,
        process_id: str,
        registered_domain: str,
        *,
        step: str,
        current_url: str | None = None,
        page_index: int | None = None,
    ) -> None:
        timestamp = _now()
        fields: dict[str, Any] = {
            "current_step": step,
            "last_step_at": timestamp,
            "updated_at": timestamp,
        }
        if current_url:
            fields["current_url"] = current_url
        if page_index is not None:
            fields["current_page_index"] = page_index
        self._process_refs.update_one(
            {"process_id": process_id, "registered_domain": registered_domain, "status": "processing"},
            {"$set": fields},
        )

    def complete_with_reused_result(self, process_id: str, registered_domain: str) -> None:
        ref = self._get_queued_ref(process_id, registered_domain)
        if not ref:
            return
        completed = self._fresh_completed_domain(registered_domain) or {}
        completed_ref = self._completed_ref(ref, reused=True, result=completed.get("result") or completed)
        self._move_to_terminal(process_id, ref, completed_ref, "queued", "completed")

    def complete_domain(self, process_id: str, domain_ref: dict[str, Any], result: dict[str, Any]) -> None:
        completed_ref = self._completed_ref(domain_ref, reused=False, result=result)
        self._move_to_terminal(process_id, domain_ref, completed_ref, "processing", "completed")
        get_search_run_service().mark_completed(domain_ref, result, process_id=process_id)
        if not domain_ref.get("uses_process_supplied_career_url"):
            self._mark_global_completed(domain_ref, result)

    def fail_domain(
        self,
        process_id: str,
        domain_ref: dict[str, Any],
        error: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        failed_ref = self._failed_ref(domain_ref, error)
        self._move_to_terminal(process_id, domain_ref, failed_ref, "processing", "failed")
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
        self._move_to_terminal(process_id, ref, failed_ref, "queued", "failed")
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
        self._move_to_terminal(process_id, domain_ref, queued_ref, "processing", "queued")
        self._release_global_domain(domain_ref["registered_domain"], decrement_attempt=decrement_attempt)

    def requeue_stale_processing(self, process_id: str) -> int:
        stale_refs = self._stale_refs(process_id)
        timed_out_refs = self._timed_out_refs(process_id)
        timed_out_domains = {str(ref.get("registered_domain") or "") for ref in timed_out_refs}
        for ref in stale_refs:
            if str(ref.get("registered_domain") or "") in timed_out_domains:
                continue
            self._requeue_process_ref(process_id, ref)
        for ref in timed_out_refs:
            self._fail_timed_out_ref(process_id, ref)
        return len(stale_refs) + len(timed_out_refs)

    def _ensure_ref_state(self, process_id: str) -> None:
        self._ref_service.ensure_indexes()
        if self._process_refs.count_documents({"process_id": process_id}, limit=1):
            self._compact_process_document(process_id)
            return
        process = self._load_process(process_id)
        refs = self._legacy_refs(process)
        if not refs:
            return
        timestamp = process.get("created_at") or _now()
        operations = [self._legacy_ref_upsert(process_id, ref, timestamp) for ref in refs]
        if operations:
            self._process_refs.bulk_write(operations, ordered=False)
        self._compact_process_document(process_id)
        self._refresh_process_totals(process_id)

    def _legacy_refs(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        refs = []
        for status, items in (process.get("domains") or {}).items():
            for item in list(items or []):
                if isinstance(item, dict) and item.get("registered_domain"):
                    refs.append({**item, "status": status})
        for item in list(process.get("process_domains") or []):
            registered_domain = item.get("registered_domain")
            if registered_domain and not any(ref.get("registered_domain") == registered_domain for ref in refs):
                refs.append({**item, "status": "queued"})
        return refs

    def _legacy_ref_upsert(self, process_id: str, ref: dict[str, Any], timestamp: datetime):
        from pymongo import UpdateOne

        status = str(ref.get("status") or "queued")
        if status not in {"queued", "processing", "completed", "failed"}:
            status = "queued"
        return UpdateOne(
            {"process_id": process_id, "registered_domain": ref["registered_domain"]},
            {
                "$setOnInsert": {
                    "process_id": process_id,
                    "registered_domain": ref["registered_domain"],
                    "created_at": ref.get("created_at") or timestamp,
                },
                "$set": {
                    **self._legacy_ref_updates(ref),
                    "status": status,
                    "domain": ref.get("domain"),
                    "career_url": ref.get("career_url") or ref.get("supplied_career_url"),
                    "enabled": ref.get("enabled", True),
                    "node_controls": ref.get("node_controls") or {},
                    "updated_at": ref.get("updated_at") or timestamp,
                },
            },
            upsert=True,
        )

    def _legacy_ref_updates(self, ref: dict[str, Any]) -> dict[str, Any]:
        cleaned = self._clean_runtime_ref(ref)
        for key in ("process_id", "registered_domain", "created_at"):
            cleaned.pop(key, None)
        return cleaned

    def _compact_process_document(self, process_id: str) -> None:
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {"domains": self._ref_service.minimal_domain_state(), "schema_version": 3, "updated_at": _now()},
                "$unset": {"process_domains": ""},
            },
        )

    def _load_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        return process

    def _queued_refs(self, process_id: str) -> list[dict[str, Any]]:
        refs = list(
            self._process_refs.find(
                {"process_id": process_id, "status": "queued", "enabled": {"$ne": False}},
            ).sort("created_at", ASCENDING)
        )
        return get_process_control_service().filter_refs(process_id, refs, "search")

    def _search_start_mode(self, process: dict[str, Any]) -> str:
        totals = self._refresh_process_totals(process["process_id"])
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

    def _reset_terminal_process(self, process_id: str) -> None:
        timestamp = _now()
        self._process_refs.update_many(
            {"process_id": process_id, "status": {"$in": ["completed", "failed"]}},
            {
                "$set": {"status": "queued", "updated_at": timestamp},
                "$unset": {
                    "worker_name": "",
                    "celery_task_id": "",
                    "heartbeat_at": "",
                    "lease_expires_at": "",
                    "dispatched_at": "",
                    "result": "",
                    "error": "",
                    "failure_type": "",
                    "failed_at": "",
                    "last_requeue_reason": "",
                    "current_step": "",
                    "current_url": "",
                    "current_page_index": "",
                    "last_step_at": "",
                },
            },
        )
        self._processes.update_one(
            {"process_id": process_id},
            {"$set": {"status": "queued", "domains": self._ref_service.minimal_domain_state(), "updated_at": timestamp}},
        )
        self._refresh_process_totals(process_id)

    def _available_process_capacity(self, process: dict[str, Any]) -> int:
        totals = self._refresh_process_totals(process["process_id"])
        agent_count = max(1, int(process.get("agent_count") or 1))
        processing = int(totals.get("processing") or 0)
        return max(0, agent_count - processing)

    def _mark_process_running(self, process_id: str) -> None:
        self._processes.update_one({"process_id": process_id}, {"$set": {"status": "running", "updated_at": _now()}})

    def _process_has_capacity(self, process_id: str) -> bool:
        process = self._load_process(process_id)
        return self._available_process_capacity(process) > 0

    def _get_queued_ref(self, process_id: str, registered_domain: str) -> dict[str, Any] | None:
        return self._process_refs.find_one({"process_id": process_id, "registered_domain": registered_domain, "status": "queued"})

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

    def _move_to_processing(self, process_id: str, ref: dict[str, Any], worker_name: str, task_id: str) -> dict[str, Any] | None:
        processing_ref = self._processing_ref(ref, worker_name, task_id)
        updated = self._process_refs.find_one_and_update(
            {"process_id": process_id, "registered_domain": ref["registered_domain"], "status": "queued"},
            {"$set": processing_ref},
            return_document=ReturnDocument.AFTER,
        )
        if not updated:
            return None
        self._refresh_process_totals(process_id)
        self._mark_process_running(process_id)
        return dict(updated)

    def _processing_ref(self, ref: dict[str, Any], worker_name: str, task_id: str) -> dict[str, Any]:
        timestamp = _now()
        return {
            "status": "processing",
            "worker_name": worker_name,
            "celery_task_id": task_id,
            "attempts": self._domain_attempts(ref["registered_domain"]),
            "started_at": timestamp,
            "heartbeat_at": timestamp,
            "lease_expires_at": timestamp + timedelta(seconds=self._settings.stale_task_seconds),
            "updated_at": timestamp,
        }

    def _domain_attempts(self, registered_domain: str) -> int:
        return get_search_run_service().attempts(registered_domain)

    def _release_global_domain(self, registered_domain: str, *, decrement_attempt: bool = False) -> None:
        get_search_run_service().requeue(registered_domain, decrement_attempt=decrement_attempt)

    def _completed_ref(self, ref: dict[str, Any], reused: bool, result: dict[str, Any] | None = None) -> dict[str, Any]:
        completed = self._clean_runtime_ref(ref)
        if completed.get("career_url") and not completed.get("supplied_career_url"):
            completed["supplied_career_url"] = completed["career_url"]
        completed["reused"] = reused
        completed["completed_at"] = _now()
        if result:
            completed.update(self._search_summary_from_result(result, reused=reused))
        return completed

    def _move_to_terminal(
        self,
        process_id: str,
        ref: dict[str, Any],
        terminal_ref: dict[str, Any],
        expected_status: str,
        target_status: str,
    ) -> None:
        update = {
            **terminal_ref,
            "status": target_status,
            "updated_at": _now(),
        }
        self._process_refs.update_one(
            {"process_id": process_id, "registered_domain": ref["registered_domain"], "status": expected_status},
            {
                "$set": update,
                "$unset": {
                    "worker_name": "",
                    "celery_task_id": "",
                    "heartbeat_at": "",
                    "lease_expires_at": "",
                    "dispatched_at": "",
                    "current_step": "",
                    "current_url": "",
                    "current_page_index": "",
                    "last_step_at": "",
                },
            },
        )
        self._refresh_process_totals(process_id)
        self._refresh_process_status(process_id)

    def _clean_runtime_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(ref)
        for key in (
            "_id",
            "worker_name",
            "celery_task_id",
            "heartbeat_at",
            "lease_expires_at",
            "uses_process_supplied_career_url",
            "dispatched_at",
            "current_step",
            "current_url",
            "current_page_index",
            "last_step_at",
        ):
            cleaned.pop(key, None)
        return cleaned

    def _failed_ref(self, ref: dict[str, Any], error: str) -> dict[str, Any]:
        failed = self._clean_runtime_ref(ref)
        failed["error"] = error
        failed["failure_type"] = classify_failure(error)
        failed["failed_at"] = _now()
        return failed

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
            fields["last_error_details"] = {"error": error, "failure_type": classify_failure(error), "failed_at": _now()}
        self._domain_tasks.update_one(
            {"registered_domain": ref["registered_domain"]},
            {"$set": fields, "$unset": {"worker_name": "", "celery_task_id": ""}},
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

    def _stale_refs(self, process_id: str) -> list[dict[str, Any]]:
        threshold = _now() - timedelta(seconds=self._settings.stale_task_seconds)
        refs = self._process_refs.find({"process_id": process_id, "status": "processing"})
        return [ref for ref in refs if self._is_stale_ref(ref, threshold)]

    def _timed_out_refs(self, process_id: str) -> list[dict[str, Any]]:
        timeout_seconds = max(60, int(self._settings.node_task_hard_time_limit_seconds))
        threshold = _now() - timedelta(seconds=timeout_seconds)
        refs = self._process_refs.find({"process_id": process_id, "status": "processing"})
        return [
            ref
            for ref in refs
            if isinstance(ref.get("started_at"), datetime)
            and self._aware_datetime(ref["started_at"]) < threshold
        ]

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
        queued_ref["status"] = "queued"
        queued_ref["updated_at"] = _now()
        self._process_refs.update_one(
            {"process_id": process_id, "registered_domain": ref["registered_domain"], "status": "processing"},
            {
                "$set": queued_ref,
                "$unset": {
                    "worker_name": "",
                    "celery_task_id": "",
                    "heartbeat_at": "",
                    "lease_expires_at": "",
                    "current_step": "",
                    "current_url": "",
                    "current_page_index": "",
                    "last_step_at": "",
                },
            },
        )
        self._release_global_domain(ref["registered_domain"])
        self._refresh_process_totals(process_id)

    def _fail_timed_out_ref(self, process_id: str, ref: dict[str, Any]) -> None:
        timeout_seconds = max(60, int(self._settings.node_task_hard_time_limit_seconds))
        error = f"Domain processing exceeded max runtime of {timeout_seconds} seconds"
        self.fail_domain(process_id, ref, error)

    def _refresh_process_totals(self, process_id: str) -> dict[str, int]:
        return self._ref_service.refresh_process_totals(process_id, self._processes)

    def _refresh_process_status(self, process_id: str) -> None:
        process = self._load_process(process_id)
        status = self._next_status(process)
        self._processes.update_one({"process_id": process_id}, {"$set": {"status": status, "updated_at": _now()}})

    def _next_status(self, process: dict[str, Any]) -> str:
        totals = self._refresh_process_totals(process["process_id"])
        if totals.get("processing", 0) > 0:
            return "running"
        if self._queued_refs(process["process_id"]):
            if process.get("status") == "running":
                return "running"
            return "queued"
        return terminal_status(
            completed=int(totals.get("completed") or 0),
            failed=int(totals.get("failed") or 0),
            blocked=int(totals.get("blocked") or 0),
        )


@lru_cache(maxsize=1)
def get_process_runtime_service() -> ProcessRuntimeService:
    return ProcessRuntimeService(get_sync_mongodb_service(), get_settings())
