from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Callable
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING, ReturnDocument

from core.config import Settings, get_settings
from services.career_category_node_service import get_career_category_node_service
from services.job_extraction_node_service import get_job_extraction_node_service
from services.job_pagination_node_service import get_job_pagination_node_service
from services.job_pattern_node_service import get_job_pattern_node_service
from services.node_lifecycle import ACTIVE_STATUSES, TERMINAL_STATUSES
from services.search_node_service import get_search_node_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("pipeline_orchestrator_service")

PIPELINE_RUNNING = "running"
PIPELINE_COMPLETED = "completed"
PIPELINE_FAILED = "failed"
PIPELINE_PAUSED = "paused"
PIPELINE_IDLE = "idle"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PipelineOrchestratorService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._jobs = mongodb.collection(settings.mongodb_domain_jobs_collection)
        self._pipeline_runs = mongodb.collection(settings.mongodb_pipeline_runs_collection)
        self._reports = mongodb.collection(settings.mongodb_client_job_reports_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._processes.create_index([("pipeline_enabled", ASCENDING), ("next_pipeline_run_at", ASCENDING)])
        self._processes.create_index([("pipeline_status", ASCENDING), ("pipeline_heartbeat_at", ASCENDING)])
        self._pipeline_runs.create_index([("pipeline_run_id", ASCENDING)], unique=True)
        self._pipeline_runs.create_index([("status", ASCENDING), ("heartbeat_at", ASCENDING)])
        self._pipeline_runs.create_index([("process_id", ASCENDING), ("started_at", DESCENDING)])
        self._reports.create_index([("process_id", ASCENDING), ("generated_at", DESCENDING)])
        self._reports.create_index([("pipeline_run_id", ASCENDING)], unique=True)
        self._indexes_ready = True

    def start_process_pipeline(
        self,
        process_id: str,
        *,
        trigger: str = "manual",
        force: bool = False,
    ) -> dict[str, Any]:
        self.ensure_indexes()
        process = self._load_process(process_id)
        if not self._is_enabled(process):
            raise RuntimeError("Pipeline is paused for this process")
        if self._is_pipeline_active(process) and not force:
            raise RuntimeError("Pipeline is already running for this process")
        run = self._create_run(process, trigger=trigger, force=force)
        self._mark_process_running(process_id, run["pipeline_run_id"], trigger=trigger)
        self._enqueue_run(run["pipeline_run_id"])
        log_event(
            logger,
            "info",
            "pipeline_process_enqueued",
            domain="pipeline",
            process_id=process_id,
            pipeline_run_id=run["pipeline_run_id"],
            trigger=trigger,
        )
        return {"process_id": process_id, "pipeline_run_id": run["pipeline_run_id"], "status": PIPELINE_RUNNING}

    def start_due_pipelines(self, *, limit: int = 25, trigger: str = "scheduled") -> dict[str, Any]:
        self.ensure_indexes()
        due = list(self._processes.find(self._due_filter()).sort("next_pipeline_run_at", ASCENDING).limit(limit))
        started = []
        skipped = []
        for process in due:
            try:
                started.append(self.start_process_pipeline(process["process_id"], trigger=trigger))
            except Exception as exc:
                skipped.append({"process_id": process.get("process_id"), "error": str(exc)})
        if started or skipped:
            log_event(
                logger,
                "info",
                "pipeline_due_scan_completed",
                domain="pipeline",
                started=len(started),
                skipped=len(skipped),
            )
        return {"started": started, "skipped": skipped, "count": len(started)}

    def start_all_enabled(self, *, trigger: str = "manual_all") -> dict[str, Any]:
        self.ensure_indexes()
        processes = list(self._processes.find({"pipeline_enabled": {"$ne": False}}).sort("created_at", DESCENDING))
        started = []
        skipped = []
        for process in processes:
            if self._is_pipeline_active(process):
                skipped.append({"process_id": process.get("process_id"), "reason": "already_running"})
                continue
            try:
                started.append(self.start_process_pipeline(process["process_id"], trigger=trigger))
            except Exception as exc:
                skipped.append({"process_id": process.get("process_id"), "reason": str(exc)})
        return {"started": started, "skipped": skipped, "count": len(started)}

    def pause_process(self, process_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        process = self._processes.find_one_and_update(
            {"process_id": process_id},
            {
                "$set": {
                    "pipeline_enabled": False,
                    "pipeline_paused_at": _now(),
                    "pipeline_status": PIPELINE_PAUSED,
                    "updated_at": _now(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        return {"process_id": process_id, "pipeline_enabled": False, "pipeline_status": PIPELINE_PAUSED}

    def resume_process(self, process_id: str) -> dict[str, Any]:
        self.ensure_indexes()
        process = self._processes.find_one_and_update(
            {"process_id": process_id},
            {
                "$set": {
                    "pipeline_enabled": True,
                    "pipeline_status": PIPELINE_IDLE,
                    "next_pipeline_run_at": _now(),
                    "updated_at": _now(),
                },
                "$unset": {"pipeline_paused_at": "", "pipeline_last_error": ""},
            },
            return_document=ReturnDocument.AFTER,
        )
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        return {"process_id": process_id, "pipeline_enabled": True, "pipeline_status": process.get("pipeline_status")}

    def execute_pipeline_run(self, pipeline_run_id: str, *, celery_task_id: str | None = None) -> dict[str, Any]:
        self.ensure_indexes()
        run = self._load_run(pipeline_run_id)
        process_id = str(run["process_id"])
        try:
            self._attach_worker_run(pipeline_run_id, celery_task_id)
            self._run_stage(pipeline_run_id, "search", lambda: self._maybe_run_search(process_id))
            self._run_stage(pipeline_run_id, "category", lambda: self._maybe_run_category(process_id))
            self._run_stage(pipeline_run_id, "pattern", lambda: self._maybe_run_pattern(process_id))
            self._run_stage(pipeline_run_id, "pagination", lambda: self._maybe_run_pagination(process_id))
            self._run_stage(pipeline_run_id, "jobs", lambda: self._maybe_run_jobs(process_id))
            report = self._create_report(pipeline_run_id, process_id)
            self._complete_run(pipeline_run_id, process_id, report)
            return {"status": PIPELINE_COMPLETED, "pipeline_run_id": pipeline_run_id, "report": report}
        except Exception as exc:
            self._fail_run(pipeline_run_id, process_id, str(exc))
            log_event(
                logger,
                "error",
                "pipeline_run_failed",
                domain="pipeline",
                process_id=process_id,
                pipeline_run_id=pipeline_run_id,
                error=str(exc),
            )
            return {"status": PIPELINE_FAILED, "pipeline_run_id": pipeline_run_id, "error": str(exc)}

    def repair_stale_runs(self) -> int:
        self.ensure_indexes()
        threshold = _now() - timedelta(seconds=self._settings.pipeline_stale_seconds)
        stale_runs = list(
            self._pipeline_runs.find(
                {"status": PIPELINE_RUNNING, "heartbeat_at": {"$lt": threshold}},
                {"pipeline_run_id": 1, "process_id": 1},
            )
        )
        for run in stale_runs:
            error = "Pipeline heartbeat expired; marked stale by watchdog"
            self._fail_run(str(run["pipeline_run_id"]), str(run["process_id"]), error, schedule_retry=True)
        if stale_runs:
            log_event(logger, "warning", "stale_pipeline_runs_repaired", domain="pipeline", count=len(stale_runs))
        return len(stale_runs)

    def list_runs(self, *, process_id: str | None = None, limit: int = 25) -> dict[str, Any]:
        self.ensure_indexes()
        query = {"process_id": process_id} if process_id else {}
        runs = list(self._pipeline_runs.find(query, {"_id": 0}).sort("started_at", DESCENDING).limit(limit))
        return {"runs": runs, "count": len(runs)}

    def _due_filter(self) -> dict[str, Any]:
        return {
            "pipeline_enabled": True,
            "pipeline_status": {"$ne": PIPELINE_RUNNING},
            "$or": [
                {"next_pipeline_run_at": {"$exists": False}},
                {"next_pipeline_run_at": None},
                {"next_pipeline_run_at": {"$lte": _now()}},
            ],
        }

    def _load_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        return process

    def _load_run(self, pipeline_run_id: str) -> dict[str, Any]:
        run = self._pipeline_runs.find_one({"pipeline_run_id": pipeline_run_id})
        if not run:
            raise ValueError(f"Pipeline run '{pipeline_run_id}' was not found")
        return run

    def _is_enabled(self, process: dict[str, Any]) -> bool:
        return process.get("pipeline_enabled") is not False

    def _is_pipeline_active(self, process: dict[str, Any]) -> bool:
        return process.get("pipeline_status") == PIPELINE_RUNNING

    def _create_run(self, process: dict[str, Any], *, trigger: str, force: bool) -> dict[str, Any]:
        timestamp = _now()
        run = {
            "pipeline_run_id": uuid4().hex,
            "process_id": process["process_id"],
            "client": process.get("client") or {},
            "status": PIPELINE_RUNNING,
            "trigger": trigger,
            "force": force,
            "stages": {},
            "started_at": timestamp,
            "updated_at": timestamp,
            "heartbeat_at": timestamp,
        }
        self._pipeline_runs.insert_one(run)
        return run

    def _mark_process_running(self, process_id: str, pipeline_run_id: str, *, trigger: str) -> None:
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "pipeline_enabled": True,
                    "pipeline_status": PIPELINE_RUNNING,
                    "pipeline_current_run_id": pipeline_run_id,
                    "pipeline_trigger": trigger,
                    "pipeline_started_at": _now(),
                    "pipeline_heartbeat_at": _now(),
                    "updated_at": _now(),
                },
                "$unset": {"pipeline_last_error": ""},
            },
        )

    def _enqueue_run(self, pipeline_run_id: str) -> None:
        from infrastructure.tasks import run_pipeline_process

        run_pipeline_process.apply_async(args=[pipeline_run_id], queue="pipeline")

    def _attach_worker_run(self, pipeline_run_id: str, celery_task_id: str | None) -> None:
        self._pipeline_runs.update_one(
            {"pipeline_run_id": pipeline_run_id},
            {"$set": {"celery_task_id": celery_task_id, "updated_at": _now(), "heartbeat_at": _now()}},
        )

    def _run_stage(self, pipeline_run_id: str, stage: str, action: Callable[[], dict[str, Any]]) -> None:
        self._stage_started(pipeline_run_id, stage)
        result = action()
        self._stage_finished(pipeline_run_id, stage, result)

    def _stage_started(self, pipeline_run_id: str, stage: str) -> None:
        self._pipeline_runs.update_one(
            {"pipeline_run_id": pipeline_run_id},
            {
                "$set": {
                    f"stages.{stage}.status": PIPELINE_RUNNING,
                    f"stages.{stage}.started_at": _now(),
                    "updated_at": _now(),
                    "heartbeat_at": _now(),
                }
            },
        )

    def _stage_finished(self, pipeline_run_id: str, stage: str, result: dict[str, Any]) -> None:
        self._pipeline_runs.update_one(
            {"pipeline_run_id": pipeline_run_id},
            {
                "$set": {
                    f"stages.{stage}.status": result.get("status", PIPELINE_COMPLETED),
                    f"stages.{stage}.result": result,
                    f"stages.{stage}.completed_at": _now(),
                    "updated_at": _now(),
                    "heartbeat_at": _now(),
                }
            },
        )

    def _heartbeat(self, pipeline_run_id: str, process_id: str, stage: str | None = None) -> None:
        fields = {"heartbeat_at": _now(), "updated_at": _now()}
        if stage:
            fields[f"stages.{stage}.heartbeat_at"] = _now()
        self._pipeline_runs.update_one({"pipeline_run_id": pipeline_run_id}, {"$set": fields})
        self._processes.update_one(
            {"process_id": process_id},
            {"$set": {"pipeline_heartbeat_at": _now(), "updated_at": _now()}},
        )

    def _maybe_run_search(self, process_id: str) -> dict[str, Any]:
        process = self._load_process(process_id)
        if not self._should_run_search(process):
            return {"status": "skipped", "reason": "fresh_success_or_no_due_work"}
        result = get_search_node_service().start_process(process_id)
        terminal = self._wait_for_process_status(process_id, "status", pipeline_stage="search")
        self._remember_stage_timestamp(process_id, "pipeline_last_search_at")
        return {"status": terminal, "start": result}

    def _maybe_run_category(self, process_id: str) -> dict[str, Any]:
        process = self._load_process(process_id)
        if not self._should_run_category(process):
            return {"status": "skipped", "reason": "fresh_success_or_search_not_ready"}
        result = get_career_category_node_service().start_process(process_id)
        terminal = self._wait_for_process_status(process_id, "career_status", pipeline_stage="category")
        self._remember_stage_timestamp(process_id, "pipeline_last_category_at")
        return {"status": terminal, "start": result}

    def _maybe_run_pattern(self, process_id: str) -> dict[str, Any]:
        process = self._load_process(process_id)
        if process.get("career_status") not in {"completed", "partial_completed"}:
            return {"status": "skipped", "reason": "category_not_successful"}
        mode = "rerun" if self._node_started(process.get("job_pattern_status")) else "start"
        result = get_job_pattern_node_service().start_process(process_id, mode=mode)
        terminal = self._wait_for_process_status(process_id, "job_pattern_status", pipeline_stage="pattern")
        self._remember_stage_timestamp(process_id, "pipeline_last_pattern_at")
        return {"status": terminal, "mode": mode, "start": result}

    def _maybe_run_pagination(self, process_id: str) -> dict[str, Any]:
        process = self._load_process(process_id)
        if process.get("job_pattern_status") not in {"completed", "partial_completed"}:
            return {"status": "skipped", "reason": "pattern_not_successful"}
        mode = "rerun" if self._node_started(process.get("job_pagination_status")) else "start"
        result = get_job_pagination_node_service().start_process(process_id, mode=mode)
        terminal = self._wait_for_process_status(process_id, "job_pagination_status", pipeline_stage="pagination")
        self._remember_stage_timestamp(process_id, "pipeline_last_pagination_at")
        return {"status": terminal, "mode": mode, "start": result}

    def _maybe_run_jobs(self, process_id: str) -> dict[str, Any]:
        process = self._load_process(process_id)
        if process.get("job_pattern_status") not in {"completed", "partial_completed"}:
            return {"status": "skipped", "reason": "pattern_not_successful"}
        mode = "rerun" if self._node_started(process.get("job_extraction_status")) else "start"
        result = get_job_extraction_node_service().start_process(process_id, mode=mode)
        terminal = self._wait_for_process_status(process_id, "job_extraction_status", pipeline_stage="jobs")
        self._remember_stage_timestamp(process_id, "pipeline_last_job_extraction_at")
        return {"status": terminal, "mode": mode, "start": result}

    def _should_run_search(self, process: dict[str, Any]) -> bool:
        totals = process.get("totals") or {}
        if process.get("status") == "running" or int(totals.get("processing") or 0) > 0:
            return False
        if process.get("status") not in {"completed", "partial_completed"}:
            return True
        last_run = self._as_datetime(process.get("pipeline_last_search_at") or process.get("updated_at"))
        return not last_run or last_run <= _now() - timedelta(hours=self._settings.pipeline_search_refresh_hours)

    def _should_run_category(self, process: dict[str, Any]) -> bool:
        totals = process.get("totals") or {}
        if int(totals.get("queued") or 0) > 0 or int(totals.get("processing") or 0) > 0:
            return False
        if process.get("career_status") in ACTIVE_STATUSES:
            return False
        if process.get("career_status") not in {"completed", "partial_completed"}:
            return int(totals.get("completed") or 0) > 0
        last_run = self._as_datetime(process.get("pipeline_last_category_at") or process.get("career_completed_at"))
        return not last_run or last_run <= _now() - timedelta(hours=self._settings.pipeline_category_refresh_hours)

    def _wait_for_process_status(self, process_id: str, status_field: str, *, pipeline_stage: str) -> str:
        deadline = time.monotonic() + self._settings.pipeline_node_wait_timeout_seconds
        while time.monotonic() < deadline:
            process = self._load_process(process_id)
            status = str(process.get(status_field) or "")
            self._heartbeat(str(process.get("pipeline_current_run_id") or ""), process_id, pipeline_stage)
            if status in TERMINAL_STATUSES:
                return status
            if status not in ACTIVE_STATUSES:
                return status or "unknown"
            time.sleep(max(1, self._settings.pipeline_poll_interval_seconds))
        raise TimeoutError(f"Pipeline stage '{pipeline_stage}' timed out waiting for {status_field}")

    def _remember_stage_timestamp(self, process_id: str, field: str) -> None:
        self._processes.update_one({"process_id": process_id}, {"$set": {field: _now(), "updated_at": _now()}})

    def _node_started(self, status: Any) -> bool:
        return bool(status and status != "not_started")

    def _create_report(self, pipeline_run_id: str, process_id: str) -> dict[str, Any]:
        process = self._load_process(process_id)
        previous_report_at = self._previous_report_time(process_id)
        domains = self._process_domains(process)
        jobs = self._new_jobs(domains, previous_report_at)
        report = {
            "report_id": uuid4().hex,
            "pipeline_run_id": pipeline_run_id,
            "process_id": process_id,
            "client": process.get("client") or {},
            "status": "ready",
            "generated_at": _now(),
            "since": previous_report_at,
            "domains": domains,
            "new_jobs_count": len(jobs),
            "jobs": jobs,
        }
        self._reports.update_one({"pipeline_run_id": pipeline_run_id}, {"$set": report}, upsert=True)
        return {"report_id": report["report_id"], "new_jobs_count": report["new_jobs_count"], "status": "ready"}

    def _previous_report_time(self, process_id: str) -> datetime | None:
        report = self._reports.find_one({"process_id": process_id, "status": "ready"}, sort=[("generated_at", DESCENDING)])
        return (report or {}).get("generated_at")

    def _process_domains(self, process: dict[str, Any]) -> list[str]:
        refs = list(process.get("domains", {}).get("completed", []) or [])
        return [str(ref.get("registered_domain") or "") for ref in refs if ref.get("registered_domain")]

    def _new_jobs(self, domains: list[str], since: datetime | None) -> list[dict[str, Any]]:
        if not domains:
            return []
        query: dict[str, Any] = {"registered_domain": {"$in": domains}, "status": "active"}
        if since:
            query["first_seen_at"] = {"$gt": since}
        cursor = self._jobs.find(
            query,
            {"_id": 0, "job_key": 1, "registered_domain": 1, "title": 1, "job_url": 1, "source_url": 1, "first_seen_at": 1},
        ).sort("first_seen_at", DESCENDING)
        return list(cursor)

    def _complete_run(self, pipeline_run_id: str, process_id: str, report: dict[str, Any]) -> None:
        timestamp = _now()
        next_run = timestamp + timedelta(hours=self._settings.pipeline_daily_interval_hours)
        self._pipeline_runs.update_one(
            {"pipeline_run_id": pipeline_run_id},
            {
                "$set": {
                    "status": PIPELINE_COMPLETED,
                    "completed_at": timestamp,
                    "updated_at": timestamp,
                    "heartbeat_at": timestamp,
                    "report": report,
                }
            },
        )
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "pipeline_status": PIPELINE_COMPLETED,
                    "pipeline_completed_at": timestamp,
                    "last_pipeline_run_at": timestamp,
                    "next_pipeline_run_at": next_run,
                    "pipeline_last_report": report,
                    "pipeline_consecutive_failures": 0,
                    "updated_at": timestamp,
                },
                "$unset": {"pipeline_current_run_id": "", "pipeline_last_error": ""},
            },
        )
        self._build_completion_alert(process_id, pipeline_run_id)

    def _build_completion_alert(self, process_id: str, pipeline_run_id: str) -> None:
        try:
            from services.client_job_alert_service import get_client_job_alert_service

            alert = get_client_job_alert_service().build_for_pipeline_run(process_id, pipeline_run_id)
            self._processes.update_one(
                {"process_id": process_id},
                {"$set": {"alert_last_summary": alert, "alert_last_built_at": _now(), "updated_at": _now()}},
            )
        except Exception as exc:
            log_event(
                logger,
                "error",
                "pipeline_completion_alert_failed",
                domain="pipeline",
                process_id=process_id,
                pipeline_run_id=pipeline_run_id,
                error=str(exc),
            )

    def _fail_run(
        self,
        pipeline_run_id: str,
        process_id: str,
        error: str,
        *,
        schedule_retry: bool = False,
    ) -> None:
        timestamp = _now()
        next_run = timestamp if schedule_retry else timestamp + timedelta(hours=self._settings.pipeline_daily_interval_hours)
        self._pipeline_runs.update_one(
            {"pipeline_run_id": pipeline_run_id},
            {
                "$set": {
                    "status": PIPELINE_FAILED,
                    "failed_at": timestamp,
                    "updated_at": timestamp,
                    "last_error": error,
                }
            },
        )
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "pipeline_status": PIPELINE_FAILED,
                    "pipeline_last_error": error,
                    "next_pipeline_run_at": next_run,
                    "updated_at": timestamp,
                },
                "$inc": {"pipeline_consecutive_failures": 1},
                "$unset": {"pipeline_current_run_id": ""},
            },
        )
        self._pause_after_repeated_failures(process_id)

    def _pause_after_repeated_failures(self, process_id: str) -> None:
        process = self._processes.find_one({"process_id": process_id}, {"pipeline_consecutive_failures": 1})
        failures = int((process or {}).get("pipeline_consecutive_failures") or 0)
        if failures < self._settings.pipeline_failure_suppression_attempts:
            return
        self._processes.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "pipeline_enabled": False,
                    "pipeline_status": PIPELINE_PAUSED,
                    "pipeline_paused_at": _now(),
                    "pipeline_last_error": "Pipeline paused after repeated failures",
                    "updated_at": _now(),
                }
            },
        )

    def _as_datetime(self, value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return None


@lru_cache(maxsize=1)
def get_pipeline_orchestrator_service() -> PipelineOrchestratorService:
    return PipelineOrchestratorService(get_sync_mongodb_service(), get_settings())
