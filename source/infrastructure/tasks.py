from __future__ import annotations

from typing import Any

from celery import Task

from core.config import get_settings
from infrastructure.celery_app import celery_app
from services.career_category_node_processor import (
    CareerCategoryNodeProcessor,
    NoCareerCategorySessionSlotAvailable,
)
from services.career_process_service import get_career_process_service
from services.job_extraction_node_processor import (
    JobExtractionNodeProcessor,
    NoJobExtractionSessionSlotAvailable,
)
from services.job_extraction_node_service import get_job_extraction_node_service
from services.job_pattern_node_processor import JobPatternNodeProcessor, NoJobPatternSessionSlotAvailable
from services.job_pattern_node_service import get_job_pattern_node_service
from services.failure_classifier import classify_failure
from services.node_lifecycle import retry_policy
from services.node_run_history_service import get_node_run_history_service
from services.process_runtime_service import get_process_runtime_service
from services.search_node_processor import NoSearchNodeSessionSlotAvailable, SearchNodeProcessor
from utils.logging import get_logger, log_event


settings = get_settings()
logger = get_logger("celery_domain_tasks")


@celery_app.task(bind=True, max_retries=None, name="infrastructure.tasks.run_search_node")
def run_search_node(self: Task, process_id: str, registered_domain: str) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "search_node_task_received",
        domain="worker",
        process_id=process_id,
        registered_domain=registered_domain,
        celery_task_id=str(self.request.id),
    )
    runtime = get_process_runtime_service()
    claim = runtime.claim_domain_for_process(
        process_id=process_id,
        registered_domain=registered_domain,
        worker_name=_worker_name(self),
        task_id=str(self.request.id),
    )
    return _handle_claim(self, process_id, registered_domain, claim)


@celery_app.task(bind=True, max_retries=None, name="infrastructure.tasks.run_career_category_node")
def run_career_category_node(self: Task, process_id: str, registered_domain: str) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "career_category_task_received",
        domain="worker",
        process_id=process_id,
        registered_domain=registered_domain,
        celery_task_id=str(self.request.id),
    )
    service = get_career_process_service()
    claim = service.claim_category_task(
        registered_domain,
        _worker_name(self),
        str(self.request.id),
    )
    return _handle_category_claim(self, process_id, registered_domain, claim)


@celery_app.task(bind=True, max_retries=None, name="infrastructure.tasks.run_job_extraction_node")
def run_job_extraction_node(self: Task, process_id: str, registered_domain: str) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "job_extraction_task_received",
        domain="worker",
        process_id=process_id,
        registered_domain=registered_domain,
        celery_task_id=str(self.request.id),
    )
    service = get_job_extraction_node_service()
    claim = service.claim_task(registered_domain, _worker_name(self), str(self.request.id))
    return _handle_job_extraction_claim(self, process_id, registered_domain, claim)


@celery_app.task(bind=True, max_retries=None, name="infrastructure.tasks.run_job_pattern_node")
def run_job_pattern_node(self: Task, process_id: str, registered_domain: str) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "job_pattern_task_received",
        domain="worker",
        process_id=process_id,
        registered_domain=registered_domain,
        celery_task_id=str(self.request.id),
    )
    service = get_job_pattern_node_service()
    claim = service.claim_task(registered_domain, _worker_name(self), str(self.request.id))
    return _handle_job_pattern_claim(self, process_id, registered_domain, claim)


def _handle_job_pattern_claim(
    task: Task,
    process_id: str,
    registered_domain: str,
    claim: dict[str, Any],
) -> dict[str, Any]:
    status = claim["status"]
    service = get_job_pattern_node_service()
    if status == "claimed":
        claim["task"]["dispatch_process_id"] = process_id
        service.mark_process_task_running(process_id)
        return _run_job_pattern_processing(task, claim["task"])
    if status == "max_attempts_exceeded":
        service.mark_process_queued_task_failed(process_id, "Maximum attempts exceeded")
        return {"status": "failed", "registered_domain": registered_domain, "error": "Maximum attempts exceeded"}
    return {"status": status, "registered_domain": registered_domain}


def _run_job_pattern_processing(task: Task, node_task: dict[str, Any]) -> dict[str, Any]:
    service = get_job_pattern_node_service()
    process_id = str(node_task.get("dispatch_process_id") or "")
    run_id = _start_node_run("job_pattern", process_id, node_task)
    try:
        result = JobPatternNodeProcessor().process(
            node_task,
            process_id=process_id,
            worker_name=str(node_task["worker_name"]),
            celery_task_id=str(node_task["celery_task_id"]),
        )
    except NoJobPatternSessionSlotAvailable as exc:
        service.requeue_task(node_task["registered_domain"], str(exc), decrement_attempt=True)
        service.mark_process_task_requeued(process_id, str(exc))
        _requeue_node_run(run_id, str(exc))
        raise task.retry(args=[process_id, node_task["registered_domain"]], countdown=_retry_countdown("job_pattern"))
    except Exception as exc:
        service.fail_task(node_task["registered_domain"], str(exc), process_id=process_id)
        _fail_node_run(run_id, str(exc))
        _log_job_pattern_processing_failed(node_task, str(exc))
        return {"status": "failed", "registered_domain": node_task["registered_domain"], "error": str(exc)}
    service.complete_task(node_task["registered_domain"], result, process_id=process_id)
    _complete_node_run(run_id, result)
    _log_job_pattern_processing_completed(node_task, result)
    return {"status": "completed", "registered_domain": node_task["registered_domain"]}


def _handle_job_extraction_claim(
    task: Task,
    process_id: str,
    registered_domain: str,
    claim: dict[str, Any],
) -> dict[str, Any]:
    status = claim["status"]
    service = get_job_extraction_node_service()
    if status == "claimed":
        claim["task"]["dispatch_process_id"] = process_id
        service.mark_process_task_running(process_id)
        return _run_job_extraction_processing(task, claim["task"])
    if status == "max_attempts_exceeded":
        service.mark_process_queued_task_failed(process_id, "Maximum attempts exceeded")
        return {"status": "failed", "registered_domain": registered_domain, "error": "Maximum attempts exceeded"}
    return {"status": status, "registered_domain": registered_domain}


def _run_job_extraction_processing(task: Task, node_task: dict[str, Any]) -> dict[str, Any]:
    service = get_job_extraction_node_service()
    process_id = str(node_task.get("dispatch_process_id") or "")
    run_id = _start_node_run("job_extraction", process_id, node_task)
    try:
        result = JobExtractionNodeProcessor().process(
            node_task,
            process_id=process_id,
            worker_name=str(node_task["worker_name"]),
            celery_task_id=str(node_task["celery_task_id"]),
        )
    except NoJobExtractionSessionSlotAvailable as exc:
        service.requeue_task(node_task["registered_domain"], str(exc), decrement_attempt=True)
        service.mark_process_task_requeued(process_id, str(exc))
        _requeue_node_run(run_id, str(exc))
        raise task.retry(args=[process_id, node_task["registered_domain"]], countdown=_retry_countdown("job_extraction"))
    except Exception as exc:
        service.fail_task(node_task["registered_domain"], str(exc), process_id=process_id)
        _fail_node_run(run_id, str(exc))
        _log_job_extraction_processing_failed(node_task, str(exc))
        return {"status": "failed", "registered_domain": node_task["registered_domain"], "error": str(exc)}
    service.complete_task(node_task["registered_domain"], result, process_id=process_id)
    _complete_node_run(run_id, result)
    _log_job_extraction_processing_completed(node_task, result)
    return {"status": "completed", "registered_domain": node_task["registered_domain"]}


def _handle_category_claim(
    task: Task,
    process_id: str,
    registered_domain: str,
    claim: dict[str, Any],
) -> dict[str, Any]:
    status = claim["status"]
    if status == "claimed":
        claim["task"]["dispatch_process_id"] = process_id
        get_career_process_service().mark_process_task_running(process_id)
        return _run_category_processing(task, claim["task"])
    if status == "max_attempts_exceeded":
        get_career_process_service().mark_process_queued_task_failed(process_id, "Maximum attempts exceeded")
        return _category_task_response(
            "failed",
            process_id=process_id,
            registered_domain=registered_domain,
            error="Maximum attempts exceeded",
        )
    return _category_task_response(status, process_id=process_id, registered_domain=registered_domain)


def _run_category_processing(task: Task, node_task: dict[str, Any]) -> dict[str, Any]:
    service = get_career_process_service()
    process_id = str(node_task.get("dispatch_process_id") or "")
    run_id = _start_node_run("career_category", process_id, node_task)
    try:
        result = CareerCategoryNodeProcessor().process(
            node_task,
            process_id=node_task.get("dispatch_process_id"),
            worker_name=str(node_task["worker_name"]),
            celery_task_id=str(node_task["celery_task_id"]),
        )
    except NoCareerCategorySessionSlotAvailable as exc:
        service.requeue_task(
            node_task["registered_domain"],
            str(exc),
            decrement_attempt=True,
        )
        service.mark_process_task_requeued(str(node_task.get("dispatch_process_id") or ""), str(exc))
        _requeue_node_run(run_id, str(exc))
        raise task.retry(args=[node_task.get("dispatch_process_id"), node_task["registered_domain"]], countdown=_retry_countdown("career_category"))
    except Exception as exc:
        error = str(exc)
        service.fail_task(
            node_task["registered_domain"],
            error,
            process_id=node_task.get("dispatch_process_id"),
        )
        _fail_node_run(run_id, error)
        _log_category_processing_failed(node_task, error)
        return _category_task_response("failed", node_task=node_task, process_id=process_id, run_id=run_id, error=error)
    service.complete_task(
        node_task["registered_domain"],
        result,
        process_id=node_task.get("dispatch_process_id"),
    )
    _complete_node_run(run_id, result)
    _log_category_processing_completed(node_task, result)
    return _category_task_response("completed", node_task=node_task, process_id=process_id, run_id=run_id, result=result)


def _handle_claim(
    task: Task,
    process_id: str,
    registered_domain: str,
    claim: dict[str, Any],
) -> dict[str, Any]:
    status = claim["status"]
    if status == "claimed":
        return _run_search_processing(task, process_id, claim["domain_ref"])
    if status == "fresh_completed":
        return _complete_with_reuse(process_id, registered_domain)
    if status == "max_attempts_exceeded":
        return _fail_queued_domain(process_id, registered_domain, "Maximum attempts exceeded")
    if status in {"busy", "process_at_capacity"}:
        raise task.retry(countdown=10)
    return {"status": status, "registered_domain": registered_domain}


def _run_search_processing(task: Task, process_id: str, domain_ref: dict[str, Any]) -> dict[str, Any]:
    runtime = get_process_runtime_service()
    run_id = _start_node_run("search", process_id, domain_ref)
    try:
        _log_domain_processing_started(process_id, domain_ref)
        result = SearchNodeProcessor().process(
            process_id,
            domain_ref,
            worker_name=str(domain_ref["worker_name"]),
            task_id=str(domain_ref["celery_task_id"]),
        )
    except NoSearchNodeSessionSlotAvailable as exc:
        runtime.requeue_domain(process_id, domain_ref, str(exc), decrement_attempt=True)
        _requeue_node_run(run_id, str(exc))
        raise task.retry(args=[process_id, domain_ref["registered_domain"]], countdown=_retry_countdown("search"))
    except Exception as exc:
        runtime.fail_domain(process_id, domain_ref, str(exc))
        _fail_node_run(run_id, str(exc))
        _log_domain_processing_failed(process_id, domain_ref, str(exc))
        return {"status": "failed", "registered_domain": domain_ref["registered_domain"], "error": str(exc)}
    if not result.get("success"):
        error = str(result.get("error") or result.get("status") or "Search node failed")
        runtime.fail_domain(process_id, domain_ref, error, result)
        _fail_node_run(run_id, error, result)
        _log_domain_processing_failed(process_id, domain_ref, error)
        return {"status": "failed", "registered_domain": domain_ref["registered_domain"], "error": error, "result": result}
    runtime.complete_domain(process_id, domain_ref, result)
    _complete_node_run(run_id, result)
    _log_domain_processing_completed(process_id, domain_ref)
    return {"status": "completed", "registered_domain": domain_ref["registered_domain"]}


def _complete_with_reuse(
    process_id: str,
    registered_domain: str,
) -> dict[str, Any]:
    runtime = get_process_runtime_service()
    runtime.complete_with_reused_result(process_id, registered_domain)
    log_event(
        logger,
        "info",
        "domain_result_reused",
        domain="worker",
        process_id=process_id,
        registered_domain=registered_domain,
    )
    return {"status": "reused", "registered_domain": registered_domain}


def _fail_queued_domain(process_id: str, registered_domain: str, error: str) -> dict[str, Any]:
    runtime = get_process_runtime_service()
    runtime.fail_queued_domain(process_id, registered_domain, error)
    log_event(
        logger,
        "warning",
        "domain_task_failed_before_claim",
        domain="worker",
        process_id=process_id,
        registered_domain=registered_domain,
        error=error,
    )
    return {"status": "failed", "registered_domain": registered_domain, "error": error}


def _category_task_response(
    status: str,
    *,
    process_id: str | None,
    registered_domain: str | None = None,
    node_task: dict[str, Any] | None = None,
    run_id: str | None = None,
    error: str | None = None,
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "node": "career_category",
        "process_id": process_id,
        "registered_domain": registered_domain or (node_task or {}).get("registered_domain"),
        "run_id": run_id,
        "worker_name": (node_task or {}).get("worker_name"),
        "celery_task_id": (node_task or {}).get("celery_task_id"),
        "input": (node_task or {}).get("input"),
    }
    if error:
        payload["error"] = error
        payload["failure_type"] = classify_failure(error)
    if result is not None:
        payload["result"] = result
        payload["summary"] = {
            "jobs_found": result.get("jobs_found"),
            "total_jobs_found": result.get("total_jobs_found"),
            "outcome": result.get("outcome"),
            "pattern_count": len(result.get("job_listing_patterns") or []),
            "duration_seconds": result.get("duration_seconds"),
        }
    return {key: value for key, value in payload.items() if value is not None}


def _start_node_run(node: str, process_id: str | None, node_task: dict[str, Any]) -> str:
    return get_node_run_history_service().start_run(
        node=node,
        process_id=process_id,
        registered_domain=node_task.get("registered_domain"),
        worker_name=node_task.get("worker_name"),
        celery_task_id=node_task.get("celery_task_id"),
        attempt=_node_attempt(node, node_task),
        metadata={"domain": node_task.get("domain")},
    )


def _complete_node_run(run_id: str, result: dict[str, Any]) -> None:
    get_node_run_history_service().complete_run(run_id, result)


def _fail_node_run(run_id: str, error: str, result: dict[str, Any] | None = None) -> None:
    get_node_run_history_service().fail_run(run_id, error, result)


def _requeue_node_run(run_id: str, error: str) -> None:
    get_node_run_history_service().requeue_run(run_id, error)


def _node_attempt(node: str, node_task: dict[str, Any]) -> int | None:
    key = {
        "search": "attempts",
        "career_category": "career_process_attempts",
        "job_pattern": "job_pattern_attempts",
        "job_extraction": "job_extraction_attempts",
    }.get(node)
    if not key:
        return None
    value = node_task.get(key)
    return int(value) if value is not None else None


def _retry_countdown(node: str) -> int:
    return retry_policy(node, settings.task_max_attempts).retry_countdown_seconds


def _log_domain_processing_started(process_id: str, domain_ref: dict[str, Any]) -> None:
    log_event(
        logger,
        "info",
        "domain_processing_started",
        domain="worker",
        process_id=process_id,
        registered_domain=domain_ref["registered_domain"],
        celery_task_id=domain_ref["celery_task_id"],
    )


def _log_domain_processing_completed(process_id: str, domain_ref: dict[str, Any]) -> None:
    log_event(
        logger,
        "info",
        "domain_processing_completed",
        domain="worker",
        process_id=process_id,
        registered_domain=domain_ref["registered_domain"],
    )


def _log_domain_processing_failed(process_id: str, domain_ref: dict[str, Any], error: str) -> None:
    log_event(
        logger,
        "warning",
        "domain_processing_failed",
        domain="worker",
        process_id=process_id,
        registered_domain=domain_ref["registered_domain"],
        error=error,
    )


def _log_category_processing_completed(node_task: dict[str, Any], result: dict[str, Any]) -> None:
    log_event(
        logger,
        "info",
        "career_category_processing_completed",
        domain="worker",
        process_id=node_task.get("dispatch_process_id"),
        registered_domain=node_task["registered_domain"],
        outcome=result.get("outcome"),
        jobs_found=result.get("jobs_found"),
    )


def _log_category_processing_failed(node_task: dict[str, Any], error: str) -> None:
    log_event(
        logger,
        "warning",
        "career_category_processing_failed",
        domain="worker",
        process_id=node_task.get("dispatch_process_id"),
        registered_domain=node_task["registered_domain"],
        error=error,
    )


def _log_job_extraction_processing_completed(node_task: dict[str, Any], result: dict[str, Any]) -> None:
    log_event(
        logger,
        "info",
        "job_extraction_processing_completed",
        domain="worker",
        process_id=node_task.get("dispatch_process_id"),
        registered_domain=node_task["registered_domain"],
        stored_jobs=(result.get("job_storage") or {}).get("seen"),
    )


def _log_job_pattern_processing_completed(node_task: dict[str, Any], result: dict[str, Any]) -> None:
    log_event(
        logger,
        "info",
        "job_pattern_processing_completed",
        domain="worker",
        process_id=node_task.get("dispatch_process_id"),
        registered_domain=node_task["registered_domain"],
        pattern_count=len(result.get("job_listing_patterns") or []),
    )


def _log_job_pattern_processing_failed(node_task: dict[str, Any], error: str) -> None:
    log_event(
        logger,
        "warning",
        "job_pattern_processing_failed",
        domain="worker",
        process_id=node_task.get("dispatch_process_id"),
        registered_domain=node_task["registered_domain"],
        error=error,
    )


def _log_job_extraction_processing_failed(node_task: dict[str, Any], error: str) -> None:
    log_event(
        logger,
        "warning",
        "job_extraction_processing_failed",
        domain="worker",
        process_id=node_task.get("dispatch_process_id"),
        registered_domain=node_task["registered_domain"],
        error=error,
    )


def _worker_name(task: Task) -> str:
    return str(getattr(task.request, "hostname", None) or "worker")
