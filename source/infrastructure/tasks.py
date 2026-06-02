from __future__ import annotations

from typing import Any

from celery import Task

from core.config import get_settings
from infrastructure.celery_app import celery_app
from services.career_category_node_processor import (
    CareerCategoryNodeProcessor,
    NoCareerCategorySessionSlotAvailable,
)
from services.process_node_task_service import get_process_node_task_service
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
    service = get_process_node_task_service()
    claim = service.claim_category_task(
        process_id,
        registered_domain,
        _worker_name(self),
        str(self.request.id),
    )
    return _handle_category_claim(self, process_id, registered_domain, claim)


def _handle_category_claim(
    task: Task,
    process_id: str,
    registered_domain: str,
    claim: dict[str, Any],
) -> dict[str, Any]:
    status = claim["status"]
    if status == "claimed":
        return _run_category_processing(task, claim["task"])
    if status == "max_attempts_exceeded":
        return {"status": "failed", "registered_domain": registered_domain, "error": "Maximum attempts exceeded"}
    return {"status": status, "registered_domain": registered_domain}


def _run_category_processing(task: Task, node_task: dict[str, Any]) -> dict[str, Any]:
    service = get_process_node_task_service()
    try:
        result = CareerCategoryNodeProcessor().process(
            node_task,
            worker_name=str(node_task["worker_name"]),
            celery_task_id=str(node_task["celery_task_id"]),
        )
    except NoCareerCategorySessionSlotAvailable as exc:
        service.requeue_task(
            node_task["process_id"],
            node_task["registered_domain"],
            str(exc),
            decrement_attempt=True,
        )
        raise task.retry(args=[node_task["process_id"], node_task["registered_domain"]], countdown=10)
    except Exception as exc:
        service.fail_task(node_task["process_id"], node_task["registered_domain"], str(exc))
        _log_category_processing_failed(node_task, str(exc))
        return {"status": "failed", "registered_domain": node_task["registered_domain"], "error": str(exc)}
    service.complete_task(node_task["process_id"], node_task["registered_domain"], result)
    _log_category_processing_completed(node_task, result)
    return {"status": "completed", "registered_domain": node_task["registered_domain"]}


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
        raise task.retry(args=[process_id, domain_ref["registered_domain"]], countdown=10)
    except Exception as exc:
        runtime.fail_domain(process_id, domain_ref, str(exc))
        _log_domain_processing_failed(process_id, domain_ref, str(exc))
        return {"status": "failed", "registered_domain": domain_ref["registered_domain"], "error": str(exc)}
    if not result.get("success"):
        error = str(result.get("error") or result.get("status") or "Search node failed")
        runtime.fail_domain(process_id, domain_ref, error)
        _log_domain_processing_failed(process_id, domain_ref, error)
        return {"status": "failed", "registered_domain": domain_ref["registered_domain"], "error": error}
    runtime.complete_domain(process_id, domain_ref, result)
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
        process_id=node_task["process_id"],
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
        process_id=node_task["process_id"],
        registered_domain=node_task["registered_domain"],
        error=error,
    )


def _worker_name(task: Task) -> str:
    return str(getattr(task.request, "hostname", None) or "worker")
