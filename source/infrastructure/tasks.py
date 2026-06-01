from __future__ import annotations

from typing import Any

from celery import Task

from core.config import get_settings
from infrastructure.celery_app import celery_app
from services.process_runtime_service import get_process_runtime_service
from services.selenium_mock_processor import NoSeleniumSessionSlotAvailable, SeleniumMockProcessor
from utils.logging import get_logger, log_event


settings = get_settings()
logger = get_logger("celery_domain_tasks")


@celery_app.task(bind=True, max_retries=None, name="infrastructure.tasks.process_domain")
def process_domain(self: Task, process_id: str, registered_domain: str) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "domain_task_received",
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


def _handle_claim(
    task: Task,
    process_id: str,
    registered_domain: str,
    claim: dict[str, Any],
) -> dict[str, Any]:
    status = claim["status"]
    if status == "claimed":
        return _run_mock_processing(task, process_id, claim["domain_ref"])
    if status == "fresh_completed":
        return _complete_with_reuse(process_id, registered_domain, claim.get("result"))
    if status == "max_attempts_exceeded":
        return _fail_queued_domain(process_id, registered_domain, "Maximum attempts exceeded")
    if status in {"busy", "process_at_capacity"}:
        raise task.retry(countdown=10)
    return {"status": status, "registered_domain": registered_domain}


def _run_mock_processing(task: Task, process_id: str, domain_ref: dict[str, Any]) -> dict[str, Any]:
    runtime = get_process_runtime_service()
    try:
        _log_domain_processing_started(process_id, domain_ref)
        result = SeleniumMockProcessor().process(
            process_id,
            domain_ref,
            worker_name=str(domain_ref["worker_name"]),
            task_id=str(domain_ref["celery_task_id"]),
        )
    except NoSeleniumSessionSlotAvailable as exc:
        runtime.requeue_domain(process_id, domain_ref, str(exc), decrement_attempt=True)
        raise task.retry(args=[process_id, domain_ref["registered_domain"]], countdown=10)
    except Exception as exc:
        runtime.fail_domain(process_id, domain_ref, str(exc))
        _log_domain_processing_failed(process_id, domain_ref, str(exc))
        return {"status": "failed", "registered_domain": domain_ref["registered_domain"], "error": str(exc)}
    runtime.complete_domain(process_id, domain_ref, result)
    _log_domain_processing_completed(process_id, domain_ref)
    return {"status": "completed", "registered_domain": domain_ref["registered_domain"]}


def _complete_with_reuse(
    process_id: str,
    registered_domain: str,
    result: dict[str, Any] | None,
) -> dict[str, Any]:
    runtime = get_process_runtime_service()
    runtime.complete_with_reused_result(process_id, registered_domain, result)
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


def _worker_name(task: Task) -> str:
    return str(getattr(task.request, "hostname", None) or "worker")
