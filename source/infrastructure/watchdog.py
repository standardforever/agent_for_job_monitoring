from __future__ import annotations

import time
from typing import Any

from core.config import get_settings
from infrastructure.tasks import run_career_category_node, run_job_extraction_node, run_job_pattern_node, run_search_node
from services.career_process_service import get_career_process_service
from services.job_extraction_node_service import get_job_extraction_node_service
from services.job_pattern_node_service import get_job_pattern_node_service
from services.process_runtime_service import get_process_runtime_service
from services.selenium_session_slot_service import get_selenium_session_slot_service
from services.sync_mongodb_service import get_sync_mongodb_service
from utils.logging import get_logger, log_event


settings = get_settings()
logger = get_logger("watchdog")


def main() -> None:
    while True:
        run_once()
        time.sleep(settings.watchdog_interval_seconds)


def run_once() -> None:
    get_selenium_session_slot_service().ensure_capacity()
    get_selenium_session_slot_service().repair_stale_slots()
    _repair_stale_processes()
    _repair_stale_career_processes()
    _repair_stale_job_pattern_processes()
    _repair_stale_job_extraction_processes()
    _enqueue_waiting_domains()
    _enqueue_waiting_category_tasks()
    _enqueue_waiting_job_pattern_tasks()
    _enqueue_waiting_job_extraction_tasks()


def _repair_stale_processes() -> None:
    for process in _processes_with_processing_domains():
        get_process_runtime_service().requeue_stale_processing(process["process_id"])


def _repair_stale_career_processes() -> None:
    get_career_process_service().requeue_stale_category_tasks()


def _repair_stale_job_extraction_processes() -> None:
    get_job_extraction_node_service().requeue_stale_tasks()


def _repair_stale_job_pattern_processes() -> None:
    get_job_pattern_node_service().requeue_stale_tasks()


def _processes_with_processing_domains() -> list[dict[str, Any]]:
    collection = _process_collection()
    return list(collection.find({"totals.processing": {"$gt": 0}}, {"process_id": 1}))


def _enqueue_waiting_domains() -> None:
    for process in _processes_with_queued_domains():
        _enqueue_process_domains(process)


def _processes_with_queued_domains() -> list[dict[str, Any]]:
    collection = _process_collection()
    return list(collection.find({"status": "running", "totals.queued": {"$gt": 0}}))


def _enqueue_process_domains(process: dict[str, Any]) -> None:
    refs = get_process_runtime_service().dispatchable_refs(process["process_id"])
    for ref in refs:
        log_event(
            logger,
            "info",
            "watchdog_domain_task_dispatched",
            domain="watchdog",
            process_id=process["process_id"],
            registered_domain=ref["registered_domain"],
        )
        run_search_node.apply_async(args=[process["process_id"], ref["registered_domain"]], queue="processes")


def _enqueue_waiting_category_tasks() -> None:
    for task in get_career_process_service().queued_category_tasks_for_watchdog():
        _enqueue_category_task(task)


def _enqueue_category_task(task: dict[str, Any]) -> None:
    registered_domain = task["registered_domain"]
    process_id = _process_id_for_category_domain(registered_domain)
    if not process_id:
        return
    log_event(
        logger,
        "info",
        "watchdog_category_task_dispatched",
        domain="watchdog",
        process_id=process_id,
        registered_domain=registered_domain,
    )
    run_career_category_node.apply_async(
        args=[process_id, registered_domain],
        queue="processes",
    )


def _process_id_for_category_domain(registered_domain: str) -> str | None:
    process = _process_collection().find_one(
        {"domains.completed.registered_domain": registered_domain},
        {"process_id": 1},
    )
    return str((process or {}).get("process_id") or "") or None


def _enqueue_waiting_job_extraction_tasks() -> None:
    for task in get_job_extraction_node_service().queued_tasks_for_watchdog():
        _enqueue_job_extraction_task(task)


def _enqueue_job_extraction_task(task: dict[str, Any]) -> None:
    registered_domain = task["registered_domain"]
    process_id = _process_id_for_job_extraction_domain(registered_domain)
    if not process_id:
        return
    log_event(
        logger,
        "info",
        "watchdog_job_extraction_task_dispatched",
        domain="watchdog",
        process_id=process_id,
        registered_domain=registered_domain,
    )
    run_job_extraction_node.apply_async(args=[process_id, registered_domain], queue="processes")


def _enqueue_waiting_job_pattern_tasks() -> None:
    for task in get_job_pattern_node_service().queued_tasks_for_watchdog():
        _enqueue_job_pattern_task(task)


def _enqueue_job_pattern_task(task: dict[str, Any]) -> None:
    registered_domain = task["registered_domain"]
    process_id = _process_id_for_job_pattern_domain(registered_domain)
    if not process_id:
        return
    log_event(
        logger,
        "info",
        "watchdog_job_pattern_task_dispatched",
        domain="watchdog",
        process_id=process_id,
        registered_domain=registered_domain,
    )
    run_job_pattern_node.apply_async(args=[process_id, registered_domain], queue="processes")


def _process_id_for_job_pattern_domain(registered_domain: str) -> str | None:
    process = _process_collection().find_one(
        {
            "job_pattern_status": "running",
            "domains.completed.registered_domain": registered_domain,
        },
        {"process_id": 1},
    )
    return str((process or {}).get("process_id") or "") or None


def _process_id_for_job_extraction_domain(registered_domain: str) -> str | None:
    process = _process_collection().find_one(
        {
            "job_extraction_status": "running",
            "domains.completed.registered_domain": registered_domain,
        },
        {"process_id": 1},
    )
    return str((process or {}).get("process_id") or "") or None


def _process_collection():
    mongodb = get_sync_mongodb_service()
    return mongodb.collection(settings.mongodb_process_uploads_collection)


if __name__ == "__main__":
    main()
