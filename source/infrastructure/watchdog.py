from __future__ import annotations

import time
from typing import Any

from core.config import get_settings
from infrastructure.tasks import process_domain
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
    _enqueue_waiting_domains()


def _repair_stale_processes() -> None:
    for process in _processes_with_processing_domains():
        get_process_runtime_service().requeue_stale_processing(process["process_id"])


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
        process_domain.apply_async(args=[process["process_id"], ref["registered_domain"]], queue="processes")


def _process_collection():
    mongodb = get_sync_mongodb_service()
    return mongodb.collection(settings.mongodb_process_uploads_collection)


if __name__ == "__main__":
    main()
