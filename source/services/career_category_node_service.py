from __future__ import annotations

from functools import lru_cache
from typing import Any

from core.config import Settings, get_settings
from services.process_node_task_service import get_process_node_task_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("career_category_node_service")


class CareerCategoryNodeService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._node_tasks = get_process_node_task_service()

    def start_process(self, process_id: str) -> dict[str, Any]:
        process = self._load_ready_process(process_id)
        tasks = self._build_tasks(process)
        summary = self._node_tasks.upsert_category_tasks(tasks)
        queued = self._node_tasks.queued_category_tasks(process_id)
        self._dispatch_tasks(queued)
        return {
            "process_id": process_id,
            "created": summary["created"],
            "failed_without_candidates": summary["failed"],
            "enqueued": len(queued),
        }

    def _load_ready_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        totals = process.get("totals", {})
        if int(totals.get("queued") or 0) > 0 or int(totals.get("processing") or 0) > 0:
            raise RuntimeError("Search node must finish before career category node starts")
        return process

    def _build_tasks(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        return [self._task_from_ref(process, ref) for ref in self._completed_refs(process)]

    def _completed_refs(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        return list(process.get("domains", {}).get("completed", []) or [])

    def _task_from_ref(self, process: dict[str, Any], ref: dict[str, Any]) -> dict[str, Any]:
        career_urls = self._career_url_candidates(ref)
        status = "queued" if career_urls else "failed"
        task = {
            "process_id": process["process_id"],
            "node": "career_page_category",
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "client_name": process.get("client", {}).get("client_name"),
            "input": {"career_urls": career_urls},
            "status": status,
            "last_error": None if career_urls else "No career URL candidates available",
        }
        return task

    def _career_url_candidates(self, ref: dict[str, Any]) -> list[str]:
        supplied = str(ref.get("career_url") or "").strip()
        if supplied:
            return [supplied]
        result = self._shared_search_result(ref["registered_domain"])
        values = list(result.get("career_urls") or [])
        if result.get("career_url"):
            values.insert(0, result["career_url"])
        return self._dedupe_urls(values)

    def _shared_search_result(self, registered_domain: str) -> dict[str, Any]:
        task = self._domain_tasks.find_one(
            {"registered_domain": registered_domain, "status": "completed"},
            {"result": 1},
        )
        result = (task or {}).get("result") or {}
        return result if isinstance(result, dict) else {}

    def _dedupe_urls(self, values: list[Any]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            url = str(value or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            result.append(url)
        return result

    def _dispatch_tasks(self, tasks: list[dict[str, Any]]) -> None:
        for task in tasks:
            self._dispatch_task(task)

    def _dispatch_task(self, task: dict[str, Any]) -> None:
        from infrastructure.tasks import run_career_category_node

        log_event(
            logger,
            "info",
            "career_category_domain_dispatched",
            domain="career_category",
            process_id=task["process_id"],
            registered_domain=task["registered_domain"],
        )
        run_career_category_node.apply_async(
            args=[task["process_id"], task["registered_domain"]],
            queue="processes",
        )


@lru_cache(maxsize=1)
def get_career_category_node_service() -> CareerCategoryNodeService:
    return CareerCategoryNodeService(get_sync_mongodb_service(), get_settings())
