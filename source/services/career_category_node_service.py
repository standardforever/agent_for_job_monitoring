from __future__ import annotations

from functools import lru_cache
from typing import Any

from core.config import Settings, get_settings
from services.career_process_service import get_career_process_service
from services.node_preflight_service import get_node_preflight_service
from services.process_control_service import get_process_control_service
from services.search_run_service import get_search_run_service
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("career_category_node_service")


class CareerCategoryNodeService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._career_process = get_career_process_service()

    def start_process(self, process_id: str) -> dict[str, Any]:
        get_node_preflight_service().require_client_openai_config(process_id)
        process = self._load_ready_process(process_id)
        mode = self._start_mode(process)
        tasks = self._build_tasks(process)
        summary = self._career_process.create_category_tasks(tasks)
        self._career_process.start_process_run(process_id, summary, mode=mode)
        self._dispatch_tasks(process_id, summary["created_tasks"])
        return {
            "process_id": process_id,
            "mode": mode,
            "created": summary["created"],
            "failed_without_candidates": summary["failed"],
            "blocked": summary["blocked"],
            "enqueued": len(summary["created_tasks"]),
        }

    def _load_ready_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        totals = process.get("totals", {})
        if int(totals.get("queued") or 0) > 0 or int(totals.get("processing") or 0) > 0:
            raise RuntimeError("Search node must finish before career category node starts")
        if self._career_process.active_process_run(process):
            raise RuntimeError("Career category node is already running for this process")
        return process

    def _start_mode(self, process: dict[str, Any]) -> str:
        status = str(process.get("career_status") or "not_started")
        totals = process.get("career_totals") or {}
        if status in {"completed", "partial_completed", "failed", "blocked"}:
            return "rerun"
        if any(int(totals.get(key) or 0) for key in ("completed", "failed", "blocked")):
            return "rerun"
        return "start"

    def _build_tasks(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        return [self._task_from_ref(process, ref) for ref in self._completed_refs(process)]

    def _completed_refs(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        refs = list(process.get("domains", {}).get("completed", []) or [])
        return get_process_control_service().filter_refs(process["process_id"], refs, "career_category")

    def _task_from_ref(self, process: dict[str, Any], ref: dict[str, Any]) -> dict[str, Any]:
        career_urls = self._career_url_candidates(ref)
        status = "queued" if career_urls else "failed"
        task = {
            "node": "career_page_category",
            "process_id": process["process_id"],
            "registered_domain": ref["registered_domain"],
            "domain": ref.get("domain"),
            "input": {"career_urls": career_urls},
            "status": status,
            "last_error": None if career_urls else "No career URL candidates available",
        }
        return task

    def _career_url_candidates(self, ref: dict[str, Any]) -> list[str]:
        supplied = str(ref.get("supplied_career_url") or "").strip()
        if supplied:
            return [supplied]
        result = self._shared_search_result(ref["registered_domain"])
        values = list(ref.get("career_urls") or [])
        values.extend(list(result.get("career_urls") or []))
        preferred = result.get("career_url") or ref.get("career_url")
        if preferred:
            values.insert(0, preferred)
        return self._filter_ignored_candidates(ref["registered_domain"], self._dedupe_urls(values))

    def _shared_search_result(self, registered_domain: str) -> dict[str, Any]:
        result = get_search_run_service().completed_result(registered_domain)
        if result:
            return result
        task = self._domain_tasks.find_one(
            {"registered_domain": registered_domain, "status": "completed"},
            {"result": 1},
        )
        result = (task or {}).get("result") or {}
        return result if isinstance(result, dict) else {}

    def _filter_ignored_candidates(self, registered_domain: str, values: list[str]) -> list[str]:
        ignored = self._ignored_candidate_urls(registered_domain)
        return [url for url in values if self._normalize_url(url) not in ignored]

    def _ignored_candidate_urls(self, registered_domain: str) -> set[str]:
        task = self._domain_tasks.find_one(
            {"registered_domain": registered_domain},
            {"career_candidate_ignore_urls.url": 1},
        )
        ignored = (task or {}).get("career_candidate_ignore_urls") or []
        return {self._normalize_url(item.get("url")) for item in ignored if isinstance(item, dict)}

    def _normalize_url(self, value: Any) -> str:
        return str(value or "").strip().rstrip("/")

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

    def _dispatch_tasks(self, process_id: str, tasks: list[dict[str, Any]]) -> None:
        for task in tasks:
            self._dispatch_task(process_id, task)

    def _dispatch_task(self, process_id: str, task: dict[str, Any]) -> None:
        from infrastructure.tasks import run_career_category_node

        if not self._career_process.mark_task_dispatched(task["registered_domain"]):
            log_event(
                logger,
                "info",
                "career_category_dispatch_skipped",
                domain="career_category",
                process_id=process_id,
                registered_domain=task["registered_domain"],
                reason="recently_dispatched",
            )
            return
        log_event(
            logger,
            "info",
            "career_category_domain_dispatched",
            domain="career_category",
            process_id=process_id,
            registered_domain=task["registered_domain"],
        )
        run_career_category_node.apply_async(
            args=[process_id, task["registered_domain"]],
            queue="processes",
        )


@lru_cache(maxsize=1)
def get_career_category_node_service() -> CareerCategoryNodeService:
    return CareerCategoryNodeService(get_sync_mongodb_service(), get_settings())
