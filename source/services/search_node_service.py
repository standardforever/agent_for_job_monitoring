from __future__ import annotations

from functools import lru_cache
from typing import Any

from services.process_runtime_service import get_process_runtime_service
from utils.logging import get_logger, log_event


logger = get_logger("search_node_service")


class SearchNodeService:
    def start_process(self, process_id: str) -> dict[str, Any]:
        prepared = get_process_runtime_service().start_or_restart_search_process(process_id)
        refs = prepared["refs"]
        self._dispatch_refs(process_id, refs)
        return {"process_id": process_id, "mode": prepared["mode"], "enqueued": len(refs)}

    def _dispatch_refs(self, process_id: str, refs: list[dict[str, Any]]) -> None:
        for ref in refs:
            self._dispatch_ref(process_id, ref)

    def _dispatch_ref(self, process_id: str, ref: dict[str, Any]) -> None:
        from infrastructure.tasks import run_search_node

        registered_domain = ref["registered_domain"]
        if not get_process_runtime_service().mark_domain_dispatched(process_id, registered_domain):
            log_event(
                logger,
                "info",
                "search_node_domain_dispatch_skipped",
                domain="search_node",
                process_id=process_id,
                registered_domain=registered_domain,
                reason="recently_dispatched",
            )
            return
        log_event(
            logger,
            "info",
            "search_node_domain_dispatched",
            domain="search_node",
            process_id=process_id,
            registered_domain=registered_domain,
        )
        run_search_node.apply_async(args=[process_id, registered_domain], queue="processes")


@lru_cache(maxsize=1)
def get_search_node_service() -> SearchNodeService:
    return SearchNodeService()
