from __future__ import annotations

from functools import lru_cache
from typing import Any

from core.config import Settings, get_settings
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service


class ProcessControlService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)

    def domain_node_enabled(self, process_id: str, registered_domain: str, node: str) -> bool:
        process = self._processes.find_one(
            {"process_id": process_id},
            {"process_domains": 1},
        )
        if not process:
            return False
        process_domains = process.get("process_domains") or []
        if not process_domains:
            return True
        domain = self._find_domain(process_domains, registered_domain)
        if not domain:
            return False
        if domain.get("enabled") is False:
            return False
        control = (domain.get("node_controls") or {}).get(node) or {}
        if control.get("enabled") is False or control.get("stopped") is True:
            return False
        return True

    def filter_refs(self, process_id: str, refs: list[dict[str, Any]], node: str) -> list[dict[str, Any]]:
        return [
            ref
            for ref in refs
            if self.domain_node_enabled(process_id, str(ref.get("registered_domain") or ""), node)
        ]

    def _find_domain(self, process_domains: list[dict[str, Any]], registered_domain: str) -> dict[str, Any] | None:
        for item in process_domains:
            if item.get("registered_domain") == registered_domain:
                return item
        return None


@lru_cache(maxsize=1)
def get_process_control_service() -> ProcessControlService:
    return ProcessControlService(get_sync_mongodb_service(), get_settings())
