from __future__ import annotations

from functools import lru_cache
from typing import Any

from core.config import Settings, get_settings
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service


class ProcessControlService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._process_refs = mongodb.collection(settings.mongodb_process_domain_refs_collection)

    def domain_node_enabled(self, process_id: str, registered_domain: str, node: str) -> bool:
        domain = self._process_refs.find_one(
            {"process_id": process_id, "registered_domain": registered_domain},
            {"enabled": 1, "node_controls": 1},
        )
        if domain is None:
            process = self._processes.find_one({"process_id": process_id}, {"process_id": 1})
            if not process:
                return False
            return True
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


@lru_cache(maxsize=1)
def get_process_control_service() -> ProcessControlService:
    return ProcessControlService(get_sync_mongodb_service(), get_settings())
