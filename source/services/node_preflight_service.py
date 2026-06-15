from __future__ import annotations

from functools import lru_cache
from typing import Any

from core.config import Settings, get_settings
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service


class NodePreflightService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._clients = mongodb.collection(settings.mongodb_clients_collection)
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)

    def require_client_openai_config(self, process_id: str) -> None:
        client = self._client_for_process(process_id)
        if not str(client.get("api_key") or "").strip():
            raise RuntimeError("Client OpenAI api_key is not configured")
        if not str(client.get("model") or "").strip():
            raise RuntimeError("Client OpenAI model is not configured")

    def _client_for_process(self, process_id: str) -> dict[str, Any]:
        process = self._processes.find_one({"process_id": process_id}, {"client.client_name": 1})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        client_name = str((process.get("client") or {}).get("client_name") or "").strip()
        client = self._clients.find_one({"client_name": client_name})
        if not client:
            raise RuntimeError(f"Client '{client_name}' was not found")
        return client


@lru_cache(maxsize=1)
def get_node_preflight_service() -> NodePreflightService:
    return NodePreflightService(get_sync_mongodb_service(), get_settings())
