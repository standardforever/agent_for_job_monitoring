from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

from pymongo import ASCENDING, ReturnDocument, UpdateOne

from core.config import Settings, get_settings
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("selenium_session_slot_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class SeleniumSessionSlotService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._nodes = mongodb.collection(settings.mongodb_selenium_nodes_collection)
        self._slots = mongodb.collection(settings.mongodb_selenium_session_slots_collection)
        self._indexes_ready = False

    def ensure_capacity(self) -> None:
        self._ensure_indexes()
        nodes = self._configured_nodes()
        self._upsert_nodes(nodes)
        self._upsert_session_slots(nodes)

    def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._nodes.create_index([("node_id", ASCENDING)], unique=True)
        self._slots.create_index([("slot_id", ASCENDING)], unique=True)
        self._slots.create_index([("status", ASCENDING), ("lease_expires_at", ASCENDING)])
        self._slots.create_index([("selenium_node_id", ASCENDING), ("session_index", ASCENDING)], unique=True)
        self._indexes_ready = True

    def _configured_nodes(self) -> list[dict[str, Any]]:
        return [self._node_config(url) for url in self._configured_urls()]

    def _configured_urls(self) -> list[str]:
        raw_urls = self._settings.selenium_grid_urls or self._settings.selenium_remote_url
        urls = [url.strip() for url in raw_urls.split(",") if url.strip()]
        return urls or [self._settings.selenium_remote_url]

    def _node_config(self, grid_url: str) -> dict[str, Any]:
        return {
            "node_id": self._node_id(grid_url),
            "grid_url": grid_url,
            "max_sessions": self._settings.max_sessions_per_selenium,
        }

    def _node_id(self, grid_url: str) -> str:
        parsed = urlparse(grid_url if "://" in grid_url else f"http://{grid_url}")
        host = parsed.netloc or parsed.path
        return host.replace(":", "_").replace("/", "_")

    def _upsert_nodes(self, nodes: list[dict[str, Any]]) -> None:
        for node in nodes:
            self._nodes.update_one(
                {"node_id": node["node_id"]},
                {
                    "$set": {
                        "grid_url": node["grid_url"],
                        "max_sessions": node["max_sessions"],
                        "status": "online",
                        "last_heartbeat_at": _now(),
                        "updated_at": _now(),
                    },
                    "$setOnInsert": {"created_at": _now()},
                },
                upsert=True,
            )

    def _upsert_session_slots(self, nodes: list[dict[str, Any]]) -> None:
        operations = []
        for node in nodes:
            operations.extend(self._session_slot_operations(node))
        if operations:
            self._slots.bulk_write(operations, ordered=False)

    def _session_slot_operations(self, node: dict[str, Any]) -> list[UpdateOne]:
        return [self._session_slot_operation(node, index) for index in range(node["max_sessions"])]

    def _session_slot_operation(self, node: dict[str, Any], session_index: int) -> UpdateOne:
        slot_id = self._slot_id(node["node_id"], session_index)
        return UpdateOne(
            {"slot_id": slot_id},
            {
                "$set": {
                    "selenium_node_id": node["node_id"],
                    "grid_url": node["grid_url"],
                    "session_index": session_index,
                    "updated_at": _now(),
                },
                "$setOnInsert": {
                    "slot_id": slot_id,
                    "status": "available",
                    "created_at": _now(),
                },
            },
            upsert=True,
        )

    def _slot_id(self, node_id: str, session_index: int) -> str:
        return f"{node_id}:session:{session_index}"

    def claim_slot(
        self,
        worker_name: str,
        task_id: str,
        *,
        process_id: str,
        registered_domain: str,
    ) -> dict[str, Any] | None:
        self.ensure_capacity()
        self.repair_stale_slots()
        return self._claim_available_slot(worker_name, task_id, process_id, registered_domain)

    def _claim_available_slot(
        self,
        worker_name: str,
        task_id: str,
        process_id: str,
        registered_domain: str,
    ) -> dict[str, Any] | None:
        for node_id in self._node_ids_by_load():
            slot = self._claim_available_slot_on_node(worker_name, task_id, process_id, registered_domain, node_id)
            if slot:
                self._log_slot_claimed(slot, registered_domain)
                return slot
        return None

    def _node_ids_by_load(self) -> list[str]:
        rows = self._slots.aggregate(
            [
                {
                    "$group": {
                        "_id": "$selenium_node_id",
                        "busy": {"$sum": {"$cond": [{"$eq": ["$status", "busy"]}, 1, 0]}},
                        "available": {"$sum": {"$cond": [{"$eq": ["$status", "available"]}, 1, 0]}},
                    }
                },
                {"$match": {"available": {"$gt": 0}}},
                {"$sort": {"busy": 1, "_id": 1}},
            ],
        )
        return [str(row["_id"]) for row in rows]

    def _claim_available_slot_on_node(
        self,
        worker_name: str,
        task_id: str,
        process_id: str,
        registered_domain: str,
        node_id: str,
    ) -> dict[str, Any] | None:
        timestamp = _now()
        return self._slots.find_one_and_update(
            {"status": "available", "selenium_node_id": node_id},
            {
                "$set": {
                    "status": "busy",
                    "claimed_by_worker": worker_name,
                    "celery_task_id": task_id,
                    "current_process_id": process_id,
                    "current_domain": registered_domain,
                    "claimed_at": timestamp,
                    "last_claimed_at": timestamp,
                    "heartbeat_at": timestamp,
                    "lease_expires_at": timestamp + timedelta(seconds=self._settings.stale_task_seconds),
                    "updated_at": timestamp,
                }
            },
            sort=[("selenium_node_id", ASCENDING), ("session_index", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    def _log_slot_claimed(self, slot: dict[str, Any], registered_domain: str) -> None:
        log_event(
            logger,
            "info",
            "selenium_session_slot_claimed",
            domain="selenium",
            slot_id=slot["slot_id"],
            selenium_node_id=slot["selenium_node_id"],
            registered_domain=registered_domain,
        )

    def heartbeat_slot(self, slot_id: str) -> None:
        timestamp = _now()
        self._slots.update_one(
            {"slot_id": slot_id, "status": "busy"},
            {
                "$set": {
                    "heartbeat_at": timestamp,
                    "lease_expires_at": timestamp + timedelta(seconds=self._settings.stale_task_seconds),
                    "updated_at": timestamp,
                }
            },
        )

    def release_slot(self, slot_id: str) -> None:
        self._slots.update_one(
            {"slot_id": slot_id, "status": "busy"},
            {
                "$set": {"status": "available", "last_released_at": _now(), "updated_at": _now()},
                "$unset": self._lease_fields(),
            },
        )

    def mark_slot_stale(self, slot_id: str, error: str) -> None:
        self._slots.update_one(
            {"slot_id": slot_id},
            {
                "$set": {"status": "stale", "last_error": error, "updated_at": _now()},
                "$unset": self._lease_fields(),
            },
        )

    def repair_stale_slots(self) -> int:
        expired = self._repair_expired_busy_slots()
        stale = self._repair_stale_marked_slots()
        return expired + stale

    def _repair_expired_busy_slots(self) -> int:
        result = self._slots.update_many(
            {"status": "busy", "lease_expires_at": {"$lt": _now()}},
            {
                "$set": {"status": "available", "updated_at": _now()},
                "$unset": self._lease_fields(),
            },
        )
        return int(result.modified_count)

    def _repair_stale_marked_slots(self) -> int:
        result = self._slots.update_many(
            {"status": "stale"},
            {
                "$set": {"status": "available", "updated_at": _now()},
                "$unset": self._lease_fields(),
            },
        )
        return int(result.modified_count)

    def _lease_fields(self) -> dict[str, str]:
        return {
            "claimed_by_worker": "",
            "celery_task_id": "",
            "claimed_at": "",
            "heartbeat_at": "",
            "lease_expires_at": "",
            "current_process_id": "",
            "current_domain": "",
        }


@lru_cache(maxsize=1)
def get_selenium_session_slot_service() -> SeleniumSessionSlotService:
    return SeleniumSessionSlotService(get_sync_mongodb_service(), get_settings())
