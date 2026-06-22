from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING

from core.config import Settings, get_settings
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service


PROCESS_DOMAIN_REF_SCHEMA_VERSION = 1


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ProcessDomainRefService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._refs = mongodb.collection(settings.mongodb_process_domain_refs_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._refs.create_index([("process_id", ASCENDING), ("registered_domain", ASCENDING)], unique=True)
        self._refs.create_index([("process_id", ASCENDING), ("status", ASCENDING), ("updated_at", ASCENDING)])
        self._refs.create_index([("registered_domain", ASCENDING), ("process_id", ASCENDING)])
        self._refs.create_index([("process_id", ASCENDING), ("enabled", ASCENDING)])
        self._indexes_ready = True

    def refs_for_process(self, process_id: str, *, statuses: list[str] | None = None) -> list[dict[str, Any]]:
        self.ensure_indexes()
        query: dict[str, Any] = {"process_id": process_id}
        if statuses:
            query["status"] = {"$in": statuses}
        return list(self._refs.find(query).sort("created_at", ASCENDING))

    def registered_domains(self, process_id: str, *, statuses: list[str] | None = None) -> list[str]:
        return [
            str(ref.get("registered_domain") or "")
            for ref in self.refs_for_process(process_id, statuses=statuses)
            if ref.get("registered_domain")
        ]

    def find_ref(self, process_id: str, registered_domain: str, *, status: str | None = None) -> dict[str, Any] | None:
        self.ensure_indexes()
        query = {"process_id": process_id, "registered_domain": registered_domain}
        if status:
            query["status"] = status
        return self._refs.find_one(query)

    def counts(self, process_id: str) -> dict[str, int]:
        self.ensure_indexes()
        totals = {"domains": 0, "queued": 0, "processing": 0, "completed": 0, "failed": 0, "supplied_career_urls": 0}
        for row in self._refs.aggregate(
            [
                {"$match": {"process_id": process_id}},
                {
                    "$group": {
                        "_id": "$status",
                        "count": {"$sum": 1},
                        "supplied": {"$sum": {"$cond": [{"$ne": [{"$ifNull": ["$career_url", ""]}, ""]}, 1, 0]}},
                    }
                },
            ]
        ):
            status = str(row.get("_id") or "queued")
            count = int(row.get("count") or 0)
            totals["domains"] += count
            totals["supplied_career_urls"] += int(row.get("supplied") or 0)
            if status in totals:
                totals[status] += count
        return totals

    def refresh_process_totals(self, process_id: str, processes_collection: Any) -> dict[str, int]:
        totals = self.counts(process_id)
        processes_collection.update_one({"process_id": process_id}, {"$set": {"totals": totals, "updated_at": _now()}})
        return totals

    def minimal_domain_state(self) -> dict[str, list[dict[str, Any]]]:
        return {"queued": [], "processing": [], "completed": [], "failed": []}


@lru_cache(maxsize=1)
def get_process_domain_ref_service() -> ProcessDomainRefService:
    return ProcessDomainRefService(get_sync_mongodb_service(), get_settings())
