from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING, UpdateOne

from core.config import Settings, get_settings
from services.admin_client_service import get_admin_client_service
from services.file_input_service import UploadDomainInput
from services.mongodb_service import MongoDBService, get_mongodb_service
from utils.tld import registered_domain
from utils.logging import get_logger, log_event


logger = get_logger("process_upload_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ProcessUploadService:
    def __init__(self, mongodb: MongoDBService, settings: Settings) -> None:
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._node_tasks = mongodb.collection(settings.mongodb_process_node_tasks_collection)
        self._indexes_ready = False

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        await self._drop_legacy_domain_task_indexes()
        await self._create_process_indexes()
        await self._create_domain_task_indexes()
        self._indexes_ready = True

    async def _drop_legacy_domain_task_indexes(self) -> None:
        indexes = await self._domain_tasks.index_information()
        for name, details in indexes.items():
            if self._should_drop_domain_task_index(name, details):
                await self._domain_tasks.drop_index(name)

    def _should_drop_domain_task_index(self, name: str, details: dict[str, Any]) -> bool:
        if name == "_id_":
            return False
        if self._is_current_domain_task_index(name):
            return False
        return self._has_legacy_process_index(details) or self._is_legacy_domain_task_index(name)

    def _is_current_domain_task_index(self, name: str) -> bool:
        return name in {"registered_domain_1", "status_1_updated_at_1", "last_completed_at_-1"}

    def _is_legacy_domain_task_index(self, name: str) -> bool:
        return name in {"status_1_stage_1_created_at_1", "registered_domain_1_created_at_-1"}

    def _has_legacy_process_index(self, details: dict[str, Any]) -> bool:
        keys = [key for key, _direction in details.get("key", [])]
        return "process_id" in keys

    async def _create_process_indexes(self) -> None:
        await self._processes.create_index([("process_id", ASCENDING)], unique=True)
        await self._processes.create_index([("client.client_name", ASCENDING), ("created_at", DESCENDING)])
        await self._processes.create_index([("status", ASCENDING), ("created_at", DESCENDING)])

    async def _create_domain_task_indexes(self) -> None:
        await self._domain_tasks.create_index([("registered_domain", ASCENDING)], unique=True)
        await self._domain_tasks.create_index([("status", ASCENDING), ("updated_at", ASCENDING)])
        await self._domain_tasks.create_index([("last_completed_at", DESCENDING)])

    async def create_process(
        self,
        *,
        client_name: str,
        agent_count: int,
        filename: str,
        domain_inputs: list[UploadDomainInput],
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        client = await self._load_client(client_name)
        domain_refs = self._build_domain_refs(domain_inputs)
        process = self._build_process(client, agent_count, filename, domain_refs)
        await self._save_process(process)
        await self._upsert_domain_tasks(domain_refs)
        return self._serialize_process(process)

    async def _load_client(self, client_name: str) -> dict[str, Any]:
        return await get_admin_client_service().get_client_snapshot(client_name)

    def _build_domain_refs(self, domain_inputs: list[UploadDomainInput]) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in domain_inputs:
            ref = self._domain_ref(item)
            if not ref["registered_domain"] or ref["registered_domain"] in seen:
                continue
            refs.append(ref)
            seen.add(ref["registered_domain"])
        if not refs:
            raise ValueError("No valid domain values were found")
        return refs

    def _domain_ref(self, item: UploadDomainInput) -> dict[str, Any]:
        return {
            "domain": item.domain,
            "registered_domain": registered_domain(item.domain),
            "career_url": item.career_url,
        }

    def _build_process(
        self,
        client: dict[str, Any],
        agent_count: int,
        filename: str,
        domain_refs: list[dict[str, Any]],
    ) -> dict[str, Any]:
        timestamp = _now()
        return {
            "process_id": uuid4().hex,
            "client": client,
            "agent_count": agent_count,
            "status": "queued",
            "source_file": self._source_file(filename, domain_refs),
            "totals": self._totals(domain_refs),
            "domains": self._initial_domain_state(domain_refs),
            "created_at": timestamp,
            "updated_at": timestamp,
        }

    def _source_file(self, filename: str, domain_refs: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "filename": filename,
            "accepted_domains": len(domain_refs),
        }

    def _totals(self, domain_refs: list[dict[str, Any]]) -> dict[str, int]:
        return {
            "domains": len(domain_refs),
            "queued": len(domain_refs),
            "processing": 0,
            "completed": 0,
            "failed": 0,
            "supplied_career_urls": self._supplied_career_url_count(domain_refs),
        }

    def _supplied_career_url_count(self, domain_refs: list[dict[str, Any]]) -> int:
        return sum(1 for item in domain_refs if item.get("career_url"))

    def _initial_domain_state(self, domain_refs: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        return {
            "queued": domain_refs,
            "processing": [],
            "completed": [],
            "failed": [],
        }

    async def _save_process(self, process: dict[str, Any]) -> None:
        await self._processes.insert_one(process)

    async def _upsert_domain_tasks(self, domain_refs: list[dict[str, Any]]) -> None:
        operations = [self._domain_task_upsert(ref) for ref in domain_refs]
        if operations:
            await self._domain_tasks.bulk_write(operations, ordered=False)

    def _domain_task_upsert(self, ref: dict[str, Any]) -> UpdateOne:
        timestamp = _now()
        return UpdateOne(
            {"registered_domain": ref["registered_domain"]},
            {
                "$setOnInsert": self._new_domain_task(ref, timestamp),
                "$set": self._domain_task_updates(ref, timestamp),
            },
            upsert=True,
        )

    def _new_domain_task(self, ref: dict[str, Any], timestamp: datetime) -> dict[str, Any]:
        return {
            "status": "queued",
            "attempts": 0,
            "last_started_at": None,
            "last_completed_at": None,
            "last_error": None,
            "created_at": timestamp,
        }

    def _domain_task_updates(self, ref: dict[str, Any], timestamp: datetime) -> dict[str, Any]:
        return {
            "domain": ref["domain"],
            "updated_at": timestamp,
        }

    def _serialize_process(self, process: dict[str, Any]) -> dict[str, Any]:
        return {
            "process_id": process["process_id"],
            "client": process["client"],
            "agent_count": process["agent_count"],
            "status": process["status"],
            "source_file": process["source_file"],
            "totals": process["totals"],
            "domains": process.get("domains", self._empty_domain_state()),
            "created_at": process["created_at"],
            "updated_at": process["updated_at"],
        }

    def _empty_domain_state(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "queued": [],
            "processing": [],
            "completed": [],
            "failed": [],
        }

    async def list_processes(self, limit: int = 25) -> dict[str, Any]:
        await self._ensure_indexes()
        cursor = self._processes.find({}).sort("created_at", DESCENDING).limit(limit)
        processes = [self._serialize_process(document) async for document in cursor]
        return {"processes": processes, "count": len(processes)}

    async def get_process(self, process_id: str) -> dict[str, Any]:
        await self._ensure_indexes()
        process = await self._load_process(process_id)
        domain_tasks = await self._load_related_domain_tasks(process)
        node_tasks = await self._load_related_node_tasks(process_id)
        return {"process": self._serialize_process(process), "domain_tasks": domain_tasks, "node_tasks": node_tasks}

    async def _load_process(self, process_id: str) -> dict[str, Any]:
        process = await self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        return process

    async def _load_related_domain_tasks(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        registered_domains = self._process_registered_domains(process)
        if not registered_domains:
            return []
        cursor = self._domain_tasks.find({"registered_domain": {"$in": registered_domains}})
        tasks = [self._serialize_domain_task(document) async for document in cursor]
        return sorted(tasks, key=lambda item: item["registered_domain"])

    def _process_registered_domains(self, process: dict[str, Any]) -> list[str]:
        domains = process.get("domains", {})
        refs = []
        refs.extend(domains.get("queued", []))
        refs.extend(domains.get("processing", []))
        refs.extend(domains.get("completed", []))
        refs.extend(domains.get("failed", []))
        return [ref["registered_domain"] for ref in refs if ref.get("registered_domain")]

    def _serialize_domain_task(self, document: dict[str, Any]) -> dict[str, Any]:
        return {
            "domain": document.get("domain"),
            "registered_domain": document.get("registered_domain"),
            "career_url": document.get("career_url"),
            "status": document.get("status"),
            "attempts": document.get("attempts", 0),
            "last_started_at": document.get("last_started_at"),
            "last_completed_at": document.get("last_completed_at"),
            "last_error": document.get("last_error"),
        }

    async def _load_related_node_tasks(self, process_id: str) -> list[dict[str, Any]]:
        cursor = self._node_tasks.find({"process_id": process_id})
        tasks = [self._serialize_node_task(document) async for document in cursor]
        return sorted(tasks, key=lambda item: (item["node"], item["registered_domain"]))

    def _serialize_node_task(self, document: dict[str, Any]) -> dict[str, Any]:
        result = document.get("result") or {}
        overview = result.get("overview") if isinstance(result, dict) else {}
        return {
            "node": document.get("node"),
            "domain": document.get("domain"),
            "registered_domain": document.get("registered_domain"),
            "status": document.get("status"),
            "input": document.get("input"),
            "attempts": document.get("attempts", 0),
            "outcome": (overview or {}).get("outcome"),
            "jobs_found": bool((overview or {}).get("jobs_found")),
            "total_jobs_found": int((overview or {}).get("total_jobs_found") or 0),
            "last_started_at": document.get("last_started_at"),
            "last_completed_at": document.get("last_completed_at"),
            "last_error": document.get("last_error"),
        }


@lru_cache(maxsize=1)
def get_process_upload_service() -> ProcessUploadService:
    return ProcessUploadService(get_mongodb_service(), get_settings())
