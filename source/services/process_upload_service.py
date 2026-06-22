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
from services.node_lifecycle import NOT_STARTED
from services.process_domain_ref_service import PROCESS_DOMAIN_REF_SCHEMA_VERSION
from utils.tld import registered_domain
from utils.logging import get_logger, log_event


logger = get_logger("process_upload_service")
PROCESS_SCHEMA_VERSION = 3
NODE_CONTROL_NAMES = ("search", "career_category", "job_pattern", "job_pagination", "job_extraction")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ProcessUploadService:
    def __init__(self, mongodb: MongoDBService, settings: Settings) -> None:
        self._processes = mongodb.collection(settings.mongodb_process_uploads_collection)
        self._process_domain_refs = mongodb.collection(settings.mongodb_process_domain_refs_collection)
        self._domain_tasks = mongodb.collection(settings.mongodb_process_domain_tasks_collection)
        self._search_runs = mongodb.collection(settings.mongodb_search_runs_collection)
        self._category_runs = mongodb.collection(settings.mongodb_career_category_runs_collection)
        self._pattern_runs = mongodb.collection(settings.mongodb_job_pattern_runs_collection)
        self._pagination_runs = mongodb.collection(settings.mongodb_job_pagination_runs_collection)
        self._extraction_runs = mongodb.collection(settings.mongodb_job_extraction_runs_collection)
        self._indexes_ready = False

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        await self._drop_legacy_domain_task_indexes()
        await self._create_process_indexes()
        await self._create_process_domain_ref_indexes()
        await self._create_domain_task_indexes()
        await self._create_search_run_indexes()
        await self._create_node_run_indexes()
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

    async def _create_process_domain_ref_indexes(self) -> None:
        await self._process_domain_refs.create_index([("process_id", ASCENDING), ("registered_domain", ASCENDING)], unique=True)
        await self._process_domain_refs.create_index([("process_id", ASCENDING), ("status", ASCENDING), ("updated_at", ASCENDING)])
        await self._process_domain_refs.create_index([("registered_domain", ASCENDING), ("process_id", ASCENDING)])
        await self._process_domain_refs.create_index([("process_id", ASCENDING), ("enabled", ASCENDING)])

    async def _create_domain_task_indexes(self) -> None:
        await self._domain_tasks.create_index([("registered_domain", ASCENDING)], unique=True)
        await self._domain_tasks.create_index([("status", ASCENDING), ("updated_at", ASCENDING)])
        await self._domain_tasks.create_index([("last_completed_at", DESCENDING)])

    async def _create_search_run_indexes(self) -> None:
        await self._search_runs.create_index([("search_run_key", ASCENDING)], unique=True)
        await self._search_runs.create_index([("registered_domain", ASCENDING), ("scope", ASCENDING)])
        await self._search_runs.create_index([("status", ASCENDING), ("updated_at", ASCENDING)])

    async def _create_node_run_indexes(self) -> None:
        await self._category_runs.create_index([("category_run_key", ASCENDING)], unique=True)
        await self._category_runs.create_index([("registered_domain", ASCENDING), ("status", ASCENDING)])
        await self._pattern_runs.create_index([("job_pattern_run_key", ASCENDING)], unique=True)
        await self._pattern_runs.create_index([("registered_domain", ASCENDING), ("status", ASCENDING)])
        await self._pagination_runs.create_index([("job_pagination_run_key", ASCENDING)], unique=True)
        await self._pagination_runs.create_index([("registered_domain", ASCENDING), ("status", ASCENDING)])
        await self._extraction_runs.create_index([("job_extraction_run_key", ASCENDING)], unique=True)
        await self._extraction_runs.create_index([("registered_domain", ASCENDING), ("status", ASCENDING)])

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
        await self._upsert_process_domain_refs(process["process_id"], domain_refs, process["created_at"])
        await self._upsert_domain_tasks(domain_refs)
        await self._upsert_search_runs(domain_refs)
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
            "schema_version": PROCESS_SCHEMA_VERSION,
            "client": client,
            "agent_count": agent_count,
            "process_config": self._process_config(agent_count),
            "status": "queued",
            "career_status": NOT_STARTED,
            "job_pattern_status": NOT_STARTED,
            "job_pagination_status": NOT_STARTED,
            "job_extraction_status": NOT_STARTED,
            "pipeline_enabled": True,
            "pipeline_status": "idle",
            "next_pipeline_run_at": timestamp,
            "source_file": self._source_file(filename, domain_refs),
            "totals": self._totals(domain_refs),
            "domains": self._empty_domain_state(),
            "created_at": timestamp,
            "updated_at": timestamp,
        }

    def _process_config(self, agent_count: int) -> dict[str, Any]:
        return {
            "agent_count": agent_count,
            "pipeline_enabled": True,
            "daily_schedule_enabled": True,
            "node_order": list(NODE_CONTROL_NAMES),
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
            "blocked": 0,
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

    def _process_domains(self, domain_refs: list[dict[str, Any]], timestamp: datetime) -> list[dict[str, Any]]:
        return [self._process_domain(ref, timestamp) for ref in domain_refs]

    def _process_domain(self, ref: dict[str, Any], timestamp: datetime) -> dict[str, Any]:
        return {
            "domain": ref["domain"],
            "registered_domain": ref["registered_domain"],
            "career_url": ref.get("career_url"),
            "enabled": True,
            "stop_reason": None,
            "stopped_at": None,
            "node_controls": self._node_controls(),
            "created_at": timestamp,
            "updated_at": timestamp,
        }

    def _node_controls(self) -> dict[str, dict[str, Any]]:
        return {name: self._node_control() for name in NODE_CONTROL_NAMES}

    def _node_control(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "stopped": False,
            "stop_reason": None,
            "stopped_at": None,
            "max_failures": 4,
        }

    async def _save_process(self, process: dict[str, Any]) -> None:
        await self._processes.insert_one(process)

    async def _upsert_process_domain_refs(self, process_id: str, domain_refs: list[dict[str, Any]], timestamp: datetime) -> None:
        operations = [self._process_domain_ref_upsert(process_id, ref, timestamp) for ref in domain_refs]
        if operations:
            await self._process_domain_refs.bulk_write(operations, ordered=False)

    def _process_domain_ref_upsert(self, process_id: str, ref: dict[str, Any], timestamp: datetime) -> UpdateOne:
        return UpdateOne(
            {"process_id": process_id, "registered_domain": ref["registered_domain"]},
            {
                "$setOnInsert": {
                    "process_id": process_id,
                    "registered_domain": ref["registered_domain"],
                    "enabled": True,
                    "stop_reason": None,
                    "stopped_at": None,
                    "node_controls": self._node_controls(),
                    "schema_version": PROCESS_DOMAIN_REF_SCHEMA_VERSION,
                    "status": "queued",
                    "created_at": timestamp,
                },
                "$set": {
                    "domain": ref["domain"],
                    "career_url": ref.get("career_url"),
                    "updated_at": timestamp,
                },
            },
            upsert=True,
        )

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

    async def _upsert_search_runs(self, domain_refs: list[dict[str, Any]]) -> None:
        operations = [self._search_run_upsert(ref) for ref in domain_refs if ref.get("registered_domain")]
        if operations:
            await self._search_runs.bulk_write(operations, ordered=False)

    def _search_run_upsert(self, ref: dict[str, Any]) -> UpdateOne:
        timestamp = _now()
        search_run_key = f"shared:{ref['registered_domain']}"
        return UpdateOne(
            {"search_run_key": search_run_key},
            {
                "$setOnInsert": {
                    "search_run_key": search_run_key,
                    "registered_domain": ref["registered_domain"],
                    "scope": "shared_domain",
                    "status": "queued",
                    "attempts": 0,
                    "run_count": 0,
                    "first_started_at": None,
                    "last_started_at": None,
                    "last_completed_at": None,
                    "last_error": None,
                    "created_at": timestamp,
                },
                "$set": {"domain": ref.get("domain"), "updated_at": timestamp},
            },
            upsert=True,
        )

    def _serialize_process(self, process: dict[str, Any]) -> dict[str, Any]:
        return {
            "process_id": process["process_id"],
            "schema_version": process.get("schema_version", 1),
            "client": process["client"],
            "agent_count": process["agent_count"],
            "process_config": process.get("process_config") or self._process_config(int(process.get("agent_count") or 1)),
            "status": process["status"],
            "career_status": process.get("career_status"),
            "career_mode": process.get("career_mode"),
            "career_totals": process.get("career_totals"),
            "career_started_at": process.get("career_started_at"),
            "career_completed_at": process.get("career_completed_at"),
            "career_last_error": process.get("career_last_error"),
            "job_pattern_status": process.get("job_pattern_status"),
            "job_pattern_mode": process.get("job_pattern_mode"),
            "job_pattern_totals": process.get("job_pattern_totals"),
            "job_pattern_started_at": process.get("job_pattern_started_at"),
            "job_pattern_completed_at": process.get("job_pattern_completed_at"),
            "job_pattern_last_error": process.get("job_pattern_last_error"),
            "job_pagination_status": process.get("job_pagination_status"),
            "job_pagination_mode": process.get("job_pagination_mode"),
            "job_pagination_totals": process.get("job_pagination_totals"),
            "job_pagination_started_at": process.get("job_pagination_started_at"),
            "job_pagination_completed_at": process.get("job_pagination_completed_at"),
            "job_pagination_last_error": process.get("job_pagination_last_error"),
            "job_extraction_status": process.get("job_extraction_status"),
            "job_extraction_mode": process.get("job_extraction_mode"),
            "job_extraction_totals": process.get("job_extraction_totals"),
            "job_extraction_started_at": process.get("job_extraction_started_at"),
            "job_extraction_completed_at": process.get("job_extraction_completed_at"),
            "job_extraction_last_error": process.get("job_extraction_last_error"),
            "pipeline_enabled": process.get("pipeline_enabled", True),
            "pipeline_status": process.get("pipeline_status", "idle"),
            "pipeline_current_run_id": process.get("pipeline_current_run_id"),
            "pipeline_started_at": process.get("pipeline_started_at"),
            "pipeline_completed_at": process.get("pipeline_completed_at"),
            "pipeline_last_error": process.get("pipeline_last_error"),
            "pipeline_last_report": process.get("pipeline_last_report"),
            "alert_last_summary": process.get("alert_last_summary"),
            "alert_last_built_at": process.get("alert_last_built_at"),
            "last_pipeline_run_at": process.get("last_pipeline_run_at"),
            "next_pipeline_run_at": process.get("next_pipeline_run_at"),
            "source_file": process["source_file"],
            "process_domains": process.get("process_domains") or [],
            "totals": process["totals"],
            "domains": process.get("domains") or self._empty_domain_state(),
            "created_at": process["created_at"],
            "updated_at": process["updated_at"],
        }

    def _process_domains_from_legacy(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        timestamp = process.get("created_at") or _now()
        refs = self._legacy_domain_refs(process)
        return self._process_domains(refs, timestamp)

    def _legacy_domain_refs(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        domains = process.get("domains", {})
        refs = []
        refs.extend(domains.get("queued", []) or [])
        refs.extend(domains.get("processing", []) or [])
        refs.extend(domains.get("completed", []) or [])
        refs.extend(domains.get("failed", []) or [])
        seen: set[str] = set()
        cleaned = []
        for ref in refs:
            registered = ref.get("registered_domain")
            if not registered or registered in seen:
                continue
            cleaned.append(
                {
                    "domain": ref.get("domain"),
                    "registered_domain": registered,
                    "career_url": ref.get("career_url"),
                }
            )
            seen.add(registered)
        return cleaned

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
        processes = [self._serialize_process(await self._ensure_process_schema(document)) async for document in cursor]
        return {"processes": processes, "count": len(processes)}

    async def get_process(self, process_id: str) -> dict[str, Any]:
        await self._ensure_indexes()
        process = await self._ensure_process_schema(await self._load_process(process_id))
        domain_tasks = await self._load_related_domain_tasks(process)
        return {"process": self._serialize_process(process), "domain_tasks": domain_tasks}

    async def stop_domain(self, process_id: str, registered_domain: str, reason: str | None = None) -> dict[str, Any]:
        return await self._set_domain_enabled(process_id, registered_domain, enabled=False, reason=reason)

    async def resume_domain(self, process_id: str, registered_domain: str) -> dict[str, Any]:
        return await self._set_domain_enabled(process_id, registered_domain, enabled=True, reason=None)

    async def stop_domain_node(
        self,
        process_id: str,
        registered_domain: str,
        node: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return await self._set_domain_node_enabled(process_id, registered_domain, node, enabled=False, reason=reason)

    async def resume_domain_node(self, process_id: str, registered_domain: str, node: str) -> dict[str, Any]:
        return await self._set_domain_node_enabled(process_id, registered_domain, node, enabled=True, reason=None)

    async def _set_domain_enabled(
        self,
        process_id: str,
        registered_domain: str,
        *,
        enabled: bool,
        reason: str | None,
    ) -> dict[str, Any]:
        timestamp = _now()
        result = await self._process_domain_refs.update_one(
            {"process_id": process_id, "registered_domain": registered_domain},
            {
                "$set": {
                    "enabled": enabled,
                    "stop_reason": None if enabled else reason or "Stopped by admin",
                    "stopped_at": None if enabled else timestamp,
                    "updated_at": timestamp,
                }
            },
        )
        if not result.matched_count:
            raise ValueError(f"Domain '{registered_domain}' was not found in this process")
        await self._processes.update_one({"process_id": process_id}, {"$set": {"updated_at": timestamp}})
        return {"process_id": process_id, "registered_domain": registered_domain, "enabled": enabled}

    async def _set_domain_node_enabled(
        self,
        process_id: str,
        registered_domain: str,
        node: str,
        *,
        enabled: bool,
        reason: str | None,
    ) -> dict[str, Any]:
        if node not in NODE_CONTROL_NAMES:
            raise ValueError(f"Unknown node '{node}'")
        target = await self._process_domain_refs.find_one({"process_id": process_id, "registered_domain": registered_domain})
        if not target:
            raise ValueError(f"Domain '{registered_domain}' was not found in this process")
        controls = target.get("node_controls") or self._node_controls()
        control = controls.setdefault(node, self._node_control())
        timestamp = _now()
        control["enabled"] = enabled
        control["stopped"] = not enabled
        control["stop_reason"] = None if enabled else reason or "Stopped by admin"
        control["stopped_at"] = None if enabled else timestamp
        await self._process_domain_refs.update_one(
            {"process_id": process_id, "registered_domain": registered_domain},
            {"$set": {"node_controls": controls, "updated_at": timestamp}},
        )
        await self._processes.update_one({"process_id": process_id}, {"$set": {"updated_at": timestamp}})
        return {
            "process_id": process_id,
            "registered_domain": registered_domain,
            "node": node,
            "enabled": enabled,
        }

    def _find_process_domain(self, process_domains: list[dict[str, Any]], registered_domain: str) -> dict[str, Any]:
        for item in process_domains:
            if item.get("registered_domain") == registered_domain:
                return item
        raise ValueError(f"Domain '{registered_domain}' was not found in this process")

    async def _ensure_process_schema(self, process: dict[str, Any]) -> dict[str, Any]:
        ref_count = await self._process_domain_refs.count_documents({"process_id": process["process_id"]})
        if ref_count == 0:
            refs = process.get("process_domains") or self._process_domains_from_legacy(process)
            await self._upsert_process_domain_refs(
                process["process_id"],
                self._refs_from_process_domains(refs),
                process.get("created_at") or _now(),
            )
        updates = {
            "schema_version": PROCESS_SCHEMA_VERSION,
            "process_config": process.get("process_config") or self._process_config(int(process.get("agent_count") or 1)),
            "domains": self._empty_domain_state(),
            "updated_at": _now(),
        }
        if int(process.get("schema_version") or 1) >= PROCESS_SCHEMA_VERSION and not process.get("process_domains") and not any((process.get("domains") or {}).values()):
            return process
        await self._processes.update_one(
            {"process_id": process["process_id"]},
            {"$set": updates, "$unset": {"process_domains": ""}},
        )
        return {**process, **updates}

    def _refs_from_process_domains(self, process_domains: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "domain": item.get("domain"),
                "registered_domain": item.get("registered_domain"),
                "career_url": item.get("career_url") or item.get("supplied_career_url"),
            }
            for item in process_domains
            if item.get("registered_domain")
        ]

    async def _load_process(self, process_id: str) -> dict[str, Any]:
        process = await self._processes.find_one({"process_id": process_id})
        if not process:
            raise ValueError(f"Process '{process_id}' was not found")
        return process

    async def _load_related_domain_tasks(self, process: dict[str, Any]) -> list[dict[str, Any]]:
        registered_domains = await self._process_registered_domains(process)
        if not registered_domains:
            return []
        process_refs = await self._process_refs_by_domain(process["process_id"], registered_domains)
        legacy_tasks = await self._documents_by_domain(self._domain_tasks, registered_domains)
        search_runs = await self._documents_by_domain(self._search_runs, registered_domains)
        category_runs = await self._documents_by_domain(self._category_runs, registered_domains)
        pattern_runs = await self._documents_by_domain(self._pattern_runs, registered_domains)
        pagination_runs = await self._documents_by_domain(self._pagination_runs, registered_domains)
        extraction_runs = await self._documents_by_domain(self._extraction_runs, registered_domains)
        tasks = [
            self._serialize_domain_task(
                registered_domain,
                process_refs.get(registered_domain, {}),
                legacy_tasks.get(registered_domain, {}),
                search_runs.get(registered_domain, {}),
                category_runs.get(registered_domain, {}),
                pattern_runs.get(registered_domain, {}),
                pagination_runs.get(registered_domain, {}),
                extraction_runs.get(registered_domain, {}),
            )
            for registered_domain in registered_domains
        ]
        return sorted(tasks, key=lambda item: item["registered_domain"])

    async def _process_refs_by_domain(self, process_id: str, registered_domains: list[str]) -> dict[str, dict[str, Any]]:
        cursor = self._process_domain_refs.find({"process_id": process_id, "registered_domain": {"$in": registered_domains}})
        rows = [document async for document in cursor]
        return {str(document.get("registered_domain") or ""): document for document in rows if document.get("registered_domain")}

    async def _documents_by_domain(self, collection: Any, registered_domains: list[str]) -> dict[str, dict[str, Any]]:
        cursor = collection.find({"registered_domain": {"$in": registered_domains}})
        rows = [document async for document in cursor]
        return {str(document.get("registered_domain") or ""): document for document in rows if document.get("registered_domain")}

    async def _process_registered_domains(self, process: dict[str, Any]) -> list[str]:
        cursor = self._process_domain_refs.find({"process_id": process["process_id"]}, {"registered_domain": 1}).sort("created_at", ASCENDING)
        refs = [document async for document in cursor]
        if refs:
            return [item["registered_domain"] for item in refs if item.get("registered_domain")]
        domains = process.get("domains", {})
        refs = []
        refs.extend(domains.get("queued", []))
        refs.extend(domains.get("processing", []))
        refs.extend(domains.get("completed", []))
        refs.extend(domains.get("failed", []))
        return [ref["registered_domain"] for ref in refs if ref.get("registered_domain")]

    def _serialize_domain_task(
        self,
        registered_domain: str,
        process_ref: dict[str, Any],
        document: dict[str, Any],
        search_run: dict[str, Any],
        category_run: dict[str, Any],
        pattern_run: dict[str, Any],
        pagination_run: dict[str, Any],
        extraction_run: dict[str, Any],
    ) -> dict[str, Any]:
        category_result = category_run.get("result") if isinstance(category_run.get("result"), dict) else document.get("career_process")
        pattern_result = pattern_run.get("result") if isinstance(pattern_run.get("result"), dict) else document.get("job_pattern_result")
        pagination_result = pagination_run.get("result") if isinstance(pagination_run.get("result"), dict) else document.get("job_pagination_result")
        extraction_result = extraction_run.get("result") if isinstance(extraction_run.get("result"), dict) else document.get("job_extraction_result")
        return {
            "domain": process_ref.get("domain") or document.get("domain") or search_run.get("domain") or category_run.get("domain") or pattern_run.get("domain") or pagination_run.get("domain") or extraction_run.get("domain"),
            "registered_domain": registered_domain,
            "career_url": process_ref.get("career_url") or document.get("career_url"),
            "process_status": process_ref.get("status"),
            "current_step": process_ref.get("current_step"),
            "current_url": process_ref.get("current_url"),
            "current_page_index": process_ref.get("current_page_index"),
            "last_step_at": process_ref.get("last_step_at"),
            "enabled": process_ref.get("enabled", True),
            "node_controls": process_ref.get("node_controls") or {},
            "stop_reason": process_ref.get("stop_reason"),
            "status": search_run.get("status") or document.get("status") or process_ref.get("status"),
            "attempts": search_run.get("attempts", document.get("attempts", 0)),
            "last_started_at": search_run.get("last_started_at") or document.get("last_started_at"),
            "last_completed_at": search_run.get("last_completed_at") or document.get("last_completed_at"),
            "last_error": search_run.get("last_error") or document.get("last_error"),
            "career_process_status": category_run.get("status") or document.get("career_process_status"),
            "career_process": category_result,
            "career_process_last_completed_at": category_run.get("last_completed_at") or document.get("career_process_last_completed_at"),
            "career_process_last_error": category_run.get("last_error") or document.get("career_process_last_error"),
            "job_pattern_status": pattern_run.get("status") or document.get("job_pattern_status"),
            "job_pattern_mode": pattern_run.get("mode") or document.get("job_pattern_mode"),
            "job_pattern_result": pattern_result,
            "job_pattern_last_completed_at": pattern_run.get("last_completed_at") or document.get("job_pattern_last_completed_at"),
            "job_pattern_last_error": pattern_run.get("last_error") or document.get("job_pattern_last_error"),
            "job_pattern_last_error_details": pattern_run.get("last_error_details") or document.get("job_pattern_last_error_details"),
            "job_pagination_status": pagination_run.get("status") or document.get("job_pagination_status"),
            "job_pagination_mode": pagination_run.get("mode") or document.get("job_pagination_mode"),
            "job_pagination_result": pagination_result,
            "job_pagination_last_completed_at": pagination_run.get("last_completed_at") or document.get("job_pagination_last_completed_at"),
            "job_pagination_last_error": pagination_run.get("last_error") or document.get("job_pagination_last_error"),
            "job_pagination_last_error_details": pagination_run.get("last_error_details") or document.get("job_pagination_last_error_details"),
            "job_extraction_status": extraction_run.get("status") or document.get("job_extraction_status"),
            "job_extraction_mode": extraction_run.get("mode") or document.get("job_extraction_mode"),
            "job_extraction_result": extraction_result,
            "job_extraction_last_completed_at": extraction_run.get("last_completed_at") or document.get("job_extraction_last_completed_at"),
            "job_extraction_last_error": extraction_run.get("last_error") or document.get("job_extraction_last_error"),
            "job_extraction_last_error_details": extraction_run.get("last_error_details") or document.get("job_extraction_last_error_details"),
            "nodes": {
                "search": self._serialize_node_run(search_run, result_field="result"),
                "career_category": self._serialize_node_run(category_run, result_field="result"),
                "job_pattern": self._serialize_node_run(pattern_run, result_field="result"),
                "job_pagination": self._serialize_node_run(pagination_run, result_field="result"),
                "job_extraction": self._serialize_node_run(extraction_run, result_field="result"),
            },
        }

    def _serialize_node_run(self, run: dict[str, Any], *, result_field: str) -> dict[str, Any]:
        if not run:
            return {}
        return {
            "status": run.get("status"),
            "attempts": run.get("attempts"),
            "run_count": run.get("run_count"),
            "mode": run.get("mode"),
            "reused": run.get("reused"),
            "last_started_at": run.get("last_started_at"),
            "last_completed_at": run.get("last_completed_at"),
            "last_error": run.get("last_error"),
            "last_failure_type": run.get("last_failure_type"),
            "last_error_details": run.get("last_error_details"),
            "current_step": run.get("current_step"),
            "current_url": run.get("current_url"),
            "current_page_index": run.get("current_page_index"),
            "last_step_at": run.get("last_step_at"),
            "result": run.get(result_field),
            "updated_at": run.get("updated_at"),
        }



@lru_cache(maxsize=1)
def get_process_upload_service() -> ProcessUploadService:
    return ProcessUploadService(get_mongodb_service(), get_settings())
