from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from api.dependencies import validate_admin_password
from core.config import get_settings
from services.mongodb_service import get_mongodb_service
from utils.logging import get_logger, log_event


router = APIRouter(prefix="/jobs")
logger = get_logger("domain_jobs_routes")


@router.get("")
async def list_domain_jobs(
    process_id: str | None = Query(default=None),
    registered_domain: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "domain_jobs_requested",
        domain="api",
        process_id=process_id,
        registered_domain=registered_domain,
        limit=limit,
    )
    settings = get_settings()
    mongodb = get_mongodb_service()
    jobs = mongodb.collection(settings.mongodb_domain_jobs_collection)
    query = await _job_query(process_id, registered_domain)
    cursor = jobs.find(query, {"_id": 0}).sort("first_seen_at", -1).limit(limit)
    rows = [document async for document in cursor]
    return {"status": "ok", "count": len(rows), "jobs": rows}


async def _job_query(process_id: str | None, registered_domain: str | None) -> dict[str, Any]:
    if registered_domain:
        return {"registered_domain": registered_domain, "status": "active"}
    if not process_id:
        return {"status": "active"}
    settings = get_settings()
    processes = get_mongodb_service().collection(settings.mongodb_process_uploads_collection)
    process = await processes.find_one({"process_id": process_id}, {"process_domains.registered_domain": 1, "domains.completed.registered_domain": 1})
    domains = _process_domains(process or {})
    if not domains:
        return {"registered_domain": {"$in": []}, "status": "active"}
    return {"registered_domain": {"$in": domains}, "status": "active"}


def _process_domains(process: dict[str, Any]) -> list[str]:
    domains = [
        str(item.get("registered_domain") or "")
        for item in process.get("process_domains") or []
        if item.get("registered_domain")
    ]
    if domains:
        return domains
    return [
        str(item.get("registered_domain") or "")
        for item in ((process.get("domains") or {}).get("completed") or [])
        if item.get("registered_domain")
    ]
