from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from urllib.parse import urldefrag

from pymongo import ASCENDING

from core.config import Settings, get_settings
from services.sync_mongodb_service import SyncMongoDBService, get_sync_mongodb_service
from utils.logging import get_logger, log_event


logger = get_logger("domain_job_service")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class DomainJobService:
    def __init__(self, mongodb: SyncMongoDBService, settings: Settings) -> None:
        self._jobs = mongodb.collection(settings.mongodb_domain_jobs_collection)
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._jobs.create_index([("job_key", ASCENDING)], unique=True)
        self._jobs.create_index([("registered_domain", ASCENDING), ("status", ASCENDING), ("last_seen_at", ASCENDING)])
        self._indexes_ready = True

    def upsert_jobs(self, registered_domain: str, jobs: list[dict[str, Any]], timestamp: datetime | None = None) -> dict[str, Any]:
        self.ensure_indexes()
        timestamp = timestamp or _now()
        counts = {"seen": 0, "new": 0, "existing": 0, "skipped": 0}
        for job in jobs:
            job_doc = self._job_document(registered_domain, job, timestamp)
            if not job_doc["title"] and not job_doc.get("job_url"):
                counts["skipped"] += 1
                continue
            result = self._jobs.update_one(
                {"job_key": job_doc["job_key"]},
                {
                    "$set": {
                        "title": job_doc["title"],
                        "job_url": job_doc.get("job_url"),
                        "source_url": job_doc.get("source_url"),
                        "registered_domain": registered_domain,
                        "status": "active",
                        "last_seen_at": timestamp,
                        "updated_at": timestamp,
                    },
                    "$setOnInsert": {
                        "job_key": job_doc["job_key"],
                        "first_seen_at": timestamp,
                        "created_at": timestamp,
                    },
                },
                upsert=True,
            )
            counts["seen"] += 1
            if result.upserted_id:
                counts["new"] += 1
            else:
                counts["existing"] += 1
        counts["status"] = "stored" if counts["seen"] else "no_jobs_to_store"
        counts["message"] = self._message(counts)
        log_event(
            logger,
            "info",
            "domain_jobs_upsert_completed",
            domain="domain_jobs",
            registered_domain=registered_domain,
            seen=counts["seen"],
            new=counts["new"],
            existing=counts["existing"],
            skipped=counts["skipped"],
        )
        return counts

    def _job_document(self, registered_domain: str, job: dict[str, Any], timestamp: datetime) -> dict[str, Any]:
        title = str(job.get("job_title") or job.get("title") or "").strip()
        job_url = self._normalize_url(job.get("job_url"))
        source_url = self._normalize_url(job.get("source_url"))
        return {
            "job_key": self._job_key(registered_domain, title, job_url, source_url),
            "registered_domain": registered_domain,
            "title": title,
            "job_url": job_url,
            "source_url": source_url,
            "last_seen_at": timestamp,
        }

    def _job_key(self, registered_domain: str, title: str, job_url: str | None, source_url: str | None) -> str:
        basis = f"{registered_domain}|url|{job_url}" if job_url else f"{registered_domain}|fallback|{title.lower()}|{source_url}"
        return hashlib.sha256(basis.encode("utf-8")).hexdigest()

    def _normalize_url(self, value: Any) -> str | None:
        url = str(value or "").strip()
        if not url:
            return None
        return urldefrag(url)[0].rstrip("/") or None

    def _message(self, counts: dict[str, Any]) -> str:
        if not counts["seen"]:
            return "No valid jobs were extracted."
        return (
            f"Stored {counts['seen']} jobs: "
            f"{counts['new']} new, {counts['existing']} already known, {counts['skipped']} skipped."
        )


@lru_cache(maxsize=1)
def get_domain_job_service() -> DomainJobService:
    return DomainJobService(get_sync_mongodb_service(), get_settings())
