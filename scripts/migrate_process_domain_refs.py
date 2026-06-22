from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient, UpdateOne

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional local convenience only
    load_dotenv = None


ROOT = Path(__file__).resolve().parents[1]
NODE_NAMES = ("search", "career_category", "job_pattern", "job_pagination", "job_extraction")


def now() -> datetime:
    return datetime.now(timezone.utc)


def load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env", override=False)
        load_dotenv(ROOT / "source" / ".env", override=False)
        return
    for path in (ROOT / ".env", ROOT / "source" / ".env"):
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def node_controls() -> dict[str, dict[str, Any]]:
    return {
        name: {
            "enabled": True,
            "stopped": False,
            "stop_reason": None,
            "stopped_at": None,
            "max_failures": 4,
        }
        for name in NODE_NAMES
    }


def clean_ref(ref: dict[str, Any], status: str, process: dict[str, Any]) -> dict[str, Any] | None:
    registered_domain = str(ref.get("registered_domain") or "").strip()
    if not registered_domain:
        return None
    timestamp = ref.get("created_at") or process.get("created_at") or now()
    cleaned = {
        "process_id": process["process_id"],
        "registered_domain": registered_domain,
        "domain": ref.get("domain"),
        "career_url": ref.get("career_url") or ref.get("supplied_career_url"),
        "status": status if status in {"queued", "processing", "completed", "failed"} else "queued",
        "enabled": ref.get("enabled", True),
        "stop_reason": ref.get("stop_reason"),
        "stopped_at": ref.get("stopped_at"),
        "node_controls": ref.get("node_controls") or node_controls(),
        "schema_version": 1,
        "created_at": timestamp,
        "updated_at": ref.get("updated_at") or process.get("updated_at") or timestamp,
    }
    for key in (
        "career_urls",
        "career_url",
        "supplied_career_url",
        "search_status",
        "source_type",
        "cache_scope",
        "reused",
        "reused_from_shared_domain",
        "completed_at",
        "failed_at",
        "error",
        "failure_type",
        "last_requeue_reason",
        "attempts",
        "started_at",
        "heartbeat_at",
        "lease_expires_at",
        "dispatched_at",
    ):
        if key in ref and ref.get(key) is not None:
            cleaned[key] = ref.get(key)
    return cleaned


def refs_from_process(process: dict[str, Any]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    refs: list[dict[str, Any]] = []
    for status, items in dict(process.get("domains") or {}).items():
        for ref in list(items or []):
            cleaned = clean_ref(ref, status, process)
            if not cleaned or cleaned["registered_domain"] in seen:
                continue
            refs.append(cleaned)
            seen.add(cleaned["registered_domain"])
    for ref in list(process.get("process_domains") or []):
        cleaned = clean_ref(ref, "queued", process)
        if not cleaned or cleaned["registered_domain"] in seen:
            continue
        refs.append(cleaned)
        seen.add(cleaned["registered_domain"])
    return refs


def totals_from_refs(refs: list[dict[str, Any]]) -> dict[str, int]:
    totals = {
        "domains": len(refs),
        "queued": 0,
        "processing": 0,
        "completed": 0,
        "failed": 0,
        "blocked": 0,
        "supplied_career_urls": 0,
    }
    for ref in refs:
        status = str(ref.get("status") or "queued")
        if status == "queued" and not search_enabled(ref):
            totals["blocked"] += 1
            continue
        if status in totals:
            totals[status] += 1
        if ref.get("career_url"):
            totals["supplied_career_urls"] += 1
    return totals


def search_enabled(ref: dict[str, Any]) -> bool:
    if ref.get("enabled") is False:
        return False
    control = ((ref.get("node_controls") or {}).get("search") or {})
    return control.get("enabled") is not False and control.get("stopped") is not True


def migrate(*, apply: bool, compact: bool) -> None:
    load_env()
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://admin:secret@127.0.0.1:27017")
    database_name = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
    process_collection = os.getenv("MONGODB_PROCESS_UPLOADS_COLLECTION", "process_uploads")
    refs_collection = os.getenv("MONGODB_PROCESS_DOMAIN_REFS_COLLECTION", "process_domain_refs")

    client = MongoClient(mongo_uri)
    db = client[database_name]
    processes = db[process_collection]
    refs = db[refs_collection]

    if apply:
        refs.create_index([("process_id", 1), ("registered_domain", 1)], unique=True)
        refs.create_index([("process_id", 1), ("status", 1), ("updated_at", 1)])
        refs.create_index([("registered_domain", 1), ("process_id", 1)])
        refs.create_index([("process_id", 1), ("enabled", 1)])

    process_count = 0
    ref_count = 0
    compact_count = 0
    for process in processes.find({}):
        process_id = process.get("process_id")
        if not process_id:
            continue
        process_refs = refs_from_process(process)
        if not process_refs:
            continue
        process_count += 1
        ref_count += len(process_refs)
        totals = totals_from_refs(process_refs)
        if not apply:
            continue
        operations = [
            UpdateOne(
                {"process_id": ref["process_id"], "registered_domain": ref["registered_domain"]},
                {
                    "$setOnInsert": {
                        "process_id": ref["process_id"],
                        "registered_domain": ref["registered_domain"],
                        "created_at": ref["created_at"],
                    },
                    "$set": ref_updates(ref),
                },
                upsert=True,
            )
            for ref in process_refs
        ]
        refs.bulk_write(operations, ordered=False)
        process_update: dict[str, Any] = {
            "$set": {"schema_version": 3, "totals": totals, "updated_at": now()},
        }
        if compact:
            process_update["$set"]["domains"] = {"queued": [], "processing": [], "completed": [], "failed": []}
            process_update["$unset"] = {"process_domains": ""}
            compact_count += 1
        processes.update_one({"_id": process["_id"]}, process_update)

    mode = "APPLIED" if apply else "DRY RUN"
    print(f"{mode}: processes={process_count}, refs={ref_count}, compacted={compact_count}")


def ref_updates(ref: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in ref.items() if key not in {"process_id", "registered_domain", "created_at"}}


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate embedded process domain arrays into process_domain_refs.")
    parser.add_argument("--apply", action="store_true", help="Write migration changes. Without this flag, only prints counts.")
    parser.add_argument("--compact", action="store_true", help="Remove bulky embedded process_domains/domains arrays after copying refs.")
    args = parser.parse_args()
    migrate(apply=args.apply, compact=args.compact)


if __name__ == "__main__":
    main()
