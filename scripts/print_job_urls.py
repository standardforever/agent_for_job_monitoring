from __future__ import annotations

import os
import sys
from pathlib import Path
from pprint import pprint

from dotenv import load_dotenv
from pymongo import MongoClient


ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    load_dotenv(ROOT / ".env", override=False)
    load_dotenv(ROOT / "source" / ".env", override=False)


def job_listing_urls() -> list[str]:
    load_env()
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://admin:secret@127.0.0.1:27017")
    database_name = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
    collection_name = os.getenv("MONGODB_PROCESS_DOMAIN_TASKS_COLLECTION", "process_domain_tasks")

    client = MongoClient(mongo_uri)
    collection = client[database_name][collection_name]

    urls: list[str] = []
    seen: set[str] = set()
    cursor = collection.find(
        {
            "career_process.jobs_found": True,
            "career_process.job_listing_patterns.listing_ui.pagination_present": True,
        },
        {
            "_id": 0,
            "career_process.job_listing_patterns.page_url": 1,
            "career_process.job_listing_patterns.listing_ui": 1,
        },
    ).sort("registered_domain", 1)
    for row in cursor:
        career_process = row.get("career_process") or {}
        add_urls(
            urls,
            seen,
            [
                pattern.get("page_url")
                for pattern in career_process.get("job_listing_patterns") or []
                if isinstance(pattern, dict)
                and (pattern.get("listing_ui") or {}).get("pagination_present") is True
            ],
        )
    return urls


def add_urls(output: list[str], seen: set[str], values: list[object]) -> None:
    for value in values:
        url = str(value or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        output.append(url)


def main() -> int:
    pprint(job_listing_urls())
    return 0


if __name__ == "__main__":
    sys.exit(main())
