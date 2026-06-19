from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .config import OUTPUT_DIR


def _load_job_pattern_modules():
    from services.job_pattern import job_main
    from services.job_pattern.utils.extraction import extract_jobs_with_diagnostics, validate_jobs
    from services.job_pattern.utils.html_extraction import extract_clean_html

    return job_main, extract_clean_html, extract_jobs_with_diagnostics, validate_jobs


def _job_key(job: dict[str, Any]) -> str:
    title = re.sub(r"\s+", " ", str(job.get("job_title") or job.get("title") or "")).strip().lower()
    url = str(job.get("job_url") or "").strip().rstrip("/")
    return f"{title}|{url}"


def _unique_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    unique = []
    for job in jobs:
        key = _job_key(job)
        if not key.strip("|") or key in seen:
            continue
        seen.add(key)
        unique.append(job)
    return unique


def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.replace(":", "_") or "jobs"
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    return f"{host}_{digest}"


def save_jobs_json(page_url: str, jobs: list[dict[str, Any]], metadata: dict[str, Any]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{_slug_from_url(page_url)}_jobs.json"
    payload = {
        "source_url": page_url,
        "total_jobs": len(jobs),
        "jobs": jobs,
        "metadata": metadata,
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


class JobPatternBridge:
    def __init__(self, pattern: dict[str, Any] | None = None) -> None:
        (
            self.job_main,
            self.extract_clean_html,
            self.extract_jobs_with_diagnostics,
            self.validate_jobs,
        ) = _load_job_pattern_modules()
        self.pattern: dict[str, Any] | None = pattern
        self.seed_result: dict[str, Any] | None = None

    async def build_pattern(self, page, url: str) -> dict[str, Any]:
        self.seed_result = await self.job_main.main(page=page, url=url)
        self.pattern = self.seed_result.get("pattern") or {}
        return self.seed_result

    async def extract_current_page(self, page, url: str, pattern_override: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.pattern:
            raise RuntimeError("Job pattern has not been generated yet.")
        pattern = pattern_override or self.pattern
        html = await self.extract_clean_html(page)
        extraction = self.extract_jobs_with_diagnostics(html, pattern, base_url=url)
        jobs = extraction.get("jobs") or []
        diagnostics = extraction.get("diagnostics") or {}
        validation = self.validate_jobs(jobs, pattern, diagnostics=diagnostics)
        return {
            "jobs": jobs,
            "diagnostics": diagnostics,
            "validation": validation,
            "html_length": len(html),
        }
