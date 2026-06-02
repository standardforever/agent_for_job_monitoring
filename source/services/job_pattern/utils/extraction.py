import html as html_lib
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .patterns import ensure_pattern_defaults, get_by_dot_path, normalize_text
from utils.logging import get_logger, log_event


logger = get_logger("job_pattern_extraction")


NOISE_TITLE_RE = re.compile(
    r"^(?:\d+|next|previous|prev|older|newer|first|last|view|apply|details?)$",
    re.I,
)

DEFAULT_NON_JOB_TITLE_TERMS = [
    "how to apply",
    "important information",
    "equality",
    "diversity",
    "modern slavery",
    "what we offer",
    "benefits",
    "about us",
]


def extract_title(container, title_pattern: Dict[str, Any]) -> Optional[str]:
    selector = title_pattern.get("selector")
    source = title_pattern.get("source")
    attribute = title_pattern.get("attribute")
    target = container.select_one(selector) if selector else container

    if not target:
        return None
    if source == "attribute":
        return normalize_text(target.get(attribute))
    return normalize_text(target.get_text(" ", strip=True))


def extract_url(container, url_pattern: Dict[str, Any], base_url: str = "") -> Optional[str]:
    strategy = url_pattern.get("strategy")
    selector = url_pattern.get("selector")
    attribute = url_pattern.get("attribute")
    json_path = url_pattern.get("json_path")
    regex = url_pattern.get("regex")

    raw_url = None
    if strategy == "element_attribute":
        target = container.select_one(selector) if selector else None
        if target and attribute:
            raw_url = target.get(attribute)
    elif strategy == "container_attribute":
        if attribute:
            raw_url = container.get(attribute)
    elif strategy == "container_json_attribute":
        if attribute:
            raw_json = container.get(attribute)
            if raw_json:
                try:
                    parsed = json.loads(html_lib.unescape(raw_json))
                    raw_url = get_by_dot_path(parsed, json_path)
                    if raw_url is None and isinstance(parsed, dict):
                        for key in ("url", "href", "link", "src", "value"):
                            if parsed.get(key):
                                raw_url = parsed[key]
                                break
                except json.JSONDecodeError:
                    raw_url = None
    elif strategy == "regex_from_container_html":
        if regex:
            match = re.search(regex, str(container))
            if match:
                raw_url = match.group(1)

    if not raw_url:
        return None

    url = urljoin(base_url, str(raw_url).strip())
    if url.startswith(("mailto:", "#", "javascript:")):
        return None
    if re.search(r"\.(docx?|pdf|xlsx?|csv|zip)$", url, re.I):
        return None
    return url


def evaluate_candidate(
    title: Optional[str],
    url: Optional[str],
    pattern: Dict[str, Any],
) -> Dict[str, Any]:
    rules = pattern.get("validation_rules", {})
    url_cfg = pattern.get("job_url", {})
    reasons: List[str] = []

    title = normalize_text(title)
    if not title:
        reasons.append("missing_title")
    elif NOISE_TITLE_RE.fullmatch(title):
        reasons.append("noise_title")
    elif len(title) < rules.get("minimum_title_length", 4):
        reasons.append("title_too_short")
    elif title.lower() in {
        value.lower().strip()
        for value in rules.get("title_must_not_equal", [])
        if value.strip()
    }:
        reasons.append("blocked_title_exact")
    elif any(
        term.lower().strip() in title.lower()
        for term in rules.get("title_must_not_contain_any", [])
        if term.strip()
    ):
        reasons.append("blocked_title_substring")
    elif any(term in title.lower() for term in DEFAULT_NON_JOB_TITLE_TERMS):
        reasons.append("matched_default_non_job_title_terms")

    url_required = url_cfg.get("required", url_cfg.get("strategy") != "none")
    allow_missing_url = rules.get("allow_missing_job_url", not url_required)

    if url:
        if any(
            part.lower().strip() in url.lower()
            for part in rules.get("url_must_not_contain_any", [])
            if part.strip()
        ):
            reasons.append("blocked_url_substring")
        if rules.get("url_must_contain_any") and not any(
            part.lower().strip() in url.lower()
            for part in rules.get("url_must_contain_any", [])
            if part.strip()
        ):
            reasons.append("url_missing_required_hint")
    elif url_required and not allow_missing_url:
        reasons.append("missing_required_url")

    return {
        "keep": len(reasons) == 0,
        "title": title,
        "url": url,
        "reasons": reasons,
    }


def extract_jobs_with_diagnostics(
    html: str,
    pattern: Dict[str, Any],
    base_url: str = "",
) -> Dict[str, Any]:
    pattern = ensure_pattern_defaults(pattern)
    if not pattern.get("is_job_listing_page"):
        log_event(
            logger,
            "info",
            "job_pattern_extraction_skipped_not_listing base_url=%s",
            base_url,
            domain=base_url or "job_pattern",
            base_url=base_url,
            is_job_listing_page=pattern.get("is_job_listing_page"),
        )
        return {
            "jobs": [],
            "diagnostics": {
                "containers_seen": 0,
                "accepted_jobs": 0,
                "rejected_candidates": [],
            },
        }

    soup = BeautifulSoup(html, "lxml")
    container_selector = pattern.get("job_container_selector")
    if not container_selector:
        log_event(
            logger,
            "warning",
            "job_pattern_extraction_missing_container_selector base_url=%s",
            base_url,
            domain=base_url or "job_pattern",
            base_url=base_url,
        )
        return {
            "jobs": [],
            "diagnostics": {
                "containers_seen": 0,
                "accepted_jobs": 0,
                "rejected_candidates": [{"reason": "missing_container_selector"}],
            },
        }

    containers = soup.select(container_selector)
    log_event(
        logger,
        "info",
        "job_pattern_extraction_started base_url=%s selector=%s containers_seen=%s",
        base_url,
        container_selector,
        len(containers),
        domain=base_url or "job_pattern",
        base_url=base_url,
        job_container_selector=container_selector,
        containers_seen=len(containers),
        html_length=len(html),
    )
    jobs = []
    seen = set()
    rejected_candidates = []

    for index, container in enumerate(containers, start=1):
        title = extract_title(container, pattern["job_title"])
        url = extract_url(container, pattern["job_url"], base_url=base_url)
        evaluation = evaluate_candidate(title, url, pattern)

        if not evaluation["keep"]:
            rejected_candidates.append(
                {
                    "container_index": index,
                    "title": evaluation["title"],
                    "url": evaluation["url"],
                    "reasons": evaluation["reasons"],
                }
            )
            continue

        key = (evaluation["title"].lower(), evaluation["url"])
        if key in seen:
            rejected_candidates.append(
                {
                    "container_index": index,
                    "title": evaluation["title"],
                    "url": evaluation["url"],
                    "reasons": ["duplicate_job"],
                }
            )
            continue

        seen.add(key)
        jobs.append({"job_title": evaluation["title"], "job_url": evaluation["url"]})

    log_event(
        logger,
        "info",
        "job_pattern_extraction_completed base_url=%s accepted_jobs=%s rejected_candidates=%s",
        base_url,
        len(jobs),
        len(rejected_candidates),
        domain=base_url or "job_pattern",
        base_url=base_url,
        job_container_selector=container_selector,
        containers_seen=len(containers),
        accepted_jobs=len(jobs),
        rejected_candidate_count=len(rejected_candidates),
        sample_rejections=rejected_candidates[:5],
    )
    return {
        "jobs": jobs,
        "diagnostics": {
            "containers_seen": len(containers),
            "accepted_jobs": len(jobs),
            "rejected_candidates": rejected_candidates,
        },
    }


def extract_jobs_from_pattern(
    html: str,
    pattern: Dict[str, Any],
    base_url: str = "",
) -> List[Dict[str, str]]:
    jobs = extract_jobs_with_diagnostics(html, pattern, base_url=base_url)["jobs"]
    log_event(
        logger,
        "info",
        "job_pattern_extract_jobs_from_pattern_completed base_url=%s job_count=%s",
        base_url,
        len(jobs),
        domain=base_url or "job_pattern",
        base_url=base_url,
        job_count=len(jobs),
    )
    return jobs


def validate_jobs(
    jobs: List[Dict[str, str]],
    pattern: Dict[str, Any],
    diagnostics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    pattern = ensure_pattern_defaults(pattern)
    rules = pattern.get("validation_rules", {})
    problems = []

    minimum = rules.get("minimum_jobs_expected", 1)
    if len(jobs) < minimum:
        problems.append(f"Expected at least {minimum} jobs, found {len(jobs)}.")

    required_url_parts = rules.get("url_must_contain_any", [])
    if required_url_parts and jobs:
        matching_urls = [
            job
            for job in jobs
            if job.get("job_url") and any(part in job["job_url"] for part in required_url_parts)
        ]
        if not matching_urls:
            problems.append("No extracted job URLs matched url_must_contain_any rule.")

    rejected_candidates = diagnostics.get("rejected_candidates", []) if diagnostics else []
    containers_seen = diagnostics.get("containers_seen", 0) if diagnostics else 0
    result = {
        "valid": len(problems) == 0,
        "job_count": len(jobs),
        "containers_seen": containers_seen,
        "rejected_candidate_count": len(rejected_candidates),
        "sample_rejections": rejected_candidates[:5],
        "problems": problems,
    }

    log_event(
        logger,
        "info",
        "job_pattern_validation_completed valid=%s job_count=%s problems=%s",
        result["valid"],
        result["job_count"],
        problems,
        domain="job_pattern",
        valid=result["valid"],
        job_count=result["job_count"],
        containers_seen=containers_seen,
        rejected_candidate_count=len(rejected_candidates),
        problems=problems,
    )
    return result
