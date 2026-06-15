from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from services.carrer_url_extractor import UrlExtractor
from services.flow_safety import extract_base_domain

from utils.logging import get_logger, log_event

logger = get_logger("url_extraction_node")


def _dedupe_urls(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _dedupe_career_items(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url", "") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        result.append(item)
    return result


def _normalize_input_target(raw_value: str) -> tuple[str, str]:
    """Returns (without_www, with_www)"""
    cleaned = str(raw_value or "").strip()
    with_scheme = cleaned if "://" in cleaned else f"https://{cleaned}"
    parsed = urlparse(with_scheme)
    
    # Strip fragment, get clean URL
    base = parsed._replace(fragment="")
    
    # Ensure no www
    netloc_no_www = base.netloc.replace("www.", "", 1)
    netloc_with_www = f"www.{netloc_no_www}" if not base.netloc.startswith("www.") else base.netloc
    
    without_www = base._replace(netloc=netloc_no_www).geturl()
    with_www = base._replace(netloc=netloc_with_www).geturl()
    
    return without_www, with_www


def _search_domain_text(raw_value: str) -> str:
    cleaned = str(raw_value or "").strip()
    with_scheme = cleaned if "://" in cleaned else f"https://{cleaned}"
    parsed = urlparse(with_scheme)
    domain = (parsed.netloc or parsed.path).strip().removeprefix("www.")
    return domain.rstrip("/")


def _is_cross_domain_redirect(original_url: str | None, final_url: str | None) -> bool:
    original_domain = extract_base_domain(original_url)
    final_domain = extract_base_domain(final_url)
    return bool(original_domain and final_domain and original_domain != final_domain)


async def career_url_extraction_node(
    navigate_to: str,
    browser_session: Any,
    *,
    registered_domain: str | None = None,
) -> dict:
    log_event(
        logger,
        "info",
        "url_extraction_started input=%s",
        navigate_to,
        domain=navigate_to,
        input_url=navigate_to,
    )
    return_dict = {
        "status": None,
        "error_message": None,
        "career_urls": [],
        "non_domain_career_urls": [],
        "diagnostics": {},
    }

    url_no_www, url_with_www = _normalize_input_target(navigate_to)
    extractor = UrlExtractor(browser_session.page)

    # Try without www first, then fall back to with www
    fallback_urls = await extractor.discover_job_urls_from_domain(
        domain=url_no_www,
        try_common_paths=False,
        extract_from_homepage=True,
        filter_domain=registered_domain,
    )

    active_url = url_no_www
    if not fallback_urls.get("success"):
        log_event(
            logger,
            "warning",
            "url_extraction_retry_with_www input=%s",
            url_no_www,
            domain=url_no_www,
            input_url=url_no_www,
        )
        fallback_urls = await extractor.discover_job_urls_from_domain(
            domain=url_with_www,
            try_common_paths=False,
            extract_from_homepage=True,
            filter_domain=registered_domain,
        )
        active_url = url_with_www
    

    fallback_meta = dict(fallback_urls.get("meta_data", {}) or {})
    redirect_detected = bool(fallback_meta.get("redirected"))
    cross_domain_redirect = _is_cross_domain_redirect(
        fallback_meta.get("original_url", active_url),
        fallback_meta.get("final_url", ""),
    )

    if not fallback_urls.get("success"):
        fallback_error = str(fallback_urls.get("error", "") or "Unknown error")
        return_dict["status"] = "domain_access_failed"
        return_dict["error_message"] = f"Failed to access domain for {active_url}: {fallback_error}"
        log_event(
            logger,
            "warning",
            "url_extraction_failed input=%s error=%s",
            active_url,
            fallback_error,
            domain=active_url,
            input_url=active_url,
            status=return_dict["status"],
            error=fallback_error,
        )
        return return_dict

    elif redirect_detected and cross_domain_redirect:
        return_dict["status"] = "domain_redirected"
        return_dict["error_message"] = (
            f"Domain redirected for {active_url}: "
            f"{fallback_meta.get('original_url', active_url)} -> {fallback_meta.get('final_url', '')}"
        )
        log_event(
            logger,
            "info",
            "url_extraction_redirected input=%s final_url=%s",
            active_url,
            fallback_meta.get("final_url", ""),
            domain=active_url,
            input_url=active_url,
            status=return_dict["status"],
            final_url=fallback_meta.get("final_url", ""),
        )
        return return_dict

    
    non_domain_careers_result = await extractor._extract_career_urls_from_page(active_url)
    search_domain = _search_domain_text(registered_domain or navigate_to)
    search_query = f"{search_domain} jobs careers vacancies openings"
    search_result = await extractor.search_duckduckgo(search_query, registered_domain or navigate_to)
    return_dict["diagnostics"]["search_query"] = search_query
    return_dict["diagnostics"]["homepage_status"] = fallback_urls.get("status")
    return_dict["diagnostics"]["search_status"] = search_result.get("status")
    return_dict["diagnostics"]["search_error"] = search_result.get("error")
    return_dict["diagnostics"]["active_url"] = active_url
    
    all_urls = _dedupe_urls(list(fallback_urls.get("meta_data", {}).get('all_urls', [])) +   list(search_result.get("meta_data", {}).get('all_urls', [])))
    non_domain_career_urls = _dedupe_career_items(list(non_domain_careers_result.get("result", []) or []))
    external_career_urls = [item["url"] for item in non_domain_career_urls]
    combined_job_urls = _dedupe_urls(
        list(fallback_urls.get("result", []) or [])
        + list(search_result.get("result", []) or [])
        + external_career_urls
    )
    return_dict["all_urls"] = all_urls
    return_dict["non_domain_career_urls"] = non_domain_career_urls
    return_dict["diagnostics"]["homepage_discovered_count"] = len(fallback_urls.get("meta_data", {}).get("all_urls", []) or [])
    return_dict["diagnostics"]["search_raw_count"] = len(search_result.get("meta_data", {}).get("all_urls", []) or [])
    return_dict["diagnostics"]["external_career_count"] = len(non_domain_career_urls)

    if not combined_job_urls:
        downstream_failures: list[str] = []
        if not bool(non_domain_careers_result.get("success", True)):
            downstream_failures.append(
                f"page_url_extraction_failed: {str(non_domain_careers_result.get('error') or non_domain_careers_result.get('status') or 'unknown_error')}"
            )
        if not bool(search_result.get("success", True)):
            downstream_failures.append(
                f"search_discovery_failed: {str(search_result.get('error') or search_result.get('status') or 'unknown_error')}"
            )

        if downstream_failures:
            return_dict["status"] = "career_page_discovery_failed"
            return_dict["error_message"] = " | ".join(downstream_failures)
            return_dict["diagnostics"]["downstream_failures"] = downstream_failures
            log_event(
                logger,
                "warning",
                "url_extraction_discovery_failed input=%s reason=%s",
                active_url,
                return_dict["error_message"],
                domain=active_url,
                input_url=active_url,
                status=return_dict["status"],
                error=return_dict["error_message"],
            )
            return return_dict

        return_dict["status"] = "no_career_page_found"
        return_dict["error_message"] = "no_job_or_career_candidates_found"
        return_dict["diagnostics"]["homepage_candidate_count"] = len(fallback_urls.get("result", []) or [])
        return_dict["diagnostics"]["search_candidate_count"] = len(search_result.get("result", []) or [])
        return_dict["diagnostics"]["non_domain_career_count"] = len(non_domain_career_urls)
        log_event(
            logger,
            "info",
            "url_extraction_no_career_page input=%s",
            active_url,
            domain=active_url,
            input_url=active_url,
            status=return_dict["status"],
        )
        return return_dict

    return_dict["status"] = "career_urls_found"
    return_dict["career_urls"] = combined_job_urls

    log_event(
        logger,
        "info",
        "url_extraction_completed input=%s status=%s job_filtered_count=%s non_domain_career_count=%s",
        active_url,
        return_dict["status"],  # fixed: was using dead local variable
        len(combined_job_urls),
        len(non_domain_career_urls),
        domain=active_url,
        input_url=active_url,
        status=return_dict["status"],
        job_filtered_count=len(combined_job_urls),
        non_domain_career_count=len(non_domain_career_urls)
    )

    return return_dict
