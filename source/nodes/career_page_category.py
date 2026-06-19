from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Callable, List

from prompts.career_category_prompt import create_job_page_analysis_prompt
from services.flow_safety import is_email_navigation_url, normalize_navigation_url, _is_external_domain, has_skip_extension, detect_blocked_platform
from services.openai_service import OpenAIAnalysisService
from urllib.parse import  urlparse

import asyncio

from services.navigation import navigate_to_url
from utils.logging import configure_logging, get_logger, log_event
from services.content_extraction import extract_page_content
from services.navigation_actions import follow_navigation_target
logger = get_logger("career_node")


EXTRACTED_CONTENT_PREVIEW_CHARS = 5000
NAVIGATION_LINK_KEYWORDS = (
    "current vacancies",
    "browse all",
    "browse vacancies",
    "vacancies",
    "jobs",
    "job search",
    "current roles",
    "open roles",
    "opportunities",
    "careers portal",
    "recruitment",
)


def _extract_interactive_targets(extracted_content: dict[str, Any]) -> list[dict[str, str]]:
    selector_map = ((extracted_content or {}).get("metadata") or {}).get("selector_map", {})
    targets: list[dict[str, str]] = []
    for item in dict(selector_map or {}).values():
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()
        action_url = str(item.get("action_url") or item.get("attributes", {}).get("href") or "").strip()
        kind = str(item.get("kind") or "").strip() or ("link" if item.get("is_link") else "button" if item.get("is_button") else "interactive")
        if not label or not action_url:
            continue
        targets.append({"label": label, "url": action_url, "kind": kind})
    return targets


def _format_interactive_targets(targets: list[dict[str, str]], limit: int = 40) -> str:
    lines = []
    for target in targets[:limit]:
        lines.append(f"- [{target['kind']}] {target['label']} -> {target['url']}")
    return "\n".join(lines)


def _find_navigation_target_from_selector_map(extracted_content: dict[str, Any]) -> dict[str, str | None]:
    best_target: dict[str, str | None] = {"url": None, "button": None, "element_type": None}
    best_score = 0
    for target in _extract_interactive_targets(extracted_content):
        label = target["label"].lower()
        url = target["url"].lower()
        haystack = f"{label} {url}"
        score = 0
        for index, keyword in enumerate(NAVIGATION_LINK_KEYWORDS):
            if keyword in haystack:
                score += max(1, len(NAVIGATION_LINK_KEYWORDS) - index)
        if "vacanc" in haystack:
            score += 20
        if "webrecruitment" in haystack or "recruit" in haystack:
            score += 10
        if score > best_score:
            best_score = score
            best_target = {
                "url": target["url"],
                "button": target["label"],
                "element_type": "link" if target["kind"] == "link" else target["kind"],
            }
    return best_target


def _normalize_match_text(value: str | None) -> str:
    return " ".join(str(value or "").lower().replace("\u2013", "-").replace("\u2014", "-").split())


def _fill_job_urls_from_selector_map(
    jobs_listed: list[dict[str, Any]],
    extracted_content: dict[str, Any],
) -> list[dict[str, Any]]:
    interactive_targets = [
        target
        for target in _extract_interactive_targets(extracted_content)
        if target.get("url") and target.get("kind") == "link"
    ]
    if not interactive_targets:
        return jobs_listed

    target_index: list[tuple[str, str, str]] = []
    for target in interactive_targets:
        label = str(target.get("label") or "").strip()
        url = str(target.get("url") or "").strip()
        if not label or not url:
            continue
        target_index.append((_normalize_match_text(label), label, url))

    filled_jobs: list[dict[str, Any]] = []
    for job in jobs_listed:
        job_copy = dict(job)
        if job_copy.get("job_url"):
            filled_jobs.append(job_copy)
            continue
        title = str(job_copy.get("title") or "").strip()
        normalized_title = _normalize_match_text(title)
        if not normalized_title:
            filled_jobs.append(job_copy)
            continue

        exact_match = next((url for normalized_label, _, url in target_index if normalized_label == normalized_title), None)
        contains_match = next(
            (
                url
                for normalized_label, _, url in target_index
                if normalized_title in normalized_label or normalized_label in normalized_title
            ),
            None,
        )
        job_copy["job_url"] = exact_match or contains_match
        filled_jobs.append(job_copy)
    return filled_jobs


def _normalize_career_analysis(response: dict[str, Any]) -> dict[str, Any]:
    next_action_target = response.get("next_action_target") or {}
    listing_ui = response.get("listing_ui") or {}
    jobs_listed = []
    for item in response.get("jobs_listed_on_page") or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "") or "").strip()
        job_url = str(item.get("job_url", "") or "").strip() or None
        jobs_listed.append(
            {
                "title": title,
                "job_url": job_url,
            }
        )

    return {
        "page_category": str(response.get("page_category", "") or "").strip(),
        "confidence": float(response.get("confidence", 0.0) or 0.0),
        "reasoning": str(response.get("reasoning", "") or "").strip(),
        "job_alert": response.get("job_alert"),
        "page_access_status": str(response.get("page_access_status", "") or "").strip(),
        "page_access_issue_detail": str(response.get("page_access_issue_detail", "") or "").strip() or None,
        "next_action_target": {
            "url": str(next_action_target.get("url", "") or "").strip() or None,
            "button": str(next_action_target.get("button", "") or "").strip() or None,
            "element_type": str(next_action_target.get("element_type", "") or "").strip() or None,
        },
        "jobs_listed_on_page": jobs_listed,
        "listing_ui": {
            "ui_category": str(listing_ui.get("ui_category", "") or "").strip() or None,
            "filter_present": bool(listing_ui.get("filter_present", False)),
            "filter_types": [str(item) for item in (listing_ui.get("filter_types") or []) if item],
            "sort_present": bool(listing_ui.get("sort_present", False)),
            "sort_types": [str(item) for item in (listing_ui.get("sort_types") or []) if item],
            "pagination_present": bool(listing_ui.get("pagination_present", False)),
            "pagination_type": str(listing_ui.get("pagination_type", "") or "").strip() or None,
            "pagination_category": str(listing_ui.get("pagination_category", "") or "").strip() or None,
            "pagination_navigation_method": str(listing_ui.get("pagination_navigation_method", "") or "").strip() or None,
            "next_page_url": str(listing_ui.get("next_page_url", "") or "").strip() or None,
        },
    }


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


def _fingerprint_extracted_content(content: str) -> str:
    normalized_content = str(content or "").strip()
    return hashlib.sha256(normalized_content.encode("utf-8")).hexdigest()


def _preview_extracted_content(content: str, limit: int = EXTRACTED_CONTENT_PREVIEW_CHARS) -> str:
    return str(content or "").strip()[:limit]


def _collect_job_listing_patterns(career_pages_analysis: list[dict]) -> list[dict[str, Any]]:
    patterns: list[dict[str, Any]] = []
    for result in career_pages_analysis:
        page_url = (
            result.get("classified_job_listing_url")
            or result.get("extracted_url")
            or result.get("current_url")
            or result.get("url")
            or result.get("navigation_url")
            or ""
        )
        if result.get("status") != "jobs_listed_on_page" or not page_url:
            continue
        patterns.append(
            {
                "page_url": page_url,
                "status": "pattern_pending",
                "pattern": None,
                "job_count": len(result.get("jobs_listed_on_page") or []),
                "example_jobs": list(result.get("jobs_listed_on_page") or [])[:2],
                "listing_ui": _clean_listing_ui(result.get("listing_ui") or {}),
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return patterns


def _clean_listing_ui(listing_ui: dict[str, Any]) -> dict[str, Any]:
    return {
        "ui_category": listing_ui.get("ui_category"),
        "filter_present": bool(listing_ui.get("filter_present", False)),
        "filter_types": [str(item) for item in (listing_ui.get("filter_types") or []) if item],
        "sort_present": bool(listing_ui.get("sort_present", False)),
        "sort_types": [str(item) for item in (listing_ui.get("sort_types") or []) if item],
        "pagination_present": bool(listing_ui.get("pagination_present", False)),
        "pagination_type": listing_ui.get("pagination_type"),
        "pagination_category": listing_ui.get("pagination_category"),
        "pagination_navigation_method": listing_ui.get("pagination_navigation_method"),
        "next_page_url": listing_ui.get("next_page_url"),
    }


def _next_candidate(candidates: list[str], checked: set[str]) -> str | None:
    for candidate in candidates:
        normalized = str(candidate or "").strip()
        if normalized and normalized not in checked:
            return normalized
    return None


_NON_ACCESSIBLE_PAGE_STATUSES = {
    "bot_detected",
    "login_required",
    "not_found",
    "empty_or_blank",
    "error",
}


def _send_heartbeat(heartbeat: Callable[[], None] | None) -> None:
    if heartbeat is None:
        return
    try:
        heartbeat()
    except Exception:
        log_event(
            logger,
            "warning",
            "career_category_heartbeat_failed",
            domain="career_category",
            exc_info=True,
        )


async def career_page_category_node(
    career_page_url: List[str],
    browser_session: Any,
    agent_index: int,
    agent_tab: dict,
    heartbeat: Callable[[], None] | None = None,
) -> dict:
    career_pages_analysis = []
    visited_urls: list[str] = []
    _send_heartbeat(heartbeat)

    for career_url in career_page_url:
        if career_url in visited_urls:
            continue
        visited_urls.append(career_url)
        _send_heartbeat(heartbeat)

        nav_response = await navigate_to_url(
            browser_session.page if browser_session is not None else None,
            agent_index=agent_index,
            tab_handle=agent_tab.get("handle"),
            url=career_url,
            post_navigation_delay_ms=0,
        )
        navigation_result = {**nav_response}
        _send_heartbeat(heartbeat)

        log_event(
            logger,
            "info",
            "navigation_result agent_index=%s url=%s status=%s",
            agent_index,
            career_url,
            navigation_result["status"],
            domain=career_url or "unknown",
            agent_index=agent_index,
            navigate_to=career_url,
            navigation_status=navigation_result["status"],
            current_url=navigation_result.get("current_url"),
        )

        if navigation_result["status"] != "navigated":
            navigation_result["navigation_url"] = career_url
            career_pages_analysis.append(navigation_result)
            continue

        await asyncio.sleep(5000 / 1000)
        _send_heartbeat(heartbeat)

        existing_job_urls: list[str] = []
        navigation_steps = 0
        navigation_history: list[dict] = []
        visited_buttons: set[str] = set()

        while True:
            # ── Extract page content ──────────────────────────────────────────
            extracted_content_response = await extract_page_content(
                browser_session.page if browser_session is not None else None,
                sections=["body"],
            )
            _send_heartbeat(heartbeat)

            if extracted_content_response is None or not extracted_content_response.get("markdown"):
                log_event(
                    logger,
                    "warning",
                    "page_content_extraction_failed navigate_to=%s",
                    career_url,
                    domain=career_url,
                    navigate_to=career_url,
                )
                navigation_result["error"] = "Unable to extract page content"
                navigation_result["status"] = "extraction_failed"
                break

            log_event(
                logger,
                "info",
                "page_content_extracted navigate_to=%s markdown_length=%s",
                career_url,
                len(extracted_content_response["markdown"]),
                domain=career_url,
                navigate_to=career_url,
                markdown_length=len(extracted_content_response["markdown"]),
                page_url=extracted_content_response.get("url"),
            )

            extracted_markdown = str(extracted_content_response["markdown"] or "")
            navigation_result["extracted_url"] = extracted_content_response.get("url")
            navigation_result["extracted_content_length"] = len(extracted_markdown)
            navigation_result["extracted_at"] = datetime.now(timezone.utc).isoformat()
            
            # NOTE: don't uncommnet it for debuging only
            # navigation_result["extracted_content"] = extracted_markdown
            # navigation_result["extracted_content_fingerprint"] = _fingerprint_extracted_content(extracted_markdown)
            # navigation_result["extracted_content_preview"] = _preview_extracted_content(extracted_markdown)
            

            interactive_targets = _extract_interactive_targets(extracted_content_response)

            # ── LLM analysis ──────────────────────────────────────────────────
            prompt = create_job_page_analysis_prompt(
                career_url,
                extracted_content_response["markdown"],
                interactive_links=_format_interactive_targets(interactive_targets),
            )
            service = OpenAIAnalysisService()
            analysis = await service.analyze_data(prompt=prompt, json_response=True)
            _send_heartbeat(heartbeat)

            if not analysis.success:
                navigation_result["status"] = "ai_analysis_failed"
                navigation_result["error"] = f"Career page categorization failed: {analysis.error}"
                break

            normalized = _normalize_career_analysis(analysis.response)
            normalized["jobs_listed_on_page"] = _fill_job_urls_from_selector_map(
                normalized["jobs_listed_on_page"],
                extracted_content_response,
            )
            navigation_result["token_used"] = analysis.token_usage
            navigation_result["llm_analysis"] = normalized
            navigation_result["job_alert"] = normalized.get("job_alert") or False
            navigation_result["page_access_status"] = normalized.get("page_access_status")
            navigation_result["page_access_issue_detail"] = normalized.get("page_access_issue_detail")

            # Fill missing navigation target from selector map if needed
            if (
                normalized["page_category"] == "navigation_required"
                and not normalized["next_action_target"]["url"]
                and not normalized["next_action_target"]["button"]
            ):
                selector_navigation_target = _find_navigation_target_from_selector_map(extracted_content_response)
                if selector_navigation_target["url"]:
                    normalized["next_action_target"] = selector_navigation_target
                    normalized["reasoning"] = (
                        f"{normalized['reasoning']} Navigation target filled from selector_map: "
                        f"{selector_navigation_target['button']}."
                    ).strip()

            # ── Accumulate job URLs ───────────────────────────────────────────
            job_urls_on_page = [
                str(item.get("job_url") or "").strip()
                for item in normalized["jobs_listed_on_page"]
                if isinstance(item, dict) and str(item.get("job_url") or "").strip()
            ]
            existing_job_urls = _dedupe_urls(existing_job_urls + job_urls_on_page)
            navigation_result["jobs_listed_on_page"] = existing_job_urls
            navigation_result["embedded_jobs_present"] = (
                bool(normalized["jobs_listed_on_page"]) and not bool(job_urls_on_page)
            )

            # ── Branch on category ────────────────────────────────────────────
            category = normalized["page_category"]
            page_access_status = normalized.get("page_access_status")
            page_inaccessible = page_access_status in _NON_ACCESSIBLE_PAGE_STATUSES

            if category in {"jobs_listed", "job_listings_preview_page"} and not page_inaccessible:
                listing_page_url = (
                    str(extracted_content_response.get("url") or "").strip()
                    or str(navigation_result.get("current_url") or "").strip()
                    or career_url
                )
                navigation_result["classified_job_listing_url"] = listing_page_url

            if category == "not_job_related":
                navigation_result["status"] = "access_issue" if page_inaccessible else "not_job_related"
                break

            elif category == "jobs_related_no_vacancies":
                if page_inaccessible:
                    navigation_result["status"] = "access_issue"
                else:
                    navigation_result["status"] = (
                        "jobs_related_no_vacancies_job_alert"
                        if navigation_result["job_alert"]
                        else "jobs_related_no_vacancies"
                    )
                break

            elif category == "jobs_related_general_info":
                navigation_result["status"] = "access_issue" if page_inaccessible else "jobs_related_general_info"
                break

            elif category == "single_job_posting":
                existing_job_urls = _dedupe_urls([career_url, *existing_job_urls])
                navigation_result["status"] = "single_job_posting"
                navigation_result["jobs_listed_on_page"] = existing_job_urls
                break

            elif category in {"jobs_listed", "job_listings_preview_page"}:
                if job_urls_on_page:
                    navigation_result["status"] = "jobs_listed_on_page"
                    navigation_result["listing_ui"] = normalized.get("listing_ui")
                    break

                target_url = normalize_navigation_url(
                    normalized["next_action_target"].get("url"), career_url
                )

                if target_url and target_url not in visited_urls and navigation_steps < 4:
                    visited_urls.append(target_url)
                    navigation_steps += 1

                    follow_status, landed_url, follow_error = await follow_navigation_target(
                        browser_session.page if browser_session is not None else None,
                        target_url,
                        None,
                        3000,
                    )
                    _send_heartbeat(heartbeat)

                    if follow_status not in {"navigated", "clicked"}:
                        navigation_result["status"] = follow_status
                        navigation_result["error"] = follow_error
                        break

                    navigation_result["status"] = "following_navigation_url"
                    continue

                # No target or already visited — resolve from page signal
                if navigation_result["job_alert"]:
                    navigation_result["status"] = "jobs_related_no_vacancies_job_alert"
                else:
                    navigation_result["status"] = "jobs_related_general_info"
                break

            elif category == "navigation_required":
                target_url = normalize_navigation_url(
                    normalized["next_action_target"]["url"], career_url
                )
                target_button = normalized["next_action_target"]["button"]

                step_record = {
                    "step": navigation_steps,
                    "from_url": navigation_result.get("extracted_url") or career_url,
                    "target_url": target_url,
                    "target_button": target_button,
                    "reasoning": normalized.get("reasoning"),
                    "status": None,
                    "landed_url": None,
                    "error": None,
                }

                # ── Guard: email / non-web — hard stop ────────────────────────
                if target_url and (
                    has_skip_extension(target_url)
                    or is_email_navigation_url(target_url)
                ):
                    step_record["status"] = (
                        "email_navigation_required" if is_email_navigation_url(target_url)
                        else "not_web_url"
                    )
                    navigation_history.append(step_record)
                    navigation_result["status"] = step_record["status"]
                    navigation_result["navigation_history"] = navigation_history
                    break

                # ── Guard: blocked platforms — hard stop ──────────────────────
                blocked_platform = detect_blocked_platform(target_url)
                if target_url and blocked_platform:
                    step_record["status"] = f"social_media_or_aggregator_{blocked_platform}"
                    navigation_history.append(step_record)
                    navigation_result["status"] = f"social_media_or_aggregator_{blocked_platform}"
                    navigation_result["blocked_platform"] = blocked_platform
                    navigation_result["blocked_platform_url"] = target_url
                    navigation_result["navigation_history"] = navigation_history
                    break

                # ── Guard: already visited — soft stop, resolve from page signal
                if target_url and target_url in visited_urls:
                    step_record["status"] = "already_visited_url"
                    navigation_history.append(step_record)
                    navigation_result["navigation_history"] = navigation_history

                    # Not a failure — derive status from what this page already told us
                    if navigation_result.get("job_alert"):
                        navigation_result["status"] = "jobs_related_no_vacancies_job_alert"
                    elif existing_job_urls:
                        navigation_result["status"] = "jobs_listed_on_page"
                    else:
                        navigation_result["status"] = "jobs_related_general_info"
                    break

                # ── Guard: step cap — only now is it a real failure ───────────
                if navigation_steps >= 4:
                    step_record["status"] = "max_navigation_steps_reached"
                    navigation_history.append(step_record)
                    navigation_result["status"] = "max_navigation_steps_reached"
                    navigation_result["navigation_history"] = navigation_history
                    break

                # ── External domain (not a blocked platform) — follow and check
                if target_url and _is_external_domain(target_url, career_url):
                    visited_urls.append(target_url)
                    navigation_steps += 1

                    follow_status, landed_url, follow_error = await follow_navigation_target(
                        browser_session.page if browser_session is not None else None,
                        target_url,
                        None,
                        3000,
                    )
                    _send_heartbeat(heartbeat)

                    step_record["status"] = f"external_domain_redirect — {follow_status}"
                    step_record["landed_url"] = landed_url
                    step_record["error"] = follow_error
                    navigation_history.append(step_record)
                    navigation_result["navigation_history"] = navigation_history
                    navigation_result["external_domain_redirect"] = target_url

                    if follow_status not in {"navigated", "clicked"}:
                        navigation_result["status"] = follow_status
                        navigation_result["error"] = follow_error
                        break

                    navigation_result["status"] = "external_domain_redirect"
                    continue

                # ── Follow URL (same domain) ──────────────────────────────────
                if target_url:
                    visited_urls.append(target_url)
                    navigation_steps += 1

                    follow_status, landed_url, follow_error = await follow_navigation_target(
                        browser_session.page if browser_session is not None else None,
                        target_url,
                        None,
                        3000,
                    )
                    _send_heartbeat(heartbeat)

                    step_record["status"] = follow_status
                    step_record["landed_url"] = landed_url
                    step_record["error"] = follow_error
                    navigation_history.append(step_record)
                    navigation_result["navigation_history"] = navigation_history

                    if follow_status not in {"navigated", "clicked"}:
                        navigation_result["status"] = follow_status
                        navigation_result["error"] = follow_error
                        break

                    navigation_result["status"] = "following_navigation_url"
                    continue

                # ── Follow button ─────────────────────────────────────────────
                elif target_button:
                    if target_button in visited_buttons:
                        step_record["status"] = "already_clicked_button"
                        navigation_history.append(step_record)
                        navigation_result["status"] = "already_clicked_button"
                        navigation_result["navigation_history"] = navigation_history
                        break

                    visited_buttons.add(target_button)
                    navigation_steps += 1

                    follow_status, landed_url, follow_error = await follow_navigation_target(
                        browser_session.page if browser_session is not None else None,
                        None,
                        target_button,
                        3000,
                    )
                    _send_heartbeat(heartbeat)

                    step_record["status"] = follow_status
                    step_record["landed_url"] = landed_url
                    step_record["error"] = follow_error
                    navigation_history.append(step_record)
                    navigation_result["navigation_history"] = navigation_history

                    if follow_status not in {"navigated", "clicked"}:
                        navigation_result["status"] = follow_status
                        navigation_result["error"] = follow_error
                        break

                    navigation_result["status"] = "follow_navigation_button"
                    continue

                else:
                    step_record["status"] = "no_navigation_target"
                    navigation_history.append(step_record)
                    navigation_result["status"] = "no_navigation_target"
                    navigation_result["navigation_history"] = navigation_history
                    break

            else:
                break

        career_pages_analysis.append(navigation_result)

    overview = _build_career_page_overview(career_pages_analysis)
    job_listing_patterns = _collect_job_listing_patterns(career_pages_analysis)

    return {
        "overview": overview,
        "career_pages_analysis": career_pages_analysis,
        "job_listing_patterns": job_listing_patterns,
    }







def _build_career_page_overview(career_pages_analysis: list[dict]) -> dict:
    all_job_urls: list[str] = []
    job_found_on_urls: list[str] = []
    listing_ui: dict | None = None
    job_alert_urls: list[str] = []

    no_vacancy_urls: list[str] = []
    general_job_info_urls: list[str] = []
    blocked_platform_urls: dict[str, list[str]] = {}
    external_redirect_urls: list[str] = []
    navigation_blocked_urls: list[str] = []
    embedded_urls: list[str] = []
    not_job_related_urls: list[str] = []
    access_issue_urls: list[dict] = []
    navigation_issues: list[dict] = []
    unknown_urls: list[str] = []

    job_found_statuses = {"jobs_listed_on_page", "single_job_posting", "external_domain_redirect"}
    no_vacancy_statuses = {"jobs_related_no_vacancies", "jobs_related_no_vacancies_job_alert"}
    general_job_info_statuses = {"jobs_related_general_info", "jobs_page"}

    # already_visited_url intentionally excluded — it resolves into a positive status above
    navigation_blocked_statuses = {
        "email_navigation_required", "not_web_url", "no_navigation_target",
        "already_clicked_button", "max_navigation_steps_reached",
    }
    access_issue_statuses = {
        "navigation_skipped", "navigation_timeout", "navigation_non_web_url",
        "action_failed", "download_started", "extraction_failed", "ai_analysis_failed", "access_issue",
    }

    for result in career_pages_analysis:
        status = result.get("status", "")
        source_url = (
            result.get("classified_job_listing_url")
            or result.get("job_listing_pattern_url")
            or result.get("extracted_url")
            or result.get("current_url")
            or result.get("url")
            or result.get("navigation_url")
            or ""
        )
        job_urls = result.get("jobs_listed_on_page") or []
        blocked_platform = result.get("blocked_platform")
        page_access_status = str(result.get("page_access_status") or "").strip()
        page_access_issue_detail = result.get("page_access_issue_detail")
        page_inaccessible = page_access_status in _NON_ACCESSIBLE_PAGE_STATUSES

        # Collect job alert pages regardless of which bucket this result falls into
        if result.get("job_alert"):
            job_alert_urls.append(source_url)

        if status in job_found_statuses and job_urls:
            all_job_urls = _dedupe_urls(all_job_urls + job_urls)
            job_found_on_urls.append(source_url)
            if listing_ui is None:
                listing_ui = result.get("listing_ui")

        elif blocked_platform:
            blocked_platform_urls.setdefault(blocked_platform, []).append(source_url)
            navigation_issues.append({
                "source_url": source_url,
                "reason": blocked_platform,
                "target_url": result.get("blocked_platform_url"),
            })

        elif status == "external_domain_redirect" and not job_urls:
            external_redirect_urls.append(result.get("external_domain_redirect") or source_url)

        elif status in no_vacancy_statuses:
            no_vacancy_urls.append(source_url)

        elif status in general_job_info_statuses:
            general_job_info_urls.append(source_url)

        elif status in navigation_blocked_statuses:
            navigation_blocked_urls.append(source_url)
            for step in result.get("navigation_history") or []:
                navigation_issues.append({
                    "source_url": source_url,
                    "reason": step.get("status") or status,
                    "target_url": step.get("target_url"),
                    "target_button": step.get("target_button"),
                    "error": step.get("error"),
                })

        elif result.get("embedded_jobs_present"):
            embedded_urls.append(source_url)

        elif page_inaccessible or status in access_issue_statuses:
            access_issue_urls.append({
                "url": source_url,
                "status": status,
                "page_access_status": page_access_status or None,
                "error": result.get("error"),
                "detail": page_access_issue_detail,
            })

        elif status == "not_job_related":
            not_job_related_urls.append(source_url)

        else:
            unknown_urls.append(source_url)

    # ── Determine top-level outcome ───────────────────────────────────────────
    if all_job_urls:
        outcome = "jobs_found"
        outcome_reason = f"{len(all_job_urls)} job(s) found across {len(job_found_on_urls)} career page(s)."

    elif no_vacancy_urls and job_alert_urls:
        outcome = "career_page_no_vacancies_with_job_alert"
        outcome_reason = "No open vacancies but career page confirmed — job alert signup available."

    elif no_vacancy_urls:
        outcome = "career_page_no_vacancies"
        outcome_reason = "Career page confirmed but no open vacancies at this time."

    elif general_job_info_urls:
        outcome = "career_page_general_job_info"
        outcome_reason = (
            "Career-related information was found, but these pages do not list vacancies and do not explicitly say there are no vacancies right now."
        )

    elif job_alert_urls:
        outcome = "no_jobs_but_job_alert_available"
        outcome_reason = "No jobs listed but at least one page offers a job alert signup."

    elif external_redirect_urls:
        outcome = "external_domain_redirect_no_jobs"
        outcome_reason = "Career page redirected to an external domain but no jobs were extractable there."

    elif blocked_platform_urls:
        platforms = ", ".join(sorted(blocked_platform_urls.keys()))
        outcome = "blocked_platform"
        outcome_reason = f"Career page points to a blocked platform ({platforms}) — not crawled."

    elif navigation_blocked_urls:
        outcome = "navigation_blocked"
        outcome_reason = "Navigation could not be completed (email link, non-web URL, or max steps reached)."

    elif embedded_urls:
        outcome = "embedded_job_board"
        outcome_reason = "Jobs appear to be loaded inside an embedded iframe/widget — not extractable from page text."

    elif not_job_related_urls and not unknown_urls and not access_issue_urls and not general_job_info_urls:
        outcome = "not_job_related"
        outcome_reason = "None of the career URLs contained job or hiring related content."

    elif access_issue_urls:
        inaccessible_count = len(access_issue_urls)
        not_job_related_count = len(not_job_related_urls)
        if inaccessible_count == len(career_pages_analysis):
            outcome = "access_issue"
            outcome_reason = "All discovered career pages had access issues, so job/career status could not be verified."
        elif all_job_urls or no_vacancy_urls or job_alert_urls:
            outcome = "jobs_found" if all_job_urls else "career_page_partial_access"
            base_reason = (
                f"{len(all_job_urls)} job(s) found across {len(job_found_on_urls)} career page(s)."
                if all_job_urls
                else "Career-related result found, but some discovered pages could not be accessed."
            )
            outcome_reason = (
                f"{base_reason} {inaccessible_count} additional page(s) had access issues."
            )
        elif not_job_related_count or general_job_info_urls:
            outcome = "access_issue"
            outcome_reason = (
                "No job/career result was confirmed, and some discovered pages could not be accessed, "
                "so the result cannot be treated as confidently not job related."
            )
        else:
            outcome = "access_issue"
            outcome_reason = "One or more career pages could not be accessed (timeout, bot detection, extraction failure)."

    else:
        outcome = "unknown"
        outcome_reason = "Could not determine career page status from the available pages."

    job_alert_present = bool(job_alert_urls)
    job_alert_note = (
        f"{len(job_alert_urls)} page(s) offer a job alert signup: " + ", ".join(job_alert_urls)
        if job_alert_present else None
    )

    return {
        "outcome": outcome,
        "outcome_reason": outcome_reason,
        "jobs_found": bool(all_job_urls),
        "total_jobs_found": len(all_job_urls),
        "job_urls": all_job_urls,
        "job_found_on_urls": job_found_on_urls,
        "listing_ui": listing_ui,
        "job_alert": job_alert_present,
        "job_alert_note": job_alert_note,
        "job_alert_urls": job_alert_urls,
        "career_page_confirmed": bool(no_vacancy_urls or all_job_urls or job_alert_urls or general_job_info_urls),
        "no_vacancy_urls": no_vacancy_urls,
        "general_job_info_urls": general_job_info_urls,
        "blocked_platform_urls": blocked_platform_urls,
        "external_redirect_urls": external_redirect_urls,
        "embedded_urls": embedded_urls,
        "navigation_blocked_urls": navigation_blocked_urls,
        "navigation_issues": navigation_issues,
        "not_job_related_urls": not_job_related_urls,
        "access_issue_urls": access_issue_urls,
        "unknown_urls": unknown_urls,
        "total_urls_processed": len(career_pages_analysis),
    }
    
    
    
