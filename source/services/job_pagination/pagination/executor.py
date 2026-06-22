from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from .browser import extract_reduced_html_from_page, light_lazy_scroll
from .click import find_click_target, next_control_state
from .config import MAX_PAGINATION_TEST_PAGES, MAX_REPAIR_ATTEMPTS
from .llm import analyse_pagination_repair, analyse_url_pattern_from_click
from .state import current_value_after_click_url_pattern, next_value_after_current_url_value
from .url_utils import _as_int, _increment_value, _make_url_from_plan
from ..extraction.extractor import JobExtractionContext
from ..pipeline.context import PipelineContext
from ..pipeline.policy import classify_action_outcome
from services.navigation import navigate_to_url
from utils.logging import get_logger, log_event


logger = get_logger("job_pagination_executor")


async def _job_list_signature(page, selector: str | None) -> dict[str, Any]:
    if not selector:
        return {"count": 0, "items": []}
    try:
        return await page.evaluate(
            """(selector) => {
                let nodes = [];
                try {
                    nodes = Array.from(document.querySelectorAll(selector));
                } catch (error) {
                    return { count: 0, items: [], error: String(error) };
                }

                const keyFor = (node) => {
                    const link = node.querySelector('a[href]');
                    const href = link ? link.href : '';
                    const text = (node.innerText || node.textContent || '')
                        .replace(/\\s+/g, ' ')
                        .trim()
                        .slice(0, 220);
                    return `${href} ${text}`.trim();
                };

                const head = nodes.slice(0, 5).map(keyFor);
                const tail = nodes.slice(Math.max(0, nodes.length - 3)).map(keyFor);
                return { count: nodes.length, items: head.concat(tail) };
            }""",
            selector,
        )
    except Exception as exc:
        return {"count": 0, "items": [], "error": str(exc)}


def _signature_changed(before: dict[str, Any], after: dict[str, Any]) -> bool:
    before_count = int(before.get("count") or 0)
    after_count = int(after.get("count") or 0)
    if before_count <= 0 or after_count <= 0:
        return False
    return before_count != after_count or before.get("items") != after.get("items")


async def _wait_for_job_signature_change(
    page,
    selector: str | None,
    before_signature: dict[str, Any],
    timeout_ms: int,
) -> tuple[bool, dict[str, Any]]:
    if not selector or int(before_signature.get("count") or 0) <= 0:
        await asyncio.sleep(timeout_ms / 1000)
        return True, before_signature

    deadline = asyncio.get_running_loop().time() + max(timeout_ms, 4000) / 1000
    latest = before_signature
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.5)
        latest = await _job_list_signature(page, selector)
        if _signature_changed(before_signature, latest):
            await asyncio.sleep(0.5)
            return True, latest
    return False, latest


def _has_constructible_url_plan(url_plan: dict[str, Any], base_url: str, value: str | None) -> bool:
    return bool(_make_url_from_plan(url_plan, base_url, str(value) if value is not None else None))


def _next_url_value(url_plan: dict[str, Any]) -> str | None:
    sequence_type = url_plan.get("sequence_type")
    increment = _as_int(url_plan.get("increment"), 1) or 1
    value = url_plan.get("next_value")
    if value:
        return str(value)
    current = url_plan.get("current_value")
    if current:
        return _increment_value(str(current), sequence_type, increment)
    return None


def _advance_url_value(url_plan: dict[str, Any], value: str | None) -> str | None:
    if value is None:
        return None
    return _increment_value(
        str(value),
        url_plan.get("sequence_type"),
        _as_int(url_plan.get("increment"), 1) or 1,
    )


def _has_click_fallback(click_plan: dict[str, Any]) -> bool:
    if not click_plan:
        return False
    return any(
        click_plan.get(key)
        for key in (
            "next_selector",
            "selector_candidates",
            "aria_label_template",
            "text_template",
            "item_selector",
            "container_selector",
        )
    )


def _navigation_parts(navigation: dict[str, Any], page_url: str) -> tuple[dict[str, Any], dict[str, Any], int, str | None]:
    url_plan = navigation.get("url") or {}
    click_plan = navigation.get("click") or {}
    max_pages = min(_as_int(navigation.get("max_pages"), MAX_PAGINATION_TEST_PAGES) or 1, MAX_PAGINATION_TEST_PAGES)
    return url_plan, click_plan, max_pages, _next_url_value(url_plan)


def _looks_like_internal_endpoint(url: str | None) -> bool:
    parsed = urlparse(str(url or ""))
    path = parsed.path.lower()
    return any(
        marker in path
        for marker in (
            "/wp-admin/admin-ajax",
            "/admin-ajax",
            "/api/",
            "/ajax/",
            "/xhr/",
            ".json",
        )
    )


def _end_reason_from_failure(detail: str, disabled: bool, successful_moves: int) -> str | None:
    lowered = str(detail or "").lower()
    if disabled or lowered.startswith("click_target_disabled"):
        return "next_control_disabled"
    if lowered.startswith("next_control_hidden"):
        return "next_control_hidden"
    if successful_moves <= 0:
        return None
    if lowered.startswith("repeated_url"):
        return "repeated_url_after_success"
    if lowered.startswith("bad_http_status:404") or lowered.startswith("bad_http_status:410"):
        return "not_found_after_success"
    if lowered.startswith("click_target_not_found") and "next" in lowered:
        return "next_control_missing_after_success"
    return None


def _repair_attempt_limit(successful_moves: int, detail: str) -> int:
    if successful_moves <= 0:
        return MAX_REPAIR_ATTEMPTS
    lowered = str(detail or "").lower()
    if lowered.startswith("timeout") or lowered.startswith("navigation_error") or lowered.startswith("click_error"):
        return 1
    if lowered.startswith("click_target_not_found") or lowered == "click_fallback_missing":
        return 1
    return 0


def _repair_says_end(repair: dict[str, Any]) -> bool:
    if repair.get("is_end_of_pagination"):
        return True
    reason = str(repair.get("stop_reason") or repair.get("reason") or "").lower()
    return any(token in reason for token in ("end", "last page", "no more", "disabled", "exhausted"))


async def _load_url_page(page, target_url: str) -> tuple[bool, str]:
    try:
        navigation = await navigate_to_url(
            page,
            agent_index=0,
            tab_handle=None,
            url=target_url,
            post_navigation_delay_ms=1500,
        )
        if navigation.get("status") != "navigated":
            return False, str(navigation.get("error") or navigation.get("status") or "navigation_failed")
        await asyncio.sleep(1.5)
        await light_lazy_scroll(page)
        return True, "status:navigated"
    except PlaywrightTimeoutError:
        return False, "timeout"
    except Exception as exc:
        return False, f"navigation_error:{exc}"


async def _click_next_page(
    page,
    click_plan: dict[str, Any],
    page_number: int,
    job_selector: str | None,
) -> tuple[bool, str, bool, dict[str, Any]]:
    target, used_selector, disabled = await find_click_target(page, click_plan, page_number)
    if target is None:
        return False, f"click_target_not_found:{used_selector}", False, {}
    if disabled:
        return False, f"click_target_disabled:{used_selector}", True, {}

    wait_ms = _as_int(click_plan.get("wait_after_click_ms"), 1500) or 1500
    before_url = page.url
    before_signature = await _job_list_signature(page, job_selector)
    evidence: dict[str, Any] = {"before_url": before_url, "before_signature": before_signature, "selector": used_selector}
    try:
        await target.scroll_into_view_if_needed(timeout=5_000)
        await target.click(timeout=10_000)
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=5_000)
        except PlaywrightTimeoutError:
            pass
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except PlaywrightTimeoutError:
            pass
        changed, after_signature = await _wait_for_job_signature_change(page, job_selector, before_signature, wait_ms + 6_000)
        await light_lazy_scroll(page)
        evidence.update(
            {
                "after_url": page.url,
                "after_signature": after_signature,
                "url_changed": page.url != before_url,
                "job_signature_changed": changed,
            }
        )
        if not changed and page.url == before_url:
            return (
                False,
                "click_no_job_change:"
                f"{used_selector}:before_count={before_signature.get('count')}:"
                f"after_count={after_signature.get('count')}",
                False,
                evidence,
            )
        return True, f"clicked:{used_selector}:url_changed={page.url != before_url}", False, evidence
    except PlaywrightTimeoutError:
        return False, f"click_timeout:{used_selector}", False, evidence
    except Exception as exc:
        return False, f"click_error:{used_selector}:{exc}", False, evidence


async def execute_pagination(
    page,
    pagination_result: dict[str, Any],
    extractor: JobExtractionContext,
    context: PipelineContext,
) -> dict[str, Any]:
    navigation = pagination_result.get("navigation") or {}
    url_plan, click_plan, max_pages, url_value = _navigation_parts(navigation, page.url)
    seen_urls = {page.url}
    url_plan_disabled = False
    reports: list[dict[str, Any]] = []
    repairs: list[dict[str, Any]] = []
    navigation_repaired = False
    successful_moves = 0
    stop_reason = "completed_or_limit"

    log_event(
        logger,
        "info",
        "pagination_execution_started",
        domain="job_pagination",
        page_url=page.url,
        max_pages=max_pages,
        method=navigation.get("method"),
    )
    for index in range(2, max_pages + 1):
        context.set_progress("pagination_page_started", page.url, index)
        repair_attempts = 0
        navigated = False
        used_method = None
        detail = ""

        while True:
            previous_url = page.url
            url_failed_this_page = False
            disabled = False
            action_evidence: dict[str, Any] = {"page_index": index, "previous_url": previous_url}

            if not url_plan_disabled and _has_constructible_url_plan(url_plan, previous_url, url_value):
                target_url = _make_url_from_plan(url_plan, previous_url, url_value)
                if _looks_like_internal_endpoint(target_url):
                    detail = f"internal_endpoint_url_rejected:{target_url}"
                    url_plan_disabled = True
                    context.add_error("pagination_execution", detail, page_index=index, method="url")
                elif target_url in seen_urls:
                    detail = f"repeated_url:{target_url}"
                    url_plan_disabled = True
                else:
                    log_event(logger, "info", "pagination_url_attempt_started", domain="job_pagination", page_url=page.url, page_index=index, target_url=target_url)
                    context.set_progress("pagination_loading_url_page", target_url, index)
                    ok, detail = await _load_url_page(page, target_url)
                    used_method = "url"
                    navigated = ok
                    action_evidence.update({"method": "url", "target_url": target_url, "after_url": page.url, "ok": ok})
                    if ok:
                        seen_urls.add(page.url)
                    else:
                        url_failed_this_page = True
                        context.add_error("pagination_execution", detail, page_index=index, method="url")
                        try:
                            await page.goto(previous_url, timeout=30_000, wait_until="domcontentloaded")
                            await asyncio.sleep(1)
                            await light_lazy_scroll(page)
                        except Exception as exc:
                            context.add_error("pagination_restore", str(exc), page_index=index)

            if not navigated and _has_click_fallback(click_plan):
                control_state, state_selector = await next_control_state(page, click_plan, index)
                if control_state in ("disabled", "hidden"):
                    disabled = control_state == "disabled"
                    detail = (
                        f"next_control_{control_state}:"
                        f"{state_selector or click_plan.get('disabled_selector') or click_plan.get('next_selector') or 'unknown'}"
                    )
                    action_evidence.update({"method": "click", "control_state": control_state, "selector": state_selector})
                    context.add_error("pagination_end_detected", detail, page_index=index, method="click")
                else:
                    log_event(logger, "info", "pagination_click_attempt_started", domain="job_pagination", page_url=page.url, page_index=index)
                    context.set_progress("pagination_clicking_next_page", page.url, index)
                    before_click_url = page.url
                    ok, detail, disabled, click_evidence = await _click_next_page(
                        page,
                        click_plan,
                        index,
                        extractor.job_container_selector,
                    )
                    action_evidence.update({"method": "click", "ok": ok, **click_evidence})
                    used_method = "click"
                    navigated = ok
                    if not ok:
                        context.add_error("pagination_execution", detail, page_index=index, method="click")

                    if navigated and page.url != before_click_url:
                        pattern = analyse_url_pattern_from_click(before_click_url, page.url)
                        log_event(
                            logger,
                            "info",
                            "pagination_url_pattern_from_click_completed",
                            domain="job_pagination",
                            page_url=page.url,
                            page_index=index,
                            can_use_url=pattern.get("can_use_url"),
                            confidence=pattern.get("confidence"),
                        )
                        if pattern.get("can_use_url") and pattern.get("url"):
                            url_plan = pattern["url"]
                            url_value = current_value_after_click_url_pattern(url_plan, url_value)
                            url_plan_disabled = False
                    elif navigated and url_failed_this_page:
                        url_plan_disabled = True
            elif not navigated:
                detail = detail or "click_fallback_missing"
                context.add_error("pagination_execution", detail, page_index=index, method="click")

            if navigated:
                context.add_decision(
                    "pagination_execution",
                    "action_accepted",
                    detail,
                    page_index=index,
                    method=used_method,
                    evidence=action_evidence,
                )
                break

            end_reason = _end_reason_from_failure(detail, disabled, successful_moves)
            if end_reason:
                stop_reason = end_reason
                detail = end_reason
                context.add_decision(
                    "pagination_execution",
                    "stop",
                    end_reason,
                    page_index=index,
                    evidence=action_evidence,
                )
                context.add_error("pagination_end_detected", end_reason, page_index=index)
                break

            repair_limit = _repair_attempt_limit(successful_moves, detail)
            if repair_attempts >= repair_limit:
                stop_reason = "navigation_failed" if successful_moves <= 0 else "pagination_end_unconfirmed"
                context.add_decision(
                    "pagination_execution",
                    "stop",
                    stop_reason,
                    page_index=index,
                    detail=detail,
                    repair_attempts=repair_attempts,
                    repair_limit=repair_limit,
                )
                break

            repair_attempts += 1
            log_event(
                logger,
                "info",
                "pagination_repair_started",
                domain="job_pagination",
                page_url=page.url,
                page_index=index,
                attempt=repair_attempts,
                repair_limit=repair_limit,
                detail=detail,
            )
            reduced_html = await extract_reduced_html_from_page(page)
            repair = analyse_pagination_repair(
                page_url=page.url,
                original_navigation=navigation,
                target_page=index,
                errors=context.navigation_errors[-10:],
                reduced_html=reduced_html,
            )
            repairs.append({"page_index": index, "attempt": repair_attempts, "repair": repair})
            log_event(
                logger,
                "info",
                "pagination_repair_completed",
                domain="job_pagination",
                page_url=page.url,
                page_index=index,
                attempt=repair_attempts,
                can_repair=repair.get("can_repair"),
                is_end_of_pagination=repair.get("is_end_of_pagination"),
                reason=repair.get("reason") or repair.get("stop_reason") or repair.get("error"),
            )

            repaired_navigation = repair.get("navigation") or {}
            if _repair_says_end(repair):
                stop_reason = repair.get("stop_reason") or repair.get("reason") or "end_of_pagination"
                detail = str(stop_reason)
                context.add_error("pagination_end_detected", detail, page_index=index, attempt=repair_attempts)
                break

            if not repair.get("can_repair") or repaired_navigation.get("method") == "none":
                detail = repair.get("reason") or repair.get("error") or detail or "repair_failed"
                context.add_error("pagination_repair", detail, page_index=index, attempt=repair_attempts)
                continue

            navigation = repaired_navigation
            url_plan, click_plan, max_pages, repaired_url_value = _navigation_parts(navigation, page.url)
            url_value = repaired_url_value or url_value
            url_plan_disabled = False
            navigation_repaired = True

        if not navigated:
            context.add_error("pagination_execution", detail or "navigation_failed", page_index=index)
            break

        context.set_progress("pagination_extracting_page_jobs", page.url, index)
        extraction = await extractor.extract_current_page(page, context, used_method or "pagination")
        url_changed = bool((action_evidence or {}).get("url_changed"))
        report = {
            "page_index": index,
            "url": page.url,
            "new_jobs": extraction.get("new_jobs", 0),
            "total_jobs": len(context.jobs),
            "source": used_method,
            "detail": detail,
            "action_outcome": classify_action_outcome(
                "",
                new_jobs=int(extraction.get("new_jobs") or 0),
                url_changed=url_changed,
            ),
            "action_evidence": action_evidence,
        }
        reports.append(report)
        log_event(
            logger,
            "info",
            "pagination_page_extracted",
            domain="job_pagination",
            page_url=page.url,
            page_index=index,
            new_jobs=report["new_jobs"],
            total_jobs=report["total_jobs"],
            source=used_method,
        )

        if int(extraction.get("new_jobs") or 0) <= 0:
            stop_reason = "no_new_jobs"
            report["action_outcome"] = classify_action_outcome(stop_reason, url_changed=url_changed)
            break

        successful_moves += 1
        url_value = next_value_after_current_url_value(url_plan, url_value)

    result = {
        "reports": reports,
        "repairs": repairs,
        "repaired_navigation": navigation_repaired,
        "final_navigation": navigation,
        "successful_moves": successful_moves,
        "new_pages_extracted": len(reports),
        "stop_reason": stop_reason,
        "outcome": classify_action_outcome(stop_reason),
    }
    context.pagination_runs.append(result)
    log_event(
        logger,
        "info",
        "pagination_execution_completed",
        domain="job_pagination",
        page_url=page.url,
        successful_moves=successful_moves,
        new_pages_extracted=len(reports),
        stop_reason=stop_reason,
        outcome=result["outcome"],
    )
    return result
