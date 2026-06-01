from __future__ import annotations

import asyncio
from urllib.parse import urlparse, urlunparse
from utils.logging import get_logger, log_event
from services.content_extraction import prepare_page_for_extraction

try:
    from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
except Exception:  # pragma: no cover - handled gracefully at runtime
    Page = None
    PlaywrightTimeoutError = TimeoutError

from schemas.agent_state import NavigationResult


WEB_NAVIGATION_SCHEMES = {"", "http", "https"}
logger = get_logger("navigation")
SECURITY_INTERSTITIAL_MARKERS = (
    "this site doesn't support a secure connection",
    "your connection is not private",
    "privacy error",
    "attackers can see and change the information",
    "net::err_ssl",
)
SECURITY_INTERSTITIAL_CONTINUE_SELECTORS = (
    "button:has-text('Continue to site')",
    "button:has-text('Continue')",
    "a:has-text('Continue to site')",
    "a:has-text('Continue')",
    "button:has-text('Proceed')",
    "a:has-text('Proceed')",
    "button:has-text('Advanced')",
    "a:has-text('Advanced')",
)


def _is_web_navigation_url(url: str | None) -> bool:
    normalized = str(url or "").strip()
    if not normalized:
        return False
    return urlparse(normalized).scheme.lower() in WEB_NAVIGATION_SCHEMES


def _is_download_start_error(exc: Exception) -> bool:
    message = str(exc)
    return "Download is starting" in message or "download is starting" in message


def _http_fallback_url(url: str | None) -> str | None:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme.lower() != "https":
        return None
    return urlunparse(parsed._replace(scheme="http"))


async def _get_page_text_snapshot(page: Page) -> str:
    try:
        title = await page.title()
    except Exception:
        title = ""

    try:
        body = await page.locator("body").inner_text(timeout=2_000)
    except Exception:
        body = ""

    return f"{title}\n{body}".lower()


async def handle_security_interstitial(
    page: Page,
    requested_url: str,
    *,
    post_navigation_delay_ms: int = 0,
) -> bool:
    snapshot = await _get_page_text_snapshot(page)
    if not any(marker in snapshot for marker in SECURITY_INTERSTITIAL_MARKERS):
        return False

    log_event(
        logger,
        "warning",
        "security_interstitial_detected url=%s current_url=%s",
        requested_url,
        page.url,
        domain=requested_url,
        url=requested_url,
        current_url=page.url,
    )

    for selector in SECURITY_INTERSTITIAL_CONTINUE_SELECTORS:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            await locator.click(timeout=2_500)
            await page.wait_for_load_state("domcontentloaded", timeout=15_000)
            if post_navigation_delay_ms > 0:
                await page.wait_for_timeout(post_navigation_delay_ms)
            log_event(
                logger,
                "info",
                "security_interstitial_dismissed_by_click url=%s selector=%s final_url=%s",
                requested_url,
                selector,
                page.url,
                domain=requested_url,
                url=requested_url,
                selector=selector,
                final_url=page.url,
            )
            return True
        except Exception:
            continue

    fallback_url = _http_fallback_url(page.url or requested_url)
    if fallback_url:
        try:
            await page.goto(fallback_url, wait_until="domcontentloaded", timeout=60_000)
            if post_navigation_delay_ms > 0:
                await page.wait_for_timeout(post_navigation_delay_ms)
            log_event(
                logger,
                "info",
                "security_interstitial_recovered_with_http_fallback url=%s fallback_url=%s",
                requested_url,
                fallback_url,
                domain=requested_url,
                url=requested_url,
                fallback_url=fallback_url,
            )
            return True
        except Exception as exc:
            log_event(
                logger,
                "warning",
                "security_interstitial_http_fallback_failed url=%s fallback_url=%s error=%s",
                requested_url,
                fallback_url,
                str(exc),
                domain=requested_url,
                url=requested_url,
                fallback_url=fallback_url,
                error=str(exc),
            )

    return False


async def _goto_with_retry(
    page: Page,
    url: str,
    post_navigation_delay_ms: int = 0,
    max_attempts: int = 3,
) -> tuple[str, str | None]:
    last_error = ""
    last_status = "navigation_failed"

    for attempt in range(1, max_attempts + 1):
        try:
            log_event(
                logger,
                "info",
                "navigation_attempt_started url=%s attempt=%s",
                url,
                attempt,
                domain=url,
                url=url,
                attempt=attempt,
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            await handle_security_interstitial(
                page,
                url,
                post_navigation_delay_ms=post_navigation_delay_ms,
            )
            if post_navigation_delay_ms > 0:
                await page.wait_for_timeout(post_navigation_delay_ms)
            log_event(
                logger,
                "info",
                "navigation_attempt_succeeded url=%s attempt=%s",
                url,
                attempt,
                domain=url,
                url=url,
                attempt=attempt,
            )
            return "navigated", None
        except PlaywrightTimeoutError as exc:
            last_status = "navigation_timeout"
            last_error = str(exc)
            log_event(
                logger,
                "warning",
                "navigation_attempt_timeout url=%s attempt=%s error=%s",
                url,
                attempt,
                last_error,
                domain=url,
                url=url,
                attempt=attempt,
                error=last_error,
            )
        except Exception as exc:
            last_status = "navigation_failed"
            last_error = str(exc)
            log_event(
                logger,
                "warning",
                "navigation_attempt_failed url=%s attempt=%s error=%s",
                url,
                attempt,
                last_error,
                domain=url,
                url=url,
                attempt=attempt,
                error=last_error,
            )
            if _is_download_start_error(exc):
                if attempt < max_attempts:
                    await asyncio.sleep(float(attempt))
                    continue
                return "navigation_download", last_error

            if "ERR_ABORTED" in str(exc) and attempt < max_attempts:
                await asyncio.sleep(float(attempt) * 0.5)
                continue

        if attempt < max_attempts:
            await asyncio.sleep(float(attempt))

    return last_status, last_error

async def navigate_urls(
    page: Page | None,
    agent_index: int,
    tab_handle: str | None,
    urls: list[str],
) -> list[NavigationResult]:
    if page is None:
        log_event(
            logger,
            "warning",
            "navigation_batch_skipped_no_page url_count=%s",
            len(urls),
            domain=urls[0] if urls else "unknown",
            agent_index=agent_index,
            url_count=len(urls),
        )
        return [
            {
                "agent_index": agent_index,
                "handle": tab_handle,
                "url": url,
                "status": "navigation_skipped",
                "current_url": None,
                "error": "Navigation requires an attached Playwright page",
            }
            for url in urls
        ]

    if not urls:
        log_event(logger, "info", "navigation_batch_idle", domain="navigation", agent_index=agent_index)
        return [
            {
                "agent_index": agent_index,
                "handle": tab_handle,
                "url": None,
                "status": "idle",
                "current_url": page.url,
                "error": None,
            }
        ]

    results: list[NavigationResult] = []
    for url in urls:
        status, error = await _goto_with_retry(page, url)
        results.append(
            {
                "agent_index": agent_index,
                "handle": tab_handle,
                "url": url,
                "status": status,
                "current_url": page.url if page else None,
                "error": error,
            }
        )

    return results


async def navigate_to_url(
    page: Page | None,
    agent_index: int,
    tab_handle: str | None,
    url: str | None,
    post_navigation_delay_ms: int = 5_000,
) -> NavigationResult:
    if page is None:
        log_event(
            logger,
            "warning",
            "navigation_skipped_no_page url=%s",
            url,
            domain=url or "unknown",
            agent_index=agent_index,
            url=url,
        )
        return {
            "agent_index": agent_index,
            "handle": tab_handle,
            "url": url,
            "status": "navigation_skipped",
            "current_url": None,
            "error": f"Navigation requires an attached Playwright page {url}",
        }

    if not url:
        log_event(logger, "info", "navigation_idle_no_url", domain="navigation", agent_index=agent_index)
        return {
            "agent_index": agent_index,
            "handle": tab_handle,
            "url": None,
            "status": "idle",
            "current_url": page.url,
            "error": None,
        }

    if not _is_web_navigation_url(url):
        log_event(
            logger,
            "warning",
            "navigation_non_web_url url=%s",
            url,
            domain=url,
            agent_index=agent_index,
            url=url,
        )
        return {
            "agent_index": agent_index,
            "handle": tab_handle,
            "url": url,
            "status": "navigation_non_web_url",
            "current_url": page.url,
            "error": f"Navigation target is not a web page URL: {url}",
        }

    status, error = await _goto_with_retry(page, url, post_navigation_delay_ms=post_navigation_delay_ms)
    log_event(
        logger,
        "info",
        "navigation_completed url=%s status=%s",
        url,
        status,
        domain=url,
        agent_index=agent_index,
        url=url,
        status=status,
        error=error,
    )
    if status == "navigated":
        preparation = await prepare_page_for_extraction(page)
        return {
            "agent_index": agent_index,
            "handle": tab_handle,
            "url": url,
            "status": status,
            "current_url": page.url,
            "error": None,
        }
    if status == "navigation_timeout":
        return {
            "agent_index": agent_index,
            "handle": tab_handle,
            "url": url,
            "status": status,
            "current_url": page.url,
            "error": error,
        }
    return {
        "agent_index": agent_index,
        "handle": tab_handle,
        "url": url,
        "status": status,
        "current_url": page.url if page else None,
        "error": error,
    }
