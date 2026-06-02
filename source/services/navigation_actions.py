from __future__ import annotations

import asyncio
import re
from urllib.parse import urljoin

try:
    from playwright.async_api import Page
except Exception:  # pragma: no cover - handled gracefully at runtime
    Page = None


def _is_download_start_error(exc: Exception) -> bool:
    message = str(exc)
    return "Download is starting" in message or "download is starting" in message


async def _goto_with_retry(
    page: Page,
    resolved_url: str,
    post_navigation_delay_ms: int = 0,
    max_attempts: int = 3,
) -> tuple[str, str | None]:
    last_error = ""

    for attempt in range(1, max_attempts + 1):
        try:
            await page.goto(resolved_url, wait_until="domcontentloaded", timeout=60_000)
            if post_navigation_delay_ms > 0:
                await page.wait_for_timeout(post_navigation_delay_ms)
            return "navigated", None
        except Exception as exc:
            last_error = str(exc)
            if _is_download_start_error(exc):
                if attempt < max_attempts:
                    await asyncio.sleep(float(attempt))
                    continue
                return "download_started", last_error
            if attempt < max_attempts:
                await asyncio.sleep(float(attempt))

    return "action_failed", last_error


async def follow_navigation_target(
    page: Page | None,
    target_url: str | None,
    target_button: str | None,
    post_navigation_delay_ms: int = 0,
) -> tuple[str, str | None, str | None]:
    if page is None:
        return "action_skipped", None, "Navigation action requires an attached Playwright page"

    if target_url:
        resolved_url = urljoin(page.url, target_url)
        status, error = await _goto_with_retry(
            page,
            resolved_url,
            post_navigation_delay_ms=post_navigation_delay_ms,
        )
        return status, resolved_url if status in {"navigated", "download_started"} else None, error

    if not target_button:
        return "idle", None, None

    target_pattern = re.compile(re.escape(target_button), re.IGNORECASE)

    last_error = ""
    locators = [
        page.get_by_role("button", name=target_pattern).first,
        page.get_by_role("link", name=target_pattern).first,
        page.get_by_text(target_pattern).first,
    ]
    for locator in locators:
        try:
            if await locator.count() == 0:
                continue
            for attempt in range(1, 4):
                try:
                    await locator.scroll_into_view_if_needed()
                    await locator.click(timeout=10_000)
                    await page.wait_for_load_state("domcontentloaded", timeout=30_000)
                    if post_navigation_delay_ms > 0:
                        await page.wait_for_timeout(post_navigation_delay_ms)
                    return "clicked", page.url, None
                except Exception as exc:
                    if _is_download_start_error(exc):
                        if attempt < 3:
                            await asyncio.sleep(float(attempt))
                            continue
                        return "download_started", page.url, str(exc)
                    last_error = str(exc)
                    if attempt < 3:
                        await asyncio.sleep(float(attempt))
        except Exception as exc:
            if _is_download_start_error(exc):
                return "download_started", page.url, str(exc)
            last_error = str(exc)

    return "action_failed", None, last_error or f"Could not click navigation target: {target_button}"
