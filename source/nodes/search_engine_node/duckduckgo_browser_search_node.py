import random
import asyncio
from urllib.parse import urlparse, parse_qs, unquote
from playwright.async_api import Page
from utils.logging import setup_logger

logger = setup_logger(__name__)


def _unwrap_ddg_url(href: str) -> str | None:
    if not href:
        return None
    if href.startswith("//"):
        href = "https:" + href
    parsed = urlparse(href)
    if "duckduckgo.com/l/" not in parsed.netloc + parsed.path:
        return href
    qs = parse_qs(parsed.query)
    uddg = qs.get("uddg")
    if not uddg:
        return None
    return unquote(uddg[0])


COOKIE_SELECTORS = [
    "button:has-text('Accept all')",
    "button:has-text('Accept All')",
    "button:has-text('I agree')",
    "button[aria-label*='Accept']",
    "#onetrust-accept-btn-handler",
]

SEARCH_BOX_SELECTORS = [
    'input[name="q"]',
    'textarea[name="q"]',
    "#searchbox_input",
]

RESULT_SELECTORS = [
    'article[data-testid="result"] a[href]',
    "ol.react-results--main a[href]",
    "div.nrn-react-div a[href]",
    "a.result__a[href]",
]


async def _find_search_box(page):
    logger.debug("Searching for search box", extra={"selectors_tried": SEARCH_BOX_SELECTORS})
    for selector in SEARCH_BOX_SELECTORS:
        try:
            locator = page.locator(selector).first
            if await locator.is_visible(timeout=3000):
                logger.debug("Search box found", extra={"selector": selector})
                return locator
        except Exception:
            continue
    logger.debug("No search box found across all selectors")
    return None


async def _handle_cookie_popup(page) -> None:
    logger.debug("Checking for cookie consent popup")
    for selector in COOKIE_SELECTORS:
        try:
            btn = page.locator(selector).first
            if await btn.is_visible(timeout=1500):
                logger.info("Cookie popup detected, dismissing", extra={"selector": selector})
                await asyncio.sleep(random.uniform(0.5, 1.0))
                await btn.click()
                await asyncio.sleep(random.uniform(0.3, 0.8))
                logger.debug("Cookie popup dismissed")
                return
        except Exception:
            continue
    logger.debug("No cookie popup detected")


async def _type_into_search_box(search_box, query: str) -> None:
    """
    Type query character-by-character with human-like delays.
    Each press() is capped at 5 000 ms to avoid the default 30 s hang.
    Falls back to fill() if any keypress times out.
    """
    try:
        for char in query:
            await search_box.press(char, timeout=5000)
            await asyncio.sleep(random.randint(50, 150) / 1000)
            if random.random() < 0.1:
                await asyncio.sleep(random.uniform(0.2, 0.5))
    except Exception as e:
        logger.warning(
            "Character-by-character typing failed, falling back to fill()",
            extra={"error": str(e)},
        )
        await search_box.fill(query, timeout=10000)


async def _extract_results(page) -> list[str]:
    logger.debug("Extracting search result URLs from page")

    for selector in RESULT_SELECTORS:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            if count > 0:
                logger.debug("Result selector matched", extra={"selector": selector, "count": count})
                urls = []
                for i in range(count):
                    try:
                        href = await locator.nth(i).get_attribute("href")
                        real_url = _unwrap_ddg_url(href)
                        if real_url and real_url.startswith("http"):
                            urls.append(real_url)
                    except Exception:
                        continue
                if urls:
                    logger.debug(
                        "URLs extracted via selector",
                        extra={"selector": selector, "urls_found": len(urls)},
                    )
                    return urls
        except Exception:
            continue

    logger.debug("No results via primary selectors, falling back to JS evaluation")
    try:
        urls = await page.evaluate("""
            () => {
                const urls = [];
                for (const link of document.querySelectorAll('a[href]')) {
                    const href = link.href;
                    if (href && href.startsWith('http') && !href.includes('duckduckgo.com')) {
                        urls.push(href);
                    }
                }
                return [...new Set(urls)];
            }
        """)
        logger.debug("JS fallback extraction complete", extra={"urls_found": len(urls or [])})
        return urls or []
    except Exception as e:
        logger.warning("JS fallback extraction failed", extra={"error": str(e)})
        return []


async def duckduckgo_browser_search_node(page: Page, query: str) -> dict:
    """
    Search DuckDuckGo using a Playwright browser with human-like behaviour.

    Args:
        page:  Playwright Page instance
        query: Search query string

    Returns:
        {"success": bool, "results": list[str], "error": str | None}
    """
    logger.info("Starting DuckDuckGo browser search", extra={"query": query})

    try:
        logger.debug("Navigating to DuckDuckGo", extra={"url": "https://duckduckgo.com/"})
        await page.goto("https://duckduckgo.com/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(0.5, 1.5))

        await _handle_cookie_popup(page)
        await asyncio.sleep(random.uniform(0.3, 0.6))

        search_box = await _find_search_box(page)
        if not search_box:
            msg = "DuckDuckGo search box not found — page is likely blocked or showing captcha"
            logger.error(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        logger.debug("Clicking search box to ensure focus", extra={"query": query})
        await search_box.click(timeout=5000)
        await asyncio.sleep(random.uniform(0.2, 0.5))

        logger.debug("Typing query into search box", extra={"query": query})
        await _type_into_search_box(search_box, query)

        await asyncio.sleep(random.uniform(0.5, 1.0))
        await search_box.press("Enter", timeout=5000)
        logger.debug("Search submitted, waiting for results")

        results_loaded = False
        for selector in ["ol.react-results--main", "div.nrn-react-div", "#react-duckduckhunt"]:
            try:
                await page.locator(selector).wait_for(state="visible", timeout=8000)
                logger.debug("Results container visible", extra={"selector": selector})
                results_loaded = True
                break
            except Exception:
                continue

        if not results_loaded:
            logger.warning(
                "Results container not found via selectors, falling back to networkidle",
                extra={"query": query},
            )
            try:
                await page.wait_for_load_state("networkidle", timeout=8000)
            except Exception as e:
                logger.warning("networkidle wait timed out", extra={"error": str(e)})

        await asyncio.sleep(random.uniform(1.0, 2.0))

        post_search_box = await _find_search_box(page)
        if not post_search_box:
            msg = "DuckDuckGo search box gone after submission — likely captcha triggered"
            logger.error(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        results = await _extract_results(page)

        seen = set()
        deduped = []
        for url in results:
            if url not in seen:
                seen.add(url)
                deduped.append(url)

        logger.debug(
            "Deduplication complete",
            extra={"raw_count": len(results), "deduped_count": len(deduped)},
        )

        if not deduped:
            msg = "DuckDuckGo returned no results"
            logger.warning(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        logger.info(
            "DuckDuckGo browser search completed successfully",
            extra={"query": query, "results_found": len(deduped)},
        )
        return {"success": True, "results": deduped, "error": None}

    except Exception as e:
        msg = f"Unexpected error during DuckDuckGo browser search: {str(e)}"
        logger.error(msg, extra={"query": query, "error": str(e)})
        return {"success": False, "results": [], "error": msg}