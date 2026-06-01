import random
import asyncio
from playwright.async_api import Page
from utils.logging import setup_logger

logger = setup_logger(__name__)


SEARCH_ENGINE_DOMAINS = frozenset({
    "google.com", "google.co", "gstatic.com", "youtube.com",
    "duckduckgo.com", "accounts.google", "policies.google",
    "support.google", "webcache.googleusercontent", "translate.google",
})

RESULT_SELECTORS = [
    "div.g a[href]",
    "div.yuRUbf a[href]",
    "div[data-sokoban-container] a[href]",
    "a[jsname][href]",
    "h3 a[href]",
    "div#search a[href]",
]

COOKIE_SELECTORS = [
    "#L2AGLb", "#W0wltc",
    "button:has-text('Accept all')",
    "button:has-text('Accept All')",
    "button:has-text('I agree')",
    "button:has-text('Reject all')",
    "button:has-text('Reject All')",
    "button[aria-label*='Accept']",
    "button[aria-label*='Reject']",
    "form[action*='consent'] button",
    "div[role='dialog'] button",
]


def _is_search_engine_url(url: str) -> bool:
    url_lower = url.lower()
    return any(domain in url_lower for domain in SEARCH_ENGINE_DOMAINS)


async def _handle_cookie_popup(page) -> None:
    logger.debug("Checking for Google cookie consent popup")

    try:
        consent_frame = page.frame_locator("iframe[src*='consent']")
        for selector in ["#L2AGLb", "button:has-text('Accept')", "button:has-text('Reject')"]:
            try:
                btn = consent_frame.locator(selector).first
                if await btn.is_visible(timeout=1000):
                    logger.info(
                        "Cookie consent popup detected in iframe, dismissing",
                        extra={"selector": selector},
                    )
                    await asyncio.sleep(random.uniform(0.8, 1.5))
                    await btn.click()
                    await asyncio.sleep(random.uniform(0.5, 1.0))
                    logger.debug("Cookie consent popup dismissed via iframe")
                    return
            except Exception:
                continue
    except Exception:
        pass

    for selector in COOKIE_SELECTORS:
        try:
            btn = page.locator(selector).first
            if await btn.is_visible(timeout=800):
                logger.info(
                    "Cookie consent popup detected on page, dismissing",
                    extra={"selector": selector},
                )
                await asyncio.sleep(random.uniform(0.6, 1.2))
                await btn.click()
                await asyncio.sleep(random.uniform(0.5, 1.0))
                logger.debug("Cookie consent popup dismissed")
                return
        except Exception:
            continue

    logger.debug("No cookie consent popup detected")


async def _extract_results(page) -> list[str]:
    logger.info("Extracting search result URLs from page")

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
                        if href and href.startswith("http") and not _is_search_engine_url(href):
                            urls.append(href)
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

    logger.info("No results via primary selectors, falling back to JS evaluation")
    try:
        urls = await page.evaluate("""
            () => {
                const searchEngineDomains = [
                    'google.com','google.co','gstatic.com','youtube.com',
                    'duckduckgo.com','accounts.google','policies.google',
                    'support.google','webcache.googleusercontent','translate.google'
                ];
                const urls = [];
                for (const link of document.querySelectorAll('a[href]')) {
                    const href = link.href;
                    const text = link.innerText?.trim() || '';
                    if (href && href.startsWith('http') && text.length > 3) {
                        const blocked = searchEngineDomains.some(d => href.toLowerCase().includes(d));
                        if (!blocked) urls.push(href);
                    }
                }
                return [...new Set(urls)];
            }
        """)
        logger.info("JS fallback extraction complete", extra={"urls_found": len(urls or [])})
        return urls or []
    except Exception as e:
        logger.warning("JS fallback extraction failed", extra={"error": str(e)})
        return []


async def google_search_node(page: Page, query: str) -> dict:
    """
    Search Google using a Playwright browser with human-like behaviour.

    Args:
        page:  Playwright Page instance
        query: Search query string

    Returns:
        {"success": bool, "results": list[str], "error": str | None}
    """
    logger.info("Starting Google browser search", extra={"query": query})

    try:
        logger.debug("Setting random viewport size")
        await page.set_viewport_size({
            "width": random.randint(1200, 1920),
            "height": random.randint(800, 1080),
        })

        logger.info("Navigating to Google", extra={"url": "https://www.google.com/"})
        await page.goto("https://www.google.com/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(0.5, 1.5))

        content = await page.content()
        if "captcha" in content.lower() or "unusual traffic" in content.lower():
            msg = "Google returned a captcha or unusual traffic page"
            logger.error(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        await _handle_cookie_popup(page)
        await asyncio.sleep(random.uniform(0.3, 0.8))

        logger.info("Searching for Google search box")
        search_box = None
        for selector in ['textarea[name="q"]', 'input[name="q"]', '#APjFqb']:
            try:
                locator = page.locator(selector).first
                if await locator.is_visible(timeout=2000):
                    logger.debug("Search box found", extra={"selector": selector})
                    search_box = locator
                    break
            except Exception:
                continue

        if not search_box:
            msg = "Google search box not found — page may be blocked or layout changed"
            logger.error(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        logger.info("Typing query into search box", extra={"query": query})
        await search_box.click()
        await asyncio.sleep(random.uniform(0.3, 0.6))
        for char in query:
            await search_box.press(char)
            await asyncio.sleep(random.randint(50, 150) / 1000)

        await asyncio.sleep(random.uniform(0.5, 1.0))
        await search_box.press("Enter")
        logger.info("Search submitted, waiting for results page to load")

        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception as e:
            logger.warning("networkidle wait timed out", extra={"error": str(e)})

        await asyncio.sleep(random.uniform(1.0, 2.0))

        content = await page.content()
        if "captcha" in content.lower() or "unusual traffic" in content.lower():
            msg = "Google returned a captcha after search submission"
            logger.error(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        results = await _extract_results(page)

        seen = set()
        deduped = []
        for url in results:
            if url not in seen:
                seen.add(url)
                deduped.append(url)

        logger.info(
            "Deduplication complete",
            extra={"raw_count": len(results), "deduped_count": len(deduped)},
        )

        if not deduped:
            msg = "Google returned no results — possible captcha or layout change"
            logger.warning(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        logger.info(
            "Google browser search completed successfully",
            extra={"query": query, "results_found": len(deduped)},
        )
        return {"success": True, "results": deduped, "error": None}

    except Exception as e:
        msg = f"Unexpected error during Google search: {str(e)}"
        logger.error(msg, extra={"query": query, "error": str(e)})
        return {"success": False, "results": [], "error": msg}