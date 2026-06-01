import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote
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


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://duckduckgo.com/",
}


async def duckduckgo_search_node(query: str) -> dict:
    """
    Search DuckDuckGo via HTTP (no browser needed).

    Args:
        query: Search query string

    Returns:
        {"success": bool, "results": list[str], "error": str | None}
    """
    logger.info("Starting DuckDuckGo HTTP search", extra={"query": query})

    try:
        logger.info(
            "Sending HTTP request to DuckDuckGo",
            extra={"url": "https://duckduckgo.com/html/", "query": query},
        )

        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(
                "https://duckduckgo.com/html/",
                params={"q": query},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    msg = f"DuckDuckGo returned HTTP {resp.status} — possible block or captcha"
                    logger.error(msg, extra={"query": query, "http_status": resp.status})
                    return {"success": False, "results": [], "error": msg}

                logger.info(
                    "HTTP response received",
                    extra={"query": query, "http_status": resp.status},
                )
                html = await resp.text()

        if "captcha" in html.lower() or "robot" in html.lower():
            msg = "DuckDuckGo returned a captcha or bot-detection page"
            logger.error(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        logger.debug("Parsing HTML response with BeautifulSoup", extra={"query": query})
        soup = BeautifulSoup(html, "lxml")
        urls: list[str] = []
        for a in soup.select("a.result__a"):
            href = a.get("href")
            real_url = _unwrap_ddg_url(href)
            if real_url and real_url.startswith("http"):
                urls.append(real_url)

        logger.info("Raw URLs extracted from HTML", extra={"query": query, "raw_count": len(urls)})

        seen = set()
        results = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                results.append(url)

        logger.info(
            "Deduplication complete",
            extra={"raw_count": len(urls), "deduped_count": len(results)},
        )

        if not results:
            msg = "DuckDuckGo returned no results — possible captcha or query issue"
            logger.warning(msg, extra={"query": query})
            return {"success": False, "results": [], "error": msg}

        logger.info(
            "DuckDuckGo HTTP search completed successfully",
            extra={"query": query, "results_found": len(results)},
        )
        return {"success": True, "results": results, "error": None}

    except aiohttp.ClientError as e:
        msg = f"Network error during DuckDuckGo HTTP search: {str(e)}"
        logger.error(msg, extra={"query": query, "error": str(e)})
        return {"success": False, "results": [], "error": msg}

    except Exception as e:
        msg = f"Unexpected error during DuckDuckGo HTTP search: {str(e)}"
        logger.error(msg, extra={"query": query, "error": str(e)})
        return {"success": False, "results": [], "error": msg}