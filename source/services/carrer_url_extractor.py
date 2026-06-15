from utils.logging import setup_logger
from playwright.async_api import Page
from utils.domain_name_filters import URLFilter
from services.flow_safety import extract_base_domain
from services.navigation import handle_security_interstitial
from utils import tld
from urllib.parse import urlparse, urlunparse
import asyncio

# Import the three search engine nodes — do NOT modify them
from nodes.search_engine_node.duckduckgo_browser_search_node import duckduckgo_browser_search_node
from nodes.search_engine_node.duckduckgo_search_node import duckduckgo_search_node
from nodes.search_engine_node.google_search_node import google_search_node


logger = setup_logger(__name__)


class UrlExtractor:
    def __init__(self, page: Page):
        self._page = page
        logger.debug("UrlExtractor initialized")

    # -------------------------------------------------------------------------
    # Public: domain-based discovery (unchanged)
    # -------------------------------------------------------------------------

    async def discover_job_urls_from_domain(
        self,
        domain: str,
        try_common_paths: bool = False,
        extract_from_homepage: bool = True,
        filter_domain: str | None = None,
    ) -> dict:
        logger.info(
            "Starting job URL discovery from domain",
            extra={
                "domain": domain,
                "try_common_paths": try_common_paths,
                "extract_from_homepage": extract_from_homepage,
            },
        )
        discovered_urls: set[str] = set()
        base_url = f"https://{domain.replace('https://', '').replace('http://', '').strip('/')}"
        logger.debug("Base URL constructed", extra={"base_url": base_url})

        if extract_from_homepage:
            logger.debug("Extracting URLs from homepage")
            response_value = await self._extract_urls_from_page(base_url)
            homepage_urls = response_value.get("result", [])
            if not homepage_urls:
                return response_value
            discovered_urls.update(homepage_urls)
            logger.debug("Homepage URLs extracted", extra={"urls_found": len(homepage_urls)})

        all_urls = list(discovered_urls)
        
        domain_filtered = URLFilter.filter_by_domain(all_urls, filter_domain or domain)
        web_filtered = URLFilter.filter_web_pages_only(domain_filtered)
        job_filtered = URLFilter.filter_job_urls(web_filtered)

        logger.info(
            "Job URL discovery completed",
            extra={
                "domain": domain,
                "total_discovered": len(all_urls),
                "domain_filtered": len(domain_filtered),
                "web_filtered": len(web_filtered),
                "job_filtered": len(job_filtered),
            },
        )
        response_value["result"] = job_filtered
        response_value["domain"] = domain
        response_value["meta_data"]["job_urls"] = len(job_filtered)
        response_value["meta_data"]["all_urls"] = all_urls
        return response_value

    # -------------------------------------------------------------------------
    # Public: search with engine fallback chain
    # -------------------------------------------------------------------------

    async def search_duckduckgo(self, query: str, domain: str) -> dict:
        """
        Search for job URLs using a three-engine fallback chain:
          1. DuckDuckGo browser  (duckduckgo_browser_search_node)
          2. Google browser      (google_search_node)
          3. DuckDuckGo HTTP     (duckduckgo_search_node)

        Each engine node is imported as-is; this method only wires them together,
        applies domain/job URL filtering, and normalises the return value.

        Returns the same dict shape as the original search_duckduckgo.
        """
        logger.info(
            "Starting search with engine fallback chain",
            extra={"query": query, "domain": domain},
        )

        # Minimal state expected by all three node functions
        base_state = {
            "search_query": query,
            "playwright_page": self._page,   # HTTP node ignores this key safely
        }

        # ------------------------------------------------------------------
        # Engine 1: DuckDuckGo browser
        # ------------------------------------------------------------------
        logger.debug("Trying engine 1: DuckDuckGo browser")
        result_state = await duckduckgo_browser_search_node(self._page, query)
        raw_urls: list[str] | None = result_state.get("results")
        
        if not raw_urls:
            logger.warning(
                "DuckDuckGo browser search failed, falling back to Google",
                extra={"error": result_state.get("search_error")},
            )

            # ----------------------------------------------------------------
            # Engine 2: Google browser
            # ----------------------------------------------------------------
            logger.debug("Trying engine 2: Google browser")
            result_state = await google_search_node(self._page, query)
            raw_urls = result_state.get("results")

            if not raw_urls:
                logger.warning(
                    "Google browser search failed, falling back to DuckDuckGo HTTP",
                    extra={"error": result_state.get("search_error")},
                )

                # ------------------------------------------------------------
                # Engine 3: DuckDuckGo HTTP (no browser)
                # ------------------------------------------------------------
                logger.debug("Trying engine 3: DuckDuckGo HTTP")
                result_state = await duckduckgo_search_node(query)
                raw_urls = result_state.get("results")

                if not raw_urls:
                    logger.error(
                        "All three search engines failed",
                        extra={"error": result_state.get("search_error")},
                    )
                    return {
                        "success": False,
                        "error": result_state.get("search_error", "All search engines failed"),
                        "status": "All search engines exhausted",
                        "result": [],
                        "meta_data": {"original_domain": domain, "job_urls": 0},
                    }

        # ------------------------------------------------------------------
        # Filter raw URLs down to job URLs on the target domain
        # ------------------------------------------------------------------

        domain_filtered = URLFilter.filter_by_domain(raw_urls, domain)
        
        web_filtered    = URLFilter.filter_web_pages_only(domain_filtered)
        job_filtered    = URLFilter.filter_job_urls(web_filtered)

        logger.info(
            "Job URL discovery completed via search engine chain",
            extra={
                "domain": domain,
                "engine_used": result_state.get("current_step", "unknown"),
                "total_raw": len(raw_urls),
                "domain_filtered": len(domain_filtered),
                "web_filtered": len(web_filtered),
                "job_filtered": len(job_filtered),
            },
        )

        return {
            "success": True,
            "result": job_filtered,
            "meta_data": {
                "original_domain": domain,
                "job_urls": len(job_filtered),
                "engine_used": result_state.get("current_step", "unknown"),
                "total_raw_results": len(raw_urls),
                "all_urls": raw_urls
            },
        }

    # -------------------------------------------------------------------------
    # Helpers (unchanged)
    # -------------------------------------------------------------------------

    def normalize_domain(self, url: str) -> str:
        ext = tld.extract(url)
        return f"{ext.domain}.{ext.suffix}".lower()

    def _normalize_url_for_same_site_compare(self, raw_url: str | None) -> str:
        parsed = urlparse(str(raw_url or "").strip())
        scheme = parsed.scheme.lower() or "https"
        hostname = (parsed.hostname or "").lower().removeprefix("www.")
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/") or "/"
        normalized = parsed._replace(
            scheme=scheme,
            netloc=hostname,
            path=path,
            params="",
            query="",
            fragment="",
        )
        return urlunparse(normalized)

    def _is_same_site_canonical_redirect(self, original_url: str, final_url: str) -> bool:
        original_domain = self.normalize_domain(urlparse(original_url).netloc.lower())
        final_domain = self.normalize_domain(urlparse(final_url).netloc.lower())
        if original_domain != final_domain:
            return False
        return (
            self._normalize_url_for_same_site_compare(original_url)
            == self._normalize_url_for_same_site_compare(final_url)
        )

    def _is_same_base_domain(self, original_url: str | None, final_url: str | None) -> bool:
        original_domain = extract_base_domain(original_url)
        final_domain = extract_base_domain(final_url)
        return bool(original_domain and final_domain and original_domain == final_domain)

    def _is_err_aborted(self, exc: Exception) -> bool:
        return "ERR_ABORTED" in str(exc)

    async def _extract_urls_from_page(self, url: str) -> dict:
        logger.debug("Extracting URLs from page", extra={"url": url})
        for i in range(3):
            try:
                await self._page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await handle_security_interstitial(self._page, url)
                await asyncio.sleep((0.5 * i) + 0.5)

                original_domain = self.normalize_domain(urlparse(url).netloc.lower())
                final_url       = self._page.url
                final_domain    = self.normalize_domain(urlparse(final_url).netloc.lower())
                redirected      = original_domain != final_domain

                logger.debug(
                    "Page loaded",
                    extra={
                        "original_url": url,
                        "final_url": final_url,
                        "redirected": redirected,
                        "attempt": i + 1,
                    },
                )

                resp = await self._extract_urls_from_current_page()
                if not resp.get("success"):
                    raise RuntimeError(
                        f"(status={resp.get('status')}, body={resp.get('error')})"
                    )
                break

            except Exception as e:
                current_url = self._page.url if self._page is not None else ""
                same_site_canonical_redirect = (
                    current_url and self._is_same_site_canonical_redirect(url, current_url)
                )
                same_base_domain_recovery = (
                    current_url
                    and self._is_err_aborted(e)
                    and self._is_same_base_domain(url, current_url)
                )
                if same_site_canonical_redirect or same_base_domain_recovery:
                    original_domain = self.normalize_domain(urlparse(url).netloc.lower())
                    final_url = current_url
                    final_domain = self.normalize_domain(urlparse(final_url).netloc.lower())
                    redirected = original_domain != final_domain
                    logger.info(
                        "Recovered from same-site navigation interruption during URL extraction",
                        extra={
                            "original_url": url,
                            "final_url": final_url,
                            "attempt": i + 1,
                            "error": str(e),
                        },
                    )
                    await asyncio.sleep((0.5 * i) + 0.5)
                    resp = await self._extract_urls_from_current_page()
                    if not resp.get("success"):
                        raise RuntimeError(
                            f"(status={resp.get('status')}, body={resp.get('error')})"
                        )
                    break

                logger.warning(
                    "Failed to load page for URL extraction",
                    extra={"url": url, "error": str(e), "attempt": i + 1},
                )
                if self._is_err_aborted(e) and i < 2:
                    await asyncio.sleep(float(i + 1) * 0.5)
                    continue
                if i == 2:
                    return {
                        "error": str(e),
                        "status": f"Failed to load page for URL {url} extraction",
                        "success": False,
                    }

        return {
            "result": resp.get("result", []),
            "meta_data": {
                "redirected": redirected,
                "original_url": url,
                "final_url": final_url,
                "original_domain": original_domain,
                "final_domain": final_domain,
            },
            "success": True,
        }

    async def _extract_urls_from_current_page(self) -> dict:
        logger.debug("Extracting URLs from current page")
        try:
            urls = await self._page.evaluate(
                """
                () => {
                    const urls = [];
                    const links = document.querySelectorAll('a[href]');
                    links.forEach(link => {
                        const href = link.href;
                        if (href && href.startsWith('http')) {
                            urls.push(href);
                        }
                    });
                    return [...new Set(urls)];
                }
                """
            )
            result = urls or []
            logger.debug("URLs extracted from current page", extra={"urls_count": len(result)})
            return {"result": result, "success": True}

        except Exception as e:
            logger.warning("Failed to extract URLs from current page", extra={"error": str(e)})
            return {
                "status": "Failed to extract URLs from current page",
                "error": str(e),
                "success": False,
            }
                    
    async def _extract_career_urls_from_page(self, base_domain: str) -> dict:
        """
        Extracts URLs that are likely career/jobs pages based on:
        1. The link text contains career-related keywords
        2. The URL itself contains career-related path keywords

        Only returns URLs whose domain differs from base_domain.
        """
        logger.debug(
            "Extracting career URLs from current page",
            extra={"base_domain": base_domain}
        )
        try:
            raw = await self._page.evaluate(
                """
                () => {
                    const CAREER_TEXT_PATTERNS = [
                        /\\bcareers?\\b/i,
                        /\\bjobs?\\b/i,
                        /\\bwork with us\\b/i,
                        /\\bjoin us\\b/i,
                        /\\bjoin our team\\b/i,
                        /\\bwe('re| are) hiring\\b/i,
                        /\\bopen (roles?|positions?|vacancies)\\b/i,
                        /\\bopportunities\\b/i,
                        /\\bvacancies\\b/i,
                        /\\bemployment\\b/i,
                    ];

                    const CAREER_URL_PATTERNS = [
                        /\\/careers?(\\/|$|\\?)/i,
                        /\\/jobs?(\\/|$|\\?)/i,
                        /\\/vacancies(\\/|$|\\?)/i,
                        /\\/opportunities(\\/|$|\\?)/i,
                        /\\/work-with-us(\\/|$|\\?)/i,
                        /\\/join-us(\\/|$|\\?)/i,
                        /\\/employment(\\/|$|\\?)/i,
                        /\\/hiring(\\/|$|\\?)/i,
                    ];

                    const results = [];
                    const seen = new Set();

                    document.querySelectorAll('a[href]').forEach(link => {
                        const href = link.href;
                        if (!href || !href.startsWith('http') || seen.has(href)) return;

                        const linkText = (link.innerText || link.textContent || '').trim();
                        const matchedByText = CAREER_TEXT_PATTERNS.some(p => p.test(linkText));
                        const matchedByUrl  = CAREER_URL_PATTERNS.some(p => p.test(href));

                        if (matchedByText || matchedByUrl) {
                            seen.add(href);
                            results.push({
                                url: href,
                                link_text: linkText,
                                matched_by: matchedByText && matchedByUrl
                                    ? 'both'
                                    : matchedByText ? 'text' : 'url',
                            });
                        }
                    });

                    return results;
                }
                """
            )

            # Filter out any URL whose domain matches base_domain
            base_ext = tld.extract(base_domain)
            base_root = f"{base_ext.domain}.{base_ext.suffix}".lower()

            external_career_urls = []
            for item in (raw or []):
                href_ext = tld.extract(item["url"])
                href_root = f"{href_ext.domain}.{href_ext.suffix}".lower()
                if href_root != base_root:
                    external_career_urls.append(item)

            logger.info(
                "Career URL extraction completed",
                extra={
                    "base_domain": base_domain,
                    "total_matched": len(raw or []),
                    "external_only": len(external_career_urls),
                },
            )

            return {
                "success": True,
                "result": external_career_urls,
                "meta_data": {
                    "base_domain": base_domain,
                    "total_matched": len(raw or []),
                    "external_career_urls": len(external_career_urls),
                },
            }

        except Exception as e:
            logger.warning(
                "Failed to extract career URLs from page",
                extra={"error": str(e)}
            )
            return {
                "success": False,
                "error": str(e),
                "status": "Failed to extract career URLs from page",
                "result": [],
            }
