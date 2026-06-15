import re
from typing import  Optional
from urllib.parse import urlparse


from utils.logging import setup_logger



# Configure logging
logger = setup_logger(__name__)


# =============================================================================
# URL Filtering Utilities
# =============================================================================


class URLFilter:
    DEFAULT_JOB_KEYWORDS = frozenset({
        "job", "jobs", "career", "careers",
        "vacancy", "vacancies", "opportunity", "opportunities",
        "hiring", "recruit", "recruitment",
        "position", "positions", "opening", "openings",
        "join", "apply", "application", "talent",
        "team", "work", "working", "people", "peoples", "about"
    })

    SKIP_EXTENSIONS = frozenset({
        ".pdf", ".doc", ".docx", ".xls", ".xlsx",
        ".ppt", ".pptx", ".zip", ".rar", ".7z",
        ".png", ".jpg", ".jpeg", ".gif", ".svg",
    })

    COMMON_JOB_PATHS = [
        "/careers",
        "/jobs",
        "/careers/",
        "/jobs/",
        "/work-with-us",
        "/join-us",
        "/join-our-team",
        "/opportunities",
        "/vacancies",
        "/openings",
        "/hiring",
        "/employment",
        "/career",
        "/job",
        "/work",
        "/about/careers",
        "/about/jobs",
        "/company/careers",
        "/en/careers",
        "/en/jobs",
    ]

    @classmethod
    def filter_web_pages_only(cls, urls: list[str]) -> list[str]:
        logger.debug(
            "Filtering web pages only",
            extra={"input_count": len(urls)},
        )
        filtered = []
        skipped_count = 0
        for url in urls:
            url_lower = url.lower().split("?")[0]
            if not any(url_lower.endswith(ext) for ext in cls.SKIP_EXTENSIONS):
                filtered.append(url)
            else:
                skipped_count += 1
                logger.debug(
                    "URL skipped due to extension",
                    extra={"url": url},
                )
        
        logger.debug(
            "Web pages filtering completed",
            extra={
                "input_count": len(urls),
                "output_count": len(filtered),
                "skipped_count": skipped_count,
            },
        )
        return filtered

    @staticmethod
    def filter_by_domain(urls: list[str], domain: str) -> list[str]:
        logger.debug(
            "Filtering URLs by domain",
            extra={"input_count": len(urls), "domain": domain},
        )
        
        normalized_domain = URLFilter._normalized_hostname(domain)
        if not normalized_domain:
            return []

        filtered = []
        for url in urls:
            try:
                parsed = urlparse(url if "://" in url else f"https://{url}")
                url_domain = URLFilter._normalized_hostname(url)

                if url_domain == normalized_domain or url_domain.endswith(f".{normalized_domain}"):
                    if url_domain == normalized_domain:
                        if (parsed.path and parsed.path != "/") or parsed.query or parsed.fragment:
                            filtered.append(url)
                            logger.debug(
                                "URL matched domain",
                                extra={"url": url, "domain": normalized_domain},
                            )
                    else:
                        filtered.append(url)
                        logger.debug(
                            "URL matched subdomain",
                            extra={"url": url, "url_domain": url_domain, "domain": normalized_domain},
                        )
            except Exception as e:
                logger.debug(
                    "Failed to parse URL for domain filtering",
                    extra={"url": url, "error": str(e)},
                )
                continue

        logger.debug(
            "Domain filtering completed",
            extra={
                "input_count": len(urls),
                "output_count": len(filtered),
                "domain": normalized_domain,
            },
        )
        return filtered

    @staticmethod
    def _normalized_hostname(value: str) -> str:
        parsed = urlparse(value if "://" in value else f"https://{value}")
        return (parsed.hostname or "").lower().removeprefix("www.")

    @classmethod
    def filter_job_urls(
        cls,
        urls: list[str],
        include_keywords: Optional[set[str]] = None,
    ) -> list[str]:
        keywords = include_keywords or cls.DEFAULT_JOB_KEYWORDS
        logger.debug(
            "Filtering job URLs",
            extra={
                "input_count": len(urls),
                "keywords_count": len(keywords),
            },
        )
        scored = []

        for url in urls:
            try:
                url_lower = url.lower()
                score = sum(
                    1 for kw in keywords
                    if re.search(rf"\b{re.escape(kw)}\b", url_lower)
                )
                if score > 0:
                    scored.append((url, score))
                    logger.debug(
                        "URL matched job keywords",
                        extra={"url": url, "score": score},
                    )
            except Exception as e:
                logger.debug(
                    "Failed to score URL",
                    extra={"url": url, "error": str(e)},
                )
                continue

        scored.sort(key=lambda x: x[1], reverse=True)
        result = [url for url, _ in scored]
        
        logger.debug(
            "Job URL filtering completed",
            extra={
                "input_count": len(urls),
                "output_count": len(result),
                "top_score": scored[0][1] if scored else 0,
            },
        )
        return result



    @classmethod
    def is_recruitment_domain_shift(
        cls,
        original_domain: str,
        new_url: str,
    ) -> tuple[bool, str]:
        """
        Check if a domain change qualifies as Category B:
        Recruitment Related Domain Shift.

        Rule: If the original company name appears anywhere in the new domain
        (subdomain, root, or recruitment-hosted), treat it as a recruitment shift.

        Returns:
            tuple[bool, str]: (is_recruitment_shift, reason)
                - True  → Continue processing (Category B)
                - False → Company name absent, likely corporate replacement (Category A)
        """
        try:
            original_domain = original_domain.replace("www.", "").lower().strip("/")
            parsed = urlparse(new_url if "://" in new_url else f"https://{new_url}")
            new_netloc = parsed.netloc.replace("www.", "").lower()

            # Extract company root name from original domain (strip TLD)
            # e.g. "abc" from "abc.com" or "abc.co.uk"
            company_name = original_domain.split(".")[0]

            logger.debug(
                "Checking recruitment domain shift",
                extra={
                    "original_domain": original_domain,
                    "new_netloc": new_netloc,
                    "company_name": company_name,
                },
            )

            # Core rule: company name must appear somewhere in the new domain
            if company_name in new_netloc:
                reason = (
                    f"Company name '{company_name}' found in new domain '{new_netloc}' "
                    f"— treating as recruitment-related domain shift"
                )
                logger.info(reason)
                return True, reason

            reason = (
                f"Company name '{company_name}' not found in new domain '{new_netloc}' "
                f"— likely corporate replacement (Category A)"
            )
            logger.debug(reason)
            return False, reason

        except Exception as e:
            logger.error(
                "Error checking recruitment domain shift",
                extra={"original_domain": original_domain, "new_url": new_url, "error": str(e)},
            )
            return False, f"Error during check: {str(e)}"
        
        
        
