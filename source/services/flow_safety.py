from __future__ import annotations

from urllib.parse import urljoin, urlparse, parse_qs
import os
from utils import tld

SKIP_EXTENSIONS = frozenset({
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".zip", ".rar", ".7z",
    ".png", ".jpg", ".jpeg", ".gif", ".svg",
})



WEB_NAVIGATION_SCHEMES = {"", "http", "https"}
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".rtf"}
_BLOCKED_PLATFORMS: dict[str, str] = {
    "linkedin.com": "linkedin",
    "indeed.com": "indeed",
    "facebook.com": "facebook",
    "instagram.com": "instagram",
    "twitter.com": "twitter",
    "x.com": "twitter",
    "tiktok.com": "tiktok",
    "youtube.com": "youtube",
    "pinterest.com": "pinterest",
    "snapchat.com": "snapchat",
    "glassdoor.com": "glassdoor",
    "monster.com": "monster",
    "ziprecruiter.com": "ziprecruiter",
    "reed.co.uk": "reed",
    "totaljobs.com": "totaljobs",
    "cv-library.co.uk": "cv_library",
}




def normalize_navigation_url(target_url: str | None, base_url: str | None) -> str | None:
    normalized_target = str(target_url or "").strip()
    if not normalized_target:
        return None
    normalized_base = str(base_url or "").strip()
    if not normalized_base:
        return normalized_target
    return urljoin(normalized_base, normalized_target)


def is_web_navigation_url(url: str | None) -> bool:
    normalized = str(url or "").strip()
    if not normalized:
        return False
    return urlparse(normalized).scheme.lower() in WEB_NAVIGATION_SCHEMES


def is_email_navigation_url(url: str | None) -> bool:
    return str(url or "").strip().lower().startswith("mailto:")


def detect_external_job_board(url: str | None) -> str | None:
    normalized = str(url or "").strip()
    if not normalized:
        return None
    domain = urlparse(normalized).netloc.lower()
    if "linkedin." in domain:
        return "linkedin"
    if "indeed." in domain:
        return "indeed"
    return None



def has_skip_extension(url: str | None) -> bool:
    if not url:
        return False

    parsed = urlparse(url)

    # 1. Check path extension
    path = parsed.path.lower()
    _, ext = os.path.splitext(path)
    if ext in SKIP_EXTENSIONS:
        return True

    # 2. Check query parameters for filenames
    query_params = parse_qs(parsed.query)

    for values in query_params.values():
        for value in values:
            value = value.lower()
            _, ext = os.path.splitext(value)
            if ext in SKIP_EXTENSIONS:
                return True

    return False


def extract_base_domain(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    hostname = (parsed.netloc or parsed.path or "").strip().lower().removeprefix("www.")
    if not hostname:
        return None

    extracted = tld.extract(hostname)
    if extracted.domain and extracted.suffix:
        return f"{extracted.domain}.{extracted.suffix}".lower()
    return hostname if "." in hostname else None




def _is_external_domain(url: str | None, main_domain: str | None) -> bool:
    target_domain = extract_base_domain(url)
    source_domain = extract_base_domain(main_domain)
    return bool(target_domain and source_domain and target_domain != source_domain)


def extract_domain(url: str) -> str | None:
    try:
        url = url.strip()
        if not url:
            return None
        # Ensure scheme exists so urlparse can identify the hostname
        if "://" not in url:
            url = "https://" + url
        hostname = urlparse(url).hostname or ""
        hostname = hostname.removeprefix("www.")
        return hostname or None
    except Exception:
        return None
    




def detect_blocked_platform(url: str | None) -> str | None:
    if not url:
        return None
    try:
        hostname = urlparse(url).hostname or ""
        hostname = hostname.removeprefix("www.").lower()
        for domain, platform in _BLOCKED_PLATFORMS.items():
            if hostname == domain or hostname.endswith(f".{domain}"):
                return platform
    except Exception:
        pass
    return None



def _is_document_url(url: str | None) -> bool:
    """Return True if the URL points to a document (PDF, Word etc.)."""
    if not url:
        return False
    parsed = urlparse(url)
    _, ext = os.path.splitext(parsed.path.lower())
    if ext in DOCUMENT_EXTENSIONS:
        return True
    query_params = parse_qs(parsed.query)
    for values in query_params.values():
        for value in values:
            _, ext = os.path.splitext(value.lower())
            if ext in DOCUMENT_EXTENSIONS:
                return True
    return False
