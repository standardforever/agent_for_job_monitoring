from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse


def _resolve_url(url, base):
    """
    Resolve url against base so the caller always gets a full absolute URL.

    /jobs?page=2        + https://example.com  -> https://example.com/jobs?page=2
    ?page=2             + https://example.com/jobs  -> https://example.com/jobs?page=2
    https://x.com/jobs  + anything             -> https://x.com/jobs  (unchanged)
    None / empty        + anything             -> None
    """
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme in ("http", "https"):
        return url
    try:
        return urljoin(base, url)
    except Exception:
        return url


def _as_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _increment_alpha(value: str, increment: int) -> str | None:
    if not value or len(value) != 1 or not value.isalpha():
        return None
    base = ord("A") if value.isupper() else ord("a")
    idx = ord(value) - base + increment
    if idx < 0 or idx > 25:
        return None
    return chr(base + idx)


def _increment_value(value: str, sequence_type: str | None, increment: int) -> str | None:
    if sequence_type == "letter":
        return _increment_alpha(value, increment)

    n = _as_int(value)
    if n is None:
        return None
    return str(n + increment)


def _make_url_from_plan(url_plan: dict[str, Any], base_url: str, value: str | None) -> str | None:
    template = url_plan.get("url_template")
    page_param = url_plan.get("page_param")
    sequence_type = url_plan.get("sequence_type")
    next_url = url_plan.get("next_url")
    next_value = url_plan.get("next_value")

    if template and value:
        if "{page}" in template:
            return _resolve_url(template.replace("{page}", str(value)), base_url)
        if "{letter}" in template:
            return _resolve_url(template.replace("{letter}", str(value)), base_url)

    if page_param and value:
        start_url = url_plan.get("next_url") or base_url
        parsed = urlparse(_resolve_url(start_url, base_url))
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query[str(page_param)] = str(value)
        return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))

    if next_url and value and next_value and str(next_value) in str(next_url):
        return _resolve_url(str(next_url).replace(str(next_value), str(value), 1), base_url)

    if next_url and sequence_type == "cursor":
        return _resolve_url(next_url, base_url)

    return None
