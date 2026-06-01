from __future__ import annotations

import tldextract

_extractor = tldextract.TLDExtract(suffix_list_urls=())


def extract(value: str | None) -> tldextract.ExtractResult:
    return _extractor(str(value or ""))


def registered_domain(value: str | None) -> str:
    result = extract(value)
    if result.domain and result.suffix:
        return f"{result.domain}.{result.suffix}".lower()
    return str(value or "").strip().lower()
