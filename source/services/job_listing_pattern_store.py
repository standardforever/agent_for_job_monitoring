from __future__ import annotations

import hashlib
import json
from typing import Any


def merge_job_listing_patterns(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for pattern in existing:
        _put_pattern(merged, order, pattern, replace=True)
    for pattern in incoming:
        _put_pattern(merged, order, pattern, replace=True)
    return [merged[key] for key in order]


def dedupe_job_listing_patterns(patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return merge_job_listing_patterns([], patterns)


def pattern_signature(pattern: dict[str, Any] | None) -> str | None:
    if not isinstance(pattern, dict) or not pattern:
        return None
    encoded = json.dumps(pattern, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def next_pattern_version(pattern_entry: dict[str, Any]) -> int:
    return max(1, int(pattern_entry.get("pattern_version") or 0) + 1)


def _put_pattern(
    merged: dict[str, dict[str, Any]],
    order: list[str],
    pattern: dict[str, Any],
    *,
    replace: bool,
) -> None:
    if not isinstance(pattern, dict):
        return
    key = _pattern_key(pattern)
    if not key:
        return
    if key not in merged:
        order.append(key)
        merged[key] = dict(pattern)
        return
    if replace:
        merged[key] = {**merged[key], **dict(pattern)}


def _pattern_key(pattern: dict[str, Any]) -> str:
    return str(pattern.get("page_url") or "").strip().rstrip("/")
