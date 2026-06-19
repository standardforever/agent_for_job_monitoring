from __future__ import annotations

from typing import Any

from .url_utils import _increment_value


def current_value_after_click_url_pattern(url_plan: dict[str, Any], fallback_value: str | None) -> str | None:
    value = url_plan.get("next_value")
    return str(value) if value not in (None, "") else fallback_value


def next_value_after_current_url_value(url_plan: dict[str, Any], current_value: str | None) -> str | None:
    if url_plan.get("sequence_type") == "cursor":
        return None
    if current_value is None:
        return None
    try:
        increment = int(url_plan.get("increment") or 1)
    except (TypeError, ValueError):
        increment = 1
    return _increment_value(
        str(current_value),
        url_plan.get("sequence_type"),
        increment,
    )


def url_plan_after_observed_move(url_plan: dict[str, Any]) -> dict[str, Any]:
    current_value = current_value_after_click_url_pattern(url_plan, None)
    next_value = next_value_after_current_url_value(url_plan, current_value)
    updated = dict(url_plan)
    if current_value is not None:
        updated["current_value"] = current_value
    if next_value is not None:
        updated["next_value"] = next_value
    return updated
