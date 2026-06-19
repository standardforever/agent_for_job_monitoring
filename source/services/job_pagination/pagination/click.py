from __future__ import annotations

import re
from typing import Any


def _render_click_selector(selector: str, page_number: int | None = None) -> str:
    if page_number is None:
        return selector
    return (
        str(selector)
        .replace("{next}", str(page_number))
        .replace("{page}", str(page_number))
        .replace("{page_number}", str(page_number))
    )


def _css_string(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def _normalize_playwright_selector(selector: str) -> str:
    def replace_contains(match):
        quote = match.group(1)
        text = match.group(2)
        escaped = text.replace("\\", "\\\\").replace('"', '\\"')
        return f':has-text("{escaped}")'

    return re.sub(r":contains\((['\"])(.*?)\1\)", replace_contains, str(selector))


def _click_selector_candidates(selector: str, page_number: int | None = None) -> list[str]:
    candidates = []
    selector = _render_click_selector(selector, page_number)
    if selector:
        selector = _normalize_playwright_selector(selector)
        candidates.append(selector)

        for attr, value in re.findall(r"\[([\w:-]+)=['\"]([^'\"]+)['\"]\]", selector):
            candidates.extend([
                f"[{attr}='{value}']",
                f"button:has([{attr}='{value}'])",
                f"label:has([{attr}='{value}'])",
                f"a:has([{attr}='{value}'])",
                f"[role='button']:has([{attr}='{value}'])",
            ])

    deduped = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _is_specific_click_selector(selector: str) -> bool:
    raw = str(selector or "").strip()
    lower = raw.lower()
    if not raw:
        return False
    if any(token in raw for token in ("{next}", "{page}", "{page_number}")):
        return True
    if any(word in lower for word in ("next", "load", "more", "show", "view")):
        return True
    if re.search(r"\[[\w:-]+=['\"][^'\"]+['\"]\]", raw):
        return True
    return False


def _relaxed_load_more_candidates(value: str) -> list[str]:
    text = str(value or "")
    match = re.search(r"load\s+(\d+)\s+more", text, flags=re.IGNORECASE)
    if not match:
        return []
    amount = match.group(1)
    return [
        f'button:has-text("Load {amount} more")',
        f'[role="button"]:has-text("Load {amount} more")',
        f'a:has-text("Load {amount} more")',
    ]


def _click_plan_selector_candidates(click_plan: dict[str, Any], page_number: int) -> list[str]:
    candidates = [
        "nav[aria-label='Pagination'] button[aria-label='Next page']",
        "[aria-label='Pagination'] button[aria-label='Next page']",
        "app-paging button[aria-label='Next page']",
        "button[aria-label='Next page']",
    ]
    candidates.extend(_relaxed_load_more_candidates(click_plan.get("next_selector") or ""))
    candidates.extend(_relaxed_load_more_candidates(click_plan.get("text_template") or ""))
    for selector in click_plan.get("selector_candidates") or []:
        candidates.extend(_relaxed_load_more_candidates(str(selector)))

    if click_plan.get("next_selector"):
        candidates.extend(_click_selector_candidates(str(click_plan["next_selector"]), page_number))

    container = click_plan.get("container_selector")
    label = _render_click_selector(click_plan.get("aria_label_template") or "", page_number)
    if label:
        attr = _css_string(label)
        if container:
            candidates.extend([
                f"{container} [aria-label='{attr}']",
                f"{container} [data-llm-label='{attr}']",
                f"{container} [title='{attr}']",
            ])
        candidates.extend([
            f"[aria-label='{attr}']",
            f"[data-llm-label='{attr}']",
            f"[title='{attr}']",
        ])

    for selector in click_plan.get("selector_candidates") or []:
        if selector and _is_specific_click_selector(str(selector)):
            candidates.extend(_click_selector_candidates(str(selector), page_number))

    deduped = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


async def _usable_locator(locator):
    try:
        count = await locator.count()
        if not count:
            return None, False
        for idx in range(min(count, 50)):
            candidate = locator.nth(idx)
            if not await candidate.is_visible():
                continue
            aria_disabled = await candidate.get_attribute("aria-disabled")
            disabled = await candidate.get_attribute("disabled")
            if aria_disabled == "true" or disabled is not None or not await candidate.is_enabled():
                return candidate, True
            return candidate, False
        return None, False
    except Exception:
        return None, False


async def find_click_target(page, click_plan: dict[str, Any], page_number: int):
    for candidate in _click_plan_selector_candidates(click_plan, page_number):
        locator = page.locator(candidate)
        usable, disabled = await _usable_locator(locator)
        if usable is not None:
            return usable, candidate, disabled

    text = _render_click_selector(click_plan.get("text_template") or "", page_number)
    container = click_plan.get("container_selector")
    if text:
        locators = []
        if container:
            locators.append(page.locator(container).get_by_text(text, exact=True))
            locators.append(page.locator(container).get_by_text(text, exact=False))
        locators.append(page.get_by_text(text, exact=True))
        locators.append(page.get_by_text(text, exact=False))
        for locator in locators:
            usable, disabled = await _usable_locator(locator)
            if usable is not None:
                return usable, f"text={text!r}", disabled

    if click_plan.get("item_selector"):
        locator = page.locator(str(click_plan["item_selector"])).filter(has_text=str(page_number))
        usable, disabled = await _usable_locator(locator)
        if usable is not None:
            return usable, f"{click_plan['item_selector']} has_text={page_number}", disabled

    return None, _render_click_selector(click_plan.get("next_selector") or "", page_number), False


async def next_control_state(page, click_plan: dict[str, Any], page_number: int) -> tuple[str, str | None]:
    candidates = []
    disabled_selector = click_plan.get("disabled_selector")
    if disabled_selector:
        candidates.extend(_click_selector_candidates(str(disabled_selector), page_number))
    candidates.extend([
        "nav[aria-label='Pagination'] button[aria-label='Next page']",
        "[aria-label='Pagination'] button[aria-label='Next page']",
        "app-paging button[aria-label='Next page']",
        "button[aria-label='Next page']",
    ])

    seen = set()
    matched_any = False
    for selector in candidates:
        if not selector or selector in seen:
            continue
        seen.add(selector)
        try:
            locator = page.locator(selector)
            count = await locator.count()
            if count:
                matched_any = True
            for idx in range(min(count, 20)):
                candidate = locator.nth(idx)
                if not await candidate.is_visible():
                    continue
                aria_disabled = await candidate.get_attribute("aria-disabled")
                disabled = await candidate.get_attribute("disabled")
                if aria_disabled == "true" or disabled is not None or not await candidate.is_enabled():
                    return "disabled", selector
                return "enabled", selector
        except Exception:
            continue
    return ("hidden", None) if matched_any else ("missing", None)


async def next_control_is_disabled(page, click_plan: dict[str, Any], page_number: int) -> tuple[bool, str | None]:
    state, selector = await next_control_state(page, click_plan, page_number)
    return state == "disabled", selector


def click_attempt_report(click_plan: dict[str, Any], page_number: int, error: str) -> dict[str, Any]:
    text = _render_click_selector(click_plan.get("text_template") or "", page_number)
    return {
        "page_number": page_number,
        "error": error,
        "selector_candidates_tried": _click_plan_selector_candidates(click_plan, page_number),
        "text_candidate": text or None,
        "item_selector": click_plan.get("item_selector"),
        "container_selector": click_plan.get("container_selector"),
        "aria_label_template": click_plan.get("aria_label_template"),
    }
