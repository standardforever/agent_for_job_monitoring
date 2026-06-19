from __future__ import annotations

import json
import re
from typing import Any

from .config import OPENAI_MODEL
from .openai_client import get_openai_client
from .prompts import SYSTEM_PROMPT
from .url_utils import _resolve_url
from services.openai_service import resolve_openai_model


def analyse_pagination(
    page_url: str,
    reduced_html: str,
    total_html_len: int,
) -> dict[str, Any]:
    client = get_openai_client()
    model = resolve_openai_model(OPENAI_MODEL)

    user_msg = (
        f"Page URL                 : {page_url}\n"
        f"Full page HTML length    : {total_html_len:,} chars\n"
        f"Reduced body HTML length : {len(reduced_html):,} chars\n"
        "\n"
        f"--- REDUCED BODY HTML ---\n{reduced_html}\n--- END REDUCED BODY HTML ---\n\n"
        "Identify the pagination type and return the JSON pattern."
    )

    response = client.chat.completions.create(
        model    = model,
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        response_format={"type": "json_object"},
        # max_tokens=1200,
    )

    raw = (response.choices[0].message.content or "{}").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\s*```$",          "", raw, flags=re.MULTILINE)

    try:
        result = json.loads(raw.strip())
    except json.JSONDecodeError:
        return {"error": "LLM returned invalid JSON", "raw": raw}

    navigation = result.get("navigation") or {}
    url_plan = navigation.get("url") or {}
    if url_plan.get("next_url"):
        url_plan["next_url"] = _resolve_url(url_plan.get("next_url"), page_url)
    if url_plan.get("url_template") and (
        str(url_plan["url_template"]).startswith("/") or str(url_plan["url_template"]).startswith("?")
    ):
        url_plan["url_template"] = _resolve_url(url_plan["url_template"], page_url)

    return result


def analyse_url_pattern_from_click(before_url: str, after_url: str) -> dict[str, Any]:
    client = get_openai_client()
    model = resolve_openai_model(OPENAI_MODEL)

    system_msg = """
You analyse two URLs observed before and after clicking a pagination control.
Return ONLY a JSON object Python can use to generate later page URLs:

{
  "can_use_url": boolean,
  "confidence": number,
  "url": {
    "next_url": string | null,
    "url_template": string | null,
    "page_param": string | null,
    "sequence_type": "number" | "offset" | "letter" | "cursor" | "unknown" | null,
    "current_value": string | null,
    "next_value": string | null,
    "increment": number | null
  },
  "notes": string
}

Rules:
- If after_url has a predictable page number, offset, or letter, set can_use_url true.
- Put "{page}" or "{letter}" in url_template where Python should substitute the next value.
- For query params like ?page=2, set page_param to "page".
- current_value is the value in before_url if present, otherwise the logical first page value.
- next_value is the value in after_url. It means the URL value for the page we have ALREADY reached after the click, not the next value to request.
- Be careful with zero-based page parameters such as pageNumber=0 for visible page 1 and pageNumber=1 for visible page 2. In that case current_value should be "0", next_value should be "1", and increment should be 1.
- increment is the amount to add each page.
- If the change uses an opaque cursor/token, set sequence_type "cursor" and can_use_url false unless later tokens can be derived.
- No prose, no markdown fences.
""".strip()

    user_msg = (
        f"Before click URL: {before_url}\n"
        f"After click URL : {after_url}\n"
        "Can Python generate page 3, page 4, etc. from this pattern?"
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
        # max_tokens=600,
    )

    raw = (response.choices[0].message.content or "{}").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)

    try:
        result = json.loads(raw.strip())
    except json.JSONDecodeError:
        return {"can_use_url": False, "error": "LLM returned invalid JSON", "raw": raw}

    url_plan = result.get("url") or {}
    if url_plan.get("next_url"):
        url_plan["next_url"] = _resolve_url(url_plan.get("next_url"), after_url)
    if url_plan.get("url_template") and (
        str(url_plan["url_template"]).startswith("/") or str(url_plan["url_template"]).startswith("?")
    ):
        url_plan["url_template"] = _resolve_url(url_plan["url_template"], after_url)

    return result


def analyse_pagination_repair(
    page_url: str,
    original_navigation: dict[str, Any],
    target_page: int,
    errors: list[dict[str, Any]],
    reduced_html: str,
) -> dict[str, Any]:
    client = get_openai_client()
    model = resolve_openai_model(OPENAI_MODEL)

    system_msg = """
You repair a failed pagination execution plan.
Return ONLY a JSON object with the same "navigation" shape:

{
  "can_repair": boolean,
  "is_end_of_pagination": boolean,
  "stop_reason": string | null,
  "navigation": {
    "method": "url" | "click" | "none",
    "max_pages": 3,
    "url": {
      "next_url": string | null,
      "url_template": string | null,
      "page_param": string | null,
      "sequence_type": "number" | "offset" | "letter" | "cursor" | "unknown" | null,
      "current_value": string | null,
      "next_value": string | null,
      "increment": number | null
    },
    "click": {
      "next_selector": string | null,
      "disabled_selector": string | null,
      "wait_after_click_ms": number,
      "selector_candidates": array,
      "container_selector": string | null,
      "item_selector": string | null,
      "active_selector": string | null,
      "aria_label_template": string | null,
      "text_template": string | null,
      "current_page": string | null,
      "next_page": string | null,
      "increment": number | null
    },
    "stop_when": array
  },
  "reason": string
}

Rules:
- First decide whether the failure means the crawler has reached the end of pagination.
- If the current DOM shows a disabled/hidden/unavailable next control, no page number for the target page, an active last page, repeated current-page controls only, or wording like no more results/jobs, return can_repair false, is_end_of_pagination true, method "none", and a stop_reason.
- If the page says there are no current vacancies, no open roles, no jobs found, no search results, no matching jobs, or similar empty-state wording, return can_repair false, is_end_of_pagination true, method "none", and stop_reason "no_jobs_listed". Do not invent a pagination selector for an empty jobs page.
- If failed attempts show repeated URLs, HTTP 404/410 after earlier successful pages, same jobs/no new jobs, or a disabled next button, prefer is_end_of_pagination true instead of inventing a new selector.
- Only use can_repair true when the current DOM still contains a credible enabled next/page/load-more control and the failure was caused by the previous navigation plan being wrong.
- Only repair when the HTML still shows a credible next page/load-more control that was missed or selected incorrectly.
- Use the failed attempts to avoid returning the same broken selector unless it is corrected.
- Prefer generic metadata fallbacks as well as a primary selector.
- When the DOM has a pagination nav with "Next page" / "Previous page" / "Current page" controls, prefer the visible enabled "Next page" selector over generic primary buttons such as "Load more".
- If repairing a URL failure, strongly prefer a click selector for the visible next/page control when present.
- If returning method "url", still populate click fallback fields when visible pagination controls exist.
- Do not return the same broken URL template or selector unless the HTML proves it is correct.
- If the current DOM shows URL pagination, return method "url".
- If the DOM shows click pagination, return method "click".
- If both URL and click navigation are visible, choose the stronger method but populate both url and click objects.
- If no reliable pagination is present, return can_repair false and method "none".
- No prose, no markdown fences.
""".strip()

    user_msg = (
        f"Page URL: {page_url}\n"
        f"Target page/action number: {target_page}\n\n"
        f"Original navigation JSON:\n{json.dumps(original_navigation, indent=2)}\n\n"
        f"Failed attempts/errors:\n{json.dumps(errors, indent=2)}\n\n"
        f"--- CURRENT REDUCED HTML ---\n{reduced_html[:80000]}\n--- END CURRENT REDUCED HTML ---"
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
        # max_tokens=1200,
    )

    raw = (response.choices[0].message.content or "{}").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)

    try:
        result = json.loads(raw.strip())
    except json.JSONDecodeError:
        return {"can_repair": False, "error": "LLM returned invalid JSON", "raw": raw}

    navigation = result.get("navigation") or {}
    url_plan = navigation.get("url") or {}
    if url_plan.get("next_url"):
        url_plan["next_url"] = _resolve_url(url_plan.get("next_url"), page_url)
    if url_plan.get("url_template") and (
        str(url_plan["url_template"]).startswith("/") or str(url_plan["url_template"]).startswith("?")
    ):
        url_plan["url_template"] = _resolve_url(url_plan["url_template"], page_url)

    return result
