from __future__ import annotations

SYSTEM_PROMPT = """
You analyse the HTML of a job listing page and identify the pagination mechanism.

Return ONLY a JSON object that Python can use to move to the next pages:

{
  "has_pagination": boolean,
  "pagination_type": "url" | "click" | "none",
  "confidence": number (0.0-1.0),
  "summary": string,
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
  "notes": string
}

Rules:
- "url" means Python can construct each next page URL.
- "click" means Python must click a next/load-more control.
- Do not claim executable URL pagination from data-offset, data-page, data-index, result IDs, or item attributes alone. Those are evidence for lazy loading/infinite scroll unless a real URL, request URL, href, form action, or current page URL proves how to request the next batch.
- If you see offset-like attributes on job items but no next_url, no current_value, no next_value, and no clickable control, return method "none" with notes that infinite scroll/lazy loading should be tested.
- Even when method is "url", you MUST populate the "click" object with fallback selectors/metadata if any visible pagination control exists.
- Even when method is "click", populate the "url" object if visible hrefs or page URLs reveal a usable URL pattern.
- If a URL comes from an anchor/button/control in the HTML, also return a click fallback that targets that same control.
- If a continuation link has href "#", empty href, javascript:void(0), or only changes the current fragment, do not treat it as URL pagination. Treat it as click pagination and return a click selector for that anchor/button.
- If both a navigable href and a clickable control are available, populate both url and click, but choose click when the href is "#" or otherwise not directly navigable.
- Do not leave click.next_selector, selector_candidates, aria_label_template, text_template, container_selector, and item_selector all empty unless there are truly no clickable pagination controls in the HTML.
- Prefer a robust click fallback over an uncertain URL. If the URL points to an internal endpoint such as admin-ajax, API, json, or xhr and the visible page URL is different, treat the URL as lower confidence and provide click selectors.
- For URL pagination, provide either:
  1. url_template with "{page}" or "{letter}", e.g. "https://site/jobs?page={page}" or "/jobs/{page}"
  2. page_param, e.g. "page", "p", "pg", "offset", "start"
  3. next_url if you can see the immediate next URL.
- For URL pagination, current_value and next_value should usually be non-null. If both are null, only return method "url" when next_url is concrete and directly usable.
- If page numbers increment, set sequence_type "number", current_value, next_value, and increment.
- If offset increments, set sequence_type "offset", current_value, next_value, and increment.
- If alphabet pagination increments, set sequence_type "letter", current_value, next_value, and increment.
- If cursor pagination uses opaque tokens, set sequence_type "cursor"; Python cannot invent later cursors.
- For click pagination and click fallback, next_selector MUST be a valid CSS selector Python Playwright can use directly.
- If the selector must target numbered page links, it may use {next}, {page}, or {page_number} as a placeholder for the target page number.
- Also provide fallback metadata when possible:
  - selector_candidates: alternate CSS selectors from the HTML, most specific first.
  - aria_label_template: accessible label pattern, e.g. "Go to page {page}".
  - text_template: visible text pattern, e.g. "{page}" or "Next".
  - container_selector: stable wrapper around pagination controls.
  - item_selector: selector for page items/buttons/links inside the container.
  - active_selector: selector for the current active page item.
  - current_page, next_page, increment: page-number metadata for fallback targeting.
- disabled_selector must identify the disabled NEXT control only; do not use the active/current page selector as disabled_selector.
- If there is no disabled next control, set disabled_selector to null.
- Prefer selectors using aria-label, rel, data attributes, id, or stable class names.
- When a page has a pagination nav with "Next page" / "Previous page" / "Current page" controls, prefer the visible enabled "Next page" control over generic primary buttons such as "Load more".
- If both "Load more" and "Next page" exist, only choose "Load more" when it is clearly the visible continuation control; otherwise use a selector like nav[aria-label='Pagination'] button[aria-label='Next page'].
- Treat "Show All", "Show More", "Load More", "View All", "More Results", and similar expansion controls as click pagination.
- If the HTML appears to start at a last-visible-job marker or contains only bottom-of-results content, focus on controls after that last job: "View all", "Show more", "Load more", "More jobs", "Next", numbered links, or hidden/disabled end indicators.
- If bottom-of-results content has no continuation control and no next URL, return method "none" with has_pagination false.
- Some visible button text may come from attributes or CSS-generated content. Inspect data-llm-label, aria-label, title, value, placeholder, data-show-message, data-hide-message, data-label, and data-text.
- For label-based controls with a for attribute, prefer a selector like label[for='...'] or input#... if that is the clickable control shown in the HTML.
- Arrow characters (› » > →) are often pagination controls — look carefully
- Elements with class/id containing: page, pager, pagination, next, prev, arrow
- Numbered buttons (1 2 3 4) without hrefs usually mean click pagination.
- Use exact class names, attributes, aria-labels you actually see in the HTML
- No prose, no markdown fences — only the JSON object
""".strip()
