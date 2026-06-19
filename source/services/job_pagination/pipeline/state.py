from __future__ import annotations

import hashlib
from typing import Any


async def capture_page_state(page, *, stage: str, job_selector: str | None = None) -> dict[str, Any]:
    return await page.evaluate(
        """({ stage, jobSelector }) => {
            const visible = (el) => !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
            const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
            const selectorText = (node) => {
                const link = node.querySelector && node.querySelector('a[href]');
                const href = link ? link.href : '';
                return `${href} ${normalize(node.innerText || node.textContent).slice(0, 220)}`.trim();
            };

            let jobs = [];
            let jobSelectorError = null;
            if (jobSelector) {
                try {
                    jobs = Array.from(document.querySelectorAll(jobSelector));
                } catch (error) {
                    jobSelectorError = String(error);
                }
            }

            const controls = Array.from(document.querySelectorAll(
                'nav[aria-label*="Pagination" i] button, nav[aria-label*="Pagination" i] a, pagination button, pagination a, button, a[href]'
            )).slice(0, 80).map((el) => ({
                tag: el.tagName.toLowerCase(),
                text: normalize(el.innerText || el.textContent).slice(0, 120),
                href: el.href || el.getAttribute('href') || '',
                aria_label: el.getAttribute('aria-label') || '',
                id: el.id || '',
                class_name: typeof el.className === 'string' ? el.className.slice(0, 160) : '',
                disabled: !!el.disabled || el.getAttribute('aria-disabled') === 'true',
                visible: visible(el),
            })).filter((control) => {
                const haystack = `${control.text} ${control.href} ${control.aria_label} ${control.id} ${control.class_name}`.toLowerCase();
                return /page|pagination|next|previous|prev|load|more|show|view/.test(haystack);
            });

            const pageIndicators = Array.from(document.querySelectorAll(
                '[aria-current], .active, [class*=active], input[aria-label*="page" i], [aria-label*="current page" i]'
            )).slice(0, 30).map((el) => ({
                tag: el.tagName.toLowerCase(),
                text: normalize(el.innerText || el.textContent || el.value).slice(0, 80),
                aria_label: el.getAttribute('aria-label') || '',
                value: el.value || '',
                class_name: typeof el.className === 'string' ? el.className.slice(0, 120) : '',
            }));

            const signatureItems = jobs.slice(0, 5).concat(jobs.slice(Math.max(0, jobs.length - 3))).map(selectorText);
            return {
                stage,
                url: location.href,
                title: document.title,
                height: document.body ? document.body.scrollHeight : 0,
                scroll_y: window.scrollY,
                job_selector: jobSelector,
                job_selector_error: jobSelectorError,
                job_count: jobs.length,
                job_signature_items: signatureItems,
                controls,
                page_indicators: pageIndicators,
            };
        }""",
        {"stage": stage, "jobSelector": job_selector},
    )


def add_signature_hash(state: dict[str, Any]) -> dict[str, Any]:
    items = "|".join(str(item) for item in state.get("job_signature_items") or [])
    state["job_signature_hash"] = hashlib.sha1(items.encode("utf-8")).hexdigest()[:16] if items else ""
    return state
