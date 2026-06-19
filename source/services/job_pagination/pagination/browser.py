from __future__ import annotations

import asyncio
from typing import Any

from playwright.async_api import async_playwright
from utils.logging import get_logger, log_event


logger = get_logger("job_pagination_browser")


def get_extraction_js() -> str:
    return r"""
    () => {
        // ═══════════════════════════════════════════════
        // HELPERS
        // ═══════════════════════════════════════════════
        function clean(text) {
            return (text || '').replace(/[\t\r\n\u00a0]+/g, ' ').replace(/  +/g, ' ').trim();
        }

        function getAttributeLabel(el) {
            const names = [
                'aria-label',
                'title',
                'value',
                'placeholder',
                'data-show-message',
                'data-hide-message',
                'data-label',
                'data-text',
                'data-name',
                'data-title'
            ];
            for (const name of names) {
                const value = clean(el.getAttribute(name) || '');
                if (value) return value;
            }
            return '';
        }

        // ═══════════════════════════════════════════════
        // WORD-COUNT REDUCTION PASS
        //
        // Runs on a CLONE of the body so the live DOM is never touched.
        //
        // Rule: strip any element whose OWN text (counting
        // only text nodes that are direct children, not
        // text that belongs to child elements) exceeds 6
        // words AND the element has no children that are:
        //   - interactive  (a, button, [role=button/link])
        //   - arrow chars  (pagination symbols)
        //   - structural   (any element with sub-children)
        //
        // Arrow-character-only text is ALWAYS kept.
        // ═══════════════════════════════════════════════
        const ARROW_RE = /^[\s›»‹«<>\u2192\u2190\u25B6\u25C0\u25B8\u25C2\u21D2\u21D0\u25BA\u25C4\uFEFF]+$/;
        const SKIP_REDUCTION = new Set(['script','style','noscript','meta','link','head',
                                         'html','body','a','button','input','select','textarea',
                                         'nav','ul','ol','table','thead','tbody','tr']);

        function ownWordCount(el) {
            // Count words contributed only by direct text nodes
            let words = 0;
            el.childNodes.forEach(n => {
                if (n.nodeType === Node.TEXT_NODE) {
                    const t = clean(n.textContent);
                    if (t) words += t.split(/\s+/).filter(Boolean).length;
                }
            });
            return words;
        }

        function isArrowOnly(el) {
            return ARROW_RE.test(el.textContent || '');
        }

        function hasInteractiveDescendant(el) {
            return !!(
                el.querySelector('a[href], a[onclick], button, [role="button"], [role="link"], input') ||
                el.querySelector('[data-href],[data-url],[data-permalink],[data-ep-wrapper-link]')
            );
        }

        function hasStructuralChildren(el) {
            // Has child elements (not just text nodes)
            return el.children.length > 0;
        }

        function shouldStrip(el) {
            const tag = el.tagName.toLowerCase();
            if (SKIP_REDUCTION.has(tag))      return false;  // never strip these
            if (isArrowOnly(el))              return false;  // always keep arrows
            if (hasInteractiveDescendant(el)) return false;  // keep containers with links/buttons
            const own = ownWordCount(el);
            if (own <= 6)                     return false;  // short enough, keep
            // Strip only if it has NO structural children that would be independently useful
            // i.e. all text is directly on this node (pure text container > 6 words)
            if (!hasStructuralChildren(el))   return true;   // leaf node, > 6 words → strip
            // Has children but own direct words > 6 → strip OWN text nodes only, keep children
            return false; // handled below via text-node pruning
        }

        // Build a reduced clone
        const bodyClone = document.body.cloneNode(true);

        // Pass A: remove noise tags from clone
        ['script','style','noscript','meta','link','header','footer','svg','img',
         'video','audio','iframe','canvas'].forEach(tag => {
            bodyClone.querySelectorAll(tag).forEach(el => el.remove());
        });

        // Pass A2: remove HTML comments like <!----> and <!-- ... -->
        const commentWalker = document.createTreeWalker(
            bodyClone,
            NodeFilter.SHOW_COMMENT
        );
        const comments = [];
        while (commentWalker.nextNode()) comments.push(commentWalker.currentNode);
        comments.forEach(node => node.remove());

        // Pass A3: expose labels stored in attributes or CSS-generated content.
        bodyClone.querySelectorAll('button, a, label, input, [role="button"], [role="link"]').forEach(el => {
            const label = getAttributeLabel(el);
            if (label) el.setAttribute('data-llm-label', label);
        });

        // Pass B: strip leaf elements with > 6 own words (bottom-up)
        // querySelectorAll returns in document order; reverse for bottom-up
        const allEls = Array.from(bodyClone.querySelectorAll('*')).reverse();
        allEls.forEach(el => {
            if (shouldStrip(el)) {
                el.remove();
                return;
            }
            // If el has > 6 direct own words but has structural children,
            // prune only the direct text nodes (keep child elements)
            const tag = el.tagName.toLowerCase();
            if (!SKIP_REDUCTION.has(tag) && !isArrowOnly(el) &&
                !hasInteractiveDescendant(el) && ownWordCount(el) > 6 &&
                hasStructuralChildren(el)) {
                Array.from(el.childNodes).forEach(n => {
                    if (n.nodeType === Node.TEXT_NODE) {
                        const t = clean(n.textContent);
                        if (t && t.split(/\s+/).filter(Boolean).length > 6) {
                            n.textContent = '';
                        }
                    }
                });
            }
        });

        // Pass C: remove elements that are now empty after pruning
        for (let pass = 0; pass < 5; pass++) {
            let removed = 0;
            Array.from(bodyClone.querySelectorAll('*')).reverse().forEach(el => {
                const tag = el.tagName.toLowerCase();
                if (SKIP_REDUCTION.has(tag)) return;
                if (getAttributeLabel(el)) return;
                if (!(el.textContent || '').trim()) { el.remove(); removed++; }
            });
            if (removed === 0) break;
        }

        return {
            page_url:        window.location.href,
            reduced_html:    bodyClone.innerHTML,
            total_html_len:  document.documentElement.outerHTML.length,
            reduced_len:     bodyClone.innerHTML.length,
        };
    }
    """


async def load_and_extract(cdp_url: str, page_url: str) -> dict[str, Any]:
    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(cdp_url)
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page    = await context.new_page()

        log_event(logger, "info", "pagination_manual_load_started", domain="job_pagination", page_url=page_url)
        await page.goto(page_url,  timeout=30_000)
        import asyncio
        await asyncio.sleep(5)
        await scroll_page(page)

        log_event(logger, "info", "pagination_manual_extract_started", domain="job_pagination", page_url=page_url)
        result = await page.evaluate(get_extraction_js())
        await browser.close()

    return result


async def scroll_page(page) -> None:
    # Gradual scroll: two pauses per step so lazy page data can settle.
    log_event(logger, "info", "pagination_manual_scroll_started", domain="job_pagination", page_url=getattr(page, "url", ""))
    scroll_y = 0
    step_px  = 400
    pause_1  = 0.4
    pause_2  = 0.4

    while True:
        total_h = await page.evaluate("() => document.body.scrollHeight")
        if scroll_y >= total_h:
            break
        scroll_y = min(scroll_y + step_px, total_h)
        await page.evaluate(f"window.scrollTo(0, {scroll_y})")
        await asyncio.sleep(pause_1)
        await page.evaluate(f"window.scrollTo(0, {scroll_y})")
        await asyncio.sleep(pause_2)

    await asyncio.sleep(1.0)


async def light_lazy_scroll(page, steps: int = 2, pause: float = 1.0) -> None:
    viewport_height = await page.evaluate("() => window.innerHeight || 800")
    for index in range(1, steps + 1):
        await page.evaluate("(distance) => window.scrollBy(0, distance)", int(viewport_height * 0.85))
        await asyncio.sleep(pause)


async def wait_for_selector_or_timeout(
    page,
    selector: str | None,
    timeout_ms: int = 12_000,
) -> bool:
    if not selector:
        await asyncio.sleep(timeout_ms / 1000)
        return False
    try:
        await page.wait_for_selector(selector, timeout=timeout_ms, state="attached")
        return True
    except Exception:
        return False


async def wait_for_job_content(
    page,
    selector: str | None,
    *,
    timeout_ms: int = 12_000,
    settle_seconds: float = 1.0,
) -> dict[str, int | bool]:
    selector_seen = await wait_for_selector_or_timeout(page, selector, timeout_ms=timeout_ms)
    await asyncio.sleep(settle_seconds)
    await light_lazy_scroll(page, steps=2, pause=0.8)
    count = 0
    if selector:
        try:
            count = int(await page.locator(selector).count())
        except Exception:
            count = 0
    return {
        "selector_seen": selector_seen,
        "container_count": count,
        "html_length": int(await page.evaluate("() => document.documentElement.outerHTML.length || 0") or 0),
    }


async def get_page_height(page) -> int:
    return int(await page.evaluate("() => document.body.scrollHeight || 0") or 0)


async def probe_bottom_for_more_content(page, wait_seconds: float = 2.5, nudge_px: int = 700) -> dict[str, int | bool]:
    before_height = await get_page_height(page)
    await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(wait_seconds)
    first_height = await get_page_height(page)

    if first_height <= before_height:
        await page.evaluate(
            "(nudge) => window.scrollTo(0, Math.max(0, document.body.scrollHeight - window.innerHeight - nudge))",
            nudge_px,
        )
        await asyncio.sleep(0.6)
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(wait_seconds)

    after_height = await get_page_height(page)
    return {
        "before_height": before_height,
        "after_height": after_height,
        "height_increased": after_height > before_height,
    }


async def gradual_scroll_probe_for_more_content(
    page,
    *,
    step_px: int = 850,
    pause_seconds: float = 0.55,
    settle_seconds: float = 1.5,
) -> dict[str, int | bool | str]:
    before_height = await get_page_height(page)
    before_url = page.url
    position = int(await page.evaluate("() => window.scrollY || 0") or 0)
    height_increased_during_scroll = False

    while True:
        total_height = await get_page_height(page)
        if position >= total_height:
            break
        position = min(position + step_px, total_height)
        await page.evaluate("(y) => window.scrollTo(0, y)", position)
        await asyncio.sleep(pause_seconds)
        latest_height = await get_page_height(page)
        if latest_height > total_height:
            height_increased_during_scroll = True

    await asyncio.sleep(settle_seconds)
    first_height = await get_page_height(page)

    if first_height <= before_height:
        retry_start = max(0, first_height - int(first_height * 0.25))
        await page.evaluate("(y) => window.scrollTo(0, y)", retry_start)
        await asyncio.sleep(0.8)
        position = retry_start
        while True:
            total_height = await get_page_height(page)
            if position >= total_height:
                break
            position = min(position + step_px, total_height)
            await page.evaluate("(y) => window.scrollTo(0, y)", position)
            await asyncio.sleep(pause_seconds)
            latest_height = await get_page_height(page)
            if latest_height > total_height:
                height_increased_during_scroll = True
        await asyncio.sleep(settle_seconds)

    after_height = await get_page_height(page)
    return {
        "before_height": before_height,
        "after_height": after_height,
        "height_increased": after_height > before_height,
        "height_increased_during_scroll": height_increased_during_scroll,
        "url_before": before_url,
        "url_after": page.url,
        "url_changed": page.url != before_url,
    }


async def extract_reduced_html_from_page(page) -> str:
    try:
        extracted = await page.evaluate(get_extraction_js())
        return extracted.get("reduced_html", "") if isinstance(extracted, dict) else ""
    except Exception:
        return ""


async def extract_html_from_last_match_downward(page, selector: str | None) -> dict[str, int | str | bool]:
    if not selector:
        return {"found_anchor": False, "html": "", "html_length": 0, "match_count": 0}
    try:
        result = await page.evaluate(
            """
            (selector) => {
                function clean(text) {
                    return (text || '').replace(/[\\t\\r\\n\\u00a0]+/g, ' ').replace(/  +/g, ' ').trim();
                }

                function cssString(value) {
                    return String(value || '').replace(/\\\\/g, '\\\\\\\\').replace(/'/g, "\\\\'");
                }

                function selectorFor(el) {
                    const tag = el.tagName.toLowerCase();
                    if (el.id) return `${tag}#${CSS.escape(el.id)}`;
                    const aria = el.getAttribute('aria-label');
                    if (aria) return `${tag}[aria-label='${cssString(aria)}']`;
                    const href = el.getAttribute('href');
                    if (tag === 'a' && href) return `a[href='${cssString(href)}']`;
                    const dataAttrs = ['data-testid', 'data-test', 'data-test-name', 'data-qa', 'data-cy', 'data-action'];
                    for (const attr of dataAttrs) {
                        const value = el.getAttribute(attr);
                        if (value) return `${tag}[${attr}='${cssString(value)}']`;
                    }
                    const classes = Array.from(el.classList || []).filter(Boolean).slice(0, 3);
                    if (classes.length) return `${tag}.${classes.map(cls => CSS.escape(cls)).join('.')}`;
                    return tag;
                }

                function candidateFrom(el) {
                    if (el.closest('header, footer')) return null;
                    const tag = el.tagName.toLowerCase();
                    const text = clean(el.innerText || el.textContent || el.getAttribute('aria-label') || el.getAttribute('title') || el.getAttribute('value') || '');
                    const href = el.getAttribute('href') || el.getAttribute('data-href') || el.getAttribute('data-url') || '';
                    const aria = el.getAttribute('aria-label') || '';
                    const selector = selectorFor(el);
                    if (!text && !href && !aria) return null;
                    return { tag, text, href, aria_label: aria, selector };
                }

                function cleanClone(root) {
                    if (root.matches && root.matches('header, footer')) {
                        return null;
                    }
                    const clone = root.cloneNode(true);
                    ['script','style','noscript','meta','link','header','footer','svg','img','video','audio','iframe','canvas'].forEach(tag => {
                        clone.querySelectorAll(tag).forEach(el => el.remove());
                    });
                    const walker = document.createTreeWalker(clone, NodeFilter.SHOW_COMMENT);
                    const comments = [];
                    while (walker.nextNode()) comments.push(walker.currentNode);
                    comments.forEach(node => node.remove());
                    return clone;
                }

                const matches = Array.from(document.querySelectorAll(selector));
                if (!matches.length) {
                    return { found_anchor: false, html: '', html_length: 0, match_count: 0 };
                }

                const last = matches[matches.length - 1];
                const wrapper = document.createElement('div');
                const controls = [];
                const anchor = document.createElement('div');
                anchor.setAttribute('data-context', 'last-visible-job');
                const anchorClone = cleanClone(last);
                anchor.innerHTML = anchorClone ? anchorClone.outerHTML : '';
                wrapper.appendChild(anchor);

                let node = last;
                while (node) {
                    let sibling = node.nextElementSibling;
                    while (sibling) {
                        sibling.querySelectorAll('a[href], button, input[type="button"], input[type="submit"], [role="button"], [role="link"], label').forEach(el => {
                            const candidate = candidateFrom(el);
                            if (candidate) controls.push(candidate);
                        });
                        const clonedSibling = cleanClone(sibling);
                        if (clonedSibling) wrapper.appendChild(clonedSibling);
                        sibling = sibling.nextElementSibling;
                    }
                    node = node.parentElement;
                }

                const html = wrapper.innerHTML;
                return {
                    found_anchor: true,
                    html,
                    html_length: html.length,
                    match_count: matches.length,
                    controls,
                };
            }
            """,
            selector,
        )
        return result if isinstance(result, dict) else {"found_anchor": False, "html": "", "html_length": 0, "match_count": 0}
    except Exception:
        return {"found_anchor": False, "html": "", "html_length": 0, "match_count": 0}
