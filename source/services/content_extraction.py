from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse

try:
    from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
except Exception:  # pragma: no cover - handled gracefully at runtime
    Page = None
    PlaywrightTimeoutError = TimeoutError

from utils.logging import get_logger, log_event
from js_helper.page_extraction import page_extraction

logger = get_logger("content_extraction")


def _page_domain(page: Page | None) -> str:
    try:
        return urlparse(page.url if page else "").netloc.lower() or "unknown"
    except Exception:
        return "unknown"


def _selector_link_lines(selector_map: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    seen: set[tuple[str, str]] = set()
    for item in dict(selector_map or {}).values():
        if not isinstance(item, dict):
            continue
        action_url = str(item.get("action_url") or "").strip()
        if not action_url:
            attributes = dict(item.get("attributes") or {})
            action_url = str(attributes.get("href") or attributes.get("data-href") or attributes.get("data-url") or "").strip()
        if not action_url:
            continue
        label = str(item.get("label") or item.get("text") or item.get("name") or "").strip() or "link"
        kind = str(item.get("kind") or "").strip() or ("link" if item.get("is_link") else "interactive")
        marker = (label.lower(), action_url)
        if marker in seen:
            continue
        seen.add(marker)
        lines.append(f"- [{kind}] {label} -> {action_url}")
    return lines


def _append_missing_selector_links(markdown: str, selector_map: dict[str, Any]) -> str:
    content = str(markdown or "").strip()
    missing_lines = []
    for line in _selector_link_lines(selector_map):
        url = line.rsplit(" -> ", 1)[-1].strip()
        if url and url not in content:
            missing_lines.append(line)

    if not missing_lines:
        return content

    section = "\n".join(["", "H2: Extracted selector links", *missing_lines])
    return f"{content}\n{section}".strip()


COOKIE_SELECTORS = [
    "button:has-text('Accept all')",
    "button:has-text('Accept All')",
    "button:has-text('Accept cookies')",
    "button:has-text('Accept Cookies')",
    "button:has-text('Allow all')",
    "button:has-text('Allow All')",
    "button:has-text('I agree')",
    "button:has-text('I Accept')",
    "button:has-text('Got it')",
    "button:has-text('OK')",
    "button:has-text('Okay')",
    "button:has-text('Continue')",
    "button:has-text('Agree')",
    "button:has-text('Consent')",
    "button:has-text('Reject all')",
    "button:has-text('Reject All')",
    "button:has-text('Decline')",
    "button:has-text('Only necessary')",
    "button:has-text('Essential only')",
    "[id*='accept-cookies']",
    "[id*='cookie-accept']",
    "[id*='gdpr-accept']",
    "[id*='consent-accept']",
    "[class*='cookie-accept']",
    "[class*='accept-cookie']",
    "[data-testid*='cookie-accept']",
    "[data-testid*='accept-cookies']",
    "#onetrust-accept-btn-handler",
    ".onetrust-accept-btn-handler",
    "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
    "#cookieconsent-button-accept",
    ".cc-accept",
    ".cc-allow",
    ".cc-dismiss",
    "#accept-cookies",
    "#cookie-consent-accept",
    ".cookie-consent-accept",
    "[aria-label='Accept cookies']",
    "[aria-label='Accept all cookies']",
]

POPUP_CLOSE_SELECTORS = [
    "button:has-text('Close')",
    "button:has-text('×')",
    "button:has-text('X')",
    "button:has-text('No thanks')",
    "button:has-text('No, thanks')",
    "button:has-text('Not now')",
    "button:has-text('Maybe later')",
    "button:has-text('Skip')",
    "button:has-text('Dismiss')",
    "[aria-label='Close']",
    "[aria-label='close']",
    "[aria-label='Dismiss']",
    "[title='Close']",
    "[title='close']",
    ".modal-close",
    ".popup-close",
    ".close-button",
    ".close-btn",
    ".dismiss-button",
    "[class*='close-modal']",
    "[class*='modal-close']",
    "[class*='popup-close']",
    "[class*='newsletter-close']",
    "[data-dismiss='modal']",
    "[data-close]",
    "button svg[class*='close']",
    "button[class*='close'] svg",
]

OVERLAY_SELECTORS = [
    "[class*='cookie-banner']",
    "[class*='cookie-notice']",
    "[class*='cookie-consent']",
    "[class*='gdpr-banner']",
    "[class*='newsletter-popup']",
    "[class*='newsletter-modal']",
    "[class*='email-popup']",
    "[class*='subscribe-popup']",
    "[class*='overlay-modal']",
    "[id*='cookie-banner']",
    "[id*='cookie-notice']",
    "[id*='newsletter-popup']",
    "#onetrust-consent-sdk",
    "#CybotCookiebotDialog",
    ".modal-backdrop",
    ".overlay-backdrop",
]


async def _wait_for_page_ready(page: Page) -> None:
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10_000)
    except PlaywrightTimeoutError:
        pass

    try:
        await page.wait_for_load_state("networkidle", timeout=5_000)
    except PlaywrightTimeoutError:
        pass


async def _wait_for_dynamic_job_app(page: Page) -> dict[str, Any]:
    try:
        page_url = str(page.url or "").lower()
        has_webitrent_marker = "webitrent.com" in page_url or await page.evaluate(
            """
            () => Boolean(
              document.querySelector('#FILTER\\\\.STD_HID_FLDS\\\\.ET_BASE\\\\.1-1')
              || document.querySelector('[id^="FILTER.STD_HID_FLDS"]')
              || document.querySelector('script[src*="mhr_webrec_job_search"]')
            )
            """
        )
        if not has_webitrent_marker:
            return {"dynamic_app": None, "waited": False, "rendered": False}

        try:
            await page.wait_for_function(
                """
                () => Boolean(
                  document.querySelector('.Mhr-jobDetail')
                  || document.querySelector('.Mhr-jobSearchResultsOuter')
                  || document.querySelector('.Mhr-jobSearchMatchCount')
                  || document.querySelector('[data-type="jobs"]')
                  || document.querySelector('[data-type="no-jobs"]')
                  || document.querySelector('[data-type="jobs-error"]')
                )
                """,
                timeout=12_000,
            )
        except PlaywrightTimeoutError:
            pass

        rendered = await page.evaluate(
            """
            () => ({
              jobCards: document.querySelectorAll('.Mhr-jobDetail').length,
              resultContainers: document.querySelectorAll('.Mhr-jobSearchResultsOuter, .Mhr-jobSearchJobs').length,
              matchText: document.querySelector('.Mhr-jobSearchMatchCount')?.innerText || '',
              displayType: document.body?.dataset?.displayType || '',
            })
            """
        )
        return {
            "dynamic_app": "webitrent",
            "waited": True,
            "rendered": bool(
                int(rendered.get("jobCards", 0) or 0)
                or int(rendered.get("resultContainers", 0) or 0)
                or str(rendered.get("matchText") or "").strip()
            ),
            "details": rendered,
        }
    except Exception as exc:
        return {
            "dynamic_app": "unknown",
            "waited": False,
            "rendered": False,
            "error": str(exc),
        }


async def _handle_cookie_consent(page: Page) -> bool:
    for selector in COOKIE_SELECTORS:
        try:
            button = page.locator(selector).first
            if await button.is_visible(timeout=500):
                await button.click(timeout=3_000)
                await asyncio.sleep(0.5)
                return True
        except Exception:
            continue
    return False


async def _handle_popups(page: Page) -> int:
    async def _should_click_popup_target(locator) -> bool:
        try:
            return bool(
                await locator.evaluate(
                    """
                    (element) => {
                      if (!(element instanceof Element)) return false;

                      const text = (
                        element.textContent ||
                        element.getAttribute('aria-label') ||
                        element.getAttribute('title') ||
                        ''
                      ).replace(/\\s+/g, ' ').trim().toLowerCase();
                      const className = typeof element.className === 'string' ? element.className.toLowerCase() : '';
                      const id = (element.id || '').toLowerCase();
                      const ariaLabel = (element.getAttribute('aria-label') || '').toLowerCase();

                      const blockedText = new Set([
                        'next', 'previous', 'prev', 'back', 'more', 'load more', 'show more',
                        '1', '2', '3', '4', '5', '6', '7', '8', '9'
                      ]);
                      if (blockedText.has(text)) return false;

                      const paginationLike = /pagination|pager|page-numbers|page-item|paginate|next|previous|prev/;
                      if (
                        paginationLike.test(className) ||
                        paginationLike.test(id) ||
                        paginationLike.test(ariaLabel)
                      ) {
                        return false;
                      }

                      const navLikeAncestor = element.closest('nav, [role="navigation"], .pagination, .pager, .page-numbers, .paginate');
                      if (navLikeAncestor) return false;

                      const modalAncestor = element.closest(
                        [
                          '[role="dialog"]',
                          '[aria-modal="true"]',
                          '.modal',
                          '.popup',
                          '.overlay',
                          '.newsletter',
                          '.cookie',
                          '[class*="modal"]',
                          '[class*="popup"]',
                          '[class*="overlay"]',
                          '[class*="newsletter"]',
                          '[class*="cookie"]',
                          '[id*="modal"]',
                          '[id*="popup"]',
                          '[id*="overlay"]',
                          '[id*="newsletter"]',
                          '[id*="cookie"]'
                        ].join(',')
                      );

                      if (modalAncestor) return true;

                      const fixedAncestor = (() => {
                        let current = element;
                        while (current instanceof Element) {
                          const style = window.getComputedStyle(current);
                          if (style.position === 'fixed' || style.position === 'sticky') {
                            const rect = current.getBoundingClientRect();
                            if (rect.width > window.innerWidth * 0.25 || rect.height > window.innerHeight * 0.15) {
                              return current;
                            }
                          }
                          current = current.parentElement;
                        }
                        return null;
                      })();

                      return Boolean(fixedAncestor);
                    }
                    """
                )
            )
        except Exception:
            return False

    closed_count = 0
    for selector in POPUP_CLOSE_SELECTORS:
        try:
            buttons = page.locator(selector)
            count = await buttons.count()
            for index in range(min(count, 3)):
                try:
                    button = buttons.nth(index)
                    if await button.is_visible(timeout=300) and await _should_click_popup_target(button):
                        await button.click(timeout=2_000)
                        closed_count += 1
                        await asyncio.sleep(0.3)
                except Exception:
                    continue
        except Exception:
            continue
    log_event(
        logger,
        "info",
        "popups_handled closed_count=%s",
        closed_count,
        domain=_page_domain(page),
        closed_count=closed_count,
    )
    return closed_count


async def _expand_accordions(page: Page) -> dict[str, int]:
    """Expand collapsed accordion/disclosure/tab elements before extraction.

    Three-phase approach:
      1. DOM manipulation  — native <details> + aria-controls panels (zero side-effects)
      2. JS click dispatch — fires event handlers for React/Vue/jQuery accordions
      3. CSS force-reveal  — fallback for CSS-only (height/max-height/display) patterns
    Sites without accordions are unaffected; each phase is a no-op when nothing matches.
    """
    try:
        # ── Phase 1: safe DOM manipulation ──────────────────────────────────────
        dom_expanded: int = await page.evaluate(
            """
            () => {
                let count = 0;

                // 1a. Open all native <details> elements
                document.querySelectorAll('details:not([open])').forEach(d => {
                    d.setAttribute('open', '');
                    count++;
                });

                // 1b. aria-controls pattern: reveal the controlled panel directly
                document.querySelectorAll('[aria-expanded="false"][aria-controls]').forEach(trigger => {
                    const panelId = trigger.getAttribute('aria-controls');
                    const panel = panelId ? document.getElementById(panelId) : null;
                    if (panel) {
                        panel.style.cssText +=
                            ';display:block!important;visibility:visible!important' +
                            ';height:auto!important;max-height:none!important' +
                            ';overflow:visible!important;opacity:1!important;';
                        panel.removeAttribute('hidden');
                        panel.setAttribute('aria-hidden', 'false');
                        trigger.setAttribute('aria-expanded', 'true');
                        count++;
                    }
                });

                // 1c. Tab panels hidden via aria-hidden
                document.querySelectorAll('[role="tabpanel"][aria-hidden="true"]').forEach(panel => {
                    panel.setAttribute('aria-hidden', 'false');
                    panel.style.cssText +=
                        ';display:block!important;visibility:visible!important;opacity:1!important;';
                    count++;
                });

                return count;
            }
            """
        )

        # ── Phase 2: JS click dispatch for event-driven accordions ───────────────
        # Uses element.click() (not Playwright click) so it fires JS handlers
        # without the browser following links or navigating.
        click_triggered: int = await page.evaluate(
            """
            () => {
                let count = 0;
                document.querySelectorAll('[aria-expanded="false"]').forEach(el => {
                    const tag = el.tagName.toLowerCase();
                    // Skip anchors to avoid navigation
                    if (tag === 'a' || el.hasAttribute('href')) return;
                    try { el.click(); count++; } catch (_) {}
                });
                return count;
            }
            """
        )

        if click_triggered > 0:
            await asyncio.sleep(0.6)  # allow CSS transitions / re-renders to settle

        # ── Phase 3: CSS force-reveal for remaining hidden content ────────────────
        # Targets common accordion/collapse/tab content class patterns.
        # Only force-reveals elements that contain meaningful text (> 20 chars).
        force_revealed: int = await page.evaluate(
            """
            () => {
                const SELECTORS = [
                    // Generic accordion patterns
                    '[class*="accordion-body"]',
                    '[class*="accordion-content"]',
                    '[class*="accordion-panel"]',
                    '[class*="accordion-collapse"]',
                    // Bootstrap / common frameworks
                    '.collapse:not(.show)',
                    '.tab-pane:not(.active)',
                    // Panel / expandable patterns
                    '[class*="panel-body"]',
                    '[class*="panel-content"]',
                    '[class*="expandable-content"]',
                    '[class*="collapsible-content"]',
                    '[class*="collapse-content"]',
                    '[class*="drawer-content"]',
                    // Height-based hiding (max-height animation pattern)
                    '[class*="accordion"] [class*="content"]',
                    '[class*="accordion"] [class*="body"]',
                ];
                let count = 0;
                const seen = new Set();
                SELECTORS.forEach(sel => {
                    document.querySelectorAll(sel).forEach(el => {
                        if (seen.has(el)) return;
                        seen.add(el);
                        const style = window.getComputedStyle(el);
                        const hidden =
                            style.display === 'none'
                            || style.visibility === 'hidden'
                            || parseFloat(style.height || '1') === 0
                            || parseFloat(style.maxHeight || '1') === 0
                            || parseFloat(style.opacity || '1') === 0;
                        if (hidden && (el.textContent || '').trim().length > 20) {
                            el.style.cssText +=
                                ';display:block!important;visibility:visible!important' +
                                ';height:auto!important;max-height:none!important' +
                                ';overflow:visible!important;opacity:1!important;';
                            count++;
                        }
                    });
                });
                return count;
            }
            """
        )

        log_event(
            logger,
            "info",
            "accordions_expanded dom_expanded=%s click_triggered=%s force_revealed=%s",
            dom_expanded,
            click_triggered,
            force_revealed,
            domain=_page_domain(page),
            dom_expanded=int(dom_expanded or 0),
            click_triggered=int(click_triggered or 0),
            force_revealed=int(force_revealed or 0),
        )
        return {
            "dom_expanded": int(dom_expanded or 0),
            "click_triggered": int(click_triggered or 0),
            "force_revealed": int(force_revealed or 0),
        }
    except Exception:
        return {"dom_expanded": 0, "click_triggered": 0, "force_revealed": 0}


async def _remove_overlays(page: Page) -> int:
    removed_count = 0
    for selector in OVERLAY_SELECTORS:
        try:
            count = await page.evaluate(
                """
                (selector) => {
                  const elements = document.querySelectorAll(selector);
                  let removed = 0;
                  elements.forEach((element) => {
                    element.remove();
                    removed += 1;
                  });
                  return removed;
                }
                """,
                selector,
            )
            removed_count += int(count or 0)
        except Exception:
            continue
    return removed_count


async def _scroll_to_load_content(page: Page, scroll_delay: float = 0.5) -> dict[str, int]:
    try:
        scroll_height = await page.evaluate("document.body.scrollHeight")
        viewport_height = await page.evaluate("window.innerHeight")
        current_position = 0
        scroll_count = 0

        while current_position < scroll_height:
            current_position += viewport_height
            await page.evaluate("(position) => window.scrollTo(0, position)", current_position)
            await asyncio.sleep(scroll_delay)
            scroll_count += 1

            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height > scroll_height:
                scroll_height = new_height

        await page.evaluate("window.scrollTo(0, 0)")
        return {"scroll_count": scroll_count, "final_height": int(scroll_height or 0)}
    except Exception:
        return {"scroll_count": 0, "final_height": 0}


async def _wait_for_content_stability(
    page: Page,
    max_wait_seconds: float = 6.0,
    poll_interval_seconds: float = 0.5,
    stable_rounds_required: int = 3,
) -> dict[str, Any]:
    try:
        elapsed_seconds = 0.0
        stable_rounds = 0
        samples = 0
        last_snapshot: tuple[int, int, int] | None = None

        while elapsed_seconds < max_wait_seconds:
            snapshot = await page.evaluate(
                """
                () => {
                  const interactiveSelector = [
                    'a[href]',
                    'button',
                    'input',
                    'select',
                    'textarea',
                    'summary',
                    '[role="button"]',
                    '[role="link"]',
                    '[data-url]',
                    '[data-href]',
                    '[data-link]',
                    '[data-permalink]',
                    '[data-job-url]',
                    '[data-action-url]',
                    '[data-ep-wrapper-link]',
                    '[onclick]'
                  ].join(',');

                  const interactiveCount = document.querySelectorAll(interactiveSelector).length;
                  const textLength = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim().length;
                  const scrollHeight = Math.max(
                    document.body?.scrollHeight || 0,
                    document.documentElement?.scrollHeight || 0
                  );

                  return { interactiveCount, textLength, scrollHeight };
                }
                """
            )

            current_snapshot = (
                int(snapshot.get("interactiveCount", 0) or 0),
                int(snapshot.get("textLength", 0) or 0),
                int(snapshot.get("scrollHeight", 0) or 0),
            )
            samples += 1

            if current_snapshot == last_snapshot:
                stable_rounds += 1
                if stable_rounds >= stable_rounds_required:
                    return {
                        "stable": True,
                        "elapsed_seconds": round(elapsed_seconds, 2),
                        "samples": samples,
                        "interactive_count": current_snapshot[0],
                        "text_length": current_snapshot[1],
                        "scroll_height": current_snapshot[2],
                    }
            else:
                stable_rounds = 0
                last_snapshot = current_snapshot

            await asyncio.sleep(poll_interval_seconds)
            elapsed_seconds += poll_interval_seconds
    except Exception:
        return {
            "stable": False,
            "elapsed_seconds": 0.0,
            "samples": 0,
            "interactive_count": 0,
            "text_length": 0,
            "scroll_height": 0,
        }

    interactive_count, text_length, scroll_height = last_snapshot or (0, 0, 0)
    return {
        "stable": False,
        "elapsed_seconds": round(elapsed_seconds, 2),
        "samples": samples,
        "interactive_count": interactive_count,
        "text_length": text_length,
        "scroll_height": scroll_height,
    }


async def prepare_page_for_extraction(page: Page | None) -> dict[str, Any]:
    if page is None:
        return {
            "page_ready": False,
            "cookie_handled": False,
            "popups_closed": 0,
            "overlays_removed": 0,
            "scroll_count": 0,
            "stability_wait_seconds": 0.0,
            "content_stable": False,
            "final_wait_seconds": 0.0,
        }

    await _wait_for_page_ready(page)
    dynamic_app_result = await _wait_for_dynamic_job_app(page)
    cookie_handled = await _handle_cookie_consent(page)
    popups_closed = await _handle_popups(page)
    overlays_removed = await _remove_overlays(page)
    accordion_result = await _expand_accordions(page)
    scroll_result = await _scroll_to_load_content(page)
    stability_result = await _wait_for_content_stability(page)

    await asyncio.sleep(2.0)

    log_event(
        logger,
        "info",
        "page_prepared cookie_handled=%s popups_closed=%s overlays_removed=%s "
        "accordions_dom=%s accordions_clicked=%s accordions_forced=%s "
        "scroll_count=%s content_stable=%s stability_wait_seconds=%s",
        cookie_handled,
        popups_closed,
        overlays_removed,
        accordion_result["dom_expanded"],
        accordion_result["click_triggered"],
        accordion_result["force_revealed"],
        scroll_result["scroll_count"],
        stability_result["stable"],
        stability_result["elapsed_seconds"],
        domain=_page_domain(page),
        dynamic_app=dynamic_app_result,
        cookie_handled=cookie_handled,
        popups_closed=popups_closed,
        overlays_removed=overlays_removed,
        accordions_dom=accordion_result["dom_expanded"],
        accordions_clicked=accordion_result["click_triggered"],
        accordions_forced=accordion_result["force_revealed"],
        scroll_count=scroll_result["scroll_count"],
        content_stable=stability_result["stable"],
        stability_wait_seconds=stability_result["elapsed_seconds"],
    )
    return {
        "page_ready": True,
        "dynamic_app": dynamic_app_result,
        "cookie_handled": cookie_handled,
        "popups_closed": popups_closed,
        "overlays_removed": overlays_removed,
        "accordions_expanded": accordion_result,
        "scroll_count": scroll_result["scroll_count"],
        "initial_scroll_count": scroll_result["scroll_count"],
        "follow_up_scroll_count": 0,
        "final_scroll_height": scroll_result["final_height"],
        "content_stable": bool(stability_result["stable"]),
        "stability_wait_seconds": float(stability_result["elapsed_seconds"]),
        "stability_samples": int(stability_result["samples"]),
        "stable_interactive_count": int(stability_result["interactive_count"]),
        "stable_text_length": int(stability_result["text_length"]),
        "stable_scroll_height": int(stability_result["scroll_height"]),
        "final_wait_seconds": 2.0,
    }


async def extract_page_content(
    page: Page | None,
    sections: list[str] | None = None,
    custom_script: str | None = None,
) -> dict | None:
    if page is None:
        return None

    # preparation = await prepare_page_for_extraction(page)
    script = custom_script or await page_extraction()
    extraction_sections = sections or ["body"]
    result: Any = await page.evaluate(script, {"sections": extraction_sections})
    
    if not isinstance(result, dict):
        return None

    title = await page.title()
    page_url = str(result.get("page_url", "") or page.url or "")
    content = str(result.get("content", "") or "")
    selector_map = result.get("selector_map", {})
    content = _append_missing_selector_links(content, dict(selector_map or {}))
    log_event(
        logger,
        "info",
        "content_extracted page_url=%s markdown_length=%s",
        page_url,
        len(content),
        domain=urlparse(page_url).netloc.lower() or _page_domain(page),
        page_url=page_url,
        markdown_length=len(content),
    )
    return {
        "title": title or "",
        "url": page_url,
        "markdown": content,
        "metadata": {
            "sections": extraction_sections,
            "selector_map": dict(selector_map or {}),
            # "preparation": preparation,
        },
    }
