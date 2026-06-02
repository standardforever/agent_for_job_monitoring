import re

from playwright.async_api import Page

from utils.logging import get_logger, log_event


logger = get_logger("job_pattern_html_extraction")


async def extract_clean_html(
    page: Page,
    text_chars_per_node: int = 80,
) -> str:
    page_url = getattr(page, "url", "") or ""
    log_event(
        logger,
        "info",
        "job_pattern_clean_html_extraction_started url=%s",
        page_url,
        domain=page_url or "job_pattern",
        page_url=page_url,
        text_chars_per_node=text_chars_per_node,
    )

    try:
        html = await page.evaluate(
            """
            ({ textCharsPerNode }) => {
                const clone = document.documentElement.cloneNode(true);
                const REMOVE_TAGS = [
                    'head', 'header', 'footer', 'nav',
                    'script', 'style', 'noscript', 'meta', 'link',
                    'img', 'iframe', 'svg', 'canvas',
                ];
                REMOVE_TAGS.forEach(tag => {
                    clone.querySelectorAll(tag).forEach(el => el.remove());
                });

                clone.querySelectorAll('ins.adsbygoogle').forEach(el => el.remove());

                const COOKIE_EXACT_IDS = [
                    'cookie-banner', 'cookie-bar', 'cookie-notice',
                    'consent-banner', 'gdpr-banner', 'cookie-consent',
                    'cookieConsent', 'cookieBanner', 'cookie_banner'
                ];
                COOKIE_EXACT_IDS.forEach(id => {
                    const el = clone.querySelector(`#${id}`);
                    if (el) el.remove();
                });

                const COOKIE_HINTS = [
                    'cookie', 'consent', 'gdpr', 'privacy', 'ot-sdk',
                    'onetrust', 'trustarc', 'cc-window', 'cc-banner',
                    'cli-modal', 'cookielawinfo', 'wt-cli'
                ];
                clone.querySelectorAll('*').forEach(el => {
                    const idText = (el.id || '').toLowerCase();
                    const classText = (el.className || '').toString().toLowerCase();
                    const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
                    const text = (el.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    const hintMatch = COOKIE_HINTS.some(
                        hint =>
                            idText.includes(hint) ||
                            classText.includes(hint) ||
                            ariaLabel.includes(hint)
                    );

                    const looksLikeCookiePanel =
                        hintMatch ||
                        (
                            text.length > 0 &&
                            text.length < 1000 &&
                            (
                                text.includes('cookie settings') ||
                                text.includes('privacy preference') ||
                                text.includes('save & accept') ||
                                text.includes('accept all cookies')
                            )
                        );

                    if (looksLikeCookiePanel) {
                        el.remove();
                    }
                });

                clone.querySelectorAll('[style]').forEach(el => el.removeAttribute('style'));
                clone.querySelectorAll('*').forEach(el => {
                    if (el.hasAttribute('id')) {
                        el.removeAttribute('id');
                    }
                    Array.from(el.attributes).forEach(attr => {
                        if (
                            attr.name.startsWith('data-') &&
                            !(attr.value || '').trim()
                        ) {
                            el.removeAttribute(attr.name);
                        }
                    });
                });

                const removeComments = (node) => {
                    node.childNodes.forEach(child => {
                        if (child.nodeType === 8) child.remove();
                        else removeComments(child);
                    });
                };
                removeComments(clone);

                const truncateTextNodes = (root, maxLen) => {
                    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
                    const nodes = [];
                    let current = walker.nextNode();

                    while (current) {
                        nodes.push(current);
                        current = walker.nextNode();
                    }

                    nodes.forEach(node => {
                        const normalized = node.textContent.replace(/\\s+/g, ' ').trim();
                        if (!normalized) {
                            node.textContent = '';
                            return;
                        }
                        node.textContent = normalized.length > maxLen
                            ? `${normalized.slice(0, maxLen)}...`
                            : normalized;
                    });
                };

                truncateTextNodes(clone, textCharsPerNode);
                const body = clone.querySelector('body');
                return body ? body.innerHTML : clone.outerHTML;
            }
            """,
            {"textCharsPerNode": text_chars_per_node},
        )

        html = html.replace("<!---->", "")
        html = html.replace("<!-- -->", "")
        html = html.replace("<!---->", "")
        html = re.sub(r"<svg\b[^>]*>.*?</svg>", "", html, flags=re.I | re.S)

        log_event(
            logger,
            "info",
            "job_pattern_clean_html_extraction_completed url=%s html_length=%s",
            page_url,
            len(html),
            domain=page_url or "job_pattern",
            page_url=page_url,
            html_length=len(html),
        )
        return html
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "job_pattern_clean_html_extraction_failed url=%s error=%s",
            page_url,
            str(exc),
            domain=page_url or "job_pattern",
            page_url=page_url,
            error=str(exc),
        )
        raise
