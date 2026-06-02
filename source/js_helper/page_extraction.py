async def page_extraction() -> str:
    js_code = r"""
    () => {
        // ═══════════════════════════════════════════════
        // HELPERS
        // ═══════════════════════════════════════════════
        function clean(text) {
            return (text || '').replace(/[\t\r\n\u00a0]+/g, ' ').replace(/  +/g, ' ').trim();
        }

        // Always use textContent (not innerText) — innerText returns '' for
        // hidden elements (display:none, visibility:hidden), which would cause
        // accordion panels, dropdown menus, and tab content to be silently lost.
        function getText(el) {
            return clean(el.textContent || '');
        }

        function resolveUrl(el) {
            if (el.href && !el.href.startsWith('javascript')) return el.href;

            const href = el.getAttribute('href');
            if (href && href !== '#' && !href.startsWith('javascript')) {
                try { return new URL(href, location.href).href; } catch(e) {}
            }

            // Elementor wrapper link
            const epw = el.getAttribute('data-ep-wrapper-link');
            if (epw) {
                try {
                    const p = JSON.parse(epw.replace(/&quot;/g, '"'));
                    if (p.url) return new URL(p.url, location.href).href;
                } catch(e) {}
            }

            // Generic data-* url attributes
            for (const attr of ['data-href','data-url','data-link','data-action','data-redirect','data-navigate','data-permalink']) {
                const v = el.getAttribute(attr);
                if (v && (v.startsWith('http') || v.startsWith('/'))) {
                    try { return new URL(v, location.href).href; } catch(e) {}
                }
            }

            // onclick patterns
            const onclick = el.getAttribute('onclick') || '';
            if (onclick) {
                for (const re of [
                    /location\.href\s*=\s*['"]([^'"]+)['"]/,
                    /window\.location\s*=\s*['"]([^'"]+)['"]/,
                    /['"]((https?:\/\/|\/)[^'"]{3,})['"]/,
                ]) {
                    const m = onclick.match(re);
                    if (m) { try { return new URL(m[1], location.href).href; } catch(e) {} }
                }
            }

            return null;
        }

        function getLabel(el) {
            // Use textContent — works even when element is hidden
            const t = getText(el);
            if (t) return t;
            const img = el.querySelector('img');
            if (img) { const alt = clean(img.getAttribute('alt') || ''); if (alt) return alt; }
            const svgTitle = el.querySelector('svg title');
            if (svgTitle) { const t2 = clean(svgTitle.textContent); if (t2) return t2; }
            return clean(el.getAttribute('aria-label') || el.getAttribute('title') || el.getAttribute('value') || '');
        }

        function stableId(path) {
            let hash = 0;
            for (let i = 0; i < path.length; i++) hash = ((hash << 5) - hash + path.charCodeAt(i)) | 0;
            return Math.abs(hash % 9000) + 1000;
        }

        function buildDomPath(el) {
            const segs = [];
            let cur = el;
            while (cur && cur instanceof Element) {
                const tag = cur.tagName.toLowerCase();
                const parent = cur.parentElement;
                if (!parent) { segs.push(`${tag}[0]`); break; }
                const siblings = Array.from(parent.children).filter(c => c.tagName === cur.tagName);
                segs.push(`${tag}[${Math.max(siblings.indexOf(cur), 0)}]`);
                cur = parent;
            }
            return segs.reverse().join('/');
        }

        function getAttributes(el) {
            const attrs = {};
            const KEEP = new Set(['id','name','type','role','placeholder','value','title','alt',
                                   'aria-label','aria-expanded','aria-haspopup','href']);
            for (const attr of Array.from(el.attributes || [])) {
                if (!attr.value) continue;
                const n = (attr.name || '').toLowerCase();
                if (KEEP.has(n) || n.includes('url') || n.includes('href') || n.includes('link'))
                    attrs[attr.name] = clean(attr.value);
            }
            return attrs;
        }

        // ═══════════════════════════════════════════════
        // EXCLUSION
        // Only exclude genuine site chrome: header, footer,
        // top-level nav. Never exclude by visibility.
        // ═══════════════════════════════════════════════
        const excluded = new Set();

        document.querySelectorAll('header, footer, [role="banner"], [role="contentinfo"]')
            .forEach(el => excluded.add(el));

        // Exclude only top-level navs (not navs inside main content)
        const CONTENT_ANCESTORS = new Set(['main','article','section','aside']);
        document.querySelectorAll('nav, [role="navigation"]').forEach(nav => {
            let cur = nav.parentElement;
            let inContent = false;
            while (cur && cur !== document.body) {
                const tag  = cur.tagName.toLowerCase();
                const role = (cur.getAttribute('role') || '').toLowerCase();
                const id   = (cur.getAttribute('id') || '').toLowerCase();
                const cls  = (cur.getAttribute('class') || '').toLowerCase();
                if (CONTENT_ANCESTORS.has(tag) || CONTENT_ANCESTORS.has(role) ||
                    id.includes('content') || id.includes('main') ||
                    cls.includes('content') || cls.includes('main')) {
                    inContent = true; break;
                }
                cur = cur.parentElement;
            }
            if (!inContent) excluded.add(nav);
        });

        function isExcluded(el) {
            let node = el;
            while (node && node !== document.body) {
                if (excluded.has(node)) return true;
                node = node.parentElement;
            }
            return false;
        }

        // ═══════════════════════════════════════════════
        // SELECTOR MAP
        // ═══════════════════════════════════════════════
        const selectorMap = {};

        function registerInteractive(el, kind) {
            const domPath = buildDomPath(el);
            const nodeId  = stableId(`frame-0:${domPath}`);
            if (selectorMap[String(nodeId)]) return;
            selectorMap[String(nodeId)] = {
                node_id:     nodeId,
                tag:         el.tagName.toLowerCase(),
                kind,
                label:       getLabel(el),
                attributes:  getAttributes(el),
                dom_path:    domPath,
                frame_index: 0,
                frame_url:   window.location.href,
                interactive: true,
                expanded:    el.hasAttribute('aria-expanded') ? el.getAttribute('aria-expanded') === 'true' : null,
                action_url:  resolveUrl(el) || null,
                is_link:     kind === 'link',
                is_button:   kind === 'button',
            };
        }

        // ═══════════════════════════════════════════════
        // CORE WALKER
        //
        // Rules:
        //   - NEVER skip by visibility — display:none / visibility:hidden
        //     elements contain real content (dropdowns, accordions, tabs).
        //   - NEVER skip aria-hidden — used on decorative icon spans inside
        //     real interactive elements.
        //   - Only skip: excluded chrome, script/style/meta tags, and
        //     elements already visited.
        //   - Always use textContent (not innerText) so hidden text is read.
        // ═══════════════════════════════════════════════
        const lines   = [];
        const visited = new Set();

        const SKIP_TAGS    = new Set(['script','style','noscript','meta','link','head']);
        const HEADING_TAGS = new Set(['h1','h2','h3','h4','h5','h6']);
        const VOID_TAGS    = new Set(['br','hr','img','input','area','base','col','embed','param','source','track','wbr']);

        function walk(node) {
            if (!node || visited.has(node)) return;
            visited.add(node);

            // TEXT NODE — emit directly (no visibility check)
            if (node.nodeType === Node.TEXT_NODE) {
                const t = clean(node.textContent);
                if (t) lines.push(t);
                return;
            }

            if (node.nodeType !== Node.ELEMENT_NODE) return;

            const el  = node;
            const tag = el.tagName.toLowerCase();

            // Skip excluded chrome
            if (isExcluded(el)) return;

            // Skip only non-content structural tags — never skip by visibility
            if (SKIP_TAGS.has(tag)) return;

            // ── data-ep-wrapper-link (Elementor clickable div/section with no <a> tag)
            //    Also handles cursor:pointer divs with any data-* url attribute.
            //    Must be checked BEFORE recursing so the URL is not lost.
            const epWrapperUrl = resolveUrl(el);
            const isClickableWrapper = (
                el.hasAttribute('data-ep-wrapper-link') ||
                el.hasAttribute('data-href') ||
                el.hasAttribute('data-url') ||
                el.hasAttribute('data-permalink') ||
                el.hasAttribute('data-link') ||
                (el.getAttribute('style') || '').includes('cursor: pointer') ||
                (el.getAttribute('style') || '').includes('cursor:pointer')
            );
            if (isClickableWrapper && epWrapperUrl) {
                // Emit the full text content of this block as a single linked line
                const label = getText(el);
                registerInteractive(el, 'link');
                lines.push(label ? label + ' → ' + epWrapperUrl : '[link] → ' + epWrapperUrl);
                // Still recurse so sub-elements (headings, text) are emitted in context
                // but mark the wrapper itself so we don't double-emit its flat text
                el.childNodes.forEach(child => walk(child));
                return;
            }

            // ── Images: emit alt text regardless of visibility
            if (tag === 'img') {
                const alt = clean(el.getAttribute('alt') || '');
                if (alt) lines.push('[IMAGE: ' + alt + ']');
                return;
            }

            // ── SVG: extract title and any <text> elements (charts, diagrams, labels)
            if (tag === 'svg') {
                const title = el.querySelector('title');
                if (title) { const t = clean(title.textContent); if (t) lines.push('[SVG: ' + t + ']'); }
                el.querySelectorAll('text').forEach(t => {
                    const txt = clean(t.textContent);
                    if (txt) lines.push(txt);
                });
                el.querySelectorAll('*').forEach(c => visited.add(c));
                return;
            }

            // ── Canvas: emit accessible fallback text content inside <canvas>
            if (tag === 'canvas') {
                const fallback = clean(el.textContent || '');
                if (fallback) lines.push('[CANVAS: ' + fallback + ']');
                el.querySelectorAll('*').forEach(c => visited.add(c));
                return;
            }

            // ── Headings: prefix for LLM structure signal
            //    Use textContent so hidden headings (inside collapsed tabs) are captured
            if (HEADING_TAGS.has(tag)) {
                const text = getText(el);
                if (text) lines.push('\n' + tag.toUpperCase() + ': ' + text + '\n');
                el.querySelectorAll('*').forEach(c => visited.add(c));
                return;
            }

            // ── Anchors: emit text → url, register in selectorMap
            if (tag === 'a') {
                const url   = resolveUrl(el);
                const text  = getText(el);
                const imgAlt = !text ? clean((el.querySelector('img') || {getAttribute:()=>''}).getAttribute('alt') || '') : '';
                const label = text || imgAlt;

                if (url) {
                    registerInteractive(el, 'link');
                    lines.push(label ? label + ' → ' + url : '[link] → ' + url);
                } else if (label) {
                    lines.push(label);
                }
                el.querySelectorAll('*').forEach(c => visited.add(c));
                return;
            }

            // ── Buttons: emit label, register in selectorMap
            if (tag === 'button' ||
                (tag === 'input' && ['button','submit','reset'].includes((el.getAttribute('type')||'').toLowerCase())) ||
                el.getAttribute('role') === 'button') {
                const text = getText(el) || clean(el.getAttribute('value') || el.getAttribute('aria-label') || '');
                const url  = resolveUrl(el);
                registerInteractive(el, 'button');
                if (text) lines.push(url ? '[BUTTON: ' + text + '] → ' + url : '[BUTTON: ' + text + ']');
                el.querySelectorAll('*').forEach(c => visited.add(c));
                return;
            }

            // ── Definition terms/values: format as key: value for LLM clarity
            if (tag === 'dt') {
                const text = getText(el);
                if (text) lines.push(text + ':');
                el.querySelectorAll('*').forEach(c => visited.add(c));
                return;
            }

            if (tag === 'dd') {
                const text = getText(el);
                if (text) lines.push('  ' + text);
                el.querySelectorAll('*').forEach(c => visited.add(c));
                return;
            }

            // ── Everything else: recurse into children
            //    This includes hidden divs, collapsed accordions, invisible tabs —
            //    their text nodes will be emitted by the TEXT_NODE branch above
            el.childNodes.forEach(child => walk(child));
        }

        // Walk entire body — no visibility gating at the top level either
        document.body.childNodes.forEach(child => walk(child));

        // ═══════════════════════════════════════════════
        // REGISTER interactive elements into selectorMap
        // Include hidden ones — they are clickable targets
        // ═══════════════════════════════════════════════
        document.querySelectorAll(
            'a[href], a[data-href], a[onclick], button, ' +
            '[role="button"], [role="link"], ' +
            '[data-href], [data-url], [data-permalink], [data-ep-wrapper-link], ' +
            '[aria-controls], [aria-expanded]'
        ).forEach(el => {
            if (isExcluded(el)) return;
            const tag  = el.tagName.toLowerCase();
            const role = (el.getAttribute('role') || '').toLowerCase();
            let kind = null;
            if (tag === 'a' || role === 'link') kind = 'link';
            else if (tag === 'button' || role === 'button') kind = 'button';
            else if (el.hasAttribute('aria-controls') || el.hasAttribute('aria-expanded')) kind = 'button';
            if (kind) registerInteractive(el, kind);
        });

        // ═══════════════════════════════════════════════
        // ASSEMBLE OUTPUT
        // ═══════════════════════════════════════════════
        const content = lines
            .filter(l => l !== null && l !== undefined)
            .reduce((acc, line) => {
                // Drop exact consecutive duplicates
                if (acc.length && acc[acc.length - 1] === line) return acc;
                // Collapse runs of more than 2 blank lines into 1
                if (!line.trim() && acc.length >= 2 && !acc[acc.length-1].trim() && !acc[acc.length-2].trim()) return acc;
                acc.push(line);
                return acc;
            }, [])
            .join('\n');

        return {
            page_url:     window.location.href,
            content,
            selector_map: selectorMap,
        };
    }
    """
    return js_code