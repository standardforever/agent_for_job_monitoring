# def create_job_page_analysis_prompt(url: str | None, text: str, interactive_links: str | None = None) -> str:
#     return f"""Analyze the webpage and classify its job-related status. Never hallucinate — only use what is explicitly in the page text.

# URL: {url}

# PAGE CONTENT:
# {text}

# VISIBLE INTERACTIVE LINKS/BUTTONS:
# {interactive_links or "None"}

# ---

# ## PAGE CATEGORIES (choose ONE)

# | Category | When to use |
# |---|---|
# | `jobs_listed` | Multiple job postings visible on this page |
# | `job_listings_preview_page` | Subset of jobs shown + link to view ALL jobs elsewhere |
# | `navigation_required` | No jobs visible but page has an explicit clickable link or button pointing to where jobs are listed |
# | `single_job_posting` | One specific job described (detailed or minimal) |
# | `not_job_related` | No job/career/hiring content at all |
# | `jobs_related_general_info` | Page is clearly about careers, recruitment, working here, or general job enquiries, but it does NOT list current vacancies and does NOT explicitly say there are currently no vacancies |
# | `jobs_related_no_vacancies` | Page is job/career/hiring related but has no open roles listed AND no clickable navigation target to jobs — includes pages with only a contact email, phone number, or general enquiry message |

# ---

# ## KEY RULES

# **URLs:**
# - If `page_category` is `single_job_posting` → `job_url` = {url}
# - If a job is listed but its `job_url` cannot be resolved from the page → `job_url` = {url}
# - Never leave `job_url` as null if {url} is available as a fallback.

# RULE — Resolving job_url from extracted links:
# BASE URL: {url}
# - Case 1 — Link starts with "/":
#   Take the Base URL above, remove everything after the domain name, then attach the link.
#   The domain name includes any subdomain — do not remove it.

#   BASE: https://www.site.com/community/jobs   +  /role-x  →  https://www.site.com/role-x
#   BASE: https://jobs.site.com/board           +  /role-x  →  https://jobs.site.com/role-x

# - Case 2 — Link starts with "http://" or "https://":
#   Use it exactly as found. Do not change anything.
# - Do not guess, infer, or modify URLs beyond these two cases.

# **`navigation_required` means a real clickable destination exists:**
# Use `navigation_required` ONLY when there is an explicit clickable link or button that points to where jobs are listed.
# - "Click here to view our vacancies → [link]" → `navigation_required`
# - "See all jobs on our careers portal → [link]" → `navigation_required`
# - "Learn about careers at our company" with no live vacancies and no explicit "no vacancies" statement → `jobs_related_general_info`
# - "Work with us", "why join us", "careers information", "recruitment process", or "send your CV" style content with no live jobs and no explicit "currently no vacancies" statement → `jobs_related_general_info`
# - "Please contact Sophie at sophie@example.com for job enquiries" → `jobs_related_general_info`
# - "No vacancies right now, email hr@example.com to register interest" → `jobs_related_no_vacancies`
# - "Call us on 020 1234 5678 to discuss opportunities" → `jobs_related_general_info`
# If the ONLY thing on the page pointing toward jobs is a contact email, phone number, or general enquiry message, and the page does NOT explicitly say there are no current vacancies, use `jobs_related_general_info`.
# If the page explicitly says there are no current vacancies and only offers a contact route, use `jobs_related_no_vacancies`.

# **Distinguish general careers info from explicit no-vacancy pages:**
# - Use `jobs_related_general_info` when the page talks about careers or recruitment in general but does NOT explicitly say there are no current vacancies/open roles right now.
# - Use `jobs_related_no_vacancies` only when the page explicitly indicates there are no current vacancies, no open roles, no positions available at the moment, or equivalent wording.
# - Do not use `jobs_related_no_vacancies` just because a page is careers-related and has no visible jobs. That wastes rerun checks.

# **Email links are NOT navigation targets:** Any link starting with `mailto:` is an email address, not a webpage.
# Never put a `mailto:` link in `next_action_target.url`. Never classify a page as `navigation_required` solely because it contains an email address.
# If the only "contact" available is an email address and no jobs are listed:
# - use `jobs_related_no_vacancies` only if the page explicitly says there are no current vacancies
# - otherwise use `jobs_related_general_info`

# **Job alert:** Set `job_alert = true` only if page explicitly mentions signing up for vacancy/job alert notifications.

# **Navigation vs job links:** Links next to job titles (Apply, View Details, More Info) are `job_url`, not navigation targets.

# **Preview page:** If SOME jobs are shown AND a "view all" link exists → `job_listings_preview_page`, populate `next_action_target`.

# **Selector map links:** The `VISIBLE INTERACTIVE LINKS/BUTTONS` section is extracted from the DOM selector map. If page text mentions a navigation link/button but the URL is missing from the markdown, use the matching URL from this section.
# Never pick a `mailto:` link from the selector map as a navigation target.

# **Access-first rule:**
# - If the page content clearly shows an access barrier like `403 Forbidden`, `Access denied`, `login required`, CAPTCHA, Cloudflare challenge, or similar, set `page_access_status` to the matching non-accessible value.
# - If `page_access_status` is anything other than `accessible`, then set `page_category` = `not_job_related`.
# - When doing this, make the reasoning explicit that the page could not be properly accessed and that job/career content could not be verified.
# - Still return the correct non-accessible `page_access_status` and `page_access_issue_detail`.

# ---

# ## JOBS LISTED ON PAGE (when page_category = `jobs_listed`)

# Do NOT return every job on the page. Return a maximum of 2 sample jobs only:
# - If there is 1 job, return that 1 job.
# - If there are 2 jobs, return both.
# - If there are more than 2 jobs, return only the first visible job and the last visible job.

# For each sampled job extract:
# - `title` — job title
# - `job_url` — full resolved URL or {url} as fallback, never null

# Then classify the page-level listing UI and pagination:

# **ui_category** — how jobs are presented:
# | Value | Meaning |
# |---|---|
# | `linked_cards` | Each job links to its own detail page |
# | `embedded_only` | Full details inline, NO separate detail page — job_url MUST be null |
# | `modal_popup` | Clicking opens an overlay on the same page |
# | `expandable_accordion` | Jobs expand in-place on click |
# | `apply_inline` | Apply form embedded directly on listing page |
# | `external_redirect` | Job links point directly to an external recruitment domain |

# **pagination_type** — if pagination controls are visible:
# | Value | Meaning |
# |---|---|
# | `numbered` | Numbered page links (1, 2, 3…) |
# | `next_prev` | Next / Previous buttons only |
# | `load_more` | Single "Load More" or "Show More" button |
# | `infinite_scroll` | Auto-loading / infinite scroll referenced |
# | `cursor_based` | URL uses cursor, token, or offset param |
# | `alphabet` | A–Z letter navigation |

# ---

# ## PAGE ACCESS STATUS

# | Value | When |
# |---|---|
# | `accessible` | Loaded normally |
# | `bot_detected` | CAPTCHA, Cloudflare challenge, access denied |
# | `login_required` | Auth required to view content |
# | `not_found` | 404 or page not found |
# | `empty_or_blank` | Loaded but no meaningful content |
# | `error` | 500 / 503 / maintenance page |

# If NOT accessible, still attempt classification at lower confidence.

# ---

# ## RESPONSE SCHEMA

# Return ONLY valid JSON. No markdown, no extra text. Start with {{ end with }}.

# {{
#     "page_category": "jobs_listed" | "job_listings_preview_page" | "navigation_required" | "single_job_posting" | "not_job_related" | "jobs_related_general_info" | "jobs_related_no_vacancies",
#     "confidence": <float 0.0–1.0>,
#     "reasoning": "<concise explanation>",
#     "job_alert": boolean | null,
#     "page_access_status": "accessible" | "bot_detected" | "login_required" | "not_found" | "empty_or_blank" | "error",
#     "page_access_issue_detail": "<short description or null>",
#     "next_action_target": {{
#         "url": "<URL or null — never a mailto: link>",
#         "button": "<text or null>",
#         "element_type": "link" | "button" | null
#     }},
#     "jobs_listed_on_page": [
#         {{
#             "title": "<job title>",
#             "job_url": "<full resolved URL or {url} as fallback, never null>"
#         }}
#     ],
#     "listing_ui": {{
#         "ui_category": "linked_cards" | "embedded_only" | "modal_popup" | "expandable_accordion" | "apply_inline" | "external_redirect" | null,
#         "filter_present": boolean,
#         "filter_types": ["<filter label>"],
#         "sort_present": boolean,
#         "sort_types": ["<sort label>"],
#         "pagination_present": boolean,
#         "pagination_type": "numbered" | "next_prev" | "load_more" | "infinite_scroll" | "cursor_based" | "alphabet" | null,
#         "next_page_url": "<URL or null>"
#     }}
# }}

# Note: `listing_ui` fields should be null/empty when page_category is not `jobs_listed`.
# """





def create_job_page_analysis_prompt(url: str | None, text: str, interactive_links: str | None = None) -> str:
    return f"""Analyze the webpage and classify its job-related status. Never hallucinate — only use what is explicitly in the page text.

URL: {url}

PAGE CONTENT:
{text}

VISIBLE INTERACTIVE LINKS/BUTTONS:
{interactive_links or "None"}

---

## PAGE CATEGORIES (choose ONE)

| Category | When to use |
|---|---|
| `jobs_listed` | Multiple job postings visible on this page |
| `job_listings_preview_page` | Subset of jobs shown + link to view ALL jobs elsewhere |
| `navigation_required` | No jobs visible but page has an explicit clickable link or button pointing to where jobs are listed |
| `single_job_posting` | One specific job described (detailed or minimal) |
| `not_job_related` | No job/career/hiring content at all |
| `jobs_related_general_info` | Page is clearly about careers, recruitment, working here, or general job enquiries, but it does NOT list current vacancies and does NOT explicitly say there are currently no vacancies |
| `jobs_related_no_vacancies` | Page is job/career/hiring related but has no open roles listed AND no clickable navigation target to jobs — includes pages with only a contact email, phone number, or general enquiry message |

---

## KEY RULES

**URLs:**
- If `page_category` is `single_job_posting` → `job_url` = {url}
- If a job is listed but its `job_url` cannot be resolved from the page → `job_url` = {url}
- Never leave `job_url` as null if {url} is available as a fallback.

RULE — Resolving job_url from extracted links:
BASE URL: {url}
- Case 1 — Link starts with "/":
  Take the Base URL above, remove everything after the domain name, then attach the link.
  The domain name includes any subdomain — do not remove it.

  BASE: https://www.site.com/community/jobs   +  /role-x  →  https://www.site.com/role-x
  BASE: https://jobs.site.com/board           +  /role-x  →  https://jobs.site.com/role-x

- Case 2 — Link starts with "http://" or "https://":
  Use it exactly as found. Do not change anything.
- Do not guess, infer, or modify URLs beyond these two cases.

**`navigation_required` means a real clickable destination exists:**
Use `navigation_required` ONLY when there is an explicit clickable link or button that points to where jobs are listed.
- "Click here to view our vacancies → [link]" → `navigation_required`
- "See all jobs on our careers portal → [link]" → `navigation_required`
- "Learn about careers at our company" with no live vacancies and no explicit "no vacancies" statement → `jobs_related_general_info`
- "Work with us", "why join us", "careers information", "recruitment process", or "send your CV" style content with no live jobs and no explicit "currently no vacancies" statement → `jobs_related_general_info`
- "Please contact Sophie at sophie@example.com for job enquiries" → `jobs_related_general_info`
- "No vacancies right now, email hr@example.com to register interest" → `jobs_related_no_vacancies`
- "Call us on 020 1234 5678 to discuss opportunities" → `jobs_related_general_info`
If the ONLY thing on the page pointing toward jobs is a contact email, phone number, or general enquiry message, and the page does NOT explicitly say there are no current vacancies, use `jobs_related_general_info`.
If the page explicitly says there are no current vacancies and only offers a contact route, use `jobs_related_no_vacancies`.

**Distinguish general careers info from explicit no-vacancy pages:**
- Use `jobs_related_general_info` when the page talks about careers or recruitment in general but does NOT explicitly say there are no current vacancies/open roles right now.
- Use `jobs_related_no_vacancies` only when the page explicitly indicates there are no current vacancies, no open roles, no positions available at the moment, or equivalent wording.
- Do not use `jobs_related_no_vacancies` just because a page is careers-related and has no visible jobs. That wastes rerun checks.

**Email links are NOT navigation targets:** Any link starting with `mailto:` is an email address, not a webpage.
Never put a `mailto:` link in `next_action_target.url`. Never classify a page as `navigation_required` solely because it contains an email address.
If the only "contact" available is an email address and no jobs are listed:
- use `jobs_related_no_vacancies` only if the page explicitly says there are no current vacancies
- otherwise use `jobs_related_general_info`

**Job alert:** Set `job_alert = true` only if page explicitly mentions signing up for vacancy/job alert notifications.

**Navigation vs job links:** Links next to job titles (Apply, View Details, More Info) are `job_url`, not navigation targets.

**Preview page:** If SOME jobs are shown AND a "view all" link exists → `job_listings_preview_page`, populate `next_action_target`.

**Selector map links:** The `VISIBLE INTERACTIVE LINKS/BUTTONS` section is extracted from the DOM selector map. If page text mentions a navigation link/button but the URL is missing from the markdown, use the matching URL from this section.
Never pick a `mailto:` link from the selector map as a navigation target.

**Access-first rule:**
- If the page content clearly shows an access barrier like `403 Forbidden`, `Access denied`, `login required`, CAPTCHA, Cloudflare challenge, or similar, set `page_access_status` to the matching non-accessible value.
- If `page_access_status` is anything other than `accessible`, then set `page_category` = `not_job_related`.
- When doing this, make the reasoning explicit that the page could not be properly accessed and that job/career content could not be verified.
- Still return the correct non-accessible `page_access_status` and `page_access_issue_detail`.

---

## JOBS LISTED ON PAGE (when page_category = `jobs_listed`)

Do NOT return every job on the page. Return a maximum of 2 sample jobs only:
- If there is 1 job, return that 1 job.
- If there are 2 jobs, return both.
- If there are more than 2 jobs, return only the first visible job and the last visible job.

For each sampled job extract:
- `title` — job title
- `job_url` — full resolved URL or {url} as fallback, never null

Then classify the page-level listing UI and pagination:

**ui_category** — how jobs are presented:
| Value | Meaning |
|---|---|
| `linked_cards` | Each job links to its own detail page |
| `embedded_only` | Full details inline, NO separate detail page — job_url MUST be null |
| `modal_popup` | Clicking opens an overlay on the same page |
| `expandable_accordion` | Jobs expand in-place on click |
| `apply_inline` | Apply form embedded directly on listing page |
| `external_redirect` | Job links point directly to an external recruitment domain |

**pagination** — classify pagination controls if visible:

pagination_category (use exact strings):
  numbered_buttons     — clickable page number buttons (1 2 3 … Next)
  next_prev_buttons    — only Prev/Next buttons, no numbers
  load_more_button     — single button appending more results below
  view_all_button      — loads every result on one page
  show_more_expand     — expands a collapsed list inline (no network call)
  url_query_page       — URL contains ?page=N or &page=N
  url_path_segment     — URL contains /page/N/ or /p/N/
  url_offset_limit     — URL contains ?offset=N, ?start=N, or ?from=N
  url_cursor           — URL contains an opaque cursor/token for next page
  hash_fragment        — URL uses #page=2 or #/jobs/2 pattern
  form_post_pagination — pagination triggered by a POST form submission
  per_page_selector    — dropdown to change results per page (10/25/50)
  tab_sections         — jobs split across clickable category/department tabs
  accordion_sections   — expandable sections each containing a subset of jobs
  modal_drawer         — jobs load inside an overlay or slide-in drawer
  no_pagination        — all results already present on the page

pagination_navigation_method — how to advance to the next page:
  link        — plain ; URL can be composed manually without JS
  js_click    — requires a browser click (JS event handler; href is # or absent)
  form_submit — requires a form POST submission

---

## PAGE ACCESS STATUS

| Value | When |
|---|---|
| `accessible` | Loaded normally |
| `bot_detected` | CAPTCHA, Cloudflare challenge, access denied |
| `login_required` | Auth required to view content |
| `not_found` | 404 or page not found |
| `empty_or_blank` | Loaded but no meaningful content |
| `error` | 500 / 503 / maintenance page |

If NOT accessible, still attempt classification at lower confidence.

---

## RESPONSE SCHEMA

Return ONLY valid JSON. No markdown, no extra text. Start with {{ end with }}.

{{
    "page_category": "jobs_listed" | "job_listings_preview_page" | "navigation_required" | "single_job_posting" | "not_job_related" | "jobs_related_general_info" | "jobs_related_no_vacancies",
    "confidence": ,
    "job_alert": boolean | null,
    "page_access_status": "accessible" | "bot_detected" | "login_required" | "not_found" | "empty_or_blank" | "error",
    "page_access_issue_detail": "",
    "next_action_target": {{
        "url": "",
        "button": "",
        "element_type": "link" | "button" | null
    }},
    "jobs_listed_on_page": [
        {{
            "title": "",
            "job_url": ""
        }}
    ],
    "listing_ui": {{
        "ui_category": "linked_cards" | "embedded_only" | "modal_popup" | "expandable_accordion" | "apply_inline" | "external_redirect" | null,
        "filter_present": boolean,
        "filter_types": [""],
        "sort_present": boolean,
        "sort_types": [""],
        "pagination_present": boolean,
        "pagination_category": "numbered_buttons" | "next_prev_buttons" | "load_more_button" | "view_all_button" | "show_more_expand" | "url_query_page" | "url_path_segment" | "url_offset_limit" | "url_cursor" | "hash_fragment" | "form_post_pagination" | "per_page_selector" | "tab_sections" | "accordion_sections" | "modal_drawer" | "no_pagination" | null,
        "pagination_navigation_method": "link" | "js_click" | "form_submit" | null
    }}
}}

Note: `listing_ui` fields should be null/empty when page_category is not `jobs_listed`.
"""