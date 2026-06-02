import json
import re
from typing import Any, Dict, Optional

from bs4 import BeautifulSoup, Comment
from openai import OpenAI

from services.openai_service import create_openai_text_response, resolve_openai_api_key, resolve_openai_model
from utils.logging import get_logger, log_event

try:
    import tiktoken
except ImportError:
    tiktoken = None


logger = get_logger("job_pattern_patterns")
MODEL = "gpt-5-nano"
DEFAULT_MODEL_INPUT_TOKEN_LIMIT = 128000
DEFAULT_OUTPUT_TOKEN_RESERVE = 4000
MODEL_INPUT_TOKEN_LIMITS = {
    "gpt-5-nano": 128000,
}

DISCOVERY_SYSTEM_PROMPT = (
    "You generate precise job extraction patterns from HTML. "
    "Stay focused on job container, title, URL, and simple validation only."
)

CORRECTION_SYSTEM_PROMPT = (
    "You repair failed job extraction patterns. "
    "Your goal is to make the extraction work on the provided HTML with no unnecessary scope."
)

FINAL_REVIEW_SYSTEM_PROMPT = (
    "You are the final reviewer for a job extraction pattern. "
    "Be strict. Only return a pattern that is highly likely to extract real jobs correctly."
)

PATTERN_SCHEMA = {
    "name": "job_listing_pattern",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "is_job_listing_page": {"type": "boolean"},
            "confidence": {"type": "number"},
            "page_structure_summary": {"type": "string"},
            "container_selection_reason": {"type": "string"},
            "job_container_selector": {
                "type": ["string", "null"],
                "description": "CSS selector for the repeating job container.",
            },
            "job_title": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "selector": {"type": ["string", "null"]},
                    "source": {"type": "string", "enum": ["text", "attribute"]},
                    "attribute": {"type": ["string", "null"]},
                },
                "required": ["selector", "source", "attribute"],
            },
            "job_url": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "required": {"type": "boolean"},
                    "strategy": {
                        "type": "string",
                        "enum": [
                            "element_attribute",
                            "container_attribute",
                            "container_json_attribute",
                            "regex_from_container_html",
                            "none",
                        ],
                    },
                    "selector": {"type": ["string", "null"]},
                    "attribute": {"type": ["string", "null"]},
                    "json_path": {"type": ["string", "null"]},
                    "regex": {"type": ["string", "null"]},
                },
                "required": [
                    "required",
                    "strategy",
                    "selector",
                    "attribute",
                    "json_path",
                    "regex",
                ],
            },
            "validation_rules": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "minimum_jobs_expected": {"type": "integer"},
                    "title_must_not_equal": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "title_must_not_contain_any": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "minimum_title_length": {"type": "integer"},
                    "allow_missing_job_url": {"type": "boolean"},
                    "url_must_contain_any": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "url_must_not_contain_any": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": [
                    "minimum_jobs_expected",
                    "title_must_not_equal",
                    "title_must_not_contain_any",
                    "minimum_title_length",
                    "allow_missing_job_url",
                    "url_must_contain_any",
                    "url_must_not_contain_any",
                ],
            },
            "notes": {"type": "string"},
        },
        "required": [
            "is_job_listing_page",
            "confidence",
            "page_structure_summary",
            "container_selection_reason",
            "job_container_selector",
            "job_title",
            "job_url",
            "validation_rules",
            "notes",
        ],
    },
    "strict": True,
}


def clean_html_for_llm(
    html: str,
    model: str = MODEL,
    max_words_per_text_node: int = 8,
    input_token_limit: Optional[int] = None,
    output_token_reserve: int = DEFAULT_OUTPUT_TOKEN_RESERVE,
    trim_ratio: float = 0.9,
) -> str:
    original_length = len(html)
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "svg", "noscript", "iframe", "canvas"]):
        tag.decompose()

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    allowed_attrs = {"class", "href", "aria-label", "title", "role"}
    for tag in soup.find_all(True):
        attrs = dict(tag.attrs)
        for attr in attrs:
            if attr.startswith("data-"):
                value = tag.attrs.get(attr)
                value_text = "" if value is None else str(value).strip()
                if not value_text:
                    del tag.attrs[attr]
                continue
            if attr in allowed_attrs:
                continue
            del tag.attrs[attr]

    for text_node in soup.find_all(string=True):
        parent_name = getattr(text_node.parent, "name", None)
        if parent_name in {"script", "style", "noscript"}:
            continue

        normalized = re.sub(r"\s+", " ", str(text_node)).strip()
        if not normalized:
            text_node.extract()
            continue

        words = normalized.split()
        if len(words) > max_words_per_text_node:
            normalized = " ".join(words[:max_words_per_text_node]) + " ..."
        text_node.replace_with(normalized)

    text = str(soup)
    text = text.replace("<!---->", "")
    text = text.replace("<!-- -->", "")
    text = text.replace("<!---->", "")
    cleaned_text = re.sub(r"<svg\b[^>]*>.*?</svg>", "", text, flags=re.I | re.S)
    fitted_text = fit_text_to_token_budget(
        text=cleaned_text,
        model=model,
        input_token_limit=input_token_limit,
        output_token_reserve=output_token_reserve,
        trim_ratio=trim_ratio,
    )
    log_event(
        logger,
        "info",
        "job_pattern_html_cleaned model=%s original_length=%s cleaned_length=%s fitted_length=%s",
        model,
        original_length,
        len(cleaned_text),
        len(fitted_text),
        domain="job_pattern",
        model=model,
        original_length=original_length,
        cleaned_length=len(cleaned_text),
        fitted_length=len(fitted_text),
    )
    return fitted_text


def prepare_html_for_llm(
    html: str,
    model: str = MODEL,
    max_words_per_text_node: int = 8,
    input_token_limit: Optional[int] = None,
    output_token_reserve: int = DEFAULT_OUTPUT_TOKEN_RESERVE,
    trim_ratio: float = 0.9,
) -> str:
    log_event(
        logger,
        "info",
        "job_pattern_prepare_html_started model=%s html_length=%s",
        model,
        len(html),
        domain="job_pattern",
        model=model,
        html_length=len(html),
    )
    return clean_html_for_llm(
        html,
        model=model,
        max_words_per_text_node=max_words_per_text_node,
        input_token_limit=input_token_limit,
        output_token_reserve=output_token_reserve,
        trim_ratio=trim_ratio,
    )


def get_model_input_token_limit(model: str, override: Optional[int] = None) -> int:
    if override is not None:
        return override
    return MODEL_INPUT_TOKEN_LIMITS.get(model, DEFAULT_MODEL_INPUT_TOKEN_LIMIT)


def get_token_encoder(model: str):
    if tiktoken is None:
        return None
    try:
        return tiktoken.encoding_for_model(model)
    except KeyError:
        return tiktoken.get_encoding("cl100k_base")


def count_text_tokens(text: str, model: str) -> int:
    encoder = get_token_encoder(model)
    if encoder is not None:
        return len(encoder.encode(text))
    return max(1, len(text) // 4)


def fit_text_to_token_budget(
    text: str,
    model: str,
    input_token_limit: Optional[int] = None,
    output_token_reserve: int = DEFAULT_OUTPUT_TOKEN_RESERVE,
    trim_ratio: float = 0.9,
) -> str:
    token_limit = get_model_input_token_limit(model, override=input_token_limit)
    usable_input_tokens = max(1, token_limit - max(0, output_token_reserve))
    fitted_text = text
    token_count = count_text_tokens(fitted_text, model)
    original_token_count = token_count

    while token_count > usable_input_tokens and len(fitted_text) > 1:
        next_length = max(1, int(len(fitted_text) * trim_ratio))
        if next_length >= len(fitted_text):
            next_length = len(fitted_text) - 1
        fitted_text = fitted_text[:next_length]
        token_count = count_text_tokens(fitted_text, model)

    if token_count != original_token_count:
        log_event(
            logger,
            "info",
            "job_pattern_text_trimmed_for_token_budget model=%s original_tokens=%s final_tokens=%s",
            model,
            original_token_count,
            token_count,
            domain="job_pattern",
            model=model,
            input_token_limit=token_limit,
            usable_input_tokens=usable_input_tokens,
            original_tokens=original_token_count,
            final_tokens=token_count,
            original_length=len(text),
            final_length=len(fitted_text),
        )
    return fitted_text


def generate_pattern_with_llm(
    html: str,
    page_url: str = "",
    prepared_html: Optional[str] = None,
    example_jobs: Optional[list[Dict[str, Any]]] = None,
    model: str = MODEL,
) -> Dict[str, Any]:
    log_event(
        logger,
        "info",
        "job_pattern_llm_discovery_started url=%s model=%s html_length=%s example_job_count=%s",
        page_url,
        model,
        len(html),
        len(example_jobs or []),
        domain=page_url or "job_pattern",
        page_url=page_url,
        model=model,
        html_length=len(html),
        example_job_count=len(example_jobs or []),
    )
    model = resolve_openai_model(model)
    client = OpenAI(api_key=resolve_openai_api_key())
    cleaned_html = prepared_html or prepare_html_for_llm(html, model=model)
    example_jobs_text = json.dumps(example_jobs or [], indent=2)

    prompt = f"""
You are an HTML job extraction pattern generator.

Task:
Analyze the HTML and return only the pattern needed to extract jobs from this page.

Focus only on job extraction:
- identify the repeating job container
- identify how to read the job title
- identify how to read the job URL, if one exists
- identify simple validation rules to help reject non-job content

Important rules:
- Return selectors and extraction rules, not the final job data.
- Prefer stable selectors and semantic classes.
- Avoid generated one-off IDs when possible.
- The repeating container must represent one job item, not the full wrapper.
- Exclude obvious non-job content such as headings, FAQs, forms, contact blocks, and generic sections.
- If the page has no real job-specific URL, set job_url.required=false and strategy=none.
- If this is not a job listing page, return is_job_listing_page=false.

Page URL:
{page_url}

Jobs already identified on this page by an earlier classifier:
{example_jobs_text}

HTML:
{cleaned_html}
"""

    return _run_pattern_prompt(
        client=client,
        model=model,
        page_url=page_url,
        stage="discovery",
        system_prompt=DISCOVERY_SYSTEM_PROMPT,
        user_prompt=prompt,
    )


def correct_pattern_with_llm(
    html: str,
    page_url: str,
    failed_pattern: Dict[str, Any],
    extracted_jobs: list[Dict[str, Any]],
    validation: Dict[str, Any],
    prepared_html: Optional[str] = None,
    example_jobs: Optional[list[Dict[str, Any]]] = None,
    model: str = MODEL,
) -> Dict[str, Any]:
    log_event(
        logger,
        "info",
        "job_pattern_llm_correction_started url=%s model=%s extracted_job_count=%s",
        page_url,
        model,
        len(extracted_jobs),
        domain=page_url or "job_pattern",
        page_url=page_url,
        model=model,
        extracted_job_count=len(extracted_jobs),
        validation_valid=validation.get("valid"),
        problems=validation.get("problems"),
        example_job_count=len(example_jobs or []),
    )
    model = resolve_openai_model(model)
    client = OpenAI(api_key=resolve_openai_api_key())
    cleaned_html = prepared_html or prepare_html_for_llm(html, model=model)
    example_jobs_text = json.dumps(example_jobs or [], indent=2)

    prompt = f"""
You are repairing a failed job extraction pattern.

Your job:
- inspect the HTML
- inspect the failed pattern
- inspect the extraction result and validation problems
- return a corrected pattern that makes the extraction work properly

Repair priorities:
- fix the repeating job container if it is too broad or too narrow
- fix title extraction if non-job content or empty values were returned
- fix URL extraction if URLs were missed, malformed, or should not be required
- tighten validation rules only where they help remove wrong extraction

Important rules:
- focus only on job extraction
- do not add pagination, sorting, or filtering logic
- be conservative and prefer a smaller correct result over a broad noisy result
- if the page truly has no job-specific URL, set job_url.required=false and strategy=none

Page URL:
{page_url}

Failed pattern:
{json.dumps(failed_pattern, indent=2)}

Validation result:
{json.dumps(validation, indent=2)}

Extracted jobs:
{json.dumps(extracted_jobs[:20], indent=2)}

Jobs already identified on this page by an earlier classifier:
{example_jobs_text}

HTML:
{cleaned_html}
"""

    return _run_pattern_prompt(
        client=client,
        model=model,
        page_url=page_url,
        stage="correction",
        system_prompt=CORRECTION_SYSTEM_PROMPT,
        user_prompt=prompt,
    )


def final_review_pattern_with_llm(
    html: str,
    page_url: str,
    failed_patterns: list[Dict[str, Any]],
    extracted_jobs: list[Dict[str, Any]],
    validation: Dict[str, Any],
    prepared_html: Optional[str] = None,
    example_jobs: Optional[list[Dict[str, Any]]] = None,
    model: str = MODEL,
) -> Dict[str, Any]:
    log_event(
        logger,
        "info",
        "job_pattern_llm_final_review_started url=%s model=%s failed_pattern_count=%s",
        page_url,
        model,
        len(failed_patterns),
        domain=page_url or "job_pattern",
        page_url=page_url,
        model=model,
        failed_pattern_count=len(failed_patterns),
        extracted_job_count=len(extracted_jobs),
        validation_valid=validation.get("valid"),
        problems=validation.get("problems"),
        example_job_count=len(example_jobs or []),
    )
    model = resolve_openai_model(model)
    client = OpenAI(api_key=resolve_openai_api_key())
    cleaned_html = prepared_html or prepare_html_for_llm(html, model=model)
    example_jobs_text = json.dumps(example_jobs or [], indent=2)

    prompt = f"""
You are doing a final strict review of a job extraction pattern after earlier attempts failed.

Your goal:
- return one final corrected pattern
- make sure the extraction works properly on this page
- leave no room for obviously wrong extraction

Be strict about:
- one real job container per job
- only real job titles
- correct handling of missing vs required job URLs
- validation rules that block headings, generic sections, and false positives

Important rules:
- focus only on extraction
- do not add pagination, sorting, or filtering logic
- prefer correctness over recall
- if uncertain between a broad selector and a narrower one, choose the narrower one

Page URL:
{page_url}

Previous failed patterns:
{json.dumps(failed_patterns, indent=2)}

Validation result:
{json.dumps(validation, indent=2)}

Extracted jobs from latest attempt:
{json.dumps(extracted_jobs[:20], indent=2)}

Jobs already identified on this page by an earlier classifier:
{example_jobs_text}

HTML:
{cleaned_html}
"""

    return _run_pattern_prompt(
        client=client,
        model=model,
        page_url=page_url,
        stage="final_review",
        system_prompt=FINAL_REVIEW_SYSTEM_PROMPT,
        user_prompt=prompt,
    )


def _run_pattern_prompt(
    client: OpenAI,
    model: str,
    page_url: str,
    stage: str,
    system_prompt: str,
    user_prompt: str,
) -> Dict[str, Any]:
    log_event(
        logger,
        "info",
        "job_pattern_llm_prompt_started url=%s stage=%s model=%s prompt_length=%s",
        page_url,
        stage,
        model,
        len(user_prompt),
        domain=page_url or "job_pattern",
        page_url=page_url,
        stage=stage,
        model=model,
        prompt_length=len(user_prompt),
    )
    try:
        output_text, token_usage = create_openai_text_response(
            client=client,
            model=model,
            input=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {"role": "user", "content": user_prompt},
            ],
            json_schema={
                "name": PATTERN_SCHEMA["name"],
                "schema": PATTERN_SCHEMA["schema"],
                "strict": True,
            },
        )
        pattern = json.loads(output_text)
        log_event(
            logger,
            "info",
            "job_pattern_llm_prompt_completed url=%s stage=%s model=%s is_job_listing_page=%s confidence=%s",
            page_url,
            stage,
            model,
            pattern.get("is_job_listing_page"),
            pattern.get("confidence"),
            domain=page_url or "job_pattern",
            page_url=page_url,
            stage=stage,
            model=model,
            token_usage=token_usage,
            is_job_listing_page=pattern.get("is_job_listing_page"),
            confidence=pattern.get("confidence"),
            job_container_selector=pattern.get("job_container_selector"),
        )
        return pattern
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "job_pattern_llm_prompt_failed url=%s stage=%s model=%s error=%s",
            page_url,
            stage,
            model,
            str(exc),
            domain=page_url or "job_pattern",
            page_url=page_url,
            stage=stage,
            model=model,
            error=str(exc),
        )
        raise


def get_by_dot_path(data: Any, path: Optional[str]) -> Optional[Any]:
    if not path:
        return None

    path = path.strip()
    if path.startswith("$."):
        path = path[2:]
    elif path.startswith("$"):
        path = path[1:]
    path = path.strip(".")
    if not path:
        return data

    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def ensure_pattern_defaults(pattern: Dict[str, Any]) -> Dict[str, Any]:
    pattern = json.loads(json.dumps(pattern))

    pattern.setdefault("container_selection_reason", "")

    title_cfg = pattern.setdefault("job_title", {})
    title_cfg.setdefault("selector", None)
    title_cfg.setdefault("source", "text")
    title_cfg.setdefault("attribute", None)

    url_cfg = pattern.setdefault("job_url", {})
    url_cfg.setdefault("required", url_cfg.get("strategy") != "none")
    url_cfg.setdefault("strategy", "none")
    url_cfg.setdefault("selector", None)
    url_cfg.setdefault("attribute", None)
    url_cfg.setdefault("json_path", None)
    url_cfg.setdefault("regex", None)

    rules = pattern.setdefault("validation_rules", {})
    rules.setdefault("minimum_jobs_expected", 1)
    rules.setdefault("title_must_not_equal", [])
    rules.setdefault("title_must_not_contain_any", [])
    rules.setdefault("minimum_title_length", 4)
    rules.setdefault("allow_missing_job_url", not url_cfg.get("required", True))
    rules.setdefault("url_must_contain_any", [])
    rules.setdefault("url_must_not_contain_any", [])

    if pattern.get("is_job_listing_page"):
        rules["minimum_jobs_expected"] = max(1, int(rules.get("minimum_jobs_expected", 1)))

    pattern.setdefault("notes", "")
    log_event(
        logger,
        "info",
        "job_pattern_defaults_applied is_job_listing_page=%s selector=%s",
        pattern.get("is_job_listing_page"),
        pattern.get("job_container_selector"),
        domain="job_pattern",
        is_job_listing_page=pattern.get("is_job_listing_page"),
        confidence=pattern.get("confidence"),
        job_container_selector=pattern.get("job_container_selector"),
        job_url_strategy=url_cfg.get("strategy"),
        minimum_jobs_expected=rules.get("minimum_jobs_expected"),
    )
    return pattern
