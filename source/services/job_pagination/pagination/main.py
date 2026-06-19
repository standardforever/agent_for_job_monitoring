from __future__ import annotations

import asyncio
import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from services.job_pagination.pagination.config import CDP_URL, MAX_PAGINATION_TEST_PAGES, TEST_URLS
    from services.job_pagination.pagination.job_bridge import save_jobs_json
    from services.job_pagination.pipeline.runner import run_pipeline
    from services.openai_service import reset_openai_runtime_config, set_openai_runtime_config
else:
    from .config import CDP_URL, MAX_PAGINATION_TEST_PAGES, TEST_URLS
    from .job_bridge import save_jobs_json
    from ..pipeline.runner import run_pipeline
    from services.openai_service import reset_openai_runtime_config, set_openai_runtime_config

from utils.logging import get_logger, log_event


logger = get_logger("job_pagination_cli")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run job extraction and pagination together.")
    parser.add_argument("urls", nargs="*", help="Job listing URLs to process.")
    parser.add_argument("--api-key", dest="api_key", help="OpenAI API key for this run.")
    return parser.parse_args(argv)


async def process_url(cdp_url: str, url: str) -> None:
    log_event(logger, "info", "job_pagination_cli_url_started", domain="job_pagination", page_url=url)
    result = await run_pipeline(cdp_url, url)
    output_path = save_jobs_json(
        page_url=url,
        jobs=result.get("jobs") or [],
        metadata={
            "pagination": result.get("pagination"),
            "pagination_discoveries": result.get("pagination_discoveries"),
            "pagination_runs": result.get("pagination_runs"),
            "page_reports": result.get("page_reports"),
            "navigation_errors": result.get("navigation_errors"),
            "infinite_scroll_runs": result.get("infinite_scroll_runs"),
            "stages": result.get("stages"),
            "validation": result.get("validation"),
            "stop_reason": result.get("stop_reason"),
            "max_pages": MAX_PAGINATION_TEST_PAGES,
        },
    )
    log_event(
        logger,
        "info",
        "job_pagination_cli_url_completed",
        domain="job_pagination",
        page_url=url,
        job_count=len(result.get("jobs") or []),
        output_path=str(output_path),
    )


async def main() -> None:
    from dotenv import load_dotenv
    import os
    load_dotenv()
    args = parse_args(sys.argv[1:])
    urls = args.urls or TEST_URLS
    if not urls:
        log_event(logger, "warning", "job_pagination_cli_no_urls", domain="job_pagination")
        return

    tokens = set_openai_runtime_config(api_key=os.getenv("OPENAI_API_KEY"))
    try:
        for url in urls:
            await process_url(CDP_URL, url)
    finally:
        reset_openai_runtime_config(tokens)


if __name__ == "__main__":
    asyncio.run(main())
