from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from threading import Lock
from urllib.parse import urlparse

import requests
from utils.logging import get_logger, log_event


from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright


from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver


logger = get_logger("grid_session")
_DRIVER_REGISTRY: dict[str, WebDriver] = {}
_DRIVER_REGISTRY_LOCK = Lock()


@dataclass(slots=True)
class BrowserSession:
    session_id: str
    cdp_url: str
    playwright: Playwright
    browser: Browser
    context: BrowserContext
    page: Page


@dataclass(slots=True)
class SessionBootstrapResult:
    session_id: str
    cdp_url: str
    reused_existing_session: bool


def _normalize_grid_url(raw_url: str) -> tuple[str, str, str]:
    url = (raw_url or "").strip()
    if not url:
        url = "http://127.0.0.1:4445/wd/hub"
    if not url.startswith(("http://", "https://")):
        url = f"http://{url}"

    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid Selenium URL: {raw_url}")

    executor_url = url
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    ws_scheme = "wss" if parsed.scheme == "https" else "ws"
    cdp_host = parsed.netloc
    return executor_url, base_url, f"{ws_scheme}://{cdp_host}"


def _get_active_grid_sessions(base_url: str) -> list:
    try:
        log_event(logger, "info", "grid_status_check_started base_url=%s", base_url, domain=base_url, base_url=base_url)
        response = requests.get(f"{base_url}/status", timeout=5)
        if response.status_code != 200:
            log_event(
                logger,
                "warning",
                "grid_status_check_non_200 base_url=%s status_code=%s",
                base_url,
                response.status_code,
                domain=base_url,
                base_url=base_url,
                status_code=response.status_code,
            )
            return []

        nodes = response.json().get("value", {}).get("nodes", [])
        active_sessions = []
        for node in nodes:
            for slot in node.get("slots", []):
                session = slot.get("session")
                if session is not None:
                    active_sessions.append(session.get("sessionId"))

        return active_sessions

    except Exception as e:
        log_event(
            logger,
            "warning",
            "grid_status_check_failed base_url=%s error=%s",
            base_url,
            str(e),
            domain=base_url,
            base_url=base_url,
            error=str(e),
        )
        return []


def _register_driver(session_id: str, driver: WebDriver) -> None:
    with _DRIVER_REGISTRY_LOCK:
        _DRIVER_REGISTRY[session_id] = driver


def _pop_driver(session_id: str) -> WebDriver | None:
    with _DRIVER_REGISTRY_LOCK:
        return _DRIVER_REGISTRY.pop(session_id, None)


def _get_registered_driver(session_id: str) -> WebDriver | None:
    with _DRIVER_REGISTRY_LOCK:
        return _DRIVER_REGISTRY.get(session_id)


def _build_stealth_options() -> Options:
    if Options is None:
        raise RuntimeError("selenium is not installed")

    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--test-type")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    return options


def patch_webdriver_flag(driver: WebDriver) -> None:
    source = """
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """
    try:
        driver.execute(
            "executeCdpCommand",
            {
                "cmd": "Page.addScriptToEvaluateOnNewDocument",
                "params": {"source": source},
            },
        )
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "webdriver_flag_patch_failed error=%s",
            str(exc),
            domain="grid",
            error=str(exc),
        )


def create_session(
    grid_url: str | None = None,
    *,
    reuse_existing: bool = False,
    preferred_session_id: str | None = None,
) -> SessionBootstrapResult | None:
    raw_grid = grid_url or os.getenv("SELENIUM_REMOTE_URL") or "http://127.0.0.1:4445/wd/hub"

    try:
        executor_url, base_url, cdp_base = _normalize_grid_url(raw_grid)
    except Exception as exc:
        log_event(
            logger,
            "error",
            "grid_url_invalid grid_url=%s error=%s",
            raw_grid,
            str(exc),
            domain=raw_grid,
            grid_url=raw_grid,
            error=str(exc),
        )
        return None

    log_event(logger, "info", "grid_connecting base_url=%s", base_url, domain=base_url, base_url=base_url)
    existing_sessions = _get_active_grid_sessions(base_url)
    if reuse_existing:
        candidate_session_id = None
        if preferred_session_id and preferred_session_id in existing_sessions:
            candidate_session_id = preferred_session_id
        elif existing_sessions:
            candidate_session_id = str(existing_sessions[0])

    else:
        candidate_session_id = None

    if candidate_session_id:
        reused_session_id = str(candidate_session_id)
        cdp_url = f"{cdp_base}/session/{reused_session_id}/se/cdp"
        log_event(
            logger,
            "info",
            "grid_session_reused session_id=%s active_count=%s",
            reused_session_id,
            len(existing_sessions),
            domain=base_url,
            session_id=reused_session_id,
            active_count=len(existing_sessions),
            cdp_url=cdp_url,
        )
        return SessionBootstrapResult(
            session_id=reused_session_id,
            cdp_url=cdp_url,
            reused_existing_session=True,
        )

    if webdriver is None:
        log_event(logger, "error", "selenium_not_installed", domain=base_url)
        return None

    driver = None
    try:
        driver = webdriver.Remote(
            command_executor=executor_url,
            options=_build_stealth_options(),
        )
        log_event(
            logger,
            "info",
            "grid_session_driver_created session_id=%s",
            driver.session_id,
            domain=base_url,
            session_id=driver.session_id,
        )
        patch_webdriver_flag(driver)
        _register_driver(str(driver.session_id), driver)
        cdp_url = f"{cdp_base}/session/{driver.session_id}/se/cdp"
        log_event(
            logger,
            "info",
            "grid_session_created session_id=%s",
            driver.session_id,
            domain=base_url,
            session_id=driver.session_id,
            cdp_url=cdp_url,
        )
        return SessionBootstrapResult(
            session_id=driver.session_id,
            cdp_url=cdp_url,
            reused_existing_session=False,
        )
    except WebDriverException as exc:
        if driver is not None:
            _close_failed_driver(driver)
        log_event(
            logger,
            "error",
            "grid_session_webdriver_error error=%s",
            str(exc),
            domain=base_url,
            error=str(exc),
        )
        return None
    except Exception as exc:
        if driver is not None:
            _close_failed_driver(driver)
        log_event(
            logger,
            "error",
            "grid_session_unexpected_error error=%s",
            str(exc),
            domain=base_url,
            error=str(exc),
        )
        return None


def _close_failed_driver(driver: WebDriver) -> None:
    try:
        driver.quit()
    except Exception:
        pass


async def create_session_async(
    grid_url: str | None = None,
    *,
    reuse_existing: bool = False,
    preferred_session_id: str | None = None,
    timeout_seconds: int | float | None = None,
) -> SessionBootstrapResult | None:
    timeout = float(timeout_seconds or os.getenv("BROWSER_SESSION_ACQUIRE_TIMEOUT_SECONDS", "600"))
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(
                create_session,
                grid_url,
                reuse_existing=reuse_existing,
                preferred_session_id=preferred_session_id,
            ),
            timeout=max(30.0, timeout),
        )
    except asyncio.TimeoutError:
        log_event(
            logger,
            "error",
            "grid_session_timeout grid_url=%s timeout_seconds=%s",
            grid_url,
            timeout,
            domain=grid_url or "grid",
            grid_url=grid_url,
            timeout_seconds=timeout,
        )
        return None


def is_grid_session_active(grid_url: str | None, session_id: str | None) -> bool:
    if not grid_url or not session_id:
        return False
    try:
        _, base_url, _ = _normalize_grid_url(grid_url)
    except Exception:
        return False
    return str(session_id) in _get_active_grid_sessions(base_url)


async def is_grid_session_active_async(grid_url: str | None, session_id: str | None) -> bool:
    return await asyncio.to_thread(is_grid_session_active, grid_url, session_id)


async def create_agent_tabs_on_cdp(cdp_url: str, count: int) -> list[BrowserSession] | None:
    """Connect to CDP once and create `count` pages — one per agent, all sharing the same browser."""
    if async_playwright is None:
        log_event(logger, "error", "playwright_not_installed", domain="browser")
        return None
    playwright = None
    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.connect_over_cdp(cdp_url, timeout=30_000)
        contexts = browser.contexts
        context = contexts[0] if contexts else await browser.new_context()
        context.set_default_navigation_timeout(60_000)
        context.set_default_timeout(60_000)
        session_id = cdp_url.rstrip("/").split("/")[-3]
        sessions = []
        for _ in range(count):
            page = await context.new_page()
            sessions.append(BrowserSession(
                session_id=session_id,
                cdp_url=cdp_url,
                playwright=playwright,
                browser=browser,
                context=context,
                page=page,
            ))
        log_event(logger, "info", "agent_tabs_created count=%s session_id=%s", count, session_id,
                  domain=cdp_url, count=count, session_id=session_id)
        return sessions
    except Exception as exc:
        log_event(logger, "error", "agent_tabs_create_failed count=%s error=%s", count, str(exc),
                  domain=cdp_url, count=count, error=str(exc))
        if playwright is not None:
            try:
                await playwright.stop()
            except Exception:
                pass
        return None


async def close_agent_tabs(sessions: list[BrowserSession | None]) -> None:
    """Close all agent pages then stop the shared playwright instance."""
    playwright_instance = None
    for session in (sessions or []):
        if session is None:
            continue
        playwright_instance = session.playwright
        try:
            if not session.page.is_closed():
                await session.page.close()
        except Exception:
            pass
    if playwright_instance is not None:
        try:
            await playwright_instance.stop()
        except Exception:
            pass


async def recreate_tab_in_session(session: BrowserSession) -> BrowserSession | None:
    """Create a fresh replacement page in the same browser context."""
    try:
        page = await session.context.new_page()
        await page.bring_to_front()
        return BrowserSession(
            session_id=session.session_id,
            cdp_url=session.cdp_url,
            playwright=session.playwright,
            browser=session.browser,
            context=session.context,
            page=page,
        )
    except Exception as exc:
        log_event(logger, "warning", "tab_recreate_failed error=%s", str(exc),
                  domain=session.cdp_url, error=str(exc))
        return None


async def attach_playwright_to_cdp(
    cdp_url: str,
    *,
    retries: int = 6,
    raise_on_failure: bool = False,
) -> BrowserSession | None:
    if async_playwright is None:
        log_event(logger, "error", "playwright_not_installed", domain="browser")
        return None

    last_exc: Exception | None = None
    for attempt in range(retries):
        playwright = None
        try:
            if attempt == 0:
                await asyncio.sleep(0.5)
            playwright = await async_playwright().start()
            browser = await playwright.chromium.connect_over_cdp(cdp_url, timeout=30_000)
            contexts = browser.contexts
            if contexts:
                context = contexts[0]
            else:
                context = await browser.new_context()
            context.set_default_navigation_timeout(60_000)
            context.set_default_timeout(60_000)
            page = await context.new_page()
            await page.bring_to_front()
            log_event(
                logger,
                "info",
                "playwright_attached_to_cdp session_id=%s attempt=%s",
                cdp_url.rstrip("/").split("/")[-3],
                attempt + 1,
                domain=cdp_url,
                session_id=cdp_url.rstrip("/").split("/")[-3],
            )
            return BrowserSession(
                session_id=cdp_url.rstrip("/").split("/")[-3],
                cdp_url=cdp_url,
                playwright=playwright,
                browser=browser,
                context=context,
                page=page,
            )
        except Exception as exc:
            last_exc = exc
            try:
                if playwright is not None:
                    await playwright.stop()
            except Exception:
                pass
            if attempt < retries - 1:
                backoff = 2.0 ** attempt  # 1s, 2s, 4s
                log_event(
                    logger,
                    "warning",
                    "playwright_attach_retry attempt=%s backoff=%.1fs error=%s",
                    attempt + 1,
                    backoff,
                    str(exc),
                    domain=cdp_url,
                    cdp_url=cdp_url,
                    attempt=attempt + 1,
                    error=str(exc),
                )
                await asyncio.sleep(backoff)

    error_message = str(last_exc)
    log_event(
        logger,
        "error",
        "playwright_attach_failed_all_retries retries=%s error=%s",
        retries,
        error_message,
        domain=cdp_url,
        cdp_url=cdp_url,
        error=error_message,
    )
    if raise_on_failure:
        raise RuntimeError(f"Could not attach Playwright to Selenium session: {error_message}")
    return None


async def close_browser_attachment(session: BrowserSession | None) -> None:
    if session is None:
        return

    try:
        if not session.page.is_closed():
            await session.page.close()
            log_event(
                logger,
                "info",
                "playwright_page_closed session_id=%s",
                session.session_id,
                domain=session.cdp_url,
                session_id=session.session_id,
            )
    except Exception:
        pass

    try:
        await session.playwright.stop()
    except Exception:
        pass


def close_shared_session(session_id: str | None) -> None:
    session_key = str(session_id or "").strip()
    if not session_key:
        return

    driver = _pop_driver(session_key)
    if driver is None:
        return

    try:
        driver.quit()
        log_event(
            logger,
            "info",
            "grid_session_closed session_id=%s",
            session_key,
            domain="grid",
            session_id=session_key,
        )
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "grid_session_close_failed session_id=%s error=%s",
            session_key,
            str(exc),
            domain="grid",
            session_id=session_key,
            error=str(exc),
        )


async def close_shared_session_async(session_id: str | None) -> None:
    await asyncio.to_thread(close_shared_session, session_id)


def close_session_via_http(grid_url: str | None, session_id: str | None) -> None:
    """Close a grid session via the REST API — works even when the driver is not in this process's registry."""
    session_key = str(session_id or "").strip()
    if not session_key or not grid_url:
        return
    try:
        executor_url, _, _ = _normalize_grid_url(grid_url)
        response = requests.delete(f"{executor_url}/session/{session_key}", timeout=10)
        log_event(
            logger,
            "info",
            "grid_session_closed_via_http session_id=%s status_code=%s",
            session_key,
            response.status_code,
            domain="grid",
            session_id=session_key,
            status_code=response.status_code,
        )
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "grid_session_close_via_http_failed session_id=%s error=%s",
            session_key,
            str(exc),
            domain="grid",
            session_id=session_key,
            error=str(exc),
        )


async def close_session_via_http_async(grid_url: str | None, session_id: str | None) -> None:
    await asyncio.to_thread(close_session_via_http, grid_url, session_id)
