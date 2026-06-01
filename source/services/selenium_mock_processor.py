from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options

from services.selenium_session_heartbeat import SeleniumSessionHeartbeat
from services.selenium_session_slot_service import get_selenium_session_slot_service
from utils.logging import get_logger, log_event


logger = get_logger("selenium_mock_processor")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SeleniumMockProcessor:
    def process(
        self,
        process_id: str,
        domain_ref: dict[str, Any],
        worker_name: str,
        task_id: str,
    ) -> dict[str, Any]:
        slot = self._claim_slot(process_id, domain_ref, worker_name, task_id)
        try:
            return self._process_with_slot(domain_ref, slot)
        except Exception as exc:
            self._mark_slot_stale(slot, str(exc))
            raise
        finally:
            self._release_slot_if_busy(slot)

    def _claim_slot(
        self,
        process_id: str,
        domain_ref: dict[str, Any],
        worker_name: str,
        task_id: str,
    ) -> dict[str, Any]:
        slot = get_selenium_session_slot_service().claim_slot(
            worker_name,
            task_id,
            process_id=process_id,
            registered_domain=domain_ref["registered_domain"],
        )
        if not slot:
            raise NoSeleniumSessionSlotAvailable("No Selenium session slot is currently available")
        return slot

    def _process_with_slot(self, domain_ref: dict[str, Any], slot: dict[str, Any]) -> dict[str, Any]:
        driver = self._open_driver(slot)
        try:
            with SeleniumSessionHeartbeat(slot["slot_id"]):
                return self._run_page_load(domain_ref, slot, driver)
        finally:
            self._close_driver(driver)

    def _run_page_load(
        self,
        domain_ref: dict[str, Any],
        slot: dict[str, Any],
        driver: webdriver.Remote,
    ) -> dict[str, Any]:
        started = time.monotonic()
        target_url = self._target_url(domain_ref)
        self._log_load_started(domain_ref, target_url)
        load_error = self._load_url(driver, target_url)
        self._wait_for_mock_processing()
        result = self._result(domain_ref, slot, target_url, driver, load_error, time.monotonic() - started)
        self._log_result(result)
        return result

    def _log_load_started(self, domain_ref: dict[str, Any], target_url: str) -> None:
        log_event(
            logger,
            "info",
            "mock_page_load_started",
            domain="selenium",
            registered_domain=domain_ref["registered_domain"],
            target_url=target_url,
        )

    def _wait_for_mock_processing(self) -> None:
        time.sleep(5)

    def _open_driver(self, slot: dict[str, Any]) -> webdriver.Remote:
        try:
            driver = webdriver.Remote(
                command_executor=slot["grid_url"],
                options=self._options(),
            )
            driver.set_page_load_timeout(30)
            return driver
        except WebDriverException as exc:
            raise NoSeleniumSessionSlotAvailable(str(exc)) from exc

    def _options(self) -> Options:
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--ignore-certificate-errors")
        return options

    def _load_url(self, driver: webdriver.Remote, target_url: str) -> str | None:
        try:
            driver.get(target_url)
            return None
        except TimeoutException as exc:
            return str(exc)
        except WebDriverException as exc:
            return str(exc)

    def _safe_current_url(self, driver: webdriver.Remote) -> str:
        try:
            return str(driver.current_url or "")
        except WebDriverException:
            return ""

    def _safe_title(self, driver: webdriver.Remote) -> str:
        try:
            return str(driver.title or "").strip()
        except WebDriverException:
            return ""

    def _target_url(self, domain_ref: dict[str, Any]) -> str:
        career_url = domain_ref.get("career_url")
        if career_url:
            return str(career_url)
        return f"https://{domain_ref['registered_domain']}"

    def _result(
        self,
        domain_ref: dict[str, Any],
        slot: dict[str, Any],
        target_url: str,
        driver: webdriver.Remote,
        load_error: str | None,
        duration_seconds: float,
    ) -> dict[str, Any]:
        load_success = load_error is None
        return {
            "domain": domain_ref["domain"],
            "registered_domain": domain_ref["registered_domain"],
            "career_url": domain_ref.get("career_url"),
            "target_url": target_url,
            "final_url": self._safe_current_url(driver),
            "page_title": self._safe_title(driver),
            "load_success": load_success,
            "load_status": "loaded" if load_success else "failed_to_load",
            "load_error": load_error,
            "error": load_error,
            "jobs_found": 0,
            "duration_seconds": round(duration_seconds, 3),
            "processor": "selenium_mock",
            "selenium_node_id": slot["selenium_node_id"],
            "selenium_session_slot_id": slot["slot_id"],
            "session_index": slot["session_index"],
            "mock": True,
            "message": "Selenium mock page load completed.",
            "processed_at": _now_iso(),
        }

    def _log_result(self, result: dict[str, Any]) -> None:
        log_event(
            logger,
            "info",
            "mock_page_load_completed",
            domain="selenium",
            registered_domain=result["registered_domain"],
            load_status=result["load_status"],
            page_title=result["page_title"],
        )

    def _close_driver(self, driver: webdriver.Remote) -> None:
        try:
            driver.quit()
        except Exception:
            pass

    def _mark_slot_stale(self, slot: dict[str, Any], error: str) -> None:
        get_selenium_session_slot_service().mark_slot_stale(slot["slot_id"], error)

    def _release_slot_if_busy(self, slot: dict[str, Any]) -> None:
        get_selenium_session_slot_service().release_slot(slot["slot_id"])


class NoSeleniumSessionSlotAvailable(RuntimeError):
    pass
