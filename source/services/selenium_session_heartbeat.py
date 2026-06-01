from __future__ import annotations

import threading
from types import TracebackType

from core.config import get_settings
from services.selenium_session_slot_service import get_selenium_session_slot_service
from utils.logging import get_logger, log_event


logger = get_logger("selenium_session_heartbeat")


class SeleniumSessionHeartbeat:
    def __init__(self, slot_id: str) -> None:
        self._slot_id = slot_id
        self._interval = self._heartbeat_interval()
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "SeleniumSessionHeartbeat":
        self.beat()
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()

    def beat(self) -> None:
        get_selenium_session_slot_service().heartbeat_slot(self._slot_id)

    def stop(self) -> None:
        self._stopped.set()
        self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stopped.wait(self._interval):
            self._safe_beat()

    def _safe_beat(self) -> None:
        try:
            self.beat()
        except Exception as exc:
            log_event(
                logger,
                "warning",
                "selenium_session_heartbeat_failed",
                domain="selenium",
                slot_id=self._slot_id,
                error=str(exc),
            )

    def _heartbeat_interval(self) -> int:
        configured = int(get_settings().heartbeat_interval_seconds)
        return max(5, configured)
