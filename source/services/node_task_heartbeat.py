from __future__ import annotations

import threading
from types import TracebackType
from typing import Callable

from core.config import get_settings
from utils.logging import get_logger, log_event


logger = get_logger("node_task_heartbeat")


class NodeTaskHeartbeat:
    def __init__(self, heartbeat: Callable[[], None]) -> None:
        self._heartbeat = heartbeat
        self._interval = self._heartbeat_interval()
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "NodeTaskHeartbeat":
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
        self._heartbeat()

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
                "node_task_heartbeat_failed",
                domain="node_task",
                error=str(exc),
            )

    def _heartbeat_interval(self) -> int:
        configured = int(get_settings().heartbeat_interval_seconds)
        return max(5, configured)
