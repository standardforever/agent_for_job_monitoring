from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from core.config import get_settings
from services.grid_session import (
    BrowserSession,
    close_agent_tabs,
    close_shared_session_async,
    create_agent_tabs_on_cdp,
    create_session_async,
    recreate_tab_in_session,
)
from utils.logging import get_logger, log_event

logger = get_logger("browser_session_manager")


@dataclass(slots=True)
class SharedSessionRuntime:
    grid_url: str | None
    session_id: str
    cdp_url: str
    recovery_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class AgentSessionRecoveryNeeded(Exception):
    pass


class BrowserSessionManager:
    async def create_process_session(self, grid_url: str | None) -> SharedSessionRuntime | None:
        settings = get_settings()
        timeout_seconds = max(30, int(settings.browser_session_acquire_timeout_seconds))
        log_event(
            logger,
            "info",
            "process_browser_session_acquire_started timeout_seconds=%s",
            timeout_seconds,
            domain=grid_url or "grid",
            timeout_seconds=timeout_seconds,
        )
        session_info = await create_session_async(
            grid_url=grid_url,
            reuse_existing=False,
            timeout_seconds=timeout_seconds,
        )
        if session_info is None or not session_info.cdp_url:
            log_event(
                logger,
                "warning",
                "process_browser_session_acquire_failed timeout_seconds=%s",
                timeout_seconds,
                domain=grid_url or "grid",
                timeout_seconds=timeout_seconds,
            )
            return None
        log_event(
            logger,
            "info",
            "process_browser_session_created session_id=%s",
            session_info.session_id,
            domain=grid_url or "grid",
            session_id=session_info.session_id,
        )
        return SharedSessionRuntime(
            grid_url=grid_url,
            session_id=session_info.session_id,
            cdp_url=session_info.cdp_url,
        )

    async def close_process_session(self, runtime: SharedSessionRuntime | None) -> None:
        if runtime is None:
            return
        log_event(
            logger,
            "info",
            "process_browser_session_close_started session_id=%s",
            runtime.session_id,
            domain=runtime.grid_url or "grid",
            session_id=runtime.session_id,
        )
        await close_shared_session_async(runtime.session_id)

    async def create_all_agent_tabs(
        self,
        runtime: SharedSessionRuntime,
        agent_count: int,
    ) -> list[BrowserSession] | None:
        sessions = await create_agent_tabs_on_cdp(runtime.cdp_url, agent_count)
        if sessions is None:
            log_event(
                logger,
                "error",
                "agent_tabs_create_failed session_id=%s agent_count=%s",
                runtime.session_id,
                agent_count,
                domain=runtime.grid_url or "grid",
                session_id=runtime.session_id,
                agent_count=agent_count,
            )
        return sessions

    async def close_all_agent_tabs(self, sessions: list[BrowserSession | None] | None) -> None:
        if sessions:
            await close_agent_tabs(sessions)

    async def recreate_agent_tab(self, session: BrowserSession) -> BrowserSession | None:
        replacement = await recreate_tab_in_session(session)
        if replacement is None:
            log_event(
                logger,
                "warning",
                "agent_tab_recreate_failed session_id=%s",
                session.session_id,
                domain=session.cdp_url,
                session_id=session.session_id,
            )
        return replacement


def is_recoverable_agent_session_error(error_text: str) -> bool:
    lowered = str(error_text or "").lower()
    markers = (
        "target page, context or browser has been closed",
        "browser has been closed",
        "websocket",
        "cdp",
        "session deleted",
        "invalid session id",
    )
    return any(marker in lowered for marker in markers)
