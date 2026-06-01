from __future__ import annotations

from fastapi import APIRouter

from utils.logging import get_logger, log_event


router = APIRouter()
logger = get_logger("health_routes")


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    log_event(logger, "info", "healthcheck_requested", domain="api")
    return {"status": "ok"}
