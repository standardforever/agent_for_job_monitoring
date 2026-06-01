from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends

from api.dependencies import validate_admin_password
from services.pipeline_observability_service import get_pipeline_observability_service
from utils.logging import get_logger, log_event


router = APIRouter()
logger = get_logger("observability_routes")


@router.get("/observability")
async def observability(_: None = Depends(validate_admin_password)) -> dict[str, Any]:
    log_event(logger, "info", "observability_requested", domain="api")
    snapshot = await asyncio.to_thread(get_pipeline_observability_service().snapshot)
    return {"status": "ok", "snapshot": snapshot}
