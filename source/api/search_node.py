from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import validate_admin_password
from services.search_node_service import get_search_node_service
from utils.logging import get_logger, log_event


router = APIRouter(prefix="/nodes/search")
logger = get_logger("search_node_routes")


@router.post("/processes/{process_id}/start")
async def start_search_node(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "search_node_start_requested", domain="api", process_id=process_id)
    try:
        result = get_search_node_service().start_process(process_id)
    except ValueError as exc:
        log_event(logger, "warning", "search_node_start_rejected", domain="api", process_id=process_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        log_event(logger, "warning", "search_node_start_blocked", domain="api", process_id=process_id)
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "started", "node": "search_engine", **result}
