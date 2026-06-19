from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import validate_admin_password
from services.client_job_alert_service import get_client_job_alert_service
from utils.logging import get_logger, log_event


router = APIRouter(prefix="/alerts")
logger = get_logger("alert_routes")


@router.post("/processes/{process_id}/build")
async def build_latest_alert_for_process(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "alert_build_requested", domain="api", process_id=process_id)
    try:
        return get_client_job_alert_service().rebuild_latest_for_process(process_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{alert_id}/send")
async def send_alert(
    alert_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "alert_send_requested", domain="api", alert_id=alert_id)
    try:
        return get_client_job_alert_service().send_alert(alert_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/send-pending")
async def send_pending_alerts(
    limit: int = Query(default=25, ge=1, le=100),
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "alert_send_pending_requested", domain="api", limit=limit)
    return get_client_job_alert_service().send_pending(limit=limit)


@router.get("")
async def list_alerts(
    process_id: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    return get_client_job_alert_service().list_alerts(process_id=process_id, limit=limit)
