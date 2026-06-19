from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import validate_admin_password
from services.job_pattern_node_service import get_job_pattern_node_service
from utils.logging import get_logger, log_event


router = APIRouter(prefix="/nodes/job-pattern")
logger = get_logger("job_pattern_node_routes")


@router.post("/processes/{process_id}/start")
async def start_job_pattern_node(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "job_pattern_start_requested", domain="api", process_id=process_id)
    return _start_job_pattern(process_id, mode="start")


@router.post("/processes/{process_id}/rerun")
async def rerun_job_pattern_node(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "job_pattern_rerun_requested", domain="api", process_id=process_id)
    return _start_job_pattern(process_id, mode="rerun")


@router.post("/processes/{process_id}/force")
async def force_job_pattern_node(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "job_pattern_force_requested", domain="api", process_id=process_id)
    return _start_job_pattern(process_id, mode="force")


def _start_job_pattern(process_id: str, *, mode: str) -> dict[str, Any]:
    try:
        result = get_job_pattern_node_service().start_process(process_id, mode=mode)
    except ValueError as exc:
        log_event(logger, "warning", "job_pattern_start_rejected", domain="api", process_id=process_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        log_event(logger, "warning", "job_pattern_start_blocked", domain="api", process_id=process_id)
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "started", "node": "job_pattern", **result}
