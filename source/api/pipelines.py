from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import validate_admin_password
from services.pipeline_orchestrator_service import get_pipeline_orchestrator_service
from utils.logging import get_logger, log_event


router = APIRouter(prefix="/pipelines")
logger = get_logger("pipeline_routes")


@router.post("/processes/{process_id}/run")
async def run_pipeline_for_process(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "pipeline_run_requested", domain="api", process_id=process_id)
    try:
        return get_pipeline_orchestrator_service().start_process_pipeline(process_id, trigger="manual")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/run-all")
async def run_all_pipelines(_: None = Depends(validate_admin_password)) -> dict[str, Any]:
    log_event(logger, "info", "pipeline_run_all_requested", domain="api")
    return get_pipeline_orchestrator_service().start_all_enabled(trigger="manual_all")


@router.post("/processes/{process_id}/pause")
async def pause_process_pipeline(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "pipeline_pause_requested", domain="api", process_id=process_id)
    try:
        return get_pipeline_orchestrator_service().pause_process(process_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/processes/{process_id}/resume")
async def resume_process_pipeline(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "pipeline_resume_requested", domain="api", process_id=process_id)
    try:
        return get_pipeline_orchestrator_service().resume_process(process_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs")
async def list_pipeline_runs(_: None = Depends(validate_admin_password)) -> dict[str, Any]:
    return get_pipeline_orchestrator_service().list_runs()
