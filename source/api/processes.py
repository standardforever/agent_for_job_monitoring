from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile

from api.dependencies import validate_admin_password
from services.file_input_service import FileInputService
from services.process_upload_service import get_process_upload_service
from utils.logging import get_logger, log_event


router = APIRouter()
logger = get_logger("process_routes")
file_input_service = FileInputService()


@router.post("/processes/upload")
async def upload_process_file(
    client_name: str = Form(...),
    agent_count: int = Form(default=1),
    upload_file: UploadFile = File(...),
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "process_upload_requested", domain="api", client_name=client_name)
    _validate_agent_count(agent_count)

    try:
        content = await _read_upload_file(upload_file)
        domain_inputs = file_input_service.extract_domain_inputs(upload_file.filename or "", content)
        process = await get_process_upload_service().create_process(
            client_name=client_name,
            agent_count=agent_count,
            filename=upload_file.filename or "upload",
            domain_inputs=domain_inputs,
        )
    except ValueError as exc:
        log_event(logger, "warning", "process_upload_rejected", domain="api", reason=str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    log_event(
        logger,
        "info",
        "process_upload_completed",
        domain="api",
        process_id=process.get("process_id"),
    )
    return {"status": "queued", "process": process}


@router.get("/processes")
async def list_processes(
    client_name: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    clean_client_name = str(client_name or "").strip() or None
    log_event(logger, "info", "process_list_requested", domain="api", client_name=clean_client_name, limit=limit)
    return await get_process_upload_service().list_processes(limit=limit, client_name=clean_client_name)


@router.get("/processes/{process_id}")
async def get_process(
    process_id: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "process_detail_requested", domain="api", process_id=process_id)

    try:
        return await get_process_upload_service().get_process(process_id)
    except ValueError as exc:
        log_event(logger, "warning", "process_not_found", domain="api", process_id=process_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/processes/{process_id}/domains/{registered_domain}/stop")
async def stop_process_domain(
    process_id: str,
    registered_domain: str,
    payload: dict[str, Any] = Body(default={}),
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    reason = str(payload.get("reason") or "").strip() or None
    try:
        return await get_process_upload_service().stop_domain(process_id, registered_domain, reason)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/processes/{process_id}/domains/{registered_domain}/resume")
async def resume_process_domain(
    process_id: str,
    registered_domain: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    try:
        return await get_process_upload_service().resume_domain(process_id, registered_domain)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/processes/{process_id}/domains/{registered_domain}/nodes/{node}/stop")
async def stop_process_domain_node(
    process_id: str,
    registered_domain: str,
    node: str,
    payload: dict[str, Any] = Body(default={}),
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    reason = str(payload.get("reason") or "").strip() or None
    try:
        return await get_process_upload_service().stop_domain_node(process_id, registered_domain, node, reason)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/processes/{process_id}/domains/{registered_domain}/nodes/{node}/resume")
async def resume_process_domain_node(
    process_id: str,
    registered_domain: str,
    node: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    try:
        return await get_process_upload_service().resume_domain_node(process_id, registered_domain, node)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


def _validate_agent_count(agent_count: int) -> None:
    if agent_count < 1:
        raise HTTPException(status_code=400, detail="agent_count must be greater than zero")


async def _read_upload_file(upload_file: UploadFile) -> bytes:
    content = await upload_file.read()
    if not content:
        raise ValueError("Uploaded file is empty")
    return content
