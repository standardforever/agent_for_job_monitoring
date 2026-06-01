from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import validate_admin_password
from models.process import ClientRegistrationRequest, ClientUpdateRequest
from services.admin_client_service import get_admin_client_service
from utils.logging import get_logger, log_event


router = APIRouter()
logger = get_logger("admin_routes")


@router.post("/clients")
async def register_client(
    request: ClientRegistrationRequest,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "client_create_requested", domain="api", client_name=request.client_name)
    admin_client_service = get_admin_client_service()

    try:
        client = await admin_client_service.create_client(
            client_name=request.client_name,
            email=request.email,
            api_key=request.api_key,
            model=request.model,
        )
    except ValueError as exc:
        log_event(logger, "warning", "client_create_rejected", domain="api", reason=str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    log_event(logger, "info", "client_create_completed", domain="api", client_name=client.get("client_name"))
    return {"status": "ready", "client": client, "message": "Client is ready."}


@router.get("/clients")
async def list_clients(_: None = Depends(validate_admin_password)) -> dict[str, Any]:
    log_event(logger, "info", "client_list_requested", domain="api")
    admin_client_service = get_admin_client_service()
    return await admin_client_service.list_clients()


@router.get("/clients/{client_name}")
async def get_client(
    client_name: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "client_detail_requested", domain="api", client_name=client_name)
    admin_client_service = get_admin_client_service()

    try:
        client = await admin_client_service.get_client(client_name)
    except ValueError as exc:
        log_event(logger, "warning", "client_not_found", domain="api", client_name=client_name)
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {"client": client}


@router.patch("/clients/{client_name}")
async def update_client(
    client_name: str,
    request: ClientUpdateRequest,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    return await _handle_update_client(client_name, request)


@router.patch("/clients/{client_name}/config")
async def update_client_config(
    client_name: str,
    request: ClientUpdateRequest,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    return await _handle_update_client(client_name, request)


@router.delete("/clients/{client_name}")
async def delete_client(
    client_name: str,
    _: None = Depends(validate_admin_password),
) -> dict[str, Any]:
    log_event(logger, "info", "client_delete_requested", domain="api", client_name=client_name)
    admin_client_service = get_admin_client_service()

    try:
        result = await admin_client_service.delete_client(client_name)
    except ValueError as exc:
        log_event(logger, "warning", "client_delete_rejected", domain="api", client_name=client_name)
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    log_event(logger, "info", "client_delete_completed", domain="api", client_name=client_name)
    return {"status": "deleted", **result}


async def _update_client_record(client_name: str, request: ClientUpdateRequest) -> dict[str, Any]:
    admin_client_service = get_admin_client_service()
    return await admin_client_service.update_client(
        client_name,
        new_client_name=request.client_name,
        email=request.email,
        api_key=request.api_key,
        model=request.model,
    )


async def _handle_update_client(client_name: str, request: ClientUpdateRequest) -> dict[str, Any]:
    log_event(logger, "info", "client_update_requested", domain="api", client_name=client_name)

    try:
        client = await _update_client_record(client_name, request)
    except ValueError as exc:
        log_event(logger, "warning", "client_update_rejected", domain="api", reason=str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    log_event(logger, "info", "client_update_completed", domain="api", client_name=client.get("client_name"))
    return {"status": "updated", "client": client}
