from __future__ import annotations

from fastapi import Header, HTTPException

from core.config import get_settings
from utils.logging import get_logger, log_event


settings = get_settings()
logger = get_logger("api_dependencies")


def validate_admin_password(x_registration_password: str | None = Header(default=None)) -> None:
    configured_password = str(settings.client_registration_password or "").strip()
    provided_password = str(x_registration_password or "").strip()

    if not configured_password:
        log_event(logger, "error", "admin_password_not_configured", domain="api")
        raise HTTPException(status_code=500, detail="Client registration password is not configured")

    if provided_password != configured_password:
        log_event(logger, "warning", "admin_password_rejected", domain="api")
        raise HTTPException(status_code=401, detail="Invalid registration password")
