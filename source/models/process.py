from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


ProcessStatus = Literal["queued", "running", "completed", "partial_completed", "failed", "stop_requested", "stopped"]


class JobProcessRequest(BaseModel):
    client_name: str = Field(default="default_client", min_length=1)
    urls: list[str] = Field(default_factory=list, min_length=1)
    agent_count: int = Field(default=1, ge=1)
    task_id: str | None = None


class ClientRegistrationRequest(BaseModel):
    client_name: str = Field(min_length=1)
    email: list[str] = Field(default_factory=list)
    api_key: str = Field(min_length=1)
    model: str = Field(default="gpt-5-nano", min_length=1)


class ClientUpdateRequest(BaseModel):
    client_name: str | None = Field(default=None, min_length=1)
    email: list[str] | None = None
    api_key: str | None = Field(default=None, min_length=1)
    model: str | None = Field(default=None, min_length=1)


class DomainProcessRecord(BaseModel):
    domain: str
    main_domain: str | None = None
    career_url_extraction: dict[str, Any] = Field(default_factory=dict)
    career_page_result: dict[str, Any] = Field(default_factory=dict)
    job_listing_patterns: list[dict[str, Any]] = Field(default_factory=list)
    extracted_jobs: list[dict[str, Any]] = Field(default_factory=list)
    status: str = "completed"
    error: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class WorkerProcessResult(BaseModel):
    agent_index: int
    status: str
    assigned_domains: list[dict[str, Any]] = Field(default_factory=list)
    processed_domains: list[str] = Field(default_factory=list)
    domain_results: list[DomainProcessRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
