from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ServiceResult:
    service: str
    success: bool
    status: str
    data: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ActionResult:
    action: str
    outcome: str
    accepted: bool
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StoredPattern:
    kind: str
    stage: str
    pattern: dict[str, Any]
    evidence: dict[str, Any] = field(default_factory=dict)
