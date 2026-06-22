from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(slots=True)
class StageResult:
    stage: str
    success: bool = True
    data: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PipelineContext:
    source_url: str
    jobs: list[dict[str, Any]] = field(default_factory=list)
    seen_job_keys: set[str] = field(default_factory=set)
    page_reports: list[dict[str, Any]] = field(default_factory=list)
    stages: list[StageResult] = field(default_factory=list)
    navigation_errors: list[dict[str, Any]] = field(default_factory=list)
    pagination_discoveries: list[dict[str, Any]] = field(default_factory=list)
    pagination_runs: list[dict[str, Any]] = field(default_factory=list)
    infinite_scroll_runs: list[dict[str, Any]] = field(default_factory=list)
    page_states: list[dict[str, Any]] = field(default_factory=list)
    decisions: list[dict[str, Any]] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)
    stop_reason: str = "not_started"
    progress: Callable[[str, str | None, int | None], None] | None = None

    def add_stage(self, result: StageResult) -> None:
        self.stages.append(result)

    def set_progress(self, step: str, current_url: str | None = None, page_index: int | None = None) -> None:
        if self.progress is None:
            return
        self.progress(step, current_url, page_index)

    def add_error(self, stage: str, detail: str, **extra: Any) -> None:
        self.navigation_errors.append({"stage": stage, "detail": detail, **extra})

    def add_page_state(self, state: dict[str, Any]) -> None:
        self.page_states.append(state)

    def add_decision(self, stage: str, decision: str, reason: str, **evidence: Any) -> None:
        self.decisions.append(
            {
                "stage": stage,
                "decision": decision,
                "reason": reason,
                "evidence": evidence,
            }
        )


class StageTimer:
    def __init__(self, stage: str) -> None:
        self.stage = stage
        self.started_at = time.monotonic()

    def result(
        self,
        *,
        success: bool = True,
        data: dict[str, Any] | None = None,
        errors: list[dict[str, Any]] | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> StageResult:
        merged_metrics = dict(metrics or {})
        merged_metrics["duration_ms"] = int((time.monotonic() - self.started_at) * 1000)
        return StageResult(
            stage=self.stage,
            success=success,
            data=data or {},
            errors=errors or [],
            metrics=merged_metrics,
        )
