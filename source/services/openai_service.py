from __future__ import annotations

import asyncio
import contextvars
import json
import os
from dataclasses import dataclass, field
from typing import Any

from openai import OpenAI

from core.config import load_environment
from utils.logging import get_logger, log_event

load_environment()
logger = get_logger("openai_service")
_RUNTIME_API_KEY: contextvars.ContextVar[str | None] = contextvars.ContextVar("runtime_openai_api_key", default=None)
_RUNTIME_MODEL: contextvars.ContextVar[str | None] = contextvars.ContextVar("runtime_openai_model", default=None)

@dataclass(slots=True)
class AnalysisResult:

    success: bool
    response: dict = field(default_factory=dict)
    token_usage: dict = field(default_factory=dict)
    error: str = ""


@dataclass(slots=True)
class APIKeyValidationResult:
    active: bool
    model: str
    error: str = ""
    user_message: str = ""


def set_openai_runtime_config(
    *,
    api_key: str | None = None,
    model: str | None = None,
) -> tuple[contextvars.Token, contextvars.Token]:
    api_token = _RUNTIME_API_KEY.set(str(api_key or "").strip() or None)
    model_token = _RUNTIME_MODEL.set(str(model or "").strip() or None)
    return api_token, model_token


def reset_openai_runtime_config(tokens: tuple[contextvars.Token, contextvars.Token]) -> None:
    api_token, model_token = tokens
    _RUNTIME_API_KEY.reset(api_token)
    _RUNTIME_MODEL.reset(model_token)


def mask_api_key(api_key: str | None) -> str | None:
    value = str(api_key or "").strip()
    if not value:
        return None
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def resolve_openai_api_key(api_key: str | None = None) -> str | None:
    return api_key or _RUNTIME_API_KEY.get() or os.getenv("OPENAI_API_KEY")


def resolve_openai_model(model: str | None = None) -> str:
    return str(model or _RUNTIME_MODEL.get() or os.getenv("OPENAI_MODEL", "gpt-5-nano")).strip() or "gpt-5-nano"


async def validate_openai_api_key(
    *,
    api_key: str,
    model: str | None = None,
) -> APIKeyValidationResult:
    resolved_model = str(model or "gpt-5-nano").strip() or "gpt-5-nano"
    return await asyncio.to_thread(_validate_openai_api_key_sync, api_key, resolved_model)


def _validate_openai_api_key_sync(api_key: str, model: str) -> APIKeyValidationResult:
    try:
        client = OpenAI(api_key=api_key)
        client.responses.create(
            model=model,
            input="ping",
            max_output_tokens=16,
        )
        log_event(
            logger,
            "info",
            "openai_api_key_validation_succeeded model=%s",
            model,
            domain="openai",
            model=model,
        )
        return APIKeyValidationResult(active=True, model=model)
    except Exception as exc:
        error = str(exc)
        user_message = _summarize_api_key_validation_error(error)
        log_event(
            logger,
            "warning",
            "openai_api_key_validation_failed model=%s error=%s",
            model,
            error,
            domain="openai",
            model=model,
            error=error,
        )
        return APIKeyValidationResult(active=False, model=model, error=error, user_message=user_message)


def _summarize_api_key_validation_error(error: str) -> str:
    normalized = str(error or "").lower()
    if "invalid_api_key" in normalized or "incorrect api key" in normalized:
        return "The OpenAI API key is invalid. Please check the key and try again."
    if "insufficient_quota" in normalized or "quota" in normalized:
        return "The OpenAI account does not have enough quota to use this model right now."
    if "model_not_found" in normalized or "does not exist" in normalized:
        return "The selected model is not available for this API key."
    if "organization" in normalized and "not found" in normalized:
        return "The API key could not be used because the associated organization settings are invalid."
    if "rate limit" in normalized or "429" in normalized:
        return "OpenAI rate-limited the validation request. Please wait a moment and try again."
    return "The OpenAI API key could not be validated. Please confirm the key and model, then try again."


class OpenAIAnalysisService:
    def __init__(self, model: str | None = None, api_key: str | None = None) -> None:
        self._model = resolve_openai_model(model)
        self._api_key = resolve_openai_api_key(api_key)
        if not self._api_key:
            raise ValueError("OPENAI_API_KEY is not configured")
        self._client = OpenAI(api_key=self._api_key)
        log_event(
            logger,
            "info",
            "openai_analysis_service_initialized model=%s",
            self._model,
            domain="openai",
            model=self._model,
        )

    async def analyze_data(
        self,
        prompt: str,
        json_response: bool = True,
    ) -> AnalysisResult:
        log_event(
            logger,
            "info",
            "openai_analysis_started model=%s json_response=%s",
            self._model,
            json_response,
            domain="openai",
            model=self._model,
            json_response=json_response,
            prompt_length=len(prompt),
        )
        return await asyncio.to_thread(self._analyze_sync, prompt, json_response)

    def _analyze_sync(self, prompt: str, json_response: bool) -> AnalysisResult:
        error = ""
        for attempt in range(1, 3):
            try:
                response = self._client.responses.create(
                    model=self._model,
                    input=prompt,
                )
                output: Any = response.output_text

                if json_response:
                    output = json.loads(output)
                
                token_usage = {}
                if response.usage:
                    token_usage = {
                        "input_token": response.usage.input_tokens,
                        "output_token": response.usage.output_tokens,
                        "total_token": response.usage.total_tokens
                    }
                log_event(
                    logger,
                    "info",
                    "openai_analysis_completed model=%s attempt=%s",
                    self._model,
                    attempt,
                    domain="openai",
                    model=self._model,
                    attempt=attempt,
                    token_usage=token_usage,
                )
                return AnalysisResult(
                    response=output if isinstance(output, dict) else {},
                    success=True,
                    token_usage=token_usage,
                    
                )
                
    
            except Exception as exc:
                error = str(exc)
                log_event(
                    logger,
                    "warning",
                    "openai_analysis_attempt_failed model=%s attempt=%s error=%s",
                    self._model,
                    attempt,
                    error,
                    domain="openai",
                    model=self._model,
                    attempt=attempt,
                    error=error,
                )
                continue

        log_event(
            logger,
            "error",
            "openai_analysis_failed model=%s error=%s",
            self._model,
            error,
            domain="openai",
            model=self._model,
            error=error,
        )
        return AnalysisResult(
            response={},
            success=False,
            error=error,
        )
