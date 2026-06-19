from __future__ import annotations

from typing import Any

from openai import OpenAI

from services.openai_service import resolve_openai_api_key


def get_openai_client(api_key: str | None = None) -> Any:
    return OpenAI(api_key=resolve_openai_api_key(api_key))
