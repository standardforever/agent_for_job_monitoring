from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..pagination.config import OUTPUT_DIR


class ArtifactStore:
    def __init__(self, output_dir: Path = OUTPUT_DIR) -> None:
        self.output_dir = output_dir

    def create_run_dir(self, source_url: str) -> Path:
        parsed = urlparse(source_url)
        host = (parsed.netloc or "site").replace(":", "_")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_dir = self.output_dir / "runs" / f"{host}_{stamp}"
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def write_json(self, run_dir: Path, name: str, payload: Any) -> Path:
        path = run_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(_jsonable(payload), indent=2, ensure_ascii=False), encoding="utf-8")
        return path


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, set):
        return sorted(_jsonable(item) for item in value)
    if hasattr(value, "__dict__"):
        return _jsonable(vars(value))
    return value
