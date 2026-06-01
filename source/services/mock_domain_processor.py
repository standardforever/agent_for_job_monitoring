from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MockDomainProcessor:
    def process(self, domain_ref: dict[str, Any]) -> dict[str, Any]:
        target_url = self._target_url(domain_ref)
        time.sleep(5)
        return self._result(domain_ref, target_url)

    def _target_url(self, domain_ref: dict[str, Any]) -> str:
        career_url = domain_ref.get("career_url")
        if career_url:
            return str(career_url)
        return f"https://{domain_ref['registered_domain']}"

    def _result(self, domain_ref: dict[str, Any], target_url: str) -> dict[str, Any]:
        return {
            "domain": domain_ref["domain"],
            "registered_domain": domain_ref["registered_domain"],
            "target_url": target_url,
            "mock": True,
            "message": "Mock domain processing completed.",
            "processed_at": _now_iso(),
        }
