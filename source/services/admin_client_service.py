from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from core.config import Settings, get_settings
from services.mongodb_service import MongoDBService, get_mongodb_service


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AdminClientService:
    def __init__(self, mongodb: MongoDBService, settings: Settings) -> None:
        self._settings = settings
        self._collection = mongodb.collection(settings.mongodb_clients_collection)
        self._indexes_ready = False

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        await self._collection.create_index([("client_name", ASCENDING)], unique=True)
        self._indexes_ready = True

    def _normalize_emails(self, emails: list[str] | None) -> list[str]:
        if not emails:
            return []
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_email in emails:
            email = str(raw_email).strip().lower()
            if not email or email in seen:
                continue
            normalized.append(email)
            seen.add(email)
        return normalized

    def _serialize_client(self, document: dict[str, Any]) -> dict[str, Any]:
        api_key = str(document.get("api_key") or "")
        preview = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else "***"
        return {
            "id": str(document.get("_id")),
            "client_name": document.get("client_name"),
            "email": document.get("email", []),
            "model": document.get("model"),
            "api_key_preview": preview,
            "created_at": document.get("created_at"),
            "updated_at": document.get("updated_at"),
        }

    async def create_client(
        self,
        *,
        client_name: str,
        email: list[str],
        api_key: str,
        model: str,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        timestamp = _now()
        document = {
            "client_name": client_name.strip(),
            "email": self._normalize_emails(email),
            "api_key": api_key.strip(),
            "model": model.strip(),
            "created_at": timestamp,
            "updated_at": timestamp,
        }
        if not document["client_name"]:
            raise ValueError("client_name is required")
        if not document["api_key"]:
            raise ValueError("api_key is required")
        if not document["model"]:
            raise ValueError("model is required")

        try:
            result = await self._collection.insert_one(document)
        except DuplicateKeyError as exc:
            raise ValueError(f"Client '{document['client_name']}' already exists") from exc

        document["_id"] = result.inserted_id
        return self._serialize_client(document)

    async def list_clients(self) -> dict[str, Any]:
        await self._ensure_indexes()
        cursor = self._collection.find({}).sort("client_name", ASCENDING)
        clients = [self._serialize_client(document) async for document in cursor]
        return {"clients": clients, "count": len(clients)}

    async def get_client(self, client_name: str) -> dict[str, Any]:
        await self._ensure_indexes()
        document = await self._collection.find_one({"client_name": client_name})
        if not document:
            raise ValueError(f"Client '{client_name}' was not found")
        return self._serialize_client(document)

    async def get_client_snapshot(self, client_name: str) -> dict[str, Any]:
        client = await self.get_client(client_name)
        return {
            "client_name": client["client_name"],
            "email": client["email"],
            "model": client["model"],
        }

    async def update_client(
        self,
        client_name: str,
        *,
        new_client_name: str | None = None,
        email: list[str] | None = None,
        api_key: str | None = None,
        model: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        update_fields: dict[str, Any] = {"updated_at": _now()}
        if new_client_name is not None:
            update_fields["client_name"] = new_client_name.strip()
        if email is not None:
            update_fields["email"] = self._normalize_emails(email)
        if api_key is not None:
            update_fields["api_key"] = api_key.strip()
        if model is not None:
            update_fields["model"] = model.strip()

        has_blank_required_field = any(
            value == ""
            for key, value in update_fields.items()
            if key in {"client_name", "api_key", "model"}
        )
        if has_blank_required_field:
            raise ValueError("client_name, api_key, and model cannot be blank")

        try:
            document = await self._collection.find_one_and_update(
                {"client_name": client_name},
                {"$set": update_fields},
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError as exc:
            raise ValueError(f"Client '{new_client_name}' already exists") from exc

        if not document:
            raise ValueError(f"Client '{client_name}' was not found")
        return self._serialize_client(document)

    async def delete_client(self, client_name: str) -> dict[str, Any]:
        await self._ensure_indexes()
        result = await self._collection.delete_one({"client_name": client_name})
        if result.deleted_count == 0:
            raise ValueError(f"Client '{client_name}' was not found")
        return {"deleted": True, "client_name": client_name}


@lru_cache(maxsize=1)
def get_admin_client_service() -> AdminClientService:
    return AdminClientService(get_mongodb_service(), get_settings())
