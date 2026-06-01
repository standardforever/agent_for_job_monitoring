from __future__ import annotations

from functools import lru_cache
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from core.config import Settings, get_settings


class MongoDBService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: AsyncIOMotorClient[dict[str, Any]] | None = None

    @property
    def client(self) -> AsyncIOMotorClient[dict[str, Any]]:
        if self._client is None:
            self._client = AsyncIOMotorClient(self._settings.mongodb_uri)
        return self._client

    @property
    def database(self) -> AsyncIOMotorDatabase[dict[str, Any]]:
        return self.client[self._settings.mongodb_database]

    def collection(self, name: str) -> AsyncIOMotorCollection[dict[str, Any]]:
        return self.database[name]

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None


@lru_cache(maxsize=1)
def get_mongodb_service() -> MongoDBService:
    return MongoDBService(get_settings())
