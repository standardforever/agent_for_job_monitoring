from __future__ import annotations

from functools import lru_cache
from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from core.config import Settings, get_settings


class SyncMongoDBService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: MongoClient[dict[str, Any]] | None = None

    @property
    def client(self) -> MongoClient[dict[str, Any]]:
        if self._client is None:
            self._client = MongoClient(self._settings.mongodb_uri)
        return self._client

    @property
    def database(self) -> Database[dict[str, Any]]:
        return self.client[self._settings.mongodb_database]

    def collection(self, name: str) -> Collection[dict[str, Any]]:
        return self.database[name]


@lru_cache(maxsize=1)
def get_sync_mongodb_service() -> SyncMongoDBService:
    return SyncMongoDBService(get_settings())
