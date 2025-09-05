from __future__ import annotations

from typing import List, Optional

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb
from domain.models.test import TrainingTest, AttemptType


class TrainingTestRepository:
    """Repository for storing participant test scores."""

    def __init__(self) -> None:
        self.collection: Collection = mongodb.db()["tests"]

    def ensure_indexes(self) -> None:
        """Ensure unique index on (eid, pid, type)."""
        self.collection.create_index(
            [("eid", ASCENDING), ("pid", ASCENDING), ("type", ASCENDING)],
            unique=True,
        )

    def save(self, test: TrainingTest) -> str:
        """Insert or update a test score."""
        result = self.collection.update_one(
            {"eid": test.eid, "pid": test.pid, "type": test.type.value},
            {"$set": test.to_mongo()},
            upsert=True,
        )
        return str(result.upserted_id) if result.upserted_id else ""

    def find(self, eid: str, pid: str, type: AttemptType) -> Optional[TrainingTest]:
        """Find a specific test by composite key."""
        doc = self.collection.find_one({"eid": eid, "pid": pid, "type": type.value})
        return TrainingTest.from_mongo(doc) if doc else None

    def find_by_event(self, eid: str) -> List[TrainingTest]:
        """Find all tests for a given event."""
        cursor = self.collection.find({"eid": eid})
        return [TrainingTest.from_mongo(doc) for doc in cursor]
