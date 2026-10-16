from __future__ import annotations

from typing import List, Optional

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb
from domain.models.test import TrainingTest, AttemptType
from repositories.participant_references import write_with_participants


class TrainingTestRepository:
    """Repository for storing participant test scores."""

    def __init__(self) -> None:
        self.collection: Collection = mongodb.collection("tests")

    def ensure_indexes(self) -> None:
        """Ensure unique index on (eid, pid, type)."""
        self.collection.create_index(
            [("eid", ASCENDING), ("pid", ASCENDING), ("type", ASCENDING)],
            unique=True,
        )

    def save(self, test: TrainingTest) -> str:
        """Insert or update a test score."""
        def write(pids, session):
            payload = {**test.to_mongo(), "pid": pids[0]}
            return self.collection.update_one(
                {"eid": test.eid, "pid": pids[0], "type": test.type.value},
                {"$set": payload}, upsert=True, session=session,
            ), pids[0]
        result, canonical_pid = write_with_participants(mongodb, [test.pid], write)
        test.pid = canonical_pid
        return str(result.upserted_id) if result.upserted_id else ""

    def find(self, eid: str, pid: str, type: AttemptType) -> Optional[TrainingTest]:
        """Find a specific test by composite key."""
        doc = self.collection.find_one({"eid": eid, "pid": pid, "type": type.value})
        return TrainingTest.from_mongo(doc) if doc else None

    def find_by_event(self, eid: str) -> List[TrainingTest]:
        """Find all tests for a given event."""
        cursor = self.collection.find({"eid": eid})
        return [TrainingTest.from_mongo(doc) for doc in cursor]
