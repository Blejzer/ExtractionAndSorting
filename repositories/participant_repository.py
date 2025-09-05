
from __future__ import annotations

from typing import List, Optional

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb
from domain.models.participant import Participant, Grade


class ParticipantRepository:
    """Repository for Participant model with CRUD operations."""

    def __init__(self) -> None:
        self.collection: Collection = mongodb.db()["participants"]

    def ensure_indexes(self) -> None:
        """Create indexes used by participant queries."""
        self.collection.create_index([("pid", ASCENDING)], unique=True)
        self.collection.create_index([("grade", ASCENDING)])

    def save(self, participant: Participant) -> str:
        """Insert a new participant document."""
        result = self.collection.insert_one(participant.to_mongo())
        return str(result.inserted_id)

    def bulk_save(self, participants: List[Participant]) -> List[str]:
        """Insert multiple participants at once."""
        result = self.collection.insert_many([p.to_mongo() for p in participants])
        return [str(_id) for _id in result.inserted_ids]

    def find_by_pid(self, pid: str) -> Optional[Participant]:
        """Find a participant by PID."""
        doc = self.collection.find_one({"pid": pid})
        return Participant.from_mongo(doc) if doc else None

    def find_by_country(self, cid: str) -> List[Participant]:
        """Find participants representing a given country CID."""
        cursor = self.collection.find({"representing_country": cid})
        return [Participant.from_mongo(doc) for doc in cursor]

    def find_by_grade(self, grade: Grade) -> List[Participant]:
        """Find participants with a specific grade."""
        cursor = self.collection.find({"grade": grade.value})
        return [Participant.from_mongo(doc) for doc in cursor]

    def update_grade(self, pid: str, grade: Grade) -> int:
        """Update the grade for a participant."""
        result = self.collection.update_one({"pid": pid}, {"$set": {"grade": grade.value}})
        return result.modified_count

    def delete(self, pid: str) -> int:
        """Delete a participant by PID."""
        result = self.collection.delete_one({"pid": pid})
        return result.deleted_count
