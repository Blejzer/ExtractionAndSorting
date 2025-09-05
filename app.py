from __future__ import annotations

from typing import List, Optional

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb
from domain.models.participant import Participant, Grade


class ParticipantRepository:
    """Repository for Participant model with common CRUD operations."""

    def __init__(self) -> None:
        # store reference to the participants collection
        self.collection: Collection = mongodb.db()["participants"]

    # ------------------------------------------------------------------
    # Index helpers
    # ------------------------------------------------------------------
    def ensure_indexes(self) -> None:
        """Create indexes needed for efficient lookups."""
        self.collection.create_index("pid", unique=True)
        self.collection.create_index("representing_country")
        self.collection.create_index("grade")

    # ------------------------------------------------------------------
    # CRUD operations
    # ------------------------------------------------------------------
    def save(self, participant: Participant) -> str:
        """Insert a new participant document."""
        result = self.collection.insert_one(participant.to_mongo())
        return str(result.inserted_id)

    def bulk_save(self, participants: List[Participant]) -> List[str]:
        """Insert multiple participants at once."""
        if not participants:
            return []
        docs = [p.to_mongo() for p in participants]
        result = self.collection.insert_many(docs)
        return [str(_id) for _id in result.inserted_ids]

    def find_by_pid(self, pid: str) -> Optional[Participant]:
        """Find a participant by its PID."""
        doc = self.collection.find_one({"pid": pid})
        return Participant.from_mongo(doc) if doc else None

    def find_all(self) -> List[Participant]:
        """Return all participants sorted by PID."""
        cursor = self.collection.find().sort("pid", ASCENDING)
        return [Participant.from_mongo(doc) for doc in cursor]

    def find_by_country(self, cid: str) -> List[Participant]:
        """Find participants associated with a given country CID."""
        cursor = self.collection.find(
            {
                "$or": [
                    {"representing_country": cid},
                    {"birth_country": cid},
                    {"citizenships": cid},
                ]
            }
        )
        return [Participant.from_mongo(doc) for doc in cursor]

    def find_by_grade(self, grade: Grade) -> List[Participant]:
        """Find participants with a specific grade."""
        cursor = self.collection.find({"grade": grade.value})
        return [Participant.from_mongo(doc) for doc in cursor]

    def update_grade(self, pid: str, grade: Grade) -> bool:
        """Update the grade for a participant."""
        result = self.collection.update_one({"pid": pid}, {"$set": {"grade": grade.value}})
        return result.modified_count > 0

    def delete(self, pid: str) -> int:
        """Delete a participant by PID."""
        result = self.collection.delete_one({"pid": pid})
        return result.deleted_count