from __future__ import annotations

from typing import List

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb


class ParticipantEventRepository:
    """Repository for linking participants to events."""

    def __init__(self) -> None:
        self.collection: Collection = mongodb.collection("participant_events")

    def ensure_indexes(self) -> None:
        """Ensure unique index on participant/event pairs."""
        self.collection.create_index([("pid", ASCENDING), ("eid", ASCENDING)], unique=True)

    def add(self, pid: str, eid: str) -> str:
        """Link a participant to an event."""
        doc = {"pid": pid, "eid": eid}
        result = self.collection.update_one(doc, {"$set": doc}, upsert=True)
        return str(result.upserted_id) if result.upserted_id else ""

    def find_events(self, pid: str) -> List[str]:
        """Return all event IDs for a participant."""
        cursor = self.collection.find({"pid": pid}, {"_id": 0, "eid": 1})
        return [doc["eid"] for doc in cursor]

    def find_participants(self, eid: str) -> List[str]:
        """Return all participant IDs for an event."""
        cursor = self.collection.find({"eid": eid}, {"_id": 0, "pid": 1})
        return [doc["pid"] for doc in cursor]
