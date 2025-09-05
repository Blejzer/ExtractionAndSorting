from __future__ import annotations

from typing import List, Optional, Dict, Any

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb
from domain.models.event import Event


class EventRepository:
    """Repository providing CRUD operations for events."""

    def __init__(self) -> None:
        self.collection: Collection = mongodb.collection("events")

    def ensure_indexes(self) -> None:
        """Ensure necessary indexes for events collection."""
        self.collection.create_index([("eid", ASCENDING)], unique=True)

    def save(self, event: Event) -> str:
        """Insert a new event document."""
        result = self.collection.insert_one(event.to_mongo())
        return str(result.inserted_id)

    def find_all(self) -> List[Event]:
        """Return all events."""
        cursor = self.collection.find()
        return [Event.from_mongo(doc) for doc in cursor]

    def find_by_eid(self, eid: str) -> Optional[Event]:
        """Find an event by its identifier."""
        doc = self.collection.find_one({"eid": eid})
        return Event.from_mongo(doc) if doc else None

    def update(self, eid: str, data: Dict[str, Any]) -> Optional[Event]:
        """Update fields for an event and return the updated event."""
        doc = self.collection.find_one_and_update(
            {"eid": eid}, {"$set": data}, return_document=True
        )
        return Event.from_mongo(doc) if doc else None

    def delete(self, eid: str) -> int:
        """Delete an event by its identifier."""
        result = self.collection.delete_one({"eid": eid})
        return result.deleted_count
