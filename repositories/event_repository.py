from __future__ import annotations

from typing import List, Optional, Dict, Any

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb
from repositories.participant_references import write_with_participants
from domain.models.event import Event


class EventRepository:
    """Repository providing CRUD operations for events."""

    def __init__(self) -> None:
        self.collection: Collection = mongodb.collection("events")

    def ensure_indexes(self) -> None:
        """Ensure necessary indexes for events collection."""
        self.collection.create_index([("eid", ASCENDING)], unique=True)

    def save(self, event: Event, *, session=None) -> str:
        """Insert a new event document."""
        payload = event.to_mongo()
        pids = payload.get("participants", [])
        if pids:
            def write(canonical, active_session):
                return self.collection.insert_one(
                    {**payload, "participants": list(dict.fromkeys(canonical))}, session=active_session
                )
            result = write_with_participants(mongodb, pids, write, session=session)
        else:
            result = self.collection.insert_one(payload, session=session)
        return str(result.inserted_id)

    def find_all(self) -> List[Event]:
        """Return all events."""
        cursor = self.collection.find()
        return [Event.from_mongo(doc) for doc in cursor]

    def find_by_eid(self, eid: str) -> Optional[Event]:
        """Find an event by its identifier."""
        doc = self.collection.find_one({"eid": eid})
        return Event.from_mongo(doc) if doc else None

    def update(self, eid: str, data: Dict[str, Any], *, session=None) -> Optional[Event]:
        """Update fields for an event and return the updated event."""
        fields = [field for field in ("participants", "participant_ids") if field in data]
        if fields and any(data[field] for field in fields):
            pids = list(dict.fromkeys(pid for field in fields for pid in data[field]))
            def write(canonical, active_session):
                mapping = dict(zip(pids, canonical))
                payload = {**data, **{field: list(dict.fromkeys(mapping[pid] for pid in data[field])) for field in fields}}
                return self.collection.find_one_and_update(
                    {"eid": eid}, {"$set": payload}, return_document=True, session=active_session
                )
            doc = write_with_participants(mongodb, pids, write, session=session)
        else:
            doc = self.collection.find_one_and_update(
                {"eid": eid}, {"$set": data}, return_document=True, session=session
            )
        return Event.from_mongo(doc) if doc else None

    def delete(self, eid: str, *, session=None) -> int:
        """Delete an event by its identifier."""
        result = self.collection.delete_one({"eid": eid}, session=session)
        return result.deleted_count
