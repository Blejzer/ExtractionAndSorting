from __future__ import annotations

from typing import Iterable, List, Optional

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb
from domain.models.event_participant import EventParticipant


class ParticipantEventRepository:
    """Repository for linking participants to events."""

    def __init__(self) -> None:
        self.collection: Collection = mongodb.collection("participant_events")

    def ensure_indexes(self) -> None:
        """Ensure unique index on participant/event pairs."""

        # Primary index uses the canonical `participant_id` and `event_id` keys
        self.collection.create_index(
            [("participant_id", ASCENDING), ("event_id", ASCENDING)],
            unique=True,
            name="participant_event_ids",
        )

    def upsert(self, event_participant: EventParticipant) -> str:
        """Create or update the snapshot for a participant attending an event."""

        payload = event_participant.to_mongo()
        query = {
            "participant_id": payload["participant_id"],
            "event_id": payload["event_id"],
        }

        result = self.collection.update_one(
            query,
            {"$set": payload},
            upsert=True,
        )
        return str(result.upserted_id) if result.upserted_id else ""

    def ensure_link(self, participant_id: str, event_id: str) -> None:
        """Guarantee the existence of a link document without overwriting data."""

        self.collection.update_one(
            {"participant_id": participant_id, "event_id": event_id},
            {
                "$setOnInsert": {
                    "participant_id": participant_id,
                    "event_id": event_id,
                }
            },
            upsert=True,
        )

    def bulk_upsert(self, entries: Iterable[EventParticipant]) -> List[str]:
        """Insert or update several event participants."""

        ids: List[str] = []
        for entry in entries:
            upserted = self.upsert(entry)
            if upserted:
                ids.append(upserted)
        return ids

    def find(self, pid: str, eid: str) -> Optional[EventParticipant]:
        """Retrieve a participant's snapshot for a specific event."""

        doc = self.collection.find_one(
            {"participant_id": pid, "event_id": eid}
        )
        return EventParticipant.from_mongo(doc)

    def find_events(self, pid: str) -> List[str]:
        """Return all event IDs for a participant."""
        cursor = self.collection.find({"participant_id": pid})
        return [
            doc.get("event_id")
            for doc in cursor
            if doc.get("event_id") is not None
        ]

    def find_participants(self, eid: str) -> List[str]:
        """Return all participant IDs for an event."""
        cursor = self.collection.find({"event_id": eid})
        return [
            doc.get("participant_id")
            for doc in cursor
            if doc.get("participant_id") is not None
        ]

    def list_for_event(self, eid: str) -> List[EventParticipant]:
        """Return the full participant snapshots for an event."""

        cursor = self.collection.find({"event_id": eid})
        results: List[EventParticipant] = []
        for doc in cursor:
            model = EventParticipant.from_mongo(doc)
            if model:
                results.append(model)
        return results

    def list_for_participant(self, pid: str) -> List[EventParticipant]:
        """Return the participant's per-event snapshots."""

        cursor = self.collection.find({"participant_id": pid})
        results: List[EventParticipant] = []
        for doc in cursor:
            model = EventParticipant.from_mongo(doc)
            if model:
                results.append(model)
        return results
