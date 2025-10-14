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

        # Primary index uses the canonical `participant_id` and `event_id` keys
        self.collection.create_index(
            [("participant_id", ASCENDING), ("event_id", ASCENDING)],
            unique=True,
            name="participant_event_ids",
        )

    def add(self, pid: str, eid: str) -> str:
        """Link a participant to an event."""
        query = {"participant_id": pid, "event_id": eid}
        update = {
            "$set": {
                "participant_id": pid,
                "event_id": eid,
            }
        }
        result = self.collection.update_one(query, update, upsert=True)
        return str(result.upserted_id) if result.upserted_id else ""

    def find_events(self, pid: str) -> List[str]:
        """Return all event IDs for a participant."""
        cursor = self.collection.find(
            {"participant_id": pid},
            {"_id": 0, "event_id": 1},
        )
        return [doc["event_id"] for doc in cursor if "event_id" in doc]

    def find_participants(self, eid: str) -> List[str]:
        """Return all participant IDs for an event."""
        cursor = self.collection.find(
            {"event_id": eid},
            {"_id": 0, "participant_id": 1},
        )
        return [doc["participant_id"] for doc in cursor if "participant_id" in doc]
