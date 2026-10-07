from __future__ import annotations

from typing import Iterable, List, Optional

from pymongo import ASCENDING
from pymongo.collection import Collection

from config.database import mongodb
from domain.models.event_participant import EventParticipant
from repositories.participant_references import write_with_participants


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

    def upsert(self, event_participant: EventParticipant, *, session=None) -> str:
        """Create or update the snapshot for a participant attending an event."""

        payload = event_participant.to_mongo()
        def write(pids, active_session):
            canonical_payload = {**payload, "participant_id": pids[0]}
            return self.collection.update_one(
                {"participant_id": pids[0], "event_id": payload["event_id"]},
                {"$set": canonical_payload}, upsert=True, session=active_session,
            )
        result = write_with_participants(mongodb, [payload["participant_id"]], write, session=session)
        return str(result.upserted_id) if result.upserted_id else ""

    def ensure_link(self, participant_id: str, event_id: str, *, session=None) -> None:
        """Guarantee the existence of a link document without overwriting data."""

        def write(pids, active_session):
            self.collection.update_one(
                {"participant_id": pids[0], "event_id": event_id},
                {"$setOnInsert": {"participant_id": pids[0], "event_id": event_id}},
                upsert=True, session=active_session,
            )
        write_with_participants(mongodb, [participant_id], write, session=session)

    def upsert_partial(self, participant_id: str, event_id: str, fields: dict, sources: list[dict], *, session=None) -> None:
        """Save supplied snapshot fields and source evidence, retaining omitted data."""
        update = {"$set": {**fields, "participant_id": participant_id, "event_id": event_id}}
        if sources:
            update["$addToSet"] = {"word_import_sources": {"$each": sources}}
        self.collection.update_one(
            {"participant_id": participant_id, "event_id": event_id}, update, upsert=True, session=session,
        )

    def bulk_upsert(self, entries: Iterable[EventParticipant], *, session=None) -> List[str]:
        """Insert or update several event participants."""

        ids: List[str] = []
        for entry in entries:
            upserted = self.upsert(entry, session=session)
            if upserted:
                ids.append(upserted)
        return ids

    def find(self, pid: str, eid: str) -> Optional[EventParticipant]:
        """Retrieve a participant's snapshot for a specific event."""

        doc = self.collection.find_one(
            {"participant_id": pid, "event_id": eid}
        )
        return EventParticipant.from_mongo(doc)

    def find_raw(self, pid: str, eid: str) -> Optional[dict]:
        """Return the stored MongoDB document without validation."""

        return self.collection.find_one({"participant_id": pid, "event_id": eid})

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
