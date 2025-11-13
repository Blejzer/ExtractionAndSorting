
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection

from config.database import mongodb
from domain.models.participant import Participant, Grade


class ParticipantRepository:
    """Repository for Participant model with CRUD operations."""

    def __init__(self) -> None:
        self.collection: Collection = mongodb.collection("participants")

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

    def find_all(self) -> List[Participant]:
        """Return all participants in the collection."""
        cursor = self.collection.find()
        return [Participant.from_mongo(doc) for doc in cursor]

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

    def update(self, pid: str, data: Dict[str, Any]) -> Optional[Participant]:
        """Update arbitrary participant fields and return the updated participant."""
        doc = self.collection.find_one_and_update(
            {"pid": pid}, {"$set": data}, return_document=True
        )
        return Participant.from_mongo(doc) if doc else None

    def delete(self, pid: str) -> int:
        """Delete a participant by PID."""
        result = self.collection.delete_one({"pid": pid})
        return result.deleted_count

    def search_participants(
        self,
        search: Optional[str],
        sort_field: str,
        direction: int,
        skip: int,
        limit: int,
    ) -> tuple[List[Participant], int]:
        """Query participants with optional search, sort, and pagination."""

        query: Dict[str, Any] = {}
        if search:
            pattern = {"$regex": search, "$options": "i"}
            query = {
                "$or": [
                    {"pid": pattern},
                    {"name": pattern},
                    {"position": pattern},
                ]
            }

        total = self.collection.count_documents(query)

        field_map = {
            "pid": "pid",
            "name": "name",
            "position": "position",
            "grade": "grade",
            "country": "representing_country",
        }
        mongo_sort_field = field_map.get(sort_field, "pid")
        mongo_direction = ASCENDING if direction >= 0 else DESCENDING

        if mongo_sort_field == "representing_country":
            docs = list(self.collection.find(query))
            participants = [Participant.from_mongo(doc) for doc in docs]
            participants.sort(
                key=lambda p: p.representing_country or "",
                reverse=direction < 0,
            )
            sliced = (
                participants[skip : skip + limit]
                if limit
                else participants[skip:]
            )
            return sliced, total

        cursor = (
            self.collection.find(query)
            .sort(mongo_sort_field, mongo_direction)
            .skip(skip)
        )
        if limit:
            cursor = cursor.limit(limit)

        participants = [Participant.from_mongo(doc) for doc in cursor]
        return participants, total

    def find_by_name_dob_and_representing_country_cid(
        self, *, name: str, dob: Optional[datetime], representing_country: str
    ) -> Optional[Participant]:
        """Lookup a participant by name, optionally confirming DOB when available."""

        base_query = {
            "name": name,
            "representing_country": representing_country,
        }

        def _normalize(value: Optional[datetime]) -> Optional[datetime]:
            if not isinstance(value, datetime):
                return None
            if value.year == 1900 and value.month == 1 and value.day == 1:
                return None
            if value.tzinfo:
                return value.astimezone(timezone.utc).replace(tzinfo=None)
            return value

        desired_dob = _normalize(dob)
        for doc in self.collection.find(base_query):
            stored_dob = _normalize(doc.get("dob"))

            if desired_dob:
                if stored_dob is None:
                    return Participant.from_mongo(doc)
                if stored_dob == desired_dob:
                    return Participant.from_mongo(doc)
                continue

            return Participant.from_mongo(doc)

        return None

    def generate_next_pid(self) -> str:
        """Return the next sequential PID using zero-padded numbering."""

        doc = self.collection.find_one(sort=[("pid", DESCENDING)])
        if not doc or not doc.get("pid"):
            return "P0001"

        current = str(doc.get("pid", "")).strip().upper()
        match = re.search(r"(\d+)$", current)
        if match:
            next_value = int(match.group(1)) + 1
        else:
            count = self.collection.count_documents({})
            next_value = count + 1
        return f"P{next_value:04d}"

