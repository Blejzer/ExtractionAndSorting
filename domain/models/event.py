from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List


@dataclass(eq=True)
class Event:
    """Event aggregate mirroring the MongoDB representation."""

    eid: str
    title: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    place: str = ""
    country: str | None = None
    type: str | None = None
    cost: float | None = None
    participants: List[str] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    audit: List[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if any((not pid) or (not str(pid).strip()) for pid in self.participants):
            raise ValueError("participants must contain only non-empty strings")

    # ----------------- Serialization helpers -----------------
    def to_mongo(self) -> dict:
        doc = {
            "eid": self.eid,
            "title": self.title,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "place": self.place,
            "country": self.country,
            "type": self.type,
            "cost": self.cost,
            "participants": list(self.participants),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        doc["_audit"] = [dict(entry) for entry in self.audit]
        return doc

    @classmethod
    def from_mongo(cls, doc: dict | None) -> Event | None:
        if not doc:
            return None

        start_date = doc.get("start_date") or doc.get("dateFrom")
        end_date = doc.get("end_date") or doc.get("dateTo")
        place = doc.get("place") or doc.get("location", "")
        participants = doc.get("participants") or doc.get("participant_ids", [])
        audit = doc.get("_audit") or []

        return cls(
            eid=doc.get("eid", ""),
            title=doc.get("title", ""),
            start_date=start_date,
            end_date=end_date,
            place=place,
            country=doc.get("country"),
            type=doc.get("type"),
            cost=doc.get("cost"),
            participants=list(participants),
            created_at=doc.get("created_at"),
            updated_at=doc.get("updated_at"),
            audit=list(audit),
        )

    # Compatibility with previous Pydantic API
    def model_dump(self, **_kwargs) -> dict:
        return {
            "eid": self.eid,
            "title": self.title,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "place": self.place,
            "country": self.country,
            "type": self.type,
            "cost": self.cost,
            "participants": list(self.participants),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "audit": [dict(entry) for entry in self.audit],
        }

    # Legacy attribute compatibility
    @property
    def date_from(self) -> datetime | None:  # pragma: no cover - backward compat
        return self.start_date

    @property
    def date_to(self) -> datetime | None:  # pragma: no cover - backward compat
        return self.end_date

    @property
    def location(self) -> str:  # pragma: no cover - backward compat
        return self.place
