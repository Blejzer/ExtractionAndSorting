from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List


@dataclass(eq=True)
class Event:
    """Simple Event entity used for tests and services."""

    eid: str
    title: str
    location: str
    date_from: date
    date_to: date
    host_country: str
    participant_ids: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.date_from > self.date_to:
            raise ValueError("date_from must be on or before date_to")
        if not self.host_country or not self.host_country.strip():
            raise ValueError("host_country must be a non-empty string")
        if any((not pid) or (not str(pid).strip()) for pid in self.participant_ids):
            raise ValueError("participant_ids must contain only non-empty strings")

    # ----------------- Serialization helpers -----------------
    def to_mongo(self) -> dict:
        return {
            "eid": self.eid,
            "title": self.title,
            "location": self.location,
            "dateFrom": self.date_from,
            "dateTo": self.date_to,
            "host_country": self.host_country,
            "participant_ids": list(self.participant_ids),
        }

    @classmethod
    def from_mongo(cls, doc: dict | None) -> Event | None:
        if not doc:
            return None
        return cls(
            eid=doc["eid"],
            title=doc["title"],
            location=doc["location"],
            date_from=doc["dateFrom"],
            date_to=doc["dateTo"],
            host_country=doc["host_country"],
            participant_ids=doc.get("participant_ids", []),
        )

    # Compatibility with previous Pydantic API
    def model_dump(self, **_kwargs) -> dict:
        return {
            "eid": self.eid,
            "title": self.title,
            "location": self.location,
            "date_from": self.date_from,
            "date_to": self.date_to,
            "host_country": self.host_country,
            "participant_ids": list(self.participant_ids),
        }
