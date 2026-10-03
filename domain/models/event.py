from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, List

from domain.reporting import COUNTRIES, TRAINING_AREAS


class EventType(StrEnum):
    training = "Training"
    workshop = "Workshop"
    study_trip = "Study trip"
    other = "Other"

@dataclass(eq=True)
class Event:
    """Event aggregate mirroring the MongoDB representation."""

    eid: str
    title: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    place: str = ""
    country: str | None = None
    type: EventType | None = None
    cost: float | None = None
    participants: List[str] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    audit: List[dict[str, Any]] = field(default_factory=list)
    # None retains title suggestions / the report's historical invitation policy.
    training_areas: list[str] | None = None
    invited_countries: list[str] | None = None
    expected_per_country: int | None = None

    def __post_init__(self) -> None:
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if any((not pid) or (not str(pid).strip()) for pid in self.participants):
            raise ValueError("participants must contain only non-empty strings")
        for values, allowed, label in (
            (self.training_areas, TRAINING_AREAS, "training areas"),
            (self.invited_countries, COUNTRIES, "invited countries"),
        ):
            if values is not None and (
                not isinstance(values, list) or any(not isinstance(value, str) or value not in allowed for value in values)
            ):
                raise ValueError(f"Select valid {label}.")
        if self.expected_per_country is not None and self.invited_countries is None:
            raise ValueError("Select invited countries when setting an event-specific allocation.")
        if self.expected_per_country is not None and (
            type(self.expected_per_country) is not int or not 1 <= self.expected_per_country <= 100
        ):
            raise ValueError("Expected participants per country must be between 1 and 100.")

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
        # Keep the shape of legacy documents unchanged until reporting is configured.
        for key in ("training_areas", "invited_countries", "expected_per_country"):
            value = getattr(self, key)
            if value is not None:
                doc[key] = value
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
            training_areas=doc.get("training_areas"),
            invited_countries=doc.get("invited_countries"),
            expected_per_country=doc.get("expected_per_country"),
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
            "training_areas": self.training_areas,
            "invited_countries": self.invited_countries,
            "expected_per_country": self.expected_per_country,
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
