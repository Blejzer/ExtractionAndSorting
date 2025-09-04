from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


class Event(BaseModel):
    """Event entity linking countries and participants."""

    model_config = ConfigDict(populate_by_name=True)

    eid: str = Field(..., min_length=1, description="Event identifier")
    title: str = Field(..., min_length=1, description="Event title")
    location: str = Field(..., min_length=1, description="Event location")
    date_from: date = Field(..., alias="dateFrom", description="Start date")
    date_to: date = Field(..., alias="dateTo", description="End date")
    host_country: str = Field(..., min_length=1, description="Country CID hosting the event")
    participant_ids: list[str] = Field(default_factory=list, description="List of participant PIDs")

    # ----------------- Validators -----------------

    @model_validator(mode="after")
    def _validate_dates(self) -> "Event":
        if self.date_from > self.date_to:
            raise ValueError("date_from must be on or before date_to")
        return self

    @field_validator("host_country", mode="after")
    @classmethod
    def _validate_host_country(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("host_country must be a non-empty string")
        return v

    @field_validator("participant_ids", mode="after")
    @classmethod
    def _validate_participant_ids(cls, v: list[str]) -> list[str]:
        if any((not pid) or (not str(pid).strip()) for pid in v):
            raise ValueError("participant_ids must contain only non-empty strings")
        return v

    # ----------------- Mongo helpers -----------------

    def to_mongo(self) -> dict:
        """Serialize to MongoDB-compatible dict using aliases."""
        return self.model_dump(by_alias=True, exclude_none=True)

    @classmethod
    def from_mongo(cls, doc: dict | None) -> "Event | None":
        """Deserialize from MongoDB document."""
        if not doc:
            return None
        return cls(**doc)

