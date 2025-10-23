# domain/models/event_participant.py (new)
from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class Transport(StrEnum):
    pov = "Personal Vehicle (POV)"
    gov = "Government (Official) Vehicle (GOV)"
    air = "Air (Airplane)"
    other = "Other"

class Role(StrEnum):
    participant = "Participant"
    instructor = "Instructor"
    guest = "Guest"
    organizer = "Organizer"
    sponsor = "Sponsor"
    observer = "Observer"

class DocType(StrEnum):
    passport = "Passport"
    other = "Other"


class IbanType(StrEnum):
    eur = "EURO"
    usd = "USD"
    multi = "Multi-Currency"

class EventParticipant(BaseModel):
    model_config = ConfigDict(use_enum_values=True, populate_by_name=True)

    event_id: str = Field(alias="eid")
    participant_id: str

    # per-event snapshot of mutable fields
    transportation: Transport
    transport_other: Optional[str] = None
    travelling_from: str
    returning_to: str

    # travel document used for this event
    travel_doc_type: DocType
    travel_doc_type_other: Optional[str] = None
    travel_doc_issue_date: Optional[date] = None
    travel_doc_expiry_date: Optional[date] = None
    travel_doc_issued_by: Optional[str] = None

    # banking used for this event’s reimbursements
    bank_name: Optional[str] = None
    iban: Optional[str] = None
    iban_type: Optional[IbanType] = None
    swift: Optional[str] = None

    # status: Optional[str] = None  # invited/confirmed/attended/no-show
    # role: Optional[Role] = None    # participant/instructor/etc.

    @model_validator(mode="after")
    def _require_other_details(self):
        if self.transportation == Transport.other and not (self.transport_other and self.transport_other.strip()):
            raise ValueError("transport_other is required when transportation is 'Other'.")
        if self.travel_doc_type == DocType.other and not (self.travel_doc_type_other and self.travel_doc_type_other.strip()):
            raise ValueError("travel_doc_type_other is required when travel_doc_type is 'Other'.")
        if (
            self.travel_doc_issue_date
            and self.travel_doc_expiry_date
            and self.travel_doc_issue_date > self.travel_doc_expiry_date
        ):
            raise ValueError("travel_doc_issue_date must be on/before travel_doc_expiry_date.")
        return self

    @property
    def eid(self) -> str:
        """Return the legacy `eid` identifier alias for the linked event."""

        return self.event_id

    def to_mongo(self) -> dict:
        """Serialize the event-participant snapshot for MongoDB."""

        payload = self.model_dump(exclude_none=True)
        payload.setdefault("event_id", self.event_id)
        return payload

    @classmethod
    def from_mongo(cls, doc: dict | None) -> "EventParticipant | None":
        """Hydrate an EventParticipant from a MongoDB document."""
        if not doc:
            return None
        try:
            data = dict(doc)
            if "event_id" not in data and data.get("eid"):
                data["event_id"] = data.get("eid")
            return cls.model_validate(data)
        except ValidationError:
            return None
