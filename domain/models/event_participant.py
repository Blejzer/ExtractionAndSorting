# domain/models/event_participant.py (new)
from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator


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
    id_card = "ID Card"


class IbanType(StrEnum):
    eur = "EURO"
    usd = "USD"
    multi = "Multi-currency"

class EventParticipant(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    event_id: str
    participant_id: str

    # per-event snapshot of mutable fields
    transportation: Transport
    transport_other: Optional[str] = None
    traveling_from: str
    returning_to: str

    # travel document used for this event
    travel_doc_type: DocType
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

    @model_validator(mode="before")
    def _normalize_travel_doc_type(cls, data: dict[str, object]):
        if not isinstance(data, dict):
            return data
        value = data.get("travel_doc_type")
        if value is None:
            return data

        if isinstance(value, DocType):
            data["travel_doc_type"] = (
                value if value == DocType.passport else DocType.id_card
            )
            return data

        if isinstance(value, str):
            if value.strip().lower() == DocType.passport.value.lower():
                data["travel_doc_type"] = DocType.passport
            else:
                data["travel_doc_type"] = DocType.id_card
            return data

        data["travel_doc_type"] = DocType.id_card
        return data

    @model_validator(mode="after")
    def _require_other_details(self):
        if self.transportation == Transport.other and not (self.transport_other and self.transport_other.strip()):
            raise ValueError("transport_other is required when transportation is 'Other'.")
        if (
            self.travel_doc_issue_date
            and self.travel_doc_expiry_date
            and self.travel_doc_issue_date > self.travel_doc_expiry_date
        ):
            raise ValueError("travel_doc_issue_date must be on/before travel_doc_expiry_date.")
        return self

    def to_mongo(self) -> dict:
        """Serialize the event-participant snapshot for MongoDB."""
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_mongo(cls, doc: dict | None) -> "EventParticipant | None":
        """Hydrate an EventParticipant from a MongoDB document."""
        if not doc:
            return None
        try:
            return cls.model_validate(doc)
        except ValidationError:
            return None
