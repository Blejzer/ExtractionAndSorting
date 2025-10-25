# domain/models/event_participant.py (new)
from __future__ import annotations

from datetime import datetime, UTC
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
    travel_doc_issue_date: Optional[datetime] = None
    travel_doc_expiry_date: Optional[datetime] = None
    travel_doc_issued_by: Optional[str] = None

    # banking used for this event’s reimbursements
    bank_name: Optional[str] = None
    iban: Optional[str] = None
    iban_type: Optional[IbanType] = None
    swift: Optional[str] = None

    @ staticmethod
    def _to_datetime_utc(value: object) -> Optional[datetime]:
        """Coerce date/str/Timestamp to timezone-aware datetime (UTC @ 00:00)."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        # pandas Timestamp
        try:
            import pandas as _pd  # local import
            if isinstance(value, _pd.Timestamp):
                dt = value.to_pydatetime()
                return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except Exception:
            pass
        # plain date
        if value.__class__.__name__ == "date":
            return datetime(value.year, value.month, value.day, tzinfo=UTC)
        # strings (several common formats)
        s = str(value).strip()
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=UTC)
            except Exception:
                continue
        try:
            # ISO with or without time
            dt = datetime.fromisoformat(s)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except Exception:
            return None

    @model_validator(mode="before")
    def _normalize_and_coerce_dates(cls, data: dict[str, object]):
        if not isinstance(data, dict):
            return data
        value = data.get("travel_doc_type")
        if value is None:
            return data

        if isinstance(value, DocType):
            data["travel_doc_type"] = (
                value if value == DocType.passport else DocType.id_card)
        # DocType normalization
        value = data.get("travel_doc_type")
        if value is not None:
            if isinstance(value, DocType):
                data["travel_doc_type"] = value if value == DocType.passport else DocType.id_card
            else:
                txt = str(value).strip().lower()
                if "passport" in txt and "id" not in txt:
                    data["travel_doc_type"] = DocType.passport
                elif "diplomatic" in txt:
                    data["travel_doc_type"] = DocType.diplomatic_passport
                elif "service" in txt:
                    data["travel_doc_type"] = DocType.service_passport
                elif "id" in txt:
                    data["travel_doc_type"] = DocType.id_card
                else:
                    data["travel_doc_type"] = DocType.other
        # ⬇️ CRITICAL: coerce dates to datetime (UTC)
        for k in ("travel_doc_issue_date", "travel_doc_expiry_date"):
            if k in data:
                coerced = EventParticipant._to_datetime_utc(data.get(k))
                data[k] = coerced
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
        """Serialize the event-participant snapshot for MongoDB.
        Pydantic already keeps datetime objects; drop None fields"""
        return self.model_dump(exclude_none=True, mode="python")

    @classmethod
    def from_mongo(cls, doc: dict | None) -> "EventParticipant | None":
        """Hydrate an EventParticipant from a MongoDB document."""
        if not doc:
            return None
        try:
            return cls.model_validate(doc)
        except ValidationError:
            return None


