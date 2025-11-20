"""
Serialization helpers for converting Pydantic models to Mongo-friendly dictionaries.

These helpers are intentionally unaware of Excel/tables/parsing logic.
"""
from datetime import datetime
from typing import Any, Dict, Optional

from domain.models.event import Event
from domain.models.event_participant import EventParticipant
from domain.models.participant import Participant


def serialize_model_for_preview(
    obj, enum_fields: tuple = (), ensure_int_fields: tuple = ()
):
    """
    Generic serializer for Pydantic models:
      • Converts Enums → .value
      • Keeps datetime fields as datetime (Mongo-compatible)
      • Casts selected fields to int (e.g., grade)
    """
    if obj is None:
        return {}

    data = obj.model_dump(exclude_none=True)
    out: Dict[str, Any] = {}

    for key, val in data.items():
        if key in enum_fields and hasattr(val, "value"):
            out[key] = val.value
        elif isinstance(val, datetime):
            # keep native datetime (already EU tz)
            out[key] = val
        elif key in ensure_int_fields:
            try:
                out[key] = int(val)
            except Exception:
                out[key] = 1
        else:
            out[key] = val
    return out


def serialize_event(event: Optional[Event]) -> Dict[str, Any]:
    """Serialize Event → dict with datetime kept for Mongo."""
    return serialize_model_for_preview(event, enum_fields=("type",))


def serialize_participant(participant: Participant) -> Dict[str, Any]:
    """Serialize Participant → dict with enums and datetimes preserved."""
    return serialize_model_for_preview(
        participant,
        enum_fields=("gender",),
        ensure_int_fields=("grade",),
    )


def serialize_participant_event(ep: EventParticipant) -> Dict[str, Any]:
    """Serialize EventParticipant → dict with enums and datetimes preserved."""
    return serialize_model_for_preview(
        ep,
        enum_fields=("transportation", "travel_doc_type", "iban_type"),
    )


def merge_attendee_preview(participant: Participant, ep: EventParticipant) -> Dict[str, Any]:
    """
    Combine Participant and EventParticipant data into a single attendee preview record.
    Keeps all datetime objects (Mongo-ready) and converts Enums to .value strings.
    """
    transportation = (
        ep.transportation.value if hasattr(ep.transportation, "value") else ep.transportation
    )
    travel_doc_type = (
        ep.travel_doc_type.value if hasattr(ep.travel_doc_type, "value") else ep.travel_doc_type
    )
    iban_type = (
        ep.iban_type.value if hasattr(ep.iban_type, "value") else ep.iban_type
    )

    attendee: Dict[str, Any] = {
        "pid": participant.pid,
        "name": participant.name,
        "representing_country": participant.representing_country,
        "gender": participant.gender.value if hasattr(participant.gender, "value") else participant.gender,
        "grade": int(participant.grade),
        "dob": participant.dob,  # keep datetime
        "pob": participant.pob,
        "birth_country": participant.birth_country,
        "citizenships": participant.citizenships or [],
        "email": participant.email,
        "phone": participant.phone,
        "diet_restrictions": participant.diet_restrictions,
        "organization": participant.organization,
        "unit": participant.unit,
        "position": participant.position,
        "rank": participant.rank,
        "intl_authority": participant.intl_authority,
        "bio_short": participant.bio_short,
        "event_id": ep.event_id,
        "participant_id": ep.participant_id,
        "transportation": transportation,
        "transport_other": ep.transport_other,
        "traveling_from": ep.traveling_from,
        "returning_to": ep.returning_to,
        "travel_doc_type": travel_doc_type,
        "travel_doc_issue_date": ep.travel_doc_issue_date,  # keep datetime
        "travel_doc_expiry_date": ep.travel_doc_expiry_date,  # keep datetime
        "travel_doc_issued_by": ep.travel_doc_issued_by,
        "bank_name": ep.bank_name,
        "iban": ep.iban,
        "iban_type": iban_type,
        "swift": ep.swift,
    }

    return attendee
