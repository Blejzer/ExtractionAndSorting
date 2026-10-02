"""Field errors shared by the import preview and pre-write validation."""

from datetime import date, datetime
from pydantic import ValidationError

from domain.models.event import EventType
from domain.models.event_participant import EventParticipant
from domain.models.participant import Participant
from utils.costs import parse_cost
from utils.dates import coerce_datetime
from utils.document_dates import document_date_errors
from utils.transportation import transportation_errors

SNAPSHOT_FIELDS = set(EventParticipant.model_fields) - {"event_id", "participant_id"}
EDITABLE_FIELDS = (set(Participant.model_fields) - {"pid", "audit", "created_at", "updated_at"}) | SNAPSHOT_FIELDS


def model_errors(model, payload, *, context=None) -> dict[str, str]:
    try:
        model.model_validate(dict(payload), context=context)
    except ValidationError as exc:
        errors = {}
        for error in exc.errors(include_url=False, include_input=False):
            field = str(error["loc"][0]) if error["loc"] else "dob" if "dob" in error["msg"] else "record"
            errors.setdefault(field, error["msg"].removeprefix("Value error, "))
        return errors
    return {}


def participant_errors(record, *, allow_missing_dob=False) -> dict[str, str]:
    payload = {**record, "pid": record.get("pid") or "TEMP"}
    errors = model_errors(Participant, payload, context={"allow_missing_dob": allow_missing_dob})
    # After validators do not run when another field already has an error.
    if not allow_missing_dob and not record.get("dob"):
        errors["dob"] = "Date of birth is required."
    return errors


def snapshot_errors(record) -> dict[str, str]:
    # Keep the targeted date and transportation messages actionable even when
    # other snapshot fields are invalid too.
    targeted = {**document_date_errors(record), **transportation_errors(record)}
    probe = dict(record)
    probe.setdefault("event_id", "TEMP")
    probe.setdefault("participant_id", "TEMP")
    # Avoid the model's early cross-field date check hiding all other errors.
    if document_date_errors(record):
        probe["travel_doc_issue_date"] = probe["travel_doc_expiry_date"] = None
    errors = model_errors(EventParticipant, probe)
    if "transport_other" in targeted:
        errors.pop("record", None)
    return {**errors, **targeted}


def event_errors(record) -> dict[str, str]:
    errors = {}
    for field in ("eid", "title", "place"):
        value = record.get(field)
        if not isinstance(value, str) or not value.strip():
            errors[field] = f"{field.replace('_', ' ').capitalize()} is required."
        elif len(value.strip()) > 500:
            errors[field] = "Use at most 500 characters."
    country = record.get("country")
    if country is not None and (not isinstance(country, str) or not country.strip() or len(country) > 500):
        errors["country"] = "Country must be a non-empty country reference."
    parsed = {}
    for field in ("start_date", "end_date"):
        value = record.get(field)
        parsed[field] = coerce_datetime(value) if isinstance(value, (str, date, datetime)) else None
        if parsed[field] is None:
            errors[field] = "A valid date is required. Use YYYY-MM-DD."
    if all(parsed.values()) and parsed["start_date"].date() > parsed["end_date"].date():
        errors.update({field: "Event end date must be on or after start date." for field in parsed})
    if record.get("cost") not in (None, ""):
        try:
            parse_cost(record["cost"])
        except ValueError as exc:
            errors["cost"] = str(exc)
    if record.get("type") not in (None, ""):
        try:
            EventType(record["type"])
        except (ValueError, TypeError):
            errors["type"] = "Choose Training, Workshop, Study trip, or Other."
    participants = record.get("participants")
    if participants is not None and (not isinstance(participants, list) or any(not isinstance(pid, str) or not pid.strip() for pid in participants)):
        errors["participants"] = "Participant IDs must be a list of non-empty strings."
    return errors


def error_message(errors) -> str:
    return " ".join(f"{field.replace('_', ' ').capitalize()}: {message}" for field, message in errors.items())
