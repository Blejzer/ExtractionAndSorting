from __future__ import annotations

from typing import Any, Dict, Optional

from datetime import datetime

from config.settings import DEBUG_PRINT
from domain.models.event import Event
from domain.models.event_participant import EventParticipant
from domain.models.participant import Participant
from utils.dates import normalize_dob, coerce_datetime
from utils.helpers import _parse_bool_value, _coerce_event_type
from utils.participants import _coerce_grade_value, _normalize_gender

try:  # pragma: no cover - import guard mirrors original usage
    from zoneinfo import ZoneInfo

    EU_TZ = ZoneInfo("Europe/Zagreb")
except Exception:  # pragma: no cover - fallback when zoneinfo unavailable
    EU_TZ = datetime.now().astimezone().tzinfo


def _build_event_from_record(record: Dict[str, str]) -> Event:
    """
    Build an Event model instance from a raw record dictionary.
    Handles basic coercions for dates, cost, and event type.
    """

    start_date = coerce_datetime(record.get("start_date"), tzinfo=EU_TZ)
    end_date = coerce_datetime(record.get("end_date"), tzinfo=EU_TZ)

    # Cost coercion
    cost_val: Optional[float] = None
    cost_raw = record.get("cost")
    if cost_raw not in (None, ""):
        try:
            cost_val = float(str(cost_raw).strip())
        except ValueError:
            cost_val = None

    return Event(
        eid=str(record.get("eid", "")),
        title=str(record.get("title", "")),
        start_date=start_date,
        end_date=end_date,
        place=str(record.get("place", "")),
        country=record.get("country") or None,
        type=_coerce_event_type(record.get("type")),
        cost=cost_val,
    )


def _build_participant_from_record(record: Dict[str, str]) -> Optional[Participant]:
    """
    Build a Participant model instance from a raw record dictionary.
    Handles normalization of gender, grade, DOB, and boolean fields.
    """

    data: Dict[str, Any] = dict(record)

    normalized_gender = _normalize_gender(data.get("gender"))
    if normalized_gender is not None:
        data["gender"] = normalized_gender

    data["grade"] = _coerce_grade_value(data.get("grade"))
    dob_dt = normalize_dob(data.get("dob"))
    if dob_dt:
        data["dob"] = dob_dt

    for field in ["intl_authority"]:
        if field in data:
            val = _parse_bool_value(data[field])
            if val is not None:
                data[field] = val

    try:
        return Participant.model_validate(data)
    except Exception as exc:
        if DEBUG_PRINT:
            print(f"[CUSTOM-XML] Failed to build Participant: {exc}")
        return None


def _build_participant_event_from_record(record: Dict[str, str]) -> Optional[EventParticipant]:
    """
    Build an EventParticipant model instance from a raw record dictionary.
    Ensures date coercion for travel documents.
    """

    data: Dict[str, Any] = dict(record)
    for key in ("travel_doc_issue_date", "travel_doc_expiry_date"):
        if key in data:
            data[key] = coerce_datetime(data.get(key), tzinfo=EU_TZ)

    try:
        return EventParticipant.model_validate(data)
    except Exception as exc:
        if DEBUG_PRINT:
            print(f"[CUSTOM-XML] Failed to build EventParticipant: {exc}")
        return None
