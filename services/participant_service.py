"""Service layer for participant CRUD operations including API and UI helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from domain.models.event_participant import DocType, IbanType, Transport
from domain.models.participant import Gender, Grade, Participant
from repositories.participant_repository import ParticipantRepository

try:  # pragma: no cover - optional during limited test runs
    from repositories.country_repository import CountryRepository
except Exception:  # pragma: no cover
    CountryRepository = None  # type: ignore


_repo = ParticipantRepository()
_country_repo = CountryRepository() if CountryRepository else None


@dataclass
class ParticipantDisplay:
    pid: str
    name: str
    position: Optional[str]
    grade: Optional[str]
    country: str

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


@dataclass
class ParticipantListResult:
    participants: List[ParticipantDisplay]
    total: int


@dataclass
class ParticipantEventDisplay:
    eid: str
    title: str
    place: str
    country: str
    start_date: Optional[Any]
    end_date: Optional[Any]
    country_code: Optional[str] = None

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    @property
    def location(self) -> str:  # pragma: no cover - template compat
        return self.place

    @property
    def dateFrom(self) -> Optional[Any]:  # pragma: no cover - template compat
        return self.start_date

    @property
    def dateTo(self) -> Optional[Any]:  # pragma: no cover - template compat
        return self.end_date


DIGITS_RE = re.compile(r"\D")


def list_participants() -> List[Participant]:
    """Return all participants."""
    return _repo.find_all()


def get_participant(pid: str) -> Optional[Participant]:
    """Fetch a participant by PID."""
    return _repo.find_by_pid(pid)


def create_participant(data: Dict[str, Any]) -> Participant:
    """Create and persist a new participant."""
    participant = Participant(**data)
    _repo.save(participant)
    return participant


def bulk_create_participants(data_list: List[Dict[str, Any]]) -> List[Participant]:
    """Create multiple participants at once, skipping invalid entries."""
    participants: List[Participant] = []
    for data in data_list:
        try:
            participants.append(Participant(**data))
        except Exception:
            continue
    if participants:
        _repo.bulk_save(participants)
    return participants


def update_participant(pid: str, updates: Dict[str, Any]) -> Optional[Participant]:
    """Update an existing participant and return the updated model."""
    existing = _repo.find_by_pid(pid)
    if not existing:
        return None
    payload = existing.model_dump()
    payload.update(updates)
    updated = Participant(**payload)
    return _repo.update(pid, updated.to_mongo())


def delete_participant(pid: str) -> bool:
    """Delete a participant by PID."""
    return _repo.delete(pid) > 0


def normalize_phone(value: object) -> Optional[str]:
    """Return phone number as ``+`` followed by digits or ``None`` if invalid."""
    digits = DIGITS_RE.sub("", "" if value is None else str(value))
    if 10 <= len(digits) <= 13:
        return f"+{digits}"
    return None


def list_participants_for_display(
    *,
    search: Optional[str],
    sort: str,
    direction: int,
    page: int,
    per_page: int,
) -> ParticipantListResult:
    """Return participants prepared for HTML rendering with pagination."""

    skip = max(page - 1, 0) * per_page
    participants, total = _repo.search_participants(
        search, sort, direction, skip, per_page
    )

    countries = _load_country_map()
    display_items = [
        _to_display_participant(participant, countries)
        for participant in participants
    ]

    if sort == "country":
        display_items.sort(
            key=lambda item: item.country or "",
            reverse=direction < 0,
        )

    return ParticipantListResult(participants=display_items, total=total)


def get_participant_for_display(pid: str) -> Optional[ParticipantDisplay]:
    participant = get_participant(pid)
    if not participant:
        return None
    countries = _load_country_map()
    return _to_display_participant(participant, countries)


def list_events_for_participant_display(pid: str) -> List[ParticipantEventDisplay]:
    try:
        from services.participant_event_service import list_events_for_participant
    except Exception:  # pragma: no cover - service unavailable in tests
        return []

    try:
        events = list_events_for_participant(pid)
    except Exception:  # pragma: no cover
        return []

    countries = _load_country_map()

    return [
        ParticipantEventDisplay(
            eid=event.eid,
            title=event.title,
            place=getattr(event, "place", getattr(event, "location", "")),
            country=countries.get(getattr(event, "country", None), getattr(event, "country", "")),
            start_date=getattr(event, "start_date", getattr(event, "dateFrom", None)),
            end_date=getattr(event, "end_date", getattr(event, "dateTo", None)),
            country_code=getattr(event, "country", None),
        )
        for event in events
    ]


def update_participant_from_form(
    pid: str,
    form_data: Mapping[str, Any],
    *,
    actor: str = "web",
) -> Optional[Participant]:
    """Normalize form values and persist participant updates from an HTML form."""

    existing = _repo.find_by_pid(pid)
    if not existing:
        return None

    payload = existing.model_dump()
    audit_history: List[dict[str, Any]] = list(payload.get("audit", []))
    countries = _load_country_map()
    country_codes = set(countries.keys())
    country_names_lookup = {
        name.lower(): cid for cid, name in countries.items() if isinstance(name, str)
    }

    updates: Dict[str, Any] = {}
    changed_fields: Dict[str, tuple[Any, Any]] = {}

    def _set_field(field: str, value: Any) -> None:
        old_value = payload.get(field)
        if field == "citizenships":
            old_value = old_value or None
        if old_value != value:
            updates[field] = value
            changed_fields[field] = (old_value, value)

    def _get(field: str) -> Optional[str]:
        raw = form_data.get(field)  # type: ignore[arg-type]
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = raw.strip()
        return raw or None

    def _get_list(field: str) -> List[str]:
        if hasattr(form_data, "getlist"):
            values = form_data.getlist(field)  # type: ignore[call-arg]
        else:
            value = form_data.get(field)  # type: ignore[arg-type]
            if isinstance(value, (list, tuple, set)):
                values = list(value)
            elif value is None:
                values = []
            else:
                values = [value]
        cleaned: List[str] = []
        for item in values:
            if item is None:
                continue
            item_str = str(item).strip()
            if item_str:
                cleaned.append(item_str)
        return cleaned

    def _parse_country(field: str, allow_blank: bool = False) -> Optional[str]:
        raw = _get(field)
        if raw is None:
            if allow_blank:
                return None
            raise ValueError(f"{field.replace('_', ' ').title()} is required.")
        if raw in country_codes:
            return raw

        normalized = raw.lower()
        matched_code = country_names_lookup.get(normalized)
        if matched_code:
            return matched_code

        existing_value = payload.get(field)
        if isinstance(existing_value, str) and normalized == existing_value.lower():
            return existing_value

        raise ValueError(f"Invalid country selection for '{field}'.")

    def _parse_date(field: str, allow_blank: bool = False) -> Optional[datetime]:
        raw = _get(field)
        if raw is None:
            if allow_blank:
                return None
            raise ValueError(f"{field.replace('_', ' ').title()} is required.")
        try:
            if len(raw) == 10:
                return datetime.fromisoformat(raw)
            return datetime.fromisoformat(raw)
        except ValueError as exc:  # pragma: no cover - defensive
            raise ValueError(f"Invalid date for '{field}'.") from exc

    def _parse_grade(field: str) -> Grade:
        raw = _get(field)
        if raw is None:
            return Grade(payload.get(field, Grade.NORMAL.value))  # type: ignore[arg-type]
        try:
            return Grade(int(raw))
        except (ValueError, TypeError) as exc:
            raise ValueError("Invalid grade selection.") from exc

    def _parse_enum(field: str, enum_cls: Any) -> Optional[Any]:
        raw = _get(field)
        if raw is None:
            return None
        try:
            return enum_cls(raw)
        except ValueError as exc:
            raise ValueError(f"Invalid selection for '{field}'.") from exc

    def _serialize_audit_value(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (list, tuple)):
            return [_serialize_audit_value(v) for v in value]
        return value

    # Required/basic fields
    name_value = _get("name")
    if not name_value:
        raise ValueError("Name is required.")
    _set_field("name", name_value)

    position_value = _get("position")
    _set_field("position", position_value)

    representing_country = _parse_country("representing_country")
    _set_field("representing_country", representing_country)

    def _parse_birth_country() -> str:
        raw = _get("birth_country")
        if raw is None:
            raise ValueError("Birth Country is required.")
        text = raw if isinstance(raw, str) else str(raw)
        stripped = text.strip()
        normalized = stripped.lower()

        if stripped in country_codes:
            return stripped

        if normalized in {"sfrj", "jugoslavia", "yugoslavia", "yug"}:
            return representing_country

        matched_code = country_names_lookup.get(normalized)
        if matched_code:
            return matched_code

        existing_birth_country = payload.get("birth_country")
        if isinstance(existing_birth_country, str) and normalized == existing_birth_country.lower():
            return existing_birth_country

        raise ValueError("Invalid country selection for 'birth_country'.")

    birth_country = _parse_birth_country()
    _set_field("birth_country", birth_country)

    citizens = _get_list("citizenships")
    for cid in citizens:
        if cid not in country_codes:
            raise ValueError("Invalid citizenship selection.")
    citizens_value = citizens or None
    if payload.get("citizenships") or citizens_value:
        existing_citizenships = payload.get("citizenships") or []
        if sorted(existing_citizenships) != sorted(citizens or []):
            updates["citizenships"] = citizens_value
            changed_fields["citizenships"] = (
                existing_citizenships or None,
                citizens_value,
            )

    grade_value = _parse_grade("grade").value
    _set_field("grade", grade_value)

    gender_value = _parse_enum("gender", Gender)
    if gender_value is None:
        raise ValueError("Gender selection is required.")
    _set_field("gender", gender_value.value)

    dob_value = _parse_date("dob")
    _set_field("dob", dob_value)

    pob_value = _get("pob")
    if not pob_value:
        raise ValueError("Place of birth is required.")
    _set_field("pob", pob_value)

    email_value = _get("email")
    _set_field("email", email_value)

    phone_value = _get("phone")
    _set_field("phone", phone_value)

    diet_value = _get("diet_restrictions")
    _set_field("diet_restrictions", diet_value)

    organization_value = _get("organization")
    _set_field("organization", organization_value)

    unit_value = _get("unit")
    _set_field("unit", unit_value)

    rank_value = _get("rank")
    _set_field("rank", rank_value)

    raw_intl = form_data.get("intl_authority")  # type: ignore[arg-type]
    if raw_intl is None:
        intl_value = payload.get("intl_authority")
    else:
        raw_str = str(raw_intl).strip().lower()
        if raw_str == "":
            intl_value = None
        else:
            intl_value = raw_str in {"true", "1", "on", "yes"}
    _set_field("intl_authority", intl_value)

    bio_value = _get("bio_short")
    _set_field("bio_short", bio_value)

    if not updates:
        return existing

    payload.update(updates)
    payload.setdefault("created_at", existing.created_at)
    payload["updated_at"] = datetime.now(timezone.utc)

    for field, (old, new) in changed_fields.items():
        audit_history.append(
            {
                "ts": payload["updated_at"],
                "actor": actor,
                "field": field,
                "from": _serialize_audit_value(old),
                "to": _serialize_audit_value(new),
            }
        )

    payload["audit"] = audit_history

    updated = Participant(**payload)
    return _repo.update(pid, updated.to_mongo())


def _to_display_participant(
    participant: Participant, countries: Dict[str, str]
) -> ParticipantDisplay:
    country_name = countries.get(participant.representing_country, participant.representing_country)
    grade_label = _format_grade(participant.grade)
    return ParticipantDisplay(
        pid=participant.pid,
        name=participant.name,
        position=participant.position,
        grade=grade_label,
        country=country_name,
    )


def _format_grade(grade: Optional[Union[Grade, int]]) -> Optional[str]:
    if grade is None:
        return None

    try:
        enum_value = grade if isinstance(grade, Grade) else Grade(int(grade))
    except (ValueError, TypeError):
        return None

    return enum_value.name.replace("_", " ").title()


def _load_country_map() -> Dict[str, str]:
    if not _country_repo:
        return {}
    try:
        return {country.cid: country.country for country in _country_repo.find_all()}
    except Exception:  # pragma: no cover - allow operation without DB in tests
        return {}


def get_country_lookup() -> Dict[str, str]:
    """Return a mapping of country CID -> country name."""

    return _load_country_map()


def get_country_choices() -> List[Tuple[str, str]]:
    """Return choices suitable for form dropdowns (cid, country name)."""

    countries = _load_country_map()
    return sorted(countries.items(), key=lambda item: item[1].lower())


def get_grade_choices() -> List[Tuple[int, str]]:
    """Return grade choices for select inputs."""

    return [(grade.value, _format_grade(grade) or str(grade.value)) for grade in Grade]


def get_gender_choices() -> List[str]:
    """Return available gender values."""

    return [gender.value for gender in Gender]


def get_transport_choices() -> List[str]:
    """Return available transportation values."""

    return [transport.value for transport in Transport]


def get_document_type_choices() -> List[str]:
    """Return available travel document types."""

    return [doc_type.value for doc_type in DocType]


def get_iban_type_choices() -> List[str]:
    """Return available IBAN type values."""

    return [iban_type.value for iban_type in IbanType]
