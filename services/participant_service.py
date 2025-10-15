"""Service layer for participant CRUD operations including API and UI helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Union

from domain.models.participant import Participant, Grade
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
    if 11 <= len(digits) <= 12:
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
    *,
    name: Optional[str],
    position: Optional[str],
    grade: Optional[str],
) -> Optional[Participant]:
    """Normalize form values and persist participant updates."""

    updates: Dict[str, Any] = {}
    if name is not None:
        updates["name"] = name.strip()
    if position is not None:
        updates["position"] = position.strip() or None

    grade_value = _parse_grade_value(grade)
    if grade_value is not None:
        updates["grade"] = grade_value

    return update_participant(pid, updates) if updates else _repo.find_by_pid(pid)


def _parse_grade_value(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None

    raw = value.strip()
    if not raw:
        return Grade.NORMAL.value

    try:
        grade_enum = Grade(int(raw))
    except (ValueError, TypeError):
        return Grade.NORMAL.value

    return grade_enum.value


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
