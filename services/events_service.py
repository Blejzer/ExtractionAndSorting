"""Service helpers for managing events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from domain.models.event import Event
from repositories.country_repository import CountryRepository
from repositories.event_repository import EventRepository
from services.participant_event_service import event_participants_with_scores


_repo = EventRepository()

try:
    _country_repo: CountryRepository | None = CountryRepository()
except Exception:  # pragma: no cover - allows running without DB connection
    _country_repo = None

_country_cache: Dict[str, str] = {}


@dataclass
class EventSummary:
    """Lightweight representation for rendering events in templates."""

    eid: str
    title: str
    place: str
    country: str
    start_date: datetime | None
    end_date: datetime | None
    type: str | None
    cost: float | None
    participant_count: int
    country_code: str | None = None

    @property
    def dateFrom(self) -> datetime | None:  # pragma: no cover - template compat
        return self.start_date

    @property
    def dateTo(self) -> datetime | None:  # pragma: no cover - template compat
        return self.end_date


@dataclass
class ParticipantSummary:
    """Participant details prepared for the event detail template."""

    pid: str
    name: str
    position: str
    grade: str | None
    country: str


@dataclass
class EventDetail:
    """Combined event and participant data for the detail view."""

    event: EventSummary
    participants: List[ParticipantSummary]


def list_events() -> List[Event]:
    """Return all events."""
    return _repo.find_all()


def get_event(eid: str) -> Optional[Event]:
    """Fetch a single event by identifier."""
    return _repo.find_by_eid(eid)


def create_event(data: Dict[str, Any]) -> Event:
    """Create a new event."""
    event = Event(**data)
    _repo.save(event)
    return event


def update_event(eid: str, updates: Dict[str, Any]) -> Optional[Event]:
    """Update an existing event and return the updated model."""
    existing = _repo.find_by_eid(eid)
    if not existing:
        return None
    payload = existing.model_dump(by_alias=True)
    payload.update(updates)
    updated = Event(**payload)
    return _repo.update(eid, updated.to_mongo())


def delete_event(eid: str) -> bool:
    """Delete an event by identifier."""
    return _repo.delete(eid) > 0


def _event_to_summary(event: Event) -> EventSummary:
    start_date = getattr(event, "start_date", getattr(event, "dateFrom", None))
    end_date = getattr(event, "end_date", getattr(event, "dateTo", None))
    participant_ids = getattr(event, "participants", getattr(event, "participant_ids", [])) or []
    country_code = getattr(event, "country", None)
    country_name = _resolve_country_name(country_code)
    place = getattr(event, "place", getattr(event, "location", ""))
    event_type = getattr(event, "type", None)
    cost = getattr(event, "cost", None)
    return EventSummary(
        eid=getattr(event, "eid", ""),
        title=getattr(event, "title", ""),
        place=place,
        country=country_name,
        start_date=start_date,
        end_date=end_date,
        type=event_type,
        cost=cost,
        participant_count=len(participant_ids),
        country_code=country_code,
    )


def _sort_event_summaries(
    events: List[EventSummary], sort: str, direction: int
) -> List[EventSummary]:
    sort_key_map = {
        "eid": lambda e: (e.eid or "").lower(),
        "title": lambda e: (e.title or "").lower(),
        "place": lambda e: (e.place or "").lower(),
        "location": lambda e: (e.place or "").lower(),
        "country": lambda e: (e.country or "").lower(),
        "start_date": lambda e: e.start_date.isoformat() if isinstance(e.start_date, datetime) else "",
        "dateFrom": lambda e: e.start_date.isoformat() if isinstance(e.start_date, datetime) else "",
    }
    key_func = sort_key_map.get(sort, sort_key_map["eid"])
    reverse = direction < 0
    return sorted(events, key=key_func, reverse=reverse)


def list_event_summaries(
    *, search: str = "", sort: str = "eid", direction: int = 1
) -> List[EventSummary]:
    """Return events prepared for UI consumption."""

    events = [_event_to_summary(event) for event in list_events()]

    if search:
        lowered = search.lower()
        events = [
            event
            for event in events
            if lowered in (event.eid or "").lower()
            or lowered in (event.title or "").lower()
            or lowered in (event.place or "").lower()
            or lowered in (event.country or "").lower()
        ]

    return _sort_event_summaries(events, sort, direction)


def _resolve_country_name(cid: str | None) -> str:
    if not cid:
        return ""
    if cid in _country_cache:
        return _country_cache[cid]
    country = _country_repo.find_by_cid(cid) if _country_repo else None
    name = country.country if country else cid
    _country_cache[cid] = name
    return name


def _participants_for_event(eid: str) -> List[ParticipantSummary]:
    try:
        event_info = event_participants_with_scores(eid)
    except Exception:  # pragma: no cover - repository dependencies optional in tests
        event_info = {"participants": []}

    participants: List[ParticipantSummary] = []
    for participant in event_info.get("participants", []):
        grade = (
            participant.grade.name.title()
            if hasattr(participant.grade, "name")
            else getattr(participant, "grade", None)
        )
        participants.append(
            ParticipantSummary(
                pid=getattr(participant, "pid", ""),
                name=getattr(participant, "name", ""),
                position=getattr(participant, "position", ""),
                grade=grade,
                country=_resolve_country_name(
                    getattr(participant, "representing_country", "")
                ),
            )
        )
    return participants


def _sort_participants(
    participants: List[ParticipantSummary], sort: str, direction: int
) -> List[ParticipantSummary]:
    sort_key_map = {
        "name": lambda p: (p.name or "").lower(),
        "position": lambda p: (p.position or "").lower(),
        "grade": lambda p: p.grade or "",
        "country": lambda p: (p.country or "").lower(),
    }
    key_func = sort_key_map.get(sort, sort_key_map["name"])
    reverse = direction < 0
    return sorted(participants, key=key_func, reverse=reverse)


def event_detail_for_display(
    eid: str, *, sort: str = "name", direction: int = 1
) -> Optional[EventDetail]:
    """Return an event along with prepared participant data for the UI."""

    event = get_event(eid)
    if not event:
        return None

    participants = _sort_participants(
        _participants_for_event(eid), sort, direction
    )

    return EventDetail(event=_event_to_summary(event), participants=participants)
