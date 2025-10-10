"""Service helpers for managing events."""

from __future__ import annotations

from typing import List, Optional, Dict, Any

from domain.models.event import Event
from repositories.event_repository import EventRepository


_repo = EventRepository()


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
