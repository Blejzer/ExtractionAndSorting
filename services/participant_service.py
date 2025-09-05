"""Service layer for participant CRUD operations including bulk upload."""

from __future__ import annotations

from typing import List, Optional, Dict, Any

from domain.models.participant import Participant
from repositories.participant_repository import ParticipantRepository


_repo = ParticipantRepository()


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
