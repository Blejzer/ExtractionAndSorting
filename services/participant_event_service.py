"""Service functions for participant-event relations and stats."""

from __future__ import annotations

from typing import Dict, Any, List

from domain.models.event import Event
from domain.models.test import AttemptType

try:  # Imports may fail in test environments without pydantic
    from repositories.participant_event_repository import ParticipantEventRepository
    from repositories.event_repository import EventRepository
    from repositories.participant_repository import ParticipantRepository
    from repositories.test_repository import TrainingTestRepository
except Exception:  # pragma: no cover - allow tests without full deps
    ParticipantEventRepository = EventRepository = ParticipantRepository = TrainingTestRepository = None  # type: ignore


try:  # pragma: no cover - repositories require full dependencies
    _participant_event_repo = ParticipantEventRepository()  # type: ignore
    _event_repo = EventRepository()  # type: ignore
    _participant_repo = ParticipantRepository()  # type: ignore
    _test_repo = TrainingTestRepository()  # type: ignore
except Exception:  # Used in tests where Mongo/pydantic isn't available
    _participant_event_repo = None  # type: ignore
    _event_repo = None  # type: ignore
    _participant_repo = None  # type: ignore
    _test_repo = None  # type: ignore


def register_participant_event(data: Dict[str, Any]) -> None:
    """Record that a participant attends an event."""
    pid = data.get("pid")
    eid = data.get("eid")
    if not pid or not eid:
        raise ValueError("'pid' and 'eid' are required")
    _participant_event_repo.add(pid, eid)


def list_events_for_participant(pid: str) -> List[Event]:
    """Return Event objects the participant has attended."""
    eids = _participant_event_repo.find_events(pid)
    return [e for eid in eids if (e := _event_repo.find_by_eid(eid))]


def event_participants_with_scores(eid: str) -> Dict[str, Any]:
    """Return participants for an event and average test scores."""
    pids = _participant_event_repo.find_participants(eid)
    participants = [
        p for pid in pids if (p := _participant_repo.find_by_pid(pid))
    ]

    tests = _test_repo.find_by_event(eid)
    pre_scores = [t.score for t in tests if t.type == AttemptType.pre]
    post_scores = [t.score for t in tests if t.type == AttemptType.post]
    avg_pre = sum(pre_scores) / len(pre_scores) if pre_scores else 0.0
    avg_post = sum(post_scores) / len(post_scores) if post_scores else 0.0

    return {
        "participants": participants,
        "avg_pre": avg_pre,
        "avg_post": avg_post,
    }
