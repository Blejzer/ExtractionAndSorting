"""Service functions for participant-event relations and stats."""

from __future__ import annotations

from typing import Dict, Any, List, Mapping, Optional

from domain.models.event import Event
from domain.models.event_participant import EventParticipant
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

    if _participant_event_repo is None:
        raise RuntimeError("ParticipantEventRepository is not configured")

    payload = dict(data)
    if "participant_id" not in payload and "pid" in payload:
        payload["participant_id"] = payload.pop("pid")
    if "event_id" not in payload and "eid" in payload:
        payload["event_id"] = payload.pop("eid")

    if not payload.get("participant_id") or not payload.get("event_id"):
        raise ValueError("'participant_id' and 'event_id' are required")

    canonical_keys = {k for k in payload.keys() if payload[k] is not None}
    minimal_keys = {"participant_id", "event_id"}
    if canonical_keys.issubset(minimal_keys):
        _participant_event_repo.ensure_link(
            participant_id=payload["participant_id"],
            event_id=payload["event_id"],
        )
        return

    event_participant = EventParticipant(**payload)
    _participant_event_repo.upsert(event_participant)


def list_events_for_participant(pid: str) -> List[Event]:
    """Return Event objects the participant has attended."""
    if not _participant_event_repo or not _event_repo:
        return []

    try:
        eids = _participant_event_repo.find_events(pid)
    except Exception:  # pragma: no cover - tolerate datastore errors
        return []

    events: List[Event] = []
    for eid in eids:
        try:
            event = _event_repo.find_by_eid(eid)
        except Exception:  # pragma: no cover
            event = None
        if event:
            events.append(event)
    return events


def event_participants_with_scores(eid: str) -> Dict[str, Any]:
    """Return participants for an event and average test scores."""
    if not _participant_event_repo or not _participant_repo:
        return {"participants": [], "avg_pre": 0.0, "avg_post": 0.0}

    try:
        pids = _participant_event_repo.find_participants(eid)
    except Exception:  # pragma: no cover - tolerate datastore errors
        pids = []

    participants: List[Any] = []
    for pid in pids:
        try:
            participant = _participant_repo.find_by_pid(pid)
        except Exception:  # pragma: no cover
            participant = None
        if participant:
            participants.append(participant)

    tests = []
    if _test_repo:
        try:
            tests = _test_repo.find_by_event(eid)
        except Exception:  # pragma: no cover
            tests = []

    pre_scores = [t.score for t in tests if t.type == AttemptType.pre]
    post_scores = [t.score for t in tests if t.type == AttemptType.post]
    avg_pre = sum(pre_scores) / len(pre_scores) if pre_scores else 0.0
    avg_post = sum(post_scores) / len(post_scores) if post_scores else 0.0

    return {
        "participants": participants,
        "avg_pre": avg_pre,
        "avg_post": avg_post,
    }


def get_participant_event_snapshot(
    pid: str, eid: str
) -> Optional[EventParticipant | Mapping[str, Any]]:
    """Return the per-event snapshot for a participant, if available."""

    if not _participant_event_repo:
        return None

    try:
        snapshot = _participant_event_repo.find(pid, eid)
    except Exception:  # pragma: no cover - tolerate datastore errors
        snapshot = None

    if snapshot is not None:
        return snapshot

    # Fall back to the raw MongoDB document if validation fails so that we can
    # still show any partial data captured for legacy records.
    try:
        raw_snapshot = _participant_event_repo.find_raw(pid, eid)
    except Exception:  # pragma: no cover - tolerate datastore errors
        raw_snapshot = None

    if not raw_snapshot:
        return None

    if not isinstance(raw_snapshot, Mapping):
        return None

    return raw_snapshot
