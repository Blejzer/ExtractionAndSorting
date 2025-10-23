from __future__ import annotations

from datetime import datetime

import pytest

from domain.models.event import Event
from domain.models.event_participant import DocType, EventParticipant, Transport
from domain.models.participant import Grade, Participant
from services.import_service import commit_import


class FakeEventRepository:
    def __init__(self, existing: list[Event] | None = None) -> None:
        self._events = {event.eid: event for event in (existing or [])}
        self.saved_event: Event | None = None
        self.ensure_indexes_called = 0

    def ensure_indexes(self) -> None:
        self.ensure_indexes_called += 1

    def find_by_eid(self, eid: str) -> Event | None:
        return self._events.get(eid)

    def save(self, event: Event) -> str:
        self.saved_event = event
        self._events[event.eid] = event
        return event.eid


class FakeParticipantRepository:
    def __init__(self, existing: list[Participant] | None = None) -> None:
        self._participants = {p.pid: p for p in (existing or [])}
        self.saved: list[Participant] = []
        self.updated: list[tuple[str, dict]] = []
        self.ensure_indexes_called = 0

    def ensure_indexes(self) -> None:
        self.ensure_indexes_called += 1

    def find_by_pid(self, pid: str) -> Participant | None:
        return self._participants.get(pid)

    def save(self, participant: Participant) -> str:
        self.saved.append(participant)
        self._participants[participant.pid] = participant
        return participant.pid

    def update(self, pid: str, data: dict) -> Participant | None:
        self.updated.append((pid, data))
        existing = self._participants.get(pid)
        if existing:
            merged = existing.model_copy(update=data)
        else:
            merged = Participant.model_validate({"pid": pid, **data})
        self._participants[pid] = merged
        return merged


class FakeParticipantEventRepository:
    def __init__(self) -> None:
        self.bulk_payloads: list[list[EventParticipant]] = []
        self.ensure_indexes_called = 0

    def ensure_indexes(self) -> None:
        self.ensure_indexes_called += 1

    def bulk_upsert(self, entries: list[EventParticipant]) -> list[str]:
        self.bulk_payloads.append(list(entries))
        return []


def _participant_payload(**overrides) -> dict:
    base = {
        "representing_country": "HR",
        "gender": "Male",
        "grade": Grade.NORMAL.value,
        "name": "John Doe",
        "dob": datetime(1990, 5, 4),
        "pob": "Zagreb",
        "birth_country": "HR",
        "citizenships": ["HR"],
    }
    base.update(overrides)
    participant = Participant.model_validate(base)
    return participant.model_dump(by_alias=True)


def _event_participant_payload(pid: str, eid: str, **overrides) -> dict:
    base = {
        "eid": eid,
        "participant_id": pid,
        "transportation": Transport.air.value,
        "travelling_from": "City A",
        "returning_to": "City B",
        "travel_doc_type": DocType.passport.value,
    }
    base.update(overrides)
    snapshot = EventParticipant.model_validate(base)
    return snapshot.model_dump(by_alias=True)


def test_commit_import_aborts_when_event_exists():
    existing_event = Event(eid="EVT-1", title="Existing Event")
    payload = {"event": {"eid": "EVT-1", "title": "Existing Event", "place": "Zagreb"}}

    event_repo = FakeEventRepository([existing_event])
    participant_repo = FakeParticipantRepository()
    participant_event_repo = FakeParticipantEventRepository()

    with pytest.raises(RuntimeError) as excinfo:
        commit_import(
            payload,
            event_repo=event_repo,
            participant_repo=participant_repo,
            participant_event_repo=participant_event_repo,
        )

    assert "Event (EVT-1" in str(excinfo.value)


def test_commit_import_updates_and_creates_participants_and_links():
    event_payload = {"eid": "EVT-42", "title": "Training", "place": "Zagreb"}

    existing_participant = Participant.model_validate(
        {
            "pid": "P-001",
            "representing_country": "HR",
            "gender": "Male",
            "grade": Grade.NORMAL.value,
            "name": "Jane Existing",
            "dob": datetime(1988, 7, 1),
            "pob": "Split",
            "birth_country": "HR",
            "citizenships": ["HR"],
            "phone": "+3851000000",
        }
    )

    participant_repo = FakeParticipantRepository([existing_participant])
    event_repo = FakeEventRepository()
    participant_event_repo = FakeParticipantEventRepository()

    updated_participant = existing_participant.model_copy(update={"phone": "+3852000000"})
    updated_payload = updated_participant.model_dump(by_alias=True)

    new_participant_payload = _participant_payload(pid="P-002", name="John New")

    event_participant_existing = _event_participant_payload("P-001", "EVT-42")
    event_participant_new = _event_participant_payload("P-002", "EVT-42")

    payload = {
        "event": event_payload,
        "participants": [updated_payload, new_participant_payload],
        "participant_events": [event_participant_existing, event_participant_new],
    }

    result = commit_import(
        payload,
        event_repo=event_repo,
        participant_repo=participant_repo,
        participant_event_repo=participant_event_repo,
    )

    assert event_repo.saved_event is not None
    assert event_repo.saved_event.participants == ["P-001", "P-002"]

    # Existing participant should be updated with the new phone number
    assert participant_repo._participants["P-001"].phone == "+3852000000"
    # New participant should be persisted
    assert "P-002" in participant_repo._participants

    # Participant-event snapshots should be forwarded for persistence
    assert participant_event_repo.bulk_payloads
    saved_snapshots = participant_event_repo.bulk_payloads[0]
    assert {snap.participant_id for snap in saved_snapshots} == {"P-001", "P-002"}
    assert all(snap.eid == "EVT-42" for snap in saved_snapshots)

    # The result echoes hydrated models
    assert len(result["participants"]) == 2
    assert {p.pid for p in result["participants"]} == {"P-001", "P-002"}
