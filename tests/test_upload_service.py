from datetime import datetime

import pytest

from domain.models.event import Event
from domain.models.participant import Participant, Grade, Gender
from services.upload_service import UploadError, upload_preview_data


class FakeEventRepo:
    def __init__(self):
        self.events: dict[str, Event] = {}

    def find_by_eid(self, eid: str):
        return self.events.get(eid)

    def save(self, event: Event):
        self.events[event.eid] = event
        return event.eid


class FakeParticipantRepo:
    def __init__(self):
        self.participants: dict[str, Participant] = {}
        self.counter = 1

    def find_by_name_dob_and_representing_country_cid(self, *, name, dob, representing_country):
        for participant in self.participants.values():
            if (
                participant.name == name
                and participant.dob == dob
                and participant.representing_country == representing_country
            ):
                return participant
        return None

    def generate_next_pid(self):
        pid = f"P{self.counter:04d}"
        self.counter += 1
        return pid

    def save(self, participant: Participant):
        self.participants[participant.pid] = participant
        return participant.pid

    def update(self, pid: str, data):
        existing = self.participants.get(pid)
        if not existing:
            return None
        payload = existing.model_dump(mode="python")
        payload.update(data)
        updated = Participant.model_validate(payload)
        self.participants[pid] = updated
        return updated


class FakeParticipantEventRepo:
    def __init__(self):
        self.snapshots = []

    def bulk_upsert(self, entries):
        self.snapshots.extend(entries)
        return [str(index) for index, _ in enumerate(entries, start=1)]


def _base_participant(**overrides):
    payload = {
        "name": "Jane Doe",
        "representing_country": "HR",
        "gender": Gender.female.value,
        "grade": Grade.NORMAL.value,
        "dob": "1990-01-01",
        "pob": "Zagreb",
        "birth_country": "HR",
        "citizenships": ["HR"],
        "email": "jane@example.com",
        "phone": "+385123456",
        "transportation": "Air (Airplane)",
        "traveling_from": "Zagreb",
        "returning_to": "Zagreb",
        "travel_doc_type": "Passport",
    }
    payload.update(overrides)
    return payload


def _base_event():
    return {
        "eid": "EVT-001",
        "title": "Sample Event",
        "start_date": "2024-01-01",
        "end_date": "2024-01-05",
        "place": "Zagreb",
    }


def test_upload_preview_data_creates_records():
    event_repo = FakeEventRepo()
    participant_repo = FakeParticipantRepo()
    event_participant_repo = FakeParticipantEventRepo()

    bundle = {
        "event": _base_event(),
        "participants": [_base_participant()],
        "participant_events": [],
    }

    result = upload_preview_data(
        bundle,
        event_repo=event_repo,
        participant_repo=participant_repo,
        participant_event_repo=event_participant_repo,
    )

    saved_event = event_repo.events["EVT-001"]
    assert saved_event.participants == ["P0001"]

    assert list(participant_repo.participants) == ["P0001"]
    stored_participant = participant_repo.participants["P0001"]
    assert stored_participant.name == "Jane DOE"
    assert stored_participant.dob == datetime(1990, 1, 1)

    assert event_participant_repo.snapshots
    snapshot = event_participant_repo.snapshots[0]
    assert snapshot.event_id == "EVT-001"
    assert snapshot.participant_id == "P0001"
    assert snapshot.traveling_from == "Zagreb"

    assert result["event"].eid == "EVT-001"
    assert result["participants"][0].pid == "P0001"
    assert result["participant_events"][0].event_id == "EVT-001"


def test_upload_preview_updates_existing_participant():
    event_repo = FakeEventRepo()
    participant_repo = FakeParticipantRepo()
    event_participant_repo = FakeParticipantEventRepo()

    existing = Participant.model_validate(
        {
            **_base_participant(pid="P1234", phone="+385999999"),
            "pid": "P1234",
        }
    )
    participant_repo.participants[existing.pid] = existing

    bundle = {
        "event": _base_event(),
        "participants": [
            _base_participant(pid="P1234", phone="+385111111", transportation="Personal Vehicle (POV)"),
        ],
        "participant_events": [
            {
                "participant_id": "P1234",
                "transportation": "Personal Vehicle (POV)",
                "transport_other": "Car",
                "traveling_from": "Split",
                "returning_to": "Zagreb",
                "travel_doc_type": "Passport",
            }
        ],
    }

    upload_preview_data(
        bundle,
        event_repo=event_repo,
        participant_repo=participant_repo,
        participant_event_repo=event_participant_repo,
    )

    updated = participant_repo.participants["P1234"]
    assert updated.phone == "+385111111"
    assert event_repo.events["EVT-001"].participants == ["P1234"]
    snapshot = event_participant_repo.snapshots[0]
    assert snapshot.transportation == "Personal Vehicle (POV)"
    assert snapshot.traveling_from == "Split"


def test_upload_preview_rejects_duplicate_event():
    event_repo = FakeEventRepo()
    event_repo.save(Event(eid="EVT-001", title="Existing"))

    with pytest.raises(UploadError):
        upload_preview_data(
            {
                "event": _base_event(),
                "participants": [_base_participant()],
                "participant_events": [],
            },
            event_repo=event_repo,
            participant_repo=FakeParticipantRepo(),
            participant_event_repo=FakeParticipantEventRepo(),
        )