import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import services.participant_event_service as svc
from domain.models.event import Event
from domain.models.test import TrainingTest, AttemptType


def test_register_participant_event_minimal_payload(monkeypatch):
    called = {}

    class DummyPERepo:
        def ensure_link(self, participant_id, event_id):
            called["ensure_link"] = (participant_id, event_id)

    monkeypatch.setattr(svc, "_participant_event_repo", DummyPERepo())

    svc.register_participant_event({"pid": "P1", "eid": "E1"})

    assert called["ensure_link"] == ("P1", "E1")


def test_list_events_for_participant(monkeypatch):
    class DummyPERepo:
        def find_events(self, pid):
            assert pid == "P1"
            return ["E1", "E2"]

    class DummyEventRepo:
        def find_by_eid(self, eid):
            return Event(
                eid=eid,
                title=f"Event {eid}",
                place="Loc",
                start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_date=datetime(2024, 1, 2, tzinfo=timezone.utc),
            )

    monkeypatch.setattr(svc, "_participant_event_repo", DummyPERepo())
    monkeypatch.setattr(svc, "_event_repo", DummyEventRepo())

    events = svc.list_events_for_participant("P1")
    assert [e.eid for e in events] == ["E1", "E2"]


def test_event_participants_with_scores(monkeypatch):
    class DummyPERepo:
        def find_participants(self, eid):
            assert eid == "E1"
            return ["P1", "P2"]

    class DummyParticipant:
        def __init__(self, pid):
            self.pid = pid
            self.participant_id = pid

        def model_dump(self):
            return {"pid": self.pid}

    class DummyParticipantRepo:
        def find_by_pid(self, pid):
            return DummyParticipant(pid)

    class DummyTestRepo:
        def find_by_event(self, eid):
            return [
                TrainingTest(eid=eid, pid="P1", type=AttemptType.pre, score=80),
                TrainingTest(eid=eid, pid="P2", type=AttemptType.pre, score=90),
                TrainingTest(eid=eid, pid="P1", type=AttemptType.post, score=85),
                TrainingTest(eid=eid, pid="P2", type=AttemptType.post, score=95),
            ]

    monkeypatch.setattr(svc, "_participant_event_repo", DummyPERepo())
    monkeypatch.setattr(svc, "_participant_repo", DummyParticipantRepo())
    monkeypatch.setattr(svc, "_test_repo", DummyTestRepo())

    result = svc.event_participants_with_scores("E1")
    assert [p.participant_id for p in result["participants"]] == ["P1", "P2"]
    assert result["avg_pre"] == 85
    assert result["avg_post"] == 90
