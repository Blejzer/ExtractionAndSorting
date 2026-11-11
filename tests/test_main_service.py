from datetime import date, datetime

import pytest

from services.statistics_service import build_statistics, count_attendances


def test_fetch_main_stats(monkeypatch):
    """Homepage attendance includes repeat visits without counting a link twice."""
    class DummyCursor:
        def __init__(self, docs):
            self.docs = list(docs)
        def __iter__(self):
            return iter(self.docs)
        def sort(self, field, direction):
            self.docs.sort(key=lambda doc: doc.get(field) or datetime.min, reverse=direction < 0)
            return self
        def limit(self, count):
            return iter(self.docs[:count])

    class DummyCollection:
        def __init__(self, docs):
            self.docs = docs
        def count_documents(self, *_):
            return len(self.docs)
        def find(self, *_, **__):
            return DummyCursor(self.docs)

    data = {
        "participants": DummyCollection([{"pid": "P1"}, {"pid": "P2"}, {"pid": "UNUSED"}]),
        "events": DummyCollection([
            {"eid": "E1", "participants": ["P1", "P1", "P2"],
             "start_date": datetime(2024, 1, 1)},
            {"eid": "E2", "participant_ids": ["P1"],
             "start_date": datetime(2024, 2, 1)},
            {"eid": "E3", "title": "Test Event", "start_date": datetime(2024, 5, 1)},
        ]),
        "participant_events": DummyCollection([
            {"event_id": "E1", "participant_id": "P1"},
            {"event_id": "E1", "participant_id": "P1"},
            {"event_id": "E2", "participant_id": "P2"},
        ]),
        "countries": DummyCollection([{"cid": "C1"}, {"cid": "C2"}]),
    }
    class DummyMongo:
        def db(self):
            return data

    import services.main_service as main_service
    monkeypatch.setattr(main_service, "mongodb", DummyMongo())
    stats = main_service.fetch_main_stats()
    assert stats["attendances"] == 4
    assert stats["events"] == 3
    assert stats["countries"] == 2
    assert stats["latest_event"] == "Test Event"
    assert stats["latest_event_date"] == datetime(2024, 5, 1)


@pytest.mark.parametrize("events,participants,links,expected", [
    ([], [{"pid": "UNUSED"}], [], 0),
    ([{"eid": "E1", "participants": ["P1", "P1"]}], [], [], 1),
    ([{"eid": "E1", "participants": ["P1"]},
      {"eid": "E2", "participants": ["P1"]}], [], [], 2),
    ([{"eid": "E1", "participants": ["P1"], "participant_ids": ["P1", "P2"]}],
     [], [{"event_id": "E1", "participant_id": "P2"},
          {"event_id": "E1", "participant_id": "P3"}], 3),
    ([{"eid": "E1"}], [], [
        {"event_id": "MISSING", "participant_id": "P1"},
        {"event_id": "E1", "participant_id": ""},
        {"participant_id": "P2"}], 0),
    ([{"_id": "mongo-event", "eid": "E1", "participants": ["mongo-person"]}],
     [{"_id": "mongo-person", "pid": "P1"}],
     [{"event_id": "mongo-event", "participant_id": "mongo-person"},
      {"eid": "E1", "pid": "P1"}], 1),
])
def test_attendances_reconcile_snapshots_rosters_and_identifier_aliases(events, participants, links, expected):
    assert count_attendances(events, participants, links) == expected
    # The front page and the unfiltered statistics report agree on attendance.
    report = build_statistics(events, participants, links, [], as_of=date(2026, 1, 1))
    assert report["summary"]["attendances"] == expected
