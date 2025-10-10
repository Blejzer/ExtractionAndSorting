from datetime import date
import os
import sys

import pytest

# Ensure the project root is on sys.path for importing the domain package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from domain.models.event import Event


def test_event_to_from_mongo_roundtrip():
    evt = Event(
        eid="E001",
        title="Summit",
        location="Zagreb",
        date_from=date(2025, 5, 1),
        date_to=date(2025, 5, 3),
        participant_ids=["p001", "p002"],
    )

    mongo_doc = evt.to_mongo()
    assert mongo_doc == {
        "eid": "E001",
        "title": "Summit",
        "location": "Zagreb",
        "dateFrom": date(2025, 5, 1),
        "dateTo": date(2025, 5, 3),
        "participant_ids": ["p001", "p002"],
    }

    evt2 = Event.from_mongo(mongo_doc)
    assert evt2 == evt

    assert Event.from_mongo(None) is None


def test_event_from_mongo_defaults_participant_ids():
    mongo_doc = {
        "eid": "E002",
        "title": "Incomplete",
        "location": "Nowhere",
        "dateFrom": date(2024, 6, 1),
        "dateTo": date(2024, 6, 2),
    }

    evt = Event.from_mongo(mongo_doc)
    assert evt.participant_ids == []


def test_event_date_validation():
    with pytest.raises(ValueError):
        Event(
            eid="E1",
            title="Invalid",
            location="X",
            date_from=date(2025, 6, 1),
            date_to=date(2025, 5, 1),
        )


def test_event_participant_ids_non_empty():
    with pytest.raises(ValueError):
        Event(
            eid="E1",
            title="Bad participants",
            location="X",
            date_from=date(2025, 5, 1),
            date_to=date(2025, 5, 2),
            participant_ids=["p001", ""],
        )

