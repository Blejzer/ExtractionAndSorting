from datetime import datetime, timezone
import os
import sys

import pytest

# Ensure the project root is on sys.path for importing the domain package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from domain.models.event import Event


def test_event_to_from_mongo_roundtrip():
    start = datetime(2025, 5, 1, tzinfo=timezone.utc)
    end = datetime(2025, 5, 3, tzinfo=timezone.utc)
    ts = datetime(2025, 5, 4, tzinfo=timezone.utc)

    evt = Event(
        eid="E001",
        title="Summit",
        start_date=start,
        end_date=end,
        place="Zagreb",
        country="C001",
        type="Training",
        cost=123.45,
        participants=["p001", "p002"],
        created_at=ts,
        updated_at=ts,
        audit=[
            {
                "ts": ts,
                "actor": "import",
                "field": "eid",
                "from": None,
                "to": "E001",
            }
        ],
    )

    mongo_doc = evt.to_mongo()
    assert mongo_doc == {
        "eid": "E001",
        "title": "Summit",
        "start_date": start,
        "end_date": end,
        "place": "Zagreb",
        "country": "C001",
        "type": "Training",
        "cost": 123.45,
        "participants": ["p001", "p002"],
        "created_at": ts,
        "updated_at": ts,
        "_audit": [
            {
                "ts": ts,
                "actor": "import",
                "field": "eid",
                "from": None,
                "to": "E001",
            }
        ],
    }

    evt2 = Event.from_mongo(mongo_doc)
    assert evt2 == evt

    assert Event.from_mongo(None) is None


def test_event_from_mongo_defaults_participant_ids():
    mongo_doc = {
        "eid": "E002",
        "title": "Incomplete",
        "location": "Nowhere",
        "dateFrom": datetime(2024, 6, 1, tzinfo=timezone.utc),
        "dateTo": datetime(2024, 6, 2, tzinfo=timezone.utc),
    }

    evt = Event.from_mongo(mongo_doc)
    assert evt.participants == []
    assert evt.place == "Nowhere"


def test_event_date_validation():
    with pytest.raises(ValueError):
        Event(
            eid="E1",
            title="Invalid",
            place="X",
            start_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 5, 1, tzinfo=timezone.utc),
        )


def test_event_participant_ids_non_empty():
    with pytest.raises(ValueError):
        Event(
            eid="E1",
            title="Bad participants",
            place="X",
            start_date=datetime(2025, 5, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 5, 2, tzinfo=timezone.utc),
            participants=["p001", ""],
        )

