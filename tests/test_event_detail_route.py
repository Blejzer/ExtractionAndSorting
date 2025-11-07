import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import create_app
import routes.events as events_routes
from services.events_service import EventDetail, EventSummary, ParticipantSummary


def _make_detail() -> EventDetail:
    event = EventSummary(
        eid="E123",
        title="Sample Event",
        place="Zagreb",
        country="Croatia",
        start_date=datetime(2024, 5, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 5, 3, tzinfo=timezone.utc),
        type="Training",
        cost=199.5,
        participant_count=2,
        country_code="CRO",
    )
    participants = [
        ParticipantSummary(
            pid="P001",
            name="Alice",
            position="Analyst",
            grade="G1",
            country="Croatia",
        ),
        ParticipantSummary(
            pid="P002",
            name="Bob",
            position="Manager",
            grade="G2",
            country="Croatia",
        ),
    ]
    return EventDetail(event=event, participants=participants)


def test_event_detail_view_includes_roster_and_links(monkeypatch):
    calls = []

    def fake_detail(eid: str, *, sort: str = "name", direction: int = 1):
        calls.append({"sort": sort, "direction": direction})
        return _make_detail()

    monkeypatch.setattr(events_routes, "event_detail_for_display", fake_detail)

    app = create_app()
    client = app.test_client()

    response = client.get(
        "/events/E123",
        query_string={
            "list_page": 2,
            "list_sort": "title",
            "list_direction": "-1",
            "list_search": "accession",
        },
    )

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Participant roster" in html
    assert "PID" in html
    assert "Cost" in html
    assert "199.50" in html
    assert (
        'href="/events?page=2&amp;sort=title&amp;direction=-1&amp;search=accession"'
        in html
    )
    assert calls == [{"sort": "name", "direction": 1}]


def test_event_edit_updates_event(monkeypatch):
    detail = _make_detail()

    monkeypatch.setattr(events_routes, "event_detail_for_display", lambda *args, **kwargs: detail)

    updated_payload = {}

    def fake_update_event(eid: str, updates: dict):
        updated_payload["eid"] = eid
        updated_payload["updates"] = updates
        return detail.event  # returning a truthy EventSummary surrogate

    monkeypatch.setattr(events_routes, "update_event", fake_update_event)

    app = create_app()
    client = app.test_client()

    response = client.post(
        "/events/E123/edit",
        data={
            "title": "Updated title",
            "type": "Workshop",
            "place": "Split",
            "country": "HR",
            "start_date": "2024-06-01",
            "end_date": "2024-06-05",
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/events/E123")

    assert updated_payload["eid"] == "E123"
    updates = updated_payload["updates"]
    assert updates["title"] == "Updated title"
    assert updates["type"] == "Workshop"
    assert updates["place"] == "Split"
    assert updates["country"] == "HR"
    assert updates["start_date"].isoformat() == "2024-06-01T00:00:00+00:00"
    assert updates["end_date"].isoformat() == "2024-06-05T00:00:00+00:00"


def test_event_edit_validation_errors(monkeypatch):
    detail = _make_detail()

    monkeypatch.setattr(events_routes, "event_detail_for_display", lambda *args, **kwargs: detail)

    app = create_app()
    client = app.test_client()

    response = client.post(
        "/events/E123/edit",
        data={
            "title": "",
            "start_date": "2024-07-10",
            "end_date": "2024-07-01",
        },
    )

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Title is required." in html
    assert "End date must be on or after the start date." in html
