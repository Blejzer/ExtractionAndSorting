from datetime import date
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import create_app
from domain.models.event_participant import EventParticipant, IbanType, Transport
import routes.participants as participant_routes


@pytest.fixture(autouse=True)
def no_test_scores_by_default(monkeypatch):
    monkeypatch.setattr(participant_routes,"get_test_score",lambda eid,pid,attempt:None)


def _build_snapshot() -> EventParticipant:
    return EventParticipant(
        event_id="E-123",
        participant_id="P-456",
        transportation=Transport.other,
        transport_other="  Chartered boat  ",
        traveling_from="HR",
        returning_to="US",
        travel_doc_type="ID Card",
        travel_doc_issue_date=date(2024, 1, 5),
        travel_doc_expiry_date=date(2024, 12, 31),
        travel_doc_issued_by="HR",
        bank_name="  Adriatic Bank  ",
        iban="HR1212345678901234567",
        iban_type=IbanType.eur,
        swift=" ADRICH22  ",
    )


def test_event_details_route_returns_all_expected_fields(monkeypatch):
    snapshot = _build_snapshot()

    monkeypatch.setattr(
        participant_routes,
        "get_participant_event_snapshot",
        lambda pid, eid: snapshot,
    )
    monkeypatch.setattr(
        participant_routes,
        "get_country_lookup",
        lambda: {"HR": "Croatia", "US": "United States"},
    )

    app = create_app()
    client = app.test_client()

    response = client.get(
        f"/participant/{snapshot.participant_id}/events/{snapshot.event_id}/details",
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["available"] is True

    expected_details = [
        {"field": "travel_doc_type", "label": "Travel Document Type", "value": "ID Card"},
        {
            "field": "travel_doc_issue_date",
            "label": "Travel Document Issue Date",
            "value": "2024-01-05",
        },
        {
            "field": "travel_doc_expiry_date",
            "label": "Travel Document Expiry Date",
            "value": "2024-12-31",
        },
        {
            "field": "travel_doc_issued_by",
            "label": "Travel Document Issued By",
            "value": "Croatia",
        },
        {"field": "transportation", "label": "Transportation", "value": "Other"},
        {
            "field": "transport_other",
            "label": "Transportation (Other)",
            "value": "Chartered boat",
        },
        {
            "field": "traveling_from",
            "label": "Traveling From",
            "value": "Croatia",
        },
        {
            "field": "returning_to",
            "label": "Returning To",
            "value": "United States",
        },
        {"field": "bank_name", "label": "Bank Name", "value": "Adriatic Bank"},
        {"field": "iban", "label": "IBAN", "value": "HR1212345678901234567"},
        {"field": "iban_type", "label": "IBAN Type", "value": "EURO"},
        {"field": "swift", "label": "SWIFT", "value": "ADRICH22"},
    ]

    assert payload["details"] == expected_details


def test_event_details_route_handles_missing_snapshot(monkeypatch):
    monkeypatch.setattr(
        participant_routes,
        "get_participant_event_snapshot",
        lambda pid, eid: None,
    )

    app = create_app()
    client = app.test_client()

    response = client.get(
        "/participant/P-456/events/E-999/details",
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {"available": False, "details": []}


def test_event_details_route_handles_raw_snapshot(monkeypatch):
    raw_snapshot = {
        "_id": "abc123",
        "event_id": "E-raw",
        "participant_id": "P-raw",
        "travel_doc_type": "Passport",
        "travel_doc_issue_date": date(2023, 5, 1),
        "travel_doc_issued_by": "US",
        "transportation": "Bus",
        "traveling_from": "  HR  ",
        "returning_to": "US",
        "bank_name": "  Coastal Credit  ",
        "iban": "  HR1212345678901234567  ",
        "iban_type": "USD",
    }

    monkeypatch.setattr(
        participant_routes,
        "get_participant_event_snapshot",
        lambda pid, eid: raw_snapshot,
    )
    monkeypatch.setattr(
        participant_routes,
        "get_country_lookup",
        lambda: {"HR": "Croatia", "US": "United States"},
    )

    app = create_app()
    client = app.test_client()

    response = client.get(
        "/participant/P-raw/events/E-raw/details",
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["available"] is True

    expected_details = [
        {"field": "travel_doc_type", "label": "Travel Document Type", "value": "Passport"},
        {
            "field": "travel_doc_issue_date",
            "label": "Travel Document Issue Date",
            "value": "2023-05-01",
        },
        {
            "field": "travel_doc_expiry_date",
            "label": "Travel Document Expiry Date",
            "value": None,
        },
        {
            "field": "travel_doc_issued_by",
            "label": "Travel Document Issued By",
            "value": "United States",
        },
        {"field": "transportation", "label": "Transportation", "value": "Bus"},
        {
            "field": "transport_other",
            "label": "Transportation (Other)",
            "value": None,
        },
        {
            "field": "traveling_from",
            "label": "Traveling From",
            "value": "Croatia",
        },
        {
            "field": "returning_to",
            "label": "Returning To",
            "value": "United States",
        },
        {"field": "bank_name", "label": "Bank Name", "value": "Coastal Credit"},
        {"field": "iban", "label": "IBAN", "value": "HR1212345678901234567"},
        {"field": "iban_type", "label": "IBAN Type", "value": "USD"},
        {"field": "swift", "label": "SWIFT", "value": None},
    ]

    assert payload["details"] == expected_details


@pytest.mark.parametrize("pre,post,expected", [
    (64,80,[("pre_test_score","64.00%"),("post_test_score","80.00%"),("test_score_change","+16.00 percentage points")]),
    (0,None,[("pre_test_score","0.00%")]),
    (None,88.5,[("post_test_score","88.50%")]),
    (50,0,[("pre_test_score","50.00%"),("post_test_score","0.00%"),("test_score_change","-50.00 percentage points")]),
    (0,0,[("pre_test_score","0.00%"),("post_test_score","0.00%"),("test_score_change","+0.00 percentage points")]),
    (None,None,[]),
])
def test_event_test_scores_are_event_scoped_and_shown_only_when_saved(monkeypatch,pre,post,expected):
    calls=[]
    def score(eid,pid,attempt):
        calls.append((eid,pid,attempt))
        value={"pre":pre,"post":post}[attempt]
        return SimpleNamespace(score=value) if value is not None else None
    monkeypatch.setattr(participant_routes,"get_test_score",score)
    monkeypatch.setattr(participant_routes,"get_participant_event_snapshot",lambda pid,eid:None)
    response=create_app().test_client().get("/participant/P0104/events/PFE26M3/details")
    assert response.status_code==200
    data=response.get_json()
    assert [(d["field"],d["value"]) for d in data["details"]]==expected
    assert data["available"] is bool(expected)
    assert calls==[("PFE26M3","P0104","pre"),("PFE26M3","P0104","post")]


def test_scores_and_existing_travel_details_are_both_returned(monkeypatch):
    monkeypatch.setattr(participant_routes,"get_participant_event_snapshot",lambda pid,eid:_build_snapshot())
    monkeypatch.setattr(participant_routes,"get_country_lookup",lambda:{"HR":"Croatia"})
    monkeypatch.setattr(participant_routes,"get_test_score",lambda eid,pid,attempt:SimpleNamespace(score=75) if attempt=="post" else None)
    response=create_app().test_client().get("/participant/P0104/events/PFE26M3/details")
    details={d["field"]:d["value"] for d in response.get_json()["details"]}
    assert details["post_test_score"]=="75.00%" and details["traveling_from"]=="Croatia"
    assert "pre_test_score" not in details and "test_score_change" not in details
