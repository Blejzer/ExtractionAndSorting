from datetime import date
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import create_app
from domain.models.event_participant import DocType, EventParticipant, IbanType, Transport
import routes.participants as participant_routes


def _build_snapshot() -> EventParticipant:
    return EventParticipant(
        event_id="E-123",
        participant_id="P-456",
        transportation=Transport.other,
        transport_other="  Chartered boat  ",
        requires_visa_hr=False,
        travelling_from="HR",
        returning_to="US",
        travel_doc_type=DocType.other,
        travel_doc_type_other="Laissez-passer",
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
        {"field": "travel_doc_type", "label": "Travel Document Type", "value": "Other"},
        {
            "field": "travel_doc_type_other",
            "label": "Travel Document Type (Other)",
            "value": "Laissez-passer",
        },
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
            "field": "travelling_from",
            "label": "Travelling From",
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
        "travelling_from": "  HR  ",
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
            "field": "travel_doc_type_other",
            "label": "Travel Document Type (Other)",
            "value": None,
        },
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
            "field": "travelling_from",
            "label": "Travelling From",
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
