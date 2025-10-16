import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app as app_module
import routes.participants as participant_routes
from domain.models.participant import Grade, Participant


def _make_participant() -> Participant:
    return Participant(
        pid="P123",
        representing_country="US",
        gender="Male",
        grade=Grade.NORMAL,
        name="Test User",
        dob=datetime(1990, 1, 1),
        pob="City",
        birth_country="US",
        email=None,
        phone=None,
        travel_doc_issue_date=None,
        travel_doc_expiry_date=None,
        travel_doc_type=None,
        transportation=None,
        intl_authority=None,
        iban=None,
    )


def test_edit_participant_prefill_uses_empty_strings(monkeypatch):
    monkeypatch.setattr(app_module, "check_and_import_data", lambda: None)

    participant = _make_participant()

    monkeypatch.setattr(participant_routes, "get_participant", lambda pid: participant)
    monkeypatch.setattr(
        participant_routes,
        "get_country_choices",
        lambda: [("US", "United States")],
    )
    monkeypatch.setattr(participant_routes, "get_grade_choices", lambda: [(Grade.NORMAL, "Normal")])
    monkeypatch.setattr(participant_routes, "get_gender_choices", lambda: ["Male", "Female"])
    monkeypatch.setattr(participant_routes, "get_transport_choices", lambda: ["Car"])
    monkeypatch.setattr(participant_routes, "get_document_type_choices", lambda: ["Passport"])
    monkeypatch.setattr(participant_routes, "get_iban_type_choices", lambda: ["EURO"])

    app = app_module.create_app()
    client = app.test_client()

    response = client.get("/participant/P123/edit")
    assert response.status_code == 200

    html = response.get_data(as_text=True)
    assert 'value="None"' not in html
    assert 'name="email" value=""' in html
    assert 'name="travel_doc_issue_date" value=""' in html
    assert 'name="phone" value=""' in html
