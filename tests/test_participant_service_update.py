from datetime import datetime

import pytest

from domain.models.participant import (
    DocType,
    Gender,
    Grade,
    Participant,
    Transport,
)
from services import participant_service


class _Form(dict):
    """Simple stand-in for ``request.form`` supporting ``getlist``."""

    def get(self, key, default=None):  # type: ignore[override]
        value = super().get(key, default)
        if isinstance(value, list):
            return value[0] if value else default
        return value

    def getlist(self, key):  # type: ignore[override]
        value = super().get(key, [])
        if isinstance(value, list):
            return value
        if value is None:
            return []
        return [value]


def _base_participant() -> Participant:
    return Participant(
        pid="P001",
        representing_country="HR",
        gender=Gender.male,
        grade=Grade.NORMAL,
        name="John Doe",
        dob=datetime(1990, 1, 1),
        pob="Zagreb",
        birth_country="HR",
        citizenships=["HR"],
        travel_doc_type=DocType.passport,
        travel_doc_issued_by="Ministry of Interior",
        transportation=Transport.pov,
        organization="Org",
        position="Analyst",
    )


def test_update_participant_from_form_updates_fields_and_audit(monkeypatch):
    participant = _base_participant()

    class DummyRepo:
        def __init__(self):
            self.updated_payload = None

        def find_by_pid(self, pid):
            return participant

        def update(self, pid, data):
            self.updated_payload = data
            return Participant.from_mongo(data)

    repo = DummyRepo()
    monkeypatch.setattr(participant_service, "_repo", repo)
    monkeypatch.setattr(
        participant_service,
        "_load_country_map",
        lambda: {"HR": "Croatia", "US": "United States"},
    )

    form = _Form(
        {
            "name": "Jane Doe",
            "representing_country": "US",
            "gender": "Male",
            "grade": "2",
            "position": "Manager",
            "organization": "Org",
            "unit": "Unit",
            "rank": "Captain",
            "intl_authority": "true",
            "dob": "1991-02-02",
            "pob": "Split",
            "birth_country": "US",
            "citizenships": ["HR", "US"],
            "email": "jane@example.com",
            "phone": "+385123456",
            "travel_doc_type": "Passport",
            "travel_doc_type_other": "",
            "travel_doc_issued_by": "U.S. Department of State",
            "travel_doc_issue_date": "2020-01-01",
            "travel_doc_expiry_date": "2030-01-01",
            "transportation": "Airplane",
            "transport_other": "",
            "travelling_from": "Washington, DC",
            "returning_to": "Zagreb, Croatia",
            "diet_restrictions": "Vegetarian",
            "bio_short": "Bio",
            "bank_name": "Bank",
            "iban": "HR123",
            "iban_type": "EURO",
            "swift": "SWIFTHR",
        }
    )

    updated = participant_service.update_participant_from_form("P001", form, actor="test")

    assert updated is not None
    assert updated.representing_country == "US"
    assert updated.grade == Grade.EXCELLENT.value
    assert updated.birth_country == "US"
    assert updated.transportation == Transport.airplane.value
    assert updated.citizenships == ["HR", "US"]
    assert updated.audit, "audit history should not be empty"
    assert any(entry["field"] == "representing_country" for entry in updated.audit)
    assert updated.updated_at is not None


def test_update_participant_from_form_invalid_country(monkeypatch):
    participant = _base_participant()

    class DummyRepo:
        def find_by_pid(self, pid):
            return participant

    monkeypatch.setattr(participant_service, "_repo", DummyRepo())
    monkeypatch.setattr(
        participant_service, "_load_country_map", lambda: {"HR": "Croatia"}
    )

    form = _Form(
        {
            "name": "Jane Doe",
            "representing_country": "XX",
            "gender": "Male",
            "grade": "1",
            "position": "Analyst",
            "dob": "1990-01-01",
            "pob": "Zagreb",
            "birth_country": "HR",
            "citizenships": ["HR"],
        }
    )

    with pytest.raises(ValueError):
        participant_service.update_participant_from_form("P001", form)
