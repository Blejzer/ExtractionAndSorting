from datetime import date

from domain.models.participant import (
    DocType,
    Gender,
    IbanType,
    Participant,
    Transport,
)


def _base_participant_data(**overrides):
    data = {
        "pid": "P001",
        "representing_country": "HR",
        "gender": Gender.male,
        "name": "John Doe",
        "dob": date(1990, 1, 1),
        "pob": "Zagreb",
        "birth_country": "HR",
        "citizenships": ["HR"],
        "travel_doc_type": DocType.passport,
        "travel_doc_issue_date": date(2020, 1, 1),
        "travel_doc_expiry_date": date(2030, 1, 1),
        "travel_doc_issued_by": "HR",
        "transportation": Transport.pov,
        "travelling_from": "Zagreb",
        "returning_to": "Zagreb",
        "diet_restrictions": "None",
        "organization": "Org",
        "unit": "Unit",
        "position": "Position",
        "rank": "Rank",
        "intl_authority": False,
        "bio_short": "Bio",
        "bank_name": "Bank",
        "iban": "HR1234567890123456789",
        "iban_type": IbanType.eur,
        "swift": "ABCDEFG",
    }
    data.update(overrides)
    return data


def test_phone_absent_allows_validation():
    data = _base_participant_data()
    data.pop("phone", None)

    participant = Participant(**data)

    assert participant.phone is None


def test_blank_phone_normalized_to_none():
    data = _base_participant_data(phone="   ")

    participant = Participant(**data)

    assert participant.phone is None
