from datetime import date, datetime

import pytest
from domain.models.participant import Gender, Grade, Participant
from pydantic import ValidationError


def _base_participant_data(**overrides):
    data = {
        "pid": "P001",
        "representing_country": "HR",
        "gender": Gender.male,
        "grade": Grade.NORMAL,
        "name": "John Doe",
        "dob": date(1990, 1, 1),
        "pob": "Zagreb",
        "birth_country": "HR",
        "citizenships": ["HR"],
        "diet_restrictions": "None",
        "organization": "Org",
        "unit": "Unit",
        "position": "Position",
        "rank": "Rank",
        "intl_authority": False,
        "bio_short": "Bio",
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


def test_from_mongo_handles_legacy_documents():
    doc = {
        "pid": "LEGACY1",
        "representing_country": "HR",
        "gender": Gender.male,
        "name": "Legacy User",
        "dob": datetime(1985, 5, 5),
        "pob": "Zagreb",
        "birth_country": "HR",
        # legacy documents may omit extended profile fields
    }

    participant = Participant.from_mongo(doc)

    assert participant.pid == "LEGACY1"
    assert participant.citizenships is None
    assert participant.organization is None


def test_from_mongo_allows_missing_dob():
    doc = {
        "pid": "LEGACY2",
        "representing_country": "HR",
        "gender": Gender.male,
        "name": "Legacy User",
        "pob": "Zagreb",
        "birth_country": "HR",
    }

    participant = Participant.from_mongo(doc)

    assert participant.dob is None


def test_missing_dob_direct_instantiation_raises():
    data = _base_participant_data()
    data.pop("dob", None)

    with pytest.raises(ValidationError) as exc:
        Participant(**data)

    assert "dob" in str(exc.value)


def test_model_validate_requires_context_for_missing_dob():
    data = _base_participant_data()
    data.pop("dob", None)

    with pytest.raises(ValidationError):
        Participant.model_validate(data)


def test_model_validate_allows_missing_dob_with_context():
    data = _base_participant_data()
    data.pop("dob", None)

    participant = Participant.model_validate(
        data, context={"allow_missing_dob": True}
    )

    assert participant.dob is None
