from datetime import datetime

import pytest

from domain.models.participant import Gender
from services import import_service


def _participant_record(**overrides):
    record = {
        "pid": "P001",
        "representing_country": "HR",
        "gender": "Male",
        "grade": "1",
        "name": "John Doe",
        "dob": datetime(1990, 1, 1),
        "pob": "Zagreb",
        "birth_country": "HR",
        "citizenships": ["HR"],
    }
    record.update(overrides)
    return record


@pytest.mark.parametrize(
    "raw_gender, expected",
    [
        ("Mr", Gender.male),
        ("Mrs", Gender.female),
        ("Ms", Gender.female),
    ],
)
def test_build_participant_normalizes_gender_titles(raw_gender, expected):
    participant = import_service._build_participant_from_record(
        _participant_record(gender=raw_gender)
    )

    assert participant is not None
    assert participant.gender == expected
