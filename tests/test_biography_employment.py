from datetime import date

import pytest

from utils.biography_employment import employment_from_bio
from utils.professional_experience import extract_professional_experience


@pytest.mark.parametrize("bio", [
    "I am an investigator in the Anti-Drugs Unit in the Ministry of Interior.",
    "Since 2015 he is an investigator in the Anti-Drugs Unit in the Ministry of Interior.",
    "Currently working as an investigator in the Anti-Drugs Unit in the Ministry of Interior.",
    "She has been an investigator in the Anti-Drugs Unit in the Ministry of Interior since 2015.",
])
def test_explicit_current_employment_uses_the_source_words(bio):
    fields, ambiguous = employment_from_bio(bio)
    assert fields["position"]["value"] == "investigator"
    assert fields["unit"]["value"] == "Anti-Drugs Unit"
    assert fields["organization"]["value"] == "Ministry of Interior"
    assert all(item["evidence"] == bio for item in fields.values())
    assert ambiguous == []


@pytest.mark.parametrize("bio", [
    "I was an investigator in the Anti-Drugs Unit in the Ministry of Interior.",
    "I am not an investigator in the Anti-Drugs Unit in the Ministry of Interior.",
    "I am attending training at the Ministry of Interior.",
    "I previously worked in the Anti-Drugs Unit.",
    "I joined the police in 1995.",
])
def test_history_negation_and_training_are_not_current_employment(bio):
    assert employment_from_bio(bio) == ({}, [])


def test_conflicting_current_employment_is_reviewed():
    bio = "I am an investigator in the Drug Unit. I am an analyst in the Cybercrime Unit."
    fields, ambiguous = employment_from_bio(bio)
    assert fields == {}
    assert set(ambiguous) == {"position", "unit"}


@pytest.mark.parametrize("bio", [
    "He joined the police in 1995.",
    "In 1995 he joined the police.",
    "I started working in the police in 1995.",
    "I joined the police\nin 1995.",
])
def test_police_start_is_independent_of_later_unit_appointment(bio):
    result = extract_professional_experience(
        bio + " Since 2015 he is an investigator in the Anti-Drugs Unit.",
        reference_date=date(2016, 5, 1))
    assert result.joining_year == 1995
    assert result.years == 21
    assert result.method == "Years since joining (estimate)"
