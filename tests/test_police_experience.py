from datetime import date

import pytest

from utils.police_experience import extract_police_experience


@pytest.mark.parametrize("bio, years", [
    ("She has 15 years of experience in the police.", 15),
    ("20 years of police service.", 20),
    ("Worked in law enforcement for 12 years.", 12),
    ("Ima 18 godina rada u policiji.", 18),
    ("He has been a police officer for 15 years.", 15),
])
def test_explicit_total_service_retains_original_evidence(bio, years):
    result = extract_police_experience(bio, reference_date=date(2020, 1, 1))
    assert result.years == years
    assert result.method == "Stated duration"
    assert result.evidence == bio


@pytest.mark.parametrize("bio", ["Joined the police in 2005.", "Police officer since 2005.", "Radi u policiji od 2005.", "Started his police career in 2005."])
def test_joining_year_is_an_estimate_at_the_supplied_date(bio):
    result = extract_police_experience(bio, reference_date=date(2024, 1, 1))
    assert result.years == 19
    assert result.joining_year == 2005
    assert "estimate" in result.method


@pytest.mark.parametrize("bio", [
    "", "Experienced detective.", "Graduated in 2005.", "Five years in the narcotics unit.",
    "10 years of investigative experience.", "15 years in financial investigations.",
])
def test_generic_career_and_role_statements_are_not_total_police_service(bio):
    assert extract_police_experience(bio, reference_date=date(2024, 1, 1)).years is None


@pytest.mark.parametrize("bio", [
    "Over 15 years in the police.", "About 15 years of police service.",
    "10–15 years in the police.", "15 years in the police department.",
    "20 years in the police. Another bio says 15 years in the police.",
    "Joined the police in 2030.", "Joined the police in 1900.",
    "Joined the police in 2005. Retired in 2020.",
    "15 years in the police cybercrime unit.",
    "15 years in police investigations.",
    "A narcotics investigator for 15 years in the police.",
    "15.5 years of police service.",
    "Not 15 years of police service.",
])
def test_uncertain_or_conflicting_evidence_requires_review(bio):
    result = extract_police_experience(bio, reference_date=date(2024, 1, 1))
    assert result.years is None
    assert result.method == "Needs review"
    assert result.evidence


def test_evidence_retains_the_matched_statement_in_a_long_sentence():
    bio = "Extensive professional biography " * 25 + "with 20 years of police service."
    result = extract_police_experience(bio, reference_date=date(2024, 1, 1))
    assert result.years == 20
    assert "20 years of police service" in result.evidence
