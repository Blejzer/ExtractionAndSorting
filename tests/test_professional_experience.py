from datetime import date

import pytest

from utils.professional_experience import extract_professional_experience
from utils.professional_profile import infer_professional_profile, resolve_professional_country


DANICA = dict(
    pid="P0104", name="Danica ARAPOVIĆ KOVAČEVIĆ",
    position="Tuzla Canton Cantonal Prosecutor's Office / Organized Crime Department Head",
    bio_short=("I have worked as a lawyer as well as in Ministry of internal affairs. "
               "I have been Cantonal prosecutor for over 20 years. "
               "In my proffesional career I have worked on many complex and hard cases that included "
               "organized criminal groups, drug distribution as well as human trafficking. "
               "I have been Head of the department for violent and sexual offences, but since first "
               "of April 2025 I have been named Head of Organized Crimes department."),
)

MIROSLAV = dict(
    pid="P0304", name="Miroslav FILIPOVIĆ", representing_country="C194",
    position="Sremska Mitrovica Higher Public Prosecutor's Office Public Prosecutor",
    organization="Public Prosecutor's Office", rank="Chief Public Prosecutor",
    bio_short=("- from 2022. - Chief Public Prosecutor at the Higher Public Prosecutor's Office in Sremska Mitrovica "
               "- 2012. - 2021. - Deputy Public Prosecutor at the Higher Public Prosecutor's Office in Belgrade "
               "- 1999. - 2011. - Deputy Public Prosecutor at the Basic Public Prosecutor's Office in Belgrade "
               "-1996. -1998. - Expert Assistant at the Basic Public Prosecution Office in Belgrade "
               "- Graduated from the Law School of the University of Belgrade in 1996; passed the Bar Exam in 1998."),
)


def extract(bio, role="Unknown"):
    return extract_professional_experience(bio, reference_date=date(2026, 5, 4), role=role)


def test_supplied_prosecutor_biography_preserves_duration_and_leadership_evidence():
    professional = infer_professional_profile(DANICA)
    assert professional.role == "Prosecutor"
    assert professional.seniority == "Department / unit head"
    assert professional.role_method == "Biography assertion"
    assert professional.seniority_evidence == DANICA["position"]
    result = extract(DANICA["bio_short"], professional.role)
    assert result.display_years == "20+"
    assert result.years is None
    assert result.min_years == 20
    assert result.qualifier == "more_than"
    assert result.scope == "Prosecution"
    assert result.method == "Stated lower bound"
    assert result.evidence == "I have been Cantonal prosecutor for over 20 years."
    assert resolve_professional_country(DANICA, {})["code"] == "BA"


@pytest.mark.parametrize("bio, display, method, scope", [
    ("I have been a judge for 18 years.", "18", "Stated duration", "Judiciary"),
    ("She has 25 years of experience as a lawyer.", "25", "Stated duration", "Legal practice"),
    ("I have been a public prosecutor for at least 20 years.", "20+", "Stated lower bound", "Prosecution"),
    ("I have been a prosecutor for 20+ years.", "20+", "Stated lower bound", "Prosecution"),
    ("Over 15 years in the police.", "15+", "Stated lower bound", "Police"),
    ("About 15 years of police service.", "about 15", "Approximate duration", "Police"),
    ("I have been a prosecutor for 10–15 years.", "10–15", "Stated range", "Prosecution"),
    ("Joined the police in 2005.", "21", "Years since joining (estimate)", "Police"),
    ("Ima 18 godina rada u policiji.", "18", "Stated duration", "Police"),
])
def test_professional_experience_keeps_exact_values_bounds_and_ranges_distinct(bio, display, method, scope):
    result = extract(bio)
    assert (result.display_years, result.method, result.scope) == (display, method, scope)


@pytest.mark.parametrize("bio", [
    "I have not been a prosecutor for 20 years.",
    "I haven't been a prosecutor for 20 years.",
    "I have been a prosecutor for 20 years. Another source says prosecutor for 15 years.",
    "15.5 years of police service.",
    "15 years in the police cybercrime unit.",
    "A narcotics investigator for 15 years in the police.",
    "I have been a prosecutor for 90 years.",
    "Joined the police in 2030.",
])
def test_unsupported_or_conflicting_evidence_does_not_become_exact_experience(bio):
    result = extract(bio)
    assert result.years is None
    assert result.display_years == "Unknown"
    assert result.method == "Needs review"


def test_different_career_durations_are_not_added_or_conflated():
    bio = "I worked as a lawyer for 5 years. I have been a prosecutor for over 20 years."
    assert extract(bio).method == "Needs review"
    selected = extract(bio, "Prosecutor")
    assert selected.scope == "Prosecution"
    assert selected.display_years == "20+"
    assert "5 years" not in selected.evidence


@pytest.mark.parametrize("profile, role, level", [
    (dict(position="Senior public prosecutor"), "Prosecutor", "Senior role"),
    (dict(position="Chief prosecutor"), "Prosecutor", "Institution leadership"),
    (dict(position="Judge"), "Judge", "Unknown"),
    (dict(position="Secretary", organization="Prosecutor's Office"), "Unknown", "Unknown"),
    (dict(position="Prosecutor's Office secretary"), "Unknown", "Unknown"),
    (dict(position="Chief Public Prosecutor's Office secretary"), "Unknown", "Unknown"),
    (dict(position="Assistant to the Chief Public Prosecutor"), "Unknown", "Unknown"),
    (dict(bio_short="I was Head of the department."), "Unknown", "Unknown"),
    (dict(position="Former chief prosecutor"), "Unknown", "Unknown"),
    (dict(bio_short="I am not a prosecutor."), "Unknown", "Unknown"),
    (dict(name="Miroslav FILIPOVIĆ"), "Unknown", "Unknown"),
])
def test_roles_use_personal_title_evidence_and_do_not_invent_formal_rank(profile, role, level):
    result = infer_professional_profile(profile)
    assert (result.role, result.seniority) == (role, level)


def test_stored_country_has_priority_over_inferred_institutional_country():
    assert resolve_professional_country({**DANICA, "representing_country": "AL"}, {})["code"] == "AL"
    country = resolve_professional_country({**DANICA, "representing_country": "Cmissing"}, {})
    assert country["code"] == "BA"
    assert country["method"] == "Inferred from institution"
    assert country["evidence"] == DANICA["position"]
    assert resolve_professional_country(dict(name=DANICA["name"], bio_short="I visited Tuzla."), {})["code"] is None


def test_wrapped_table_text_and_nonbreaking_spaces_do_not_hide_experience():
    bio = "I have been Cantonal\nprosecutor for over\n20\u00a0years."
    professional = infer_professional_profile(dict(bio_short=bio))
    assert professional.role == "Prosecutor"
    result = extract(bio, professional.role)
    assert result.display_years == "20+"
    assert result.evidence == bio


def test_country_conflicts_are_not_resolved_by_picking_the_first_country():
    assert resolve_professional_country(dict(position="Regional liaison for Albania and Serbia"), {})["code"] is None


def test_full_report_populates_supplied_example_without_fabricating_exact_statistics():
    from services.statistics_service import build_statistics
    profile = {**DANICA, "representing_country": "unresolved", "rank": ""}
    report = build_statistics(
        [dict(eid="E1", start_date="2026-05-04", participants=["P0104"], title="Organized crime")],
        [profile], [], [], as_of=date(2026, 10, 5))
    record = report["experience"]["records"][0]
    assert record["country"] == "BiH"
    assert record["country_method"] == "Inferred from institution"
    assert (record["role"], record["seniority"]) == ("Prosecutor", "Department / unit head")
    assert record["display_years"] == "20+"
    assert record["reference_date"] == "2026-05-04"
    assert record["evidence"] == "I have been Cantonal prosecutor for over 20 years."
    assert report["experience"]["known"] == 1
    assert report["experience"]["lower_bounds"] == 1
    assert report["experience"]["median_years"] is None
    assert report["experience"]["bands"] == []
    assert report["diversity"]["professional_role"][0]["label"] == "Prosecutor"
    assert report["diversity"]["seniority"][0]["label"] == "Department / unit head"
    # An inferred jurisdiction is not confirmed invited-country representation.
    assert report["summary"]["unresolved_attendances"] == 1
    assert all(row["no_show_events"] == 0 for row in report["countries"])
    assert profile["rank"] == ""


def test_exact_and_bounded_durations_have_separate_statistical_denominators():
    from services.statistics_service import build_statistics
    report = build_statistics(
        [dict(eid="E1", start_date="2026-05-04", participants=["P", "Q", "R"])],
        [dict(pid="P", bio_short="15 years of police service."),
         dict(pid="Q", bio_short="Prosecutor for over 20 years."),
         dict(pid="R", bio_short="Judge for about 18 years.")], [], [])
    assert report["experience"]["known"] == 3
    assert report["experience"]["median_years"] == 15
    assert report["experience"]["point_values"] == 1
    assert report["experience"]["lower_bounds"] == 1
    assert report["experience"]["approximate"] == 1
    assert report["experience"]["bands"][0]["percent"] == 100


def test_attendance_country_fills_missing_profile_country_before_institution_inference():
    from services.statistics_service import build_statistics
    report = build_statistics(
        [dict(eid="E1", start_date="2026-05-04")], [DANICA],
        [dict(event_id="E1", participant_id="P0104", representing_country="BA")], [])
    record = report["experience"]["records"][0]
    assert record["country"] == "BiH"
    assert record["country_method"] == "Attendance country"
    assert report["experience"]["inferred_countries"] == 0


def test_actual_miroslav_record_separates_prosecution_office_career_from_prosecutor_appointment():
    from services.statistics_service import build_statistics
    report = build_statistics([dict(eid="PFE26M3", start_date="2026-05-04", participants=["P0304"])],
                              [MIROSLAV], [], [], as_of=date(2026, 10, 5))
    record = report["experience"]["records"][0]
    assert record["role"] == "Prosecutor"
    assert record["seniority"] == "Institution leadership"
    assert record["seniority_evidence"] == "Chief Public Prosecutor"
    assert record["country"] == "Serbia"
    assert record["country_method"] == "Inferred from institution"
    assert record["years"] == 30
    assert record["display_years"] == "30 (estimate)"
    assert record["method"] == "Career timeline (estimate)"
    assert record["scope"] == "Prosecution (including support roles)"
    assert record["career_start_year"] == 1996
    assert record["role_start_year"] == 1999
    assert record["role_years"] == 27
    assert "Expert Assistant" in record["evidence"]
    assert "Graduated" not in record["evidence"]


def test_supplied_danica_rank_typo_retains_other_evidence():
    profile = {**DANICA, "representing_country": "C027",
               "organization": "Cantonal Prosecutor's Office of Tuzla Canton",
               "rank": "Cantonal Prossecutor, Head of Organized Crimes department"}
    professional = infer_professional_profile(profile)
    assert professional.role == "Prosecutor"
    assert professional.role_method == "Stored rank"
    assert professional.seniority == "Department / unit head"
    assert extract(profile["bio_short"], professional.role).display_years == "20+"


def test_historical_selection_clips_employment_timeline_to_event_year():
    result = extract_professional_experience(MIROSLAV["bio_short"], reference_date=date(2018, 5, 4), role="Prosecutor")
    assert result.years == 22
    assert result.role_years == 19


@pytest.mark.parametrize("bio", [
    "Graduated from law school in 1996. Passed the bar exam in 1998.",
    "1996. - Graduated in law. 1998. - Passed the Bar Exam.",
])
def test_education_dates_are_not_employment_experience(bio):
    result = extract(bio, "Prosecutor")
    assert result.years is None
    assert result.method == "Not found"


@pytest.mark.parametrize("bio", [
    "1996. - 1998. - Prosecutor. From 2022. - Chief Public Prosecutor.",
    "1996. - 1990. - Prosecutor. From 2022. - Chief Public Prosecutor.",
    "1996. - Prosecutor. From 2022. - Chief Public Prosecutor.",
    "From 1996. - Prosecutor. From 2022. - Chief Public Prosecutor.",
    "1996. - 1998. - Police officer. From 1999. - Chief Public Prosecutor.",
])
def test_gapped_conflicting_and_undated_endpoints_do_not_become_continuous_service(bio):
    result = extract(bio, "Prosecutor")
    assert result.years is None
    assert result.method == "Needs review"


def test_undated_event_cannot_supply_timeline_reference_year():
    from services.statistics_service import build_statistics
    report = build_statistics([dict(eid="E1", participants=["P0304"])], [MIROSLAV], [], [])
    record = report["experience"]["records"][0]
    assert record["years"] is None
    assert record["role_years"] is None
    assert record["display_years"] == "Unknown"
    assert record["method"] == "Needs review"


def test_supplied_country_catalog_resolves_both_attendees_and_country_shortfalls():
    from services.statistics_service import build_statistics
    countries = [dict(cid="C027", country="Bosnia and Herzegovina, Europe & Eurasia"),
                 dict(cid="C194", country="Serbia, Europe & Eurasia")]
    report = build_statistics(
        [dict(eid="PFE26M3", start_date="2026-05-04", participants=["P0104", "P0304"])],
        [{**DANICA, "representing_country": "C027"}, MIROSLAV], [], countries,
        as_of=date(2026, 10, 5))
    records = {record["pid"]: record for record in report["experience"]["records"]}
    assert records["P0104"]["country"] == "BiH"
    assert records["P0304"]["country"] == "Serbia"
    assert all(record["country_method"] == "Profile country" for record in records.values())
    assert report["experience"]["inferred_countries"] == 0
    assert report["summary"]["unresolved_attendances"] == 0
    for code in ("BA", "RS"):
        country = next(row for row in report["countries"] if row["code"] == code)
        assert country["attendances"] == 1
        assert country["partial_events"] == 1
        assert country["shortfall"] == 2
    assert next(row for row in report["countries"] if row["code"] == "AL")["no_show_events"] == 1
    assert records["P0104"]["display_years"] == "20+"
    assert records["P0304"]["role_years"] == 27
