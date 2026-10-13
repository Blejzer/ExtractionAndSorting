"""Service-sector starts from the programme owner's supplied biographies."""

import csv
from datetime import date, datetime
from io import StringIO

import pytest

from services.word_extraction_service import extract_docx
from services.word_export_service import EXPORT_COLUMNS, export_csv
from services.word_import_service import read_export
from services.word_matching_service import WordMatchContext
from tests.test_word_extraction import docx, table
from utils.biography_employment import employment_from_bio
from utils.professional_experience import extract_professional_experience, extract_service_start


SASHKO_BIO = """Education: Graduated from the Faculty of Security in Skopje
Professional experience: Employed with the Ministry of Interior since 1995
1995-2000 –Police officer
2000-2008 Junior Inspector - Anti-Drugs Unit/ Sector for Internal Affairs in Skopje
2009-2009 Inspector – Anti-Drugs Unit/ Sector for Internal Affairs in Skopje
2009-2011 Inspector- Violent and Serious Crime Unit/ Organized Crime Department
2011-2012 Inspector –Homicide and sexual offences Unit
2012-2014 Inspector-Anti-Drugs Unit, Center for Suppression of Organized and
Serious Crime
2014-2015 Inspector - Anti-Drugs Unit at regional and national level, Center for Suppression of
Organized and Serious Crime
2015- currently Inspector- Anti-Drugs Unit, Sector for criminal investigations /Dept. for
Suppression of Organized and Serious rime"""

KASIM_BIO = """I graduated from University of Sarajevo, Faculty of Law in 1976, and in 1984 I passed the Bar exam.
From 1976 until 1992 I worked in different firms and companies in Bosnia and Hercegovina.
From 1992 to 2013 I worked in the Cantonal Prosecutors office of Tuzla Canton as a cantonal prosecutor.
In February of 2013 I was elected as a state prosecutor in The Prosecutors Office of Bosnia and Herzegovina, where I have been working as one of the prosecutors in the Department III until today."""


@pytest.mark.parametrize("bio,start,scope", [
    (SASHKO_BIO, 1995, "Police"),
    (KASIM_BIO, 1992, "Prosecution"),
])
@pytest.mark.parametrize("event_year", [2015, 2016, 2020])
def test_supplied_bios_use_sector_start_and_event_year(bio, start, scope, event_year):
    result = extract_professional_experience(bio, reference_date=date(event_year, 5, 1))
    assert result.joining_year == start
    assert result.years == event_year - start
    assert result.scope == scope
    assert "estimate" in result.method.lower()
    service_start = extract_service_start(bio, reference_date=date(event_year, 5, 1))
    assert service_start.joining_year == start
    if scope == "Prosecution":
        assert "different firms and companies" not in result.evidence
        assert "passed the Bar exam" not in result.evidence


def test_current_timeline_and_continuing_appointment_fill_employment():
    fields, ambiguous = employment_from_bio(SASHKO_BIO)
    assert not ambiguous
    assert fields["position"]["value"] == "Inspector"
    assert fields["unit"]["value"] == "Anti-Drugs Unit"
    assert fields["organization"]["value"] == "Ministry of Interior"
    fields, ambiguous = employment_from_bio(KASIM_BIO)
    assert not ambiguous
    assert fields["position"]["value"] == "state prosecutor"
    assert fields["unit"]["value"] == "Department III"
    assert fields["organization"]["value"] == "Prosecutors Office of Bosnia and Herzegovina"


@pytest.mark.parametrize("bio,start,years", [(SASHKO_BIO, "1995", 20), (KASIM_BIO, "1992", 23)])
def test_word_preview_uses_actual_event_start_year_before_filename_year(bio, start, years):
    item = extract_docx(docx(table([
        ["Name:", "Example PARTICIPANT"], ["Short biography:", bio],
    ])), "PFE16M1.docx")[0]
    context = WordMatchContext([], [], [
        {"eid": "PFE16M1", "start_date": datetime(2015, 5, 1)},
    ])
    context.check(item)
    assert item["fields"]["police_joined"] == start  # Compatible internal field key.
    experience = item["professional_experience"]
    assert experience["display_years"] == f"{years} (estimate; event year only)"
    assert experience["reference"] == "2015 (event start date)"
    rows = list(csv.reader(StringIO(export_csv([item]).decode("utf-8-sig"))))
    assert "Service Start" in rows[0]
    assert "Police service start" not in rows[0]
    values = dict(zip(EXPORT_COLUMNS, rows[1]))
    assert values["police_joined"] == start


def test_legacy_police_service_start_csv_column_is_still_importable():
    rows = read_export(b"Name,Country CID,Police service start\nExample PARTICIPANT,C194,1992\n", "legacy.csv")
    assert rows[0]["fields"]["police_joined"] == "1992"


@pytest.mark.parametrize("events", [
    [],
    [{"eid": "PFE16M1", "start_date": "invalid"}],
    [{"eid": "PFE16M1", "start_date": "2015-05-01"},
     {"eid": "PFE16M1", "start_date": "2016-05-01"}],
])
def test_missing_invalid_or_conflicting_event_dates_do_not_fabricate_years(events):
    item = extract_docx(docx(table([
        ["Name:", "Example PARTICIPANT"], ["Biography:", KASIM_BIO],
    ])), "PFE16M1.docx")[0]
    WordMatchContext([], [], events).check(item)
    assert item["fields"]["police_joined"] == "1992"
    assert item["professional_experience"]["display_years"] == "Unknown"
    assert item["professional_experience"]["method"] == "Event date needed for estimate"


def test_service_start_does_not_turn_education_or_company_work_into_prosecution():
    result = extract_professional_experience(
        "Graduated in 1976. From 1976 until 1992 I worked in different firms and companies.",
        reference_date=date(2016, 5, 1))
    assert result.years is None
    assert result.joining_year is None


def test_ongoing_ministry_employment_can_establish_start_without_a_timeline():
    result = extract_professional_experience(
        "Employed with the Ministry of Interior since 1995.",
        reference_date=date(2015, 5, 1))
    assert result.joining_year == 1995
    assert result.years == 20
    assert result.scope == "Police"


def test_statistics_use_the_same_service_sector_and_selected_event_date():
    from services.statistics_service import build_statistics

    report = build_statistics(
        [{"eid": "EVENT", "start_date": "2015-05-01", "participants": ["P1", "P2"]}],
        [{"pid": "P1", "bio_short": SASHKO_BIO}, {"pid": "P2", "bio_short": KASIM_BIO}],
        [], [], as_of=date(2026, 1, 1),
    )
    records = {row["pid"]: row for row in report["experience"]["records"]}
    assert records["P1"]["years"] == 20 and records["P1"]["scope"] == "Police"
    assert records["P2"]["years"] == 23 and records["P2"]["scope"] == "Prosecution"
    assert all(row["reference_date"] == "2015-05-01" for row in records.values())


def test_stated_bounds_remain_stated_even_when_start_is_available():
    bio = "Employed with the Ministry of Interior since 1995. Over 20 years in the police."
    result = extract_professional_experience(bio, reference_date=date(2026, 1, 1))
    assert result.years is None
    assert result.display_years == "20+"
    assert extract_service_start(bio, reference_date=date(2026, 1, 1)).joining_year == 1995


def test_transfer_within_prosecution_does_not_restart_service():
    bio = (
        "1992-2013 - Public prosecutor. From 2013 - State prosecutor. "
        "Employed with the Public Prosecutors Office since 2013."
    )
    result = extract_professional_experience(bio, reference_date=date(2015, 5, 1))
    assert result.joining_year == 1992
    assert result.years == 23
    assert result.scope == "Prosecution"


def test_plural_prosecutors_office_is_a_prosecution_employer():
    from utils.professional_profile import organization_group

    assert organization_group({"organization": "Prosecutors Office of Bosnia and Herzegovina"}) == "Prosecutor"
