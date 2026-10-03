from datetime import date

from services.statistics_service import build_statistics
import services.statistics_service as statistics_service


def participant(pid, country="AL", **fields):
    return dict(pid=pid, name=pid, representing_country=country, **fields)


def event(eid="E1", when="2024-05-01", **fields):
    return dict(eid=eid, start_date=when, title="Cybercrime and cryptocurrency on the dark web", **fields)


def calculate(events, participants=(), links=(), countries=(), **filters):
    return build_statistics(events, participants, links, countries, as_of=date(2026, 10, 3), **filters)


def country(report, code):
    return next(row for row in report["countries"] if row["code"] == code)


def cell(report, eid, code):
    row = next(row for row in report["events"] if row["eid"] == eid)
    return next(item for item in row["countries"] if item["code"] == code)


def test_no_shows_partial_attendance_and_extras_are_separate():
    people = [participant(f"A{i}") for i in range(5)] + [participant("B", "BA")]
    report = calculate([event(participants=[p["pid"] for p in people])], people, policy_change=date(2021, 1, 1))
    assert cell(report, "E1", "AL")["status"] == "Above allocation"
    assert cell(report, "E1", "BA")["status"] == "Attended below allocation"
    assert cell(report, "E1", "HR")["status"] == "No-show"
    assert country(report, "AL")["additional"] == 2
    assert country(report, "AL")["shortfall"] == 0
    assert country(report, "BA")["shortfall"] == 2
    assert country(report, "HR")["shortfall"] == 3
    assert country(report, "AL")["places_filled_percent"] == 100


def test_croatia_is_not_a_no_show_before_policy_change_and_cutoff_is_inclusive():
    report = calculate([event("old", "2020-12-31", participants=["P"]), event("new", "2021-01-01", participants=["P"])],
                       [participant("P")], policy_change=date(2021, 1, 1))
    assert cell(report, "old", "HR")["status"] == "Not invited"
    assert cell(report, "old", "AL")["expected"] == 4
    assert cell(report, "new", "HR")["status"] == "No-show"
    assert country(report, "HR")["invited_events"] == 1
    assert country(report, "AL")["expected_places"] == 7


def test_event_override_takes_precedence_and_noninvited_attendance_is_retained():
    report = calculate([event(invited_countries=["AL"], expected_per_country=2, participants=["P", "Q"])],
                       [participant("P"), participant("Q", "HR")], policy_change=date(2021, 1, 1))
    assert cell(report, "E1", "HR") == dict(code="HR", count=1, status="Not invited", expected=None)
    assert cell(report, "E1", "AL")["expected"] == 2
    assert country(report, "HR")["attendances"] == 1
    assert country(report, "HR")["invited_events"] == 0


def test_unknown_policy_never_infers_a_no_show_from_absence():
    report = calculate([event(participants=["P"])], [participant("P")])
    assert report["summary"]["unconfigured_events"] == 1
    assert cell(report, "E1", "HR")["status"] == "Policy not set"
    assert country(report, "HR")["no_show_events"] == 0
    assert country(report, "HR")["places_filled_percent"] is None


def test_missing_profile_does_not_create_false_country_shortfalls():
    report = calculate([event(participants=["P", "missing"])], [participant("P")], policy_change=date(2021, 1, 1))
    assert report["summary"]["missing_profiles"] == 1
    assert report["summary"]["unique_people"] == 2
    assert cell(report, "E1", "HR")["status"] == "Roster unresolved"
    assert country(report, "HR")["no_show_events"] == 0
    assert country(report, "AL")["shortfall"] == 0
    assert {row["label"]: row["count"] for row in report["diversity"]["gender"]} == {"Unknown": 2}


def test_union_of_partial_links_and_legacy_roster_is_deduplicated():
    report = calculate([event("E1", participants=["P", "P", "Q"]), event("E2", participants=["P"])],
                       [participant("P"), participant("Q"), participant("R")],
                       [dict(event_id="E1", participant_id="P"), dict(event_id="E1", participant_id="R"),
                        dict(event_id="E1", participant_id="R"), dict(event_id="orphan", participant_id="Q")])
    assert report["summary"]["attendances"] == 4
    assert report["summary"]["unique_people"] == 3
    assert country(report, "AL")["attendances"] == 4
    assert country(report, "AL")["unique_people"] == 3


def test_multiarea_counts_deduplicate_people_within_each_area():
    report = calculate([event("E1", participants=["P"]), event("E2", participants=["P", "Q"])],
                       [participant("P"), participant("Q")])
    for key in ("cybercrime", "crypto", "dark_web"):
        row = next(row for row in report["areas"] if row["key"] == key)
        assert (row["events"], row["attendances"], row["unique_people"]) == (2, 3, 2)
        assert row["countries"][0] == dict(code="AL", attendances=3, unique_people=2)
        assert all(cell["attendances"] == 0 for cell in row["countries"][1:])


def test_explicit_tags_and_explicit_unclassified_override_title_suggestions():
    report = calculate([event("E1", training_areas=["narcotics"]), event("E2", training_areas=[])])
    assert next(row for row in report["areas"] if row["key"] == "cybercrime")["events"] == 0
    assert next(row for row in report["areas"] if row["key"] == "narcotics")["events"] == 1
    assert next(row for row in report["areas"] if row["key"] == "unclassified")["events"] == 1
    assert all(row["area_source"] == "Reviewed" for row in report["events"])


def test_year_and_area_filters_exclude_future_and_undated_events():
    events = [event("2024", "2024-01-01", participants=["P"]), event("2025", "2025-01-01", participants=["Q"]),
              event("future", "2030-01-01"), event("undated", None)]
    report = calculate(events, [participant("P"), participant("Q")], year=2024, area="crypto")
    assert report["years"] == [2025, 2024]
    assert report["summary"]["events"] == 1
    assert report["summary"]["unique_people"] == 1
    assert report["experience"]["records"][0]["pid"] == "P"
    assert calculate(events)["summary"]["undated_events"] == 1


def test_country_aliases_and_stored_cids_are_resolved_without_citizenship_counting():
    report = calculate([event(participants=["P", "Q", "R"])],
                       [participant("P", "c010", citizenships=["AL", "HR"]),
                        participant("Q", "Bosna i Hercegovina"), participant("R", "MKD")],
                       countries=[dict(cid="C010", country="Albania")])
    assert country(report, "AL")["attendances"] == 1
    assert country(report, "BA")["attendances"] == 1
    assert country(report, "MK")["attendances"] == 1
    assert country(report, "HR")["attendances"] == 0


def test_diversity_is_person_weighted_and_age_uses_latest_selected_event():
    people = [participant("P", gender="Female", dob="1995-06-01", bio_short="Joined the police in 2010."),
              participant("Q", gender="Male", dob="1980-01-01", bio_short="15 years of service in the police.")]
    report = calculate([event("E1", "2024-01-01", participants=["P", "Q"]), event("E2", "2025-06-01", participants=["P"])], people)
    assert {row["label"]: row["count"] for row in report["diversity"]["gender"]} == {"Female": 1, "Male": 1}
    assert {row["label"] for row in report["diversity"]["age"]} == {"30–39", "40–49"}
    assert report["experience"]["known"] == 2
    assert report["experience"]["median_years"] == 15
    p = next(row for row in report["experience"]["records"] if row["pid"] == "P")
    assert p["reference_date"] == "2025-06-01"
    assert p["years"] == 15
    filtered = calculate([event("E1", "2024-01-01", participants=["P", "Q"]), event("E2", "2025-06-01", participants=["P"])], people, year=2024)
    assert next(row for row in filtered["experience"]["records"] if row["pid"] == "P")["years"] == 14


def test_undated_attendance_does_not_estimate_years_since_joining():
    report = calculate([event(when=None, participants=["P"])], [participant("P", bio_short="Joined the police in 2000.")])
    assert report["experience"]["known"] == 0
    assert report["experience"]["records"][0]["method"] == "Needs review"


def test_empty_database_has_no_fake_zeros_for_experience_or_allocation():
    report = calculate([])
    assert report["summary"]["unique_people"] == 0
    assert report["experience"]["median_years"] is None
    assert all(row["places_filled_percent"] is None for row in report["countries"])


def test_event_without_uploaded_attendance_is_not_a_whole_region_no_show():
    report = calculate([event()], policy_change=date(2021, 1, 1))
    assert report["summary"]["empty_rosters"] == 1
    assert cell(report, "E1", "AL")["status"] == "No attendance uploaded"
    assert all(row["no_show_events"] == 0 and row["shortfall"] == 0 for row in report["countries"])


def test_database_adapter_reads_only_reporting_fields_without_writes(monkeypatch):
    import sys
    documents = {
        "events": [event(participants=["P"])],
        "participants": [participant("P")],
        "participant_events": [], "countries": [],
    }
    calls = []
    class Collection:
        def __init__(self, name):
            self.name = name
        def find(self, query, projection):
            calls.append((self.name, query, projection))
            return documents[self.name]
    class Mongo:
        def collection(self, name):
            return Collection(name)
    monkeypatch.setattr(sys.modules["config.database"], "mongodb", Mongo())
    report = statistics_service.fetch_statistics(as_of=date(2026, 10, 3))
    assert report["summary"]["attendances"] == 1
    assert len(calls) == 4
    assert all(query == {} for _, query, _ in calls)
    private_fields = {"email", "phone", "travel_doc_number", "iban", "swift", "bank_name"}
    assert all(not (private_fields & set(projection)) for _, _, projection in calls)
