"""Synthetic documents only: no uploaded participant information in fixtures."""

import csv
from datetime import datetime
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZipFile

from flask import Flask
from pymongo.errors import ServerSelectionTimeoutError
import pytest

from routes import word_extraction as routes
from services import word_draft_store as drafts
from services.word_export_service import EXPORT_COLUMNS, export_csv
from services.word_extraction_service import FIELDS, WordExtractionError, extract_docx, extract_files, parse_date
from services.word_matching_service import WordMatchContext, load_match_context


def paragraph(text):
    runs = "<w:tab/>".join(f"<w:t>{escape(part)}</w:t>" for part in text.split("\t"))
    return f"<w:p><w:r>{runs}</w:r></w:p>"


def table(rows):
    return "<w:tbl>" + "".join("<w:tr>" + "".join(
        "<w:tc>" + "".join(paragraph(line) for line in cell.split("\n")) + "</w:tc>"
        for cell in row) + "</w:tr>" for row in rows) + "</w:tbl>"


def docx(*blocks):
    xml = '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>' + "".join(blocks) + '</w:body></w:document>'
    stream = BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("word/document.xml", xml)
    return stream.getvalue()


def form(name="Ana TEST", country="Albania", dob="25.03.1980."):
    return docx(table([
        ["COUNTRY:", country], ["NAME & LAST NAME:", name, "DOB:", dob, "POB:", "Tirana"],
        ["PASSPORT NUMBER:", "001234", "GENDER:", "F"],
        ["INSTITUTION:", "Test institution", "POSITION:", "Specialist"],
        ["SHORT PROFESSIONAL BIOGRAPHY:", "First paragraph.\nSecond paragraph."],
    ]))


COUNTRIES = [
    {"cid": "AL_TEST", "country": "Albania, Europe & Eurasia", "iso": "AL"},
    {"cid": "BA_TEST", "country": "Bosnia and Herzegovina, Europe & Eurasia", "iso": "BA"},
    {"cid": "MK_TEST", "country": "North Macedonia, Europe & Eurasia", "iso": "MK"},
    {"cid": "RS_TEST", "country": "Serbia, Europe & Eurasia", "iso": "RS"},
    {"cid": "XK_TEST", "country": "Kosovo, Europe & Eurasia", "iso": "XK"},
]
PARTICIPANTS = [{"pid": "PTEST1", "name": "Ana TEST", "representing_country": "AL_TEST",
                 "dob": datetime(1980, 3, 25), "email": "ana@example.test"}]


def record(name="Ana TEST", country="Albania", dob="1980-03-25"):
    return {"fields": {field: "" for field in FIELDS} | {"name": name, "country_label": country, "dob": dob},
            "source_file": "test.docx", "sources": ["test.docx: Table 1"], "evidence": {},
            "warnings": [], "matches": [], "match_status": "Not checked"}


def test_registration_table_preserves_fields_and_multiline_bio():
    result = extract_docx(form(), "PFE14M1.docx")
    assert len(result) == 1
    fields = result[0]["fields"]
    assert fields["name"] == "Ana TEST"
    assert fields["dob"] == "1980-03-25"
    assert fields["country_label"] == "Albania"
    assert fields["representing_country"] == ""
    assert fields["gender"] == "Female"
    assert fields["bio_short"] == "First paragraph.\nSecond paragraph."
    assert fields["event_reference"] == "PFE14M1"
    assert fields["travel_doc_number"] == "001234"
    assert fields["travel_doc_type"] == "Passport"


def test_multiple_forms_and_mixed_date_formats_require_review():
    data = docx(table([["Name:", "Ana TEST", "DOB:", "07/30/1980"]]),
                table([["Name:", "Bora TEST", "DOB:", "30/07/1981"]]),
                table([["Name:", "Cora TEST", "DOB:", "2/10/1982"]]))
    result = extract_docx(data, "forms.docx")
    assert [r["fields"]["dob"] for r in result] == ["1980-07-30", "1981-07-30", ""]
    assert result[2]["fields"]["dob_raw"] == "2/10/1982"
    assert any("ambiguous" in warning for warning in result[2]["warnings"])
    assert extract_docx(data, "forms.docx", "dmy")[2]["fields"]["dob"] == "1982-10-02"


def test_repeated_name_labels_in_one_table_start_separate_entries():
    data = docx(table([["Name:", "Ana TEST", "DOB:", "25.03.1980"],
                       ["Name:", "Bora TEST", "DOB:", "26.03.1981"]]))
    result = extract_docx(data, "forms.docx")
    assert [(r["fields"]["name"], r["fields"]["dob"]) for r in result] == [
        ("Ana TEST", "1980-03-25"), ("Bora TEST", "1981-03-26")]


def test_tabbed_lists_keep_column_boundaries_and_date_evidence():
    data = docx(paragraph("Albania"), paragraph("Ana TEST\t07/30/1980\tMajor\tDirector"),
                paragraph("Bora TEST\t2/10/1982\tOfficer\tSpecialist"))
    result = extract_docx(data, "list.docx")
    assert len(result) == 2
    assert result[1]["fields"]["dob"] == "1982-02-10"
    assert result[0]["fields"]["rank"] == "Major"
    assert result[0]["fields"]["position"] == "Director"


def test_numbered_biographies_and_country_sections():
    data = docx(paragraph("Montenegro"), paragraph("1. Ana TEST, rođena 25.03.1980. u Podgorici."),
                paragraph("An additional biography paragraph."), paragraph("Makedonija:"),
                paragraph("Let Skopje-Zagreb, 21.4."), paragraph("1. Bora TEST (26.03.1981.)"),
                paragraph("Srbija:"), paragraph("Automobilima iz Beograda"), paragraph("1. Cora TEST"))
    result = extract_docx(data, "PFE13M3.docx")
    assert len(result) == 3
    assert result[0]["fields"]["bio_short"].endswith("An additional biography paragraph.")
    assert result[1]["fields"]["country_label"] == "Makedonija"
    assert result[1]["fields"]["travel_notes"].startswith("Let Skopje")
    assert result[2]["fields"]["country_label"] == "Srbija"
    assert result[2]["fields"]["dob"] == ""
    assert result[2]["fields"]["travel_notes"].startswith("Automobilima")


def test_prose_and_supplemental_roster_combine_only_known_identity():
    data = docx(paragraph("Capt. ANA TEST - PK#1234"), paragraph("DOB: 25.03.1980"),
                paragraph("Position: Specialist"), paragraph("Passport No: 001234"),
                table([["CASE ID", "NAME", "DOB", "UNIT NAME"], ["V001", "TEST, Ana", "03/25/1980", "Investigations"]]))
    result = extract_docx(data, "Kosovo.docx")
    assert len(result) == 1
    fields = result[0]["fields"]
    assert fields["name"] == "ANA TEST"
    assert fields["unit"] == "Investigations"
    assert fields["case_id"] == "V001"
    assert fields["service_number"] == "1234"
    assert fields["travel_doc_number"] == "001234"
    assert fields["country_label"] == ""  # No inference from filename or institution.
    assert len(result[0]["evidence"]["name"]) == 2


def test_same_name_and_dob_with_different_countries_stay_separate():
    data = docx(table([["Country:", "Albania"], ["Name:", "Ana TEST", "DOB:", "25.03.1980"]]),
                table([["Country:", "Serbia"], ["Name:", "Ana TEST", "DOB:", "25.03.1980"]]))
    assert len(extract_docx(data, "forms.docx")) == 2


def test_conflicting_source_values_preserved_for_review():
    data = docx(paragraph("Name: Ana TEST"), paragraph("DOB: 25.03.1980"),
                paragraph("Position: Specialist"), paragraph("Position: Director"))
    result = extract_docx(data, "form.docx")[0]
    assert result["fields"]["position"] == "Specialist"
    assert [item["value"] for item in result["evidence"]["position"]] == ["Specialist", "Director"]
    assert any("Different position" in item for item in result["warnings"])


def test_country_before_name_in_paragraph_form():
    data = docx(paragraph("Country: Republic of Serbia"), paragraph("Name:"), paragraph("Ana TEST"), paragraph("DOB: 25.03.1980"))
    result = extract_docx(data, "form.docx")[0]
    assert result["fields"]["country_label"] == "Republic of Serbia"
    assert result["fields"]["name"] == "Ana TEST"


def test_partial_batch_errors_and_cross_file_duplicates():
    result = extract_files([("one.docx", form()), ("two.docx", form()), ("old.doc", b"old"), ("bad.docx", b"bad")])
    assert len(result["records"]) == 2
    assert len(result["file_errors"]) == 2
    assert all(any("another uploaded file" in warning for warning in item["warnings"]) for item in result["records"])


def test_entry_limits_stop_parsing_before_allocating_a_large_batch(monkeypatch):
    monkeypatch.setattr("services.word_extraction_service.MAX_RECORDS", 2)
    with pytest.raises(WordExtractionError, match="document contains more than 2"):
        extract_docx(docx(paragraph("Name: Ana TEST"), paragraph("Name: Bora TEST"), paragraph("Name: Cora TEST")), "large.docx")
    with pytest.raises(WordExtractionError, match="batch contains more than 2"):
        extract_files([("one.docx", form()), ("two.docx", form()), ("three.docx", form())])


@pytest.mark.parametrize("value,expected", [
    ("2/10/1982", ""), ("30/07/1980", "1980-07-30"), ("07/30/1980", "1980-07-30"),
    ("25. 03. 1980.", "1980-03-25"), ("March 25, 1980", "1980-03-25"),
    ("1980-03-25T00:00:00Z", "1980-03-25"), (datetime(1980, 3, 25), "1980-03-25"),
    ("1967/2/10", "1967-02-10"), ("1967-2-10", "1967-02-10"),
    ("1971/11/26", "1971-11-26"), ("1971-11-26", "1971-11-26"), ("11/26/1971", "1971-11-26"),
    ("1971/26/11", ""), ("1967-2-29", ""), ("1968/2/29", "1968-02-29"),
    ("NOV/261/1971", ""), ("31.02.1980", ""), ("03/25/80", ""), ("March", ""), ("March 1980", ""),
])
def test_dates_never_guess_ambiguous_or_invalid_values(value, expected):
    assert parse_date(value) == expected


@pytest.mark.parametrize("content", [b"not a zip", docx(paragraph("No names here")),
                                     docx(paragraph("&ignored;"))])
def test_unsupported_documents_return_extraction_errors(content):
    with pytest.raises(WordExtractionError):
        extract_docx(content, "invalid.docx")


def test_xml_entities_and_oversized_documents_rejected(monkeypatch):
    stream = BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("word/document.xml", '<!DOCTYPE x [<!ENTITY a "expanded">]><x>&a;</x>')
    with pytest.raises(WordExtractionError, match="entity"):
        extract_docx(stream.getvalue(), "entities.docx")
    monkeypatch.setattr("services.word_extraction_service.MAX_XML_BYTES", 10)
    with pytest.raises(WordExtractionError, match="limit"):
        extract_docx(form(), "large.docx")


def test_authoritative_country_cid_and_identity_match():
    context = WordMatchContext(PARTICIPANTS, COUNTRIES)
    item = record(name="TÉST, Ana")
    context.check(item)
    assert item["fields"]["representing_country"] == "AL_TEST"
    assert item["match_status"] == "Exact match"
    assert item["matches"][0]["pid"] == "PTEST1"
    assert context.resolve_country("Bosnia and Herzegovina – Republic of Srpska Entity") == "BA_TEST"
    assert context.resolve_country("Makedonija") == "MK_TEST"


@pytest.mark.parametrize("changes", [{"dob": ""}, {"dob": "1981-03-25"}, {"country_label": ""}, {"country_label": "Serbia"}, {"name": "Anna TEST"}])
def test_missing_conflicting_or_similar_identity_is_only_possible(changes):
    item = record()
    item["fields"].update(changes)
    WordMatchContext(PARTICIPANTS, COUNTRIES).check(item)
    assert item["match_status"] == "Possible match"
    assert not item["matches"][0]["exact"]


def test_duplicates_and_unknown_country_remain_reviewable():
    context = WordMatchContext(PARTICIPANTS + [PARTICIPANTS[0] | {"pid": "PTEST2"}], COUNTRIES)
    item = record()
    context.check(item)
    assert item["match_status"] == "Multiple exact matches"
    item["fields"]["representing_country"] = "INVALID_CID"
    context.check(item)
    assert item["match_status"] == "Review required"
    assert item["matches"] == []
    item = record(name="Unrelated Person", country="")
    context.check(item)
    assert item["match_status"] == "Review required"
    item["fields"]["country_label"] = "Serbia"
    context.check(item)
    assert item["match_status"] == "No candidate found"


def test_match_context_uses_only_read_queries_and_raw_legacy_documents(monkeypatch):
    import config.database as database
    calls = []
    class ReadOnlyCollection:
        def __init__(self, name):
            self.name = name
        def find(self, query, projection):
            calls.append((self.name, query, projection))
            return PARTICIPANTS if self.name == "participants" else COUNTRIES
    class ReadOnlyDatabase:
        def collection(self, name):
            return ReadOnlyCollection(name)
    monkeypatch.setattr(database, "mongodb", ReadOnlyDatabase())
    context = load_match_context()
    item = record()
    context.check(item)
    assert item["match_status"] == "Exact match"
    assert [call[0] for call in calls] == ["participants", "countries"]
    assert all(call[1] == {} and call[2]["_id"] == 0 for call in calls)


def test_csv_preserves_columns_zeros_and_neutralizes_formulas():
    item = record()
    item["fields"].update(name="=1+1", phone="+1234", travel_doc_number="001234", bio_short="First\nSecond")
    csv_rows = list(csv.reader(StringIO(export_csv([item]).decode("utf-8-sig"))))
    assert csv_rows[0] == list(EXPORT_COLUMNS.values())
    csv_row = dict(zip(EXPORT_COLUMNS, csv_rows[1]))
    assert csv_row["name"] == "'=1+1"
    assert csv_row["phone"] == "'+1234"
    assert csv_row["travel_doc_number"] == "001234"
    assert csv_row["bio_short"] == "First\nSecond"


def test_csv_preserves_long_source_text():
    item = record()
    item["fields"]["bio_short"] = "x" * 32768
    assert item["fields"]["bio_short"] in export_csv([item]).decode("utf-8-sig")


@pytest.fixture
def app(tmp_path, monkeypatch):
    application = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"))
    application.config.update(TESTING=True, SECRET_KEY="word-test", WORD_EXTRACTION_DIR=str(tmp_path / "drafts"))
    application.add_url_rule("/", endpoint="main.show_home", view_func=lambda: "home")
    for endpoint in ("participants.show_participants", "events.show_events", "imports.upload_form", "auth.login", "auth.logout"):
        application.add_url_rule("/" + endpoint, endpoint=endpoint, view_func=lambda: "stub")
    application.add_url_rule("/participants/<pid>", endpoint="participants.participant_detail", view_func=lambda pid: pid)
    application.register_blueprint(routes.word_extraction_bp)
    monkeypatch.setattr(routes, "load_match_context", lambda: WordMatchContext(PARTICIPANTS, COUNTRIES))
    return application


def login(client, username="tester"):
    with client.session_transaction() as session:
        session["username"] = username
    client.get("/imports/word")
    with client.session_transaction() as session:
        return session["word_extraction_csrf"]


def upload(client, token, content=None, filename="form.docx"):
    return client.post("/imports/word", data={"csrf_token": token, "files": (BytesIO(content or form()), filename)}, content_type="multipart/form-data")


def test_authenticated_upload_edit_export_and_clear(app):
    client = app.test_client()
    assert client.get("/imports/word").status_code == 302
    token = login(client)
    assert upload(client, "wrong-token").status_code == 400
    response = upload(client, token)
    assert response.status_code == 302
    location = response.headers["Location"]
    page = client.get(location)
    assert b"Exact match" in page.data and b"PTEST1" in page.data
    assert page.headers["Cache-Control"] == "no-store"
    assert b"missing_country" not in page.data and b"Download Excel" not in page.data
    assert client.post(location + "/export.xlsx", data={"csrf_token": token}).status_code == 404
    exported = client.post(location + "/export.csv", data={"csrf_token": token, "r0_position": "Updated specialist"})
    assert exported.status_code == 200
    rows = list(csv.reader(StringIO(exported.data.decode("utf-8-sig"))))
    values = dict(zip(EXPORT_COLUMNS, rows[1]))
    assert values["position"] == "Updated specialist"
    assert values["matching_pids"] == "PTEST1"
    assert len(list(Path(app.config["WORD_EXTRACTION_DIR"]).glob("*.json"))) == 1
    assert client.post(location + "/clear", data={"csrf_token": token}).status_code == 302
    assert client.get(location).status_code == 410
    assert not list(Path(app.config["WORD_EXTRACTION_DIR"]).glob("*.json"))


def test_country_can_be_selected_per_entry(app):
    client = app.test_client()
    token = login(client)
    response = upload(client, token, form(country=""))
    location = response.headers["Location"]
    assert b"Possible match" in client.get(location).data
    assert client.post(location + "/check", data={"csrf_token": token, "r0_representing_country": "AL_TEST"}).status_code == 302
    assert b"Exact match" in client.get(location).data


def test_another_session_cannot_view_edit_export_or_clear_a_draft(app):
    first, second = app.test_client(), app.test_client()
    location = upload(first, login(first)).headers["Location"]
    token = login(second)  # Even the same username must not inherit the other session's upload.
    assert second.get(location).status_code == 404
    for operation in ("check", "export.csv", "clear"):
        assert second.post(location + "/" + operation, data={"csrf_token": token}).status_code == 404
    assert first.get(location).status_code == 200


def test_unavailable_database_still_allows_extraction_and_export(app, monkeypatch):
    def unavailable():
        raise ServerSelectionTimeoutError("offline")
    monkeypatch.setattr(routes, "load_match_context", unavailable)
    client = app.test_client()
    token = login(client)
    location = upload(client, token).headers["Location"]
    page = client.get(location)
    assert b"Not checked" in page.data
    assert b"No candidate found" not in page.data.split(b"<tbody>")[1]
    response = client.post(location + "/export.csv", data={"csrf_token": token})
    assert response.status_code == 200
    rows = list(csv.reader(StringIO(response.data.decode("utf-8-sig"))))
    assert dict(zip(EXPORT_COLUMNS, rows[1]))["match_status"] == "Not checked"


def test_invalid_dates_missing_files_and_disabled_tool(app):
    client = app.test_client()
    token = login(client)
    assert client.post("/imports/word", data={"csrf_token": token}).status_code == 400
    location = upload(client, token).headers["Location"]
    assert client.post(location + "/check", data={"csrf_token": token, "r0_dob": "31.02.1980"}).status_code == 400
    assert b"Review required" in client.get(location).data
    app.config["WORD_EXTRACTION_ENABLED"] = False
    assert client.get("/imports/word").status_code == 404
    assert client.post(location + "/export.xlsx", data={"csrf_token": token}).status_code == 404


@pytest.mark.parametrize("operation", ["check", "export.csv"])
def test_invalid_date_retains_all_edits_and_can_be_corrected(app, operation):
    client = app.test_client()
    token = login(client)
    location = upload(client, token, docx(table([
        ["Name", "Ana TEST"], ["Date of birth", "25.03.1980"],
        ["Name", "Bora TEST"], ["Date of birth", "26.03.1980"],
    ]))).headers["Location"]
    response = client.post(location + "/" + operation, data={"csrf_token": token, "r0_dob": "1971-2-30", "r0_position": "Edited specialist", "r1_position": "Edited second row"})
    assert response.status_code == 400
    assert b'id="word-review-form"' in response.data and b'aria-invalid="true"' in response.data
    assert b"Your edits are saved" in response.data
    for value in (b"1971-2-30", b"Edited specialist", b"Edited second row"):
        assert value in response.data and value in client.get(location).data
    exported = client.post(location + "/export.csv", data={"csrf_token": token, "r0_dob": "1971/11/26"})
    assert exported.status_code == 200
    rows = list(csv.reader(StringIO(exported.data.decode("utf-8-sig"))))
    assert dict(zip(EXPORT_COLUMNS, rows[1]))["dob"] == "1971-11-26"
    assert dict(zip(EXPORT_COLUMNS, rows[2]))["position"] == "Edited second row"
    assert b'aria-invalid="true"' not in client.get(location).data


@pytest.mark.parametrize("value,expected", [("1967/2/10", "1967-02-10"), ("1967-2-10", "1967-02-10"), ("1971/11/26", "1971-11-26"), ("1971-11-26", "1971-11-26"), ("11/26/1971", "1971-11-26")])
def test_year_first_edits_work_for_save_and_csv_download(app, value, expected):
    client = app.test_client()
    token = login(client)
    location = upload(client, token).headers["Location"]
    assert client.post(location + "/check", data={"csrf_token": token, "r0_dob": value}).status_code == 302
    exported = client.post(location + "/export.csv", data={"csrf_token": token, "r0_dob": value})
    assert exported.status_code == 200
    rows = list(csv.reader(StringIO(exported.data.decode("utf-8-sig"))))
    assert dict(zip(EXPORT_COLUMNS, rows[1]))["dob"] == expected


def test_expired_drafts_are_removed_and_not_stored_in_session(app):
    client = app.test_client()
    location = upload(client, login(client)).headers["Location"]
    path = next(Path(app.config["WORD_EXTRACTION_DIR"]).glob("*.json"))
    assert path.stat().st_mode & 0o777 == 0o600
    with client.session_transaction() as session:
        assert "Ana TEST" not in json.dumps(dict(session))
    past = path.stat().st_mtime - drafts.TTL_SECONDS - 1
    os.utime(path, (past, past))
    assert client.get(location).status_code == 410
    assert not path.exists()


@pytest.mark.parametrize("name_cell", [
    "NAME (AS IN PASSPORT)Ana TESTIĆ",
    "NAME & LAST NAME(AS IN PASSPORT)Ana TESTIĆ",
    "Name: (AS IN PASSPORT)Ana TESTIĆ",
    "Name: & LAST NAME(AS IN PASSPORT)Ana TESTIĆ",
])
def test_passport_name_instructions_joined_to_values_are_not_names(name_cell):
    documents = [
        docx(paragraph(name_cell)),
        docx(table([[name_cell]])),
    ]
    for content in documents:
        result = extract_docx(content, "name-form.docx")
        assert len(result) == 1
        assert result[0]["fields"]["name"] == "Ana TESTIĆ"


@pytest.mark.parametrize("value", [
    "(AS IN PASSPORT)Ana TESTIĆ",
    "& LAST NAME(AS IN PASSPORT)Ana TESTIĆ",
    "(as in the passport): Ana TESTIĆ",
])
def test_passport_instructions_in_a_separate_value_cell_are_removed(value):
    result = extract_docx(docx(table([["Name:", value]])), "name-form.docx")
    assert len(result) == 1
    assert result[0]["fields"]["name"] == "Ana TESTIĆ"


def test_passport_instruction_alone_keeps_waiting_for_the_name():
    documents = [
        docx(paragraph("Name:"), paragraph("(AS IN PASSPORT)"), paragraph("Ana TESTIĆ")),
        docx(table([["Name:", "(AS IN PASSPORT)", "Ana TESTIĆ"]])),
    ]
    for content in documents:
        result = extract_docx(content, "name-form.docx")
        assert len(result) == 1
        assert result[0]["fields"]["name"] == "Ana TESTIĆ"


def test_roster_passport_name_header_is_not_a_participant():
    content = docx(table([
        ["NAME & LAST NAME(AS IN PASSPORT)", "DOB"],
        ["Ana TESTIĆ", "25.03.1980"],
        ["Bora TESTIĆ", "26.03.1981"],
    ]))
    result = extract_docx(content, "roster.docx")
    assert [row["fields"]["name"] for row in result] == ["Ana TESTIĆ", "Bora TESTIĆ"]
    assert [row["fields"]["dob"] for row in result] == ["1980-03-25", "1981-03-26"]


def test_name_cleanup_preserves_actual_name_punctuation_and_matching():
    result = extract_docx(form(name="Ana (Annie) TESTIĆ"), "name-form.docx")
    assert result[0]["fields"]["name"] == "Ana (Annie) TESTIĆ"
    item = extract_docx(form(name="(AS IN PASSPORT)Ana TEST"), "name-form.docx")[0]
    WordMatchContext(PARTICIPANTS, COUNTRIES).check(item)
    assert item["match_status"] == "Exact match"
    assert item["matches"][0]["pid"] == "PTEST1"


_WORD_ALIAS_COUNTRIES = COUNTRIES + [
    {"cid": "ME_TEST", "country": "Montenegro, Europe & Eurasia"},
    {"cid": "SI_TEST", "country": "Slovenia, Europe & Eurasia"},
]


@pytest.mark.parametrize("label,cid", [
    ("Slovenia", "SI_TEST"),
    ("MONTENEGRO", "ME_TEST"),
    ("R.Macedonia", "MK_TEST"),
    ("Republik of Kosova", "XK_TEST"),
    ("Bosnia and Hercegovina", "BA_TEST"),
])
def test_word_country_variants_resolve_authoritative_catalog_cids(label, cid):
    context = WordMatchContext([], _WORD_ALIAS_COUNTRIES)
    item = extract_docx(form(country=label), "country-form.docx")[0]
    context.check(item)
    assert item["fields"]["country_label"] == label
    assert item["fields"]["representing_country"] == cid
    # Bare country headings must also apply to the following participant.
    headed = extract_docx(docx(paragraph(label), paragraph("Name: Ana TEST")), "section.docx")[0]
    context.check(headed)
    assert headed["fields"]["representing_country"] == cid


def test_word_country_variants_require_a_unique_existing_catalog_record():
    context = WordMatchContext([], COUNTRIES)
    assert context.resolve_country("Slovenia") == ""
    duplicate = _WORD_ALIAS_COUNTRIES + [
        {"cid": "SI_OTHER", "country": "Slovenia, Europe & Eurasia", "iso": "SI"},
    ]
    assert WordMatchContext([], duplicate).resolve_country("Slovenia") == ""
    assert WordMatchContext([], _WORD_ALIAS_COUNTRIES).resolve_country("Slovenia, Montenegro") == ""


@pytest.mark.parametrize("employment", [
    table([["Position:", "Investigator"], ["Unit:", "Anti-Drugs Unit"], ["Organization:", "Ministry of Interior"]]),
    table([["Position:"], ["Investigator"], ["Unit:"], ["Anti-Drugs Unit"], ["Organization:"], ["Ministry of Interior"]]),
    table([["Position:\nInvestigator\nUnit:\nAnti-Drugs Unit\nOrganization:\nMinistry of Interior"]]),
    table([["Current Position/Title:", "Investigator"], ["Department/Unit:", "Anti-Drugs Unit"], ["Institution/Organization:", "Ministry of Interior"]]),
])
def test_employment_fields_survive_rows_wrapped_cells_and_continuation_tables(employment):
    content = docx(table([["Name:", "Ana TEST"], ["DOB:", "25.03.1980"]]), employment)
    result = extract_docx(content, "employment.docx")
    assert len(result) == 1
    fields = result[0]["fields"]
    assert (fields["position"], fields["unit"], fields["organization"]) == (
        "Investigator", "Anti-Drugs Unit", "Ministry of Interior")
    assert fields["dob"] == "1980-03-25"
    assert any("Table 2" in source for source in result[0]["sources"])


def test_biography_can_continue_after_a_table_and_fill_missing_employment():
    bio = ("He joined the police in 1995. Since 2015 he is an investigator "
           "in the Anti-Drugs Unit in the Ministry of Interior.")
    content = docx(table([["Name:", "Ana TEST"], ["Short biography:"]]), paragraph(bio))
    item = extract_docx(content, "PFE16M1.docx")[0]
    fields = item["fields"]
    assert fields["bio_short"] == bio
    assert fields["position"] == "investigator"
    assert fields["unit"] == "Anti-Drugs Unit"
    assert fields["organization"] == "Ministry of Interior"
    assert fields["police_joined"] == "1995"
    experience = item["professional_experience"]
    assert experience["display_years"] == "21 (estimate; event year only)"
    assert experience["reference"] == "2016 (event reference)"
    assert experience["scope"] == "Police"
    assert item["biography_fields"]["position"]["evidence"] in bio


def test_each_person_keeps_their_own_labeled_fields_and_biography():
    content = docx(
        table([["Name:", "Ana TEST"]]),
        table([["Position:", "Investigator"], ["Biography:", "I joined the police in 1995."]]),
        table([["Name:", "Bora TEST"]]),
        table([["Organization:", "Public Prosecutor's Office"],
               ["Biography:", "I have been a prosecutor for over 20 years."]]),
    )
    first, second = extract_docx(content, "PFE16M1.docx")
    assert first["fields"]["position"] == "Investigator"
    assert first["fields"]["police_joined"] == "1995"
    assert "1995" not in second["fields"]["bio_short"]
    assert second["fields"]["police_joined"] == ""
    assert second["professional_experience"]["display_years"] == "20+"
    assert second["professional_experience"]["scope"] == "Prosecution"


def test_explicit_employment_fields_take_priority_over_biography():
    content = docx(table([
        ["Name:", "Ana TEST"], ["Position:", "Senior specialist"], ["Unit:", "Current Unit"],
        ["Organization:", "Explicit employer"],
        ["Biography:", "I am an investigator in the Anti-Drugs Unit in the Ministry of Interior."],
    ]))
    item = extract_docx(content, "employment.docx")[0]
    fields = item["fields"]
    assert (fields["position"], fields["unit"], fields["organization"]) == (
        "Senior specialist", "Current Unit", "Explicit employer")
    assert item["biography_fields"] == {}


def test_years_in_a_unit_do_not_become_total_police_service():
    content = docx(table([["Name:", "Ana TEST"],
                         ["Biography:", "I have 10 years in the police narcotics unit."]]))
    item = extract_docx(content, "PFE16M1.docx")[0]
    assert item["fields"]["police_joined"] == ""
    assert item["professional_experience"]["display_years"] == "Unknown"


def test_no_event_reference_does_not_estimate_service_at_today():
    content = docx(table([["Name:", "Ana TEST"], ["Biography:", "I joined the police in 1995."]]))
    item = extract_docx(content, "employment.docx")[0]
    assert item["fields"]["police_joined"] == "1995"
    assert item["professional_experience"]["display_years"] == "Unknown"
    assert item["professional_experience"]["method"] == "Event date needed for estimate"


def test_csv_includes_service_evidence_and_keeps_countries_explicit():
    item = extract_docx(docx(table([
        ["Name:", "Ana TEST"], ["Biography:", "I joined the police in 1995."],
    ])), "PFE16M1.docx")[0]
    values = list(csv.reader(StringIO(export_csv([item]).decode("utf-8-sig"))))
    row = dict(zip(EXPORT_COLUMNS, values[1]))
    assert row["experience_years"] == "21 (estimate; event year only)"
    assert row["experience_evidence"] == "I joined the police in 1995."
    assert row["representing_country"] == ""


def test_biography_edits_refresh_service_and_preserve_manual_position(app):
    client = app.test_client()
    token = login(client)
    content = docx(table([
        ["Name:", "Ana TEST"], ["Country:", "Albania"], ["DOB:", "25.03.1980"],
        ["Biography:", "I joined the police in 1995. I am an investigator in the Anti-Drugs Unit in the Ministry of Interior."],
    ]))
    location = upload(client, token, content, "PFE16M1.docx").headers["Location"]
    page = client.get(location)
    assert b"From biography" in page.data
    assert b"21 (estimate; event year only)" in page.data
    exported = client.post(location + "/export.csv", data={
        "csrf_token": token, "r0_position": "Manually corrected title",
        "r0_bio_short": "I joined the police in 2000. I am an analyst in the Financial Crime Unit in the Ministry of Interior.",
    })
    assert exported.status_code == 200
    values = list(csv.reader(StringIO(exported.data.decode("utf-8-sig"))))
    row = dict(zip(EXPORT_COLUMNS, values[1]))
    assert row["position"] == "Manually corrected title"
    assert row["unit"] == "Financial Crime Unit"
    assert row["police_joined"] == "2000"
    assert row["experience_years"] == "16 (estimate; event year only)"
