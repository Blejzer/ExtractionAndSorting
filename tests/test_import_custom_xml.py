import zipfile
from pathlib import Path

import services.import_service as import_service
from domain.models.participant import Participant
from domain.models.event_participant import EventParticipant


XML_CONTENT = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<data>
  <participant>
    <pid>P-001</pid>
    <representing_country>C123</representing_country>
    <gender>Male</gender>
    <grade>2</grade>
    <name>John Doe</name>
    <dob>2024-01-05</dob>
    <pob>Zagreb</pob>
    <birth_country>C123</birth_country>
    <citizenships>C123</citizenships>
    <email>john.doe@example.com</email>
    <phone>+385123456</phone>
    <diet_restrictions>None</diet_restrictions>
    <organization>ACME</organization>
    <unit>Unit 7</unit>
    <position>Analyst</position>
    <rank>Senior</rank>
    <intl_authority>true</intl_authority>
    <bio_short>Bio</bio_short>
  </participant>
  <participant_event>
    <event_id>EVT-001</event_id>
    <participant_id>P-001</participant_id>
    <transportation>Air (Airplane)</transportation>
    <transport_other></transport_other>
    <travelling_from>Zagreb</travelling_from>
    <returning_to>Zagreb</returning_to>
    <travel_doc_type>Passport</travel_doc_type>
    <travel_doc_type_other></travel_doc_type_other>
    <travel_doc_issue_date>2024-01-01</travel_doc_issue_date>
    <travel_doc_expiry_date>2025-01-01</travel_doc_expiry_date>
    <travel_doc_issued_by>MOI</travel_doc_issued_by>
    <bank_name>Bank</bank_name>
    <iban>HR1234567890</iban>
    <iban_type>EURO</iban_type>
    <swift>SWIFTHR</swift>
  </participant_event>
  <event>
    <eid>EVT-001</eid>
    <title>Sample Event</title>
    <start_date>2024-02-01</start_date>
    <end_date>2024-02-05</end_date>
    <place>Zagreb</place>
    <country>C123</country>
    <type>Training</type>
    <cost>199.5</cost>
  </event>
</data>
"""


def _write_custom_xml_file(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("customXml/item1.xml", XML_CONTENT)
    return path


def test_validate_accepts_custom_xml(tmp_path):
    xlsx_path = _write_custom_xml_file(tmp_path / "custom.xlsx")
    ok, missing, seen = import_service.validate_excel_file_for_import(str(xlsx_path))
    assert ok
    assert missing == []
    assert seen["custom_xml"] is True
    assert seen["participants"] == 1
    assert seen["participant_events"] == 1


def test_parse_custom_xml_creates_objects(tmp_path):
    xlsx_path = _write_custom_xml_file(tmp_path / "custom.xlsx")
    result = import_service.parse_for_commit(str(xlsx_path))

    objects = result.get("objects")
    assert objects is not None
    event_obj = objects["event"]
    assert event_obj is not None
    assert event_obj.eid == "EVT-001"

    participants = objects["participants"]
    participant_events = objects["participant_events"]
    assert len(participants) == 1
    assert len(participant_events) == 1
    assert isinstance(participants[0], Participant)
    assert isinstance(participant_events[0], EventParticipant)

    attendees = result["attendees"]
    assert len(attendees) == 1
    attendee = attendees[0]
    assert attendee["transportation"] == "Air (Airplane)"
    assert attendee["iban_type"] == "EURO"

    preview = result["preview"]
    assert preview["event"]["start_date"] == "2024-02-01"
    assert preview["participants"][0]["grade"] == 2
    assert preview["participant_events"][0]["participant_id"] == "P-001"
