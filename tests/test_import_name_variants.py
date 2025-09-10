from io import BytesIO

from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter
from datetime import datetime

import services.import_service as import_service


def _build_workbook_bytes() -> bytes:
    """Construct a minimal workbook containing the required tables."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Participants"
    ws["A1"] = "E1 TITLE"
    ws["A2"] = "JUNE 1 - 3 - Zagreb"

    # ParticipantsLista with position/phone/email
    ws_list = wb.create_sheet("List")
    ws_list.append(["Name (Latin)", "Position", "Phone", "Email"])
    ws_list.append(["BAJIĆ BRALIĆ, Ana Marija", "Advisor", "123", "ana@example.com"])
    tbl_list = Table(displayName="ParticipantsLista", ref="A1:D2")
    tbl_list.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
    ws_list.add_table(tbl_list)

    # MAIN ONLINE → ParticipantsList (minimal)
    ws_online = wb.create_sheet("MAIN ONLINE")
    online_cols = [
        "Name", "Middle name", "Last name", "Gender", "Date of Birth (DOB)",
        "Place Of Birth (POB)", "Country of Birth", "Citizenship(s)",
        "Email address", "Phone number", "Travelling document type",
        "Travelling document number", "Travelling document issuance date",
        "Travelling document expiry date", "Travelling document issued by",
        "Do you require Visa to travel to Croatia", "Returning to",
        "Diet restrictions", "Organization", "Unit", "Position", "Rank",
        "Authority", "Short professional biography", "Bank name", "IBAN",
        "IBAN Type", "SWIFT",
    ]
    ws_online.append(online_cols)
    ws_online.append([
        "Ana", "Marija", "Bajic Bralic", "female", datetime(1973, 5, 25),
        "Radac", "Kosovo, Europe & Eurasia, World", "Kosovo, Europe & Eurasia, World",
        "ana@example.com", "123", "Passport", "P01415451", datetime(2019, 3, 27),
        datetime(2029, 3, 26), "Republic of Kosovo", "No", "Pristina",
        "No pork, no chilli", "Prosecution System", "Peja Basic Prosecutor's Office",
        "Advisor", "Chief prosecutor", "Yes", "bio",
        "BANKA KOMBETARE TREGTARE KOSOVE SHA", "XK051920315886321195",
        "EURO", "NCBA XK PR",
    ])
    last_col = get_column_letter(len(online_cols))
    tbl_online = Table(displayName="ParticipantsList", ref=f"A1:{last_col}2")
    tbl_online.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
    ws_online.add_table(tbl_online)

    # Country table with the attendee
    ws_country = wb.create_sheet("Cro")
    ws_country.append(["Name and last name", "Grade"])
    ws_country.append(["BAJIĆ BRALIĆ, Ana Marija", ""])
    ws_country.append(["TOTAL", ""])
    tbl_country = Table(displayName="tableCro", ref="A1:B3")
    tbl_country.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
    ws_country.add_table(tbl_country)

    stream = BytesIO()
    wb.save(stream)
    return stream.getvalue()


def test_bajic_bralic_lookup(tmp_path):
    content = _build_workbook_bytes()
    path = tmp_path / "sample.xlsx"
    with open(path, "wb") as fh:
        fh.write(content)

    result = import_service.parse_for_commit(str(path))
    attendees = result["attendees"]
    assert len(attendees) == 1
    attendee = attendees[0]
    assert attendee["name"] == "Ana Marija BAJIĆ BRALIĆ"
    assert attendee["position"] == "Advisor"
    assert attendee["phone"] == "123"
    assert attendee["email"] == "ana@example.com"
    assert attendee["gender"] == "female"
    assert attendee["dob"] == "1973-05-25"
    assert attendee["pob"] == "Radac"
    assert attendee["birth_country"] == "Kosovo, Europe & Eurasia, World"
    assert attendee["citizenships"] == ["Kosovo", "Europe & Eurasia", "World"]
    assert attendee["travel_doc_type"] == "Passport"
    assert attendee["travel_doc_number"] == "P01415451"
    assert attendee["travel_doc_issue_date"] == "2019-03-27"
    assert attendee["travel_doc_expiry_date"] == "2029-03-26"
    assert attendee["travel_doc_issued_by"] == "Republic of Kosovo"
    assert attendee["requires_visa_hr"] is False
    assert attendee["returning_to"] == "Pristina"
    assert attendee["diet_restrictions"] == "No pork, no chilli"
    assert attendee["organization"] == "Prosecution System"
    assert attendee["unit"] == "Peja Basic Prosecutor's Office"
    assert attendee["rank"] == "Chief prosecutor"
    assert attendee["intl_authority"] is True
    assert attendee["bio_short"] == "bio"
    assert attendee["bank_name"] == "BANKA KOMBETARE TREGTARE KOSOVE SHA"
    assert attendee["iban"] == "XK051920315886321195"
    assert attendee["iban_type"] == "EURO"
    assert attendee["swift"] == "NCBA XK PR"

    # Ensure debug-only data is not present by default
    assert "initial_attendees" not in result

