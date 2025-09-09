from io import BytesIO

from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo

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
    ws_online.append(["Name", "Middle name", "Last name", "Email address", "Phone number", "Position"])
    # ParticipantsList entry deliberately omits accents to ensure country-table
    # spelling is preserved in the output
    ws_online.append(["Ana", "Marija", "Bajic Bralic", "ana@example.com", "123", "Advisor"])
    tbl_online = Table(displayName="ParticipantsList", ref="A1:F2")
    tbl_online.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
    ws_online.add_table(tbl_online)

    # Country table with the attendee
    ws_country = wb.create_sheet("Cro")
    ws_country.append(["Name and last name", "Grade"])
    ws_country.append(["BAJIĆ BRALIĆ, Ana Marija", ""])
    tbl_country = Table(displayName="tableCro", ref="A1:B2")
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

