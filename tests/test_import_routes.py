import os
from io import BytesIO

import pytest
from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo

from app import create_app


def _build_workbook_bytes(valid: bool) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Participants"
    ws["A1"] = "E1 Title"
    ws["A2"] = "2024"

    if valid:
        ws_list = wb.create_sheet("List")
        ws_list.append(["Name", "Position"])
        ws_list.append(["Doe John", "Leader"])
        tbl = Table(displayName="ParticipantsLista", ref="A1:B2")
        tbl.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
        ws_list.add_table(tbl)

        ws_country = wb.create_sheet("Alb")
        ws_country.append(["Name", "Grade"])
        ws_country.append(["John Doe", "10"])
        tbl2 = Table(displayName="tableAlb", ref="A1:B2")
        tbl2.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
        ws_country.add_table(tbl2)

    stream = BytesIO()
    wb.save(stream)
    return stream.getvalue()


@pytest.fixture
def client(tmp_path):
    app = create_app()
    app.config["TESTING"] = True
    app.config["UPLOADS_DIR"] = tmp_path
    os.makedirs(tmp_path, exist_ok=True)
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess["username"] = "tester"
        yield client


def test_upload_valid_file(client):
    data = {"file": (BytesIO(_build_workbook_bytes(True)), "sample.xlsx")}
    resp = client.post("/import", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    assert b"File is OK" in resp.data


def test_upload_invalid_file(client):
    data = {"file": (BytesIO(_build_workbook_bytes(False)), "bad.xlsx")}
    resp = client.post("/import", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    assert b"File is not formatted correctly" in resp.data


def test_proceed_and_discard(client, tmp_path):
    content = _build_workbook_bytes(True)
    path = tmp_path / "sample.xlsx"
    with open(path, "wb") as fh:
        fh.write(content)

    resp = client.post("/import/proceed", data={"filename": "sample.xlsx"})
    assert resp.status_code == 302

    # recreate file for discard test
    with open(path, "wb") as fh:
        fh.write(content)
    assert path.exists()
    resp = client.post("/import/discard", data={"filename": "sample.xlsx"})
    assert resp.status_code == 302
    assert not path.exists()
