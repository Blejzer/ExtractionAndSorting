from services.docx_import_service import DocxImportService


def test_convert_to_participant_json_keeps_extended_form_fields() -> None:
    service = DocxImportService.__new__(DocxImportService)

    source = {
        "pid": "P0873",
        "name": "Mario XHAJA",
        "representing_country": "C002",
        "transportation": "Air (Airplane)",
        "transport_other": "",
        "traveling_from": "Tirana",
        "grade": 1,
        "position": "NBI Investigator",
        "phone": "+355684612950",
        "email": "mario.xhaja@spak.gov.al",
        "gender": "Male",
        "dob": "1993-12-11",
        "pob": "Memmingen",
        "birth_country": "C083",
        "citizenships": ["C002"],
        "travel_doc_type": "Passport",
        "travel_doc_number": "BD7178397",
        "travel_doc_issue_date": "2022-05-24",
        "travel_doc_expiry_date": "2032-05-23",
        "travel_doc_issued_by": "MPB",
        "returning_to": "Tirana",
        "diet_restrictions": "No Pork, No Alcohol",
        "organization": "LEVER",
        "unit": "NBI",
        "rank": "Investigator",
        "intl_authority": True,
        "bio_short": "bio",
        "bank_name": "Credins Bank",
        "iban": "AL51212110090000000001900345",
        "iban_type": "USD",
        "swift": "CDISALTR",
        "ignored": "value",
    }

    payload = service.convert_to_participant_json(source)

    assert payload["travel_doc_type"] == "Passport"
    assert payload["transport_other"] == ""
    assert payload["traveling_from"] == "Tirana"
    assert payload["returning_to"] == "Tirana"
    assert payload["unit"] == "NBI"
    assert payload["intl_authority"] is True
    assert payload["pid"] == "P0873"
    assert "ignored" not in payload
