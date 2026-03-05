from services.docx_import_service import DocxImportService


def test_convert_to_participant_json_matches_participant_collection_shape() -> None:
    service = DocxImportService.__new__(DocxImportService)

    source = {
        "_id": "mongo-generated",
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
        "_audit": [{"x": 1}],
        "pid": "P0873",
        "name": "Mario XHAJA",
        "representing_country": "C002",
        "grade": 1,
        "position": "NBI Investigator",
        "phone": "+355684612950",
        "email": "mario.xhaja@spak.gov.al",
        "gender": "Male",
        "dob": "1993-12-11",
        "pob": "Memmingen",
        "birth_country": "C083",
        "citizenships": ["C002"],
        "diet_restrictions": "No Pork, No Alcohol",
        "organization": "LEVER",
        "unit": "NBI",
        "rank": "Investigator",
        "intl_authority": True,
        "bio_short": "bio",
        # participant_event-only fields should not be present in participant payload
        "transportation": "Air (Airplane)",
        "transport_other": "",
        "traveling_from": "Tirana",
        "returning_to": "Tirana",
        "travel_doc_type": "Passport",
        "travel_doc_number": "BD7178397",
        "travel_doc_issue_date": "2022-05-24",
        "travel_doc_expiry_date": "2032-05-23",
        "travel_doc_issued_by": "MPB",
    }

    payload = service.convert_to_participant_json(source)

    assert payload["pid"] == "P0873"
    assert payload["name"] == "Mario XHAJA"
    assert payload["intl_authority"] is True
    assert payload["organization"] == "LEVER"

    assert "_id" not in payload
    assert "created_at" not in payload
    assert "updated_at" not in payload
    assert "_audit" not in payload

    assert "transportation" not in payload
    assert "travel_doc_type" not in payload
    assert "travel_doc_number" not in payload
    assert "traveling_from" not in payload
