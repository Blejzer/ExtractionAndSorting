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
        "transportation": "Air (Airplane)",
        "travel_doc_type": "Passport",
    }

    participant_payload = service.convert_to_participant_json(source)
    event_payload = service.convert_to_participant_event_json(source)

    assert participant_payload["pid"] == "P0873"
    assert participant_payload["name"] == "Mario XHAJA"
    assert participant_payload["intl_authority"] is True
    assert participant_payload["organization"] == "LEVER"

    assert "_id" not in participant_payload
    assert "created_at" not in participant_payload
    assert "updated_at" not in participant_payload
    assert "_audit" not in participant_payload

    # participant_event fields are kept separately, not dropped
    assert "transportation" not in participant_payload
    assert event_payload["transportation"] == "Air (Airplane)"
    assert event_payload["travel_doc_type"] == "Passport"


def test_extract_participants_merges_participant_and_participant_event_fields(monkeypatch) -> None:
    service = DocxImportService.__new__(DocxImportService)

    monkeypatch.setattr(
        service,
        "parse_docx",
        lambda _path: [
            {
                "name": "Mario XHAJA",
                "representing_country": "C002",
                "transportation": "Air (Airplane)",
                "travel_doc_type": "Passport",
                "citizenships": ["C002"],
            }
        ],
    )
    monkeypatch.setattr(service, "normalize_fields", lambda row: row)

    bundle = service.extract_participants(["/tmp/mario.docx"], "E001")

    row = bundle["participants"][0]
    assert row["name"] == "Mario XHAJA"
    assert row["transportation"] == "Air (Airplane)"
    assert row["travel_doc_type"] == "Passport"
