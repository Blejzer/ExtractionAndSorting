from services.docx_import_service import DocxImportService
import services.docx_import_service as mod


def test_parse_docx_raises_when_key_missing(monkeypatch) -> None:
    service = DocxImportService.__new__(DocxImportService)

    monkeypatch.setattr(mod, "extract_docx_text", lambda _path: "some text")
    monkeypatch.setattr(mod, "_resolve_api_key", lambda: None)

    try:
        service.parse_docx("dummy.docx")
    except RuntimeError as exc:
        assert "OPENAIAPI" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected RuntimeError when API key is missing")


def test_extract_participants_sets_openai_engine(monkeypatch) -> None:
    service = DocxImportService.__new__(DocxImportService)

    monkeypatch.setattr(service, "parse_docx", lambda _path: [{"name": "John Doe", "representing_country": "", "citizenships": []}])
    monkeypatch.setattr(service, "normalize_fields", lambda row: row)

    bundle = service.extract_participants(["/tmp/demo.docx"], "E001")

    assert bundle["warnings"] == []
    assert bundle["participants"][0]["_extraction_engine"] == "openai"
    assert bundle["participants"][0]["_source_file"] == "demo.docx"


def test_save_participant_updates_event_snapshot(monkeypatch) -> None:
    service = DocxImportService.__new__(DocxImportService)

    class _DummyEventCollection:
        def __init__(self) -> None:
            self.calls = []

        def update_one(self, query, payload):
            self.calls.append((query, payload))

    class _DummyEventRepo:
        def __init__(self) -> None:
            self.collection = _DummyEventCollection()
            self.links = []

        def ensure_link(self, participant_id: str, event_id: str):
            self.links.append((participant_id, event_id))

    class _DummyParticipantRepo:
        def update(self, pid, payload):
            self.last = (pid, payload)

        def generate_next_pid(self):
            return "P9999"

        def save(self, _model):
            return None

    service.participant_event_repo = _DummyEventRepo()
    service.participant_repo = _DummyParticipantRepo()

    monkeypatch.setattr(service, "compare_with_db", lambda _p: type("R", (), {"existing": {"pid": "P0001"}})())

    participant = {
        "pid": "P0001",
        "name": "John Doe",
        "representing_country": "C001",
        "transportation": "Air (Airplane)",
        "travel_doc_type": "Passport",
    }

    pid = service.save_participant(participant, "E001")

    assert pid == "P0001"
    assert service.participant_event_repo.links == [("P0001", "E001")]
    query, payload = service.participant_event_repo.collection.calls[0]
    assert query == {"participant_id": "P0001", "event_id": "E001"}
    assert payload["$set"]["transportation"] == "Air (Airplane)"
    assert payload["$set"]["travel_doc_type"] == "Passport"
