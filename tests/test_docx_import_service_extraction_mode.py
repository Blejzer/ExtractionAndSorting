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
