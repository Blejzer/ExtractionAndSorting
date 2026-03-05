from services.docx_import_service import DocxImportService
import services.docx_import_service as mod


def test_parse_docx_falls_back_to_legacy_when_key_missing(monkeypatch) -> None:
    service = DocxImportService.__new__(DocxImportService)

    monkeypatch.setattr(mod, "extract_docx_text", lambda _path: "some text")
    monkeypatch.setattr(mod, "_resolve_api_key", lambda: None)
    monkeypatch.setattr(mod, "parse_docx_legacy", lambda _path: [{"name": "Legacy User"}])

    participants, engine, warning = service.parse_docx("dummy.docx")

    assert participants == [{"name": "Legacy User"}]
    assert engine == "legacy"
    assert warning and "not configured" in warning


def test_extract_participants_sets_engine_and_collects_warnings(monkeypatch) -> None:
    service = DocxImportService.__new__(DocxImportService)

    monkeypatch.setattr(
        service,
        "parse_docx",
        lambda _path: ([{"name": "John Doe", "representing_country": "", "citizenships": []}], "legacy", "fallback"),
    )
    monkeypatch.setattr(service, "normalize_fields", lambda row: row)

    bundle = service.extract_participants(["/tmp/demo.docx"], "E001")

    assert bundle["warnings"] == ["demo.docx: fallback"]
    assert bundle["participants"][0]["_extraction_engine"] == "legacy"
    assert bundle["participants"][0]["_source_file"] == "demo.docx"
