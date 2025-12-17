from __future__ import annotations

from tests.test_import_service_gender import _workbook_bytes_with_gender

import services.import_service_v2 as import_service


def test_parse_for_commit_loads_workbook_once(monkeypatch, tmp_path):
    workbook_path = tmp_path / "cache.xlsx"
    workbook_path.write_bytes(_workbook_bytes_with_gender("Male"))

    load_calls = 0
    real_load = import_service.openpyxl.load_workbook

    def counting_loader(*args, **kwargs):
        nonlocal load_calls
        load_calls += 1
        return real_load(*args, **kwargs)

    monkeypatch.setattr(import_service.openpyxl, "load_workbook", counting_loader)

    result = import_service.parse_for_commit(str(workbook_path))

    assert result["preview"]["participants"], "Expected JSON preview data"
    assert load_calls == 1, "Workbook should only be loaded once"
