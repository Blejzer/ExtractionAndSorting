"""Untrusted layout proposals are executed against real source cells, never AI scores."""

from io import BytesIO
import json

from openpyxl import Workbook, load_workbook
import pytest
import requests

import services.prepost_ai_service as agent
from services.prepost_import_service import TestImportError as ImportErrorForTest, interpret_test_results, match_test_rows
from tests.test_prepost_import import serialize


def unfamiliar_bytes():
    book = Workbook()
    book.remove(book.active)
    for title in ("PreTest", "PostTest"):
        sheet = book.create_sheet(title)
        sheet.append([None, 1 if title == "PreTest" else "Ana EXAMPLE", 2 if title == "PreTest" else "Bojan SAMPLE"])
        sheet.append([1, "a", "b"])
        sheet.append([2, "b", "b"])
        sheet.append(["SCORE", 16 if title == "PreTest" else 20, 11 if title == "PreTest" else 21])
        sheet.append(["PERC", "=B4/25", "=C4/25"])
        sheet["B5"].number_format = sheet["C5"].number_format = "0.00%"
    return serialize(book)


def record(sheet="PreTest", column="B", **changes):
    item = dict(attempt="pre" if sheet == "PreTest" else "post", sheet=sheet, name_cell=column+"1",
                score_cell=column+"5", unit="fraction", maximum_cell=None, question_range=None, evidence_cell="A5")
    item.update(changes)
    return item


def plan():
    return dict(records=[record(sheet, col) for sheet in ("PreTest", "PostTest") for col in ("B", "C")], notes=["Pre-test names are participant codes."])


def execute(proposal=None, data=None):
    data = data or unfamiliar_bytes()
    return agent.execute_layout(load_workbook(BytesIO(data)), load_workbook(BytesIO(data), data_only=True), proposal or plan())


def test_verified_percentages_and_no_column_position_identity_guessing():
    rows, notes = execute()
    assert len(rows) == 4
    unnamed = [r for r in rows if r["needs_identity_review"]]
    named = [r for r in rows if not r["needs_identity_review"]]
    assert [r["pre"]["score"] for r in unnamed] == [64, 44]
    assert [r["post"]["score"] for r in named] == [80, 84]
    roster = [dict(pid="P1", name="Ana EXAMPLE"), dict(pid="P2", name="Bojan SAMPLE")]
    match_test_rows(rows, roster)
    assert [r["selected_pid"] for r in unnamed] == ["", ""]
    assert [r["selected_pid"] for r in named] == ["P1", "P2"]
    assert interpret_test_results(rows)["paired_count"] == 0
    assert notes and all(r["pre"] is None for r in named)
    assert all(not {"answers", "points", "correct"} & score.keys() for r in rows for score in (r["pre"],r["post"]) if score)


@pytest.mark.parametrize("changes", [
    {"score": 99}, {"score_cell": "B999"}, {"score_cell": "[remote.xlsx]Sheet!B5"},
    {"score_cell": "=WEBSERVICE(1)"}, {"sheet": "Missing"}, {"attempt": "post"},
    {"unit": "unknown"}, {"attempt": []}, {"name_cell": {}},
])
def test_malformed_and_invented_proposals_are_rejected(changes):
    proposal = dict(records=[record(**changes)], notes=[])
    with pytest.raises(ImportErrorForTest):
        execute(proposal)


def test_duplicate_score_references_and_aggregate_names_are_rejected():
    with pytest.raises(ImportErrorForTest, match="same result cell"):
        execute(dict(records=[record(),record()], notes=[]))
    book = load_workbook(BytesIO(unfamiliar_bytes()))
    book["PostTest"]["B1"] = "TOTAL"
    with pytest.raises(ImportErrorForTest, match="aggregate"):
        execute(dict(records=[record("PostTest")],notes=[]),serialize(book))


def test_uncertain_units_and_invalid_percentages_do_not_import():
    book = load_workbook(BytesIO(unfamiliar_bytes()))
    book["PreTest"]["B5"] = 1.2
    with pytest.raises(ImportErrorForTest, match="between 0 and 100"):
        execute(dict(records=[record()],notes=[]),serialize(book))
    book["PreTest"]["B5"].number_format = "General"
    book["PreTest"]["A5"] = "Score"
    with pytest.raises(ImportErrorForTest, match="scale could not be verified"):
        execute(dict(records=[record(unit="percent")],notes=[]),serialize(book))


def test_points_maximum_requires_evidence_and_is_calculated_locally():
    book = load_workbook(BytesIO(unfamiliar_bytes()))
    sheet = book["PreTest"]
    sheet["E4"],sheet["F4"] = "Maximum points",25
    proposal=dict(records=[record(score_cell="B4",unit="points",maximum_cell="F4")],notes=[])
    assert execute(proposal,serialize(book))[0][0]["pre"]["score"] == 64
    sheet["E4"] = "Other number"
    with pytest.raises(ImportErrorForTest, match="maximum points"):
        execute(proposal,serialize(book))
    proposal["records"][0].update(maximum_cell=None,question_range="A2:A3")
    sheet["B4"] = 1
    assert execute(proposal,serialize(book))[0][0]["pre"]["score"] == 50


def test_missing_formula_cache_fails_without_executing_arbitrary_formulas():
    book = load_workbook(BytesIO(unfamiliar_bytes()))
    book["PreTest"]["B5"] = '=WEBSERVICE("https://example.test")'
    with pytest.raises(ImportErrorForTest,match="Recalculate"):
        execute(dict(records=[record()],notes=[]),serialize(book))


def test_client_uses_structured_output_existing_environment_key_and_untrusted_data(monkeypatch):
    monkeypatch.setenv("OPENAIAPI","test-secret")
    monkeypatch.delenv("OPENAI_API_KEY",raising=False)
    monkeypatch.setenv("PREPOST_AI_MODEL","test-model")
    captured={}
    class Response:
        content=b"{}"
        def raise_for_status(self): pass
        def json(self): return dict(choices=[dict(finish_reason="stop",message=dict(content=json.dumps(plan())))])
    def post(url,**kwargs):
        captured.update(url=url,**kwargs)
        return Response()
    monkeypatch.setattr(agent.requests,"post",post)
    data=unfamiliar_bytes()
    rows,notes=agent.analyze_prepost_with_ai(data)
    assert len(rows)==4 and captured["url"]=="https://api.openai.com/v1/chat/completions"
    assert captured["headers"]["Authorization"]=="Bearer test-secret"
    assert captured["json"]["model"]=="test-model"
    assert captured["json"]["response_format"]["json_schema"]["strict"] is True
    assert "untrusted workbook DATA" in captured["json"]["messages"][0]["content"]
    assert "test-secret" not in captured["json"]["messages"][1]["content"]
    assert captured["timeout"]==(5,45)


def test_provider_failures_and_missing_configuration_are_safe(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY",raising=False)
    monkeypatch.delenv("OPENAIAPI",raising=False)
    with pytest.raises(agent.AIImportUnavailable,match="Configure"):
        agent.request_layout("[]","partial")
    monkeypatch.setenv("OPENAIAPI","private-secret")
    def fail(*args,**kwargs): raise requests.HTTPError("private-secret and workbook content")
    monkeypatch.setattr(agent.requests,"post",fail)
    with pytest.raises(agent.AIImportUnavailable) as exc:
        agent.request_layout("[]","partial")
    assert "private-secret" not in str(exc.value)


def test_descriptive_interpretation_pairs_by_identity_and_preserves_zero():
    rows=[dict(pid="P1",pre=dict(score=40),post=dict(score=70)),
          dict(pid="P2",pre=dict(score=0),post=dict(score=0)),
          dict(pid="P3",pre=None,post=dict(score=100))]
    result=interpret_test_results(rows)
    assert result["pre_count"]==2 and result["post_count"]==3
    assert result["pre_average"]==20 and result["post_average"]==pytest.approx(56.67)
    assert result["paired_count"]==2 and result["paired_change"]==15
    assert result["improved"]==1 and result["unchanged"]==1 and result["declined"]==0
