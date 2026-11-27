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


def test_identical_repeated_source_selections_are_counted_once():
    proposal=plan()
    proposal["records"] += [record(score_cell="'PreTest'!$B$5",name_cell=" $b$1 "), record("PostTest")]
    rows,notes=execute(proposal)
    assert len(rows)==4
    assert sum(row["pre"] is not None for row in rows)==2
    assert sum(row["post"] is not None for row in rows)==2
    assert interpret_test_results(rows)["pre_average"]==54
    assert any("Collapsed 2 repeated AI selections" in note for note in notes)


def test_aggregate_names_are_rejected():
    book = load_workbook(BytesIO(unfamiliar_bytes()))
    book["PostTest"]["B1"] = "TOTAL"
    with pytest.raises(ImportErrorForTest, match="aggregate"):
        execute(dict(records=[record("PostTest")],notes=[]),serialize(book))


@pytest.mark.parametrize("changes", [dict(name_cell="C1"),dict(name_cell=None),dict(attempt="post")])
def test_repeated_cell_with_conflicting_identity_or_attempt_is_rejected(changes):
    book=load_workbook(BytesIO(unfamiliar_bytes()))
    book["PreTest"].title="Results"
    first=record(sheet="Results",attempt="pre")
    second=dict(first,**changes)
    with pytest.raises(ImportErrorForTest) as exc:
        execute(dict(records=[first,second],notes=[]),serialize(book))
    message=str(exc.value)
    assert "same result cell Results!B5" in message and "conflicting" in message
    assert "results 1 and 2" in message


def test_repeated_cell_with_conflicting_verified_scale_is_rejected():
    book=load_workbook(BytesIO(unfamiliar_bytes()))
    sheet=book["PreTest"]
    sheet["B5"],sheet["B5"].number_format=0.5,"General"
    sheet["A5"],sheet["A4"]="Percentage","Fraction 0-1"
    first=record(unit="percent",evidence_cell="A5")
    second=record(unit="fraction",evidence_cell="A4")
    with pytest.raises(ImportErrorForTest,match="conflicting"):
        execute(dict(records=[first,second],notes=[]),serialize(book))


def test_equal_scores_from_different_source_cells_are_not_silently_merged():
    book=load_workbook(BytesIO(unfamiliar_bytes()))
    sheet=book["PostTest"]
    sheet["C1"]="Ana EXAMPLE"
    sheet["C4"]=20
    with pytest.raises(ImportErrorForTest,match="same person and attempt"):
        execute(dict(records=[record("PostTest","B"),record("PostTest","C")],notes=[]),serialize(book))


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


@pytest.mark.parametrize("address", [
    "b5", " $B$5 ", "$b5", "B$5", "PreTest!B5", "'PreTest'!$B$5",
    "='pretest'!$b$5", "B5:B5", "'PreTest'!B5:'PreTest'!$B$5",
])
def test_normal_excel_reference_variants_read_the_same_source_value(address):
    proposal=dict(records=[record(score_cell=address,name_cell="'pretest'!$B$1",evidence_cell=" $a$5 ")],notes=[])
    rows,_=execute(proposal)
    assert rows[0]["pre"]["score"]==64
    assert rows[0]["pre"]["cell"]=="B5"
    assert rows[0]["needs_identity_review"] is True


def test_quoted_sheet_names_with_spaces_and_apostrophes():
    book=load_workbook(BytesIO(unfamiliar_bytes()))
    book["PreTest"].title="Pre Test's results"
    proposal=dict(records=[record(sheet="Pre Test's results",attempt="pre",score_cell="'Pre Test''s results'!$B$5",
                                  name_cell="'Pre Test''s results'!$B$1")],notes=[])
    rows,_=execute(proposal,serialize(book))
    assert rows[0]["pre"]["score"]==64


@pytest.mark.parametrize("value", [None,"", "null", "None", " N/A "])
def test_unused_optional_references_do_not_block_valid_percentages(value):
    proposal=dict(records=[record(name_cell=value,maximum_cell=value,question_range=value,evidence_cell=value)],notes=[])
    rows,_=execute(proposal)
    assert rows[0]["pre"]["score"]==64 and rows[0]["needs_identity_review"] is True


@pytest.mark.parametrize("address,reason", [
    ("PostTest!B5","different worksheet"), ("[external.xlsx]PreTest!B5","External workbook"),
    ("B4:B5","one cell, not a range"), ("B999","outside"), ("R5C2","single Excel cell"),
])
def test_invalid_reference_diagnostics_identify_field_sheet_and_reference(address,reason):
    with pytest.raises(ImportErrorForTest) as exc:
        execute(dict(records=[record(score_cell=address)],notes=[]))
    message=str(exc.value)
    assert "AI result 1" in message and "PreTest" in message and "score_cell=" in message
    assert address in message and reason in message


def test_absolute_sheet_qualified_question_range_is_counted_without_broadening_it():
    book=load_workbook(BytesIO(unfamiliar_bytes()))
    book["PreTest"]["B4"]=1
    proposal=dict(records=[record(score_cell=" $b$4 ",unit="points",question_range="'PreTest'!$a$2:$a$3")],notes=[])
    rows,_=execute(proposal,serialize(book))
    assert rows[0]["pre"]["score"]==50


def test_a_rejected_reference_does_not_expand_worksheet_dimensions():
    book=load_workbook(BytesIO(unfamiliar_bytes()))
    sheet=book["PreTest"]
    dimensions=(sheet.max_row,sheet.max_column)
    with pytest.raises(ImportErrorForTest):
        agent._cell(sheet,"ZZ9999")
    assert (sheet.max_row,sheet.max_column)==dimensions
