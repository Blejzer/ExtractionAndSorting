"""Workbook scoring and participant identity review, with entirely fictional data."""

from io import BytesIO

from openpyxl import Workbook
import pytest

from services.prepost_import_service import (
    TestImportError as ImportErrorForTest, match_test_rows, parse_prepost_workbook, prepost_csv, score_table,
)
from repositories.prepost_import_repository import import_plan


def workbook_bytes(*, formulas=True, names=("Ana EXAMPLE", "Bojan SAMPLE", "MF")):
    workbook = Workbook()
    workbook.remove(workbook.active)
    for title in ("PreTest", "PostTest"):
        sheet = workbook.create_sheet(title)
        sheet.append(["Pitanja", *names])
        for number, correct in enumerate(("a", "b", "c"), 1):
            row = number + 1
            sheet.cell(row, 1, f"Pitanje {number}")
            workbook["PreTest"].cell(row, 10, correct)
            for column in range(2, len(names) + 2):
                letter = sheet.cell(row, column).column_letter
                key = f"$J{row}" if title == "PreTest" else f"PreTest!$J{row}"
                sheet.cell(row, column, f"=IF({letter}{row+7}={key},1,0)" if formulas else int(number != 2 or title == "PostTest"))
                sheet.cell(row+7, column, correct.upper() if number != 2 or title == "PostTest" else "d")
        sheet.cell(5, 1, "Total")
        sheet.cell(5, 2, "=SUBTOTAL(101,B2:B4)")  # No cached result in newly saved workbooks.
    return serialize(workbook)


def serialize(workbook):
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def edited(mutator, **kwargs):
    from openpyxl import load_workbook
    workbook = load_workbook(BytesIO(workbook_bytes(**kwargs)))
    mutator(workbook)
    return serialize(workbook)


def test_calculates_both_tests_without_formula_caches_and_excludes_mf():
    rows = parse_prepost_workbook(workbook_bytes())
    assert len(rows) == 2 and {r["name"] for r in rows} == {"Ana EXAMPLE", "Bojan SAMPLE"}
    for row in rows:
        assert row["pre"]["score"] == pytest.approx(200 / 3)
        assert row["post"]["score"] == 100
        assert not {"answers", "points", "correct", "question_count", "answered"} & row["pre"].keys()


def test_missing_attempt_is_unknown_but_partial_blank_answer_is_wrong():
    def change(workbook):
        for row in range(9, 12):
            workbook["PostTest"].cell(row, 2).value = None
        workbook["PreTest"]["C9"].value = None
    rows = parse_prepost_workbook(edited(change))
    assert rows[0]["post"] is None
    assert rows[1]["pre"]["score"] == pytest.approx(100 / 3)


def test_direct_zero_and_one_points_are_supported():
    rows = parse_prepost_workbook(workbook_bytes(formulas=False))
    assert rows[0]["pre"]["method"] == "question_points"
    zero = parse_prepost_workbook(edited(lambda w: [setattr(w["PreTest"].cell(r, 2), "value", 0) for r in range(2, 5)], formulas=False))
    assert zero[0]["pre"]["score"] == 0


@pytest.mark.parametrize("mutator,expected", [
    (lambda w: setattr(w["PreTest"]["B2"], "value", '=WEBSERVICE("https://example.test")'), "expected 0/1"),
    (lambda w: setattr(w["PreTest"]["J2"], "value", None), "missing answer key"),
    (lambda w: setattr(w["PreTest"]["B2"], "value", 2), "expected 0/1"),
    (lambda w: setattr(w["PreTest"]["C1"], "value", "ANA example"), "duplicate participant"),
])
def test_rejects_unreadable_or_ambiguous_scores(mutator, expected):
    with pytest.raises(ImportErrorForTest, match=expected):
        parse_prepost_workbook(edited(mutator))


def test_explicit_points_can_mix_with_answer_formulas():
    rows = parse_prepost_workbook(edited(lambda w: setattr(w["PreTest"]["B2"], "value", 1)))
    assert rows[0]["pre"]["score"] == pytest.approx(200 / 3)


@pytest.mark.parametrize("data", [b"", b"not excel", b"x" * 5_000_001])
def test_rejects_bad_upload(data):
    with pytest.raises(ImportErrorForTest):
        parse_prepost_workbook(data)


def test_name_matching_uses_only_unique_full_roster_names():
    roster = [dict(pid="P1", name="  ANA   example "), dict(pid="P2", name="Bojan SAMPLE"),
              dict(pid="P3", name="BOJAN sample"), dict(pid="P4", name="Čedomir TEST")]
    rows = match_test_rows([dict(name=n) for n in ["Ana EXAMPLE", "Bojan sample", "Cedomir TEST", "XX"]], roster)
    assert rows[0]["selected_pid"] == "P1"
    assert rows[1]["selected_pid"] == "" and rows[1]["candidates"] == ["P2", "P3"]
    assert rows[2]["selected_pid"] == "" and rows[2]["candidates"] == ["P4"]
    assert rows[3]["selected_pid"] == "" and rows[3]["candidates"] == []


def test_plan_checks_event_roster_duplicates_and_explicit_overwrites():
    rows = parse_prepost_workbook(workbook_bytes())
    state = dict(eid="E1", roster=[dict(pid="P1"), dict(pid="P2")], tests=[])
    assert len(import_plan(state, rows, ["P1", "P2"])) == 4
    assert len(import_plan(state, rows, ["P1", ""])) == 2
    for selections in (["P1", "P1"], ["P1", "NOT_ON_EVENT"], ["", ""]):
        with pytest.raises(ImportErrorForTest):
            import_plan(state, rows, selections)
    state["tests"] = [dict(pid="P1", type="pre", score=50)]
    with pytest.raises(ImportErrorForTest, match="saved scores differ"):
        import_plan(state, rows, ["P1", "P2"])
    assert len(import_plan(state, rows, ["P1", "P2"], True)) == 4
    rows[0]["post"] = None
    plan = import_plan(state, rows, ["P1", ""], True)
    assert len(plan) == 1 and plan[0]["type"] == "pre"


def test_manual_links_can_pair_different_pre_and_post_name_spellings():
    rows = parse_prepost_workbook(workbook_bytes())
    rows[0]["post"], rows[1]["pre"] = None, None
    state = dict(eid="E1", roster=[dict(pid="P1")], tests=[])
    plan = import_plan(state, rows, ["P1", "P1"])
    assert [(p["pid"], p["type"]) for p in plan] == [("P1", "pre"), ("P1", "post")]


def test_table_and_csv_preserve_missing_zero_and_negative_changes():
    roster = [dict(pid="P1", name="=A1", country="C1"), dict(pid="P2", name="B", country="C2")]
    tests = [dict(pid="P1", type="pre", score=50), dict(pid="P1", type="post", score=0)]
    table = score_table(roster, tests)
    assert table[0]["gain"] == -50 and table[1]["gain"] is None
    result = prepost_csv("E1", table).decode("utf-8-sig")
    assert "E1,P1,'=A1,C1,50,0,-50" in result
    assert "E1,P2,B,C2,,," in result
    assert tests[1]["score"] == 0
