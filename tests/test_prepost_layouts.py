"""Fictional examples of the Forms, matrix, cached and partial-credit layouts."""

from io import BytesIO
from zipfile import ZipFile
from xml.etree import ElementTree

from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.formula import ArrayFormula
import pytest

from services.prepost_import_service import TestImportError as ImportErrorForTest, parse_prepost_workbook
from tests.test_prepost_import import serialize, workbook_bytes, edited


def forms_bytes(*, percentage=True, weighted=False, maximum=False):
    book = Workbook()
    book.remove(book.active)
    for title in ("Pre Test", "Post Test"):
        sheet = book.create_sheet(title)
        sheet.append(["ID", "Start time", "Total points", "Score" if title == "Pre Test" else "Quiz feedback",
                      "SELECT YOUR NAME:", "Question one", "Points - Question one", "Question two",
                      "Points - Question two", "Question three", "Points", "Maximum points"])
        names = ["Ana EXAMPLE", "Bojan SAMPLE"] if title == "Pre Test" else ["Bojan SAMPLE", "Ana EXAMPLE"]
        for number, name in enumerate(names, 1):
            points = [1, 0, 1] if title == "Pre Test" else [1, 1, 1]
            if weighted:
                points = [2, 1, 1]
            sheet.append([number, "2026-02-23", sum(points), 0.6667 if percentage and title == "Pre Test" else None,
                          name, "answer", points[0], "answer", points[1], "answer", points[2], 5 if maximum else None])
            sheet.cell(number + 1, 4).number_format = "0.00%"
        sheet.append(["Total"])
    return serialize(book)


def partial_bytes():
    book = Workbook()
    book.remove(book.active)
    for title in ("Pre Test - bez parcijale", "Pre Test", "Post Test - bez parcijale", "Prost Test"):
        sheet = book.create_sheet(title)
        exact = "bez parcijale" in title
        post = title.startswith(("Post", "Prost"))
        sheet.append(["Pitanja", "Ana EXAMPLE", "UKUPNO"])
        sheet["J1"] = "Odgovori"
        for number, (key, answer) in enumerate(zip(["ab", "ab", "bc"], ["ab", "ab", "bc"] if post else ["a", "ac", "bc"]), 1):
            r = number + 1
            sheet.cell(r, 1, f"Pitanje {number}")
            sheet.cell(r, 10, key)
            sheet.cell(r + 7, 2, answer)
            formula = f'=MAX(0,(SUMPRODUCT(--ISNUMBER(SEARCH(MID(B{r+7},ROW(INDIRECT("1:"&LEN(B{r+7}))),1),$J{r})))-(LEN(B{r+7})-SUMPRODUCT(--ISNUMBER(SEARCH(MID(B{r+7},ROW(INDIRECT("1:"&LEN(B{r+7}))),1),$J{r})))))/LEN($J{r}))'
            sheet.cell(r, 2, f"=IF(B{r+7}=$J{r},1,0)" if exact else ArrayFormula(ref=f"B{r}", text=formula))
            sheet.cell(r, 3, "=SUM(B2:B4)")  # Aggregate column must not become a participant.
        sheet["A5"], sheet["B5"] = "Total", "=AVERAGE(B2:B4)"
        sheet["B5"].number_format = "0.00%"
    return serialize(book)


def with_cache(data, coordinate, value, sheet_index=1):
    """Model an Excel-saved formula value without requiring an Excel runtime."""
    result = BytesIO()
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with ZipFile(BytesIO(data)) as source, ZipFile(result, "w") as target:
        for entry in source.infolist():
            content = source.read(entry.filename)
            if entry.filename == f"xl/worksheets/sheet{sheet_index}.xml":
                root = ElementTree.fromstring(content)
                cell = root.find(f'.//x:c[@r="{coordinate}"]', namespace)
                cell.find('x:v', namespace).text = str(value)
                content = ElementTree.tostring(root)
            target.writestr(entry, content)
    return result.getvalue()


def test_forms_uses_existing_percentage_and_calculates_missing_percent_from_points():
    rows = parse_prepost_workbook(forms_bytes())
    assert len(rows) == 2
    for row in rows:
        assert row["pre"]["score"] == 66.67  # Preserve reported rounding, not 66.66666667.
        assert row["pre"]["method"] == "reported_percentage"
        assert row["post"]["score"] == 100
        assert row["post"]["method"] == "total_points"
        assert set(row["pre"]) == {"score", "sheet", "row", "method"}
    assert rows[0]["pre"]["row"] == 2 and rows[0]["post"]["row"] == 3


def test_forms_calculates_both_tests_without_reported_percentages():
    rows = parse_prepost_workbook(forms_bytes(percentage=False))
    assert rows[0]["pre"]["score"] == pytest.approx(200 / 3)
    assert rows[0]["post"]["score"] == 100


def test_weighted_forms_need_explicit_maximum():
    with pytest.raises(ImportErrorForTest, match="maximum points"):
        parse_prepost_workbook(forms_bytes(percentage=False, weighted=True))
    rows = parse_prepost_workbook(forms_bytes(percentage=False, weighted=True, maximum=True))
    assert rows[0]["pre"]["score"] == 80 and rows[0]["post"]["score"] == 80


def test_partial_credit_is_default_and_alternatives_never_mix():
    partial = parse_prepost_workbook(partial_bytes())
    exact = parse_prepost_workbook(partial_bytes(), scoring="exact")
    assert len(partial) == len(exact) == 1
    assert partial[0]["pre"]["sheet"] == "Pre Test"
    assert partial[0]["post"]["sheet"] == "Prost Test"
    assert partial[0]["pre"]["score"] == 50
    assert exact[0]["pre"]["score"] == pytest.approx(100 / 3)
    assert partial[0]["post"]["score"] == exact[0]["post"]["score"] == 100
    assert "bez parcijale" in exact[0]["pre"]["sheet"]


def test_cached_percentage_is_used_without_recalculating_reported_rounding():
    data = with_cache(workbook_bytes(), "B5", 0.6667)
    rows = parse_prepost_workbook(data)
    assert rows[0]["pre"]["score"] == 66.67
    assert rows[0]["pre"]["method"] == "reported_percentage"
    assert rows[1]["pre"]["score"] == pytest.approx(200 / 3)


def test_fractional_question_points_calculate_percentages():
    data = edited(lambda w: setattr(w["PreTest"]["B2"], "value", 0.5), formulas=False)
    assert parse_prepost_workbook(data)[0]["pre"]["score"] == 50


def test_text_percentages_with_decimal_comma_are_supported():
    book = load_workbook(BytesIO(forms_bytes()))
    book["Pre Test"]["D2"] = "66,67%"
    assert parse_prepost_workbook(serialize(book))[0]["pre"]["score"] == 66.67


def test_percentages_do_not_accept_values_above_100():
    book = load_workbook(BytesIO(forms_bytes()))
    book["Pre Test"]["D2"] = 1.2
    with pytest.raises(ImportErrorForTest, match="between 0 and 100"):
        parse_prepost_workbook(serialize(book))


def test_unknown_scoring_preference_and_duplicate_alternative_sheets_are_rejected():
    with pytest.raises(ImportErrorForTest, match="scoring"):
        parse_prepost_workbook(partial_bytes(), scoring="unknown")
    book = load_workbook(BytesIO(workbook_bytes()))
    book.copy_worksheet(book["PreTest"]).title = "Pre Test"
    with pytest.raises(ImportErrorForTest, match="same test attempt"):
        parse_prepost_workbook(serialize(book))


def raw_summary_bytes(*, names=True, caches=False):
    book=Workbook()
    book.remove(book.active)
    for title in ("PreTest","PostTest"):
        sheet=book.create_sheet(title)
        sheet.append([None,"Ana EXAMPLE" if names or title=="PostTest" else 1,
                      "Bojan SAMPLE" if names or title=="PostTest" else 2,
                      "Answer key","Correct","Question rate","Incorrect","Blank"])
        answers=[["a","b","x"],["a","x","x"]] if title=="PreTest" else [["a","b","c"],["a","b","x"]]
        for number,key in enumerate(["a","b","c"],1):
            row=number+1
            sheet.append([number,answers[0][number-1],answers[1][number-1],key,
                          f"=COUNTIF(B{row}:C{row},D{row})",f"=E{row}/2",None,None])
            sheet.cell(row,6).number_format="0.00%"
        sheet.append(["SCORE",ArrayFormula(ref="B5",text="=SUMPRODUCT(--(B2:B4=$D$2:$D$4))"),
                      ArrayFormula(ref="C5",text="=SUMPRODUCT(--(C2:C4=$D$2:$D$4))"),None,"=SUM(E2:E4)"])
        sheet.append(["PERC","=B5/3","=C5/3",None,"=E5/(2*3)"])
        for column in (2,3,5):sheet.cell(6,column).number_format="0.00%"
    data=serialize(book)
    if caches:
        for index,totals in [(1,[2,1]),(2,[3,2])]:
            for col,points in zip(["B","C"],totals):
                data=with_cache(data,col+"5",points,index)
                data=with_cache(data,col+"6",points/3,index)
    return data


@pytest.mark.parametrize("caches",[False,True])
def test_raw_answer_score_percentage_rows_import_participants_not_question_rates(caches):
    rows=parse_prepost_workbook(raw_summary_bytes(caches=caches))
    assert len(rows)==2
    assert [r["name"] for r in rows]==["Ana EXAMPLE","Bojan SAMPLE"]
    assert rows[0]["pre"]["score"]==pytest.approx(200/3)
    assert rows[0]["post"]["score"]==100
    assert rows[1]["pre"]["score"]==pytest.approx(100/3)
    assert rows[1]["post"]["score"]==pytest.approx(200/3)
    assert all(not r["needs_identity_review"] for r in rows)
    assert all(s["cell"].startswith(("B","C")) for r in rows for s in (r["pre"],r["post"]))


def test_raw_answer_matrix_without_pre_names_preserves_manual_cross_reference():
    rows=parse_prepost_workbook(raw_summary_bytes(names=False))
    assert len(rows)==4
    assert sum(r["pre"] is not None for r in rows)==2
    assert sum(r["post"] is not None for r in rows)==2
    assert sum(r["needs_identity_review"] for r in rows)==2
    assert all(r["pre"] is None for r in rows if not r["needs_identity_review"])
