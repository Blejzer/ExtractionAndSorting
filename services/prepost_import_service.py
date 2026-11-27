"""Read PFE question-by-participant workbooks without executing Excel formulas."""

import csv
import math
from io import BytesIO, StringIO
import re
import unicodedata
from zipfile import BadZipFile, ZipFile

from openpyxl import load_workbook
from openpyxl.utils.cell import coordinate_to_tuple
from openpyxl.utils.exceptions import InvalidFileException

from utils.names import participant_name_key

MAX_UPLOAD_BYTES = 5_000_000
MAX_UNCOMPRESSED_BYTES = 30_000_000
MAX_ROWS, MAX_COLUMNS = 1000, 300
QUESTION = re.compile(r"^(?:pitanje|question)\s*([0-9]+)\s*[.:]?\s*$", re.I)
REFERENCE = r"(?:(?:'[^']+'|[\w ]+)!)?\$?[A-Z]{1,3}\$?[1-9][0-9]*"
COMPARISON = re.compile(rf"^=IF\(\s*({REFERENCE})\s*=\s*({REFERENCE})\s*,\s*1\s*,\s*0\s*\)$", re.I)


class TestImportError(ValueError):
    pass


def _text(value):
    return " ".join(str(value).split()) if value is not None else ""


def _reference(workbook, default_sheet, reference):
    parts = reference.rsplit("!", 1)
    sheet = parts[0].strip("'").replace("''", "'") if len(parts) == 2 else default_sheet
    address = parts[-1].replace("$", "").upper()
    row, column = coordinate_to_tuple(address)
    if row > MAX_ROWS or column > MAX_COLUMNS or sheet not in workbook.sheetnames:
        raise TestImportError("An answer formula refers outside the supported workbook area.")
    cell = workbook[sheet][address]
    if cell.data_type in ("f", "e"):
        raise TestImportError(f"{sheet}!{address}: expected a plain answer, not a formula or error.")
    return _text(cell.value).casefold()


def _formula(cell):
    return getattr(cell.value, "text", cell.value) if cell.data_type == "f" else ""


def _numeric(value):
    if type(value) in (int, float) and math.isfinite(value):
        return float(value)
    return None


def _percentage(cell, cached, *, fraction=False, percent_label=False):
    """Read explicitly identified percentages, never guess a raw score's unit."""
    value = cached.value if cell.data_type == "f" else cell.value
    if value is None:
        return None
    if isinstance(value, str) and value.strip().endswith("%"):
        try:
            result = float(value.strip()[:-1].replace(",", ".").strip())
        except ValueError as exc:
            raise TestImportError(f"{cell.coordinate}: invalid percentage.") from exc
    else:
        number = _numeric(value)
        if number is None:
            if percent_label or "%" in cell.number_format or fraction:
                raise TestImportError(f"{cell.coordinate}: invalid percentage.")
            return None
        if "%" in cell.number_format or fraction:
            result = number * 100
        elif percent_label:
            result = number
        else:
            return None
    if not math.isfinite(result) or not 0 <= result <= 100:
        raise TestImportError(f"{cell.coordinate}: percentage must be between 0 and 100.")
    return round(result, 8)


PARTIAL_TEMPLATE = '=MAX(0,(SUMPRODUCT(--ISNUMBER(SEARCH(MID({answer},ROW(INDIRECT("1:"&LEN({answer}))),1),{key})))-(LEN({answer})-SUMPRODUCT(--ISNUMBER(SEARCH(MID({answer},ROW(INDIRECT("1:"&LEN({answer}))),1),{key})))))/LEN({key}))'


def _partial_references(formula):
    """Recognize the supplied partial-credit expression exactly; never eval it."""
    compact = re.sub(r"\s+", "", formula)
    references = list(dict.fromkeys(re.findall(REFERENCE, compact, re.I)))
    if len(references) == 2:
        answer, key = references
        if compact.casefold() == PARTIAL_TEMPLATE.format(answer=answer, key=key).casefold():
            return answer, key
    return None


def _question_value(workbook, sheet, cell):
    formula = _formula(cell)
    comparison = COMPARISON.fullmatch(formula) if isinstance(formula, str) else None
    partial = _partial_references(formula) if formula and not comparison else None
    if comparison or partial:
        answer_ref, key_ref = (comparison[1], comparison[2]) if comparison else partial
        answer = _reference(workbook, sheet.title, answer_ref)
        key = _reference(workbook, sheet.title, key_ref)
        if not key:
            raise TestImportError(f"{sheet.title}!{cell.coordinate}: missing answer key.")
        if comparison:
            return float(bool(answer) and answer == key), bool(answer), "answers"
        matches = sum(character in key for character in answer)
        return max(0, (2 * matches - len(answer)) / len(key)), bool(answer), "answers"
    number = _numeric(cell.value)
    if number is not None and 0 <= number <= 1:
        return number, True, "points"
    if cell.value is None:
        return 0.0, False, "blank"
    raise TestImportError(f"{sheet.title}!{cell.coordinate}: expected 0/1 or fractional question points, an IF(answer=key,1,0) formula, or the supported partial-credit formula.")


def _matrix_scores(workbook, sheet, cached):
    question_rows = []
    for row in range(2, sheet.max_row + 1):
        match = QUESTION.fullmatch(_text(sheet.cell(row, 1).value))
        if match:
            question_rows.append((row, int(match[1])))
        elif question_rows:
            break
    if not question_rows or [n for _, n in question_rows] != list(range(1, len(question_rows) + 1)):
        raise TestImportError(f"{sheet.title}: expected consecutive Pitanje/Question rows starting at 1.")
    header_row, total_row = question_rows[0][0] - 1, question_rows[-1][0] + 1
    has_total = _text(sheet.cell(total_row, 1).value).casefold() in {"total", "average", "ukupno", "percentage", "score"}
    results = []
    for column in range(2, sheet.max_column + 1):
        name = _text(sheet.cell(header_row, column).value)
        if not name or participant_name_key(name) in {"mf", "odgovori", "answers", "answer key", "total", "average", "ukupno"}:
            continue
        values = [_question_value(workbook, sheet, sheet.cell(row, column)) for row, _ in question_rows]
        modes = {mode for _, _, mode in values}
        if "answers" in modes and modes - {"answers", "points"}:
            raise TestImportError(f"{sheet.title}: {name} has a mixture of answer formulas and blank points. Complete the question scores before importing.")
        absent = not any(answered for _, answered, _ in values)
        percentage, method = None, "question_points"
        if has_total and not absent:
            total = sheet.cell(total_row, column)
            mean_formula = bool(re.match(r"^=(?:SUBTOTAL\(\s*(?:1|101)\s*,|AVERAGE\()", str(_formula(total)), re.I))
            percentage = _percentage(total, cached.cell(total_row, column), fraction=mean_formula)
            if percentage is not None:
                method = "reported_percentage"
        if percentage is None and not absent:
            percentage = round(sum(value for value, _, _ in values) * 100 / len(question_rows), 8)
            method = "answer_key" if modes == {"answers"} else "question_points"
        if percentage is not None and not 0 <= percentage <= 100:
            raise TestImportError(f"{sheet.title}: calculated percentage is outside 0–100.")
        score = None if absent else dict(score=percentage, sheet=sheet.title,
                                         column=sheet.cell(header_row, column).column_letter, method=method)
        results.append((name, score))
    if not results:
        raise TestImportError(f"{sheet.title}: no participant columns were found.")
    return results


def _header(value):
    return _text(value).casefold().strip(" :")


NAME_HEADERS = {"select your name", "name", "full name", "participant", "participant name", "name and surname"}


def _forms_header(sheet):
    for row in range(1, min(sheet.max_row, 10) + 1):
        headers = {c.column: _header(c.value) for c in sheet[row] if c.value is not None}
        names = [col for col, label in headers.items() if label in NAME_HEADERS]
        if len(names) == 1:
            return row, names[0], headers
    return None


def _forms_scores(sheet, cached, layout):
    header_row, name_column, headers = layout
    summary_columns = [col for col, label in headers.items() if col < name_column or label in {"score", "percentage", "score (%)", "percent"}]
    point_columns = [col for col, label in headers.items() if re.match(r"^points(?:\s*[-–:]|$)", label)]
    total_columns = [col for col, label in headers.items() if label == "total points"]
    maximum_columns = [col for col, label in headers.items() if label in {"max points", "maximum points", "maximum score"}]
    results = []
    for row in range(header_row + 1, sheet.max_row + 1):
        name = _text(sheet.cell(row, name_column).value)
        if _header(sheet.cell(row, 1).value) in {"total", "average", "ukupno"}:
            continue
        if not name:
            if any(c.value is not None for c in sheet[row]):
                raise TestImportError(f"{sheet.title}, row {row}: missing participant name.")
            continue
        if participant_name_key(name) == "mf":
            continue
        percentages = []
        for column in summary_columns:
            label = headers[column]
            value = _percentage(sheet.cell(row, column), cached.cell(row, column),
                                percent_label=label in {"percentage", "score (%)", "percent"})
            if value is not None:
                percentages.append(value)
        if percentages and any(abs(value - percentages[0]) > 0.000001 for value in percentages[1:]):
            raise TestImportError(f"{sheet.title}, row {row}: conflicting percentage columns.")
        percentage = percentages[0] if percentages else None
        method = "reported_percentage"
        if percentage is None:
            total = _numeric(cached.cell(row, total_columns[0]).value) if len(total_columns) == 1 else None
            points = [_numeric(cached.cell(row, col).value) for col in point_columns]
            maximum = _numeric(cached.cell(row, maximum_columns[0]).value) if len(maximum_columns) == 1 else None
            if total is None and not any(p is not None for p in points):
                results.append((name, None))
                continue
            if maximum is None:
                if not point_columns or any(p is not None and not 0 <= p <= 1 for p in points):
                    raise TestImportError(f"{sheet.title}, row {row}: a percentage or explicit maximum points is needed for weighted scores.")
                maximum = len(point_columns)
            if total is None:
                total = sum(p or 0 for p in points)
            if maximum <= 0 or not 0 <= total <= maximum:
                raise TestImportError(f"{sheet.title}, row {row}: points are outside the stated maximum.")
            percentage = round(total / maximum * 100, 8)
            method = "total_points"
        results.append((name, dict(score=percentage, sheet=sheet.title, row=row, method=method)))
    if not results:
        raise TestImportError(f"{sheet.title}: no participant rows were found.")
    return results


def _select_sheets(workbook, scoring):
    if scoring not in {"partial", "exact"}:
        raise TestImportError("Choose partial credit or all-or-nothing scoring.")
    groups = {"pre": [], "post": []}
    for sheet in workbook:
        title = re.sub(r"[\s_-]", "", sheet.title).casefold()
        match = re.fullmatch(r"(pretest|posttest|prosttest)(bezparcijale)?", title)
        if not match:
            continue
        if sheet.max_row > MAX_ROWS or sheet.max_column > MAX_COLUMNS:
            raise TestImportError("The workbook exceeds the supported 1000 rows / 300 columns per sheet.")
        kind = "pre" if match[1] == "pretest" else "post"
        partial = any("SUMPRODUCT" in str(_formula(cell)).upper() for row in sheet.iter_rows(min_row=2, max_row=min(sheet.max_row, 4)) for cell in row)
        variant = "exact" if match[2] else "partial" if partial else "standard"
        groups[kind].append((sheet, variant))
    selected = {}
    for kind, candidates in groups.items():
        if len(candidates) > 1:
            candidates = [(sheet, variant) for sheet, variant in candidates if variant == scoring]
        if len(candidates) > 1 or not candidates and groups[kind]:
            raise TestImportError(f"More than one sheet identifies the same test attempt ({kind}), or no sheet matches the scoring choice.")
        if candidates:
            selected[kind] = candidates[0][0]
    if not selected:
        raise TestImportError("Expected a PreTest and/or PostTest worksheet.")
    return selected


def parse_prepost_workbook(data, scoring="partial"):
    if not data or len(data) > MAX_UPLOAD_BYTES:
        raise TestImportError("Choose an .xlsx file of at most 5 MB.")
    workbook = cached = None
    try:
        with ZipFile(BytesIO(data)) as archive:
            if sum(entry.file_size for entry in archive.infolist()) > MAX_UNCOMPRESSED_BYTES:
                raise TestImportError("The expanded workbook is too large.")
        workbook = load_workbook(BytesIO(data), data_only=False, keep_links=False)
        cached = load_workbook(BytesIO(data), data_only=True, keep_links=False)
        rows = {}
        for attempt, sheet in _select_sheets(workbook, scoring).items():
            layout = _forms_header(sheet)
            scores = _forms_scores(sheet, cached[sheet.title], layout) if layout else _matrix_scores(workbook, sheet, cached[sheet.title])
            for name, score in scores:
                key = participant_name_key(name)
                row = rows.setdefault(key, dict(name=name, pre=None, post=None))
                if attempt in row.get("present", []):
                    raise TestImportError(f"{sheet.title}: duplicate participant entry for {name}. Rename or remove it before importing.")
                row.setdefault("present", []).append(attempt)
                row[attempt] = score
        result = list(rows.values())
        if not any(row["pre"] is not None or row["post"] is not None for row in result):
            raise TestImportError("No completed test attempts were found.")
        return result
    except TestImportError:
        raise
    except (BadZipFile, ValueError, KeyError, OSError, SyntaxError, InvalidFileException) as exc:
        raise TestImportError("The file could not be read as an Excel .xlsx workbook.") from exc
    finally:
        if workbook is not None:
            workbook.close()
        if cached is not None:
            cached.close()


def match_test_rows(rows, roster):
    """Case/space-insensitive exact names auto-match only a unique roster entry."""
    exact, accent = {}, {}
    def folded(name):
        return "".join(c for c in unicodedata.normalize("NFD", participant_name_key(name).replace("đ", "d")) if not unicodedata.combining(c))
    for person in roster:
        exact.setdefault(participant_name_key(person["name"]), []).append(person["pid"])
        accent.setdefault(folded(person["name"]), []).append(person["pid"])
    for row in rows:
        candidates = exact.get(participant_name_key(row["name"]), [])
        row["selected_pid"] = candidates[0] if len(candidates) == 1 else ""
        row["match_status"] = "Matched" if len(candidates) == 1 else "Ambiguous name — select participant" if candidates else "Select participant"
        row["candidates"] = candidates or accent.get(folded(row["name"]), [])
    return rows


def score_table(roster, tests):
    lookup = {person["pid"]: dict(**person, pre=None, post=None) for person in roster}
    for test in tests:
        if test.get("type") not in ("pre", "post"):
            continue
        pid = test["pid"]
        person = lookup.setdefault(pid, dict(pid=pid, name=pid, country="", pre=None, post=None))
        person[test["type"]] = test
    for person in lookup.values():
        person["gain"] = round(person["post"]["score"] - person["pre"]["score"], 8) if person["pre"] is not None and person["post"] is not None else None
    return sorted(lookup.values(), key=lambda p: participant_name_key(p["name"]))


def prepost_csv(eid, table):
    stream = StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(["event_id", "participant_id", "name", "country", "pre_percent", "post_percent", "change_percentage_points"])
    def safe(value):
        value = str(value)
        return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value
    for row in table:
        writer.writerow([safe(eid), safe(row["pid"]), safe(row["name"]), safe(row["country"]),
                         row["pre"]["score"] if row["pre"] is not None else "",
                         row["post"]["score"] if row["post"] is not None else "",
                         row["gain"] if row["gain"] is not None else ""])
    return stream.getvalue().encode("utf-8-sig")
