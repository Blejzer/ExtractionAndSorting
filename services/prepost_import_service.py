"""Read PFE question-by-participant workbooks without executing Excel formulas."""

import csv
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


def _sheet_scores(workbook, sheet):
    question_rows = []
    for row in range(2, sheet.max_row + 1):
        match = QUESTION.fullmatch(_text(sheet.cell(row, 1).value))
        if match:
            question_rows.append((row, int(match[1])))
        elif question_rows:
            break
    if not question_rows or [n for _, n in question_rows] != list(range(1, len(question_rows) + 1)):
        raise TestImportError(f"{sheet.title}: expected consecutive Pitanje/Question rows starting at 1.")
    header_row = question_rows[0][0] - 1
    results = []
    for column in range(2, sheet.max_column + 1):
        name = _text(sheet.cell(header_row, column).value)
        if not name or participant_name_key(name) in {"mf", "odgovori", "answers", "answer key", "total", "average"}:
            continue
        points, answers, modes, answered = [], [], set(), 0
        for row, number in question_rows:
            cell = sheet.cell(row, column)
            match = COMPARISON.fullmatch(str(cell.value)) if cell.data_type == "f" else None
            if match:
                answer = _reference(workbook, sheet.title, match[1])
                correct = _reference(workbook, sheet.title, match[2])
                if not correct:
                    raise TestImportError(f"{sheet.title}!{cell.coordinate}: missing answer key.")
                points.append(int(bool(answer) and answer == correct))
                answers.append(answer)
                answered += bool(answer)
                modes.add("answers")
            elif cell.value in (0, 1) and cell.data_type != "e" and type(cell.value) in (int, float):
                points.append(int(cell.value))
                answers.append(None)
                answered += 1
                modes.add("points")
            elif cell.value is None:
                points.append(0)
                answers.append(None)
                modes.add("blank")
            else:
                raise TestImportError(f"{sheet.title}!{cell.coordinate}: expected 0/1 points or an IF(answer=key,1,0) formula.")
        if "answers" in modes and modes != {"answers"}:
            raise TestImportError(f"{sheet.title}: {name} has a mixture of answer formulas and points. Use one scoring method per column.")
        # An absent attempt must not become a zero simply because IF(blank=key) returns zero.
        absent = not any(answers) if modes == {"answers"} else modes == {"blank"}
        count = len(question_rows)
        score = None if absent else dict(
            score=round(sum(points) * 100 / count, 8), correct=sum(points), question_count=count,
            answered=answered,
            points=points, answers=answers, sheet=sheet.title,
            column=sheet.cell(header_row, column).column_letter,
            method="answer_key" if modes == {"answers"} else "question_points",
        )
        results.append((name, score))
    if not results:
        raise TestImportError(f"{sheet.title}: no participant columns were found.")
    return results


def parse_prepost_workbook(data):
    if not data or len(data) > MAX_UPLOAD_BYTES:
        raise TestImportError("Choose an .xlsx file of at most 5 MB.")
    try:
        with ZipFile(BytesIO(data)) as archive:
            if sum(entry.file_size for entry in archive.infolist()) > MAX_UNCOMPRESSED_BYTES:
                raise TestImportError("The expanded workbook is too large.")
        workbook = load_workbook(BytesIO(data), data_only=False, keep_links=False)
    except TestImportError:
        raise
    except (BadZipFile, ValueError, KeyError, OSError, SyntaxError, InvalidFileException) as exc:
        raise TestImportError("The file could not be read as an Excel .xlsx workbook.") from exc
    try:
        sheets = {}
        for sheet in workbook:
            kind = re.sub(r"[\s_-]", "", sheet.title).casefold()
            if kind not in ("pretest", "posttest"):
                continue
            if kind in sheets:
                raise TestImportError("More than one sheet identifies the same test attempt.")
            if sheet.max_row > MAX_ROWS or sheet.max_column > MAX_COLUMNS:
                raise TestImportError("The workbook exceeds the supported 1000 rows / 300 columns per sheet.")
            sheets[kind] = sheet
        if not sheets:
            raise TestImportError("Expected a PreTest and/or PostTest worksheet.")
        rows = {}
        for sheet_name, sheet in sheets.items():
            attempt = "pre" if sheet_name == "pretest" else "post"
            for name, score in _sheet_scores(workbook, sheet):
                key = participant_name_key(name)
                row = rows.setdefault(key, dict(name=name, pre=None, post=None))
                if attempt in row.get("present", []):
                    raise TestImportError(f"{sheet.title}: duplicate participant column for {name}. Rename or remove it before importing.")
                row.setdefault("present", []).append(attempt)
                row[attempt] = score
        result = list(rows.values())
        if not any(row["pre"] is not None or row["post"] is not None for row in result):
            raise TestImportError("No completed test attempts were found.")
        return result
    finally:
        workbook.close()


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
