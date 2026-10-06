"""Uniform, formula-safe exports of reviewed Word extraction rows."""

import csv
from io import StringIO

from services.word_extraction_service import FIELDS


EXPORT_COLUMNS = {"source_file": "Source file", "source_refs": "Source locations", **FIELDS,
                  "match_status": "Database check", "matching_pids": "Matching PIDs",
                  "match_reasons": "Match reasons", "review_notes": "Review notes"}


def export_rows(records):
    for record in records:
        yield {**record["fields"], "source_file": record["source_file"], "source_refs": "\n".join(record["sources"]),
               "match_status": record["match_status"],
               "matching_pids": "; ".join(m["pid"] for m in record["matches"]),
               "match_reasons": "\n".join(m["pid"] + ": " + "; ".join(m["reasons"]) for m in record["matches"]),
               "review_notes": "\n".join(record["warnings"] + record.get("match_notes", []))}


def export_csv(records):
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(EXPORT_COLUMNS.values())
    for row in export_rows(records):
        values = [str(row.get(key, "")) for key in EXPORT_COLUMNS]
        writer.writerow(["'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value for value in values])
    return output.getvalue().encode("utf-8-sig")
